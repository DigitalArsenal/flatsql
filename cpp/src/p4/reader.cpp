// Store format 4: reads (design §5.4, CONTRACT §3.5-§3.9, C-37 (6), (11),
// C-38).
//
// Reads never wait on the data layer: they take no lock a writer holds across
// I/O, open files read-write without CREATE and query_only, and hold one short
// read transaction per page (a connection is checked out of the shared pool
// for one page and returned). A stream resumes by key. SQLITE_BUSY and I/O
// errors are errors, never misses (M9).
//
// One scan engine serves every record op and p4_reader.h's cursors. A scan
// first picks its feed files: the lane filter's feeds (provider and source
// name a feed file; a batch, content key or producer peer narrows to the
// feeds holding such an instance), else every feed of the type. Each file is
// walked in the scan's order through its own indexes (r_s, r_w, r_c, r_ke) a
// chunk at a time, and the walks are merged (C-38 (4): ties by feed id), so a
// record held by N feeds answers once per feed (C-38 (5)). A page of
// candidates is resolved by reading each record's rows (one rid range of its
// file, one read transaction per file); a record answers with the first
// matching row's copy (the lowest token) and the earliest matching instance
// of its feed, or, in a newest-first page (SCAN 5/6), the newest one: the
// delivery the page orders it by (C-41 N9). provider and source come from
// the row's feed file, batch and the rest from the row's ids: never blank
// when the record has a tag. A CID is looked up in each feed file's CID
// index.
//
// The random-keyed indexes (r_c, r_ke, r_w) hold merged rows only: a walk or
// probe through them also takes the feed's staged rows from its view, which
// each step (a walk's chunk, the predicates' probes, an EPOCH pass over a
// file) pins before its SQL transaction starts and lets go when it ends, so
// a view outlives no step and no wait on the host (staged.cpp). A row merged
// after the pin comes from both with the same key and seq and is taken once
// (a record's entries are adjacent in every order and collapse); one merged
// before it is in the step's SQL snapshot, which starts after the pin.
#include <algorithm>
#include <cmath>
#include <functional>
#include <queue>
#include <set>

#include "internal.h"
#include "sql_bridge.h"

namespace flatsql {
namespace p4 {

// ---- the reader pool ------------------------------------------------------------------------------
Conn* ReaderPool::acquire(const std::string& path, OpenKind kind, int* rc, std::string* err) {
    {
        std::lock_guard<std::mutex> g(mu_);
        auto it = idle_.find(path);
        if (it != idle_.end()) {
            Conn* c = it->second;
            unlinkIdle(c);
            return c;
        }
    }
    // A feed file of large records (IQC: ~1.9 KB) gets a page cache that
    // holds as many rows as the default holds of 600-byte ones (up to 8x the
    // default); it counts that much against the pool's budget.
    Conn* c = nullptr;
    int r = openConn(path, kind, cacheKiB_, 0, &c, err);
    if ((r & 0xff) == SQLITE_NOMEM) {
        closeAll();  // the heap is full: idle readers give way, then once more
        r = openConn(path, kind, cacheKiB_, 0, &c, err);
    }
    if (r != SQLITE_OK) {
        *rc = r;
        return nullptr;
    }
    c->cacheKiB = cacheKiB_;
    const bool feedFile = path.size() > 3 && path.compare(path.size() - 3, 3, ".db") == 0;
    if (sqlite3_stmt* m = feedFile ? c->sql("SELECT k, v FROM meta WHERE k IN ('rows','bytes')") : nullptr) {
        int64_t n = 0, bytes = 0;
        while (sqlite3_step(m) == SQLITE_ROW) {
            const char* k = reinterpret_cast<const char*>(sqlite3_column_text(m, 0));
            if (k && std::strcmp(k, "rows") == 0) n = sqlite3_column_int64(m, 1);
            else if (k && std::strcmp(k, "bytes") == 0) bytes = sqlite3_column_int64(m, 1);
        }
        sqlite3_reset(m);
        if (n > 0 && bytes / n > 600) {
            const int64_t kib = std::min<int64_t>(int64_t(cacheKiB_) * 8, int64_t(cacheKiB_) * (bytes / n) / 600);
            char sql[64];
            std::snprintf(sql, sizeof sql, "PRAGMA cache_size=-%lld", (long long)kib);
            if (c->exec(sql) == SQLITE_OK) c->cacheKiB = uint32_t(kib);
        }
    }
    open_.fetch_add(1);
    std::lock_guard<std::mutex> g(mu_);
    usedKiB_ += c->cacheKiB;
    return c;
}

void ReaderPool::unlinkIdle(Conn* v) {
    auto pit = pos_.find(v);
    if (pit != pos_.end()) {
        lru_.erase(pit->second);
        pos_.erase(pit);
    }
    auto range = idle_.equal_range(v->path);
    for (auto it = range.first; it != range.second; ++it)
        if (it->second == v) {
            idle_.erase(it);
            break;
        }
}

// Idle connections close, oldest first, while the pool is over its count or
// its cache budget; while the engine's heap is past the pressure line, up to
// an eighth of the pool more per release (the heap falls as they close).
void ReaderPool::trim(std::vector<Conn*>* close) {
    const bool pressed = pressure_ && p4sql_heap_used() > pressure_;
    size_t extra = 0;
    while (!lru_.empty()) {
        const bool over = open_.load() > cap_ || usedKiB_ > budgetKiB_;
        if (!over && !(pressed && extra < std::max<size_t>(1, cap_ / 8))) break;
        if (!over) extra++;
        Conn* v = lru_.back();
        unlinkIdle(v);
        usedKiB_ -= std::min<uint64_t>(usedKiB_, v->cacheKiB);
        close->push_back(v);
        open_.fetch_sub(1);
    }
}

void ReaderPool::release(Conn* c) {
    if (!c) return;
    // An idle connection holds no read transaction: a statement left
    // mid-step (a read that stopped at its limit) would pin its WAL snapshot
    // and hold every checkpoint of the file behind it.
    for (sqlite3_stmt* s = sqlite3_next_stmt(c->db, nullptr); s; s = sqlite3_next_stmt(c->db, s))
        if (sqlite3_stmt_busy(s)) sqlite3_reset(s);
    if (!sqlite3_get_autocommit(c->db)) c->exec("ROLLBACK");
    std::vector<Conn*> close;
    {
        std::lock_guard<std::mutex> g(mu_);
        idle_.emplace(c->path, c);
        lru_.push_front(c);
        pos_[c] = lru_.begin();
        trim(&close);
    }
    for (Conn* v : close) delete v;
}

void ReaderPool::dropPath(const std::string& path) {
    std::vector<Conn*> close;
    {
        std::lock_guard<std::mutex> g(mu_);
        auto range = idle_.equal_range(path);
        for (auto it = range.first; it != range.second; ++it) close.push_back(it->second);
        for (Conn* v : close) {
            unlinkIdle(v);
            usedKiB_ -= std::min<uint64_t>(usedKiB_, v->cacheKiB);
        }
        open_.fetch_sub(uint32_t(close.size()));
    }
    for (Conn* v : close) delete v;
}

void ReaderPool::closeAll() {
    std::vector<Conn*> close;
    {
        std::lock_guard<std::mutex> g(mu_);
        for (auto& kv : idle_) close.push_back(kv.second);
        idle_.clear();
        lru_.clear();
        pos_.clear();
        usedKiB_ = 0;
        open_.fetch_sub(uint32_t(close.size()));
    }
    for (Conn* v : close) delete v;
}

namespace {

// |e - at|, exact over the whole int64 range (an epoch clamped to it, C-46 (1),
// and any instant): the EPOCH point profiles rank by it first.
uint64_t epochDistance(int64_t e, int64_t at) { return e > at ? uint64_t(e) - uint64_t(at) : uint64_t(at) - uint64_t(e); }

// The stream generation of a feed file's read transaction, which the caller
// has begun and not yet read in: its first read (the file's meta) fixes the
// snapshot. A generation a compaction has since replaced and no longer keeps
// starts the transaction over (bounded).
int32_t snapStream(Feed* f, Conn* c, std::shared_ptr<Stream>* out) {
    out->reset();
    for (int tries = 0; tries < 4; tries++) {
        if (tries) {
            c->exec("COMMIT");
            c->exec("BEGIN");
        }
        uint32_t gen = 0;
        int32_t st = snapGen(c, &gen);
        if (st != P4_OK) return st;
        *out = streamAt(f, gen, &st);
        if (st != P4_OK) return st;
        if (*out) return P4_OK;
    }
    return P4_E_BUSY;
}

// ---- the scan ---------------------------------------------------------------------------------------
struct FRef {
    Feed* f = nullptr;
    uint32_t fid = 0;
    std::string path, provider, source;
    bool local = false, indexed = true;
    Counters k;
};

// A row of a feed file, as a scan reads it.
struct RowV {
    int fi = -1;  // FRef index
    int64_t rid = 0;
    uint32_t n = 0, b = 0, c = 0, u = 0;
    int64_t at = 0, ts = 0, len = 0, e = 0, off = -1;
    bool hasE = false;
    KVal k;
    uint8_t key[32];
    std::string sig, fcols, data;
    bool sealed = false, hasData = false;
    std::string producer, peer;
    bool inst = false;
    std::string batch, ppeer, pkey, ckey, url;
};

// A record answered (its row set in one feed file): its chosen row and
// matched tag.
struct Row {
    int64_t seq = 0, ts = 0, e = 0, w = 0, len = 0;
    uint32_t fid = 0;
    bool hasE = false;
    uint8_t key[32];
    KVal k;
    std::string producer, peer, sig, data, fcols;
    bool sealed = false, hasData = false;
    bool hasTag = false;
    std::string provider, source, url, batch, ckey, ppeer, pkey;
    int64_t at = 0;
    std::vector<int64_t> ats;  // every matching instance's at (HEAD max_at)
};

// A candidate record: its seq in one feed file (FRef index), and what the
// walk knows of it. A keyed walk (w, delivery time, ts) meets a record once
// per order value its rows carry; the candidate is answered only at the
// value of the row it answers with (checkOv), so the record answers once.
struct Cand {
    int64_t seq = 0;
    int fi = 0;
    uint8_t key[32] = {};
    bool haveKey = false;
    int64_t w = 0;
    int64_t ov = 0;
    bool checkOv = false;
};

struct Spec2 {
    std::string type;
    // lane filter: empty = any
    bool lane = false;
    std::string lf[6];
    bool lfSet[6] = {};
    bool hasCid = false;
    uint8_t cidKey[32];
    std::string peer, producer;
    bool hasPeer = false, hasProducer = false;
    int64_t seqAfter = 0, seqThrough = 0;
    struct Pred {
        uint8_t field, op;
        std::vector<ps::rb1::Cell> vals;
    };
    std::vector<Pred> preds;
    std::string search;
    int order = 0;  // 0: the op's default
    int part = 0;   // 0 every file; 1 the feed files (tagged records); 2 the local file (C-39 two-part pages,
                    // "<TYPE>@local"); 3 the local file plus the feed files the lane filter selects (C-43 B2:
                    // format 1's "local" partition, its untagged records and any feed whose source is "local")
    bool hydrate = false;
    uint64_t limit = 0, offset = 0, bound = 0;
    // internal
    bool needTags = true;     // REC tag columns
    bool eNotNull = false;    // INDEX_PAGE phase 1, EPOCH windows
    bool eNull = false;       // INDEX_PAGE phase 2
    bool wAsc = false;        // EPOCH window (w ascending)
    int64_t wLo = INT64_MIN, wHi = INT64_MAX;  // w range from the predicates
};

int cmpCell(const ps::rb1::Cell& a, int64_t i, const std::string* s, bool isInt) {
    // SQLite ordering: NULL < numbers < text < blob; numbers compare numerically.
    if (isInt) {
        if (a.type == ps::rb1::kInt) return i < a.i ? -1 : i > a.i;
        if (a.type == ps::rb1::kReal) return double(i) < a.d ? -1 : double(i) > a.d;
        if (a.type == ps::rb1::kNull) return 1;
        return -1;  // number < text/blob
    }
    if (a.type == ps::rb1::kText || a.type == ps::rb1::kBlob) {
        const int c = std::memcmp(s->data(), a.s.data(), std::min(s->size(), a.s.size()));
        if (c) return c < 0 ? -1 : 1;
        return s->size() < a.s.size() ? -1 : s->size() > a.s.size();
    }
    return 1;  // text > number/null
}

bool predOn(const Spec2::Pred& p, bool present, int64_t i, const std::string* s, bool isInt) {
    if (p.op == P4_OP_NOTNULL) return present;
    if (!present) return false;
    auto cmp = [&](size_t k) { return cmpCell(p.vals[k], i, s, isInt); };
    switch (p.op) {
        case P4_OP_EQ: return p.vals.size() == 1 && cmp(0) == 0;
        case P4_OP_NE: return p.vals.size() == 1 && cmp(0) != 0;
        case P4_OP_LT: return p.vals.size() == 1 && cmp(0) < 0;
        case P4_OP_LE: return p.vals.size() == 1 && cmp(0) <= 0;
        case P4_OP_GT: return p.vals.size() == 1 && cmp(0) > 0;
        case P4_OP_GE: return p.vals.size() == 1 && cmp(0) >= 0;
        case P4_OP_BETWEEN: return p.vals.size() == 2 && cmp(0) >= 0 && cmp(1) <= 0;
        case P4_OP_IN:
            for (size_t k = 0; k < p.vals.size(); k++)
                if (cmp(k) == 0) return true;
            return false;
        case P4_OP_LIKE: {
            if (p.vals.size() != 1 || p.vals[0].type != ps::rb1::kText) return false;
            const std::string text = isInt ? std::to_string(i) : *s;
            return sqlite3_strlike(p.vals[0].s.c_str(), text.c_str(), 0) == 0;
        }
        default: return false;
    }
}

// The order of a record's matching instances, the first answers: the
// earliest at (§3.6), or the newest (a newest-first page, C-41 N9), then the
// smallest identity.
int tagCmp(const Row& a, const RowV& b, const FRef& fb, bool newest) {
    if (a.at != b.at) return (a.at < b.at) != newest ? -1 : 1;
    const std::string* x[6] = {&a.provider, &a.source, &a.batch, &a.ckey, &a.ppeer, &a.pkey};
    const std::string* y[6] = {&fb.provider, &fb.source, &b.batch, &b.ckey, &b.ppeer, &b.pkey};
    for (int i = 0; i < 6; i++) {
        const int c = x[i]->compare(*y[i]);
        if (c) return c < 0 ? -1 : 1;
    }
    return 0;
}

// An entry of a feed file's walk: one of its rows, in the walk's order (ov:
// a keyed walk's order value, the row's w, at or ts).
struct Ent {
    int64_t seq = 0, w = 0, rid = 0, ov = 0;
    uint8_t key[32];
    bool haveKey = false;
};

// A feed file's walk in the scan's order (C-38 (4)): the entries ahead, read
// a chunk at a time, and where the walk resumes. A W walk reads whole w
// groups and puts each in CID order (r_w holds no CID).
struct Stream {
    int fi = 0;
    bool done = false, started = false;
    std::deque<Ent> buf;
    int64_t rKey = 0;             // SEQ: the last seq (r_s) or rid (rows); keyed: the last whole key group
    std::string rCid;             // CID: the last entry's cid
    int64_t rRid = 0;             // CID: the last entry's rid
    int64_t lastSeq = INT64_MIN;  // the last record the walk took (a record's rows are adjacent)
};

// Keyed walks: kWDesc / kWAsc (r_w), kAtDesc (r_a: delivery time desc, CID
// asc), kAtDescSeq (r_a: delivery time desc, seq desc), kTsDesc (r_t, the
// local file: ts desc, CID asc).
enum WalkOrder { kSeqAsc, kSeqDesc, kWDesc, kWAsc, kCid, kAtDesc, kAtDescSeq, kTsDesc };

class Scan {
public:
    Scan(P4Lane* L, Type* t, Spec2 spec) : L_(L), e_(L->e), t_(t), s_(std::move(spec)) {}
    int32_t open();
    // 1 row, 0 end, < 0 status. The row stays valid until the next call.
    int32_t next(Row** out);
    int64_t vis() const { return vis_; }
    const Spec* spec() const { return sp_.get(); }
    Type* type() const { return t_; }
    int32_t cols(const Row& r, ps::Extracted* x, uint8_t* scratch, size_t n);
    // EPOCH points (2 nearest, 3 as_of, 4 forward): one pick per entity over
    // the scan's feed files.
    struct Pick {
        int64_t e = 0, seq = 0;
        uint8_t key[32];
        uint32_t fid = 0;
    };
    // What a count or a limited page needs of the points (C-46 (5), the
    // push-down of a58f818 and 226872c). Every profile ranks by the distance
    // first, so an entity's best pick is within maxDelta iff one feed file's
    // pick is. A count counts an entity at its first such pick and probes it
    // no more. A page of the first n entities (text order) with a pick within
    // maxDelta keeps those n confirmed; no entity past the n-th confirmed one
    // is probed (it can never return: the n-th only moves down), and a
    // text-keyed object walk stops there (okey order is text order). pruned:
    // an entity was skipped or dropped, so `best` holds the page's entities only.
    struct EpochWant {
        bool countOnly = false;
        size_t n = 0;
        int64_t maxDelta = 0;
        std::unordered_set<std::string> counted;
        std::set<std::string> conf;
        bool pruned = false;
    };
    int32_t epochPoints(int profile, int64_t at, std::map<std::string, Pick>* best, bool* handled, EpochWant* want);
    // The given records, each answered as the scan answers (filters, tags).
    int32_t answer(std::vector<Cand>& cands, std::vector<std::pair<int64_t, Row>>* out);
    // The given (feed id, seq) records (those in the scan's files).
    int32_t answerPicks(const std::vector<std::pair<uint32_t, int64_t>>& picks, std::vector<std::pair<int64_t, Row>>* out);

private:
    int32_t pickFiles();
    // A file's view of staged rows, pinned by a step before its SQL runs.
    std::shared_ptr<const Staged> staged(const FRef& fr) const;
    int32_t boundCut();
    int32_t candidatesFromPreds(bool* used);
    int32_t page(std::vector<Cand>* out);  // the next page of candidates in order; empty at the end
    int32_t fetch(Stream& s);
    int32_t fill(Stream& s);  // fetches until the stream has an entry or is done
    bool before(const Ent& a, int fa, const Ent& b, int fb) const;
    bool keyed() const { return wo_ == kWDesc || wo_ == kWAsc || wo_ == kAtDesc || wo_ == kAtDescSeq || wo_ == kTsDesc; }
    bool byAt() const { return wo_ == kAtDesc || wo_ == kAtDescSeq; }  // a walk by delivery time (SCAN 5/6)
    int64_t orderValue(const Row& r) const;  // the answered row's value in a keyed walk
    int32_t resolve(std::vector<Cand>& page, std::vector<std::pair<int64_t, Row>>* out);
    bool evaluate(int64_t seq, std::vector<RowV>& rows, Row* out);
    int32_t loadRows(int fi, Conn* c, const std::vector<int64_t>& seqs, std::map<std::pair<int64_t, int>, std::vector<RowV>>* out);
    int32_t ftsHits(const std::vector<int64_t>& seqs, std::unordered_set<int64_t>* hit);
    bool needData() const;
    bool laneMatch(const RowV& r) const;
    bool copyMatch(const RowV& r) const;
    uint32_t tokOf(const std::string& producer);
    int32_t check();

    P4Lane* L_;
    Engine* e_;
    Type* t_;
    Spec2 s_;
    std::shared_ptr<const Spec> sp_;
    std::vector<FRef> files_;
    std::unordered_map<uint32_t, int> fiOf_;  // fid -> FRef index
    WalkOrder wo_ = kSeqAsc;
    int64_t vis_ = 0, lo_ = 0, hi_ = 0;  // seq range (lo exclusive, hi inclusive)
    bool fts_ = false;
    bool done_ = false;
    std::vector<Stream> streams_;
    std::vector<int> heap_;  // stream indexes with entries; the front is the next in order
    bool heapReady_ = false;
    size_t chunk_ = 512;
    std::vector<Cand> cands_;  // candidates from the predicates (in order), when used
    bool candMode_ = false;
    size_t candAt_ = 0;
    std::deque<Row> queue_;
    Row out_;
    uint64_t skipped_ = 0, emitted_ = 0;
    std::unordered_map<std::string, uint32_t> tokIds_;
};

int32_t Scan::check() {
    if (L_->trip) return L_->trip;
    if (L_->h && L_->h->cancel.load(std::memory_order_acquire)) return L_->trip = P4_E_CANCELLED;
    if (e_->stopWord->load(std::memory_order_acquire)) return L_->trip = P4_E_STOPPED;
    if (L_->maxRows && L_->rowsExamined > L_->maxRows) return L_->trip = P4_E_BUDGET;
    if (L_->maxBytes && L_->bytesRead > L_->maxBytes) return L_->trip = P4_E_BUDGET;
    return P4_OK;
}

uint32_t Scan::tokOf(const std::string& producer) {
    auto it = tokIds_.find(producer);
    if (it != tokIds_.end()) return it->second;
    uint32_t id;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        id = tokFor(t_, producer, std::string(), false);
    }
    if (!id) id = UINT32_MAX;
    tokIds_[producer] = id;
    return id;
}

bool Scan::needData() const {
    bool need = s_.hydrate;
    for (auto& p : s_.preds) need = need || p.field >= P4_F_COL0;
    return need;
}

bool Scan::laneMatch(const RowV& r) const {
    if (!r.inst) return false;
    const FRef& f = files_[size_t(r.fi)];
    const std::string* v[6] = {&f.provider, &f.source, &r.batch, &r.ckey, &r.ppeer, &r.pkey};
    for (int i = 0; i < 6; i++)
        if (s_.lfSet[i] && *v[i] != s_.lf[i]) return false;
    return true;
}

bool Scan::copyMatch(const RowV& r) const {
    if (s_.hasPeer && r.peer != s_.peer) return false;
    if (s_.hasProducer && r.producer != s_.producer) return false;
    return true;
}

// The scan's feed files (C-37 (6)): the lane filter's feeds, else every
// feed, in feed-id order.
int32_t Scan::pickFiles() {
    std::lock_guard<std::mutex> g(t_->mu);
    for (auto& fp : t_->feeds) {
        Feed* f = fp.get();
        if (!f->created || f->k.recs <= 0) continue;
        if ((s_.part == 1 && f->local) || (s_.part == 2 && !f->local)) continue;
        // A lane filter matches tagged records only; part 3 keeps the local
        // file beside the feed files the filter selects.
        if (s_.lane && f->local && s_.part != 3) continue;
        if (s_.lane && !f->local) {
            if (s_.lfSet[0] && f->provider != s_.lf[0]) continue;
            if (s_.lfSet[1] && f->source != s_.lf[1]) continue;
            if (s_.lfSet[2] || s_.lfSet[3] || s_.lfSet[4] || s_.lfSet[5]) {
                bool any = false;
                for (auto& kv : f->inst) {
                    const InstCount& ic = kv.second;
                    if (ic.n <= 0) continue;
                    if (s_.lfSet[2] && ic.batch != s_.lf[2]) continue;
                    if (s_.lfSet[3] && ic.ckey != s_.lf[3]) continue;
                    if (s_.lfSet[4] && ic.ppeer != s_.lf[4]) continue;
                    if (s_.lfSet[5] && ic.pkey != s_.lf[5]) continue;
                    any = true;
                    break;
                }
                if (!any) continue;
            }
        }
        if (s_.eNull && sp_->hasEpochRule && f->k.nnull == 0) continue;            // INDEX_PAGE phase 2: none without an epoch
        if (s_.eNotNull && sp_->hasEpochRule && f->k.nnull >= f->k.recs) continue;  // none with one
        if (s_.eNotNull && !sp_->hasEpochRule) continue;
        if (f->quarantined) return P4_E_CORRUPT;
        FRef r;
        r.f = f;
        r.fid = f->fid;
        r.path = f->path;
        r.provider = f->provider;
        r.source = f->source;
        r.local = f->local;
        r.indexed = f->indexed;
        r.k = f->k;
        files_.push_back(std::move(r));
    }
    for (size_t i = 0; i < files_.size(); i++) fiOf_[files_[i].fid] = int(i);
    return P4_OK;
}

std::shared_ptr<const Staged> Scan::staged(const FRef& fr) const {
    std::lock_guard<std::mutex> g(t_->mu);
    return fr.f->staged;
}

// A18 (C-31): the bound is the newest N records of lane.source when it is
// set (its feed files), or of the local file ("<TYPE>@local", with part 3 also
// the feed files of the source "local"), else of the type (every feed file);
// the cut is the N-th newest seq of those files merged, and every other
// filter applies above it.
int32_t Scan::boundCut() {
    if (!s_.bound || files_.empty()) return P4_OK;
    struct H {
        std::string path;
        bool indexed = true, done = false;
        int64_t last = 0;
        std::deque<int64_t> buf;
    };
    std::vector<H> hs;
    if (s_.lfSet[1] || s_.part >= 2) {
        for (const FRef& f : files_) hs.push_back(H{f.path, f.indexed, false, hi_ + 1, {}});
    } else {
        std::lock_guard<std::mutex> g(t_->mu);
        for (auto& f : t_->feeds)
            if (f->created && f->k.recs > 0 && !f->quarantined) hs.push_back(H{f->path, f->indexed, false, hi_ + 1, {}});
    }
    const size_t chunk = size_t(std::min<uint64_t>(s_.bound, 4096));
    auto fetch = [&](H& h) -> int32_t {
        while (h.buf.empty() && !h.done) {
            int rc = 0;
            Conn* c = e_->rpool.acquire(h.path, OpenKind::Reader, &rc, nullptr);
            if (!c) return statusOfSqlite(rc);
            sqlite3_stmt* q = c->sql(h.indexed ? "SELECT seq FROM r INDEXED BY r_s WHERE seq<?1 ORDER BY seq DESC LIMIT ?2"
                                               : "SELECT seq FROM r WHERE rid<(?1<<16) ORDER BY rid DESC LIMIT ?2");
            int32_t st = q ? P4_OK : P4_E_INTERNAL;
            size_t got = 0;
            if (q) {
                sqlite3_bind_int64(q, 1, h.last);
                sqlite3_bind_int64(q, 2, int64_t(chunk));
                int r;
                while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                    got++;
                    const int64_t sq = sqlite3_column_int64(q, 0);
                    if (sq != h.last) h.buf.push_back(sq);
                    h.last = sq;
                }
                sqlite3_reset(q);
                if (r != SQLITE_DONE) st = statusOfSqlite(r);
            }
            e_->rpool.release(c);
            if (st != P4_OK) return st;
            if (got < chunk) h.done = true;
        }
        return P4_OK;
    };
    uint64_t n = 0;
    int64_t last = 0;
    while (n < s_.bound) {
        int best = -1;
        for (size_t i = 0; i < hs.size(); i++) {
            const int32_t st = fetch(hs[i]);
            if (st != P4_OK) return st;
            if (!hs[i].buf.empty() && (best < 0 || hs[i].buf.front() > hs[size_t(best)].buf.front())) best = int(i);
        }
        if (best < 0) break;
        last = hs[size_t(best)].buf.front();
        hs[size_t(best)].buf.pop_front();
        n++;
    }
    L_->rowsExamined += n;
    if (n >= s_.bound && n > 0) lo_ = std::max(lo_, last - 1);
    return P4_OK;
}

// Candidates instead of a walk: an exact CID (one probe of each feed file's
// CID index), or an equality / IN on the object rule's first column (each
// file's object index). They are checked against every filter when their
// rows are read.
int32_t Scan::candidatesFromPreds(bool* used) {
    *used = false;
    const Spec2::Pred* kp = nullptr;
    const int oc = sp_->hasObject ? sp_->tc.firstObjectCol() : -1;
    if (!s_.hasCid && oc >= 0 && oc <= 3)
        for (const auto& p : s_.preds)
            if (p.field == P4_F_COL0 + oc && !p.vals.empty() && (p.op == P4_OP_EQ || p.op == P4_OP_IN)) {
                bool ok = true;
                for (const auto& v : p.vals) ok = ok && (v.type == ps::rb1::kInt || v.type == ps::rb1::kText);
                if (ok) kp = &p;
                break;
            }
    if (!s_.hasCid && !kp) return P4_OK;
    for (const FRef& fr : files_)
        if (!fr.indexed) return P4_OK;  // a file before REBUILD 1: the walk
    struct E {
        int64_t seq, w, ov;
        uint8_t key[32];
        int fi;
    };
    // The order value a keyed walk sorts by: w, delivery time or ts.
    const char* ovCol = wo_ == kAtDesc || wo_ == kAtDescSeq ? "at" : wo_ == kTsDesc ? "ts" : "w";
    std::vector<E> ents;
    const size_t kMax = 200000;
    auto bindCell = [](sqlite3_stmt* q, int i, const ps::rb1::Cell& c) {
        if (c.type == ps::rb1::kInt) sqlite3_bind_int64(q, i, c.i);
        else sqlite3_bind_text(q, i, c.s.data(), int(c.s.size()), SQLITE_TRANSIENT);
    };
    for (size_t fi = 0; fi < files_.size() && ents.size() <= kMax; fi++) {
        const FRef& fr = files_[fi];
        const std::shared_ptr<const Staged> view = staged(fr);
        int rc = 0;
        Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
        if (!c) return statusOfSqlite(rc);
        const std::string sql = std::string("SELECT seq, w, cid, ") + ovCol +
                                (s_.hasCid ? " FROM r INDEXED BY r_c WHERE cp=substr(?1, 1, 8) AND cid=?1 AND m=1 AND seq>?2 AND seq<=?3"
                                           : " FROM r INDEXED BY r_ke WHERE kk=(SELECT id FROM okey WHERE k=?1) AND m=1 AND seq>?2 AND seq<=?3");
        sqlite3_stmt* q = c->sql(sql);
        if (!q) {
            e_->rpool.release(c);
            return P4_OK;  // no object index: the walk
        }
        c->exec("BEGIN");
        int32_t st = P4_OK;
        const size_t nv = s_.hasCid ? 1 : kp->vals.size();
        for (size_t vi = 0; vi < nv && st == P4_OK && ents.size() <= kMax; vi++) {
            if (s_.hasCid) sqlite3_bind_blob(q, 1, s_.cidKey, 32, SQLITE_STATIC);
            else bindCell(q, 1, kp->vals[vi]);
            sqlite3_bind_int64(q, 2, lo_);
            sqlite3_bind_int64(q, 3, hi_);
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                if (sqlite3_column_bytes(q, 2) != 32) continue;
                E en;
                en.seq = sqlite3_column_int64(q, 0);
                en.fi = int(fi);
                en.w = sqlite3_column_int64(q, 1);
                en.ov = sqlite3_column_int64(q, 3);
                std::memcpy(en.key, sqlite3_column_blob(q, 2), 32);
                ents.push_back(en);
                if (ents.size() > kMax) break;
            }
            sqlite3_reset(q);
            if (r != SQLITE_DONE && r != SQLITE_ROW) st = statusOfSqlite(r);
        }
        c->exec("COMMIT");
        e_->rpool.release(c);
        if (st != P4_OK) return st;
        // The file's staged rows of the CID or the object keys.
        if (view && view->rows) {
            std::vector<const StRow*> v;
            for (size_t vi = 0; vi < nv && ents.size() <= kMax; vi++) {
                if (s_.hasCid) {
                    view->ofCid(s_.cidKey, &v);
                } else {
                    KVal kv;
                    const ps::rb1::Cell& cell = kp->vals[vi];
                    kv.type = cell.type == ps::rb1::kInt ? 1 : 3;
                    kv.i = cell.type == ps::rb1::kInt ? cell.i : 0;
                    if (kv.type == 3) kv.s = cell.s;
                    view->ofK(kv, &v);
                }
                for (const StRow* x : v) {
                    if (x->seq() <= lo_ || x->seq() > hi_) continue;
                    E en;
                    en.seq = x->seq();
                    en.fi = int(fi);
                    en.w = x->w;
                    en.ov = wo_ == kAtDesc || wo_ == kAtDescSeq ? x->at : wo_ == kTsDesc ? x->ts : x->w;
                    std::memcpy(en.key, x->cid, 32);
                    ents.push_back(en);
                }
            }
        }
    }
    if (ents.size() > kMax) return P4_OK;  // the walk
    // One candidate per (file, seq), in the scan's order; in a keyed order
    // one per (file, seq, order value), each answered only at its own value.
    std::map<std::tuple<int, int64_t, int64_t>, Cand> bySeq;
    const bool kd = keyed();
    for (const E& en : ents) {
        if (en.w < s_.wLo || en.w > s_.wHi) continue;
        Cand& c = bySeq[std::make_tuple(en.fi, en.seq, kd ? en.ov : int64_t(0))];
        c.seq = en.seq;
        c.fi = en.fi;
        c.w = en.w;
        c.ov = en.ov;
        c.checkOv = kd;
        std::memcpy(c.key, en.key, 32);
        c.haveKey = true;
    }
    for (auto& kv : bySeq) cands_.push_back(std::move(kv.second));
    auto asEnt = [](const Cand& c) {
        Ent e;
        e.seq = c.seq;
        e.w = c.w;
        e.ov = c.ov;
        std::memcpy(e.key, c.key, 32);
        e.haveKey = true;
        return e;
    };
    std::stable_sort(cands_.begin(), cands_.end(),
                     [&](const Cand& a, const Cand& b) { return before(asEnt(a), a.fi, asEnt(b), b.fi); });
    candMode_ = true;
    *used = true;
    return P4_OK;
}

int32_t Scan::open() {
    sp_ = t_->spec();
    vis_ = t_->vis.load(std::memory_order_acquire);
    hi_ = vis_;
    if (s_.seqThrough > 0 && s_.seqThrough < hi_) hi_ = s_.seqThrough;
    lo_ = s_.seqAfter > 0 ? s_.seqAfter : 0;
    if (!s_.search.empty()) {
        // FTS5 (background index; C-4's exception), checked a page at a time.
        // A type without records answers none (below).
        if (!sp_->fullText || (t_->hasFiles.load(std::memory_order_acquire) && !ioExists(t_->pFts))) return P4_E_UNSUPPORTED;
        fts_ = true;
    }
    if (s_.order == 0) s_.order = P4_ORDER_SEQ_ASC;
    switch (s_.order) {
        case P4_ORDER_SEQ_DESC: wo_ = kSeqDesc; break;
        case P4_ORDER_W_DESC: wo_ = s_.wAsc ? kWAsc : kWDesc; break;
        case P4_ORDER_CID: wo_ = kCid; break;
        case P4_ORDER_NEWEST: wo_ = s_.part == 2 ? kTsDesc : kAtDesc; break;   // local rows: their ts (format 1's untagged order)
        case P4_ORDER_RECENT: wo_ = s_.part == 2 ? kSeqDesc : kAtDescSeq; break;
        case P4_ORDER_W_ASC: wo_ = kWAsc; break;
        default: wo_ = kSeqAsc; break;
    }
    if (!t_->hasFiles.load(std::memory_order_acquire)) {
        done_ = true;
        return P4_OK;
    }
    int32_t rc = pickFiles();
    if (rc != P4_OK) return rc;
    if (files_.empty() || lo_ >= hi_) {
        done_ = true;
        return P4_OK;
    }
    rc = boundCut();
    if (rc != P4_OK) return rc;
    if (lo_ >= hi_) {
        done_ = true;
        return P4_OK;
    }
    chunk_ = std::max<size_t>(32, std::min<size_t>(512, 1024 / files_.size()));
    for (size_t i = 0; i < files_.size(); i++) {
        Stream s;
        s.fi = int(i);
        streams_.push_back(std::move(s));
    }
    bool used = false;
    return candidatesFromPreds(&used);
}

// ---- walks -------------------------------------------------------------------------------------------
bool Scan::before(const Ent& a, int fa, const Ent& b, int fb) const {
    switch (wo_) {
        case kSeqAsc:
            if (a.seq != b.seq) return a.seq < b.seq;
            return fa < fb;
        case kSeqDesc:
            if (a.seq != b.seq) return a.seq > b.seq;
            return fa > fb;
        case kWDesc:
        case kWAsc:
        case kAtDesc:
        case kTsDesc: {
            if (a.ov != b.ov) return wo_ == kWAsc ? a.ov < b.ov : a.ov > b.ov;
            const int k = std::memcmp(a.key, b.key, 32);
            if (k) return k < 0;
            return fa < fb;
        }
        case kAtDescSeq:
            if (a.ov != b.ov) return a.ov > b.ov;
            if (a.seq != b.seq) return a.seq > b.seq;
            return fa < fb;
        default: {
            const int k = std::memcmp(a.key, b.key, 32);
            if (k) return k < 0;
            return fa < fb;
        }
    }
}

// The next chunk of one feed file's walk.
int32_t Scan::fetch(Stream& s) {
    if (s.done) return P4_OK;
    const FRef& fr = files_[size_t(s.fi)];
    const std::shared_ptr<const Staged> view = staged(fr);
    int orc = 0;
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &orc, nullptr);
    if (!c) {
        e_->bump(kStReadErrors);
        return statusOfSqlite(orc);
    }
    const int64_t ridLo = ((lo_ + 1) << 16), ridHi = (hi_ << 16) | 0xffff;
    const bool wBound = s_.wLo != INT64_MIN || s_.wHi != INT64_MAX;
    std::vector<Ent> got;
    int32_t rc = P4_OK;
    // kind 0: seq; 1: rid, w; 2: rid, order value (w, at or ts), cid
    auto read = [&](sqlite3_stmt* q, int kind) -> int32_t {
        int r;
        while ((r = sqlite3_step(q)) == SQLITE_ROW) {
            Ent en;
            if (kind == 0) {
                en.seq = sqlite3_column_int64(q, 0);
                en.rid = en.seq << 16;
            } else {
                en.rid = sqlite3_column_int64(q, 0);
                en.seq = en.rid >> 16;
                en.w = en.ov = sqlite3_column_int64(q, 1);
                en.haveKey = kind == 2 && sqlite3_column_bytes(q, 2) == 32;
                if (en.haveKey) std::memcpy(en.key, sqlite3_column_blob(q, 2), 32);
                else std::memset(en.key, 0, 32);
            }
            got.push_back(en);
        }
        sqlite3_reset(q);
        return r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
    };
    c->exec("BEGIN");
    if (wo_ == kSeqAsc || wo_ == kSeqDesc) {
        const bool desc = wo_ == kSeqDesc;
        // The seqs alone (r_s), or the rows' rids (a file before REBUILD 1, or a w range).
        const bool idx = fr.indexed && !wBound;
        std::string sql;
        if (idx) sql = desc ? "SELECT seq FROM r INDEXED BY r_s WHERE seq<?1 AND seq>?2 ORDER BY seq DESC LIMIT ?3"
                            : "SELECT seq FROM r INDEXED BY r_s WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3";
        else sql = desc ? "SELECT rid, w FROM r WHERE rid<?1 AND rid>=?2 AND w>=?4 AND w<=?5 ORDER BY rid DESC LIMIT ?3"
                        : "SELECT rid, w FROM r WHERE rid>?1 AND rid<=?2 AND w>=?4 AND w<=?5 ORDER BY rid LIMIT ?3";
        sqlite3_stmt* q = c->sql(sql);
        if (!q) rc = P4_E_INTERNAL;
        else {
            if (!s.started) s.rKey = desc ? (idx ? hi_ + 1 : ridHi + 1) : (idx ? lo_ : ridLo - 1);
            sqlite3_bind_int64(q, 1, s.rKey);
            sqlite3_bind_int64(q, 2, idx ? (desc ? lo_ : hi_) : (desc ? ridLo : ridHi));
            sqlite3_bind_int64(q, 3, int64_t(chunk_));
            if (!idx) {
                sqlite3_bind_int64(q, 4, s_.wLo);
                sqlite3_bind_int64(q, 5, s_.wHi);
            }
            rc = read(q, idx ? 0 : 1);
            if (rc == P4_OK) {
                if (!got.empty()) s.rKey = idx ? got.back().seq : got.back().rid;
                if (got.size() < chunk_) s.done = true;
                s.started = true;
            }
        }
    } else if (keyed()) {
        // A keyed walk, whole key groups: w (r_w, within the predicates' w
        // range), delivery time (r_a, a feed file) or the copies' ts (r_t,
        // the local file). None of them holds a CID: each group is put in
        // its tie order from the rows. r_w holds the merged rows: a w walk
        // also takes the staged rows of the same w interval from the view.
        const bool asc = wo_ == kWAsc, isW = wo_ == kWDesc || wo_ == kWAsc;
        const std::string col = isW ? "w" : wo_ == kTsDesc ? "ts" : "at";
        const std::string idx = isW ? "r_w" : wo_ == kTsDesc ? "r_t" : "r_a";
        const std::string mOnly = isW ? " AND m=1" : "";
        const int64_t lo = isW ? s_.wLo : INT64_MIN, hi = isW ? s_.wHi : INT64_MAX;
        if (!fr.indexed) rc = P4_E_UNSUPPORTED;
        // A file made before its r_a / r_t (an older store): the same answer
        // through a sort.
        auto prep = [&](const std::string& tail) {
            sqlite3_stmt* q = c->sql("SELECT rid, " + col + ", cid FROM r INDEXED BY " + idx + " WHERE " + tail);
            if (!q && !isW) q = c->sql("SELECT rid, " + col + ", cid FROM r WHERE " + tail);
            return q;
        };
        bool full = false;
        if (rc == P4_OK) {
            if (!s.started) s.rKey = asc ? lo : hi;
            const std::string tail = col + (asc ? (s.started ? ">?1 AND " : ">=?1 AND ") : (s.started ? "<?1 AND " : "<=?1 AND ")) +
                                     col + (asc ? "<=?2" : ">=?2") + mOnly + " AND rid>=?3 AND rid<=?4 ORDER BY " + col +
                                     (asc ? " ASC" : " DESC") + " LIMIT ?5";
            sqlite3_stmt* q = prep(tail);
            if (!q) rc = P4_E_INTERNAL;
            else {
                sqlite3_bind_int64(q, 1, s.rKey);
                sqlite3_bind_int64(q, 2, asc ? hi : lo);
                sqlite3_bind_int64(q, 3, ridLo);
                sqlite3_bind_int64(q, 4, ridHi);
                sqlite3_bind_int64(q, 5, int64_t(chunk_));
                rc = read(q, 2);
            }
            full = got.size() >= chunk_;
            if (rc == P4_OK && full) {
                // The last key group whole.
                const int64_t last = got.back().ov;
                while (!got.empty() && got.back().ov == last) got.pop_back();
                sqlite3_stmt* g = prep(col + "=?1" + mOnly + " AND rid>=?2 AND rid<=?3");
                if (!g) rc = P4_E_INTERNAL;
                else {
                    sqlite3_bind_int64(g, 1, last);
                    sqlite3_bind_int64(g, 2, ridLo);
                    sqlite3_bind_int64(g, 3, ridHi);
                    rc = read(g, 2);
                }
            }
            if (rc == P4_OK && isW && view && view->rows) {
                // The staged rows of the interval this chunk covers: to its
                // last group when the chunk is full, else to the range's end
                // (whole groups past a chunk; the rest on the next fetch,
                // and the chunk's merged rows past them with it).
                std::vector<const StRow*> v;
                bool more = false;
                const int64_t to = full ? got.back().ov : (asc ? hi : lo);
                view->wRange(!asc, s.rKey, !s.started, to, ridLo, ridHi, full ? SIZE_MAX : chunk_, &v, &more);
                if (more) {
                    const int64_t cut = v.back()->w;
                    while (!got.empty() && (asc ? got.back().ov > cut : got.back().ov < cut)) got.pop_back();
                    full = true;
                }
                for (const StRow* x : v) {
                    Ent en;
                    en.rid = x->rid;
                    en.seq = x->seq();
                    en.w = en.ov = x->w;
                    std::memcpy(en.key, x->cid, 32);
                    en.haveKey = true;
                    got.push_back(en);
                }
                if (!v.empty())
                    std::stable_sort(got.begin(), got.end(), [asc](const Ent& a, const Ent& b) { return asc ? a.ov < b.ov : a.ov > b.ov; });
            }
            if (rc == P4_OK) {
                // Each group in CID order (RECENT: seq desc), then rid: a
                // record's rows stay adjacent.
                const bool bySeq = wo_ == kAtDescSeq;
                size_t a = 0;
                while (a < got.size()) {
                    size_t b = a;
                    while (b < got.size() && got[b].ov == got[a].ov) b++;
                    std::sort(got.begin() + long(a), got.begin() + long(b), [bySeq](const Ent& x, const Ent& y) {
                        if (bySeq) return x.seq != y.seq ? x.seq > y.seq : x.rid < y.rid;
                        const int k = std::memcmp(x.key, y.key, 32);
                        return k ? k < 0 : x.rid < y.rid;
                    });
                    a = b;
                }
                if (!got.empty()) s.rKey = got.back().ov;
                if (!full) s.done = true;
                s.started = true;
            }
        }
    } else {  // CID
        // r_c holds the merged rows; the staged rows of the same CID
        // interval come from the view.
        if (!fr.indexed) rc = P4_E_UNSUPPORTED;
        // cp (r_c's key) is the CID key's first 8 bytes: cp order is CID order,
        // a cp group put in order by the CID; the walk seeks to the resume
        // CID's prefix (bound as a blob of its own: a zero-length one at the
        // start, never NULL).
        sqlite3_stmt* q = rc == P4_OK ? c->sql("SELECT rid, w, cid FROM r INDEXED BY r_c WHERE cp>=?6 AND (cid, rid) > (?1, ?2) AND m=1"
                                               " AND rid>=?3 AND rid<=?4 ORDER BY cp, cid, rid LIMIT ?5")
                                      : nullptr;
        if (rc == P4_OK && !q) rc = P4_E_INTERNAL;
        if (rc == P4_OK) {
            if (!s.started) {
                s.rCid.clear();
                s.rRid = 0;
            }
            sqlite3_bind_blob(q, 1, s.rCid.data(), int(s.rCid.size()), SQLITE_TRANSIENT);
            sqlite3_bind_int64(q, 2, s.rRid);
            sqlite3_bind_int64(q, 3, ridLo);
            sqlite3_bind_int64(q, 4, ridHi);
            sqlite3_bind_int64(q, 5, int64_t(chunk_));
            if (s.rCid.size() >= 8) sqlite3_bind_blob(q, 6, s.rCid.data(), 8, SQLITE_TRANSIENT);
            else sqlite3_bind_zeroblob(q, 6, 0);
            rc = read(q, 2);
            bool full = got.size() >= chunk_;
            if (rc == P4_OK && view && view->rows) {
                std::vector<const StRow*> v;
                bool more = false;
                const Ent* last = full ? &got.back() : nullptr;
                view->cidRange(s.rCid, s.rRid, last ? last->key : nullptr, last ? last->rid : 0, ridLo, ridHi, full ? SIZE_MAX : chunk_, &v,
                                &more);
                auto before = [](const uint8_t* ak, int64_t ar, const uint8_t* bk, int64_t br) {
                    const int k = std::memcmp(ak, bk, 32);
                    return k ? k < 0 : ar < br;
                };
                if (more) {
                    const StRow* cut = v.back();
                    while (!got.empty() && before(cut->cid, cut->rid, got.back().key, got.back().rid)) got.pop_back();
                    full = true;
                }
                for (const StRow* x : v) {
                    Ent en;
                    en.rid = x->rid;
                    en.seq = x->seq();
                    en.w = en.ov = x->w;
                    std::memcpy(en.key, x->cid, 32);
                    en.haveKey = true;
                    got.push_back(en);
                }
                if (!v.empty())
                    std::sort(got.begin(), got.end(), [&](const Ent& a, const Ent& b) { return before(a.key, a.rid, b.key, b.rid); });
            }
            if (rc == P4_OK) {
                if (!got.empty()) {
                    s.rCid.assign(reinterpret_cast<const char*>(got.back().key), 32);
                    s.rRid = got.back().rid;
                }
                if (!full) s.done = true;
                s.started = true;
            }
        }
    }
    c->exec("COMMIT");
    e_->rpool.release(c);
    if (rc != P4_OK) {
        if (rc != P4_E_UNSUPPORTED) e_->bump(kStReadErrors);
        return rc;
    }
    for (Ent& en : got) s.buf.push_back(en);
    return P4_OK;
}

int32_t Scan::fill(Stream& s) {
    while (s.buf.empty() && !s.done) {
        const int32_t rc = fetch(s);
        if (rc != P4_OK) return rc;
        const int32_t ck = check();
        if (ck != P4_OK) return ck;
    }
    return P4_OK;
}

// The next page of candidates: the scan's feed files merged in its order
// (ties by feed id), each record's row set in a file once.
int32_t Scan::page(std::vector<Cand>* out) {
    out->clear();
    if (candMode_) {
        for (size_t i = 0; i < 256 && candAt_ < cands_.size(); i++) out->push_back(cands_[candAt_++]);
        if (candAt_ >= cands_.size()) done_ = true;
        return P4_OK;
    }
    auto later = [&](int x, int y) {  // the heap's order: the front comes first
        return before(streams_[size_t(y)].buf.front(), streams_[size_t(y)].fi, streams_[size_t(x)].buf.front(), streams_[size_t(x)].fi);
    };
    if (!heapReady_) {
        heapReady_ = true;
        for (size_t i = 0; i < streams_.size(); i++) {
            const int32_t rc = fill(streams_[i]);
            if (rc != P4_OK) return rc;
            if (!streams_[i].buf.empty()) {
                heap_.push_back(int(i));
                std::push_heap(heap_.begin(), heap_.end(), later);
            }
        }
    }
    const size_t kPage = 512;
    while (out->size() < kPage && !heap_.empty()) {
        std::pop_heap(heap_.begin(), heap_.end(), later);
        const int i = heap_.back();
        heap_.pop_back();
        Stream& s = streams_[size_t(i)];
        const Ent en = s.buf.front();
        s.buf.pop_front();
        const int32_t rc = fill(s);
        if (rc != P4_OK) return rc;
        if (!s.buf.empty()) {
            heap_.push_back(i);
            std::push_heap(heap_.begin(), heap_.end(), later);
        }
        L_->rowsExamined++;
        if (en.seq == s.lastSeq) continue;
        s.lastSeq = en.seq;
        Cand c;
        c.seq = en.seq;
        c.fi = s.fi;
        c.w = en.w;
        c.ov = en.ov;
        c.checkOv = keyed();
        c.haveKey = en.haveKey;
        if (en.haveKey) std::memcpy(c.key, en.key, 32);
        out->push_back(c);
    }
    if (heap_.empty()) done_ = true;
    return check();
}

// ---- rows ---------------------------------------------------------------------------------------------
int32_t Scan::loadRows(int fi, Conn* c, const std::vector<int64_t>& seqs,
                       std::map<std::pair<int64_t, int>, std::vector<RowV>>* out) {
    FRef& fr = files_[size_t(fi)];
    const bool nd = needData();
    std::shared_ptr<p4::Stream> stream;  // the feed's record stream (Scan::Stream is a walk)
    if (nd) {
        const int32_t st = snapStream(fr.f, c, &stream);
        if (st != P4_OK) return st;
    }
    if (!dictEnsure(fr.f, c)) return P4_E_IO;
    sqlite3_stmt* q = c->get(S_R_SEQ);
    if (!q) return P4_E_INTERNAL;
    for (int64_t seq : seqs) {
        sqlite3_bind_int64(q, 1, seq << 16);
        sqlite3_bind_int64(q, 2, (seq << 16) | 0xffff);
        int r;
        std::vector<RowV>& rows = (*out)[{seq, fi}];
        while ((r = sqlite3_step(q)) == SQLITE_ROW) {
            if (sqlite3_column_bytes(q, 6) != 32) continue;
            RowV x;
            x.fi = fi;
            x.rid = sqlite3_column_int64(q, 0);
            x.n = uint32_t(sqlite3_column_int64(q, 1));
            x.b = uint32_t(sqlite3_column_int64(q, 2));
            x.c = uint32_t(sqlite3_column_int64(q, 3));
            x.u = uint32_t(sqlite3_column_int64(q, 4));
            x.at = sqlite3_column_int64(q, 5);
            std::memcpy(x.key, sqlite3_column_blob(q, 6), 32);
            x.hasE = sqlite3_column_type(q, 7) != SQLITE_NULL;
            x.e = sqlite3_column_int64(q, 7);
            x.k.from(q, 8);
            x.ts = sqlite3_column_int64(q, 9);
            x.sealed = sqlite3_column_type(q, 10) != SQLITE_NULL;
            if (x.sealed) x.fcols.assign(static_cast<const char*>(sqlite3_column_blob(q, 10)), size_t(sqlite3_column_bytes(q, 10)));
            if (sqlite3_column_type(q, 11) != SQLITE_NULL)
                x.sig.assign(static_cast<const char*>(sqlite3_column_blob(q, 11)), size_t(sqlite3_column_bytes(q, 11)));
            x.len = sqlite3_column_int64(q, 12);
            x.off = sqlite3_column_int64(q, 13);
            if (nd) {
                const int32_t st = streamRead(stream.get(), x.off, x.len, &x.data);
                if (st != P4_OK) {
                    sqlite3_reset(q);
                    return st;
                }
                x.hasData = true;
            }
            NodeDef nodeDef;
            if (!dictNode(fr.f, c, x.n, &nodeDef)) {
                sqlite3_reset(q);
                return P4_E_CORRUPT;
            }
            x.producer = nodeDef.producer;
            x.peer = nodeDef.peer;
            if (x.b) {
                BatchDef bd;
                if (!dictBatch(fr.f, c, x.b, &bd)) {
                    sqlite3_reset(q);
                    return P4_E_CORRUPT;
                }
                x.inst = true;
                x.batch = bd.batch;
                x.ppeer = bd.ppeer;
                x.pkey = bd.pkey;
            }
            L_->bytesRead += uint64_t(x.len) + 64;
            rows.push_back(std::move(x));
        }
        sqlite3_reset(q);
        if (r != SQLITE_DONE) return statusOfSqlite(r);
    }
    // Content keys and urls of the rows with an instance.
    for (auto& kv : *out) {
        if (kv.first.second != fi) continue;
        for (RowV& x : kv.second) {
            if (x.c) x.ckey = dictCkey(c, x.c);
            if (x.u && (s_.needTags || s_.lane)) x.url = dictUrl(c, x.u);
        }
    }
    return P4_OK;
}

// The page's seqs that the search matches, from the full-text index (one
// range read when the page's seqs are dense, else a probe per seq).
int32_t Scan::ftsHits(const std::vector<int64_t>& seqs, std::unordered_set<int64_t>* hit) {
    if (seqs.empty()) return P4_OK;
    int rc = 0;
    Conn* c = e_->rpool.acquire(t_->pFts, OpenKind::Reader, &rc, nullptr);
    if (!c) return statusOfSqlite(rc);
    int64_t lo = INT64_MAX, hi = INT64_MIN;
    for (int64_t s : seqs) {
        lo = std::min(lo, s);
        hi = std::max(hi, s);
    }
    int32_t status = P4_OK;
    if (uint64_t(hi - lo) <= uint64_t(seqs.size()) * 4) {
        sqlite3_stmt* q = c->sql("SELECT rowid FROM fts WHERE fts MATCH ?1 AND rowid>=?2 AND rowid<=?3");
        if (!q) status = P4_E_SQL;
        else {
            sqlite3_bind_text(q, 1, s_.search.data(), int(s_.search.size()), SQLITE_STATIC);
            sqlite3_bind_int64(q, 2, lo);
            sqlite3_bind_int64(q, 3, hi);
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) hit->insert(sqlite3_column_int64(q, 0));
            sqlite3_reset(q);
            if (r != SQLITE_DONE) status = P4_E_SQL;
        }
    } else {
        sqlite3_stmt* q = c->sql("SELECT 1 FROM fts WHERE fts MATCH ?1 AND rowid=?2");
        if (!q) status = P4_E_SQL;
        for (size_t i = 0; q && i < seqs.size() && status == P4_OK; i++) {
            sqlite3_bind_text(q, 1, s_.search.data(), int(s_.search.size()), SQLITE_STATIC);
            sqlite3_bind_int64(q, 2, seqs[i]);
            const int r = sqlite3_step(q);
            sqlite3_reset(q);
            if (r == SQLITE_ROW) hit->insert(seqs[i]);
            else if (r != SQLITE_DONE) status = P4_E_SQL;
        }
    }
    e_->rpool.release(c);
    return status;
}

int32_t Scan::cols(const Row& r, ps::Extracted* x, uint8_t* scratch, size_t n) {
    *x = ps::Extracted();
    if (r.sealed) {
        // r.f: [u8 col][u8 type][u64 | u16 len + bytes]
        const uint8_t* p = reinterpret_cast<const uint8_t*>(r.fcols.data());
        const uint8_t* end = p + r.fcols.size();
        size_t used = 0;
        while (end - p >= 2) {
            const uint8_t c = p[0], ty = p[1];
            p += 2;
            if (c >= ps::kMaxCols) break;
            if (ty == 1 && end - p >= 8) {
                x->cols[c].present = true;
                x->cols[c].isU64 = true;
                x->cols[c].u = ld64(p);
                p += 8;
            } else if (ty == 3 && end - p >= 2) {
                const uint16_t len = ld16(p);
                p += 2;
                if (size_t(end - p) < len || used + len > n) break;
                std::memcpy(scratch + used, p, len);
                x->cols[c].present = true;
                x->cols[c].s = scratch + used;
                x->cols[c].n = len;
                used += len;
                p += len;
            } else {
                break;
            }
        }
        return P4_OK;
    }
    if (!r.hasData) return P4_E_INTERNAL;
    std::string frame(r.data.size() + 4, '\0');
    st32(reinterpret_cast<uint8_t*>(&frame[0]), uint32_t(r.data.size()));
    std::memcpy(&frame[4], r.data.data(), r.data.size());
    // scratch must outlive x: the caller's.
    static thread_local std::string keep;
    keep.swap(frame);
    sp_->tc.extract(reinterpret_cast<const uint8_t*>(keep.data()), keep.size(), x, scratch, n);
    return P4_OK;
}

// A record's rows (the scan's files) -> its answer, or false when it does
// not match. The copy is the lowest token's matching row (the first copy,
// C-12); the tag the earliest matching instance (§3.6), or in a walk by
// delivery time the newest, the delivery the record is ordered by (C-41 N9).
bool Scan::evaluate(int64_t seq, std::vector<RowV>& rows, Row* out) {
    if (rows.empty()) return false;
    if (s_.hasCid && std::memcmp(rows[0].key, s_.cidKey, 32) != 0) return false;
    const RowV* pick = nullptr;
    uint32_t pickTok = UINT32_MAX;
    bool pickLane = false;
    for (const RowV& x : rows) {
        if (!copyMatch(x)) continue;
        const bool ln = !s_.lane || laneMatch(x);
        const uint32_t tk = tokOf(x.producer);
        // a row matching the lane filter too, then the lowest token
        if (!pick || (ln && !pickLane) || (ln == pickLane && tk < pickTok)) {
            pick = &x;
            pickTok = tk;
            pickLane = ln;
        }
    }
    if (!pick) return false;
    Row& r = *out;
    r = Row();
    r.seq = seq;
    std::memcpy(r.key, pick->key, 32);
    r.hasE = pick->hasE;
    r.e = pick->e;
    r.k = pick->k;
    r.ts = pick->ts;
    r.w = r.hasE ? r.e : r.ts;
    r.len = pick->len;
    r.producer = pick->producer;
    r.peer = pick->peer;
    r.sig = pick->sig;
    r.sealed = pick->sealed;
    r.fcols = pick->fcols;
    if (pick->hasData) {
        r.data = pick->data;
        r.hasData = true;
    }
    if (s_.eNotNull && !r.hasE) return false;
    if (s_.eNull && r.hasE) return false;
    // matched tag
    for (const RowV& x : rows) {
        if (!x.inst) continue;
        if (s_.lane && !laneMatch(x)) continue;
        const FRef& f = files_[size_t(x.fi)];
        r.ats.push_back(x.at);
        if (!r.hasTag || tagCmp(r, x, f, byAt()) > 0) {
            r.hasTag = true;
            r.at = x.at;
            r.provider = f.provider;
            r.source = f.source;
            r.batch = x.batch;
            r.ckey = x.ckey;
            r.ppeer = x.ppeer;
            r.pkey = x.pkey;
            r.url = x.url;
        }
    }
    // A lane filter keeps tagged records; part 3 also the local file's.
    if (s_.lane && !r.hasTag && !(s_.part == 3 && files_[size_t(pick->fi)].local)) return false;
    if (s_.preds.empty()) return true;
    bool needCols = false;
    for (auto& p : s_.preds) needCols = needCols || p.field >= P4_F_COL0;
    ps::Extracted x;
    uint8_t scratch[2048];
    if (needCols && cols(r, &x, scratch, sizeof scratch) != P4_OK) return false;
    for (auto& p : s_.preds) {
        switch (p.field) {
            case P4_F_EPOCH:
                if (!predOn(p, r.hasE, r.e, nullptr, true)) return false;
                break;
            case P4_F_TS:
                if (!predOn(p, true, r.ts, nullptr, true)) return false;
                break;
            case P4_F_W:
                if (!predOn(p, true, r.w, nullptr, true)) return false;
                break;
            case P4_F_EPOCH_DAY: {
                char d[11];
                if (r.hasE) dayText(r.e, d);
                const std::string ds = r.hasE ? std::string(d, 10) : std::string();
                if (!predOn(p, r.hasE, 0, &ds, false)) return false;
                break;
            }
            default: {
                const int col = p.field - P4_F_COL0;
                if (col < 0 || col > 3) return false;
                const ps::ColValue& cv = x.cols[col];
                if (cv.isU64) {
                    if (!predOn(p, cv.present, int64_t(cv.u), nullptr, true)) return false;
                } else {
                    const std::string sv = cv.present ? std::string(reinterpret_cast<const char*>(cv.s), cv.n) : "";
                    if (!predOn(p, cv.present, 0, &sv, false)) return false;
                }
            }
        }
    }
    return true;
}

// A record's place in a keyed walk: the w of the copy it answers with, the
// delivery time of the instance it answers with (its newest matching one,
// format 1's newest tag row), or that copy's ts (local).
int64_t Scan::orderValue(const Row& r) const {
    if (wo_ == kTsDesc) return r.ts;
    if (byAt()) return r.hasTag ? r.at : INT64_MIN;
    return r.w;
}

int32_t Scan::resolve(std::vector<Cand>& page, std::vector<std::pair<int64_t, Row>>* out) {
    std::map<int, std::vector<int64_t>> want;
    for (const Cand& c : page) want[c.fi].push_back(c.seq);
    std::map<std::pair<int64_t, int>, std::vector<RowV>> rows;
    for (auto& kv : want) {
        std::sort(kv.second.begin(), kv.second.end());
        kv.second.erase(std::unique(kv.second.begin(), kv.second.end()), kv.second.end());
        int orc = 0;
        Conn* c = e_->rpool.acquire(files_[size_t(kv.first)].path, OpenKind::Reader, &orc, nullptr);
        if (!c) {
            e_->bump(kStReadErrors);
            return statusOfSqlite(orc);
        }
        c->exec("BEGIN");
        const int32_t st = loadRows(kv.first, c, kv.second, &rows);
        c->exec("COMMIT");
        e_->rpool.release(c);
        if (st != P4_OK) {
            e_->bump(kStReadErrors);
            return st;
        }
    }
    std::unordered_set<int64_t> hits;
    if (fts_) {
        std::vector<int64_t> seqs;
        for (const Cand& c : page) seqs.push_back(c.seq);
        const int32_t st = ftsHits(seqs, &hits);
        if (st != P4_OK) return st;
    }
    for (const Cand& c : page) {
        if (fts_ && !hits.count(c.seq)) continue;
        auto it = rows.find({c.seq, c.fi});
        if (it == rows.end()) continue;
        Row r;
        if (evaluate(c.seq, it->second, &r)) {
            if (c.checkOv && c.ov != orderValue(r)) continue;  // answered at its own row's value
            r.fid = files_[size_t(c.fi)].fid;
            out->push_back({c.seq, std::move(r)});
        }
    }
    return P4_OK;
}

int32_t Scan::answer(std::vector<Cand>& cands, std::vector<std::pair<int64_t, Row>>* out) {
    for (size_t at = 0; at < cands.size(); at += 256) {
        std::vector<Cand> page(cands.begin() + long(at), cands.begin() + long(std::min(cands.size(), at + 256)));
        const int32_t rc = resolve(page, out);
        if (rc != P4_OK) return rc;
    }
    return P4_OK;
}

int32_t Scan::answerPicks(const std::vector<std::pair<uint32_t, int64_t>>& picks, std::vector<std::pair<int64_t, Row>>* out) {
    std::vector<Cand> cands;
    for (auto& p : picks) {
        auto it = fiOf_.find(p.first);
        if (it == fiOf_.end()) continue;
        Cand c;
        c.seq = p.second;
        c.fi = it->second;
        cands.push_back(c);
    }
    return answer(cands, out);
}

int32_t Scan::next(Row** out) {
    for (;;) {
        int32_t rc = check();
        if (rc != P4_OK) return rc;
        if (s_.limit && emitted_ >= s_.limit) return 0;
        if (!queue_.empty()) {
            out_ = std::move(queue_.front());
            queue_.pop_front();
            if (skipped_ < s_.offset) {
                skipped_++;
                continue;
            }
            emitted_++;
            *out = &out_;
            return 1;
        }
        if (done_ && (!candMode_ || candAt_ >= cands_.size())) return 0;
        std::vector<Cand> pg;
        rc = page(&pg);
        if (rc != P4_OK) return rc;
        if (pg.empty()) {
            if (done_) return 0;
            continue;
        }
        std::vector<std::pair<int64_t, Row>> got;
        rc = resolve(pg, &got);
        if (rc != P4_OK) return rc;
        for (auto& g : got) queue_.push_back(std::move(g.second));
    }
}

// EPOCH points (2 nearest, 3 as_of, 4 forward) for every object: one seek per
// object in each feed file's object index (r_ke), read in rank order until an
// epoch group has a record that passes the filters; ties at the best epoch go
// to the lowest CID (format 1's ranking), then the lowest feed id. Records
// without an object are their own entities (their CID).
int32_t Scan::epochPoints(int profile, int64_t at, std::map<std::string, Pick>* best, bool* handled, EpochWant* want) {
    *handled = false;
    if (!sp_->ek || !s_.search.empty() || profile < 2 || profile > 4) return P4_OK;
    for (const FRef& fr : files_)
        if (!fr.indexed) return P4_OK;  // a file before REBUILD 1: the walk
    *handled = true;
    if (files_.empty() || done_) return P4_OK;
    bool needCheck = s_.hasPeer || s_.hasProducer || s_.hasCid || s_.lane;
    for (const auto& p : s_.preds) needCheck = needCheck || (p.field != P4_F_EPOCH && p.field != P4_F_W);
    const int oc = sp_->tc.firstObjectCol();
    const Spec2::Pred* kp = nullptr;  // narrows the objects walked
    for (const auto& p : s_.preds)
        if (oc >= 0 && oc <= 3 && p.field == P4_F_COL0 + oc && !p.vals.empty() && (p.op == P4_OP_EQ || p.op == P4_OP_IN)) {
            bool ok = true;
            for (const auto& v : p.vals) ok = ok && (v.type == ps::rb1::kInt || v.type == ps::rb1::kText);
            if (ok) kp = &p;
            break;
        }
    const int64_t wLo = s_.wLo, wHi = s_.wHi;
    auto better = [&](int64_t ae, const uint8_t* ak, int64_t be, const uint8_t* bk) {
        if (profile == 4) {
            if (ae != be) return ae < be;
        } else if (profile == 2) {
            const uint64_t da = epochDistance(ae, at), db = epochDistance(be, at);
            if (da != db) return da < db;
            if ((ae <= at) != (be <= at)) return ae <= at;
            if (ae != be) return ae > be;
        } else {
            if (ae != be) return ae > be;
        }
        return std::memcmp(ak, bk, 32) < 0;
    };
    // An entity's best pick so far; a later feed file's pick replaces it only
    // when strictly better (ties stay with the lower feed id).
    auto offer = [&](const std::string& ent, const Pick& p) {
        auto it = best->find(ent);
        if (it == best->end() || better(p.e, p.key, it->second.e, it->second.key)) (*best)[ent] = p;
    };
    // EpochWant: whether an entity takes no probe, and a feed file's pick for one.
    auto skip = [&](const std::string& ent) -> bool {
        if (!want) return false;
        if (want->countOnly) return want->counted.count(ent) != 0;
        if (want->n && want->conf.size() >= want->n && ent > *want->conf.rbegin()) {
            want->pruned = true;
            return true;
        }
        return false;
    };
    auto take = [&](const std::string& ent, const Pick& p) {
        const bool within = !want || want->maxDelta <= 0 || epochDistance(p.e, at) <= uint64_t(want->maxDelta);
        if (want && want->countOnly) {
            if (within) want->counted.insert(ent);
            return;
        }
        offer(ent, p);
        if (!want || !want->n || !within) return;
        want->conf.insert(ent);
        if (want->conf.size() > want->n) {
            auto last = std::prev(want->conf.end());
            best->erase(*last);
            want->conf.erase(last);
            want->pruned = true;
        }
    };
    int32_t rc = P4_OK;
    for (size_t fi = 0; fi < files_.size() && rc == P4_OK; fi++) {
        const uint32_t fid = files_[fi].fid;
        // r_ke holds the merged rows; the file's staged rows come from its view.
        const std::shared_ptr<const Staged> pin = staged(files_[fi]);
        const Staged* st = pin && pin->rows ? pin.get() : nullptr;
        int orc = 0;
        Conn* c = e_->rpool.acquire(files_[fi].path, OpenKind::Reader, &orc, nullptr);
        if (!c) return statusOfSqlite(orc);
        // The object index holds each key's okey id (kk): the objects are
        // okey's rows, in key order (a key whose rows are gone finds none),
        // each one's entity its key; an object predicate's values are looked
        // up there.
        sqlite3_stmt* keys = c->sql("SELECT id, k FROM okey ORDER BY k");
        sqlite3_stmt* keyId = c->sql("SELECT id FROM okey WHERE k=?1");
        sqlite3_stmt* below = c->sql("SELECT e, cid, seq FROM r INDEXED BY r_ke WHERE kk=?1 AND m=1 AND e<=?2 AND e>=?3 AND seq>?4"
                                     " AND seq<=?5 ORDER BY e DESC");
        sqlite3_stmt* above = c->sql("SELECT e, cid, seq FROM r INDEXED BY r_ke WHERE kk=?1 AND m=1 AND e>=?2 AND e<=?3 AND seq>?4"
                                     " AND seq<=?5 ORDER BY e ASC");
        sqlite3_stmt* noK = c->sql("SELECT e, cid, seq FROM r INDEXED BY r_ke WHERE kk IS NULL AND m=1 AND e>=?1 AND e<=?2 AND seq>?3"
                                   " AND seq<=?4");
        if (!keys || !keyId || !below || !above || !noK) {
            e_->rpool.release(c);
            *handled = false;
            best->clear();
            return P4_OK;
        }
        // A candidate passes the scan's filters: its rows read (only with a
        // filter beyond the epoch range).
        auto passes = [&](int64_t seq) -> int32_t {
            if (!needCheck) return 1;
            Cand cd;
            cd.seq = seq;
            cd.fi = int(fi);
            std::vector<Cand> one{cd};
            std::vector<std::pair<int64_t, Row>> got;
            const int32_t r = resolve(one, &got);
            if (r != P4_OK) return r;
            return got.empty() ? 0 : 1;
        };
        c->exec("BEGIN");
        // The first e-group (in rank order) of one object's run that has a
        // record passing the filters: its lowest-CID passing record. The
        // merged rows (q, the object's okey id kk; 0: none here) and its
        // staged rows (sk, (e, rid) order) are read together in rank order.
        auto pick = [&](sqlite3_stmt* q, int64_t kk, const std::vector<const StRow*>* sk, bool down, Pick* out) -> int32_t {
            const int64_t lim = down ? std::min(at, wHi) : std::max(at, wLo);
            if (down ? lim < wLo : lim > wHi) return 0;
            if (kk) sqlite3_bind_int64(q, 1, kk);
            else sqlite3_bind_null(q, 1);
            sqlite3_bind_int64(q, 2, lim);
            sqlite3_bind_int64(q, 3, down ? wLo : wHi);
            sqlite3_bind_int64(q, 4, lo_);
            sqlite3_bind_int64(q, 5, hi_);
            std::vector<const StRow*> sv;
            if (sk)
                for (const StRow* x : *sk)
                    if (x->hasE && x->seq() > lo_ && x->seq() <= hi_ && (down ? x->e <= lim && x->e >= wLo : x->e >= lim && x->e <= wHi))
                        sv.push_back(x);
            if (down) std::reverse(sv.begin(), sv.end());
            struct G {
                int64_t e, seq;
                uint8_t key[32];
            };
            std::vector<G> group;
            bool have = false;
            auto settle = [&]() -> int32_t {
                std::sort(group.begin(), group.end(), [](const G& a, const G& b) { return std::memcmp(a.key, b.key, 32) < 0; });
                for (const G& g : group) {
                    const int32_t ok = passes(g.seq);
                    if (ok < 0) return ok;
                    if (!ok) continue;
                    out->e = g.e;
                    out->seq = g.seq;
                    std::memcpy(out->key, g.key, 32);
                    out->fid = fid;
                    have = true;
                    return P4_OK;
                }
                group.clear();
                return P4_OK;
            };
            size_t si = 0;
            bool sqlDone = false, pend = false;
            G pg{};
            for (;;) {
                if (!pend && !sqlDone) {
                    const int r = sqlite3_step(q);
                    if (r == SQLITE_ROW) {
                        L_->rowsExamined++;
                        if (sqlite3_column_bytes(q, 1) != 32) continue;
                        pg.e = sqlite3_column_int64(q, 0);
                        std::memcpy(pg.key, sqlite3_column_blob(q, 1), 32);
                        pg.seq = sqlite3_column_int64(q, 2);
                        pend = true;
                    } else {
                        sqlDone = true;
                        if (r != SQLITE_DONE) {
                            sqlite3_reset(q);
                            return statusOfSqlite(r);
                        }
                    }
                }
                G g;
                if (si < sv.size() && (!pend || (down ? sv[si]->e >= pg.e : sv[si]->e <= pg.e))) {
                    const StRow* x = sv[si++];
                    L_->rowsExamined++;
                    g.e = x->e;
                    g.seq = x->seq();
                    std::memcpy(g.key, x->cid, 32);
                } else if (pend) {
                    g = pg;
                    pend = false;
                } else {
                    break;
                }
                if (!group.empty() && group[0].e != g.e) {
                    const int32_t s2 = settle();
                    if (s2 != P4_OK) {
                        sqlite3_reset(q);
                        return s2;
                    }
                    if (have) break;
                }
                group.push_back(g);
            }
            sqlite3_reset(q);
            if (!have && !group.empty()) {
                const int32_t s2 = settle();
                if (s2 != P4_OK) return s2;
            }
            return have ? 1 : 0;
        };
        auto object = [&](const std::string& ent, int64_t kk, const std::vector<const StRow*>* sk) -> int32_t {
            Pick b, a;
            int got = 0;
            if (profile != 4) {
                const int32_t r = pick(below, kk, sk, true, &b);
                if (r < 0) return r;
                got |= r;
            }
            if (profile != 3) {
                const int32_t r = pick(above, kk, sk, false, &a);
                if (r < 0) return r;
                got |= r << 1;
            }
            if (!got) return P4_OK;
            const Pick& p = got == 1 ? b : got == 2 ? a : (better(b.e, b.key, a.e, a.key) ? b : a);
            take(ent, p);
            return check();
        };
        // An object's entity: its key's text.
        auto entOf = [](const KVal& k, std::string* tmp) -> const std::string& {
            if (k.type == 3) return k.s;
            *tmp = k.text();
            return *tmp;
        };
        // The staged rows by object (keys in order; rows without an object first).
        std::vector<std::pair<KVal, std::vector<const StRow*>>> stObj;
        if (st && !kp) st->byObject(&stObj);
        size_t oi = 0;
        const std::vector<const StRow*>* stNoK = nullptr;
        if (!stObj.empty() && stObj[0].first.type == 0) stNoK = &stObj[oi++].second;
        if (kp) {
            for (const auto& v : kp->vals) {
                KVal kv;
                kv.type = v.type == ps::rb1::kInt ? 1 : 3;
                kv.i = v.type == ps::rb1::kInt ? v.i : 0;
                if (kv.type == 3) kv.s = v.s;
                std::vector<const StRow*> sk;
                if (st) st->ofK(kv, &sk);
                kv.bind(keyId, 1);
                int64_t kk = 0;
                const int kr = sqlite3_step(keyId);
                if (kr == SQLITE_ROW) kk = sqlite3_column_int64(keyId, 0);
                sqlite3_reset(keyId);
                if (kr != SQLITE_ROW && kr != SQLITE_DONE) rc = statusOfSqlite(kr);
                std::string tmp;
                const std::string& ent = entOf(kv, &tmp);
                if (rc == P4_OK && (kk || !sk.empty()) && !skip(ent)) rc = object(ent, kk, st ? &sk : nullptr);
                if (rc != P4_OK) break;
            }
        } else {
            // okey's keys (one r_ke seek each for the merged rows) and the
            // staged rows' keys (every one of them is in okey: a staged row's
            // key is added with it), merged in key order.
            KVal sqlK;
            int64_t sqlId = 0;
            bool haveSql = false;
            int r;
            auto nextSql = [&]() {
                r = sqlite3_step(keys);
                haveSql = r == SQLITE_ROW;
                if (haveSql) {
                    sqlId = sqlite3_column_int64(keys, 0);
                    sqlK.from(keys, 1);
                }
                if (r != SQLITE_ROW && r != SQLITE_DONE) rc = statusOfSqlite(r);
            };
            nextSql();
            uint64_t skipped = 0;
            while (rc == P4_OK && (haveSql || oi < stObj.size())) {
                const int cmp = !haveSql ? 1 : oi >= stObj.size() ? -1 : kCmp(sqlK, stObj[oi].first);
                const KVal& k = cmp <= 0 ? sqlK : stObj[oi].first;
                std::string tmp;
                const std::string& ent = entOf(k, &tmp);
                if (!skip(ent)) {
                    rc = object(ent, cmp <= 0 ? sqlId : 0, cmp >= 0 ? &stObj[oi].second : nullptr);
                } else {
                    // Keys come in SQLite's order (integers, then text in text
                    // order): past a page's skipped text key every key is skipped.
                    if (!want->countOnly && k.type == 3) break;
                    if ((++skipped & 4095) == 0) rc = check();
                }
                if (cmp >= 0) oi++;
                if (rc == P4_OK && cmp <= 0) nextSql();
            }
            sqlite3_reset(keys);
            // Records without an object: each its own entity (its CID).
            if (rc == P4_OK) {
                const int64_t a0 = profile == 4 ? std::max(at, wLo) : wLo;
                const int64_t a1 = profile == 3 ? std::min(at, wHi) : wHi;
                sqlite3_bind_int64(noK, 1, a0);
                sqlite3_bind_int64(noK, 2, a1);
                sqlite3_bind_int64(noK, 3, lo_);
                sqlite3_bind_int64(noK, 4, hi_);
                std::vector<Pick> ps;
                while (rc == P4_OK && (r = sqlite3_step(noK)) == SQLITE_ROW) {
                    L_->rowsExamined++;
                    if (sqlite3_column_bytes(noK, 1) != 32) continue;
                    Pick p;
                    p.e = sqlite3_column_int64(noK, 0);
                    std::memcpy(p.key, sqlite3_column_blob(noK, 1), 32);
                    p.seq = sqlite3_column_int64(noK, 2);
                    p.fid = fid;
                    ps.push_back(p);
                }
                if (rc == P4_OK && r != SQLITE_DONE) rc = statusOfSqlite(r);
                sqlite3_reset(noK);
                if (stNoK)
                    for (const StRow* x : *stNoK) {
                        if (!x->hasE || x->e < a0 || x->e > a1 || x->seq() <= lo_ || x->seq() > hi_) continue;
                        L_->rowsExamined++;
                        Pick p;
                        p.e = x->e;
                        std::memcpy(p.key, x->cid, 32);
                        p.seq = x->seq();
                        p.fid = fid;
                        ps.push_back(p);
                    }
                for (const Pick& p : ps) {
                    if (rc != P4_OK) break;
                    char cid[60];
                    cidTextFromKey(p.key, cid);
                    const std::string ent(cid, kCidText);
                    if (skip(ent)) continue;
                    const int32_t ok = passes(p.seq);
                    if (ok < 0) rc = ok;
                    if (ok <= 0) continue;
                    take(ent, p);
                }
            }
        }
        c->exec("COMMIT");
        e_->rpool.release(c);
    }
    return rc;
}

// ---- request decoding ------------------------------------------------------------------------------
int32_t decodePreds(const std::vector<Tlv>& v, std::vector<Spec2::Pred>* out) {
    for (const Tlv& t : v) {
        if (t.tag != 17) continue;
        if (t.n < 4) return P4_E_ARG;
        Spec2::Pred p;
        p.field = t.v[0];
        p.op = t.v[1];
        const uint16_t n = ld16(t.v + 2);
        std::vector<ps::rb1::Cell> cells;
        std::vector<uint8_t> buf(4);
        st32(buf.data(), n);
        buf.insert(buf.end(), t.v + 4, t.v + t.n);
        if (!ps::rb1::decodeParams(buf.data(), buf.size(), &cells) || cells.size() != n) return P4_E_ARG;
        const bool okField = (p.field >= P4_F_EPOCH && p.field <= P4_F_EPOCH_DAY) || (p.field >= P4_F_COL0 && p.field <= P4_F_COL3);
        if (!okField || p.op < P4_OP_EQ || p.op > P4_OP_NOTNULL) return P4_E_ARG;
        if ((p.op <= P4_OP_GE || p.op == P4_OP_LIKE) && n != 1) return P4_E_ARG;
        if (p.op == P4_OP_BETWEEN && n != 2) return P4_E_ARG;
        if (p.op == P4_OP_IN && n < 1) return P4_E_ARG;
        if (p.op == P4_OP_NOTNULL && n != 0) return P4_E_ARG;
        p.vals = std::move(cells);
        out->push_back(std::move(p));
    }
    return P4_OK;
}

// The first second of a "YYYY-MM-DD" day (false: not such a day).
bool dayStart(const ps::rb1::Cell& c, int64_t* sec) {
    if (c.type != ps::rb1::kText || c.s.size() != 10 || c.s[4] != '-' || c.s[7] != '-') return false;
    int v[3] = {0, 0, 0};
    const int at[3] = {0, 5, 8}, len[3] = {4, 2, 2};
    for (int k = 0; k < 3; k++)
        for (int i = 0; i < len[k]; i++) {
            const char ch = c.s[size_t(at[k] + i)];
            if (ch < '0' || ch > '9') return false;
            v[k] = v[k] * 10 + (ch - '0');
        }
    if (v[1] < 1 || v[1] > 12 || v[2] < 1 || v[2] > 31) return false;
    // days from 1970-01-01 (civil calendar)
    const int64_t y = v[0] - (v[1] <= 2);
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const int64_t yoe = y - era * 400;
    const int64_t doy = (153 * (v[1] + (v[1] > 2 ? -3 : 9)) + 2) / 5 + v[2] - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    *sec = (era * 146097 + doe - 719468) * 86400;
    return true;
}

// The w range implied by EPOCH / W / EPOCH_DAY predicates (pruning only;
// every row is checked). An EPOCH_DAY bound needs an epoch, and w is the
// epoch when there is one.
void predRange(Spec2* s) {
    for (auto& p : s->preds) {
        if (p.field == P4_F_EPOCH_DAY) {
            int64_t a = 0, b = 0;
            const bool okA = !p.vals.empty() && dayStart(p.vals[0], &a);
            const bool okB = p.vals.size() > 1 && dayStart(p.vals[1], &b);
            switch (p.op) {
                case P4_OP_EQ: if (okA) { s->wLo = std::max(s->wLo, a); s->wHi = std::min(s->wHi, a + 86399); } break;
                case P4_OP_GE: if (okA) s->wLo = std::max(s->wLo, a); break;
                case P4_OP_GT: if (okA) s->wLo = std::max(s->wLo, a + 86400); break;
                case P4_OP_LE: if (okA) s->wHi = std::min(s->wHi, a + 86399); break;
                case P4_OP_LT: if (okA) s->wHi = std::min(s->wHi, a - 1); break;
                case P4_OP_BETWEEN: if (okA && okB) { s->wLo = std::max(s->wLo, a); s->wHi = std::min(s->wHi, b + 86399); } break;
                case P4_OP_IN: {
                    int64_t lo = INT64_MAX, hi = INT64_MIN, d = 0;
                    bool all = true;
                    for (const auto& v : p.vals) {
                        if (!dayStart(v, &d)) { all = false; break; }
                        lo = std::min(lo, d);
                        hi = std::max(hi, d + 86399);
                    }
                    if (all && !p.vals.empty()) { s->wLo = std::max(s->wLo, lo); s->wHi = std::min(s->wHi, hi); }
                    break;
                }
                default: break;
            }
            continue;
        }
        if (p.field != P4_F_EPOCH && p.field != P4_F_W) continue;
        auto iv = [&](size_t k, int64_t* out) {
            if (k >= p.vals.size()) return false;
            if (p.vals[k].type == ps::rb1::kInt) { *out = p.vals[k].i; return true; }
            if (p.vals[k].type == ps::rb1::kReal && std::isfinite(p.vals[k].d)) { *out = int64_t(std::floor(p.vals[k].d)); return true; }
            return false;
        };
        int64_t a, b;
        switch (p.op) {
            case P4_OP_EQ: if (iv(0, &a)) { s->wLo = std::max(s->wLo, a); s->wHi = std::min(s->wHi, a); } break;
            case P4_OP_GE: if (iv(0, &a)) s->wLo = std::max(s->wLo, a); break;
            case P4_OP_GT: if (iv(0, &a) && p.vals[0].type == ps::rb1::kInt) s->wLo = std::max(s->wLo, a + 1); break;
            case P4_OP_LE: if (iv(0, &a)) s->wHi = std::min(s->wHi, a); break;
            case P4_OP_LT: if (iv(0, &a) && p.vals[0].type == ps::rb1::kInt) s->wHi = std::min(s->wHi, a - 1); break;
            case P4_OP_BETWEEN: if (iv(0, &a) && iv(1, &b)) { s->wLo = std::max(s->wLo, a); s->wHi = std::min(s->wHi, b); } break;
            default: break;
        }
    }
}

int32_t decodeSpec(const std::vector<Tlv>& v, Spec2* s) {
    bool bad = false;
    tlvText(v, 1, &s->type);
    uint8_t u8 = 0;
    if (tlvU8(v, 2, &u8, &bad)) s->hydrate = u8 != 0;
    uint64_t u64 = 0;
    if (tlvU64(v, 3, &u64, &bad)) s->limit = u64;
    if (tlvU64(v, 4, &u64, &bad)) s->offset = u64;
    if (tlvU8(v, 5, &u8, &bad)) s->order = u8;
    int64_t i64 = 0;
    if (tlvI64(v, 6, &i64, &bad)) s->seqAfter = i64;
    if (tlvI64(v, 7, &i64, &bad)) s->seqThrough = i64;
    const Tlv* cid = tlvFind(v, 8);
    if (cid) {
        if (cid->n != kCidBin || !cidBinValid(cid->v)) return P4_E_ARG;
        cidKeyFromDigest(cid->v + 4, s->cidKey);
        s->hasCid = true;
    }
    s->hasPeer = tlvText(v, 9, &s->peer);
    s->hasProducer = tlvText(v, 10, &s->producer);
    for (int i = 0; i < 6; i++) {
        if (tlvText(v, uint16_t(11 + i), &s->lf[i])) {
            s->lfSet[i] = true;
            s->lane = true;
        }
    }
    tlvText(v, 18, &s->search);
    // Tag 20 (C-43 B2): which files a read covers. 0 every file; 2 the type's
    // local file ("<TYPE>@local"); 3 the local file plus the feed files the
    // lane filter selects (format 1's "local" partition with lane source
    // "local").
    if (tlvU8(v, 20, &u8, &bad)) {
        if (u8 != 0 && u8 != 2 && u8 != 3) return P4_E_ARG;
        s->part = u8;
    }
    if (bad) return P4_E_ARG;
    const int32_t rc = decodePreds(v, &s->preds);
    if (rc != P4_OK) return rc;
    predRange(s);
    return P4_OK;
}

// ---- output -------------------------------------------------------------------------------------------
const std::vector<std::string>& recCols() {
    static const std::vector<std::string> c = {"seq", "cid", "producer", "peer", "ts", "epoch", "key", "sig", "data",
                                               "len", "provider", "source", "source_url", "batch", "content_key_id",
                                               "producer_peer", "producer_pubkey", "at"};
    return c;
}

struct Out {
    P4Lane* L;
    ps::rb1::Encoder enc;
    explicit Out(P4Lane* l) : L(l), enc(&l->out) {}
    // The header goes at once: a stream cut by a cap stays header, blocks, RB1E.
    void header(const std::vector<std::string>& cols) {
        enc.header(cols);
        flushFinal(L);
    }
    // After a row: ship whole blocks; the result-rows cap.
    int32_t rowDone() {
        L->rowsOut++;
        if (L->maxResultRows && L->rowsOut > L->maxResultRows) return L->trip = P4_E_BUDGET;
        if (enc.blockBytes() == 0 && !L->out.empty()) return flushOut(L);
        return L->trip;
    }
    void finish(int32_t status, const std::string& err0) {
        // An I/O-class failure names SQLite's last error on this thread.
        const std::string err = status == P4_E_IO && tSqlLog[0] ? err0 + ": " + tSqlLog : err0;
        // Blocks still buffered when a cap or cancel tripped are dropped; the
        // RB1E always goes.
        if (L->trip) L->out.clear();
        enc.flushBlock();
        if (!L->trip) flushOut(L);
        L->out.clear();
        enc.end(status, L->rowsOut, L->rowsExamined, L->bytesRead);
        flushFinal(L);
        slotDoneLane(L, status, err);
    }
};

void putText(ps::rb1::Encoder& enc, const std::string& s) { enc.text(s.data(), s.size()); }

void writeRec(Out& o, const Row& r, const std::string* entityKey, bool tagsNull) {
    auto& enc = o.enc;
    enc.beginRow();
    enc.i64(r.seq);
    char cid[60];
    cidTextFromKey(r.key, cid);
    enc.text(cid, kCidText);
    putText(enc, r.producer);
    putText(enc, r.peer);
    enc.i64(r.ts);
    if (r.hasE) enc.i64(r.e); else enc.null();
    if (entityKey) putText(enc, *entityKey);
    else if (r.k.type == 1) enc.i64(r.k.i);
    else if (r.k.type == 3) putText(enc, r.k.s);
    else enc.null();
    if (!r.sig.empty()) enc.blob(r.sig.data(), r.sig.size()); else enc.null();
    if (r.hasData) enc.blob(r.data.data(), r.data.size()); else enc.null();
    enc.i64(r.len);
    if (!tagsNull && r.hasTag) {
        putText(enc, r.provider);
        putText(enc, r.source);
        putText(enc, r.url);
        putText(enc, r.batch);
        putText(enc, r.ckey);
        putText(enc, r.ppeer);
        putText(enc, r.pkey);
        enc.i64(r.at);
    } else {
        for (int i = 0; i < 8; i++) enc.null();
    }
    enc.endRow();
}

int32_t typeOf(P4Lane* L, const std::string& name, Type** t) {
    *t = L->e->findType(name);
    return *t ? P4_OK : P4_E_NOTYPE;
}

// A CID's rows in the feed files holding it (GET, TAGS; C-38 (1): each
// file's CID index), in feed-id order: the first file only (GET, C-38 (4)),
// or every file (TAGS). Each file's record has its own seq there.
struct Held {
    struct File {
        Feed* f;
        int64_t seq;
        std::vector<RowV> rows;
    };
    std::vector<File> files;
};
int32_t heldRows(P4Lane* L, Type* t, const uint8_t key[32], bool hydrate, bool tags, bool firstOnly, Held* out) {
    *out = Held();
    if (!t->hasFiles.load(std::memory_order_acquire)) return P4_OK;
    const int64_t vis = t->vis.load(std::memory_order_acquire);
    struct F {
        Feed* f;
        bool indexed;
        std::shared_ptr<const Staged> st;  // pinned before the reads below
    };
    std::vector<F> files;
    bool quarantined = false;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& fp : t->feeds) {
            if (!fp->created || fp->k.recs <= 0) continue;
            if (fp->quarantined) {
                quarantined = true;
                continue;
            }
            files.push_back(F{fp.get(), fp->indexed, fp->staged});
        }
    }
    for (const F& fr : files) {
        Feed* f = fr.f;
        int orc = 0;
        Conn* c = L->e->rpool.acquire(f->path, OpenKind::Reader, &orc, nullptr);
        if (!c) return statusOfSqlite(orc);
        std::vector<RowV> rows;
        int64_t seq = 0;
        c->exec("BEGIN");
        std::shared_ptr<p4::Stream> stream;
        int32_t st = hydrate ? snapStream(f, c, &stream) : P4_OK;
        if (st == P4_OK) st = seqOfCid(c, fr.indexed, fr.st.get(), key, &seq);
        if (st == P4_OK && seq && seq <= vis && !dictEnsure(f, c)) st = P4_E_IO;
        sqlite3_stmt* q = st == P4_OK && seq && seq <= vis ? c->get(S_R_SEQ) : nullptr;
        if (st == P4_OK && seq && seq <= vis && !q) st = P4_E_INTERNAL;
        if (q) {
            sqlite3_bind_int64(q, 1, seq << 16);
            sqlite3_bind_int64(q, 2, (seq << 16) | 0xffff);
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                if (sqlite3_column_bytes(q, 6) != 32 || std::memcmp(sqlite3_column_blob(q, 6), key, 32) != 0) continue;
                RowV v;
                v.rid = sqlite3_column_int64(q, 0);
                v.n = uint32_t(sqlite3_column_int64(q, 1));
                v.b = uint32_t(sqlite3_column_int64(q, 2));
                v.c = uint32_t(sqlite3_column_int64(q, 3));
                v.u = uint32_t(sqlite3_column_int64(q, 4));
                v.at = sqlite3_column_int64(q, 5);
                std::memcpy(v.key, key, 32);
                v.hasE = sqlite3_column_type(q, 7) != SQLITE_NULL;
                v.e = sqlite3_column_int64(q, 7);
                v.k.from(q, 8);
                v.ts = sqlite3_column_int64(q, 9);
                if (sqlite3_column_type(q, 11) != SQLITE_NULL)
                    v.sig.assign(static_cast<const char*>(sqlite3_column_blob(q, 11)), size_t(sqlite3_column_bytes(q, 11)));
                v.len = sqlite3_column_int64(q, 12);
                v.off = sqlite3_column_int64(q, 13);
                if (hydrate) {
                    st = streamRead(stream.get(), v.off, v.len, &v.data);
                    if (st != P4_OK) break;
                    v.hasData = true;
                }
                NodeDef nd;
                if (!dictNode(f, c, v.n, &nd)) {
                    st = P4_E_CORRUPT;
                    break;
                }
                v.producer = nd.producer;
                v.peer = nd.peer;
                if (v.b) {
                    BatchDef bd;
                    if (!dictBatch(f, c, v.b, &bd)) {
                        st = P4_E_CORRUPT;
                        break;
                    }
                    v.inst = true;
                    v.batch = bd.batch;
                    v.ppeer = bd.ppeer;
                    v.pkey = bd.pkey;
                }
                L->rowsExamined++;
                L->bytesRead += uint64_t(v.len);
                rows.push_back(std::move(v));
            }
            sqlite3_reset(q);
            if (st == P4_OK && r != SQLITE_DONE) st = statusOfSqlite(r);
            if (st == P4_OK && tags)
                for (RowV& v : rows) {
                    if (v.c) v.ckey = dictCkey(c, v.c);
                    if (v.u) v.url = dictUrl(c, v.u);
                }
        }
        c->exec("COMMIT");
        L->e->rpool.release(c);
        if (st != P4_OK) return st;
        if (!rows.empty()) {
            out->files.push_back(Held::File{f, seq, std::move(rows)});
            if (firstOnly) return P4_OK;
        }
    }
    // A quarantined file may hold the CID: a miss is not certain.
    if (quarantined && out->files.empty()) return P4_E_CORRUPT;
    return P4_OK;
}

// ---- ops ----------------------------------------------------------------------------------------------
int32_t opScanLike(P4Lane* L, const std::vector<Tlv>& v, uint32_t op) {
    Out o(L);
    o.header(recCols());
    Spec2 s;
    int32_t rc = decodeSpec(v, &s);
    Type* t = nullptr;
    if (rc == P4_OK) rc = typeOf(L, s.type, &t);
    if (rc == P4_OK) {
        if (op == P4_OPC_SCAN) {
            if (s.order == 0) s.order = P4_ORDER_SEQ_ASC;
            if (s.order != P4_ORDER_SEQ_ASC && s.order != P4_ORDER_SEQ_DESC && s.order != P4_ORDER_NEWEST &&
                s.order != P4_ORDER_RECENT && s.order != P4_ORDER_W_ASC)
                rc = P4_E_ARG;
            // The two-part orders choose their files themselves (tag 20 is not theirs).
            if (s.part != 0 && (s.order == P4_ORDER_NEWEST || s.order == P4_ORDER_RECENT || s.order == P4_ORDER_W_ASC))
                rc = P4_E_ARG;
        } else {
            if (s.order == 0) s.order = P4_ORDER_W_DESC;
            if (s.order != P4_ORDER_W_DESC && s.order != P4_ORDER_CID) rc = P4_E_ARG;
        }
    }
    if (rc != P4_OK) {
        o.finish(rc, rc == P4_E_NOTYPE ? "type not registered" : "bad request");
        return rc;
    }
    // Format 1's two-part pages (C-39 E2, E3): the tagged records (the feed
    // files) in the order, the offset over them; then, without a lane
    // filter, the untagged records (the local file) from the start, as many
    // as the limit leaves (format 1 runs its untagged query without the
    // offset).
    const bool twoPart = op == P4_OPC_SCAN && (s.order == P4_ORDER_NEWEST || s.order == P4_ORDER_RECENT || s.order == P4_ORDER_W_ASC);
    uint64_t emitted = 0;
    auto run = [&](const Spec2& spec) -> int32_t {
        Scan sc(L, t, spec);
        int32_t st = sc.open();
        Row* r;
        while (st == P4_OK) {
            const int32_t n = sc.next(&r);
            if (n <= 0) {
                st = n;
                break;
            }
            writeRec(o, *r, nullptr, false);
            emitted++;
            st = o.rowDone();
        }
        return st;
    };
    if (!twoPart) {
        rc = run(s);
    } else {
        Spec2 tagged = s;
        tagged.part = 1;
        rc = run(tagged);
        if (rc == P4_OK && !s.lane && (!s.limit || emitted < s.limit)) {
            Spec2 untagged = s;
            untagged.part = 2;
            untagged.offset = 0;
            untagged.limit = s.limit ? s.limit - emitted : 0;
            rc = run(untagged);
        }
    }
    L->e->bump(kStReads);
    o.finish(rc, rc == P4_OK ? "" : "read failed");
    return rc;
}

int32_t opGet(P4Lane* L, const std::vector<Tlv>& v) {
    Engine* e = L->e;
    Out o(L);
    o.header(recCols());
    std::string name;
    tlvText(v, 1, &name);
    bool bad = false;
    uint8_t hydrate = 0, every = 0;
    tlvU8(v, 2, &hydrate, &bad);
    tlvU8(v, 41, &every, &bad);
    const Tlv* cids = tlvFind(v, 40);
    Type* t = nullptr;
    int32_t rc = bad || !cids || cids->n < 4 || (cids->n - 4) != size_t(ld32(cids->v)) * kCidBin ? P4_E_ARG
                                                                                                  : typeOf(L, name, &t);
    if (rc != P4_OK) {
        o.finish(rc, "bad request");
        return rc;
    }
    const uint32_t n = ld32(cids->v);
    for (uint32_t i = 0; i < n && rc == P4_OK; i++) {
        const uint8_t* c36 = cids->v + 4 + size_t(i) * kCidBin;
        if (!cidBinValid(c36)) continue;  // not a bafkrei CID: never stored, a miss
        uint8_t key[32];
        cidKeyFromDigest(c36 + 4, key);
        Held h;
        rc = heldRows(L, t, key, hydrate != 0, false, true, &h);
        if (rc != P4_OK || h.files.empty()) continue;
        // One row per copy (token), the lowest token first (C-12), from the
        // first feed file holding the CID (C-38 (4)).
        const Held::File& hf = h.files[0];
        std::map<uint32_t, const RowV*> copies;
        std::map<std::string, uint32_t> ids;
        for (const RowV& x : hf.rows) {
            auto it = ids.find(x.producer);
            uint32_t id;
            if (it == ids.end()) {
                std::lock_guard<std::mutex> g(t->mu);
                id = tokFor(t, x.producer, std::string(), false);
                if (!id) id = UINT32_MAX - uint32_t(ids.size());
                ids[x.producer] = id;
            } else {
                id = it->second;
            }
            if (!copies.count(id)) copies[id] = &x;
        }
        for (auto& kv : copies) {
            const RowV& x = *kv.second;
            o.enc.beginRow();
            o.enc.i64(hf.seq);
            char cid[60];
            cidTextFromKey(key, cid);
            o.enc.text(cid, kCidText);
            putText(o.enc, x.producer);
            putText(o.enc, x.peer);
            o.enc.i64(x.ts);
            if (x.hasE) o.enc.i64(x.e); else o.enc.null();
            if (x.k.type == 1) o.enc.i64(x.k.i);
            else if (x.k.type == 3) putText(o.enc, x.k.s);
            else o.enc.null();
            if (!x.sig.empty()) o.enc.blob(x.sig.data(), x.sig.size()); else o.enc.null();
            if (hydrate && x.hasData) o.enc.blob(x.data.data(), x.data.size()); else o.enc.null();
            o.enc.i64(x.len);
            for (int k = 0; k < 8; k++) o.enc.null();
            o.enc.endRow();
            rc = o.rowDone();
            if (rc != P4_OK || !every) break;
        }
    }
    e->bump(kStReads);
    o.finish(rc, rc == P4_OK ? "" : "read failed");
    return rc;
}

int32_t opTags(P4Lane* L, const std::vector<Tlv>& v) {
    Engine* e = L->e;
    Out o(L);
    o.header({"cid", "seq", "producer", "provider", "source", "source_url", "batch", "content_key_id",
                  "producer_peer", "producer_pubkey", "at"});
    std::string name;
    tlvText(v, 1, &name);
    const Tlv* cids = tlvFind(v, 40);
    Type* t = nullptr;
    int32_t rc = !cids || cids->n < 4 || (cids->n - 4) != size_t(ld32(cids->v)) * kCidBin ? P4_E_ARG : typeOf(L, name, &t);
    if (rc != P4_OK) {
        o.finish(rc, "bad request");
        return rc;
    }
    const uint32_t n = ld32(cids->v);
    for (uint32_t i = 0; i < n && rc == P4_OK; i++) {
        const uint8_t* c36 = cids->v + 4 + size_t(i) * kCidBin;
        if (!cidBinValid(c36)) continue;
        uint8_t key[32];
        cidKeyFromDigest(c36 + 4, key);
        Held h;
        rc = heldRows(L, t, key, false, true, false, &h);
        if (rc != P4_OK) break;
        // One row per (cid, tag identity) over the feed files holding the
        // CID, merged over copies: at = the earliest; source_url = the
        // instance's (its rows carry the latest write's); producer = the copy
        // holding the earliest at; seq = the record's in that feed file.
        struct Merged {
            std::string provider, source, batch, ckey, ppeer, pkey, url, producer;
            int64_t at, seq;
            uint32_t tok;
        };
        std::vector<Merged> rows;
        for (const Held::File& hf : h.files)
            for (const RowV& x : hf.rows) {
                if (!x.inst) continue;
                uint32_t tok;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    tok = tokFor(t, x.producer, std::string(), false);
                }
                Merged* m = nullptr;
                for (Merged& y : rows)
                    if (y.provider == hf.f->provider && y.source == hf.f->source && y.batch == x.batch && y.ckey == x.ckey &&
                        y.ppeer == x.ppeer && y.pkey == x.pkey)
                        m = &y;
                if (!m) {
                    rows.push_back(Merged{hf.f->provider, hf.f->source, x.batch, x.ckey, x.ppeer, x.pkey, x.url, x.producer,
                                          x.at, hf.seq, tok});
                    continue;
                }
                if (x.at < m->at || (x.at == m->at && tok < m->tok)) {
                    m->at = x.at;
                    m->producer = x.producer;
                    m->tok = tok;
                }
            }
        std::sort(rows.begin(), rows.end(), [](const Merged& a, const Merged& b) {
            if (a.at != b.at) return a.at < b.at;
            const std::string* fx[6] = {&a.provider, &a.source, &a.batch, &a.ckey, &a.ppeer, &a.pkey};
            const std::string* fy[6] = {&b.provider, &b.source, &b.batch, &b.ckey, &b.ppeer, &b.pkey};
            for (int k = 0; k < 6; k++)
                if (*fx[k] != *fy[k]) return *fx[k] < *fy[k];
            return false;
        });
        char cid[60];
        cidTextFromKey(key, cid);
        for (const Merged& m : rows) {
            o.enc.beginRow();
            o.enc.text(cid, kCidText);
            o.enc.i64(m.seq);
            putText(o.enc, m.producer);
            putText(o.enc, m.provider);
            putText(o.enc, m.source);
            putText(o.enc, m.url);
            putText(o.enc, m.batch);
            putText(o.enc, m.ckey);
            putText(o.enc, m.ppeer);
            putText(o.enc, m.pkey);
            o.enc.i64(m.at);
            o.enc.endRow();
            rc = o.rowDone();
            if (rc != P4_OK) break;
        }
    }
    e->bump(kStReads);
    o.finish(rc, rc == P4_OK ? "" : "read failed");
    return rc;
}

int32_t opHead(P4Lane* L, const std::vector<Tlv>& v) {
    Out o(L);
    o.header({"n", "bytes", "max_seq", "max_ts", "max_at", "through", "more"});
    Spec2 s;
    int32_t rc = decodeSpec(v, &s);
    bool bad = false;
    uint64_t cap = 0;
    tlvU64(v, 19, &cap, &bad);
    Type* t = nullptr;
    if (rc == P4_OK && bad) rc = P4_E_ARG;
    if (rc == P4_OK) rc = typeOf(L, s.type, &t);
    if (rc != P4_OK) {
        o.finish(rc, "bad request");
        return rc;
    }
    int64_t n = 0, bytes = 0, maxSeq = 0, maxTs = 0, maxAt = 0;
    bool more = false;
    const int64_t through = t->vis.load(std::memory_order_acquire);
    // No filter: the type's counters.
    const bool plain = !s.lane && s.part == 0 && s.preds.empty() && !s.hasCid && !s.hasPeer && !s.hasProducer &&
                       s.search.empty() && s.seqAfter == 0 && s.seqThrough == 0 && !cap && s.offset == 0 && s.limit == 0;
    if (plain) {
        {
            std::lock_guard<std::mutex> g(t->mu);
            const TypeTotals tt = t->totals();
            n = tt.recs;
            bytes = tt.rbytes;
            for (auto& f : t->feeds) {
                if (!f->created || f->k.recs <= 0) continue;
                maxSeq = std::max(maxSeq, std::min(f->k.maxseq, through));
                maxTs = std::max(maxTs, f->k.maxts);
                for (auto& kv : f->inst) maxAt = std::max(maxAt, kv.second.maxat);
            }
        }
        o.enc.beginRow();
        o.enc.i64(n);
        o.enc.i64(bytes);
        o.enc.i64(maxSeq);
        o.enc.i64(maxTs);
        o.enc.i64(maxAt);
        o.enc.i64(through);
        o.enc.i64(0);
        o.enc.endRow();
        rc = o.rowDone();
        L->e->bump(kStReads);
        o.finish(rc, "");
        return rc;
    }
    s.needTags = true;
    if (cap) {
        if (s.order == 0) s.order = P4_ORDER_W_DESC;
        if (s.order != P4_ORDER_W_DESC && s.order != P4_ORDER_CID) s.order = P4_ORDER_W_DESC;
    } else {
        s.order = P4_ORDER_SEQ_ASC;
    }
    Scan sc(L, t, s);
    rc = sc.open();
    Row* r;
    while (rc == P4_OK) {
        const int32_t k = sc.next(&r);
        if (k <= 0) {
            rc = k;
            break;
        }
        if (cap && uint64_t(bytes + r->len) > cap) {
            more = true;
            break;
        }
        n++;
        bytes += r->len;
        maxSeq = std::max(maxSeq, r->seq);
        maxTs = std::max(maxTs, r->ts);
        for (int64_t a : r->ats) maxAt = std::max(maxAt, a);
    }
    if (rc == P4_OK) {
        o.enc.beginRow();
        o.enc.i64(n);
        o.enc.i64(bytes);
        o.enc.i64(maxSeq);
        o.enc.i64(maxTs);
        o.enc.i64(maxAt);
        o.enc.i64(through);
        o.enc.i64(more ? 1 : 0);
        o.enc.endRow();
        rc = o.rowDone();
    }
    L->e->bump(kStReads);
    o.finish(rc, rc == P4_OK ? "" : "read failed");
    return rc;
}

// The object rule starts with COL0: r.k is COL0 when it is an integer.
bool kIsCol0(const Spec& sp) {
    const size_t at = sp.rules.find("object ");
    if (at == std::string::npos) return false;
    size_t i = at + 7;
    while (i < sp.rules.size() && sp.rules[i] == ' ') i++;
    return i < sp.rules.size() && sp.rules[i] == '0' && (i + 1 >= sp.rules.size() || sp.rules[i + 1] < '0' || sp.rules[i + 1] > '9');
}

int32_t opIndexPage(P4Lane* L, const std::vector<Tlv>& v) {
    Out o(L);
    o.header({"c0", "epoch", "cid"});
    Spec2 s;
    int32_t rc = decodeSpec(v, &s);
    Type* t = nullptr;
    if (rc == P4_OK) rc = typeOf(L, s.type, &t);
    if (rc != P4_OK) {
        o.finish(rc, "bad request");
        return rc;
    }
    std::shared_ptr<const Spec> sp = t->spec();
    const bool k0 = kIsCol0(*sp);
    s.needTags = s.lane;
    const uint64_t offset = s.offset, limit = s.limit;
    auto emit = [&](Scan& sc, Row& r) -> int32_t {
        o.enc.beginRow();
        if (r.k.type == 1 && k0) {
            o.enc.i64(r.k.i);
        } else if (!k0 || r.k.type == 0) {
            // COL0 by extraction (rows of a page only)
            ps::Extracted x;
            uint8_t scratch[2048];
            bool have = false;
            if (r.hasData || r.sealed) {
                if (sc.cols(r, &x, scratch, sizeof scratch) == P4_OK && x.cols[0].present && x.cols[0].isU64) {
                    o.enc.i64(int64_t(x.cols[0].u));
                    have = true;
                }
            }
            if (!have) o.enc.null();
        } else {
            o.enc.null();
        }
        if (r.hasE) o.enc.i64(r.e); else o.enc.null();
        char cid[60];
        cidTextFromKey(r.key, cid);
        o.enc.text(cid, kCidText);
        o.enc.endRow();
        return o.rowDone();
    };
    // Phase 1: records with an epoch, epoch DESC, cid ASC. Phase 2: the rest, cid ASC.
    uint64_t emitted = 0;
    if (sp->hasEpochRule) {
        Spec2 p1 = s;
        p1.order = P4_ORDER_W_DESC;
        p1.eNotNull = true;
        p1.hydrate = !k0;
        Scan sc(L, t, p1);
        rc = sc.open();
        Row* r;
        while (rc == P4_OK) {
            const int32_t k = sc.next(&r);
            if (k <= 0) {
                rc = k;
                break;
            }
            emitted++;
            rc = emit(sc, *r);
        }
        if (rc == P4_OK && (!limit || emitted < limit)) {
            int64_t nnull = 0;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& f : t->feeds) nnull += f->k.nnull;
            }
            if (nnull > 0) {
                // the phase-1 total decides the phase-2 offset
                uint64_t total = 0;
                Spec2 cnt = s;
                cnt.order = P4_ORDER_W_DESC;
                cnt.eNotNull = true;
                cnt.offset = 0;
                cnt.limit = 0;
                cnt.hydrate = false;
                cnt.needTags = s.lane;
                {
                    Scan cs(L, t, cnt);
                    rc = cs.open();
                    Row* rr;
                    while (rc == P4_OK) {
                        const int32_t k = cs.next(&rr);
                        if (k < 0) rc = k;
                        if (k <= 0) break;
                        total++;
                    }
                }
                Spec2 p2 = s;
                p2.order = P4_ORDER_CID;
                p2.eNull = true;
                p2.offset = offset > total ? offset - total : 0;
                p2.limit = limit ? limit - emitted : 0;
                p2.hydrate = !k0;
                Scan sc2(L, t, p2);
                if (rc == P4_OK) rc = sc2.open();
                while (rc == P4_OK) {
                    const int32_t k = sc2.next(&r);
                    if (k <= 0) {
                        rc = k;
                        break;
                    }
                    rc = emit(sc2, *r);
                }
            }
        }
    } else {
        s.order = P4_ORDER_CID;
        s.hydrate = !k0;
        Scan sc(L, t, s);
        rc = sc.open();
        Row* r;
        while (rc == P4_OK) {
            const int32_t k = sc.next(&r);
            if (k <= 0) {
                rc = k;
                break;
            }
            rc = emit(sc, *r);
        }
    }
    L->e->bump(kStReads);
    o.finish(rc, rc == P4_OK ? "" : "read failed");
    return rc;
}

// ---- EPOCH ------------------------------------------------------------------------------------------
int32_t opEpoch(P4Lane* L, const std::vector<Tlv>& v) {
    Out o(L);
    Spec2 s;
    int32_t rc = decodeSpec(v, &s);
    bool bad = false;
    uint8_t profile = 0, countOnly = 0;
    int64_t at = 0, maxDelta = 0;
    tlvU8(v, 30, &profile, &bad);
    tlvI64(v, 31, &at, &bad);
    tlvI64(v, 32, &maxDelta, &bad);
    tlvU8(v, 33, &countOnly, &bad);
    if (rc == P4_OK && (bad || profile < 1 || profile > 5)) rc = P4_E_ARG;
    Type* t = nullptr;
    if (rc == P4_OK) rc = typeOf(L, s.type, &t);
    if (countOnly) o.header({"n"});
    else if (profile == 5) o.header({"day", "n", "min_epoch", "max_epoch"});
    else o.header(recCols());
    if (rc != P4_OK) {
        o.finish(rc, "bad request");
        return rc;
    }
    s.eNotNull = true;
    if (profile == 1 || profile == 5) {
        Spec2 w = s;
        w.order = P4_ORDER_W_DESC;
        // A window is epoch ascending; a count or coverage takes any order
        // (the index's own: one range read a page).
        w.wAsc = !countOnly && profile == 1;
        if (countOnly || profile == 5) {
            w.limit = 0;
            w.offset = 0;
            w.hydrate = false;
            w.needTags = s.lane;
        }
        int64_t count = 0;
        std::map<std::string, std::array<int64_t, 3>> days;
        Scan sc(L, t, w);
        rc = sc.open();
        Row* r;
        while (rc == P4_OK) {
            const int32_t k = sc.next(&r);
            if (k <= 0) {
                rc = k;
                break;
            }
            if (countOnly) {
                count++;
            } else if (profile == 5) {
                char d[11];
                dayText(r->e, d);
                auto it = days.find(d);
                if (it == days.end()) days[d] = {1, r->e, r->e};
                else {
                    it->second[0]++;
                    it->second[1] = std::min(it->second[1], r->e);
                    it->second[2] = std::max(it->second[2], r->e);
                }
            } else {
                writeRec(o, *r, nullptr, false);
                rc = o.rowDone();
            }
        }
        if (rc == P4_OK && countOnly) {
            o.enc.beginRow();
            o.enc.i64(count);
            o.enc.endRow();
            rc = o.rowDone();
        }
        if (rc == P4_OK && profile == 5 && !countOnly) {
            for (auto& kv : days) {
                o.enc.beginRow();
                putText(o.enc, kv.first);
                o.enc.i64(kv.second[0]);
                o.enc.i64(kv.second[1]);
                o.enc.i64(kv.second[2]);
                o.enc.endRow();
                rc = o.rowDone();
                if (rc != P4_OK) break;
            }
        }
        L->e->bump(kStReads);
        o.finish(rc, rc == P4_OK ? "" : "read failed");
        return rc;
    }
    // Point profiles: one record per entity, ranked exactly as format 1.
    auto better = [&](int64_t ae, const uint8_t* ak, int64_t be, const uint8_t* bk) {
        if (profile == 4) {
            if (ae != be) return ae < be;
        } else if (profile == 2) {
            const uint64_t da = epochDistance(ae, at), db = epochDistance(be, at);
            if (da != db) return da < db;
            if ((ae <= at) != (be <= at)) return ae <= at;
            if (ae != be) return ae > be;
        } else {
            if (ae != be) return ae > be;
        }
        return std::memcmp(ak, bk, 32) < 0;
    };
    Spec2 w = s;
    w.order = P4_ORDER_W_DESC;
    w.limit = 0;
    w.offset = 0;
    w.hydrate = false;
    w.needTags = s.lane;
    if (profile == 3) w.wHi = std::min(w.wHi, at);
    if (profile == 4) w.wLo = std::max(w.wLo, at);
    std::map<std::string, Scan::Pick> best;
    // The entities' best records over the type's feed files, in one snapshot
    // (need: only what a count or a limited page asks); files before REBUILD 1
    // are walked (*handled false).
    auto points = [&](Scan::EpochWant* need, bool* handled) -> int32_t {
        best.clear();
        Scan sc(L, t, w);
        int32_t st = sc.open();
        *handled = false;
        if (st == P4_OK) st = sc.epochPoints(profile, at, &best, handled, need);
        Row* r;
        while (st == P4_OK && !*handled) {
            const int32_t k = sc.next(&r);
            if (k <= 0) {
                st = k;
                break;
            }
            if (profile == 3 && r->e > at) continue;
            if (profile == 4 && r->e < at) continue;
            std::string ent = r->k.text();
            if (ent.empty()) {
                char cid[60];
                cidTextFromKey(r->key, cid);
                ent.assign(cid, kCidText);
            }
            auto it = best.find(ent);
            if (it == best.end() || better(r->e, r->key, it->second.e, it->second.key)) {
                Scan::Pick p;
                p.e = r->e;
                p.seq = r->seq;
                p.fid = r->fid;
                std::memcpy(p.key, r->key, 32);
                best[ent] = p;
            }
        }
        return st;
    };
    // The records are answered a chunk of picks at a time. A count takes each
    // entity once; a page of limit records after offset that fits one chunk
    // takes its first offset + limit entities only (C-46 (5)).
    constexpr uint64_t kChunk = 4096;
    Scan::EpochWant need;
    need.countOnly = countOnly != 0;
    need.maxDelta = maxDelta;
    if (!countOnly && s.limit && s.offset < kChunk && s.limit <= kChunk - s.offset) need.n = size_t(s.offset + s.limit);
    bool handled = false;
    rc = points(need.countOnly || need.n ? &need : nullptr, &handled);
    const bool pruned = handled && need.n && need.pruned;
    if (rc == P4_OK) {
        std::vector<std::pair<std::string, Scan::Pick>> picks;
        // The picks within maxDelta, in entity order (cut: a pruned page's
        // n-th entity, past which `best` holds nothing complete).
        auto pickList = [&](const std::string* cut) {
            picks.clear();
            for (auto& kv : best) {
                if (cut && kv.first > *cut) break;
                if (maxDelta > 0 && epochDistance(kv.second.e, at) > uint64_t(maxDelta)) continue;
                picks.push_back(kv);
            }
        };
        pickList(pruned ? &*need.conf.rbegin() : nullptr);
        if (countOnly) {
            o.enc.beginRow();
            o.enc.i64(handled ? int64_t(need.counted.size()) : int64_t(picks.size()));
            o.enc.endRow();
            rc = o.rowDone();
        } else {
            // The records with their matched tags, written in the picks' order
            // (entity text ascending).
            Spec2 out = s;
            out.order = P4_ORDER_SEQ_ASC;
            out.limit = 0;
            out.offset = 0;
            out.needTags = true;
            if (profile == 3) out.wHi = std::min(out.wHi, at);
            if (profile == 4) out.wLo = std::max(out.wLo, at);
            uint64_t emitted = 0;
            // false: a pick of a pruned page (whole: its one chunk) is gone
            // since it was picked (a delete in between); nothing is written.
            auto emit = [&](Scan& os, bool whole) -> bool {
                size_t pos = size_t(std::min<uint64_t>(s.offset, picks.size()));
                while (rc == P4_OK && pos < picks.size() && !(s.limit && emitted >= s.limit)) {
                    const size_t end = std::min(picks.size(), pos + size_t(s.limit ? std::min(kChunk, s.limit - emitted) : kChunk));
                    std::vector<std::pair<uint32_t, int64_t>> chunk;
                    for (size_t i = pos; i < end; i++) chunk.push_back({picks[i].second.fid, picks[i].second.seq});
                    std::vector<std::pair<int64_t, Row>> got;
                    rc = os.answerPicks(chunk, &got);
                    std::map<std::pair<uint32_t, int64_t>, Row*> bySeq;
                    for (auto& g : got) bySeq[{g.second.fid, g.first}] = &g.second;
                    if (rc == P4_OK && whole)
                        for (const auto& c : chunk)
                            if (!bySeq.count(c)) return false;
                    for (size_t i = pos; rc == P4_OK && i < end; i++) {
                        auto it = bySeq.find({picks[i].second.fid, picks[i].second.seq});
                        if (it == bySeq.end()) continue;  // gone since the pick, or filtered out
                        writeRec(o, *it->second, &picks[i].first, false);
                        rc = o.rowDone();
                        emitted++;
                    }
                    pos = end;
                }
                return true;
            };
            bool done;
            {
                Scan os(L, t, out);
                rc = os.open();
                done = rc != P4_OK || emit(os, pruned);
            }
            if (!done) {
                // The page as an unpruned one: every entity picked again (a new
                // snapshot), the page filled from the next entities.
                rc = points(nullptr, &handled);
                pickList(nullptr);
                Scan os(L, t, out);
                if (rc == P4_OK) rc = os.open();
                if (rc == P4_OK) emit(os, false);
            }
        }
    }
    L->e->bump(kStReads);
    o.finish(rc, rc == P4_OK ? "" : "read failed");
    return rc;
}

// ---- SUMMARY ---------------------------------------------------------------------------------------
int32_t opSummary(P4Lane* L, const std::vector<Tlv>& v) {
    Engine* e = L->e;
    Out o(L);
    bool bad = false;
    uint8_t kind = 0;
    tlvU8(v, 45, &kind, &bad);
    std::string only;
    tlvText(v, 1, &only);
    static const std::vector<std::string> cols[6] = {
        {},
        {"type", "records", "copies", "bytes", "copy_bytes", "min_epoch", "max_epoch", "min_ts", "max_ts", "max_seq", "through"},
        {"type", "producer", "peer", "records", "bytes", "min_ts", "max_ts", "max_seq", "files"},
        {"type", "producer", "provider", "source", "batch", "content_key_id", "producer_peer", "producer_pubkey",
         "source_url", "records", "bytes", "max_seq", "first", "updated", "min_w", "max_w"},
        {"type", "files", "db_bytes", "wal_bytes", "journal_bytes", "index_bytes", "fts_bytes", "free_bytes"},
        {"type", "state", "through"}};
    if (bad || kind < 1 || kind > 5) {
        o.header({});
        o.finish(P4_E_ARG, "SUMMARY kind (tag 45) is 1-5");
        return P4_E_ARG;
    }
    o.header(cols[kind]);
    std::vector<Type*> types;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        for (auto& t : e->types)
            if (only.empty() || t->name == only) types.push_back(t.get());
    }
    if (!only.empty() && types.empty()) {
        o.finish(P4_E_NOTYPE, "type not registered");
        return P4_E_NOTYPE;
    }
    int32_t rc = P4_OK;
    for (Type* t : types) {
        if (rc != P4_OK) break;
        auto nullOr = [&](int64_t x, int64_t none) {
            if (x == none) o.enc.null(); else o.enc.i64(x);
        };
        if (kind == 1) {
            // C-38 (5): a record held by N feeds counts once per feed.
            int64_t uniq, uniqBytes, copies, copyBytes, mine, maxe, mints, maxts, maxseq = 0;
            {
                std::lock_guard<std::mutex> g(t->mu);
                const TypeTotals tt = t->totals();
                uniq = tt.recs;
                uniqBytes = tt.rbytes;
                copies = tt.copies;
                copyBytes = tt.cbytes;
                mine = tt.mine;
                maxe = tt.maxe;
                mints = tt.mints;
                maxts = tt.maxts;
                maxseq = tt.maxseq;
            }
            const int64_t through = t->vis.load();
            o.enc.beginRow();
            putText(o.enc, t->name);
            o.enc.i64(uniq);
            o.enc.i64(copies);
            o.enc.i64(uniqBytes);
            o.enc.i64(copyBytes);
            nullOr(uniq ? mine : INT64_MAX, INT64_MAX);
            nullOr(uniq ? maxe : INT64_MIN, INT64_MIN);
            o.enc.i64(mints == INT64_MAX || !uniq ? 0 : mints);
            o.enc.i64(uniq ? maxts : 0);
            o.enc.i64(std::min(maxseq, through));
            o.enc.i64(through);
            o.enc.endRow();
            rc = o.rowDone();
        } else if (kind == 2) {
            // One row per producer token: the copies it holds over the feed
            // files (each file's token counts summed).
            std::vector<TokDef> toks;
            std::vector<TokCount> cnt;
            int64_t files = 0;
            {
                std::lock_guard<std::mutex> g(t->mu);
                toks = t->toks;
                cnt = t->totals().toks;
                for (auto& f : t->feeds) files += f->created && f->k.recs > 0;
            }
            for (size_t i = 0; i < toks.size() && i < cnt.size(); i++) {
                const TokDef& d = toks[i];
                const TokCount& tc = cnt[i];
                if (tc.n <= 0) continue;
                o.enc.beginRow();
                putText(o.enc, t->name);
                putText(o.enc, d.token);
                putText(o.enc, d.peer);
                o.enc.i64(tc.n);
                o.enc.i64(tc.bytes);
                o.enc.i64(tc.mints == INT64_MAX ? 0 : tc.mints);
                o.enc.i64(tc.maxts);
                o.enc.i64(tc.maxseq);
                o.enc.i64(files);
                o.enc.endRow();
                rc = o.rowDone();
                if (rc != P4_OK) break;
            }
        } else if (kind == 3) {
            // One row per (feed, instance): the feed's provider and source, the
            // instance's batch, key and producer (C-37 (11)).
            struct R {
                std::string provider, source;
                InstCount c;
            };
            std::vector<R> rows;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& f : t->feeds) {
                    if (!f->created || f->local) continue;
                    for (auto& kv : f->inst)
                        if (kv.second.n > 0) rows.push_back(R{f->provider, f->source, kv.second});
                }
            }
            for (const R& x : rows) {
                const InstCount& c = x.c;
                o.enc.beginRow();
                putText(o.enc, t->name);
                putText(o.enc, std::string());
                putText(o.enc, x.provider);
                putText(o.enc, x.source);
                putText(o.enc, c.batch);
                putText(o.enc, c.ckey);
                putText(o.enc, c.ppeer);
                putText(o.enc, c.pkey);
                putText(o.enc, c.url);
                o.enc.i64(c.n);
                o.enc.i64(c.bytes);
                o.enc.i64(c.maxseq);
                o.enc.i64(c.first);
                o.enc.i64(c.updated);
                nullOr(c.minw, INT64_MAX);
                nullOr(c.maxw, INT64_MIN);
                o.enc.endRow();
                rc = o.rowDone();
                if (rc != P4_OK) break;
            }
        } else if (kind == 4) {
            // Maintained sizes, no file scans: each file's pages and free
            // pages after its writer's last commit, its WAL's frames at its
            // last commit, and the T/ files as the maintenance thread last
            // measured them. A file not written since the open is measured
            // once. Rollback journals: none in WAL mode (0).
            // `db` is the feeds' data: each index file and each stream (its
            // committed end, plus a generation a compaction replaced until it
            // goes); `files` counts the feeds.
            std::vector<std::pair<Feed*, std::string>> unread;
            int64_t db = 0, wal = 0, jn = 0, free = 0;
            size_t files = 0;
            std::vector<std::string> paths;
            std::vector<Feed*> feeds;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& f : t->feeds) {
                    if (!f->created) continue;
                    files++;
                    paths.push_back(f->path);
                    feeds.push_back(f.get());
                    if (f->dbBytes < 0) unread.push_back({f.get(), f->path});
                    else db += f->dbBytes;
                    db += f->streamBytes;
                    free += f->freeBytes;
                }
            }
            for (Feed* f : feeds) {
                std::lock_guard<std::mutex> g(f->smu);
                db += f->retiredBytes;
            }
            for (auto& u : unread) {
                const int64_t n = std::max<int64_t>(0, ioSize(u.second));
                db += n;
                std::lock_guard<std::mutex> g(t->mu);
                if (u.first->dbBytes < 0) u.first->dbBytes = n;
            }
            for (const std::string& p : paths) wal += walBytesOf(t->e, p);
            const int64_t idx = t->idxBytes.load(std::memory_order_relaxed) + walBytesOf(t->e, t->pIdx);
            const int64_t fts = t->ftsBytes.load(std::memory_order_relaxed) + walBytesOf(t->e, t->pFts);
            o.enc.beginRow();
            putText(o.enc, t->name);
            o.enc.i64(int64_t(files));
            o.enc.i64(db);
            o.enc.i64(wal);
            o.enc.i64(jn);
            o.enc.i64(idx);
            o.enc.i64(fts);
            o.enc.i64(free);
            o.enc.endRow();
            rc = o.rowDone();
        } else {
            std::shared_ptr<const Spec> sp = t->spec();
            std::string state;
            int64_t through;
            {
                // A type without records yet has nothing to index: ready, so a
                // search answers no rows as format 1 does (C-46 (2)).
                std::lock_guard<std::mutex> g(t->ftsMu);
                const bool empty = !t->hasFiles.load(std::memory_order_acquire);
                state = !sp->fullText ? "off" : t->ftsState == 2 || empty ? "ready" : "building";
                through = t->ftsThrough;
            }
            o.enc.beginRow();
            putText(o.enc, t->name);
            putText(o.enc, state);
            o.enc.i64(through);
            o.enc.endRow();
            rc = o.rowDone();
        }
    }
    e->bump(kStReads);
    o.finish(rc, rc == P4_OK ? "" : "summary failed");
    return rc;
}

}  // namespace

int32_t runRead(P4Lane* L, uint32_t op) {
    std::vector<Tlv> v;
    if (!tlvParse(L->req, L->reqLen, &v)) {
        respondEmpty(L, {}, P4_E_ARG, "malformed request");
        return P4_E_ARG;
    }
    tSqlLog[0] = 0;
    switch (op) {
        case P4_OPC_GET: return opGet(L, v);
        case P4_OPC_TAGS: return opTags(L, v);
        case P4_OPC_SCAN:
        case P4_OPC_WINDOW: return opScanLike(L, v, op);
        case P4_OPC_HEAD: return opHead(L, v);
        case P4_OPC_INDEX_PAGE: return opIndexPage(L, v);
        case P4_OPC_EPOCH: return opEpoch(L, v);
        case P4_OPC_SUMMARY: return opSummary(L, v);
        default:
            respondEmpty(L, {}, P4_E_ARG, "unknown op");
            return P4_E_ARG;
    }
}

}  // namespace p4
}  // namespace flatsql

// ---- p4_reader.h (the SQL surface's view of the engine) ------------------------------------------------
using namespace flatsql::p4;

struct P4Cursor {
    P4Lane* lane = nullptr;
    std::unique_ptr<flatsql::p4::Scan> scan;
    Row* row = nullptr;
    char cid[60];
    P4Tag tag{};
    std::string producer, peer, provider, source, url, batch, ckey, ppeer, pkey;
};

// The sources of the type's feeds (live: with records), sorted and distinct,
// in the lane's buffers (valid until the lane's next call).
static int32_t laneSources(P4Lane* lane, const char* type, bool live, const char* const** out, uint32_t* n) {
    if (!lane || !type || !out || !n) return P4_E_ARG;
    Type* t = lane->e->findType(type);
    if (!t) return P4_E_NOTYPE;
    std::set<std::string> names;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& f : t->feeds) {
            if (f->local || f->provider.rfind("\x1f#", 0) == 0) continue;   // the local file; an id placeholder
            if (live ? f->created && f->k.recs > 0 : f->created || f->registered) names.insert(f->source);
        }
    }
    lane->srcStore.assign(names.begin(), names.end());
    lane->srcPtrs.clear();
    for (auto& s : lane->srcStore) lane->srcPtrs.push_back(s.c_str());
    *out = lane->srcPtrs.data();
    *n = uint32_t(lane->srcPtrs.size());
    return P4_OK;
}

extern "C" {

int32_t p4_cursor_open(P4Lane* lane, const P4ScanSpec* spec, P4Cursor** out) {
    *out = nullptr;
    if (!lane || !spec || !spec->type) return P4_E_ARG;
    Type* t = lane->e->findType(spec->type);
    if (!t) return P4_E_NOTYPE;
    Spec2 s;
    s.type = spec->type;
    const char* lf[6] = {spec->lane.provider, spec->lane.source, spec->lane.batch, spec->lane.contentKeyId,
                         spec->lane.producerPeer, spec->lane.producerPubkey};
    for (int i = 0; i < 6; i++)
        if (lf[i]) {
            s.lf[i] = lf[i];
            s.lfSet[i] = true;
            s.lane = true;
        }
    if (spec->cid) {
        if (!cidBinValid(spec->cid)) return P4_E_ARG;
        cidKeyFromDigest(spec->cid + 4, s.cidKey);
        s.hasCid = true;
    }
    if (spec->peer) {
        s.peer = spec->peer;
        s.hasPeer = true;
    }
    if (spec->producer) {
        s.producer = spec->producer;
        s.hasProducer = true;
    }
    s.seqAfter = spec->seqAfter;
    s.seqThrough = spec->seqThrough;
    for (uint32_t i = 0; i < spec->nPreds; i++) {
        const P4Pred& p = spec->preds[i];
        Spec2::Pred q;
        q.field = p.field;
        q.op = p.op;
        for (uint16_t k = 0; k < p.nvals; k++) {
            const P4Value& pv = p.vals[k];
            flatsql::ps::rb1::Cell c;
            c.type = pv.type;
            c.i = pv.i;
            c.d = pv.d;
            if (pv.type == 3 || pv.type == 4) c.s.assign(reinterpret_cast<const char*>(pv.s), pv.n);
            q.vals.push_back(std::move(c));
        }
        s.preds.push_back(std::move(q));
    }
    if (spec->search) s.search = spec->search;
    s.order = spec->order ? spec->order : P4_ORDER_SEQ_ASC;
    if (s.order < P4_ORDER_SEQ_ASC || s.order > P4_ORDER_CID) return P4_E_ARG;
    s.hydrate = spec->hydrate != 0;
    s.needTags = spec->noTags == 0;
    if (spec->part != 0 && spec->part != 2 && spec->part != 3) return P4_E_ARG;
    s.part = spec->part;
    s.limit = spec->limit;
    s.offset = spec->offset;
    s.bound = spec->bound;
    predRange(&s);
    auto* c = new (std::nothrow) P4Cursor();
    if (!c) return P4_E_NOMEM;
    c->lane = lane;
    c->scan.reset(new (std::nothrow) flatsql::p4::Scan(lane, t, std::move(s)));
    if (!c->scan) {
        delete c;
        return P4_E_NOMEM;
    }
    const int32_t rc = c->scan->open();
    if (rc != P4_OK) {
        delete c;
        return rc;
    }
    *out = c;
    return P4_OK;
}

int32_t p4_cursor_next(P4Cursor* c, P4Row* row) {
    if (!c || !row) return P4_E_ARG;
    Row* r = nullptr;
    const int32_t k = c->scan->next(&r);
    if (k <= 0) return k;
    std::memset(row, 0, sizeof *row);
    row->seq = r->seq;
    row->ts = r->ts;
    row->epoch = r->e;
    row->hasEpoch = r->hasE ? 1 : 0;
    row->keyType = uint8_t(r->k.type);
    row->keyInt = r->k.i;
    row->keyText = reinterpret_cast<const uint8_t*>(r->k.s.data());
    row->keyTextLen = uint32_t(r->k.s.size());
    cidTextFromKey(r->key, c->cid);
    row->cid = c->cid;
    c->producer = r->producer;
    c->peer = r->peer;
    row->producer = c->producer.c_str();
    row->peer = c->peer.c_str();
    row->sig = r->sig.empty() ? nullptr : reinterpret_cast<const uint8_t*>(r->sig.data());
    row->sigLen = uint32_t(r->sig.size());
    row->data = r->hasData ? reinterpret_cast<const uint8_t*>(r->data.data()) : nullptr;
    row->dataLen = r->hasData ? uint32_t(r->data.size()) : 0;
    row->len = r->len;
    row->tag = nullptr;
    if (r->hasTag) {
        c->provider = r->provider;
        c->source = r->source;
        c->url = r->url;
        c->batch = r->batch;
        c->ckey = r->ckey;
        c->ppeer = r->ppeer;
        c->pkey = r->pkey;
        c->tag = P4Tag{c->provider.c_str(), c->source.c_str(), c->url.c_str(), c->batch.c_str(), c->ckey.c_str(),
                       c->ppeer.c_str(), c->pkey.c_str(), r->at};
        row->tag = &c->tag;
    }
    return 1;
}

void p4_cursor_close(P4Cursor* c) { delete c; }

int32_t p4_lane_check(P4Lane* lane) {
    if (!lane) return P4_E_ARG;
    if (lane->trip) return lane->trip;
    if (lane->h && lane->h->cancel.load(std::memory_order_acquire)) return lane->trip = P4_E_CANCELLED;
    if (lane->e->stopWord->load(std::memory_order_acquire)) return lane->trip = P4_E_STOPPED;
    if (lane->maxRows && lane->rowsExamined > lane->maxRows) return lane->trip = P4_E_BUDGET;
    if (lane->maxBytes && lane->bytesRead > lane->maxBytes) return lane->trip = P4_E_BUDGET;
    return P4_OK;
}

int32_t p4_emit(P4Lane* lane, const uint8_t* bytes, uint32_t n) {
    if (!lane) return P4_E_ARG;
    return emitBytes(lane, bytes, n);
}

P4Engine* p4_lane_engine(P4Lane* lane) { return lane ? lane->e : nullptr; }
void* p4_lane_sql_state(P4Lane* lane) { return lane ? lane->sqlState : nullptr; }
void p4_lane_set_sql_state(P4Lane* lane, void* state) {
    if (lane) lane->sqlState = state;
}

int32_t p4_types(P4Lane* lane, const P4TypeInfo** out, uint32_t* n) {
    if (!lane || !out || !n) return P4_E_ARG;
    lane->specRefs.clear();
    lane->typeNames.clear();
    lane->typeInfos.clear();
    {
        std::lock_guard<std::mutex> g(lane->e->typesMu);
        for (auto& t : lane->e->types) {
            lane->specRefs.push_back(t->spec());
            lane->typeNames.push_back(t->name);
        }
    }
    for (size_t i = 0; i < lane->specRefs.size(); i++) {
        const auto& sp = lane->specRefs[i];
        P4TypeInfo ti{};
        ti.name = lane->typeNames[i].c_str();
        ti.bfbs = sp->tc.bfbs().data();
        ti.bfbsLen = uint32_t(sp->tc.bfbs().size());
        std::memcpy(ti.fid, sp->tc.fid(), 4);
        ti.a18Bound = sp->a18Bound;
        ti.epochProfile = sp->epochProfile;
        ti.fullText = sp->fullText ? 1 : 0;
        ti.rules = sp->rules.c_str();
        ti.rulesLen = uint32_t(sp->rules.size());
        lane->typeInfos.push_back(ti);
    }
    *out = lane->typeInfos.data();
    *n = uint32_t(lane->typeInfos.size());
    return P4_OK;
}


int32_t p4_sources(P4Lane* lane, const char* type, const char* const** out, uint32_t* n) {
    return laneSources(lane, type, true, out, n);
}

int32_t p4_feed_sources(P4Lane* lane, const char* type, const char* const** out, uint32_t* n) {
    return laneSources(lane, type, false, out, n);
}

int64_t p4_type_rows(P4Lane* lane, const char* type) {
    if (!lane || !type) return -1;
    flatsql::p4::Type* t = lane->e->findType(type);
    if (!t) return -1;
    std::lock_guard<std::mutex> g(t->mu);
    return t->totals().recs;
}

int64_t p4_visible_through(P4Engine* e, const char* type) {
    if (!e || !type) return 0;
    Type* t = e->findType(type);
    return t ? t->vis.load(std::memory_order_acquire) : 0;
}

uint64_t p4_lane_heap_cap(P4Lane* lane) { return lane ? lane->heapCap : 0; }

void p4_lane_set_error(P4Lane* lane, const char* msg, uint32_t n) {
    if (!lane) return;
    lane->err.assign(msg ? msg : "", msg ? std::min<uint32_t>(n, 255) : 0);
}

void p4_lane_set_rows(P4Lane* lane, uint64_t rows) {
    if (lane) lane->rowsOut = rows;
}

void p4_lane_counters(P4Lane* lane, uint64_t* rowsExamined, uint64_t* bytesRead) {
    if (!lane) return;
    if (rowsExamined) *rowsExamined = lane->rowsExamined;
    if (bytesRead) *bytesRead = lane->bytesRead;
}

}  // extern "C"
