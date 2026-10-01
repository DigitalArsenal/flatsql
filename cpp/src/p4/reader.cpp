// Store format 4: reads (design §5.4, CONTRACT §3.5-§3.9).
//
// Reads never wait on the data layer: they take no lock a writer holds across
// I/O, open files read-write without CREATE and query_only, and hold one short
// read transaction per page (a connection is checked out of the shared pool
// for one page and returned). A stream resumes by key. SQLITE_BUSY and I/O
// errors are errors, never misses (M9).
//
// One scan engine serves every record op and p4_reader.h's cursors:
//   SEQ (asc/desc)  per-file pages by rowid, merged by seq; copies collapse to
//                   the lowest pid that matches; clamped to visible-through;
//                   a source filter walks rl(sid, seq) when it is selective;
//   W (desc)        per-file pages on r_w, merged by w; equal-w groups put in
//                   CID order; copies collapse by CID;
//   CID             the type index c (every month merged) plus pending
//                   entries, rows fetched by (pid, tb, seq).
#include <algorithm>
#include <cmath>
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
            idle_.erase(it);
            auto pit = pos_.find(c);
            if (pit != pos_.end()) {
                lru_.erase(pit->second);
                pos_.erase(pit);
            }
            return c;
        }
    }
    Conn* c = nullptr;
    const int r = openConn(path, kind, cacheKiB_, 0, &c, err);
    if (r != SQLITE_OK) {
        *rc = r;
        return nullptr;
    }
    open_.fetch_add(1);
    return c;
}

void ReaderPool::release(Conn* c) {
    if (!c) return;
    std::vector<Conn*> close;
    {
        std::lock_guard<std::mutex> g(mu_);
        idle_.emplace(c->path, c);
        lru_.push_front(c);
        pos_[c] = lru_.begin();
        while (open_.load() > cap_ && !lru_.empty()) {
            Conn* v = lru_.back();
            lru_.pop_back();
            pos_.erase(v);
            auto range = idle_.equal_range(v->path);
            for (auto it = range.first; it != range.second; ++it)
                if (it->second == v) {
                    idle_.erase(it);
                    break;
                }
            close.push_back(v);
            open_.fetch_sub(1);
        }
    }
    for (Conn* v : close) delete v;
}

void ReaderPool::dropPath(const std::string& path) {
    std::vector<Conn*> close;
    {
        std::lock_guard<std::mutex> g(mu_);
        auto range = idle_.equal_range(path);
        for (auto it = range.first; it != range.second; ++it) {
            close.push_back(it->second);
            auto pit = pos_.find(it->second);
            if (pit != pos_.end()) {
                lru_.erase(pit->second);
                pos_.erase(pit);
            }
        }
        idle_.erase(range.first, range.second);
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
        open_.fetch_sub(uint32_t(close.size()));
    }
    for (Conn* v : close) delete v;
}

namespace {

// ---- the scan engine -----------------------------------------------------------------------------------
struct FRef {
    File* f = nullptr;
    uint32_t pid = 0;
    int64_t tb = 0;
    std::string path, producer, peer;
    int64_t n = 0, minseq = 0, maxseq = 0, minw = 0, maxw = 0, nnull = 0;
    int64_t laneN = 0;  // rows of the filter's lanes (selectivity)
    std::map<uint32_t, std::string> url0;  // lane id -> url of an instance whose rl.u is NULL
};

struct TagInst {
    uint32_t sid, lane;
    int64_t at;
    std::string url;
};

struct Row {
    int64_t seq = 0, ts = 0, e = 0, w = 0, len = 0;
    bool hasE = false;
    uint8_t key[32];
    int kType = 0;
    int64_t kInt = 0;
    std::string kText;
    std::string peer, sig, data, fcols;  // peer: empty when it is the file's (filePeer)
    bool sealed = false, hasData = false, filePeer = false;
    int fi = -1;  // FRef index
    std::vector<TagInst> tags;
    int sel = -1;  // the matched tag (index into tags)
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

class Scan {
public:
    Scan(P4Lane* L, Type* t, Spec2 spec) : L_(L), e_(L->e), t_(t), s_(std::move(spec)) {}
    ~Scan() {}
    int32_t open();
    // 1 row, 0 end, < 0 status. The row stays valid until the next call.
    int32_t next(Row** out);
    int64_t vis() const { return vis_; }
    const FRef& file(int i) const { return files_[size_t(i)]; }
    const std::string& peerOf(const Row& r) const { return r.filePeer ? files_[size_t(r.fi)].peer : r.peer; }
    const Spec* spec() const { return sp_.get(); }
    Type* type() const { return t_; }
    LaneDef* laneDef(uint32_t id) {
        auto it = lanes_.find(id);
        return it == lanes_.end() ? nullptr : &it->second;
    }
    int32_t cols(const Row& r, ps::Extracted* x, uint8_t* scratch, size_t n);  // COL values of a row
    struct EpochPick {
        int64_t e = 0, seq = 0;
        uint8_t key[32];
        int fi = -1;
        uint32_t pid = 0;
    };
    // EPOCH point profiles (2 nearest, 3 as_of, 4 forward) through the type's
    // object directory; *handled = false when the scan must answer instead.
    int32_t epochByObject(int profile, int64_t at, std::map<std::string, EpochPick>* best, bool* handled);
    // The row at (file, seq) with its tags, if it passes the scan's filters:
    // 1, 0, or < 0 status. emit: with every tag and, when the request
    // hydrates, the bytes (the row is output).
    int32_t rowAt(int fi, int64_t seq, Row* out, bool emit = false, bool hydrate = false);

private:
    struct FileCur {
        std::deque<Row> rows;
        int64_t resumeSeq = 0;  // SEQ: last seq fetched
        int64_t resumeW = INT64_MAX;  // W: last (w, seq) fetched
        bool started = false;
        bool done = false;
        bool opened = false;
        size_t candPos = 0;     // object-key candidates taken so far
    };
    int32_t collectCandidates();
    int32_t fetchCandidates(int fi, bool wOrder);
    bool kDriven_ = false;                    // rows come from object-key candidates
    std::vector<std::vector<int64_t>> cand_;  // per file, ascending seqs
    int32_t fetchSeq(int fi);
    int32_t fetchW(int fi);
    int32_t loadRows(Conn* c, int fi, const std::vector<int64_t>& seqs, std::deque<Row>* out);
    void rowFrom(sqlite3_stmt* q, int col0, int fi, bool needData, Row* row);
    bool needData() const;
    int32_t loadTags(Conn* c, int fi, std::deque<Row>& rows);
    bool laneMatch(const TagInst& ti);
    bool rowMatches(Row& r);
    bool countRow(const Row& r);
    int32_t nextSeq(Row** out);
    int32_t nextW(Row** out);
    int32_t nextCid(Row** out);
    int32_t check();

    P4Lane* L_;
    Engine* e_;
    Type* t_;
    Spec2 s_;
    std::shared_ptr<const Spec> sp_;
    std::vector<FRef> files_;
    std::vector<FileCur> cur_;
    std::unordered_map<uint32_t, LaneDef> lanes_;
    std::unordered_set<uint32_t> laneIds_;  // lanes matching the filter
    std::unordered_set<uint32_t> sids_;     // their sources
    int64_t vis_ = 0, lo_ = 0, hi_ = 0;     // seq range (lo exclusive, hi inclusive)
    std::vector<int> order_;                // files in opening order
    size_t nextOpen_ = 0;
    // SEQ merge
    std::vector<int> heap_;
    // W merge
    std::vector<Row> group_;
    size_t groupAt_ = 0;
    // CID
    struct CidEnt {
        std::array<uint8_t, 32> key;
        uint32_t pid = 0;
        int64_t seq = 0, tb = 0;
    };
    struct CidMonth {
        int64_t tb = 0;
        std::vector<CidEnt> buf;
        size_t pos = 0;
        bool exhausted = false;
        std::array<uint8_t, 32> lastKey;
        int64_t lastPid = -1;
    };
    int32_t cidFill(size_t m);
    int32_t cidNextEntry(CidEnt* out, bool* have);
    std::vector<CidMonth> months_;
    std::vector<CidEnt> pendIns_, pendDel_;
    size_t pendAt_ = 0;
    std::unordered_map<uint64_t, int> fileOf_;
    bool cidLoaded_ = false;
    Row out_;
    uint64_t skipped_ = 0, emitted_ = 0, boundSeen_ = 0;
    int64_t lastSeq_ = INT64_MIN;
    uint8_t lastKey_[32] = {};
    bool haveLast_ = false;
    std::unordered_set<int64_t> ftsSeqs_;
    bool fts_ = false;
};

int32_t Scan::check() {
    if (L_->trip) return L_->trip;
    if (L_->h && L_->h->cancel.load(std::memory_order_acquire)) return L_->trip = P4_E_CANCELLED;
    if (e_->stopWord->load(std::memory_order_acquire)) return L_->trip = P4_E_STOPPED;
    if (L_->maxRows && L_->rowsExamined > L_->maxRows) return L_->trip = P4_E_BUDGET;
    if (L_->maxBytes && L_->bytesRead > L_->maxBytes) return L_->trip = P4_E_BUDGET;
    return P4_OK;
}

int32_t Scan::open() {
    sp_ = t_->spec();
    vis_ = t_->vis.load(std::memory_order_acquire);
    hi_ = vis_;
    if (s_.seqThrough > 0 && s_.seqThrough < hi_) hi_ = s_.seqThrough;
    lo_ = s_.seqAfter > 0 ? s_.seqAfter : 0;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        if (s_.lane) {
            for (auto& l : t_->lanes) {
                const std::string* f[6] = {&l->provider, &l->source, &l->batch, &l->ckey, &l->ppeer, &l->pkey};
                bool ok = true;
                for (int i = 0; i < 6 && ok; i++) ok = !s_.lfSet[i] || *f[i] == s_.lf[i];
                if (ok) {
                    laneIds_.insert(l->id);
                    sids_.insert(l->sid);
                }
            }
        }
        for (auto& l : t_->lanes) lanes_[l->id] = *l;
        for (auto& p : t_->parts) {
            if (s_.hasProducer && p->producer != s_.producer) continue;
            for (auto& kv : p->files) {
                File* f = kv.second;
                if (!f->created || f->retired || f->n <= 0 || f->quarantined) continue;
                if (s_.order != P4_ORDER_CID && !s_.bound && (f->maxseq <= lo_ || f->minseq > hi_)) continue;
                FRef r;
                r.f = f;
                r.pid = p->pid;
                r.tb = f->tb;
                r.path = f->path;
                r.producer = p->producer;
                r.peer = p->peer;
                r.n = f->n;
                r.minseq = f->minseq;
                r.maxseq = f->maxseq;
                r.minw = f->minw;
                r.maxw = f->maxw;
                r.nnull = f->nnull;
                for (auto& lk : f->lanes) {
                    r.url0[lk.first] = lk.second.url0;
                    if (laneIds_.count(lk.first)) r.laneN += lk.second.n;
                }
                if (s_.lane && r.laneN == 0) continue;  // no instance of the filter's lanes here
                if (s_.wLo != INT64_MIN || s_.wHi != INT64_MAX) {
                    if (f->maxw < s_.wLo || f->minw > s_.wHi) continue;
                }
                files_.push_back(std::move(r));
            }
        }
    }
    if (s_.lane && laneIds_.empty()) files_.clear();
    cur_.resize(files_.size());
    order_.resize(files_.size());
    for (size_t i = 0; i < files_.size(); i++) order_[i] = int(i);
    if (!s_.search.empty()) {
        // FTS5: the matching seqs (background index; C-4's exception).
        std::lock_guard<std::mutex> g(t_->ftsMu);
        if (!t_->fts) return P4_E_UNSUPPORTED;
        sqlite3_stmt* q = t_->fts->sql("SELECT rowid FROM fts WHERE fts MATCH ?1");
        if (!q) return P4_E_SQL;
        sqlite3_bind_text(q, 1, s_.search.data(), int(s_.search.size()), SQLITE_STATIC);
        int r;
        while ((r = sqlite3_step(q)) == SQLITE_ROW) ftsSeqs_.insert(sqlite3_column_int64(q, 0));
        sqlite3_reset(q);
        if (r != SQLITE_DONE) return P4_E_SQL;
        fts_ = true;
    }
    if (s_.order == P4_ORDER_SEQ_DESC || s_.bound) {
        std::sort(order_.begin(), order_.end(), [&](int a, int b) { return files_[a].maxseq > files_[b].maxseq; });
    } else if (s_.order == P4_ORDER_SEQ_ASC) {
        std::sort(order_.begin(), order_.end(), [&](int a, int b) { return files_[a].minseq < files_[b].minseq; });
    } else if (s_.order == P4_ORDER_W_DESC) {
        if (s_.wAsc)
            std::sort(order_.begin(), order_.end(), [&](int a, int b) { return files_[a].minw < files_[b].minw; });
        else
            std::sort(order_.begin(), order_.end(), [&](int a, int b) { return files_[a].maxw > files_[b].maxw; });
    }
    // A18 (C-17): the bound is the type's newest N seqs, before every other
    // filter: the cut is the N-th newest seq of the type.
    if (s_.bound) {
        std::vector<std::pair<int, std::deque<int64_t>>> heads;
        int64_t cut = 0;
        uint64_t seen = 0;
        int64_t last = INT64_MAX;
        struct HC {
            int fi;
            int64_t at;  // next seq <= at
            std::vector<int64_t> buf;
            size_t pos = 0;
            bool done = false;
        };
        std::vector<HC> hc;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            for (auto& p : t_->parts)
                for (auto& kv : p->files) {
                    File* f = kv.second;
                    if (!f->created || f->retired || f->n <= 0) continue;
                    HC h;
                    h.fi = -1;
                    h.at = vis_;
                    h.buf.clear();
                    hc.push_back(h);
                    hc.back().fi = int(hc.size()) - 1;
                    (void)f;
                }
        }
        std::vector<std::string> paths;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            for (auto& p : t_->parts)
                for (auto& kv : p->files) {
                    File* f = kv.second;
                    if (!f->created || f->retired || f->n <= 0) continue;
                    paths.push_back(f->path);
                }
        }
        hc.resize(paths.size());
        auto refill = [&](size_t i) -> int32_t {
            HC& h = hc[i];
            h.buf.clear();
            h.pos = 0;
            int rc = 0;
            Conn* c = e_->rpool.acquire(paths[i], OpenKind::Reader, &rc, nullptr);
            if (!c) return statusOfSqlite(rc);
            sqlite3_stmt* q = c->sql("SELECT seq FROM r INDEXED BY r_s WHERE seq<=?1 ORDER BY seq DESC LIMIT 4096");
            if (!q) q = c->sql("SELECT seq FROM r WHERE seq<=?1 ORDER BY seq DESC LIMIT 4096");  // before REBUILD 1
            int r = SQLITE_DONE;
            if (q) {
                sqlite3_bind_int64(q, 1, h.at);
                while ((r = sqlite3_step(q)) == SQLITE_ROW) h.buf.push_back(sqlite3_column_int64(q, 0));
                sqlite3_reset(q);
            }
            e_->rpool.release(c);
            if (r != SQLITE_DONE) return statusOfSqlite(r);
            if (h.buf.empty()) h.done = true;
            else h.at = h.buf.back() - 1;
            return P4_OK;
        };
        for (size_t i = 0; i < hc.size(); i++) {
            hc[i].at = vis_;
            const int32_t rc = refill(i);
            if (rc != P4_OK) return rc;
        }
        while (seen < s_.bound) {
            int best = -1;
            for (size_t i = 0; i < hc.size(); i++) {
                if (hc[i].pos >= hc[i].buf.size()) continue;
                if (best < 0 || hc[i].buf[hc[i].pos] > hc[size_t(best)].buf[hc[size_t(best)].pos]) best = int(i);
            }
            if (best < 0) break;
            HC& h = hc[size_t(best)];
            const int64_t sq = h.buf[h.pos++];
            if (h.pos >= h.buf.size() && !h.done) {
                const int32_t rc = refill(size_t(best));
                if (rc != P4_OK) return rc;
            }
            if (sq == last) continue;  // a copy
            last = sq;
            seen++;
            cut = sq;
        }
        if (seen >= s_.bound && cut > 0) lo_ = std::max(lo_, cut - 1);
        // Files wholly below the cut are out.
        std::vector<FRef> keep;
        for (auto& f : files_)
            if (f.maxseq > lo_) keep.push_back(std::move(f));
        files_.swap(keep);
        cur_.assign(files_.size(), FileCur());
        order_.resize(files_.size());
        for (size_t i = 0; i < files_.size(); i++) order_[i] = int(i);
        if (s_.order == P4_ORDER_SEQ_ASC)
            std::sort(order_.begin(), order_.end(), [&](int a, int b) { return files_[a].minseq < files_[b].minseq; });
        else
            std::sort(order_.begin(), order_.end(), [&](int a, int b) { return files_[a].maxseq > files_[b].maxseq; });
    }
    return collectCandidates();
}

// An equality, IN or range predicate on the object rule's first column (the
// object key k, when that column is present) drives the scan through the
// partition's object index instead of walking the window: ent gives each
// object's w range, r_dk(wd, k, w) one seek per day (epoch types), r_k(k, w)
// the rows (object types without an epoch). The candidates are rechecked by
// rowMatches. A COL0 equality inside OMM's 400,000 bound reads its object's
// rows, not the window (R17/R19).
int32_t Scan::collectCandidates() {
    if (s_.order == P4_ORDER_CID || files_.empty() || !sp_->hasObject) return P4_OK;
    const int oc = sp_->tc.firstObjectCol();
    if (oc < 0 || oc > 3) return P4_OK;
    const Spec2::Pred* kp = nullptr;
    for (const auto& p : s_.preds)
        if (p.field == P4_F_COL0 + oc && !p.vals.empty() &&
            (p.op == P4_OP_EQ || p.op == P4_OP_IN || p.op == P4_OP_BETWEEN || p.op == P4_OP_GE || p.op == P4_OP_GT ||
             p.op == P4_OP_LE || p.op == P4_OP_LT)) {
            kp = &p;
            break;
        }
    if (!kp) return P4_OK;
    for (const auto& v : kp->vals)
        if (v.type != ps::rb1::kInt && v.type != ps::rb1::kText && v.type != ps::rb1::kReal) return P4_OK;
    auto bindCell = [](sqlite3_stmt* q, int i, const ps::rb1::Cell& c) {
        if (c.type == ps::rb1::kInt) sqlite3_bind_int64(q, i, c.i);
        else if (c.type == ps::rb1::kReal) sqlite3_bind_double(q, i, c.d);
        else sqlite3_bind_text(q, i, c.s.data(), int(c.s.size()), SQLITE_TRANSIENT);
    };
    const bool eq = kp->op == P4_OP_EQ || kp->op == P4_OP_IN;
    const char* lo = kp->op == P4_OP_GT ? ">" : ">=";
    const char* hi = kp->op == P4_OP_LT ? "<" : "<=";
    const bool hasLo = kp->op == P4_OP_BETWEEN || kp->op == P4_OP_GE || kp->op == P4_OP_GT;
    const bool hasHi = kp->op == P4_OP_BETWEEN || kp->op == P4_OP_LE || kp->op == P4_OP_LT;
    const size_t kMaxSeeks = 50000, kMaxCand = 200000;
    size_t seeks = 0, total = 0;
    std::vector<std::vector<int64_t>> cand(files_.size());
    for (size_t fi = 0; fi < files_.size(); fi++) {
        FRef& fr = files_[fi];
        bool indexed;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            indexed = fr.f->indexed;
        }
        if (!indexed) return P4_OK;  // a migration before REBUILD 1: the window walk
        int rc = 0;
        fr.f->users.fetch_add(1);
        Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
        if (!c) {
            fr.f->users.fetch_sub(1);
            return statusOfSqlite(rc);
        }
        c->exec("BEGIN");
        int32_t status = P4_OK;
        bool giveUp = false;
        std::vector<int64_t>& out = cand[fi];
        auto collect = [&](sqlite3_stmt* q) {
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) out.push_back(sqlite3_column_int64(q, 0));
            sqlite3_reset(q);
            if (r != SQLITE_DONE) status = statusOfSqlite(r);
        };
        if (sp_->ek) {
            struct Obj {
                sqlite3_value* k;
                int64_t fw, lw;
            };
            std::vector<Obj> objs;
            auto takeObjs = [&](sqlite3_stmt* q) {
                int r;
                while ((r = sqlite3_step(q)) == SQLITE_ROW)
                    objs.push_back({sqlite3_value_dup(sqlite3_column_value(q, 0)), sqlite3_column_int64(q, 1),
                                    sqlite3_column_int64(q, 2)});
                sqlite3_reset(q);
                if (r != SQLITE_DONE) status = statusOfSqlite(r);
            };
            if (eq) {
                sqlite3_stmt* q = c->sql("SELECT k, fw, lw FROM ent WHERE k=?1 AND n>0");
                if (!q) status = P4_E_INTERNAL;
                for (size_t i = 0; status == P4_OK && i < kp->vals.size(); i++) {
                    bindCell(q, 1, kp->vals[i]);
                    takeObjs(q);
                }
            } else {
                std::string sql = "SELECT k, fw, lw FROM ent WHERE n>0";
                if (hasLo) sql += std::string(" AND k") + lo + "?1";
                if (hasHi) sql += std::string(" AND k") + hi + "?2";
                sqlite3_stmt* q = c->sql(sql.c_str());
                if (!q) status = P4_E_INTERNAL;
                else {
                    if (hasLo) bindCell(q, 1, kp->vals[0]);
                    if (hasHi) bindCell(q, 2, kp->vals[kp->op == P4_OP_BETWEEN ? 1 : 0]);
                    takeObjs(q);
                }
            }
            sqlite3_stmt* q = c->sql("SELECT seq FROM r INDEXED BY r_dk WHERE wd=?1 AND k=?2 AND seq>?3 AND seq<=?4");
            if (!q && status == P4_OK) status = P4_E_INTERNAL;
            for (const Obj& o : objs) {
                if (status != P4_OK || giveUp) break;
                for (int64_t d = o.fw / 86400; d <= o.lw / 86400 && status == P4_OK; d++) {
                    if (++seeks > kMaxSeeks) {
                        giveUp = true;
                        break;
                    }
                    sqlite3_bind_int64(q, 1, d);
                    sqlite3_bind_value(q, 2, o.k);
                    sqlite3_bind_int64(q, 3, lo_);
                    sqlite3_bind_int64(q, 4, hi_);
                    collect(q);
                }
            }
            for (Obj& o : objs) sqlite3_value_free(o.k);
        } else {
            std::string sql = "SELECT seq FROM r INDEXED BY r_k WHERE seq>?3 AND seq<=?4";
            if (eq) sql += " AND k=?1";
            else {
                if (hasLo) sql += std::string(" AND k") + lo + "?1";
                if (hasHi) sql += std::string(" AND k") + hi + "?2";
                if (!hasLo) sql += " AND k IS NOT NULL";
            }
            sqlite3_stmt* q = c->sql(sql.c_str());
            if (!q) status = P4_E_INTERNAL;
            else if (eq) {
                for (size_t i = 0; status == P4_OK && i < kp->vals.size(); i++) {
                    bindCell(q, 1, kp->vals[i]);
                    sqlite3_bind_int64(q, 3, lo_);
                    sqlite3_bind_int64(q, 4, hi_);
                    collect(q);
                }
            } else {
                if (hasLo) bindCell(q, 1, kp->vals[0]);
                if (hasHi) bindCell(q, 2, kp->vals[kp->op == P4_OP_BETWEEN ? 1 : 0]);
                sqlite3_bind_int64(q, 3, lo_);
                sqlite3_bind_int64(q, 4, hi_);
                collect(q);
            }
        }
        c->exec("COMMIT");
        e_->rpool.release(c);
        fr.f->users.fetch_sub(1);
        if (status != P4_OK) return status;
        if (giveUp) return P4_OK;  // too many objects x days: the window walk
        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
        total += out.size();
        if (total > kMaxCand) return P4_OK;
    }
    cand_ = std::move(cand);
    kDriven_ = true;
    return P4_OK;
}

// A page of a file's candidates in the scan's order (W: all of them at once,
// so a w group never spans pages).
int32_t Scan::fetchCandidates(int fi, bool wOrder) {
    FileCur& fc = cur_[size_t(fi)];
    FRef& fr = files_[size_t(fi)];
    const std::vector<int64_t>& all = cand_[size_t(fi)];
    if (fc.done) return P4_OK;
    if (fr.f->retired || fc.candPos >= all.size()) {
        fc.done = true;
        return P4_OK;
    }
    const bool desc = !wOrder && (s_.order == P4_ORDER_SEQ_DESC || (s_.bound && s_.order != P4_ORDER_SEQ_ASC));
    const size_t page = wOrder ? all.size() : 256;
    std::vector<int64_t> seqs;
    for (size_t i = 0; i < page && fc.candPos < all.size(); i++, fc.candPos++)
        seqs.push_back(desc ? all[all.size() - 1 - fc.candPos] : all[fc.candPos]);
    if (fc.candPos >= all.size()) fc.done = true;
    std::vector<int64_t> asc = seqs;
    std::sort(asc.begin(), asc.end());
    int rc = 0;
    fr.f->users.fetch_add(1);
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
    if (!c) {
        fr.f->users.fetch_sub(1);
        return statusOfSqlite(rc);
    }
    c->exec("BEGIN");
    std::deque<Row> rows;
    int32_t status = loadRows(c, fi, asc, &rows);
    if (status == P4_OK) status = loadTags(c, fi, rows);
    c->exec("COMMIT");
    e_->rpool.release(c);
    fr.f->users.fetch_sub(1);
    if (status != P4_OK) return status;
    if (wOrder) {
        const bool wAsc = s_.wAsc;
        std::vector<Row> v(std::make_move_iterator(rows.begin()), std::make_move_iterator(rows.end()));
        std::stable_sort(v.begin(), v.end(), [&](const Row& a, const Row& b) {
            if (a.w != b.w) return wAsc ? a.w < b.w : a.w > b.w;
            return a.seq < b.seq;
        });
        for (auto& rw : v)
            if (rw.w >= s_.wLo && rw.w <= s_.wHi) fc.rows.push_back(std::move(rw));
    } else {
        if (desc) std::reverse(rows.begin(), rows.end());
        for (auto& rw : rows) fc.rows.push_back(std::move(rw));
    }
    return P4_OK;
}

bool Scan::laneMatch(const TagInst& ti) {
    if (!s_.lane) return true;
    return laneIds_.count(ti.lane) > 0;
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

bool Scan::rowMatches(Row& r) {
    if (s_.hasCid && std::memcmp(r.key, s_.cidKey, 32) != 0) return false;
    if (s_.hasPeer && peerOf(r) != s_.peer) return false;
    if (s_.eNotNull && !r.hasE) return false;
    if (s_.eNull && r.hasE) return false;
    if (fts_ && !ftsSeqs_.count(r.seq)) return false;
    // matched tag
    r.sel = -1;
    if (s_.lane) {
        for (size_t i = 0; i < r.tags.size(); i++) {
            if (!laneMatch(r.tags[i])) continue;
            if (r.sel < 0) { r.sel = int(i); continue; }
            const TagInst& a = r.tags[i];
            const TagInst& b = r.tags[size_t(r.sel)];
            if (a.at < b.at || (a.at == b.at && a.lane < b.lane)) r.sel = int(i);
        }
        if (r.sel < 0) return false;
    } else if (!r.tags.empty()) {
        r.sel = 0;
        for (size_t i = 1; i < r.tags.size(); i++) {
            const TagInst& a = r.tags[i];
            const TagInst& b = r.tags[size_t(r.sel)];
            if (a.at < b.at || (a.at == b.at && a.lane < b.lane)) r.sel = int(i);
        }
    }
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

// Rows by seq (ascending), in the connection's read transaction.
// A row from columns (cid, e, k, ts, x, length(d), p, f, d) starting at col0.
void Scan::rowFrom(sqlite3_stmt* q, int col0, int fi, bool needData, Row* row) {
    row->fi = fi;
    std::memcpy(row->key, sqlite3_column_blob(q, col0), 32);
    row->hasE = sqlite3_column_type(q, col0 + 1) != SQLITE_NULL;
    row->e = sqlite3_column_int64(q, col0 + 1);
    const int kt = sqlite3_column_type(q, col0 + 2);
    if (kt == SQLITE_INTEGER) {
        row->kType = 1;
        row->kInt = sqlite3_column_int64(q, col0 + 2);
    } else if (kt == SQLITE_TEXT) {
        row->kType = 3;
        row->kText.assign(reinterpret_cast<const char*>(sqlite3_column_text(q, col0 + 2)),
                          size_t(sqlite3_column_bytes(q, col0 + 2)));
    }
    row->ts = sqlite3_column_int64(q, col0 + 3);
    row->w = row->hasE ? row->e : row->ts;
    if (sqlite3_column_type(q, col0 + 4) != SQLITE_NULL)
        row->sig.assign(static_cast<const char*>(sqlite3_column_blob(q, col0 + 4)), size_t(sqlite3_column_bytes(q, col0 + 4)));
    row->len = sqlite3_column_int64(q, col0 + 5);
    if (sqlite3_column_type(q, col0 + 6) != SQLITE_NULL)
        row->peer.assign(reinterpret_cast<const char*>(sqlite3_column_text(q, col0 + 6)), size_t(sqlite3_column_bytes(q, col0 + 6)));
    else
        row->filePeer = true;  // the partition's peer (C-2), read from the FRef when needed
    row->sealed = sqlite3_column_type(q, col0 + 7) != SQLITE_NULL;
    if (row->sealed)
        row->fcols.assign(static_cast<const char*>(sqlite3_column_blob(q, col0 + 7)), size_t(sqlite3_column_bytes(q, col0 + 7)));
    if (needData && sqlite3_column_type(q, col0 + 8) != SQLITE_NULL) {
        row->data.assign(static_cast<const char*>(sqlite3_column_blob(q, col0 + 8)), size_t(sqlite3_column_bytes(q, col0 + 8)));
        row->hasData = true;
    }
    L_->bytesRead += uint64_t(row->len) + 64;
}

bool Scan::needData() const {
    bool need = s_.hydrate || s_.preds.size();
    for (auto& p : s_.preds) need = need || p.field >= P4_F_COL0;
    return need;
}

int32_t Scan::loadRows(Conn* c, int fi, const std::vector<int64_t>& seqs, std::deque<Row>* out) {
    const bool nd = needData();
    sqlite3_stmt* q = c->get(nd ? S_R_ROW : S_R_GET);
    if (!q) return P4_E_INTERNAL;
    for (int64_t sq : seqs) {
        sqlite3_bind_int64(q, 1, sq);
        const int r = sqlite3_step(q);
        if (r == SQLITE_ROW && sqlite3_column_bytes(q, 0) == 32) {
            Row row;
            row.seq = sq;
            rowFrom(q, 0, fi, nd, &row);
            out->push_back(std::move(row));
        } else if (r != SQLITE_ROW && r != SQLITE_DONE) {
            sqlite3_reset(q);
            return statusOfSqlite(r);
        }
        sqlite3_reset(q);
    }
    return P4_OK;
}

int32_t Scan::loadTags(Conn* c, int fi, std::deque<Row>& rows) {
    if (rows.empty()) return P4_OK;
    if (!s_.needTags && !s_.lane) return P4_OK;
    int64_t a = INT64_MAX, b = INT64_MIN;
    for (auto& r : rows) {
        a = std::min(a, r.seq);
        b = std::max(b, r.seq);
    }
    std::unordered_map<int64_t, Row*> bySeq;
    for (auto& r : rows) bySeq[r.seq] = &r;
    const FRef& fr = files_[size_t(fi)];
    sqlite3_stmt* q;
    if (rows.size() * 8 < size_t(b - a + 1)) {
        q = c->get(S_RL_OF);
        for (auto& r : rows) {
            sqlite3_bind_int64(q, 1, r.seq);
            while (sqlite3_step(q) == SQLITE_ROW) {
                TagInst ti;
                ti.sid = uint32_t(sqlite3_column_int64(q, 0));
                ti.lane = uint32_t(sqlite3_column_int64(q, 1));
                ti.at = sqlite3_column_int64(q, 2);
                if (sqlite3_column_type(q, 3) != SQLITE_NULL)
                    ti.url.assign(reinterpret_cast<const char*>(sqlite3_column_text(q, 3)), size_t(sqlite3_column_bytes(q, 3)));
                else {
                    auto it = fr.url0.find(ti.lane);
                    if (it != fr.url0.end()) ti.url = it->second;
                }
                r.tags.push_back(std::move(ti));
            }
            sqlite3_reset(q);
        }
        return P4_OK;
    }
    q = c->sql("SELECT seq, sid, lane, at, u FROM rl INDEXED BY rl_seq WHERE seq>=?1 AND seq<=?2");
    if (!q) q = c->sql("SELECT seq, sid, lane, at, u FROM rl WHERE seq>=?1 AND seq<=?2");
    if (!q) return P4_E_INTERNAL;
    sqlite3_bind_int64(q, 1, a);
    sqlite3_bind_int64(q, 2, b);
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        auto it = bySeq.find(sqlite3_column_int64(q, 0));
        if (it == bySeq.end()) continue;
        TagInst ti;
        ti.sid = uint32_t(sqlite3_column_int64(q, 1));
        ti.lane = uint32_t(sqlite3_column_int64(q, 2));
        ti.at = sqlite3_column_int64(q, 3);
        if (sqlite3_column_type(q, 4) != SQLITE_NULL)
            ti.url.assign(reinterpret_cast<const char*>(sqlite3_column_text(q, 4)), size_t(sqlite3_column_bytes(q, 4)));
        else {
            auto u = fr.url0.find(ti.lane);
            if (u != fr.url0.end()) ti.url = u->second;
        }
        it->second->tags.push_back(std::move(ti));
    }
    sqlite3_reset(q);
    return r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}

// One page of a file in seq order (one read transaction).
int32_t Scan::fetchSeq(int fi) {
    if (kDriven_) return fetchCandidates(fi, false);
    FileCur& fc = cur_[size_t(fi)];
    FRef& fr = files_[size_t(fi)];
    if (fc.done) return P4_OK;
    if (fr.f->retired) {
        fc.done = true;
        return P4_OK;
    }
    const bool desc = s_.order == P4_ORDER_SEQ_DESC || (s_.bound && s_.order != P4_ORDER_SEQ_ASC);
    if (!fc.started) {
        fc.resumeSeq = desc ? hi_ + 1 : lo_;
        fc.started = true;
    }
    int rc = 0;
    fr.f->users.fetch_add(1);
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
    if (!c) {
        fr.f->users.fetch_sub(1);
        e_->bump(kStReadErrors);
        return statusOfSqlite(rc);
    }
    int32_t status = P4_OK;
    c->exec("BEGIN");
    const int page = 256;
    std::vector<int64_t> seqs;
    // Tag-driven when a source filter is selective here.
    const bool byTag = s_.lane && sids_.size() == 1 && fr.laneN * 2 < fr.n;
    sqlite3_stmt* q;
    if (byTag) {
        q = c->sql(desc ? "SELECT seq FROM rl WHERE sid=?1 AND seq<?2 AND seq>?3 ORDER BY seq DESC LIMIT ?4"
                        : "SELECT seq FROM rl WHERE sid=?1 AND seq>?2 AND seq<=?3 ORDER BY seq LIMIT ?4");
        sqlite3_bind_int64(q, 1, *sids_.begin());
        if (desc) {
            sqlite3_bind_int64(q, 2, fc.resumeSeq);
            sqlite3_bind_int64(q, 3, lo_);
        } else {
            sqlite3_bind_int64(q, 2, fc.resumeSeq);
            sqlite3_bind_int64(q, 3, hi_);
        }
        sqlite3_bind_int64(q, 4, page * 2);
    } else if (s_.lane && fr.laneN * 4 < fr.n * 3) {
        // A source filter that leaves out a quarter or more here (and is not
        // selective enough for rl): the page's seqs from r_s, their tags,
        // then the rows of the matching seqs only.
        q = c->sql(desc ? "SELECT seq FROM r INDEXED BY r_s WHERE seq<?1 AND seq>?2 ORDER BY seq DESC LIMIT ?3"
                        : "SELECT seq FROM r INDEXED BY r_s WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3");
        if (!q)
            q = c->sql(desc ? "SELECT seq FROM r WHERE seq<?1 AND seq>?2 ORDER BY seq DESC LIMIT ?3"
                            : "SELECT seq FROM r WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3");
        std::deque<Row> probe;
        if (!q) status = P4_E_INTERNAL;
        else {
            sqlite3_bind_int64(q, 1, fc.resumeSeq);
            sqlite3_bind_int64(q, 2, desc ? lo_ : hi_);
            sqlite3_bind_int64(q, 3, page);
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                Row pr;
                pr.seq = sqlite3_column_int64(q, 0);
                pr.fi = fi;
                fc.resumeSeq = pr.seq;
                probe.push_back(std::move(pr));
            }
            sqlite3_reset(q);
            if (r != SQLITE_DONE) status = statusOfSqlite(r);
        }
        if (status == P4_OK && probe.empty()) fc.done = true;
        if (status == P4_OK) status = loadTags(c, fi, probe);
        std::vector<int64_t> keep;
        std::unordered_map<int64_t, std::vector<TagInst>> tagsOf;
        for (auto& pr : probe) {
            bool any = false;
            for (const TagInst& ti : pr.tags) any = any || laneMatch(ti);
            if (!any) {
                L_->rowsExamined++;  // the kept rows are counted by next()
                continue;
            }
            keep.push_back(pr.seq);
            tagsOf[pr.seq] = std::move(pr.tags);
        }
        if (status == P4_OK && !keep.empty()) {
            std::sort(keep.begin(), keep.end());
            std::deque<Row> rows;
            status = loadRows(c, fi, keep, &rows);
            if (status == P4_OK) {
                for (auto& rw : rows) rw.tags = std::move(tagsOf[rw.seq]);
                if (desc) std::reverse(rows.begin(), rows.end());
                for (auto& rw : rows) fc.rows.push_back(std::move(rw));
            }
        }
        c->exec("COMMIT");
        e_->rpool.release(c);
        fr.f->users.fetch_sub(1);
        if (status != P4_OK) e_->bump(kStReadErrors);
        return status;
    } else {
        // One range read returns the page's rows (no per-seq lookups).
        const bool nd = needData();
        static const char* kSql[2][2] = {
            {"SELECT seq, cid, e, k, ts, x, length(d), p, f, NULL FROM r WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3",
             "SELECT seq, cid, e, k, ts, x, length(d), p, f, d FROM r WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3"},
            {"SELECT seq, cid, e, k, ts, x, length(d), p, f, NULL FROM r WHERE seq<?1 AND seq>?2 ORDER BY seq DESC LIMIT ?3",
             "SELECT seq, cid, e, k, ts, x, length(d), p, f, d FROM r WHERE seq<?1 AND seq>?2 ORDER BY seq DESC LIMIT ?3"}};
        q = c->sql(kSql[desc ? 1 : 0][nd ? 1 : 0]);
        if (!q) status = P4_E_INTERNAL;
        else {
            sqlite3_bind_int64(q, 1, fc.resumeSeq);
            sqlite3_bind_int64(q, 2, desc ? lo_ : hi_);
            sqlite3_bind_int64(q, 3, page);
            std::deque<Row> rows;
            int r;
            int got = 0;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                got++;
                if (sqlite3_column_bytes(q, 1) != 32) continue;
                Row row;
                row.seq = sqlite3_column_int64(q, 0);
                rowFrom(q, 1, fi, nd, &row);
                fc.resumeSeq = row.seq;
                rows.push_back(std::move(row));
            }
            sqlite3_reset(q);
            if (r != SQLITE_DONE) status = statusOfSqlite(r);
            if (status == P4_OK && got == 0) fc.done = true;
            if (status == P4_OK) status = loadTags(c, fi, rows);
            if (status == P4_OK)
                for (auto& rw : rows) fc.rows.push_back(std::move(rw));
        }
        c->exec("COMMIT");
        e_->rpool.release(c);
        fr.f->users.fetch_sub(1);
        if (status != P4_OK) e_->bump(kStReadErrors);
        return status;
    }
    int r;
    int64_t lastSeen = fc.resumeSeq;
    int got = 0;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        const int64_t sq = sqlite3_column_int64(q, 0);
        got++;
        lastSeen = sq;
        if (seqs.empty() || seqs.back() != sq) seqs.push_back(sq);
    }
    sqlite3_reset(q);
    if (r != SQLITE_DONE) status = statusOfSqlite(r);
    if (status == P4_OK) {
        if (got == 0) fc.done = true;
        else fc.resumeSeq = lastSeen;
        std::vector<int64_t> asc = seqs;
        std::sort(asc.begin(), asc.end());
        std::deque<Row> rows;
        status = loadRows(c, fi, asc, &rows);
        if (status == P4_OK) status = loadTags(c, fi, rows);
        if (status == P4_OK) {
            if (desc) std::reverse(rows.begin(), rows.end());
            for (auto& rw : rows) fc.rows.push_back(std::move(rw));
        }
    }
    c->exec("COMMIT");
    e_->rpool.release(c);
    fr.f->users.fetch_sub(1);
    if (status != P4_OK) e_->bump(kStReadErrors);
    return status;
}

int32_t Scan::nextSeq(Row** out) {
    const bool desc = s_.order == P4_ORDER_SEQ_DESC || (s_.bound && s_.order != P4_ORDER_SEQ_ASC);
    auto key = [&](int fi) { return cur_[size_t(fi)].rows.front().seq; };
    auto better = [&](int a, int b) {  // a before b
        const int64_t x = key(a), y = key(b);
        if (x != y) return desc ? x > y : x < y;
        return files_[size_t(a)].pid < files_[size_t(b)].pid;
    };
    for (;;) {
        int32_t rc = check();
        if (rc != P4_OK) return rc;
        // Open files lazily, in seq order, while they can hold the next row.
        while (nextOpen_ < order_.size()) {
            const int fi = order_[nextOpen_];
            int best = -1;
            for (int h : heap_)
                if (best < 0 || better(h, best)) best = h;
            const FRef& fr = files_[size_t(fi)];
            const bool may = best < 0 || (desc ? fr.maxseq >= key(best) : fr.minseq <= key(best));
            if (!may) break;
            nextOpen_++;
            rc = fetchSeq(fi);
            if (rc != P4_OK) return rc;
            while (cur_[size_t(fi)].rows.empty() && !cur_[size_t(fi)].done) {
                rc = fetchSeq(fi);
                if (rc != P4_OK) return rc;
            }
            if (!cur_[size_t(fi)].rows.empty()) heap_.push_back(fi);
        }
        if (heap_.empty()) return 0;
        int best = 0;
        for (size_t i = 1; i < heap_.size(); i++)
            if (better(heap_[i], heap_[size_t(best)])) best = int(i);
        const int fi = heap_[size_t(best)];
        Row row = std::move(cur_[size_t(fi)].rows.front());
        cur_[size_t(fi)].rows.pop_front();
        if (cur_[size_t(fi)].rows.empty()) {
            while (cur_[size_t(fi)].rows.empty() && !cur_[size_t(fi)].done) {
                rc = fetchSeq(fi);
                if (rc != P4_OK) return rc;
            }
            if (cur_[size_t(fi)].rows.empty()) heap_.erase(heap_.begin() + best);
        }
        L_->rowsExamined++;
        // Copies collapse: one row per seq, the lowest pid that matches.
        if (haveLast_ && row.seq == lastSeq_) continue;
        if (!rowMatches(row)) continue;
        lastSeq_ = row.seq;
        haveLast_ = true;
        if (skipped_ < s_.offset) {
            skipped_++;
            continue;
        }
        if (s_.limit && emitted_ >= s_.limit) return 0;
        emitted_++;
        out_ = std::move(row);
        *out = &out_;
        return 1;
    }
}

// One page of a file in w order (descending, or ascending for EPOCH windows).
int32_t Scan::fetchW(int fi) {
    if (kDriven_) return fetchCandidates(fi, true);
    FileCur& fc = cur_[size_t(fi)];
    FRef& fr = files_[size_t(fi)];
    if (fc.done) return P4_OK;
    if (fr.f->retired) {
        fc.done = true;
        return P4_OK;
    }
    const bool asc = s_.wAsc;
    if (!fc.started) {
        fc.resumeW = asc ? (s_.wLo == INT64_MIN ? INT64_MIN : s_.wLo - 1) : (s_.wHi == INT64_MAX ? INT64_MAX : s_.wHi + 1);
        fc.resumeSeq = asc ? INT64_MIN : INT64_MIN;
        fc.started = true;
    }
    int rc = 0;
    fr.f->users.fetch_add(1);
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
    if (!c) {
        fr.f->users.fetch_sub(1);
        return statusOfSqlite(rc);
    }
    c->exec("BEGIN");
    int32_t status = P4_OK;
    const int page = 256;
    std::vector<std::pair<int64_t, int64_t>> ws;  // (w, seq) in scan order
    // Rows of the resume w with a larger seq, then the rows past it.
    sqlite3_stmt* q = c->sql("SELECT seq FROM r INDEXED BY r_w WHERE w=?1 AND seq>?2 ORDER BY seq");
    if (q && fc.resumeSeq != INT64_MIN) {
        sqlite3_bind_int64(q, 1, fc.resumeW);
        sqlite3_bind_int64(q, 2, fc.resumeSeq);
        while (sqlite3_step(q) == SQLITE_ROW) ws.push_back({fc.resumeW, sqlite3_column_int64(q, 0)});
        sqlite3_reset(q);
    }
    q = c->sql(asc ? "SELECT w, seq FROM r INDEXED BY r_w WHERE w>?1 AND w<=?2 ORDER BY w ASC, seq ASC LIMIT ?3"
                   : "SELECT w, seq FROM r INDEXED BY r_w WHERE w<?1 AND w>=?2 ORDER BY w DESC, seq ASC LIMIT ?3");
    if (!q) status = P4_E_INTERNAL;
    else {
        sqlite3_bind_int64(q, 1, fc.resumeW);
        sqlite3_bind_int64(q, 2, asc ? s_.wHi : s_.wLo);
        sqlite3_bind_int64(q, 3, page);
        int r;
        int got = 0;
        while ((r = sqlite3_step(q)) == SQLITE_ROW) {
            ws.push_back({sqlite3_column_int64(q, 0), sqlite3_column_int64(q, 1)});
            got++;
        }
        sqlite3_reset(q);
        if (r != SQLITE_DONE) status = statusOfSqlite(r);
        // Finish the last w group, so a group never spans pages.
        if (status == P4_OK && got == page) {
            const int64_t lw = ws.back().first, ls = ws.back().second;
            sqlite3_stmt* g = c->sql("SELECT seq FROM r INDEXED BY r_w WHERE w=?1 AND seq>?2 ORDER BY seq");
            sqlite3_bind_int64(g, 1, lw);
            sqlite3_bind_int64(g, 2, ls);
            while (sqlite3_step(g) == SQLITE_ROW) ws.push_back({lw, sqlite3_column_int64(g, 0)});
            sqlite3_reset(g);
        }
        if (status == P4_OK && got < page) fc.done = true;
    }
    if (status == P4_OK && !ws.empty()) {
        fc.resumeW = ws.back().first;
        fc.resumeSeq = ws.back().second;
        std::vector<int64_t> seqs;
        for (auto& x : ws) seqs.push_back(x.second);
        std::vector<int64_t> asc2 = seqs;
        std::sort(asc2.begin(), asc2.end());
        std::deque<Row> rows;
        status = loadRows(c, fi, asc2, &rows);
        if (status == P4_OK) status = loadTags(c, fi, rows);
        if (status == P4_OK) {
            std::unordered_map<int64_t, Row*> bySeq;
            for (auto& rw : rows) bySeq[rw.seq] = &rw;
            for (auto& x : ws) {
                auto it = bySeq.find(x.second);
                if (it != bySeq.end()) fc.rows.push_back(std::move(*it->second));
            }
        }
    } else if (status == P4_OK) {
        fc.done = true;
    }
    c->exec("COMMIT");
    e_->rpool.release(c);
    fr.f->users.fetch_sub(1);
    return status;
}

int32_t Scan::nextW(Row** out) {
    const bool asc = s_.wAsc;
    for (;;) {
        int32_t rc = check();
        if (rc != P4_OK) return rc;
        if (groupAt_ < group_.size()) {
            Row& row = group_[groupAt_++];
            if (s_.limit && emitted_ >= s_.limit) return 0;
            emitted_++;
            out_ = std::move(row);
            *out = &out_;
            return 1;
        }
        group_.clear();
        groupAt_ = 0;
        // The next w across files; open files lazily by their w range.
        auto headW = [&](int fi) { return cur_[size_t(fi)].rows.front().w; };
        int64_t top = 0;
        bool have = false;
        for (int h : heap_)
            if (!have || (asc ? headW(h) < top : headW(h) > top)) {
                top = headW(h);
                have = true;
            }
        while (nextOpen_ < order_.size()) {
            const int fi = order_[nextOpen_];
            const FRef& fr = files_[size_t(fi)];
            if (have && (asc ? fr.minw > top : fr.maxw < top)) break;
            nextOpen_++;
            while (cur_[size_t(fi)].rows.empty() && !cur_[size_t(fi)].done) {
                rc = fetchW(fi);
                if (rc != P4_OK) return rc;
            }
            if (!cur_[size_t(fi)].rows.empty()) {
                heap_.push_back(fi);
                const int64_t w = headW(fi);
                if (!have || (asc ? w < top : w > top)) {
                    top = w;
                    have = true;
                }
            }
        }
        if (!have) return 0;
        // Every row of w == top, from every open file.
        std::vector<Row> g;
        for (size_t i = 0; i < heap_.size();) {
            const int fi = heap_[i];
            FileCur& fc = cur_[size_t(fi)];
            while (!fc.rows.empty() && fc.rows.front().w == top) {
                g.push_back(std::move(fc.rows.front()));
                fc.rows.pop_front();
                if (fc.rows.empty() && !fc.done) {
                    rc = fetchW(fi);
                    if (rc != P4_OK) return rc;
                }
            }
            if (fc.rows.empty() && fc.done) heap_.erase(heap_.begin() + long(i));
            else i++;
        }
        std::sort(g.begin(), g.end(), [&](const Row& a, const Row& b) {
            const int c = std::memcmp(a.key, b.key, 32);
            if (c) return c < 0;
            return files_[size_t(a.fi)].pid < files_[size_t(b.fi)].pid;
        });
        // Copies are adjacent (CID order): one row per CID, the lowest pid
        // that matches; a copy of a skipped (offset) CID is skipped with it.
        bool taken = false;
        uint8_t takenKey[32];
        for (size_t i = 0; i < g.size(); i++) {
            L_->rowsExamined++;
            if (taken && std::memcmp(takenKey, g[i].key, 32) == 0) continue;
            if (!rowMatches(g[i])) continue;
            std::memcpy(takenKey, g[i].key, 32);
            taken = true;
            if (skipped_ < s_.offset) {
                skipped_++;
                continue;
            }
            group_.push_back(std::move(g[i]));
        }
    }
}

int32_t Scan::rowAt(int fi, int64_t seq, Row* out, bool emit, bool hydrate) {
    struct Restore {
        Spec2& s;
        bool tags, data;
        ~Restore() {
            s.needTags = tags;
            s.hydrate = data;
        }
    } restore{s_, s_.needTags, s_.hydrate};
    if (emit) {
        s_.needTags = true;
        s_.hydrate = hydrate;
    }
    FRef& fr = files_[size_t(fi)];
    int rc = 0;
    fr.f->users.fetch_add(1);
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
    if (!c) {
        fr.f->users.fetch_sub(1);
        return statusOfSqlite(rc);
    }
    c->exec("BEGIN");
    std::deque<Row> rows;
    int32_t st = loadRows(c, fi, {seq}, &rows);
    if (st == P4_OK) st = loadTags(c, fi, rows);
    c->exec("COMMIT");
    e_->rpool.release(c);
    fr.f->users.fetch_sub(1);
    if (st != P4_OK) return st;
    if (rows.empty()) return 0;
    *out = std::move(rows.front());
    return rowMatches(*out) ? 1 : 0;
}

// The object directory (design §3, G6): obj rows (the flushed copy) and, for
// the objects and files changed since, the files' own ent rows. Per object the
// files are visited in order of the bound their [fw, lw] puts on the answer;
// each file answers with one or two seeks of r_dk per day; a file that cannot
// beat the best found so far is not opened. Records without an object (k
// NULL) are their own entities, read from r_nk.
int32_t Scan::epochByObject(int profile, int64_t at, std::map<std::string, EpochPick>* best, bool* handled) {
    *handled = false;
    if (gEpochScanOnly.load(std::memory_order_relaxed)) return P4_OK;
    if (!sp_->ek || !s_.search.empty() || profile < 2 || profile > 4) return P4_OK;
    const int oc = sp_->tc.firstObjectCol();
    const Spec2::Pred* kp = nullptr;
    bool otherPreds = false;
    for (const auto& p : s_.preds) {
        if (p.field == P4_F_EPOCH || p.field == P4_F_W) continue;  // the w range (wLo, wHi) and rowMatches
        if (!kp && oc >= 0 && oc <= 3 && p.field == P4_F_COL0 + oc &&
            (p.op == P4_OP_EQ || p.op == P4_OP_IN || p.op == P4_OP_BETWEEN || p.op == P4_OP_GE || p.op == P4_OP_GT ||
             p.op == P4_OP_LE || p.op == P4_OP_LT)) {
            kp = &p;
            continue;
        }
        otherPreds = true;
    }
    if (otherPreds) return P4_OK;  // a filter that can hide an object's nearest record: the scan
    const bool needCheck = s_.lane || s_.hasPeer || s_.hasCid || kp || !s_.preds.empty();
    std::map<std::pair<uint32_t, int64_t>, int> fileIx;
    for (size_t i = 0; i < files_.size(); i++) fileIx[{files_[i].pid, files_[i].tb}] = int(i);
    std::set<int> fullDirty;
    std::map<int, std::set<std::string>> keyDirty;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (size_t i = 0; i < files_.size(); i++) {
            File* f = files_[i].f;
            if (!f->indexed) return P4_OK;  // a migration before REBUILD 1: no r_dk
            if (f->objRefresh || f->objRefreshing) fullDirty.insert(int(i));
        }
        for (const auto* set : {&t_->touchedObj, &t_->touchedObjFlushing})
            for (const std::string& k : *set) {
                if (k.size() < 13) continue;
                const uint8_t* b = reinterpret_cast<const uint8_t*>(k.data());
                auto it = fileIx.find({ld32(b), int64_t(ld64(b + 4))});
                if (it != fileIx.end()) keyDirty[it->second].insert(k.substr(12));
            }
    }
    // Directory keys: 'i' + 8 bytes (int) or 't' + text, as the touch keys.
    auto keyOfValue = [](sqlite3_stmt* q, int col) {
        std::string k;
        if (sqlite3_column_type(q, col) == SQLITE_INTEGER) {
            k.push_back('i');
            uint8_t b[8];
            st64(b, uint64_t(sqlite3_column_int64(q, col)));
            k.append(reinterpret_cast<const char*>(b), 8);
        } else {
            k.push_back('t');
            k.append(reinterpret_cast<const char*>(sqlite3_column_text(q, col)), size_t(sqlite3_column_bytes(q, col)));
        }
        return k;
    };
    auto keyPasses = [&](const std::string& k) {
        if (!kp) return true;
        if (k[0] == 'i') return predOn(*kp, true, int64_t(ld64(reinterpret_cast<const uint8_t*>(k.data()) + 1)), nullptr, true);
        const std::string text = k.substr(1);
        return predOn(*kp, true, 0, &text, false);
    };
    auto bindKey = [](sqlite3_stmt* q, int i, const std::string& k) {
        if (k[0] == 'i') sqlite3_bind_int64(q, i, int64_t(ld64(reinterpret_cast<const uint8_t*>(k.data()) + 1)));
        else sqlite3_bind_text(q, i, k.data() + 1, int(k.size() - 1), SQLITE_TRANSIENT);
    };
    struct Ent {
        int fi;
        int64_t fw, lw;
    };
    std::map<std::string, std::vector<Ent>> dir;
    int32_t rc = P4_OK;
    {
        int irc = 0;
        Conn* x = indexReader(L_, t_, &irc);
        if (!x) return statusOfSqlite(irc);
        x->exec("BEGIN");
        sqlite3_stmt* q = x->sql("SELECT k, tb, pid, fw, lw FROM obj WHERE n>0");
        int r = SQLITE_ERROR;
        if (q) {
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                auto it = fileIx.find({uint32_t(sqlite3_column_int64(q, 2)), sqlite3_column_int64(q, 1)});
                if (it == fileIx.end() || fullDirty.count(it->second)) continue;
                std::string k = keyOfValue(q, 0);
                auto kd = keyDirty.find(it->second);
                if (kd != keyDirty.end() && kd->second.count(k)) continue;
                if (!keyPasses(k)) continue;
                dir[k].push_back({it->second, sqlite3_column_int64(q, 3), sqlite3_column_int64(q, 4)});
            }
            sqlite3_reset(q);
        }
        x->exec("COMMIT");
        if (r != SQLITE_DONE) return statusOfSqlite(r);
    }
    // Per file: a reader connection held for the op, its statements checked once.
    struct FileConn {
        Conn* c = nullptr;
        sqlite3_stmt* below = nullptr;
        sqlite3_stmt* above = nullptr;
    };
    std::map<int, FileConn> conns;
    auto releaseAll = [&] {
        for (auto& kv : conns) {
            conns[kv.first].c->exec("COMMIT");
            e_->rpool.release(kv.second.c);
            files_[size_t(kv.first)].f->users.fetch_sub(1);
        }
        conns.clear();
    };
    bool missingIndex = false;
    auto connOf = [&](int fi) -> FileConn* {
        auto it = conns.find(fi);
        if (it != conns.end()) return &it->second;
        FRef& fr = files_[size_t(fi)];
        int orc = 0;
        fr.f->users.fetch_add(1);
        Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &orc, nullptr);
        if (!c) {
            fr.f->users.fetch_sub(1);
            rc = statusOfSqlite(orc);
            return nullptr;
        }
        c->exec("BEGIN");
        FileConn fc;
        fc.c = c;
        fc.below = c->sql(
            "SELECT w, seq FROM r INDEXED BY r_dk WHERE wd=?1 AND k=?2 AND w<=?3 AND w>=?4 AND e IS NOT NULL AND seq>?5 "
            "AND seq<=?6 ORDER BY w DESC, cid ASC LIMIT ?7");
        fc.above = c->sql(
            "SELECT w, seq FROM r INDEXED BY r_dk WHERE wd=?1 AND k=?2 AND w>=?3 AND w<=?4 AND e IS NOT NULL AND seq>?5 "
            "AND seq<=?6 ORDER BY w ASC, cid ASC LIMIT ?7");
        conns[fi] = fc;
        if (!fc.below || !fc.above) missingIndex = true;
        return &conns[fi];
    };
    // ent of the files changed since the last flush
    for (auto& kv : keyDirty)
        if (!fullDirty.count(kv.first)) {
            FileConn* fc = connOf(kv.first);
            if (!fc) break;
            sqlite3_stmt* q = fc->c->sql("SELECT fw, lw FROM ent WHERE k=?1 AND n>0");
            for (const std::string& k : kv.second) {
                if (!q || !keyPasses(k)) continue;
                bindKey(q, 1, k);
                if (sqlite3_step(q) == SQLITE_ROW) dir[k].push_back({kv.first, sqlite3_column_int64(q, 0), sqlite3_column_int64(q, 1)});
                sqlite3_reset(q);
            }
        }
    for (int fi : fullDirty) {
        if (rc != P4_OK) break;
        FileConn* fc = connOf(fi);
        if (!fc) break;
        sqlite3_stmt* q = fc->c->sql("SELECT k, fw, lw FROM ent WHERE n>0");
        while (q && sqlite3_step(q) == SQLITE_ROW) {
            std::string k = keyOfValue(q, 0);
            if (keyPasses(k)) dir[k].push_back({fi, sqlite3_column_int64(q, 1), sqlite3_column_int64(q, 2)});
        }
        if (q) sqlite3_reset(q);
    }
    if (rc != P4_OK || missingIndex) {
        releaseAll();
        return rc;  // missing indexes (an older file): the scan
    }
    const int64_t wLo = s_.wLo, wHi = s_.wHi;
    auto better = [&](int64_t ae, const uint8_t* ak, int64_t be, const uint8_t* bk) {
        if (profile == 4) {
            if (ae != be) return ae < be;
        } else if (profile == 2) {
            const int64_t da = ae > at ? ae - at : at - ae, db = be > at ? be - at : at - be;
            if (da != db) return da < db;
            if ((ae <= at) != (be <= at)) return ae <= at;
            if (ae != be) return ae > be;
        } else {
            if (ae != be) return ae > be;
        }
        return std::memcmp(ak, bk, 32) < 0;
    };
    // One direction of one file for one object: the first record (in w order
    // from the target) that passes the scan's filters.
    auto probe = [&](int fi, const std::string& k, bool below, int64_t fw, int64_t lw, EpochPick* out) -> int32_t {
        FileConn* fc = connOf(fi);
        if (!fc) return rc;
        sqlite3_stmt* q = below ? fc->below : fc->above;
        const int64_t lim = below ? std::min(at, wHi) : std::max(at, wLo);
        int64_t d0 = below ? std::min(lim, lw) : std::max(lim, fw);
        if (below ? d0 < std::max(fw, wLo) : d0 > std::min(lw, wHi)) return 0;
        d0 = d0 >= 0 ? d0 / 86400 : -((-d0 + 86399) / 86400);
        const int64_t dEnd = below ? (fw >= 0 ? fw / 86400 : -((-fw + 86399) / 86400)) : (lw >= 0 ? lw / 86400 : -((-lw + 86399) / 86400));
        for (int64_t d = d0; below ? d >= dEnd : d <= dEnd; d += below ? -1 : 1) {
            sqlite3_bind_int64(q, 1, d);
            bindKey(q, 2, k);
            sqlite3_bind_int64(q, 3, below ? lim : lim);
            sqlite3_bind_int64(q, 4, below ? wLo : wHi);
            sqlite3_bind_int64(q, 5, lo_);
            sqlite3_bind_int64(q, 6, hi_);
            sqlite3_bind_int64(q, 7, needCheck ? -1 : 1);
            std::vector<std::pair<int64_t, int64_t>> cand;
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) cand.push_back({sqlite3_column_int64(q, 0), sqlite3_column_int64(q, 1)});
            sqlite3_reset(q);
            if (r != SQLITE_DONE) return statusOfSqlite(r);
            for (auto& cw : cand) {
                L_->rowsExamined++;
                if (needCheck) {
                    Row row;
                    const int32_t got = rowAt(fi, cw.second, &row);
                    if (got < 0) return got;
                    if (got == 0) continue;
                    out->e = row.e;
                    std::memcpy(out->key, row.key, 32);
                } else {
                    sqlite3_stmt* kq = fc->c->sql("SELECT cid, e FROM r WHERE seq=?1");
                    if (!kq) return P4_E_INTERNAL;
                    sqlite3_bind_int64(kq, 1, cw.second);
                    const int kr = sqlite3_step(kq);
                    if (kr == SQLITE_ROW && sqlite3_column_bytes(kq, 0) == 32) {
                        std::memcpy(out->key, sqlite3_column_blob(kq, 0), 32);
                        out->e = sqlite3_column_int64(kq, 1);
                    }
                    sqlite3_reset(kq);
                    if (kr != SQLITE_ROW) continue;
                }
                out->seq = cw.second;
                out->fi = fi;
                out->pid = files_[size_t(fi)].pid;
                return 1;
            }
            if (rc != P4_OK) return rc;
            if (int32_t cr = check(); cr != P4_OK) return cr;
        }
        return 0;
    };
    for (auto& kv : dir) {
        const std::string& k = kv.first;
        std::vector<Ent>& ents = kv.second;
        auto lbOf = [&](const Ent& en) -> int64_t {  // the best |e - at| (2), or e bound (3, 4)
            if (profile == 2) return (en.fw <= at && at <= en.lw) ? 0 : std::min(std::llabs(en.fw - at), std::llabs(en.lw - at));
            if (profile == 3) return en.fw > at ? INT64_MAX : at - std::min(en.lw, at);
            return en.lw < at ? INT64_MAX : std::max(en.fw, at) - at;
        };
        std::sort(ents.begin(), ents.end(), [&](const Ent& a, const Ent& b) { return lbOf(a) < lbOf(b); });
        bool have = false;
        EpochPick bp;
        for (const Ent& en : ents) {
            const int64_t lb = lbOf(en);
            if (lb == INT64_MAX) break;
            if (have) {
                const int64_t cur = profile == 2 ? std::llabs(bp.e - at) : profile == 3 ? at - bp.e : bp.e - at;
                if (lb > cur) break;
            }
            for (int dir2 = 0; dir2 < 2; dir2++) {
                const bool below = profile == 3 || (profile == 2 && dir2 == 0);
                if (dir2 == 1 && profile != 2) break;
                EpochPick cand;
                const int32_t got = probe(en.fi, k, below, en.fw, en.lw, &cand);
                if (got < 0) {
                    releaseAll();
                    return got;
                }
                if (got == 1 && (!have || better(cand.e, cand.key, bp.e, bp.key))) {
                    bp = cand;
                    have = true;
                }
            }
        }
        if (have) {
            std::string ent;
            if (k[0] == 'i') ent = std::to_string(int64_t(ld64(reinterpret_cast<const uint8_t*>(k.data()) + 1)));
            else ent = k.substr(1);
            (*best)[ent] = bp;
        }
    }
    // Records without an object: each its own entity (its CID), in the files
    // that hold any (File::nk).
    std::vector<int> withNk;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (size_t fi = 0; fi < files_.size(); fi++)
            if (files_[fi].f->nk > 0) withNk.push_back(int(fi));
    }
    for (int fi : withNk) {
        if (rc != P4_OK) break;
        FileConn* fc = connOf(fi);
        if (!fc) break;
        sqlite3_stmt* q = fc->c->sql(
            "SELECT seq, cid, e FROM r INDEXED BY r_nk WHERE k IS NULL AND e IS NOT NULL AND w>=?1 AND w<=?2 AND seq>?3 "
            "AND seq<=?4");
        if (!q) {
            missingIndex = true;
            break;
        }
        sqlite3_bind_int64(q, 1, wLo);
        sqlite3_bind_int64(q, 2, wHi);
        sqlite3_bind_int64(q, 3, lo_);
        sqlite3_bind_int64(q, 4, hi_);
        std::vector<int64_t> seqs;
        int r;
        while ((r = sqlite3_step(q)) == SQLITE_ROW) seqs.push_back(sqlite3_column_int64(q, 0));
        sqlite3_reset(q);
        if (r != SQLITE_DONE) {
            rc = statusOfSqlite(r);
            break;
        }
        for (int64_t sq : seqs) {
            Row row;
            const int32_t got = rowAt(fi, sq, &row);
            if (got < 0) {
                rc = got;
                break;
            }
            if (got == 0 || (profile == 3 && row.e > at) || (profile == 4 && row.e < at)) continue;
            char cid[60];
            cidTextFromKey(row.key, cid);
            const std::string ent(cid, kCidText);
            auto it = best->find(ent);
            if (it == best->end() || better(row.e, row.key, it->second.e, it->second.key)) {
                EpochPick p;
                p.e = row.e;
                p.seq = sq;
                std::memcpy(p.key, row.key, 32);
                p.fi = fi;
                p.pid = files_[size_t(fi)].pid;
                (*best)[ent] = p;
            }
        }
    }
    releaseAll();
    if (rc != P4_OK) return rc;
    if (missingIndex) {
        best->clear();
        return P4_OK;  // the scan answers
    }
    *handled = true;
    return P4_OK;
}

// CID order from the type index: each month's range of c, paged and merged,
// with the pending layer (inserts added, deletes hiding index rows).
int32_t Scan::cidFill(size_t m) {
    CidMonth& cm = months_[m];
    cm.buf.clear();
    cm.pos = 0;
    int32_t rc = P4_OK;
    Conn* c = indexReader(L_, t_, &rc);
    if (!c) return rc;
    sqlite3_stmt* q = c->sql(
        "SELECT cid, pid, seq FROM c WHERE tb=?1 AND (cid>?2 OR (cid=?2 AND pid>?3)) ORDER BY cid, pid LIMIT 1024");
    if (!q) return P4_E_INTERNAL;
    sqlite3_bind_int64(q, 1, cm.tb);
    sqlite3_bind_blob(q, 2, cm.lastKey.data(), 32, SQLITE_STATIC);
    sqlite3_bind_int64(q, 3, cm.lastPid);
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        CidEnt x;
        std::memcpy(x.key.data(), sqlite3_column_blob(q, 0), 32);
        x.pid = uint32_t(sqlite3_column_int64(q, 1));
        x.seq = sqlite3_column_int64(q, 2);
        x.tb = cm.tb;
        cm.buf.push_back(x);
    }
    sqlite3_reset(q);
    if (r != SQLITE_DONE) return statusOfSqlite(r);
    if (cm.buf.size() < 1024) cm.exhausted = true;
    if (!cm.buf.empty()) {
        cm.lastKey = cm.buf.back().key;
        cm.lastPid = cm.buf.back().pid;
    }
    return P4_OK;
}

// The next live index entry in (cid, pid) order, or false at the end.
int32_t Scan::cidNextEntry(CidEnt* out, bool* have) {
    *have = false;
    for (;;) {
        int best = -1;
        for (size_t m = 0; m < months_.size(); m++) {
            CidMonth& cm = months_[m];
            if (cm.pos >= cm.buf.size() && !cm.exhausted) {
                const int32_t rc = cidFill(m);
                if (rc != P4_OK) return rc;
            }
            if (cm.pos >= cm.buf.size()) continue;
            if (best < 0) { best = int(m); continue; }
            const CidEnt& a = cm.buf[cm.pos];
            const CidEnt& b = months_[size_t(best)].buf[months_[size_t(best)].pos];
            const int c = std::memcmp(a.key.data(), b.key.data(), 32);
            if (c < 0 || (c == 0 && a.pid < b.pid)) best = int(m);
        }
        const bool pendLeft = pendAt_ < pendIns_.size();
        if (best < 0 && !pendLeft) return P4_OK;
        CidEnt cand;
        bool fromPend = false;
        if (best >= 0) cand = months_[size_t(best)].buf[months_[size_t(best)].pos];
        if (pendLeft) {
            const CidEnt& p = pendIns_[pendAt_];
            const int c = best < 0 ? -1 : std::memcmp(p.key.data(), cand.key.data(), 32);
            if (best < 0 || c < 0 || (c == 0 && p.pid <= cand.pid)) {
                fromPend = true;
                // the same entry in the index too: the pending one wins
                if (best >= 0 && c == 0 && p.pid == cand.pid && p.tb == cand.tb) months_[size_t(best)].pos++;
                cand = p;
            }
        }
        if (fromPend) pendAt_++;
        else months_[size_t(best)].pos++;
        // hidden by a pending delete?
        if (!fromPend) {
            bool dead = false;
            for (const CidEnt& d : pendDel_)
                if (d.pid == cand.pid && d.tb == cand.tb && d.key == cand.key) dead = true;
            if (dead) continue;
        }
        *out = cand;
        *have = true;
        return P4_OK;
    }
}

int32_t Scan::nextCid(Row** out) {
    if (!cidLoaded_) {
        cidLoaded_ = true;
        std::lock_guard<std::mutex> g(t_->mu);
        for (int64_t tb : t_->tbs) {
            CidMonth cm;
            cm.tb = tb;
            cm.lastKey.fill(0);
            cm.lastPid = -1;
            months_.push_back(cm);
        }
        if (months_.empty()) {
            CidMonth cm;
            cm.tb = 0;
            cm.lastKey.fill(0);
            cm.lastPid = -1;
            months_.push_back(cm);
        }
        for (const PMap* m : {&t_->flushing, &t_->pend})
            for (const CEnt& x : m->raw()) {
                if (x.st != 1 && x.st != 2) continue;
                CidEnt e;
                std::memcpy(e.key.data(), x.key, 32);
                e.pid = x.pid;
                e.seq = x.seq;
                e.tb = x.tb;
                if (x.st == 1) pendIns_.push_back(e);
                else pendDel_.push_back(e);
            }
        std::sort(pendIns_.begin(), pendIns_.end(), [](const CidEnt& a, const CidEnt& b) {
            const int c = std::memcmp(a.key.data(), b.key.data(), 32);
            return c ? c < 0 : a.pid < b.pid;
        });
        for (size_t i = 0; i < files_.size(); i++) fileOf_[(uint64_t(files_[i].pid) << 32) ^ uint64_t(files_[i].tb)] = int(i);
    }
    const bool simple = !s_.lane && s_.preds.empty() && !s_.hasPeer && !s_.eNull && !s_.eNotNull && !fts_ && !s_.hasCid &&
                        !s_.hasProducer;
    for (;;) {
        int32_t rc = check();
        if (rc != P4_OK) return rc;
        CidEnt x;
        bool have;
        rc = cidNextEntry(&x, &have);
        if (rc != P4_OK) return rc;
        if (!have) return 0;
        L_->rowsExamined++;
        if (haveLast_ && std::memcmp(lastKey_, x.key.data(), 32) == 0) continue;  // a copy of a taken CID
        if (x.seq > hi_) continue;  // not yet visible
        auto it = fileOf_.find((uint64_t(x.pid) << 32) ^ uint64_t(x.tb));
        if (it == fileOf_.end()) continue;
        if (simple && skipped_ < s_.offset) {
            std::memcpy(lastKey_, x.key.data(), 32);
            haveLast_ = true;
            skipped_++;
            continue;
        }
        const int fi = it->second;
        std::deque<Row> rows;
        int rc2 = 0;
        FRef& fr = files_[size_t(fi)];
        fr.f->users.fetch_add(1);
        Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc2, nullptr);
        if (!c) {
            fr.f->users.fetch_sub(1);
            return statusOfSqlite(rc2);
        }
        c->exec("BEGIN");
        int32_t st = loadRows(c, fi, {x.seq}, &rows);
        if (st == P4_OK) st = loadTags(c, fi, rows);
        c->exec("COMMIT");
        e_->rpool.release(c);
        fr.f->users.fetch_sub(1);
        if (st != P4_OK) return st;
        if (rows.empty() || std::memcmp(rows.front().key, x.key.data(), 32) != 0) continue;
        Row row = std::move(rows.front());
        if (!rowMatches(row)) continue;  // a later copy of this CID may match
        std::memcpy(lastKey_, x.key.data(), 32);
        haveLast_ = true;
        if (skipped_ < s_.offset) {
            skipped_++;
            continue;
        }
        if (s_.limit && emitted_ >= s_.limit) return 0;
        emitted_++;
        out_ = std::move(row);
        *out = &out_;
        return 1;
    }
}

int32_t Scan::next(Row** out) {
    if (s_.limit && emitted_ >= s_.limit) return 0;
    switch (s_.order) {
        case P4_ORDER_W_DESC: return nextW(out);
        case P4_ORDER_CID: return nextCid(out);
        default: return nextSeq(out);
    }
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

// The w range implied by EPOCH / W predicates (pruning only; every row is checked).
void predRange(Spec2* s) {
    for (auto& p : s->preds) {
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
    void finish(int32_t status, const std::string& err) {
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

void writeRec(Out& o, Scan& sc, const Row& r, const std::string* entityKey, bool tagsNull) {
    auto& enc = o.enc;
    const FRef& fr = sc.file(r.fi);
    enc.beginRow();
    enc.i64(r.seq);
    char cid[60];
    cidTextFromKey(r.key, cid);
    enc.text(cid, kCidText);
    putText(enc, fr.producer);
    putText(enc, sc.peerOf(r));
    enc.i64(r.ts);
    if (r.hasE) enc.i64(r.e); else enc.null();
    if (entityKey) putText(enc, *entityKey);
    else if (r.kType == 1) enc.i64(r.kInt);
    else if (r.kType == 3) putText(enc, r.kText);
    else enc.null();
    if (!r.sig.empty()) enc.blob(r.sig.data(), r.sig.size()); else enc.null();
    if (r.hasData) enc.blob(r.data.data(), r.data.size()); else enc.null();
    enc.i64(r.len);
    const TagInst* ti = (!tagsNull && r.sel >= 0) ? &r.tags[size_t(r.sel)] : nullptr;
    LaneDef* l = ti ? sc.laneDef(ti->lane) : nullptr;
    if (l) {
        putText(enc, l->provider);
        putText(enc, l->source);
        putText(enc, ti->url);
        putText(enc, l->batch);
        putText(enc, l->ckey);
        putText(enc, l->ppeer);
        putText(enc, l->pkey);
        enc.i64(ti->at);
    } else {
        for (int i = 0; i < 8; i++) enc.null();
    }
    enc.endRow();
}

int32_t typeOf(P4Lane* L, const std::string& name, Type** t) {
    *t = L->e->findType(name);
    return *t ? P4_OK : P4_E_NOTYPE;
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
            if (s.order != P4_ORDER_SEQ_ASC && s.order != P4_ORDER_SEQ_DESC) rc = P4_E_ARG;
        } else {
            if (s.order == 0) s.order = P4_ORDER_W_DESC;
            if (s.order != P4_ORDER_W_DESC && s.order != P4_ORDER_CID) rc = P4_E_ARG;
        }
    }
    if (rc != P4_OK) {
        o.finish(rc, rc == P4_E_NOTYPE ? "type not registered" : "bad request");
        return rc;
    }
    Scan sc(L, t, s);
    rc = sc.open();
    Row* r;
    while (rc == P4_OK) {
        const int32_t n = sc.next(&r);
        if (n <= 0) {
            rc = n;
            break;
        }
        writeRec(o, sc, *r, nullptr, false);
        rc = o.rowDone();
    }
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
    Spec2 s;
    s.type = name;
    s.hydrate = hydrate != 0;
    s.order = P4_ORDER_CID;
    s.needTags = false;
    Scan sc(L, t, s);
    rc = sc.open();
    std::vector<int64_t> tbs;
    {
        std::lock_guard<std::mutex> g(t->mu);
        tbs = t->tbs;
    }
    if (tbs.empty()) tbs.push_back(0);
    const uint32_t n = ld32(cids->v);
    std::vector<Holder> hs;
    for (uint32_t i = 0; i < n && rc == P4_OK; i++) {
        const uint8_t* c36 = cids->v + 4 + size_t(i) * kCidBin;
        if (!cidBinValid(c36)) continue;  // not a bafkrei CID: never stored, a miss
        uint8_t key[32];
        cidKeyFromDigest(c36 + 4, key);
        for (int64_t tb : tbs) {
            rc = holdersOf(L, t, tb, key, &hs);
            if (rc != P4_OK || hs.empty()) continue;
            int emitted = 0;
            for (const Holder& h : hs) {
                if (emitted && !every) break;
                // the file of (pid, tb)
                int fi = -1;
                File* f = nullptr;
                std::string producer, peer;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    Part* p = t->partById(h.pid);
                    if (p) {
                        auto it = p->files.find(tb);
                        if (it != p->files.end() && it->second->created && !it->second->retired) f = it->second;
                        producer = p->producer;
                        peer = p->peer;
                    }
                }
                if (!f) continue;
                int orc = 0;
                f->users.fetch_add(1);
                Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &orc, nullptr);
                if (!c) {
                    f->users.fetch_sub(1);
                    rc = statusOfSqlite(orc);
                    break;
                }
                sqlite3_stmt* q = c->get(hydrate ? S_R_ROW : S_R_GET);
                sqlite3_bind_int64(q, 1, h.seq);
                const int sr = sqlite3_step(q);
                if (sr == SQLITE_ROW && sqlite3_column_bytes(q, 0) == 32 && std::memcmp(sqlite3_column_blob(q, 0), key, 32) == 0) {
                    L->rowsExamined++;
                    L->bytesRead += uint64_t(sqlite3_column_int64(q, 5));
                    o.enc.beginRow();
                    o.enc.i64(h.seq);
                    char cid[60];
                    cidTextFromKey(key, cid);
                    o.enc.text(cid, kCidText);
                    putText(o.enc, producer);
                    if (sqlite3_column_type(q, 6) != SQLITE_NULL)
                        o.enc.text(sqlite3_column_text(q, 6), size_t(sqlite3_column_bytes(q, 6)));
                    else
                        putText(o.enc, peer);
                    o.enc.i64(sqlite3_column_int64(q, 3));
                    if (sqlite3_column_type(q, 1) != SQLITE_NULL) o.enc.i64(sqlite3_column_int64(q, 1)); else o.enc.null();
                    const int kt = sqlite3_column_type(q, 2);
                    if (kt == SQLITE_INTEGER) o.enc.i64(sqlite3_column_int64(q, 2));
                    else if (kt == SQLITE_TEXT) o.enc.text(sqlite3_column_text(q, 2), size_t(sqlite3_column_bytes(q, 2)));
                    else o.enc.null();
                    if (sqlite3_column_type(q, 4) != SQLITE_NULL) o.enc.blob(sqlite3_column_blob(q, 4), size_t(sqlite3_column_bytes(q, 4)));
                    else o.enc.null();
                    if (hydrate && sqlite3_column_type(q, 8) != SQLITE_NULL)
                        o.enc.blob(sqlite3_column_blob(q, 8), size_t(sqlite3_column_bytes(q, 8)));
                    else
                        o.enc.null();
                    o.enc.i64(sqlite3_column_int64(q, 5));
                    for (int k = 0; k < 8; k++) o.enc.null();
                    o.enc.endRow();
                    emitted++;
                    sqlite3_reset(q);
                    e->rpool.release(c);
                    f->users.fetch_sub(1);
                    rc = o.rowDone();
                    if (rc != P4_OK) break;
                    continue;
                }
                sqlite3_reset(q);
                e->rpool.release(c);
                f->users.fetch_sub(1);
                if (sr != SQLITE_ROW && sr != SQLITE_DONE) {
                    rc = statusOfSqlite(sr);
                    break;
                }
                (void)fi;
            }
            if (emitted) break;
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
    std::vector<int64_t> tbs;
    std::unordered_map<uint32_t, LaneDef> lanes;
    {
        std::lock_guard<std::mutex> g(t->mu);
        tbs = t->tbs;
        for (auto& l : t->lanes) lanes[l->id] = *l;
    }
    if (tbs.empty()) tbs.push_back(0);
    std::vector<Holder> hs;
    const uint32_t n = ld32(cids->v);
    for (uint32_t i = 0; i < n && rc == P4_OK; i++) {
        const uint8_t* c36 = cids->v + 4 + size_t(i) * kCidBin;
        if (!cidBinValid(c36)) continue;
        uint8_t key[32];
        cidKeyFromDigest(c36 + 4, key);
        struct Merged {
            uint32_t lane;
            int64_t at, urlAt;
            std::string url, producer;
            uint32_t pid;
            int64_t seq;
        };
        std::map<uint32_t, Merged> byLane;
        int64_t seq = 0;
        for (int64_t tb : tbs) {
            rc = holdersOf(L, t, tb, key, &hs);
            if (rc != P4_OK || hs.empty()) continue;
            for (const Holder& h : hs) {
                File* f = nullptr;
                std::string producer;
                std::map<uint32_t, std::string> url0;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    Part* p = t->partById(h.pid);
                    if (p) {
                        auto it = p->files.find(tb);
                        if (it != p->files.end() && it->second->created && !it->second->retired) {
                            f = it->second;
                            for (auto& kv : f->lanes) url0[kv.first] = kv.second.url0;
                        }
                        producer = p->producer;
                    }
                }
                if (!f) continue;
                int orc = 0;
                Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &orc, nullptr);
                if (!c) {
                    rc = statusOfSqlite(orc);
                    break;
                }
                c->exec("BEGIN");
                sqlite3_stmt* chk = c->get(S_R_LEN);
                sqlite3_bind_int64(chk, 1, h.seq);
                const bool live = sqlite3_step(chk) == SQLITE_ROW && sqlite3_column_bytes(chk, 1) == 32 &&
                                  std::memcmp(sqlite3_column_blob(chk, 1), key, 32) == 0;
                sqlite3_reset(chk);
                if (live) {
                    seq = h.seq;
                    sqlite3_stmt* q = c->get(S_RL_OF);
                    sqlite3_bind_int64(q, 1, h.seq);
                    while (sqlite3_step(q) == SQLITE_ROW) {
                        const uint32_t lane = uint32_t(sqlite3_column_int64(q, 1));
                        const int64_t at = sqlite3_column_int64(q, 2);
                        std::string url;
                        if (sqlite3_column_type(q, 3) != SQLITE_NULL)
                            url.assign(reinterpret_cast<const char*>(sqlite3_column_text(q, 3)), size_t(sqlite3_column_bytes(q, 3)));
                        else if (url0.count(lane))
                            url = url0[lane];
                        auto it = byLane.find(lane);
                        if (it == byLane.end()) {
                            byLane[lane] = Merged{lane, at, at, url, producer, h.pid, h.seq};
                        } else {
                            if (at < it->second.at || (at == it->second.at && h.pid < it->second.pid)) {
                                it->second.at = at;
                                it->second.producer = producer;
                                it->second.pid = h.pid;
                            }
                            if (at > it->second.urlAt) {
                                it->second.urlAt = at;
                                it->second.url = url;
                            }
                        }
                        L->rowsExamined++;
                    }
                    sqlite3_reset(q);
                }
                c->exec("COMMIT");
                e->rpool.release(c);
            }
            if (!byLane.empty() || seq) break;
        }
        std::vector<Merged> rows;
        for (auto& kv : byLane) rows.push_back(kv.second);
        std::sort(rows.begin(), rows.end(), [&](const Merged& a, const Merged& b) {
            if (a.at != b.at) return a.at < b.at;
            const LaneDef& x = lanes[a.lane];
            const LaneDef& y = lanes[b.lane];
            const std::string* fx[6] = {&x.provider, &x.source, &x.batch, &x.ckey, &x.ppeer, &x.pkey};
            const std::string* fy[6] = {&y.provider, &y.source, &y.batch, &y.ckey, &y.ppeer, &y.pkey};
            for (int k = 0; k < 6; k++)
                if (*fx[k] != *fy[k]) return *fx[k] < *fy[k];
            return false;
        });
        char cid[60];
        cidTextFromKey(key, cid);
        for (const Merged& m : rows) {
            const LaneDef& l = lanes[m.lane];
            o.enc.beginRow();
            o.enc.text(cid, kCidText);
            o.enc.i64(m.seq);
            putText(o.enc, m.producer);
            putText(o.enc, l.provider);
            putText(o.enc, l.source);
            putText(o.enc, m.url);
            putText(o.enc, l.batch);
            putText(o.enc, l.ckey);
            putText(o.enc, l.ppeer);
            putText(o.enc, l.pkey);
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
    const bool noFilter = !s.lane && s.preds.empty() && !s.hasCid && !s.hasPeer && !s.hasProducer && s.search.empty() &&
                          s.seqAfter == 0 && s.seqThrough == 0 && !cap && s.offset == 0 && s.limit == 0;
    bool exactLane = s.lane && s.preds.empty() && !s.hasCid && !s.hasPeer && !s.hasProducer && s.search.empty() &&
                     s.seqAfter == 0 && s.seqThrough == 0 && !cap && s.offset == 0 && s.limit == 0;
    for (int i = 0; i < 6; i++) exactLane = exactLane && s.lfSet[i];
    if (noFilter || exactLane) {
        std::lock_guard<std::mutex> g(t->mu);
        if (exactLane && t->copies != 0) exactLane = false;
        if (noFilter || exactLane) {
            uint32_t laneId = 0;
            if (exactLane) {
                LaneDef* l = laneFor(t, s.lf, false);
                laneId = l ? l->id : 0;
            }
            if (noFilter) {
                n = t->uniq;
                bytes = t->uniqBytes;
            }
            for (auto& p : t->parts)
                for (auto& kv : p->files) {
                    File* f = kv.second;
                    if (!f->created || f->retired) continue;
                    if (noFilter) {
                        maxSeq = std::max(maxSeq, std::min(f->maxseq, through));
                        maxTs = std::max(maxTs, f->maxts);
                        for (auto& lk : f->lanes) maxAt = std::max(maxAt, lk.second.maxat);
                    } else if (laneId) {
                        auto it = f->lanes.find(laneId);
                        if (it == f->lanes.end()) continue;
                        n += it->second.n;
                        bytes += it->second.bytes;
                        maxSeq = std::max(maxSeq, std::min(it->second.maxseq, through));
                        maxTs = std::max(maxTs, it->second.maxts);
                        maxAt = std::max(maxAt, it->second.maxat);
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
        for (const TagInst& ti : r->tags)
            if (!s.lane || r->sel < 0 || ti.lane == r->tags[size_t(r->sel)].lane || ti.at <= r->tags[size_t(r->sel)].at)
                maxAt = std::max(maxAt, ti.at);
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
    uint64_t offset = s.offset, limit = s.limit;
    auto emit = [&](Scan& sc, Row& r) -> int32_t {
        o.enc.beginRow();
        if (r.kType == 1 && k0) {
            o.enc.i64(r.kInt);
        } else if (!k0 || r.kType == 0) {
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
        uint64_t seen = 0;
        while (rc == P4_OK) {
            const int32_t k = sc.next(&r);
            if (k <= 0) {
                rc = k;
                break;
            }
            seen++;
            emitted++;
            rc = emit(sc, *r);
        }
        // rows the offset skipped in phase 1
        if (rc == P4_OK && (!limit || emitted < limit)) {
            int64_t nnull = 0;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& p : t->parts)
                    for (auto& kv : p->files) nnull += kv.second->nnull;
            }
            if (nnull > 0) {
                // the phase-1 total decides the phase-2 offset
                Spec2 cnt = s;
                cnt.order = P4_ORDER_W_DESC;
                cnt.eNotNull = true;
                cnt.offset = 0;
                cnt.limit = 0;
                cnt.needTags = s.lane;
                Scan cs(L, t, cnt);
                rc = cs.open();
                uint64_t total = 0;
                Row* rr;
                while (rc == P4_OK && cs.next(&rr) == 1) total++;
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
        (void)seen;
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
    std::shared_ptr<const Spec> sp = t->spec();
    s.eNotNull = true;
    if (profile == 1 || profile == 5) {
        Spec2 w = s;
        w.order = P4_ORDER_W_DESC;
        w.wAsc = true;
        if (countOnly || profile == 5) {
            w.limit = 0;
            w.offset = 0;
            w.hydrate = false;
        }
        Scan sc(L, t, w);
        rc = sc.open();
        Row* r;
        int64_t count = 0;
        std::map<std::string, std::array<int64_t, 3>> days;
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
                writeRec(o, sc, *r, nullptr, false);
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
    struct Pick {
        int64_t e, seq;
        uint8_t key[32];
        int fi;
        uint32_t pid;
    };
    std::map<std::string, Pick> best;
    auto better = [&](int64_t ae, const uint8_t* ak, int64_t be, const uint8_t* bk) {
        if (profile == 4) {
            if (ae != be) return ae < be;
        } else if (profile == 2) {
            const int64_t da = ae > at ? ae - at : at - ae, db = be > at ? be - at : at - be;
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
    // The object directory first (G6); the scan when it cannot answer.
    Scan sc(L, t, w);
    rc = sc.open();
    bool handled = false;
    if (rc == P4_OK) {
        std::map<std::string, Scan::EpochPick> viaDir;
        rc = sc.epochByObject(profile, at, &viaDir, &handled);
        if (rc == P4_OK && handled)
            for (auto& kv : viaDir) {
                Pick p{kv.second.e, kv.second.seq, {}, kv.second.fi, kv.second.pid};
                std::memcpy(p.key, kv.second.key, 32);
                best[kv.first] = p;
            }
    }
    Row* r;
    while (rc == P4_OK && !handled) {
        const int32_t k = sc.next(&r);
        if (k <= 0) {
            rc = k;
            break;
        }
        if (profile == 3 && r->e > at) continue;
        if (profile == 4 && r->e < at) continue;
        std::string ent;
        if (r->kType == 1) ent = std::to_string(r->kInt);
        else if (r->kType == 3) ent = r->kText;
        if (ent.empty()) {
            char cid[60];
            cidTextFromKey(r->key, cid);
            ent.assign(cid, kCidText);
        }
        auto it = best.find(ent);
        if (it == best.end() || better(r->e, r->key, it->second.e, it->second.key)) {
            Pick p{r->e, r->seq, {}, r->fi, sc.file(r->fi).pid};
            std::memcpy(p.key, r->key, 32);
            best[ent] = p;
        }
    }
    if (rc == P4_OK) {
        std::vector<std::pair<std::string, Pick>> picks;
        for (auto& kv : best) {
            if (maxDelta > 0) {
                const int64_t d = kv.second.e > at ? kv.second.e - at : at - kv.second.e;
                if (d > maxDelta) continue;
            }
            picks.push_back(kv);
        }
        if (countOnly) {
            o.enc.beginRow();
            o.enc.i64(int64_t(picks.size()));
            o.enc.endRow();
            rc = o.rowDone();
        } else {
            uint64_t emitted = 0, skipped = 0;
            for (auto& pk : picks) {
                if (skipped < s.offset) {
                    skipped++;
                    continue;
                }
                if (s.limit && emitted >= s.limit) break;
                // the record, with its matched tag, from the file it was picked in
                Row rr;
                const int32_t got = sc.rowAt(pk.second.fi, pk.second.seq, &rr, true, s.hydrate);
                if (got < 0) {
                    rc = got;
                    break;
                }
                if (got == 1) {
                    writeRec(o, sc, rr, &pk.first, false);
                    rc = o.rowDone();
                    emitted++;
                }
                if (rc != P4_OK) break;
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
            std::lock_guard<std::mutex> g(t->mu);
            int64_t rows = 0, copyBytes = 0, mine = INT64_MAX, maxe = INT64_MIN, mints = INT64_MAX, maxts = 0, maxseq = 0;
            for (auto& p : t->parts)
                for (auto& kv : p->files) {
                    File* f = kv.second;
                    if (!f->created || f->retired) continue;
                    rows += f->n;
                    copyBytes += f->bytes;
                    if (f->n) {
                        mine = std::min(mine, f->mine);
                        maxe = std::max(maxe, f->maxe);
                        mints = std::min(mints, f->mints);
                        maxts = std::max(maxts, f->maxts);
                        maxseq = std::max(maxseq, f->maxseq);
                    }
                }
            const int64_t through = t->vis.load();
            o.enc.beginRow();
            putText(o.enc, t->name);
            o.enc.i64(t->uniq);
            o.enc.i64(rows);
            o.enc.i64(t->uniqBytes);
            o.enc.i64(copyBytes);
            nullOr(mine, INT64_MAX);
            nullOr(maxe, INT64_MIN);
            o.enc.i64(mints == INT64_MAX ? 0 : mints);
            o.enc.i64(maxts);
            o.enc.i64(std::min(maxseq, through));
            o.enc.i64(through);
            o.enc.endRow();
            rc = o.rowDone();
        } else if (kind == 2 || kind == 3) {
            struct LaneAgg {
                LaneCount c;
                bool any = false;
            };
            std::vector<std::vector<std::string>> textRows;
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& p : t->parts) {
                if (kind == 2) {
                    int64_t n = 0, bytes = 0, mints = INT64_MAX, maxts = 0, maxseq = 0, files = 0;
                    for (auto& kv : p->files) {
                        File* f = kv.second;
                        if (!f->created || f->retired) continue;
                        files++;
                        n += f->n;
                        bytes += f->bytes;
                        if (f->n) {
                            mints = std::min(mints, f->mints);
                            maxts = std::max(maxts, f->maxts);
                            maxseq = std::max(maxseq, f->maxseq);
                        }
                    }
                    o.enc.beginRow();
                    putText(o.enc, t->name);
                    putText(o.enc, p->producer);
                    putText(o.enc, p->peer);
                    o.enc.i64(n);
                    o.enc.i64(bytes);
                    o.enc.i64(mints == INT64_MAX ? 0 : mints);
                    o.enc.i64(maxts);
                    o.enc.i64(maxseq);
                    o.enc.i64(files);
                    o.enc.endRow();
                    rc = o.rowDone();
                    if (rc != P4_OK) break;
                    continue;
                }
                std::map<uint32_t, LaneAgg> agg;
                for (auto& kv : p->files) {
                    File* f = kv.second;
                    if (!f->created || f->retired) continue;
                    for (auto& lk : f->lanes) {
                        if (lk.second.n <= 0) continue;
                        LaneAgg& a = agg[lk.first];
                        const LaneCount& c = lk.second;
                        if (!a.any) {
                            a.c = c;
                            a.any = true;
                            continue;
                        }
                        a.c.n += c.n;
                        a.c.bytes += c.bytes;
                        a.c.maxseq = std::max(a.c.maxseq, c.maxseq);
                        a.c.minw = std::min(a.c.minw, c.minw);
                        a.c.maxw = std::max(a.c.maxw, c.maxw);
                        if (c.created && (!a.c.created || c.created < a.c.created)) a.c.created = c.created;
                        if (c.updated > a.c.updated) {
                            a.c.updated = c.updated;
                            a.c.url = c.url;
                        }
                    }
                }
                for (auto& kv : agg) {
                    LaneDef* l = t->laneById(kv.first);
                    if (!l) continue;
                    const LaneCount& c = kv.second.c;
                    o.enc.beginRow();
                    putText(o.enc, t->name);
                    putText(o.enc, p->producer);
                    putText(o.enc, l->provider);
                    putText(o.enc, l->source);
                    putText(o.enc, l->batch);
                    putText(o.enc, l->ckey);
                    putText(o.enc, l->ppeer);
                    putText(o.enc, l->pkey);
                    putText(o.enc, c.url);
                    o.enc.i64(c.n);
                    o.enc.i64(c.bytes);
                    o.enc.i64(c.maxseq);
                    o.enc.i64(c.created);
                    o.enc.i64(c.updated);
                    nullOr(c.minw, INT64_MAX);
                    nullOr(c.maxw, INT64_MIN);
                    o.enc.endRow();
                    rc = o.rowDone();
                    if (rc != P4_OK) break;
                }
            }
        } else if (kind == 4) {
            std::vector<std::string> paths;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& p : t->parts)
                    for (auto& kv : p->files)
                        if (kv.second->created && !kv.second->retired) paths.push_back(kv.second->path);
            }
            int64_t db = 0, wal = 0, jn = 0, free = 0;
            for (auto& path : paths) {
                db += std::max<int64_t>(0, ioSize(path));
                wal += std::max<int64_t>(0, ioSize(path + "-wal"));
                jn += std::max<int64_t>(0, ioSize(path + "-journal"));
                int orc = 0;
                Conn* c = e->rpool.acquire(path, OpenKind::Reader, &orc, nullptr);
                if (c) {
                    sqlite3_stmt* q = c->sql("SELECT freelist_count * page_size FROM pragma_freelist_count, pragma_page_size");
                    if (q && sqlite3_step(q) == SQLITE_ROW) free += sqlite3_column_int64(q, 0);
                    if (q) sqlite3_reset(q);
                    e->rpool.release(c);
                }
            }
            int64_t idx = 0, fts = 0;
            for (const std::string* p : {&t->pIdx, &t->pJnl})
                for (const char* sfx : {"", "-wal"}) idx += std::max<int64_t>(0, ioSize(*p + sfx));
            for (const char* sfx : {"", "-wal"}) fts += std::max<int64_t>(0, ioSize(t->pFts + sfx));
            o.enc.beginRow();
            putText(o.enc, t->name);
            o.enc.i64(int64_t(paths.size()));
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
                std::lock_guard<std::mutex> g(t->ftsMu);
                state = !sp->fullText ? "off" : t->ftsState == 2 ? "ready" : "building";
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

std::atomic<bool> gEpochScanOnly{false};

int32_t runRead(P4Lane* L, uint32_t op) {
    std::vector<Tlv> v;
    if (!tlvParse(L->req, L->reqLen, &v)) {
        respondEmpty(L, {}, P4_E_ARG, "malformed request");
        return P4_E_ARG;
    }
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
    std::string provider, source, url, batch, ckey, ppeer, pkey;
};

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
    row->keyType = uint8_t(r->kType);
    row->keyInt = r->kInt;
    row->keyText = reinterpret_cast<const uint8_t*>(r->kText.data());
    row->keyTextLen = uint32_t(r->kText.size());
    cidTextFromKey(r->key, c->cid);
    row->cid = c->cid;
    const auto& fr = c->scan->file(r->fi);
    row->producer = fr.producer.c_str();
    row->peer = c->scan->peerOf(*r).c_str();
    row->sig = r->sig.empty() ? nullptr : reinterpret_cast<const uint8_t*>(r->sig.data());
    row->sigLen = uint32_t(r->sig.size());
    row->data = r->hasData ? reinterpret_cast<const uint8_t*>(r->data.data()) : nullptr;
    row->dataLen = r->hasData ? uint32_t(r->data.size()) : 0;
    row->len = r->len;
    row->tag = nullptr;
    if (r->sel >= 0) {
        const auto& ti = r->tags[size_t(r->sel)];
        LaneDef* l = c->scan->laneDef(ti.lane);
        if (l) {
            c->provider = l->provider;
            c->source = l->source;
            c->url = ti.url;
            c->batch = l->batch;
            c->ckey = l->ckey;
            c->ppeer = l->ppeer;
            c->pkey = l->pkey;
            c->tag = P4Tag{c->provider.c_str(), c->source.c_str(), c->url.c_str(), c->batch.c_str(), c->ckey.c_str(),
                           c->ppeer.c_str(), c->pkey.c_str(), ti.at};
            row->tag = &c->tag;
        }
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
    if (!lane || !type || !out || !n) return P4_E_ARG;
    Type* t = lane->e->findType(type);
    if (!t) return P4_E_NOTYPE;
    std::set<std::string> names;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& p : t->parts)
            for (auto& kv : p->files) {
                if (!kv.second->created || kv.second->retired) continue;
                for (auto& lk : kv.second->lanes) {
                    if (lk.second.n <= 0) continue;
                    LaneDef* l = t->laneById(lk.first);
                    if (l) names.insert(l->source);
                }
            }
    }
    lane->srcStore.assign(names.begin(), names.end());
    lane->srcPtrs.clear();
    for (auto& s : lane->srcStore) lane->srcPtrs.push_back(s.c_str());
    *out = lane->srcPtrs.data();
    *n = uint32_t(lane->srcPtrs.size());
    return P4_OK;
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
