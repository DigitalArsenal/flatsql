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
//   CID             the type index's c rows (C-34: the one CID index) in
//                   (CID, pid) order, the unflushed entries merged over
//                   them; a page's rows read by seq, one transaction per
//                   file; copies collapse by CID.
// The A18 bound (C-31) is the newest N of lane.source when set (rl_sid walked
// newest first), else of the type (r_s). EPOCH points take one r_ke seek per
// object per partition.
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
    Part* f = nullptr;
    uint32_t pid = 0;
    std::string path, producer, peer;
    int64_t n = 0, minseq = 0, maxseq = 0, minw = 0, maxw = 0, nnull = 0;
    int64_t laneN = 0;  // rows of the filter's lanes
    int64_t laneMin = INT64_MAX, laneMax = 0;  // the filter's lanes' seq bounds here
    bool indexed = true;  // secondary indexes present (false before a migration's REBUILD 1)
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
    // A lane's identity, looked up when a row needs it (LaneDefs are never
    // removed or moved, so the pointer stays valid).
    LaneDef* laneDef(uint32_t id) {
        auto it = lanes_.find(id);
        if (it != lanes_.end()) return it->second;
        LaneDef* l;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            l = t_->laneById(id);
        }
        lanes_.emplace(id, l);
        return l;
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
    int32_t collectEpochCandidates();
    int32_t collectSourceCandidates();
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
    std::unordered_map<uint32_t, LaneDef*> lanes_;
    std::unordered_set<uint32_t> laneIds_;  // lanes matching the filter
    std::unordered_set<uint32_t> sids_;     // their sources
    std::string srcSql_[2];                 // one source: its seqs from rl_sid (asc, desc); empty otherwise
    int64_t vis_ = 0, lo_ = 0, hi_ = 0;     // seq range (lo exclusive, hi inclusive)
    std::vector<int> order_;                // files in opening order
    size_t nextOpen_ = 0;
    // SEQ merge
    std::vector<int> heap_;
    // W merge
    std::vector<Row> group_;
    size_t groupAt_ = 0;
    // CID order: (key, pid, seq) entries of the type index and the pending
    // layer, merged; a page of them resolved to rows.
    struct CidEnt {
        uint8_t key[32];
        uint32_t pid;
        int64_t seq;
        uint8_t st;  // 1 live, 2 deleted (pending only)
    };
    int32_t cidStart();
    int32_t cidNextEnt(CidEnt* out, bool* have);
    int32_t cidPage();
    bool cidStarted_ = false, cidIdxDone_ = false, cidPeeked_ = false;
    std::vector<CidEnt> cidPend_;  // unflushed entries of the scan's files, (key, pid) order
    size_t cidPendAt_ = 0;
    std::deque<CidEnt> cidIdx_;   // a page of c rows
    uint8_t cidIdxKey_[32] = {};
    uint32_t cidIdxPid_ = 0;
    CidEnt cidPeek_{};
    std::unordered_map<uint32_t, int> fiOfPid_;
    std::deque<Row> cidRows_;
    bool cidDone_ = false;
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
    std::unordered_map<uint32_t, size_t> sidLanes;  // live lanes per source
    {
        std::lock_guard<std::mutex> g(t_->mu);
        if (s_.lane) {
            // The filter's lanes among the live ones (no copy of the registry).
            std::unordered_set<uint32_t> seen;
            for (auto& p : t_->parts)
                for (auto& lk : p->lanes) {
                    if (!seen.insert(lk.first).second) continue;
                    LaneDef* l = t_->laneById(lk.first);
                    if (!l) continue;
                    sidLanes[l->sid]++;
                    const std::string* f[6] = {&l->provider, &l->source, &l->batch, &l->ckey, &l->ppeer, &l->pkey};
                    bool ok = true;
                    for (int i = 0; i < 6 && ok; i++) ok = !s_.lfSet[i] || *f[i] == s_.lf[i];
                    if (ok) {
                        laneIds_.insert(l->id);
                        sids_.insert(l->sid);
                    }
                }
        }
        for (auto& p : t_->parts) {
            if (s_.hasProducer && p->producer != s_.producer) continue;
            Part* f = &*p;
            if (!f->created || f->n <= 0 || f->quarantined) continue;
            if (s_.eNull && f->nnull == 0) continue;               // INDEX_PAGE phase 2: no record without an epoch
            if (s_.eNotNull && f->nnull >= f->n && sp_->hasEpochRule) continue;  // no record with one
            if (s_.wLo != INT64_MIN || s_.wHi != INT64_MAX) {
                if (f->maxw < s_.wLo || f->minw > s_.wHi) continue;
            }
            FRef r;
            r.f = f;
            r.pid = p->pid;
            r.path = f->path;
            r.producer = p->producer;
            r.peer = p->peer;
            r.n = f->n;
            r.minseq = f->minseq;
            r.maxseq = f->maxseq;
            r.minw = f->minw;
            r.maxw = f->maxw;
            r.nnull = f->nnull;
            r.indexed = f->indexed;
            for (auto& lk : f->lanes) {
                r.url0[lk.first] = lk.second.url0;
                if (!laneIds_.count(lk.first)) continue;
                r.laneN += lk.second.n;
                r.laneMin = std::min(r.laneMin, lk.second.minseq);
                r.laneMax = std::max(r.laneMax, lk.second.maxseq);
            }
            if (s_.lane) {
                if (r.laneN == 0) continue;  // no instance of the filter's lanes here
                // The filter's rows lie inside its lanes' seq bounds.
                r.minseq = std::max(r.minseq, r.laneMin);
                r.maxseq = std::min(r.maxseq, r.laneMax);
            }
            if (s_.order != P4_ORDER_CID && !s_.bound && (r.maxseq <= lo_ || r.minseq > hi_)) continue;
            files_.push_back(std::move(r));
        }
    }
    if (s_.lane && laneIds_.empty()) files_.clear();
    if (s_.lane && sids_.size() == 1) {
        // One source: its seqs from rl_sid, the filter's lanes checked inside
        // the index when it names fewer than the source has.
        const uint32_t sid = *sids_.begin();
        std::string in;
        if (laneIds_.size() < sidLanes[sid] && laneIds_.size() <= 64) {
            for (uint32_t id : laneIds_) in += (in.empty() ? " AND lane IN (" : ",") + std::to_string(id);
            in += ")";
        }
        srcSql_[0] = "SELECT seq FROM rl INDEXED BY rl_sid WHERE sid=?1 AND seq>?2 AND seq<=?3" + in + " ORDER BY seq LIMIT ?4";
        srcSql_[1] = "SELECT seq FROM rl INDEXED BY rl_sid WHERE sid=?1 AND seq<?2 AND seq>?3" + in + " ORDER BY seq DESC LIMIT ?4";
    }
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
    // A18 (C-31): the bound is the newest N records of lane.source when it
    // is set (each partition's rl_sid walked newest first, merged), else of
    // the type (r_s); the cut is the N-th newest seq, and every other filter
    // applies above it. Only partitions holding the source take part, a
    // partition is opened only once its newest seq could still be above the
    // cut (its in-memory bound), and its pages start small.
    if (s_.bound) {
        const bool bySource = s_.lfSet[1];
        struct HC {
            std::string path;
            uint32_t sid = 0;
            int64_t at = 0;  // next seq <= at (before the first page: the partition's bound)
            std::vector<int64_t> buf;
            size_t pos = 0;
            size_t page = 64;
            bool opened = false, done = false;
        };
        std::vector<HC> hc;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            std::vector<uint32_t> bsids;
            if (bySource)
                for (auto& sd : t_->srcs)
                    if (sd->source == s_.lf[1] && (!s_.lfSet[0] || sd->provider == s_.lf[0])) bsids.push_back(sd->id);
            for (auto& p : t_->parts) {
                if (!p->created || p->n <= 0 || p->quarantined) continue;
                if (!bySource) {
                    HC h;
                    h.path = p->path;
                    h.at = std::min(p->maxseq, vis_);
                    hc.push_back(std::move(h));
                    continue;
                }
                for (uint32_t sid : bsids) {
                    int64_t mx = 0;
                    for (auto& lk : p->lanes) {
                        LaneDef* l = t_->laneById(lk.first);
                        if (l && l->sid == sid) mx = std::max(mx, lk.second.maxseq);
                    }
                    if (!mx) continue;  // no row of the source here
                    HC h;
                    h.path = p->path;
                    h.sid = sid;
                    h.at = std::min(mx, vis_);
                    hc.push_back(std::move(h));
                }
            }
        }
        auto refill = [&](HC& h) -> int32_t {
            h.buf.clear();
            h.pos = 0;
            h.opened = true;
            int rc = 0;
            Conn* c = e_->rpool.acquire(h.path, OpenKind::Reader, &rc, nullptr);
            if (!c) return statusOfSqlite(rc);
            sqlite3_stmt* q;
            if (bySource) {
                q = c->sql("SELECT seq FROM rl INDEXED BY rl_sid WHERE sid=?2 AND seq<=?1 ORDER BY seq DESC LIMIT ?3");
                if (!q) q = c->sql("SELECT seq FROM rl WHERE sid=?2 AND seq<=?1 ORDER BY seq DESC LIMIT ?3");  // before REBUILD 1
            } else {
                q = c->sql("SELECT seq FROM r INDEXED BY r_s WHERE seq<=?1 ORDER BY seq DESC LIMIT ?3");
                if (!q) q = c->sql("SELECT seq FROM r WHERE seq<=?1 ORDER BY seq DESC LIMIT ?3");  // before REBUILD 1
            }
            int r = SQLITE_DONE;
            if (q) {
                sqlite3_bind_int64(q, 1, h.at);
                if (bySource) sqlite3_bind_int64(q, 2, h.sid);
                sqlite3_bind_int64(q, 3, int64_t(h.page));
                while ((r = sqlite3_step(q)) == SQLITE_ROW) h.buf.push_back(sqlite3_column_int64(q, 0));
                sqlite3_reset(q);
            }
            e_->rpool.release(c);
            if (r != SQLITE_DONE) return statusOfSqlite(r);
            if (h.buf.size() < h.page) h.done = true;
            if (!h.buf.empty()) h.at = h.buf.back() - 1;
            h.page = std::min<size_t>(h.page * 2, 4096);
            return P4_OK;
        };
        // Its next seq (unopened: its bound); 0 when it has none left.
        auto keyOf = [&](const HC& h) -> int64_t {
            if (!h.opened) return h.at;
            return h.pos < h.buf.size() ? h.buf[h.pos] : 0;
        };
        std::priority_queue<std::pair<int64_t, size_t>> pq;
        for (size_t i = 0; i < hc.size(); i++)
            if (keyOf(hc[i]) > 0) pq.push({keyOf(hc[i]), i});
        uint64_t seen = 0;
        int64_t cut = 0, last = INT64_MAX;
        while (seen < s_.bound && !pq.empty()) {
            const size_t i = pq.top().second;
            pq.pop();
            HC& h = hc[i];
            if (!h.opened) {
                const int32_t rc = refill(h);
                if (rc != P4_OK) return rc;
                if (keyOf(h) > 0) pq.push({keyOf(h), i});
                continue;
            }
            const int64_t sq = h.buf[h.pos++];
            if (h.pos >= h.buf.size() && !h.done) {
                const int32_t rc = refill(h);
                if (rc != P4_OK) return rc;
            }
            if (keyOf(h) > 0) pq.push({keyOf(h), i});
            if (sq == last) continue;  // a copy, or a second tag of the source
            last = sq;
            seen++;
            cut = sq;
        }
        if (bySource && hc.empty()) files_.clear();
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

// Candidates instead of a walk: an exact CID (tag 8) is one type-index probe
// (its holders' seqs); an equality, IN or range predicate on the object rule's
// first column (the object key k, when that column is present) reads r_ke(k,
// e). The candidates are rechecked by rowMatches. A COL0 equality inside OMM's
// 400,000 bound reads its object's rows, not the window (R17/R19); an
// exact-CID HEAD is one probe, not a type scan.
int32_t Scan::collectCandidates() {
    if (s_.order == P4_ORDER_CID || files_.empty()) return P4_OK;
    if (s_.hasCid) {
        std::vector<Holder> hs;
        const int32_t rc = holdersOf(L_, t_, s_.cidKey, &hs);
        if (rc != P4_OK) return rc;
        cand_.assign(files_.size(), {});
        for (const Holder& h : hs)
            for (size_t fi = 0; fi < files_.size(); fi++)
                if (files_[fi].pid == h.pid && h.seq > lo_ && h.seq <= hi_) cand_[fi].push_back(h.seq);
        kDriven_ = true;
        return P4_OK;
    }
    auto bindCell = [](sqlite3_stmt* q, int i, const ps::rb1::Cell& c) {
        if (c.type == ps::rb1::kInt) sqlite3_bind_int64(q, i, c.i);
        else if (c.type == ps::rb1::kReal) sqlite3_bind_double(q, i, c.d);
        else sqlite3_bind_text(q, i, c.s.data(), int(c.s.size()), SQLITE_TRANSIENT);
    };
    const Spec2::Pred* kp = nullptr;
    const int oc = sp_->hasObject ? sp_->tc.firstObjectCol() : -1;
    if (oc >= 0 && oc <= 3)
        for (const auto& p : s_.preds)
            if (p.field == P4_F_COL0 + oc && !p.vals.empty() &&
                (p.op == P4_OP_EQ || p.op == P4_OP_IN || p.op == P4_OP_BETWEEN || p.op == P4_OP_GE || p.op == P4_OP_GT ||
                 p.op == P4_OP_LE || p.op == P4_OP_LT || (p.op == P4_OP_LIKE && p.vals[0].type == ps::rb1::kText))) {
                kp = &p;
                break;
            }
    if (!kp) {
        const int32_t rc = collectEpochCandidates();
        if (rc != P4_OK || kDriven_) return rc;
        return collectSourceCandidates();
    }
    if (kp)
        for (const auto& v : kp->vals)
            if (v.type != ps::rb1::kInt && v.type != ps::rb1::kText && v.type != ps::rb1::kReal) return P4_OK;
    const bool eq = kp && (kp->op == P4_OP_EQ || kp->op == P4_OP_IN);
    const bool like = kp->op == P4_OP_LIKE;  // a superset (k may come from a later object column): rows are rechecked
    const bool hasLo = kp && (kp->op == P4_OP_BETWEEN || kp->op == P4_OP_GE || kp->op == P4_OP_GT);
    const bool hasHi = kp && (kp->op == P4_OP_BETWEEN || kp->op == P4_OP_LE || kp->op == P4_OP_LT);
    std::string sql = "SELECT seq FROM r INDEXED BY r_ke WHERE seq>?3 AND seq<=?4";
    if (eq) sql += " AND k=?1";
    else if (like) sql += " AND k LIKE ?1";
    else {
        if (hasLo) sql += std::string(" AND k") + (kp->op == P4_OP_GT ? ">" : ">=") + "?1";
        if (hasHi) sql += std::string(" AND k") + (kp->op == P4_OP_LT ? "<" : "<=") + "?2";
        if (!hasLo) sql += " AND k IS NOT NULL";
    }
    const size_t kMaxCand = 200000;
    size_t total = 0;
    std::vector<std::vector<int64_t>> cand(files_.size());
    for (size_t fi = 0; fi < files_.size(); fi++) {
        FRef& fr = files_[fi];
        bool indexed;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            indexed = fr.f->indexed;
        }
        if (!indexed) return P4_OK;  // a migration before REBUILD 1: the walk
        int rc = 0;
        Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
        if (!c) return statusOfSqlite(rc);
        std::vector<int64_t>& out = cand[fi];
        int32_t status = P4_OK;
        sqlite3_stmt* q = c->sql(sql);
        auto collect = [&]() {
            sqlite3_bind_int64(q, 3, lo_);
            sqlite3_bind_int64(q, 4, hi_);
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) out.push_back(sqlite3_column_int64(q, 0));
            sqlite3_reset(q);
            if (r != SQLITE_DONE) status = statusOfSqlite(r);
        };
        c->exec("BEGIN");
        if (!q) {
            status = P4_E_INTERNAL;
        } else if (eq) {
            for (size_t i = 0; status == P4_OK && i < kp->vals.size(); i++) {
                bindCell(q, 1, kp->vals[i]);
                collect();
            }
        } else if (like) {
            bindCell(q, 1, kp->vals[0]);
            collect();
        } else {
            if (hasLo) bindCell(q, 1, kp->vals[0]);
            if (hasHi) bindCell(q, 2, kp->vals[kp->op == P4_OP_BETWEEN ? 1 : 0]);
            collect();
        }
        c->exec("COMMIT");
        e_->rpool.release(c);
        if (status != P4_OK) return status;
        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
        total += out.size();
        if (total > kMaxCand) return P4_OK;
    }
    cand_ = std::move(cand);
    kDriven_ = true;
    return P4_OK;
}

// An epoch-ordered (W) scan of one source holding a small part of its files
// (at most 20,000 rows and a quarter of each file's): the source's seqs from
// rl_sid, then its rows sorted by w; otherwise the epoch index is walked and
// the tags checked.
int32_t Scan::collectSourceCandidates() {
    if (s_.order != P4_ORDER_W_DESC || srcSql_[0].empty()) return P4_OK;
    int64_t total = 0;
    for (const FRef& fr : files_) {
        if (!fr.indexed || fr.laneN * 4 > fr.n) return P4_OK;
        total += fr.laneN;
    }
    if (total > 20000) return P4_OK;
    std::vector<std::vector<int64_t>> cand(files_.size());
    for (size_t fi = 0; fi < files_.size(); fi++) {
        const FRef& fr = files_[fi];
        int rc = 0;
        Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
        if (!c) return statusOfSqlite(rc);
        sqlite3_stmt* q = c->sql(srcSql_[0]);
        int32_t status = q ? P4_OK : P4_E_INTERNAL;
        if (q) {
            sqlite3_bind_int64(q, 1, *sids_.begin());
            sqlite3_bind_int64(q, 2, std::max(lo_, fr.minseq - 1));
            sqlite3_bind_int64(q, 3, std::min(hi_, fr.maxseq));
            sqlite3_bind_int64(q, 4, 40000);
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                const int64_t sq = sqlite3_column_int64(q, 0);
                if (cand[fi].empty() || cand[fi].back() != sq) cand[fi].push_back(sq);
            }
            sqlite3_reset(q);
            if (r != SQLITE_DONE) status = statusOfSqlite(r);
        }
        e_->rpool.release(c);
        if (status != P4_OK) return status;
    }
    cand_ = std::move(cand);
    kDriven_ = true;
    return P4_OK;
}

// A seq-ordered scan with an epoch window (EPOCH / EPOCH_DAY / W bounds):
// its seqs from the epoch index r_w, then the rows in seq order. Over 200,000
// candidates the scan walks instead.
int32_t Scan::collectEpochCandidates() {
    const bool seqOrder = s_.order == P4_ORDER_SEQ_ASC || s_.order == P4_ORDER_SEQ_DESC;
    if (!seqOrder || (s_.wLo == INT64_MIN && s_.wHi == INT64_MAX)) return P4_OK;
    const size_t kMaxCand = 200000;
    size_t total = 0;
    std::vector<std::vector<int64_t>> cand(files_.size());
    for (size_t fi = 0; fi < files_.size(); fi++) {
        const FRef& fr = files_[fi];
        if (!fr.indexed) return P4_OK;  // a migration before REBUILD 1: the walk
        int rc = 0;
        Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
        if (!c) return statusOfSqlite(rc);
        sqlite3_stmt* q = c->sql("SELECT seq FROM r INDEXED BY r_w WHERE w>=?1 AND w<=?2 AND seq>?3 AND seq<=?4");
        int32_t status = q ? P4_OK : P4_E_INTERNAL;
        if (q) {
            sqlite3_bind_int64(q, 1, s_.wLo);
            sqlite3_bind_int64(q, 2, s_.wHi);
            sqlite3_bind_int64(q, 3, s_.lane ? std::max(lo_, fr.minseq - 1) : lo_);
            sqlite3_bind_int64(q, 4, s_.lane ? std::min(hi_, fr.maxseq) : hi_);
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                cand[fi].push_back(sqlite3_column_int64(q, 0));
                if (total + cand[fi].size() > kMaxCand) break;
            }
            sqlite3_reset(q);
            if (r != SQLITE_DONE && r != SQLITE_ROW) status = statusOfSqlite(r);
        }
        e_->rpool.release(c);
        if (status != P4_OK) return status;
        total += cand[fi].size();
        if (total > kMaxCand) return P4_OK;
        std::sort(cand[fi].begin(), cand[fi].end());
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
    if (fc.candPos >= all.size()) {
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
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
    if (!c) {
        return statusOfSqlite(rc);
    }
    c->exec("BEGIN");
    std::deque<Row> rows;
    int32_t status = loadRows(c, fi, asc, &rows);
    if (status == P4_OK) status = loadTags(c, fi, rows);
    c->exec("COMMIT");
    e_->rpool.release(c);
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
    q = c->sql("SELECT seq, sid, lane, at, u FROM rl WHERE seq>=?1 AND seq<=?2");
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

// One page of a file in seq order (one read transaction). Two read paths: a
// single source's seqs from rl_sid (the filter's lanes checked inside the
// index), then its rows by seq; otherwise one range read of the rows.
int32_t Scan::fetchSeq(int fi) {
    if (kDriven_) return fetchCandidates(fi, false);
    FileCur& fc = cur_[size_t(fi)];
    FRef& fr = files_[size_t(fi)];
    if (fc.done) return P4_OK;
    const bool desc = s_.order == P4_ORDER_SEQ_DESC || (s_.bound && s_.order != P4_ORDER_SEQ_ASC);
    // A lane filter's rows lie inside its lanes' seq bounds in this file.
    const int64_t lo = s_.lane ? std::max(lo_, fr.minseq - 1) : lo_;
    const int64_t hi = s_.lane ? std::min(hi_, fr.maxseq) : hi_;
    if (!fc.started) {
        fc.resumeSeq = desc ? hi + 1 : lo;
        fc.started = true;
    }
    int rc = 0;
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
    if (!c) {
        e_->bump(kStReadErrors);
        return statusOfSqlite(rc);
    }
    int32_t status = P4_OK;
    c->exec("BEGIN");
    const int page = 256;
    std::deque<Row> rows;
    if (!srcSql_[0].empty() && fr.indexed) {
        sqlite3_stmt* q = c->sql(srcSql_[desc ? 1 : 0]);
        if (!q) status = P4_E_INTERNAL;
        std::vector<int64_t> seqs;
        int got = 0;
        if (q) {
            sqlite3_bind_int64(q, 1, *sids_.begin());
            sqlite3_bind_int64(q, 2, fc.resumeSeq);
            sqlite3_bind_int64(q, 3, desc ? lo : hi);
            sqlite3_bind_int64(q, 4, page * 2);
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                const int64_t sq = sqlite3_column_int64(q, 0);
                got++;
                fc.resumeSeq = sq;
                if (seqs.empty() || seqs.back() != sq) seqs.push_back(sq);
            }
            sqlite3_reset(q);
            if (r != SQLITE_DONE) status = statusOfSqlite(r);
        }
        if (status == P4_OK && got == 0) fc.done = true;
        if (status == P4_OK) {
            std::sort(seqs.begin(), seqs.end());
            status = loadRows(c, fi, seqs, &rows);
        }
        if (status == P4_OK) status = loadTags(c, fi, rows);
        if (status == P4_OK && desc) std::reverse(rows.begin(), rows.end());
    } else {
        const bool nd = needData();
        static const char* kSql[2][2] = {
            {"SELECT seq, cid, e, k, ts, x, length(d), p, f, NULL FROM r WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3",
             "SELECT seq, cid, e, k, ts, x, length(d), p, f, d FROM r WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3"},
            {"SELECT seq, cid, e, k, ts, x, length(d), p, f, NULL FROM r WHERE seq<?1 AND seq>?2 ORDER BY seq DESC LIMIT ?3",
             "SELECT seq, cid, e, k, ts, x, length(d), p, f, d FROM r WHERE seq<?1 AND seq>?2 ORDER BY seq DESC LIMIT ?3"}};
        sqlite3_stmt* q = c->sql(kSql[desc ? 1 : 0][nd ? 1 : 0]);
        if (!q) status = P4_E_INTERNAL;
        else {
            sqlite3_bind_int64(q, 1, fc.resumeSeq);
            sqlite3_bind_int64(q, 2, desc ? lo : hi);
            sqlite3_bind_int64(q, 3, page);
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
        }
    }
    c->exec("COMMIT");
    e_->rpool.release(c);
    if (status != P4_OK) {
        e_->bump(kStReadErrors);
        return status;
    }
    for (auto& rw : rows) fc.rows.push_back(std::move(rw));
    return P4_OK;
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
    const bool asc = s_.wAsc;
    if (!fc.started) {
        fc.resumeW = asc ? (s_.wLo == INT64_MIN ? INT64_MIN : s_.wLo - 1) : (s_.wHi == INT64_MAX ? INT64_MAX : s_.wHi + 1);
        fc.resumeSeq = asc ? INT64_MIN : INT64_MIN;
        fc.started = true;
    }
    int rc = 0;
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
    if (!c) {
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
    Conn* c = e_->rpool.acquire(fr.path, OpenKind::Reader, &rc, nullptr);
    if (!c) {
        return statusOfSqlite(rc);
    }
    c->exec("BEGIN");
    std::deque<Row> rows;
    int32_t st = loadRows(c, fi, {seq}, &rows);
    if (st == P4_OK) st = loadTags(c, fi, rows);
    c->exec("COMMIT");
    e_->rpool.release(c);
    if (st != P4_OK) return st;
    if (rows.empty()) return 0;
    *out = std::move(rows.front());
    return rowMatches(*out) ? 1 : 0;
}

// EPOCH points (2 nearest, 3 as_of, 4 forward) for every object, one r_ke(k,
// e) seek per object per partition (C-32): the objects are walked in k order
// (a seek to the next k), and each object's rows nearest the target are read
// from its run in rank order until one passes the scan's filters; ties at the
// best epoch go to the lowest CID (format 1's ranking). Records without an
// object are their own entities (their CID), read from r_ke's NULL prefix.
// *handled = false when the scan must answer instead.
int32_t Scan::epochByObject(int profile, int64_t at, std::map<std::string, EpochPick>* best, bool* handled) {
    *handled = false;
    if (!sp_->ek || !s_.search.empty() || profile < 2 || profile > 4) return P4_OK;
    const int oc = sp_->tc.firstObjectCol();
    const Spec2::Pred* kp = nullptr;  // narrows the objects walked (rowMatches still checks it)
    for (const auto& p : s_.preds)
        if (oc >= 0 && oc <= 3 && p.field == P4_F_COL0 + oc && !p.vals.empty() &&
            (p.op == P4_OP_EQ || p.op == P4_OP_IN)) {
            bool ok = true;
            for (const auto& v : p.vals) ok = ok && (v.type == ps::rb1::kInt || v.type == ps::rb1::kText);
            if (ok) kp = &p;
            break;
        }
    bool rangeOnly = true;  // every predicate is the epoch window (wLo/wHi)
    for (const auto& p : s_.preds) rangeOnly = rangeOnly && (p.field == P4_F_EPOCH || p.field == P4_F_W);
    // A lane filter alone is checked on the candidate's tag rows in the same
    // read transaction (rl's key starts with seq); anything else reads the row.
    const bool needCheck = s_.hasPeer || s_.hasCid || !rangeOnly;
    std::string laneSql;
    if (s_.lane && !needCheck) {
        laneSql = "SELECT 1 FROM rl WHERE seq=?1 AND sid IN (";
        bool first = true;
        for (uint32_t sid : sids_) {
            laneSql += (first ? "" : ",") + std::to_string(sid);
            first = false;
        }
        laneSql += ") AND lane IN (";
        first = true;
        for (uint32_t id : laneIds_) {
            laneSql += (first ? "" : ",") + std::to_string(id);
            first = false;
        }
        laneSql += ") LIMIT 1";
    }
    const bool rowCheck = needCheck || (s_.lane && laneSql.empty());
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
    auto offer = [&](const std::string& ent, const EpochPick& p) {
        auto it = best->find(ent);
        if (it == best->end() || better(p.e, p.key, it->second.e, it->second.key)) (*best)[ent] = p;
    };
    for (size_t fi = 0; fi < files_.size(); fi++) {
        {
            std::lock_guard<std::mutex> g(t_->mu);
            if (!files_[fi].f->indexed) {  // a migration before REBUILD 1: no r_ke
                best->clear();
                return P4_OK;
            }
        }
        int orc = 0;
        Conn* c = e_->rpool.acquire(files_[fi].path, OpenKind::Reader, &orc, nullptr);
        if (!c) return statusOfSqlite(orc);
        // Rank order from the target: below = e <= lim descending, above = e >= lim ascending.
        sqlite3_stmt* nextK = c->sql("SELECT k FROM r INDEXED BY r_ke WHERE k>?1 ORDER BY k LIMIT 1");
        sqlite3_stmt* firstK = c->sql("SELECT k FROM r INDEXED BY r_ke WHERE k IS NOT NULL ORDER BY k LIMIT 1");
        sqlite3_stmt* below = c->sql(
            "SELECT e, seq, cid FROM r INDEXED BY r_ke WHERE k=?1 AND e<=?2 AND e>=?3 AND seq>?4 AND seq<=?5 ORDER BY e DESC");
        sqlite3_stmt* above = c->sql(
            "SELECT e, seq, cid FROM r INDEXED BY r_ke WHERE k=?1 AND e>=?2 AND e<=?3 AND seq>?4 AND seq<=?5 ORDER BY e ASC");
        sqlite3_stmt* noK = c->sql(
            "SELECT e, seq, cid FROM r INDEXED BY r_ke WHERE k IS NULL AND e>=?1 AND e<=?2 AND seq>?3 AND seq<=?4");
        sqlite3_stmt* laneQ = laneSql.empty() ? nullptr : c->sql(laneSql);
        if (!nextK || !firstK || !below || !above || !noK || (!laneSql.empty() && !laneQ)) {
            e_->rpool.release(c);
            best->clear();
            return P4_OK;  // an older file without r_ke: the scan
        }
        // 1: the candidate passes the filters, 0: it does not, < 0: status.
        auto passes = [&](int64_t seq) -> int32_t {
            if (laneQ) {
                sqlite3_bind_int64(laneQ, 1, seq);
                const int r = sqlite3_step(laneQ);
                sqlite3_reset(laneQ);
                if (r != SQLITE_ROW && r != SQLITE_DONE) return statusOfSqlite(r);
                return r == SQLITE_ROW ? 1 : 0;
            }
            if (!rowCheck) return 1;
            Row row;
            return rowAt(int(fi), seq, &row);
        };
        int32_t rc = P4_OK;
        c->exec("BEGIN");
        // The first e-group (in rank order) of one object's run that has a row
        // passing the filters: its lowest-CID passing row.
        auto pick = [&](sqlite3_stmt* q, sqlite3_value* k, bool down, EpochPick* out) -> int32_t {
            const int64_t lim = down ? std::min(at, wHi) : std::max(at, wLo);
            if (down ? lim < wLo : lim > wHi) return 0;
            sqlite3_bind_value(q, 1, k);
            sqlite3_bind_int64(q, 2, lim);
            sqlite3_bind_int64(q, 3, down ? wLo : wHi);
            sqlite3_bind_int64(q, 4, lo_);
            sqlite3_bind_int64(q, 5, hi_);
            bool have = false, groupHas = false;
            int64_t groupE = 0;
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                const int64_t e = sqlite3_column_int64(q, 0);
                if (groupHas && e != groupE) break;  // the group with a passing row is complete
                groupE = e;
                L_->rowsExamined++;
                if (sqlite3_column_bytes(q, 2) != 32) continue;
                const uint8_t* key = static_cast<const uint8_t*>(sqlite3_column_blob(q, 2));
                const int64_t seq = sqlite3_column_int64(q, 1);
                const int32_t got = passes(seq);
                if (got < 0) {
                    sqlite3_reset(q);
                    return got;
                }
                if (got == 0) continue;
                if (!have || std::memcmp(key, out->key, 32) < 0) {
                    out->e = e;
                    out->seq = seq;
                    std::memcpy(out->key, key, 32);
                    out->fi = int(fi);
                    out->pid = files_[fi].pid;
                }
                have = groupHas = true;
            }
            sqlite3_reset(q);
            if (r != SQLITE_ROW && r != SQLITE_DONE) return statusOfSqlite(r);
            return have ? 1 : 0;
        };
        auto object = [&](sqlite3_value* k) -> int32_t {
            EpochPick b, a;
            int got = 0;
            if (profile != 4) {
                const int32_t r = pick(below, k, true, &b);
                if (r < 0) return r;
                got |= r;
            }
            if (profile != 3) {
                const int32_t r = pick(above, k, false, &a);
                if (r < 0) return r;
                got |= r << 1;
            }
            if (!got) return P4_OK;
            const EpochPick& p = got == 1 ? b : got == 2 ? a : (better(b.e, b.key, a.e, a.key) ? b : a);
            std::string ent;
            if (sqlite3_value_type(k) == SQLITE_INTEGER) ent = std::to_string(sqlite3_value_int64(k));
            else ent.assign(reinterpret_cast<const char*>(sqlite3_value_text(k)), size_t(sqlite3_value_bytes(k)));
            offer(ent, p);
            return check();
        };
        if (kp) {
            // The predicate's objects only.
            for (const auto& v : kp->vals) {
                sqlite3_stmt* lit = c->sql("SELECT ?1");
                if (!lit) {
                    rc = P4_E_INTERNAL;
                    break;
                }
                if (v.type == ps::rb1::kInt) sqlite3_bind_int64(lit, 1, v.i);
                else sqlite3_bind_text(lit, 1, v.s.data(), int(v.s.size()), SQLITE_TRANSIENT);
                if (sqlite3_step(lit) == SQLITE_ROW) rc = object(sqlite3_column_value(lit, 0));
                sqlite3_reset(lit);
                if (rc != P4_OK) break;
            }
        } else {
            // Every object: a seek to the next k after each.
            sqlite3_value* k = nullptr;
            int r = sqlite3_step(firstK);
            if (r == SQLITE_ROW) k = sqlite3_value_dup(sqlite3_column_value(firstK, 0));
            sqlite3_reset(firstK);
            if (r != SQLITE_ROW && r != SQLITE_DONE) rc = statusOfSqlite(r);
            while (k && rc == P4_OK) {
                rc = object(k);
                if (rc != P4_OK) break;
                sqlite3_bind_value(nextK, 1, k);
                sqlite3_value_free(k);
                k = nullptr;
                r = sqlite3_step(nextK);
                if (r == SQLITE_ROW) k = sqlite3_value_dup(sqlite3_column_value(nextK, 0));
                sqlite3_reset(nextK);
                if (r != SQLITE_ROW && r != SQLITE_DONE) rc = statusOfSqlite(r);
            }
            if (k) sqlite3_value_free(k);
        }
        // Records without an object: each its own entity (its CID).
        if (rc == P4_OK && !kp) {
            const int64_t a0 = profile == 4 ? std::max(at, wLo) : wLo;
            const int64_t a1 = profile == 3 ? std::min(at, wHi) : wHi;
            sqlite3_bind_int64(noK, 1, a0);
            sqlite3_bind_int64(noK, 2, a1);
            sqlite3_bind_int64(noK, 3, lo_);
            sqlite3_bind_int64(noK, 4, hi_);
            int r;
            while (rc == P4_OK && (r = sqlite3_step(noK)) == SQLITE_ROW) {
                L_->rowsExamined++;
                if (sqlite3_column_bytes(noK, 2) != 32) continue;
                EpochPick p;
                p.e = sqlite3_column_int64(noK, 0);
                p.seq = sqlite3_column_int64(noK, 1);
                std::memcpy(p.key, sqlite3_column_blob(noK, 2), 32);
                p.fi = int(fi);
                p.pid = files_[fi].pid;
                const int32_t got = passes(p.seq);
                if (got < 0) rc = got;
                if (got <= 0) continue;
                char cid[60];
                cidTextFromKey(p.key, cid);
                offer(std::string(cid, kCidText), p);
            }
            if (rc == P4_OK && r != SQLITE_DONE) rc = statusOfSqlite(r);
            sqlite3_reset(noK);
        }
        c->exec("COMMIT");
        e_->rpool.release(c);
        if (rc != P4_OK) return rc;
    }
    *handled = true;
    return P4_OK;
}

// CID order (C-34): the type index's c rows in (CID, pid) order with the
// pending layer (unflushed inserts and deletes, snapshotted at the start)
// merged over them, restricted to the scan's files. Entries past the
// visible-through cut are skipped; a page's rows are read by seq, one read
// transaction per file, and copies collapse to the lowest pid that matches.
int32_t Scan::cidStart() {
    cidStarted_ = true;
    for (size_t fi = 0; fi < files_.size(); fi++) fiOfPid_[files_[fi].pid] = int(fi);
    if (s_.hasCid) {
        // An exact CID: its holders (one type-index probe).
        std::vector<Holder> hs;
        const int32_t rc = holdersOf(L_, t_, s_.cidKey, &hs);
        if (rc != P4_OK) return rc;
        for (const Holder& h : hs) {
            if (!fiOfPid_.count(h.pid)) continue;
            CidEnt x;
            std::memcpy(x.key, s_.cidKey, 32);
            x.pid = h.pid;
            x.seq = h.seq;
            x.st = 1;
            cidPend_.push_back(x);
        }
        cidIdxDone_ = true;
        return P4_OK;
    }
    {
        std::lock_guard<std::mutex> g(t_->mu);
        std::unordered_map<std::string, size_t> at;  // key + pid -> cidPend_ index
        auto take = [&](const CEnt& x, bool newer) {
            if (x.st != 1 && x.st != 2) return;
            if (!fiOfPid_.count(x.pid)) return;
            std::string k(reinterpret_cast<const char*>(x.key), 32);
            k.append(reinterpret_cast<const char*>(&x.pid), 4);
            CidEnt c;
            std::memcpy(c.key, x.key, 32);
            c.pid = x.pid;
            c.seq = x.seq;
            c.st = x.st;
            auto it = at.find(k);
            if (it == at.end()) {
                at.emplace(std::move(k), cidPend_.size());
                cidPend_.push_back(c);
            } else if (newer) {
                cidPend_[it->second] = c;
            }
        };
        for (const CEnt& x : t_->flushing.raw()) take(x, false);
        for (const CEnt& x : t_->pend.raw()) take(x, true);  // the newer state
    }
    std::sort(cidPend_.begin(), cidPend_.end(), [](const CidEnt& a, const CidEnt& b) {
        const int c = std::memcmp(a.key, b.key, 32);
        return c ? c < 0 : a.pid < b.pid;
    });
    return P4_OK;
}

// The next entry of the merge: *have = false at the end.
int32_t Scan::cidNextEnt(CidEnt* out, bool* have) {
    for (;;) {
        if (cidIdx_.empty() && !cidIdxDone_) {
            int32_t rc = P4_OK;
            Conn* x = indexReader(L_, t_, &rc);
            if (!x) {
                if (rc != P4_OK) return rc;
                cidIdxDone_ = true;
            } else {
                sqlite3_stmt* q = x->sql("SELECT cid, pid, seq FROM c WHERE (cid, pid) > (?1, ?2) ORDER BY cid, pid LIMIT 1024");
                if (!q) return P4_E_INTERNAL;
                sqlite3_bind_blob(q, 1, cidIdxKey_, 32, SQLITE_STATIC);
                sqlite3_bind_int64(q, 2, cidIdxPid_);  // from (0^32, 0): pids start at 1
                int r;
                size_t got = 0;
                while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                    got++;
                    if (sqlite3_column_bytes(q, 0) != 32) continue;
                    CidEnt c;
                    std::memcpy(c.key, sqlite3_column_blob(q, 0), 32);
                    c.pid = uint32_t(sqlite3_column_int64(q, 1));
                    c.seq = sqlite3_column_int64(q, 2);
                    c.st = 1;
                    std::memcpy(cidIdxKey_, c.key, 32);
                    cidIdxPid_ = c.pid;
                    if (fiOfPid_.count(c.pid)) cidIdx_.push_back(c);
                }
                sqlite3_reset(q);
                if (r != SQLITE_DONE) return statusOfSqlite(r);
                if (got < 1024) cidIdxDone_ = true;
                if (cidIdx_.empty() && !cidIdxDone_) continue;  // a page of other files' entries
            }
        }
        const bool hi = !cidIdx_.empty(), hp = cidPendAt_ < cidPend_.size();
        if (!hi && !hp) {
            *have = false;
            return P4_OK;
        }
        int c = 0;
        if (hi && hp) {
            const CidEnt& a = cidIdx_.front();
            const CidEnt& b = cidPend_[cidPendAt_];
            c = std::memcmp(a.key, b.key, 32);
            if (!c) c = a.pid < b.pid ? -1 : a.pid > b.pid ? 1 : 0;
        } else {
            c = hi ? -1 : 1;
        }
        if (c < 0) {
            *out = cidIdx_.front();
            cidIdx_.pop_front();
        } else {
            if (c == 0) cidIdx_.pop_front();  // the pending state replaces the index row
            *out = cidPend_[cidPendAt_++];
            if (out->st != 1) continue;  // deleted
        }
        *have = true;
        return P4_OK;
    }
}

// The next page of rows: whole CIDs (every copy of the last one), each
// resolved to its lowest-pid copy that passes the filters.
int32_t Scan::cidPage() {
    const bool simple = !s_.lane && s_.preds.empty() && !s_.hasPeer && !s_.eNull && !s_.eNotNull && !fts_ && !s_.hasCid &&
                        !s_.hasProducer;
    std::vector<CidEnt> ents;
    for (;;) {
        CidEnt x;
        bool have = false;
        if (cidPeeked_) {
            x = cidPeek_;
            have = true;
            cidPeeked_ = false;
        } else {
            const int32_t rc = cidNextEnt(&x, &have);
            if (rc != P4_OK) return rc;
        }
        if (!have) {
            cidDone_ = true;
            break;
        }
        L_->rowsExamined++;
        if (x.seq > hi_) continue;  // not yet visible
        const bool sameCid = !ents.empty() && std::memcmp(ents.back().key, x.key, 32) == 0;
        if (!sameCid && simple && skipped_ < s_.offset) {
            // The offset of an unfiltered window: whole CIDs, no rows read.
            if (!haveLast_ || std::memcmp(lastKey_, x.key, 32) != 0) skipped_++;
            std::memcpy(lastKey_, x.key, 32);
            haveLast_ = true;
            continue;
        }
        if (haveLast_ && std::memcmp(lastKey_, x.key, 32) == 0 && ents.empty()) continue;  // a copy of a skipped CID
        if (!sameCid && ents.size() >= 256) {
            cidPeek_ = x;
            cidPeeked_ = true;
            break;
        }
        ents.push_back(x);
        int32_t rc = check();
        if (rc != P4_OK) return rc;
    }
    if (ents.empty()) return P4_OK;
    // The rows, one read transaction per file.
    std::map<int, std::vector<int64_t>> seqsOf;
    for (const CidEnt& x : ents) seqsOf[fiOfPid_[x.pid]].push_back(x.seq);
    std::map<std::pair<int, int64_t>, Row> rows;
    for (auto& kv : seqsOf) {
        std::sort(kv.second.begin(), kv.second.end());
        kv.second.erase(std::unique(kv.second.begin(), kv.second.end()), kv.second.end());
        int rc = 0;
        Conn* c = e_->rpool.acquire(files_[size_t(kv.first)].path, OpenKind::Reader, &rc, nullptr);
        if (!c) {
            e_->bump(kStReadErrors);
            return statusOfSqlite(rc);
        }
        c->exec("BEGIN");
        std::deque<Row> got;
        int32_t st = loadRows(c, kv.first, kv.second, &got);
        if (st == P4_OK) st = loadTags(c, kv.first, got);
        c->exec("COMMIT");
        e_->rpool.release(c);
        if (st != P4_OK) {
            e_->bump(kStReadErrors);
            return st;
        }
        for (Row& r : got) rows.emplace(std::make_pair(kv.first, r.seq), std::move(r));
    }
    for (size_t i = 0; i < ents.size();) {
        size_t j = i;
        while (j < ents.size() && std::memcmp(ents[j].key, ents[i].key, 32) == 0) j++;
        for (size_t k = i; k < j; k++) {
            auto it = rows.find({fiOfPid_[ents[k].pid], ents[k].seq});
            if (it == rows.end() || std::memcmp(it->second.key, ents[k].key, 32) != 0) continue;
            if (!rowMatches(it->second)) continue;  // a later copy of this CID may match
            std::memcpy(lastKey_, ents[k].key, 32);
            haveLast_ = true;
            if (skipped_ < s_.offset) skipped_++;
            else cidRows_.push_back(std::move(it->second));
            break;
        }
        i = j;
    }
    return P4_OK;
}

int32_t Scan::nextCid(Row** out) {
    if (!cidStarted_) {
        const int32_t rc = cidStart();
        if (rc != P4_OK) return rc;
    }
    for (;;) {
        int32_t rc = check();
        if (rc != P4_OK) return rc;
        if (!cidRows_.empty()) {
            if (s_.limit && emitted_ >= s_.limit) return 0;
            emitted_++;
            out_ = std::move(cidRows_.front());
            cidRows_.pop_front();
            *out = &out_;
            return 1;
        }
        if (cidDone_) return 0;
        rc = cidPage();
        if (rc != P4_OK) return rc;
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
    const uint32_t n = ld32(cids->v);
    std::vector<Holder> hs;
    for (uint32_t i = 0; i < n && rc == P4_OK; i++) {
        const uint8_t* c36 = cids->v + 4 + size_t(i) * kCidBin;
        if (!cidBinValid(c36)) continue;  // not a bafkrei CID: never stored, a miss
        uint8_t key[32];
        cidKeyFromDigest(c36 + 4, key);
        {
            rc = holdersOf(L, t, key, &hs);
            if (rc != P4_OK || hs.empty()) continue;
            int emitted = 0;
            for (const Holder& h : hs) {
                if (emitted && !every) break;
                Part* f = nullptr;
                std::string producer, peer;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    Part* p = t->partById(h.pid);
                    if (p) {
                        if (p->created) f = p;
                        producer = p->producer;
                        peer = p->peer;
                    }
                }
                if (!f) continue;
                int orc = 0;
                Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &orc, nullptr);
                if (!c) {
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
                    rc = o.rowDone();
                    if (rc != P4_OK) break;
                    continue;
                }
                sqlite3_reset(q);
                e->rpool.release(c);
                if (sr != SQLITE_ROW && sr != SQLITE_DONE) {
                    rc = statusOfSqlite(sr);
                    break;
                }
            }
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
    std::unordered_map<uint32_t, LaneDef> lanes;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& l : t->lanes) lanes[l->id] = *l;
    }
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
        {
            rc = holdersOf(L, t, key, &hs);
            for (const Holder& h : hs) {
                if (rc != P4_OK) break;
                Part* f = nullptr;
                std::string producer;
                std::map<uint32_t, std::string> url0;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    Part* p = t->partById(h.pid);
                    if (p) {
                        if (p->created) {
                            f = p;
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
    // No filter: the type's counters. A lane filter only: the counters of the
    // lanes it selects, summed over the partitions (format 1's source
    // summary, which format 2's HEAD sums the same way).
    const bool plain = s.preds.empty() && !s.hasCid && !s.hasPeer && !s.hasProducer && s.search.empty() &&
                       s.seqAfter == 0 && s.seqThrough == 0 && !cap && s.offset == 0 && s.limit == 0;
    if (plain) {
        {
            std::lock_guard<std::mutex> g(t->mu);
            if (!s.lane) {
                n = t->uniq;
                bytes = t->uniqBytes;
            }
            std::unordered_map<uint32_t, bool> match;  // lane id -> selected
            for (auto& p : t->parts) {
                if (!p->created) continue;
                if (!s.lane) {
                    maxSeq = std::max(maxSeq, std::min(p->maxseq, through));
                    maxTs = std::max(maxTs, p->maxts);
                }
                for (auto& lk : p->lanes) {
                    const LaneCount& lc = lk.second;
                    if (!s.lane) {
                        maxAt = std::max(maxAt, lc.maxat);
                        continue;
                    }
                    auto mi = match.find(lk.first);
                    if (mi == match.end()) {
                        const LaneDef* l = t->laneById(lk.first);
                        bool ok = l != nullptr;
                        const std::string* f[6] = {nullptr};
                        if (l) {
                            f[0] = &l->provider; f[1] = &l->source; f[2] = &l->batch;
                            f[3] = &l->ckey; f[4] = &l->ppeer; f[5] = &l->pkey;
                        }
                        for (int i = 0; i < 6 && ok; i++) ok = !s.lfSet[i] || *f[i] == s.lf[i];
                        mi = match.emplace(lk.first, ok).first;
                    }
                    if (!mi->second) continue;
                    n += lc.n;
                    bytes += lc.bytes;
                    maxSeq = std::max(maxSeq, std::min(lc.maxseq, through));
                    maxTs = std::max(maxTs, lc.maxts);
                    maxAt = std::max(maxAt, lc.maxat);
                }
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
                for (auto& p : t->parts) nnull += p->nnull;
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
            for (auto& p : t->parts) {
                    Part* f = &*p;
                    if (!f->created) continue;
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
                    {
                        Part* f = &*p;
                        if (!f->created) continue;
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
                {
                    Part* f = &*p;
                    if (!f->created) continue;
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
            // Maintained sizes, no file scans: each file's pages and free
            // pages after its writer's last commit, its WAL's frames at its
            // last commit, and the T/ files as the maintenance thread last
            // measured them. A file not written since the open is measured
            // once. Rollback journals: none in WAL mode (0).
            std::vector<std::pair<Part*, std::string>> unread;
            int64_t db = 0, wal = 0, jn = 0, free = 0;
            size_t files = 0;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& p : t->parts) {
                    if (!p->created) continue;
                    files++;
                    if (p->dbBytes < 0) unread.push_back({p.get(), p->path});
                    else db += p->dbBytes;
                    free += p->freeBytes;
                }
            }
            for (auto& u : unread) {
                const int64_t n = std::max<int64_t>(0, ioSize(u.second));
                db += n;
                std::lock_guard<std::mutex> g(t->mu);
                if (u.first->dbBytes < 0) u.first->dbBytes = n;
            }
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& p : t->parts)
                    if (p->created) wal += walBytesOf(p->path);
            }
            const int64_t idx = t->idxBytes.load(std::memory_order_relaxed) + walBytesOf(t->pIdx) + walBytesOf(t->pJnl);
            const int64_t fts = t->ftsBytes.load(std::memory_order_relaxed) + walBytesOf(t->pFts);
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
        for (auto& p : t->parts) {
                if (!p->created) continue;
                for (auto& lk : p->lanes) {
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
