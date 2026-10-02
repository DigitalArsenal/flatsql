// Store format 4: partition files and the write path (design §2.1, §4).
//
// A partition (producer x type) is pinned to one writer thread, which holds
// the one writer connection of its file. A group (one or more queued PUT
// calls of the partition, at most group-commit records) is:
//   1. prepared outside any lock: checks, extraction, the CID key;
//   2. deduplicated: holders probed without the type lock; under the type's
//      dedupe lock only the keys that looked new are rechecked, then seqs are
//      assigned in content-time order and the range is in flight (M7);
//   3. journaled (synchronous=FULL) before the partition file commits;
//   4. written: one transaction (rows, tag instances, CAT supersede, the
//      file's lane and meta counters);
//   5. published: entries flushable, counters, the visible-through watermark;
//   6. acked, after the commit and the publish (C-4).
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

// ---- partition files ------------------------------------------------------------------------
namespace {
const char* kFileSchema =
    "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS src(id INTEGER PRIMARY KEY, provider TEXT NOT NULL, source TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS lane(id INTEGER PRIMARY KEY, sid INTEGER NOT NULL, batch TEXT NOT NULL,"
    " ckey TEXT NOT NULL, ppeer TEXT NOT NULL, pkey TEXT NOT NULL, url TEXT, url0 TEXT, created INTEGER,"
    " updated INTEGER, maxat INTEGER, n INTEGER, bytes INTEGER, minw INTEGER, maxw INTEGER, maxseq INTEGER,"
    " maxts INTEGER, minseq INTEGER);"
    "CREATE TABLE IF NOT EXISTS r(seq INTEGER PRIMARY KEY, cid BLOB NOT NULL, e INTEGER, k, ts INTEGER NOT NULL,"
    " p TEXT, f BLOB, s INTEGER, x BLOB, d BLOB NOT NULL,"
    " w INTEGER GENERATED ALWAYS AS (coalesce(e, ts)) VIRTUAL);"
    // Tag instances keyed by seq first: a page's tags are one key range (no
    // lookups); rl_sid(sid, seq) serves source-ordered scans and supersede.
    "CREATE TABLE IF NOT EXISTS rl(sid INTEGER NOT NULL, seq INTEGER NOT NULL, lane INTEGER NOT NULL,"
    " at INTEGER NOT NULL, u TEXT, PRIMARY KEY(seq, sid, lane)) WITHOUT ROWID;"
    // Rows with an epoch per hour (Derived): epoch window and day counts.
    "CREATE TABLE IF NOT EXISTS wh(b INTEGER PRIMARY KEY, n INTEGER NOT NULL);";
}  // namespace

// The file's indexes (owner layout, C-32): arrival seq (the rowid, and r_s:
// the seqs alone, 12 B a row, for newest-N cuts and oldest-first quota);
// source newest-first (rl_sid); object + epoch (r_ke: one seek per object for
// EPOCH nearest / as_of / forward, object predicates, CAT supersede); epoch
// windows (r_w on w = coalesce(e, ts)). The type index is the one CID index
// (C-34): a file keeps none.
int32_t fileCreateIndexes(Type* t, Conn* c) {
    std::shared_ptr<const Spec> sp = t->spec();
    std::string ddl =
        "BEGIN IMMEDIATE;"
        "CREATE INDEX IF NOT EXISTS r_w ON r(w DESC);"
        "CREATE INDEX IF NOT EXISTS r_s ON r(seq);"
        "CREATE INDEX IF NOT EXISTS rl_sid ON rl(sid, seq);";
    if (sp->hasObject) ddl += "CREATE INDEX IF NOT EXISTS r_ke ON r(k, e);";
    ddl += "INSERT OR REPLACE INTO meta(k, v) VALUES('ix', 1);";
    int rc = c->exec(ddl.c_str());
    if (rc == SQLITE_OK) rc = derivedRecount(t, c);
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) c->exec("ROLLBACK");
    return rc == SQLITE_OK ? P4_OK : statusOfSqlite(rc);
}

int32_t fileSchema(Type* t, Conn* c, Part* f, bool indexes) {
    std::shared_ptr<const Spec> sp = t->spec();
    std::string ddl = std::string("BEGIN IMMEDIATE;") + kFileSchema;
    int rc = c->exec(ddl.c_str());
    if (rc != SQLITE_OK) {
        c->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    auto meta = [&](const char* k, const std::string& v) {
        sqlite3_stmt* s = c->get(S_META_SET);
        sqlite3_bind_text(s, 1, k, -1, SQLITE_STATIC);
        sqlite3_bind_text(s, 2, v.data(), int(v.size()), SQLITE_TRANSIENT);
        const int r = sqlite3_step(s);
        sqlite3_reset(s);
        return r == SQLITE_DONE;
    };
    bool ok = meta("format", "4") && meta("type", t->name) && meta("producer", f->producer) &&
              meta("peer", f->peer) && meta("pid", std::to_string(f->pid)) && meta("wh", "1");
    if (ok && !indexes) {
        sqlite3_stmt* s = c->sql("INSERT OR IGNORE INTO meta(k, v) VALUES('ix', 0)");
        ok = s && sqlite3_step(s) == SQLITE_DONE;
        if (s) sqlite3_reset(s);
    }
    if (!ok) {
        c->exec("ROLLBACK");
        return P4_E_IO;
    }
    rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) {
        c->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    if (indexes) return fileCreateIndexes(t, c);
    return P4_OK;
}

// ---- the object count and the epoch histogram -----------------------------------------------------
void KVal::bind(sqlite3_stmt* q, int at) const {
    if (type == 1) sqlite3_bind_int64(q, at, i);
    else if (type == 3) sqlite3_bind_text(q, at, s.data(), int(s.size()), SQLITE_TRANSIENT);
    else sqlite3_bind_null(q, at);
}

void KVal::from(sqlite3_stmt* q, int col) {
    const int ct = sqlite3_column_type(q, col);
    type = ct == SQLITE_INTEGER ? 1 : ct == SQLITE_TEXT ? 3 : 0;
    i = type == 1 ? sqlite3_column_int64(q, col) : 0;
    if (type == 3) s.assign(reinterpret_cast<const char*>(sqlite3_column_text(q, col)), size_t(sqlite3_column_bytes(q, col)));
    else s.clear();
}

int Derived::load(Conn* c, bool objects) {
    nobj = -1;
    wh = false;
    dh.clear();
    sqlite3_stmt* q = c->sql("SELECT k, v FROM meta WHERE k IN ('nobj','wh')");
    if (!q) return SQLITE_ERROR;
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        if (sqlite3_column_type(q, 1) == SQLITE_NULL) continue;
        const char* k = reinterpret_cast<const char*>(sqlite3_column_text(q, 0));
        if (k && std::strcmp(k, "nobj") == 0 && objects) nobj = sqlite3_column_int64(q, 1);
        else if (k && std::strcmp(k, "wh") == 0) wh = sqlite3_column_int64(q, 1) == 1;
    }
    sqlite3_reset(q);
    return r == SQLITE_DONE ? SQLITE_OK : r;
}

bool Derived::fresh(Conn* c, const KVal& k) {
    if (nobj < 0 || k.type == 0) return false;
    sqlite3_stmt* q = c->sql("SELECT 1 FROM r INDEXED BY r_ke WHERE k=?1 LIMIT 1");
    if (!q) {
        nobj = -2;  // cannot keep it: dropped at save
        return false;
    }
    k.bind(q, 1);
    const int r = sqlite3_step(q);
    sqlite3_reset(q);
    if (r != SQLITE_ROW && r != SQLITE_DONE) nobj = -2;
    return r == SQLITE_DONE;
}

void Derived::added(bool freshK, bool hasE, int64_t e) {
    if (freshK && nobj >= 0) nobj++;
    if (wh && hasE) dh[hourOf(e)]++;
}

int Derived::removed(Conn* c, const KVal& k, bool hasE, int64_t e) {
    if (wh && hasE) dh[hourOf(e)]--;
    if (nobj >= 0 && k.type != 0 && fresh(c, k)) nobj--;
    return SQLITE_OK;
}

int Derived::save(Conn* c) {
    if (wh) {
        sqlite3_stmt* up = c->sql("INSERT INTO wh(b, n) VALUES(?1, ?2) ON CONFLICT(b) DO UPDATE SET n=n+excluded.n");
        sqlite3_stmt* gone = c->sql("DELETE FROM wh WHERE b=?1 AND n<=0");
        if (!up || !gone) return SQLITE_ERROR;
        for (auto& kv : dh) {
            if (kv.second == 0) continue;
            sqlite3_bind_int64(up, 1, kv.first);
            sqlite3_bind_int64(up, 2, kv.second);
            int r = sqlite3_step(up);
            sqlite3_reset(up);
            if (r != SQLITE_DONE) return r;
            if (kv.second > 0) continue;
            sqlite3_bind_int64(gone, 1, kv.first);
            r = sqlite3_step(gone);
            sqlite3_reset(gone);
            if (r != SQLITE_DONE) return r;
        }
        dh.clear();
    }
    if (nobj == -2) {  // a check failed: the count is no longer known
        const int r = c->exec("DELETE FROM meta WHERE k='nobj'");
        nobj = -1;
        return r;
    }
    if (nobj >= 0) {
        sqlite3_stmt* s = c->get(S_META_SET);
        if (!s) return SQLITE_ERROR;
        sqlite3_bind_text(s, 1, "nobj", -1, SQLITE_STATIC);
        sqlite3_bind_int64(s, 2, nobj);
        const int r = sqlite3_step(s);
        sqlite3_reset(s);
        if (r != SQLITE_DONE) return r;
    }
    return SQLITE_OK;
}

int derivedRecount(Type* t, Conn* c) {
    const bool objects = t->spec()->hasObject;
    int rc = c->exec("CREATE TABLE IF NOT EXISTS wh(b INTEGER PRIMARY KEY, n INTEGER NOT NULL); DELETE FROM wh;");
    if (rc != SQLITE_OK) return rc;
    // One pass: r_ke's (k, e) keys when the file has them, else the rows.
    sqlite3_stmt* q = c->sql(objects ? "SELECT k, e FROM r INDEXED BY r_ke" : "SELECT NULL, e FROM r WHERE e IS NOT NULL");
    if (!q) return SQLITE_ERROR;
    std::map<int64_t, int64_t> hours;
    int64_t nobj = 0;
    KVal prev, cur;
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        if (objects) {
            cur.from(q, 0);
            if (cur.type != 0 && (cur.type != prev.type || cur.i != prev.i || cur.s != prev.s)) {
                nobj++;
                std::swap(prev, cur);
            }
        }
        if (sqlite3_column_type(q, 1) != SQLITE_NULL) hours[hourOf(sqlite3_column_int64(q, 1))]++;
    }
    sqlite3_reset(q);
    if (r != SQLITE_DONE) return r;
    sqlite3_stmt* ins = c->sql("INSERT INTO wh(b, n) VALUES(?1, ?2)");
    if (!ins) return SQLITE_ERROR;
    for (auto& kv : hours) {
        sqlite3_bind_int64(ins, 1, kv.first);
        sqlite3_bind_int64(ins, 2, kv.second);
        r = sqlite3_step(ins);
        sqlite3_reset(ins);
        if (r != SQLITE_DONE) return r;
    }
    std::string meta = "INSERT OR REPLACE INTO meta(k, v) VALUES('wh', 1);";
    if (objects) meta += "INSERT OR REPLACE INTO meta(k, v) VALUES('nobj', " + std::to_string(nobj) + ");";
    return c->exec(meta.c_str());
}

// ---- the file's counters ------------------------------------------------------------------------
Counters countersOf(const Part* f) {
    Counters k;
    k.n = f->n; k.bytes = f->bytes; k.ncopy = f->ncopy; k.minseq = f->minseq; k.maxseq = f->maxseq;
    k.minw = f->minw; k.maxw = f->maxw; k.maxts = f->maxts; k.nnull = f->nnull;
    k.mints = f->mints; k.mine = f->mine; k.maxe = f->maxe;
    return k;
}

void countersTo(Part* f, const Counters& k) {
    f->n = k.n; f->bytes = k.bytes; f->ncopy = k.ncopy; f->minseq = k.minseq; f->maxseq = k.maxseq;
    f->minw = k.minw; f->maxw = k.maxw; f->maxts = k.maxts; f->nnull = k.nnull;
    f->mints = k.mints; f->mine = k.mine; f->maxe = k.maxe;
}

int writeMeta(Conn* c, const Counters& k, int64_t now) {
    const bool any = k.n > 0;
    const struct {
        const char* k;
        bool set;
        int64_t v;
    } kv[] = {{"n", true, k.n},
              {"bytes", true, k.bytes},
              {"ncopy", true, k.ncopy},
              {"minseq", any && k.minseq != INT64_MAX, k.minseq},
              {"maxseq", true, k.maxseq},
              {"minw", any && k.minw != INT64_MAX, k.minw},
              {"maxw", any && k.maxw != INT64_MIN, k.maxw},
              {"maxts", true, k.maxts},
              {"nnull", true, k.nnull},
              {"mints", any && k.mints != INT64_MAX, k.mints},
              {"mine", k.mine != INT64_MAX, k.mine},
              {"maxe", k.maxe != INT64_MIN, k.maxe},
              {"updated", true, now}};
    for (auto& x : kv) {
        sqlite3_stmt* s = c->get(S_META_SET);
        if (!s) return SQLITE_ERROR;
        sqlite3_bind_text(s, 1, x.k, -1, SQLITE_STATIC);
        if (x.set) sqlite3_bind_int64(s, 2, x.v);
        else sqlite3_bind_null(s, 2);
        const int r = sqlite3_step(s);
        sqlite3_reset(s);
        if (r != SQLITE_DONE) return r;
    }
    return SQLITE_OK;
}

int readMeta(Conn* c, Counters* k, bool* indexed) {
    *k = Counters();
    *indexed = true;
    sqlite3_stmt* s = c->sql("SELECT k, v FROM meta");
    if (!s) return SQLITE_ERROR;
    int r;
    while ((r = sqlite3_step(s)) == SQLITE_ROW) {
        if (sqlite3_column_type(s, 1) == SQLITE_NULL) continue;
        const char* key = reinterpret_cast<const char*>(sqlite3_column_text(s, 0));
        const int64_t v = sqlite3_column_int64(s, 1);
        if (!key) continue;
        const std::string n(key);
        if (n == "n") k->n = v;
        else if (n == "bytes") k->bytes = v;
        else if (n == "ncopy") k->ncopy = v;
        else if (n == "minseq") k->minseq = v;
        else if (n == "maxseq") k->maxseq = v;
        else if (n == "minw") k->minw = v;
        else if (n == "maxw") k->maxw = v;
        else if (n == "maxts") k->maxts = v;
        else if (n == "nnull") k->nnull = v;
        else if (n == "mints") k->mints = v;
        else if (n == "mine") k->mine = v;
        else if (n == "maxe") k->maxe = v;
        else if (n == "ix") *indexed = v != 0;
    }
    sqlite3_reset(s);
    return r == SQLITE_DONE ? SQLITE_OK : r;
}

// ---- writer connections (≤ writer conns open; the LRU never closes a pinned one) ----------------
Conn* writerPin(Engine* e, Part* f, int32_t* rc, std::string* err) {
    {
        std::lock_guard<std::mutex> g(e->wconnMu);
        if (f->w) {
            f->wPins++;
            if (f->inLru) e->wlru.erase(f->lru);
            e->wlru.push_front(f);
            f->lru = e->wlru.begin();
            f->inLru = true;
            return f->w;
        }
    }
    Type* t = f->type;
    std::shared_ptr<const Spec> sp = t->spec();
    if (!f->created) {
        // The directory chain and the empty file, made durable, before SQLite opens it.
        const int32_t r = ioTouch(f->path);
        if (r != P4_OK) {
            *rc = r;
            if (err) *err = "create " + f->path;
            return nullptr;
        }
    }
    Conn* c = nullptr;
    const int r = openConn(f->path, OpenKind::Writer, e->cfg.writerCacheKiB, sp->pageSize, &c, err);
    if (r != SQLITE_OK) {
        *rc = statusOfSqlite(r);
        if (err) *err = f->path + ": " + *err + " (" + std::to_string(r) + ")";
        return nullptr;
    }
    sqlite3_wal_hook(c->db, walHook, e);
    if (!f->created) {
        bool migrating = false;
        {
            std::lock_guard<std::mutex> g(t->mu);
            migrating = !f->indexed;
        }
        const int32_t s = fileSchema(t, c, f, !migrating);
        if (s != P4_OK) {
            delete c;
            *rc = s;
            if (err) *err = "schema " + f->path;
            return nullptr;
        }
    }
    std::vector<Conn*> victims;
    {
        std::lock_guard<std::mutex> g(e->wconnMu);
        while (e->nWConn >= e->cfg.writerConns && !e->wlru.empty()) {
            auto it = e->wlru.end();
            Part* v = nullptr;
            while (it != e->wlru.begin()) {
                --it;
                if ((*it)->wPins == 0) {
                    v = *it;
                    break;
                }
            }
            if (!v) break;
            e->wlru.erase(v->lru);
            v->inLru = false;
            victims.push_back(v->w);
            v->w = nullptr;
            e->nWConn--;
        }
        f->w = c;
        f->wPins++;
        e->nWConn++;
        e->wlru.push_front(f);
        f->lru = e->wlru.begin();
        f->inLru = true;
    }
    // Closing a connection may checkpoint: the maintenance thread does it.
    if (!victims.empty()) {
        std::lock_guard<std::mutex> g(e->maintMu);
        for (Conn* v : victims) {
            MaintTask mt;
            mt.kind = MaintTask::kClose;
            mt.conn = v;
            e->maintQ.push_back(mt);
        }
    }
    if (!victims.empty()) e->kickMaintenance();
    return c;
}

void writerUnpin(Engine* e, Part* f) {
    std::lock_guard<std::mutex> g(e->wconnMu);
    if (f->wPins > 0) f->wPins--;
}

// ---- PUT -------------------------------------------------------------------------------------------
namespace {

constexpr uint16_t kFSealed = 1, kFIdent = 2, kFSeq = 4, kFPeer = 8, kFTags = 16;

struct TagIn {
    std::string f6[6];  // provider, source, batch, ckey, ppeer, pkey
    std::string url;
    bool valid = true;
    LaneDef* lane = nullptr;
};

struct Rec {
    // input
    uint16_t flags = 0;
    const uint8_t* cid36 = nullptr;
    int64_t ts = 0;
    const uint8_t* frame = nullptr;
    uint32_t frameLen = 0;
    const uint8_t* sealed = nullptr;
    uint32_t sealedLen = 0;
    const uint8_t* sig = nullptr;
    uint16_t sigLen = 0;
    const uint8_t* ident = nullptr;
    int64_t seqIn = 0;
    std::string peer;  // non-empty only when PEER differs from the call's
    std::vector<std::pair<uint16_t, int64_t>> tagsIn;
    // derived
    int32_t reject = 0;
    uint8_t key[32];
    bool hasE = false;
    int64_t e = 0, w = 0;
    int kType = 0;  // 0 none, 1 int, 3 text
    int64_t kInt = 0;
    std::string kText;
    std::string fCols;  // sealed: the extracted COL values
    bool hasSup = false;
    std::string supIdentity;
    uint32_t supSid = 0;  // the scope of this write (0: unattributed)
    const uint8_t* d = nullptr;
    uint32_t dLen = 0;
    // decision
    int action = 0;
    int64_t seq = 0;
    bool inFile = false;  // written to this partition's file (a row or tag instances)
    bool write = false;   // a new row in this partition (NEW, COPY, MIGRATED)
    bool dupOf = false;   // a repeat of an earlier record of the group
    size_t first = 0;     // that record (group index)
    std::vector<uint8_t> own;  // COPY: the holder's bytes
    bool identNew = false;
    uint64_t identSrc = 0;
    uint8_t identCid[32];
    std::vector<std::pair<uint32_t, int64_t>> inst;  // (lane id, at)
    std::vector<std::string> instUrl;
    bool retagged = false;
    bool urlChanged = false;
    bool superseded = false;  // retired by a later record of the group
};

struct Call {
    WriteTask* task = nullptr;
    uint32_t slot = 0;
    int mode = 0;
    std::string peer, token;
    std::vector<TagIn> tags;
    int64_t at = 0;
    std::vector<size_t> recs;  // group indexes
    int32_t status = P4_OK;
    std::string err;
};

// A deletion: a row of this partition's file.
struct Del {
    int64_t seq, len, w;
    bool hasE = false;
    int64_t e = 0;
    KVal k;
    uint8_t key[32];
    std::vector<std::pair<uint32_t, uint32_t>> lanes;  // (sid, lane) of its tags
    int others = 0;  // other holders of the CID
};

void bindK(sqlite3_stmt* s, int i, const Rec& r) {
    if (r.kType == 1) sqlite3_bind_int64(s, i, r.kInt);
    else if (r.kType == 3) sqlite3_bind_text(s, i, r.kText.data(), int(r.kText.size()), SQLITE_STATIC);
    else sqlite3_bind_null(s, i);
}

// The supersede identity of a stored record (unsealed bytes only).
std::string identityOf(const ps::TypeConfig& tc, const uint8_t* d, size_t n) {
    if (!d || n < 8) return std::string();
    std::vector<uint8_t> frame(n + 4);
    st32(frame.data(), uint32_t(n));
    std::memcpy(frame.data() + 4, d, n);
    ps::Extracted x;
    uint8_t scratch[2048];
    tc.extract(frame.data(), frame.size(), &x, scratch, sizeof scratch);
    return x.identity ? std::string(reinterpret_cast<const char*>(x.identity), x.identityLen) : std::string();
}

// Sealed records keep their extracted COL values in r.f: [u8 col][u8 type 1|3][u64 | u16 len + bytes].
std::string encodeCols(const ps::Extracted& x, uint32_t nCols) {
    std::string out;
    for (uint32_t c = 0; c < nCols && c < 4; c++) {
        const ps::ColValue& v = x.cols[c];
        if (!v.present) continue;
        out.push_back(char(c));
        if (v.isU64) {
            out.push_back(1);
            uint8_t b[8];
            st64(b, v.u);
            out.append(reinterpret_cast<const char*>(b), 8);
        } else {
            out.push_back(3);
            uint8_t b[2];
            const uint16_t n = uint16_t(v.n > 0xffff ? 0xffff : v.n);
            st16(b, n);
            out.append(reinterpret_cast<const char*>(b), 2);
            out.append(reinterpret_cast<const char*>(v.s), n);
        }
    }
    return out;
}

class Group {
public:
    Group(Engine* e, uint32_t writer, Part* p) : e_(e), p_(p), t_(p->type) {
        (void)writer;
        L_.e = e;
        L_.thread = writer;
        L_.cls = P4_CLASS_WRITE;
    }
    ~Group() {
        for (auto& kv : L_.idx) delete kv.second;
        L_.idx.clear();
    }
    void run(std::vector<WriteTask*>& tasks);

private:
    bool parseCall(Call& c);
    void prepare(Rec& r, const Call& c);
    int32_t probe();
    int32_t assign();
    int32_t writeJournal();
    int32_t writeFile(std::vector<size_t>& idxs);
    void publish(bool committed);
    void respond();
    int32_t readHolder(const Holder& h, Rec& r);
    void fail(int32_t status, const std::string& err);

    Engine* e_;
    Part* p_;
    Type* t_;
    std::shared_ptr<const Spec> sp_;
    P4Lane L_;  // the probes' type-index connections
    std::vector<Call> calls_;
    std::vector<Rec> recs_;
    std::vector<uint32_t> callOf_;  // record index -> its call
    bool writes_ = false;     // the group writes the partition file
    std::vector<Del> dels_;   // CAT supersede-on-ingest: rows retired by this group
    std::vector<std::pair<int64_t, int64_t>> inflight_;
    int64_t jfirst_ = 0;
    uint64_t flushEpoch_ = 0;
    std::vector<uint32_t> newLanes_, newSrcs_;
    bool partNew_ = false;
    std::string lastErr_;
};

bool Group::parseCall(Call& c) {
    Engine* e = e_;
    SlotHeader* h = e->slot(c.slot);
    const uint8_t* req = e->slotReq(c.slot);
    std::vector<Tlv> v;
    if (h->reqLen > e->reqBytes[0] || !tlvParse(req, h->reqLen, &v)) {
        c.status = P4_E_ARG;
        c.err = "malformed request";
        return false;
    }
    bool bad = false;
    uint8_t mode = 0;
    tlvU8(v, 52, &mode, &bad);
    c.mode = mode;
    if (mode > 1) bad = true;
    tlvText(v, 50, &c.peer);
    c.at = 0;
    if (!tlvI64(v, 54, &c.at, &bad)) c.at = nowSec();
    if (bad) {
        c.status = P4_E_ARG;
        c.err = "a typed tag has the wrong length";
        return false;
    }
    for (const Tlv& t : v) {
        if (t.tag != 51) continue;
        std::vector<Tlv> tv;
        TagIn tag;
        if (!tlvParse(t.v, t.n, &tv)) {
            c.status = P4_E_ARG;
            c.err = "malformed tag";
            return false;
        }
        static const uint16_t map6[6] = {1, 2, 4, 5, 6, 7};
        for (int i = 0; i < 6; i++) tlvText(tv, map6[i], &tag.f6[i]);
        tlvText(tv, 3, &tag.url);
        tag.valid = !tag.f6[0].empty() && !tag.f6[1].empty();
        c.tags.push_back(std::move(tag));
    }
    const Tlv* recs = tlvFind(v, 53);
    if (!recs || recs->n < 4) {
        c.status = P4_E_ARG;
        c.err = "no records (tag 53)";
        return false;
    }
    const uint8_t* p = recs->v;
    const uint8_t* end = recs->v + recs->n;
    const uint32_t count = ld32(p);
    p += 4;
    auto need = [&](size_t n) { return size_t(end - p) >= n; };
    for (uint32_t i = 0; i < count; i++) {
        Rec r;
        if (!need(2 + 2 + kCidBin + 8 + 4)) goto malformed;
        r.flags = ld16(p);
        if (r.flags & ~uint16_t(0x1f)) goto malformed;
        p += 4;
        r.cid36 = p;
        p += kCidBin;
        r.ts = int64_t(ld64(p));
        p += 8;
        r.frameLen = ld32(p);
        p += 4;
        if (!need(r.frameLen)) goto malformed;
        r.frame = p;
        p += r.frameLen;
        if (r.flags & kFSealed) {
            if (!need(4)) goto malformed;
            r.sealedLen = ld32(p);
            p += 4;
            if (!need(r.sealedLen)) goto malformed;
            r.sealed = p;
            p += r.sealedLen;
        }
        if (!need(2)) goto malformed;
        r.sigLen = ld16(p);
        p += 2;
        if (!need(r.sigLen)) goto malformed;
        r.sig = p;
        p += r.sigLen;
        if (r.flags & kFIdent) {
            if (!need(32)) goto malformed;
            r.ident = p;
            p += 32;
        }
        if (r.flags & kFSeq) {
            if (!need(8)) goto malformed;
            r.seqIn = int64_t(ld64(p));
            p += 8;
        }
        if (r.flags & kFPeer) {
            if (!need(2)) goto malformed;
            const uint16_t n = ld16(p);
            p += 2;
            if (!need(n)) goto malformed;
            r.peer.assign(reinterpret_cast<const char*>(p), n);
            p += n;
        }
        if (r.flags & kFTags) {
            if (!need(2)) goto malformed;
            const uint16_t n = ld16(p);
            p += 2;
            if (!need(size_t(n) * 10)) goto malformed;
            for (uint16_t k = 0; k < n; k++) {
                r.tagsIn.push_back({ld16(p), int64_t(ld64(p + 2))});
                p += 10;
            }
        }
        if (c.mode == 0 && (r.flags & (kFSeq | kFTags))) {
            c.status = P4_E_ARG;
            c.err = "SEQ and TAGS are migrate-mode only";
            return false;
        }
        c.recs.push_back(recs_.size());
        recs_.push_back(std::move(r));
        callOf_.push_back(uint32_t(&c - calls_.data()));
    }
    if (p != end) goto malformed;
    return true;
malformed:
    c.status = P4_E_ARG;
    c.err = "malformed record entries";
    c.recs.clear();
    return false;
}

void Group::prepare(Rec& r, const Call& c) {
    const ps::TypeConfig& tc = sp_->tc;
    if (!cidBinValid(r.cid36)) { r.reject = P4_REJ_CID_FORM; return; }
    if (r.frameLen < 4 || ld32(r.frame) != r.frameLen - 4) { r.reject = P4_REJ_FRAME_SIZE; return; }
    if (r.frameLen > tc.maxFrame() || uint64_t(r.frameLen) + r.sealedLen > uint64_t(e_->reqBytes[0]) - (64u << 10)) {
        r.reject = P4_REJ_TOO_LARGE;
        return;
    }
    const int32_t fc = tc.checkFrame(r.frame, r.frameLen);
    if (fc) { r.reject = fc; return; }
    if (tc.flags() & ps::TypeConfig::kVerifyCid) {
        uint8_t dg[32];
        ps::sha256(r.frame + 4, r.frameLen - 4, dg);
        if (std::memcmp(dg, r.cid36 + 4, 32) != 0) { r.reject = P4_REJ_CID; return; }
    }
    if (r.sealed && !ps::sealedEnvelopeValid(r.sealed, r.sealedLen)) { r.reject = P4_REJ_SEALED; return; }
    if (!r.peer.empty() && ps::producerToken(reinterpret_cast<const uint8_t*>(r.peer.data()), r.peer.size()) != c.token) {
        r.reject = P4_REJ_TAG;
        return;
    }
    if (c.mode == 1) {
        if (r.seqIn <= 0) { r.reject = P4_REJ_SEQ; return; }
        for (auto& ti : r.tagsIn)
            if (ti.first >= c.tags.size() || !c.tags[ti.first].valid) { r.reject = P4_REJ_TAG; return; }
    } else {
        for (const TagIn& ti : c.tags)
            if (!ti.valid) { r.reject = P4_REJ_TAG; return; }
    }
    cidKeyFromDigest(r.cid36 + 4, r.key);
    ps::Extracted x;
    uint8_t scratch[2048];
    tc.extract(r.frame, r.frameLen, &x, scratch, sizeof scratch);
    r.hasE = x.hasEpoch;
    r.e = x.epochSec;
    r.w = r.hasE ? r.e : r.ts;
    if (x.objectCol >= 0 && x.objectCol < int(ps::kMaxCols) && x.cols[x.objectCol].present) {
        const ps::ColValue& cv = x.cols[x.objectCol];
        if (cv.isU64) {
            r.kType = 1;
            r.kInt = int64_t(cv.u);
        } else {
            r.kType = 3;
            r.kText.assign(reinterpret_cast<const char*>(cv.s), cv.n);
        }
    }
    if (x.identity && x.identityLen) {
        r.hasSup = sp_->hasSupersede;
        r.supIdentity.assign(reinterpret_cast<const char*>(x.identity), x.identityLen);
    }
    if (r.sealed) {
        r.fCols = encodeCols(x, tc.nCols());
        r.d = r.sealed;
        r.dLen = r.sealedLen;
    } else {
        r.d = r.frame + 4;
        r.dLen = r.frameLen - 4;
    }
    // Tag instances.
    if (c.mode == 1) {
        for (auto& ti : r.tagsIn) r.inst.push_back({ti.first, ti.second});
    } else {
        for (size_t i = 0; i < c.tags.size(); i++) r.inst.push_back({uint32_t(i), c.at});
    }
}

int32_t Group::readHolder(const Holder& h, Rec& r) {
    Part* hf = nullptr;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        Part* hp = t_->partById(h.pid);
        if (hp && hp->created) hf = hp;
    }
    if (!hf) return 0;
    int rc = 0;
    std::string err;
    Conn* c = e_->rpool.acquire(hf->path, OpenKind::Reader, &rc, &err);
    if (!c) return statusOfSqlite(rc);
    sqlite3_stmt* s = c->get(S_R_HOLDER);
    int found = 0;
    int32_t status = P4_OK;
    if (!s) status = P4_E_INTERNAL;
    else {
        sqlite3_bind_int64(s, 1, h.seq);
        const int sr = sqlite3_step(s);
        if (sr == SQLITE_ROW && sqlite3_column_bytes(s, 2) == 32 && std::memcmp(sqlite3_column_blob(s, 2), r.key, 32) == 0) {
            const uint8_t* d = static_cast<const uint8_t*>(sqlite3_column_blob(s, 0));
            r.own.assign(d, d + sqlite3_column_bytes(s, 0));
            r.ts = sqlite3_column_int64(s, 1);
            found = 1;
        } else if (sr != SQLITE_ROW && sr != SQLITE_DONE) {
            status = statusOfSqlite(sr);
        }
        sqlite3_reset(s);
    }
    e_->rpool.release(c);
    return status != P4_OK ? status : found;
}

// Dedupe probes (no type lock), then the recheck and seq assignment under dmu.
int32_t Group::probe() {
    {
        std::lock_guard<std::mutex> g(t_->mu);
        flushEpoch_ = uint64_t(t_->lastFlushMs);
    }
    // In-group repeats: sort by key; later occurrences repeat the first.
    std::vector<size_t> ord;
    for (size_t i = 0; i < recs_.size(); i++)
        if (!recs_[i].reject && recs_[i].frame) ord.push_back(i);
    std::stable_sort(ord.begin(), ord.end(),
                     [&](size_t a, size_t b) { return std::memcmp(recs_[a].key, recs_[b].key, 32) < 0; });
    std::vector<Holder> hs;
    for (size_t oi = 0; oi < ord.size(); oi++) {
        Rec& r = recs_[ord[oi]];
        if (oi > 0) {
            Rec& q = recs_[ord[oi - 1]];
            if (std::memcmp(q.key, r.key, 32) == 0) {
                r.dupOf = true;
                r.first = q.dupOf ? q.first : ord[oi - 1];
                continue;
            }
        }
        int32_t rc = holdersOf(&L_, t_, r.key, &hs);
        if (rc != P4_OK) return rc;
        const Holder* mine = nullptr;
        for (const Holder& h : hs)
            if (h.pid == p_->pid) mine = &h;
        const bool migrate = calls_[0].mode == 1;  // a group never mixes modes (run() splits them)
        if (mine) {
            r.seq = mine->seq;
            if (migrate && r.seqIn != mine->seq) { r.reject = P4_REJ_SEQ; continue; }
            r.action = migrate ? P4_ACT_MIGRATED : P4_ACT_DUP;  // DUP becomes RETAG if an instance is new
            continue;
        }
        bool copied = false;
        for (const Holder& h : hs) {
            if (migrate) {
                if (r.seqIn != h.seq) { r.reject = P4_REJ_SEQ; break; }
                // C-35: a migrated copy stores its OWN bytes and ts (that
                // format-1 table's stored bytes; a sealed envelope differs per
                // copy). Stored bytes never change.
                r.seq = h.seq;
                r.action = P4_ACT_COPY;
                r.write = true;
                copied = true;
                break;
            }
            const int32_t got = readHolder(h, r);
            if (got < 0) return got;
            if (got == 1) {
                r.seq = h.seq;
                r.action = P4_ACT_COPY;
                r.write = true;
                r.d = r.own.data();
                r.dLen = uint32_t(r.own.size());
                r.sealed = nullptr;
                copied = true;
                break;
            }
        }
        if (r.reject || copied) continue;
        if (migrate && sp_->identity && r.ident && !r.tagsIn.empty()) {
            // A migrated record keeps its ingest identity (format 1's
            // sdn_record_ingest_identity row) for later ingest-mode repeats.
            const Call& c = calls_[callOf_[ord[oi]]];
            if (r.tagsIn[0].first < c.tags.size()) {
                const std::string b = c.tags[r.tagsIn[0].first].f6[0] + '\0' + c.tags[r.tagsIn[0].first].f6[1];
                uint8_t dg[32];
                ps::sha256(b.data(), b.size(), dg);
                r.identSrc = ld64(dg) & 0x7fffffffffffffffull;
                r.identNew = true;
                std::memcpy(r.identCid, r.key, 32);
            }
        }
        // An ingest identity the lane already holds (IQC).
        if (sp_->identity && r.ident && !migrate) {
            const Call* call = &calls_[callOf_[ord[oi]]];
            if (!call->tags.empty()) {
                std::string ps2[2] = {call->tags[0].f6[0], call->tags[0].f6[1]};
                std::string b = ps2[0] + '\0' + ps2[1];
                uint8_t dg[32];
                ps::sha256(b.data(), b.size(), dg);
                r.identSrc = ld64(dg) & 0x7fffffffffffffffull;
                int64_t hseq = 0;
                uint8_t hcid[32];
                rc = identHolder(&L_, t_, r.identSrc, r.ident, &hseq, hcid);
                if (rc != P4_OK) return rc;
                if (hseq) {
                    std::vector<Holder> ih;
                    rc = holdersOf(&L_, t_, hcid, &ih);
                    if (rc != P4_OK) return rc;
                    if (!ih.empty()) {
                        // The holder takes this write's tags: retag it here, or copy it here.
                        r.action = P4_ACT_IDENT_DUP;
                        std::memcpy(r.key, hcid, 32);
                        r.seq = ih[0].seq;
                        bool here = false;
                        for (const Holder& h : ih)
                            if (h.pid == p_->pid) {
                                here = true;
                                r.seq = h.seq;
                            }
                        if (!here) {
                            const int32_t got = readHolder(ih[0], r);
                            if (got < 0) return got;
                            if (got == 1) {
                                r.write = true;
                                r.d = r.own.data();
                                r.dLen = uint32_t(r.own.size());
                                r.sealed = nullptr;
                                r.fCols.clear();
                            } else {
                                r.action = 0;  // stale: a new record after all
                            }
                        }
                        if (r.action) continue;
                    }
                }
                r.identNew = true;
                std::memcpy(r.identCid, r.key, 32);
            }
        }
        r.action = migrate ? P4_ACT_MIGRATED : P4_ACT_NEW;
        r.write = true;
    }
    return P4_OK;
}

int32_t Group::assign() {
    std::lock_guard<std::mutex> dg(t_->dmu);
    const bool migrate = calls_[0].mode == 1;
    bool flushed;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        flushed = uint64_t(t_->lastFlushMs) != flushEpoch_;
    }
    std::vector<size_t> fresh;
    for (size_t i = 0; i < recs_.size(); i++) {
        Rec& r = recs_[i];
        if (r.reject || r.dupOf || !r.write || (r.action != P4_ACT_NEW && r.action != P4_ACT_MIGRATED)) continue;
        // Recheck: another writer of the type may have taken it since the probe.
        std::vector<Holder> hs;
        if (flushed) {
            const int32_t rc = holdersOf(&L_, t_, r.key, &hs);
            if (rc != P4_OK) return rc;
        } else {
            std::lock_guard<std::mutex> g(t_->mu);
            auto note = [&](const CEnt& x) {
                if (x.st == 1 || x.st == 4) {
                    for (auto& h : hs)
                        if (h.pid == x.pid) return;
                    hs.push_back(Holder{x.pid, x.seq});
                }
            };
            t_->pend.each(r.key, note);
            t_->flushing.each(r.key, note);
        }
        if (!hs.empty()) {
            if (migrate) {
                if (hs[0].seq != r.seqIn) { r.reject = P4_REJ_SEQ; continue; }
                r.action = P4_ACT_COPY;
                r.seq = hs[0].seq;
                continue;
            }
            bool mine = false;
            for (auto& h : hs) mine = mine || h.pid == p_->pid;
            if (mine) {
                r.action = P4_ACT_DUP;
                r.write = false;
                r.seq = hs[0].seq;
                continue;
            }
            const int32_t got = readHolder(hs[0], r);
            if (got < 0) return got;
            r.action = P4_ACT_COPY;
            r.seq = hs[0].seq;
            if (got == 1) {
                r.d = r.own.data();
                r.dLen = uint32_t(r.own.size());
                r.sealed = nullptr;
                r.fCols.clear();
            }
            // got == 0: the holder is in flight (another producer's group,
            // its file not committed yet). The CID still has one seq
            // (§3.8.2): this copy takes it and keeps its own bytes (the same
            // CID is the same plaintext).
            continue;
        }
        fresh.push_back(i);
    }
    std::sort(fresh.begin(), fresh.end(), [&](size_t a, size_t b) {
        const Rec& x = recs_[a];
        const Rec& y = recs_[b];
        if (x.w != y.w) return x.w < y.w;
        return std::memcmp(x.key, y.key, 32) < 0;
    });
    int64_t lo = INT64_MAX, hi = 0;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (size_t i : fresh) {
            Rec& r = recs_[i];
            if (migrate) {
                // Format 1's rowid, unique per type: a seq another row of this
                // file holds is refused by the insert (P4_REJ_SEQ); a repeat of
                // a migrated record is found by its CID above.
                r.seq = r.seqIn;
                if (r.seq >= t_->nextSeq) t_->nextSeq = r.seq + 1;
            } else {
                r.seq = t_->nextSeq++;
            }
            lo = std::min(lo, r.seq);
            hi = std::max(hi, r.seq);
        }
        for (Rec& r : recs_) {
            if (r.reject || r.dupOf || !r.write) continue;
            t_->pend.put(r.key, p_->pid, r.seq, 4);
            if (r.identNew) {
                IdentEnt ie;
                ie.src = r.identSrc;
                std::memcpy(ie.h, r.ident, 32);
                std::memcpy(ie.cid, r.identCid, 32);
                ie.seq = r.seq;
                ie.st = 4;
                t_->identPend[identMapKey(ie.src, ie.h)] = ie;
            }
        }
        if (hi) {
            t_->inflight.push_back({lo, hi});
            inflight_.push_back({lo, hi});
            if (t_->inflight.size() > e_->stat[kStMaxInflight].load()) e_->stat[kStMaxInflight].store(t_->inflight.size());
            t_->visRecompute();
        }
    }
    // Repeats inside the group take their first record's seq.
    for (Rec& r : recs_)
        if (r.dupOf) {
            const Rec& f = recs_[r.first];
            r.seq = f.seq;
            r.action = f.reject ? 0 : P4_ACT_DUP;
            if (f.reject) r.reject = f.reject;
        }
    // The seq block is durable before any seq of it can commit.
    int64_t need = 0;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        if (t_->nextSeq - 1 > t_->seqReserved) {
            t_->seqReserved = t_->nextSeq - 1 + int64_t(e_->cfg.seqBlock);
            need = t_->seqReserved;
        }
    }
    if (need) {
        const int32_t rc = journalReserve(t_, need);
        if (rc != P4_OK) return rc;
    }
    return P4_OK;
}

int32_t Group::writeJournal() {
    std::lock_guard<std::mutex> jg(t_->jmu);
    Conn* j = t_->jdb;
    int rc = j->exec("BEGIN IMMEDIATE");
    int64_t rows = 0;
    auto addRow = [&](int op, const uint8_t* k, const uint8_t* c, int64_t pid, int64_t seq, const std::string* s,
                      int64_t v) {
        if (rc != SQLITE_OK) return;
        sqlite3_stmt* q = j->get(S_J_INS);
        if (!q) { rc = SQLITE_ERROR; return; }
        sqlite3_bind_int(q, 1, op);
        if (k) sqlite3_bind_blob(q, 2, k, 32, SQLITE_STATIC); else sqlite3_bind_null(q, 2);
        if (c) sqlite3_bind_blob(q, 3, c, 32, SQLITE_STATIC); else sqlite3_bind_null(q, 3);
        sqlite3_bind_int64(q, 4, pid);
        sqlite3_bind_int64(q, 5, seq);
        if (s) sqlite3_bind_text(q, 6, s->data(), int(s->size()), SQLITE_STATIC); else sqlite3_bind_null(q, 6);
        sqlite3_bind_int64(q, 7, v);
        const int r = sqlite3_step(q);
        sqlite3_reset(q);
        if (r != SQLITE_DONE) rc = r;
        rows++;
    };
    if (partNew_) {
        const std::string s = p_->producer + '\x1f' + p_->peer;
        addRow(J_PART, nullptr, nullptr, p_->pid, 0, &s, 0);
    }
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (uint32_t id : newSrcs_) {
            SrcDef* d = t_->srcById(id);
            const std::string s = d->provider + '\x1f' + d->source;
            addRow(J_SRC, nullptr, nullptr, 0, id, &s, 0);
        }
        for (uint32_t id : newLanes_) {
            LaneDef* l = t_->laneById(id);
            const std::string s = l->provider + '\x1f' + l->source + '\x1f' + l->batch + '\x1f' + l->ckey + '\x1f' +
                                  l->ppeer + '\x1f' + l->pkey;
            addRow(J_LANE, nullptr, nullptr, 0, id, &s, l->sid);
        }
    }
    if (writes_) addRow(J_FILE, nullptr, nullptr, p_->pid, 0, nullptr, 0);
    for (Rec& r : recs_) {
        if (r.reject || r.dupOf || !r.write || r.superseded) continue;
        addRow(J_C, r.key, nullptr, p_->pid, r.seq, nullptr, 0);
        if (r.identNew) addRow(J_IDENT, r.ident, r.identCid, p_->pid, r.seq, nullptr, int64_t(r.identSrc));
    }
    for (Del& d : dels_) addRow(J_DEL, d.key, nullptr, p_->pid, d.seq, nullptr, d.len);
    if (rc == SQLITE_OK) rc = j->exec("COMMIT");
    if (rc != SQLITE_OK) {
        lastErr_ = std::string(sqlite3_errmsg(j->db)) + " (" + std::to_string(rc) + ")";
        j->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    e_->bump(kStJournalSyncs);
    const int64_t last = sqlite3_last_insert_rowid(j->db);
    jfirst_ = rows ? last - rows + 1 : 0;
    std::lock_guard<std::mutex> g(t_->mu);
    if (last > t_->jlast) t_->jlast = last;
    if (jfirst_) t_->jinflight.push_back(jfirst_);
    if (partNew_) p_->journaled = true;
    for (uint32_t id : newSrcs_) t_->srcJournaled[id - 1] = 1;
    for (uint32_t id : newLanes_) t_->laneJournaled[id - 1] = 1;
    return P4_OK;
}


struct FileOut : Counters {
    std::map<uint32_t, LaneCount> lanes;  // touched lanes, their new counts
};

int32_t Group::writeFile(std::vector<size_t>& idxs) {
    Part* f = p_;
    std::vector<Del>& dels = dels_;
    int32_t status = P4_OK;
    std::string err;
    Conn* c = writerPin(e_, f, &status, &err);
    if (!c) {
        lastErr_ = "writer open: " + err;
        return status;
    }
    FileOut o;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        static_cast<Counters&>(o) = countersOf(f);
    }
    auto laneCount = [&](uint32_t id) -> LaneCount& {
        auto it = o.lanes.find(id);
        if (it != o.lanes.end()) return it->second;
        LaneCount lc;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            auto fi = f->lanes.find(id);
            if (fi != f->lanes.end()) lc = fi->second;
        }
        return o.lanes.emplace(id, lc).first->second;
    };
    int rc = c->exec("BEGIN IMMEDIATE");
    auto bad = [&](int r) {
        if (rc == SQLITE_OK && r != SQLITE_OK && r != SQLITE_DONE && r != SQLITE_ROW) rc = r;
    };
    Derived dv;
    if (rc == SQLITE_OK) {
        bool ix;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            ix = f->indexed;
        }
        bad(dv.load(c, sp_->hasObject && ix));
    }
    std::unordered_set<uint32_t> sids;
    const int64_t now = nowSec();
    // A migration carries format 1's lane times: first seen = the earliest
    // tag instance, updated = the latest (format 1's source summary).
    const bool migrateTimes = !calls_.empty() && calls_[0].mode == 1;
    std::sort(idxs.begin(), idxs.end(), [&](size_t a, size_t b) { return recs_[a].seq < recs_[b].seq; });
    for (size_t gi : idxs) {
        if (rc != SQLITE_OK) break;
        Rec& r = recs_[gi];
        const Call* call = &calls_[callOf_[gi]];
        int64_t rowLen = r.dLen;
        if (r.write && !r.superseded) {
            KVal kv;
            kv.type = r.kType;
            kv.i = r.kInt;
            kv.s = r.kText;
            const bool freshK = dv.fresh(c, kv);
            sqlite3_stmt* s = c->get(S_INS);
            sqlite3_bind_int64(s, 1, r.seq);
            sqlite3_bind_blob(s, 2, r.key, 32, SQLITE_STATIC);
            if (r.hasE) sqlite3_bind_int64(s, 3, r.e); else sqlite3_bind_null(s, 3);
            bindK(s, 4, r);
            sqlite3_bind_int64(s, 5, r.ts);
            if (!r.peer.empty() && r.peer != p_->peer) sqlite3_bind_text(s, 6, r.peer.data(), int(r.peer.size()), SQLITE_STATIC);
            else if (call && call->peer != p_->peer && !call->peer.empty()) sqlite3_bind_text(s, 6, call->peer.data(), int(call->peer.size()), SQLITE_STATIC);
            else sqlite3_bind_null(s, 6);
            if (r.sealed && !r.fCols.empty()) sqlite3_bind_blob(s, 7, r.fCols.data(), int(r.fCols.size()), SQLITE_STATIC);
            else if (r.sealed) sqlite3_bind_blob(s, 7, "", 0, SQLITE_STATIC);
            else sqlite3_bind_null(s, 7);
            if (sp_->hasSupersede && r.supSid) sqlite3_bind_int64(s, 8, r.supSid); else sqlite3_bind_null(s, 8);
            if (r.sigLen) sqlite3_bind_blob(s, 9, r.sig, r.sigLen, SQLITE_STATIC); else sqlite3_bind_null(s, 9);
            sqlite3_bind_blob(s, 10, r.d, int(r.dLen), SQLITE_STATIC);
            const int ir = sqlite3_step(s);
            sqlite3_reset(s);
            if (ir == SQLITE_CONSTRAINT || (ir & 0xff) == SQLITE_CONSTRAINT) {
                r.reject = P4_REJ_SEQ;  // the seq is held by another row of this file
                continue;
            }
            bad(ir);
            if (rc != SQLITE_OK) break;
            dv.added(freshK, r.hasE, r.e);
            o.n++;
            o.bytes += r.dLen;
            if (r.action == P4_ACT_COPY || (r.action == P4_ACT_IDENT_DUP && r.write)) o.ncopy++;
            if (r.seq < o.minseq) o.minseq = r.seq;
            if (r.seq > o.maxseq) o.maxseq = r.seq;
            if (r.w < o.minw) o.minw = r.w;
            if (r.w > o.maxw) o.maxw = r.w;
            if (r.ts > o.maxts) o.maxts = r.ts;
            if (r.ts < o.mints) o.mints = r.ts;
            if (r.hasE && r.e < o.mine) o.mine = r.e;
            if (r.hasE && r.e > o.maxe) o.maxe = r.e;
            if (!r.hasE && sp_->hasEpochRule) o.nnull++;
        } else if (!r.inst.empty()) {
            // A held row takes tags: it must be this CID's row.
            sqlite3_stmt* s = c->get(S_R_LEN);
            sqlite3_bind_int64(s, 1, r.seq);
            const int sr = sqlite3_step(s);
            const bool ok = sr == SQLITE_ROW && sqlite3_column_bytes(s, 1) == 32 &&
                            std::memcmp(sqlite3_column_blob(s, 1), r.key, 32) == 0;
            if (ok) {
                rowLen = sqlite3_column_int64(s, 0);
                r.w = sqlite3_column_int64(s, 4);
            }
            sqlite3_reset(s);
            if (sr != SQLITE_ROW && sr != SQLITE_DONE) { bad(sr); break; }
            if (!ok) { r.reject = P4_REJ_BAD_ENTRY; continue; }
        }
        for (size_t ii = 0; ii < r.inst.size() && rc == SQLITE_OK; ii++) {
            const TagIn& tag = call->tags[r.inst[ii].first];
            LaneDef* l = tag.lane;
            const int64_t at = r.inst[ii].second;
            sids.insert(l->sid);
            LaneCount& lc = laneCount(l->id);
            if (lc.n == 0 && lc.url0.empty() && lc.created == 0) {
                // The lane's first instance in this file.
                lc.url0 = tag.url;
                lc.created = at;
            }
            sqlite3_stmt* s = c->get(S_RL_INS);
            sqlite3_bind_int64(s, 1, l->sid);
            sqlite3_bind_int64(s, 2, r.seq);
            sqlite3_bind_int64(s, 3, l->id);
            sqlite3_bind_int64(s, 4, at);
            if (tag.url != lc.url0) sqlite3_bind_text(s, 5, tag.url.data(), int(tag.url.size()), SQLITE_STATIC);
            else sqlite3_bind_null(s, 5);
            bad(sqlite3_step(s));
            sqlite3_reset(s);
            if (rc != SQLITE_OK) break;
            if (sqlite3_changes(c->db) > 0) {
                lc.n++;
                lc.bytes += rowLen;
                if (r.w < lc.minw) lc.minw = r.w;
                if (r.w > lc.maxw) lc.maxw = r.w;
                if (r.seq > lc.maxseq) lc.maxseq = r.seq;
                if (r.seq < lc.minseq) lc.minseq = r.seq;
                if (at > lc.maxat) lc.maxat = at;
                if (r.ts > lc.maxts) lc.maxts = r.ts;
                if (!r.write) r.retagged = true;
            } else {
                // DUP: source_url follows the latest write (C-3); at is unchanged.
                sqlite3_stmt* q = c->get(S_RL_ONE);
                sqlite3_bind_int64(q, 1, l->sid);
                sqlite3_bind_int64(q, 2, r.seq);
                sqlite3_bind_int64(q, 3, l->id);
                std::string cur = lc.url0;
                if (sqlite3_step(q) == SQLITE_ROW && sqlite3_column_type(q, 1) != SQLITE_NULL)
                    cur.assign(reinterpret_cast<const char*>(sqlite3_column_text(q, 1)), sqlite3_column_bytes(q, 1));
                sqlite3_reset(q);
                if (cur != tag.url) {
                    sqlite3_stmt* u = c->get(S_RL_URL);
                    sqlite3_bind_int64(u, 1, l->sid);
                    sqlite3_bind_int64(u, 2, r.seq);
                    sqlite3_bind_int64(u, 3, l->id);
                    if (tag.url != lc.url0) sqlite3_bind_text(u, 4, tag.url.data(), int(tag.url.size()), SQLITE_STATIC);
                    else sqlite3_bind_null(u, 4);
                    bad(sqlite3_step(u));
                    sqlite3_reset(u);
                    r.urlChanged = true;
                }
            }
            lc.url = tag.url;
            if (migrateTimes) {
                if (at > lc.updated) lc.updated = at;
                if (at < lc.created || lc.created == 0) lc.created = at;
            } else if (now > lc.updated) {
                lc.updated = now;
            }
        }
    }
    // CAT supersede (and batch deletes): rows retired in this transaction.
    for (Del& d : dels) {
        if (rc != SQLITE_OK) break;
        sqlite3_stmt* s = c->get(S_RL_DEL_SEQ);
        sqlite3_bind_int64(s, 1, d.seq);
        bad(sqlite3_step(s));
        sqlite3_reset(s);
        s = c->get(S_R_DEL);
        sqlite3_bind_int64(s, 1, d.seq);
        bad(sqlite3_step(s));
        const bool gone = sqlite3_changes(c->db) > 0;
        sqlite3_reset(s);
        if (!gone) continue;
        bad(dv.removed(c, d.k, d.hasE, d.e));
        o.n--;
        o.bytes -= d.len;
        for (auto& sl : d.lanes) {
            LaneCount& lc = laneCount(sl.second);
            lc.n--;
            lc.bytes -= d.len;
            sids.insert(sl.first);
        }
    }
    // The file's lane, source and meta rows, in the same transaction.
    for (auto& kv : o.lanes) {
        if (rc != SQLITE_OK) break;
        LaneDef* l;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            l = t_->laneById(kv.first);
        }
        if (kv.second.n <= 0) {
            sqlite3_stmt* s = c->get(S_LANE_DEL);
            sqlite3_bind_int64(s, 1, kv.first);
            bad(sqlite3_step(s));
            sqlite3_reset(s);
            continue;
        }
        const LaneCount& lc = kv.second;
        sqlite3_stmt* s = c->get(S_LANE_UP);
        sqlite3_bind_int64(s, 1, kv.first);
        sqlite3_bind_int64(s, 2, l->sid);
        sqlite3_bind_text(s, 3, l->batch.data(), int(l->batch.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 4, l->ckey.data(), int(l->ckey.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 5, l->ppeer.data(), int(l->ppeer.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 6, l->pkey.data(), int(l->pkey.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 7, lc.url.data(), int(lc.url.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 8, lc.url0.data(), int(lc.url0.size()), SQLITE_STATIC);
        sqlite3_bind_int64(s, 9, lc.created);
        sqlite3_bind_int64(s, 10, lc.updated);
        sqlite3_bind_int64(s, 11, lc.maxat);
        sqlite3_bind_int64(s, 12, lc.n);
        sqlite3_bind_int64(s, 13, lc.bytes);
        if (lc.minw != INT64_MAX) sqlite3_bind_int64(s, 14, lc.minw); else sqlite3_bind_null(s, 14);
        if (lc.maxw != INT64_MIN) sqlite3_bind_int64(s, 15, lc.maxw); else sqlite3_bind_null(s, 15);
        sqlite3_bind_int64(s, 16, lc.maxseq);
        sqlite3_bind_int64(s, 17, lc.maxts);
        if (lc.minseq != INT64_MAX) sqlite3_bind_int64(s, 18, lc.minseq); else sqlite3_bind_null(s, 18);
        bad(sqlite3_step(s));
        sqlite3_reset(s);
    }
    for (uint32_t sid : sids) {
        if (rc != SQLITE_OK) break;
        SrcDef* d;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            d = t_->srcById(sid);
        }
        sqlite3_stmt* s = c->get(S_SRC_INS);
        sqlite3_bind_int64(s, 1, sid);
        sqlite3_bind_text(s, 2, d->provider.data(), int(d->provider.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 3, d->source.data(), int(d->source.size()), SQLITE_STATIC);
        bad(sqlite3_step(s));
        sqlite3_reset(s);
    }
    if (rc == SQLITE_OK) rc = dv.save(c);
    if (rc == SQLITE_OK) rc = writeMeta(c, o, now);
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) {
        lastErr_ = std::string(sqlite3_errmsg(c->db)) + " (" + std::to_string(rc) + ") " + f->path;
        c->exec("ROLLBACK");
        writerUnpin(e_, f);
        if ((rc & 0xff) == SQLITE_CORRUPT || (rc & 0xff) == SQLITE_NOTADB) {
            std::lock_guard<std::mutex> g(t_->mu);
            f->quarantined = true;
        }
        return statusOfSqlite(rc);
    }
    int64_t freeBytes = 0;
    const int64_t dbBytes = dbBytesOf(c, &freeBytes);
    writerUnpin(e_, f);
    e_->bump(kStGroupCommits);
    // Publish this file's counters.
    std::lock_guard<std::mutex> g(t_->mu);
    if (dbBytes >= 0) {
        f->dbBytes = dbBytes;
        f->freeBytes = freeBytes;
    }
    countersTo(f, o);
    for (auto& kv : o.lanes) {
        if (kv.second.n <= 0) f->lanes.erase(kv.first);
        else f->lanes[kv.first] = kv.second;
    }
    if (!f->created) e_->rpool.addFiles(1);
    f->created = true;
    f->touched = true;
    return P4_OK;
}

void Group::fail(int32_t status, const std::string& err) {
    for (auto& c : calls_)
        if (c.status == P4_OK) {
            c.status = status;
            c.err = err;
        }
}

void Group::publish(bool fileCommitted) {
    std::lock_guard<std::mutex> g(t_->mu);
    for (Rec& r : recs_) {
        if (r.dupOf || !r.write) continue;
        const bool committed = fileCommitted && r.inFile && !r.reject && !r.superseded;
        if (r.reject && !r.inFile) continue;
        if (committed) {
            t_->pend.put(r.key, p_->pid, r.seq, 1);
            if (r.action == P4_ACT_NEW || r.action == P4_ACT_MIGRATED) {
                t_->uniq++;
                t_->uniqBytes += r.dLen;
            } else {
                t_->copies++;
            }
            if (r.identNew) {
                auto it = t_->identPend.find(identMapKey(r.identSrc, r.ident));
                if (it != t_->identPend.end()) it->second.st = 1;
            }
        } else {
            t_->pend.kill(r.key, p_->pid);
            if (r.identNew) t_->identPend.erase(identMapKey(r.identSrc, r.ident));
        }
    }
    if (fileCommitted)
        for (Del& d : dels_) {
            t_->pend.kill(d.key, p_->pid);
            t_->pend.put(d.key, p_->pid, d.seq, 2);
            if (d.others) t_->copies--;
            else {
                t_->uniq--;
                t_->uniqBytes -= d.len;
            }
        }
    for (auto& r : inflight_) {
        for (size_t i = 0; i < t_->inflight.size(); i++)
            if (t_->inflight[i] == r) {
                t_->inflight.erase(t_->inflight.begin() + long(i));
                break;
            }
    }
    t_->visRecompute();
    if (jfirst_) {
        for (size_t i = 0; i < t_->jinflight.size(); i++)
            if (t_->jinflight[i] == jfirst_) {
                t_->jinflight.erase(t_->jinflight.begin() + long(i));
                break;
            }
    }
}

void Group::respond() {
    for (Call& c : calls_) {
        SlotOut out(e_, c.slot);
        out.enc.header({"i", "action", "seq", "reject"});
        if (c.status != P4_OK) {
            out.end(c.status, c.err);
            continue;
        }
        uint64_t i = 0;
        uint64_t nNew = 0, nCopy = 0, nRetag = 0, nDup = 0, nIdent = 0, nRej = 0;
        for (size_t gi : c.recs) {
            Rec& r = recs_[gi];
            int action = r.reject ? P4_ACT_REJECTED : r.action;
            if (action == P4_ACT_DUP && r.retagged) action = P4_ACT_RETAG;
            switch (action) {
                case P4_ACT_NEW: case P4_ACT_MIGRATED: nNew++; break;
                case P4_ACT_COPY: nCopy++; break;
                case P4_ACT_RETAG: nRetag++; break;
                case P4_ACT_DUP: nDup++; break;
                case P4_ACT_IDENT_DUP: nIdent++; break;
                default: nRej++; break;
            }
            out.enc.beginRow();
            out.enc.i64(int64_t(i++));
            out.enc.i64(action);
            if (r.reject) out.enc.i64(0); else out.enc.i64(r.seq);
            out.enc.i64(r.reject);
            out.enc.endRow();
            out.rows++;
            if (out.enc.blockBytes() == 0 && out.buf.size() >= (64u << 10)) out.flush();
        }
        e_->bump(kStPuts);
        e_->bump(kStPutRecords, c.recs.size());
        e_->bump(kStNew, nNew);
        e_->bump(kStCopies, nCopy);
        e_->bump(kStRetags, nRetag);
        e_->bump(kStDups, nDup);
        e_->bump(kStIdentDups, nIdent);
        e_->bump(kStRejects, nRej);
        out.end(P4_OK, std::string());
    }
}

void Group::run(std::vector<WriteTask*>& tasks) {
    sp_ = t_->spec();
    for (WriteTask* wt : tasks) {
        Call c;
        c.task = wt;
        c.slot = wt->slot;
        calls_.push_back(std::move(c));
    }
    for (Call& c : calls_) {
        if (!parseCall(c)) continue;
        c.token = ps::producerToken(reinterpret_cast<const uint8_t*>(c.peer.data()), c.peer.size());
        if (c.token != p_->producer) {
            c.status = P4_E_ARG;
            c.err = "peer does not map to this partition";
        }
    }
    // Lanes of every call (type-wide ids; new ones are journaled with the group).
    {
        std::lock_guard<std::mutex> g(t_->mu);
        partNew_ = !p_->journaled;
        for (Call& c : calls_) {
            if (c.status != P4_OK) continue;
            for (TagIn& tag : c.tags) {
                if (!tag.valid) continue;
                const size_t ns = t_->srcs.size();
                tag.lane = laneFor(t_, tag.f6, true);
                if (!t_->laneJournaled[tag.lane->id - 1] &&
                    std::find(newLanes_.begin(), newLanes_.end(), tag.lane->id) == newLanes_.end())
                    newLanes_.push_back(tag.lane->id);
                if (!t_->srcJournaled[tag.lane->sid - 1] &&
                    std::find(newSrcs_.begin(), newSrcs_.end(), tag.lane->sid) == newSrcs_.end())
                    newSrcs_.push_back(tag.lane->sid);
                (void)ns;
            }
        }
    }
    for (Call& c : calls_) {
        if (c.status != P4_OK) continue;
        uint32_t scope = 0;
        for (const TagIn& tag : c.tags)
            if (tag.valid && tag.lane && !scope) scope = tag.lane->sid;
        for (size_t gi : c.recs) {
            Rec& r = recs_[gi];
            prepare(r, c);
            if (c.mode == 1) {
                uint32_t sc = 0;
                bool mixed = false;
                for (auto& ti : r.tagsIn) {
                    if (ti.first >= c.tags.size() || !c.tags[ti.first].lane) continue;
                    const uint32_t sid = c.tags[ti.first].lane->sid;
                    if (sc && sc != sid) mixed = true;
                    sc = sid;
                }
                r.supSid = mixed ? 0 : sc;
            } else {
                r.supSid = scope;
            }
        }
    }
    {
        bool quarantined;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            quarantined = p_->quarantined;
        }
        if (quarantined) fail(P4_E_CORRUPT, "partition file quarantined: " + p_->path);
    }
    bool any = false;
    for (Call& c : calls_) any = any || (c.status == P4_OK && !c.recs.empty());
    if (!any) {
        respond();
        return;
    }
    // The type's T/ files exist from its first write (C-32).
    {
        std::string err;
        const int32_t frc = typeFilesEnsure(t_, &err);
        if (frc != P4_OK) {
            fail(frc, "type files: " + err);
            respond();
            return;
        }
    }
    int32_t rc = probe();
    if (rc != P4_OK) {
        fail(rc, "dedupe probe failed");
        respond();
        return;
    }
    Part* f = p_;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (Rec& r : recs_) {
            if (r.reject || r.dupOf || !r.action) continue;
            if (!r.write && r.inst.empty()) continue;
            r.inFile = true;
            writes_ = true;
        }
        if (calls_[0].mode == 1 && !f->created) f->indexed = false;
    }
    // Repeats inside the group that add tags write with their first record.
    for (Rec& r : recs_)
        if (r.dupOf && !r.inst.empty()) r.inFile = recs_[r.first].inFile;
    // CAT supersede-on-ingest: this partition's rows of the same (source,
    // object identity), retired in the same transaction (record_supersede.go).
    if (sp_->hasSupersede && calls_[0].mode == 0) {
        std::unordered_map<std::string, size_t> lastOf;
        for (size_t i = 0; i < recs_.size(); i++) {
            Rec& r = recs_[i];
            if (r.reject || r.dupOf || !r.write || !r.hasSup) continue;
            const std::string key = std::to_string(r.supSid) + '\x1f' + r.supIdentity;
            auto it = lastOf.find(key);
            if (it != lastOf.end()) recs_[it->second].superseded = true;
            lastOf[key] = i;
        }
        bool created;
        {
            std::lock_guard<std::mutex> g(t_->mu);
            created = f->created;
        }
        Conn* c = nullptr;
        for (Rec& r : recs_) {
            if (r.reject || r.dupOf || !r.write || !r.hasSup || r.superseded || !r.kType || !created) continue;
            if (!c) {
                int32_t prc = P4_OK;
                c = writerPin(e_, f, &prc, nullptr);
                if (!c) {
                    fail(prc, "writer open failed");
                    break;
                }
            }
            sqlite3_stmt* s = c->get(S_SUP_K);
            bindK(s, 1, r);
            sqlite3_bind_int64(s, 2, 0);
            std::vector<Del> cand;
            while (sqlite3_step(s) == SQLITE_ROW) {
                const bool scoped = sqlite3_column_type(s, 3) != SQLITE_NULL;
                const uint32_t ssid = scoped ? uint32_t(sqlite3_column_int64(s, 3)) : 0;
                if (scoped && ssid != r.supSid) continue;
                if (sqlite3_column_bytes(s, 1) == 32 && std::memcmp(sqlite3_column_blob(s, 1), r.key, 32) == 0) continue;
                const std::string id = identityOf(sp_->tc, static_cast<const uint8_t*>(sqlite3_column_blob(s, 4)),
                                                  size_t(sqlite3_column_bytes(s, 4)));
                if (id != r.supIdentity) continue;
                Del d;
                d.seq = sqlite3_column_int64(s, 0);
                d.len = sqlite3_column_int64(s, 2);
                d.w = sqlite3_column_int64(s, 5);
                d.hasE = sqlite3_column_type(s, 6) != SQLITE_NULL;
                d.e = sqlite3_column_int64(s, 6);
                d.k.type = r.kType;
                d.k.i = r.kInt;
                d.k.s = r.kText;
                std::memcpy(d.key, sqlite3_column_blob(s, 1), 32);
                cand.push_back(std::move(d));
            }
            sqlite3_reset(s);
            for (Del& d : cand) {
                bool seen = false;
                for (Del& x : dels_) seen = seen || x.seq == d.seq;
                if (seen) continue;
                sqlite3_stmt* q = c->get(S_RL_OF);
                sqlite3_bind_int64(q, 1, d.seq);
                while (sqlite3_step(q) == SQLITE_ROW)
                    d.lanes.push_back({uint32_t(sqlite3_column_int64(q, 0)), uint32_t(sqlite3_column_int64(q, 1))});
                sqlite3_reset(q);
                dels_.push_back(std::move(d));
            }
        }
        if (c) writerUnpin(e_, f);
        if (!dels_.empty()) writes_ = true;
        e_->bump(kStCatSuperseded, dels_.size());
    }
    bool ok = true;
    for (Call& c : calls_) ok = ok && c.status == P4_OK;
    if (!ok && std::all_of(calls_.begin(), calls_.end(), [](const Call& c) { return c.status != P4_OK; })) {
        respond();
        return;
    }
    rc = assign();
    if (rc != P4_OK) {
        fail(rc, "seq assignment failed");
        publish(false);
        respond();
        return;
    }
    rc = writeJournal();
    if (rc != P4_OK) {
        fail(rc, "journal commit failed: " + lastErr_);
        publish(false);
        respond();
        return;
    }
    bool committed = false;
    if (writes_) {
        std::vector<size_t> idxs;
        for (size_t i = 0; i < recs_.size(); i++)
            if (recs_[i].inFile && !recs_[i].reject) idxs.push_back(i);
        rc = writeFile(idxs);
        committed = rc == P4_OK;
        if (!committed)
            for (Call& c : calls_)
                for (size_t gi : c.recs)
                    if (recs_[gi].inFile && c.status == P4_OK) {
                        c.status = rc;
                        c.err = "partition file commit failed: " + lastErr_;
                    }
    }
    {
        // A retired row's last-copy decision is atomic with the publish (dmu).
        std::lock_guard<std::mutex> dg(t_->dmu);
        if (committed)
            for (Del& d : dels_) d.others = othersHolding(&L_, t_, d.key, p_->pid);
        publish(committed);
    }
    respond();
    bool kick;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        kick = t_->pend.live() >= e_->cfg.flushEntries || t_->pend.bytes() >= e_->cfg.pendingBytes / 4;
    }
    if (kick) e_->kickMaintenance();
}

}  // namespace

void putGroup(Engine* e, uint32_t writer, Part* p, std::vector<WriteTask*>& tasks) {
    Group g(e, writer, p);
    g.run(tasks);
}

}  // namespace p4
}  // namespace flatsql
