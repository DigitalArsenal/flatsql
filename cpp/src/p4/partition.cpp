// Store format 4: feed files and the PUT path (CONTRACT C-37, C-38, §3.7).
//
// A feed file (one source feed x standard) is the feed's table:
//   r(rid PK, seq, n, b, c, u, at, cid, e, k, ts, f, x, d, w = coalesce(e, ts))
//     rid = seq << 16 | j: a record's rows are one rowid range (arrival order);
//     n the publishing node (node: producer token, peer), b the batch (batch:
//     batch, producer peer, producer key; 0 on local rows), c the content key
//     and u the url (small ids, 0 = ""), at when this feed delivered it;
//     d the record bytes verbatim (sealed bytes when sealed), x the
//     signature, f a sealed record's extracted COL values.
//   indexes: r_s(seq) arrival, r_c(cid) the file's one CID index, r_ke(k, e)
//     object + epoch, r_w(w DESC) epoch, r_b(b) batch
//   inst(b, c, ...) each instance's counters; tokc(producer, ...) each
//   token's copies; ident(h, seq) ingest identities (IQC); meta: the file's
//   counters
// The file is the feed: its provider and source are in its meta, never in a row.
//
// A PUT group (one or more queued PUT calls of a type, at most group-commit
// records) runs on the type's writer: checks and extraction; each record
// delivered to each feed file its tags name (local without a tag), found
// there by its CID (ingest: a record new to a feed gets its own seq, C-38
// (3)) or by format 1's rowid (migrate: the same seq in every feed holding
// it); seqs assigned for new records in content-time order; then the
// journal, the feed files and the type index commit (record.cpp), and every
// call is acked after its records are durable and visible (C-4).
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

// ---- feed files ------------------------------------------------------------------------------
namespace {
const char* kFileSchema =
    "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS node(id INTEGER PRIMARY KEY, producer TEXT NOT NULL, peer TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS batch(id INTEGER PRIMARY KEY, batch TEXT NOT NULL, ppeer TEXT NOT NULL, pkey TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS ckey(id INTEGER PRIMARY KEY, ckey TEXT NOT NULL UNIQUE);"
    "CREATE TABLE IF NOT EXISTS url(id INTEGER PRIMARY KEY, url TEXT NOT NULL UNIQUE);"
    "CREATE TABLE IF NOT EXISTS inst(b INTEGER NOT NULL, c INTEGER NOT NULL, n INTEGER NOT NULL, bytes INTEGER NOT NULL,"
    " minw, maxw, minseq, maxseq, first, updated, maxat, maxts, url TEXT, PRIMARY KEY(b, c)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS tokc(producer TEXT PRIMARY KEY, n INTEGER NOT NULL, bytes INTEGER NOT NULL, mints, maxts,"
    " maxseq) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS ident(h BLOB PRIMARY KEY, seq INTEGER NOT NULL) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS r(rid INTEGER PRIMARY KEY, seq INTEGER NOT NULL, n INTEGER NOT NULL, b INTEGER NOT NULL,"
    " c INTEGER NOT NULL, u INTEGER NOT NULL, at INTEGER NOT NULL, cid BLOB NOT NULL, e INTEGER, k, ts INTEGER NOT NULL,"
    " f BLOB, x BLOB, d BLOB NOT NULL, w INTEGER GENERATED ALWAYS AS (coalesce(e, ts)) VIRTUAL);";
}  // namespace

// The file's indexes (C-37 (4), C-38 (1)): arrival seq (r_s: the seqs alone,
// for newest-N cuts, merges by seq and oldest-first quota), the one CID index
// (r_c: re-fetch dedupe in the feed and by-CID lookups), object + epoch
// (r_ke: one seek per object for EPOCH nearest / as_of / forward, object
// predicates, CAT supersede), epoch windows (r_w on w = coalesce(e, ts); ties
// are put in CID order from the rows), and the batch (r_b: batch supersede
// and batch-filtered reads).
int32_t fileCreateIndexes(Type* t, Conn* c) {
    std::shared_ptr<const Spec> sp = t->spec();
    std::string ddl =
        "BEGIN IMMEDIATE;"
        "CREATE INDEX IF NOT EXISTS r_s ON r(seq);"
        "CREATE INDEX IF NOT EXISTS r_c ON r(cid);"
        "CREATE INDEX IF NOT EXISTS r_w ON r(w DESC);"
        "CREATE INDEX IF NOT EXISTS r_b ON r(b);";
    if (sp->hasObject) ddl += "CREATE INDEX IF NOT EXISTS r_ke ON r(k, e);";
    ddl += "INSERT OR REPLACE INTO meta(k, v) VALUES('ix', 1);";
    int rc = c->exec(ddl.c_str());
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) c->exec("ROLLBACK");
    return rc == SQLITE_OK ? P4_OK : statusOfSqlite(rc);
}

int32_t fileSchema(Type* t, Conn* c, Feed* f, bool indexes) {
    std::string ddl = std::string("BEGIN IMMEDIATE;") + kFileSchema;
    int rc = c->exec(ddl.c_str());
    if (rc != SQLITE_OK) {
        c->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    auto meta = [&](const char* k, const std::string& v) {
        sqlite3_stmt* s = c->get(S_META_SET);
        if (!s) return false;
        sqlite3_bind_text(s, 1, k, -1, SQLITE_STATIC);
        sqlite3_bind_text(s, 2, v.data(), int(v.size()), SQLITE_TRANSIENT);
        const int r = sqlite3_step(s);
        sqlite3_reset(s);
        return r == SQLITE_DONE;
    };
    bool ok = meta("format", "4") && meta("layout", "feed") && meta("type", t->name) && meta("provider", f->provider) &&
              meta("source", f->source) && meta("fid", std::to_string(f->fid));
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

int writeMeta(Conn* c, const Counters& k, int64_t now) {
    const bool any = k.rows > 0;
    const struct {
        const char* k;
        bool set;
        int64_t v;
    } kv[] = {{"rows", true, k.rows},
              {"recs", true, k.recs},
              {"bytes", true, k.bytes},
              {"nnull", true, k.nnull},
              {"rbytes", true, k.rbytes},
              {"copies", true, k.copies},
              {"cbytes", true, k.cbytes},
              {"minseq", any && k.minseq != INT64_MAX, k.minseq},
              {"maxseq", true, k.maxseq},
              {"minw", any && k.minw != INT64_MAX, k.minw},
              {"maxw", any && k.maxw != INT64_MIN, k.maxw},
              {"mints", any && k.mints != INT64_MAX, k.mints},
              {"maxts", true, k.maxts},
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
        if (!key) continue;
        const std::string n(key);
        const int64_t v = sqlite3_column_int64(s, 1);
        if (n == "rows") k->rows = v;
        else if (n == "recs") k->recs = v;
        else if (n == "bytes") k->bytes = v;
        else if (n == "nnull") k->nnull = v;
        else if (n == "rbytes") k->rbytes = v;
        else if (n == "copies") k->copies = v;
        else if (n == "cbytes") k->cbytes = v;
        else if (n == "minseq") k->minseq = v;
        else if (n == "maxseq") k->maxseq = v;
        else if (n == "minw") k->minw = v;
        else if (n == "maxw") k->maxw = v;
        else if (n == "mints") k->mints = v;
        else if (n == "maxts") k->maxts = v;
        else if (n == "mine") k->mine = v;
        else if (n == "maxe") k->maxe = v;
        else if (n == "ix") *indexed = v != 0;
    }
    sqlite3_reset(s);
    return r == SQLITE_DONE ? SQLITE_OK : r;
}

int readInst(Feed* f, Conn* c, std::map<InstId, InstCount>* out) {
    out->clear();
    if (!dictEnsure(f, c)) return SQLITE_ERROR;
    sqlite3_stmt* s = c->sql("SELECT b, c, n, bytes, minw, maxw, minseq, maxseq, first, updated, maxat, maxts, url FROM inst");
    if (!s) return SQLITE_ERROR;
    int r;
    std::vector<std::pair<InstId, InstCount>> got;
    while ((r = sqlite3_step(s)) == SQLITE_ROW) {
        InstCount ic;
        const InstId id(uint32_t(sqlite3_column_int64(s, 0)), uint32_t(sqlite3_column_int64(s, 1)));
        ic.n = sqlite3_column_int64(s, 2);
        ic.bytes = sqlite3_column_int64(s, 3);
        ic.minw = sqlite3_column_type(s, 4) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 4);
        ic.maxw = sqlite3_column_type(s, 5) == SQLITE_NULL ? INT64_MIN : sqlite3_column_int64(s, 5);
        ic.minseq = sqlite3_column_type(s, 6) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 6);
        ic.maxseq = sqlite3_column_int64(s, 7);
        ic.first = sqlite3_column_int64(s, 8);
        ic.updated = sqlite3_column_int64(s, 9);
        ic.maxat = sqlite3_column_int64(s, 10);
        ic.maxts = sqlite3_column_int64(s, 11);
        const unsigned char* u = sqlite3_column_text(s, 12);
        if (u) ic.url = reinterpret_cast<const char*>(u);
        if (ic.n > 0) got.push_back({id, std::move(ic)});
    }
    sqlite3_reset(s);
    if (r != SQLITE_DONE) return r;
    for (auto& kv : got) {
        BatchDef bd;
        if (!dictBatch(f, c, kv.first.first, &bd)) return SQLITE_CORRUPT;
        kv.second.batch = bd.batch;
        kv.second.ppeer = bd.ppeer;
        kv.second.pkey = bd.pkey;
        kv.second.ckey = dictCkey(c, kv.first.second);
        (*out)[kv.first] = std::move(kv.second);
    }
    return SQLITE_OK;
}

int readTokc(Type* t, Conn* c, std::map<uint32_t, TokCount>* out) {
    out->clear();
    sqlite3_stmt* s = c->sql("SELECT producer, n, bytes, mints, maxts, maxseq FROM tokc WHERE n>0");
    if (!s) return SQLITE_ERROR;
    int r;
    std::vector<std::pair<std::string, TokCount>> got;
    while ((r = sqlite3_step(s)) == SQLITE_ROW) {
        TokCount tc;
        const unsigned char* p = sqlite3_column_text(s, 0);
        tc.n = sqlite3_column_int64(s, 1);
        tc.bytes = sqlite3_column_int64(s, 2);
        tc.mints = sqlite3_column_type(s, 3) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 3);
        tc.maxts = sqlite3_column_int64(s, 4);
        tc.maxseq = sqlite3_column_int64(s, 5);
        got.push_back({p ? reinterpret_cast<const char*>(p) : "", tc});
    }
    sqlite3_reset(s);
    if (r != SQLITE_DONE) return r;
    std::lock_guard<std::mutex> g(t->mu);
    for (auto& kv : got) (*out)[tokFor(t, kv.first, std::string(), true)] = kv.second;
    return SQLITE_OK;
}

// The token rows that changed, in the caller's write transaction.
int writeTokc(Type* t, Conn* c, const std::map<uint32_t, TokCount>& before, const std::map<uint32_t, TokCount>& after) {
    std::set<uint32_t> ids;
    for (auto& kv : before) ids.insert(kv.first);
    for (auto& kv : after) ids.insert(kv.first);
    for (uint32_t id : ids) {
        auto a = after.find(id), b = before.find(id);
        const bool inA = a != after.end() && a->second.n > 0;
        if (inA && b != before.end() && b->second.n == a->second.n && b->second.bytes == a->second.bytes &&
            b->second.mints == a->second.mints && b->second.maxts == a->second.maxts && b->second.maxseq == a->second.maxseq)
            continue;
        if (!inA && b == before.end()) continue;
        std::string producer;
        {
            std::lock_guard<std::mutex> g(t->mu);
            TokDef* d = t->tokById(id);
            if (!d) continue;
            producer = d->token;
        }
        sqlite3_stmt* q = c->get(inA ? S_TOKC_PUT : S_TOKC_DEL);
        if (!q) return SQLITE_ERROR;
        sqlite3_bind_text(q, 1, producer.data(), int(producer.size()), SQLITE_STATIC);
        if (inA) {
            const TokCount& tc = a->second;
            sqlite3_bind_int64(q, 2, tc.n);
            sqlite3_bind_int64(q, 3, tc.bytes);
            if (tc.mints == INT64_MAX) sqlite3_bind_null(q, 4);
            else sqlite3_bind_int64(q, 4, tc.mints);
            sqlite3_bind_int64(q, 5, tc.maxts);
            sqlite3_bind_int64(q, 6, tc.maxseq);
        }
        const int r = sqlite3_step(q);
        sqlite3_reset(q);
        if (r != SQLITE_DONE) return r;
    }
    return SQLITE_OK;
}

int32_t seqOfCid(Conn* c, bool indexed, const uint8_t key[32], int64_t* seq) {
    *seq = 0;
    sqlite3_stmt* q = c->get(indexed ? S_R_CID : S_R_CIDSCAN);
    if (!q) return P4_E_INTERNAL;
    sqlite3_bind_blob(q, 1, key, 32, SQLITE_STATIC);
    const int r = sqlite3_step(q);
    if (r == SQLITE_ROW) *seq = sqlite3_column_int64(q, 0);
    sqlite3_reset(q);
    return r == SQLITE_ROW || r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}

int32_t fileHoldsSeq(Conn* c, int64_t seq, bool* held) {
    *held = false;
    sqlite3_stmt* q = c->get(S_R_MAXRID);
    if (!q) return P4_E_INTERNAL;
    sqlite3_bind_int64(q, 1, seq << 16);
    sqlite3_bind_int64(q, 2, (seq << 16) | 0xffff);
    const int r = sqlite3_step(q);
    if (r == SQLITE_ROW) *held = sqlite3_column_type(q, 0) != SQLITE_NULL;
    sqlite3_reset(q);
    return r == SQLITE_ROW || r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}

// ---- the dictionary -------------------------------------------------------------------------------
namespace {
// Loads the node and batch tables through c (ids only grow; a reload adds
// what a newer snapshot has). dictMu held.
bool dictLoad(Feed* f, Conn* c) {
    Dict& d = f->dict;
    sqlite3_stmt* q = c->sql("SELECT id, producer, peer FROM node WHERE id>?1 ORDER BY id");
    if (!q) return false;
    sqlite3_bind_int64(q, 1, int64_t(d.node.size()));
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        const uint32_t id = uint32_t(sqlite3_column_int64(q, 0));
        if (id != d.node.size() + 1) break;  // ids are dense from 1
        NodeDef n{reinterpret_cast<const char*>(sqlite3_column_text(q, 1)), reinterpret_cast<const char*>(sqlite3_column_text(q, 2))};
        d.nodeId[nodeKey(n.producer, n.peer)] = id;
        d.node.push_back(std::move(n));
    }
    sqlite3_reset(q);
    if (r != SQLITE_DONE && r != SQLITE_ROW) return false;
    q = c->sql("SELECT id, batch, ppeer, pkey FROM batch WHERE id>?1 ORDER BY id");
    if (!q) return false;
    sqlite3_bind_int64(q, 1, int64_t(d.batch.size()));
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        const uint32_t id = uint32_t(sqlite3_column_int64(q, 0));
        if (id != d.batch.size() + 1) break;
        BatchDef b{reinterpret_cast<const char*>(sqlite3_column_text(q, 1)), reinterpret_cast<const char*>(sqlite3_column_text(q, 2)),
                   reinterpret_cast<const char*>(sqlite3_column_text(q, 3))};
        d.batchId[batchKey(b.batch, b.ppeer, b.pkey)] = id;
        d.batch.push_back(std::move(b));
    }
    sqlite3_reset(q);
    if (r != SQLITE_DONE && r != SQLITE_ROW) return false;
    d.loaded = true;
    return true;
}
}  // namespace

bool dictEnsure(Feed* f, Conn* c) {
    std::lock_guard<std::mutex> g(f->dictMu);
    if (f->dict.loaded) return true;
    return dictLoad(f, c);
}

bool dictNode(Feed* f, Conn* c, uint32_t id, NodeDef* out) {
    std::lock_guard<std::mutex> g(f->dictMu);
    if (id == 0) return false;
    if (id > f->dict.node.size() && !dictLoad(f, c)) return false;
    if (id > f->dict.node.size()) return false;
    *out = f->dict.node[id - 1];
    return true;
}

bool dictBatch(Feed* f, Conn* c, uint32_t id, BatchDef* out) {
    std::lock_guard<std::mutex> g(f->dictMu);
    if (id == 0) return false;
    if (id > f->dict.batch.size() && !dictLoad(f, c)) return false;
    if (id > f->dict.batch.size()) return false;
    *out = f->dict.batch[id - 1];
    return true;
}

namespace {
std::string textOf(Conn* c, StmtId id, uint32_t v) {
    if (v == 0) return std::string();
    sqlite3_stmt* q = c->get(id);
    if (!q) return std::string();
    sqlite3_bind_int64(q, 1, v);
    std::string out;
    if (sqlite3_step(q) == SQLITE_ROW) {
        const unsigned char* t = sqlite3_column_text(q, 0);
        if (t) out.assign(reinterpret_cast<const char*>(t), size_t(sqlite3_column_bytes(q, 0)));
    }
    sqlite3_reset(q);
    return out;
}
}  // namespace

std::string dictCkey(Conn* c, uint32_t id) { return textOf(c, S_CKEY_TEXT, id); }
std::string dictUrl(Conn* c, uint32_t id) { return textOf(c, S_URL_TEXT, id); }

// ---- writer connections (<= writer conns open; the LRU never closes a pinned one) ----------------
Conn* writerPin(Engine* e, Feed* f, int32_t* rc, std::string* err) {
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
    bool created, indexed;
    {
        std::lock_guard<std::mutex> g(t->mu);
        created = f->created;
        indexed = f->indexed;
    }
    if (!created) {
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
    if (!created) {
        const int32_t s = fileSchema(t, c, f, indexed);
        if (s != P4_OK) {
            delete c;
            *rc = s;
            if (err) *err = "schema " + f->path;
            return nullptr;
        }
        std::lock_guard<std::mutex> g(t->mu);
        f->created = true;
    }
    std::vector<Conn*> victims;
    {
        std::lock_guard<std::mutex> g(e->wconnMu);
        while (e->nWConn >= e->cfg.writerConns && !e->wlru.empty()) {
            auto it = e->wlru.end();
            Feed* v = nullptr;
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
        {
            std::lock_guard<std::mutex> g(e->maintMu);
            for (Conn* v : victims) {
                MaintTask mt;
                mt.kind = MaintTask::kClose;
                mt.conn = v;
                e->maintQ.push_back(mt);
            }
        }
        e->kickMaintenance();
    }
    return c;
}

void writerUnpin(Engine* e, Feed* f) {
    std::lock_guard<std::mutex> g(e->wconnMu);
    if (f->wPins > 0) f->wPins--;
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

// ---- PUT -------------------------------------------------------------------------------------------
namespace {

constexpr uint16_t kFSealed = 1, kFIdent = 2, kFSeq = 4, kFPeer = 8, kFTags = 16;

struct TagIn {
    std::string f6[6];  // provider, source, batch, ckey, ppeer, pkey
    std::string url;
    bool valid = true;
    uint32_t fid = 0;
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
    KVal k;
    std::string fCols;  // sealed: the extracted COL values
    bool hasSup = false;
    std::string supIdentity;
    const uint8_t* d = nullptr;
    uint32_t dLen = 0;
    // outcome
    int action = 0;
    RecState* rs = nullptr;
};

struct Call {
    WriteTask* task = nullptr;
    uint32_t slot = 0;
    int mode = 0;
    std::string peer, token;
    uint32_t tok = 0;
    std::vector<TagIn> tags;
    int64_t at = 0;
    std::vector<size_t> recs;  // group indexes
    int32_t status = P4_OK;
    std::string err;
};

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
    Group(Engine* e, Type* t) : e_(e), t_(t) {}
    void run(std::vector<WriteTask*>& tasks);

private:
    bool parseCall(Call& c);
    void prepare(Rec& r, const Call& c);
    void fail(int32_t status, const std::string& err);
    void respond();
    int32_t supersedeOnIngest(WriteCtx& w, Rec& r, RecState* rs, uint32_t scope);
    int32_t ingest(WriteCtx& w, Call& c, Rec& r);
    int32_t migrate(WriteCtx& w, Call& c, Rec& r);
    int32_t identSeq(uint32_t fid, const uint8_t h[32], int64_t* seq);
    RowR copyOf(const Call& c, const Rec& r) const;

    Engine* e_;
    Type* t_;
    std::shared_ptr<const Spec> sp_;
    std::vector<Call> calls_;
    std::vector<Rec> recs_;
    std::vector<uint32_t> callOf_;  // record index -> its call
    uint32_t localFid_ = 0;
    std::map<std::pair<uint32_t, std::string>, RecState*> supOf_;    // (feed, identity) -> its record in this group
    std::map<std::pair<uint32_t, std::string>, RecState*> identOf_;  // (feed, ingest identity) -> its record in this group
    std::vector<RecState*> fresh_;  // records new to their feed file, in delivery order
    std::unordered_set<RecState*> freshSet_;
    std::unordered_map<int64_t, std::string> seqKey_;  // migrate: a seq's CID key in this group
    std::unordered_map<std::string, std::vector<RecState*>> byCid_;  // CID key -> its records this group delivered
    int32_t holders(WriteCtx& w, const Rec& r, bool feedsToo, std::vector<RecState*>* feeds, RecState** local);
    int32_t absorbLocal(WriteCtx& w, RecState* local, RecState* into);
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
        if (r.seqIn <= 0 || r.seqIn > (int64_t(1) << 46)) { r.reject = P4_REJ_SEQ; return; }
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
            r.k.type = 1;
            r.k.i = int64_t(cv.u);
        } else {
            r.k.type = 3;
            r.k.s.assign(reinterpret_cast<const char*>(cv.s), cv.n);
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
}

void Group::fail(int32_t status, const std::string& err) {
    for (auto& c : calls_)
        if (c.status == P4_OK) {
            c.status = status;
            c.err = err;
        }
}

// CAT supersede-on-ingest (record_supersede.go): the scope feed's records of
// the same object identity are retired from that feed in the same write.
int32_t Group::supersedeOnIngest(WriteCtx& w, Rec& r, RecState* rs, uint32_t scope) {
    const auto key = std::make_pair(scope, r.supIdentity);
    auto it = supOf_.find(key);
    if (it != supOf_.end() && it->second != rs) {
        w.dropAll(it->second);
        e_->bump(kStCatSuperseded);
    }
    supOf_[key] = rs;
    Feed* f;
    bool usable;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        f = t_->feedById(scope);
        usable = f && f->created && f->indexed && !f->quarantined;
    }
    if (!usable || rs->k.type == 0) return P4_OK;
    int32_t st = P4_OK;
    std::string er;
    Conn* c = writerPin(e_, f, &st, &er);
    if (!c) return st;
    std::vector<int64_t> seqs;
    sqlite3_stmt* q = c->sql("SELECT rid, seq FROM r INDEXED BY r_ke WHERE k=?1 AND seq<>?2");
    int rc = SQLITE_ERROR;
    std::vector<std::pair<int64_t, int64_t>> cand;
    if (q) {
        rs->k.bind(q, 1);
        sqlite3_bind_int64(q, 2, rs->seq);
        while ((rc = sqlite3_step(q)) == SQLITE_ROW) cand.push_back({sqlite3_column_int64(q, 0), sqlite3_column_int64(q, 1)});
        sqlite3_reset(q);
    }
    if (rc == SQLITE_DONE) {
        sqlite3_stmt* dq = c->get(S_R_D);
        int64_t last = 0;
        for (auto& x : cand) {
            if (x.second == last) continue;  // one row per record decides
            sqlite3_bind_int64(dq, 1, x.first);
            const int r2 = sqlite3_step(dq);
            std::string id;
            if (r2 == SQLITE_ROW)
                id = identityOf(sp_->tc, static_cast<const uint8_t*>(sqlite3_column_blob(dq, 0)), size_t(sqlite3_column_bytes(dq, 0)));
            sqlite3_reset(dq);
            if (r2 != SQLITE_ROW && r2 != SQLITE_DONE) {
                rc = r2;
                break;
            }
            last = x.second;
            if (id == r.supIdentity) seqs.push_back(x.second);
        }
    }
    writerUnpin(e_, f);
    if (rc != SQLITE_DONE) return statusOfSqlite(rc);
    for (int64_t s : seqs) {
        int32_t lrc = P4_OK;
        RecState* other = w.bySeq(scope, s, &lrc);
        if (!other) return lrc;
        if (other == rs) continue;
        w.dropAll(other);
        e_->bump(kStCatSuperseded);
    }
    return P4_OK;
}

// The holder of an ingest identity in a feed file (seq 0 when none).
int32_t Group::identSeq(uint32_t fid, const uint8_t h[32], int64_t* seq) {
    *seq = 0;
    Feed* f;
    bool created;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        f = t_->feedById(fid);
        created = f && f->created;
    }
    if (!created) return P4_OK;
    int32_t st = P4_OK;
    std::string er;
    Conn* c = writerPin(e_, f, &st, &er);
    if (!c) return st;
    sqlite3_stmt* q = c->get(S_IDENT_GET);
    int r = SQLITE_ERROR;
    if (q) {
        sqlite3_bind_blob(q, 1, h, 32, SQLITE_STATIC);
        r = sqlite3_step(q);
        if (r == SQLITE_ROW) *seq = sqlite3_column_int64(q, 0);
        sqlite3_reset(q);
    }
    writerUnpin(e_, f);
    return r == SQLITE_ROW || r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}

// The copy a record delivers: this write's producer token, peer and
// signature, with the record's own bytes.
RowR Group::copyOf(const Call& c, const Rec& r) const {
    RowR copy;
    copy.tok = c.tok;
    copy.peer = r.peer.empty() ? c.peer : r.peer;
    if (r.sigLen) copy.sig.assign(reinterpret_cast<const char*>(r.sig), r.sigLen);
    copy.hasD = true;
    copy.d.assign(reinterpret_cast<const char*>(r.d), r.dLen);
    copy.len = r.dLen;
    copy.sealed = r.sealed != nullptr;
    copy.fcols = r.fCols;
    return copy;
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
            const int action = r.reject ? P4_ACT_REJECTED : r.action;
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
            out.enc.i64(r.reject || !r.rs ? 0 : r.rs->seq);
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

bool liveRows(const RecState* r) {
    for (const RowR& x : r->rows)
        if (x.live()) return true;
    return false;
}

void setRecord(RecState* rs, const Rec& r) {
    rs->hasE = r.hasE;
    rs->e = r.e;
    rs->k = r.k;
    rs->ts = r.ts;
    rs->w = r.w;
}

// Ingest mode: the record goes to each feed its call's tags name (local
// without a tag), found there by its CID (C-38 (3): a record new to a feed is
// a new row set with its own seq).
int32_t Group::ingest(WriteCtx& w, Call& c, Rec& r) {
    struct Target {
        uint32_t fid;
        std::vector<WriteCtx::Inst> insts;
    };
    std::vector<Target> targets;
    for (const TagIn& tag : c.tags) {
        Target* t = nullptr;
        for (Target& x : targets)
            if (x.fid == tag.fid) t = &x;
        if (!t) {
            targets.push_back(Target{tag.fid, {}});
            t = &targets.back();
        }
        t->insts.push_back(WriteCtx::Inst{tag.f6[2], tag.f6[4], tag.f6[5], tag.f6[3], tag.url, c.at});
    }
    // The record's other rows: the feeds holding its CID and its local rows.
    // A record has no source only while no feed holds it (C-37 (1)): an
    // untagged write of a held CID is a copy of the record in each feed
    // holding it (format 1's copy of a tagged record), and a tagged write
    // takes the record's local rows into its feed.
    std::vector<RecState*> held_;
    RecState* localRs = nullptr;
    int32_t rc = holders(w, r, c.tags.empty(), &held_, &localRs);
    if (rc != P4_OK) return rc;
    if (targets.empty()) {
        for (RecState* h : held_) targets.push_back(Target{h->fid, {}});
        if (targets.empty()) targets.push_back(Target{localFid_, {}});
        else if (localRs) {
            rc = absorbLocal(w, localRs, held_[0]);  // local rows beside a feed's (never left by a write)
            if (rc != P4_OK) return rc;
            localRs = nullptr;
        }
    }
    const uint32_t scope = c.tags.empty() ? 0 : c.tags[0].fid;
    // An ingest identity (IQC) of the first tag's feed: a repeat of a held
    // capture tags the held record (C-26), in that feed.
    RecState* held = nullptr;
    std::string ikey;
    const bool ingestIdent = sp_->identity && r.ident && !c.tags.empty();
    if (ingestIdent) {
        ikey.assign(reinterpret_cast<const char*>(r.ident), 32);
        auto it = identOf_.find({scope, ikey});
        if (it != identOf_.end()) {
            if (std::memcmp(it->second->key, r.key, 32) != 0) held = it->second;
        } else {
            int64_t hseq = 0;
            rc = identSeq(scope, r.ident, &hseq);
            if (rc != P4_OK) return rc;
            if (hseq) {
                RecState* hr = w.bySeq(scope, hseq, &rc);
                if (!hr) return rc;
                if (hr->existed && liveRows(hr) && std::memcmp(hr->key, r.key, 32) != 0) held = hr;
            }
        }
    }
    bool anyNew = false, anyCopy = false, anyInst = false;
    for (Target& tg : targets) {
        RecState* rs = held && tg.fid == scope ? held : w.byKey(tg.fid, r.key, &rc);
        if (!rs) return rc;
        const bool isNew = !rs->existed && !freshSet_.count(rs);
        // A copy of a record the feed holds stores the holder's bytes and ts
        // with this write's signature and peer (format 1's
        // mirrorRoutedRecordFromExisting).
        RowR copy;
        const RowR* holder = nullptr;
        if (!isNew)
            for (const RowR& x : rs->rows)
                if (x.live() && (!holder || x.tok < holder->tok)) holder = &x;
        if (holder) {
            copy.tok = c.tok;
            copy.peer = r.peer.empty() ? c.peer : r.peer;
            if (r.sigLen) copy.sig.assign(reinterpret_cast<const char*>(r.sig), r.sigLen);
            copy.len = holder->len;
            copy.sealed = holder->sealed;
            copy.fcols = holder->fcols;
            if (holder->hasD) {
                copy.hasD = true;
                copy.d = holder->d;
            } else if (holder->loaded) {
                copy.srcFid = rs->fid;
                copy.srcRid = holder->rid;
            } else {
                copy.srcFid = holder->srcFid;
                copy.srcRid = holder->srcRid;
            }
        } else {
            copy = copyOf(c, r);
        }
        // A record new to a feed that has local rows takes them, with its
        // seq: the same record, now with a source (format 1: the untagged
        // record gains the tag).
        const bool absorb = isNew && localRs && rs->fid != localRs->fid && liveRows(localRs);
        bool copyKnown = false;  // the call's producer already had a copy (local)
        // A local record of this write has no seq yet: the feed's record
        // takes a new one.
        const bool keepSeq = absorb && localRs->existed;
        if (isNew) setRecord(rs, r);
        if (keepSeq) {
            rs->seq = localRs->seq;
            rs->ts = localRs->ts;
        }
        if (absorb) {
            const RowR* own = nullptr;  // the call producer's local copy
            const RowR* lh = nullptr;   // the record's first copy
            for (const RowR& x : localRs->rows) {
                if (!x.live()) continue;
                if (x.tok == c.tok && !own) own = &x;
                if (!lh || x.tok < lh->tok) lh = &x;
            }
            const RowR* src = own ? own : lh;
            copyKnown = own != nullptr;
            if (src) {
                if (own) {
                    copy.peer = own->peer;
                    copy.sig = own->sig;
                }
                copy.len = src->len;
                copy.sealed = src->sealed;
                copy.fcols = src->fcols;
                copy.hasD = src->hasD;
                copy.d = src->hasD ? src->d : std::string();
                copy.srcFid = src->hasD ? 0 : (src->loaded ? localRs->fid : src->srcFid);
                copy.srcRid = src->hasD ? 0 : (src->loaded ? src->rid : src->srcRid);
            }
        }
        WriteCtx::Out out;
        w.deliver(rs, copy, tg.insts, &out);
        if (absorb) {
            rc = absorbLocal(w, localRs, rs);
            if (rc != P4_OK) return rc;
            if (copyKnown) out.copyNew = false;
        }
        byCid_[std::string(reinterpret_cast<const char*>(r.key), 32)].push_back(rs);
        if (isNew && !keepSeq) {
            fresh_.push_back(rs);
            freshSet_.insert(rs);
            anyNew = anyNew || !absorb;
        } else if (isNew) {
            freshSet_.insert(rs);
        }
        anyCopy = anyCopy || out.copyNew;
        anyInst = anyInst || out.instNew;
        if (!r.rs) r.rs = rs;
        if (ingestIdent && !held && isNew && tg.fid == scope) {
            IdentNew in;
            std::memcpy(in.h, r.ident, 32);
            in.rec = rs;
            w.idents.push_back(in);
            identOf_[{scope, ikey}] = rs;
        }
        // CAT supersede-on-ingest (C-20: ingest mode only), in the scope feed.
        if (r.hasSup && tg.fid == scope && scope && (isNew || out.copyNew) && !(held && rs == held)) {
            rc = supersedeOnIngest(w, r, rs, scope);
            if (rc != P4_OK) return rc;
        }
    }
    if (held) r.action = P4_ACT_IDENT_DUP;
    else if (anyNew) r.action = P4_ACT_NEW;
    else if (anyCopy) r.action = P4_ACT_COPY;
    else if (anyInst) r.action = P4_ACT_RETAG;
    else r.action = P4_ACT_DUP;
    return P4_OK;
}

// The records of r's CID in local and, with feedsToo (an untagged write), in
// every feed file holding it (each file's CID index; this group's
// deliveries too).
int32_t Group::holders(WriteCtx& w, const Rec& r, bool feedsToo, std::vector<RecState*>* feeds, RecState** local) {
    feeds->clear();
    *local = nullptr;
    struct F {
        uint32_t fid;
        bool local;
    };
    std::vector<F> files;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (auto& f : t_->feeds)
            if (f->created && f->k.recs > 0 && !f->quarantined && (feedsToo || f->local)) files.push_back(F{f->fid, f->local});
    }
    const std::string key(reinterpret_cast<const char*>(r.key), 32);
    std::set<RecState*> seen;
    auto take = [&](RecState* rs, bool isLocal) {
        if (!rs || !seen.insert(rs).second) return;
        if (!(rs->existed || freshSet_.count(rs)) || !liveRows(rs)) return;
        if (isLocal) *local = rs;
        else feeds->push_back(rs);
    };
    int32_t rc = P4_OK;
    for (const F& f : files) {
        RecState* rs = w.byKey(f.fid, r.key, &rc);
        if (!rs) return rc;
        take(rs, f.local);
    }
    auto it = byCid_.find(key);
    if (it != byCid_.end())
        for (RecState* rs : it->second) {
            bool isLocal;
            {
                std::lock_guard<std::mutex> g(t_->mu);
                Feed* f = t_->feedById(rs->fid);
                isLocal = f && f->local;
            }
            if (isLocal || feedsToo) take(rs, isLocal);
        }
    std::sort(feeds->begin(), feeds->end(), [](const RecState* a, const RecState* b) { return a->fid < b->fid; });
    return P4_OK;
}

// A record's local rows move into a feed file: each local copy joins the
// record's instances there; the local rows go.
int32_t Group::absorbLocal(WriteCtx& w, RecState* local, RecState* into) {
    std::vector<RowR> moving;
    for (const RowR& x : local->rows) {
        if (!x.live()) continue;
        bool seen = false;
        for (const RowR& m : moving) seen = seen || m.tok == x.tok;
        if (seen) continue;
        RowR m = x;
        m.loaded = false;
        m.rid = 0;
        m.inst = false;
        m.urlSet = false;
        m.nId = m.bId = m.cId = m.uId = 0;
        if (!x.hasD) {
            m.srcFid = x.loaded ? local->fid : x.srcFid;
            m.srcRid = x.loaded ? x.rid : x.srcRid;
        }
        moving.push_back(std::move(m));
    }
    for (const RowR& m : moving) {
        WriteCtx::Out out;
        w.deliver(into, m, {}, &out);
    }
    w.dropAll(local);
    return P4_OK;
}

// Migrate mode (C-38 (3)): format 1's rowid is the record's seq in every feed
// file holding it. The record goes to each feed of its tag instances and to
// each feed already holding its seq; with none, to local. A copy sent without
// instances joins the record's instances in those feeds; local rows of the
// seq move into the feeds once it has an instance.
int32_t Group::migrate(WriteCtx& w, Call& c, Rec& r) {
    const int64_t seq = r.seqIn;
    const std::string key(reinterpret_cast<const char*>(r.key), 32);
    auto sk = seqKey_.find(seq);
    if (sk != seqKey_.end() && sk->second != key) {
        r.reject = P4_REJ_SEQ;
        return P4_OK;
    }
    // Every feed file holding the seq (this write's records, else the files
    // whose seq range covers it).
    struct FInfo {
        uint32_t fid;
        bool local, created;
        int64_t lo, hi;
    };
    std::vector<FInfo> feeds;
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (auto& f : t_->feeds) feeds.push_back(FInfo{f->fid, f->local, f->created, f->k.minseq, f->k.maxseq});
    }
    int32_t rc = P4_OK;
    std::vector<RecState*> holders;
    RecState* localHolder = nullptr;
    for (const FInfo& fi : feeds) {
        RecState* rs = w.known(fi.fid, seq);
        if (!rs && fi.created && seq >= fi.lo && seq <= fi.hi) {
            rs = w.bySeq(fi.fid, seq, &rc);
            if (!rs) return rc;
        }
        if (!rs || !(rs->existed || freshSet_.count(rs)) || !liveRows(rs)) continue;
        if (std::memcmp(rs->key, r.key, 32) != 0) {
            r.reject = P4_REJ_SEQ;  // the seq is held by another CID
            return P4_OK;
        }
        if (fi.local) localHolder = rs;
        else holders.push_back(rs);
    }
    const bool isNewRecord = holders.empty() && !localHolder;
    // The targets: the instances' feeds, then the feeds holding the seq.
    struct Target {
        uint32_t fid;
        std::vector<WriteCtx::Inst> insts;
    };
    std::vector<Target> targets;
    auto target = [&](uint32_t fid) -> Target* {
        for (Target& x : targets)
            if (x.fid == fid) return &x;
        targets.push_back(Target{fid, {}});
        return &targets.back();
    };
    for (auto& ti : r.tagsIn) {
        const TagIn& tag = c.tags[ti.first];
        target(tag.fid)->insts.push_back(WriteCtx::Inst{tag.f6[2], tag.f6[4], tag.f6[5], tag.f6[3], tag.url, ti.second});
    }
    for (RecState* h : holders) target(h->fid);
    if (targets.empty()) target(localFid_);
    seqKey_[seq] = key;
    const RowR copy = copyOf(c, r);
    bool copyNew = false;
    for (Target& tg : targets) {
        RecState* rs = w.bySeq(tg.fid, seq, &rc, r.key);
        if (!rs) return rc;
        const bool isNew = !rs->existed && !freshSet_.count(rs);
        if (isNew) setRecord(rs, r);
        WriteCtx::Out out;
        w.deliver(rs, copy, tg.insts, &out);
        copyNew = copyNew || (!isNew && out.copyNew);
        if (isNew) {
            fresh_.push_back(rs);
            freshSet_.insert(rs);
        }
        if (!r.rs) r.rs = rs;
    }
    // Local rows of the seq move into the feeds (each copy with each instance).
    if (localHolder && !(targets.size() == 1 && targets[0].fid == localFid_)) {
        std::vector<RowR> moving;
        for (const RowR& x : localHolder->rows) {
            if (!x.live()) continue;
            bool seen = false;
            for (const RowR& m : moving) seen = seen || m.tok == x.tok;
            if (seen) continue;
            RowR m = x;
            m.loaded = false;
            m.rid = 0;
            m.inst = false;
            m.urlSet = false;
            m.nId = m.bId = m.cId = m.uId = 0;
            if (!x.hasD) {
                m.srcFid = localHolder->fid;
                m.srcRid = x.loaded ? x.rid : x.srcRid;
                if (!x.loaded) m.srcFid = x.srcFid;
            }
            moving.push_back(std::move(m));
        }
        for (Target& tg : targets) {
            if (tg.fid == localFid_) continue;
            RecState* rs = w.bySeq(tg.fid, seq, &rc, r.key);
            if (!rs) return rc;
            for (const RowR& m : moving) {
                WriteCtx::Out out;
                w.deliver(rs, m, {}, &out);
            }
        }
        w.dropAll(localHolder);
    }
    // C-21: a migrated record registers format 1's ingest identity in the
    // feed of its first tag instance.
    if (isNewRecord && sp_->identity && r.ident && !r.tagsIn.empty()) {
        const uint32_t idFid = c.tags[r.tagsIn[0].first].fid;
        RecState* rs = w.known(idFid, seq);
        if (rs) {
            IdentNew in;
            std::memcpy(in.h, r.ident, 32);
            in.rec = rs;
            w.idents.push_back(in);
        }
    }
    r.action = isNewRecord ? P4_ACT_MIGRATED : copyNew ? P4_ACT_COPY : P4_ACT_MIGRATED;
    return P4_OK;
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
    }
    for (Call& c : calls_) {
        if (c.status != P4_OK) continue;
        for (size_t gi : c.recs) prepare(recs_[gi], c);
    }
    bool any = false;
    for (Call& c : calls_) any = any || (c.status == P4_OK && !c.recs.empty());
    if (!any) {
        respond();
        return;
    }
    // The type's T/ files exist from its first write.
    {
        std::string err;
        const int32_t frc = typeFilesEnsure(t_, &err);
        if (frc != P4_OK) {
            fail(frc, "type files: " + err);
            respond();
            return;
        }
    }
    {
        std::lock_guard<std::mutex> g(t_->mu);
        if (t_->broken) {
            const std::string why = t_->brokenWhy;
            fail(P4_E_IO, "the type index is behind its feed files: " + why);
        } else {
            // The feeds of every call's tags, local when a call may need it
            // (an untagged ingest, any migration), and each call's token (copy).
            for (Call& c : calls_) {
                if (c.status != P4_OK) continue;
                c.tok = tokFor(t_, c.token, c.peer, true);
                for (TagIn& tag : c.tags) {
                    if (!tag.valid) continue;
                    Feed* f = feedFor(t_, tag.f6[0], tag.f6[1], true);
                    tag.fid = f->fid;
                    if (f->quarantined) {
                        c.status = P4_E_CORRUPT;
                        c.err = "feed file quarantined: " + f->path;
                    }
                }
                if (c.status == P4_OK && (c.tags.empty() || c.mode == 1)) {
                    Feed* local = feedFor(t_, "", "", true);
                    localFid_ = local->fid;
                    if (local->quarantined) {
                        c.status = P4_E_CORRUPT;
                        c.err = "feed file quarantined: " + local->path;
                    }
                }
            }
        }
    }
    WriteCtx w(e_, t_);
    w.migrate = false;
    for (Call& c : calls_)
        if (c.status == P4_OK) w.migrate = c.mode == 1;  // a group never mixes modes
    int32_t rc = P4_OK;
    for (Call& c : calls_) {
        if (c.status != P4_OK || rc != P4_OK) continue;
        for (size_t gi : c.recs) {
            if (rc != P4_OK) break;
            Rec& r = recs_[gi];
            if (r.reject) continue;
            rc = c.mode == 1 ? migrate(w, c, r) : ingest(w, c, r);
        }
    }
    if (rc != P4_OK) {
        fail(rc, "write planning failed: " + w.err);
        respond();
        return;
    }
    // Seqs: records new to a feed in content-time order (ingest), format 1's
    // (migrate, already set).
    std::vector<RecState*> assign;
    for (RecState* rs : fresh_)
        if (!w.migrate && !rs->seq) assign.push_back(rs);
    std::sort(assign.begin(), assign.end(), [](const RecState* a, const RecState* b) {
        if (a->w != b->w) return a->w < b->w;
        const int c = std::memcmp(a->key, b->key, 32);
        if (c) return c < 0;
        return a->fid < b->fid;
    });
    std::pair<int64_t, int64_t> inflight(INT64_MAX, 0);
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (RecState* rs : assign) rs->seq = t_->nextSeq++;
        for (RecState* rs : fresh_) {
            if (rs->seq >= t_->nextSeq) t_->nextSeq = rs->seq + 1;
            inflight.first = std::min(inflight.first, rs->seq);
            inflight.second = std::max(inflight.second, rs->seq);
        }
        if (inflight.second) {
            t_->inflight.push_back(inflight);
            if (t_->inflight.size() > e_->stat[kStMaxInflight].load()) e_->stat[kStMaxInflight].store(t_->inflight.size());
            t_->visRecompute();
        }
    }
    rc = w.commit();
    {
        std::lock_guard<std::mutex> g(t_->mu);
        for (size_t i = 0; i < t_->inflight.size(); i++)
            if (t_->inflight[i] == inflight) {
                t_->inflight.erase(t_->inflight.begin() + long(i));
                break;
            }
        t_->visRecompute();
    }
    if (rc != P4_OK) fail(rc, "commit failed: " + w.err);
    respond();
}

}  // namespace

void putGroup(Engine* e, uint32_t writer, Type* t, std::vector<WriteTask*>& tasks) {
    (void)writer;
    Group g(e, t);
    g.run(tasks);
}

}  // namespace p4
}  // namespace flatsql
