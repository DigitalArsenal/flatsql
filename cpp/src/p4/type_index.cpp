// Store format 4: the per-standard type index (CONTRACT C-37 (5)), derived
// from the feed files and written with every write.
//
//   feed(fid, provider, source, name, counters)   the feed ids <-> (provider, source) and file names,
//                                                  with each file's counters (its meta, mirrored)
//   inst(fid, b, c, batch, ...)                    each feed file's live instances and counters, mirrored
//   tok(id, token, peer, counters)                 the type's producer tokens (copies)
//   x(seq, fid, cid, len, w, k, e, cp)             one entry per (record, feed file holding it):
//     PRIMARY KEY (seq, fid)                         arrival order (datasync, quota, the type's newest N)
//     x_c (cid, seq, fid, e)                         CID -> (seq, feed): lookup, dedupe, CID windows
//     x_w (w DESC, cid, seq, fid, e)                 type windows, index pages, epoch windows
//     x_k (k, e, cid, seq, fid)                      EPOCH per object (types with an object rule)
//   ident(src, h, seq, cid)                        IQC ingest identities
//   meta                                           the type's counters, next seq, full text through
//
// A type-wide read walks x and reads only the feed files that hold the
// page's records; a read of one feed uses that file's own indexes. The type's
// writer commits the index right after the feed files of every write; the
// journal covers the gap (journal.cpp).
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {
const char* kIndexSchema =
    "CREATE TABLE IF NOT EXISTS feed(fid INTEGER PRIMARY KEY, provider TEXT NOT NULL, source TEXT NOT NULL,"
    " name TEXT NOT NULL, rows, recs, bytes, nnull, minseq, maxseq, minw, maxw, mints, maxts, mine, maxe, ix);"
    "CREATE TABLE IF NOT EXISTS inst(fid INTEGER NOT NULL, b INTEGER NOT NULL, c INTEGER NOT NULL, batch TEXT,"
    " ppeer TEXT, pkey TEXT, ckey TEXT, n, bytes, minw, maxw, minseq, maxseq, first, updated, maxat, maxts, url TEXT,"
    " PRIMARY KEY(fid, b, c)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS tok(id INTEGER PRIMARY KEY, token TEXT NOT NULL, peer TEXT NOT NULL, n, bytes, mints,"
    " maxts, maxseq);"
    "CREATE TABLE IF NOT EXISTS x(seq INTEGER NOT NULL, fid INTEGER NOT NULL, cid BLOB NOT NULL, len INTEGER NOT NULL,"
    " w INTEGER NOT NULL, k, e INTEGER, cp BLOB NOT NULL, PRIMARY KEY(seq, fid)) WITHOUT ROWID;"
    "CREATE INDEX IF NOT EXISTS x_c ON x(cid, seq, fid, e);"
    "CREATE INDEX IF NOT EXISTS x_w ON x(w DESC, cid, seq, fid, e);"
    "CREATE TABLE IF NOT EXISTS ident(src INTEGER NOT NULL, h BLOB NOT NULL, seq INTEGER NOT NULL,"
    " cid BLOB NOT NULL, PRIMARY KEY(src, h)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v) WITHOUT ROWID;";

const char* ctext(sqlite3_stmt* s, int i) {
    const unsigned char* t = sqlite3_column_text(s, i);
    return t ? reinterpret_cast<const char*>(t) : "";
}
int64_t opt(sqlite3_stmt* s, int i, int64_t none) {
    return sqlite3_column_type(s, i) == SQLITE_NULL ? none : sqlite3_column_int64(s, i);
}
void bindOpt(sqlite3_stmt* s, int i, int64_t v, int64_t none) {
    if (v == none) sqlite3_bind_null(s, i);
    else sqlite3_bind_int64(s, i, v);
}
}  // namespace

int32_t typeIndexOpen(Type* t, std::string* err) {
    Conn* c = nullptr;
    int rc = openConn(t->pIdx, OpenKind::Index, 8192, 4096, &c, err);
    if (rc != SQLITE_OK) return statusOfSqlite(rc);
    std::string ddl = kIndexSchema;
    if (t->spec()->hasObject) ddl += "CREATE INDEX IF NOT EXISTS x_k ON x(k, e, cid, seq, fid);";
    rc = c->exec(ddl.c_str());
    if (rc != SQLITE_OK) {
        if (err) *err = sqlite3_errmsg(c->db);
        delete c;
        return statusOfSqlite(rc);
    }
    sqlite3_wal_hook(c->db, walHook, t->e);  // its WAL is checkpointed by the maintenance thread
    t->idx = c;
    std::lock_guard<std::mutex> g(t->mu);
    // Every registry read either completes or fails the open (M9): a statement
    // that cannot run must not read as an empty registry.
    auto broken = [&](const char* what) {
        if (err) *err = std::string("type index: ") + what + ": " + sqlite3_errmsg(c->db);
        return P4_E_IO;
    };
    sqlite3_stmt* s = c->sql("SELECT k, v FROM meta");
    if (!s) return broken("prepare");
    int sr;
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        if (sqlite3_column_type(s, 1) == SQLITE_NULL) continue;
        const std::string k = ctext(s, 0);
        const int64_t v = sqlite3_column_int64(s, 1);
        if (k == "uniq") t->uniq = v;
        else if (k == "uniq_bytes") t->uniqBytes = v;
        else if (k == "copies") t->copies = v;
        else if (k == "copy_bytes") t->copyBytes = v;
        else if (k == "next_seq") t->nextSeq = std::max(t->nextSeq, v);
        else if (k == "fts_through") t->ftsThrough = v;
        else if (k == "mine") t->mine = v;
        else if (k == "maxe") t->maxe = v;
        else if (k == "mints") t->mints = v;
        else if (k == "maxts") t->maxts = v;
        else if (k == "maxseq") t->maxseq = v;
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read meta");
    s = c->sql(
        "SELECT fid, provider, source, name, rows, recs, bytes, nnull, minseq, maxseq, minw, maxw, mints, maxts, mine,"
        " maxe, ix FROM feed ORDER BY fid");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        Feed* f = feedRestore(t, uint32_t(sqlite3_column_int64(s, 0)), ctext(s, 1), ctext(s, 2), ctext(s, 3));
        Counters& k = f->k;
        k.rows = opt(s, 4, 0);
        k.recs = opt(s, 5, 0);
        k.bytes = opt(s, 6, 0);
        k.nnull = opt(s, 7, 0);
        k.minseq = opt(s, 8, INT64_MAX);
        k.maxseq = opt(s, 9, 0);
        k.minw = opt(s, 10, INT64_MAX);
        k.maxw = opt(s, 11, INT64_MIN);
        k.mints = opt(s, 12, INT64_MAX);
        k.maxts = opt(s, 13, 0);
        k.mine = opt(s, 14, INT64_MAX);
        k.maxe = opt(s, 15, INT64_MIN);
        f->indexed = opt(s, 16, 1) != 0;
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read feeds");
    s = c->sql(
        "SELECT fid, b, c, batch, ppeer, pkey, ckey, n, bytes, minw, maxw, minseq, maxseq, first, updated, maxat, maxts,"
        " url FROM inst WHERE n>0");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        Feed* f = t->feedById(uint32_t(sqlite3_column_int64(s, 0)));
        if (!f) continue;
        InstCount ic;
        ic.batch = ctext(s, 3);
        ic.ppeer = ctext(s, 4);
        ic.pkey = ctext(s, 5);
        ic.ckey = ctext(s, 6);
        ic.n = opt(s, 7, 0);
        ic.bytes = opt(s, 8, 0);
        ic.minw = opt(s, 9, INT64_MAX);
        ic.maxw = opt(s, 10, INT64_MIN);
        ic.minseq = opt(s, 11, INT64_MAX);
        ic.maxseq = opt(s, 12, 0);
        ic.first = opt(s, 13, 0);
        ic.updated = opt(s, 14, 0);
        ic.maxat = opt(s, 15, 0);
        ic.maxts = opt(s, 16, 0);
        ic.url = ctext(s, 17);
        f->inst[InstId(uint32_t(sqlite3_column_int64(s, 1)), uint32_t(sqlite3_column_int64(s, 2)))] = std::move(ic);
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read instances");
    s = c->sql("SELECT id, token, peer, n, bytes, mints, maxts, maxseq FROM tok ORDER BY id");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        const uint32_t id = uint32_t(sqlite3_column_int64(s, 0));
        while (t->toks.size() + 1 < id) {
            TokDef ph;
            ph.token = "\x1f#" + std::to_string(t->toks.size() + 1);
            t->toks.push_back(ph);
        }
        TokDef d;
        d.token = ctext(s, 1);
        d.peer = ctext(s, 2);
        d.n = opt(s, 3, 0);
        d.bytes = opt(s, 4, 0);
        d.mints = opt(s, 5, INT64_MAX);
        d.maxts = opt(s, 6, 0);
        d.maxseq = opt(s, 7, 0);
        d.registered = true;
        if (t->toks.size() + 1 == id) t->toks.push_back(d);
        else t->toks[id - 1] = d;
        t->tokByToken[d.token] = id;
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read tokens");
    t->visRecompute();
    return P4_OK;
}

// ---- lookups --------------------------------------------------------------------------------
namespace {
int32_t readEnts(sqlite3_stmt* s, bool bySeq, int64_t seq, const uint8_t* key, std::vector<XEnt>* out) {
    int r;
    while ((r = sqlite3_step(s)) == SQLITE_ROW) {
        XEnt x;
        if (bySeq) {
            x.seq = seq;
            x.fid = uint32_t(sqlite3_column_int64(s, 0));
            if (sqlite3_column_bytes(s, 1) != 32) continue;
            std::memcpy(x.key, sqlite3_column_blob(s, 1), 32);
        } else {
            x.seq = sqlite3_column_int64(s, 0);
            x.fid = uint32_t(sqlite3_column_int64(s, 1));
            std::memcpy(x.key, key, 32);
        }
        x.len = sqlite3_column_int64(s, 2);
        x.w = sqlite3_column_int64(s, 3);
        x.k.from(s, 4);
        x.hasE = sqlite3_column_type(s, 5) != SQLITE_NULL;
        x.e = sqlite3_column_int64(s, 5);
        cpDecode(sqlite3_column_blob(s, 6), size_t(sqlite3_column_bytes(s, 6)), &x.cp);
        out->push_back(std::move(x));
    }
    sqlite3_reset(s);
    return r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}
}  // namespace

int32_t xOfCid(Conn* c, const uint8_t key[32], std::vector<XEnt>* out) {
    out->clear();
    sqlite3_stmt* s = c->get(S_X_CID);
    if (!s) return P4_E_INTERNAL;
    sqlite3_bind_blob(s, 1, key, 32, SQLITE_STATIC);
    const int32_t rc = readEnts(s, false, 0, key, out);
    std::sort(out->begin(), out->end(), [](const XEnt& a, const XEnt& b) { return a.fid < b.fid; });
    return rc;
}

int32_t xOfSeq(Conn* c, int64_t seq, std::vector<XEnt>* out) {
    out->clear();
    sqlite3_stmt* s = c->get(S_X_SEQ);
    if (!s) return P4_E_INTERNAL;
    sqlite3_bind_int64(s, 1, seq);
    return readEnts(s, true, seq, nullptr, out);
}

Conn* indexReader(P4Lane* L, Type* t, int32_t* rc) {
    *rc = P4_OK;
    if (!t->hasFiles.load(std::memory_order_acquire)) return nullptr;  // no data yet
    auto it = L->idx.find(t);
    if (it != L->idx.end()) return it->second;
    Conn* c = nullptr;
    const int r = openConn(t->pIdx, OpenKind::IndexReader, 2048, 0, &c, nullptr);
    if (r != SQLITE_OK) {
        *rc = statusOfSqlite(r);
        return nullptr;
    }
    L->idx[t] = c;
    return c;
}

uint64_t identSrcOf(const std::string& provider, const std::string& source) {
    const std::string b = provider + '\0' + source;
    uint8_t dg[32];
    ps::sha256(b.data(), b.size(), dg);
    return ld64(dg) & 0x7fffffffffffffffull;
}

int32_t identGet(Conn* c, uint64_t src, const uint8_t h[32], int64_t* seq, uint8_t cid[32]) {
    *seq = 0;
    sqlite3_stmt* s = c->get(S_IDENT_GET);
    if (!s) return P4_E_INTERNAL;
    sqlite3_bind_int64(s, 1, int64_t(src));
    sqlite3_bind_blob(s, 2, h, 32, SQLITE_STATIC);
    const int r = sqlite3_step(s);
    if (r == SQLITE_ROW && sqlite3_column_bytes(s, 1) == 32) {
        *seq = sqlite3_column_int64(s, 0);
        std::memcpy(cid, sqlite3_column_blob(s, 1), 32);
    }
    sqlite3_reset(s);
    return r == SQLITE_ROW || r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}

// ---- counters ---------------------------------------------------------------------------------
TypeCounts typeCountsOf(Type* t) {
    TypeCounts c;
    c.uniq = t->uniq;
    c.uniqBytes = t->uniqBytes;
    c.copies = t->copies;
    c.copyBytes = t->copyBytes;
    c.mine = t->mine;
    c.maxe = t->maxe;
    c.mints = t->mints;
    c.maxts = t->maxts;
    c.maxseq = t->maxseq;
    c.nextSeq = t->nextSeq;
    c.toks = t->toks;
    return c;
}

void typeCountsTo(Type* t, const TypeCounts& c) {
    t->uniq = c.uniq;
    t->uniqBytes = c.uniqBytes;
    t->copies = c.copies;
    t->copyBytes = c.copyBytes;
    t->mine = c.mine;
    t->maxe = c.maxe;
    t->mints = c.mints;
    t->maxts = c.maxts;
    t->maxseq = c.maxseq;
    for (size_t i = 0; i < c.toks.size() && i < t->toks.size(); i++) {
        TokDef& d = t->toks[i];
        d.n = c.toks[i].n;
        d.bytes = c.toks[i].bytes;
        d.mints = c.toks[i].mints;
        d.maxts = c.toks[i].maxts;
        d.maxseq = c.toks[i].maxseq;
    }
}

namespace {
// A record's copies over its entries: token -> its smallest length.
std::map<uint32_t, int64_t> copiesOf(const std::vector<XEnt>& v) {
    std::map<uint32_t, int64_t> m;
    for (const XEnt& x : v)
        for (const CopyLen& c : x.cp) {
            auto it = m.find(c.tok);
            if (it == m.end() || c.len < it->second) m[c.tok] = c.len;
        }
    return m;
}
}  // namespace

// A record's bytes are its smallest copy's (unsealed copies are the same
// bytes; sealed envelopes may differ in length).
void xCount(const XChange& x, TypeCounts* tc) {
    const std::map<uint32_t, int64_t> b = copiesOf(x.before), a = copiesOf(x.after);
    const bool hadB = !x.before.empty(), hasA = !x.after.empty();
    auto minLen = [](const std::map<uint32_t, int64_t>& m) {
        int64_t v = INT64_MAX;
        for (auto& kv : m) v = std::min(v, kv.second);
        return m.empty() ? int64_t(0) : v;
    };
    tc->uniq += int64_t(hasA) - int64_t(hadB);
    tc->uniqBytes += (hasA ? minLen(a) : 0) - (hadB ? minLen(b) : 0);
    int64_t sumA = 0, sumB = 0;
    for (auto& kv : a) sumA += kv.second;
    for (auto& kv : b) sumB += kv.second;
    tc->copies += int64_t(a.size()) - int64_t(b.size());
    tc->copyBytes += sumA - sumB;
    const XEnt* rep = hasA ? &x.after[0] : nullptr;
    std::set<uint32_t> toks;
    for (auto& kv : a) toks.insert(kv.first);
    for (auto& kv : b) toks.insert(kv.first);
    for (uint32_t tok : toks) {
        if (tok == 0) continue;
        while (tc->toks.size() < tok) tc->toks.emplace_back();
        TokDef& d = tc->toks[tok - 1];
        auto ia = a.find(tok), ib = b.find(tok);
        const bool inA = ia != a.end(), inB = ib != b.end();
        tc->dirtyToks.insert(tok);
        d.n += int64_t(inA) - int64_t(inB);
        d.bytes += (inA ? ia->second : 0) - (inB ? ib->second : 0);
        if (inA && rep) {
            d.mints = std::min(d.mints, x.ts);
            d.maxts = std::max(d.maxts, x.ts);
            d.maxseq = std::max(d.maxseq, rep->seq);
        }
    }
    if (hasA && rep) {
        tc->maxseq = std::max(tc->maxseq, rep->seq);
        tc->mints = std::min(tc->mints, x.ts);
        tc->maxts = std::max(tc->maxts, x.ts);
        if (rep->hasE) {
            tc->mine = std::min(tc->mine, rep->e);
            tc->maxe = std::max(tc->maxe, rep->e);
        }
    }
}

int xWrite(Conn* idx, const XChange& x) {
    std::set<uint32_t> fids;
    for (const XEnt& e : x.before) fids.insert(e.fid);
    for (const XEnt& e : x.after) fids.insert(e.fid);
    const int64_t seq = !x.after.empty() ? x.after[0].seq : !x.before.empty() ? x.before[0].seq : 0;
    for (uint32_t fid : fids) {
        const XEnt* a = nullptr;
        for (const XEnt& e : x.after)
            if (e.fid == fid) a = &e;
        int r;
        if (a) {
            sqlite3_stmt* s = idx->get(S_X_PUT);
            if (!s) return SQLITE_ERROR;
            sqlite3_bind_int64(s, 1, a->seq);
            sqlite3_bind_int64(s, 2, a->fid);
            sqlite3_bind_blob(s, 3, a->key, 32, SQLITE_STATIC);
            sqlite3_bind_int64(s, 4, a->len);
            sqlite3_bind_int64(s, 5, a->w);
            a->k.bind(s, 6);
            if (a->hasE) sqlite3_bind_int64(s, 7, a->e);
            else sqlite3_bind_null(s, 7);
            const std::string cp = cpEncode(a->cp);
            sqlite3_bind_blob(s, 8, cp.data(), int(cp.size()), SQLITE_TRANSIENT);
            r = sqlite3_step(s);
            sqlite3_reset(s);
        } else {
            sqlite3_stmt* s = idx->get(S_X_DEL);
            if (!s) return SQLITE_ERROR;
            sqlite3_bind_int64(s, 1, seq);
            sqlite3_bind_int64(s, 2, fid);
            r = sqlite3_step(s);
            sqlite3_reset(s);
        }
        if (r != SQLITE_DONE) return r;
    }
    return SQLITE_OK;
}

FeedSnap feedSnapOf(Feed* f) {
    FeedSnap s;
    s.fid = f->fid;
    s.provider = f->provider;
    s.source = f->source;
    s.name = f->name;
    s.k = f->k;
    s.indexed = f->indexed;
    s.inst = f->inst;
    return s;
}

int indexPutFeed(Conn* idx, const FeedSnap& f) {
    sqlite3_stmt* s = idx->sql(
        "INSERT OR REPLACE INTO feed(fid, provider, source, name, rows, recs, bytes, nnull, minseq, maxseq, minw, maxw,"
        " mints, maxts, mine, maxe, ix) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)");
    if (!s) return SQLITE_ERROR;
    sqlite3_bind_int64(s, 1, f.fid);
    sqlite3_bind_text(s, 2, f.provider.data(), int(f.provider.size()), SQLITE_STATIC);
    sqlite3_bind_text(s, 3, f.source.data(), int(f.source.size()), SQLITE_STATIC);
    sqlite3_bind_text(s, 4, f.name.data(), int(f.name.size()), SQLITE_STATIC);
    sqlite3_bind_int64(s, 5, f.k.rows);
    sqlite3_bind_int64(s, 6, f.k.recs);
    sqlite3_bind_int64(s, 7, f.k.bytes);
    sqlite3_bind_int64(s, 8, f.k.nnull);
    bindOpt(s, 9, f.k.minseq, INT64_MAX);
    sqlite3_bind_int64(s, 10, f.k.maxseq);
    bindOpt(s, 11, f.k.minw, INT64_MAX);
    bindOpt(s, 12, f.k.maxw, INT64_MIN);
    bindOpt(s, 13, f.k.mints, INT64_MAX);
    sqlite3_bind_int64(s, 14, f.k.maxts);
    bindOpt(s, 15, f.k.mine, INT64_MAX);
    bindOpt(s, 16, f.k.maxe, INT64_MIN);
    sqlite3_bind_int(s, 17, f.indexed ? 1 : 0);
    int r = sqlite3_step(s);
    sqlite3_reset(s);
    if (r != SQLITE_DONE) return r;
    sqlite3_stmt* d = idx->sql("DELETE FROM inst WHERE fid=?1");
    if (!d) return SQLITE_ERROR;
    sqlite3_bind_int64(d, 1, f.fid);
    r = sqlite3_step(d);
    sqlite3_reset(d);
    if (r != SQLITE_DONE) return r;
    sqlite3_stmt* q = idx->sql(
        "INSERT INTO inst(fid, b, c, batch, ppeer, pkey, ckey, n, bytes, minw, maxw, minseq, maxseq, first, updated, maxat,"
        " maxts, url) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)");
    if (!q) return SQLITE_ERROR;
    for (auto& kv : f.inst) {
        const InstCount& ic = kv.second;
        if (ic.n <= 0) continue;
        sqlite3_bind_int64(q, 1, f.fid);
        sqlite3_bind_int64(q, 2, kv.first.first);
        sqlite3_bind_int64(q, 3, kv.first.second);
        sqlite3_bind_text(q, 4, ic.batch.data(), int(ic.batch.size()), SQLITE_STATIC);
        sqlite3_bind_text(q, 5, ic.ppeer.data(), int(ic.ppeer.size()), SQLITE_STATIC);
        sqlite3_bind_text(q, 6, ic.pkey.data(), int(ic.pkey.size()), SQLITE_STATIC);
        sqlite3_bind_text(q, 7, ic.ckey.data(), int(ic.ckey.size()), SQLITE_STATIC);
        sqlite3_bind_int64(q, 8, ic.n);
        sqlite3_bind_int64(q, 9, ic.bytes);
        bindOpt(q, 10, ic.minw, INT64_MAX);
        bindOpt(q, 11, ic.maxw, INT64_MIN);
        bindOpt(q, 12, ic.minseq, INT64_MAX);
        sqlite3_bind_int64(q, 13, ic.maxseq);
        sqlite3_bind_int64(q, 14, ic.first);
        sqlite3_bind_int64(q, 15, ic.updated);
        sqlite3_bind_int64(q, 16, ic.maxat);
        sqlite3_bind_int64(q, 17, ic.maxts);
        sqlite3_bind_text(q, 18, ic.url.data(), int(ic.url.size()), SQLITE_STATIC);
        r = sqlite3_step(q);
        sqlite3_reset(q);
        if (r != SQLITE_DONE) return r;
    }
    return SQLITE_OK;
}

int indexPutTok(Conn* idx, uint32_t id, const TokDef& d) {
    sqlite3_stmt* s = idx->sql(
        "INSERT OR REPLACE INTO tok(id, token, peer, n, bytes, mints, maxts, maxseq) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)");
    if (!s) return SQLITE_ERROR;
    sqlite3_bind_int64(s, 1, id);
    sqlite3_bind_text(s, 2, d.token.data(), int(d.token.size()), SQLITE_STATIC);
    sqlite3_bind_text(s, 3, d.peer.data(), int(d.peer.size()), SQLITE_STATIC);
    sqlite3_bind_int64(s, 4, d.n);
    sqlite3_bind_int64(s, 5, d.bytes);
    bindOpt(s, 6, d.mints, INT64_MAX);
    sqlite3_bind_int64(s, 7, d.maxts);
    sqlite3_bind_int64(s, 8, d.maxseq);
    const int r = sqlite3_step(s);
    sqlite3_reset(s);
    return r == SQLITE_DONE ? SQLITE_OK : r;
}

int indexPutCounts(Conn* idx, const TypeCounts& c) {
    const struct {
        const char* k;
        int64_t v;
        bool set;
    } kv[] = {{"uniq", c.uniq, true},           {"uniq_bytes", c.uniqBytes, true}, {"copies", c.copies, true},
              {"copy_bytes", c.copyBytes, true}, {"next_seq", c.nextSeq, true},     {"mine", c.mine, c.mine != INT64_MAX},
              {"maxe", c.maxe, c.maxe != INT64_MIN}, {"mints", c.mints, c.mints != INT64_MAX},
              {"maxts", c.maxts, true},          {"maxseq", c.maxseq, true}};
    for (auto& x : kv) {
        sqlite3_stmt* s = idx->sql("INSERT OR REPLACE INTO meta(k, v) VALUES(?1, ?2)");
        if (!s) return SQLITE_ERROR;
        sqlite3_bind_text(s, 1, x.k, -1, SQLITE_STATIC);
        if (x.set) sqlite3_bind_int64(s, 2, x.v);
        else sqlite3_bind_null(s, 2);
        const int r = sqlite3_step(s);
        sqlite3_reset(s);
        if (r != SQLITE_DONE) return r;
    }
    for (uint32_t id : c.dirtyToks) {
        if (id == 0 || id > c.toks.size()) continue;
        const int r = indexPutTok(idx, id, c.toks[id - 1]);
        if (r != SQLITE_OK) return r;
    }
    return SQLITE_OK;
}

}  // namespace p4
}  // namespace flatsql
