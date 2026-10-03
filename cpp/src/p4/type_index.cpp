// Store format 4: the per-standard type index (CONTRACT C-38 (2)): the feed
// registry, the type-wide token registry, each feed file's counters mirrored
// and the type's next seq. It holds no per-record entry: a record is found in
// its feed files (each keeps its own CID index), and a type-wide read merges
// the feed files.
//
//   feed(fid, provider, source, name, counters)  feed id <-> (provider, source) and file name;
//                                                 each file's counters (its meta), mirrored
//   inst(fid, b, c, batch, ...)                   each feed file's live instances, mirrored
//   tok(id, token, peer)                          the type's producer tokens (copies), in order
//   ftok(fid, tok, counters)                      each feed file's copies per token, mirrored
//   meta                                          next_seq
//
// The type's writer commits the mirrors of the feed files a write changed
// right after them; the journal covers the gap (journal.cpp). Every mirror is
// derived: REBUILD 2 recounts it from the files.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {
const char* kIndexSchema =
    "CREATE TABLE IF NOT EXISTS feed(fid INTEGER PRIMARY KEY, provider TEXT NOT NULL, source TEXT NOT NULL,"
    " name TEXT NOT NULL, rows, recs, bytes, nnull, rbytes, copies, cbytes, minseq, maxseq, minw, maxw, mints, maxts,"
    " mine, maxe, ix);"
    "CREATE TABLE IF NOT EXISTS inst(fid INTEGER NOT NULL, b INTEGER NOT NULL, c INTEGER NOT NULL, batch TEXT,"
    " ppeer TEXT, pkey TEXT, ckey TEXT, n, bytes, minw, maxw, minseq, maxseq, first, updated, maxat, maxts, url TEXT,"
    " PRIMARY KEY(fid, b, c)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS tok(id INTEGER PRIMARY KEY, token TEXT NOT NULL, peer TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS ftok(fid INTEGER NOT NULL, tok INTEGER NOT NULL, n, bytes, mints, maxts, maxseq,"
    " PRIMARY KEY(fid, tok)) WITHOUT ROWID;"
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
    int rc = openConn(t->pIdx, OpenKind::Index, 2048, 4096, &c, err);
    if (rc != SQLITE_OK) return statusOfSqlite(rc);
    rc = c->exec(kIndexSchema);
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
        if (k == "next_seq") t->nextSeq = std::max(t->nextSeq, sqlite3_column_int64(s, 1));
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read meta");
    s = c->sql("SELECT id, token, peer FROM tok ORDER BY id");
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
        d.registered = true;
        if (t->toks.size() + 1 == id) t->toks.push_back(d);
        else t->toks[id - 1] = d;
        t->tokByToken[d.token] = id;
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read tokens");
    s = c->sql(
        "SELECT fid, provider, source, name, rows, recs, bytes, nnull, rbytes, copies, cbytes, minseq, maxseq, minw, maxw,"
        " mints, maxts, mine, maxe, ix FROM feed ORDER BY fid");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        Feed* f = feedRestore(t, uint32_t(sqlite3_column_int64(s, 0)), ctext(s, 1), ctext(s, 2), ctext(s, 3));
        Counters& k = f->k;
        k.rows = opt(s, 4, 0);
        k.recs = opt(s, 5, 0);
        k.bytes = opt(s, 6, 0);
        k.nnull = opt(s, 7, 0);
        k.rbytes = opt(s, 8, 0);
        k.copies = opt(s, 9, 0);
        k.cbytes = opt(s, 10, 0);
        k.minseq = opt(s, 11, INT64_MAX);
        k.maxseq = opt(s, 12, 0);
        k.minw = opt(s, 13, INT64_MAX);
        k.maxw = opt(s, 14, INT64_MIN);
        k.mints = opt(s, 15, INT64_MAX);
        k.maxts = opt(s, 16, 0);
        k.mine = opt(s, 17, INT64_MAX);
        k.maxe = opt(s, 18, INT64_MIN);
        f->indexed = opt(s, 19, 1) != 0;
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
    s = c->sql("SELECT fid, tok, n, bytes, mints, maxts, maxseq FROM ftok WHERE n>0");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        Feed* f = t->feedById(uint32_t(sqlite3_column_int64(s, 0)));
        const uint32_t tok = uint32_t(sqlite3_column_int64(s, 1));
        if (!f || !t->tokById(tok)) continue;
        TokCount& tc = f->tokc[tok];
        tc.n = opt(s, 2, 0);
        tc.bytes = opt(s, 3, 0);
        tc.mints = opt(s, 4, INT64_MAX);
        tc.maxts = opt(s, 5, 0);
        tc.maxseq = opt(s, 6, 0);
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read feed tokens");
    t->visRecompute();
    return P4_OK;
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
    s.tokc = f->tokc;
    return s;
}

int indexPutFeed(Conn* idx, const FeedSnap& f) {
    sqlite3_stmt* s = idx->sql(
        "INSERT OR REPLACE INTO feed(fid, provider, source, name, rows, recs, bytes, nnull, rbytes, copies, cbytes, minseq,"
        " maxseq, minw, maxw, mints, maxts, mine, maxe, ix) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,"
        "?17,?18,?19,?20)");
    if (!s) return SQLITE_ERROR;
    sqlite3_bind_int64(s, 1, f.fid);
    sqlite3_bind_text(s, 2, f.provider.data(), int(f.provider.size()), SQLITE_STATIC);
    sqlite3_bind_text(s, 3, f.source.data(), int(f.source.size()), SQLITE_STATIC);
    sqlite3_bind_text(s, 4, f.name.data(), int(f.name.size()), SQLITE_STATIC);
    sqlite3_bind_int64(s, 5, f.k.rows);
    sqlite3_bind_int64(s, 6, f.k.recs);
    sqlite3_bind_int64(s, 7, f.k.bytes);
    sqlite3_bind_int64(s, 8, f.k.nnull);
    sqlite3_bind_int64(s, 9, f.k.rbytes);
    sqlite3_bind_int64(s, 10, f.k.copies);
    sqlite3_bind_int64(s, 11, f.k.cbytes);
    bindOpt(s, 12, f.k.minseq, INT64_MAX);
    sqlite3_bind_int64(s, 13, f.k.maxseq);
    bindOpt(s, 14, f.k.minw, INT64_MAX);
    bindOpt(s, 15, f.k.maxw, INT64_MIN);
    bindOpt(s, 16, f.k.mints, INT64_MAX);
    sqlite3_bind_int64(s, 17, f.k.maxts);
    bindOpt(s, 18, f.k.mine, INT64_MAX);
    bindOpt(s, 19, f.k.maxe, INT64_MIN);
    sqlite3_bind_int(s, 20, f.indexed ? 1 : 0);
    int r = sqlite3_step(s);
    sqlite3_reset(s);
    if (r != SQLITE_DONE) return r;
    for (const char* del : {"DELETE FROM inst WHERE fid=?1", "DELETE FROM ftok WHERE fid=?1"}) {
        sqlite3_stmt* d = idx->sql(del);
        if (!d) return SQLITE_ERROR;
        sqlite3_bind_int64(d, 1, f.fid);
        r = sqlite3_step(d);
        sqlite3_reset(d);
        if (r != SQLITE_DONE) return r;
    }
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
    sqlite3_stmt* tq = idx->sql("INSERT INTO ftok(fid, tok, n, bytes, mints, maxts, maxseq) VALUES(?1,?2,?3,?4,?5,?6,?7)");
    if (!tq) return SQLITE_ERROR;
    for (auto& kv : f.tokc) {
        const TokCount& tc = kv.second;
        if (tc.n <= 0) continue;
        sqlite3_bind_int64(tq, 1, f.fid);
        sqlite3_bind_int64(tq, 2, kv.first);
        sqlite3_bind_int64(tq, 3, tc.n);
        sqlite3_bind_int64(tq, 4, tc.bytes);
        bindOpt(tq, 5, tc.mints, INT64_MAX);
        sqlite3_bind_int64(tq, 6, tc.maxts);
        sqlite3_bind_int64(tq, 7, tc.maxseq);
        r = sqlite3_step(tq);
        sqlite3_reset(tq);
        if (r != SQLITE_DONE) return r;
    }
    return SQLITE_OK;
}

int indexPutTok(Conn* idx, uint32_t id, const TokDef& d) {
    sqlite3_stmt* s = idx->sql("INSERT OR REPLACE INTO tok(id, token, peer) VALUES(?1,?2,?3)");
    if (!s) return SQLITE_ERROR;
    sqlite3_bind_int64(s, 1, id);
    sqlite3_bind_text(s, 2, d.token.data(), int(d.token.size()), SQLITE_STATIC);
    sqlite3_bind_text(s, 3, d.peer.data(), int(d.peer.size()), SQLITE_STATIC);
    const int r = sqlite3_step(s);
    sqlite3_reset(s);
    return r == SQLITE_DONE ? SQLITE_OK : r;
}

int indexPutNextSeq(Conn* idx, int64_t nextSeq) {
    sqlite3_stmt* s = idx->sql("INSERT OR REPLACE INTO meta(k, v) VALUES('next_seq', ?1)");
    if (!s) return SQLITE_ERROR;
    sqlite3_bind_int64(s, 1, nextSeq);
    const int r = sqlite3_step(s);
    sqlite3_reset(s);
    return r == SQLITE_DONE ? SQLITE_OK : r;
}

// ---- totals ------------------------------------------------------------------------------------
TypeTotals Type::totals() {
    TypeTotals x;
    x.toks.resize(toks.size());
    for (auto& fp : feeds) {
        const Feed& f = *fp;
        if (!f.created || f.k.recs <= 0) continue;
        x.recs += f.k.recs;
        x.rbytes += f.k.rbytes;
        x.copies += f.k.copies;
        x.cbytes += f.k.cbytes;
        x.mine = std::min(x.mine, f.k.mine);
        x.maxe = std::max(x.maxe, f.k.maxe);
        x.mints = std::min(x.mints, f.k.mints);
        x.maxts = std::max(x.maxts, f.k.maxts);
        x.maxseq = std::max(x.maxseq, f.k.maxseq);
        for (auto& kv : f.tokc) {
            if (kv.first == 0 || kv.first > x.toks.size()) continue;
            TokCount& d = x.toks[kv.first - 1];
            d.n += kv.second.n;
            d.bytes += kv.second.bytes;
            d.mints = std::min(d.mints, kv.second.mints);
            d.maxts = std::max(d.maxts, kv.second.maxts);
            d.maxseq = std::max(d.maxseq, kv.second.maxseq);
        }
    }
    return x;
}

}  // namespace p4
}  // namespace flatsql
