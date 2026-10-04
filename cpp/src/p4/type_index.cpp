// Store format 4: the per-standard type index (CONTRACT C-38 (2)): the feed
// registry and the type-wide token registry. It holds no per-record entry and
// no counters: a record is found in its feed files (each keeps its own CID
// index), a type-wide read merges the feed files, and each feed file's
// counters are committed in the file with its rows and read at open.
//
//   feed(fid, provider, source, name, gen)   feed id <-> (provider, source), file name, and the
//                                             stream generation (a missing index's rebuild reads it)
//   tok(id, token, peer)                      the type's producer tokens (copies), in order
//
// A write registers a new feed or token here (synchronous=FULL) before the
// first feed file commit that uses it.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {
const char* kIndexSchema =
    "CREATE TABLE IF NOT EXISTS feed(fid INTEGER PRIMARY KEY, provider TEXT NOT NULL, source TEXT NOT NULL,"
    " name TEXT NOT NULL, gen INTEGER NOT NULL DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS tok(id INTEGER PRIMARY KEY, token TEXT NOT NULL, peer TEXT NOT NULL);";

const char* ctext(sqlite3_stmt* s, int i) {
    const unsigned char* t = sqlite3_column_text(s, i);
    return t ? reinterpret_cast<const char*>(t) : "";
}
}  // namespace

int32_t typeIndexOpen(Type* t, std::string* err) {
    Conn* c = nullptr;
    int rc = openConn(t->pIdx, OpenKind::Index, 512, 4096, &c, err);
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
    int sr;
    sqlite3_stmt* s = c->sql("SELECT id, token, peer FROM tok ORDER BY id");
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
    s = c->sql("SELECT fid, provider, source, name, gen FROM feed ORDER BY fid");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        Feed* f = feedRestore(t, uint32_t(sqlite3_column_int64(s, 0)), ctext(s, 1), ctext(s, 2), ctext(s, 3));
        f->regGen = uint32_t(sqlite3_column_int64(s, 4));
        f->gen = f->regGen;
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read feeds");
    return P4_OK;
}

int indexPutFeed(Conn* idx, const Feed& f, uint32_t gen) {
    sqlite3_stmt* s = idx->sql("INSERT OR REPLACE INTO feed(fid, provider, source, name, gen) VALUES(?1,?2,?3,?4,?5)");
    if (!s) return SQLITE_ERROR;
    sqlite3_bind_int64(s, 1, f.fid);
    sqlite3_bind_text(s, 2, f.provider.data(), int(f.provider.size()), SQLITE_STATIC);
    sqlite3_bind_text(s, 3, f.source.data(), int(f.source.size()), SQLITE_STATIC);
    sqlite3_bind_text(s, 4, f.name.data(), int(f.name.size()), SQLITE_STATIC);
    sqlite3_bind_int64(s, 5, gen);
    const int r = sqlite3_step(s);
    sqlite3_reset(s);
    return r == SQLITE_DONE ? SQLITE_OK : r;
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
