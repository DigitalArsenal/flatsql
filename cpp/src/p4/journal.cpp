// Store format 4: the per-type intent journal (design §3.2, B3; CONTRACT C-38).
//
// T/<TYPE>.jnl is a SQLite file with synchronous=FULL. Before a write's
// first feed file commits, one journal transaction names the feed files it
// touches (J_TOUCH), the feed and token ids it registers (J_FEED, J_TOK) and
// the seq reservation (jm). The feed files then commit, each in one
// transaction with its counters, then the type index (record.cpp); the next
// write's journal transaction cuts the rows the index has applied.
//
// A feed file's transaction is the whole truth about that file: there is no
// cross-feed identity (C-38). The one write that spans two files with one
// record is a move: a local record taken into a feed (J_MOVE), committed
// feed first. Open replays the tail: the ids are registered, a cut move is
// finished, and every touched feed file's counters, instances and token
// counts (committed with its rows) are mirrored into the type index. Replay
// costs O(the tail), not O(rows).
//
// Journal ids are AUTOINCREMENT, so they never restart when the table empties.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {
const char* kJournalSchema =
    "CREATE TABLE IF NOT EXISTS j(id INTEGER PRIMARY KEY AUTOINCREMENT, op INTEGER NOT NULL, fid INTEGER, seq INTEGER,"
    " k BLOB, s TEXT, v INTEGER);"
    "CREATE TABLE IF NOT EXISTS jm(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID;";

std::vector<std::string> splitUnit(const std::string& s, size_t want) {
    std::vector<std::string> out;
    size_t at = 0;
    while (out.size() + 1 < want) {
        const size_t x = s.find('\x1f', at);
        if (x == std::string::npos) break;
        out.push_back(s.substr(at, x - at));
        at = x + 1;
    }
    out.push_back(s.substr(at));
    while (out.size() < want) out.emplace_back();
    return out;
}

const char* colText(sqlite3_stmt* s, int i) {
    const unsigned char* t = sqlite3_column_text(s, i);
    return t ? reinterpret_cast<const char*>(t) : "";
}
}  // namespace

int32_t journalOpen(Type* t, std::string* err) {
    Conn* c = nullptr;
    int rc = openConn(t->pJnl, OpenKind::Journal, 1024, 4096, &c, err);
    if (rc != SQLITE_OK) return statusOfSqlite(rc);
    sqlite3_wal_hook(c->db, walHook, t->e);  // its WAL is checkpointed by the maintenance thread
    rc = c->exec(kJournalSchema);
    if (rc != SQLITE_OK) {
        if (err) *err = sqlite3_errmsg(c->db);
        delete c;
        return statusOfSqlite(rc);
    }
    t->jdb = c;
    return P4_OK;
}

int32_t journalReplay(Type* t, std::string* err) {
    Conn* j = t->jdb;
    if (!j) return P4_E_INTERNAL;
    {
        sqlite3_stmt* s = j->sql("SELECT v FROM jm WHERE k='seq_reserved'");
        if (s && sqlite3_step(s) == SQLITE_ROW) {
            std::lock_guard<std::mutex> g(t->mu);
            t->seqReserved = sqlite3_column_int64(s, 0);
            if (t->seqReserved >= t->nextSeq) t->nextSeq = t->seqReserved + 1;
        }
        if (s) sqlite3_reset(s);
    }
    struct JE {
        int op;
        uint32_t fid;
        int64_t seq, v;
        std::string s;
    };
    std::vector<JE> rows;
    sqlite3_stmt* q = j->sql("SELECT op, fid, v, s, seq FROM j ORDER BY id");
    if (!q) {
        if (err) *err = sqlite3_errmsg(j->db);
        return P4_E_IO;
    }
    int rc;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        JE x;
        x.op = sqlite3_column_int(q, 0);
        x.fid = uint32_t(sqlite3_column_int64(q, 1));
        x.v = sqlite3_column_int64(q, 2);
        x.s = colText(q, 3);
        x.seq = sqlite3_column_int64(q, 4);
        rows.push_back(std::move(x));
    }
    sqlite3_reset(q);
    if (rc != SQLITE_DONE) {
        if (err) *err = sqlite3_errmsg(j->db);
        return statusOfSqlite(rc);
    }
    if (rows.empty()) return P4_OK;

    // 1. Ids: feeds and tokens.
    std::set<uint32_t> files;
    std::vector<uint32_t> newToks;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (const JE& x : rows) {
            if (x.op == J_FEED) {
                const auto f = splitUnit(x.s, 3);
                Feed* fd = feedRestore(t, x.fid, f[0], f[1], f[2]);
                fd->created = ioExists(fd->path);
                files.insert(x.fid);
            } else if (x.op == J_TOK) {
                const auto f = splitUnit(x.s, 2);
                const uint32_t id = uint32_t(x.v);
                while (t->toks.size() + 1 < id) {
                    TokDef ph;
                    ph.token = "\x1f#" + std::to_string(t->toks.size() + 1);
                    t->toks.push_back(ph);
                }
                if (t->toks.size() + 1 == id) {
                    TokDef d;
                    d.token = f[0];
                    d.peer = f[1];
                    t->toks.push_back(d);
                } else if (t->toks[id - 1].token.rfind("\x1f#", 0) == 0) {
                    t->toks[id - 1].token = f[0];
                    t->toks[id - 1].peer = f[1];
                }
                t->toks[id - 1].registered = true;
                t->tokByToken[t->toks[id - 1].token] = id;
                newToks.push_back(id);
            } else if (x.op == J_TOUCH || x.op == J_MOVE) {
                files.insert(x.fid);
            }
        }
    }
    // 2. Moves cut between their two files: the source file's rows of the
    //    seq go once another file holds the seq (the destination committed
    //    first; a destination that did not commit leaves the record where it
    //    was).
    for (const JE& x : rows) {
        if (x.op != J_MOVE || !x.seq) continue;
        std::vector<Feed*> created;
        Feed* src = nullptr;
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& f : t->feeds)
                if (f->created && !f->quarantined) created.push_back(f.get());
            src = t->feedById(x.fid);
        }
        if (!src || !src->created) continue;
        auto holds = [&](Feed* f, bool* held) -> int32_t {
            int32_t st = P4_OK;
            std::string er;
            Conn* c = writerPin(t->e, f, &st, &er);
            if (!c) {
                if (err) *err = "replay: open " + f->path + ": " + er;
                return st;
            }
            st = fileHoldsSeq(c, x.seq, held);
            writerUnpin(t->e, f);
            return st;
        };
        bool srcHolds = false, elsewhere = false;
        int32_t st = holds(src, &srcHolds);
        if (st != P4_OK) return st;
        for (size_t i = 0; srcHolds && !elsewhere && i < created.size(); i++) {
            if (created[i] == src) continue;
            st = holds(created[i], &elsewhere);
            if (st != P4_OK) return st;
        }
        if (!srcHolds || !elsewhere) continue;
        WriteCtx w(t->e, t);
        w.replaying = true;
        RecState* r = w.bySeq(src->fid, x.seq, &st);
        if (!r) {
            if (err) *err = "replay: move of seq " + std::to_string(x.seq) + ": " + w.err;
            return st;
        }
        w.dropAll(r);
        st = w.commit();
        if (st != P4_OK) {
            if (err) *err = "replay: move of seq " + std::to_string(x.seq) + ": " + w.err;
            return st;
        }
    }
    // 3. Every touched feed file's counters, instances and token counts, from
    //    the file (committed with its rows): the cut write may have committed
    //    it without the index.
    std::vector<FeedSnap> snaps;
    for (uint32_t fid : files) {
        Feed* f;
        bool created;
        {
            std::lock_guard<std::mutex> g(t->mu);
            f = t->feedById(fid);
            if (f && !f->created) f->created = ioExists(f->path);
            created = f && f->created;
        }
        if (!f) continue;
        if (created) {
            int32_t st = P4_OK;
            std::string er;
            Conn* c = writerPin(t->e, f, &st, &er);
            if (!c) {
                if (err) *err = "replay: open " + f->path + ": " + er;
                return st;
            }
            Counters k;
            bool indexed = true;
            std::map<InstId, InstCount> inst;
            std::map<uint32_t, TokCount> tokc;
            int r = readMeta(c, &k, &indexed);
            if (r == SQLITE_OK) r = readInst(f, c, &inst);
            if (r == SQLITE_OK) r = readTokc(t, c, &tokc);
            writerUnpin(t->e, f);
            if (r != SQLITE_OK) {
                if (err) *err = "replay: counters of " + f->path;
                return statusOfSqlite(r);
            }
            std::lock_guard<std::mutex> g(t->mu);
            f->k = k;
            f->indexed = indexed;
            f->inst = std::move(inst);
            f->tokc = std::move(tokc);
        }
        std::lock_guard<std::mutex> g(t->mu);
        snaps.push_back(feedSnapOf(f));
    }
    // 4. The mirrors and registries, in one index transaction.
    Conn* x = t->idx;
    if (!x) return P4_E_INTERNAL;
    std::vector<std::pair<uint32_t, TokDef>> toks;
    int64_t nextSeq;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (size_t i = 0; i < t->toks.size(); i++)
            if (t->toks[i].token.rfind("\x1f#", 0) != 0) toks.push_back({uint32_t(i + 1), t->toks[i]});
        nextSeq = t->nextSeq;
    }
    rc = x->exec("BEGIN IMMEDIATE");
    for (auto& d : toks)
        if (rc == SQLITE_OK) rc = indexPutTok(x, d.first, d.second);
    for (const FeedSnap& s : snaps)
        if (rc == SQLITE_OK) rc = indexPutFeed(x, s);
    if (rc == SQLITE_OK) rc = indexPutNextSeq(x, nextSeq);
    if (rc == SQLITE_OK) rc = x->exec("COMMIT");
    if (rc != SQLITE_OK) {
        if (err) *err = std::string("replay: type index: ") + sqlite3_errmsg(x->db);
        x->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    // 5. Applied: the journal's rows go.
    rc = j->exec("DELETE FROM j");
    if (rc != SQLITE_OK) {
        if (err) *err = std::string("replay: journal cut: ") + sqlite3_errmsg(j->db);
        return statusOfSqlite(rc);
    }
    std::lock_guard<std::mutex> g(t->mu);
    t->jcut = 0;
    t->visRecompute();
    return P4_OK;
}

}  // namespace p4
}  // namespace flatsql
