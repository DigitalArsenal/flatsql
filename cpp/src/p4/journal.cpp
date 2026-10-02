// Store format 4: the per-type intent journal (design §3.2, B3; CONTRACT C-37).
//
// T/<TYPE>.jnl is a SQLite file with synchronous=FULL. Before a write's
// first feed file commits, one journal transaction names what it touches:
// J_TOUCH (feed file, seq, CID) for every record row set it changes, the
// feed and token ids it registers (J_FEED, J_TOK), the ingest identities it
// adds (J_IDENT), and the seq reservation (jm). The feed files then commit,
// then the type index (record.cpp); the next write's journal transaction
// cuts the rows the index has applied.
//
// Open replays the tail: the ids are registered; every touched feed file's
// counters are reloaded from its meta (they commit with its rows); every
// touched record is read from its feed files and brought in line (every
// copy with every instance; local rows only without one: a write cut
// between two feed files leaves rows that only add, completed here); its
// type-index entries and the type's counters are set from its rows. Replay
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
        std::string k, s;
    };
    std::vector<JE> rows;
    sqlite3_stmt* q = j->sql("SELECT op, fid, seq, k, s, v FROM j ORDER BY id");
    if (!q) {
        if (err) *err = sqlite3_errmsg(j->db);
        return P4_E_IO;
    }
    int rc;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        JE x;
        x.op = sqlite3_column_int(q, 0);
        x.fid = uint32_t(sqlite3_column_int64(q, 1));
        x.seq = sqlite3_column_int64(q, 2);
        if (sqlite3_column_type(q, 3) == SQLITE_BLOB)
            x.k.assign(static_cast<const char*>(sqlite3_column_blob(q, 3)), size_t(sqlite3_column_bytes(q, 3)));
        x.s = colText(q, 4);
        x.v = sqlite3_column_int64(q, 5);
        rows.push_back(std::move(x));
    }
    sqlite3_reset(q);
    if (rc != SQLITE_DONE) {
        if (err) *err = sqlite3_errmsg(j->db);
        return statusOfSqlite(rc);
    }
    if (rows.empty()) return P4_OK;

    // 1. Ids: feeds and tokens.
    std::map<int64_t, std::set<uint32_t>> touched;  // seq -> feed files
    std::set<uint32_t> files;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (const JE& x : rows) {
            if (x.op == J_FEED) {
                const auto f = splitUnit(x.s, 3);
                Feed* fd = feedRestore(t, x.fid, f[0], f[1], f[2]);
                fd->created = ioExists(fd->path);
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
            } else if (x.op == J_TOUCH) {
                touched[x.seq].insert(x.fid);
                files.insert(x.fid);
                if (x.seq >= t->nextSeq) t->nextSeq = x.seq + 1;
            }
        }
    }
    // 2. Every touched feed file's counters, from its meta (committed with
    //    its rows): the cut write may have committed it without the index.
    for (uint32_t fid : files) {
        Feed* f;
        {
            std::lock_guard<std::mutex> g(t->mu);
            f = t->feedById(fid);
            if (f && !f->created) f->created = ioExists(f->path);
        }
        if (!f) continue;
        bool created;
        {
            std::lock_guard<std::mutex> g(t->mu);
            created = f->created;
        }
        if (!created) continue;
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
        int r = readMeta(c, &k, &indexed);
        if (r == SQLITE_OK) r = readInst(f, c, &inst);
        writerUnpin(t->e, f);
        if (r != SQLITE_OK) {
            if (err) *err = "replay: counters of " + f->path;
            return statusOfSqlite(r);
        }
        std::lock_guard<std::mutex> g(t->mu);
        f->k = k;
        f->indexed = indexed;
        f->inst = std::move(inst);
    }
    // 3. Every touched record across its feed files, brought in line; its
    //    index entries and the counters from its rows.
    WriteCtx w(t->e, t);
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& f : t->feeds) w.migrate = w.migrate || !f->indexed;
    }
    for (auto& kv : touched) {
        int32_t lrc = P4_OK;
        RecState* r = w.bySeq(kv.first, &lrc, &kv.second);
        if (!r) {
            if (err) *err = "replay: seq " + std::to_string(kv.first) + ": " + w.err;
            return lrc;
        }
        w.touchAll(r);
        for (uint32_t fid : kv.second) r->touched.insert(fid);
        if (!r->rows.empty()) w.normalize(r);
    }
    for (const JE& x : rows) {
        if (x.op != J_IDENT || x.k.size() != 64) continue;
        RecState* r = w.known(x.seq);
        if (!r) {
            int32_t lrc = P4_OK;
            r = w.bySeq(x.seq, &lrc);
            if (!r) {
                if (err) *err = "replay: identity of seq " + std::to_string(x.seq) + ": " + w.err;
                return lrc;
            }
        }
        if (std::memcmp(r->key, x.k.data() + 32, 32) != 0) continue;
        IdentNew in;
        in.src = uint64_t(x.v);
        std::memcpy(in.h, x.k.data(), 32);
        in.rec = r;
        w.idents.push_back(in);
        if (r->touched.empty()) w.touchAll(r);
    }
    const int32_t crc = w.commit(false);
    if (crc != P4_OK) {
        if (err) *err = "replay: " + w.err;
        return crc;
    }
    // 4. Applied: the journal's rows go.
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
