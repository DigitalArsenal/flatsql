// Store format 4: the maintenance thread (design §5.3, §7, §8). Never on a
// caller's path: type-index flushes, PASSIVE/RESTART checkpoints, closing
// evicted writer connections (a close may checkpoint), unlinking retired
// files, QUOTA_GC and REBUILD, and the background FTS index.
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>

#include "flatsql/record_search.h"
#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {

struct MaintState {
    std::unordered_map<std::string, Conn*> conns;  // checkpoint connections by path
    std::list<std::string> lru;
};

std::mutex gWalMu;
std::unordered_map<std::string, int64_t> gWalPages;  // writer connections' WAL frames
std::unordered_set<std::string> gCkptQueued;

Conn* maintConn(MaintState& m, const std::string& path) {
    auto it = m.conns.find(path);
    if (it != m.conns.end()) return it->second;
    if (m.conns.size() >= 32) {
        const std::string victim = m.lru.back();
        m.lru.pop_back();
        delete m.conns[victim];
        m.conns.erase(victim);
    }
    Conn* c = nullptr;
    if (openConn(path, OpenKind::Maint, 1024, 0, &c, nullptr) != SQLITE_OK) return nullptr;
    m.conns[path] = c;
    m.lru.push_front(path);
    return c;
}

void maintDrop(MaintState& m, const std::string& path) {
    auto it = m.conns.find(path);
    if (it == m.conns.end()) return;
    delete it->second;
    m.conns.erase(it);
    m.lru.remove(path);
}

void checkpoint(Engine* e, MaintState& m, const std::string& path) {
    Conn* c = maintConn(m, path);
    if (!c) return;
    int log = 0, ck = 0;
    sqlite3_wal_checkpoint_v2(c->db, nullptr, SQLITE_CHECKPOINT_PASSIVE, &log, &ck);
    e->bump(kStPassive);
    const int64_t walBytes = ioSize(path + "-wal");
    int64_t total = 0;
    {
        std::lock_guard<std::mutex> g(gWalMu);
        for (auto& kv : gWalPages) total += kv.second * 4096;
    }
    if (walBytes > int64_t(e->cfg.restartBytes) || total > int64_t(e->cfg.walTotal)) {
        // The writer waits; readers do not (their transactions are one page long).
        sqlite3_wal_checkpoint_v2(c->db, nullptr, SQLITE_CHECKPOINT_RESTART, &log, &ck);
        e->bump(kStRestart);
    }
}

void unlinkFile(Engine* e, MaintState& m, File* f) {
    // Wait for the writer to let go, then close its connection here.
    for (int i = 0; i < 30000; i++) {
        Conn* w = nullptr;
        {
            std::lock_guard<std::mutex> g(e->wconnMu);
            if (f->wPins == 0) {
                w = f->w;
                f->w = nullptr;
                if (f->inLru) {
                    e->wlru.erase(f->lru);
                    f->inLru = false;
                    e->nWConn--;
                }
                delete w;
                break;
            }
        }
        ps::sleepNs(1000000);
    }
    maintDrop(m, f->path);
    e->rpool.dropPath(f->path);
    for (int i = 0; i < 30000 && f->users.load() > 0; i++) ps::sleepNs(1000000);
    e->rpool.dropPath(f->path);
    {
        std::lock_guard<std::mutex> g(gWalMu);
        gWalPages.erase(f->path);
    }
    // The main file last: a leftover -wal never meets a new file at the path
    // (and paths are never reused anyway: generation suffixes).
    ioUnlink(f->path + "-wal");
    ioUnlink(f->path + "-journal");
    ioUnlink(f->path);
    e->bump(kStUnlinked);
}

}  // namespace

int walHook(void* arg, sqlite3* db, const char* zDb, int nPages) {
    Engine* e = static_cast<Engine*>(arg);
    const char* path = sqlite3_db_filename(db, zDb);
    if (!path) return SQLITE_OK;
    bool kick = false;
    {
        std::lock_guard<std::mutex> g(gWalMu);
        gWalPages[path] = nPages;
        int64_t total = 0;
        for (auto& kv : gWalPages) total += kv.second;
        e->stat[kStWalBytes].store(uint64_t(total) * 4096, std::memory_order_relaxed);
        if (uint32_t(nPages) >= e->cfg.passivePages && !gCkptQueued.count(path)) {
            gCkptQueued.insert(path);
            kick = true;
        }
    }
    if (kick) {
        {
            std::lock_guard<std::mutex> g(e->maintMu);
            MaintTask mt;
            mt.kind = MaintTask::kCheckpoint;
            mt.path = path;
            e->maintQ.push_back(mt);
        }
        e->kickMaintenance();
    }
    return SQLITE_OK;
}

// ---- QUOTA_GC (mode 1: by content month, owner D1) -------------------------------------------------
namespace {
int64_t diskBytes(const std::string& path) {
    int64_t n = 0;
    for (const char* sfx : {"", "-wal", "-journal"}) {
        const int64_t s = ioSize(path + sfx);
        if (s > 0) n += s;
    }
    return n;
}
}  // namespace

int32_t quotaGc(Engine* e, uint64_t maxBytes, int64_t* filesDropped, int64_t* recordsDropped, int64_t* bytesFreed,
                bool enforce) {
    *filesDropped = *recordsDropped = *bytesFreed = 0;
    if (e->cfg.quotaMode != 1) return P4_E_UNSUPPORTED;
    std::vector<Type*> types;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        for (auto& t : e->types) types.push_back(t.get());
    }
    for (;;) {
        int64_t total = 0;
        Type* oldestType = nullptr;
        int64_t oldest = INT64_MAX;
        for (Type* t : types) {
            std::vector<std::pair<int64_t, std::string>> files;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& p : t->parts)
                    for (auto& kv : p->files)
                        if (kv.second->created && !kv.second->retired) files.push_back({kv.first, kv.second->path});
            }
            int64_t current = monthOfSec(nowSec());
            for (auto& f : files) {
                total += diskBytes(f.second);
                if (f.first > 0 && f.first < oldest && f.first < current) {
                    oldest = f.first;
                    oldestType = t;
                }
            }
            total += diskBytes(t->pIdx) + diskBytes(t->pJnl) + diskBytes(t->pFts);
        }
        if (uint64_t(total) <= maxBytes) {
            if (enforce)
                for (Type* t : types) {
                    std::lock_guard<std::mutex> g(t->mu);
                    t->overQuota = false;
                }
            return P4_OK;
        }
        if (!oldestType) {
            // Nothing droppable but the current month: under the configured
            // quota, writes refuse (P4_E_NOSPACE) while reads continue.
            if (enforce)
                for (Type* t : types) {
                    std::lock_guard<std::mutex> g(t->mu);
                    t->overQuota = true;
                }
            return P4_OK;
        }
        Type* t = oldestType;
        typeIndexFlush(t, true);
        std::lock_guard<std::mutex> fg(t->flushMu);
        std::vector<File*> drop;
        int64_t rows = 0, payload = 0;
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& p : t->parts) {
                auto it = p->files.find(oldest);
                if (it != p->files.end() && !it->second->retired) {
                    it->second->dropping = true;  // new claims are refused from here
                    drop.push_back(it->second);
                }
            }
        }
        // Wait until every claimed write has published and no writer has the
        // files pinned; then nothing writes them again.
        bool quiet = false;
        for (int i = 0; i < 30000 && !quiet; i++) {
            quiet = true;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (File* f : drop) quiet = quiet && f->inflight == 0;
            }
            if (quiet) {
                std::lock_guard<std::mutex> g(e->wconnMu);
                for (File* f : drop) quiet = quiet && f->wPins == 0;
            }
            if (!quiet) ps::sleepNs(1000000);
        }
        if (!quiet) {
            std::lock_guard<std::mutex> g(t->mu);
            for (File* f : drop) f->dropping = false;
            return P4_E_BUSY;  // the next pass retries
        }
        int64_t distinct = 0;
        {
            sqlite3_stmt* s = t->idx->sql("SELECT count(DISTINCT cid) FROM c WHERE tb=?1");
            sqlite3_bind_int64(s, 1, oldest);
            if (sqlite3_step(s) == SQLITE_ROW) distinct = sqlite3_column_int64(s, 0);
            sqlite3_reset(s);
        }
        for (File* f : drop) {
            const int64_t db = diskBytes(f->path);
            {
                std::lock_guard<std::mutex> g(t->mu);
                rows += f->n;
                payload += f->bytes;
                // Pending entries of the month go with it.
                for (CEnt& x : t->pend.raw())
                    if ((x.st == 1 || x.st == 2) && x.tb == oldest && x.pid == f->part->pid) t->pend.kill(x.tb, x.key, x.pid);
            }
            retireFile(e, f);
            *bytesFreed += db;
            (*filesDropped)++;
        }
        // One range delete per index table (WHERE tb = ?).
        Conn* c = t->idx;
        c->exec("BEGIN IMMEDIATE");
        for (const char* sql : {"DELETE FROM c WHERE tb=?1", "DELETE FROM ident WHERE tb=?1", "DELETE FROM obj WHERE tb=?1",
                                "DELETE FROM file WHERE tb=?1", "DELETE FROM lanecnt WHERE tb=?1"}) {
            sqlite3_stmt* s = c->sql(sql);
            if (!s) continue;
            sqlite3_bind_int64(s, 1, oldest);
            sqlite3_step(s);
            sqlite3_reset(s);
        }
        {
            std::lock_guard<std::mutex> g(t->mu);
            t->uniq -= distinct;
            t->copies -= rows - distinct;
            t->uniqBytes -= rows ? int64_t(double(payload) * double(distinct) / double(rows)) : 0;
            if (t->uniq < 0) t->uniq = 0;
            if (t->copies < 0) t->copies = 0;
            if (t->uniqBytes < 0) t->uniqBytes = 0;
            t->tbs.erase(std::remove(t->tbs.begin(), t->tbs.end(), oldest), t->tbs.end());
            const char* mk[3] = {"uniq", "uniq_bytes", "copies"};
            const int64_t mv[3] = {t->uniq, t->uniqBytes, t->copies};
            for (int i = 0; i < 3; i++) {
                sqlite3_stmt* s = c->sql("INSERT OR REPLACE INTO meta(k, v) VALUES(?1,?2)");
                sqlite3_bind_text(s, 1, mk[i], -1, SQLITE_STATIC);
                sqlite3_bind_int64(s, 2, mv[i]);
                sqlite3_step(s);
                sqlite3_reset(s);
            }
        }
        c->exec("COMMIT");
        *recordsDropped += rows;
        e->bump(kStQuotaFiles, drop.size());
    }
}

// ---- REBUILD ------------------------------------------------------------------------------------------
namespace {
struct Verify {
    int64_t entries = 0, mismatches = 0;
};
// REBUILD verify's findings, one line each, in native debug runs only.
void why(const char* what, const std::string& path, int64_t a, int64_t b) {
#if !defined(__wasm__)
    static const bool on = std::getenv("P4_VERIFY_DEBUG") != nullptr;
    if (on) std::fprintf(stderr, "verify: %s %s (%lld vs %lld)\n", what, path.c_str(), (long long)a, (long long)b);
#else
    (void)what; (void)path; (void)a; (void)b;
#endif
}

// Compares (and with fix, rewrites) the type index's c, file and lanecnt rows
// against the partition files, and obj against the files' ent rows.
int32_t indexAgainstFiles(Engine* e, Type* t, bool fix, Verify* v) {
    std::shared_ptr<const Spec> sp = t->spec();
    int32_t rc = typeIndexFlush(t, true);
    if (rc != P4_OK) return rc;
    std::lock_guard<std::mutex> fg(t->flushMu);
    std::vector<File*> files;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& p : t->parts)
            for (auto& kv : p->files)
                if (kv.second->created && !kv.second->retired) files.push_back(kv.second);
    }
    Conn* x = t->idx;
    // An error ends the pass with that error (M9): a statement that stops
    // early must not read as missing rows, and a fix must not rewrite the
    // index from a partial read.
    Conn* c = nullptr;
    auto fail = [&](int src) -> int32_t {
        if (c) delete c;
        if (fix) x->exec("ROLLBACK");
        return statusOfSqlite(src == SQLITE_OK || src == SQLITE_ROW || src == SQLITE_DONE ? SQLITE_ERROR : src);
    };
    if (fix && x->exec("BEGIN IMMEDIATE") != SQLITE_OK) return P4_E_IO;
    int64_t cRows = 0;
    {
        sqlite3_stmt* s = x->sql("SELECT count(*) FROM c");
        const int src = s ? sqlite3_step(s) : SQLITE_ERROR;
        if (src == SQLITE_ROW) cRows = sqlite3_column_int64(s, 0);
        if (s) sqlite3_reset(s);
        if (src != SQLITE_ROW) return fail(src);
    }
    int64_t fileRows = 0;
    if (fix) {
        for (const char* sql : {"DELETE FROM c", "DELETE FROM obj", "DELETE FROM file", "DELETE FROM lanecnt"}) {
            const int src = x->exec(sql);
            if (src != SQLITE_OK) return fail(src);
        }
    }
    for (File* f : files) {
        c = nullptr;
        {
            const int orc = openConn(f->path, OpenKind::Maint, 4096, 0, &c, nullptr);
            if (orc != SQLITE_OK) {
                c = nullptr;
                return fail(orc);
            }
        }
        const uint32_t pid = f->part->pid;
        sqlite3_stmt* s = c->sql("SELECT seq, cid, length(d), w, e FROM r");
        if (!s) return fail(SQLITE_ERROR);
        int64_t n = 0, bytes = 0, minseq = INT64_MAX, maxseq = 0, minw = INT64_MAX, maxw = INT64_MIN, nnull = 0;
        int src;
        while ((src = sqlite3_step(s)) == SQLITE_ROW) {
            const int64_t seq = sqlite3_column_int64(s, 0);
            n++;
            bytes += sqlite3_column_int64(s, 2);
            minseq = std::min(minseq, seq);
            maxseq = std::max(maxseq, seq);
            minw = std::min(minw, sqlite3_column_int64(s, 3));
            maxw = std::max(maxw, sqlite3_column_int64(s, 3));
            if (sqlite3_column_type(s, 4) == SQLITE_NULL && sp->hasEpochRule) nnull++;
            v->entries++;
            if (fix) {
                sqlite3_stmt* ins = x->get(S_C_INS);
                sqlite3_bind_int64(ins, 1, f->tb);
                sqlite3_bind_blob(ins, 2, sqlite3_column_blob(s, 1), 32, SQLITE_TRANSIENT);
                sqlite3_bind_int64(ins, 3, pid);
                sqlite3_bind_int64(ins, 4, seq);
                const int irc = sqlite3_step(ins);
                sqlite3_reset(ins);
                if (irc != SQLITE_DONE) {
                    sqlite3_reset(s);
                    return fail(irc);
                }
            } else {
                sqlite3_stmt* q = x->sql("SELECT seq FROM c WHERE tb=?1 AND cid=?2 AND pid=?3");
                sqlite3_bind_int64(q, 1, f->tb);
                sqlite3_bind_blob(q, 2, sqlite3_column_blob(s, 1), 32, SQLITE_TRANSIENT);
                sqlite3_bind_int64(q, 3, pid);
                const int qrc = sqlite3_step(q);
                const bool ok = qrc == SQLITE_ROW && sqlite3_column_int64(q, 0) == seq;
                sqlite3_reset(q);
                if (qrc != SQLITE_ROW && qrc != SQLITE_DONE) {
                    sqlite3_reset(s);
                    return fail(qrc);
                }
                if (!ok) {
                    v->mismatches++;
                    why("c entry", f->path, seq, 0);
                }
            }
        }
        sqlite3_reset(s);
        if (src != SQLITE_DONE) return fail(src);
        fileRows += n;
        int64_t fn, fbytes;
        std::map<uint32_t, LaneCount> mem;
        {
            std::lock_guard<std::mutex> g(t->mu);
            fn = f->n;
            fbytes = f->bytes;
            mem = f->lanes;
        }
        if (fn != n || fbytes != bytes) {
            v->mismatches++;
            why("file counters n", f->path, fn, n);
            why("file counters bytes", f->path, fbytes, bytes);
        }
        // Lane rows against the tag instances, and the in-memory counters.
        // Aggregated here, not with GROUP BY: no sorter (its memory counts
        // against the engine's heap limit) for millions of tag instances.
        std::map<uint32_t, std::pair<int64_t, int64_t>> real;
        s = c->sql("SELECT rl.lane, length(r.d) FROM rl JOIN r ON r.seq=rl.seq");
        if (!s) return fail(SQLITE_ERROR);
        while ((src = sqlite3_step(s)) == SQLITE_ROW) {
            auto& a = real[uint32_t(sqlite3_column_int64(s, 0))];
            a.first++;
            a.second += sqlite3_column_int64(s, 1);
        }
        sqlite3_reset(s);
        if (src != SQLITE_DONE) return fail(src);
        for (auto& kv : real) {
            auto it = mem.find(kv.first);
            if (it == mem.end() || it->second.n != kv.second.first || it->second.bytes != kv.second.second) {
                v->mismatches++;
                why("lane n", f->path, it == mem.end() ? -1 : it->second.n, kv.second.first);
                why("lane bytes", f->path, it == mem.end() ? -1 : it->second.bytes, kv.second.second);
            }
        }
        for (auto& kv : mem)
            if (!real.count(kv.first)) {
                v->mismatches++;
                why("lane in memory only", f->path, kv.first, kv.second.n);
            }
        if (fix) {
            std::lock_guard<std::mutex> g(t->mu);
            f->n = n;
            f->bytes = bytes;
            f->minseq = minseq;
            f->maxseq = maxseq;
            f->minw = minw;
            f->maxw = maxw;
            f->nnull = nnull;
            for (auto& kv : real) {
                LaneCount& lc = f->lanes[kv.first];
                lc.n = kv.second.first;
                lc.bytes = kv.second.second;
            }
            for (auto it = f->lanes.begin(); it != f->lanes.end();)
                if (!real.count(it->first)) it = f->lanes.erase(it);
                else ++it;
            f->touched = true;
            f->objRefresh = true;
        }
        // obj against ent
        if (sp->ek && !fix) {
            sqlite3_stmt* q = c->sql("SELECT k, n, fw, lw FROM ent WHERE n>0");
            if (!q) return fail(SQLITE_ERROR);
            while ((src = sqlite3_step(q)) == SQLITE_ROW) {
                sqlite3_stmt* o = x->sql("SELECT n, fw, lw FROM obj WHERE k=?1 AND tb=?2 AND pid=?3");
                sqlite3_bind_value(o, 1, sqlite3_column_value(q, 0));
                sqlite3_bind_int64(o, 2, f->tb);
                sqlite3_bind_int64(o, 3, pid);
                const bool ok = sqlite3_step(o) == SQLITE_ROW && sqlite3_column_int64(o, 0) == sqlite3_column_int64(q, 1) &&
                                sqlite3_column_int64(o, 1) == sqlite3_column_int64(q, 2) &&
                                sqlite3_column_int64(o, 2) == sqlite3_column_int64(q, 3);
                sqlite3_reset(o);
                if (!ok) {
                    v->mismatches++;
                    why("obj", f->path, 0, 0);
                }
            }
            sqlite3_reset(q);
            if (src != SQLITE_DONE) return fail(src);
        }
        delete c;
        c = nullptr;
    }
    if (!fix && cRows != fileRows) {
        v->mismatches += std::max<int64_t>(cRows - fileRows, fileRows - cRows);
        why("c rows vs file rows", t->name, cRows, fileRows);
    }
    if (fix) {
        const int crc = x->exec("COMMIT");
        if (crc != SQLITE_OK) return fail(crc);
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& p : t->parts) {
                p->n = p->bytes = 0;
                for (auto& kv : p->files) {
                    p->n += kv.second->n;
                    p->bytes += kv.second->bytes;
                }
            }
        }
    }
    (void)e;
    return P4_OK;
}
}  // namespace

namespace {
// C-27: PRAGMA integrity_check through the engine (a maintenance connection,
// which also recovers a WAL first). Anything but "ok" (or an open failure) is
// a damaged file.
bool fileIntact(const std::string& path) {
    Conn* c = nullptr;
    if (openConn(path, OpenKind::Maint, 4096, 0, &c, nullptr) != SQLITE_OK) return false;
    sqlite3_stmt* s = c->sql("PRAGMA integrity_check");
    bool ok = false;
    if (s) {
        const int rc = sqlite3_step(s);
        ok = rc == SQLITE_ROW && std::strcmp(reinterpret_cast<const char*>(sqlite3_column_text(s, 0)), "ok") == 0 &&
             sqlite3_step(s) == SQLITE_DONE;
        sqlite3_reset(s);
    }
    delete c;
    return ok;
}
}  // namespace

int32_t rebuildOp(Engine* e, Type* only, uint32_t what, std::vector<std::array<int64_t, 2>>* rows,
                  std::vector<Type*>* rowTypes, std::string* firstBad) {
    std::vector<Type*> types;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        for (auto& t : e->types)
            if (!only || t.get() == only) types.push_back(t.get());
    }
    for (Type* t : types) {
        Verify v;
        if (what & 1) {
            // Partition secondary indexes after a migration's bulk append.
            std::vector<File*> files;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& p : t->parts)
                    for (auto& kv : p->files)
                        if (kv.second->created && !kv.second->retired) files.push_back(kv.second);
            }
            for (File* f : files) {
                Conn* c = nullptr;
                if (openConn(f->path, OpenKind::Maint, 65536, 0, &c, nullptr) != SQLITE_OK) return P4_E_IO;
                const int32_t rc = fileCreateIndexes(t, c);
                delete c;
                if (rc != P4_OK) return rc;
                std::lock_guard<std::mutex> g(t->mu);
                f->indexed = true;
                f->touched = true;
                v.entries++;
            }
            e->bump(kStRebuilds);
        }
        if (what & 2) {
            int32_t rc = indexAgainstFiles(e, t, true, &v);
            if (rc != P4_OK) return rc;
            rc = typeIndexFlush(t, true);
            if (rc != P4_OK) return rc;
            e->bump(kStRebuilds);
        }
        if (what & 4) {
            {
                std::lock_guard<std::mutex> g(t->ftsMu);
                if (t->fts) t->fts->exec("DELETE FROM fts");
                t->ftsThrough = 0;
            }
            ftsCatchUp(e, t, true);
        }
        if (what & 8) {
            Verify vv;
            const int32_t rc = indexAgainstFiles(e, t, false, &vv);
            if (rc != P4_OK) return rc;
            v.entries = vv.entries;
            v.mismatches = vv.mismatches;
            // C-27: every live file of the type, its index and its journal.
            std::vector<std::string> paths;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& p : t->parts)
                    for (auto& kv : p->files)
                        if (kv.second->created && !kv.second->retired) paths.push_back(kv.second->path);
            }
            paths.push_back(t->pIdx);
            paths.push_back(t->pJnl);
            for (const std::string& path : paths)
                if (!fileIntact(path)) {
                    v.mismatches++;
                    if (firstBad && firstBad->empty()) *firstBad = path;
                }
        }
        rows->push_back({v.entries, v.mismatches});
        rowTypes->push_back(t);
    }
    return P4_OK;
}

// ---- FTS (background; never on the ingest path) -------------------------------------------------------
int32_t ftsCatchUp(Engine* e, Type* t, bool all) {
    std::shared_ptr<const Spec> sp = t->spec();
    if (!sp->fullText) return P4_OK;
    std::lock_guard<std::mutex> g(t->ftsMu);
    if (!t->fts) {
        Conn* c = nullptr;
        if (openConn(t->pFts, OpenKind::Index, 4096, 4096, &c, nullptr) != SQLITE_OK) return P4_E_IO;
        if (c->exec("CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(t, content='', contentless_delete=1);"
                    "CREATE TABLE IF NOT EXISTS ftsmeta(k TEXT PRIMARY KEY, v) WITHOUT ROWID;") != SQLITE_OK) {
            delete c;
            return P4_E_IO;
        }
        sqlite3_stmt* s = c->sql("SELECT v FROM ftsmeta WHERE k='through'");
        if (s && sqlite3_step(s) == SQLITE_ROW) t->ftsThrough = sqlite3_column_int64(s, 0);
        if (s) sqlite3_reset(s);
        t->fts = c;
    }
    const int64_t vis = t->vis.load(std::memory_order_acquire);
    if (t->ftsThrough >= vis) {
        t->ftsState = 2;
        return P4_OK;
    }
    t->ftsState = 1;
    if (e->ftsHold.load()) return P4_OK;
    std::vector<std::string> paths;
    {
        std::lock_guard<std::mutex> tg(t->mu);
        for (auto& p : t->parts)
            for (auto& kv : p->files)
                if (kv.second->created && !kv.second->retired && kv.second->maxseq > t->ftsThrough)
                    paths.push_back(kv.second->path);
    }
    const std::string fid(reinterpret_cast<const char*>(sp->tc.fid()), 4);
    // The cut: seqs interleave across files, so a bounded pass indexes every
    // file through the same seq (the limit-th smallest pending one).
    int64_t through = vis;
    if (!all) {
        const int64_t limit = 20000;
        std::vector<int64_t> seqs;
        for (const std::string& path : paths) {
            int rc = 0;
            Conn* c = e->rpool.acquire(path, OpenKind::Reader, &rc, nullptr);
            if (!c) return statusOfSqlite(rc);
            sqlite3_stmt* s = c->sql("SELECT seq FROM r WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3");
            sqlite3_bind_int64(s, 1, t->ftsThrough);
            sqlite3_bind_int64(s, 2, vis);
            sqlite3_bind_int64(s, 3, limit);
            while (s && sqlite3_step(s) == SQLITE_ROW) seqs.push_back(sqlite3_column_int64(s, 0));
            if (s) sqlite3_reset(s);
            e->rpool.release(c);
        }
        if (int64_t(seqs.size()) > limit) {
            std::nth_element(seqs.begin(), seqs.begin() + (limit - 1), seqs.end());
            through = seqs[size_t(limit - 1)];
        }
    }
    t->fts->exec("BEGIN IMMEDIATE");
    for (const std::string& path : paths) {
        int rc = 0;
        Conn* c = e->rpool.acquire(path, OpenKind::Reader, &rc, nullptr);
        if (!c) {
            t->fts->exec("ROLLBACK");
            return statusOfSqlite(rc);
        }
        sqlite3_stmt* s = c->sql("SELECT seq, d, f FROM r WHERE seq>?1 AND seq<=?2 ORDER BY seq");
        sqlite3_bind_int64(s, 1, t->ftsThrough);
        sqlite3_bind_int64(s, 2, through);
        while (s && sqlite3_step(s) == SQLITE_ROW) {
            if (sqlite3_column_type(s, 2) != SQLITE_NULL) continue;  // sealed: no plaintext to index
            std::string text, err;
            const auto* d = static_cast<const uint8_t*>(sqlite3_column_blob(s, 1));
            if (!reflectedRecordSearchText(sp->tc.bfbs().data(), sp->tc.bfbs().size(), fid, d,
                                           size_t(sqlite3_column_bytes(s, 1)), text, &err))
                continue;
            sqlite3_stmt* ins = t->fts->sql("INSERT OR REPLACE INTO fts(rowid, t) VALUES(?1,?2)");
            sqlite3_bind_int64(ins, 1, sqlite3_column_int64(s, 0));
            sqlite3_bind_text(ins, 2, text.data(), int(text.size()), SQLITE_TRANSIENT);
            sqlite3_step(ins);
            sqlite3_reset(ins);
            e->bump(kStFtsRows);
        }
        if (s) sqlite3_reset(s);
        e->rpool.release(c);
    }
    sqlite3_stmt* m = t->fts->sql("INSERT OR REPLACE INTO ftsmeta(k, v) VALUES('through', ?1)");
    sqlite3_bind_int64(m, 1, through);
    sqlite3_step(m);
    sqlite3_reset(m);
    t->fts->exec("COMMIT");
    t->ftsThrough = through;
    t->ftsState = through >= vis ? 2 : 1;
    return P4_OK;
}

// ---- the loop -------------------------------------------------------------------------------------------
namespace {
void runMaintSlot(Engine* e, uint32_t slot) {
    SlotHeader* h = e->slot(slot);
    SlotOut out(e, slot);
    std::vector<Tlv> v;
    const bool parsed = h->reqLen <= e->reqBytes[0] && tlvParse(e->slotReq(slot), h->reqLen, &v);
    if (h->op == P4_OPC_QUOTA_GC) {
        out.enc.header({"files_dropped", "records_dropped", "bytes_freed"});
        uint64_t maxBytes = 0;
        bool bad = !parsed;
        if (parsed && !tlvU64(v, 62, &maxBytes, &bad)) maxBytes = e->quota.load();
        if (bad) {
            out.end(P4_E_ARG, "QUOTA_GC: tag 62 is a u64");
            return;
        }
        int64_t files = 0, records = 0, bytes = 0;
        const int32_t rc = maxBytes ? quotaGc(e, maxBytes, &files, &records, &bytes, false) : P4_OK;
        if (rc == P4_OK) {
            out.enc.beginRow();
            out.enc.i64(files);
            out.enc.i64(records);
            out.enc.i64(bytes);
            out.enc.endRow();
            out.rows = 1;
        }
        out.end(rc, rc == P4_OK ? "" : "QUOTA_GC failed");
        return;
    }
    out.enc.header({"type", "entries", "mismatches"});
    std::string type;
    uint32_t what = 0;
    bool bad = !parsed;
    if (parsed) {
        tlvText(v, 1, &type);
        tlvU32(v, 63, &what, &bad);
    }
    if (bad || what == 0 || (what & ~15u)) {
        out.end(P4_E_ARG, "REBUILD needs what (tag 63)");
        return;
    }
    Type* only = nullptr;
    if (!type.empty()) {
        only = e->findType(type);
        if (!only) {
            out.end(P4_E_NOTYPE, "type not registered: " + type);
            return;
        }
    }
    std::vector<std::array<int64_t, 2>> rows;
    std::vector<Type*> rt;
    std::string damaged;
    const int32_t rc = rebuildOp(e, only, what, &rows, &rt, &damaged);
    if (rc == P4_OK) {
        for (size_t i = 0; i < rows.size(); i++) {
            out.enc.beginRow();
            out.enc.text(rt[i]->name.data(), rt[i]->name.size());
            out.enc.i64(rows[i][0]);
            out.enc.i64(rows[i][1]);
            out.enc.endRow();
            out.rows++;
        }
    }
    out.end(rc, rc != P4_OK         ? std::string("REBUILD failed")
                : damaged.empty() ? std::string()
                                  : "integrity_check failed: " + damaged);
}
}  // namespace

void maintenanceLoop(Engine* e, uint32_t thread) {
    tThread = thread;
    Bell& b = e->bells[thread];
    MaintState m;
    uint64_t lastTick = 0;
    int rebuildTicks = 0;
    for (;;) {
        const uint32_t seq = b.doorbell.load(std::memory_order_acquire);
        for (;;) {
            MaintTask mt;
            {
                std::lock_guard<std::mutex> g(e->maintMu);
                if (e->maintQ.empty()) break;
                mt = e->maintQ.front();
                e->maintQ.pop_front();
            }
            switch (mt.kind) {
                case MaintTask::kClose:
                    delete mt.conn;
                    break;
                case MaintTask::kCheckpoint:
                    {
                        std::lock_guard<std::mutex> g(gWalMu);
                        gCkptQueued.erase(mt.path);
                    }
                    checkpoint(e, m, mt.path);
                    break;
                case MaintTask::kUnlink:
                    unlinkFile(e, m, mt.file);
                    break;
                case MaintTask::kSlot:
                    e->slot(mt.slot)->thread = thread;
                    runMaintSlot(e, mt.slot);
                    break;
            }
        }
        if (e->stopping.load()) break;
        const uint64_t now = monoNs();
        if (now - lastTick >= 100ull * 1000 * 1000) {
            lastTick = now;
            std::vector<Type*> types;
            {
                std::lock_guard<std::mutex> g(e->typesMu);
                for (auto& t : e->types) types.push_back(t.get());
            }
            uint64_t pending = 0;
            for (Type* t : types) {
                std::lock_guard<std::mutex> g(t->mu);
                pending += t->pend.bytes();
            }
            for (Type* t : types) typeIndexFlush(t, pending >= e->cfg.pendingBytes);
            for (Type* t : types) ftsCatchUp(e, t, false);
            if (++rebuildTicks >= 50) {  // every 5 s: files with removals since the last look
                rebuildTicks = 0;
                maybeRebuildFiles(e);
            }
            const uint64_t q = e->quota.load();
            if (q) {
                int64_t a, bb, c;
                quotaGc(e, q, &a, &bb, &c, true);
            }
        }
        b.state.store(0, std::memory_order_seq_cst);
        {
            std::lock_guard<std::mutex> g(e->maintMu);
            if (!e->maintQ.empty()) {
                b.state.store(1);
                continue;
            }
        }
        if (b.doorbell.load(std::memory_order_acquire) == seq) ps::waitU32(&b.doorbell, seq, 100ull * 1000 * 1000);
        b.state.store(1, std::memory_order_seq_cst);
    }
    for (auto& kv : m.conns) delete kv.second;
    m.conns.clear();
    b.state.store(2);
}

}  // namespace p4
}  // namespace flatsql
