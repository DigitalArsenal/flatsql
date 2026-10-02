// Store format 4: the maintenance thread (design §5.3, §7, §8). Never on a
// caller's path: type-index flushes, PASSIVE/RESTART checkpoints, closing
// evicted writer connections (a close may checkpoint), QUOTA_GC and REBUILD,
// and the background FTS index.
#include <algorithm>
#include <chrono>
#include <cmath>
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

Conn* maintConn(Engine* e, MaintState& m, const std::string& path) {
    auto it = m.conns.find(path);
    if (it != m.conns.end()) return it->second;
    if (m.conns.size() >= 32) {
        const std::string victim = m.lru.back();
        m.lru.pop_back();
        delete m.conns[victim];  // the file's last connection deletes its WAL
        m.conns.erase(victim);
        if (!ioExists(victim + "-wal")) {
            walNote(e, victim, 0);
            walExtNote(e, victim, 0);
        }
    }
    Conn* c = nullptr;
    if (openConn(path, OpenKind::Maint, 1024, 0, &c, nullptr) != SQLITE_OK) return nullptr;
    sqlite3_busy_timeout(c->db, 0);  // a checkpoint never waits (see checkpoint)
    m.conns[path] = c;
    m.lru.push_front(path);
    return c;
}

// A checkpoint never waits for the file's writer. PASSIVE passes copy the WAL
// into the file while the writer keeps committing (no writer lock); once a
// pass leaves little behind, RESTART with no busy handler takes the writer
// lock only if it is free at that moment, copies the last few frames and
// makes the writer's next transaction start the WAL over (which bounds the
// WAL file). A reader inside the WAL or a busy writer: the next kick retries.
void checkpoint(Engine* e, MaintState& m, const std::string& path) {
    Conn* c = maintConn(e, m, path);
    if (!c) return;
    int log = 0, ck = 0, prev = -1;
    bool settled = false;  // the last pass found little new: a RESTART copies only a few frames
    for (int pass = 0; pass < 4 && !settled; pass++) {
        const int r = sqlite3_wal_checkpoint_v2(c->db, nullptr, SQLITE_CHECKPOINT_PASSIVE, &log, &ck);
        e->bump(kStPassive);
        if (r != SQLITE_OK) return;
        walNote(e, path, log >= ck ? log - ck : 0);
        if (log > ck) return;  // a reader holds frames back: nothing to restart yet
        settled = prev >= 0 && log - prev <= 256;
        prev = log;
    }
    // Still busy (or a small WAL): no restart now, so the writer lock is never
    // held across a large copy; the next kick tries again.
    if (!settled || log < int(e->cfg.passivePages / 4)) return;
    if (sqlite3_wal_checkpoint_v2(c->db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, &log, &ck) == SQLITE_OK) {
        e->bump(kStRestart);
        walNote(e, path, 0);
        walExtNote(e, path, 0);
    }
}

}  // namespace

void walNote(Engine* e, const std::string& path, int64_t frames) {
    std::lock_guard<std::mutex> g(e->walMu);
    auto it = e->walPages.find(path);
    const int64_t old = it == e->walPages.end() ? 0 : it->second;
    if (frames <= 0) {
        if (it != e->walPages.end()) e->walPages.erase(it);
    } else if (it != e->walPages.end()) {
        it->second = frames;
    } else {
        e->walPages.emplace(path, frames);
    }
    e->walSum += std::max<int64_t>(0, frames) - old;
    e->stat[kStWalBytes].store(uint64_t(std::max<int64_t>(0, e->walSum)) * 4096, std::memory_order_relaxed);
}

void walExtNote(Engine* e, const std::string& path, int64_t frames) {
    std::lock_guard<std::mutex> g(e->walMu);
    auto it = e->walExt.find(path);
    const int64_t old = it == e->walExt.end() ? 0 : it->second;
    if (frames <= 0) {
        if (it != e->walExt.end()) e->walExt.erase(it);
    } else {
        e->walExt[path] = frames;
    }
    e->walExtSum += std::max<int64_t>(0, frames) - old;
}

int64_t walBytesOf(Engine* e, const std::string& path) {
    std::lock_guard<std::mutex> g(e->walMu);
    auto it = e->walPages.find(path);
    return it == e->walPages.end() ? 0 : it->second * 4096;
}

void typeFileBytes(Type* t) {
    if (!t->hasFiles.load(std::memory_order_acquire)) return;
    t->idxBytes.store(std::max<int64_t>(0, ioSize(t->pIdx)) + std::max<int64_t>(0, ioSize(t->pJnl)), std::memory_order_relaxed);
    t->ftsBytes.store(std::max<int64_t>(0, ioSize(t->pFts)), std::memory_order_relaxed);
}

int walHook(void* arg, sqlite3* db, const char* zDb, int nPages) {
    Engine* e = static_cast<Engine*>(arg);
    const char* path = sqlite3_db_filename(db, zDb);
    if (!path) return SQLITE_OK;
    walNote(e, path, nPages);
    // The WAL file's bound. Under steady commits a PASSIVE pass never finds
    // the WAL fully copied at the moment the writer starts its next
    // transaction, so the WAL never starts over by itself and the file grows
    // without end. The file's one writer (this connection, which just
    // committed, so no frame is added meanwhile) restarts it once it passes
    // a quarter of walTotal, or 32 MiB while the instance's WALs are over
    // three quarters of it: TRUNCATE copies what the PASSIVE passes left,
    // waits out the readers still inside the WAL (a reader starting after
    // the copy reads the file only) and truncates the WAL to 0.
    int64_t ext;
    {
        std::lock_guard<std::mutex> g(e->walMu);
        auto it = e->walExt.find(path);
        const int64_t old = it == e->walExt.end() ? 0 : it->second;
        e->walExt[path] = nPages;
        e->walExtSum += int64_t(nPages) - old;
        ext = e->walExtSum;
    }
    const uint64_t frames = uint64_t(nPages) * 4096;
    if (frames >= e->cfg.walTotal / 4 || (uint64_t(ext) * 4096 > e->cfg.walTotal / 4 * 3 && frames >= (32ull << 20))) {
        sqlite3_busy_timeout(db, 2000);
        int log = 0, ck = 0;
        const int r = sqlite3_wal_checkpoint_v2(db, zDb, SQLITE_CHECKPOINT_TRUNCATE, &log, &ck);
        sqlite3_busy_timeout(db, 30000);
        if (r == SQLITE_OK) {
            e->bump(kStTruncate);
            walNote(e, path, 0);
            walExtNote(e, path, 0);
            return SQLITE_OK;
        }
    }
    // A WAL past the PASSIVE pages (at most 32 MiB, so the passes keep up and
    // the restart above copies little), or any WAL of 4 MiB or more while the
    // instance's WALs are over their total, is checkpointed.
    bool kick = false;
    {
        std::lock_guard<std::mutex> g(e->walMu);
        const bool over = uint64_t(e->walSum) * 4096 > e->cfg.walTotal && nPages >= 1024;
        const uint32_t passive = std::min<uint32_t>(e->cfg.passivePages, 8192);
        if ((uint32_t(nPages) >= passive || over) && e->ckptQueued.insert(path).second) kick = true;
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

// ---- QUOTA_GC: the oldest records by arrival (C-32) ----------------------------------------------
namespace {
int64_t sizeOf(const std::string& path) {
    const int64_t n = ioSize(path);
    return n > 0 ? n : 0;
}

std::vector<Type*> typesWithFiles(Engine* e) {
    std::vector<Type*> types;
    std::lock_guard<std::mutex> g(e->typesMu);
    for (auto& t : e->types)
        if (t->hasFiles.load(std::memory_order_acquire)) types.push_back(t.get());
    return types;
}

std::vector<Part*> createdFiles(Type* t) {
    std::vector<Part*> files;
    std::lock_guard<std::mutex> g(t->mu);
    for (auto& p : t->parts)
        if (p->created) files.push_back(p.get());
    return files;
}

// Bytes the store occupies. On disk: every file with its WAL and rollback
// journal (an upper bound, no page counts). In use: each partition file's
// pages less its free pages (SQLite reuses them; pages still in the WAL are
// counted by page_count, so the WAL file itself is not added), plus the type
// files and any rollback journal.
int64_t storeBytes(Engine* e, bool inUse) {
    int64_t total = 0;
    for (Type* t : typesWithFiles(e)) {
        for (Part* f : createdFiles(t)) {
            total += sizeOf(f->path + "-journal");
            if (!inUse) {
                total += sizeOf(f->path) + sizeOf(f->path + "-wal");
                continue;
            }
            int rc = 0;
            Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &rc, nullptr);
            if (!c) {
                total += sizeOf(f->path) + sizeOf(f->path + "-wal");
                continue;
            }
            sqlite3_stmt* q = c->sql("SELECT ((SELECT page_count FROM pragma_page_count) - (SELECT freelist_count FROM"
                                     " pragma_freelist_count)) * (SELECT page_size FROM pragma_page_size)");
            if (q && sqlite3_step(q) == SQLITE_ROW) total += sqlite3_column_int64(q, 0);
            if (q) sqlite3_reset(q);
            e->rpool.release(c);
        }
        for (const std::string* p : {&t->pIdx, &t->pJnl, &t->pFts})
            for (const char* sfx : {"", "-journal"}) total += sizeOf(*p + sfx);
        if (!inUse)
            for (const std::string* p : {&t->pIdx, &t->pJnl, &t->pFts}) total += sizeOf(*p + "-wal");
    }
    return total;
}

// The oldest record of a type by arrival: its seq and ts (seq 0: none).
int32_t oldestOf(Engine* e, Type* t, int64_t* seq, int64_t* ts) {
    *seq = 0;
    *ts = INT64_MAX;
    for (Part* f : createdFiles(t)) {
        int rc = 0;
        Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &rc, nullptr);
        if (!c) return statusOfSqlite(rc);
        sqlite3_stmt* q = c->sql("SELECT seq, ts FROM r ORDER BY seq LIMIT 1");
        int r = q ? sqlite3_step(q) : SQLITE_ERROR;
        if (r == SQLITE_ROW && (!*seq || sqlite3_column_int64(q, 1) < *ts)) {
            *seq = sqlite3_column_int64(q, 0);
            *ts = sqlite3_column_int64(q, 1);
        }
        if (q) sqlite3_reset(q);
        e->rpool.release(c);
        if (r != SQLITE_ROW && r != SQLITE_DONE) return statusOfSqlite(r);
    }
    return P4_OK;
}

// Deletes the type's `want` oldest records by arrival (copies included):
// the cut is merged over its partitions' seq indexes, and each partition's
// writer deletes its rows (per-partition writers).
int32_t quotaPass(Engine* e, Type* t, size_t want, int64_t* records, int64_t* bytes) {
    std::vector<std::pair<int64_t, uint32_t>> all;  // (seq, pid)
    std::vector<Part*> parts;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& p : t->parts)
            if (p->created && p->n > 0) parts.push_back(p.get());
    }
    for (Part* p : parts) {
        int rc = 0;
        Conn* c = e->rpool.acquire(p->path, OpenKind::Reader, &rc, nullptr);
        if (!c) return statusOfSqlite(rc);
        sqlite3_stmt* q = c->sql("SELECT seq FROM r INDEXED BY r_s ORDER BY seq LIMIT ?1");
        if (!q) q = c->sql("SELECT seq FROM r ORDER BY seq LIMIT ?1");
        int r = SQLITE_ERROR;
        if (q) {
            sqlite3_bind_int64(q, 1, int64_t(want));
            while ((r = sqlite3_step(q)) == SQLITE_ROW) all.push_back({sqlite3_column_int64(q, 0), p->pid});
            sqlite3_reset(q);
        }
        e->rpool.release(c);
        if (r != SQLITE_DONE) return statusOfSqlite(r);
    }
    std::sort(all.begin(), all.end());
    // The want oldest distinct seqs, with every partition's copy of each.
    std::map<uint32_t, std::vector<int64_t>> byPid;
    size_t distinct = 0;
    int64_t last = 0;
    for (auto& x : all) {
        if (x.first != last) {
            if (distinct == want) break;
            distinct++;
            last = x.first;
        }
        byPid[x.second].push_back(x.first);
    }
    if (byPid.empty()) return P4_OK;
    Shared* s = new (std::nothrow) Shared();
    if (!s) return P4_E_NOMEM;
    s->kind = Shared::kQuota;
    s->type = t;
    s->remaining.store(int(byPid.size()) + 1);
    for (auto& kv : byPid) {
        WriteTask* wt = new WriteTask();
        wt->op = P4_OPC_QUOTA_GC;
        wt->part = t->partById(kv.first);
        wt->shared = s;
        wt->dels = std::move(kv.second);
        pushTask(e, wt->part, wt);
    }
    finishShared(e, s);  // this thread's own reference
    while (!s->done.load(std::memory_order_acquire)) ps::sleepNs(1000000);
    const int32_t status = s->status.load();
    *records += s->a.load();
    *bytes += s->b.load();
    delete s;
    return status;
}
}  // namespace

int32_t quotaGc(Engine* e, uint64_t maxBytes, int64_t* records, int64_t* bytesFreed, bool enforce) {
    *records = *bytesFreed = 0;
    auto setOver = [&](bool over) {
        if (!enforce) return;
        std::lock_guard<std::mutex> g(e->typesMu);
        for (auto& t : e->types) {
            std::lock_guard<std::mutex> tg(t->mu);
            t->overQuota = over;
        }
    };
    // The file sizes first: a store whose files fit needs no page counts.
    if (uint64_t(storeBytes(e, false)) <= maxBytes) {
        setOver(false);
        return P4_OK;
    }
    const int64_t before = storeBytes(e, true);
    int64_t used = before;
    while (uint64_t(used) > maxBytes && !e->stopping.load()) {
        // The type whose oldest record arrived first gives up its oldest.
        Type* victim = nullptr;
        int64_t oldestTs = INT64_MAX;
        for (Type* t : typesWithFiles(e)) {
            int64_t seq = 0, ts = 0;
            const int32_t rc = oldestOf(e, t, &seq, &ts);
            if (rc != P4_OK) return rc;
            if (seq && ts < oldestTs) {
                oldestTs = ts;
                victim = t;
            }
        }
        if (!victim) {
            // Nothing left to delete: under the configured quota, writes
            // refuse (P4_E_NOSPACE) while reads continue.
            setOver(true);
            return P4_OK;
        }
        // A batch sized to the excess at the store's mean bytes per record
        // (at most 32,768 per transaction); the next pass measures again.
        int64_t held = 0;
        for (Type* t : typesWithFiles(e)) {
            std::lock_guard<std::mutex> g(t->mu);
            held += t->uniq + t->copies;
        }
        const double perRecord = held > 0 ? double(used) / double(held) : double(used);
        const double excess = double(used) - double(maxBytes);
        const size_t want = size_t(std::min(32768.0, std::max(1.0, std::ceil(excess / perRecord))));
        int64_t n = 0, b = 0;
        const int32_t rc = quotaPass(e, victim, want, &n, &b);
        if (rc != P4_OK) return rc;
        *records += n;
        if (n == 0) break;
        used = storeBytes(e, true);
    }
    *bytesFreed = std::max<int64_t>(0, before - used);
    setOver(uint64_t(used) > maxBytes);
    return P4_OK;
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

// Compares (and with fix, repairs) the type index against the partition
// files: its c rows against every file's rows (point probes both ways), and
// the files' counters and lane counts against their rows.
int32_t indexAgainstFiles(Engine* e, Type* t, bool fix, Verify* v) {
    (void)e;
    if (!t->hasFiles.load(std::memory_order_acquire)) return P4_OK;
    std::shared_ptr<const Spec> sp = t->spec();
    int32_t rc = typeIndexFlush(t, true);
    if (rc != P4_OK) return rc;
    std::lock_guard<std::mutex> fg(t->flushMu);
    std::vector<Part*> files = createdFiles(t);
    Conn* x = t->idx;
    // An error ends the pass with that error (M9): a statement that stops
    // early must not read as missing rows, and a fix must not rewrite the
    // index from a partial read.
    std::vector<Conn*> conns;
    std::string where;  // the file being read (diagnostics)
    auto fail = [&](int src) -> int32_t {
        why("verify failed", where, src, 0);
        for (Conn* c : conns) delete c;
        if (fix) x->exec("ROLLBACK");
        return statusOfSqlite(src == SQLITE_OK || src == SQLITE_ROW || src == SQLITE_DONE ? SQLITE_ERROR : src);
    };
    for (Part* f : files) {
        Conn* c = nullptr;
        where = f->path;
        const int orc = openConn(f->path, OpenKind::Maint, 4096, 0, &c, nullptr);
        if (orc != SQLITE_OK) return fail(orc);
        conns.push_back(c);
    }
    if (fix && x->exec("BEGIN IMMEDIATE") != SQLITE_OK) return fail(SQLITE_BUSY);
    // 1. Verify: each file's counters and lanes against its own rows (a fix
    //    recounts them on each partition's writer first: repairPartitions).
    for (size_t fi = 0; fi < files.size() && !fix; fi++) {
        Part* f = files[fi];
        Conn* c = conns[fi];
        where = f->path;
        sqlite3_stmt* s = c->sql("SELECT count(*), coalesce(sum(length(d)),0) FROM r");
        if (!s) return fail(SQLITE_ERROR);
        int64_t n = 0, bytes = 0;
        int src;
        while ((src = sqlite3_step(s)) == SQLITE_ROW) {
            n = sqlite3_column_int64(s, 0);
            bytes = sqlite3_column_int64(s, 1);
        }
        sqlite3_reset(s);
        if (src != SQLITE_DONE) return fail(src);
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
    }
    // 2. CID entries (C-34: the type index is the one CID index). Every
    //    file row has its c entry (cid, pid) -> seq, and every c entry names
    //    a row of its file with that CID: two walks with point probes, no
    //    sort. A fix inserts the missing entries and deletes the dangling
    //    ones, a page at a time.
    {
        sqlite3_stmt* probe = x->sql("SELECT seq FROM c WHERE cid=?1 AND pid=?2");
        sqlite3_stmt* put = x->get(S_C_INS);
        if (!probe || !put) return fail(SQLITE_ERROR);
        for (size_t fi = 0; fi < files.size(); fi++) {
            where = files[fi]->path;
            const uint32_t pid = files[fi]->pid;
            sqlite3_stmt* s = conns[fi]->sql("SELECT seq, cid FROM r");
            if (!s) return fail(SQLITE_ERROR);
            int src;
            while ((src = sqlite3_step(s)) == SQLITE_ROW) {
                if (sqlite3_column_bytes(s, 1) != 32) continue;
                const int64_t seq = sqlite3_column_int64(s, 0);
                v->entries++;
                sqlite3_bind_blob(probe, 1, sqlite3_column_blob(s, 1), 32, SQLITE_TRANSIENT);
                sqlite3_bind_int64(probe, 2, pid);
                const int pr = sqlite3_step(probe);
                const bool ok = pr == SQLITE_ROW && sqlite3_column_int64(probe, 0) == seq;
                sqlite3_reset(probe);
                if (pr != SQLITE_ROW && pr != SQLITE_DONE) {
                    sqlite3_reset(s);
                    return fail(pr);
                }
                if (ok) continue;
                v->mismatches++;
                why("c entry missing", where, seq, pid);
                if (!fix) continue;
                sqlite3_reset(put);
                sqlite3_bind_blob(put, 1, sqlite3_column_blob(s, 1), 32, SQLITE_TRANSIENT);
                sqlite3_bind_int64(put, 2, pid);
                sqlite3_bind_int64(put, 3, seq);
                const int irc = sqlite3_step(put);
                sqlite3_reset(put);
                if (irc != SQLITE_DONE) {
                    sqlite3_reset(s);
                    return fail(irc);
                }
            }
            sqlite3_reset(s);
            if (src != SQLITE_DONE) return fail(src);
        }
        std::unordered_map<uint32_t, Conn*> connOf;
        for (size_t fi = 0; fi < files.size(); fi++) connOf[files[fi]->pid] = conns[fi];
        sqlite3_stmt* page = x->sql("SELECT cid, pid, seq FROM c WHERE (cid, pid) > (?1, ?2) ORDER BY cid, pid LIMIT 65536");
        sqlite3_stmt* del = x->get(S_C_DEL);
        if (!page || !del) return fail(SQLITE_ERROR);
        uint8_t lastKey[32] = {};
        int64_t lastPid = 0;
        for (bool more = true; more;) {
            std::vector<std::pair<std::array<uint8_t, 32>, uint32_t>> dangling;
            sqlite3_bind_blob(page, 1, lastKey, 32, SQLITE_TRANSIENT);
            sqlite3_bind_int64(page, 2, lastPid);
            int src;
            size_t got = 0;
            while ((src = sqlite3_step(page)) == SQLITE_ROW) {
                got++;
                if (sqlite3_column_bytes(page, 0) != 32) continue;
                std::array<uint8_t, 32> key;
                std::memcpy(key.data(), sqlite3_column_blob(page, 0), 32);
                const uint32_t pid = uint32_t(sqlite3_column_int64(page, 1));
                const int64_t seq = sqlite3_column_int64(page, 2);
                std::memcpy(lastKey, key.data(), 32);
                lastPid = pid;
                bool ok = false;
                auto it = connOf.find(pid);
                if (it != connOf.end()) {
                    sqlite3_stmt* q = it->second->get(S_R_LEN);
                    if (!q) {
                        sqlite3_reset(page);
                        return fail(SQLITE_ERROR);
                    }
                    sqlite3_bind_int64(q, 1, seq);
                    const int r = sqlite3_step(q);
                    ok = r == SQLITE_ROW && sqlite3_column_bytes(q, 1) == 32 && std::memcmp(sqlite3_column_blob(q, 1), key.data(), 32) == 0;
                    sqlite3_reset(q);
                    if (r != SQLITE_ROW && r != SQLITE_DONE) {
                        sqlite3_reset(page);
                        return fail(r);
                    }
                }
                if (ok) continue;
                v->mismatches++;
                why("c entry dangling", t->name, seq, pid);
                if (fix) dangling.push_back({key, pid});
            }
            sqlite3_reset(page);
            if (src != SQLITE_DONE) return fail(src);
            more = got == 65536;
            for (auto& d : dangling) {
                sqlite3_reset(del);
                sqlite3_bind_blob(del, 1, d.first.data(), 32, SQLITE_TRANSIENT);
                sqlite3_bind_int64(del, 2, d.second);
                const int r = sqlite3_step(del);
                sqlite3_reset(del);
                if (r != SQLITE_DONE) return fail(r);
            }
        }
    }
    // 3. The CID buckets against c (a fix writes them from c).
    {
        where = t->name;
        std::vector<int64_t> want, have;
        int r = cbCount(x, &want);
        if (r == SQLITE_OK) r = cbRead(x, &have);
        if (r != SQLITE_OK) return fail(r);
        for (int b = 0; b < kCidBuckets; b++)
            if (want[size_t(b)] != have[size_t(b)]) {
                v->mismatches++;
                why("CID bucket", where, have[size_t(b)], want[size_t(b)]);
            }
        if (fix && want != have && (r = cbWrite(x, want)) != SQLITE_OK) return fail(r);
    }
    for (Conn* c : conns) delete c;
    conns.clear();
    if (fix) {
        const int crc = x->exec("COMMIT");
        if (crc != SQLITE_OK) return fail(crc);
    }
    return P4_OK;
}
}  // namespace

namespace {
// REBUILD 2's recount: each partition's counters and lane rows from its own
// rows and tag instances, on the partition's writer (per-partition writers),
// written back to its file; the waiter is this maintenance work.
int32_t repairPartitions(Engine* e, Type* t, Verify* v) {
    std::vector<Part*> parts = createdFiles(t);
    if (parts.empty()) return P4_OK;
    Shared* s = new (std::nothrow) Shared();
    if (!s) return P4_E_NOMEM;
    s->kind = Shared::kRepair;
    s->type = t;
    s->remaining.store(int(parts.size()) + 1);
    for (Part* p : parts) {
        WriteTask* wt = new WriteTask();
        wt->op = P4_OPC_REBUILD;
        wt->part = p;
        wt->shared = s;
        pushTask(e, p, wt);
    }
    finishShared(e, s);  // this thread's own reference
    while (!s->done.load(std::memory_order_acquire)) ps::sleepNs(1000000);
    const int32_t status = s->status.load();
    v->mismatches += s->b.load();
    delete s;
    return status;
}

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
            for (Part* f : createdFiles(t)) {
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
            int32_t rc = repairPartitions(e, t, &v);
            if (rc != P4_OK) return rc;
            rc = indexAgainstFiles(e, t, true, &v);
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
            for (Part* f : createdFiles(t)) paths.push_back(f->path);
            if (t->hasFiles.load()) {
                paths.push_back(t->pIdx);
                paths.push_back(t->pJnl);
            }
            for (const std::string& path : paths)
                if (!fileIntact(path)) {
                    v.mismatches++;
                    if (firstBad && firstBad->empty()) *firstBad = path;
                }
            // Each partition's object count and epoch histogram against its rows.
            for (Part* f : createdFiles(t))
                if (!derivedIntact(t, f->path)) {
                    v.mismatches++;
                    if (firstBad && firstBad->empty()) *firstBad = f->path + " (object count / epoch histogram)";
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
    if (!sp->fullText || !t->hasFiles.load(std::memory_order_acquire)) return P4_OK;  // no data: no .fts yet
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
        sqlite3_wal_hook(c->db, walHook, e);
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
            if (p->created && p->maxseq > t->ftsThrough) paths.push_back(p->path);
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
        int64_t records = 0, bytes = 0;
        const int32_t rc = maxBytes ? quotaGc(e, maxBytes, &records, &bytes, false) : P4_OK;
        if (rc == P4_OK) {
            out.enc.beginRow();
            out.enc.i64(0);  // files_dropped: a partition keeps its file (C-32)
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
    int sizeTicks = 0;
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
                case MaintTask::kClose: {
                    // An evicted writer connection: its WAL copied first, so a
                    // close that is not the file's last leaves nothing behind
                    // (the last one deletes the WAL).
                    const char* fn = sqlite3_db_filename(mt.conn->db, "main");
                    const std::string path = fn ? fn : mt.conn->path;
                    int log = 0, ck = 0;
                    const int r = sqlite3_wal_checkpoint_v2(mt.conn->db, nullptr, SQLITE_CHECKPOINT_PASSIVE, &log, &ck);
                    delete mt.conn;
                    if (!ioExists(path + "-wal")) {
                        walNote(e, path, 0);
                        walExtNote(e, path, 0);
                    } else if (r == SQLITE_OK) {
                        walNote(e, path, log >= ck ? log - ck : 0);
                    }
                    break;
                }
                case MaintTask::kCheckpoint:
                    {
                        std::lock_guard<std::mutex> g(e->walMu);
                        e->ckptQueued.erase(mt.path);
                    }
                    checkpoint(e, m, mt.path);
                    break;
                case MaintTask::kSlot: {
                    std::lock_guard<std::mutex> g(e->slowMu);
                    e->slowQ.push_back(mt.slot);
                    e->slowCv.notify_one();
                    break;
                }
            }
        }
        if (e->stopping.load()) break;
        const uint64_t now = monoNs();
        if (now - lastTick >= 100ull * 1000 * 1000) {
            lastTick = now;
            // Over the WAL total with files that no longer commit (their
            // hook never fires again): the largest WALs, down to half the total.
            std::vector<std::string> idle;
            {
                std::lock_guard<std::mutex> g(e->walMu);
                if (uint64_t(e->walSum) * 4096 > e->cfg.walTotal) {
                    std::vector<std::pair<int64_t, std::string>> big;
                    for (auto& kv : e->walPages)
                        if (!e->ckptQueued.count(kv.first)) big.push_back({kv.second, kv.first});
                    std::sort(big.begin(), big.end(), [](const auto& a, const auto& b) { return a.first > b.first; });
                    int64_t sum = e->walSum;
                    for (auto& b2 : big) {
                        if (uint64_t(sum) * 4096 <= e->cfg.walTotal / 2) break;
                        sum -= b2.first;
                        e->ckptQueued.insert(b2.second);
                        idle.push_back(b2.second);
                    }
                }
            }
            if (!idle.empty()) {
                std::lock_guard<std::mutex> g(e->maintMu);
                for (auto& pth : idle) {
                    MaintTask mt;
                    mt.kind = MaintTask::kCheckpoint;
                    mt.path = pth;
                    e->maintQ.push_back(mt);
                }
            }
            const std::vector<Type*> types = typesWithFiles(e);
            uint64_t pending = 0;
            for (Type* t : types) {
                std::lock_guard<std::mutex> g(t->mu);
                pending += t->pend.bytes();
            }
            for (Type* t : types) typeIndexFlush(t, pending >= e->cfg.pendingBytes, false);
            if (++sizeTicks >= 10) {  // the T/ files' sizes for SUMMARY 4, once a second
                sizeTicks = 0;
                for (Type* t : types) typeFileBytes(t);
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

// The long work: REBUILD and QUOTA_GC slots in arrival order, then every
// 100 ms the full-text catch-up and once a second the configured quota.
void slowLoop(Engine* e) {
    uint64_t lastTick = 0;
    int quotaTicks = 0;
    // A fresh process: each type index read once, start to end, so the first
    // writes' dedupe probes and the first reads' CID probes find its pages in
    // the host's cache instead of reading them one at a time (a cold type
    // index cost the first ingest calls 2-6x a warm call).
    for (Type* t : typesWithFiles(e)) {
        if (e->stopping.load()) break;
        ioPrefault(t->pIdx);
    }
    for (;;) {
        uint32_t slot = 0;
        bool have = false;
        std::vector<uint32_t> refused;
        {
            std::unique_lock<std::mutex> g(e->slowMu);
            if (e->slowQ.empty() && !e->slowStop) e->slowCv.wait_for(g, std::chrono::milliseconds(100));
            if (e->slowStop) {
                refused.assign(e->slowQ.begin(), e->slowQ.end());
                e->slowQ.clear();
            } else if (!e->slowQ.empty()) {
                slot = e->slowQ.front();
                e->slowQ.pop_front();
                have = true;
            }
        }
        if (e->slowStop) {
            // Stopping: queued REBUILD / QUOTA_GC calls are refused, not run.
            for (uint32_t s : refused) {
                SlotOut out(e, s);
                out.enc.header({});
                out.end(P4_E_STOPPED, "stopping");
            }
            return;
        }
        if (have) {
            e->slot(slot)->thread = e->maintThread;
            runMaintSlot(e, slot);
            continue;
        }
        if (e->stopping.load()) continue;  // stop drains the queue, then returns
        const uint64_t now = monoNs();
        if (now - lastTick < 100ull * 1000 * 1000) continue;
        lastTick = now;
        for (Type* t : typesWithFiles(e)) ftsCatchUp(e, t, false);
        const uint64_t q = e->quota.load();
        if (q && ++quotaTicks >= 10) {
            quotaTicks = 0;
            int64_t a, bb;
            quotaGc(e, q, &a, &bb, true);
        }
    }
}

}  // namespace p4
}  // namespace flatsql
