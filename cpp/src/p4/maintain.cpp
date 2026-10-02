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

// ---- work on a type's writer -----------------------------------------------------------------------
int32_t runOnWriter(Engine* e, Type* t, int op, Internal* in) {
    WriteTask* wt = new (std::nothrow) WriteTask();
    if (!wt) return P4_E_NOMEM;
    wt->op = op;
    wt->type = t;
    wt->internal = in;
    pushTask(e, t, wt);
    while (!in->done.load(std::memory_order_acquire)) ps::sleepNs(1000000);
    return in->status;
}

// ---- QUOTA_GC: the oldest records by arrival (C-37 (9)) --------------------------------------------
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

std::vector<Feed*> createdFeeds(Type* t) {
    std::vector<Feed*> files;
    std::lock_guard<std::mutex> g(t->mu);
    for (auto& f : t->feeds)
        if (f->created) files.push_back(f.get());
    return files;
}

// Bytes the store occupies. On disk: every file with its WAL and rollback
// journal (an upper bound, no page counts). In use: each feed file's pages
// less its free pages (SQLite reuses them; pages still in the WAL are counted
// by page_count, so the WAL file itself is not added), plus the type files
// and any rollback journal.
int64_t storeBytes(Engine* e, bool inUse) {
    int64_t total = 0;
    for (Type* t : typesWithFiles(e)) {
        for (Feed* f : createdFeeds(t)) {
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

// The oldest records of a type by arrival: up to `want` seqs (the type
// index's seq order), and the first one's ts.
int32_t oldestOf(Engine* e, Type* t, size_t want, std::vector<int64_t>* seqs, int64_t* ts) {
    seqs->clear();
    *ts = INT64_MAX;
    Conn* c = nullptr;
    if (openConn(t->pIdx, OpenKind::IndexReader, 1024, 0, &c, nullptr) != SQLITE_OK) return P4_E_IO;
    const int64_t vis = t->vis.load(std::memory_order_acquire);
    sqlite3_stmt* q = c->sql("SELECT DISTINCT seq FROM x WHERE seq<=?1 ORDER BY seq LIMIT ?2");
    int r = SQLITE_ERROR;
    if (q) {
        sqlite3_bind_int64(q, 1, vis);
        sqlite3_bind_int64(q, 2, int64_t(want));
        while ((r = sqlite3_step(q)) == SQLITE_ROW) seqs->push_back(sqlite3_column_int64(q, 0));
        sqlite3_reset(q);
    }
    uint32_t fid = 0;
    if (r == SQLITE_DONE && !seqs->empty()) {
        sqlite3_stmt* f = c->sql("SELECT fid FROM x WHERE seq=?1 LIMIT 1");
        if (f) {
            sqlite3_bind_int64(f, 1, (*seqs)[0]);
            if (sqlite3_step(f) == SQLITE_ROW) fid = uint32_t(sqlite3_column_int64(f, 0));
            sqlite3_reset(f);
        }
    }
    delete c;
    if (r != SQLITE_DONE) return statusOfSqlite(r);
    if (fid) {
        Feed* f;
        {
            std::lock_guard<std::mutex> g(t->mu);
            f = t->feedById(fid);
        }
        int orc = 0;
        Conn* rc = f ? e->rpool.acquire(f->path, OpenKind::Reader, &orc, nullptr) : nullptr;
        if (rc) {
            sqlite3_stmt* q2 = rc->sql("SELECT ts FROM r WHERE rid>=?1 AND rid<=?2 LIMIT 1");
            if (q2) {
                sqlite3_bind_int64(q2, 1, (*seqs)[0] << 16);
                sqlite3_bind_int64(q2, 2, ((*seqs)[0] << 16) | 0xffff);
                if (sqlite3_step(q2) == SQLITE_ROW) *ts = sqlite3_column_int64(q2, 0);
                sqlite3_reset(q2);
            }
            e->rpool.release(rc);
        }
        if (*ts == INT64_MAX) *ts = INT64_MIN;  // unknown: oldest
    }
    return P4_OK;
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
        // A batch sized to the excess at the store's mean bytes per record
        // (at most 32,768 per transaction); the next pass measures again.
        int64_t held = 0;
        for (Type* t : typesWithFiles(e)) {
            std::lock_guard<std::mutex> g(t->mu);
            held += t->uniq;
        }
        const double perRecord = held > 0 ? double(used) / double(held) : double(used);
        const double excess = double(used) - double(maxBytes);
        const size_t want = size_t(std::min(32768.0, std::max(1.0, std::ceil(excess / perRecord))));
        // The type whose oldest record arrived first gives up its oldest.
        Type* victim = nullptr;
        int64_t oldestTs = INT64_MAX;
        std::vector<int64_t> victimSeqs;
        for (Type* t : typesWithFiles(e)) {
            std::vector<int64_t> seqs;
            int64_t ts = 0;
            const int32_t rc = oldestOf(e, t, want, &seqs, &ts);
            if (rc != P4_OK) return rc;
            if (!seqs.empty() && (!victim || ts < oldestTs)) {
                oldestTs = ts;
                victim = t;
                victimSeqs.swap(seqs);
            }
        }
        if (!victim) {
            // Nothing left to delete: under the configured quota, writes
            // refuse (P4_E_NOSPACE) while reads continue.
            setOver(true);
            return P4_OK;
        }
        Internal in;
        in.seqs = std::move(victimSeqs);
        const int32_t rc = runOnWriter(e, victim, P4_OPC_QUOTA_GC, &in);
        if (rc != P4_OK) return rc;
        *records += in.a;
        if (in.a == 0) break;
        used = storeBytes(e, true);
    }
    *bytesFreed = std::max<int64_t>(0, before - used);
    setOver(uint64_t(used) > maxBytes);
    return P4_OK;
}

// ---- REBUILD ------------------------------------------------------------------------------------------
namespace {
int32_t rebuildOp(Engine* e, Type* only, uint32_t what, std::vector<std::array<int64_t, 2>>* rows,
                  std::vector<Type*>* rowTypes, std::string* firstBad) {
    std::vector<Type*> types;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        for (auto& t : e->types)
            if (!only || t.get() == only) types.push_back(t.get());
    }
    for (Type* t : types) {
        int64_t entries = 0, mismatches = 0;
        if (what & (1 | 2 | 8)) {
            Internal in;
            in.what = what & (1 | 2 | 8);
            const int32_t rc = runOnWriter(e, t, P4_OPC_REBUILD, &in);
            if (rc != P4_OK) return rc;
            entries = in.a;
            mismatches = in.b;
            if (firstBad && firstBad->empty() && !in.firstBad.empty()) *firstBad = in.firstBad;
        }
        if (what & 4) {
            {
                std::lock_guard<std::mutex> g(t->ftsMu);
                if (t->fts) t->fts->exec("DELETE FROM fts");
                t->ftsThrough = 0;
            }
            ftsCatchUp(e, t, true);
        }
        rows->push_back({entries, mismatches});
        rowTypes->push_back(t);
    }
    return P4_OK;
}
}  // namespace

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
    const std::string fid(reinterpret_cast<const char*>(sp->tc.fid()), 4);
    // Pages of the type index's seq order, each record read once from one of
    // its feed files (the first), a file at a time.
    Conn* x = nullptr;
    if (openConn(t->pIdx, OpenKind::IndexReader, 1024, 0, &x, nullptr) != SQLITE_OK) return P4_E_IO;
    int64_t through = t->ftsThrough;
    const int64_t limit = all ? INT64_MAX : 20000;
    int64_t done = 0;
    int32_t status = P4_OK;
    while (status == P4_OK && done < limit) {
        std::vector<std::pair<int64_t, uint32_t>> page;  // (seq, fid)
        sqlite3_stmt* q = x->sql("SELECT seq, min(fid) FROM x WHERE seq>?1 AND seq<=?2 GROUP BY seq ORDER BY seq LIMIT 4096");
        if (!q) {
            status = P4_E_INTERNAL;
            break;
        }
        sqlite3_bind_int64(q, 1, through);
        sqlite3_bind_int64(q, 2, vis);
        int r;
        while ((r = sqlite3_step(q)) == SQLITE_ROW) page.push_back({sqlite3_column_int64(q, 0), uint32_t(sqlite3_column_int64(q, 1))});
        sqlite3_reset(q);
        if (r != SQLITE_DONE) {
            status = statusOfSqlite(r);
            break;
        }
        if (page.empty()) {
            through = vis;
            break;
        }
        std::map<uint32_t, std::vector<int64_t>> byFile;
        for (auto& p : page) byFile[p.second].push_back(p.first);
        t->fts->exec("BEGIN IMMEDIATE");
        for (auto& kv : byFile) {
            Feed* f;
            {
                std::lock_guard<std::mutex> tg(t->mu);
                f = t->feedById(kv.first);
            }
            if (!f) continue;
            int rc = 0;
            Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &rc, nullptr);
            if (!c) {
                status = statusOfSqlite(rc);
                break;
            }
            sqlite3_stmt* s = c->sql("SELECT d, f FROM r WHERE rid>=?1 AND rid<=?2 LIMIT 1");
            c->exec("BEGIN");
            for (int64_t seq : kv.second) {
                if (!s) break;
                sqlite3_bind_int64(s, 1, seq << 16);
                sqlite3_bind_int64(s, 2, (seq << 16) | 0xffff);
                if (sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 1) == SQLITE_NULL) {
                    std::string text, err;
                    const auto* d = static_cast<const uint8_t*>(sqlite3_column_blob(s, 0));
                    if (reflectedRecordSearchText(sp->tc.bfbs().data(), sp->tc.bfbs().size(), fid, d,
                                                  size_t(sqlite3_column_bytes(s, 0)), text, &err)) {
                        sqlite3_stmt* ins = t->fts->sql("INSERT OR REPLACE INTO fts(rowid, t) VALUES(?1,?2)");
                        sqlite3_bind_int64(ins, 1, seq);
                        sqlite3_bind_text(ins, 2, text.data(), int(text.size()), SQLITE_TRANSIENT);
                        sqlite3_step(ins);
                        sqlite3_reset(ins);
                        e->bump(kStFtsRows);
                    }
                }
                sqlite3_reset(s);
            }
            c->exec("COMMIT");
            e->rpool.release(c);
        }
        if (status != P4_OK) {
            t->fts->exec("ROLLBACK");
            break;
        }
        through = page.back().first;
        sqlite3_stmt* m = t->fts->sql("INSERT OR REPLACE INTO ftsmeta(k, v) VALUES('through', ?1)");
        sqlite3_bind_int64(m, 1, through);
        sqlite3_step(m);
        sqlite3_reset(m);
        t->fts->exec("COMMIT");
        t->ftsThrough = through;
        done += int64_t(page.size());
        if (page.size() < 4096) {
            through = vis;
            break;
        }
    }
    delete x;
    if (status == P4_OK && through >= vis) {
        sqlite3_stmt* m = t->fts->sql("INSERT OR REPLACE INTO ftsmeta(k, v) VALUES('through', ?1)");
        sqlite3_bind_int64(m, 1, vis);
        sqlite3_step(m);
        sqlite3_reset(m);
        t->ftsThrough = vis;
    }
    t->ftsState = t->ftsThrough >= vis ? 2 : 1;
    return status;
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
