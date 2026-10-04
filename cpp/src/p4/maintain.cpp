// Store format 4: the maintenance thread (design §5.3, §7, §8). Never on a
// caller's path: type-index flushes, PASSIVE/RESTART checkpoints, closing
// evicted writer connections (a close may checkpoint), QUOTA_GC and REBUILD,
// and the background FTS index.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <queue>
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
    t->idxBytes.store(std::max<int64_t>(0, ioSize(t->pIdx)), std::memory_order_relaxed);
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

// A SQLite file's pages less its free pages (SQLite reuses them; pages still
// in the WAL are counted by page_count). The file's size when it cannot be
// opened.
int64_t pagesInUse(Conn* c) {
    sqlite3_stmt* q = c->sql("SELECT ((SELECT page_count FROM pragma_page_count) - (SELECT freelist_count FROM"
                             " pragma_freelist_count)) * (SELECT page_size FROM pragma_page_size)");
    int64_t n = -1;
    if (q && sqlite3_step(q) == SQLITE_ROW) n = sqlite3_column_int64(q, 0);
    if (q) sqlite3_reset(q);
    return n;
}

int64_t typeFileInUse(const std::string& path) {
    if (sizeOf(path) == 0) return 0;
    Conn* c = nullptr;
    if (openConn(path, OpenKind::IndexReader, 64, 0, &c, nullptr) != SQLITE_OK) return sizeOf(path) + sizeOf(path + "-wal");
    const int64_t n = pagesInUse(c);
    delete c;
    return n >= 0 ? n : sizeOf(path) + sizeOf(path + "-wal");
}

// Bytes the store occupies. On disk: every file with its WAL and rollback
// journal (an upper bound, no page counts), and every stream (its committed
// end, plus a generation a compaction replaced until it goes). In use: every
// index file's pages less its free pages (the WAL files are not added), the
// type files alike, plus each stream's live frames (deleted records' frames
// stay on disk until a compaction gives them back, but are not in use), plus
// any rollback journal.
int64_t storeBytes(Engine* e, bool inUse) {
    int64_t total = 0;
    for (Type* t : typesWithFiles(e)) {
        for (Feed* f : createdFeeds(t)) {
            total += sizeOf(f->path + "-journal");
            {
                std::lock_guard<std::mutex> g(t->mu);
                total += inUse ? f->k.fbytes : f->streamBytes;
            }
            if (!inUse) {
                {
                    std::lock_guard<std::mutex> g(f->smu);
                    total += f->retiredBytes;
                }
                total += sizeOf(f->path) + sizeOf(f->path + "-wal");
                continue;
            }
            int rc = 0;
            Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &rc, nullptr);
            if (!c) {
                total += sizeOf(f->path) + sizeOf(f->path + "-wal");
                continue;
            }
            const int64_t n = pagesInUse(c);
            total += n >= 0 ? n : sizeOf(f->path) + sizeOf(f->path + "-wal");
            e->rpool.release(c);
        }
        for (const std::string* p : {&t->pIdx, &t->pFts}) {
            total += sizeOf(*p + "-journal");
            total += inUse ? typeFileInUse(*p) : sizeOf(*p) + sizeOf(*p + "-wal");
        }
    }
    return total;
}

// The type's feed files merged by arrival (C-38 (4)): (seq, feed) pairs in
// seq order, ties by feed id, read a chunk at a time from each file's r_s
// (its rows before REBUILD 1). A record's rows in a file count once.
class SeqMerge {
public:
    SeqMerge(Engine* e, Type* t, int64_t after, int64_t through, size_t chunk) : e_(e), through_(through), chunk_(chunk) {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& f : t->feeds) {
            if (!f->created || f->k.recs <= 0 || f->quarantined) continue;
            S s;
            s.fid = f->fid;
            s.path = f->path;
            s.indexed = f->indexed;
            s.last = after;
            ss_.push_back(std::move(s));
        }
    }
    // 1: an entry; 0: the end; < 0: a status.
    int32_t next(int64_t* seq, uint32_t* fid) {
        if (!started_) {
            started_ = true;
            for (size_t i = 0; i < ss_.size(); i++) {
                const int32_t rc = fill(ss_[i]);
                if (rc != P4_OK) return rc;
                if (!ss_[i].buf.empty()) heap_.push(Head{ss_[i].buf.front(), ss_[i].fid, i});
            }
        }
        if (heap_.empty()) return 0;
        const Head h = heap_.top();
        heap_.pop();
        S& s = ss_[h.i];
        s.buf.pop_front();
        if (s.buf.empty() && !s.done) {
            const int32_t rc = fill(s);
            if (rc != P4_OK) return rc;
        }
        if (!s.buf.empty()) heap_.push(Head{s.buf.front(), s.fid, h.i});
        *seq = h.seq;
        *fid = h.fid;
        return 1;
    }

private:
    struct S {
        uint32_t fid = 0;
        std::string path;
        bool indexed = true, done = false;
        int64_t last = 0;
        std::deque<int64_t> buf;
    };
    struct Head {
        int64_t seq;
        uint32_t fid;
        size_t i;
        bool operator<(const Head& o) const { return seq != o.seq ? seq > o.seq : fid > o.fid; }  // a min-heap
    };
    int32_t fill(S& s) {
        if (s.done) return P4_OK;
        int orc = 0;
        Conn* c = e_->rpool.acquire(s.path, OpenKind::Reader, &orc, nullptr);
        if (!c) return statusOfSqlite(orc);
        sqlite3_stmt* q = c->sql(s.indexed ? "SELECT seq FROM r INDEXED BY r_s WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3"
                                           : "SELECT seq FROM r WHERE rid>=((?1+1)<<16) AND rid<=((?2<<16)|65535) ORDER BY rid LIMIT ?3");
        int32_t st = q ? P4_OK : P4_E_INTERNAL;
        size_t got = 0;
        if (q) {
            sqlite3_bind_int64(q, 1, s.last);
            sqlite3_bind_int64(q, 2, through_);
            sqlite3_bind_int64(q, 3, int64_t(chunk_));
            int r;
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                got++;
                const int64_t v = sqlite3_column_int64(q, 0);
                if (v != s.last) s.buf.push_back(v);
                s.last = v;
            }
            sqlite3_reset(q);
            if (r != SQLITE_DONE) st = statusOfSqlite(r);
        }
        e_->rpool.release(c);
        if (st == P4_OK && got < chunk_) s.done = true;
        if (st == P4_OK && s.buf.empty() && !s.done) return fill(s);  // a chunk of one record's rows
        return st;
    }
    Engine* e_;
    int64_t through_;
    size_t chunk_;
    std::vector<S> ss_;
    std::priority_queue<Head> heap_;
    bool started_ = false;
};

// The oldest record row sets of a type by arrival: up to `want` (feed, seq)
// pairs, and the first one's ts.
int32_t oldestOf(Engine* e, Type* t, size_t want, std::vector<std::pair<uint32_t, int64_t>>* seqs, int64_t* ts) {
    seqs->clear();
    *ts = INT64_MAX;
    const int64_t vis = t->vis.load(std::memory_order_acquire);
    SeqMerge m(e, t, 0, vis, std::min<size_t>(want, 4096));
    while (seqs->size() < want) {
        int64_t seq = 0;
        uint32_t fid = 0;
        const int32_t rc = m.next(&seq, &fid);
        if (rc < 0) return rc;
        if (rc == 0) break;
        seqs->push_back({fid, seq});
    }
    if (!seqs->empty()) {
        Feed* f;
        {
            std::lock_guard<std::mutex> g(t->mu);
            f = t->feedById((*seqs)[0].first);
        }
        int orc = 0;
        Conn* rc = f ? e->rpool.acquire(f->path, OpenKind::Reader, &orc, nullptr) : nullptr;
        if (rc) {
            sqlite3_stmt* q2 = rc->sql("SELECT ts FROM r WHERE rid>=?1 AND rid<=?2 LIMIT 1");
            if (q2) {
                sqlite3_bind_int64(q2, 1, (*seqs)[0].second << 16);
                sqlite3_bind_int64(q2, 2, ((*seqs)[0].second << 16) | 0xffff);
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
            held += t->totals().recs;
        }
        const double perRecord = held > 0 ? double(used) / double(held) : double(used);
        const double excess = double(used) - double(maxBytes);
        const size_t want = size_t(std::min(32768.0, std::max(1.0, std::ceil(excess / perRecord))));
        // The type whose oldest record arrived first gives up its oldest.
        Type* victim = nullptr;
        int64_t oldestTs = INT64_MAX;
        std::vector<std::pair<uint32_t, int64_t>> victimSeqs;
        for (Type* t : typesWithFiles(e)) {
            std::vector<std::pair<uint32_t, int64_t>> seqs;
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
        ftsCatchUp(e, victim, false);  // the evicted records' full-text rows go before the next measure
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
    // Seqs whose row set left a feed file since the last pass: their
    // full-text rows go, for those the full text holds (a seq past
    // ftsThrough was never added, and catching up skips it: no rows). A
    // migrated seq (below the store's seq floor) can be held by several feed
    // files: its row goes once none holds it.
    std::vector<int64_t> gone;
    {
        std::lock_guard<std::mutex> gg(t->ftsGoneMu);
        gone.swap(t->ftsGone);
    }
    if (!gone.empty()) {
        std::vector<Feed*> files = createdFeeds(t);
        std::vector<int64_t> del;
        bool ok = true;
        for (int64_t seq : gone) {
            if (seq > t->ftsThrough) continue;
            bool held = false;
            if (seq < int64_t(e->cfg.gseqFloor) && files.size() > 1)
                for (Feed* f : files) {
                    int orc = 0;
                    Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &orc, nullptr);
                    if (!c) {
                        ok = false;
                        break;
                    }
                    const int32_t st = fileHoldsSeq(c, seq, &held);
                    e->rpool.release(c);
                    if (st != P4_OK) ok = false;
                    if (held || !ok) break;
                }
            if (!ok) break;
            if (!held) del.push_back(seq);
        }
        sqlite3_stmt* q = ok ? t->fts->sql("DELETE FROM fts WHERE rowid=?1") : nullptr;
        ok = q && t->fts->exec("BEGIN IMMEDIATE") == SQLITE_OK;
        for (size_t i = 0; ok && i < del.size(); i++) {
            sqlite3_bind_int64(q, 1, del[i]);
            const int r = sqlite3_step(q);
            sqlite3_reset(q);
            ok = r == SQLITE_DONE;
        }
        if (ok) ok = t->fts->exec("COMMIT") == SQLITE_OK;
        if (!ok) {
            if (q) t->fts->exec("ROLLBACK");
            std::lock_guard<std::mutex> gg(t->ftsGoneMu);  // the next pass retries
            t->ftsGone.insert(t->ftsGone.end(), gone.begin(), gone.end());
        }
    }
    const int64_t vis = t->vis.load(std::memory_order_acquire);
    if (t->ftsThrough >= vis) {
        t->ftsState = 2;
        return P4_OK;
    }
    t->ftsState = 1;
    if (e->ftsHold.load()) return P4_OK;
    const std::string fid(reinterpret_cast<const char*>(sp->tc.fid()), 4);
    // Pages of the feed files merged by seq, each seq read once from the
    // first feed file holding it, a file at a time.
    int64_t through = t->ftsThrough;
    const int64_t limit = all ? INT64_MAX : 20000;
    int64_t done = 0;
    int32_t status = P4_OK;
    SeqMerge merge(e, t, through, vis, 1024);
    bool end = false;
    while (status == P4_OK && done < limit && !end) {
        std::vector<std::pair<int64_t, uint32_t>> page;  // (seq, fid)
        while (page.size() < 4096) {
            int64_t seq = 0;
            uint32_t ffid = 0;
            const int32_t rc = merge.next(&seq, &ffid);
            if (rc < 0) {
                status = rc;
                break;
            }
            if (rc == 0) {
                end = true;
                break;
            }
            if (!page.empty() && page.back().first == seq) continue;  // a migrated seq held by several feeds
            page.push_back({seq, ffid});
        }
        if (status != P4_OK) break;
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
            sqlite3_stmt* s = c->sql("SELECT off, len, f FROM r WHERE rid>=?1 AND rid<=?2 LIMIT 1");
            // The bytes come from the stream generation this read transaction
            // sees (a compaction may have replaced it since: then a new one).
            std::shared_ptr<Stream> stream;
            for (int tries = 0; tries < 4 && !stream && status == P4_OK; tries++) {
                if (tries) c->exec("COMMIT");
                c->exec("BEGIN");
                uint32_t gen = 0;
                int32_t st = snapGen(c, &gen);
                if (st == P4_OK) stream = streamAt(f, gen, &st);
                if (st != P4_OK) status = st;
            }
            if (!stream && status == P4_OK) status = P4_E_BUSY;
            std::string bytes;
            for (int64_t seq : kv.second) {
                if (!s || status != P4_OK) break;
                sqlite3_bind_int64(s, 1, seq << 16);
                sqlite3_bind_int64(s, 2, (seq << 16) | 0xffff);
                if (sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 2) == SQLITE_NULL &&
                    streamRead(stream.get(), sqlite3_column_int64(s, 0), sqlite3_column_int64(s, 1), &bytes) == P4_OK) {
                    std::string text, err;
                    if (reflectedRecordSearchText(sp->tc.bfbs().data(), sp->tc.bfbs().size(), fid,
                                                  reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), text, &err)) {
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
            if (status != P4_OK) break;
        }
        if (status != P4_OK) {
            t->fts->exec("ROLLBACK");
            break;
        }
        through = end ? vis : page.back().first;
        sqlite3_stmt* m = t->fts->sql("INSERT OR REPLACE INTO ftsmeta(k, v) VALUES('through', ?1)");
        sqlite3_bind_int64(m, 1, through);
        sqlite3_step(m);
        sqlite3_reset(m);
        t->fts->exec("COMMIT");
        t->ftsThrough = through;
        done += int64_t(page.size());
    }
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
        // Streams: a generation a compaction replaced goes once its grace is
        // over (unlinked when the last reader holding it lets go), and a
        // stream more than half dead is compacted on its type's writer.
        for (Type* t : typesWithFiles(e)) {
            std::vector<uint32_t> due;
            std::vector<Feed*> fs;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& f : t->feeds) {
                    fs.push_back(f.get());
                    if (compactDue(f.get())) due.push_back(f->fid);
                }
            }
            for (Feed* f : fs) {
                std::shared_ptr<Stream> gone;
                std::lock_guard<std::mutex> g(f->smu);
                if (f->retired && now >= f->retireAt) {
                    gone.swap(f->retired);
                    f->retiredBytes = 0;
                    e->bump(kStUnlinked);
                }
            }
            for (uint32_t fid : due) {
                if (e->stopping.load()) break;
                Internal in;
                in.what = fid;
                runOnWriter(e, t, kOpCompact, &in);
            }
        }
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
