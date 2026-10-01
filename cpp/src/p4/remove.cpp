// Store format 4: removals (design §4 batch supersede W-f/W-g, delete W-i,
// §7 empty files). Each runs on the partition's owner writer (or the
// maintenance thread for file drops): delete intents are journaled before the
// file commits, counters change in the same transaction, a lane row goes at
// 0 and a file is unlinked at 0 rows.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {

struct Gone {
    int64_t seq = 0, len = 0, w = 0;
    uint8_t key[32];
    sqlite3_value* k = nullptr;
    int others = 0;
};
struct Inst {
    uint32_t sid, lane;
    int64_t seq;
};

int32_t journalRows(Type* t, const std::vector<std::array<int64_t, 6>>& rows, const std::vector<std::array<uint8_t, 32>>& keys,
                    int64_t* first) {
    *first = 0;
    if (rows.empty()) return P4_OK;
    std::lock_guard<std::mutex> jg(t->jmu);
    Conn* j = t->jdb;
    int rc = j->exec("BEGIN IMMEDIATE");
    for (size_t i = 0; i < rows.size() && rc == SQLITE_OK; i++) {
        const auto& r = rows[i];  // op, tb, pid, seq, gen, v
        sqlite3_stmt* q = j->get(S_J_INS);
        if (!q) { rc = SQLITE_ERROR; break; }
        sqlite3_bind_int(q, 1, int(r[0]));
        sqlite3_bind_int64(q, 2, r[1]);
        if (i < keys.size() && r[0] != J_FILE && r[0] != J_DROP) sqlite3_bind_blob(q, 3, keys[i].data(), 32, SQLITE_STATIC);
        else sqlite3_bind_null(q, 3);
        sqlite3_bind_null(q, 4);
        sqlite3_bind_int64(q, 5, r[2]);
        sqlite3_bind_int64(q, 6, r[3]);
        sqlite3_bind_int64(q, 7, r[4]);
        sqlite3_bind_null(q, 8);
        sqlite3_bind_int64(q, 9, r[5]);
        const int s = sqlite3_step(q);
        sqlite3_reset(q);
        if (s != SQLITE_DONE) rc = s;
    }
    if (rc == SQLITE_OK) rc = j->exec("COMMIT");
    if (rc != SQLITE_OK) {
        j->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    t->e->bump(kStJournalSyncs);
    const int64_t last = sqlite3_last_insert_rowid(j->db);
    *first = last - int64_t(rows.size()) + 1;
    std::lock_guard<std::mutex> g(t->mu);
    if (last > t->jlast) t->jlast = last;
    t->jinflight.push_back(*first);
    return P4_OK;
}

void jinflightDone(Type* t, int64_t first) {
    if (!first) return;
    for (size_t i = 0; i < t->jinflight.size(); i++)
        if (t->jinflight[i] == first) {
            t->jinflight.erase(t->jinflight.begin() + long(i));
            return;
        }
}

std::string objKeyOfValue(uint32_t pid, int64_t tb, sqlite3_value* v) {
    std::string k(12, '\0');
    st32(reinterpret_cast<uint8_t*>(&k[0]), pid);
    st64(reinterpret_cast<uint8_t*>(&k[4]), uint64_t(tb));
    if (sqlite3_value_type(v) == SQLITE_INTEGER) {
        k.push_back('i');
        uint8_t b[8];
        st64(b, uint64_t(sqlite3_value_int64(v)));
        k.append(reinterpret_cast<const char*>(b), 8);
    } else {
        k.push_back('t');
        const unsigned char* t = sqlite3_value_text(v);
        if (t) k.append(reinterpret_cast<const char*>(t), size_t(sqlite3_value_bytes(v)));
    }
    return k;
}

// Removes tag instances and whole rows from one file, in one transaction,
// after journaling the rows' delete intents. *emptied: the file has no row left.
int32_t removeFromFile(Engine* e, Part* p, File* f, const std::vector<Inst>& insts, std::vector<Gone>& gone,
                       bool* emptied) {
    Type* t = p->type;
    std::shared_ptr<const Spec> sp = t->spec();
    *emptied = false;
    int32_t status = P4_OK;
    Conn* c = writerPin(e, f, &status, nullptr);
    if (!c) return status;
    // Keys, lengths and ent keys of the rows that go; other holders (counters).
    for (Gone& g : gone) {
        sqlite3_stmt* s = c->sql("SELECT cid, length(d), w, k FROM r WHERE seq=?1");
        sqlite3_bind_int64(s, 1, g.seq);
        if (sqlite3_step(s) == SQLITE_ROW && sqlite3_column_bytes(s, 0) == 32) {
            std::memcpy(g.key, sqlite3_column_blob(s, 0), 32);
            g.len = sqlite3_column_int64(s, 1);
            g.w = sqlite3_column_int64(s, 2);
            if (sqlite3_column_type(s, 3) != SQLITE_NULL) g.k = sqlite3_value_dup(sqlite3_column_value(s, 3));
        } else {
            g.seq = 0;  // already gone
        }
        sqlite3_reset(s);
    }
    gone.erase(std::remove_if(gone.begin(), gone.end(), [](const Gone& g) { return g.seq == 0; }), gone.end());
    // The file itself (replay recounts it from its rows), then each row's intent.
    std::vector<std::array<int64_t, 6>> jrows = {{J_FILE, f->tb, int64_t(p->pid), 0, f->gen, 0}};
    std::vector<std::array<uint8_t, 32>> jkeys(1);
    for (Gone& g : gone) {
        jrows.push_back({J_DEL, f->tb, int64_t(p->pid), g.seq, 0, g.len});
        std::array<uint8_t, 32> k;
        std::memcpy(k.data(), g.key, 32);
        jkeys.push_back(k);
    }
    writerUnpin(e, f);
    int64_t jfirst = 0;
    status = journalRows(t, jrows, jkeys, &jfirst);
    if (status != P4_OK) {
        for (Gone& g : gone) if (g.k) sqlite3_value_free(g.k);
        return status;
    }
    c = writerPin(e, f, &status, nullptr);
    if (!c) {
        std::lock_guard<std::mutex> g(t->mu);
        jinflightDone(t, jfirst);
        return status;
    }
    int64_t n, bytes;
    std::map<uint32_t, LaneCount> lanes;
    {
        std::lock_guard<std::mutex> g(t->mu);
        n = f->n;
        bytes = f->bytes;
        lanes = f->lanes;
    }
    std::unordered_map<int64_t, int64_t> lenOf;  // seq -> stored length (instances)
    int rc = c->exec("BEGIN IMMEDIATE");
    auto bad = [&](int r) {
        if (rc == SQLITE_OK && r != SQLITE_OK && r != SQLITE_DONE && r != SQLITE_ROW) rc = r;
    };
    std::vector<std::string> objKeys;
    for (const Inst& in : insts) {
        if (rc != SQLITE_OK) break;
        auto lit = lenOf.find(in.seq);
        if (lit == lenOf.end()) {
            sqlite3_stmt* s = c->get(S_R_LEN);
            sqlite3_bind_int64(s, 1, in.seq);
            const int64_t len = sqlite3_step(s) == SQLITE_ROW ? sqlite3_column_int64(s, 0) : 0;
            sqlite3_reset(s);
            lit = lenOf.emplace(in.seq, len).first;
        }
        sqlite3_stmt* s = c->sql("DELETE FROM rl WHERE sid=?1 AND seq=?2 AND lane=?3");
        sqlite3_bind_int64(s, 1, in.sid);
        sqlite3_bind_int64(s, 2, in.seq);
        sqlite3_bind_int64(s, 3, in.lane);
        bad(sqlite3_step(s));
        const bool hit = sqlite3_changes(c->db) > 0;
        sqlite3_reset(s);
        if (!hit) continue;
        LaneCount& lc = lanes[in.lane];
        lc.n--;
        lc.bytes -= lit->second;
    }
    for (Gone& g : gone) {
        if (rc != SQLITE_OK) break;
        // Its remaining tags (a DELETE takes the record with all of them).
        sqlite3_stmt* q = c->get(S_RL_OF);
        sqlite3_bind_int64(q, 1, g.seq);
        std::vector<std::pair<uint32_t, uint32_t>> rest;
        while (sqlite3_step(q) == SQLITE_ROW)
            rest.push_back({uint32_t(sqlite3_column_int64(q, 0)), uint32_t(sqlite3_column_int64(q, 1))});
        sqlite3_reset(q);
        for (auto& sl : rest) {
            LaneCount& lc = lanes[sl.second];
            lc.n--;
            lc.bytes -= g.len;
        }
        sqlite3_stmt* s = c->get(S_RL_DEL_SEQ);
        sqlite3_bind_int64(s, 1, g.seq);
        bad(sqlite3_step(s));
        sqlite3_reset(s);
        s = c->get(S_R_DEL);
        sqlite3_bind_int64(s, 1, g.seq);
        bad(sqlite3_step(s));
        sqlite3_reset(s);
        n--;
        bytes -= g.len;
        if (sp->ek && g.k) {
            sqlite3_stmt* u = c->get(S_ENT_DEC);
            sqlite3_bind_value(u, 1, g.k);
            bad(sqlite3_step(u));
            sqlite3_reset(u);
            u = c->get(S_ENT_GONE);
            sqlite3_bind_value(u, 1, g.k);
            bad(sqlite3_step(u));
            sqlite3_reset(u);
            objKeys.push_back(objKeyOfValue(p->pid, f->tb, g.k));
        }
    }
    for (auto& kv : lanes) {
        if (rc != SQLITE_OK) break;
        if (kv.second.n > 0) {
            sqlite3_stmt* s = c->sql("UPDATE lane SET n=?2, bytes=?3 WHERE id=?1");
            sqlite3_bind_int64(s, 1, kv.first);
            sqlite3_bind_int64(s, 2, kv.second.n);
            sqlite3_bind_int64(s, 3, kv.second.bytes);
            bad(sqlite3_step(s));
            sqlite3_reset(s);
        } else {
            sqlite3_stmt* s = c->get(S_LANE_DEL);
            sqlite3_bind_int64(s, 1, kv.first);
            bad(sqlite3_step(s));
            sqlite3_reset(s);
        }
    }
    const std::pair<const char*, int64_t> meta[] = {{"n", n}, {"bytes", bytes}, {"updated", nowSec()}};
    for (auto& m : meta) {
        if (rc != SQLITE_OK) break;
        sqlite3_stmt* s = c->get(S_META_SET);
        sqlite3_bind_text(s, 1, m.first, -1, SQLITE_STATIC);
        sqlite3_bind_int64(s, 2, m.second);
        bad(sqlite3_step(s));
        sqlite3_reset(s);
    }
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) c->exec("ROLLBACK");
    writerUnpin(e, f);
    // Whether a gone row was a CID's last copy is decided atomically with the
    // publish, against every other writer's published removals (dmu).
    std::lock_guard<std::mutex> dg(t->dmu);
    if (rc == SQLITE_OK) {
        P4Lane L;
        L.e = e;
        for (Gone& g : gone) {
            std::vector<Holder> hs;
            g.others = 0;
            if (holdersOf(&L, t, f->tb, g.key, &hs) == P4_OK)
                for (auto& h : hs) g.others += h.pid != p->pid;
        }
    }
    {
        std::lock_guard<std::mutex> g(t->mu);
        jinflightDone(t, jfirst);
        if (rc == SQLITE_OK) {
            f->n = n;
            f->bytes = bytes;
            for (auto& kv : lanes) {
                if (kv.second.n <= 0) f->lanes.erase(kv.first);
                else f->lanes[kv.first] = kv.second;
            }
            f->touched = true;
            f->removed += int64_t(gone.size());
            for (Gone& gg : gone) {
                t->pend.kill(f->tb, gg.key, p->pid);
                t->pend.put(gg.key, f->tb, p->pid, gg.seq, 2);
                if (gg.others) t->copies--;
                else {
                    t->uniq--;
                    t->uniqBytes -= gg.len;
                }
            }
            for (auto& k : objKeys) t->touchedObj.insert(k);
            p->n = p->bytes = 0;
            for (auto& kv : p->files) {
                p->n += kv.second->n;
                p->bytes += kv.second->bytes;
            }
            *emptied = n <= 0;
        }
    }
    for (Gone& g : gone) if (g.k) sqlite3_value_free(g.k);
    if (rc != SQLITE_OK) return statusOfSqlite(rc);
    e->bump(kStGroupCommits);
    return P4_OK;
}

}  // namespace

void fileRelease(Type* t, File* f) {
    std::lock_guard<std::mutex> g(t->mu);
    f->inflight--;
}

// A file with no row left is retired at once (readers skip it, writers make a
// new generation) and unlinked by the maintenance thread once nothing uses it.
int32_t retireFile(Engine* e, File* f) {
    Part* p = f->part;
    Type* t = p->type;
    int64_t jfirst = 0;
    std::vector<std::array<int64_t, 6>> rows = {{J_DROP, f->tb, int64_t(p->pid), 0, f->gen, 0}};
    int32_t rc = journalRows(t, rows, {}, &jfirst);
    if (rc != P4_OK) return rc;
    {
        std::lock_guard<std::mutex> g(t->mu);
        jinflightDone(t, jfirst);
        f->retired = true;
        f->touched = true;
        auto it = p->files.find(f->tb);
        if (it != p->files.end() && it->second == f) p->files.erase(it);
        p->n = p->bytes = 0;
        for (auto& kv : p->files) {
            p->n += kv.second->n;
            p->bytes += kv.second->bytes;
        }
    }
    {
        std::lock_guard<std::mutex> g(e->maintMu);
        MaintTask mt;
        mt.kind = MaintTask::kUnlink;
        mt.file = f;
        e->maintQ.push_back(mt);
    }
    e->kickMaintenance();
    return P4_OK;
}

void finishShared(Engine* e, Shared* s) {
    if (s->remaining.fetch_sub(1) != 1) return;
    SlotOut out(e, s->slot);
    const int32_t status = s->status.load();
    if (s->kind == Shared::kSupersede) {
        out.enc.header({"tags_deleted", "records_deleted", "files_deleted"});
        if (status == P4_OK) {
            out.enc.beginRow();
            out.enc.i64(s->a.load());
            out.enc.i64(s->b.load());
            out.enc.i64(s->c.load());
            out.enc.endRow();
            out.rows = 1;
        }
    } else {
        out.enc.header({"deleted"});
        if (status == P4_OK) {
            out.enc.beginRow();
            out.enc.i64(s->a.load());
            out.enc.endRow();
            out.rows = 1;
        }
    }
    std::string err;
    {
        std::lock_guard<std::mutex> g(s->errMu);
        err = s->err;
    }
    out.end(status, err);
    delete s;
}

namespace {
void sharedFail(Shared* s, int32_t status, const std::string& err) {
    int32_t expect = P4_OK;
    if (s->status.compare_exchange_strong(expect, status)) {
        std::lock_guard<std::mutex> g(s->errMu);
        s->err = err;
    }
}
}  // namespace

// Batch supersede (§3.8.7): every tag instance of (provider, source) whose
// batch is not the kept one, then every record left with no tag; per file in
// chunks of 32,768 instances per transaction.
void supersedePart(Engine* e, Part* p, WriteTask* task) {
    Shared* s = task->shared;
    Type* t = p->type;
    std::vector<uint32_t> drop;
    uint32_t sid = 0;
    std::vector<File*> files;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& l : t->lanes)
            if (l->provider == s->provider && l->source == s->source && l->batch != s->keep) {
                drop.push_back(l->id);
                sid = l->sid;
            }
        for (auto& kv : p->files) {
            File* f = kv.second;
            if (!f->created || f->retired) continue;
            bool has = false;
            for (uint32_t id : drop) has = has || f->lanes.count(id);
            if (has && fileClaim(f)) files.push_back(f);  // a dropping file goes anyway
        }
    }
    struct Claims {
        Type* t;
        std::vector<File*>& files;
        ~Claims() {
            for (File* f : files) fileRelease(t, f);
        }
    } claims{t, files};
    std::string in;
    for (uint32_t id : drop) in += (in.empty() ? "" : ",") + std::to_string(id);
    for (File* f : files) {
        if (s->status.load() != P4_OK) break;
        if (!s->apply) {
            int64_t tags = 0;
            int64_t fileN = 0;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (uint32_t id : drop) {
                    auto it = f->lanes.find(id);
                    if (it != f->lanes.end()) tags += it->second.n;
                }
                fileN = f->n;
            }
            int rc = 0;
            Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &rc, nullptr);
            if (!c) {
                sharedFail(s, statusOfSqlite(rc), "open " + f->path);
                break;
            }
            sqlite3_stmt* q = c->sql(
                "SELECT count(*) FROM (SELECT seq FROM rl WHERE sid=?1 AND lane IN (" + in +
                ") GROUP BY seq) x WHERE NOT EXISTS (SELECT 1 FROM rl y WHERE y.seq=x.seq AND NOT (y.sid=?1 AND y.lane IN (" +
                in + ")))");
            int64_t recs = 0;
            if (q) {
                sqlite3_bind_int64(q, 1, sid);
                if (sqlite3_step(q) == SQLITE_ROW) recs = sqlite3_column_int64(q, 0);
                sqlite3_reset(q);
            }
            e->rpool.release(c);
            s->a += tags;
            s->b += recs;
            if (recs >= fileN && fileN > 0) s->c += 1;
            continue;
        }
        for (;;) {
            int32_t prc = P4_OK;
            Conn* c = writerPin(e, f, &prc, nullptr);
            if (!c) {
                sharedFail(s, prc, "open " + f->path);
                break;
            }
            std::vector<Inst> insts;
            sqlite3_stmt* q = c->sql("SELECT seq, lane FROM rl WHERE sid=?1 AND lane IN (" + in + ") LIMIT 32768");
            if (q) {
                sqlite3_bind_int64(q, 1, sid);
                while (sqlite3_step(q) == SQLITE_ROW)
                    insts.push_back(Inst{sid, uint32_t(sqlite3_column_int64(q, 1)), sqlite3_column_int64(q, 0)});
                sqlite3_reset(q);
            }
            // Records whose every tag is in this chunk go with it.
            std::vector<Gone> gone;
            {
                std::unordered_map<int64_t, int> chunkOf;
                for (const Inst& x : insts) chunkOf[x.seq]++;
                for (auto& kv : chunkOf) {
                    sqlite3_stmt* r = c->sql("SELECT count(*) FROM rl WHERE seq=?1");
                    sqlite3_bind_int64(r, 1, kv.first);
                    int64_t all = 0;
                    if (sqlite3_step(r) == SQLITE_ROW) all = sqlite3_column_int64(r, 0);
                    sqlite3_reset(r);
                    if (all == kv.second) {
                        Gone g;
                        g.seq = kv.first;
                        gone.push_back(g);
                    }
                }
            }
            writerUnpin(e, f);
            if (insts.empty()) break;
            bool emptied = false;
            const int32_t rc = removeFromFile(e, p, f, insts, gone, &emptied);
            if (rc != P4_OK) {
                sharedFail(s, rc, "supersede commit failed");
                break;
            }
            s->a += int64_t(insts.size());
            s->b += int64_t(gone.size());
            e->bump(kStSupersedeTags, insts.size());
            e->bump(kStSupersedeRecords, gone.size());
            if (emptied) {
                s->c += 1;
                retireFile(e, f);
                break;
            }
            if (insts.size() < 32768) break;
        }
    }
    finishShared(e, s);
}

// DELETE: every copy of the given CIDs held by this partition.
void deletePart(Engine* e, Part* p, WriteTask* task) {
    Shared* s = task->shared;
    Type* t = p->type;
    std::map<File*, std::vector<Gone>> byFile;
    std::vector<File*> claimed;
    for (const auto& h : task->dels) {  // (tb, seq)
        File* f;
        {
            std::lock_guard<std::mutex> g(t->mu);
            auto it = p->files.find(h.first);
            f = it == p->files.end() || !it->second->created || it->second->retired ? nullptr : it->second;
            if (f && !byFile.count(f)) {
                if (!fileClaim(f)) f = nullptr;  // being dropped: the rows go with it
                else claimed.push_back(f);
            }
        }
        if (!f) continue;
        Gone g;
        g.seq = h.second;
        byFile[f].push_back(g);
    }
    struct Claims {
        Type* t;
        std::vector<File*>& files;
        ~Claims() {
            for (File* f : files) fileRelease(t, f);
        }
    } claims{t, claimed};
    for (auto& kv : byFile) {
        bool emptied = false;
        const size_t want = kv.second.size();
        (void)want;
        const int32_t rc = removeFromFile(e, p, kv.first, {}, kv.second, &emptied);
        if (rc != P4_OK) {
            sharedFail(s, rc, "delete commit failed");
            break;
        }
        s->a += int64_t(kv.second.size());
        e->bump(kStDeletes, kv.second.size());
        if (emptied) retireFile(e, kv.first);
    }
    finishShared(e, s);
}

// ---- file rebuild (design §7) -------------------------------------------------------------------
int32_t rebuildFile(Engine* e, File* f) {
    Part* p = f->part;
    Type* t = p->type;
    std::lock_guard<std::mutex> fg(t->flushMu);
    int32_t gen = 0;
    {
        std::lock_guard<std::mutex> g(t->mu);
        auto it = p->files.find(f->tb);
        if (f->retired || !f->created || f->dropping || it == p->files.end() || it->second != f) return P4_OK;
        f->dropping = true;  // claims are refused from here (writers answer P4_E_BUSY)
        for (auto& x : p->all)
            if (x->tb == f->tb) gen = std::max(gen, x->gen + 1);
        if (auto mg = p->maxGen.find(f->tb); mg != p->maxGen.end()) gen = std::max(gen, mg->second + 1);
    }
    auto release = [&] {
        std::lock_guard<std::mutex> g(t->mu);
        f->dropping = false;
    };
    // Quiet: no claimed write and no writer pin.
    bool quiet = false;
    for (int i = 0; i < 30000 && !quiet; i++) {
        {
            std::lock_guard<std::mutex> g(t->mu);
            quiet = f->inflight == 0;
        }
        if (quiet) {
            std::lock_guard<std::mutex> g(e->wconnMu);
            quiet = f->wPins == 0;
        }
        if (!quiet) ps::sleepNs(1000000);
    }
    if (!quiet) {
        release();
        return P4_E_BUSY;
    }
    while (ioExists(t->filePath(p->pid, f->tb, gen)) || ioExists(t->filePath(p->pid, f->tb, gen) + "-wal")) gen++;
    const std::string np = t->filePath(p->pid, f->tb, gen);
    // The generation is used from here (B9; a crash leaves a file the open sweeps).
    {
        sqlite3_stmt* s = t->idx->sql(
            "INSERT INTO gens(pid, tb, gen) VALUES(?1,?2,?3) ON CONFLICT(pid, tb) DO UPDATE SET gen=max(gen, excluded.gen)");
        int rc = s ? SQLITE_OK : SQLITE_ERROR;
        if (s) {
            sqlite3_bind_int64(s, 1, p->pid);
            sqlite3_bind_int64(s, 2, f->tb);
            sqlite3_bind_int64(s, 3, gen);
            rc = sqlite3_step(s) == SQLITE_DONE ? SQLITE_OK : SQLITE_ERROR;
            sqlite3_reset(s);
        }
        std::lock_guard<std::mutex> g(t->mu);
        p->maxGen[f->tb] = gen;
        if (rc != SQLITE_OK) {
            f->dropping = false;
            return P4_E_IO;
        }
    }
    int32_t status = P4_OK;
    {
        Conn* c = nullptr;
        int rc = openConn(f->path, OpenKind::Maint, 16384, 0, &c, nullptr);
        if (rc == SQLITE_OK) {
            sqlite3_stmt* s = c->sql("VACUUM INTO ?1");
            if (!s) rc = SQLITE_ERROR;
            else {
                sqlite3_bind_text(s, 1, np.data(), int(np.size()), SQLITE_STATIC);
                rc = sqlite3_step(s);
                rc = rc == SQLITE_DONE ? SQLITE_OK : rc;
                sqlite3_reset(s);
            }
            delete c;
        }
        // WAL mode, made durable (the journal-mode change syncs the file).
        if (rc == SQLITE_OK) {
            std::shared_ptr<const Spec> sp = t->spec();
            Conn* w = nullptr;
            rc = openConn(np, OpenKind::Writer, 1024, sp->pageSize, &w, nullptr);
            if (rc == SQLITE_OK) rc = w->exec("PRAGMA wal_checkpoint(TRUNCATE)");
            delete w;
        }
        if (rc != SQLITE_OK) status = statusOfSqlite(rc);
    }
    int64_t jfirst = 0;
    if (status == P4_OK) {
        // J_FILE (the new generation) before J_DROP (the old one): a replay
        // makes the new file live first, so the drop never matches a live
        // file and the index rows stay.
        status = journalRows(t, {{J_FILE, f->tb, int64_t(p->pid), 0, gen, 0}, {J_DROP, f->tb, int64_t(p->pid), 0, f->gen, 0}},
                             {}, &jfirst);
    }
    if (status != P4_OK) {
        ioUnlink(np + "-wal");
        ioUnlink(np);
        release();
        return status;
    }
    {
        std::lock_guard<std::mutex> g(t->mu);
        auto nf = std::make_unique<File>();
        nf->part = p;
        nf->tb = f->tb;
        nf->gen = gen;
        nf->path = np;
        nf->n = f->n; nf->bytes = f->bytes; nf->ncopy = f->ncopy; nf->minseq = f->minseq; nf->maxseq = f->maxseq;
        nf->minw = f->minw; nf->maxw = f->maxw; nf->maxts = f->maxts; nf->nnull = f->nnull;
        nf->mints = f->mints; nf->mine = f->mine; nf->maxe = f->maxe;
        nf->lanes = f->lanes;
        nf->indexed = f->indexed;
        nf->created = true;
        nf->touched = true;
        p->files[f->tb] = nf.get();
        p->all.push_back(std::move(nf));
        f->retired = true;
        f->touched = true;
        f->dropping = false;
        jinflightDone(t, jfirst);
    }
    {
        std::lock_guard<std::mutex> g(e->maintMu);
        MaintTask mt;
        mt.kind = MaintTask::kUnlink;
        mt.file = f;
        e->maintQ.push_back(mt);
    }
    e->kickMaintenance();
    e->bump(kStRebuilds);
    return P4_OK;
}

void maybeRebuildFiles(Engine* e) {
    std::vector<Type*> types;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        for (auto& t : e->types) types.push_back(t.get());
    }
    for (Type* t : types) {
        std::vector<File*> cand;
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& p : t->parts)
                for (auto& kv : p->files)
                    if (kv.second->created && !kv.second->retired && kv.second->removed > 0) {
                        kv.second->removed = 0;
                        cand.push_back(kv.second);
                    }
        }
        for (File* f : cand) {
            Conn* c = nullptr;
            if (openConn(f->path, OpenKind::Maint, 256, 0, &c, nullptr) != SQLITE_OK) continue;
            int64_t freePages = 0, pages = 0, pageSize = 0;
            sqlite3_stmt* s = c->sql("SELECT (SELECT freelist_count FROM pragma_freelist_count), (SELECT page_count FROM"
                                     " pragma_page_count), (SELECT page_size FROM pragma_page_size)");
            if (s && sqlite3_step(s) == SQLITE_ROW) {
                freePages = sqlite3_column_int64(s, 0);
                pages = sqlite3_column_int64(s, 1);
                pageSize = sqlite3_column_int64(s, 2);
            }
            if (s) sqlite3_reset(s);
            delete c;
            if (pages > 0 && uint64_t(freePages * pageSize) >= e->cfg.rebuildMinBytes &&
                uint64_t(freePages) * 1000 >= uint64_t(pages) * e->cfg.rebuildFreePermille)
                rebuildFile(e, f);
        }
    }
}

}  // namespace p4
}  // namespace flatsql
