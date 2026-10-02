// Store format 4: removals (design §4 batch supersede W-f/W-g, delete W-i,
// quota). Each runs on the partition's owner writer: delete intents are
// journaled before the file commits, counters change in the same
// transaction, and a lane row goes at 0. A partition keeps its one file for
// its life (C-32): an emptied file stays, and SQLite reuses its free pages.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {

struct Gone {
    int64_t seq = 0, len = 0;
    uint8_t key[32];
    int others = 0;
};
struct Inst {
    uint32_t sid, lane;
    int64_t seq;
};

// rows: (op, pid, seq, v); keys[i] for J_C/J_DEL rows.
int32_t journalRows(Type* t, const std::vector<std::array<int64_t, 4>>& rows, const std::vector<std::array<uint8_t, 32>>& keys,
                    int64_t* first) {
    *first = 0;
    if (rows.empty()) return P4_OK;
    std::lock_guard<std::mutex> jg(t->jmu);
    Conn* j = t->jdb;
    int rc = j->exec("BEGIN IMMEDIATE");
    for (size_t i = 0; i < rows.size() && rc == SQLITE_OK; i++) {
        const auto& r = rows[i];
        sqlite3_stmt* q = j->get(S_J_INS);
        if (!q) { rc = SQLITE_ERROR; break; }
        sqlite3_bind_int(q, 1, int(r[0]));
        if (i < keys.size() && r[0] != J_FILE) sqlite3_bind_blob(q, 2, keys[i].data(), 32, SQLITE_STATIC);
        else sqlite3_bind_null(q, 2);
        sqlite3_bind_null(q, 3);
        sqlite3_bind_int64(q, 4, r[1]);
        sqlite3_bind_int64(q, 5, r[2]);
        sqlite3_bind_null(q, 6);
        sqlite3_bind_int64(q, 7, r[3]);
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

// Removes tag instances and whole rows from the partition's file, in one
// transaction, after journaling the rows' delete intents.
int32_t removeFromFile(Engine* e, Part* p, const std::vector<Inst>& insts, std::vector<Gone>& gone) {
    Type* t = p->type;
    Part* f = p;
    int32_t status = P4_OK;
    Conn* c = writerPin(e, f, &status, nullptr);
    if (!c) return status;
    // Keys and lengths of the rows that go.
    for (Gone& g : gone) {
        sqlite3_stmt* s = c->get(S_R_LEN);
        sqlite3_bind_int64(s, 1, g.seq);
        if (sqlite3_step(s) == SQLITE_ROW && sqlite3_column_bytes(s, 1) == 32) {
            std::memcpy(g.key, sqlite3_column_blob(s, 1), 32);
            g.len = sqlite3_column_int64(s, 0);
        } else {
            g.seq = 0;  // already gone
        }
        sqlite3_reset(s);
    }
    gone.erase(std::remove_if(gone.begin(), gone.end(), [](const Gone& g) { return g.seq == 0; }), gone.end());
    // The file itself (replay recounts it from its rows), then each row's intent.
    std::vector<std::array<int64_t, 4>> jrows = {{J_FILE, int64_t(p->pid), 0, 0}};
    std::vector<std::array<uint8_t, 32>> jkeys(1);
    for (Gone& g : gone) {
        jrows.push_back({J_DEL, int64_t(p->pid), g.seq, g.len});
        std::array<uint8_t, 32> k;
        std::memcpy(k.data(), g.key, 32);
        jkeys.push_back(k);
    }
    int64_t jfirst = 0;
    status = journalRows(t, jrows, jkeys, &jfirst);
    if (status != P4_OK) {
        writerUnpin(e, f);
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
        std::sort(gone.begin(), gone.end(), [](const Gone& a, const Gone& b) { return std::memcmp(a.key, b.key, 32) < 0; });
        for (Gone& g : gone) g.others = othersHolding(&L, t, g.key, p->pid);
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
            for (Gone& gg : gone) {
                t->pend.kill(gg.key, p->pid);
                t->pend.put(gg.key, p->pid, gg.seq, 2);
                if (gg.others) t->copies--;
                else {
                    t->uniq--;
                    t->uniqBytes -= gg.len;
                }
            }
        }
    }
    if (rc != SQLITE_OK) return statusOfSqlite(rc);
    e->bump(kStGroupCommits);
    return P4_OK;
}

}  // namespace

void finishShared(Engine* e, Shared* s) {
    if (s->remaining.fetch_sub(1) != 1) return;
    if (s->kind == Shared::kQuota) {
        s->done.store(true, std::memory_order_release);  // the waiter reads the counts and deletes it
        return;
    }
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
// batch is not the kept one, then every record left with no tag; in chunks of
// 32,768 instances per transaction.
void supersedePart(Engine* e, Part* p, WriteTask* task) {
    Shared* s = task->shared;
    Type* t = p->type;
    Part* f = p;
    std::vector<uint32_t> drop;
    uint32_t sid = 0;
    bool has = false;
    int64_t fileN = 0, tags = 0;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& l : t->lanes)
            if (l->provider == s->provider && l->source == s->source && l->batch != s->keep) {
                drop.push_back(l->id);
                sid = l->sid;
            }
        if (f->created)
            for (uint32_t id : drop) {
                auto it = f->lanes.find(id);
                if (it == f->lanes.end()) continue;
                has = true;
                tags += it->second.n;
            }
        fileN = f->n;
    }
    std::string in;
    for (uint32_t id : drop) in += (in.empty() ? "" : ",") + std::to_string(id);
    if (has && !s->apply) {
        int rc = 0;
        Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &rc, nullptr);
        if (!c) {
            sharedFail(s, statusOfSqlite(rc), "open " + f->path);
        } else {
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
            (void)fileN;
        }
        has = false;
    }
    while (has && s->status.load() == P4_OK) {
        int32_t prc = P4_OK;
        Conn* c = writerPin(e, f, &prc, nullptr);
        if (!c) {
            sharedFail(s, prc, "open " + f->path);
            break;
        }
        std::vector<Inst> insts;
        sqlite3_stmt* q = c->sql("SELECT seq, lane FROM rl INDEXED BY rl_sid WHERE sid=?1 AND lane IN (" + in + ") LIMIT 32768");
        if (!q) q = c->sql("SELECT seq, lane FROM rl WHERE sid=?1 AND lane IN (" + in + ") LIMIT 32768");
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
            sqlite3_stmt* r = c->sql("SELECT count(*) FROM rl WHERE seq=?1");
            for (auto& kv : chunkOf) {
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
        std::sort(gone.begin(), gone.end(), [](const Gone& a, const Gone& b) { return a.seq < b.seq; });
        const int32_t rc = removeFromFile(e, p, insts, gone);
        if (rc != P4_OK) {
            sharedFail(s, rc, "supersede commit failed");
            break;
        }
        s->a += int64_t(insts.size());
        s->b += int64_t(gone.size());
        e->bump(kStSupersedeTags, insts.size());
        e->bump(kStSupersedeRecords, gone.size());
        if (insts.size() < 32768) break;
    }
    finishShared(e, s);
}

// DELETE and quota: the given seqs of this partition's file.
void deletePart(Engine* e, Part* p, WriteTask* task) {
    Shared* s = task->shared;
    bool created;
    {
        std::lock_guard<std::mutex> g(p->type->mu);
        created = p->created;
    }
    std::vector<int64_t> seqs = task->dels;
    std::sort(seqs.begin(), seqs.end());
    seqs.erase(std::unique(seqs.begin(), seqs.end()), seqs.end());
    for (size_t at = 0; created && at < seqs.size() && s->status.load() == P4_OK; at += 32768) {
        std::vector<Gone> gone;
        for (size_t i = at; i < seqs.size() && i < at + 32768; i++) {
            Gone g;
            g.seq = seqs[i];
            gone.push_back(g);
        }
        const int32_t rc = removeFromFile(e, p, {}, gone);
        if (rc != P4_OK) {
            sharedFail(s, rc, s->kind == Shared::kQuota ? "quota commit failed" : "delete commit failed");
            break;
        }
        int64_t bytes = 0;
        for (const Gone& g : gone) bytes += g.len;
        s->a += int64_t(gone.size());
        s->b += bytes;
        if (s->kind == Shared::kDelete) e->bump(kStDeletes, gone.size());
    }
    finishShared(e, s);
}

}  // namespace p4
}  // namespace flatsql
