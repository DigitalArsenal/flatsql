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
    int64_t seq = 0, len = 0, e = 0;
    uint8_t key[32];
    bool eNull = false;
    KVal k;
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
            g.eNull = sqlite3_column_type(s, 2) == SQLITE_NULL;
            g.e = sqlite3_column_int64(s, 2);
            g.k.from(s, 3);
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
    Counters k;
    std::map<uint32_t, LaneCount> lanes;
    const bool epochRule = t->spec()->hasEpochRule;
    {
        std::lock_guard<std::mutex> g(t->mu);
        k = countersOf(f);
        lanes = f->lanes;
    }
    std::unordered_map<int64_t, int64_t> lenOf;  // seq -> stored length (instances)
    int rc = c->exec("BEGIN IMMEDIATE");
    auto bad = [&](int r) {
        if (rc == SQLITE_OK && r != SQLITE_OK && r != SQLITE_DONE && r != SQLITE_ROW) rc = r;
    };
    Derived dv;
    if (rc == SQLITE_OK && !gone.empty()) {
        bool ix;
        {
            std::lock_guard<std::mutex> g(t->mu);
            ix = f->indexed;
        }
        bad(dv.load(c, t->spec()->hasObject && ix));
    }
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
        bad(dv.removed(c, g.k, !g.eNull, g.e));
        k.n--;
        k.bytes -= g.len;
        if (g.eNull && epochRule) k.nnull--;
    }
    if (rc == SQLITE_OK && !gone.empty()) {
        // The seq and w bounds again (rowid and r_w ends: O(log n)); the
        // epoch bounds equal w's when every row has an epoch. mints/maxts
        // (no index) and an epoch bound beside rows without one stay bounds.
        sqlite3_stmt* s = c->sql("SELECT min(seq), max(seq) FROM r");
        if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) {
            k.minseq = sqlite3_column_int64(s, 0);
            k.maxseq = sqlite3_column_int64(s, 1);
        }
        if (s) sqlite3_reset(s);
        s = c->sql("SELECT min(w) FROM r");
        if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.minw = sqlite3_column_int64(s, 0);
        if (s) sqlite3_reset(s);
        s = c->sql("SELECT max(w) FROM r");
        if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.maxw = sqlite3_column_int64(s, 0);
        if (s) sqlite3_reset(s);
        if (k.n == 0) {
            const int64_t keepMaxseq = k.maxseq;
            k = Counters();
            k.maxseq = keepMaxseq;  // seqs never go back
        } else if (k.nnull == 0 && epochRule) {
            k.mine = k.minw;
            k.maxe = k.maxw;
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
    if (rc == SQLITE_OK) rc = dv.save(c);
    if (rc == SQLITE_OK) rc = writeMeta(c, k, nowSec());
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) c->exec("ROLLBACK");
    int64_t freeBytes = 0;
    const int64_t dbBytes = rc == SQLITE_OK ? dbBytesOf(c, &freeBytes) : -1;
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
            if (dbBytes >= 0) {
                f->dbBytes = dbBytes;
                f->freeBytes = freeBytes;
            }
            countersTo(f, k);
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
    if (s->internal()) {
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

void repairPart(Engine* e, Part* p, WriteTask* task) {
    Shared* s = task->shared;
    Type* t = p->type;
    bool created;
    {
        std::lock_guard<std::mutex> g(t->mu);
        created = p->created && !p->quarantined;
    }
    if (!created) {
        finishShared(e, s);
        return;
    }
    const bool epochRule = t->spec()->hasEpochRule;
    int32_t status = P4_OK;
    Conn* c = writerPin(e, p, &status, nullptr);
    if (!c) {
        sharedFail(s, status, "open " + p->path);
        finishShared(e, s);
        return;
    }
    Counters k, old;
    std::map<uint32_t, LaneCount> before, lanes;
    {
        std::lock_guard<std::mutex> g(t->mu);
        old = countersOf(p);
        before = p->lanes;
    }
    int rc = c->exec("BEGIN IMMEDIATE");
    auto bad = [&](int r) {
        if (rc == SQLITE_OK && r != SQLITE_OK && r != SQLITE_DONE && r != SQLITE_ROW) rc = r;
    };
    // The file's counters from its rows.
    sqlite3_stmt* q = rc == SQLITE_OK ? c->sql(
        "SELECT count(*), coalesce(sum(length(d)),0), min(seq), max(seq), min(w), max(w), coalesce(max(ts),0),"
        " coalesce(sum(e IS NULL),0), min(ts), min(e), max(e) FROM r") : nullptr;
    if (rc == SQLITE_OK && !q) rc = SQLITE_ERROR;
    if (q) {
        const int r = sqlite3_step(q);
        if (r == SQLITE_ROW) {
            auto opt = [&](int i, int64_t none) { return sqlite3_column_type(q, i) == SQLITE_NULL ? none : sqlite3_column_int64(q, i); };
            k.n = sqlite3_column_int64(q, 0);
            k.bytes = sqlite3_column_int64(q, 1);
            k.minseq = opt(2, INT64_MAX);
            k.maxseq = std::max<int64_t>(opt(3, 0), old.maxseq);  // seqs never go back
            k.minw = opt(4, INT64_MAX);
            k.maxw = opt(5, INT64_MIN);
            k.maxts = sqlite3_column_int64(q, 6);
            k.nnull = epochRule ? sqlite3_column_int64(q, 7) : 0;
            k.mints = opt(8, INT64_MAX);
            k.mine = opt(9, INT64_MAX);
            k.maxe = opt(10, INT64_MIN);
            k.ncopy = old.ncopy;
        }
        bad(r);
        sqlite3_reset(q);
    }
    // The lane rows: identity and url fields from the file's lane table (or
    // memory), the counts from the tag instances (aggregated here, no sorter).
    std::map<uint32_t, LaneCount> table;
    q = rc == SQLITE_OK ? c->sql("SELECT id, url, url0, created, updated FROM lane") : nullptr;
    while (q && rc == SQLITE_OK) {
        const int r = sqlite3_step(q);
        if (r != SQLITE_ROW) {
            bad(r);
            break;
        }
        LaneCount lc;
        auto txt = [&](int i) {
            const unsigned char* x = sqlite3_column_text(q, i);
            return x ? std::string(reinterpret_cast<const char*>(x)) : std::string();
        };
        lc.url = txt(1);
        lc.url0 = txt(2);
        lc.created = sqlite3_column_int64(q, 3);
        lc.updated = sqlite3_column_int64(q, 4);
        table[uint32_t(sqlite3_column_int64(q, 0))] = lc;
    }
    if (q) sqlite3_reset(q);
    q = rc == SQLITE_OK ? c->sql("SELECT rl.lane, rl.at, r.seq, length(r.d), r.w, r.ts FROM rl JOIN r ON r.seq=rl.seq") : nullptr;
    while (q && rc == SQLITE_OK) {
        const int r = sqlite3_step(q);
        if (r != SQLITE_ROW) {
            bad(r);
            break;
        }
        const uint32_t id = uint32_t(sqlite3_column_int64(q, 0));
        auto it = lanes.find(id);
        if (it == lanes.end()) {
            LaneCount lc;
            auto ti = table.find(id);
            auto bi = before.find(id);
            if (ti != table.end()) lc = ti->second;
            else if (bi != before.end()) lc = bi->second;
            lc.n = lc.bytes = lc.maxseq = lc.maxat = lc.maxts = 0;
            lc.minw = lc.minseq = INT64_MAX;
            lc.maxw = INT64_MIN;
            it = lanes.emplace(id, lc).first;
        }
        LaneCount& lc = it->second;
        const int64_t at = sqlite3_column_int64(q, 1), seq = sqlite3_column_int64(q, 2), w = sqlite3_column_int64(q, 4),
                      ts = sqlite3_column_int64(q, 5);
        lc.n++;
        lc.bytes += sqlite3_column_int64(q, 3);
        lc.minw = std::min(lc.minw, w);
        lc.maxw = std::max(lc.maxw, w);
        lc.minseq = std::min(lc.minseq, seq);
        lc.maxseq = std::max(lc.maxseq, seq);
        lc.maxat = std::max(lc.maxat, at);
        lc.maxts = std::max(lc.maxts, ts);
        if (!lc.created || at < lc.created) lc.created = at;  // the lane's first instance here
    }
    if (q) sqlite3_reset(q);
    int64_t changed = 0;
    for (auto& kv : lanes) {
        if (rc != SQLITE_OK) break;
        LaneDef* l;
        {
            std::lock_guard<std::mutex> g(t->mu);
            l = t->laneById(kv.first);
        }
        if (!l) {
            rc = SQLITE_CORRUPT;  // a tag instance of an unknown lane
            break;
        }
        const LaneCount& lc = kv.second;
        auto bi = before.find(kv.first);
        if (bi == before.end() || bi->second.n != lc.n || bi->second.bytes != lc.bytes) changed++;
        sqlite3_stmt* u = c->get(S_LANE_UP);
        sqlite3_bind_int64(u, 1, kv.first);
        sqlite3_bind_int64(u, 2, l->sid);
        sqlite3_bind_text(u, 3, l->batch.data(), int(l->batch.size()), SQLITE_STATIC);
        sqlite3_bind_text(u, 4, l->ckey.data(), int(l->ckey.size()), SQLITE_STATIC);
        sqlite3_bind_text(u, 5, l->ppeer.data(), int(l->ppeer.size()), SQLITE_STATIC);
        sqlite3_bind_text(u, 6, l->pkey.data(), int(l->pkey.size()), SQLITE_STATIC);
        sqlite3_bind_text(u, 7, lc.url.data(), int(lc.url.size()), SQLITE_STATIC);
        sqlite3_bind_text(u, 8, lc.url0.data(), int(lc.url0.size()), SQLITE_STATIC);
        sqlite3_bind_int64(u, 9, lc.created);
        sqlite3_bind_int64(u, 10, lc.updated);
        sqlite3_bind_int64(u, 11, lc.maxat);
        sqlite3_bind_int64(u, 12, lc.n);
        sqlite3_bind_int64(u, 13, lc.bytes);
        sqlite3_bind_int64(u, 14, lc.minw);
        sqlite3_bind_int64(u, 15, lc.maxw);
        sqlite3_bind_int64(u, 16, lc.maxseq);
        sqlite3_bind_int64(u, 17, lc.maxts);
        sqlite3_bind_int64(u, 18, lc.minseq);
        bad(sqlite3_step(u));
        sqlite3_reset(u);
    }
    for (auto& kv : table) {
        if (rc != SQLITE_OK || lanes.count(kv.first)) continue;
        changed++;
        sqlite3_stmt* d = c->get(S_LANE_DEL);
        sqlite3_bind_int64(d, 1, kv.first);
        bad(sqlite3_step(d));
        sqlite3_reset(d);
    }
    if (k.n != old.n || k.bytes != old.bytes || k.nnull != old.nnull) changed++;
    if (rc == SQLITE_OK) {
        bool ix;
        {
            std::lock_guard<std::mutex> g(t->mu);
            ix = p->indexed;
        }
        // The object count and epoch histogram from the rows (also gives a
        // file an older engine wrote both).
        if (ix) rc = derivedRecount(t, c);
    }
    if (rc == SQLITE_OK) rc = writeMeta(c, k, nowSec());
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) c->exec("ROLLBACK");
    writerUnpin(e, p);
    if (rc != SQLITE_OK) {
        sharedFail(s, statusOfSqlite(rc), "repair " + p->path);
    } else {
        std::lock_guard<std::mutex> g(t->mu);
        countersTo(p, k);
        p->lanes = std::move(lanes);
        p->touched = true;
        s->a += 1;
        s->b += changed;
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
