// Store format 4: the per-type intent journal (design §3.2, B3).
//
// T/<TYPE>.jnl is a SQLite file with synchronous=FULL. Before any partition
// file of a group commits, one journal transaction appends the group's
// intents: the partition, new sources and lanes, every file it writes, one row
// per new CID entry and ingest identity, and delete intents. Seq blocks are
// reserved in jm before any seq of the block can commit. The type index is
// derived and flushed lazily; open replays the journal tail into it, keeping
// each CID entry only if its row exists in its file (read by rowid, CID
// compared), and recounts every touched file from its own rows.
//
// Journal ids are AUTOINCREMENT, so they never restart when the table empties
// (a restart once let a flush delete rows it had not merged).
#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {
const char* kJournalSchema =
    "CREATE TABLE IF NOT EXISTS j(id INTEGER PRIMARY KEY AUTOINCREMENT, op INTEGER NOT NULL, tb INTEGER,"
    " k BLOB, c BLOB, pid INTEGER, seq INTEGER, gen INTEGER, s TEXT, v INTEGER);"
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
    rc = c->exec(kJournalSchema);
    if (rc != SQLITE_OK) {
        if (err) *err = sqlite3_errmsg(c->db);
        delete c;
        return statusOfSqlite(rc);
    }
    t->jdb = c;
    return P4_OK;
}

int32_t journalReserve(Type* t, int64_t through) {
    std::lock_guard<std::mutex> g(t->jmu);
    sqlite3_stmt* s = t->jdb->get(S_JM_SET);
    if (!s) return P4_E_INTERNAL;
    sqlite3_bind_text(s, 1, "seq_reserved", -1, SQLITE_STATIC);
    sqlite3_bind_int64(s, 2, through);
    const int rc = sqlite3_step(s);
    sqlite3_reset(s);
    t->e->bump(kStJournalSyncs);
    return rc == SQLITE_DONE ? P4_OK : statusOfSqlite(rc);
}

int32_t journalReplay(Type* t, std::string* err) {
    Conn* j = t->jdb;
    {
        sqlite3_stmt* s = j->sql("SELECT v FROM jm WHERE k='seq_reserved'");
        if (s && sqlite3_step(s) == SQLITE_ROW) t->seqReserved = sqlite3_column_int64(s, 0);
        if (s) sqlite3_reset(s);
        s = j->sql("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='j'),0)");
        if (s && sqlite3_step(s) == SQLITE_ROW) t->jlast = sqlite3_column_int64(s, 0);
        if (s) sqlite3_reset(s);
    }
    if (t->seqReserved >= t->nextSeq) t->nextSeq = t->seqReserved + 1;

    struct JE {
        int op;
        int64_t tb, seq, v;
        uint32_t pid;
        int32_t gen;
        uint8_t k[32];
        uint8_t c[32];
        std::string s;
    };
    std::vector<JE> rows;
    sqlite3_stmt* q = j->sql("SELECT op, tb, k, c, pid, seq, gen, s, v FROM j ORDER BY id");
    if (!q) { *err = sqlite3_errmsg(j->db); return P4_E_IO; }
    int rc;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        JE x{};
        x.op = sqlite3_column_int(q, 0);
        x.tb = sqlite3_column_int64(q, 1);
        if (sqlite3_column_bytes(q, 2) == 32) std::memcpy(x.k, sqlite3_column_blob(q, 2), 32);
        if (sqlite3_column_bytes(q, 3) == 32) std::memcpy(x.c, sqlite3_column_blob(q, 3), 32);
        x.pid = uint32_t(sqlite3_column_int64(q, 4));
        x.seq = sqlite3_column_int64(q, 5);
        x.gen = sqlite3_column_int(q, 6);
        x.s = colText(q, 7);
        x.v = sqlite3_column_int64(q, 8);
        rows.push_back(std::move(x));
    }
    sqlite3_reset(q);
    if (rc != SQLITE_DONE) { *err = sqlite3_errmsg(j->db); return statusOfSqlite(rc); }
    if (rows.empty()) return P4_OK;

    std::lock_guard<std::mutex> g(t->mu);
    // 1. Registry: partitions, sources, lanes, files.
    for (const JE& x : rows) {
        if (x.op == J_PART) {
            const auto f = splitUnit(x.s, 2);
            while (t->parts.size() < x.pid) {
                // pids are assigned in order; a gap is a partition whose J_PART row
                // is later in the tail (never expected): keep numbering stable.
                Part* p = partFor(t, "\x1f#" + std::to_string(t->parts.size() + 1), "", true);
                (void)p;
            }
            Part* p = t->partById(x.pid);
            if (p && p->producer.rfind("\x1f#", 0) == 0) {
                t->partByProducer.erase(p->producer);
                p->producer = f[0];
                p->peer = f[1];
                t->partByProducer.emplace(p->producer, p->pid);
            }
            if (p) p->journaled = true;
        } else if (x.op == J_SRC) {
            const auto f = splitUnit(x.s, 2);
            while (t->srcs.size() < uint64_t(x.seq)) srcFor(t, "\x1f#" + std::to_string(t->srcs.size() + 1), "", true);
            SrcDef* s = t->srcById(uint32_t(x.seq));
            if (s && s->provider.rfind("\x1f#", 0) == 0) {
                t->srcByName.erase(s->provider + '\x1f' + s->source);
                s->provider = f[0];
                s->source = f[1];
                t->srcByName.emplace(s->provider + '\x1f' + s->source, s->id);
            }
            if (s) t->srcJournaled[s->id - 1] = 1;
        } else if (x.op == J_LANE) {
            const auto f = splitUnit(x.s, 6);
            while (t->lanes.size() < uint64_t(x.seq)) {
                std::string ph[6] = {"\x1f#" + std::to_string(t->lanes.size() + 1), "", "", "", "", ""};
                laneFor(t, ph, true);
            }
            LaneDef* l = t->laneById(uint32_t(x.seq));
            if (l && l->provider.rfind("\x1f#", 0) == 0) {
                std::string old[6] = {l->provider, l->source, l->batch, l->ckey, l->ppeer, l->pkey};
                std::string key;
                for (auto& o : old) key += o + '\x1f';
                t->laneByIdentity.erase(key);
                l->provider = f[0];
                l->source = f[1];
                l->batch = f[2];
                l->ckey = f[3];
                l->ppeer = f[4];
                l->pkey = f[5];
                l->sid = uint32_t(x.v);
                std::string nf[6] = {f[0], f[1], f[2], f[3], f[4], f[5]};
                l->h = laneHash(nf);
                std::string nk;
                for (auto& o : nf) nk += o + '\x1f';
                t->laneByIdentity.emplace(nk, l->id);
            }
            if (l) t->laneJournaled[l->id - 1] = 1;
        } else if (x.op == J_FILE) {
            Part* p = t->partById(x.pid);
            if (!p) continue;
            File* f = nullptr;
            auto it = p->files.find(x.tb);
            if (it != p->files.end() && it->second->gen == x.gen) {
                f = it->second;
            } else if (it == p->files.end() || it->second->gen < x.gen) {
                auto nf = std::make_unique<File>();
                nf->part = p;
                nf->tb = x.tb;
                nf->gen = x.gen;
                nf->path = t->filePath(p->pid, x.tb, x.gen);
                f = nf.get();
                if (it != p->files.end()) it->second->retired = true;
                p->files[x.tb] = f;
                p->all.push_back(std::move(nf));
            }
            if (f) {
                f->created = ioExists(f->path);
                f->touched = true;
                f->objRefresh = true;
                noteTb(t, x.tb);
            }
        } else if (x.op == J_DROP) {
            Part* p = t->partById(x.pid);
            if (!p) continue;
            auto it = p->files.find(x.tb);
            if (it != p->files.end() && it->second->gen == x.gen) {
                it->second->retired = true;
                p->files.erase(it);
            }
        }
    }
    // 2. Entries, checked against the files. One maintenance connection per
    //    file, opened here: opening it recovers the file's WAL before any read
    //    is served (M8).
    std::unordered_map<File*, Conn*> conns;
    auto connOf = [&](File* f) -> Conn* {
        auto it = conns.find(f);
        if (it != conns.end()) return it->second;
        Conn* c = nullptr;
        if (f->created && openConn(f->path, OpenKind::Maint, 2048, 0, &c, nullptr) != SQLITE_OK) c = nullptr;
        conns[f] = c;
        return c;
    };
    auto fileOf = [&](uint32_t pid, int64_t tb) -> File* {
        Part* p = t->partById(pid);
        if (!p) return nullptr;
        auto it = p->files.find(tb);
        return it == p->files.end() ? nullptr : it->second;
    };
    Conn* idx = t->idx;
    std::unordered_set<int64_t> liveSeqs;
    std::shared_ptr<const Spec> sp = t->spec_;
    for (const JE& x : rows) {
        if (x.op != J_C && x.op != J_DEL) continue;
        File* f = fileOf(x.pid, x.tb);
        Conn* c = f ? connOf(f) : nullptr;
        bool present = false;
        int64_t len = 0;
        if (c) {
            sqlite3_stmt* s = c->get(S_R_LEN);
            if (s) {
                sqlite3_bind_int64(s, 1, x.seq);
                if (sqlite3_step(s) == SQLITE_ROW && sqlite3_column_bytes(s, 1) == 32 &&
                    std::memcmp(sqlite3_column_blob(s, 1), x.k, 32) == 0) {
                    present = true;
                    len = sqlite3_column_int64(s, 0);
                }
                sqlite3_reset(s);
            }
        }
        bool inIndex = false;
        {
            sqlite3_stmt* s = idx->sql("SELECT 1 FROM c WHERE tb=?1 AND cid=?2 AND pid=?3");
            if (s) {
                sqlite3_bind_int64(s, 1, x.tb);
                sqlite3_bind_blob(s, 2, x.k, 32, SQLITE_STATIC);
                sqlite3_bind_int64(s, 3, x.pid);
                inIndex = sqlite3_step(s) == SQLITE_ROW;
                sqlite3_reset(s);
            }
        }
        // Other holders: the pending layer over the index (a pending delete hides its row).
        int others = 0;
        {
            std::vector<Holder> hs;
            holdersWith(t, idx, x.tb, x.k, &hs, true);
            for (auto& h : hs) others += h.pid != x.pid;
        }
        if (x.op == J_C) {
            if (!present) continue;  // its file never committed
            liveSeqs.insert(x.seq);
            if (x.seq >= t->nextSeq) t->nextSeq = x.seq + 1;
            if (!inIndex) {
                bool pendingHas = false;
                t->pend.each(x.tb, x.k, [&](const CEnt& ce) { pendingHas = pendingHas || (ce.pid == x.pid && ce.st == 1); });
                if (!pendingHas) {
                    if (others) t->copies++;
                    else { t->uniq++; t->uniqBytes += len; }
                    t->pend.put(x.k, x.tb, x.pid, x.seq, 1);
                }
                noteTb(t, x.tb);
            }
        } else {  // J_DEL: applied when the row is gone
            if (present) continue;
            bool pendingLive = false;
            t->pend.each(x.tb, x.k, [&](const CEnt& ce) { pendingLive = pendingLive || (ce.pid == x.pid && ce.st == 1); });
            if (inIndex || pendingLive) {
                if (others) t->copies--;
                else { t->uniq--; t->uniqBytes -= x.v; }
                t->pend.kill(x.tb, x.k, x.pid);
                t->pend.put(x.k, x.tb, x.pid, x.seq, 2);
            }
        }
    }
    for (const JE& x : rows) {
        if (x.op != J_IDENT || !liveSeqs.count(x.seq)) continue;
        IdentEnt ie;
        ie.tb = x.tb;
        ie.src = uint64_t(x.v);
        std::memcpy(ie.h, x.k, 32);
        std::memcpy(ie.cid, x.c, 32);
        ie.seq = x.seq;
        ie.st = 1;
        t->identPend[identMapKey(ie.tb, ie.src, ie.h)] = ie;
    }
    // 3. Recount every touched file from its own meta and lane rows.
    for (auto& p : t->parts) {
        bool any = false;
        for (auto& kv : p->files) {
            File* f = kv.second;
            if (!f->touched) continue;
            any = true;
            Conn* c = connOf(f);
            if (!c) continue;
            f->n = f->bytes = f->ncopy = f->nnull = f->maxts = 0;
            f->minseq = INT64_MAX;
            f->maxseq = 0;
            f->minw = INT64_MAX;
            f->maxw = INT64_MIN;
            sqlite3_stmt* s = c->sql(
                "SELECT count(*), coalesce(sum(length(d)),0), min(seq), max(seq), min(w), max(w), coalesce(max(ts),0),"
                " coalesce(sum(e IS NULL),0), min(ts), min(e), max(e) FROM r");
            if (s && sqlite3_step(s) == SQLITE_ROW) {
                f->n = sqlite3_column_int64(s, 0);
                f->bytes = sqlite3_column_int64(s, 1);
                if (f->n) {
                    f->minseq = sqlite3_column_int64(s, 2);
                    f->maxseq = sqlite3_column_int64(s, 3);
                    f->minw = sqlite3_column_int64(s, 4);
                    f->maxw = sqlite3_column_int64(s, 5);
                }
                f->maxts = sqlite3_column_int64(s, 6);
                f->nnull = sqlite3_column_int64(s, 7);
                f->mints = sqlite3_column_type(s, 8) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 8);
                f->mine = sqlite3_column_type(s, 9) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 9);
                f->maxe = sqlite3_column_type(s, 10) == SQLITE_NULL ? INT64_MIN : sqlite3_column_int64(s, 10);
            }
            if (s) sqlite3_reset(s);
            s = c->sql("SELECT v FROM meta WHERE k='ncopy'");
            if (s && sqlite3_step(s) == SQLITE_ROW) f->ncopy = sqlite3_column_int64(s, 0);
            if (s) sqlite3_reset(s);
            s = c->sql("SELECT v FROM meta WHERE k='ix'");
            if (s && sqlite3_step(s) == SQLITE_ROW) f->indexed = sqlite3_column_int64(s, 0) != 0;
            if (s) sqlite3_reset(s);
            f->lanes.clear();
            s = c->sql(
                "SELECT id, n, bytes, minw, maxw, maxseq, created, updated, maxat, url, url0, maxts FROM lane WHERE n>0");
            while (s && sqlite3_step(s) == SQLITE_ROW) {
                LaneCount lc;
                lc.n = sqlite3_column_int64(s, 1);
                lc.bytes = sqlite3_column_int64(s, 2);
                lc.minw = sqlite3_column_type(s, 3) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 3);
                lc.maxw = sqlite3_column_type(s, 4) == SQLITE_NULL ? INT64_MIN : sqlite3_column_int64(s, 4);
                lc.maxseq = sqlite3_column_int64(s, 5);
                lc.created = sqlite3_column_int64(s, 6);
                lc.updated = sqlite3_column_int64(s, 7);
                lc.maxat = sqlite3_column_int64(s, 8);
                lc.url = colText(s, 9);
                lc.url0 = colText(s, 10);
                lc.maxts = sqlite3_column_int64(s, 11);
                f->lanes[uint32_t(sqlite3_column_int64(s, 0))] = lc;
            }
            if (s) sqlite3_reset(s);
            if (f->maxseq >= t->nextSeq) t->nextSeq = f->maxseq + 1;
        }
        if (any) {
            p->n = p->bytes = 0;
            for (auto& kv : p->files) {
                p->n += kv.second->n;
                p->bytes += kv.second->bytes;
            }
        }
    }
    for (auto& kv : conns) delete kv.second;
    (void)sp;
    t->visRecompute();
    return P4_OK;
}

}  // namespace p4
}  // namespace flatsql
