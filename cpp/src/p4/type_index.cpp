// Store format 4: the per-type index (design §3), derived and rebuildable.
//
//   c(tb, cid, pid, seq)            CID -> every stored copy; tb = content month (0 without one)
//   ident(tb, src, h, seq, cid)     IQC ingest identity -> holder
//   obj(k, tb, pid, n, fw, lw)      object directory across partitions (epoch nearest / as_of)
//   part, src, lanes, file, lanecnt the registry and counters, cached from the partition files
//   meta                            uniq, uniq_bytes, copies, next_seq, fts_through
//
// New entries live in an in-memory pending map until the maintenance thread
// flushes them in key order (one transaction per flush). Entries of groups in
// flight are visible to dedupe and never flushed.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

// ---- the pending map -----------------------------------------------------------------------
void PMap::init(uint32_t want) {
    uint32_t cap = 16;
    while (cap < want) cap <<= 1;
    a_.assign(cap, CEnt{});
    used_ = live_ = 0;
}

void PMap::grow() {
    std::vector<CEnt> old;
    old.swap(a_);
    a_.assign(old.empty() ? 1024 : old.size() * 2, CEnt{});
    used_ = live_ = 0;
    for (const CEnt& x : old)
        if (x.st == 1 || x.st == 2 || x.st == 4) put(x.key, x.tb, x.pid, x.seq, x.st);
}

void PMap::put(const uint8_t* key, int64_t tb, uint32_t pid, int64_t seq, uint8_t st) {
    if (a_.empty() || uint64_t(used_ + 1) * 10 > uint64_t(a_.size()) * 7) grow();
    const uint32_t mask = uint32_t(a_.size() - 1);
    for (uint32_t i = hashOf(key) & mask;; i = (i + 1) & mask) {
        CEnt& x = a_[i];
        if (x.st == 0) {
            std::memcpy(x.key, key, 32);
            x.tb = tb;
            x.pid = pid;
            x.seq = seq;
            x.st = st;
            used_++;
            live_++;
            return;
        }
        if (x.pid == pid && x.tb == tb && std::memcmp(x.key, key, 32) == 0) {
            if (x.st == 3) live_++;
            x.seq = seq;
            x.st = st;
            return;
        }
    }
}

void PMap::kill(int64_t tb, const uint8_t* key, uint32_t pid) {
    if (a_.empty()) return;
    const uint32_t mask = uint32_t(a_.size() - 1);
    for (uint32_t i = hashOf(key) & mask;; i = (i + 1) & mask) {
        CEnt& x = a_[i];
        if (x.st == 0) return;
        if (x.st != 3 && x.pid == pid && x.tb == tb && std::memcmp(x.key, key, 32) == 0) {
            x.st = 3;
            live_--;
            return;
        }
    }
}

std::string identMapKey(int64_t tb, uint64_t src, const uint8_t h[32]) {
    std::string k(48, '\0');
    st64(reinterpret_cast<uint8_t*>(&k[0]), uint64_t(tb));
    st64(reinterpret_cast<uint8_t*>(&k[8]), src);
    std::memcpy(&k[16], h, 32);
    return k;
}

void noteTb(Type* t, int64_t tb) {
    for (int64_t x : t->tbs)
        if (x == tb) return;
    t->tbs.push_back(tb);
    std::sort(t->tbs.begin(), t->tbs.end(), [](int64_t a, int64_t b) { return a > b; });
}

// ---- open: schema, registry and counters ---------------------------------------------------
namespace {
const char* kIndexSchema =
    "CREATE TABLE IF NOT EXISTS c(tb INTEGER NOT NULL, cid BLOB NOT NULL, pid INTEGER NOT NULL, seq INTEGER NOT NULL,"
    " PRIMARY KEY(tb, cid, pid)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS ident(tb INTEGER NOT NULL, src INTEGER NOT NULL, h BLOB NOT NULL, seq INTEGER NOT NULL,"
    " cid BLOB NOT NULL, PRIMARY KEY(tb, src, h)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS obj(k, tb INTEGER NOT NULL, pid INTEGER NOT NULL, n INTEGER, fw INTEGER, lw INTEGER,"
    " PRIMARY KEY(k, tb, pid)) WITHOUT ROWID;"
    "CREATE INDEX IF NOT EXISTS obj_tb ON obj(tb, pid);"
    "CREATE TABLE IF NOT EXISTS part(pid INTEGER PRIMARY KEY, producer TEXT NOT NULL, peer TEXT);"
    "CREATE TABLE IF NOT EXISTS src(id INTEGER PRIMARY KEY, provider TEXT NOT NULL, source TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS lanes(id INTEGER PRIMARY KEY, sid INTEGER NOT NULL, h INTEGER NOT NULL,"
    " batch TEXT, ckey TEXT, ppeer TEXT, pkey TEXT);"
    "CREATE TABLE IF NOT EXISTS file(pid INTEGER NOT NULL, tb INTEGER NOT NULL, gen INTEGER NOT NULL, n, bytes, ncopy,"
    " minseq, maxseq, minw, maxw, maxts, nnull, ix, mints, mine, maxe, PRIMARY KEY(pid, tb)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS gens(pid INTEGER NOT NULL, tb INTEGER NOT NULL, gen INTEGER NOT NULL,"
    " PRIMARY KEY(pid, tb)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS lanecnt(lane INTEGER NOT NULL, pid INTEGER NOT NULL, tb INTEGER NOT NULL, h INTEGER,"
    " n, bytes, minw, maxw, maxseq, created, updated, maxat, url, url0, maxts, PRIMARY KEY(lane, pid, tb)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v) WITHOUT ROWID;";

const char* ctext(sqlite3_stmt* s, int i) {
    const unsigned char* t = sqlite3_column_text(s, i);
    return t ? reinterpret_cast<const char*>(t) : "";
}
int64_t metaGet(Conn* c, const char* k, int64_t dflt) {
    sqlite3_stmt* s = c->sql("SELECT v FROM meta WHERE k=?1");
    int64_t v = dflt;
    if (!s) return v;
    sqlite3_bind_text(s, 1, k, -1, SQLITE_STATIC);
    if (sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
    sqlite3_reset(s);
    return v;
}
}  // namespace

int32_t typeIndexOpen(Type* t, std::string* err) {
    Conn* c = nullptr;
    int rc = openConn(t->pIdx, OpenKind::Index, 8192, 4096, &c, err);
    if (rc != SQLITE_OK) return statusOfSqlite(rc);
    rc = c->exec(kIndexSchema);
    if (rc != SQLITE_OK) {
        if (err) *err = sqlite3_errmsg(c->db);
        delete c;
        return statusOfSqlite(rc);
    }
    t->idx = c;
    std::lock_guard<std::mutex> g(t->mu);
    t->uniq = metaGet(c, "uniq", 0);
    t->uniqBytes = metaGet(c, "uniq_bytes", 0);
    t->copies = metaGet(c, "copies", 0);
    t->nextSeq = metaGet(c, "next_seq", 1);
    t->ftsThrough = metaGet(c, "fts_through", 0);
    sqlite3_stmt* s = c->sql("SELECT pid, producer, peer FROM part ORDER BY pid");
    while (s && sqlite3_step(s) == SQLITE_ROW) {
        const uint32_t pid = uint32_t(sqlite3_column_int64(s, 0));
        while (t->parts.size() + 1 < pid) partFor(t, "\x1f#" + std::to_string(t->parts.size() + 1), "", true);
        Part* p = partFor(t, ctext(s, 1), ctext(s, 2), true);
        p->journaled = true;
    }
    if (s) sqlite3_reset(s);
    s = c->sql("SELECT id, provider, source FROM src ORDER BY id");
    while (s && sqlite3_step(s) == SQLITE_ROW) {
        const uint32_t id = uint32_t(sqlite3_column_int64(s, 0));
        while (t->srcs.size() + 1 < id) srcFor(t, "\x1f#" + std::to_string(t->srcs.size() + 1), "", true);
        SrcDef* d = srcFor(t, ctext(s, 1), ctext(s, 2), true);
        t->srcJournaled[d->id - 1] = 1;
    }
    if (s) sqlite3_reset(s);
    s = c->sql("SELECT id, sid, batch, ckey, ppeer, pkey FROM lanes ORDER BY id");
    while (s && sqlite3_step(s) == SQLITE_ROW) {
        const uint32_t id = uint32_t(sqlite3_column_int64(s, 0));
        SrcDef* src = t->srcById(uint32_t(sqlite3_column_int64(s, 1)));
        while (t->lanes.size() + 1 < id) {
            std::string ph[6] = {"\x1f#" + std::to_string(t->lanes.size() + 1), "", "", "", "", ""};
            laneFor(t, ph, true);
        }
        std::string f6[6] = {src ? src->provider : "", src ? src->source : "", ctext(s, 2), ctext(s, 3), ctext(s, 4),
                             ctext(s, 5)};
        LaneDef* l = laneFor(t, f6, true);
        t->laneJournaled[l->id - 1] = 1;
    }
    if (s) sqlite3_reset(s);
    s = c->sql("SELECT pid, tb, gen, n, bytes, ncopy, minseq, maxseq, minw, maxw, maxts, nnull, ix, mints, mine, maxe FROM file");
    while (s && sqlite3_step(s) == SQLITE_ROW) {
        Part* p = t->partById(uint32_t(sqlite3_column_int64(s, 0)));
        if (!p) continue;
        auto f = std::make_unique<File>();
        f->part = p;
        f->tb = sqlite3_column_int64(s, 1);
        f->gen = sqlite3_column_int(s, 2);
        f->path = t->filePath(p->pid, f->tb, f->gen);
        f->n = sqlite3_column_int64(s, 3);
        f->bytes = sqlite3_column_int64(s, 4);
        f->ncopy = sqlite3_column_int64(s, 5);
        f->minseq = sqlite3_column_type(s, 6) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 6);
        f->maxseq = sqlite3_column_int64(s, 7);
        f->minw = sqlite3_column_type(s, 8) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 8);
        f->maxw = sqlite3_column_type(s, 9) == SQLITE_NULL ? INT64_MIN : sqlite3_column_int64(s, 9);
        f->maxts = sqlite3_column_int64(s, 10);
        f->nnull = sqlite3_column_int64(s, 11);
        f->indexed = sqlite3_column_type(s, 12) == SQLITE_NULL || sqlite3_column_int64(s, 12) != 0;
        f->mints = sqlite3_column_type(s, 13) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 13);
        f->mine = sqlite3_column_type(s, 14) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 14);
        f->maxe = sqlite3_column_type(s, 15) == SQLITE_NULL ? INT64_MIN : sqlite3_column_int64(s, 15);
        f->created = true;
        p->n += f->n;
        p->bytes += f->bytes;
        p->files[f->tb] = f.get();
        noteTb(t, f->tb);
        p->all.push_back(std::move(f));
    }
    if (s) sqlite3_reset(s);
    s = c->sql("SELECT pid, tb, gen FROM gens");
    while (s && sqlite3_step(s) == SQLITE_ROW) {
        Part* p = t->partById(uint32_t(sqlite3_column_int64(s, 0)));
        if (!p) continue;
        int32_t& g = p->maxGen[sqlite3_column_int64(s, 1)];
        g = std::max(g, sqlite3_column_int(s, 2));
    }
    if (s) sqlite3_reset(s);
    s = c->sql(
        "SELECT lane, pid, tb, n, bytes, minw, maxw, maxseq, created, updated, maxat, url, url0, maxts FROM lanecnt WHERE n>0");
    while (s && sqlite3_step(s) == SQLITE_ROW) {
        Part* p = t->partById(uint32_t(sqlite3_column_int64(s, 1)));
        if (!p) continue;
        auto it = p->files.find(sqlite3_column_int64(s, 2));
        if (it == p->files.end()) continue;
        LaneCount lc;
        lc.n = sqlite3_column_int64(s, 3);
        lc.bytes = sqlite3_column_int64(s, 4);
        lc.minw = sqlite3_column_type(s, 5) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 5);
        lc.maxw = sqlite3_column_type(s, 6) == SQLITE_NULL ? INT64_MIN : sqlite3_column_int64(s, 6);
        lc.maxseq = sqlite3_column_int64(s, 7);
        lc.created = sqlite3_column_int64(s, 8);
        lc.updated = sqlite3_column_int64(s, 9);
        lc.maxat = sqlite3_column_int64(s, 10);
        lc.url = ctext(s, 11);
        lc.url0 = ctext(s, 12);
        lc.maxts = sqlite3_column_int64(s, 13);
        it->second->lanes[uint32_t(sqlite3_column_int64(s, 0))] = lc;
    }
    if (s) sqlite3_reset(s);
    s = c->sql("SELECT DISTINCT tb FROM c");
    while (s && sqlite3_step(s) == SQLITE_ROW) noteTb(t, sqlite3_column_int64(s, 0));
    if (s) sqlite3_reset(s);
    t->visRecompute();
    return P4_OK;
}

// ---- probes ---------------------------------------------------------------------------------
Conn* indexReader(P4Lane* L, Type* t, int32_t* rc) {
    auto it = L->idx.find(t);
    if (it != L->idx.end()) return it->second;
    Conn* c = nullptr;
    const int r = openConn(t->pIdx, OpenKind::IndexReader, 2048, 0, &c, nullptr);
    if (r != SQLITE_OK) {
        *rc = statusOfSqlite(r);
        return nullptr;
    }
    L->idx[t] = c;
    return c;
}

// Every copy of (tb, key): the pending layers (newest state per pid; a
// delete hides the index row) over the type index. locked: Type::mu is held.
int32_t holdersWith(Type* t, Conn* c, int64_t tb, const uint8_t* key, std::vector<Holder>* out, bool locked) {
    out->clear();
    struct Seen {
        uint32_t pid;
        int64_t seq;
        bool del;
    };
    Seen seen[16];
    int ns = 0;
    {
        std::unique_lock<std::mutex> g(t->mu, std::defer_lock);
        if (!locked) g.lock();
        auto note = [&](const CEnt& x) {
            for (int i = 0; i < ns; i++)
                if (seen[i].pid == x.pid) return;
            if (ns < 16) seen[ns++] = Seen{x.pid, x.seq, x.st == 2};
        };
        t->pend.each(tb, key, note);
        t->flushing.each(tb, key, note);
    }
    sqlite3_stmt* s = c->get(S_C_GET);
    if (!s) return P4_E_INTERNAL;
    sqlite3_bind_int64(s, 1, tb);
    sqlite3_bind_blob(s, 2, key, 32, SQLITE_STATIC);
    int r;
    while ((r = sqlite3_step(s)) == SQLITE_ROW) {
        const uint32_t pid = uint32_t(sqlite3_column_int64(s, 0));
        bool dup = false;
        for (int i = 0; i < ns; i++) dup = dup || seen[i].pid == pid;
        if (!dup && ns < 16) seen[ns++] = Seen{pid, sqlite3_column_int64(s, 1), false};
    }
    sqlite3_reset(s);
    if (r != SQLITE_DONE) return statusOfSqlite(r);
    for (int i = 0; i < ns; i++)
        if (!seen[i].del) out->push_back(Holder{seen[i].pid, seen[i].seq});
    std::sort(out->begin(), out->end(), [](const Holder& a, const Holder& b) { return a.pid < b.pid; });
    return P4_OK;
}

int32_t holdersOf(P4Lane* L, Type* t, int64_t tb, const uint8_t* key, std::vector<Holder>* out) {
    int32_t rc = P4_OK;
    Conn* c = indexReader(L, t, &rc);
    if (!c) return rc;
    return holdersWith(t, c, tb, key, out, false);
}

int32_t identHolder(P4Lane* L, Type* t, int64_t tb, uint64_t src, const uint8_t h[32], int64_t* seq, uint8_t cid[32]) {
    *seq = 0;
    const std::string k = identMapKey(tb, src, h);
    {
        std::lock_guard<std::mutex> g(t->mu);
        auto it = t->identPend.find(k);
        if (it == t->identPend.end()) {
            it = t->identFlushing.find(k);
            if (it == t->identFlushing.end()) it = t->identPend.end();
        }
        if (it != t->identPend.end()) {
            if (it->second.st == 2) return P4_OK;
            *seq = it->second.seq;
            std::memcpy(cid, it->second.cid, 32);
            return P4_OK;
        }
    }
    int32_t rc = P4_OK;
    Conn* c = indexReader(L, t, &rc);
    if (!c) return rc;
    sqlite3_stmt* s = c->get(S_IDENT_GET);
    if (!s) return P4_E_INTERNAL;
    sqlite3_bind_int64(s, 1, tb);
    sqlite3_bind_int64(s, 2, int64_t(src));
    sqlite3_bind_blob(s, 3, h, 32, SQLITE_STATIC);
    const int r = sqlite3_step(s);
    if (r == SQLITE_ROW && sqlite3_column_bytes(s, 1) == 32) {
        *seq = sqlite3_column_int64(s, 0);
        std::memcpy(cid, sqlite3_column_blob(s, 1), 32);
    }
    sqlite3_reset(s);
    return r == SQLITE_ROW || r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}

// ---- flush ----------------------------------------------------------------------------------
namespace {
struct FileSnap {
    uint32_t pid;
    int64_t tb;
    int32_t gen;
    bool live;
    bool created;  // its schema committed; until then only the generation is recorded
    int64_t n, bytes, ncopy, minseq, maxseq, minw, maxw, maxts, nnull, mints, mine, maxe;
    bool indexed, objRefresh;
    std::string path;
    std::vector<std::pair<uint32_t, LaneCount>> lanes;
};
struct ObjTouch {
    uint32_t pid;
    int64_t tb;
    std::string k;  // the object key as stored (int: 8 bytes LE with a 'i' tag; text: 't' + bytes)
};
}  // namespace

// Completes a drop (J_DROP) a crash interrupted before the index rows went:
// the partition's rows of that month leave c, obj, lanecnt and file, ident
// rows whose record had no other copy leave too, and the counters lose the
// copies (a record held only here is no longer unique). Opening only
// (Type::mu held, the index connection unshared).
int32_t dropIndexRows(Type* t, uint32_t pid, int64_t tb) {
    Conn* c = t->idx;
    int64_t n = 0, last = 0, fileBytes = 0, fileN = 0;
    sqlite3_stmt* s = c->sql(
        "SELECT count(*), coalesce(sum(NOT EXISTS (SELECT 1 FROM c y WHERE y.tb=x.tb AND y.cid=x.cid AND y.pid<>x.pid)),0)"
        " FROM c x WHERE x.tb=?1 AND x.pid=?2");
    if (!s) return P4_E_IO;
    sqlite3_bind_int64(s, 1, tb);
    sqlite3_bind_int64(s, 2, pid);
    int rc = sqlite3_step(s);
    if (rc == SQLITE_ROW) {
        n = sqlite3_column_int64(s, 0);
        last = sqlite3_column_int64(s, 1);
    }
    sqlite3_reset(s);
    if (rc != SQLITE_ROW) return statusOfSqlite(rc);
    s = c->sql("SELECT coalesce(sum(n),0), coalesce(sum(bytes),0) FROM file WHERE pid=?1 AND tb=?2");
    sqlite3_bind_int64(s, 1, pid);
    sqlite3_bind_int64(s, 2, tb);
    if (sqlite3_step(s) == SQLITE_ROW) {
        fileN = sqlite3_column_int64(s, 0);
        fileBytes = sqlite3_column_int64(s, 1);
    }
    sqlite3_reset(s);
    if (n == 0 && fileN == 0) return P4_OK;
    rc = c->exec("BEGIN IMMEDIATE");
    const char* sqls[] = {
        ("DELETE FROM ident WHERE tb=?1 AND cid IN (SELECT x.cid FROM c x WHERE x.tb=?1 AND x.pid=?2 AND NOT EXISTS"
         " (SELECT 1 FROM c y WHERE y.tb=x.tb AND y.cid=x.cid AND y.pid<>x.pid))"),
        "DELETE FROM c WHERE tb=?1 AND pid=?2", "DELETE FROM obj WHERE tb=?1 AND pid=?2",
        "DELETE FROM lanecnt WHERE tb=?1 AND pid=?2", "DELETE FROM file WHERE tb=?1 AND pid=?2"};
    for (const char* sql : sqls) {
        if (rc != SQLITE_OK) break;
        sqlite3_stmt* d = c->sql(sql);
        if (!d) { rc = SQLITE_ERROR; break; }
        sqlite3_bind_int64(d, 1, tb);
        sqlite3_bind_int64(d, 2, pid);
        const int r = sqlite3_step(d);
        sqlite3_reset(d);
        if (r != SQLITE_DONE) rc = r;
    }
    t->uniq = std::max<int64_t>(0, t->uniq - last);
    t->copies = std::max<int64_t>(0, t->copies - (n - last));
    if (fileN > 0) t->uniqBytes = std::max<int64_t>(0, t->uniqBytes - int64_t(double(fileBytes) * double(last) / double(fileN)));
    const char* mk[3] = {"uniq", "uniq_bytes", "copies"};
    const int64_t mv[3] = {t->uniq, t->uniqBytes, t->copies};
    for (int i = 0; i < 3 && rc == SQLITE_OK; i++) {
        sqlite3_stmt* m = c->sql("INSERT OR REPLACE INTO meta(k, v) VALUES(?1,?2)");
        sqlite3_bind_text(m, 1, mk[i], -1, SQLITE_STATIC);
        sqlite3_bind_int64(m, 2, mv[i]);
        const int r = sqlite3_step(m);
        sqlite3_reset(m);
        if (r != SQLITE_DONE) rc = r;
    }
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) {
        c->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    bool held = false;  // another partition may still hold the month
    for (auto& p : t->parts) held = held || p->files.count(tb);
    if (!held) t->tbs.erase(std::remove(t->tbs.begin(), t->tbs.end(), tb), t->tbs.end());
    return P4_OK;
}

int32_t typeIndexFlush(Type* t, bool force) {
    Engine* e = t->e;
    std::lock_guard<std::mutex> fg(t->flushMu);
    std::vector<CEnt> ents;
    std::vector<IdentEnt> idents;
    std::vector<FileSnap> files;
    std::vector<std::pair<uint32_t, std::pair<std::string, std::string>>> parts;
    std::vector<SrcDef> srcs;
    std::vector<LaneDef> lanes;
    std::vector<std::string> touched;
    int64_t meta[4];
    int64_t jcut;
    {
        std::lock_guard<std::mutex> g(t->mu);
        bool dirty = t->pend.live() > 0 || !t->identPend.empty() || !t->touchedObj.empty();
        for (auto& p : t->parts)
            for (auto& f : p->all) dirty = dirty || f->touched;
        if (!dirty) return P4_OK;
        if (!force && t->pend.live() < e->cfg.flushEntries &&
            wallMs() - t->lastFlushMs < 30000 && t->pend.bytes() < e->cfg.pendingBytes / 4)
            return P4_OK;
        // Swap: in-flight entries stay pending.
        std::swap(t->pend, t->flushing);
        t->pend.init(1024);
        for (CEnt& x : t->flushing.raw()) {
            if (x.st == 4) {
                t->pend.put(x.key, x.tb, x.pid, x.seq, 4);
                x.st = 3;
            } else if (x.st == 1 || x.st == 2) {
                ents.push_back(x);
            }
        }
        t->identFlushing.swap(t->identPend);
        t->identPend.clear();
        for (auto it = t->identFlushing.begin(); it != t->identFlushing.end();) {
            if (it->second.st == 4) {
                t->identPend[it->first] = it->second;
                it = t->identFlushing.erase(it);
            } else {
                idents.push_back(it->second);
                ++it;
            }
        }
        // Journal rows up to the cut belong to groups whose entries are all in
        // this flush: every group journaled before now that has committed.
        jcut = t->jlast;
        for (int64_t first : t->jinflight)
            if (first - 1 < jcut) jcut = first - 1;
        for (auto& p : t->parts) {
            parts.push_back({p->pid, {p->producer, p->peer}});
            for (auto& f : p->all) {
                if (!f->touched) continue;
                f->touched = false;
                FileSnap fs;
                fs.pid = p->pid;
                fs.tb = f->tb;
                fs.gen = f->gen;
                auto lf = p->files.find(f->tb);
                fs.live = !f->retired && lf != p->files.end() && lf->second == f.get();
                fs.created = f->created;
                fs.n = f->n; fs.bytes = f->bytes; fs.ncopy = f->ncopy; fs.minseq = f->minseq; fs.maxseq = f->maxseq;
                fs.minw = f->minw; fs.maxw = f->maxw; fs.maxts = f->maxts; fs.nnull = f->nnull;
                fs.mints = f->mints; fs.mine = f->mine; fs.maxe = f->maxe;
                fs.indexed = f->indexed;
                fs.objRefresh = f->objRefresh && f->created && !f->retired;
                f->objRefresh = false;
                fs.path = f->path;
                for (auto& kv : f->lanes) fs.lanes.push_back(kv);
                files.push_back(std::move(fs));
            }
        }
        for (auto& s : t->srcs) srcs.push_back(*s);
        for (auto& l : t->lanes) lanes.push_back(*l);
        touched.assign(t->touchedObj.begin(), t->touchedObj.end());
        t->touchedObj.clear();
        meta[0] = t->uniq;
        meta[1] = t->uniqBytes;
        meta[2] = t->copies;
        meta[3] = t->nextSeq;
        t->lastFlushMs = wallMs();
    }
    std::sort(ents.begin(), ents.end(), [](const CEnt& a, const CEnt& b) {
        if (a.tb != b.tb) return a.tb < b.tb;
        const int c = std::memcmp(a.key, b.key, 32);
        if (c) return c < 0;
        return a.pid < b.pid;
    });
    Conn* c = t->idx;
    int rc = c->exec("BEGIN IMMEDIATE");
    auto step = [&](sqlite3_stmt* s) {
        if (!s) { rc = SQLITE_ERROR; return; }
        const int r = sqlite3_step(s);
        if (r != SQLITE_DONE && rc == SQLITE_OK) rc = r;
        sqlite3_reset(s);
    };
    for (const CEnt& x : ents) {
        if (rc != SQLITE_OK) break;
        if (x.st == 1) {
            sqlite3_stmt* s = c->get(S_C_INS);
            sqlite3_bind_int64(s, 1, x.tb);
            sqlite3_bind_blob(s, 2, x.key, 32, SQLITE_STATIC);
            sqlite3_bind_int64(s, 3, x.pid);
            sqlite3_bind_int64(s, 4, x.seq);
            step(s);
        } else {
            sqlite3_stmt* s = c->get(S_C_DEL);
            sqlite3_bind_int64(s, 1, x.tb);
            sqlite3_bind_blob(s, 2, x.key, 32, SQLITE_STATIC);
            sqlite3_bind_int64(s, 3, x.pid);
            step(s);
        }
    }
    for (const IdentEnt& x : idents) {
        if (rc != SQLITE_OK) break;
        if (x.st == 1) {
            sqlite3_stmt* s = c->get(S_IDENT_INS);
            sqlite3_bind_int64(s, 1, x.tb);
            sqlite3_bind_int64(s, 2, int64_t(x.src));
            sqlite3_bind_blob(s, 3, x.h, 32, SQLITE_STATIC);
            sqlite3_bind_int64(s, 4, x.seq);
            sqlite3_bind_blob(s, 5, x.cid, 32, SQLITE_STATIC);
            step(s);
        } else {
            sqlite3_stmt* s = c->get(S_IDENT_DEL);
            sqlite3_bind_int64(s, 1, x.tb);
            sqlite3_bind_int64(s, 2, int64_t(x.src));
            sqlite3_bind_blob(s, 3, x.h, 32, SQLITE_STATIC);
            step(s);
        }
    }
    for (auto& p : parts) {
        if (rc != SQLITE_OK || p.second.first.rfind("\x1f#", 0) == 0) continue;
        sqlite3_stmt* s = c->sql("INSERT OR REPLACE INTO part(pid, producer, peer) VALUES(?1,?2,?3)");
        sqlite3_bind_int64(s, 1, p.first);
        sqlite3_bind_text(s, 2, p.second.first.data(), int(p.second.first.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 3, p.second.second.data(), int(p.second.second.size()), SQLITE_STATIC);
        step(s);
    }
    for (auto& s0 : srcs) {
        if (rc != SQLITE_OK || s0.provider.rfind("\x1f#", 0) == 0) continue;
        sqlite3_stmt* s = c->sql("INSERT OR IGNORE INTO src(id, provider, source) VALUES(?1,?2,?3)");
        sqlite3_bind_int64(s, 1, s0.id);
        sqlite3_bind_text(s, 2, s0.provider.data(), int(s0.provider.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 3, s0.source.data(), int(s0.source.size()), SQLITE_STATIC);
        step(s);
    }
    for (auto& l : lanes) {
        if (rc != SQLITE_OK || l.provider.rfind("\x1f#", 0) == 0) continue;
        sqlite3_stmt* s =
            c->sql("INSERT OR IGNORE INTO lanes(id, sid, h, batch, ckey, ppeer, pkey) VALUES(?1,?2,?3,?4,?5,?6,?7)");
        sqlite3_bind_int64(s, 1, l.id);
        sqlite3_bind_int64(s, 2, l.sid);
        sqlite3_bind_int64(s, 3, int64_t(l.h));
        sqlite3_bind_text(s, 4, l.batch.data(), int(l.batch.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 5, l.ckey.data(), int(l.ckey.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 6, l.ppeer.data(), int(l.ppeer.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 7, l.pkey.data(), int(l.pkey.size()), SQLITE_STATIC);
        step(s);
    }
    for (const FileSnap& f : files) {
        if (rc != SQLITE_OK) break;
        {
            sqlite3_stmt* s = c->sql(
                "INSERT INTO gens(pid, tb, gen) VALUES(?1,?2,?3) ON CONFLICT(pid, tb) DO UPDATE SET gen=max(gen, excluded.gen)");
            sqlite3_bind_int64(s, 1, f.pid);
            sqlite3_bind_int64(s, 2, f.tb);
            sqlite3_bind_int64(s, 3, f.gen);
            step(s);
        }
        if (!f.live) {
            sqlite3_stmt* s = c->sql("DELETE FROM file WHERE pid=?1 AND tb=?2 AND gen=?3");
            sqlite3_bind_int64(s, 1, f.pid);
            sqlite3_bind_int64(s, 2, f.tb);
            sqlite3_bind_int64(s, 3, f.gen);
            step(s);
            continue;
        }
        // A file row means a file with its schema (a reopen trusts it); one
        // whose creation has not committed is only a used generation.
        if (!f.created) continue;
        sqlite3_stmt* s = c->sql(
            "INSERT OR REPLACE INTO file(pid, tb, gen, n, bytes, ncopy, minseq, maxseq, minw, maxw, maxts, nnull, ix,"
            " mints, mine, maxe) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)");
        sqlite3_bind_int64(s, 1, f.pid);
        sqlite3_bind_int64(s, 2, f.tb);
        sqlite3_bind_int64(s, 3, f.gen);
        sqlite3_bind_int64(s, 4, f.n);
        sqlite3_bind_int64(s, 5, f.bytes);
        sqlite3_bind_int64(s, 6, f.ncopy);
        if (f.n) sqlite3_bind_int64(s, 7, f.minseq); else sqlite3_bind_null(s, 7);
        sqlite3_bind_int64(s, 8, f.maxseq);
        if (f.n) sqlite3_bind_int64(s, 9, f.minw); else sqlite3_bind_null(s, 9);
        if (f.n) sqlite3_bind_int64(s, 10, f.maxw); else sqlite3_bind_null(s, 10);
        sqlite3_bind_int64(s, 11, f.maxts);
        sqlite3_bind_int64(s, 12, f.nnull);
        sqlite3_bind_int(s, 13, f.indexed ? 1 : 0);
        if (f.mints != INT64_MAX) sqlite3_bind_int64(s, 14, f.mints); else sqlite3_bind_null(s, 14);
        if (f.mine != INT64_MAX) sqlite3_bind_int64(s, 15, f.mine); else sqlite3_bind_null(s, 15);
        if (f.maxe != INT64_MIN) sqlite3_bind_int64(s, 16, f.maxe); else sqlite3_bind_null(s, 16);
        step(s);
        s = c->sql("DELETE FROM lanecnt WHERE pid=?1 AND tb=?2 AND lane=?3");
        sqlite3_stmt* del = s;
        (void)del;
        {
            sqlite3_stmt* d = c->sql("SELECT lane FROM lanecnt WHERE pid=?1 AND tb=?2");
            std::vector<int64_t> have;
            sqlite3_bind_int64(d, 1, f.pid);
            sqlite3_bind_int64(d, 2, f.tb);
            while (sqlite3_step(d) == SQLITE_ROW) have.push_back(sqlite3_column_int64(d, 0));
            sqlite3_reset(d);
            for (int64_t lane : have) {
                bool live = false;
                for (auto& kv : f.lanes) live = live || int64_t(kv.first) == lane;
                if (live) continue;
                sqlite3_stmt* x = c->sql("DELETE FROM lanecnt WHERE lane=?1 AND pid=?2 AND tb=?3");
                sqlite3_bind_int64(x, 1, lane);
                sqlite3_bind_int64(x, 2, f.pid);
                sqlite3_bind_int64(x, 3, f.tb);
                step(x);
            }
        }
        for (auto& kv : f.lanes) {
            const LaneCount& lc = kv.second;
            sqlite3_stmt* x = c->sql(
                "INSERT OR REPLACE INTO lanecnt(lane, pid, tb, h, n, bytes, minw, maxw, maxseq, created, updated, maxat,"
                " url, url0, maxts) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)");
            uint64_t h = 0;
            for (auto& l : lanes)
                if (l.id == kv.first) h = l.h;
            sqlite3_bind_int64(x, 1, kv.first);
            sqlite3_bind_int64(x, 2, f.pid);
            sqlite3_bind_int64(x, 3, f.tb);
            sqlite3_bind_int64(x, 4, int64_t(h));
            sqlite3_bind_int64(x, 5, lc.n);
            sqlite3_bind_int64(x, 6, lc.bytes);
            if (lc.minw != INT64_MAX) sqlite3_bind_int64(x, 7, lc.minw); else sqlite3_bind_null(x, 7);
            if (lc.maxw != INT64_MIN) sqlite3_bind_int64(x, 8, lc.maxw); else sqlite3_bind_null(x, 8);
            sqlite3_bind_int64(x, 9, lc.maxseq);
            sqlite3_bind_int64(x, 10, lc.created);
            sqlite3_bind_int64(x, 11, lc.updated);
            sqlite3_bind_int64(x, 12, lc.maxat);
            sqlite3_bind_text(x, 13, lc.url.data(), int(lc.url.size()), SQLITE_STATIC);
            sqlite3_bind_text(x, 14, lc.url0.data(), int(lc.url0.size()), SQLITE_STATIC);
            sqlite3_bind_int64(x, 15, lc.maxts);
            step(x);
        }
    }
    // obj: whole files (replay) and touched (pid, tb, k).
    {
        std::unordered_map<std::string, Conn*> fconns;
        auto fileConn = [&](const std::string& path) -> Conn* {
            auto it = fconns.find(path);
            if (it != fconns.end()) return it->second;
            Conn* fc = nullptr;
            if (openConn(path, OpenKind::Maint, 2048, 0, &fc, nullptr) != SQLITE_OK) fc = nullptr;
            fconns[path] = fc;
            return fc;
        };
        std::shared_ptr<const Spec> sp = t->spec();
        if (sp->ek) {
            for (const FileSnap& f : files) {
                if (rc != SQLITE_OK) break;
                if (!f.objRefresh && f.live) continue;
                sqlite3_stmt* d = c->sql("DELETE FROM obj WHERE tb=?1 AND pid=?2");
                sqlite3_bind_int64(d, 1, f.tb);
                sqlite3_bind_int64(d, 2, f.pid);
                step(d);
                if (!f.live) continue;
                Conn* fc = fileConn(f.path);
                if (!fc) continue;
                sqlite3_stmt* q = fc->sql("SELECT k, n, fw, lw FROM ent WHERE n>0");
                while (q && sqlite3_step(q) == SQLITE_ROW) {
                    sqlite3_stmt* x = c->sql("INSERT OR REPLACE INTO obj(k, tb, pid, n, fw, lw) VALUES(?1,?2,?3,?4,?5,?6)");
                    sqlite3_bind_value(x, 1, sqlite3_column_value(q, 0));
                    sqlite3_bind_int64(x, 2, f.tb);
                    sqlite3_bind_int64(x, 3, f.pid);
                    sqlite3_bind_int64(x, 4, sqlite3_column_int64(q, 1));
                    sqlite3_bind_int64(x, 5, sqlite3_column_int64(q, 2));
                    sqlite3_bind_int64(x, 6, sqlite3_column_int64(q, 3));
                    step(x);
                }
                if (q) sqlite3_reset(q);
            }
            std::sort(touched.begin(), touched.end());
            for (const std::string& k : touched) {
                if (rc != SQLITE_OK) break;
                // pid (4) | tb (8) | 'i' + i64 | 't' + text
                if (k.size() < 13) continue;
                const uint8_t* b = reinterpret_cast<const uint8_t*>(k.data());
                const uint32_t pid = ld32(b);
                const int64_t tb = int64_t(ld64(b + 4));
                std::string path;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    Part* p = t->partById(pid);
                    auto it = p ? p->files.find(tb) : decltype(p->files.end()){};
                    if (p && it != p->files.end() && it->second->created && !it->second->retired) path = it->second->path;
                }
                auto bindK = [&](sqlite3_stmt* s, int i) {
                    if (b[12] == 'i' && k.size() == 21) sqlite3_bind_int64(s, i, int64_t(ld64(b + 13)));
                    else sqlite3_bind_text(s, i, k.data() + 13, int(k.size() - 13), SQLITE_STATIC);
                };
                int64_t n = 0, fw = 0, lw = 0;
                Conn* fc = path.empty() ? nullptr : fileConn(path);
                if (fc) {
                    sqlite3_stmt* q = fc->get(S_ENT_ONE);
                    bindK(q, 1);
                    if (sqlite3_step(q) == SQLITE_ROW) {
                        n = sqlite3_column_int64(q, 0);
                        fw = sqlite3_column_int64(q, 1);
                        lw = sqlite3_column_int64(q, 2);
                    }
                    sqlite3_reset(q);
                }
                if (n > 0) {
                    sqlite3_stmt* x = c->sql("INSERT OR REPLACE INTO obj(k, tb, pid, n, fw, lw) VALUES(?1,?2,?3,?4,?5,?6)");
                    bindK(x, 1);
                    sqlite3_bind_int64(x, 2, tb);
                    sqlite3_bind_int64(x, 3, pid);
                    sqlite3_bind_int64(x, 4, n);
                    sqlite3_bind_int64(x, 5, fw);
                    sqlite3_bind_int64(x, 6, lw);
                    step(x);
                } else {
                    sqlite3_stmt* x = c->sql("DELETE FROM obj WHERE k=?1 AND tb=?2 AND pid=?3");
                    bindK(x, 1);
                    sqlite3_bind_int64(x, 2, tb);
                    sqlite3_bind_int64(x, 3, pid);
                    step(x);
                }
            }
        }
        for (auto& kv : fconns) delete kv.second;
    }
    {
        const char* mk[4] = {"uniq", "uniq_bytes", "copies", "next_seq"};
        for (int i = 0; i < 4 && rc == SQLITE_OK; i++) {
            sqlite3_stmt* s = c->sql("INSERT OR REPLACE INTO meta(k, v) VALUES(?1,?2)");
            sqlite3_bind_text(s, 1, mk[i], -1, SQLITE_STATIC);
            sqlite3_bind_int64(s, 2, meta[i]);
            step(s);
        }
    }
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) {
        c->exec("ROLLBACK");
        // Put everything back: the next flush retries.
        std::lock_guard<std::mutex> g(t->mu);
        for (const CEnt& x : ents) {
            bool newer = false;
            t->pend.each(x.tb, x.key, [&](const CEnt& y) { newer = newer || y.pid == x.pid; });
            if (!newer) t->pend.put(x.key, x.tb, x.pid, x.seq, x.st);
        }
        for (const IdentEnt& x : idents) {
            const std::string k = identMapKey(x.tb, x.src, x.h);
            if (!t->identPend.count(k)) t->identPend[k] = x;
        }
        for (auto& p : t->parts)
            for (auto& f : p->all)
                for (const FileSnap& fs : files)
                    if (fs.pid == p->pid && fs.tb == f->tb && fs.gen == f->gen) {
                        f->touched = true;
                        f->objRefresh = f->objRefresh || fs.objRefresh;
                    }
        for (const std::string& k : touched) t->touchedObj.insert(k);
        t->flushing.clear();
        t->identFlushing.clear();
        return statusOfSqlite(rc);
    }
    e->bump(kStIndexFlushes);
    e->bump(kStIndexFlushEntries, ents.size());
    if (jcut > 0) {
        std::lock_guard<std::mutex> g(t->jmu);
        sqlite3_stmt* s = t->jdb->get(S_J_DEL);
        if (s) {
            sqlite3_bind_int64(s, 1, jcut);
            sqlite3_step(s);
            sqlite3_reset(s);
        }
        journalReclaim(t, 256);  // 1 MiB of slack for the next entries
    }
    std::lock_guard<std::mutex> g(t->mu);
    t->flushing.clear();
    t->flushing.init(16);
    t->identFlushing.clear();
    return P4_OK;
}

}  // namespace p4
}  // namespace flatsql
