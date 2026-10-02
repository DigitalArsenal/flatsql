// Store format 4: the per-type index (design §3), derived and rebuildable.
//
//   c(cid, pid, seq)                CID -> every stored copy (dedupe, GET, TAGS, DELETE)
//   ident(src, h, seq, cid)         IQC ingest identity -> holder
//   part, src, lanes, lanecnt       the registry and each partition's counters, cached from its file
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
        if (x.st == 1 || x.st == 2 || x.st == 4) put(x.key, x.pid, x.seq, x.st);
}

void PMap::put(const uint8_t* key, uint32_t pid, int64_t seq, uint8_t st) {
    if (a_.empty() || uint64_t(used_ + 1) * 10 > uint64_t(a_.size()) * 7) grow();
    const uint32_t mask = uint32_t(a_.size() - 1);
    for (uint32_t i = hashOf(key) & mask;; i = (i + 1) & mask) {
        CEnt& x = a_[i];
        if (x.st == 0) {
            std::memcpy(x.key, key, 32);
            x.pid = pid;
            x.seq = seq;
            x.st = st;
            used_++;
            live_++;
            return;
        }
        if (x.pid == pid && std::memcmp(x.key, key, 32) == 0) {
            if (x.st == 3) live_++;
            x.seq = seq;
            x.st = st;
            return;
        }
    }
}

void PMap::kill(const uint8_t* key, uint32_t pid) {
    if (a_.empty()) return;
    const uint32_t mask = uint32_t(a_.size() - 1);
    for (uint32_t i = hashOf(key) & mask;; i = (i + 1) & mask) {
        CEnt& x = a_[i];
        if (x.st == 0) return;
        if (x.st != 3 && x.pid == pid && std::memcmp(x.key, key, 32) == 0) {
            x.st = 3;
            live_--;
            return;
        }
    }
}

std::string identMapKey(uint64_t src, const uint8_t h[32]) {
    std::string k(40, '\0');
    st64(reinterpret_cast<uint8_t*>(&k[0]), src);
    std::memcpy(&k[8], h, 32);
    return k;
}

// ---- open: schema, registry and counters ---------------------------------------------------
namespace {
const char* kIndexSchema =
    "CREATE TABLE IF NOT EXISTS c(cid BLOB NOT NULL, pid INTEGER NOT NULL, seq INTEGER NOT NULL,"
    " PRIMARY KEY(cid, pid)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS ident(src INTEGER NOT NULL, h BLOB NOT NULL, seq INTEGER NOT NULL,"
    " cid BLOB NOT NULL, PRIMARY KEY(src, h)) WITHOUT ROWID;"
    // A partition's counters are NULL until its file has its schema.
    "CREATE TABLE IF NOT EXISTS part(pid INTEGER PRIMARY KEY, producer TEXT NOT NULL, peer TEXT, n, bytes, ncopy,"
    " minseq, maxseq, minw, maxw, maxts, nnull, ix, mints, mine, maxe);"
    "CREATE TABLE IF NOT EXISTS src(id INTEGER PRIMARY KEY, provider TEXT NOT NULL, source TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS lanes(id INTEGER PRIMARY KEY, sid INTEGER NOT NULL, h INTEGER NOT NULL,"
    " batch TEXT, ckey TEXT, ppeer TEXT, pkey TEXT);"
    "CREATE TABLE IF NOT EXISTS lanecnt(lane INTEGER NOT NULL, pid INTEGER NOT NULL, h INTEGER,"
    " n, bytes, minw, maxw, maxseq, created, updated, maxat, url, url0, maxts, minseq, PRIMARY KEY(lane, pid)) WITHOUT ROWID;"
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
    sqlite3_wal_hook(c->db, walHook, t->e);  // its WAL is checkpointed by the maintenance thread
    t->idx = c;
    std::lock_guard<std::mutex> g(t->mu);
    // Every registry read either completes or fails the open (M9): a statement
    // that cannot run must not read as an empty registry.
    auto broken = [&](const char* what) {
        if (err) *err = std::string("type index: ") + what + ": " + sqlite3_errmsg(c->db);
        return P4_E_IO;
    };
    t->uniq = metaGet(c, "uniq", 0);
    t->uniqBytes = metaGet(c, "uniq_bytes", 0);
    t->copies = metaGet(c, "copies", 0);
    t->nextSeq = metaGet(c, "next_seq", 1);
    t->ftsThrough = metaGet(c, "fts_through", 0);
    sqlite3_stmt* s = c->sql(
        "SELECT pid, producer, peer, n, bytes, ncopy, minseq, maxseq, minw, maxw, maxts, nnull, ix, mints, mine, maxe"
        " FROM part ORDER BY pid");
    if (!s) return broken("prepare");
    int sr;
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        const uint32_t pid = uint32_t(sqlite3_column_int64(s, 0));
        while (t->parts.size() + 1 < pid) partFor(t, "\x1f#" + std::to_string(t->parts.size() + 1), "", true);
        Part* f = partFor(t, ctext(s, 1), ctext(s, 2), true);
        f->journaled = true;
        if (sqlite3_column_type(s, 3) == SQLITE_NULL) continue;  // no file yet
        auto opt = [&](int i, int64_t none) { return sqlite3_column_type(s, i) == SQLITE_NULL ? none : sqlite3_column_int64(s, i); };
        f->n = sqlite3_column_int64(s, 3);
        f->bytes = sqlite3_column_int64(s, 4);
        f->ncopy = sqlite3_column_int64(s, 5);
        f->minseq = opt(6, INT64_MAX);
        f->maxseq = sqlite3_column_int64(s, 7);
        f->minw = opt(8, INT64_MAX);
        f->maxw = opt(9, INT64_MIN);
        f->maxts = sqlite3_column_int64(s, 10);
        f->nnull = sqlite3_column_int64(s, 11);
        f->indexed = sqlite3_column_type(s, 12) == SQLITE_NULL || sqlite3_column_int64(s, 12) != 0;
        f->mints = opt(13, INT64_MAX);
        f->mine = opt(14, INT64_MAX);
        f->maxe = opt(15, INT64_MIN);
        f->created = true;
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read");
    s = c->sql("SELECT id, provider, source FROM src ORDER BY id");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        const uint32_t id = uint32_t(sqlite3_column_int64(s, 0));
        while (t->srcs.size() + 1 < id) srcFor(t, "\x1f#" + std::to_string(t->srcs.size() + 1), "", true);
        SrcDef* d = srcFor(t, ctext(s, 1), ctext(s, 2), true);
        t->srcJournaled[d->id - 1] = 1;
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read");
    s = c->sql("SELECT id, sid, batch, ckey, ppeer, pkey FROM lanes ORDER BY id");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
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
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read");
    s = c->sql(
        "SELECT lane, pid, n, bytes, minw, maxw, maxseq, created, updated, maxat, url, url0, maxts, minseq FROM lanecnt"
        " WHERE n>0");
    if (!s) return broken("prepare");
    while ((sr = sqlite3_step(s)) == SQLITE_ROW) {
        Part* p = t->partById(uint32_t(sqlite3_column_int64(s, 1)));
        if (!p) continue;
        LaneCount lc;
        lc.n = sqlite3_column_int64(s, 2);
        lc.bytes = sqlite3_column_int64(s, 3);
        lc.minw = sqlite3_column_type(s, 4) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 4);
        lc.maxw = sqlite3_column_type(s, 5) == SQLITE_NULL ? INT64_MIN : sqlite3_column_int64(s, 5);
        lc.maxseq = sqlite3_column_int64(s, 6);
        lc.created = sqlite3_column_int64(s, 7);
        lc.updated = sqlite3_column_int64(s, 8);
        lc.maxat = sqlite3_column_int64(s, 9);
        lc.url = ctext(s, 10);
        lc.url0 = ctext(s, 11);
        lc.maxts = sqlite3_column_int64(s, 12);
        lc.minseq = sqlite3_column_type(s, 13) == SQLITE_NULL ? INT64_MAX : sqlite3_column_int64(s, 13);
        p->lanes[uint32_t(sqlite3_column_int64(s, 0))] = lc;
    }
    sqlite3_reset(s);
    if (sr != SQLITE_DONE) return broken("read");
    t->visRecompute();
    return P4_OK;
}

// ---- probes ---------------------------------------------------------------------------------
Conn* indexReader(P4Lane* L, Type* t, int32_t* rc) {
    *rc = P4_OK;
    if (!t->hasFiles.load(std::memory_order_acquire)) return nullptr;  // no data yet
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

// Every copy of key: the pending layers (newest state per pid; a delete
// hides the index row) over the type index. locked: Type::mu is held.
int32_t holdersWith(Type* t, Conn* c, const uint8_t* key, std::vector<Holder>* out, bool locked) {
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
        t->pend.each(key, note);
        t->flushing.each(key, note);
    }
    sqlite3_stmt* s = c->get(S_C_GET);
    if (!s) return P4_E_INTERNAL;
    sqlite3_bind_blob(s, 1, key, 32, SQLITE_STATIC);
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

int32_t holdersOf(P4Lane* L, Type* t, const uint8_t* key, std::vector<Holder>* out) {
    int32_t rc = P4_OK;
    Conn* c = indexReader(L, t, &rc);
    if (!c) {
        out->clear();
        return rc;  // P4_OK: the type has no data yet
    }
    return holdersWith(t, c, key, out, false);
}

int othersHolding(P4Lane* L, Type* t, const uint8_t* key, uint32_t pid) {
    {
        std::lock_guard<std::mutex> g(t->mu);
        if (t->copies <= 0) {
            // No published CID has a second holder: only pending (in-flight or
            // unflushed) entries of other partitions can.
            uint32_t seen[16];
            int n = 0;
            auto note = [&](const CEnt& x) {
                if (x.pid == pid || (x.st != 1 && x.st != 4)) return;
                for (int i = 0; i < n; i++)
                    if (seen[i] == x.pid) return;
                if (n < 16) seen[n++] = x.pid;
            };
            t->pend.each(key, note);
            t->flushing.each(key, note);
            return n;
        }
    }
    std::vector<Holder> hs;
    if (holdersOf(L, t, key, &hs) != P4_OK) return 0;
    int n = 0;
    for (const Holder& h : hs) n += h.pid != pid;
    return n;
}

int32_t identHolder(P4Lane* L, Type* t, uint64_t src, const uint8_t h[32], int64_t* seq, uint8_t cid[32]) {
    *seq = 0;
    const std::string k = identMapKey(src, h);
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
    sqlite3_bind_int64(s, 1, int64_t(src));
    sqlite3_bind_blob(s, 2, h, 32, SQLITE_STATIC);
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
    std::string producer, peer;
    int64_t n, bytes, ncopy, minseq, maxseq, minw, maxw, maxts, nnull, mints, mine, maxe;
    bool indexed;
    std::vector<std::pair<uint32_t, LaneCount>> lanes;
};
}  // namespace

int32_t typeIndexFlush(Type* t, bool force) {
    Engine* e = t->e;
    if (!t->hasFiles.load(std::memory_order_acquire)) return P4_OK;  // no data yet
    std::lock_guard<std::mutex> fg(t->flushMu);
    std::vector<CEnt> ents;
    std::vector<IdentEnt> idents;
    std::vector<FileSnap> files;
    std::vector<std::pair<uint32_t, std::pair<std::string, std::string>>> parts;  // new partitions without a file
    std::vector<SrcDef> srcs;
    std::vector<LaneDef> lanes;
    int64_t meta[4];
    int64_t jcut;
    {
        std::lock_guard<std::mutex> g(t->mu);
        bool dirty = t->pend.live() > 0 || !t->identPend.empty();
        for (auto& p : t->parts) dirty = dirty || p->touched;
        if (!dirty) return P4_OK;
        if (!force && t->pend.live() < e->cfg.flushEntries &&
            wallMs() - t->lastFlushMs < 30000 && t->pend.bytes() < e->cfg.pendingBytes / 4)
            return P4_OK;
        // Swap: in-flight entries stay pending.
        std::swap(t->pend, t->flushing);
        t->pend.init(1024);
        for (CEnt& x : t->flushing.raw()) {
            if (x.st == 4) {
                t->pend.put(x.key, x.pid, x.seq, 4);
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
            Part* f = &*p;
            // Counters mean a file with its schema (a reopen trusts them).
            if (!f->touched || !f->created) {
                parts.push_back({p->pid, {p->producer, p->peer}});
                continue;
            }
            f->touched = false;
            FileSnap fs;
            fs.pid = p->pid;
            fs.producer = p->producer;
            fs.peer = p->peer;
            fs.n = f->n; fs.bytes = f->bytes; fs.ncopy = f->ncopy; fs.minseq = f->minseq; fs.maxseq = f->maxseq;
            fs.minw = f->minw; fs.maxw = f->maxw; fs.maxts = f->maxts; fs.nnull = f->nnull;
            fs.mints = f->mints; fs.mine = f->mine; fs.maxe = f->maxe;
            fs.indexed = f->indexed;
            for (auto& kv : f->lanes) fs.lanes.push_back(kv);
            files.push_back(std::move(fs));
        }
        for (auto& s : t->srcs) srcs.push_back(*s);
        for (auto& l : t->lanes) lanes.push_back(*l);
        meta[0] = t->uniq;
        meta[1] = t->uniqBytes;
        meta[2] = t->copies;
        meta[3] = t->nextSeq;
        t->lastFlushMs = wallMs();
    }
    std::sort(ents.begin(), ents.end(), [](const CEnt& a, const CEnt& b) {
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
            sqlite3_bind_blob(s, 1, x.key, 32, SQLITE_STATIC);
            sqlite3_bind_int64(s, 2, x.pid);
            sqlite3_bind_int64(s, 3, x.seq);
            step(s);
        } else {
            sqlite3_stmt* s = c->get(S_C_DEL);
            sqlite3_bind_blob(s, 1, x.key, 32, SQLITE_STATIC);
            sqlite3_bind_int64(s, 2, x.pid);
            step(s);
        }
    }
    for (const IdentEnt& x : idents) {
        if (rc != SQLITE_OK) break;
        if (x.st == 1) {
            sqlite3_stmt* s = c->get(S_IDENT_INS);
            sqlite3_bind_int64(s, 1, int64_t(x.src));
            sqlite3_bind_blob(s, 2, x.h, 32, SQLITE_STATIC);
            sqlite3_bind_int64(s, 3, x.seq);
            sqlite3_bind_blob(s, 4, x.cid, 32, SQLITE_STATIC);
            step(s);
        } else {
            sqlite3_stmt* s = c->get(S_IDENT_DEL);
            sqlite3_bind_int64(s, 1, int64_t(x.src));
            sqlite3_bind_blob(s, 2, x.h, 32, SQLITE_STATIC);
            step(s);
        }
    }
    for (auto& p : parts) {
        if (rc != SQLITE_OK || p.second.first.rfind("\x1f#", 0) == 0) continue;
        sqlite3_stmt* s = c->sql("INSERT OR IGNORE INTO part(pid, producer, peer) VALUES(?1,?2,?3)");
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
        sqlite3_stmt* s = c->sql(
            "INSERT OR REPLACE INTO part(pid, producer, peer, n, bytes, ncopy, minseq, maxseq, minw, maxw, maxts, nnull,"
            " ix, mints, mine, maxe) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)");
        sqlite3_bind_int64(s, 1, f.pid);
        sqlite3_bind_text(s, 2, f.producer.data(), int(f.producer.size()), SQLITE_STATIC);
        sqlite3_bind_text(s, 3, f.peer.data(), int(f.peer.size()), SQLITE_STATIC);
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
        // The file's lane rows: the live set replaces what the index had.
        sqlite3_stmt* d = c->sql("DELETE FROM lanecnt WHERE pid=?1");
        sqlite3_bind_int64(d, 1, f.pid);
        step(d);
        for (auto& kv : f.lanes) {
            const LaneCount& lc = kv.second;
            sqlite3_stmt* x = c->sql(
                "INSERT OR REPLACE INTO lanecnt(lane, pid, h, n, bytes, minw, maxw, maxseq, created, updated, maxat,"
                " url, url0, maxts, minseq) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)");
            uint64_t h = 0;
            if (kv.first >= 1 && kv.first <= lanes.size()) h = lanes[kv.first - 1].h;
            sqlite3_bind_int64(x, 1, kv.first);
            sqlite3_bind_int64(x, 2, f.pid);
            sqlite3_bind_int64(x, 3, int64_t(h));
            sqlite3_bind_int64(x, 4, lc.n);
            sqlite3_bind_int64(x, 5, lc.bytes);
            if (lc.minw != INT64_MAX) sqlite3_bind_int64(x, 6, lc.minw); else sqlite3_bind_null(x, 6);
            if (lc.maxw != INT64_MIN) sqlite3_bind_int64(x, 7, lc.maxw); else sqlite3_bind_null(x, 7);
            sqlite3_bind_int64(x, 8, lc.maxseq);
            sqlite3_bind_int64(x, 9, lc.created);
            sqlite3_bind_int64(x, 10, lc.updated);
            sqlite3_bind_int64(x, 11, lc.maxat);
            sqlite3_bind_text(x, 12, lc.url.data(), int(lc.url.size()), SQLITE_STATIC);
            sqlite3_bind_text(x, 13, lc.url0.data(), int(lc.url0.size()), SQLITE_STATIC);
            sqlite3_bind_int64(x, 14, lc.maxts);
            if (lc.minseq != INT64_MAX) sqlite3_bind_int64(x, 15, lc.minseq); else sqlite3_bind_null(x, 15);
            step(x);
        }
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
            t->pend.each(x.key, [&](const CEnt& y) { newer = newer || y.pid == x.pid; });
            if (!newer) t->pend.put(x.key, x.pid, x.seq, x.st);
        }
        for (const IdentEnt& x : idents) {
            const std::string k = identMapKey(x.src, x.h);
            if (!t->identPend.count(k)) t->identPend[k] = x;
        }
        for (const FileSnap& fs : files)
            if (Part* p = t->partById(fs.pid)) p->touched = true;
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
    }
    std::lock_guard<std::mutex> g(t->mu);
    t->flushing.clear();
    t->flushing.init(16);
    t->identFlushing.clear();
    return P4_OK;
}

}  // namespace p4
}  // namespace flatsql
