// Store format 4: the store (markers, the type catalog, registration,
// activation, stop, stats) and the feed / token registry.
#include <cstdio>

#include "flatsql/flatsql_io.h"
#include "internal.h"
#include "sql_bridge.h"

namespace flatsql {
namespace p4 {

std::string pathJoin(const std::string& a, const std::string& b) { return a + "/" + b; }

void Type::visRecompute() {
    int64_t v = nextSeq - 1;
    for (const auto& r : inflight)
        if (r.first - 1 < v) v = r.first - 1;
    vis.store(v, std::memory_order_release);
}

// ---- specs (§3.3 type spec TLV) ------------------------------------------------------------
namespace {
std::string trimLine(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && (s[a] == ' ' || s[a] == '\t' || s[a] == '\r')) a++;
    while (b > a && (s[b - 1] == ' ' || s[b - 1] == '\t' || s[b - 1] == '\r')) b--;
    return s.substr(a, b - a);
}
bool validTypeName(const std::string& n) {
    if (n.empty() || n.size() > 64) return false;
    for (char c : n)
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) return false;
    return true;
}
}  // namespace

int32_t buildSpec(const uint8_t* p, size_t n, std::shared_ptr<Spec>* out, std::string* err) {
    std::vector<Tlv> v;
    if (!tlvParse(p, n, &v)) { *err = "spec: malformed TLV"; return P4_E_ARG; }
    auto s = std::make_shared<Spec>();
    std::string schema;
    if (!tlvText(v, 1, &schema)) { *err = "spec: no schema name (tag 1)"; return P4_E_ARG; }
    s->name = schema.substr(0, schema.find('.'));
    if (!validTypeName(s->name)) { *err = "spec: bad type name " + s->name; return P4_E_ARG; }
    const Tlv* fid = tlvFind(v, 2);
    if (!fid || fid->n != 4) { *err = "spec: tag 2 (file identifier) must be 4 bytes"; return P4_E_ARG; }
    bool bad = false;
    uint32_t u32 = 0;
    uint64_t u64 = 0;
    uint8_t u8 = 0;
    if (tlvU32(v, 8, &u32, &bad)) {
        if (u32 < 512 || u32 > 65536 || (u32 & (u32 - 1))) { *err = "spec: page size"; return P4_E_ARG; }
        s->pageSize = u32;
    }
    if (tlvU8(v, 9, &u8, &bad)) s->identity = u8 != 0;
    if (tlvU32(v, 10, &u32, &bad) && u32) s->a18Bound = u32;
    if (tlvU8(v, 11, &u8, &bad)) {
        if (u8 > 2) { *err = "spec: epoch profile"; return P4_E_ARG; }
        s->epochProfile = u8;
    }
    if (tlvU8(v, 12, &u8, &bad)) s->fullText = u8 != 0;
    if (tlvU64(v, 5, &u64, &bad) && u64 == 0) bad = true;
    if (tlvU32(v, 7, &u32, &bad)) {}
    if (bad) { *err = "spec: a typed tag has the wrong length"; return P4_E_ARG; }
    // Only tags 1-7 reach the type config (format 2's TypeConfig::build shape).
    std::vector<uint8_t> tc;
    for (const Tlv& t : v)
        if (t.tag >= 1 && t.tag <= 7) tlvPut(tc, t.tag, t.v, t.n);
    const std::string e = s->tc.parse(tc.data(), tc.size());
    if (!e.empty()) { *err = "spec: " + e; return P4_E_ARG; }
    s->bytes.assign(p, p + n);
    s->rules = s->tc.rules();
    s->hasEpochRule = s->tc.hasEpochRule();
    s->hasSupersede = s->tc.hasSupersede();
    size_t at = 0;
    const std::string& r = s->rules;
    while (at <= r.size()) {
        size_t nl = r.find('\n', at);
        if (nl == std::string::npos) nl = r.size();
        std::string line = r.substr(at, nl - at);
        const size_t hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
        line = trimLine(line);
        if (line.rfind("object ", 0) == 0) s->hasObject = true;
        if (line.rfind("epoch ", 0) == 0) s->epochRule += line + "\n";
        if (line.rfind("object ", 0) == 0 || line.rfind("col ", 0) == 0) s->keyRules += line + "\n";
        at = nl + 1;
    }
    s->ek = s->hasEpochRule && s->hasObject;
    *out = std::move(s);
    return P4_OK;
}

namespace {
// .spec: the TLV, then [u16 0xFFFF][u32 4][crc32c of the TLV].
std::vector<uint8_t> specFileBytes(const std::vector<uint8_t>& tlv) {
    std::vector<uint8_t> b = tlv;
    uint8_t c[4];
    st32(c, ps::crc32c(tlv.data(), tlv.size()));
    tlvPut(b, 0xFFFF, c, 4);
    return b;
}
bool specFromFile(const std::vector<uint8_t>& b, std::vector<uint8_t>* tlv) {
    if (b.size() < 10) return false;
    const size_t at = b.size() - 10;
    if (ld16(&b[at]) != 0xFFFF || ld32(&b[at + 2]) != 4) return false;
    if (ld32(&b[at + 6]) != ps::crc32c(b.data(), at)) return false;
    tlv->assign(b.begin(), b.begin() + long(at));
    return true;
}
// T/TYPES: records [u32 crc32c(name)][u16 len][name]; a torn tail is ignored.
std::vector<std::string> catalogRead(const std::string& path) {
    std::vector<std::string> names;
    std::vector<uint8_t> b;
    if (ioReadAll(path, &b) != P4_OK) return names;
    size_t at = 0;
    while (b.size() - at >= 6) {
        const uint32_t crc = ld32(&b[at]);
        const uint16_t len = ld16(&b[at + 4]);
        if (b.size() - at - 6 < len) break;
        std::string name(reinterpret_cast<const char*>(&b[at + 6]), len);
        if (ps::crc32c(name.data(), name.size()) != crc) break;
        bool dup = false;
        for (const auto& x : names) dup = dup || x == name;
        if (!dup) names.push_back(name);
        at += 6 + len;
    }
    return names;
}
int32_t catalogAppend(const std::string& path, const std::string& name) {
    std::vector<uint8_t> rec(6 + name.size());
    st32(rec.data(), ps::crc32c(name.data(), name.size()));
    st16(rec.data() + 4, uint16_t(name.size()));
    std::memcpy(rec.data() + 6, name.data(), name.size());
    return ioAppend(path, rec.data(), rec.size());
}

std::mutex gGlobalMu;
bool gGlobalDone = false;
// SQLite's own error log, kept per thread: a read that fails names the
// SQLite error behind its status.
void sqlLog(void*, int code, const char* msg) {
    if ((code & 0xff) == SQLITE_NOTICE || (code & 0xff) == SQLITE_WARNING) return;
    std::snprintf(tSqlLog, sizeof tSqlLog, "%s (%d)", msg ? msg : "", code);
}
int32_t globalInit() {
    std::lock_guard<std::mutex> g(gGlobalMu);
    if (gGlobalDone) return P4_OK;
    // p4sql installs its per-lane allocators before SQLite initializes (§3.9).
    int32_t rc = p4sql_global_init();
    if (rc != P4_OK) return rc;
    sqlite3_config(SQLITE_CONFIG_LOG, sqlLog, nullptr);  // refused (ignored) once SQLite is initialized
    if (sqlite3_initialize() != SQLITE_OK) return P4_E_INTERNAL;
    if (flatsql::registerFlatSqlIoVfs(false) != SQLITE_OK) return P4_E_INTERNAL;
    gGlobalDone = true;
    return P4_OK;
}

void randomUuid(uint8_t out[16]) {
    // Wall clock and a counter through SHA-256: unique per store, never secret.
    static std::atomic<uint64_t> n{0};
    uint8_t seed[24];
    st64(seed, uint64_t(wallMs()));
    st64(seed + 8, monoNs());
    st64(seed + 16, n.fetch_add(1) ^ uint64_t(uintptr_t(out)));
    uint8_t d[32];
    ps::sha256(seed, sizeof seed, d);
    std::memcpy(out, d, 16);
    out[6] = uint8_t((out[6] & 0x0f) | 0x40);
    out[8] = uint8_t((out[8] & 0x3f) | 0x80);
}

int32_t readMarkers(Engine* e, Markers* m) {
    std::vector<uint8_t> s, g;
    const std::string sp = pathJoin(e->cfg.root, "STORE"), mp = pathJoin(e->cfg.root, "MIGRATED");
    const bool hs = ioExists(sp), hm = ioExists(mp);
    if (hs && ioReadAll(sp, &s) != P4_OK) return P4_E_IO;
    if (hm && ioReadAll(mp, &g) != P4_OK) return P4_E_IO;
    *m = Markers();
    decodeMarkers(hs ? s.data() : nullptr, s.size(), hm ? g.data() : nullptr, g.size(), m);
    if (hs && !s.data()) m->storePresent = true;
    if (hm && !g.data()) m->migratedPresent = true;
    return P4_OK;
}

int32_t writeMarkers(Engine* e, const uint8_t uuid[16], uint64_t floor, uint32_t from, bool migratedToo) {
    if (migratedToo) {
        uint8_t mig[40];
        encodeMigrated(mig, uuid, wallMs());
        const int32_t rc = ioWriteNew(pathJoin(e->cfg.root, "MIGRATED"), mig, sizeof mig);
        if (rc != P4_OK) return rc;
    }
    uint8_t st[64];
    encodeStore(st, uuid, wallMs(), floor, from);
    return ioWriteNew(pathJoin(e->cfg.root, "STORE"), st, sizeof st);
}

bool markersActivated(const Markers& m) {
    return m.storeValid && m.migratedValid && std::memcmp(m.uuid, m.migratedUuid, 16) == 0 && m.format == 4 &&
           m.migratedFormat == 4;
}

// The type index and journal: opened (made when absent), the journal's tail
// replayed (the touched feed files' counters mirrored into the index) before
// any read (M8), the seq floor applied.
int32_t typeFilesOpen(Type* t, std::string* err) {
    int32_t rc = typeIndexOpen(t, err);
    if (rc != P4_OK) return rc;
    rc = journalOpen(t, err);
    if (rc != P4_OK) return rc;
    {
        // Every feed the index names, against the disk: a missing file
        // without records is made again by its next write; a missing file
        // with records is quarantined (P4_E_CORRUPT, named), never silently
        // remade.
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& f : t->feeds) {
            f->created = ioExists(f->path);
            if (!f->created && f->k.recs > 0) f->quarantined = true;
        }
    }
    rc = journalReplay(t, err);
    if (rc != P4_OK) return rc;
    std::lock_guard<std::mutex> g(t->mu);
    if (t->nextSeq < int64_t(t->e->cfg.gseqFloor)) t->nextSeq = int64_t(t->e->cfg.gseqFloor);
    t->visRecompute();
    return P4_OK;
}

// A registered type. Its T/ files are opened only when they exist: a type
// that never had a write costs its .spec and nothing else.
int32_t openType(Engine* e, Type* t, const std::shared_ptr<const Spec>& sp, std::string* err) {
    t->e = e;
    t->name = sp->name;
    t->spec_ = sp;
    t->pDir = pathJoin(pathJoin(e->cfg.root, "P"), t->name);
    t->pIdx = pathJoin(pathJoin(e->cfg.root, "T"), t->name + ".idx");
    t->pJnl = pathJoin(pathJoin(e->cfg.root, "T"), t->name + ".jnl");
    t->pFts = pathJoin(pathJoin(e->cfg.root, "T"), t->name + ".fts");
    t->pSpec = pathJoin(pathJoin(e->cfg.root, "T"), t->name + ".spec");
    // Every feed file of a type is written by one writer thread (one writer
    // per file).
    t->owner = e->cfg.writers ? e->nextOwner.fetch_add(1) % e->cfg.writers : 0;
    {
        std::lock_guard<std::mutex> g(t->mu);
        t->nextSeq = int64_t(e->cfg.gseqFloor);
        t->visRecompute();
    }
    if (!ioExists(t->pIdx) && !ioExists(t->pJnl)) return P4_OK;
    const int32_t rc = typeFilesOpen(t, err);
    if (rc != P4_OK) return rc;
    t->hasFiles.store(true, std::memory_order_release);
    typeFileBytes(t);
    return P4_OK;
}

// A file name for a feed: its provider and source, URL-escaped outside
// [A-Za-z0-9._-], joined by '@' ("local" for no source). A name that would
// collide with another feed's on a case-insensitive file system, or is long,
// carries the feed id.
std::string feedFileName(Type* t, const std::string& provider, const std::string& source, uint32_t fid) {
    if (provider.empty() && source.empty()) return "local";
    auto enc = [](const std::string& s) {
        static const char* hx = "0123456789ABCDEF";
        std::string o;
        for (unsigned char c : s) {
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' ||
                c == '-') {
                o.push_back(char(c));
            } else {
                o.push_back('%');
                o.push_back(hx[c >> 4]);
                o.push_back(hx[c & 15]);
            }
        }
        return o;
    };
    std::string name = enc(provider) + "@" + enc(source);
    if (name.size() > 160) name = name.substr(0, 140) + "~" + std::to_string(fid);
    auto lower = [](std::string s) {
        for (char& c : s)
            if (c >= 'A' && c <= 'Z') c = char(c - 'A' + 'a');
        return s;
    };
    const std::string ln = lower(name);
    bool clash = name[0] == '.';
    for (auto& f : t->feeds) clash = clash || lower(f->name) == ln;
    if (clash) name += "~" + std::to_string(fid);
    return name;
}
}  // namespace

int32_t typeFilesEnsure(Type* t, std::string* err) {
    if (t->hasFiles.load(std::memory_order_acquire)) return P4_OK;
    std::lock_guard<std::mutex> g(t->openMu);
    if (t->hasFiles.load(std::memory_order_acquire)) return P4_OK;
    const int32_t rc = typeFilesOpen(t, err);
    if (rc != P4_OK) {
        delete t->idx;
        t->idx = nullptr;
        delete t->jdb;
        t->jdb = nullptr;
        return rc;
    }
    t->hasFiles.store(true, std::memory_order_release);
    return P4_OK;
}

// ---- registry --------------------------------------------------------------------------
Feed* feedRestore(Type* t, uint32_t fid, const std::string& provider, const std::string& source, const std::string& name) {
    while (t->feeds.size() < fid) {
        // Ids are assigned in order; a gap (never expected) keeps numbering stable.
        auto ph = std::make_unique<Feed>();
        ph->type = t;
        ph->fid = uint32_t(t->feeds.size() + 1);
        ph->provider = "\x1f#" + std::to_string(ph->fid);
        ph->name = "\x1f#" + std::to_string(ph->fid);
        t->feeds.push_back(std::move(ph));
    }
    Feed* f = t->feeds[fid - 1].get();
    if (f->provider.rfind("\x1f#", 0) == 0 || f->name.empty()) {
        f->provider = provider;
        f->source = source;
        f->name = name;
        f->local = provider.empty() && source.empty();
        f->path = pathJoin(t->pDir, name + ".db");
        t->feedByKey[provider + '\x1f' + source] = fid;
    }
    f->registered = true;
    return f;
}

Feed* feedFor(Type* t, const std::string& provider, const std::string& source, bool create, bool* made) {
    if (made) *made = false;
    auto it = t->feedByKey.find(provider + '\x1f' + source);
    if (it != t->feedByKey.end()) return t->feeds[it->second - 1].get();
    if (!create) return nullptr;
    auto f = std::make_unique<Feed>();
    f->type = t;
    f->fid = uint32_t(t->feeds.size() + 1);
    f->provider = provider;
    f->source = source;
    f->local = provider.empty() && source.empty();
    f->name = feedFileName(t, provider, source, f->fid);
    f->path = pathJoin(t->pDir, f->name + ".db");
    Feed* raw = f.get();
    t->feeds.push_back(std::move(f));
    t->feedByKey.emplace(provider + '\x1f' + source, raw->fid);
    if (made) *made = true;
    return raw;
}

uint32_t tokFor(Type* t, const std::string& token, const std::string& peer, bool create, bool* made) {
    if (made) *made = false;
    auto it = t->tokByToken.find(token);
    if (it != t->tokByToken.end()) return it->second;
    if (!create) return 0;
    TokDef d;
    d.token = token;
    d.peer = peer;
    t->toks.push_back(std::move(d));
    const uint32_t id = uint32_t(t->toks.size());
    t->tokByToken.emplace(token, id);
    if (made) *made = true;
    return id;
}

// ---- init ----------------------------------------------------------------------------------
int32_t engineInit(Engine* e, const uint8_t* cfgBytes, size_t n, std::string* err) {
    int32_t rc = parseConfig(cfgBytes, n, &e->cfg, err);
    if (rc != P4_OK) return rc;
    rc = globalInit();
    if (rc != P4_OK) { *err = "SQLite or the SQL surface failed to initialize"; return rc; }
    for (auto& s : e->stat) s.store(0);
    sqlite3_soft_heap_limit64(int64_t(e->cfg.softHeap));
    sqlite3_hard_heap_limit64(int64_t(e->cfg.hardHeap));
    flatsql::setFlatSqlIoReadahead(int(e->cfg.raStreams), int(e->cfg.raBytes));
    // The reader pool's share of the one budget: its connections' caches,
    // and idle readers close while the heap is past three quarters of the
    // soft limit (the writers' share is never taken by idle readers).
    e->rpool.configure(e->cfg.readerConns, e->cfg.readerCacheKiB, e->cfg.softHeap / 4 * 3);

    Markers& m = e->markers;
    rc = readMarkers(e, &m);
    if (rc != P4_OK) { *err = "markers: read failed"; return rc; }
    if (m.storeValid && (m.format > 4 || m.layout > 1)) { *err = "STORE is a newer format or layout"; return P4_E_FORMAT; }
    if (m.migratedValid && m.migratedFormat > 4) { *err = "MIGRATED is a newer format"; return P4_E_FORMAT; }
    const std::string catalog = pathJoin(pathJoin(e->cfg.root, "T"), "TYPES");
    const std::vector<std::string> names = catalogRead(catalog);
    uint64_t floor = e->cfg.gseqFloor;
    switch (e->cfg.createMode) {
        case 0:
            if (!markersActivated(m)) { *err = "not an activated format-4 store (STORE and MIGRATED)"; return P4_E_FORMAT; }
            floor = m.gseqFloor;
            break;
        case 1:
            if (markersActivated(m)) {
                floor = m.gseqFloor;
            } else if (!m.storePresent && !m.migratedPresent && names.empty()) {
                uint8_t uuid[16];
                randomUuid(uuid);
                rc = writeMarkers(e, uuid, floor, 0, true);
                if (rc != P4_OK) { *err = "markers: write failed"; return rc; }
                readMarkers(e, &m);
            } else if (m.storePresent && !m.storeValid && m.migratedValid && names.empty()) {
                // A torn STORE next to a valid MIGRATED: activation in progress.
                rc = writeMarkers(e, m.migratedUuid, floor, 0, false);
                if (rc != P4_OK) { *err = "markers: write failed"; return rc; }
                readMarkers(e, &m);
            } else {
                *err = "markers: neither an activated store nor an empty directory";
                return P4_E_FORMAT;
            }
            break;
        case 2:
            // A valid STORE is an activated store. A torn STORE next to a
            // valid MIGRATED is an activation in progress (C-22): reopened
            // here, rewritten by flatsql_p4_activate.
            if (m.storeValid) { *err = "a migration target has no valid STORE yet"; return P4_E_FORMAT; }
            if (m.storePresent && !m.migratedValid) { *err = "a torn STORE without a valid MIGRATED"; return P4_E_FORMAT; }
            e->ftsHold.store(true);
            break;
    }
    e->cfg.gseqFloor = floor;
    rc = mailboxInit(e, err);
    if (rc != P4_OK) return rc;
    // Types: the catalog, then each type's spec; the type index and journal
    // tail of the types that have them.
    for (const std::string& name : names) {
        std::vector<uint8_t> fb, tlv;
        const std::string sp = pathJoin(pathJoin(e->cfg.root, "T"), name + ".spec");
        if (ioReadAll(sp, &fb) != P4_OK || !specFromFile(fb, &tlv)) continue;  // re-registered by the host
        std::shared_ptr<Spec> spec;
        std::string e2;
        if (buildSpec(tlv.data(), tlv.size(), &spec, &e2) != P4_OK || spec->name != name) continue;
        auto t = std::make_unique<Type>();
        rc = openType(e, t.get(), spec, &e2);
        if (rc != P4_OK) { *err = name + ": " + e2; return rc; }
        e->typeByName[name] = t.get();
        e->types.push_back(std::move(t));
    }
    e->markers = m;
    return P4_OK;
}

int32_t engineRegisterType(Engine* e, const uint8_t* p, size_t n, std::string* err) {
    std::shared_ptr<Spec> spec;
    int32_t rc = buildSpec(p, n, &spec, err);
    if (rc != P4_OK) return rc;
    std::lock_guard<std::mutex> g(e->typesMu);
    auto it = e->typeByName.find(spec->name);
    if (it != e->typeByName.end()) {
        Type* t = it->second;
        std::shared_ptr<const Spec> cur = t->spec();
        if (cur->bytes == spec->bytes) return P4_OK;
        bool hasData;
        {
            std::lock_guard<std::mutex> tg(t->mu);
            hasData = !t->feeds.empty();
        }
        if (hasData && cur->epochRule != spec->epochRule) {
            *err = "the epoch rule of a type with data cannot change (C-5)";
            return P4_E_FORMAT;
        }
        // Rows keep the object key (r.k, r_ke) and sealed records their
        // COL values from the rules they were written under.
        if (hasData && cur->keyRules != spec->keyRules) {
            *err = "the object and col rules of a type with data cannot change (C-5)";
            return P4_E_FORMAT;
        }
        const std::vector<uint8_t> fb = specFileBytes(spec->bytes);
        rc = ioWriteNew(t->pSpec, fb.data(), fb.size());
        if (rc != P4_OK) { *err = "spec write failed"; return rc; }
        std::lock_guard<std::mutex> tg(t->mu);
        t->spec_ = spec;
        return P4_OK;
    }
    const std::string tdir = pathJoin(e->cfg.root, "T");
    const std::vector<uint8_t> fb = specFileBytes(spec->bytes);
    rc = ioWriteNew(pathJoin(tdir, spec->name + ".spec"), fb.data(), fb.size());
    if (rc != P4_OK) { *err = "spec write failed"; return rc; }
    rc = catalogAppend(pathJoin(tdir, "TYPES"), spec->name);
    if (rc != P4_OK) { *err = "type catalog write failed"; return rc; }
    auto t = std::make_unique<Type>();
    rc = openType(e, t.get(), spec, err);
    if (rc != P4_OK) return rc;
    e->typeByName[spec->name] = t.get();
    e->types.push_back(std::move(t));
    return P4_OK;
}

// ---- activation (§2.3 step 1) -------------------------------------------------------------
int32_t engineActivate(Engine* e) {
    if (e->cfg.createMode != 2) return P4_E_FORMAT;
    Markers m;
    if (readMarkers(e, &m) != P4_OK) return P4_E_IO;
    if (markersActivated(m)) return P4_OK;  // idempotent
    std::vector<Type*> types;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        for (auto& t : e->types)
            if (t->hasFiles.load()) types.push_back(t.get());
    }
    for (Type* t : types) {
        // The type index is written with every write (nothing pending); the
        // migration's journal, applied, goes at rest with its free pages (no
        // writer runs on a store being activated).
        if (t->jdb) {
            t->jdb->exec("DELETE FROM j");
            t->jdb->exec("VACUUM");
        }
        std::vector<std::string> paths;
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& f : t->feeds)
                if (f->created) paths.push_back(f->path);
        }
        paths.push_back(t->pIdx);
        paths.push_back(t->pJnl);
        for (const std::string& path : paths) {
            Conn* c = nullptr;
            if (openConn(path, OpenKind::Maint, 1024, 0, &c, nullptr) != SQLITE_OK) return P4_E_IO;
            int log = 0, ck = 0;
            const int r = sqlite3_wal_checkpoint_v2(c->db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, &log, &ck);
            delete c;
            if (r != SQLITE_OK) return statusOfSqlite(r);
            walNote(e, path, 0);
            walExtNote(e, path, 0);
            e->bump(kStTruncate);
        }
    }
    uint8_t uuid[16];
    if (m.migratedValid && m.storePresent && !m.storeValid)
        std::memcpy(uuid, m.migratedUuid, 16);
    else
        randomUuid(uuid);
    const bool rewriteMigrated = !(m.migratedValid && m.storePresent && !m.storeValid);
    const int32_t rc = writeMarkers(e, uuid, e->cfg.gseqFloor, 1, rewriteMigrated);
    if (rc == P4_OK) e->ftsHold.store(false);  // full text builds from seq 0 (§11 step 5)
    return rc;
}

// ---- stats (§3.10) -------------------------------------------------------------------------
int32_t engineStats(Engine* e, uint8_t* out, int32_t cap) {
    uint64_t v[kStCount];
    for (int i = 0; i < kStCount; i++) v[i] = e->stat[i].load(std::memory_order_relaxed);
    uint64_t files = 0, feeds = 0, quarantined = 0, types = 0;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        types = e->types.size();
        for (auto& t : e->types) {
            std::lock_guard<std::mutex> tg(t->mu);
            feeds += t->feeds.size();
            for (auto& f : t->feeds) {
                if (f->created) files++;
                if (f->quarantined) quarantined++;
            }
        }
    }
    {
        std::lock_guard<std::mutex> g(e->wconnMu);
        v[kStWriterConns] = e->nWConn;
    }
    v[kStReaderConns] = e->rpool.open();
    // C-30: SQLite runs without memory statistics; the SQL surface's
    // allocator counts (0 without the surface).
    v[kStHeap] = p4sql_heap_used();
    v[kStHeapPeak] = p4sql_heap_peak();
    v[kStPendingBytes] = 0;  // the type index is written with every write
    v[kStLiveFiles] = files;
    v[kStTypes] = types;
    v[kStPartitions] = feeds;
    v[kStQuarantined] = quarantined;
    const int32_t need = int32_t(sizeof v);
    const int32_t n = cap < need ? (cap / 8) * 8 : need;
    for (int32_t i = 0; i < n / 8; i++) st64(out + 8 * i, v[i]);
    return n;
}

}  // namespace p4
}  // namespace flatsql

flatsql::p4::Type* P4Engine::findType(const std::string& name) {
    std::lock_guard<std::mutex> g(typesMu);
    auto it = typeByName.find(name);
    return it == typeByName.end() ? nullptr : it->second;
}
