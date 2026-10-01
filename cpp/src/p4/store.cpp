// Store format 4: the store (markers, the type catalog, registration,
// activation, stop, stats) and the partition / lane / source registry.
#include <cstdio>

#include "flatsql/flatsql_io.h"
#include "internal.h"
#include "sql_bridge.h"

namespace flatsql {
namespace p4 {

std::string pathJoin(const std::string& a, const std::string& b) { return a + "/" + b; }

std::string Type::filePath(uint32_t pid, int64_t tb, int32_t gen) const {
    char buf[64];
    std::snprintf(buf, sizeof buf, "/%u/%lld.%d.db", pid, (long long)tb, gen);
    return pDir + buf;
}

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
    s->hasBucket = s->tc.hasBucketTime();
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
        if (line.rfind("epoch ", 0) == 0 || line.rfind("bucket ", 0) == 0) s->timeRules += line + "\n";
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
int32_t globalInit() {
    std::lock_guard<std::mutex> g(gGlobalMu);
    if (gGlobalDone) return P4_OK;
    // p4sql installs its per-lane allocators before SQLite initializes (§3.9).
    int32_t rc = p4sql_global_init();
    if (rc != P4_OK) return rc;
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

// Retired generations left on disk by a crash between the drop (J_DROP) and
// the unlink. Every path a type ever used is <month>.<gen>.db with gen at
// most the gens table's maximum, so no directory listing is needed (the wasm
// host has none). A path is removed only when no live file holds it.
void sweepRetired(Type* t) {
    for (auto& p : t->parts) {
        // A live, empty file whose path is gone (its index row outlived it): it
        // has no rows to lose, so it is uncreated again and the writer remakes it.
        for (auto& kv : p->files) {
            File* f = kv.second;
            if (f->created && !f->retired && f->n == 0 && !ioExists(f->path)) f->created = false;
        }
        std::map<int64_t, int32_t> top = p->maxGen;
        for (auto& f : p->all) {
            int32_t& g = top[f->tb];
            g = std::max(g, f->gen);
        }
        for (auto& kv : top) {
            // The swept generations stay used (B9): a path is never reused, so
            // no reader's or VFS node's state can meet a new file on it.
            if (p->maxGen[kv.first] < kv.second) {
                p->maxGen[kv.first] = kv.second;
                sqlite3_stmt* s = t->idx->sql(
                    "INSERT INTO gens(pid, tb, gen) VALUES(?1,?2,?3) ON CONFLICT(pid, tb) DO UPDATE SET gen=max(gen, excluded.gen)");
                if (s) {
                    sqlite3_bind_int64(s, 1, p->pid);
                    sqlite3_bind_int64(s, 2, kv.first);
                    sqlite3_bind_int64(s, 3, kv.second);
                    sqlite3_step(s);
                    sqlite3_reset(s);
                }
            }
            for (int32_t g = 0; g <= kv.second; g++) {
                auto live = p->files.find(kv.first);
                if (live != p->files.end() && live->second->gen == g && !live->second->retired) continue;
                const std::string path = t->filePath(p->pid, kv.first, g);
                for (const char* sfx : {"-wal", "-journal", ""})
                    if (ioExists(path + sfx)) {
                        ioUnlink(path + sfx);
                        t->e->bump(kStUnlinked);
                    }
            }
        }
    }
}

int32_t openType(Engine* e, Type* t, const std::shared_ptr<const Spec>& sp, std::string* err) {
    t->e = e;
    t->name = sp->name;
    t->spec_ = sp;
    t->pDir = pathJoin(pathJoin(e->cfg.root, "P"), t->name);
    t->pIdx = pathJoin(pathJoin(e->cfg.root, "T"), t->name + ".idx");
    t->pJnl = pathJoin(pathJoin(e->cfg.root, "T"), t->name + ".jnl");
    t->pFts = pathJoin(pathJoin(e->cfg.root, "T"), t->name + ".fts");
    t->pSpec = pathJoin(pathJoin(e->cfg.root, "T"), t->name + ".spec");
    t->pend.init(1024);
    t->flushing.init(16);
    int32_t rc = typeIndexOpen(t, err);
    if (rc != P4_OK) return rc;
    rc = journalOpen(t, err);
    if (rc != P4_OK) return rc;
    rc = journalReplay(t, err);
    if (rc != P4_OK) return rc;
    sweepRetired(t);
    return P4_OK;
}
}  // namespace


// ---- registry --------------------------------------------------------------------------
Part* partFor(Type* t, const std::string& producer, const std::string& peer, bool create) {
    auto it = t->partByProducer.find(producer);
    if (it != t->partByProducer.end()) return t->parts[it->second - 1].get();
    if (!create) return nullptr;
    auto p = std::make_unique<Part>();
    p->type = t;
    p->pid = uint32_t(t->parts.size() + 1);
    p->producer = producer;
    p->peer = peer;
    Engine* e = t->e;
    p->owner = e->writers.empty() ? 0 : e->nextOwner.fetch_add(1) % uint32_t(e->writers.size());
    Part* raw = p.get();
    t->parts.push_back(std::move(p));
    t->partByProducer.emplace(producer, raw->pid);
    return raw;
}

uint64_t laneHash(const std::string* f6) {
    std::string b;
    for (int i = 0; i < 6; i++) {
        b += f6[i];
        b.push_back('\0');
    }
    uint8_t d[32];
    ps::sha256(b.data(), b.size(), d);
    return ld64(d) & 0x7fffffffffffffffull;
}

namespace {
std::string identityKey(const std::string* f6) {
    std::string b;
    for (int i = 0; i < 6; i++) {
        b += f6[i];
        b.push_back('\x1f');
    }
    return b;
}
}  // namespace

SrcDef* srcFor(Type* t, const std::string& provider, const std::string& source, bool create) {
    const std::string key = provider + '\x1f' + source;
    auto it = t->srcByName.find(key);
    if (it != t->srcByName.end()) return t->srcs[it->second - 1].get();
    if (!create) return nullptr;
    auto s = std::make_unique<SrcDef>();
    s->id = uint32_t(t->srcs.size() + 1);
    s->provider = provider;
    s->source = source;
    SrcDef* raw = s.get();
    t->srcs.push_back(std::move(s));
    t->srcJournaled.push_back(0);
    t->srcByName.emplace(key, raw->id);
    return raw;
}

LaneDef* laneFor(Type* t, const std::string* f6, bool create) {
    const std::string key = identityKey(f6);
    auto it = t->laneByIdentity.find(key);
    if (it != t->laneByIdentity.end()) return t->lanes[it->second - 1].get();
    if (!create) return nullptr;
    SrcDef* s = srcFor(t, f6[0], f6[1], true);
    auto l = std::make_unique<LaneDef>();
    l->id = uint32_t(t->lanes.size() + 1);
    l->sid = s->id;
    l->h = laneHash(f6);
    l->provider = f6[0];
    l->source = f6[1];
    l->batch = f6[2];
    l->ckey = f6[3];
    l->ppeer = f6[4];
    l->pkey = f6[5];
    LaneDef* raw = l.get();
    t->lanes.push_back(std::move(l));
    t->laneJournaled.push_back(0);
    t->laneByIdentity.emplace(key, raw->id);
    return raw;
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
    e->rpool.configure(e->cfg.readerConns, e->cfg.readerCacheKiB);

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
    // Types: the catalog, then each type's spec, type index and journal tail.
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
        {
            std::lock_guard<std::mutex> g(t->mu);
            if (t->nextSeq < int64_t(floor)) t->nextSeq = int64_t(floor);
            t->visRecompute();
        }
        e->typeByName[name] = t.get();
        e->types.push_back(std::move(t));
    }
    e->markers = m;
    e->cfg.gseqFloor = floor;
    rc = mailboxInit(e, err);
    if (rc != P4_OK) return rc;
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
            hasData = !t->parts.empty();
        }
        if (hasData && cur->timeRules != spec->timeRules) {
            *err = "the epoch/bucket rule of a type with data cannot change (C-5)";
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
    {
        std::lock_guard<std::mutex> tg(t->mu);
        if (t->nextSeq < int64_t(e->cfg.gseqFloor)) t->nextSeq = int64_t(e->cfg.gseqFloor);
        t->visRecompute();
    }
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
        for (auto& t : e->types) types.push_back(t.get());
    }
    for (Type* t : types) {
        int32_t rc = typeIndexFlush(t, true);
        if (rc != P4_OK) return rc;
        {
            std::lock_guard<std::mutex> g(t->jmu);
            journalReclaim(t, 0);
        }
        std::vector<std::string> paths;
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& p : t->parts)
                for (auto& kv : p->files)
                    if (kv.second->created) paths.push_back(kv.second->path);
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
    uint64_t files = 0, parts = 0, quarantined = 0, pending = 0, types = 0;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        types = e->types.size();
        for (auto& t : e->types) {
            std::lock_guard<std::mutex> tg(t->mu);
            parts += t->parts.size();
            pending += t->pend.bytes() + t->flushing.bytes();
            for (auto& p : t->parts)
                for (auto& f : p->all) {
                    if (f->created && !f->retired) files++;
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
    v[kStPendingBytes] = pending;
    v[kStLiveFiles] = files;
    v[kStTypes] = types;
    v[kStPartitions] = parts;
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
