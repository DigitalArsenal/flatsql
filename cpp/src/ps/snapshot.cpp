// FlatSQL partition store: the reader's view of committed state (see
// ps/snapshot.h; design §8, A12, A14, A15, A16, A17, A20).
#include "flatsql/ps/snapshot.h"

#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/reflection.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>

#include "flatsql/ps/platform.h"

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------
namespace {
void fidHexOf(uint32_t id, char hex[9]) {
    uint8_t fid[4];
    putU32(fid, id);
    static const char* d = "0123456789abcdef";
    for (int i = 0; i < 4; i++) {
        hex[2 * i] = d[fid[i] >> 4];
        hex[2 * i + 1] = d[fid[i] & 15];
    }
    hex[8] = 0;
}
void finishPath(PathBuf* out, int n) {
    out->len = (n < 0) ? 0 : (size_t(n) >= sizeof(out->buf) ? sizeof(out->buf) - 1 : size_t(n));
}
}  // namespace

void filePath(const std::string& root, const FileKey& k, PathBuf* out) {
    const char* r = root.c_str();
    char hex[9];
    int n = 0;
    switch (k.space) {
        case FileKey::kStore:
            switch (k.letter) {
                case 'R': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/registry.fsl", r); break;
                case 'H': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/registry.fsh", r); break;
                case 'S': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/STORE", r); break;
                case 'M': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/MIGRATED", r); break;
                default: n = -1;
            }
            break;
        case FileKey::kPart:
            switch (k.letter) {
                case 'h': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/h.fsh", r, k.id); break;
                case 'l': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/l.fsl", r, k.id); break;
                case 'm': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/m-%06x.fsl", r, k.id, k.seg); break;
                case 'x':
                    n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/x-%06x-%04x.fsx", r, k.id, k.seg, k.gen);
                    break;
                case 'f': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/mf-%06x.fsm", r, k.id, k.gen); break;
                case 'd':
                case 'r':
                case 'a': {
                    const char* ext = k.letter == 'd' ? "fsd" : k.letter == 'r' ? "fsr" : "fsa";
                    if (k.gen)  // T3 SWAP seam: compaction outputs
                        n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/c-%06x-%04x.%s", r, k.id, k.seg,
                                     k.gen, ext);
                    else
                        n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/%c-%06x.%s", r, k.id, k.letter,
                                     k.seg, ext);
                    break;
                }
                default: n = -1;
            }
            break;
        case FileKey::kType:
            fidHexOf(k.id, hex);
            switch (k.letter) {
                case 'h': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/h.fsh", r, hex); break;
                case 'm': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/m-%06x.fsl", r, hex, k.seg); break;
                case 'g': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/g-%06x.fsg", r, hex, k.seg); break;
                case 'F': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/%s", r, hex, kArrivalFenceName); break;
                case 'x': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/x-%06x.fsx", r, hex, k.gen); break;
                case 'f': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/mf-%06x.fsm", r, hex, k.gen); break;
                case 's':
                    n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/s-%016llx.fsc", r, hex,
                                 (unsigned long long)k.extra);
                    break;
                default: n = -1;
            }
            break;
        default: n = -1;
    }
    finishPath(out, n);
}

FileClass fileClassOf(const FileKey& k) {
    if (k.space == FileKey::kStore) return k.letter == 'R' || k.letter == 'H' ? FileClass::Registry : FileClass::Store;
    if (k.space == FileKey::kPart) {
        switch (k.letter) {
            case 'h': return FileClass::Head;
            case 'm': return FileClass::Meta;
            case 'd': return FileClass::Data;
            case 'r': return FileClass::Rows;
            case 'a': return FileClass::Attrs;
            case 'x': return FileClass::Index;
            case 'f': return FileClass::Manifest;
            case 'l': return FileClass::Lanes;
        }
        return FileClass::Store;
    }
    switch (k.letter) {
        case 'h': return FileClass::TypeHead;
        case 'm': return FileClass::TypeMeta;
        case 'g':
        case 'F': return FileClass::Arrivals;
        case 'x': return FileClass::Index;
        case 'f': return FileClass::Manifest;
        case 's': return FileClass::Config;
    }
    return FileClass::Store;
}

namespace {
FileKey pk(char letter, uint32_t pid, uint32_t seg = 0, uint32_t gen = 0) {
    FileKey k;
    k.space = FileKey::kPart;
    k.letter = letter;
    k.id = pid;
    k.seg = seg;
    k.gen = gen;
    return k;
}
FileKey tk(char letter, const uint8_t fid[4], uint32_t seg = 0, uint32_t gen = 0, uint64_t extra = 0) {
    FileKey k;
    k.space = FileKey::kType;
    k.letter = letter;
    k.id = fidU32(fid);
    k.seg = seg;
    k.gen = gen;
    k.extra = extra;
    return k;
}
FileKey sk(char letter) {
    FileKey k;
    k.space = FileKey::kStore;
    k.letter = letter;
    return k;
}
}  // namespace

// ---------------------------------------------------------------------------
// LaneIo
// ---------------------------------------------------------------------------
LaneIo::LaneIo(Io* io, const std::string& root, uint32_t maxHandles)
    : root_(root), ctx_(io ? io : importIo(), &ioStats_), max_(maxHandles ? maxHandles : 64) {}

LaneIo::~LaneIo() { closeAll(); }

void LaneIo::closeAll() {
    for (auto& kv : map_) ctx_.close(&kv.second.ref);
    map_.clear();
    lru_.clear();
}

void LaneIo::forget(const FileKey& key) {
    auto it = map_.find(key);
    if (it == map_.end()) return;
    ctx_.close(&it->second.ref);
    lru_.erase(it->second.lru);
    map_.erase(it);
}

int32_t LaneIo::get(const FileKey& key, FileRef* out) {
    auto it = map_.find(key);
    if (it != map_.end()) {
        it->second.lastSeq = stmtSeq_;
        lru_.splice(lru_.begin(), lru_, it->second.lru);
        *out = it->second.ref;
        return 0;
    }
    PathBuf path;
    filePath(root_, key, &path);
    if (!path.len) return FLATSQL_IO_ERR_GENERIC;
    FileRef f;
    const int32_t rc = ctx_.open(path.c_str(), path.len, FLATSQL_IO_READ, fileClassOf(key), &f);
    if (rc < 0) return rc;
    while (map_.size() >= max_ && !lru_.empty()) {
        auto vit = map_.find(lru_.back());
        if (vit->second.lastSeq == stmtSeq_) break;  // pinned by the running statement
        ctx_.close(&vit->second.ref);
        map_.erase(vit);
        lru_.pop_back();
    }
    lru_.push_front(key);
    Entry e;
    e.ref = f;
    e.lastSeq = stmtSeq_;
    e.lru = lru_.begin();
    map_.emplace(key, e);
    *out = f;
    return 0;
}

int64_t LaneIo::read(const FileRef& f, void* dst, size_t len, uint64_t off) {
    const int64_t n = ctx_.read(f, dst, len, off);
    if (stats_) {
        stats_->preads++;
        if (n > 0) stats_->bytesRead += uint64_t(n);
    }
    return n;
}

int64_t LaneIo::size(const FileRef& f) { return ctx_.size(f); }

// ---------------------------------------------------------------------------
// Columns (reflection over the type's BFBS)
// ---------------------------------------------------------------------------
const char* ColumnDef::sqlType() const {
    switch (baseType) {
        case reflection::Bool:
        case reflection::Byte:
        case reflection::UByte:
        case reflection::Short:
        case reflection::UShort:
        case reflection::Int:
        case reflection::UInt:
        case reflection::Long:
        case reflection::ULong: return "INTEGER";
        case reflection::Float:
        case reflection::Double: return "REAL";
        case reflection::String: return "TEXT";
        case reflection::Vector:
            return (element == reflection::UByte || element == reflection::Byte) ? "BLOB" : "";
        default: return "";
    }
}

std::vector<ColumnDef> columnsOf(const TypeConfig& cfg) {
    std::vector<ColumnDef> out;
    const reflection::Schema* s = cfg.schema();
    if (!s || !s->root_table() || !s->root_table()->fields()) return out;
    std::vector<const reflection::Field*> fields;
    for (const reflection::Field* f : *s->root_table()->fields()) fields.push_back(f);
    std::sort(fields.begin(), fields.end(),
              [](const reflection::Field* a, const reflection::Field* b) { return a->id() < b->id(); });
    for (const reflection::Field* f : fields) {
        if (f->type()->base_type() == reflection::UType) continue;  // union discriminants
        ColumnDef c;
        c.name = f->name()->str();
        c.voffset = f->offset();
        c.baseType = uint8_t(f->type()->base_type());
        c.element = uint8_t(f->type()->element());
        c.defInt = f->default_integer();
        c.defReal = f->default_real();
        out.push_back(std::move(c));
    }
    return out;
}

// ---------------------------------------------------------------------------
// Registry view helpers
// ---------------------------------------------------------------------------
namespace {
bool ieq(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); i++)
        if (std::tolower(uint8_t(a[i])) != std::tolower(uint8_t(b[i]))) return false;
    return true;
}
}  // namespace

const TypeInfo* RegistryView::typeByFid(const uint8_t fid[4]) const {
    for (const auto& t : types)
        if (std::memcmp(t->fid, fid, 4) == 0) return t.get();
    return nullptr;
}

const TypeInfo* RegistryView::typeByName(const std::string& name) const {
    for (const auto& t : types)
        if (ieq(t->typeName, name)) return t.get();
    return nullptr;
}

const PartInfo* RegistryView::partBySqlName(const std::string& name) const {
    for (const auto& p : parts)
        if (p.pid && ieq(p.sqlName, name)) return &p;
    return nullptr;
}

const ManifestSegRef* Manifest::segFor(uint32_t seg) const {
    for (const auto& s : segs)
        if (s.seg == seg) return &s;
    return nullptr;
}

const ManifestSegRef* Manifest::segForPseq(uint64_t pseq) const {
    for (const auto& s : segs)
        if (pseq >= s.firstPseq && pseq < s.mergedEnd) return &s;
    return nullptr;
}

// ---------------------------------------------------------------------------
// Parsed L0 blocks and posting sources
// ---------------------------------------------------------------------------
struct L0Parsed {
    std::vector<uint8_t> bytes;
    std::vector<L0KindInfo> kinds;
    const L0KindInfo* find(uint16_t kind) const {
        for (const auto& k : kinds)
            if (k.kind == kind) return &k;
        return nullptr;
    }
};

struct PostingScan::Source {
    const uint8_t* key = nullptr;
    uint16_t klen = 0;
    const uint8_t* val = nullptr;
    uint8_t vlen = 0;
    bool valid = false;
    virtual ~Source() = default;
    virtual bool advance(LaneStore* st) = 0;
    int32_t err = 0;
};

namespace {

inline bool inRange(const uint8_t* k, size_t kl, const std::string& lo, bool hasLo, const std::string& hi,
                    bool hasHi) {
    if (hasLo && keyCmp(k, kl, reinterpret_cast<const uint8_t*>(lo.data()), lo.size()) < 0) return false;
    if (hasHi && keyCmp(k, kl, reinterpret_cast<const uint8_t*>(hi.data()), hi.size()) >= 0) return false;
    return true;
}

// One kind section of a parsed L0 block, restricted to the range.
struct L0Source : PostingScan::Source {
    std::shared_ptr<const L0Parsed> blk;
    std::vector<const uint8_t*> ents;
    int64_t idx = -1;
    bool desc = false;
    uint8_t v = 0;
    bool init(std::shared_ptr<const L0Parsed> b, uint16_t kind, const std::string& lo, bool hasLo,
              const std::string& hi, bool hasHi, bool d) {
        blk = std::move(b);
        desc = d;
        const L0KindInfo* ki = blk->find(kind);
        if (!ki || ki->n == 0) return false;
        v = ki->vlen;
        EntryIter it;
        it.p = blk->bytes.data() + ki->entriesOff;
        it.end = it.p + ki->entriesBytes;
        it.vlen = ki->vlen;
        const uint8_t *k, *val;
        uint16_t kl;
        const uint8_t* at = it.p;
        while (it.next(&k, &kl, &val)) {
            if (inRange(k, kl, lo, hasLo, hi, hasHi)) ents.push_back(at);
            else if (hasHi && keyCmp(k, kl, reinterpret_cast<const uint8_t*>(hi.data()), hi.size()) >= 0)
                break;
            at = it.p;
        }
        if (ents.empty()) return false;
        idx = desc ? int64_t(ents.size()) : -1;
        return true;
    }
    bool advance(LaneStore*) override {
        idx += desc ? -1 : 1;
        if (idx < 0 || idx >= int64_t(ents.size())) return valid = false;
        const uint8_t* p = ents[size_t(idx)];
        klen = getU16(p);
        key = p + 2;
        val = p + 2 + klen;
        vlen = v;
        return valid = true;
    }
};

// One kind of an L1 run: blocks read on demand (4 KiB each).
struct L1Source : PostingScan::Source {
    std::shared_ptr<const L1Run> run;
    FileKey file;
    const std::vector<L1Fence>* fences = nullptr;
    int64_t block = -1;
    int64_t idx = -1;
    bool desc = false;
    uint8_t v = 0;
    bool done = false;
    std::string lo, hi;
    bool hasLo = false, hasHi = false;
    uint8_t buf[kL1BlockBytes];
    std::vector<uint16_t> ents;  // entry offsets within buf (in range)

    bool loadBlock(LaneStore* st, int64_t b) {
        ents.clear();
        if (b < 0 || b >= int64_t(fences->size())) return false;
        block = b;
        const int32_t rc = st->readBlock(file, (*fences)[size_t(b)].blockOff, buf);
        if (rc < 0) {
            err = rc;
            return false;
        }
        EntryIter it = l1BlockIter(buf, v);
        const uint8_t *k, *val;
        uint16_t kl;
        const uint8_t* at = it.p;
        bool pastHi = false, beforeLo = false;
        while (it.next(&k, &kl, &val)) {
            if (hasLo && keyCmp(k, kl, reinterpret_cast<const uint8_t*>(lo.data()), lo.size()) < 0) {
                beforeLo = true;
            } else if (hasHi && keyCmp(k, kl, reinterpret_cast<const uint8_t*>(hi.data()), hi.size()) >= 0) {
                pastHi = true;
                break;
            } else {
                ents.push_back(uint16_t(at - buf));
            }
            at = it.p;
        }
        // Direction-aware end: past the range in the scan direction.
        done = desc ? beforeLo : pastHi;
        return true;
    }
    bool init(LaneStore* st, std::shared_ptr<const L1Run> r, const FileKey& f, uint16_t kind, const std::string& l,
              bool hl, const std::string& h, bool hh, bool d) {
        run = std::move(r);
        file = f;
        fences = run->fences(kind);
        if (!fences || fences->empty()) return false;
        v = run->kindVlen(kind);
        lo = l;
        hi = h;
        hasLo = hl;
        hasHi = hh;
        desc = d;
        int64_t start;
        if (!desc) {
            // Last block whose prefix is strictly below lo's prefix.
            start = 0;
            if (hasLo) {
                size_t a = 0, b2 = fences->size();
                while (a < b2) {
                    const size_t mid = (a + b2) / 2;
                    if (prefixCmp(reinterpret_cast<const uint8_t*>(lo.data()), lo.size(), (*fences)[mid]) > 0)
                        a = mid + 1;
                    else
                        b2 = mid;
                }
                start = a == 0 ? 0 : int64_t(a) - 1;
            }
        } else {
            // Last block whose prefix is <= hi's prefix.
            start = int64_t(fences->size()) - 1;
            if (hasHi) {
                size_t a = 0, b2 = fences->size();
                while (a < b2) {
                    const size_t mid = (a + b2) / 2;
                    if (prefixCmp(reinterpret_cast<const uint8_t*>(hi.data()), hi.size(), (*fences)[mid]) >= 0)
                        a = mid + 1;
                    else
                        b2 = mid;
                }
                start = int64_t(a) - 1;
                if (start < 0) return false;
            }
        }
        if (st->stats()) st->stats()->fenceReads++;
        if (!loadBlock(st, start)) return false;
        idx = desc ? int64_t(ents.size()) : -1;
        return true;
    }
    bool advance(LaneStore* st) override {
        if (err) return valid = false;
        for (;;) {
            idx += desc ? -1 : 1;
            if (idx >= 0 && idx < int64_t(ents.size())) {
                const uint8_t* p = buf + ents[size_t(idx)];
                klen = getU16(p);
                key = p + 2;
                val = p + 2 + klen;
                vlen = v;
                return valid = true;
            }
            if (done) return valid = false;
            const int64_t nb = block + (desc ? -1 : 1);
            if (nb < 0 || nb >= int64_t(fences->size())) return valid = false;
            // Past the range by the fence prefix alone?
            if (!desc && hasHi &&
                prefixCmp(reinterpret_cast<const uint8_t*>(hi.data()), hi.size(), (*fences)[size_t(nb)]) < 0)
                return valid = false;
            if (!loadBlock(st, nb)) return valid = false;
            idx = desc ? int64_t(ents.size()) : -1;
        }
    }
};

}  // namespace

PostingScan::PostingScan() = default;
PostingScan::~PostingScan() = default;
PostingScan::PostingScan(PostingScan&&) noexcept = default;
PostingScan& PostingScan::operator=(PostingScan&&) noexcept = default;

const uint8_t* PostingScan::key() const { return srcs_[size_t(cur_)]->key; }
uint16_t PostingScan::klen() const { return srcs_[size_t(cur_)]->klen; }
const uint8_t* PostingScan::val() const { return srcs_[size_t(cur_)]->val; }
uint8_t PostingScan::vlen() const { return srcs_[size_t(cur_)]->vlen; }

void PostingScan::pick() {
    cur_ = -1;
    for (size_t i = 0; i < srcs_.size(); i++) {
        const Source* s = srcs_[i].get();
        if (!s->valid) continue;
        if (cur_ < 0) {
            cur_ = int(i);
            continue;
        }
        const Source* b = srcs_[size_t(cur_)].get();
        int c = keyCmp(s->key, s->klen, b->key, b->klen);
        if (c == 0) c = std::memcmp(s->val, b->val, std::min(s->vlen, b->vlen));
        if (desc_ ? c > 0 : c < 0) cur_ = int(i);
    }
}

bool PostingScan::next() {
    if (cur_ < 0) return false;
    Source* s = srcs_[size_t(cur_)].get();
    s->advance(store_);
    if (s->err && !err_) err_ = s->err;
    if (store_ && store_->stats()) store_->stats()->indexEntries++;
    pick();
    return cur_ >= 0;
}

// ---------------------------------------------------------------------------
// LaneStore
// ---------------------------------------------------------------------------
LaneStore::LaneStore(const LaneStoreConfig& cfg) : cfg_(cfg), io_(cfg.io, cfg.root, cfg.maxHandles) {
    stats_ = &scratchStats_;
    io_.setStats(stats_);
}

LaneStore::~LaneStore() = default;

void LaneStore::beginStatement(ReadStats* stats, const WorkGuard* guard) {
    stats_ = stats ? stats : &scratchStats_;
    io_.setStats(stats_);
    io_.beginStatement();
    guard_ = guard;
}

void LaneStore::endStatement() {
    stats_ = &scratchStats_;
    io_.setStats(stats_);
    guard_ = nullptr;
}

void LaneStore::cachePut(const FileKey& key, std::shared_ptr<const void> obj, uint64_t bytes) {
    auto it = cache_.find(key);
    if (it != cache_.end()) return;
    while (cacheUsed_ + bytes > cfg_.cacheBytes && !cacheLru_.empty()) {
        auto vit = cache_.find(cacheLru_.back());
        cacheUsed_ -= vit->second.bytes;
        cache_.erase(vit);
        cacheLru_.pop_back();
    }
    cacheLru_.push_front(key);
    CacheEntry e;
    e.obj = std::move(obj);
    e.bytes = bytes;
    e.lru = cacheLru_.begin();
    cache_.emplace(key, std::move(e));
    cacheUsed_ += bytes;
}

std::shared_ptr<const void> LaneStore::cacheGet(const FileKey& key) {
    auto it = cache_.find(key);
    if (it == cache_.end()) return nullptr;
    cacheLru_.splice(cacheLru_.begin(), cacheLru_, it->second.lru);
    return it->second.obj;
}

int32_t LaneStore::open() {
    if (opened_) return 0;
    FileRef f;
    int32_t rc = io_.get(sk('S'), &f);
    if (rc < 0) return rc;
    StoreFile sf{};
    if (io_.read(f, &sf, sizeof(sf), 0) != int64_t(sizeof(sf)) || sf.magic != kMagicStore ||
        sf.format != kFormat || sf.crc != crc32c(&sf, offsetof(StoreFile, crc)))
        return kRsCorrupt;
    rc = io_.get(sk('M'), &f);
    if (rc == FLATSQL_IO_ERR_NOENT) return kRsNotMigrated;
    if (rc < 0) return rc;
    MigratedFile mf{};
    if (io_.read(f, &mf, sizeof(mf), 0) != int64_t(sizeof(mf)) || mf.magic != kMagicMigrated ||
        std::memcmp(mf.uuid, sf.uuid, 16) != 0)
        return kRsNotMigrated;
    opened_ = true;
    return refreshRegistry();
}

int32_t LaneStore::readHead(const FileKey& key, uint16_t kind, std::vector<uint8_t>* slot, bool* missing) {
    *missing = false;
    FileRef f;
    int32_t rc = io_.get(key, &f);
    if (rc == FLATSQL_IO_ERR_NOENT) {
        *missing = true;
        return 0;
    }
    if (rc < 0) return rc;
    uint8_t buf[2 * kHeadSlotBytes];
    const int64_t n = io_.read(f, buf, sizeof(buf), 0);
    if (n < 0) return int32_t(n);
    if (stats_) stats_->headReads++;
    int best = -1;
    uint64_t bestGen = 0;
    for (int s = 0; s < 2; s++) {
        if (n < int64_t(s) * kHeadSlotBytes + int64_t(sizeof(HeadPrefix))) continue;
        const uint8_t* p = buf + size_t(s) * kHeadSlotBytes;
        const size_t avail = size_t(std::min<int64_t>(kHeadSlotBytes, n - int64_t(s) * kHeadSlotBytes));
        if (!validHeadSlot(p, avail, kind)) continue;
        HeadPrefix hp;
        std::memcpy(&hp, p, sizeof(hp));
        if (best < 0 || hp.gen > bestGen) {
            best = s;
            bestGen = hp.gen;
        }
    }
    if (best < 0) {
        *missing = true;  // registered, never committed (or both slots torn: -3, §10)
        return 0;
    }
    const uint8_t* p = buf + size_t(best) * kHeadSlotBytes;
    HeadPrefix hp;
    std::memcpy(&hp, p, sizeof(hp));
    slot->assign(p, p + hp.usedLen);
    return 0;
}

int32_t LaneStore::loadTypeConfig(TypeInfo* t) {
    FileRef f;
    int32_t rc = io_.get(tk('s', t->fid, 0, 0, t->configFp), &f);
    if (rc < 0) return rc;
    const int64_t size = io_.size(f);
    if (size < 16) return kRsCorrupt;
    std::vector<uint8_t> buf(static_cast<size_t>(size));
    if (io_.read(f, buf.data(), buf.size(), 0) != size) return FLATSQL_IO_ERR_IO;
    if (getU32(buf.data()) != kMagicTypeConfig || getU32(buf.data() + 8) != buf.size() - 16 ||
        crc32c(buf.data() + 16, buf.size() - 16) != getU32(buf.data() + 12))
        return kRsCorrupt;
    auto cfg = std::make_shared<TypeConfig>();
    if (!cfg->parse(buf.data() + 16, buf.size() - 16).empty()) return kRsCorrupt;
    t->cfg = cfg;
    t->cols = columnsOf(*cfg);
    return 0;
}

int32_t LaneStore::refreshRegistry() {
    std::vector<uint8_t> slot;
    bool missing = false;
    int32_t rc = readHead(sk('H'), kHeadRegistry, &slot, &missing);
    if (rc < 0) return rc;
    if (missing) return FLATSQL_IO_ERR_NOENT;
    RegistryHeadFixed h;
    std::memcpy(&h, slot.data(), sizeof(h));
    if (reg_ && reg_->incarnation == h.incarnation && reg_->frames >= h.frameCount) return 0;
    const bool incremental = reg_ && reg_->incarnation == h.incarnation && h.fslEnd >= regFslRead_;
    std::shared_ptr<RegistryView> v = std::make_shared<RegistryView>();
    uint64_t from = 0;
    if (incremental) {
        *v = *reg_;
        from = regFslRead_;
        // Types are shared; the ones that gain partitions are copied below.
    } else {
        v->frames = 0;
    }
    v->incarnation = h.incarnation;
    FileRef lf;
    rc = io_.get(sk('R'), &lf);
    if (rc < 0) return rc;
    const uint64_t end = h.fslEnd;
    std::vector<uint8_t> buf(end > from ? size_t(end - from) : 0);
    if (!buf.empty() && io_.read(lf, buf.data(), buf.size(), from) != int64_t(buf.size())) return FLATSQL_IO_ERR_IO;
    size_t off = 0;
    std::map<uint32_t, std::shared_ptr<TypeInfo>> touched;
    auto typeFor = [&](const uint8_t fid[4]) -> std::shared_ptr<TypeInfo> {
        for (auto& t : v->types) {
            if (std::memcmp(t->fid, fid, 4) != 0) continue;
            auto tt = touched.find(fidU32(fid));
            if (tt != touched.end()) return tt->second;
            auto copy = std::make_shared<TypeInfo>(*t);
            t = copy;
            touched[fidU32(fid)] = copy;
            return copy;
        }
        return nullptr;
    };
    while (off + 10 <= buf.size() && v->frames < h.frameCount) {
        const uint32_t len = getU32(buf.data() + off);
        if (len < 2 || off + 8 + len > buf.size()) break;
        if (crc32c(buf.data() + off + 8, len) != getU32(buf.data() + off + 4)) break;
        const uint16_t kind = getU16(buf.data() + off + 8);
        const uint8_t* p = buf.data() + off + 10;
        const uint8_t* pe = buf.data() + off + 8 + len;
        auto getStr = [&](std::string* s) {
            if (p + 2 > pe) return false;
            const uint16_t n = getU16(p);
            if (p + 2 + n > pe) return false;
            s->assign(reinterpret_cast<const char*>(p + 2), n);
            p += 2 + n;
            return true;
        };
        if (kind == kRegPartitionAdd && p + 8 <= pe) {
            PartInfo pi;
            pi.pid = getU32(p);
            std::memcpy(pi.fid, p + 4, 4);
            p += 8;
            if (getStr(&pi.token) && getStr(&pi.sqlName)) {
                if (v->parts.size() <= pi.pid) v->parts.resize(size_t(pi.pid) + 1);
                v->parts[pi.pid] = pi;
                if (auto t = typeFor(pi.fid)) t->pids.push_back(pi.pid);
            }
        } else if (kind == kRegTypeAdd && p + 4 <= pe) {
            auto t = std::make_shared<TypeInfo>();
            std::memcpy(t->fid, p, 4);
            p += 4;
            if (getStr(&t->schemaName) && p + 24 <= pe) {
                t->configFp = getU64(p);
                t->typeName = t->schemaName;
                const size_t dot = t->typeName.find('.');
                if (dot != std::string::npos) t->typeName = t->typeName.substr(0, dot);
                auto key = std::make_pair(fidU32(t->fid), t->configFp);
                auto cached = typeInfos_.find(key);
                if (cached != typeInfos_.end()) {
                    t->cfg = cached->second->cfg;
                    t->cols = cached->second->cols;
                } else {
                    rc = loadTypeConfig(t.get());
                    if (rc < 0) return rc;
                    typeInfos_[key] = t;
                }
                bool replaced = false;
                for (auto& existing : v->types)
                    if (std::memcmp(existing->fid, t->fid, 4) == 0) {
                        t->pids = existing->pids;
                        existing = t;
                        replaced = true;
                    }
                if (!replaced) v->types.push_back(t);
                touched.erase(fidU32(t->fid));
            }
        } else if ((kind == kRegQuarantine || kind == kRegUnquarantine || kind == kRegDrop) && p + 4 <= pe) {
            const uint32_t pid = getU32(p);
            if (pid < v->parts.size() && v->parts[pid].pid) {
                if (kind == kRegDrop) v->parts[pid].dropped = true;
                else v->parts[pid].quarantined = kind == kRegQuarantine;
            }
        }
        off += 8 + len;
        v->frames++;
    }
    regFslRead_ = from + off;
    v->fslEnd = regFslRead_;
    reg_ = v;
    return 0;
}

int32_t LaneStore::loadManifest(uint32_t pid, uint32_t gen, std::shared_ptr<const Manifest>* out) {
    const FileKey key = pk('f', pid, 0, gen);
    if (auto c = cacheGet(key)) {
        *out = std::static_pointer_cast<const Manifest>(c);
        return 0;
    }
    FileRef f;
    int32_t rc = io_.get(key, &f);
    if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
    if (rc < 0) return rc;
    const int64_t size = io_.size(f);
    if (size < 32) return kRsCorrupt;
    std::vector<uint8_t> buf(static_cast<size_t>(size));
    if (io_.read(f, buf.data(), buf.size(), 0) != size) return FLATSQL_IO_ERR_IO;
    const size_t body = buf.size() - 8;
    if (getU32(buf.data()) != kMagicManifest || crc32c(buf.data(), body) != getU32(buf.data() + body))
        return kRsCorrupt;
    auto m = std::make_shared<Manifest>();
    m->gen = getU32(buf.data() + 8);
    const uint16_t nSegs = getU16(buf.data() + 6);
    size_t off = 24;
    for (uint16_t i = 0; i < nSegs; i++) {
        if (off + 80 > body) return kRsCorrupt;
        const uint8_t* s = buf.data() + off;
        ManifestSegRef r;
        r.seg = getU32(s);
        const uint32_t flags = getU32(s + 4);
        r.sealed = flags & 1;
        r.firstPseq = getU64(s + 8);
        r.endPseq = getU64(s + 16);
        r.mergedEnd = getU64(s + 24);
        r.dLen = getU64(s + 32);
        r.rLen = getU64(s + 40);
        r.aLen = getU64(s + 48);
        r.minEpoch = int64_t(getU64(s + 56));
        r.maxEpoch = int64_t(getU64(s + 64));
        const uint32_t nRuns = getU32(s + 72);
        r.cgen = getU32(s + 76);  // T3 SWAP seam (0: original d/r/a files)
        off += 80;
        for (uint32_t k = 0; k < nRuns; k++) {
            if (off + 24 > body) return kRsCorrupt;
            SegRunRef run;
            run.gen = getU32(buf.data() + off);
            run.nEntries = getU64(buf.data() + off + 8);
            run.fileLen = getU64(buf.data() + off + 16);
            r.runs.push_back(run);
            off += 24;
        }
        m->segs.push_back(std::move(r));
    }
    std::sort(m->segs.begin(), m->segs.end(), [](const ManifestSegRef& a, const ManifestSegRef& b) {
        return a.seg < b.seg;
    });
    cachePut(key, m, buf.size() + 256);
    *out = m;
    return 0;
}

int32_t LaneStore::loadPart(uint32_t pid, PartSnap* out, bool withManifest) {
    *out = PartSnap();
    out->pid = pid;
    if (reg_) {
        if (const PartInfo* pi = reg_->part(pid)) std::memcpy(out->fid, pi->fid, 4);
    }
    std::vector<uint8_t> slot;
    bool missing = false;
    int32_t rc = readHead(pk('h', pid), kHeadPartition, &slot, &missing);
    if (rc < 0) return rc;
    if (missing) {
        out->empty = true;
        return 0;
    }
    std::memcpy(&out->head, slot.data(), sizeof(PartitionHeadFixed));
    const PartitionHeadFixed& h = out->head;
    size_t off = sizeof(PartitionHeadFixed);
    if (h.nL0 > kMaxL0Dir || off + size_t(h.nL0) * sizeof(L0DirEntry) + 4 > slot.size()) return kRsCorrupt;
    out->l0.resize(h.nL0);
    if (h.nL0) std::memcpy(out->l0.data(), slot.data() + off, size_t(h.nL0) * sizeof(L0DirEntry));
    off += size_t(h.nL0) * sizeof(L0DirEntry);
    if (h.nLanes == 0xffff) {
        out->lanesOverflow = true;
    } else {
        if (off + size_t(h.nLanes) * sizeof(LaneCounter) + 4 > slot.size()) return kRsCorrupt;
        out->lanes.resize(h.nLanes);
        if (h.nLanes) std::memcpy(out->lanes.data(), slot.data() + off, size_t(h.nLanes) * sizeof(LaneCounter));
    }
    out->empty = false;
    out->visible = h.pseqHi;
    if (h.manifestGen && withManifest) {
        rc = loadManifest(pid, h.manifestGen, &out->manifest);
        if (rc < 0) return rc;
    }
    return 0;
}

int32_t LaneStore::laneCounters(const PartSnap& s, std::vector<LaneCounter>* out) {
    out->clear();
    if (s.empty) return 0;
    if (!s.lanesOverflow) {
        *out = s.lanes;
        return 0;
    }
    FileRef f;
    int32_t rc = io_.get(pk('m', s.pid, s.head.lanesOverflowSeg), &f);
    if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
    if (rc < 0) return rc;
    uint8_t nb[4];
    if (io_.read(f, nb, 4, s.head.lanesOverflowOff) != 4) return FLATSQL_IO_ERR_IO;
    const uint32_t n = getU32(nb);
    if (n > 1u << 20) return kRsCorrupt;
    out->resize(n);
    if (n && io_.read(f, out->data(), size_t(n) * sizeof(LaneCounter), s.head.lanesOverflowOff + 4) !=
                 int64_t(size_t(n) * sizeof(LaneCounter)))
        return FLATSQL_IO_ERR_IO;
    return 0;
}

int32_t LaneStore::laneTuples(const PartSnap& s, std::vector<LaneTuple>* out) {
    out->clear();
    if (s.empty) return 0;
    FileRef f;
    int32_t rc = io_.get(pk('l', s.pid), &f);
    if (rc == FLATSQL_IO_ERR_NOENT) return 0;
    if (rc < 0) return rc;
    const int64_t size = io_.size(f);
    if (size <= 0) return 0;
    std::vector<uint8_t> buf(static_cast<size_t>(size));
    if (io_.read(f, buf.data(), buf.size(), 0) != size) return FLATSQL_IO_ERR_IO;
    size_t off = 0;
    while (off + 8 <= buf.size()) {
        const uint32_t len = getU32(buf.data() + off);
        if (len < 14 || off + 8 + len > buf.size()) break;  // zero fill or a torn tail
        if (crc32c(buf.data() + off + 8, len) != getU32(buf.data() + off + 4)) break;
        const uint8_t* q = buf.data() + off + 8;
        const uint8_t* end = q + len;
        LaneTuple t;
        t.id = getU32(q);
        q += 4;
        std::string* fields[5] = {&t.provider, &t.source, &t.batch, &t.peer, &t.pubkey};
        bool ok = true;
        for (auto* fs : fields) {
            if (q + 2 > end) {
                ok = false;
                break;
            }
            const uint16_t n = getU16(q);
            if (q + 2 + n > end) {
                ok = false;
                break;
            }
            fs->assign(reinterpret_cast<const char*>(q + 2), n);
            q += 2 + n;
        }
        if (!ok) break;
        out->push_back(std::move(t));
        off += 8 + len;
    }
    return 0;
}

int32_t LaneStore::loadTypeLabels(const uint8_t fid[4], TypeSnap* t) {
    const TypeHeadFixed& h = t->head;
    LabelCache& lc = labelCache_[fidU32(fid)];
    FileRef f;
    int32_t rc = io_.get(tk('m', fid, h.labelCkptSeg), &f);
    if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsSnapshotGone : rc;
    uint64_t off;
    if (lc.ckptOff == h.labelCkptOff && lc.scannedTo && lc.scannedTo <= h.mEnd && lc.commitSeq <= h.commitSeq) {
        off = lc.scannedTo;  // incremental: only batches after the last scan
    } else {
        lc = LabelCache();
        lc.ckptOff = h.labelCkptOff;
        off = h.labelCkptOff;
    }
    const uint64_t end = h.mEnd;
    std::vector<uint8_t> labels;
    std::unordered_map<uint32_t, uint64_t> map = lc.labels;
    while (off + sizeof(TypeBatchHeader) <= end) {
        TypeBatchHeader bh;
        if (io_.read(f, &bh, sizeof(bh), off) != int64_t(sizeof(bh))) return FLATSQL_IO_ERR_IO;
        if (bh.magic != kMagicTypeBatch || bh.batchLen < sizeof(bh) + 8 || off + bh.batchLen > end) return kRsCorrupt;
        labels.resize(size_t(bh.nLabel) * sizeof(LabelEntry));
        if (bh.nLabel && io_.read(f, labels.data(), labels.size(), off + sizeof(bh)) != int64_t(labels.size()))
            return FLATSQL_IO_ERR_IO;
        for (uint32_t i = 0; i < bh.nLabel; i++) {
            LabelEntry le;
            std::memcpy(&le, labels.data() + size_t(i) * sizeof(le), sizeof(le));
            map[le.pid] = le.labeledThrough;
        }
        off += bh.batchLen;
    }
    lc.labels = map;
    lc.scannedTo = off;
    lc.commitSeq = h.commitSeq;
    t->labeled = std::make_shared<const std::unordered_map<uint32_t, uint64_t>>(std::move(map));
    return 0;
}

int32_t LaneStore::loadType(const uint8_t fid[4], TypeSnap* out) {
    *out = TypeSnap();
    std::memcpy(out->fid, fid, 4);
    std::vector<uint8_t> slot;
    bool missing = false;
    int32_t rc = readHead(tk('h', fid), kHeadType, &slot, &missing);
    if (rc < 0) return rc;
    if (missing) {
        out->empty = true;
        out->labeled = std::make_shared<const std::unordered_map<uint32_t, uint64_t>>();
        out->fence = std::make_shared<const std::vector<ArrivalFence>>();
        out->segStart.assign(1, 0);
        return 0;
    }
    std::memcpy(&out->head, slot.data(), sizeof(TypeHeadFixed));
    const TypeHeadFixed& h = out->head;
    size_t off = sizeof(TypeHeadFixed);
    if (h.nL0 > kMaxTypeL0Dir || off + size_t(h.nL0) * sizeof(TypeL0DirEntry) + 4 > slot.size()) return kRsCorrupt;
    out->l0.resize(h.nL0);
    if (h.nL0) std::memcpy(out->l0.data(), slot.data() + off, size_t(h.nL0) * sizeof(TypeL0DirEntry));
    off += size_t(h.nL0) * sizeof(TypeL0DirEntry);
    if (h.nLabels != 0xffff) {
        if (off + size_t(h.nLabels) * sizeof(LabelEntry) + 4 > slot.size()) return kRsCorrupt;
        auto map = std::make_shared<std::unordered_map<uint32_t, uint64_t>>();
        for (uint16_t i = 0; i < h.nLabels; i++) {
            LabelEntry le;
            std::memcpy(&le, slot.data() + off + size_t(i) * sizeof(le), sizeof(le));
            (*map)[le.pid] = le.labeledThrough;
        }
        out->labeled = map;
    } else {
        rc = loadTypeLabels(fid, out);
        if (rc < 0) return rc;
    }
    // Catalog runs (type manifest).
    if (h.manifestGen) {
        const FileKey mk = tk('f', fid, 0, h.manifestGen);
        std::shared_ptr<const std::vector<SegRunRef>> runs;
        if (auto c = cacheGet(mk)) {
            runs = std::static_pointer_cast<const std::vector<SegRunRef>>(c);
        } else {
            FileRef f;
            rc = io_.get(mk, &f);
            if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
            if (rc < 0) return rc;
            const int64_t size = io_.size(f);
            if (size < 24) return kRsCorrupt;
            std::vector<uint8_t> man(static_cast<size_t>(size));
            if (io_.read(f, man.data(), man.size(), 0) != size) return FLATSQL_IO_ERR_IO;
            if (getU32(man.data()) != kMagicManifest) return kRsCorrupt;
            const uint32_t n = getU32(man.data() + 4);
            const size_t body = 16 + size_t(n) * 16;
            if (body + 8 > man.size() || crc32c(man.data(), body) != getU32(man.data() + body)) return kRsCorrupt;
            auto v = std::make_shared<std::vector<SegRunRef>>();
            for (uint32_t i = 0; i < n; i++) {
                SegRunRef r;
                r.gen = getU32(man.data() + 16 + size_t(i) * 16);
                r.fileLen = getU64(man.data() + 16 + size_t(i) * 16 + 8);
                v->push_back(r);
            }
            cachePut(mk, v, man.size() + 64);
            runs = v;
        }
        out->runs = *runs;
    }
    // Arrivals fence index (sealed segments are immutable: cache by count).
    const FileKey fk = tk('F', fid, h.gSeg);
    if (h.gSeg == 0) {
        out->fence = std::make_shared<const std::vector<ArrivalFence>>();
    } else if (auto c = cacheGet(fk)) {
        out->fence = std::static_pointer_cast<const std::vector<ArrivalFence>>(c);
    } else {
        FileRef f;
        FileKey fileKey = tk('F', fid);
        rc = io_.get(fileKey, &f);
        if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsCorrupt : rc;
        auto v = std::make_shared<std::vector<ArrivalFence>>(h.gSeg);
        if (io_.read(f, v->data(), size_t(h.gSeg) * sizeof(ArrivalFence), 0) !=
            int64_t(size_t(h.gSeg) * sizeof(ArrivalFence)))
            return FLATSQL_IO_ERR_IO;
        for (uint32_t i = 0; i < h.gSeg; i++) {
            const ArrivalFence& e = (*v)[i];
            if (e.seg != i || e.crc != crc32c(&e, offsetof(ArrivalFence, crc))) return kRsCorrupt;
        }
        if (stats_) stats_->fenceReads++;
        cachePut(fk, v, size_t(h.gSeg) * sizeof(ArrivalFence) + 64);
        out->fence = v;
    }
    out->segStart.assign(size_t(h.gSeg) + 1, 0);
    for (uint32_t i = 0; i < h.gSeg; i++) out->segStart[i + 1] = out->segStart[i] + (*out->fence)[i].count;
    out->empty = false;
    return 0;
}

FileKey LaneStore::segKey(const PartSnap& s, char letter, uint32_t seg) const {
    uint32_t cgen = 0;
    if (s.manifest)
        if (const ManifestSegRef* m = s.manifest->segFor(seg)) cgen = m->cgen;
    return pk(letter, s.pid, seg, cgen);
}

int32_t LaneStore::readRows(const PartSnap& s, uint64_t first, uint32_t n, RecRow* out) {
    if (!n) return 0;
    if (s.empty || first == 0 || first + n - 1 > s.head.pseqHi) return FLATSQL_IO_ERR_NOENT;
    uint64_t pseq = first;
    uint32_t done = 0;
    while (done < n) {
        FileRef f;
        uint64_t off;
        uint64_t avail;  // rows readable contiguously from this region
        int32_t rc;
        if (pseq <= s.head.mergedThrough) {
            const ManifestSegRef* m = s.manifest ? s.manifest->segForPseq(pseq) : nullptr;
            if (!m) return kRsCorrupt;
            FileKey k = pk('r', s.pid, m->seg, m->cgen);
            rc = io_.get(k, &f);
            off = (pseq - m->firstPseq) * sizeof(RecRow);
            avail = m->mergedEnd - pseq;
        } else {
            // Unmerged: the L0 directory entry holding pseq (binary search).
            size_t a = 0, b = s.l0.size();
            while (a < b) {
                const size_t mid = (a + b) / 2;
                if (s.l0[mid].firstPseq + s.l0[mid].nRows <= pseq) a = mid + 1;
                else b = mid;
            }
            if (a >= s.l0.size() || pseq < s.l0[a].firstPseq) return kRsCorrupt;
            const L0DirEntry& e = s.l0[a];
            rc = io_.get(pk('m', s.pid, e.mSeg), &f);
            off = e.mOff + sizeof(BatchHeader) + (pseq - e.firstPseq) * sizeof(RecRow);
            avail = e.firstPseq + e.nRows - pseq;
        }
        if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
        if (rc < 0) return rc;
        const uint32_t take = uint32_t(std::min<uint64_t>(avail, n - done));
        const int64_t got = io_.read(f, out + done, size_t(take) * sizeof(RecRow), off);
        if (got != int64_t(size_t(take) * sizeof(RecRow))) return got < 0 ? int32_t(got) : kRsCorrupt;
        for (uint32_t i = 0; i < take; i++)
            if (out[done + i].pseq != pseq + i) return kRsCorrupt;
        done += take;
        pseq += take;
    }
    if (stats_) stats_->rowsExamined += n;
    return 0;
}

int32_t LaneStore::readRow(const PartSnap& s, uint64_t pseq, RecRow* out) { return readRows(s, pseq, 1, out); }

int32_t LaneStore::readFramePart(const PartSnap& s, const RecRow& r, uint64_t off, uint32_t n, uint8_t* dst) {
    FileRef f;
    int32_t rc = io_.get(segKey(s, 'd', r.seg), &f);
    if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
    if (rc < 0) return rc;
    const int64_t got = io_.read(f, dst, n, uint64_t(r.off) + off);
    if (got != int64_t(n)) return got < 0 ? int32_t(got) : kRsSnapshotGone;
    return 0;
}

int32_t LaneStore::readFrame(const PartSnap& s, const RecRow& r, uint8_t* dst) {
    const int32_t rc = readFramePart(s, r, 0, r.len, dst);
    if (rc < 0) return rc;
    if (cfg_.verifyFrameCrc && r.dataCrc && crc32c(dst, r.len) != r.dataCrc) return kRsCorrupt;
    return 0;
}

int32_t LaneStore::readAttr(const PartSnap& s, const RecRow& r, std::vector<uint8_t>* out) {
    out->clear();
    if (!(r.flags & kRowHasAttr) || r.attrLen == 0) return 0;
    FileRef f;
    const FileKey k = (r.flags & kRowAttrInM) ? pk('m', s.pid, r.seg) : segKey(s, 'a', r.seg);
    int32_t rc = io_.get(k, &f);
    if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
    if (rc < 0) return rc;
    out->resize(r.attrLen);
    const int64_t got = io_.read(f, out->data(), r.attrLen, r.attrOff);
    if (got != int64_t(r.attrLen)) return got < 0 ? int32_t(got) : kRsCorrupt;
    return 0;
}

int32_t LaneStore::readBlock(const FileKey& key, uint64_t off, uint8_t* dst) {
    FileRef f;
    const int32_t rc = io_.get(key, &f);
    if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsSnapshotGone : rc;
    const int64_t n = io_.read(f, dst, kL1BlockBytes, off);
    if (n != int64_t(kL1BlockBytes)) return n < 0 ? int32_t(n) : kRsCorrupt;
    if (!l1BlockValid(dst)) return kRsCorrupt;
    return 0;
}

std::shared_ptr<const L0Parsed> LaneStore::l0Block(uint32_t pid, uint32_t mSeg, uint64_t off, uint32_t len,
                                                   int32_t* rc) {
    FileKey key = pk('0', pid, mSeg);
    key.extra = off;
    if (auto c = cacheGet(key)) return std::static_pointer_cast<const L0Parsed>(c);
    FileRef f;
    *rc = io_.get(pk('m', pid, mSeg), &f);
    if (*rc < 0) {
        if (*rc == FLATSQL_IO_ERR_NOENT) *rc = kRsSnapshotGone;
        return nullptr;
    }
    auto b = std::make_shared<L0Parsed>();
    b->bytes.resize(len);
    const int64_t got = io_.read(f, b->bytes.data(), len, off);
    if (got != int64_t(len)) {
        *rc = got < 0 ? int32_t(got) : kRsCorrupt;
        return nullptr;
    }
    L0Header lh;
    std::memcpy(&lh, b->bytes.data(), sizeof(lh));
    if (lh.magic != kMagicL0 || lh.blockLen > len) {
        *rc = kRsCorrupt;
        return nullptr;
    }
    b->bytes.resize(lh.blockLen);
    L0KindInfo kinds[64];
    size_t nk = 0;
    if (!parseL0Block(b->bytes.data(), b->bytes.size(), kinds, 64, &nk)) {
        *rc = kRsCorrupt;
        return nullptr;
    }
    b->kinds.assign(kinds, kinds + nk);
    cachePut(key, b, b->bytes.size() + 128);
    *rc = 0;
    return b;
}

std::shared_ptr<const L0Parsed> LaneStore::typeL0Block(const uint8_t fid[4], const TypeL0DirEntry& e, int32_t* rc) {
    FileKey key = tk('0', fid, e.mSeg);
    key.extra = e.mOff;
    if (auto c = cacheGet(key)) return std::static_pointer_cast<const L0Parsed>(c);
    FileRef f;
    *rc = io_.get(tk('m', fid, e.mSeg), &f);
    if (*rc < 0) {
        if (*rc == FLATSQL_IO_ERR_NOENT) *rc = kRsSnapshotGone;
        return nullptr;
    }
    auto b = std::make_shared<L0Parsed>();
    b->bytes.resize(e.l0Len);
    const int64_t got = io_.read(f, b->bytes.data(), e.l0Len, e.mOff + e.l0Off);
    if (got != int64_t(e.l0Len)) {
        *rc = got < 0 ? int32_t(got) : kRsCorrupt;
        return nullptr;
    }
    L0KindInfo kinds[64];
    size_t nk = 0;
    if (!parseL0Block(b->bytes.data(), b->bytes.size(), kinds, 64, &nk)) {
        *rc = kRsCorrupt;
        return nullptr;
    }
    b->kinds.assign(kinds, kinds + nk);
    cachePut(key, b, b->bytes.size() + 128);
    *rc = 0;
    return b;
}

std::shared_ptr<const L1Run> LaneStore::run(uint32_t pid, uint32_t seg, uint32_t gen, uint64_t fileLen, FileKey* keyOut,
                                            int32_t* rc) {
    const FileKey key = pk('x', pid, seg, gen);
    *keyOut = key;
    FileKey ck = key;
    ck.letter = 'X';
    *rc = 0;
    if (auto c = cacheGet(ck)) return std::static_pointer_cast<const L1Run>(c);
    FileRef file;
    *rc = io_.get(key, &file);
    if (*rc < 0) {
        if (*rc == FLATSQL_IO_ERR_NOENT) *rc = kRsSnapshotGone;
        return nullptr;
    }
    auto r = std::make_shared<L1Run>();
    const uint64_t before = io_.ioStats().totalReadBytes();
    if (r->load(&io_.ctx(), file, fileLen) < 0) {
        *rc = kRsCorrupt;
        return nullptr;
    }
    if (stats_) {
        stats_->bytesRead += io_.ioStats().totalReadBytes() - before;
        stats_->fenceReads++;
    }
    cachePut(ck, r, r->memoryBytes() + 256);
    *rc = 0;
    return r;
}

std::shared_ptr<const L1Run> LaneStore::typeRun(const uint8_t fid[4], uint32_t gen, uint64_t fileLen, FileKey* keyOut,
                                                int32_t* rc) {
    const FileKey key = tk('x', fid, 0, gen);
    *keyOut = key;
    FileKey ck = key;
    ck.letter = 'X';
    *rc = 0;
    if (auto c = cacheGet(ck)) return std::static_pointer_cast<const L1Run>(c);
    FileRef file;
    *rc = io_.get(key, &file);
    if (*rc < 0) {
        if (*rc == FLATSQL_IO_ERR_NOENT) *rc = kRsSnapshotGone;
        return nullptr;
    }
    auto r = std::make_shared<L1Run>();
    const uint64_t before = io_.ioStats().totalReadBytes();
    if (r->load(&io_.ctx(), file, fileLen) < 0) {
        *rc = kRsCorrupt;
        return nullptr;
    }
    if (stats_) {
        stats_->bytesRead += io_.ioStats().totalReadBytes() - before;
        stats_->fenceReads++;
    }
    cachePut(ck, r, r->memoryBytes() + 256);
    *rc = 0;
    return r;
}

// ---------------------------------------------------------------------------
// Scans and lookups
// ---------------------------------------------------------------------------
PostingScan LaneStore::scan(const PartSnap& s, uint16_t kind, const uint8_t* lo, size_t lol, const uint8_t* hi,
                            size_t hil, bool desc) {
    PostingScan ps;
    ps.store_ = this;
    ps.desc_ = desc;
    if (s.empty) return ps;
    const std::string los = lo ? std::string(reinterpret_cast<const char*>(lo), lol) : std::string();
    const std::string his = hi ? std::string(reinterpret_cast<const char*>(hi), hil) : std::string();
    for (const L0DirEntry& e : s.l0) {
        int32_t rc = 0;
        auto blk = l0Block(s.pid, e.mSeg, e.mOff + e.l0Off, e.batchLen - e.l0Off, &rc);
        if (!blk) {
            ps.err_ = rc;
            return ps;
        }
        std::unique_ptr<L0Source> src(new L0Source());
        if (!src->init(blk, kind, los, lo != nullptr, his, hi != nullptr, desc)) continue;
        src->advance(this);
        ps.srcs_.push_back(std::move(src));
    }
    if (s.manifest) {
        for (const ManifestSegRef& m : s.manifest->segs) {
            for (const SegRunRef& rr : m.runs) {
                FileKey f;
                int32_t rc = 0;
                auto run = this->run(s.pid, m.seg, rr.gen, rr.fileLen, &f, &rc);
                if (!run) {
                    ps.err_ = rc;
                    return ps;
                }
                if (!run->hasKind(kind)) continue;
                std::unique_ptr<L1Source> src(new L1Source());
                if (!src->init(this, run, f, kind, los, lo != nullptr, his, hi != nullptr, desc)) {
                    if (src->err) {
                        ps.err_ = src->err;
                        return ps;
                    }
                    continue;
                }
                src->advance(this);
                if (src->err) {
                    ps.err_ = src->err;
                    return ps;
                }
                ps.srcs_.push_back(std::move(src));
            }
        }
    }
    ps.pick();
    return ps;
}

PostingScan LaneStore::scanType(const TypeSnap& t, uint16_t kind, const uint8_t* lo, size_t lol, const uint8_t* hi,
                                size_t hil, bool desc) {
    PostingScan ps;
    ps.store_ = this;
    ps.desc_ = desc;
    if (t.empty) return ps;
    const std::string los = lo ? std::string(reinterpret_cast<const char*>(lo), lol) : std::string();
    const std::string his = hi ? std::string(reinterpret_cast<const char*>(hi), hil) : std::string();
    for (const TypeL0DirEntry& e : t.l0) {
        int32_t rc = 0;
        auto blk = typeL0Block(t.fid, e, &rc);
        if (!blk) {
            ps.err_ = rc;
            return ps;
        }
        std::unique_ptr<L0Source> src(new L0Source());
        if (!src->init(blk, kind, los, lo != nullptr, his, hi != nullptr, desc)) continue;
        src->advance(this);
        ps.srcs_.push_back(std::move(src));
    }
    for (const SegRunRef& rr : t.runs) {
        FileKey f;
        int32_t rc = 0;
        auto run = typeRun(t.fid, rr.gen, rr.fileLen, &f, &rc);
        if (!run) {
            ps.err_ = rc;
            return ps;
        }
        if (!run->hasKind(kind)) continue;
        std::unique_ptr<L1Source> src(new L1Source());
        if (!src->init(this, run, f, kind, los, lo != nullptr, his, hi != nullptr, desc)) {
            if (src->err) {
                ps.err_ = src->err;
                return ps;
            }
            continue;
        }
        src->advance(this);
        ps.srcs_.push_back(std::move(src));
    }
    ps.pick();
    return ps;
}

namespace {
// Exact-key lookup in one L0 section (entries sorted by (key, value)).
template <typename F>
bool l0Lookup(const L0Parsed& b, uint16_t kind, const uint8_t* key, size_t klen, uint64_t h, F&& visit) {
    const L0KindInfo* ki = b.find(kind);
    if (!ki || !ki->n) return true;
    if (ki->bloomBytes && !bloomTestHash(b.bytes.data() + ki->bloomOff, ki->bloomBytes, h)) return true;
    EntryIter it;
    it.p = b.bytes.data() + ki->entriesOff;
    it.end = it.p + ki->entriesBytes;
    it.vlen = ki->vlen;
    const uint8_t *k, *v;
    uint16_t kl;
    while (it.next(&k, &kl, &v)) {
        const int c = keyCmp(k, kl, key, klen);
        if (c < 0) continue;
        if (c > 0) break;
        if (!visit(v)) return false;
    }
    return true;
}
}  // namespace

int32_t LaneStore::lookup(const PartSnap& s, uint16_t kind, const uint8_t* key, size_t klen,
                          const std::function<bool(const uint8_t*)>& visit) {
    if (s.empty) return 0;
    const uint64_t h = bloomHash(key, klen);
    for (const L0DirEntry& e : s.l0) {
        int32_t rc = 0;
        auto blk = l0Block(s.pid, e.mSeg, e.mOff + e.l0Off, e.batchLen - e.l0Off, &rc);
        if (!blk) return rc;
        if (!l0Lookup(*blk, kind, key, klen, h, visit)) return 0;
    }
    if (!s.manifest) return 0;
    uint8_t scratch[kL1BlockBytes];
    for (const ManifestSegRef& m : s.manifest->segs) {
        for (const SegRunRef& rr : m.runs) {
            FileKey fk;
            int32_t rc = 0;
            auto run = this->run(s.pid, m.seg, rr.gen, rr.fileLen, &fk, &rc);
            if (!run) return rc;
            if (!run->mayContainHash(kind, h)) continue;
            FileRef f;
            rc = io_.get(fk, &f);
            if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsSnapshotGone : rc;
            const uint64_t before = io_.ioStats().totalReadBytes();
            bool stop = false;
            const int64_t n = run->lookup(&io_.ctx(), f, kind, key, klen, scratch,
                                          [&](const uint8_t*, uint16_t, const uint8_t* v) {
                                              if (!stop && !visit(v)) stop = true;
                                          });
            if (stats_) stats_->bytesRead += io_.ioStats().totalReadBytes() - before;
            if (n < 0) return kRsCorrupt;
            if (stop) return 0;
        }
    }
    return 0;
}

int32_t LaneStore::lookupType(const TypeSnap& t, uint16_t kind, const uint8_t* key, size_t klen,
                              const std::function<bool(const uint8_t*)>& visit) {
    if (t.empty) return 0;
    const uint64_t h = bloomHash(key, klen);
    for (const TypeL0DirEntry& e : t.l0) {
        int32_t rc = 0;
        auto blk = typeL0Block(t.fid, e, &rc);
        if (!blk) return rc;
        if (!l0Lookup(*blk, kind, key, klen, h, visit)) return 0;
    }
    uint8_t scratch[kL1BlockBytes];
    for (const SegRunRef& rr : t.runs) {
        FileKey fk;
        int32_t rc = 0;
        auto run = typeRun(t.fid, rr.gen, rr.fileLen, &fk, &rc);
        if (!run) return rc;
        if (!run->mayContainHash(kind, h)) continue;
        FileRef f;
        rc = io_.get(fk, &f);
        if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsSnapshotGone : rc;
        const uint64_t before = io_.ioStats().totalReadBytes();
        bool stop = false;
        const int64_t n = run->lookup(&io_.ctx(), f, kind, key, klen, scratch,
                                      [&](const uint8_t*, uint16_t, const uint8_t* v) {
                                          if (!stop && !visit(v)) stop = true;
                                      });
        if (stats_) stats_->bytesRead += io_.ioStats().totalReadBytes() - before;
        if (n < 0) return kRsCorrupt;
        if (stop) return 0;
    }
    return 0;
}

int32_t LaneStore::isDead(const PartSnap& s, uint64_t pseq, uint64_t bound, bool* dead) {
    *dead = false;
    uint8_t k[8];
    putBE64(k, pseq);
    return lookup(s, kIxDead, k, 8, [&](const uint8_t* v) {
        if (getBE64(v) <= bound) {
            *dead = true;
            return false;
        }
        return true;
    });
}

int32_t LaneStore::isTagDead(const PartSnap& s, uint64_t inst, uint64_t bound, bool* dead) {
    *dead = false;
    uint8_t k[8];
    putBE64(k, inst);
    return lookup(s, kIxTagDead, k, 8, [&](const uint8_t* v) {
        if (getBE64(v) <= bound) {
            *dead = true;
            return false;
        }
        return true;
    });
}

int32_t LaneStore::labelOf(const TypeSnap& t, uint32_t pid, uint64_t pseq, uint8_t* label, uint64_t* gseq,
                           bool* found) {
    *found = false;
    uint8_t k[12];
    putBE32(k, pid);
    putBE64(k + 4, pseq);
    uint64_t bestTcs = 0;
    const int32_t rc = lookupType(t, kIxTypeLabel, k, 12, [&](const uint8_t* v) {
        const uint64_t tcs = getBE64(v);
        if (!*found || tcs > bestTcs) {
            bestTcs = tcs;
            *gseq = getBE64(v + 8);
            *label = v[16];
            *found = true;
        }
        return true;
    });
    return rc;
}

int32_t LaneStore::rehomeOf(const TypeSnap& t, uint64_t gseq, uint32_t* pid, uint64_t* pseq, bool* found) {
    *found = false;
    uint8_t k[8];
    putBE64(k, gseq);
    uint64_t bestTcs = 0;
    return lookupType(t, kIxTypeRehome, k, 8, [&](const uint8_t* v) {
        const uint64_t tcs = getBE64(v);
        if (!*found || tcs > bestTcs) {
            bestTcs = tcs;
            *pid = getBE32(v + 8);
            *pseq = getBE64(v + 12);
            *found = true;
        }
        return true;
    });
}

int32_t LaneStore::catalog(const TypeSnap& t, const uint8_t cid[kCidLen], std::vector<CatalogCopy>* out) {
    out->clear();
    uint8_t key[kCidKeyLen];
    cidSortKey(cid, key);
    const int32_t rc = lookupType(t, kIxTypeCid, key, kCidKeyLen, [&](const uint8_t* v) {
        CatalogCopy c;
        c.pid = getBE32(v);
        c.pseq = getBE64(v + 4);
        c.tcs = getBE64(v + 12);
        c.label = v[20];
        c.gseq = getBE64(v + 21);
        c.len = getBE32(v + 29);
        for (auto& e : *out) {
            if (e.pid == c.pid && e.pseq == c.pseq) {
                if (c.tcs >= e.tcs) e = c;
                return true;
            }
        }
        out->push_back(c);
        return true;
    });
    return rc;
}

// ---------------------------------------------------------------------------
// Arrivals (A15)
// ---------------------------------------------------------------------------
int32_t LaneStore::arrivalsRead(const TypeSnap& t, uint64_t pos, uint32_t n, ArrivalEntry* out) {
    uint32_t done = 0;
    while (done < n) {
        const uint64_t p = pos + done;
        if (p >= t.arrivalsTotal()) return FLATSQL_IO_ERR_NOENT;
        // Segment of p: the last segStart <= p.
        const size_t seg = size_t(std::upper_bound(t.segStart.begin(), t.segStart.end(), p) - t.segStart.begin()) - 1;
        const uint64_t segEnd =
            seg < t.segStart.size() - 1 ? t.segStart[seg + 1] : t.segStart.back() + t.head.gLen / kArrivalBytes;
        const uint32_t take = uint32_t(std::min<uint64_t>(segEnd - p, n - done));
        FileRef f;
        int32_t rc = io_.get(tk('g', t.fid, uint32_t(seg)), &f);
        if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
        if (rc < 0) return rc;
        const int64_t got = io_.read(f, out + done, size_t(take) * kArrivalBytes, (p - t.segStart[seg]) * kArrivalBytes);
        if (got != int64_t(size_t(take) * kArrivalBytes)) return got < 0 ? int32_t(got) : kRsCorrupt;
        done += take;
    }
    return 0;
}

int32_t LaneStore::arrivalAt(const TypeSnap& t, uint64_t pos, ArrivalEntry* out) {
    const int32_t rc = arrivalsRead(t, pos, 1, out);
    if (stats_) stats_->fenceReads++;
    return rc;
}

int32_t LaneStore::arrivalsUpperBound(const TypeSnap& t, uint64_t gseq, uint64_t* pos) {
    const uint64_t total = t.arrivalsTotal();
    if (t.empty || total == 0) {
        *pos = 0;
        return 0;
    }
    // Segment by the fence index: the first sealed segment whose last gseq >
    // gseq, else the active one.
    size_t seg = t.fence->size();
    for (size_t i = 0; i < t.fence->size(); i++)
        if ((*t.fence)[i].lastGseq > gseq) {
            seg = i;
            break;
        }
    uint64_t a = t.segStart[seg];
    uint64_t b = seg < t.fence->size() ? t.segStart[seg + 1] : total;
    while (a < b) {
        const uint64_t mid = (a + b) / 2;
        ArrivalEntry e;
        const int32_t rc = arrivalAt(t, mid, &e);
        if (rc < 0) return rc;
        if (e.gseq <= gseq) a = mid + 1;
        else b = mid;
    }
    *pos = a;
    return 0;
}

// ---------------------------------------------------------------------------
// CID text
// ---------------------------------------------------------------------------
bool cidFromText(const char* s, size_t n, uint8_t cid[kCidLen]) {
    if (n != 1 + (kCidLen * 8 + 4) / 5 || s[0] != 'b') return false;
    std::memset(cid, 0, kCidLen);
    size_t bit = 0;
    for (size_t i = 1; i < n; i++) {
        const char c = s[i];
        uint8_t v;
        if (c >= 'a' && c <= 'z') v = uint8_t(c - 'a');
        else if (c >= '2' && c <= '7') v = uint8_t(26 + c - '2');
        else return false;
        for (int b = 4; b >= 0; b--, bit++) {
            if (bit >= kCidLen * 8) {
                if ((v >> b) & 1) return false;  // non-zero padding bits
                continue;
            }
            if ((v >> b) & 1) cid[bit >> 3] |= uint8_t(1u << (7 - (bit & 7)));
        }
    }
    return true;
}

std::string cidToText(const uint8_t cid[kCidLen]) {
    char buf[2 + kCidLen * 8 / 5 + 2];
    const size_t n = cidText(cid, kCidLen, buf);
    return std::string(buf, n);
}

}  // namespace ps
}  // namespace flatsql
