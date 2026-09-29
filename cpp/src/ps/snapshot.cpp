// FlatSQL partition store: the reader's view of committed state (see
// ps/snapshot.h; design §8, A12, A14, A15, A16, A17, A20).
#include "flatsql/ps/snapshot.h"

#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/reflection.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>

#include "flatsql/ps/lane_arena.h"
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
                case 'G': n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/ga-%06x.fsg", r, hex, k.gen); break;
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
        case 'G':
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

void LaneIo::closeIdle(uint64_t nowNs, uint64_t idleNs, bool idle) {
    if (nowNs - lastSweepNs_ < 1000000000ull) return;  // at most once a second
    lastSweepNs_ = nowNs;
    for (auto it = map_.begin(); it != map_.end();) {
        // Handles of the running statement stay (between statements none runs).
        if ((idle || it->second.lastSeq != stmtSeq_) && it->second.lastNs + idleNs < nowNs) {
            ctx_.close(&it->second.ref);
            lru_.erase(it->second.lru);
            it = map_.erase(it);
        } else {
            ++it;
        }
    }
}

int32_t LaneIo::get(const FileKey& key, FileRef* out) {
    auto it = map_.find(key);
    if (it != map_.end()) {
        it->second.lastSeq = stmtSeq_;
        it->second.lastNs = monoNs();
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
    e.lastNs = monoNs();
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
size_t L0Section::lowerBound(const uint8_t* key, size_t klen) const {
    size_t a = 0, b = offs.size();
    while (a < b) {
        const size_t mid = (a + b) / 2;
        const uint8_t* p = entries.data() + offs[mid];
        if (keyCmp(p + 2, getU16(p), key, klen) < 0) a = mid + 1;
        else b = mid;
    }
    return a;
}

// Kind directory of an L0 block (offsets relative to the block).
struct L0Dir {
    uint32_t blockLen = 0;
    std::vector<L0KindInfo> kinds;
};

struct PostingScan::Source {
    const uint8_t* key = nullptr;
    uint16_t klen = 0;
    const uint8_t* val = nullptr;
    uint8_t vlen = 0;
    bool valid = false;
    // A lazy source holds only a lower bound of its first entry (key = the
    // bound, no value) until it reaches the top of the heap; materialize()
    // then reads its first block. A window over many runs reads the blocks
    // of the runs it actually draws from.
    bool lazy = false;
    virtual ~Source() = default;
    virtual bool advance(LaneStore* st) = 0;
    virtual bool materialize(LaneStore*) { return valid; }
    int32_t err = 0;
};

namespace {


// One kind section of a parsed L0 block, restricted to the range.
struct L0Source : PostingScan::Source {
    std::shared_ptr<const L0Section> sec;
    int64_t idx = -1;
    int64_t first = 0, last = 0;  // entry index range [first, last)
    bool desc = false;
    bool init(std::shared_ptr<const L0Section> sc, const std::string& lo, bool hasLo, const std::string& hi, bool hasHi,
              bool d) {
        sec = std::move(sc);
        desc = d;
        if (!sec || sec->offs.empty()) return false;
        first = hasLo ? int64_t(sec->lowerBound(reinterpret_cast<const uint8_t*>(lo.data()), lo.size())) : 0;
        last = hasHi ? int64_t(sec->lowerBound(reinterpret_cast<const uint8_t*>(hi.data()), hi.size()))
                     : int64_t(sec->offs.size());
        if (first >= last) return false;
        idx = desc ? last : first - 1;
        return true;
    }
    bool advance(LaneStore*) override {
        idx += desc ? -1 : 1;
        if (idx < first || idx >= last) return valid = false;
        const uint8_t* p = sec->entry(size_t(idx));
        klen = getU16(p);
        key = p + 2;
        val = p + 2 + klen;
        vlen = sec->vlen;
        return valid = true;
    }
};

// The first block b in [0, n) for which pred(fence b) is false, the fences
// being partitioned (every true before every false).
template <class Pred>
int32_t fencePartition(FenceView& fv, Pred pred, uint32_t* out) {
    uint32_t a = 0, b = fv.size();
    while (a < b) {
        const uint32_t mid = a + (b - a) / 2;
        int32_t rc = 0;
        const L1Fence* f = fv.at(mid, &rc);
        if (!f) return rc;
        if (pred(*f)) a = mid + 1;
        else b = mid;
    }
    *out = a;
    return 0;
}

// One kind of an L1 run: blocks read on demand (4 KiB each).
struct L1Source : PostingScan::Source {
    std::shared_ptr<LazyRun> run;
    FileKey file;
    FenceView fences;  // read a page at a time
    int64_t block = -1;
    int64_t idx = -1;
    bool desc = false;
    uint8_t v = 0;
    bool done = false;
    // The scan's bounds, shared by its sources: lo = bounds[0, loLen),
    // hi = bounds[loLen, end).
    std::shared_ptr<const std::string> bounds;
    const uint8_t* loP = nullptr;
    const uint8_t* hiP = nullptr;
    size_t loN = 0, hiN = 0;
    bool hasLo = false, hasHi = false;
    int64_t pending = -1;  // lazy: the block materialize() reads first
    L1Fence startFence{};  // lazy: its fence (key points into its prefix)
    std::unique_ptr<uint8_t[]> bufMem;  // one 4 KiB block, allocated on first read
    uint8_t* buf = nullptr;
    std::vector<uint16_t> ents;  // entry offsets within buf (in range)

    // The fence of block b; nullptr with err set when unreadable.
    const L1Fence* fenceAt(int64_t b) {
        int32_t rc = 0;
        const L1Fence* f = fences.at(uint32_t(b), &rc);
        if (!f) err = rc;
        return f;
    }
    bool loadBlock(LaneStore* st, int64_t b) {
        ents.clear();
        if (b < 0 || b >= int64_t(fences.size())) return false;
        block = b;
        if (!buf) {
            bufMem.reset(new uint8_t[kL1BlockBytes]);
            buf = bufMem.get();
        }
        const L1Fence* fe = fenceAt(b);
        if (!fe) return false;
        const int32_t rc = st->readBlock(file, fe->blockOff, buf);
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
            if (hasLo && keyCmp(k, kl, loP, loN) < 0) {
                beforeLo = true;
            } else if (hasHi && keyCmp(k, kl, hiP, hiN) >= 0) {
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
    bool init(LaneStore* st, std::shared_ptr<LazyRun> r, const FileKey& f, uint16_t kind,
              std::shared_ptr<const std::string> bnd, size_t loLen, bool hl, bool hh, bool d, bool allowLazy) {
        run = std::move(r);
        file = f;
        if (!run->fences(st, kind, &fences) || fences.empty()) return false;
        v = run->kindVlen(kind);
        bounds = std::move(bnd);
        loP = reinterpret_cast<const uint8_t*>(bounds->data());
        loN = loLen;
        hiP = loP + loLen;
        hiN = bounds->size() - loLen;
        hasLo = hl;
        hasHi = hh;
        desc = d;
        int64_t start;
        if (!desc) {
            // Last block whose prefix is strictly below lo's prefix.
            start = 0;
            if (hasLo) {
                uint32_t a = 0;
                const int32_t rc =
                    fencePartition(fences, [&](const L1Fence& fe) { return prefixCmp(loP, loN, fe) > 0; }, &a);
                if (rc < 0) {
                    err = rc;
                    return false;
                }
                start = a == 0 ? 0 : int64_t(a) - 1;
            }
        } else {
            // Last block whose prefix is <= hi's prefix.
            start = int64_t(fences.size()) - 1;
            if (hasHi) {
                uint32_t a = 0;
                const int32_t rc =
                    fencePartition(fences, [&](const L1Fence& fe) { return prefixCmp(hiP, hiN, fe) >= 0; }, &a);
                if (rc < 0) {
                    err = rc;
                    return false;
                }
                start = int64_t(a) - 1;
                if (start < 0) return false;
            }
        }
        if (st->stats()) st->stats()->fenceReads++;
        if (allowLazy && !desc) {
            // Ascending: every entry in range is >= lo and >= the first key
            // of block `start`, whose fence prefix is a prefix of it.
            const L1Fence* fp = fenceAt(start);
            if (!fp) return false;
            startFence = *fp;
            const L1Fence& fe = startFence;
            if (hasHi && prefixCmp(hiP, hiN, fe) < 0) return false;
            pending = start;
            lazy = true;
            if (hasLo) {
                key = loP;
                klen = uint16_t(loN);
            } else {
                key = fe.prefix;
                klen = fe.prefixLen;
            }
            val = nullptr;
            vlen = 0;
            return valid = true;
        }
        if (!loadBlock(st, start)) return false;
        idx = desc ? int64_t(ents.size()) : -1;
        return true;
    }
    bool materialize(LaneStore* st) override {
        if (!lazy) return valid;
        lazy = false;
        const int64_t b = pending;
        pending = -1;
        if (!loadBlock(st, b)) return valid = false;
        idx = -1;
        return advance(st);
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
            if (nb < 0 || nb >= int64_t(fences.size())) return valid = false;
            // Past the range by the fence prefix alone?
            if (!desc && hasHi) {
                const L1Fence* fe = fenceAt(nb);
                if (!fe) return valid = false;
                if (prefixCmp(hiP, hiN, *fe) < 0) return valid = false;
            }
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

// Sources merge through a binary heap of indices (a partition can list up
// to 48 unmerged L0 blocks plus its runs).
bool PostingScan::before(int a, int b) const {
    const Source* x = srcs_[size_t(a)].get();
    const Source* y = srcs_[size_t(b)].get();
    int c = keyCmp(x->key, x->klen, y->key, y->klen);
    if (c == 0 && (x->lazy || y->lazy)) {
        // A lazy bound sorts before any entry with the same key (ascending
        // only: descending scans never go lazy).
        if (x->lazy != y->lazy) return x->lazy;
        return a < b;
    }
    if (c == 0) c = std::memcmp(x->val, y->val, std::min(x->vlen, y->vlen));
    if (c == 0) c = a < b ? -1 : 1;
    return desc_ ? c > 0 : c < 0;
}

void PostingScan::siftDown(size_t i) {
    const size_t n = heap_.size();
    for (;;) {
        size_t m = i;
        const size_t l = 2 * i + 1, r = 2 * i + 2;
        if (l < n && before(heap_[l], heap_[m])) m = l;
        if (r < n && before(heap_[r], heap_[m])) m = r;
        if (m == i) return;
        std::swap(heap_[i], heap_[m]);
        i = m;
    }
}

void PostingScan::pick() {
    heap_.clear();
    for (size_t i = 0; i < srcs_.size(); i++)
        if (srcs_[i]->valid) heap_.push_back(int(i));
    for (size_t i = heap_.size() / 2; i-- > 0;) siftDown(i);
    settle();
}

// Materializes lazy sources until the top of the heap holds a real entry.
void PostingScan::settle() {
    while (!heap_.empty()) {
        Source* s = srcs_[size_t(heap_[0])].get();
        if (!s->lazy) break;
        s->materialize(store_);
        if (s->err && !err_) err_ = s->err;
        if (!s->valid) {
            heap_[0] = heap_.back();
            heap_.pop_back();
        }
        if (!heap_.empty()) siftDown(0);
    }
    cur_ = heap_.empty() ? -1 : heap_[0];
}

bool PostingScan::next() {
    if (cur_ < 0) return false;
    Source* s = srcs_[size_t(cur_)].get();
    s->advance(store_);
    if (s->err && !err_) err_ = s->err;
    if (store_ && store_->stats()) store_->stats()->indexEntries++;
    if (!s->valid) {
        heap_[0] = heap_.back();
        heap_.pop_back();
    }
    if (!heap_.empty()) siftDown(0);
    settle();
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
    io_.closeIdle(monoNs(), 5000000000ull);
    guard_ = guard;
}

void LaneStore::endStatement() {
    stats_ = &scratchStats_;
    io_.setStats(stats_);
    guard_ = nullptr;
}

void LaneStore::cachePut(const FileKey& key, std::shared_ptr<const void> obj, uint64_t bytes) {
    // Larger than the whole budget: its reader keeps it for the statement,
    // the next one reads it again (the budget is a bound, never exceeded).
    if (bytes > cfg_.cacheBytes) return;
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

LaneStoreUsage LaneStore::usage() const {
    LaneStoreUsage u;
    u.cacheBytes = cacheUsed_;
    u.runBytes = runUsed_;
    u.frontBytes = frontUsed_;
    u.cacheEntries = cache_.size();
    u.runs = runs_.size();
    u.frontEntries = front_.size();
    u.verifiedRuns = verifiedRuns_.size();
    return u;
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
    // Manifests are cached whole: the handle is not kept (a retired manifest
    // is unlinked with UNLINK_IF_UNUSED, A12).
    io_.forget(key);
    ManifestDesc md;
    if (!decodeManifest(buf.data(), buf.size(), &md)) return kRsCorrupt;
    auto m = std::make_shared<Manifest>();
    m->gen = md.gen;
    m->prevGen = md.prevGen;
    for (const auto& d : md.segs) {
        ManifestSegRef r;
        r.seg = d.seg;
        r.lastSeg = d.lastSeg;
        r.sealed = d.sealed;
        r.empty = d.empty;
        r.cgen = d.cgen;
        r.firstPseq = d.firstPseq;
        r.endPseq = d.endPseq;
        r.mergedEnd = d.mergedEnd;
        r.dLen = d.dLen;
        r.rLen = d.rLen;
        r.aLen = d.aLen;
        r.minEpoch = d.minEpoch;
        r.maxEpoch = d.maxEpoch;
        r.minArrival = d.minArrival;
        r.maxArrival = d.maxArrival;
        r.killThrough = d.killThrough;
        r.prevGen = d.prevGen;
        for (const auto& run : d.runs) {
            SegRunRef rr;
            rr.gen = run.gen;
            rr.nEntries = run.nEntries;
            rr.fileLen = run.fileLen;
            r.runs.push_back(rr);
        }
        m->segs.push_back(std::move(r));
    }
    // Charged its memory (about 200 bytes per segment), not its file size.
    uint64_t mb = sizeof(Manifest) + m->segs.capacity() * sizeof(ManifestSegRef);
    for (const auto& r : m->segs) mb += r.runs.capacity() * sizeof(SegRunRef) + (r.runs.empty() ? 0 : 32);
    cachePut(key, m, mb);
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
    uint32_t seg;
    uint64_t off;
    if (lc.ckptOff == h.labelCkptOff && lc.ckptSeg == h.labelCkptSeg && lc.scannedTo &&
        lc.scannedSeg <= h.mSeg && (lc.scannedSeg < h.mSeg || lc.scannedTo <= h.mEnd) &&
        lc.commitSeq <= h.commitSeq) {
        seg = lc.scannedSeg;  // incremental: only batches after the last scan
        off = lc.scannedTo;
    } else {
        lc = LabelCache();
        lc.ckptOff = h.labelCkptOff;
        lc.ckptSeg = h.labelCkptSeg;
        seg = h.labelCkptSeg;
        off = h.labelCkptOff;
    }
    if (seg > h.mSeg) return kRsCorrupt;
    std::vector<uint8_t> labels;
    std::unordered_map<uint32_t, uint64_t> map = lc.labels;
    // The checkpoint's segment may precede the head's (T3, A9: a rotation
    // before the next checkpoint); a sealed segment ends at its last batch.
    for (;; seg++, off = 0) {
        FileRef f;
        int32_t rc = io_.get(tk('m', fid, seg), &f);
        if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsSnapshotGone : rc;
        uint64_t end = h.mEnd;
        if (seg < h.mSeg) {
            const int64_t sz = io_.size(f);
            if (sz < 0) return int32_t(sz);
            end = uint64_t(sz);
        }
        while (off + sizeof(TypeBatchHeader) <= end) {
            TypeBatchHeader bh;
            if (io_.read(f, &bh, sizeof(bh), off) != int64_t(sizeof(bh))) return FLATSQL_IO_ERR_IO;
            if (bh.magic != kMagicTypeBatch || bh.batchLen < sizeof(bh) + 8 || off + bh.batchLen > end)
                return kRsCorrupt;
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
        if (seg >= h.mSeg) break;
    }
    lc.labels = map;
    lc.scannedSeg = seg;
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
    // Catalog runs (type manifest) and, T3b, the compacted arrivals table.
    struct TypeManifestView {
        std::vector<SegRunRef> runs;
        std::vector<ArrOverride> arr;
    };
    if (h.manifestGen) {
        const FileKey mk = tk('f', fid, 0, h.manifestGen);
        std::shared_ptr<const TypeManifestView> runs;
        if (auto c = cacheGet(mk)) {
            runs = std::static_pointer_cast<const TypeManifestView>(c);
        } else {
            FileRef f;
            rc = io_.get(mk, &f);
            if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
            if (rc < 0) return rc;
            const int64_t size = io_.size(f);
            if (size < 24) return kRsCorrupt;
            std::vector<uint8_t> man(static_cast<size_t>(size));
            if (io_.read(f, man.data(), man.size(), 0) != size) return FLATSQL_IO_ERR_IO;
            io_.forget(mk);  // cached whole; a retired manifest goes with UNLINK_IF_UNUSED (A12)
            if (getU32(man.data()) != kMagicManifest) return kRsCorrupt;
            const uint32_t n = getU32(man.data() + 4);
            const size_t body = 16 + size_t(n) * 16;
            if (body + 8 > man.size() || crc32c(man.data(), body) != getU32(man.data() + body)) return kRsCorrupt;
            auto v = std::make_shared<TypeManifestView>();
            for (uint32_t i = 0; i < n; i++) {
                SegRunRef r;
                r.gen = getU32(man.data() + 16 + size_t(i) * 16);
                r.fileLen = getU64(man.data() + 16 + size_t(i) * 16 + 8);
                v->runs.push_back(r);
            }
            if (!parseTypeArrivalsTable(man.data(), man.size(), body + 8, &v->arr)) return kRsCorrupt;
            cachePut(mk, v,
                     sizeof(*v) + v->runs.capacity() * sizeof(SegRunRef) + v->arr.capacity() * sizeof(ArrOverride) + 64);
            runs = v;
        }
        out->runs = runs->runs;
        out->arr = runs->arr;
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
    if (!out->arr.empty()) {
        // T3b (A15): the rewritten segments' counts and gseq bounds.
        auto eff = std::make_shared<std::vector<ArrivalFence>>(*out->fence);
        out->arrOf.assign(h.gSeg, -1);
        for (size_t i = 0; i < out->arr.size(); i++) {
            const ArrOverride& o = out->arr[i];
            if (o.seg >= h.gSeg) return kRsCorrupt;
            out->arrOf[o.seg] = int32_t(i);
            ArrivalFence& f = (*eff)[o.seg];
            f.count = o.count;
            f.firstGseq = o.firstGseq;
            f.lastGseq = o.lastGseq;
        }
        out->fence = eff;
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
            if (m->cgen) {
                // Compacted (T3): surviving rows are dense by rank; a pseq the
                // directory lacks was removed and reads as VOID (dead).
                const uint64_t end = std::min<uint64_t>(m->mergedEnd, pseq + (n - done));
                if (m->empty) {
                    for (uint64_t ps = pseq; ps < end; ps++) voidRow(&out[done + (ps - pseq)], ps, m->seg);
                    done += uint32_t(end - pseq);
                    pseq = end;
                    continue;
                }
                std::shared_ptr<const CompactDir> dir;
                rc = compactDir(s, *m, &dir);
                if (rc < 0) return rc;
                const uint64_t cnt = dir->countIn(pseq, end);
                std::vector<RecRow> present(static_cast<size_t>(cnt));
                if (cnt) {
                    FileRef rf;
                    rc = io_.get(pk('r', s.pid, m->seg, m->cgen), &rf);
                    if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
                    if (rc < 0) return rc;
                    const uint64_t idx0 = dir->countIn(dir->firstPseq, pseq);
                    const size_t bytes = size_t(cnt) * sizeof(RecRow);
                    const int64_t got = io_.read(rf, present.data(), bytes, CompactDir::rowOffset(idx0));
                    if (got != int64_t(bytes)) return got < 0 ? int32_t(got) : kRsCorrupt;
                }
                size_t k = 0;
                for (uint64_t ps = pseq; ps < end; ps++) {
                    RecRow& r = out[done + (ps - pseq)];
                    if (dir->contains(ps)) {
                        if (k >= present.size() || present[k].pseq != ps) return kRsCorrupt;
                        r = present[k++];
                    } else {
                        voidRow(&r, ps, m->seg);
                    }
                }
                done += uint32_t(end - pseq);
                pseq = end;
                continue;
            }
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

int32_t LaneStore::compactDir(const PartSnap& s, const ManifestSegRef& m, std::shared_ptr<const CompactDir>* out) {
    FileKey ck = pk('C', s.pid, m.seg, m.cgen);  // cache key only (no file)
    if (auto c = cacheGet(ck)) {
        *out = std::static_pointer_cast<const CompactDir>(c);
        return 0;
    }
    const FileKey key = pk('r', s.pid, m.seg, m.cgen);
    FileRef f;
    int32_t rc = io_.get(key, &f);
    if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
    if (rc < 0) return rc;
    uint8_t hb[sizeof(CompactRowsHeader)];
    CompactRowsHeader h;
    if (io_.read(f, hb, sizeof(hb), 0) != int64_t(sizeof(hb)) || !parseCompactHeader(hb, sizeof(hb), &h) ||
        h.firstPseq != m.firstPseq || h.endPseq != m.endPseq)
        return kRsCorrupt;
    std::vector<uint8_t> db(size_t(h.nBlocks) * sizeof(CompactDirBlock));
    if (!db.empty() && io_.read(f, db.data(), db.size(), h.dirOff) != int64_t(db.size())) return FLATSQL_IO_ERR_IO;
    auto dir = std::make_shared<CompactDir>();
    if (!parseCompactDir(h, db.data(), db.size(), dir.get())) return kRsCorrupt;
    cachePut(ck, dir, dir->memoryBytes());
    *out = dir;
    return 0;
}

int32_t LaneStore::resolveForBound(PartSnap* s, uint64_t bound) {
    if (!s->manifest) return 0;
    bool need = false;
    for (const auto& m : s->manifest->segs)
        if (m.killThrough > bound) need = true;
    if (!need) return 0;
    // Replace each such segment by its description in the manifest before
    // the SWAP that set the bound, repeatedly (a segment compacted twice).
    auto merged = std::make_shared<Manifest>(*s->manifest);
    for (int round = 0; round < 64; round++) {
        bool changed = false;
        std::vector<ManifestSegRef> next;
        for (const auto& m : merged->segs) {
            if (m.killThrough <= bound || !m.prevGen) {
                next.push_back(m);
                continue;
            }
            std::shared_ptr<const Manifest> prev;
            const int32_t rc = loadManifest(s->pid, m.prevGen, &prev);
            if (rc < 0) return rc;
            const uint32_t last = m.lastSeg ? m.lastSeg : m.seg;
            size_t added = 0;
            for (const auto& pm : prev->segs) {
                if (pm.seg < m.seg || pm.seg > last) continue;
                next.push_back(pm);
                added++;
            }
            if (!added) return kRsCorrupt;
            changed = true;
        }
        std::sort(next.begin(), next.end(),
                  [](const ManifestSegRef& a, const ManifestSegRef& b) { return a.seg < b.seg; });
        merged->segs.swap(next);
        if (!changed) break;
    }
    s->manifest = merged;
    return 0;
}

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

std::shared_ptr<const void> LaneStore::sharedGet(const FileKey& key) {
    if (!cfg_.shared) return cacheGet(key);
    auto it = front_.find(key);
    if (it != front_.end()) {
        frontLru_.splice(frontLru_.begin(), frontLru_, it->second.lru);
        return it->second.obj;
    }
    uint64_t bytes = 0;
    auto obj = cfg_.shared->get(key, &bytes);
    if (obj) frontAdmit(key, obj, bytes);
    return obj;
}

void LaneStore::sharedPut(const FileKey& key, std::shared_ptr<const void> obj, uint64_t bytes) {
    if (!cfg_.shared) {
        cachePut(key, std::move(obj), bytes);
        return;
    }
    frontAdmit(key, obj, bytes);
    cfg_.shared->put(key, std::move(obj), bytes);
}

// The front keeps, within frontBytes, what this lane used last: evicted
// from the shared cache or not, least recently used first out.
void LaneStore::frontAdmit(const FileKey& key, std::shared_ptr<const void> obj, uint64_t bytes) {
    if (bytes > cfg_.frontBytes) return;
    auto it = front_.find(key);
    if (it != front_.end()) {
        frontUsed_ -= it->second.bytes;
        frontLru_.erase(it->second.lru);
        front_.erase(it);
    }
    while (!frontLru_.empty() && (frontUsed_ + bytes > cfg_.frontBytes || front_.size() >= cfg_.frontEntries)) {
        auto vit = front_.find(frontLru_.back());
        frontUsed_ -= vit->second.bytes;
        front_.erase(vit);
        frontLru_.pop_back();
    }
    frontLru_.push_front(key);
    FrontEntry e;
    e.obj = std::move(obj);
    e.bytes = bytes;
    e.lru = frontLru_.begin();
    front_.emplace(key, std::move(e));
    frontUsed_ += bytes;
}

namespace {
std::shared_ptr<L0Section> extractSection(const uint8_t* block, const L0KindInfo& k) {
    auto sec = std::make_shared<L0Section>();
    sec->vlen = k.vlen;
    sec->entries.assign(block + k.entriesOff, block + k.entriesOff + k.entriesBytes);
    if (k.bloomBytes) sec->bloom.assign(block + k.bloomOff, block + k.bloomOff + k.bloomBytes);
    return sec;
}
bool indexSection(L0Section* sec, uint32_t n) {
    sec->offs.reserve(n);
    EntryIter it;
    it.p = sec->entries.data();
    it.end = it.p + sec->entries.size();
    it.vlen = sec->vlen;
    const uint8_t *k, *v;
    uint16_t kl;
    const uint8_t* at = it.p;
    while (it.next(&k, &kl, &v)) {
        sec->offs.push_back(uint32_t(at - sec->entries.data()));
        at = it.p;
    }
    return sec->offs.size() == n;
}
}  // namespace

std::shared_ptr<const L0Section> LaneStore::section(const FileKey& mfile, uint64_t blockOff, uint32_t maxLen,
                                                    uint16_t kind, int32_t* rc) {
    *rc = 0;
    FileKey sk = mfile;
    sk.letter = mfile.letter == 'm' ? 'S' : 's';
    sk.extra = blockOff;
    sk.gen = kind;
    if (auto c = sharedGet(sk)) {
        auto sec = std::static_pointer_cast<const L0Section>(c);
        return sec->offs.empty() && sec->entries.empty() && sec->vlen == 0xff ? nullptr : sec;
    }
    FileKey dk = mfile;
    dk.letter = mfile.letter == 'm' ? 'D' : 'd';
    dk.extra = blockOff;
    std::shared_ptr<const L0Dir> dir;
    std::shared_ptr<L0Section> sec;
    const L0KindInfo* ki = nullptr;
    if (auto c = sharedGet(dk)) {
        dir = std::static_pointer_cast<const L0Dir>(c);
        for (const auto& k : dir->kinds)
            if (k.kind == kind) ki = &k;
        if (ki) {
            // The directory is known (the block's CRC was checked when it was
            // built): read just this section.
            FileRef f;
            *rc = io_.get(mfile, &f);
            if (*rc < 0) {
                if (*rc == FLATSQL_IO_ERR_NOENT) *rc = kRsSnapshotGone;
                return nullptr;
            }
            sec = std::make_shared<L0Section>();
            sec->vlen = ki->vlen;
            sec->entries.resize(ki->entriesBytes);
            if (ki->entriesBytes &&
                io_.read(f, sec->entries.data(), ki->entriesBytes, blockOff + ki->entriesOff) != int64_t(ki->entriesBytes)) {
                *rc = FLATSQL_IO_ERR_IO;
                return nullptr;
            }
            if (ki->bloomBytes) {
                sec->bloom.resize(ki->bloomBytes);
                if (io_.read(f, sec->bloom.data(), ki->bloomBytes, blockOff + ki->bloomOff) != int64_t(ki->bloomBytes)) {
                    *rc = FLATSQL_IO_ERR_IO;
                    return nullptr;
                }
            }
        }
    } else {
        // First touch: the whole block, CRC-checked; the directory is kept.
        FileRef f;
        *rc = io_.get(mfile, &f);
        if (*rc < 0) {
            if (*rc == FLATSQL_IO_ERR_NOENT) *rc = kRsSnapshotGone;
            return nullptr;
        }
        L0Header lh;
        if (io_.read(f, &lh, sizeof(lh), blockOff) != int64_t(sizeof(lh))) {
            *rc = FLATSQL_IO_ERR_IO;
            return nullptr;
        }
        if (lh.magic != kMagicL0 || lh.blockLen > maxLen || lh.blockLen < sizeof(L0Header) + 8) {
            *rc = kRsCorrupt;
            return nullptr;
        }
        std::vector<uint8_t> block(lh.blockLen);
        if (io_.read(f, block.data(), block.size(), blockOff) != int64_t(block.size())) {
            *rc = FLATSQL_IO_ERR_IO;
            return nullptr;
        }
        L0KindInfo kinds[64];
        size_t nk = 0;
        if (!parseL0Block(block.data(), block.size(), kinds, 64, &nk)) {
            *rc = kRsCorrupt;
            return nullptr;
        }
        auto d = std::make_shared<L0Dir>();
        d->blockLen = lh.blockLen;
        d->kinds.assign(kinds, kinds + nk);
        sharedPut(dk, d, 64 + nk * sizeof(L0KindInfo));
        for (const auto& k : d->kinds)
            if (k.kind == kind) ki = &k;
        if (ki) sec = extractSection(block.data(), *ki);
        dir = d;
    }
    if (!ki) {
        // Absent kind: cache a marker so the next statement does not look.
        auto none = std::make_shared<L0Section>();
        none->vlen = 0xff;
        sharedPut(sk, none, 96);
        return nullptr;
    }
    if (!indexSection(sec.get(), ki->n)) {
        *rc = kRsCorrupt;
        return nullptr;
    }
    sharedPut(sk, sec, sec->bytes());
    return sec;
}

std::shared_ptr<const L0Section> LaneStore::l0Section(const PartSnap& s, const L0DirEntry& e, uint16_t kind,
                                                      int32_t* rc) {
    return section(pk('m', s.pid, e.mSeg), e.mOff + e.l0Off, e.batchLen - e.l0Off, kind, rc);
}

std::shared_ptr<const L0Section> LaneStore::typeL0Section(const TypeSnap& t, const TypeL0DirEntry& e, uint16_t kind,
                                                          int32_t* rc) {
    return section(tk('m', t.fid, e.mSeg), e.mOff + e.l0Off, e.l0Len, kind, rc);
}

const SectionList* LaneStore::sections(const PartSnap& s, uint16_t kind, int32_t* rc) {
    *rc = 0;
    auto it = s.secs.find(kind);
    if (it != s.secs.end()) return &it->second;
    SectionList list;
    list.reserve(s.l0.size());
    for (const L0DirEntry& e : s.l0) {
        list.push_back(l0Section(s, e, kind, rc));
        if (*rc < 0) return nullptr;
    }
    return &s.secs.emplace(kind, std::move(list)).first->second;
}

const SectionList* LaneStore::sections(const TypeSnap& t, uint16_t kind, int32_t* rc) {
    *rc = 0;
    auto it = t.secs.find(kind);
    if (it != t.secs.end()) return &it->second;
    SectionList list;
    list.reserve(t.l0.size());
    for (const TypeL0DirEntry& e : t.l0) {
        list.push_back(typeL0Section(t, e, kind, rc));
        if (*rc < 0) return nullptr;
    }
    return &t.secs.emplace(kind, std::move(list)).first->second;
}

std::shared_ptr<LazyRun> LaneStore::runGet(const FileKey& key, uint64_t fileLen, int32_t* rc) {
    *rc = 0;
    auto it = runs_.find(key);
    if (it != runs_.end()) {
        runLru_.splice(runLru_.begin(), runLru_, it->second.lru);
        return it->second.run;
    }
    auto r = std::make_shared<LazyRun>();
    const bool verified = verifiedRuns_.count(key) != 0;
    *rc = r->open(this, key, fileLen, verified);
    if (*rc < 0) return nullptr;
    reserveRunBytes(r->bytes());
    runLru_.push_front(key);
    runs_.emplace(key, RunEntry{r, runLru_.begin()});
    runUsed_ += r->bytes();
    return r;
}

void LaneStore::noteVerified(const FileKey& key) {
    auto it = verifiedRuns_.find(key);
    if (it != verifiedRuns_.end()) {
        verifiedOrder_.splice(verifiedOrder_.begin(), verifiedOrder_, it->second);
        return;
    }
    verifiedOrder_.push_front(key);
    verifiedRuns_.emplace(key, verifiedOrder_.begin());
    while (verifiedOrder_.size() > cfg_.verifiedRuns) {
        verifiedRuns_.erase(verifiedOrder_.back());
        verifiedOrder_.pop_back();
    }
}

void LaneStore::chargeRun(LazyRun* run, uint64_t bytes) {
    run->bytes_ += bytes;
    auto it = runs_.find(run->key());
    if (it == runs_.end() || it->second.run.get() != run) return;  // not resident (its user holds it)
    runUsed_ += bytes;
    reserveRunBytes(0);
}

int32_t LaneStore::readFences(const FileKey& run, uint64_t off, uint32_t n, std::vector<L1Fence>* out) {
    FileRef f;
    const int32_t rc = io_.get(run, &f);
    if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsSnapshotGone : rc;
    out->resize(n);
    const size_t bytes = size_t(n) * sizeof(L1Fence);
    if (bytes && io_.read(f, out->data(), bytes, off) != int64_t(bytes)) return FLATSQL_IO_ERR_IO;
    if (stats_) stats_->fenceReads++;
    return 0;
}

bool LaneStore::reserveRunBytes(uint64_t bytes) {
    const uint64_t budget = cfg_.cacheBytes;
    // Evict least recently used runs (never the most recent: it is in use).
    while (runUsed_ + bytes > budget && runLru_.size() > 1) {
        auto vit = runs_.find(runLru_.back());
        const uint64_t b = vit->second.run->bytes();
        runUsed_ = runUsed_ >= b ? runUsed_ - b : 0;
        runs_.erase(vit);
        noteVerified(runLru_.back());  // reopened soon: no second CRC pass
        runLru_.pop_back();
    }
    return runUsed_ + bytes <= budget;
}

std::shared_ptr<LazyRun> LaneStore::run(uint32_t pid, uint32_t seg, uint32_t gen, uint64_t fileLen, FileKey* keyOut,
                                        int32_t* rc) {
    const FileKey key = pk('x', pid, seg, gen);
    *keyOut = key;
    return runGet(key, fileLen, rc);
}

std::shared_ptr<LazyRun> LaneStore::typeRun(const uint8_t fid[4], uint32_t gen, uint64_t fileLen, FileKey* keyOut,
                                            int32_t* rc) {
    const FileKey key = tk('x', fid, 0, gen);
    *keyOut = key;
    return runGet(key, fileLen, rc);
}

// ---------------------------------------------------------------------------
// LazyRun
// ---------------------------------------------------------------------------
LazyRun::Kind* LazyRun::find(uint16_t kind) {
    for (auto& k : kinds_)
        if (k.toc.kind == kind) return &k;
    return nullptr;
}
const LazyRun::Kind* LazyRun::find(uint16_t kind) const {
    for (const auto& k : kinds_)
        if (k.toc.kind == kind) return &k;
    return nullptr;
}
uint8_t LazyRun::kindVlen(uint16_t kind) const {
    const Kind* k = find(kind);
    return k ? k->toc.vlen : valueLenOf(kind);
}
uint64_t LazyRun::kindEntries(uint16_t kind) const {
    const Kind* k = find(kind);
    return k ? k->toc.nEntries : 0;
}

int32_t LazyRun::open(LaneStore* st, const FileKey& key, uint64_t fileLen, bool verified) {
    key_ = key;
    if (fileLen < sizeof(L1Header) + sizeof(L1Footer)) return kRsCorrupt;
    FileRef f;
    int32_t rc = st->io().get(key, &f);
    if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsSnapshotGone : rc;
    L1Footer ft;
    if (st->io().read(f, &ft, sizeof(ft), fileLen - sizeof(ft)) != int64_t(sizeof(ft))) return FLATSQL_IO_ERR_IO;
    if (ft.magic != kMagicL1Footer || ft.crc != crc32c(&ft, 28)) return kRsCorrupt;
    if (ft.metaOff > ft.tocOff || ft.tocOff + ft.tocLen + sizeof(ft) != fileLen) return kRsCorrupt;
    if (!verified) {
        // The footer's CRC covers fences, blooms and TOC together: check the
        // region once per lane, streaming (nothing retained).
        uint32_t crc = 0;
        std::vector<uint8_t> buf(1u << 20);
        uint64_t off = ft.metaOff;
        const uint64_t end = ft.tocOff + ft.tocLen;
        while (off < end) {
            const size_t n = size_t(std::min<uint64_t>(buf.size(), end - off));
            if (st->io().read(f, buf.data(), n, off) != int64_t(n)) return FLATSQL_IO_ERR_IO;
            crc = crc32c(crc, buf.data(), n);
            off += n;
        }
        if (crc != ft.tocCrc) return kRsCorrupt;
        if (st->stats()) st->stats()->fenceReads++;
    }
    const size_t nk = ft.tocLen / sizeof(L1TocEntry);
    std::vector<uint8_t> toc(ft.tocLen);
    if (ft.tocLen && st->io().read(f, toc.data(), toc.size(), ft.tocOff) != int64_t(toc.size())) return FLATSQL_IO_ERR_IO;
    kinds_.resize(nk);
    for (size_t i = 0; i < nk; i++) std::memcpy(&kinds_[i].toc, toc.data() + i * sizeof(L1TocEntry), sizeof(L1TocEntry));
    bytes_ = 256 + nk * sizeof(Kind);
    return 0;
}

bool LazyRun::fences(LaneStore* st, uint16_t kind, FenceView* out) {
    const Kind* k = find(kind);
    if (!k) return false;
    *out = FenceView(st, key_, kind, k->toc.fenceOff, k->toc.nBlocks);
    return true;
}

FenceView::FenceView(LaneStore* st, const FileKey& run, uint16_t kind, uint64_t fenceOff, uint32_t n)
    : st_(st), run_(run), kind_(kind), off_(fenceOff), n_(n),
      page_n_(std::max<uint32_t>(1, st->config().fencePage)) {}

const L1Fence* FenceView::at(uint32_t i, int32_t* rc) {
    const uint32_t pageNo = i / page_n_;
    if (pageNo != pageNo_ || !page_) {
        // Page key: the run's key, letter F, extra = kind | page << 16.
        FileKey pk = run_;
        pk.letter = 'F';
        pk.extra = uint64_t(kind_) | (uint64_t(pageNo) << 16);
        if (auto c = st_->sharedGet(pk)) {
            page_ = std::static_pointer_cast<const std::vector<L1Fence>>(c);
        } else {
            auto v = std::make_shared<std::vector<L1Fence>>();
            const uint32_t first = pageNo * page_n_;
            const uint32_t n = std::min(page_n_, n_ - first);
            *rc = st_->readFences(run_, off_ + uint64_t(first) * sizeof(L1Fence), n, v.get());
            if (*rc < 0) {
                page_.reset();
                pageNo_ = UINT32_MAX;
                return nullptr;
            }
            st_->sharedPut(pk, v, uint64_t(n) * sizeof(L1Fence) + 64);
            page_ = std::move(v);
        }
        pageNo_ = pageNo;
    }
    return &(*page_)[i - pageNo * page_n_];
}

int32_t LazyRun::lookup(LaneStore* st, uint16_t kind, const uint8_t* key, size_t klen, uint64_t hash,
                        const std::function<bool(const uint8_t*)>& visit, bool* stop) {
    *stop = false;
    Kind* k = find(kind);
    if (!k || k->toc.nBlocks == 0) return 0;
    // Bloom: shared, resident while it fits (an eighth of the shared cache).
    if (k->bloomState == 0 && k->toc.bloomBytes) {
        k->bloomState = 2;
        FileKey bk = key_;
        bk.letter = key_.letter == 'x' ? 'B' : 'b';
        bk.extra = kind;
        if (auto c = st->sharedGet(bk)) {
            k->bloom = std::static_pointer_cast<const std::vector<uint8_t>>(c);
            k->bloomState = 1;
        } else if (k->toc.bloomBytes <= st->sharedBudget() / 8) {
            FileRef f;
            int32_t rc = st->io().get(key_, &f);
            if (rc < 0) return rc == FLATSQL_IO_ERR_NOENT ? kRsSnapshotGone : rc;
            auto b = std::make_shared<std::vector<uint8_t>>(k->toc.bloomBytes);
            if (st->io().read(f, b->data(), b->size(), k->toc.bloomOff) != int64_t(b->size())) return FLATSQL_IO_ERR_IO;
            st->sharedPut(bk, b, b->size() + 64);
            k->bloom = b;
            k->bloomState = 1;
        }
        // The run holds its bloom while resident: charged to the lane's runs.
        if (k->bloomState == 1) st->chargeRun(this, k->bloom->size() + 64);
    }
    if (k->bloomState == 1 && !bloomTestHash(k->bloom->data(), k->bloom->size(), hash)) return 0;
    FenceView f(st, key_, kind, k->toc.fenceOff, k->toc.nBlocks);
    // Last block whose first-key prefix is strictly below the key's.
    uint32_t a = 0;
    int32_t rc = fencePartition(f, [&](const L1Fence& fe) { return prefixCmp(key, klen, fe) > 0; }, &a);
    if (rc < 0) return rc;
    uint8_t buf[kL1BlockBytes];
    for (uint32_t blk = a == 0 ? 0 : a - 1; blk < f.size(); blk++) {
        const L1Fence* fe = f.at(blk, &rc);
        if (!fe) return rc;
        if (blk > 0 && prefixCmp(key, klen, *fe) < 0) break;
        rc = st->readBlock(key_, fe->blockOff, buf);
        if (rc < 0) return rc;
        EntryIter it = l1BlockIter(buf, k->toc.vlen);
        const uint8_t *ek, *ev;
        uint16_t el;
        bool past = false;
        while (it.next(&ek, &el, &ev)) {
            const int c = keyCmp(ek, el, key, klen);
            if (c < 0) continue;
            if (c > 0) {
                past = true;
                break;
            }
            if (!visit(ev)) {
                *stop = true;
                return 0;
            }
        }
        if (past) break;
    }
    return 0;
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
    const auto bounds = std::make_shared<const std::string>(los + his);
    {
        int32_t rc = 0;
        const SectionList* list = sections(s, kind, &rc);
        if (!list) {
            ps.err_ = rc;
            return ps;
        }
        for (const auto& sec : *list) {
        if (!sec) continue;
        std::unique_ptr<L0Source> src(new L0Source());
        if (!src->init(sec, los, lo != nullptr, his, hi != nullptr, desc)) continue;
        src->advance(this);
        ps.srcs_.push_back(std::move(src));
        }
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
                std::unique_ptr<L1Source> src(new L1Source);
                if (!src->init(this, run, f, kind, bounds, los.size(), lo != nullptr, hi != nullptr, desc, true)) {
                    if (src->err) {
                        ps.err_ = src->err;
                        return ps;
                    }
                    continue;
                }
                if (!src->lazy) src->advance(this);
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
    const auto bounds = std::make_shared<const std::string>(los + his);
    {
        int32_t rc = 0;
        const SectionList* list = sections(t, kind, &rc);
        if (!list) {
            ps.err_ = rc;
            return ps;
        }
        for (const auto& sec : *list) {
            if (!sec) continue;
            std::unique_ptr<L0Source> src(new L0Source());
            if (!src->init(sec, los, lo != nullptr, his, hi != nullptr, desc)) continue;
            src->advance(this);
            ps.srcs_.push_back(std::move(src));
        }
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
        std::unique_ptr<L1Source> src(new L1Source);
        if (!src->init(this, run, f, kind, bounds, los.size(), lo != nullptr, hi != nullptr, desc, true)) {
            if (src->err) {
                ps.err_ = src->err;
                return ps;
            }
            continue;
        }
        if (!src->lazy) src->advance(this);
        ps.srcs_.push_back(std::move(src));
    }
    ps.pick();
    return ps;
}

namespace {
// Exact-key lookup in one L0 section (entries sorted by (key, value)).
template <typename F>
bool l0Lookup(const L0Section& b, const uint8_t* key, size_t klen, uint64_t h, F&& visit) {
    if (b.offs.empty()) return true;
    if (!b.bloom.empty() && !bloomTestHash(b.bloom.data(), b.bloom.size(), h)) return true;
    for (size_t i = b.lowerBound(key, klen); i < b.offs.size(); i++) {
        const uint8_t* p = b.entry(i);
        const uint16_t kl = getU16(p);
        if (keyCmp(p + 2, kl, key, klen) != 0) break;
        if (!visit(p + 2 + kl)) return false;
    }
    return true;
}
}  // namespace

int32_t LaneStore::lookup(const PartSnap& s, uint16_t kind, const uint8_t* key, size_t klen,
                          const std::function<bool(const uint8_t*)>& visit) {
    if (s.empty) return 0;
    const uint64_t h = bloomHash(key, klen);
    {
        int32_t rc = 0;
        const SectionList* list = sections(s, kind, &rc);
        if (!list) return rc;
        for (const auto& sec : *list)
            if (sec && !l0Lookup(*sec, key, klen, h, visit)) return 0;
    }
    if (!s.manifest) return 0;
    for (const ManifestSegRef& m : s.manifest->segs) {
        for (const SegRunRef& rr : m.runs) {
            FileKey fk;
            int32_t rc = 0;
            auto run = this->run(s.pid, m.seg, rr.gen, rr.fileLen, &fk, &rc);
            if (!run) return rc;
            bool stop = false;
            rc = run->lookup(this, kind, key, klen, h, visit, &stop);
            if (rc < 0) return rc;
            if (stop) return 0;
        }
    }
    return 0;
}

int32_t LaneStore::lookupType(const TypeSnap& t, uint16_t kind, const uint8_t* key, size_t klen,
                              const std::function<bool(const uint8_t*)>& visit) {
    if (t.empty) return 0;
    const uint64_t h = bloomHash(key, klen);
    {
        int32_t rc = 0;
        const SectionList* list = sections(t, kind, &rc);
        if (!list) return rc;
        for (const auto& sec : *list)
            if (sec && !l0Lookup(*sec, key, klen, h, visit)) return 0;
    }
    for (const SegRunRef& rr : t.runs) {
        FileKey fk;
        int32_t rc = 0;
        auto run = typeRun(t.fid, rr.gen, rr.fileLen, &fk, &rc);
        if (!run) return rc;
        bool stop = false;
        rc = run->lookup(this, kind, key, klen, h, visit, &stop);
        if (rc < 0) return rc;
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
        // Equal tcs (one type batch labeled the copy REPEAT, then promoted
        // it): FIRST is the later state.
        if (!*found || tcs > bestTcs || (tcs == bestTcs && v[16] == kLblFirst)) {
            bestTcs = tcs;
            *gseq = getBE64(v + 8);
            *label = v[16];
            *found = true;
        }
        return true;
    });
    return rc;
}

int32_t LaneStore::everRepeat(const TypeSnap& t, uint32_t pid, uint64_t pseq, bool* repeat) {
    *repeat = false;
    uint8_t k[12];
    putBE32(k, pid);
    putBE64(k + 4, pseq);
    return lookupType(t, kIxTypeRepeat, k, 12, [&](const uint8_t*) {
        *repeat = true;
        return false;
    });
}

int32_t LaneStore::rehomeOf(const TypeSnap& t, uint64_t gseq, std::vector<std::pair<uint32_t, uint64_t>>* cands) {
    cands->clear();
    uint8_t k[8];
    putBE64(k, gseq);
    uint64_t bestTcs = 0;
    // Every REHOME of the latest type commit that promoted this gseq: one
    // batch can promote, kill and promote again; its entries share a tcs and
    // at most one of them is still live (the caller checks).
    return lookupType(t, kIxTypeRehome, k, 8, [&](const uint8_t* v) {
        const uint64_t tcs = getBE64(v);
        if (cands->empty() || tcs > bestTcs) {
            bestTcs = tcs;
            cands->clear();
        }
        if (tcs == bestTcs) cands->push_back({getBE32(v + 8), getBE64(v + 12)});
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
                // Equal tcs: the label that can only come later wins (the
                // writer's rule, type_owner.cpp upsertCopy).
                auto rank = [](uint8_t l) { return l == kLblDead ? 9 : int(l); };
                if (c.tcs > e.tcs || (c.tcs == e.tcs && rank(c.label) >= rank(e.label))) e = c;
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
        const ArrOverride* o =
            (!t.arrOf.empty() && seg < t.arrOf.size() && t.arrOf[seg] >= 0) ? &t.arr[size_t(t.arrOf[seg])] : nullptr;
        int32_t rc = io_.get(o ? tk('G', t.fid, 0, o->gen) : tk('g', t.fid, uint32_t(seg)), &f);
        if (rc == FLATSQL_IO_ERR_NOENT) return kRsSnapshotGone;
        if (rc < 0) return rc;
        const int64_t got = io_.read(f, out + done, size_t(take) * kArrivalBytes,
                                     (o ? o->off : 0) + (p - t.segStart[seg]) * kArrivalBytes);
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

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// GONE counting (offset paging, dead-history skipping)
// ---------------------------------------------------------------------------
int32_t LaneStore::isGone(const TypeSnap& t, uint64_t gseq, bool* gone) {
    *gone = false;
    uint8_t k[8];
    putBE64(k, gseq);
    return lookupType(t, kIxTypeGone, k, 8, [&](const uint8_t*) {
        *gone = true;
        return false;
    });
}

int32_t LaneStore::goneCount(const TypeSnap& t, uint64_t lo, uint64_t hi, uint64_t* count) {
    *count = 0;
    if (t.empty || lo >= hi) return 0;
    uint8_t klo[8], khi[8];
    putBE64(klo, lo);
    putBE64(khi, hi);
    // Unmerged type L0 blocks: scan their (small) GONE sections.
    {
        int32_t rc = 0;
        const SectionList* list = sections(t, kIxTypeGone, &rc);
        if (!list) return rc;
        for (const auto& sec : *list)
            if (sec) *count += sec->lowerBound(khi, 8) - sec->lowerBound(klo, 8);
    }
    // L1 runs: fences give whole-block counts; the two edge blocks are read.
    uint8_t buf[kL1BlockBytes];
    for (const SegRunRef& rr : t.runs) {
        FileKey fk;
        int32_t rc = 0;
        auto run = typeRun(t.fid, rr.gen, rr.fileLen, &fk, &rc);
        if (!run) return rc;
        FenceView f;
        if (!run->fences(this, kIxTypeGone, &f) || f.empty()) continue;
        // Block b holds keys in [prefix(b), prefix(b+1)); GONE keys are 8
        // bytes, so a fence prefix is the block's exact first key.
        auto blockFor = [&](const uint8_t* key, uint32_t* out) -> int32_t {  // last block with first key <= key
            uint32_t a = 0;
            const int32_t r2 = fencePartition(f, [&](const L1Fence& fe) { return prefixCmp(key, 8, fe) >= 0; }, &a);
            if (r2 < 0) return r2;
            *out = a == 0 ? 0 : a - 1;
            return 0;
        };
        uint32_t ba = 0, bb = 0;
        rc = blockFor(klo, &ba);
        if (rc >= 0) rc = blockFor(khi, &bb);
        if (rc < 0) return rc;
        auto countBlock = [&](uint32_t b) -> int32_t {
            int32_t r2 = 0;
            const L1Fence* fe = f.at(b, &r2);
            if (!fe) return r2;
            r2 = readBlock(fk, fe->blockOff, buf);
            if (r2 < 0) return r2;
            if (stats_) stats_->fenceReads++;
            EntryIter it = l1BlockIter(buf, 8);
            const uint8_t *k, *v;
            uint16_t kl;
            while (it.next(&k, &kl, &v))
                if (keyCmp(k, kl, klo, 8) >= 0 && keyCmp(k, kl, khi, 8) < 0) (*count)++;
            return 0;
        };
        rc = countBlock(ba);
        if (rc < 0) return rc;
        if (bb != ba) {
            for (uint32_t b = ba + 1; b < bb; b++) {
                const L1Fence* fe = f.at(b, &rc);
                if (!fe) return rc;
                *count += fe->n;
            }
            rc = countBlock(bb);
            if (rc < 0) return rc;
        }
    }
    return 0;
}

int32_t LaneStore::liveBetween(const TypeSnap& t, uint64_t a, uint64_t b, uint64_t* live) {
    *live = 0;
    if (b <= a) return 0;
    ArrivalEntry ea, eb;
    int32_t rc = arrivalAt(t, a, &ea);
    if (rc < 0) return rc;
    uint64_t hiG = UINT64_MAX;
    if (b < t.arrivalsTotal()) {
        rc = arrivalAt(t, b, &eb);
        if (rc < 0) return rc;
        hiG = eb.gseq;
    }
    uint64_t gone = 0;
    rc = goneCount(t, ea.gseq, hiG, &gone);
    if (rc < 0) return rc;
    *live = (b - a) >= gone ? (b - a) - gone : 0;
    return 0;
}

int32_t LaneStore::seekLive(const TypeSnap& t, uint64_t begin, uint64_t end, uint64_t offset, bool desc,
                            uint64_t* pos) {
    if (offset == 0) {
        *pos = desc ? end : begin;
        return 0;
    }
    uint64_t lo = begin, hi = end;
    if (!desc) {
        // Smallest p in [begin, end] with liveBetween(begin, p) >= offset.
        while (lo < hi) {
            const uint64_t mid = lo + (hi - lo) / 2;
            uint64_t live = 0;
            const int32_t rc = liveBetween(t, begin, mid, &live);
            if (rc < 0) return rc;
            if (live >= offset) hi = mid;
            else lo = mid + 1;
        }
        *pos = lo;
    } else {
        // Largest p in [begin, end] with liveBetween(p, end) >= offset.
        while (lo < hi) {
            const uint64_t mid = lo + (hi - lo + 1) / 2;
            uint64_t live = 0;
            const int32_t rc = liveBetween(t, mid, end, &live);
            if (rc < 0) return rc;
            if (live >= offset) lo = mid;
            else hi = mid - 1;
        }
        *pos = lo;
    }
    return 0;
}

}  // namespace ps
}  // namespace flatsql

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// ReaderCache
// ---------------------------------------------------------------------------
ReaderCache::ReaderCache(uint64_t bytes, uint32_t shards)
    : shards_(new Shard[shards ? shards : 1]), n_(shards ? shards : 1), cap_(bytes) {}

uint32_t ReaderCache::shardsFor(uint64_t bytes) {
    return uint32_t(std::max<uint64_t>(1, std::min<uint64_t>(64, bytes >> 20)));
}

ReaderCache::Shard& ReaderCache::shardOf(const FileKey& key) { return shards_[FileKeyHash()(key) % n_]; }

// Lanes never wait on the shared cache: a shard another lane holds is a miss
// (get) or a skipped insert (put); the caller reads the file instead.
bool ReaderCache::lock(Shard& s) {
    const bool got = s.mu.try_lock();
    recordLaneLockWait(0, !got);
    return got;
}

std::shared_ptr<const void> ReaderCache::get(const FileKey& key, uint64_t* bytes) {
    Shard& s = shardOf(key);
    if (!lock(s)) return nullptr;
    std::shared_ptr<const void> out;
    auto it = s.map.find(key);
    if (it != s.map.end()) {
        s.lru.splice(s.lru.begin(), s.lru, it->second.lru);
        out = it->second.obj;
        if (bytes) *bytes = it->second.bytes;
    }
    s.mu.unlock();
    return out;
}

void ReaderCache::put(const FileKey& key, std::shared_ptr<const void> obj, uint64_t bytes) {
    Shard& s = shardOf(key);
    const uint64_t cap = cap_ / n_;
    if (bytes > cap) return;  // never over budget: its reader keeps it for the statement
    std::vector<std::shared_ptr<const void>> evicted;  // released outside the lock
    if (!lock(s)) return;
    if (!s.map.count(key)) {
        while (s.used + bytes > cap && !s.lru.empty()) {
            auto vit = s.map.find(s.lru.back());
            s.used -= vit->second.bytes;
            evicted.push_back(std::move(vit->second.obj));
            s.map.erase(vit);
            s.lru.pop_back();
        }
        s.lru.push_front(key);
        Entry e;
        e.obj = std::move(obj);
        e.bytes = bytes;
        e.lru = s.lru.begin();
        s.map.emplace(key, std::move(e));
        s.used += bytes;
    }
    s.mu.unlock();
}

uint64_t ReaderCache::used() const {
    uint64_t u = 0;
    for (uint32_t i = 0; i < n_; i++) {
        std::lock_guard<std::mutex> g(shards_[i].mu);
        u += shards_[i].used;
    }
    return u;
}

}  // namespace ps
}  // namespace flatsql
