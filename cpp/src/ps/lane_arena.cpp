// FlatSQL partition store: per-lane arenas, the SQLite allocator dispatch,
// instrumented SQLite mutexes and the null VFS (see ps/lane_arena.h).
#include "flatsql/ps/lane_arena.h"

#include <sqlite3.h>

#include <cstdlib>
#include <cstring>
#include <mutex>

#include "flatsql/ps/platform.h"

#if !defined(__wasm__)
#include <sys/mman.h>
#endif

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// TLSF arena
// ---------------------------------------------------------------------------
// Block header (16 bytes): prevSize (valid when PREV_FREE), size | flags.
// Free blocks keep their list links in the first 16 payload bytes. A zero-size
// used sentinel closes the region, so coalescing never walks off the end.
struct LaneArena::Block {
    uint64_t prevSize;
    uint64_t sizeFlags;
    Block* nextFree;
    Block* prevFree;
};

namespace {
constexpr uint64_t kFree = 1;
constexpr uint64_t kPrevFree = 2;
constexpr uint64_t kFlagMask = 15;
constexpr size_t kHdr = 16;
constexpr size_t kMinBlock = 32;

inline size_t align16(size_t n) { return (n + 15) & ~size_t(15); }
inline int log2floor(uint64_t v) { return 63 - __builtin_clzll(v); }

inline uint64_t bsize(const void* b) { return reinterpret_cast<const uint64_t*>(b)[1] & ~kFlagMask; }
}  // namespace

LaneArena::~LaneArena() {
    if (!base_) return;
#if !defined(__wasm__)
    munmap(reinterpret_cast<void*>(base_), mapped_);
#else
    std::free(reinterpret_cast<void*>(base_));
#endif
}

bool LaneArena::init(size_t bytes) {
    if (base_) return false;
    size_t cap = (bytes + 0xffff) & ~size_t(0xffff);
    if (cap < 0x10000) cap = 0x10000;
#if !defined(__wasm__)
    void* p = mmap(nullptr, cap, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (p == MAP_FAILED) return false;
#else
    void* p = std::aligned_alloc(64, cap);
    if (!p) return false;
#endif
    mapped_ = cap;
    base_ = reinterpret_cast<uintptr_t>(p);
    end_ = base_ + cap;
    cap_ = cap;
    used_ = 0;
    high_.store(0);
    flBitmap_ = 0;
    std::memset(slBitmap_, 0, sizeof(slBitmap_));
    std::memset(heads_, 0, sizeof(heads_));
    Block* first = reinterpret_cast<Block*>(base_);
    first->prevSize = 0;
    first->sizeFlags = (cap - kHdr) | kFree;
    Block* sentinel = reinterpret_cast<Block*>(end_ - kHdr);
    sentinel->prevSize = cap - kHdr;
    sentinel->sizeFlags = 0 | kPrevFree;
    insertFree(first);
    return true;
}

void LaneArena::mapping(size_t size, int* fl, int* sl) {
    if (size < (size_t(1) << kFlShift)) {
        *fl = 0;
        *sl = int(size >> 4) & (kSlCount - 1);
        return;
    }
    const int f = log2floor(size);
    *sl = int((size >> (f - kSlLog2)) ^ (size_t(1) << kSlLog2));
    *fl = f - (kFlShift - 1);
}

void LaneArena::insertFree(Block* b) {
    int fl, sl;
    mapping(bsize(b), &fl, &sl);
    b->prevFree = nullptr;
    b->nextFree = heads_[fl][sl];
    if (b->nextFree) b->nextFree->prevFree = b;
    heads_[fl][sl] = b;
    flBitmap_ |= uint64_t(1) << fl;
    slBitmap_[fl] |= 1u << sl;
}

void LaneArena::removeFree(Block* b) {
    int fl, sl;
    mapping(bsize(b), &fl, &sl);
    if (b->prevFree) b->prevFree->nextFree = b->nextFree;
    else heads_[fl][sl] = b->nextFree;
    if (b->nextFree) b->nextFree->prevFree = b->prevFree;
    if (!heads_[fl][sl]) {
        slBitmap_[fl] &= ~(1u << sl);
        if (!slBitmap_[fl]) flBitmap_ &= ~(uint64_t(1) << fl);
    }
}

LaneArena::Block* LaneArena::findFree(size_t size) {
    size_t s = size;
    if (s >= (size_t(1) << kFlShift)) s += (size_t(1) << (log2floor(s) - kSlLog2)) - 1;
    int fl, sl;
    mapping(s, &fl, &sl);
    if (fl >= kFlCount) return nullptr;
    uint32_t slMap = slBitmap_[fl] & (~0u << sl);
    if (!slMap) {
        const uint64_t flMap = fl + 1 < 64 ? (flBitmap_ & (~uint64_t(0) << (fl + 1))) : 0;
        if (!flMap) return nullptr;
        fl = __builtin_ctzll(flMap);
        slMap = slBitmap_[fl];
    }
    sl = __builtin_ctz(slMap);
    return heads_[fl][sl];
}

void* LaneArena::alloc(size_t n) {
    if (!base_ || n > (size_t(1) << 38)) {
        failures_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    size_t size = align16(n ? n : 1) + kHdr;
    if (size < kMinBlock) size = kMinBlock;
    Block* b = findFree(size);
    if (!b) {
        failures_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    removeFree(b);
    const uint64_t bs = bsize(b);
    const uint64_t keepFlags = b->sizeFlags & kPrevFree;
    uint8_t* raw = reinterpret_cast<uint8_t*>(b);
    if (bs - size >= kMinBlock) {
        Block* r = reinterpret_cast<Block*>(raw + size);
        r->sizeFlags = (bs - size) | kFree;
        r->prevSize = 0;
        Block* next = reinterpret_cast<Block*>(raw + bs);
        next->prevSize = bs - size;  // next keeps PREV_FREE (r is free)
        b->sizeFlags = size | keepFlags;
        insertFree(r);
    } else {
        Block* next = reinterpret_cast<Block*>(raw + bs);
        next->sizeFlags &= ~kPrevFree;
        b->sizeFlags = bs | keepFlags;
    }
    used_ += bsize(b);
    if (used_ > high_.load(std::memory_order_relaxed)) high_.store(used_, std::memory_order_relaxed);
    return raw + kHdr;
}

void LaneArena::free(void* p) {
    if (!p) return;
    Block* b = reinterpret_cast<Block*>(static_cast<uint8_t*>(p) - kHdr);
    used_ -= bsize(b);
    b->sizeFlags |= kFree;
    if (b->sizeFlags & kPrevFree) {
        Block* prev = reinterpret_cast<Block*>(reinterpret_cast<uint8_t*>(b) - b->prevSize);
        removeFree(prev);
        prev->sizeFlags = (bsize(prev) + bsize(b)) | kFree | (prev->sizeFlags & kPrevFree);
        b = prev;
    }
    Block* next = reinterpret_cast<Block*>(reinterpret_cast<uint8_t*>(b) + bsize(b));
    if (next->sizeFlags & kFree) {
        removeFree(next);
        b->sizeFlags = (bsize(b) + bsize(next)) | kFree | (b->sizeFlags & kPrevFree);
        next = reinterpret_cast<Block*>(reinterpret_cast<uint8_t*>(b) + bsize(b));
    }
    next->prevSize = bsize(b);
    next->sizeFlags |= kPrevFree;
    insertFree(b);
}

void* LaneArena::realloc(void* p, size_t n) {
    if (!p) return alloc(n);
    if (n > (size_t(1) << 38)) return nullptr;
    Block* b = reinterpret_cast<Block*>(static_cast<uint8_t*>(p) - kHdr);
    uint8_t* raw = reinterpret_cast<uint8_t*>(b);
    size_t need = align16(n ? n : 1) + kHdr;
    if (need < kMinBlock) need = kMinBlock;
    const uint64_t bs = bsize(b);
    if (need <= bs) {
        // Shrink in place when the tail is worth returning.
        if (bs - need >= 4 * kMinBlock) {
            Block* r = reinterpret_cast<Block*>(raw + need);
            r->sizeFlags = (bs - need) | kFree;
            b->sizeFlags = need | (b->sizeFlags & kPrevFree);
            used_ -= bs - need;
            Block* next = reinterpret_cast<Block*>(raw + bs);
            if (next->sizeFlags & kFree) {
                removeFree(next);
                r->sizeFlags = (bsize(r) + bsize(next)) | kFree;
                next = reinterpret_cast<Block*>(reinterpret_cast<uint8_t*>(r) + bsize(r));
            }
            next->prevSize = bsize(r);
            next->sizeFlags |= kPrevFree;
            insertFree(r);
        }
        return p;
    }
    Block* next = reinterpret_cast<Block*>(raw + bs);
    if ((next->sizeFlags & kFree) && bs + bsize(next) >= need) {
        const uint64_t ns = bsize(next);
        removeFree(next);
        const uint64_t total = bs + ns;
        Block* after = reinterpret_cast<Block*>(raw + total);
        if (total - need >= kMinBlock) {
            Block* r = reinterpret_cast<Block*>(raw + need);
            r->sizeFlags = (total - need) | kFree;
            after->prevSize = total - need;
            after->sizeFlags |= kPrevFree;
            b->sizeFlags = need | (b->sizeFlags & kPrevFree);
            insertFree(r);
            used_ += need - bs;
        } else {
            after->sizeFlags &= ~kPrevFree;
            b->sizeFlags = total | (b->sizeFlags & kPrevFree);
            used_ += total - bs;
        }
        if (used_ > high_.load(std::memory_order_relaxed)) high_.store(used_, std::memory_order_relaxed);
        return p;
    }
    void* q = alloc(n);
    if (!q) return nullptr;
    std::memcpy(q, p, bs - kHdr);
    free(p);
    return q;
}

size_t LaneArena::usable(const void* p) const {
    const Block* b = reinterpret_cast<const Block*>(static_cast<const uint8_t*>(p) - kHdr);
    return bsize(b) - kHdr;
}

bool LaneArena::check(std::string* why) const {
    uintptr_t at = base_;
    bool prevFree = false;
    uint64_t prevSize = 0;
    size_t used = 0, freeBlocks = 0;
    while (at < end_ - kHdr) {
        const Block* b = reinterpret_cast<const Block*>(at);
        const uint64_t s = bsize(b);
        if (s < kMinBlock || (s & 15) || at + s > end_ - kHdr) {
            if (why) *why = "bad block size";
            return false;
        }
        if (bool(b->sizeFlags & kPrevFree) != prevFree) {
            if (why) *why = "PREV_FREE mismatch";
            return false;
        }
        if (prevFree && b->prevSize != prevSize) {
            if (why) *why = "prevSize mismatch";
            return false;
        }
        const bool isFree = b->sizeFlags & kFree;
        if (isFree && prevFree) {
            if (why) *why = "adjacent free blocks";
            return false;
        }
        if (isFree) freeBlocks++;
        else used += s;
        prevFree = isFree;
        prevSize = s;
        at += s;
    }
    const Block* sentinel = reinterpret_cast<const Block*>(end_ - kHdr);
    if (bsize(sentinel) != 0 || bool(sentinel->sizeFlags & kPrevFree) != prevFree) {
        if (why) *why = "sentinel";
        return false;
    }
    if (used != used_) {
        if (why) *why = "used accounting";
        return false;
    }
    size_t listed = 0;
    for (int fl = 0; fl < kFlCount; fl++)
        for (int sl = 0; sl < kSlCount; sl++)
            for (const Block* b = heads_[fl][sl]; b; b = b->nextFree) {
                if (!(b->sizeFlags & kFree)) {
                    if (why) *why = "used block on a free list";
                    return false;
                }
                listed++;
            }
    if (listed != freeBlocks) {
        if (why) *why = "free list count";
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// SQLite allocator dispatch
// ---------------------------------------------------------------------------
namespace {
thread_local LaneArena* tArena = nullptr;
thread_local ThreadClass tClass = ThreadClass::Other;

constexpr int kMaxArenas = 256;
std::atomic<LaneArena*> gArenas[kMaxArenas];
std::mutex gArenaRegMu;  // registration only (lane start and stop)

void registerArena(LaneArena* a) {
    std::lock_guard<std::mutex> g(gArenaRegMu);
    for (auto& s : gArenas)
        if (s.load() == a) return;
    for (auto& s : gArenas) {
        LaneArena* expect = nullptr;
        if (s.compare_exchange_strong(expect, a)) return;
    }
}

LaneArena* ownerOf(const void* p) {
    LaneArena* a = tArena;
    if (a && a->owns(p)) return a;
    for (auto& s : gArenas) {
        LaneArena* c = s.load(std::memory_order_acquire);
        if (c && c->owns(p)) return c;
    }
    return nullptr;
}

constexpr uint64_t kFallbackMagic = 0x464c5153464c4250ull;  // "PBLFSQLF"
struct FallbackHdr {
    uint64_t magic;
    uint64_t size;
};

void* memMalloc(int n) {
    if (n < 0) return nullptr;
    if (LaneArena* a = tArena) return a->alloc(size_t(n));
    FallbackHdr* h = static_cast<FallbackHdr*>(std::malloc(sizeof(FallbackHdr) + size_t(n)));
    if (!h) return nullptr;
    h->magic = kFallbackMagic;
    h->size = uint64_t(n);
    return h + 1;
}

void memFree(void* p) {
    if (!p) return;
    if (LaneArena* a = ownerOf(p)) {
        a->free(p);
        return;
    }
    std::free(static_cast<FallbackHdr*>(p) - 1);
}

void* memRealloc(void* p, int n) {
    if (!p) return memMalloc(n);
    if (n < 0) return nullptr;
    if (LaneArena* a = ownerOf(p)) return a->realloc(p, size_t(n));
    FallbackHdr* h = static_cast<FallbackHdr*>(std::realloc(static_cast<FallbackHdr*>(p) - 1,
                                                            sizeof(FallbackHdr) + size_t(n)));
    if (!h) return nullptr;
    h->size = uint64_t(n);
    return h + 1;
}

int memSize(void* p) {
    if (!p) return 0;
    if (LaneArena* a = ownerOf(p)) return int(a->usable(p));
    return int((static_cast<FallbackHdr*>(p) - 1)->size);
}

int memRoundup(int n) { return (n + 15) & ~15; }
int memInit(void*) { return SQLITE_OK; }
void memShutdown(void*) {}

// ---------------------------------------------------------------------------
// Instrumented mutexes
// ---------------------------------------------------------------------------
sqlite3_mutex_methods gDefaultMutex;

struct MutexStats {
    std::atomic<uint64_t> lane{0}, writer{0}, other{0}, laneMaxWait{0}, laneContended{0};
};
constexpr int kMutexIds = 16;  // 0 fast, 1 recursive, 2.. static ids
MutexStats gMutexStats[kMutexIds];
std::atomic<uint64_t> gLaneMaxWait{0};

struct WrappedMutex {
    sqlite3_mutex* real;
    int id;
};
WrappedMutex gStatic[kMutexIds];

int mxInit() { return gDefaultMutex.xMutexInit(); }
int mxEnd() { return gDefaultMutex.xMutexEnd(); }

sqlite3_mutex* mxAlloc(int id) {
    sqlite3_mutex* real = gDefaultMutex.xMutexAlloc(id);
    if (!real) return nullptr;
    if (id >= 2 && id < kMutexIds) {
        gStatic[id].real = real;
        gStatic[id].id = id;
        return reinterpret_cast<sqlite3_mutex*>(&gStatic[id]);
    }
    WrappedMutex* w = static_cast<WrappedMutex*>(std::malloc(sizeof(WrappedMutex)));
    if (!w) {
        gDefaultMutex.xMutexFree(real);
        return nullptr;
    }
    w->real = real;
    w->id = id < 2 ? id : 1;
    return reinterpret_cast<sqlite3_mutex*>(w);
}

void mxFree(sqlite3_mutex* m) {
    WrappedMutex* w = reinterpret_cast<WrappedMutex*>(m);
    gDefaultMutex.xMutexFree(w->real);
    if (w->id < 2) std::free(w);
}

void record(int id, uint64_t waitNs, bool contended) {
    MutexStats& s = gMutexStats[id];
    switch (tClass) {
        case ThreadClass::Lane: {
            s.lane.fetch_add(1, std::memory_order_relaxed);
            if (contended) s.laneContended.fetch_add(1, std::memory_order_relaxed);
            uint64_t cur = s.laneMaxWait.load(std::memory_order_relaxed);
            while (waitNs > cur && !s.laneMaxWait.compare_exchange_weak(cur, waitNs)) {}
            cur = gLaneMaxWait.load(std::memory_order_relaxed);
            while (waitNs > cur && !gLaneMaxWait.compare_exchange_weak(cur, waitNs)) {}
            break;
        }
        case ThreadClass::Writer: s.writer.fetch_add(1, std::memory_order_relaxed); break;
        default: s.other.fetch_add(1, std::memory_order_relaxed); break;
    }
}

void mxEnter(sqlite3_mutex* m) {
    WrappedMutex* w = reinterpret_cast<WrappedMutex*>(m);
    if (gDefaultMutex.xMutexTry(w->real) == SQLITE_OK) {
        record(w->id, 0, false);
        return;
    }
    const uint64_t t0 = monoNs();
    gDefaultMutex.xMutexEnter(w->real);
    record(w->id, monoNs() - t0, true);
}

int mxTry(sqlite3_mutex* m) {
    WrappedMutex* w = reinterpret_cast<WrappedMutex*>(m);
    const int rc = gDefaultMutex.xMutexTry(w->real);
    if (rc == SQLITE_OK) record(w->id, 0, false);
    return rc;
}

void mxLeave(sqlite3_mutex* m) { gDefaultMutex.xMutexLeave(reinterpret_cast<WrappedMutex*>(m)->real); }

int mxHeld(sqlite3_mutex* m) {
    return gDefaultMutex.xMutexHeld ? gDefaultMutex.xMutexHeld(reinterpret_cast<WrappedMutex*>(m)->real) : 1;
}
int mxNotHeld(sqlite3_mutex* m) {
    return gDefaultMutex.xMutexNotheld ? gDefaultMutex.xMutexNotheld(reinterpret_cast<WrappedMutex*>(m)->real) : 1;
}

const char* mutexName(int id) {
    switch (id) {
        case 0: return "fast";
        case 1: return "recursive";
        case SQLITE_MUTEX_STATIC_MAIN: return "static_main";
        case SQLITE_MUTEX_STATIC_MEM: return "static_mem";
        case SQLITE_MUTEX_STATIC_OPEN: return "static_open";
        case SQLITE_MUTEX_STATIC_PRNG: return "static_prng";
        case SQLITE_MUTEX_STATIC_LRU: return "static_lru";
        case SQLITE_MUTEX_STATIC_PMEM: return "static_pmem";
        case SQLITE_MUTEX_STATIC_APP1: return "static_app1";
        case SQLITE_MUTEX_STATIC_APP2: return "static_app2";
        case SQLITE_MUTEX_STATIC_APP3: return "static_app3";
        case SQLITE_MUTEX_STATIC_VFS1: return "static_vfs1";
        case SQLITE_MUTEX_STATIC_VFS2: return "static_vfs2";
        case SQLITE_MUTEX_STATIC_VFS3: return "static_vfs3";
        case 15: return "reader_cache";
        default: return "other";
    }
}

// ---------------------------------------------------------------------------
// Null VFS: lanes never open a file through SQLite.
// ---------------------------------------------------------------------------
std::atomic<uint64_t> gRandState{0};

int nvOpen(sqlite3_vfs*, const char*, sqlite3_file* f, int, int*) {
    if (f) f->pMethods = nullptr;
    return SQLITE_CANTOPEN;
}
int nvDelete(sqlite3_vfs*, const char*, int) { return SQLITE_OK; }
int nvAccess(sqlite3_vfs*, const char*, int, int* out) {
    *out = 0;
    return SQLITE_OK;
}
int nvFullPathname(sqlite3_vfs*, const char* name, int nOut, char* out) {
    const size_t n = std::strlen(name);
    if (int(n) >= nOut) return SQLITE_CANTOPEN;
    std::memcpy(out, name, n + 1);
    return SQLITE_OK;
}
int nvRandomness(sqlite3_vfs*, int n, char* out) {
    uint64_t x = gRandState.fetch_add(0x9E3779B97F4A7C15ull, std::memory_order_relaxed) ^ monoNs();
    for (int i = 0; i < n; i++) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out[i] = char(x);
    }
    return n;
}
int nvSleep(sqlite3_vfs*, int us) {
    sleepNs(uint64_t(us) * 1000);
    return us;
}
int nvCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64* out) {
    // Julian day number in milliseconds.
    *out = sqlite3_int64(210866760000000ll) + wallMs();
    return SQLITE_OK;
}
int nvCurrentTime(sqlite3_vfs* v, double* out) {
    sqlite3_int64 ms;
    nvCurrentTimeInt64(v, &ms);
    *out = double(ms) / 86400000.0;
    return SQLITE_OK;
}
int nvGetLastError(sqlite3_vfs*, int, char*) { return 0; }

sqlite3_vfs gNullVfs = {
    2,                     // iVersion
    int(sizeof(sqlite3_file)),
    512,                   // mxPathname
    nullptr,               // pNext
    kNullVfsName,          // zName
    nullptr,               // pAppData
    nvOpen, nvDelete, nvAccess, nvFullPathname,
    nullptr, nullptr, nullptr, nullptr,  // dlopen family
    nvRandomness, nvSleep, nvCurrentTime, nvGetLastError, nvCurrentTimeInt64,
    nullptr, nullptr, nullptr,
};

std::once_flag gInitOnce;
int32_t gInitRc = 0;
std::string gInitErr;

}  // namespace

void laneArenaBind(LaneArena* a) {
    if (a) registerArena(a);
    tArena = a;
}

LaneArena* laneArenaCurrent() { return tArena; }

void setThreadClass(ThreadClass c) { tClass = c; }

int32_t readerSqliteInit(std::string* err) {
    std::call_once(gInitOnce, [] {
        gRandState.store(monoNs() ^ uint64_t(reinterpret_cast<uintptr_t>(&gRandState)));
        sqlite3_mem_methods mem = {memMalloc, memFree, memRealloc, memSize, memRoundup,
                                   memInit, memShutdown, nullptr};
        int rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &mem);
        if (rc == SQLITE_OK) rc = sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
        if (rc == SQLITE_OK) rc = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
        // The default mutex methods exist only once SQLite has initialized:
        // initialize, read them, shut down, then install the wrappers.
        if (rc == SQLITE_OK) rc = sqlite3_initialize();
        if (rc == SQLITE_OK) rc = sqlite3_shutdown();
        if (rc == SQLITE_OK) rc = sqlite3_config(SQLITE_CONFIG_GETMUTEX, &gDefaultMutex);
        if (rc == SQLITE_OK && !gDefaultMutex.xMutexAlloc) rc = SQLITE_ERROR;
        if (rc == SQLITE_OK) {
            sqlite3_mutex_methods wrapped = {mxInit, mxEnd, mxAlloc, mxFree, mxEnter,
                                             mxTry, mxLeave, mxHeld, mxNotHeld};
            rc = sqlite3_config(SQLITE_CONFIG_MUTEX, &wrapped);
        }
        if (rc == SQLITE_OK) rc = sqlite3_initialize();
        if (rc == SQLITE_OK) rc = sqlite3_vfs_register(&gNullVfs, 0);
        if (rc != SQLITE_OK) {
            gInitRc = -1;
            gInitErr = std::string("sqlite configuration failed: ") + sqlite3_errstr(rc);
        }
    });
    if (gInitRc < 0 && err) *err = gInitErr;
    return gInitRc;
}

void recordLaneLockWait(uint64_t waitNs, bool contended) { record(15, waitNs, contended); }

LockReport lockReport() {
    LockReport r;
    for (int id = 0; id < kMutexIds; id++) {
        const MutexStats& s = gMutexStats[id];
        const uint64_t total = s.lane.load() + s.writer.load() + s.other.load();
        if (!total) continue;
        LockReport::Entry e;
        e.name = mutexName(id);
        e.laneAcquires = s.lane.load();
        e.writerAcquires = s.writer.load();
        e.otherAcquires = s.other.load();
        e.laneMaxWaitNs = s.laneMaxWait.load();
        e.laneContended = s.laneContended.load();
        r.entries.push_back(e);
    }
    r.laneMaxWaitNs = gLaneMaxWait.load();
    return r;
}

void lockReportReset() {
    for (auto& s : gMutexStats) {
        s.lane = 0;
        s.writer = 0;
        s.other = 0;
        s.laneMaxWait = 0;
        s.laneContended = 0;
    }
    gLaneMaxWait = 0;
}

}  // namespace ps
}  // namespace flatsql
