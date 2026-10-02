// FlatSQL store format 4, SQL surface: the per-lane allocator (CONTRACT.md
// §3.9: "the lane heap cap (tag 48) through the per-lane allocator"; M2).
//
// Copy-adapted from format 2's lane arenas (ps/lane_arena.cpp): SQLite's
// allocator is installed process-wide with SQLITE_CONFIG_MALLOC and
// dispatches on the calling thread's bound arena; a pointer always finds its
// owner (an arena by address, else the system allocator through a tagged
// header).
//
// A lane binds its arena for the statements it runs SANDBOXED (untrusted
// SQL): the arena is a fixed region the size of the sandbox heap cap, carved
// by a TLSF allocator that takes no lock, so a runaway statement fails with
// SQLITE_NOMEM inside its own region (reported as P4_E_BUDGET) and never
// contends with, or takes memory from, the writers. Measured on this
// machine before the arena: a capped statement churning its heap through the
// shared allocator cut a writer thread's SQLite ingest rate by 67-78%. Trusted
// statements (SDN's own epoch and module SQL) and every engine call use the
// system allocator: the epoch window over 400,000 OMM rows sorts more than a
// sandbox arena holds, as it does in format 1's engine.
//
// C-30: SQLite runs without memory statistics (SQLITE_DEFAULT_MEMSTATUS=0),
// so this allocator counts every SQLite allocation in one atomic counter and
// refuses one past the hard heap limit (sqlite3_hard_heap_limit64, config tag
// 27). The counter serves stats 29/30 (p4sql_heap_used / p4sql_heap_peak).
#include <sqlite3.h>

#include <cstdlib>
#include <cstring>
#include <mutex>

#include "internal.h"

#if !defined(__wasm__)
#include <sys/mman.h>
#endif

namespace flatsql {
namespace p4sql {

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
// Largest request (256 GiB): in 64-bit arithmetic, so the bound also holds
// where size_t is 32 bits (wasm32), where a size_t shift by 38 is undefined.
constexpr uint64_t kMaxRequest = uint64_t(1) << 38;

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
    if (!base_ || uint64_t(n) > kMaxRequest) {
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
    if (uint64_t(n) > kMaxRequest) return nullptr;
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

constexpr int kMaxArenas = 256;
std::atomic<LaneArena*> gArenas[kMaxArenas];
std::atomic<int> gArenaSlots{0};   // slots ever used: every free scans only these
std::mutex gArenaRegMu;            // registration only (arena creation and destruction)

LaneArena* ownerOf(const void* p) {
    LaneArena* a = tArena;
    if (a && a->owns(p)) return a;
    const int n = gArenaSlots.load(std::memory_order_acquire);
    for (int i = 0; i < n; i++) {
        LaneArena* c = gArenas[i].load(std::memory_order_acquire);
        if (c && c->owns(p)) return c;
    }
    return nullptr;
}

constexpr uint64_t kFallbackMagic = 0x464c5153464c4250ull;  // "PBLFSQLF"
struct FallbackHdr {
    uint64_t magic;
    uint64_t size;
};

// C-30: bytes live (each block's usable size, what memSize reports), the
// high-water mark, and the hard heap limit (0 = none).
std::atomic<uint64_t> gUsed{0};
std::atomic<uint64_t> gPeak{0};
std::atomic<uint64_t> gLimit{0};

// Counts `n` more bytes in. false: past the hard limit, nothing counted.
bool reserve(uint64_t n) {
    const uint64_t now = gUsed.fetch_add(n, std::memory_order_relaxed) + n;
    const uint64_t lim = gLimit.load(std::memory_order_relaxed);
    if (lim && now > lim) {
        gUsed.fetch_sub(n, std::memory_order_relaxed);
        return false;
    }
    uint64_t peak = gPeak.load(std::memory_order_relaxed);
    while (now > peak && !gPeak.compare_exchange_weak(peak, now, std::memory_order_relaxed)) {
    }
    return true;
}

void release(uint64_t n) { gUsed.fetch_sub(n, std::memory_order_relaxed); }

size_t sizeOf(LaneArena* a, void* p) {
    return a ? a->usable(p) : size_t((static_cast<FallbackHdr*>(p) - 1)->size);
}

void* memMalloc(int n) {
    if (n < 0 || !reserve(uint64_t(n))) return nullptr;
    void* p = nullptr;
    if (LaneArena* a = tArena) {
        p = a->alloc(size_t(n));
        if (p) gUsed.fetch_add(a->usable(p) - size_t(n), std::memory_order_relaxed);   // the block's rounding
    } else if (FallbackHdr* h = static_cast<FallbackHdr*>(std::malloc(sizeof(FallbackHdr) + size_t(n)))) {
        h->magic = kFallbackMagic;
        h->size = uint64_t(n);
        p = h + 1;
    }
    if (!p) release(uint64_t(n));
    return p;
}

void memFree(void* p) {
    if (!p) return;
    LaneArena* a = ownerOf(p);
    release(sizeOf(a, p));
    if (a) a->free(p);
    else std::free(static_cast<FallbackHdr*>(p) - 1);
}

void* memRealloc(void* p, int n) {
    if (!p) return memMalloc(n);
    if (n < 0) return nullptr;
    LaneArena* a = ownerOf(p);
    const size_t old = sizeOf(a, p);
    if (size_t(n) > old && !reserve(uint64_t(n) - old)) return nullptr;
    void* q = nullptr;
    if (a) {
        q = a->realloc(p, size_t(n));
    } else if (FallbackHdr* h = static_cast<FallbackHdr*>(
                   std::realloc(static_cast<FallbackHdr*>(p) - 1, sizeof(FallbackHdr) + size_t(n)))) {
        h->size = uint64_t(n);
        q = h + 1;
    }
    // Settle the counter to the block's size now (or back, on failure).
    const size_t counted = size_t(n) > old ? size_t(n) : old;
    const size_t now = q ? sizeOf(a, q) : old;
    if (now > counted) gUsed.fetch_add(now - counted, std::memory_order_relaxed);
    else release(counted - now);
    return q;
}

int memSize(void* p) {
    if (!p) return 0;
    return int(sizeOf(ownerOf(p), p));
}

int memRoundup(int n) { return (n + 15) & ~15; }
int memInit(void*) { return SQLITE_OK; }
void memShutdown(void*) {}

const sqlite3_mem_methods kMethods = {memMalloc, memFree, memRealloc, memSize, memRoundup,
                                      memInit,   memShutdown, nullptr};

}  // namespace

bool arenaRegister(LaneArena* a) {
    std::lock_guard<std::mutex> g(gArenaRegMu);
    for (auto& s : gArenas)
        if (s.load() == a) return true;
    for (int i = 0; i < kMaxArenas; i++) {
        LaneArena* expect = nullptr;
        if (gArenas[i].compare_exchange_strong(expect, a)) {
            if (gArenaSlots.load() < i + 1) gArenaSlots.store(i + 1, std::memory_order_release);
            return true;
        }
    }
    return false;
}

void arenaUnregister(LaneArena* a) {
    std::lock_guard<std::mutex> g(gArenaRegMu);
    for (auto& s : gArenas) {
        LaneArena* expect = a;
        if (s.compare_exchange_strong(expect, nullptr)) return;
    }
}

int32_t heapInstall() {
    const int rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &kMethods);
    return rc == SQLITE_OK ? P4_OK : P4_E_INTERNAL;
}

// The engine sets the limit in flatsql_p4_init after the allocator is
// installed, so a lane reads it at its start (never inside an allocation:
// sqlite3_hard_heap_limit64 takes SQLite's mem0 mutex).
void heapLimitRefresh() {
    const sqlite3_int64 lim = sqlite3_hard_heap_limit64(-1);
    if (lim >= 0) gLimit.store(uint64_t(lim), std::memory_order_relaxed);
}

void arenaBind(LaneArena* a) { tArena = a; }
LaneArena* arenaBound() { return tArena; }

}  // namespace p4sql
}  // namespace flatsql

extern "C" uint64_t p4sql_heap_used(void) { return flatsql::p4sql::gUsed.load(std::memory_order_relaxed); }
extern "C" uint64_t p4sql_heap_peak(void) { return flatsql::p4sql::gPeak.load(std::memory_order_relaxed); }
