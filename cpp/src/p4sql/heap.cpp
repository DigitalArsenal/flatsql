// FlatSQL store format 4, SQL surface: the per-lane allocator (CONTRACT.md
// §3.9: "the lane heap cap through the per-lane allocator").
//
// Copy-adapted from format 2's lane arenas (ps/lane_arena.cpp): SQLite's
// allocator is installed process-wide with SQLITE_CONFIG_MALLOC and
// dispatches on the calling thread's bound lane, and a pointer always finds
// its owner. Format 2 carved each lane a fixed TLSF region because its
// readers shared no memory with the writer instance. Format 4 runs readers,
// writers and the engine's own reader connections in one instance, and a
// legitimate trusted statement (the epoch window over 400,000 OMM rows) sorts
// far more than a fixed per-lane region of a 2 GiB wasm32 memory could hold.
// So the region is replaced by accounting: every allocation carries a 16-byte
// header naming its lane, the lane's counter is atomic (a pointer may be freed
// from any thread), and a sandboxed statement's limit fails its allocation
// with SQLITE_NOMEM long before the process-wide hard heap limit can starve a
// writer.
#include <sqlite3.h>

#include <cstdlib>

#include "internal.h"

namespace flatsql {
namespace p4sql {

namespace {

thread_local LaneHeap* tHeap = nullptr;

// 16 bytes on wasm32 and 64-bit hosts: keeps SQLite's 8-byte alignment.
struct Hdr {
    uint64_t size;
    uint64_t owner;   // LaneHeap*, 0 = unaccounted
};
static_assert(sizeof(Hdr) == 16, "allocation header must be 16 bytes");

inline LaneHeap* ownerOf(const Hdr* h) { return reinterpret_cast<LaneHeap*>(static_cast<uintptr_t>(h->owner)); }

// Whether `h` may grow by `n` bytes; records a trip when it may not.
bool admit(LaneHeap* h, int64_t n) {
    if (!h || n <= 0) return true;
    const int64_t lim = h->limit.load(std::memory_order_relaxed);
    if (lim > 0 && h->used.load(std::memory_order_relaxed) + n > lim) {
        h->tripped.store(true, std::memory_order_relaxed);
        return false;
    }
    return true;
}

void* memMalloc(int n) {
    if (n < 0) return nullptr;
    LaneHeap* h = tHeap;
    if (!admit(h, n)) return nullptr;
    Hdr* p = static_cast<Hdr*>(std::malloc(sizeof(Hdr) + size_t(n)));
    if (!p) return nullptr;
    p->size = uint64_t(n);
    p->owner = reinterpret_cast<uintptr_t>(h);
    if (h) h->used.fetch_add(n, std::memory_order_relaxed);
    return p + 1;
}

void memFree(void* q) {
    if (!q) return;
    Hdr* p = static_cast<Hdr*>(q) - 1;
    if (LaneHeap* h = ownerOf(p)) h->used.fetch_sub(int64_t(p->size), std::memory_order_relaxed);
    std::free(p);
}

void* memRealloc(void* q, int n) {
    if (!q) return memMalloc(n);
    if (n < 0) return nullptr;
    Hdr* p = static_cast<Hdr*>(q) - 1;
    LaneHeap* h = ownerOf(p);   // the allocation stays its owner's
    const int64_t delta = int64_t(n) - int64_t(p->size);
    if (!admit(h, delta)) return nullptr;
    Hdr* r = static_cast<Hdr*>(std::realloc(p, sizeof(Hdr) + size_t(n)));
    if (!r) return nullptr;
    r->size = uint64_t(n);
    if (h) h->used.fetch_add(delta, std::memory_order_relaxed);
    return r + 1;
}

int memSize(void* q) { return q ? int((static_cast<Hdr*>(q) - 1)->size) : 0; }
int memRoundup(int n) { return (n + 7) & ~7; }
int memInit(void*) { return SQLITE_OK; }
void memShutdown(void*) {}

const sqlite3_mem_methods kMethods = {memMalloc, memFree, memRealloc, memSize, memRoundup,
                                      memInit,   memShutdown, nullptr};

}  // namespace

int32_t heapInstall() {
    const int rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &kMethods);
    return rc == SQLITE_OK ? P4_OK : P4_E_INTERNAL;
}

void heapBind(LaneHeap* h) { tHeap = h; }
LaneHeap* heapBound() { return tHeap; }

}  // namespace p4sql
}  // namespace flatsql
