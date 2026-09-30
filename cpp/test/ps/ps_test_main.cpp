// Partition store tests: framework, fixtures, inspector, main.
//
// Usage: flatsql_ps_test [--test=<substring>] [--all] [--list] [--<param>=<value>]
// Slow tests (full acceptance durations) run only with --all or when named.
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/idl.h>
#include <flatbuffers/reflection.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <new>

#include "flatsql/ps/lane_arena.h"
#include "flatsql/ps/platform.h"
#include "ps/ps_test.h"

// ---- hot-path allocation counter (acceptance T1 #8) ---------------------------------
std::atomic<uint64_t> gHotAllocs{0};
std::atomic<uint64_t> gAllAllocs{0};
#if !defined(__wasm__)
#include <execinfo.h>
#endif
static void traceHot() {
#if !defined(__wasm__)
    static std::atomic<int> printed{0};
    if (!std::getenv("PS_TRACE_HOT") || printed.fetch_add(1) >= 4) return;
    const int saved = flatsql::ps::tHotPathDepth;
    flatsql::ps::tHotPathDepth = 0;
    void* frames[32];
    const int n = backtrace(frames, 32);
    backtrace_symbols_fd(frames, n, 2);
    std::fprintf(stderr, "----\n");
    flatsql::ps::tHotPathDepth = saved;
#endif
}
// Allocation failure: bad_alloc where exceptions exist; the wasm commands are
// built without them (-fno-exceptions), where it aborts.
[[noreturn]] static void allocFailed() {
#if defined(__cpp_exceptions)
    throw std::bad_alloc();
#else
    std::fprintf(stderr, "operator new: out of memory\n");
    std::abort();
#endif
}
// ---- reader memory accounting (reader_memory_test.cpp) ------------------------------
// Every operator new block carries a 16-byte header: its size and whether a
// reader lane thread (one with a bound lane arena) allocated it. The live
// bytes of lane allocations are what a reader instance holds beyond its
// fixed arenas, caches included, wherever the block is later freed.
std::atomic<int64_t> gLaneLiveBytes{0};
std::atomic<int64_t> gAllLiveBytes{0};
namespace {
constexpr uint64_t kTagLane = 0x6c616e656c616e65ull;   // "lanelane"
constexpr uint64_t kTagOther = 0x6f746865726f7468ull;  // "otheroth"
struct alignas(16) AllocHeader {
    uint64_t tag;
    uint64_t n;
};
static_assert(sizeof(AllocHeader) == 16, "keeps malloc's 16-byte alignment");
void* allocTracked(std::size_t n) {
    AllocHeader* h = static_cast<AllocHeader*>(std::malloc(sizeof(AllocHeader) + (n ? n : 1)));
    if (!h) return nullptr;
    const bool lane = flatsql::ps::laneArenaCurrent() != nullptr;
    h->tag = lane ? kTagLane : kTagOther;
    h->n = n;
    if (lane) gLaneLiveBytes.fetch_add(int64_t(n), std::memory_order_relaxed);
    gAllLiveBytes.fetch_add(int64_t(n), std::memory_order_relaxed);
    return h + 1;
}
void freeTracked(void* p) {
    if (!p) return;
    AllocHeader* h = static_cast<AllocHeader*>(p) - 1;
    if (h->tag == kTagLane) gLaneLiveBytes.fetch_sub(int64_t(h->n), std::memory_order_relaxed);
    gAllLiveBytes.fetch_sub(int64_t(h->n), std::memory_order_relaxed);
    std::free(h);
}
}  // namespace
void* operator new(std::size_t n) {
    gAllAllocs.fetch_add(1, std::memory_order_relaxed);
    if (flatsql::ps::tHotPathDepth > 0) {
        gHotAllocs.fetch_add(1, std::memory_order_relaxed);
        traceHot();
    }
    void* p = allocTracked(n);
    if (!p) allocFailed();
    return p;
}
void* operator new[](std::size_t n) {
    gAllAllocs.fetch_add(1, std::memory_order_relaxed);
    if (flatsql::ps::tHotPathDepth > 0) gHotAllocs.fetch_add(1, std::memory_order_relaxed);
    void* p = allocTracked(n);
    if (!p) allocFailed();
    return p;
}
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    if (flatsql::ps::tHotPathDepth > 0) gHotAllocs.fetch_add(1, std::memory_order_relaxed);
    return allocTracked(n);
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    if (flatsql::ps::tHotPathDepth > 0) gHotAllocs.fetch_add(1, std::memory_order_relaxed);
    return allocTracked(n);
}
void operator delete(void* p) noexcept { freeTracked(p); }
void operator delete[](void* p) noexcept { freeTracked(p); }
void operator delete(void* p, std::size_t) noexcept { freeTracked(p); }
void operator delete[](void* p, std::size_t) noexcept { freeTracked(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { freeTracked(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { freeTracked(p); }

namespace pst {

extern uint32_t gReaderFencePage;  // reader_fixtures.cpp
int gFailures = 0;
static std::vector<std::string> gArgs;

std::vector<Test>& registry() {
    static std::vector<Test> r;
    return r;
}

long argInt(const char* name, long def) {
    const std::string key = std::string("--") + name + "=";
    for (const auto& a : gArgs)
        if (a.compare(0, key.size(), key) == 0) return std::atol(a.c_str() + key.size());
    return def;
}

std::string argStr(const char* name, const std::string& def) {
    const std::string key = std::string("--") + name + "=";
    for (const auto& a : gArgs)
        if (a.compare(0, key.size(), key) == 0) return a.substr(key.size());
    return def;
}

void report(const char* key, double value, const char* unit) {
    std::printf("  MEASURED %s = %.3f %s\n", key, value, unit);
}

}  // namespace pst

int main(int argc, char** argv) {
#if defined(__wasm__)
    // Grow the heap to the memory's maximum, on this thread, before any guest
    // thread starts, so that memory never grows while threads run. V8 (Node,
    // browsers) bounds-checks memory.fill/copy and atomics against each
    // thread's cached memory size, which another thread's memory.grow updates
    // only at this thread's next stack check: a thread filling a block carved
    // from memory another thread has just grown traps (the ps-wasm trap in
    // writeTypeMergeOutputs, docs/PARTITION-STORE-WASM.md §1). A partial
    // pre-grow only moves that. dlmalloc keeps the freed blocks (wasm memory
    // never shrinks) and grows at most 2 GiB per call, hence the steps. The
    // heap stops 16 MiB short of 4 GiB: a segment ending at 2^32 would wrap
    // the allocator's pointer arithmetic. PS_WASM_HEAP_MB caps the growth.
    {
        const char* mb = std::getenv("PS_WASM_HEAP_MB");
        const size_t cap = mb ? size_t(std::atol(mb)) << 20 : SIZE_MAX;
        const uint64_t limit = (uint64_t(1) << 32) - (uint64_t(16) << 20);
        void* volatile held[64];  // volatile: the allocations are not elided
        int n = 0;
        size_t got = 0;
        for (size_t step = size_t(1) << 30; step >= (size_t(1) << 20) && n < 64;) {
            const uint64_t mem = uint64_t(__builtin_wasm_memory_size(0)) << 16;
            void* p = step <= cap - got && mem + step + (1u << 20) <= limit ? std::malloc(step) : nullptr;
            if (!p) {
                step >>= 1;
                continue;
            }
            held[n++] = p;
            got += step;
        }
        for (int i = 0; i < n; i++) std::free(held[i]);
    }
#endif
    for (int i = 1; i < argc; i++) pst::gArgs.push_back(argv[i]);
    // Readers the fixtures build read this many fences per page (0: 128).
    pst::gReaderFencePage = uint32_t(pst::argInt("fence-page", 0));
    const std::string filter = pst::argStr("test", "");
    bool all = false;
    for (const auto& a : pst::gArgs) {
        if (a == "--all") all = true;
        if (a == "--list") {
            // One test per line: "<name> fast|slow" (the default suite is the fast ones).
            for (const auto& t : pst::registry()) std::printf("%s %s\n", t.name, t.slow ? "slow" : "fast");
            return 0;
        }
    }
    int ran = 0;
    bool exact = false;
    for (const auto& t : pst::registry())
        if (filter == t.name) exact = true;
    for (const auto& t : pst::registry()) {
        const bool named = !filter.empty() && (exact ? filter == t.name
                                                     : std::string(t.name).find(filter) != std::string::npos);
        if (!filter.empty() && !named) continue;
        if (t.slow && !all && !named) continue;
        const int before = pst::gFailures;
        const uint64_t t0 = flatsql::ps::monoNs();
        std::printf("[ RUN  ] %s\n", t.name);
        std::fflush(stdout);
        t.fn();
        const double ms = double(flatsql::ps::monoNs() - t0) / 1e6;
        std::printf("[ %s ] %s (%.0f ms)\n", pst::gFailures == before ? " OK " : "FAIL", t.name, ms);
        std::fflush(stdout);
        ran++;
    }
    std::printf("%d tests, %d failures\n", ran, pst::gFailures);
    return pst::gFailures ? 1 : 0;
}
