// Partition store tests: framework, fixtures, inspector, main.
//
// Usage: flatsql_ps_test [--test=<substring>] [--all] [--<param>=<value>]
// Slow tests (full acceptance durations) run only with --all or when named.
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/idl.h>
#include <flatbuffers/reflection.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <new>

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
void* operator new(std::size_t n) {
    gAllAllocs.fetch_add(1, std::memory_order_relaxed);
    if (flatsql::ps::tHotPathDepth > 0) {
        gHotAllocs.fetch_add(1, std::memory_order_relaxed);
        traceHot();
    }
    void* p = std::malloc(n ? n : 1);
    if (!p) allocFailed();
    return p;
}
void* operator new[](std::size_t n) {
    gAllAllocs.fetch_add(1, std::memory_order_relaxed);
    if (flatsql::ps::tHotPathDepth > 0) gHotAllocs.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(n ? n : 1);
    if (!p) allocFailed();
    return p;
}
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    if (flatsql::ps::tHotPathDepth > 0) gHotAllocs.fetch_add(1, std::memory_order_relaxed);
    return std::malloc(n ? n : 1);
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    if (flatsql::ps::tHotPathDepth > 0) gHotAllocs.fetch_add(1, std::memory_order_relaxed);
    return std::malloc(n ? n : 1);
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace pst {

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
    // Grow the heap once, on this thread, before any guest thread starts:
    // V8 refreshes each thread's view of a shared memory's size lazily, so a
    // thread touching memory another thread has just grown can fault. The
    // allocator keeps the freed block (wasm memory never shrinks).
    {
        const char* mb = std::getenv("PS_WASM_HEAP_MB");
        const size_t bytes = size_t(mb ? std::atol(mb) : 1536) << 20;
        if (bytes) {
            void* volatile p = std::malloc(bytes);  // volatile: not elided
            std::free(p);
        }
    }
#endif
    for (int i = 1; i < argc; i++) pst::gArgs.push_back(argv[i]);
    const std::string filter = pst::argStr("test", "");
    bool all = false;
    for (const auto& a : pst::gArgs)
        if (a == "--all") all = true;
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
