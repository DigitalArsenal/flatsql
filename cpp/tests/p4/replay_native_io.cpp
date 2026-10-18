// Native replay instrumentation: count the real seven-import sync calls,
// including SQLite WAL syncs. The production native implementation is built
// unchanged with only its sync symbol renamed in this test target.
#if !defined(__wasm__)
#include <atomic>
#include <cstdint>
#include "flatsql/flatsql_io.h"

namespace {
std::atomic<uint64_t> replaySyncCalls{0};
}
extern "C" int32_t p4ReplayNativeSync(int32_t handle);
extern "C" int32_t flatsql_io_sync(int32_t handle) {
    replaySyncCalls.fetch_add(1, std::memory_order_relaxed);
    return p4ReplayNativeSync(handle);
}
uint64_t p4ReplayIoSyncs() { return replaySyncCalls.load(std::memory_order_relaxed); }
#endif
