// Native replay instrumentation: count the real seven-import sync calls,
// including SQLite WAL syncs, and fail one chosen read. The production native
// implementation is built unchanged with only its sync and read symbols
// renamed in this test target.
#if !defined(__wasm__)
#include <atomic>
#include <cstdint>
#include "flatsql/flatsql_io.h"

namespace {
std::atomic<uint64_t> replaySyncCalls{0};
std::atomic<uint64_t> replayFailedReads{0};
// One armed read failure, on the arming thread only: the next read of handle
// failHandle that starts at or below failAtOrBelow.
thread_local bool failArmed = false;
thread_local int32_t failHandle = -1;
thread_local double failAtOrBelow = 0;
}
extern "C" int32_t p4ReplayNativeSync(int32_t handle);
extern "C" int32_t p4ReplayNativeRead(int32_t handle, void* dst, int32_t len, double offset);
extern "C" int32_t flatsql_io_sync(int32_t handle) {
    replaySyncCalls.fetch_add(1, std::memory_order_relaxed);
    return p4ReplayNativeSync(handle);
}
extern "C" int32_t flatsql_io_read(int32_t handle, void* dst, int32_t len, double offset) {
    if (failArmed && handle == failHandle && offset <= failAtOrBelow) {
        failArmed = false;
        replayFailedReads.fetch_add(1, std::memory_order_relaxed);
        return FLATSQL_IO_ERR_GENERIC;
    }
    return p4ReplayNativeRead(handle, dst, len, offset);
}
uint64_t p4ReplayIoSyncs() { return replaySyncCalls.load(std::memory_order_relaxed); }
void p4ReplayFailNextRead(int32_t handle, uint64_t atOrBelow) {
    failHandle = handle;
    failAtOrBelow = double(atOrBelow);
    failArmed = true;
}
uint64_t p4ReplayFailedReads() { return replayFailedReads.load(std::memory_order_relaxed); }
#endif
