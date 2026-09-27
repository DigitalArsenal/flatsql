// FlatSQL partition store: platform primitives.
//
// Everything the engine needs from its host that is not file I/O lives here:
// monotonic time, 32-bit wait/notify on shared memory, CRC32C, SHA-256 and a
// 64-bit hash. On wasm32-wasip1-threads the wait/notify pair is
// memory.atomic.wait32 / memory.atomic.notify; natively it is the kernel's
// address wait (futex on Linux, __ulock on darwin). No function here throws
// or allocates.
#ifndef FLATSQL_PS_PLATFORM_H
#define FLATSQL_PS_PLATFORM_H

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace flatsql {
namespace ps {

static_assert(sizeof(void*) == 4 || sizeof(void*) == 8, "32- or 64-bit targets only");

// Monotonic nanoseconds (arbitrary origin).
uint64_t monoNs();
// Wall clock, milliseconds since the Unix epoch.
int64_t wallMs();

// Block while *addr == expected, for at most timeoutNs (0 = do not block).
// Spurious wakeups are allowed; callers re-check their condition.
void waitU32(std::atomic<uint32_t>* addr, uint32_t expected, uint64_t timeoutNs);
// Wake up to `count` waiters on addr (count < 0 wakes all).
void wakeU32(std::atomic<uint32_t>* addr, int count);
void cpuRelax();
// Sleep the calling thread (tests and backoff only; never on a hot path).
void sleepNs(uint64_t ns);

// CRC32C (Castagnoli), the checksum of every on-disk structure.
uint32_t crc32c(uint32_t crc, const void* data, size_t len);
inline uint32_t crc32c(const void* data, size_t len) { return crc32c(0, data, len); }

// SHA-256 (FIPS 180-4). Used to verify the CID of unsealed frames.
void sha256(const void* data, size_t len, uint8_t out[32]);

// 64-bit non-cryptographic hash (bloom filters, tuple identities, tables).
uint64_t hash64(const void* data, size_t len, uint64_t seed = 0);

// Hot-path allocation accounting (acceptance T1 #8). The engine raises this
// depth around per-record work; a test binary that overrides operator new can
// count allocations made while it is non-zero. The engine itself never reads it.
extern thread_local int tHotPathDepth;
struct HotPathScope {
    HotPathScope() { ++tHotPathDepth; }
    ~HotPathScope() { --tHotPathDepth; }
};

}  // namespace ps
}  // namespace flatsql

#endif
