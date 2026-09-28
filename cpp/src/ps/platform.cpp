// FlatSQL partition store: platform primitives (see ps/platform.h).
#include "flatsql/ps/platform.h"

#include <cstring>
#include <ctime>

#if defined(__wasm__)
#  include <sched.h>
#elif defined(__APPLE__)
#  include <errno.h>
#  include <sched.h>
#  include <unistd.h>
extern "C" int __ulock_wait(uint32_t operation, void* addr, uint64_t value, uint32_t timeoutUs);
extern "C" int __ulock_wake(uint32_t operation, void* addr, uint64_t wakeValue);
#  define PS_UL_COMPARE_AND_WAIT 1u
#  define PS_ULF_WAKE_ALL 0x00000100u
#  define PS_ULF_NO_ERRNO 0x01000000u
#elif defined(__linux__)
#  include <linux/futex.h>
#  include <sched.h>
#  include <sys/syscall.h>
#  include <unistd.h>
#  include <climits>
#endif

namespace flatsql {
namespace ps {

thread_local int tHotPathDepth = 0;

uint64_t monoNs() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return uint64_t(ts.tv_sec) * 1000000000ull + uint64_t(ts.tv_nsec);
}

int64_t wallMs() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return int64_t(ts.tv_sec) * 1000 + int64_t(ts.tv_nsec / 1000000);
}

void cpuRelax() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield");
#endif
}

void sleepNs(uint64_t ns) {
    struct timespec ts;
    ts.tv_sec = time_t(ns / 1000000000ull);
    ts.tv_nsec = long(ns % 1000000000ull);
    nanosleep(&ts, nullptr);
}

void waitU32(std::atomic<uint32_t>* addr, uint32_t expected, uint64_t timeoutNs) {
    if (timeoutNs == 0) return;
    if (addr->load(std::memory_order_acquire) != expected) return;
#if defined(__wasm__)
    __builtin_wasm_memory_atomic_wait32(reinterpret_cast<int*>(addr), int(expected),
                                        int64_t(timeoutNs));
#elif defined(__APPLE__)
    uint64_t us = timeoutNs / 1000;
    if (us == 0) us = 1;
    if (us > 0xffffffffull) us = 0xffffffffull;
    __ulock_wait(PS_UL_COMPARE_AND_WAIT | PS_ULF_NO_ERRNO, addr, expected, uint32_t(us));
#elif defined(__linux__)
    struct timespec ts;
    ts.tv_sec = time_t(timeoutNs / 1000000000ull);
    ts.tv_nsec = long(timeoutNs % 1000000000ull);
    syscall(SYS_futex, reinterpret_cast<uint32_t*>(addr), FUTEX_WAIT, expected, &ts, nullptr, 0);
#else
    sleepNs(timeoutNs < 50000 ? timeoutNs : 50000);
#endif
}

void wakeU32(std::atomic<uint32_t>* addr, int count) {
#if defined(__wasm__)
    __builtin_wasm_memory_atomic_notify(reinterpret_cast<int*>(addr),
                                        count < 0 ? 0x7fffffff : unsigned(count));
#elif defined(__APPLE__)
    uint32_t op = PS_UL_COMPARE_AND_WAIT | PS_ULF_NO_ERRNO;
    if (count < 0 || count > 1) op |= PS_ULF_WAKE_ALL;
    __ulock_wake(op, addr, 0);
#elif defined(__linux__)
    syscall(SYS_futex, reinterpret_cast<uint32_t*>(addr), FUTEX_WAKE,
            count < 0 ? INT_MAX : count, nullptr, nullptr, 0);
#else
    (void)addr;
    (void)count;
#endif
}

// ---------------------------------------------------------------------------
// CRC32C, slice-by-8. Software only: byte-identical on every host.
// ---------------------------------------------------------------------------
namespace {

struct Crc32cTables {
    uint32_t t[8][256];
    Crc32cTables() {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : (c >> 1);
            t[0][i] = c;
        }
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = t[0][i];
            for (int s = 1; s < 8; s++) {
                c = t[0][c & 0xff] ^ (c >> 8);
                t[s][i] = c;
            }
        }
    }
};

[[maybe_unused]] const Crc32cTables& crcTables() {  // unused with hardware CRC32C
    static const Crc32cTables tables;
    return tables;
}

inline uint64_t load64le(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}

inline uint32_t load32le(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}

}  // namespace

#if !defined(__wasm__) && defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#  include <arm_acle.h>
#  define PS_HW_CRC32C 1
#elif !defined(__wasm__) && defined(__x86_64__) && defined(__SSE4_2__)
#  include <nmmintrin.h>
#  define PS_HW_CRC32C 2
#endif

uint32_t crc32c(uint32_t crc, const void* data, size_t len) {
#if defined(PS_HW_CRC32C)
    // Same polynomial, same result as the table path (and the wasm artifact).
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t c = ~crc;
    while (len >= 8) {
        uint64_t w;
        std::memcpy(&w, p, 8);
#  if PS_HW_CRC32C == 1
        c = __crc32cd(c, w);
#  else
        c = uint32_t(_mm_crc32_u64(c, w));
#  endif
        p += 8;
        len -= 8;
    }
    while (len--) {
#  if PS_HW_CRC32C == 1
        c = __crc32cb(c, *p++);
#  else
        c = _mm_crc32_u8(c, *p++);
#  endif
    }
    return ~c;
#else
    const auto& T = crcTables().t;
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t c = ~crc;
    while (len && (reinterpret_cast<uintptr_t>(p) & 7)) {
        c = T[0][(c ^ *p++) & 0xff] ^ (c >> 8);
        len--;
    }
    while (len >= 8) {
        const uint64_t w = load64le(p) ^ c;
        c = T[7][w & 0xff] ^ T[6][(w >> 8) & 0xff] ^ T[5][(w >> 16) & 0xff] ^
            T[4][(w >> 24) & 0xff] ^ T[3][(w >> 32) & 0xff] ^ T[2][(w >> 40) & 0xff] ^
            T[1][(w >> 48) & 0xff] ^ T[0][(w >> 56) & 0xff];
        p += 8;
        len -= 8;
    }
    while (len--) c = T[0][(c ^ *p++) & 0xff] ^ (c >> 8);
    return ~c;
#endif
}

// ---------------------------------------------------------------------------
// SHA-256
// ---------------------------------------------------------------------------
namespace {

const uint32_t kSha256K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
    0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
    0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
    0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
    0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
    0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
    0xc67178f2};

inline uint32_t rotr(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }

void sha256Block(uint32_t h[8], const uint8_t* block) {
    uint32_t w[64];
    for (int i = 0; i < 16; i++)
        w[i] = (uint32_t(block[4 * i]) << 24) | (uint32_t(block[4 * i + 1]) << 16) |
               (uint32_t(block[4 * i + 2]) << 8) | uint32_t(block[4 * i + 3]);
    for (int i = 16; i < 64; i++) {
        const uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
        const uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], hh = h[7];
    for (int i = 0; i < 64; i++) {
        const uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
        const uint32_t ch = (e & f) ^ (~e & g);
        const uint32_t t1 = hh + S1 + ch + kSha256K[i] + w[i];
        const uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
        const uint32_t mj = (a & b) ^ (a & c) ^ (b & c);
        const uint32_t t2 = S0 + mj;
        hh = g;
        g = f;
        f = e;
        e = d + t1;
        d = c;
        c = b;
        b = a;
        a = t1 + t2;
    }
    h[0] += a;
    h[1] += b;
    h[2] += c;
    h[3] += d;
    h[4] += e;
    h[5] += f;
    h[6] += g;
    h[7] += hh;
}

}  // namespace

void sha256(const void* data, size_t len, uint8_t out[32]) {
    uint32_t h[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                     0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    const uint8_t* p = static_cast<const uint8_t*>(data);
    size_t rem = len;
    while (rem >= 64) {
        sha256Block(h, p);
        p += 64;
        rem -= 64;
    }
    uint8_t tail[128];
    std::memset(tail, 0, sizeof(tail));
    if (rem) std::memcpy(tail, p, rem);
    tail[rem] = 0x80;
    const size_t tailLen = (rem + 1 + 8 <= 64) ? 64 : 128;
    const uint64_t bits = uint64_t(len) * 8;
    for (int i = 0; i < 8; i++) tail[tailLen - 1 - i] = uint8_t(bits >> (8 * i));
    sha256Block(h, tail);
    if (tailLen == 128) sha256Block(h, tail + 64);
    for (int i = 0; i < 8; i++) {
        out[4 * i] = uint8_t(h[i] >> 24);
        out[4 * i + 1] = uint8_t(h[i] >> 16);
        out[4 * i + 2] = uint8_t(h[i] >> 8);
        out[4 * i + 3] = uint8_t(h[i]);
    }
}

// ---------------------------------------------------------------------------
// XXH64: stable across hosts (explicit little-endian loads), persisted in
// bloom filters, so it must never change.
// ---------------------------------------------------------------------------
namespace {
const uint64_t P1 = 11400714785074694791ull;
const uint64_t P2 = 14029467366897019727ull;
const uint64_t P3 = 1609587929392839161ull;
const uint64_t P4 = 9650029242287828579ull;
const uint64_t P5 = 2870177450012600261ull;
inline uint64_t rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
inline uint64_t round64(uint64_t acc, uint64_t input) {
    acc += input * P2;
    acc = rotl64(acc, 31);
    return acc * P1;
}
inline uint64_t merge64(uint64_t acc, uint64_t val) {
    acc ^= round64(0, val);
    return acc * P1 + P4;
}
}  // namespace

uint64_t hash64(const void* data, size_t len, uint64_t seed) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    const uint8_t* end = p + len;
    uint64_t h;
    if (len >= 32) {
        const uint8_t* limit = end - 32;
        uint64_t v1 = seed + P1 + P2, v2 = seed + P2, v3 = seed, v4 = seed - P1;
        do {
            v1 = round64(v1, load64le(p));
            v2 = round64(v2, load64le(p + 8));
            v3 = round64(v3, load64le(p + 16));
            v4 = round64(v4, load64le(p + 24));
            p += 32;
        } while (p <= limit);
        h = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
        h = merge64(h, v1);
        h = merge64(h, v2);
        h = merge64(h, v3);
        h = merge64(h, v4);
    } else {
        h = seed + P5;
    }
    h += uint64_t(len);
    while (p + 8 <= end) {
        h ^= round64(0, load64le(p));
        h = rotl64(h, 27) * P1 + P4;
        p += 8;
    }
    if (p + 4 <= end) {
        h ^= uint64_t(load32le(p)) * P1;
        h = rotl64(h, 23) * P2 + P3;
        p += 4;
    }
    while (p < end) {
        h ^= (*p++) * P5;
        h = rotl64(h, 11) * P1;
    }
    h ^= h >> 33;
    h *= P2;
    h ^= h >> 29;
    h *= P3;
    h ^= h >> 32;
    return h;
}

}  // namespace ps
}  // namespace flatsql
