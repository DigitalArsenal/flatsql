// FlatSQL partition store: format 2 helpers (see ps/format.h).
#include "flatsql/ps/format.h"

#include <cstdio>

#include "flatsql/ps/platform.h"

namespace flatsql {
namespace ps {

void encF64(uint8_t* out, double v) {
    uint64_t bits;
    std::memcpy(&bits, &v, 8);
    if (bits & 0x8000000000000000ull) bits = ~bits;
    else bits ^= 0x8000000000000000ull;
    putBE64(out, bits);
}

size_t encStrI64(uint8_t* out, const uint8_t* s, size_t len, int64_t v) {
    size_t n = 0;
    for (size_t i = 0; i < len; i++) {
        out[n++] = s[i];
        if (s[i] == 0) out[n++] = 0xff;
    }
    out[n++] = 0x00;
    out[n++] = 0x01;
    encI64(out + n, v);
    return n + 8;
}

size_t capKey(uint8_t* out, const uint8_t* s, size_t len) {
    if (len <= kMaxKeyLen) {
        std::memcpy(out, s, len);
        return len;
    }
    const size_t keep = kMaxKeyLen - 8;
    std::memcpy(out, s, keep);
    putBE64(out + keep, hash64(s, len, 0x6b6579));
    return kMaxKeyLen;
}

namespace {
// Base32 symbol value <-> text-order rank ('2'..'7' sort before 'a'..'z'):
// rank(v) = v >= 26 ? v - 26 : v + 6 (the kRank table below).
inline uint8_t valueOfRank(uint8_t r) { return r < 6 ? uint8_t(r + 26) : uint8_t(r - 6); }
constexpr int kCidSymbols = (kCidLen * 8 + 4) / 5;  // 58
}  // namespace

void cidSortKey(const uint8_t cid[kCidLen], uint8_t out[kCidKeyLen]) {
    // Stream 5-bit base32 groups MSB-first through 64-bit accumulators.
    static const uint8_t kRank[32] = {6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                                      22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 0,  1,  2,  3,  4,  5};
    uint64_t in = 0, outAcc = 0;
    int inBits = 0, outBits = 0;
    size_t ii = 0, oi = 0;
    for (int sym = 0; sym < kCidSymbols; sym++) {
        if (inBits < 5) {
            in = (in << 8) | (ii < kCidLen ? cid[ii] : 0);
            ii++;
            inBits += 8;
        }
        const uint8_t v = uint8_t((in >> (inBits - 5)) & 31);
        inBits -= 5;
        outAcc = (outAcc << 5) | kRank[v];
        outBits += 5;
        while (outBits >= 8) {
            out[oi++] = uint8_t(outAcc >> (outBits - 8));
            outBits -= 8;
        }
    }
    if (outBits) out[oi++] = uint8_t(outAcc << (8 - outBits));
    while (oi < kCidKeyLen) out[oi++] = 0;
}

void cidFromSortKey(const uint8_t key[kCidKeyLen], uint8_t cid[kCidLen]) {
    std::memset(cid, 0, kCidLen);
    for (int i = 0; i < kCidSymbols; i++) {
        uint8_t r = 0;
        for (int b = 0; b < 5; b++) {
            const int bit = 5 * i + b;
            r = uint8_t((r << 1) | ((key[bit >> 3] >> (7 - (bit & 7))) & 1));
        }
        const uint8_t v = valueOfRank(r);
        for (int b = 0; b < 5; b++) {
            const int bit = 5 * i + b;
            if (bit >= int(kCidLen * 8)) break;
            if ((v >> (4 - b)) & 1) cid[bit >> 3] |= uint8_t(1u << (7 - (bit & 7)));
        }
    }
}

size_t cidText(const uint8_t* cid, size_t len, char* out) {
    static const char* kAlphabet = "abcdefghijklmnopqrstuvwxyz234567";
    size_t n = 0;
    out[n++] = 'b';
    const size_t bits = len * 8;
    for (size_t bit = 0; bit < bits; bit += 5) {
        uint8_t v = 0;
        for (int b = 0; b < 5; b++) {
            const size_t k = bit + size_t(b);
            uint8_t x = 0;
            if (k < bits) x = (cid[k >> 3] >> (7 - (k & 7))) & 1;
            v = uint8_t((v << 1) | x);
        }
        out[n++] = kAlphabet[v];
    }
    out[n] = 0;
    return n;
}

void computeCid(const void* data, size_t len, uint8_t out[kCidLen]) {
    out[0] = 0x01;  // CIDv1
    out[1] = 0x55;  // raw
    out[2] = 0x12;  // sha2-256
    out[3] = 0x20;  // 32 bytes
    sha256(data, len, out + 4);
}

void sealHeadSlot(uint8_t* slot, uint32_t usedLen) {
    putU32(slot + offsetof(HeadPrefix, usedLen), usedLen);
    const uint32_t crc = crc32c(slot, usedLen - 4);
    putU32(slot + usedLen - 4, crc);
}

uint32_t validHeadSlot(const uint8_t* slot, size_t avail, uint16_t kind) {
    if (avail < sizeof(HeadPrefix) + 4) return 0;
    HeadPrefix p;
    std::memcpy(&p, slot, sizeof(p));
    if (p.magic != kMagicHead || p.format != kFormat || p.kind != kind) return 0;
    if (p.usedLen < sizeof(HeadPrefix) + 4 || p.usedLen > kHeadSlotBytes || p.usedLen > avail)
        return 0;
    if (crc32c(slot, p.usedLen - 4) != getU32(slot + p.usedLen - 4)) return 0;
    return p.usedLen;
}

// ---- paths ------------------------------------------------------------------
namespace {
void finish(PathBuf* out, int n) {
    out->len = (n < 0) ? 0 : (size_t(n) >= sizeof(out->buf) ? sizeof(out->buf) - 1 : size_t(n));
}
void fidHex(const uint8_t fid[4], char hex[9]) {
    static const char* d = "0123456789abcdef";
    for (int i = 0; i < 4; i++) {
        hex[2 * i] = d[fid[i] >> 4];
        hex[2 * i + 1] = d[fid[i] & 15];
    }
    hex[8] = 0;
}
}  // namespace

void pathStore(PathBuf* out, const char* root, const char* rel) {
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/%s", root, rel));
}
void pathPartitionDir(PathBuf* out, const char* root, uint32_t pid) {
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x", root, pid));
}
void pathPartition(PathBuf* out, const char* root, uint32_t pid, const char* name) {
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/%s", root, pid, name));
}
void pathPartitionSeg(PathBuf* out, const char* root, uint32_t pid, char letter, uint32_t seg,
                      const char* ext) {
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/%c-%06x.%s", root, pid,
                         letter, seg, ext));
}
void pathPartitionRun(PathBuf* out, const char* root, uint32_t pid, uint32_t seg, uint32_t gen) {
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/x-%06x-%04x.fsx", root,
                         pid, seg, gen));
}
void pathPartitionManifest(PathBuf* out, const char* root, uint32_t pid, uint32_t gen) {
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/mf-%06x.fsm", root, pid,
                         gen));
}
void pathTypeDir(PathBuf* out, const char* root, const uint8_t fid[4]) {
    char hex[9];
    fidHex(fid, hex);
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s", root, hex));
}
void pathType(PathBuf* out, const char* root, const uint8_t fid[4], const char* name) {
    char hex[9];
    fidHex(fid, hex);
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/%s", root, hex, name));
}
void pathTypeSeg(PathBuf* out, const char* root, const uint8_t fid[4], char letter, uint32_t seg,
                 const char* ext) {
    char hex[9];
    fidHex(fid, hex);
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/%c-%06x.%s", root, hex,
                         letter, seg, ext));
}
void pathTypeRun(PathBuf* out, const char* root, const uint8_t fid[4], uint32_t gen) {
    char hex[9];
    fidHex(fid, hex);
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/x-%06x.fsx", root, hex, gen));
}
void pathTypeConfig(PathBuf* out, const char* root, const uint8_t fid[4], uint64_t fp) {
    char hex[9];
    fidHex(fid, hex);
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/t/%s/s-%016llx.fsc", root, hex,
                         (unsigned long long)fp));
}

void pathJournal(PathBuf* out, const char* root, uint32_t writer, int file) {
    finish(out, snprintf(out->buf, sizeof(out->buf), "%s/fsql2/j/%02x-%c.fsj", root, writer, file ? 'b' : 'a'));
}

}  // namespace ps
}  // namespace flatsql
