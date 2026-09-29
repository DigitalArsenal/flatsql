// FlatSQL partition store: L0 blocks, blooms, key helpers (see ps/index.h).
#include <algorithm>

#include "flatsql/ps/index.h"
#include "flatsql/ps/platform.h"

namespace flatsql {
namespace ps {

int keyCmp(const uint8_t* a, size_t an, const uint8_t* b, size_t bn) {
    const size_t n = an < bn ? an : bn;
    const int c = n ? std::memcmp(a, b, n) : 0;
    if (c) return c;
    return an < bn ? -1 : (an > bn ? 1 : 0);
}

bool isLookupKind(uint16_t kind) {
    switch (kind) {
        case kIxCid:
        case kIxSupersede:
        case kIxDead:
        case kIxTagDead:
        case kIxTagOf:
        case kIxLicence:
        case kIxTypeCid:
        case kIxTypeLabel:
        case kIxTypeRehome:
        case kIxTypeGone:
        case kIxTypeRepeat:  // readers test REPEAT before any LABEL lookup
            return true;
        default:
            return false;
    }
}

uint8_t valueLenOf(uint16_t kind) {
    switch (kind) {
        case kIxTypeCid: return 33;     // pid u32, pseq u64, tcs u64, label u8, gseq u64, len u32 (BE)
        case kIxTypeLabel: return 17;   // tcs u64 BE, gseq u64 BE, label u8
        case kIxTypeRehome: return 20;  // tcs u64 BE, pid u32 BE, pseq u64 BE
        case kIxTypeRepeat: return 8;   // tcs u64 BE
        case kIxTypeGone: return 8;     // tcs u64 BE
        default: return 8;              // partition kinds: pseq u64 BE
    }
}

uint8_t keyTypeOf(uint16_t kind) {
    switch (kind) {
        case kIxCid:
        case kIxTypeCid: return kKeyCid;
        case kIxEpoch: return kKeyI64;
        case kIxEpochCid: return kKeyEpochCid;
        case kIxDead:
        case kIxTagDead:
        case kIxSpatial:
        case kIxTypeRehome:
        case kIxTypeGone: return kKeyU64;
        case kIxSourceEpoch:
        case kIxProviderEpoch:
        case kIxObjectEpoch: return kKeyStrI64;
        case kIxTagOf: return kKeyU64U64;
        case kIxTypeLabel:
        case kIxTypeRepeat: return kKeyU32U64;
        default: return kKeyBytes;
    }
}

size_t bloomBytesFor(uint32_t nKeys) {
    uint64_t bits = uint64_t(nKeys) * kBloomBitsPerKey;
    if (bits < 64) bits = 64;
    return size_t((bits + 63) / 64 * 8);
}

void bloomAdd(uint8_t* bits, size_t bytes, const uint8_t* key, size_t klen) {
    const uint64_t h = hash64(key, klen, 0x626c6f6f6d);
    const uint64_t m = uint64_t(bytes) * 8;
    const uint64_t h1 = h & 0xffffffffull;
    const uint64_t h2 = (h >> 32) | 1;
    // bit_i = (h1 + i*h2) mod m, stepped with two divisions instead of K.
    uint64_t bit = h1 % m;
    const uint64_t step = h2 % m;
    for (int i = 0; i < kBloomK; i++) {
        bits[bit >> 3] |= uint8_t(1u << (bit & 7));
        bit += step;
        if (bit >= m) bit -= m;
    }
}

uint64_t bloomHash(const uint8_t* key, size_t klen) { return hash64(key, klen, 0x626c6f6f6d); }

bool bloomTest(const uint8_t* bits, size_t bytes, const uint8_t* key, size_t klen) {
    return bloomTestHash(bits, bytes, bloomHash(key, klen));
}

bool bloomTestHash(const uint8_t* bits, size_t bytes, uint64_t h) {
    const uint64_t m = uint64_t(bytes) * 8;
    const uint64_t h1 = h & 0xffffffffull;
    const uint64_t h2 = (h >> 32) | 1;
    uint64_t bit = h1 % m;
    const uint64_t step = h2 % m;
    for (int i = 0; i < kBloomK; i++) {
        if (!(bits[bit >> 3] & (1u << (bit & 7)))) return false;
        bit += step;
        if (bit >= m) bit -= m;
    }
    return true;
}

namespace {
inline bool stagedLess(const StagedEntry* a, const StagedEntry* b) {
    if (a->kind != b->kind) return a->kind < b->kind;
    const int c = keyCmp(a->key, a->klen, b->key, b->klen);
    if (c) return c < 0;
    return keyCmp(a->val, a->vlen, b->val, b->vlen) < 0;
}
}  // namespace

void sortStaged(StagedEntry** e, size_t n) { std::sort(e, e + n, stagedLess); }

namespace {
inline bool keyValLess(const StagedEntry* a, const StagedEntry* b) {
    const int c = keyCmp(a->key, a->klen, b->key, b->klen);
    if (c) return c < 0;
    return keyCmp(a->val, a->vlen, b->val, b->vlen) < 0;
}
}  // namespace

void sortStagedBuckets(StagedEntry** e, size_t n, StagedEntry** tmp) {
    // Counting pass by kind (few distinct kinds per batch), stable scatter,
    // then each kind on its own: most kinds of a record batch arrive already
    // in (key, value) order (tag postings share one key, values are pseqs in
    // append order), so a linear check skips them; the rest sort alone.
    constexpr size_t kMaxKinds = 64;
    uint16_t kinds[kMaxKinds];
    size_t counts[kMaxKinds];
    size_t nk = 0;
    for (size_t i = 0; i < n; i++) {
        const uint16_t k = e[i]->kind;
        size_t j = 0;
        while (j < nk && kinds[j] != k) j++;
        if (j == nk) {
            if (nk == kMaxKinds) {
                sortStaged(e, n);
                return;
            }
            kinds[nk] = k;
            counts[nk++] = 0;
        }
        counts[j]++;
    }
    // Kinds ascending; offsets in that order.
    size_t order[kMaxKinds];
    for (size_t j = 0; j < nk; j++) order[j] = j;
    std::sort(order, order + nk, [&](size_t a, size_t b) { return kinds[a] < kinds[b]; });
    size_t start[kMaxKinds], fill[kMaxKinds];
    size_t at = 0;
    for (size_t r = 0; r < nk; r++) {
        start[order[r]] = at;
        fill[order[r]] = at;
        at += counts[order[r]];
    }
    for (size_t i = 0; i < n; i++) {
        const uint16_t k = e[i]->kind;
        size_t j = 0;
        while (kinds[j] != k) j++;
        tmp[fill[j]++] = e[i];
    }
    for (size_t j = 0; j < nk; j++) {
        StagedEntry** b = tmp + start[j];
        const size_t m = counts[j];
        bool sorted = true;
        for (size_t i = 1; i < m && sorted; i++)
            if (keyValLess(b[i], b[i - 1])) sorted = false;
        if (!sorted) std::sort(b, b + m, keyValLess);
    }
    std::memcpy(e, tmp, n * sizeof(StagedEntry*));
}

namespace {
// Distinct keys of one kind run [i, j) (bloom sizing).
uint32_t distinctKeys(StagedEntry* const* e, size_t i, size_t j) {
    uint32_t d = 0;
    for (size_t k = i; k < j; k++)
        if (k == i || keyCmp(e[k]->key, e[k]->klen, e[k - 1]->key, e[k - 1]->klen) != 0) d++;
    return d;
}
}  // namespace

size_t l0BlockSize(StagedEntry* const* e, size_t n) {
    size_t total = sizeof(L0Header);
    size_t i = 0;
    while (i < n) {
        size_t j = i;
        size_t bytes = 0;
        while (j < n && e[j]->kind == e[i]->kind) {
            bytes += 2 + e[j]->klen + e[j]->vlen;
            j++;
        }
        total += sizeof(L0KindHeader) + pad8(bytes);
        if (isLookupKind(e[i]->kind)) total += pad8(bloomBytesFor(distinctKeys(e, i, j)));
        i = j;
    }
    return total + 8;  // crc + pad
}

size_t writeL0Block(uint8_t* out, StagedEntry* const* e, size_t n, uint64_t firstPseq,
                    uint64_t lastPseq) {
    L0Header h{};
    h.magic = kMagicL0;
    h.firstPseq = firstPseq;
    h.lastPseq = lastPseq;
    size_t off = sizeof(L0Header);
    uint16_t nKinds = 0;
    size_t i = 0;
    while (i < n) {
        size_t j = i;
        size_t bytes = 0;
        while (j < n && e[j]->kind == e[i]->kind) {
            bytes += 2 + e[j]->klen + e[j]->vlen;
            j++;
        }
        L0KindHeader kh{};
        kh.kind = e[i]->kind;
        kh.keyType = keyTypeOf(e[i]->kind);
        kh.vlen = e[i]->vlen;
        kh.n = uint32_t(j - i);
        kh.entriesBytes = uint32_t(bytes);
        const bool bloom = isLookupKind(e[i]->kind);
        kh.bloomBytes = bloom ? uint32_t(bloomBytesFor(distinctKeys(e, i, j))) : 0;
        kh.bloomK = bloom ? kBloomK : 0;
        kh.sectionBytes = uint32_t(sizeof(L0KindHeader) + pad8(bytes) + pad8(kh.bloomBytes));
        uint8_t* sec = out + off;
        uint8_t* p = sec + sizeof(L0KindHeader);
        for (size_t k = i; k < j; k++) {
            putU16(p, e[k]->klen);
            std::memcpy(p + 2, e[k]->key, e[k]->klen);
            std::memcpy(p + 2 + e[k]->klen, e[k]->val, e[k]->vlen);
            p += 2 + e[k]->klen + e[k]->vlen;
        }
        const size_t padEntries = pad8(bytes) - bytes;
        std::memset(p, 0, padEntries);
        p += padEntries;
        if (bloom) {
            std::memset(p, 0, pad8(kh.bloomBytes));
            for (size_t k = i; k < j; k++) bloomAdd(p, kh.bloomBytes, e[k]->key, e[k]->klen);
        }
        std::memcpy(sec, &kh, sizeof(kh));
        off += kh.sectionBytes;
        nKinds++;
        i = j;
    }
    h.nKinds = nKinds;
    h.blockLen = uint32_t(off + 8);
    std::memcpy(out, &h, sizeof(h));
    const uint32_t crc = crc32c(out, off);
    putU32(out + off, crc);
    putU32(out + off + 4, 0);
    return off + 8;
}

bool parseL0Block(const uint8_t* block, size_t len, L0KindInfo* kinds, size_t maxKinds,
                  size_t* nKinds) {
    *nKinds = 0;
    if (len < sizeof(L0Header) + 8) return false;
    L0Header h;
    std::memcpy(&h, block, sizeof(h));
    if (h.magic != kMagicL0 || h.blockLen != len) return false;
    if (crc32c(block, len - 8) != getU32(block + len - 8)) return false;
    size_t off = sizeof(L0Header);
    for (uint16_t k = 0; k < h.nKinds; k++) {
        if (off + sizeof(L0KindHeader) > len - 8) return false;
        L0KindHeader kh;
        std::memcpy(&kh, block + off, sizeof(kh));
        if (kh.sectionBytes < sizeof(L0KindHeader) || off + kh.sectionBytes > len - 8) return false;
        if (sizeof(L0KindHeader) + pad8(kh.entriesBytes) + pad8(kh.bloomBytes) != kh.sectionBytes)
            return false;
        if (*nKinds < maxKinds) {
            L0KindInfo& info = kinds[*nKinds];
            info.kind = kh.kind;
            info.vlen = kh.vlen;
            info.n = kh.n;
            info.entriesOff = uint32_t(off + sizeof(L0KindHeader));
            info.entriesBytes = kh.entriesBytes;
            info.bloomOff = uint32_t(off + sizeof(L0KindHeader) + pad8(kh.entriesBytes));
            info.bloomBytes = kh.bloomBytes;
            (*nKinds)++;
        }
        off += kh.sectionBytes;
    }
    return true;
}

bool EntryIter::next(const uint8_t** key, uint16_t* klen, const uint8_t** val) {
    if (p + 2 > end) return false;
    const uint16_t kl = getU16(p);
    if (p + 2 + kl + vlen > end) return false;
    *klen = kl;
    *key = p + 2;
    *val = p + 2 + kl;
    p += 2 + kl + vlen;
    return true;
}

}  // namespace ps
}  // namespace flatsql
