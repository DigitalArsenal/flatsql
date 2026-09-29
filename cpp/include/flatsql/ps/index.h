// FlatSQL partition store: postings (design §4.3 L0, §4.5 L1).
//
// One generic multimap format serves partitions and type owners: an entry is
// (kind, key, value), sorted by (kind, key, value) with plain byte order.
// Keys use the order-preserving encodings in format.h. A kind's value width
// is fixed (partition kinds: an 8-byte big-endian pseq).
//
// L0 block: one per group commit, inside the meta batch, covered by the
// batch CRC and fsync. Layout:
//   L0Header(32) | per kind: KindHeader(32) entries[pad8] bloom[pad8] | crc u32 pad u32
//   entry = u16 klen | key | value
// Lookup kinds carry a bloom filter (10 bits/key, k=7) so a writer can keep
// only blooms in memory and read a kind section on a hit.
//
// L1 run (x-*.fsx): one per merge. Per kind, 4 KiB blocks of entries
// (never spanning a block), a fence per block (offset, count, live count,
// 23-byte key prefix), a bloom for lookup kinds, then a TOC and a footer.
#ifndef FLATSQL_PS_INDEX_H
#define FLATSQL_PS_INDEX_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "flatsql/ps/format.h"
#include "flatsql/ps/io.h"

namespace flatsql {
namespace ps {

constexpr int kBloomK = 7;
constexpr uint32_t kBloomBitsPerKey = 10;
constexpr uint32_t kL1BlockBytes = 4096;
constexpr uint32_t kFencePrefix = 23;

int keyCmp(const uint8_t* a, size_t an, const uint8_t* b, size_t bn);

bool isLookupKind(uint16_t kind);
uint8_t valueLenOf(uint16_t kind);
uint8_t keyTypeOf(uint16_t kind);

size_t bloomBytesFor(uint32_t nKeys);
void bloomAdd(uint8_t* bits, size_t bytes, const uint8_t* key, size_t klen);
bool bloomTest(const uint8_t* bits, size_t bytes, const uint8_t* key, size_t klen);
// The same test with the key hash computed once (bloomHash) for many blooms.
uint64_t bloomHash(const uint8_t* key, size_t klen);
bool bloomTestHash(const uint8_t* bits, size_t bytes, uint64_t h);

struct StagedEntry {
    uint16_t kind;
    uint16_t klen;
    uint8_t vlen;
    uint8_t pad[3];
    const uint8_t* key;
    const uint8_t* val;
};
// Sorts by (kind, key, value). Allocation-free (in-place introsort).
void sortStaged(StagedEntry** e, size_t n);
// The same order, by kind buckets (tmp: n pointers of scratch): kinds whose
// entries already arrive in (key, value) order cost one linear check.
struct SortKey {
    uint64_t k;
    StagedEntry* e;
};
// kp (n entries of scratch, optional) speeds up buckets with random keys.
void sortStagedBuckets(StagedEntry** e, size_t n, StagedEntry** tmp, SortKey* kp = nullptr);

#pragma pack(push, 1)
struct L0Header {
    uint32_t magic;
    uint16_t nKinds;
    uint16_t pad;
    uint64_t firstPseq;
    uint64_t lastPseq;
    uint32_t blockLen;
    uint32_t pad2;
};
static_assert(sizeof(L0Header) == 32, "L0Header layout");

struct L0KindHeader {
    uint16_t kind;
    uint8_t keyType;
    uint8_t vlen;
    uint32_t n;
    uint32_t entriesBytes;  // unpadded
    uint32_t bloomBytes;    // 0 when the kind has no bloom
    uint32_t sectionBytes;  // header + padded entries + padded bloom
    uint8_t bloomK;
    uint8_t pad[11];
};
static_assert(sizeof(L0KindHeader) == 32, "L0KindHeader layout");

struct L1Header {
    uint32_t magic;
    uint16_t ver;
    uint16_t nKinds;
    uint32_t seg;
    uint32_t gen;
    uint16_t level;
    uint16_t pad;
    uint32_t pad2;
    uint64_t firstPseq;
    uint64_t lastPseq;
    uint64_t nEntries;
    uint64_t pad3[2];
};
static_assert(sizeof(L1Header) == 64, "L1Header layout");

struct L1Fence {
    uint64_t blockOff;
    uint32_t n;
    uint32_t live;
    uint8_t prefixLen;
    uint8_t prefix[kFencePrefix];
};
static_assert(sizeof(L1Fence) == 40, "L1Fence layout");

struct L1TocEntry {
    uint16_t kind;
    uint8_t keyType;
    uint8_t vlen;
    uint32_t nBlocks;
    uint64_t nEntries;
    uint64_t fenceOff;
    uint64_t bloomOff;
    uint32_t bloomBytes;
    uint32_t pad;
};
static_assert(sizeof(L1TocEntry) == 40, "L1TocEntry layout");

struct L1Footer {
    uint64_t tocOff;
    uint32_t tocLen;
    uint32_t tocCrc;      // CRC32C of TOC + fences + blooms (fenceOff.. tocOff+tocLen)
    uint64_t metaOff;     // first fence byte
    uint32_t magic;
    uint32_t crc;         // CRC32C of this footer's first 28 bytes
};
static_assert(sizeof(L1Footer) == 32, "L1Footer layout");
#pragma pack(pop)

// ---- L0 ----------------------------------------------------------------------
// Exact encoded size of an L0 block over sorted entries.
size_t l0BlockSize(StagedEntry* const* e, size_t n);
// Encodes the block into out (l0BlockSize bytes). Returns bytes written.
size_t writeL0Block(uint8_t* out, StagedEntry* const* e, size_t n, uint64_t firstPseq,
                    uint64_t lastPseq);

struct L0KindInfo {
    uint16_t kind;
    uint8_t vlen;
    uint32_t n;
    uint32_t entriesOff;   // relative to block start
    uint32_t entriesBytes;
    uint32_t bloomOff;     // relative to block start
    uint32_t bloomBytes;
};
// Validates the block (magic, bounds, crc). Fills up to maxKinds infos.
bool parseL0Block(const uint8_t* block, size_t len, L0KindInfo* kinds, size_t maxKinds,
                  size_t* nKinds);

// Iterates [u16 klen | key | value] entries of one kind section.
struct EntryIter {
    const uint8_t* p = nullptr;
    const uint8_t* end = nullptr;
    uint8_t vlen = 0;
    bool next(const uint8_t** key, uint16_t* klen, const uint8_t** val);
};

// ---- L1 ----------------------------------------------------------------------
class L1Writer {
public:
    L1Writer(IoCtx* io, FileRef out, uint32_t seg, uint32_t gen, uint16_t level,
             uint64_t firstPseq, uint64_t lastPseq);
    // Kinds must be added in ascending order, entries in (key, value) order.
    // expectedKeys sizes the bloom (an upper bound is fine).
    int32_t beginKind(uint16_t kind, uint64_t expectedKeys);
    int32_t add(const uint8_t* key, uint16_t klen, const uint8_t* val, bool live = true);
    int32_t endKind();
    // Writes fences, blooms, TOC and footer. Returns the file length or < 0.
    int64_t finish();
    uint64_t entries() const { return total_; }

private:
    int32_t flushBlock();
    int32_t flushBuf();
    IoCtx* io_;
    FileRef out_;
    L1Header hdr_{};
    uint64_t off_ = 0;       // file write position of buf_[0]
    std::vector<uint8_t> buf_;
    std::vector<uint8_t> block_;
    uint32_t blockN_ = 0;
    uint32_t blockLive_ = 0;
    uint8_t firstKey_[kFencePrefix];
    uint8_t firstKeyLen_ = 0;
    struct KindAcc {
        L1TocEntry toc{};
        std::vector<L1Fence> fences;
        std::vector<uint8_t> bloom;
    };
    std::vector<KindAcc> kinds_;
    KindAcc* cur_ = nullptr;
    uint64_t total_ = 0;
    int32_t err_ = 0;
};

// In-memory accelerators of one L1 run (TOC, fences, blooms).
class L1Run {
public:
    int32_t load(IoCtx* io, const FileRef& f, uint64_t fileLen);
    bool mayContain(uint16_t kind, const uint8_t* key, size_t klen) const;
    bool mayContainHash(uint16_t kind, uint64_t h) const;
    // Calls visit(key, klen, val) for every entry whose key == key. Reads the
    // candidate blocks into `scratch` (>= kL1BlockBytes). Returns entries
    // visited or < 0 on I/O error.
    template <typename Visit>
    int64_t lookup(IoCtx* io, const FileRef& f, uint16_t kind, const uint8_t* key, size_t klen,
                   uint8_t* scratch, Visit&& visit) const;
    // Streams every entry of a kind in order (merges, reconcile scans).
    template <typename Visit>
    int64_t scanKind(IoCtx* io, const FileRef& f, uint16_t kind, uint8_t* scratch,
                     Visit&& visit) const;
    // Range scan of entries with lo <= key < hi (hi == nullptr: no upper bound).
    template <typename Visit>
    int64_t scanRange(IoCtx* io, const FileRef& f, uint16_t kind, const uint8_t* lo, size_t lol,
                      const uint8_t* hi, size_t hil, uint8_t* scratch, Visit&& visit) const;
    uint64_t entries() const { return nEntries_; }
    uint32_t seg() const { return seg_; }
    uint32_t gen() const { return gen_; }
    uint16_t level() const { return level_; }
    uint64_t memoryBytes() const;
    bool hasKind(uint16_t kind) const { return findKind(kind) != nullptr; }
    std::vector<uint16_t> kinds() const;
    uint64_t kindEntries(uint16_t kind) const;
    // Block offsets of a kind in order (streaming merges).
    const std::vector<L1Fence>* fences(uint16_t kind) const;
    uint8_t kindVlen(uint16_t kind) const;

private:
    struct KindView {
        L1TocEntry toc;
        std::vector<L1Fence> fences;
        std::vector<uint8_t> bloom;
    };
    const KindView* findKind(uint16_t kind) const;
    size_t firstCandidate(const KindView& k, const uint8_t* key, size_t klen) const;
    std::vector<KindView> kinds_;
    uint64_t nEntries_ = 0;
    uint32_t seg_ = 0;
    uint32_t gen_ = 0;
    uint16_t level_ = 0;
};

// Streaming k-way merge of L0 kind sections and existing L1 runs into a new
// L1 run (maintenance only; memory O(inputs x one block)).
struct MergeL0Input {
    const uint8_t* block;
    size_t len;
};
struct MergeRunInput {
    const L1Run* run;
    FileRef file;
};
// Keeps an entry in the output (compaction drops the postings of removed rows).
using PostingFilter = bool (*)(void* ctx, uint16_t kind, const uint8_t* key, uint16_t klen, const uint8_t* val,
                               uint8_t vlen);
// Visits every entry of one kind in the inputs, input by input (T3: the
// type merge's dead-copy pass; the visitor's result is ignored).
int32_t scanKind(IoCtx* io, uint16_t kind, const std::vector<MergeL0Input>& l0s,
                 const std::vector<MergeRunInput>& runs, PostingFilter visit, void* ctx);
// Returns the new file length (< 0 on error); *entries receives the count.
int64_t mergeToL1(IoCtx* io, FileRef out, uint32_t seg, uint32_t gen, uint16_t level, uint64_t first,
                  uint64_t last, const std::vector<MergeL0Input>& l0s, const std::vector<MergeRunInput>& runs,
                  uint64_t* entries, PostingFilter filter = nullptr, void* filterCtx = nullptr);

// Validates a 4 KiB L1 block and iterates its entries.
bool l1BlockValid(const uint8_t* block);
inline EntryIter l1BlockIter(const uint8_t* block, uint8_t vlen) {
    EntryIter it;
    const uint16_t used = getU16(block + 2);
    it.p = block + 4;
    it.end = block + 4 + used;
    it.vlen = vlen;
    return it;
}

int prefixCmp(const uint8_t* key, size_t klen, const L1Fence& f);

template <typename Visit>
int64_t L1Run::lookup(IoCtx* io, const FileRef& f, uint16_t kind, const uint8_t* key, size_t klen,
                      uint8_t* scratch, Visit&& visit) const {
    const KindView* k = findKind(kind);
    if (!k || k->fences.empty()) return 0;
    if (!k->bloom.empty() && !bloomTest(k->bloom.data(), k->bloom.size(), key, klen)) return 0;
    int64_t hits = 0;
    for (size_t b = firstCandidate(*k, key, klen); b < k->fences.size(); b++) {
        if (b > 0 && prefixCmp(key, klen, k->fences[b]) < 0) break;
        const int64_t n = io->read(f, scratch, kL1BlockBytes, k->fences[b].blockOff);
        if (n != int64_t(kL1BlockBytes)) return n < 0 ? n : -1;
        if (!l1BlockValid(scratch)) return -1;
        EntryIter it = l1BlockIter(scratch, k->toc.vlen);
        const uint8_t* ek;
        const uint8_t* ev;
        uint16_t el;
        bool past = false;
        while (it.next(&ek, &el, &ev)) {
            const int c = keyCmp(ek, el, key, klen);
            if (c < 0) continue;
            if (c > 0) { past = true; break; }
            visit(ek, el, ev);
            hits++;
        }
        if (past) break;
    }
    return hits;
}

template <typename Visit>
int64_t L1Run::scanKind(IoCtx* io, const FileRef& f, uint16_t kind, uint8_t* scratch,
                        Visit&& visit) const {
    const KindView* k = findKind(kind);
    if (!k) return 0;
    int64_t n = 0;
    for (const auto& fence : k->fences) {
        const int64_t r = io->read(f, scratch, kL1BlockBytes, fence.blockOff);
        if (r != int64_t(kL1BlockBytes)) return r < 0 ? r : -1;
        if (!l1BlockValid(scratch)) return -1;
        EntryIter it = l1BlockIter(scratch, k->toc.vlen);
        const uint8_t* ek;
        const uint8_t* ev;
        uint16_t el;
        while (it.next(&ek, &el, &ev)) {
            if (!visit(ek, el, ev)) return n;
            n++;
        }
    }
    return n;
}

template <typename Visit>
int64_t L1Run::scanRange(IoCtx* io, const FileRef& f, uint16_t kind, const uint8_t* lo,
                         size_t lol, const uint8_t* hi, size_t hil, uint8_t* scratch,
                         Visit&& visit) const {
    const KindView* k = findKind(kind);
    if (!k || k->fences.empty()) return 0;
    int64_t n = 0;
    for (size_t b = firstCandidate(*k, lo, lol); b < k->fences.size(); b++) {
        if (hi && b > 0 && prefixCmp(hi, hil, k->fences[b]) < 0) break;
        const int64_t r = io->read(f, scratch, kL1BlockBytes, k->fences[b].blockOff);
        if (r != int64_t(kL1BlockBytes)) return r < 0 ? r : -1;
        if (!l1BlockValid(scratch)) return -1;
        EntryIter it = l1BlockIter(scratch, k->toc.vlen);
        const uint8_t* ek;
        const uint8_t* ev;
        uint16_t el;
        while (it.next(&ek, &el, &ev)) {
            if (keyCmp(ek, el, lo, lol) < 0) continue;
            if (hi && keyCmp(ek, el, hi, hil) >= 0) return n;
            if (!visit(ek, el, ev)) return n;
            n++;
        }
    }
    return n;
}

}  // namespace ps
}  // namespace flatsql

#endif
