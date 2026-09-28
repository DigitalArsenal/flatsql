// FlatSQL partition store: compaction, reclamation and disk accounting
// (design §11, §13, A9, A11, A12, A13; T3). The on-disk pieces shared by the
// writer and the readers live here:
//
//   - manifest version 2 (mf-<gen>.fsm): per segment, the compaction kill
//     bound and the manifest holding its previous file set, the arrival
//     zone (quota order), the last original segment a coalesced output
//     covers, and an EMPTY flag (every row compacted away, no files);
//   - the compacted rows file (c-<seg>-<gen>.fsr): the rows that survived,
//     ascending by pseq, plus a presence directory. pseqs are preserved: a
//     pseq the directory marks absent reads as a VOID row (dead, gone);
//   - retire items: the files a MERGE_DONE, SWAP or meta-segment retirement
//     stopped naming, persisted as the RETIRE set until they are unlinked.
#ifndef FLATSQL_PS_COMPACTION_H
#define FLATSQL_PS_COMPACTION_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "flatsql/ps/format.h"

namespace flatsql {
namespace ps {

// ---- manifest ------------------------------------------------------------------
constexpr uint16_t kManifestVer = 2;
enum ManifestSegFlag : uint32_t {
    kMsegSealed = 1,
    kMsegEmpty = 2,   // compacted with no surviving row: no c-* files, no run
};

#pragma pack(push, 1)
struct ManifestHeader {
    uint32_t magic;
    uint16_t ver;
    uint16_t nSegs;
    uint32_t gen;
    uint32_t pid;
    uint32_t prevGen;      // the manifest this one replaced (0: none)
    uint32_t pad;
};
static_assert(sizeof(ManifestHeader) == 24, "ManifestHeader layout");

struct ManifestSegV1 {     // version 1 (T1/T2), still parsed
    uint32_t seg;
    uint32_t flags;
    uint64_t firstPseq, endPseq, mergedEnd, dLen, rLen, aLen;
    int64_t minEpoch, maxEpoch;
    uint32_t nRuns;
    uint32_t cgen;
};
static_assert(sizeof(ManifestSegV1) == 80, "ManifestSegV1 layout");

struct ManifestSegV2 {
    uint32_t seg;
    uint32_t flags;
    uint64_t firstPseq, endPseq, mergedEnd, dLen, rLen, aLen;
    int64_t minEpoch, maxEpoch;
    uint32_t nRuns;
    uint32_t cgen;         // 0 = d/r/a-<seg>; else c-<seg>-<cgen>.{fsd,fsr,fsa}
    uint64_t killThrough;  // readers below this bound read the previous file set (prevGen)
    uint32_t prevGen;      // manifest holding the previous file set of [seg, lastSeg] (0: none)
    uint32_t lastSeg;      // last original segment covered (a coalesced output), == seg otherwise
    int64_t minArrival, maxArrival;
};
static_assert(sizeof(ManifestSegV2) == 112, "ManifestSegV2 layout");

struct ManifestRun {
    uint32_t gen;
    uint32_t pad;
    uint64_t nEntries;
    uint64_t fileLen;
};
static_assert(sizeof(ManifestRun) == 24, "ManifestRun layout");
#pragma pack(pop)

struct ManifestSegDesc {
    uint32_t seg = 0;
    uint32_t lastSeg = 0;
    bool sealed = false;
    bool empty = false;
    uint32_t cgen = 0;
    uint64_t firstPseq = 0, endPseq = 0, mergedEnd = 0, dLen = 0, rLen = 0, aLen = 0;
    int64_t minEpoch = INT64_MAX, maxEpoch = INT64_MIN;
    int64_t minArrival = INT64_MAX, maxArrival = INT64_MIN;
    uint64_t killThrough = 0;
    uint32_t prevGen = 0;
    std::vector<ManifestRun> runs;
};

struct ManifestDesc {
    uint32_t gen = 0;
    uint32_t pid = 0;
    uint32_t prevGen = 0;
    std::vector<ManifestSegDesc> segs;  // ascending seg
};

// Encodes version 2 (magic, CRC trailer: [u32 crc][u32 pad]).
std::vector<uint8_t> encodeManifest(const ManifestDesc& m);
// Decodes version 1 or 2; false on a bad magic, version, bound or CRC.
bool decodeManifest(const uint8_t* buf, size_t len, ManifestDesc* out);

// ---- compacted rows (c-<seg>-<gen>.fsr) ---------------------------------------------
constexpr uint32_t kMagicCompactRows = fourcc('F', 'S', 'C', 'R');
constexpr uint32_t kCompactBlockPseqs = 512;

#pragma pack(push, 1)
struct CompactRowsHeader {
    uint32_t magic;
    uint16_t ver;
    uint16_t pad;
    uint32_t seg;
    uint32_t gen;
    uint64_t firstPseq;
    uint64_t endPseq;      // exclusive
    uint64_t nRows;        // rows at [64, 64 + 128 n)
    uint64_t dirOff;       // directory: nBlocks x CompactDirBlock
    uint32_t nBlocks;
    uint32_t dirCrc;       // CRC32C of the directory
    uint32_t pad2;
    uint32_t crc;          // CRC32C of this header's first 60 bytes
};
static_assert(sizeof(CompactRowsHeader) == 64, "CompactRowsHeader layout");

// Presence of 512 consecutive pseqs: bit i of bits[i / 64] = pseq
// first + 512 b + i survived; rank = rows before this block.
struct CompactDirBlock {
    uint64_t bits[8];
    uint32_t rank;
    uint32_t pad;
};
static_assert(sizeof(CompactDirBlock) == 72, "CompactDirBlock layout");
#pragma pack(pop)

// The parsed directory of a compacted rows file (immutable; shared).
struct CompactDir {
    uint32_t seg = 0;
    uint32_t gen = 0;
    uint64_t firstPseq = 0;
    uint64_t endPseq = 0;
    uint64_t nRows = 0;
    std::vector<CompactDirBlock> blocks;
    // Row index of pseq, or -1 when the pseq is absent (compacted away).
    int64_t indexOf(uint64_t pseq) const;
    bool contains(uint64_t pseq) const { return indexOf(pseq) >= 0; }
    // Rows present in [a, b).
    uint64_t countIn(uint64_t a, uint64_t b) const;
    uint64_t memoryBytes() const { return blocks.size() * sizeof(CompactDirBlock) + 64; }
    static uint64_t rowOffset(uint64_t index) { return sizeof(CompactRowsHeader) + index * 128; }
};
// Validates a header (magic, CRC) and a directory image against it.
bool parseCompactHeader(const uint8_t* buf, size_t len, CompactRowsHeader* out);
bool parseCompactDir(const CompactRowsHeader& h, const uint8_t* dir, size_t len, CompactDir* out);
// Builds the directory for a sorted list of surviving pseqs in [first, end).
void buildCompactDir(uint64_t first, uint64_t end, const std::vector<uint64_t>& kept,
                     std::vector<CompactDirBlock>* out);

// A pseq removed by compaction reads as this row: kind VOID, never live.
inline void voidRow(RecRow* r, uint64_t pseq, uint32_t seg) {
    std::memset(r, 0, sizeof(*r));
    r->pseq = pseq;
    r->seg = seg;
    r->kind = kRowVoid;
}

// ---- retire items (the RETIRE set, A12) ---------------------------------------------
// letters: 'd' 'r' 'a' 'm' (<letter>-<seg>), 'D' 'R' 'A' (c-<seg>-<gen>.fsd/.fsr/.fsa),
// 'x' (x-<seg>-<gen>.fsx), 'f' (mf-<gen>.fsm).
#pragma pack(push, 1)
struct RetireItem {
    uint8_t letter;
    uint8_t pad;
    uint16_t pad2;
    uint32_t seg;
    uint32_t gen;
    uint32_t size;         // bytes on disk when retired (disk_bytes drops by it at UNLINKED)
};
static_assert(sizeof(RetireItem) == 16, "RetireItem layout");

// Body of kCtlRetire (the whole outstanding set) and kCtlUnlinked (the items
// unlinked since the previous set). A RETIRE body also records the state a
// torn-head rebuild needs once older meta segments are gone (A9).
struct RetireSetHeader {
    uint32_t n;
    uint32_t manifestGen;
    uint64_t mergedThrough;
    uint32_t firstLiveMSeg;
    uint32_t nextGen;
};
static_assert(sizeof(RetireSetHeader) == 24, "RetireSetHeader layout");
#pragma pack(pop)

void retirePath(PathBuf* out, const char* root, uint32_t pid, const RetireItem& it);
inline RetireItem retireItem(char letter, uint32_t seg, uint32_t gen, uint64_t size) {
    RetireItem it{};
    it.letter = uint8_t(letter);
    it.seg = seg;
    it.gen = gen;
    it.size = size > 0xffffffffull ? 0xffffffffu : uint32_t(size);
    return it;
}
inline bool sameFile(const RetireItem& a, const RetireItem& b) {
    return a.letter == b.letter && a.seg == b.seg && a.gen == b.gen;
}

// ---- ctl bodies ---------------------------------------------------------------------
// INTENT_COMPACT{seg u32, segEnd u32, gen u32, pad u32}: outputs c-<seg>-<gen>.*,
// x-<seg>-<gen>.fsx and mf-<gen>.fsm; discarded at open unless a SWAP follows.
// SWAP{seg u32, gen u32, oldCgen u32, segEnd u32}.
constexpr uint16_t kIntentCompactBytes = 16;
constexpr uint16_t kSwapBytes = 16;

}  // namespace ps
}  // namespace flatsql

#endif
