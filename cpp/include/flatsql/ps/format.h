// FlatSQL partition store: on-disk format 2 (design §4, amendments A2, A9,
// A10, A11, A17; implementation notes in docs/PARTITION-STORE.md).
//
// All integers are little-endian; every checksum is CRC32C. Structures are
// packed and copied in and out of byte buffers with memcpy, never referenced
// in place, so unaligned buffers are safe on every host.
#ifndef FLATSQL_PS_FORMAT_H
#define FLATSQL_PS_FORMAT_H

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace flatsql {
namespace ps {

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "format 2 is little-endian");

constexpr uint16_t kFormat = 2;

// ---- magics ---------------------------------------------------------------
constexpr uint32_t fourcc(char a, char b, char c, char d) {
    return uint32_t(uint8_t(a)) | (uint32_t(uint8_t(b)) << 8) | (uint32_t(uint8_t(c)) << 16) |
           (uint32_t(uint8_t(d)) << 24);
}
constexpr uint32_t kMagicStore = fourcc('F', 'S', 'Q', '2');
constexpr uint32_t kMagicMigrated = fourcc('F', 'S', 'Q', 'M');
constexpr uint32_t kMagicHead = fourcc('F', 'S', 'H', '2');
constexpr uint32_t kMagicBatch = fourcc('F', 'S', 'M', 'B');
constexpr uint32_t kMagicTrailer = fourcc('F', 'S', 'M', 'T');
constexpr uint32_t kMagicTypeBatch = fourcc('F', 'S', 'T', 'B');
constexpr uint32_t kMagicL0 = fourcc('F', 'S', 'X', '0');
constexpr uint32_t kMagicL1 = fourcc('F', 'S', 'X', '1');
constexpr uint32_t kMagicL1Footer = fourcc('F', 'S', 'X', 'T');
constexpr uint32_t kMagicManifest = fourcc('F', 'S', 'M', 'F');
constexpr uint32_t kMagicTypeConfig = fourcc('F', 'S', 'T', 'C');
constexpr uint32_t kMagicLane = fourcc('F', 'S', 'L', 'N');

// ---- sizes ----------------------------------------------------------------
constexpr uint32_t kHeadSlotBytes = 4096;
constexpr uint32_t kHeadReadBytes = 1024;  // speculative first read per slot
constexpr uint32_t kCidLen = 36;           // CIDv1 raw sha2-256, binary
constexpr uint32_t kCidKeyLen = 37;        // A17 text-order sort key
constexpr uint32_t kMaxKeyLen = 512;       // longer string keys: prefix + hash
constexpr uint32_t kMaxL0Dir = 48;         // unmerged batches listed in a head
constexpr uint32_t kMaxInlineLanes = 32;
constexpr uint32_t kMaxTypeL0Dir = 48;
constexpr uint32_t kMaxInlineLabels = 128;
constexpr uint32_t kArrivalBytes = 24;

// ---- row kinds and flags (RecRow.kind / .flags) ----------------------------
enum RowKind : uint8_t {
    kRowPut = 1,
    kRowTomb = 2,
    kRowLicence = 3,
    kRowCtl = 4,
    kRowCtlTomb = 5,
    kRowRetag = 6,    // A2: another tag instance of a live PUT (target_pseq)
    kRowTagTomb = 7,  // A2: retires one tag instance (target_pseq = instance)
    kRowVoid = 8,     // T3: never stored; a pseq compaction removed reads as this row (dead)
};

enum RowFlag : uint8_t {
    kRowSealed = 0x01,
    kRowHasAttr = 0x02,
    kRowAttrInM = 0x04,
    kRowSupersedes = 0x08,
    kRowJumbo = 0x10,
    kRowCidVerified = 0x20,
    kRowTxn = 0x40,
    // A PUT whose RecordAttr carries migrated_gseq (store-migrate, design
    // §16.1-5). The type owner reads the gseq from the attribute when it
    // labels the row (PARTITION-STORE.md §31).
    kRowMigratedGseq = 0x80,
};

// ---- index kinds (L0 / L1 postings) ----------------------------------------
// Partition kinds map key -> pseq (8-byte big-endian value).
enum IndexKind : uint16_t {
    kIxCid = 1,            // cid sort key (A17) -> PUT pseq
    kIxEpoch = 2,          // i64 epoch_ms -> pseq
    kIxTagProvider = 3,    // provider_id -> instance pseq
    kIxTagSource = 4,
    kIxTagBatch = 5,
    kIxTagPeer = 6,
    kIxTagPubkey = 7,
    kIxSourceEpoch = 8,    // (source_name, epoch_ms)
    kIxProviderEpoch = 9,  // (provider_id, epoch_ms)
    kIxSupersede = 10,     // stored supersede key -> PUT pseq
    kIxDead = 11,          // u64 target pseq -> killer pseq
    kIxSpatial = 12,       // u64 cell
    kIxText = 13,          // token (T8)
    kIxObjectEpoch = 14,   // (object key, epoch_ms) (A18)
    kIxTagOf = 15,         // (PUT pseq, tag hash) -> instance pseq (A2)
    kIxTagPS = 16,         // (provider_id, source_name) -> instance pseq (RECONCILE)
    kIxLicence = 17,       // licence key -> LICENCE pseq
    kIxTagDead = 18,       // u64 tag-instance pseq -> TAG_TOMB pseq (A2; distinct
                           // from DEAD: a PUT row is also its first instance)
    kIxEpochCid = 19,      // (complemented floor(epoch_ms/1000), A17 cid key) -> PUT pseq:
                           // ascending = the A19 default order (seconds DESC, CID ASC)
    kIxColBase = 0x100,    // COL(n) = kIxColBase + n
    // Type-owner kinds (t/<fid>/): value layouts in type_owner.h.
    kIxTypeCid = 0x200,    // cid sort key -> {pid, pseq, tcs, label, gseq}
    kIxTypeLabel = 0x201,  // (pid, pseq) -> {tcs, gseq, label} (A16)
    kIxTypeRehome = 0x202, // gseq -> {pid, pseq, tcs} (A14)
    kIxTypeRepeat = 0x203, // (pid, pseq) -> {} REPEAT run
    kIxTypeGone = 0x204,   // gseq -> tcs: the gseq's last live copy died (T2: offset
                           // paging and dead-history syncs count dead arrivals by fences)
};

enum KeyType : uint8_t {
    kKeyBytes = 0,
    kKeyI64 = 1,
    kKeyF64 = 2,
    kKeyU64 = 3,
    kKeyStrI64 = 4,
    kKeyCid = 5,
    kKeyU32U64 = 6,
    kKeyU64U64 = 7,
    kKeyEpochCid = 8,
};

// kIxEpochCid key: 8 bytes ~(floor(epoch_ms / 1000) ^ sign) big-endian, then
// the 37-byte CID sort key (A17), so an ascending scan is seconds descending,
// text CID ascending (A19's default order), and copies of one CID are adjacent.
constexpr uint32_t kEpochCidKeyLen = 8 + kCidKeyLen;
inline int64_t epochSecFloor(int64_t ms) { return ms >= 0 ? ms / 1000 : -((-ms + 999) / 1000); }
inline void encEpochSecDesc(uint8_t out[8], int64_t sec) {
    uint64_t v = ~(uint64_t(sec) ^ 0x8000000000000000ull);
    for (int i = 7; i >= 0; i--) {
        out[i] = uint8_t(v);
        v >>= 8;
    }
}

// ---- ctl records inside a meta batch ---------------------------------------
enum CtlKind : uint16_t {
    kCtlSeal = 1,          // {seg u32, d_len u64, end_pseq u64}
    kCtlMergeDone = 2,     // {seg u32, gen u32, through u64, r_len u64, a_len u64, manifest_gen u32}
    kCtlIntentCompact = 3,
    kCtlSwap = 4,
    kCtlRetire = 5,
    kCtlSplit = 6,
    kCtlReconcileDone = 7, // {rseq u64}
    kCtlTxnEnd = 8,        // {txn_id u64}
    kCtlIntentMerge = 9,   // A11 {seg, gen, r_off, a_off, through, first_pseq}
    kCtlUnlinked = 10,     // A12
    kCtlLaneCkpt = 11,     // full lane counter table (overflow past 32 inline)
    kCtlQuarantine = 12,
    // Bodies of kCtlIntentCompact, kCtlSwap, kCtlRetire and kCtlUnlinked: ps/compaction.h.
};

// ---- registry frame kinds (A10, §4.7) --------------------------------------
enum RegistryKind : uint16_t {
    kRegPartitionAdd = 1,
    kRegTypeAdd = 2,
    kRegQuarantine = 3,
    kRegUnquarantine = 4,
    kRegSplit = 5,
    kRegDrop = 6,
    kRegSchemaChange = 7,
    kRegWriterIncarnation = 8,
};

enum HeadKind : uint16_t { kHeadPartition = 1, kHeadType = 2, kHeadRegistry = 3 };
enum HeadFlag : uint32_t { kHeadDurableCkpt = 1, kHeadQuarantined = 2 };

#pragma pack(push, 1)

struct Counters {
    uint64_t totalCount = 0;  // PUT rows ever appended
    uint64_t totalBytes = 0;  // sum(len - 4) of those PUTs
    uint64_t liveCount = 0;   // PUT rows not dead
    uint64_t liveBytes = 0;   // sum(len - 4) of live PUTs (minor 10)
    uint64_t tombCount = 0;   // TOMB rows
    uint64_t diskBytes = 0;   // bytes of the partition's files (§13)
    int64_t minEpoch = INT64_MAX;
    int64_t maxEpoch = INT64_MIN;
    int64_t latestArrival = INT64_MIN;
};
static_assert(sizeof(Counters) == 72, "Counters layout");

// RecRow: 128 bytes, fixed (§4.3 plus A2's RETAG / TAG_TOMB).
struct RecRow {
    uint64_t pseq;
    uint32_t seg;
    uint32_t off;
    uint32_t len;          // includes the 4-byte size prefix
    uint8_t kind;
    uint8_t flags;
    uint8_t fid[4];
    uint8_t cidLen;
    uint8_t pad0;
    uint32_t dataCrc;      // CRC32C of the frame bytes [off, off+len)
    int64_t epochMs;       // COALESCE(payload epoch, arrival_ms)
    int64_t arrivalMs;
    uint64_t targetPseq;   // TOMB/RETAG/TAG_TOMB target; PUT+SUPERSEDES: superseded
    uint64_t attrOff;      // offset in a-<seg> or, with ATTR_IN_M, in m-<seg>
    uint32_t attrLen;
    uint32_t laneId;       // lane of this row's tag instance (0 = none)
    uint8_t cid[36];
    uint64_t supersedeHash;
    uint64_t tagHash;      // A2 tag-tuple identity hash (0 = no tag)
    uint32_t aux;          // CTL: txn id low bits; otherwise 0
};
static_assert(sizeof(RecRow) == 128, "RecRow is 128 bytes");
static_assert(offsetof(RecRow, cid) == 72, "RecRow.cid at 72");
static_assert(offsetof(RecRow, supersedeHash) == 108, "RecRow.supersedeHash at 108");

struct BatchHeader {
    uint32_t magic;
    uint16_t ver;
    uint16_t flags;
    uint64_t commitSeq;
    uint64_t firstPseq;
    uint32_t nRows;
    uint32_t nAttr;
    uint32_t dSeg;
    uint32_t batchLen;     // whole batch including trailer
    uint64_t dOff;
    uint64_t dLen;
    uint32_t attrBytes;
    uint32_t l0Bytes;
};
static_assert(sizeof(BatchHeader) == 64, "BatchHeader is 64 bytes");
// BatchHeader.flags
enum BatchFlag : uint16_t {
    kBatchHasData = 1,
    // A8: written under the commit journal. Its d/l bytes were not synced
    // before it, so open adopts it only when the journal replayed it.
    kBatchJournaled = 2,
};
// TypeBatchHeader.flags bit for the same rule (the other bits live in
// type_owner.cpp).
constexpr uint16_t kTypeBatchJournaled = 8;

struct BatchTrailer {
    uint32_t magic;
    uint32_t batchLen;
    uint64_t commitSeq;
    uint64_t pseqHi;
    uint64_t dCommitted;   // committed end of the batch's data segment
    uint32_t activeSeg;
    uint32_t ownerEpoch;
    Counters counters;     // cumulative after this batch (disk bytes excluded)
    uint32_t incarnation;  // store open counter: stale batches never chain
    uint8_t writerId;
    uint8_t pad[3];
    uint32_t crc;          // CRC32C of header .. trailer[0..crc)
    uint32_t pad2;
};
static_assert(sizeof(BatchTrailer) == 128, "BatchTrailer is 128 bytes");

struct HeadPrefix {
    uint32_t magic;
    uint16_t format;
    uint16_t kind;
    uint64_t gen;
    uint32_t ownerEpoch;
    uint32_t id;           // pid, fid (as u32) or 0
    uint32_t usedLen;      // bytes covered by the slot, crc included
    uint32_t flags;
};
static_assert(sizeof(HeadPrefix) == 32, "HeadPrefix is 32 bytes");

struct L0DirEntry {
    uint32_t mSeg;
    uint32_t nRows;
    uint64_t mOff;
    uint64_t firstPseq;
    uint32_t batchLen;
    uint32_t l0Off;        // offset of the L0 block within the batch
};
static_assert(sizeof(L0DirEntry) == 32, "L0DirEntry is 32 bytes");

struct LaneCounter {
    uint32_t laneId;
    uint32_t pad;
    int64_t count;
    int64_t bytes;
    uint64_t maxPseq;
    uint64_t maxGseq;      // not maintained by the partition writer (0 = absent)
    int64_t firstSeen;
    int64_t updated;
};
static_assert(sizeof(LaneCounter) == 56, "LaneCounter is 56 bytes");

struct LaneDelta {
    uint32_t laneId;
    uint32_t pad;
    int64_t dCount;
    int64_t dBytes;
    uint64_t maxPseq;
    int64_t firstSeen;
    int64_t updated;
};
static_assert(sizeof(LaneDelta) == 48, "LaneDelta is 48 bytes");

struct PartitionHeadFixed {
    HeadPrefix p;
    uint64_t commitSeq;
    uint64_t pseqHi;
    uint32_t mSeg;
    uint32_t incarnation;
    uint64_t mEnd;
    uint32_t dSeg;
    uint32_t nextSeg;
    uint64_t dLen;
    uint64_t mergedThrough;
    uint32_t manifestGen;
    uint16_t nL0;
    uint16_t nLanes;
    uint32_t firstLiveMSeg;
    uint32_t nextGen;
    uint64_t schemaFp;
    uint64_t producerHash;
    Counters counters;
    uint32_t nextLaneId;
    uint32_t lanesOverflowSeg;
    uint64_t segFirstPseq;
    uint32_t intentSeg;    // A11 outstanding merge intent (intentGen 0 = none)
    uint32_t intentGen;
    uint64_t intentROff;
    uint64_t intentAOff;
    uint64_t intentThrough;
    uint64_t lanesOverflowOff;  // LANE_CKPT offset in m-<lanesOverflowSeg> (0 = none)
    uint64_t rLen;         // committed r-<dSeg> length
    uint64_t aLen;         // committed a-<dSeg> length
    // T3: outstanding compaction intent (A11 rule; cIntentGen 0 = none): its
    // outputs are c-<cIntentSeg>-<gen>.*, x-<cIntentSeg>-<gen>.fsx, mf-<gen>.fsm.
    uint32_t cIntentSeg;
    uint32_t cIntentGen;
    // T3 (A12): the latest RETIRE set record, in m-<retireSeg> at retireOff
    // (retireN items; 0 = nothing retired and not yet unlinked).
    uint32_t retireSeg;
    uint32_t retireN;
    uint64_t retireOff;
};
static_assert(sizeof(PartitionHeadFixed) == 288, "PartitionHeadFixed layout");

struct TypeHeadFixed {
    HeadPrefix p;
    uint64_t commitSeq;
    uint64_t gseqHi;       // published: every gseq <= this is durable
    uint32_t gSeg;
    uint32_t incarnation;
    uint64_t gLen;         // arrivals bytes in g-<gSeg>
    uint32_t mSeg;
    uint32_t nextSeg;
    uint64_t mEnd;
    uint64_t arrivalsCount;
    uint32_t manifestGen;
    uint16_t nL0;
    uint16_t nLabels;
    uint64_t firstLiveCount;
    uint64_t firstLiveBytes;
    uint64_t labelCkptOff; // A10: labeled_through checkpoint block (0 = inline)
    uint32_t labelCkptSeg;
    uint32_t nextGen;
    uint64_t mergedThroughCommit;
    uint64_t gSegFirstGseq;
    uint64_t tcsHi;        // highest type commit sequence (== commitSeq)
    uint32_t firstLiveMSeg; // T3 (A9): type meta segments below this are retired
    uint32_t reserved;
};
static_assert(sizeof(TypeHeadFixed) == 160, "TypeHeadFixed layout");

struct TypeL0DirEntry {
    uint32_t mSeg;
    uint32_t l0Len;
    uint64_t mOff;
    uint64_t commitSeq;
    uint32_t batchLen;
    uint32_t l0Off;
};
static_assert(sizeof(TypeL0DirEntry) == 32, "TypeL0DirEntry is 32 bytes");

struct LabelEntry {
    uint32_t pid;
    uint32_t pad;
    uint64_t labeledThrough;
};
static_assert(sizeof(LabelEntry) == 16, "LabelEntry is 16 bytes");

struct RegistryHeadFixed {
    HeadPrefix p;
    uint64_t frameCount;
    uint64_t fslEnd;
    uint32_t maxPid;
    uint32_t incarnation;
    uint32_t nTypes;
    uint32_t nPartitions;
};
static_assert(sizeof(RegistryHeadFixed) == 64, "RegistryHeadFixed layout");

struct StoreFile {
    uint32_t magic;
    uint16_t format;
    uint16_t flags;
    uint8_t uuid[16];
    int64_t createdMs;
    uint64_t gseqFloor;
    uint32_t migratedFrom;  // 0 = fresh store, 1 = legacy flatsql
    uint32_t rsv;
    uint64_t rsv2;
    uint32_t crc;
    uint32_t pad;
};
static_assert(sizeof(StoreFile) == 64, "StoreFile layout");

struct MigratedFile {
    uint32_t magic;
    uint16_t format;
    uint16_t rsv;
    uint8_t uuid[16];
    int64_t migratedMs;
    uint32_t crc;
    uint32_t pad;
};
static_assert(sizeof(MigratedFile) == 40, "MigratedFile layout");

// Type batch (A10): arrivals written to g, the batch to the type m log.
struct TypeBatchHeader {
    uint32_t magic;
    uint16_t ver;
    uint16_t flags;
    uint64_t commitSeq;
    uint64_t firstGseq;    // 0 when n_arrivals == 0
    uint32_t nArrivals;
    uint32_t gSeg;
    uint64_t gOff;
    uint32_t gCrc;         // CRC32C of the arrivals bytes [g_off, g_off + 24n)
    uint32_t nLabel;
    uint64_t gseqHi;
    uint32_t batchLen;
    uint32_t l0Off;
    uint32_t incarnation;
    uint32_t mergeGen;             // MERGE_DONE: catalog manifest generation
    uint64_t firstLiveCount;
    uint64_t firstLiveBytes;
    uint64_t arrivalsCount;
    uint64_t mergedThroughCommit;  // MERGE_DONE: L0 blocks of commits <= this are in the run
};
static_assert(sizeof(TypeBatchHeader) == 104, "TypeBatchHeader layout");

struct ArrivalEntry {
    uint64_t gseq;
    uint32_t pid;
    uint16_t flags;        // FIRST=1, PROMOTED=2
    uint16_t rsv;
    uint64_t pseq;
};
static_assert(sizeof(ArrivalEntry) == 24, "ArrivalEntry is 24 bytes");

#pragma pack(pop)

enum ArrivalFlag : uint16_t { kArrivalFirst = 1, kArrivalPromoted = 2 };

// A15: arrivals are segmented (g-<seg>.fsg). t/<fid>/g.fsf is the gseq fence
// index: one entry per sealed segment, entry i describing segment i. Segment s
// holds the gseqs in [first_s, first_{s+1}). An entry is written and fsynced
// in the same round-1 sync as the first arrivals of segment s+1, before the
// type batch that switches segments, so every committed switch has its entry.
struct ArrivalFence {
    uint32_t seg;
    uint32_t rsv;
    uint64_t firstGseq;
    uint64_t lastGseq;
    uint64_t count;        // entries in the sealed segment
    uint32_t crc;          // CRC32C of the bytes before it
    uint32_t pad;
};
static_assert(sizeof(ArrivalFence) == 40, "ArrivalFence is 40 bytes");
constexpr const char* kArrivalFenceName = "g.fsf";

// T3b (A15, arrivals half): a sealed arrivals segment rewritten by a type
// merge without the entries whose GONE postings that merge dropped. Its
// entries live in ga-<gen>.fsg at `off`; the type manifest lists every such
// segment (an appendix after the retire set).
struct ArrOverride {
    uint32_t seg;
    uint32_t gen;          // ga-<gen>.fsg
    uint64_t off;          // byte offset of the segment's entries in that file
    uint64_t count;        // entries left (0: every entry was dead)
    uint64_t firstGseq;    // of the entries left (0 when none)
    uint64_t lastGseq;
};
static_assert(sizeof(ArrOverride) == 40, "ArrOverride is 40 bytes");

// ---- A8 per-writer commit journal (§22.4 ruling 5) --------------------------
// One record per commit round of a writer: every byte the round wrote to
// partition and type files (d, l, m, g, g.fsf, type m) as offset-addressed
// parts. The round fsyncs only the journal; the files themselves are synced
// by an asynchronous checkpoint, after which the journal file is truncated.
// Open replays both journal files of every writer (records in seq order) into
// the files, fsyncs them, then runs the normal durable-tail open.
constexpr uint32_t kMagicJournal = fourcc('F', 'S', 'J', 'R');
constexpr uint32_t kMagicJournalEnd = fourcc('F', 'S', 'J', 'T');
struct JournalRecHeader {
    uint32_t magic;
    uint16_t ver;
    uint16_t flags;
    uint32_t len;          // whole record, trailer included (multiple of 8)
    uint32_t nParts;
    uint64_t seq;          // per writer, strictly increasing across both files
    uint32_t incarnation;
    uint32_t writer;
};
static_assert(sizeof(JournalRecHeader) == 32, "JournalRecHeader layout");
enum JournalFile : uint8_t {
    kJrnData = 1,        // p/<id>/d-<seg>.fsd
    kJrnMeta = 2,        // p/<id>/m-<seg>.fsl
    kJrnLanes = 3,       // p/<id>/l.fsl
    kJrnArrivals = 4,    // t/<fid>/g-<seg>.fsg
    kJrnFence = 5,       // t/<fid>/g.fsf
    kJrnTypeMeta = 6,    // t/<fid>/m-<seg>.fsl
};
struct JournalPart {
    uint8_t file;
    uint8_t rsv[3];
    uint32_t len;          // payload bytes (padded to 8 in the record)
    uint32_t id;           // pid, or the fid bytes
    uint32_t seg;
    uint64_t off;
};
static_assert(sizeof(JournalPart) == 24, "JournalPart layout");
struct JournalTrailer {
    uint32_t crc;          // CRC32C of header + parts
    uint32_t magic;        // kMagicJournalEnd
};

// ---- little-endian helpers -------------------------------------------------
inline void putU16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
inline void putU32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
inline void putU64(uint8_t* p, uint64_t v) { std::memcpy(p, &v, 8); }
inline uint16_t getU16(const uint8_t* p) { uint16_t v; std::memcpy(&v, p, 2); return v; }
inline uint32_t getU32(const uint8_t* p) { uint32_t v; std::memcpy(&v, p, 4); return v; }
inline uint64_t getU64(const uint8_t* p) { uint64_t v; std::memcpy(&v, p, 8); return v; }
inline void putBE64(uint8_t* p, uint64_t v) {
    for (int i = 7; i >= 0; i--) { p[i] = uint8_t(v); v >>= 8; }
}
inline uint64_t getBE64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
    return v;
}
inline void putBE32(uint8_t* p, uint32_t v) {
    for (int i = 3; i >= 0; i--) { p[i] = uint8_t(v); v >>= 8; }
}
inline uint32_t getBE32(const uint8_t* p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; i++) v = (v << 8) | p[i];
    return v;
}
inline size_t pad8(size_t n) { return (n + 7) & ~size_t(7); }

// ---- order-preserving key encodings (§4.3) ---------------------------------
inline void encI64(uint8_t* out, int64_t v) { putBE64(out, uint64_t(v) ^ 0x8000000000000000ull); }
inline int64_t decI64(const uint8_t* in) { return int64_t(getBE64(in) ^ 0x8000000000000000ull); }
void encF64(uint8_t* out, double v);
// Escaped string + 0x00 0x01 terminator + i64: (string, i64) composite.
// Returns bytes written; out must hold 2*len + 10.
size_t encStrI64(uint8_t* out, const uint8_t* s, size_t len, int64_t v);
// Caps a string key at kMaxKeyLen: longer keys keep a prefix + 8-byte hash.
size_t capKey(uint8_t* out, const uint8_t* s, size_t len);

// A17: the CID sort key. Base32-lower text of the binary CID (without the
// multibase 'b'), each symbol replaced by its ASCII rank ('2'-'7' -> 0-5,
// 'a'-'z' -> 6-31), packed 5 bits per symbol. Byte order == text order.
void cidSortKey(const uint8_t cid[kCidLen], uint8_t out[kCidKeyLen]);
void cidFromSortKey(const uint8_t key[kCidKeyLen], uint8_t cid[kCidLen]);
// "b" + base32 lower text of a binary CID (tests and diagnostics).
size_t cidText(const uint8_t* cid, size_t len, char* out /* >= 2 + len*8/5 + 1 */);

// CIDv1 raw sha2-256 of bytes: 0x01 0x55 0x12 0x20 + digest.
void computeCid(const void* data, size_t len, uint8_t out[kCidLen]);

// ---- head slots -------------------------------------------------------------
// Seal a slot image: fills usedLen, gen, crc (last 4 bytes of the used length).
void sealHeadSlot(uint8_t* slot, uint32_t usedLen);
// Validates magic, kind, format, bounds and crc. Returns usedLen or 0.
uint32_t validHeadSlot(const uint8_t* slot, size_t avail, uint16_t kind);

// ---- paths ------------------------------------------------------------------
struct PathBuf {
    char buf[480];
    size_t len = 0;
    const char* c_str() const { return buf; }
};
// <root>/fsql2/<rel>
void pathStore(PathBuf* out, const char* root, const char* rel);
void pathPartition(PathBuf* out, const char* root, uint32_t pid, const char* name);
void pathPartitionSeg(PathBuf* out, const char* root, uint32_t pid, char letter,
                      uint32_t seg, const char* ext);
void pathPartitionRun(PathBuf* out, const char* root, uint32_t pid, uint32_t seg, uint32_t gen);
void pathPartitionManifest(PathBuf* out, const char* root, uint32_t pid, uint32_t gen);
// Compaction outputs (T3 seam): c-<seg:06x>-<gen:04x>.<ext>
void pathPartitionCompact(PathBuf* out, const char* root, uint32_t pid, uint32_t seg, uint32_t gen, const char* ext);
void pathType(PathBuf* out, const char* root, const uint8_t fid[4], const char* name);
void pathTypeSeg(PathBuf* out, const char* root, const uint8_t fid[4], char letter,
                 uint32_t seg, const char* ext);
void pathTypeRun(PathBuf* out, const char* root, const uint8_t fid[4], uint32_t gen);
void pathTypeManifest(PathBuf* out, const char* root, const uint8_t fid[4], uint32_t gen);
void pathTypeArrivalsCompact(PathBuf* out, const char* root, const uint8_t fid[4], uint32_t gen);  // ga-<gen>.fsg
void pathTypeConfig(PathBuf* out, const char* root, const uint8_t fid[4], uint64_t fp);
// A8 commit journal of writer w, file 0 (a) or 1 (b).
void pathJournal(PathBuf* out, const char* root, uint32_t writer, int file);
void pathPartitionDir(PathBuf* out, const char* root, uint32_t pid);
void pathTypeDir(PathBuf* out, const char* root, const uint8_t fid[4]);

inline uint32_t fidU32(const uint8_t fid[4]) { return getU32(fid); }

}  // namespace ps
}  // namespace flatsql

#endif
