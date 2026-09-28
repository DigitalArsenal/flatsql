// FlatSQL partition store: implementation internals shared by src/ps/*.cpp.
#ifndef FLATSQL_PS_INTERNAL_H
#define FLATSQL_PS_INTERNAL_H

#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>

#include "flatsql/ps/platform.h"
#include "flatsql/ps/writer.h"

namespace flatsql {
namespace ps {

// Tag tuple of one instance (views into a RecordAttr).
struct TagView {
    bool present = false;
    const uint8_t* provider = nullptr; size_t providerLen = 0;
    const uint8_t* source = nullptr; size_t sourceLen = 0;
    const uint8_t* url = nullptr; size_t urlLen = 0;
    const uint8_t* batch = nullptr; size_t batchLen = 0;
    const uint8_t* contentKey = nullptr; size_t contentKeyLen = 0;
    const uint8_t* peer = nullptr; size_t peerLen = 0;
    const uint8_t* pubkey = nullptr; size_t pubkeyLen = 0;
};

struct AttrView {
    bool valid = false;
    int nTags = 0;
    TagView tag;
    const uint8_t* supersedeKey = nullptr;
    size_t supersedeKeyLen = 0;
    bool hasSupersedeKey = false;
    const uint8_t* licenceKey = nullptr;
    size_t licenceKeyLen = 0;
};

// Verifies and views a RecordAttr (flatsql_attr.fbs). len == 0: no attr.
bool parseAttr(const uint8_t* attr, size_t len, AttrView* out);
// A2 tuple identity hash (provider, source, batch, content key, producer
// peer, producer key); 0 is reserved for "no tag".
uint64_t tagTupleHash(const TagView& t);
bool tagTupleEqual(const TagView& a, const TagView& b);
// Lane identity hash: (provider, source, batch, producer peer, producer key).
uint64_t laneHash(const TagView& t);

// Per-writer staging scratch, reused for every partition (one at a time).
struct StageScratch {
    RecRow* rows = nullptr;
    uint32_t capRows = 0;
    uint8_t* attrs = nullptr;
    uint32_t capAttrs = 0;
    StagedEntry* entries = nullptr;
    StagedEntry** order = nullptr;
    uint32_t capEntries = 0;
    uint8_t* keys = nullptr;
    uint32_t capKeys = 0;
    uint8_t* plain = nullptr;      // plaintext of a sealed entry / entry copy
    uint32_t capPlain = 0;
    uint8_t* extract = nullptr;    // extraction scratch
    uint32_t capExtract = 0;
    uint8_t* section = nullptr;    // L0 section reads
    uint32_t capSection = 0;

    // per-partition staging state
    uint32_t nRows = 0;
    uint32_t attrBytes = 0;
    uint32_t nAttr = 0;
    uint32_t nEntries = 0;
    uint32_t keyBytes = 0;

    // cid states touched in this batch
    struct Cid {
        uint8_t key[kCidKeyLen];
        uint64_t putPseq;   // live PUT (0 = none)
        uint32_t putLen;
        int32_t next;
    };
    static constexpr uint32_t kCidCap = 8192;
    static constexpr uint32_t kCidBuckets = 16384;
    Cid* cids = nullptr;
    int32_t* cidBuckets = nullptr;
    uint32_t nCids = 0;

    // kills in this batch: target pseq -> killer pseq
    static constexpr uint32_t kDeadCap = 32768;
    uint64_t* deadKeys = nullptr;   // open addressing, 0 = empty
    uint64_t* deadVals = nullptr;
    uint32_t nDead = 0;

    // tag instances staged in this batch: (put, tagHash) -> instance pseq
    static constexpr uint32_t kInstCap = 16384;
    uint64_t* instPut = nullptr;
    uint64_t* instHash = nullptr;
    uint64_t* instPseq = nullptr;
    uint32_t nInst = 0;

    // Type owner scratch: catalog state of cids touched by one type batch.
    struct TCopy {
        uint32_t pid;
        uint32_t len;
        uint64_t pseq;
        uint64_t tcs;
        uint64_t gseq;
        uint8_t label;
        int32_t next;
    };
    struct TCid {
        uint8_t key[kCidKeyLen];
        int32_t copies;   // head of the TCopy list (-1 = none)
        int32_t next;
    };
    static constexpr uint32_t kTCidCap = 16384;
    static constexpr uint32_t kTCopyCap = 65536;
    TCid* tcids = nullptr;
    int32_t* tcidBuckets = nullptr;
    uint32_t nTCids = 0;
    TCopy* tcopies = nullptr;
    uint32_t nTCopies = 0;
    // Every committed catalog entry of one cid, gathered before resolution:
    // a cid keeps its dead copies until compaction (T3), so it can hold far
    // more entries than live copies. Grows (outside the hot-path count) only
    // for such a cid.
    std::vector<TCopy> gather;
    ArrivalEntry* arrivals = nullptr;
    uint32_t nArrivals = 0;
    static constexpr uint32_t kArrivalCap = 65536;
    RecRow* trows = nullptr;          // partition rows read by the type owner
    static constexpr uint32_t kTRowCap = 4096;

    LaneDelta deltas[256];
    uint32_t nDeltas = 0;
    uint8_t ctl[65536];
    uint32_t ctlBytes = 0;
    uint32_t nCtl = 0;
    uint8_t laneFrames[32768];
    uint32_t laneFrameBytes = 0;
    uint32_t firstNewLaneIndex = 0;

    bool init(const EngineConfig& cfg);
    void freeAll();
    void resetPartition();
};

// Staged batch of one partition (lives in the writer's batch arena).
struct Staged {
    Partition* p = nullptr;
    uint64_t endPos = 0;          // ring position after the last consumed entry
    uint64_t lastRseq = 0;        // highest rseq consumed (acked with this commit)
    bool consumed = false;
    uint64_t firstPseq = 0;
    uint64_t nextPseq = 0;
    uint32_t nRows = 0;
    uint8_t* frames = nullptr;    // in the frames arena
    uint64_t dOff = 0;
    uint32_t dBytes = 0;
    uint32_t dSeg = 0;
    uint8_t* batch = nullptr;     // in the batch arena
    uint32_t batchLen = 0;
    uint64_t mOff = 0;
    uint32_t mSeg = 0;
    uint32_t l0Off = 0;
    uint32_t l0Len = 0;
    uint8_t* laneFrames = nullptr;
    uint32_t laneFrameBytes = 0;
    uint64_t lOff = 0;
    Counters counters{};
    LaneDelta* deltas = nullptr;
    uint32_t nDeltas = 0;
    uint32_t firstNewLaneIndex = 0;
    bool sealAfter = false;       // this batch carries SEAL; switch segment after
    uint64_t laneCkptOff = 0;     // absolute m offset of a LANE_CKPT body (0 = none)
    bool reconcileDone = false;
    bool consumedPendingCtl = false;  // INTENT_MERGE (and friends) ride this batch
    bool mergeDone = false;           // MERGE_DONE rides this batch
    uint64_t intentThrough = 0;   // merge intents / done carried in ctl
    int32_t err = 0;
    bool committed = false;
    uint64_t commitStartNs = 0;
    // kill tickets satisfied by this commit
    std::atomic<int32_t>* tickets[64];
    uint32_t nTickets = 0;
    uint32_t killsTaken = 0;      // type-level kills consumed (popped at publish)
    bool rangeStep = false;       // a TOMB_RANGE step staged TOMBs
    bool rangeDone = false;
    uint64_t rangeNext = 0;
};

// Staged type batch (A10).
struct StagedType {
    TypeOwner* t = nullptr;
    uint64_t commitSeq = 0;
    uint8_t* arrivals = nullptr;  // in the frames arena
    uint32_t nArrivals = 0;
    uint64_t gOff = 0;
    uint32_t gSeg = 0;
    bool gSeal = false;           // A15: the arrivals start segment gSeg (fence entry below)
    ArrivalFence fence{};
    uint64_t fenceOff = 0;
    uint64_t lastGseq = 0;        // last arrival gseq of this batch
    uint64_t firstGseq = 0;
    uint64_t gseqHi = 0;
    uint8_t* batch = nullptr;
    uint32_t batchLen = 0;
    uint64_t mOff = 0;
    uint32_t mSeg = 0;
    uint32_t l0Off = 0;
    uint32_t l0Len = 0;
    uint64_t firstLiveCount = 0;
    uint64_t firstLiveBytes = 0;
    uint64_t arrivalsCount = 0;
    struct Label {
        Partition* p;
        uint64_t through;
    };
    Label* labels = nullptr;
    uint32_t nLabels = 0;
    int32_t err = 0;
    bool committed = false;
    bool mergeDone = false;
    std::atomic<int32_t>* tickets[64];
    uint32_t nTickets = 0;
};

// ---- partition log (partition_log.cpp) -----------------------------------
int32_t partitionWarm(Writer* w, Partition* p);
// Publishes the partition's accounted memory (L1 accelerators, lanes) for
// Engine::stats, which never touches owner-thread structures.
void partitionAccount(Partition* p);
void partitionCool(Writer* w, Partition* p);
bool partitionStage(Writer* w, Partition* p, StageScratch* sc, Arena* frames, Arena* batches);
int32_t partitionReadRow(Writer* w, Partition* p, uint64_t pseq, RecRow* out);
int32_t partitionEnsureFiles(Writer* w, Partition* p);
int32_t partitionWriteHead(Writer* w, Partition* p, bool durable);
void partitionPublish(Writer* w, Partition* p, Staged* st);
void partitionRollback(Writer* w, Partition* p, Staged* st);
int32_t partitionMergeStep(Writer* w, Partition* p);   // maintenance
void partitionMergeApply(Writer* w, Partition* p);     // publish of MERGE_DONE
void partitionMergeAbort(Writer* w, Partition* p);     // handoff / failure
int32_t partitionSwapStep(Writer* w, Partition* p);    // maintenance: SWAP outputs + ctl (T3 seam)
void partitionSwapApply(Writer* w, Partition* p);      // publish of the SWAP batch
void mergeDoneBody(const Partition* p, uint8_t body[40]);
bool partitionWantsMerge(const Engine* e, const Partition* p);
int32_t partitionPrecreate(Writer* w, Partition* p);
int32_t ensureExtent(IoCtx* io, const FileRef& f, uint64_t* extent, uint64_t end, uint64_t step);
void encodePartitionHead(const Partition* p, uint8_t* slot, uint32_t* used, bool durable);
int32_t loadAccelFromBlock(Partition* p, SlabPool& pool, uint32_t idx, const uint8_t* block,
                           uint32_t len);
int32_t partitionLoadLanes(Writer* w, Partition* p);

// ---- type owner (type_owner.cpp) ---------------------------------------------
bool typeStage(Writer* w, TypeOwner* t, StageScratch* sc, Arena* frames, Arena* batches);
void typePublish(Writer* w, TypeOwner* t, StagedType* st);
void typeRollback(Writer* w, TypeOwner* t, StagedType* st);
int32_t typeWriteHead(Writer* w, TypeOwner* t, bool durable);
int32_t typeEnsureFiles(IoCtx* io, Engine* e, TypeOwner* t);
int32_t typeWarm(Writer* w, TypeOwner* t);
int32_t typeMergeStep(Writer* w, TypeOwner* t);
void typePostNotice(TypeOwner* t, uint32_t pid);
void encodeTypeHead(const TypeOwner* t, uint8_t* slot, uint32_t* used, bool durable);

// ---- helpers ---------------------------------------------------------------------
inline uint64_t nowNs() { return monoNs(); }
constexpr uint32_t kBatchOverhead = sizeof(BatchHeader) + sizeof(BatchTrailer);

}  // namespace ps
}  // namespace flatsql

#endif
