// FlatSQL partition store: the writer instance (design §5.2, §6, §7, §10).
//
// Engine = one writer instance: N writer threads, each owning a set of
// partitions and type owners (pinned least-loaded, moved only by HANDOFF),
// a shared slab pool, the registry and a store-global gseq counter (A6).
//
// Durability: a partition's group commit writes frames to d-<seg>, fsyncs,
// writes the meta batch (rows, attributes, L0 index delta, lane deltas, ctl
// records, trailer) to m-<seg>, fsyncs, then pwrites its head. Every dirty
// partition and type of one writer iteration shares the two sync rounds
// (A8). Acks are released only after that (§7).
#ifndef FLATSQL_PS_WRITER_H
#define FLATSQL_PS_WRITER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "flatsql/ps/extract.h"
#include "flatsql/ps/format.h"
#include "flatsql/ps/index.h"
#include "flatsql/ps/io.h"
#include "flatsql/ps/registry.h"
#include "flatsql/ps/ring.h"

namespace flatsql {
namespace ps {

class Engine;
class Writer;
struct TypeOwner;
struct Staged;
struct StagedType;
struct StageScratch;

// ---- configuration -----------------------------------------------------------
struct EngineConfig {
    std::string root;               // store directory; the engine uses <root>/fsql2
    uint32_t writers = 1;           // N writer threads (clamp(cores-2, 1, 16) in production)
    uint32_t syncThreads = 0;       // A8 concurrent sync pool; 0 = auto: clamp(writers, 4, 8)
    uint64_t poolBytes = 192ull << 20;
    uint32_t slabBytes = 64u << 10;
    uint64_t reserveBytes = 16ull << 20;   // control partition / registrations
    uint64_t arenaBytes = 24ull << 20;     // per-writer commit scratch
    uint64_t defaultRingCap = 4ull << 20;
    uint64_t maxEntryBytes = (1ull << 20) + 4096;  // one entry (A27: larger frames are jumbo)
    uint64_t commitBytes = 4ull << 20;     // per partition per commit
    uint32_t commitFrames = 1024;
    uint64_t sealBytes = 64ull << 20;
    uint64_t sealRecords = 1000000;
    int64_t sealAgeMs = 3600 * 1000;
    uint32_t ckptIntervalMs = 250;
    uint64_t ckptMetaBytes = 64u << 10;
    uint32_t mergeL0Blocks = 16;
    uint64_t mergeL0Bytes = 1ull << 20;
    uint64_t mergeMinL0Bytes = 64u << 10;  // below this, wait for more blocks (tiny commits)
    uint32_t mergeHelpers = 1;             // 0 = merges build on the writer thread
    uint64_t zeroFillStep = 1ull << 20;    // A8 zero-fill ahead (0 = off)
    uint32_t noticeQueue = 1024;           // A25 (a full queue drops the notice)
    uint32_t typeCommitRows = 8192;        // rows labeled per type commit
    uint32_t reconcileStep = 4096;         // instances per RECONCILE step
    uint32_t idleReclaimMs = 1000;         // ring slabs of idle partitions
    uint32_t idleCloseMs = 10000;          // handles + accelerators of idle partitions
    uint32_t activeWaitUs = 5000;
    uint32_t idleWaitUs = 50000;
    bool cooperative = false;       // pump mode: no threads (§5.2)
    bool create = true;             // create a fresh store when absent
    bool requireMigrated = true;    // refuse a store without MIGRATED (§10)
    bool freshMarksMigrated = true; // a fresh store is written MIGRATED (A5)
    bool audit = false;             // single-writer audit log (tests, T1 #4)
    uint32_t auditCapacity = 1u << 20;
    bool lockStats = false;         // instrument shared critical sections (T1 #8)
    // Tests (A26): every Nth merge helper job stalls this long before writing,
    // standing in for a SIGSTOPped helper.
    uint64_t testHelperStallNs = 0;
    uint32_t testHelperStallEvery = 0;
    Io* io = nullptr;               // default: the seven imports
    int64_t (*clockMs)(void*) = nullptr;  // injectable wall clock
    void* clockCtx = nullptr;
};

// ---- instrumentation ---------------------------------------------------------
struct LockHist {
    // Hold-time histogram, log2 buckets of nanoseconds (bucket i: [2^i, 2^(i+1))).
    std::atomic<uint64_t> buckets[48];
    std::atomic<uint64_t> maxNs{0};
    std::atomic<uint64_t> count{0};
    LockHist() {
        for (auto& b : buckets) b.store(0);
    }
    void record(uint64_t ns);
    uint64_t percentileNs(double q) const;
};

struct AuditRecord {
    uint32_t pid;
    uint8_t writer;
    uint8_t pad[3];
    uint32_t epoch;
    uint32_t osTid;
    uint64_t firstPseq;
    uint64_t lastPseq;
    uint64_t startNs;
    uint64_t endNs;
};

// ---- staged state lives in the writer arena -------------------------------------
class Arena {
public:
    bool init(size_t bytes);
    ~Arena();
    void reset() { used_ = 0; }
    void* alloc(size_t n, size_t align = 8);
    void truncate(size_t used) {
        if (used < used_) used_ = used;
    }
    uint8_t* at(size_t off) const { return base_ + off; }
    template <typename T>
    T* make() {
        void* p = alloc(sizeof(T), alignof(T) < 8 ? 8 : alignof(T));
        return p ? new (p) T() : nullptr;
    }
    size_t used() const { return used_; }
    size_t capacity() const { return cap_; }
    size_t highWater() const { return high_; }
    size_t remaining() const { return cap_ - used_; }

private:
    uint8_t* base_ = nullptr;
    size_t cap_ = 0;
    size_t used_ = 0;
    size_t high_ = 0;
};

// FIFO allocator over pool slabs (L0 accelerators): allocations are freed in
// the order they were made, so a ring of slabs suffices. No malloc.
class SlabChain {
public:
    static constexpr uint32_t kMaxSlabs = 64;
    void* alloc(SlabPool& pool, size_t n, uint64_t* pos);
    // Frees every slab strictly before the slab holding `pos`.
    void freeBefore(SlabPool& pool, uint64_t pos);
    void freeAll(SlabPool& pool);
    uint32_t slabs() const { return uint32_t(tail_ - head_); }

private:
    uint32_t ids_[kMaxSlabs];
    uint64_t head_ = 0;   // slab sequence numbers
    uint64_t tail_ = 0;
    uint32_t used_ = 0;   // bytes used in the tail slab
    uint32_t slabBytes_ = 0;
};

// In-memory accelerators of one unmerged L0 block (bloom per lookup kind).
struct L0Accel {
    static constexpr int kMaxKinds = 24;
    uint32_t mSeg = 0;
    uint64_t mOff = 0;       // batch offset
    uint64_t firstPseq = 0;
    uint32_t nRows = 0;
    uint32_t batchLen = 0;
    uint64_t chainPos = 0;
    uint64_t l0Off = 0;      // absolute file offset of the L0 block
    uint32_t l0Len = 0;
    uint8_t nKinds = 0;
    struct Kind {
        uint16_t kind;
        uint8_t vlen;
        uint32_t n;
        uint64_t entriesOff;  // absolute file offset
        uint32_t entriesBytes;
        const uint8_t* bloom; // in the partition's SlabChain (nullptr: no bloom)
        uint32_t bloomBytes;
    } kinds[kMaxKinds];
    const Kind* find(uint16_t k) const {
        for (int i = 0; i < nKinds; i++)
            if (kinds[i].kind == k) return &kinds[i];
        return nullptr;
    }
};

struct SegRun {
    uint32_t gen = 0;
    FileRef file;
    uint64_t fileLen = 0;
    std::unique_ptr<L1Run> run;
};

struct SegmentInfo {
    uint32_t seg = 0;
    bool sealed = false;
    uint64_t firstPseq = 0;
    uint64_t endPseq = 0;     // exclusive; merged rows cover [firstPseq, mergedEnd)
    uint64_t mergedEnd = 0;
    uint64_t dLen = 0;
    uint64_t rLen = 0;
    uint64_t aLen = 0;
    int64_t minEpoch = INT64_MAX;
    int64_t maxEpoch = INT64_MIN;
    std::vector<SegRun> runs;
    FileRef r, a, d, m;       // read handles (lazy)
};

struct Lane {
    uint32_t id = 0;
    std::string provider, source, batch, peer, pubkey;
    LaneCounter c{};
    bool inHead = false;
};

// Partition state published to the type owner (seqlock, same instance).
struct PublishedPart {
    uint64_t commitSeq = 0;
    uint64_t pseqHi = 0;
    uint32_t nL0 = 0;
    L0DirEntry l0[kMaxL0Dir];
};

class SeqLock {
public:
    uint32_t readBegin() const {
        uint32_t s;
        while ((s = seq_.load(std::memory_order_acquire)) & 1) {}
        return s;
    }
    bool readRetry(uint32_t s) const {
        std::atomic_thread_fence(std::memory_order_acquire);
        return seq_.load(std::memory_order_relaxed) != s;
    }
    void writeBegin() {
        seq_.fetch_add(1, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);
    }
    void writeEnd() { seq_.fetch_add(1, std::memory_order_release); }

private:
    std::atomic<uint32_t> seq_{0};
};

struct ReconcileState {
    bool active = false;
    uint64_t rseq = 0;
    uint64_t entryPos = 0;
    uint64_t entryLen = 0;
    std::string provider, source, keep;
    std::vector<uint64_t> instances;  // collected at start (control op)
    size_t next = 0;
    std::vector<uint64_t> affected;   // PUT pseqs that lost an instance
    size_t nextAffected = 0;
    bool phase2 = false;
};

// A merge in flight (design §4.5 + A11), pipelined through commit rounds:
// INTENT rides one batch; outputs are written between rounds; their syncs
// join the next round's first sync phase and MERGE_DONE its second.
enum MergePhase : uint8_t {
    kMergeIdle = 0,
    kMergeIntentQueued,    // INTENT_MERGE queued for the next batch
    kMergeIntentDurable,   // outputs may be written
    kMergeBuilding,        // a helper thread is writing the outputs
    kMergeOutputsWritten,  // outputs written (unsynced); DONE rides the next batch
};

// Snapshot of one segment for the manifest a merge writes (taken on the
// owner at plan time; the helper never reads live partition state).
struct SegSnap {
    uint32_t seg = 0;
    bool sealed = false;
    uint64_t firstPseq = 0, endPseq = 0, mergedEnd = 0, dLen = 0, rLen = 0, aLen = 0;
    int64_t minEpoch = INT64_MAX, maxEpoch = INT64_MIN;
    struct Run {
        uint32_t gen;
        uint64_t nEntries;
        uint64_t fileLen;
    };
    std::vector<Run> runs;
};

struct MergePlan {
    uint32_t seg = 0;
    uint32_t gen = 0;
    uint32_t k = 0;             // L0 batches merged
    uint64_t firstPseq = 0;
    uint64_t through = 0;
    uint64_t rOff = 0;
    uint64_t aOff = 0;
    uint64_t rLen = 0;
    uint64_t aLen = 0;
    int64_t minEpoch = INT64_MAX;
    int64_t maxEpoch = INT64_MIN;
    uint32_t fold = 0;          // newest runs of the segment folded into the new one
    uint64_t segFirstPseq = 0;
    uint32_t ownerEpoch = 0;    // A26: the helper's result is applied only under it
    // A26: the helper re-validates {epoch, OWNED} with an acquire load before
    // every file mutation.
    const std::atomic<uint64_t>* ownerWord = nullptr;
    std::vector<L0DirEntry> batches;      // the merged batches (copy)
    std::vector<MergeRunInput> foldRuns;  // folded runs (immutable while in flight)
    std::vector<SegSnap> snap;            // manifest input
    FileRef r, a, mf;
    SegRun run;                 // the new L1 run, accelerators preloaded
    std::atomic<int32_t> result{0};   // helper: 0 running, 1 done, < 0 error
};

struct PendingKill {
    uint8_t cid[kCidLen];
    std::atomic<int32_t>* remaining;  // type-level delete ticket (may be null)
};

struct Partition {
    // identity
    uint32_t pid = 0;
    uint8_t fid[4] = {0, 0, 0, 0};
    std::string token;
    TypeOwner* type = nullptr;
    RingDesc* ring = nullptr;
    std::atomic<uint32_t> ownerWriter{0};   // mirror of ring->ownerWordV writer
    std::atomic<uint32_t> handoffTarget{0xff};

    // committed state (owner thread only)
    uint64_t commitSeq = 0;
    uint64_t pseqHi = 0;
    uint32_t mSeg = 0;
    uint64_t mEnd = 0;
    uint32_t dSeg = 0;
    uint64_t dLen = 0;
    uint32_t nextSeg = 1;
    uint64_t segFirstPseq = 1;
    uint64_t segRecords = 0;
    int64_t segOpenedMs = 0;
    uint64_t mergedThrough = 0;
    uint32_t manifestGen = 0;
    uint32_t nextGen = 1;
    uint32_t incarnation = 0;        // of the last committed batch
    Counters counters{};
    uint32_t nextLaneId = 1;
    uint64_t headGen = 0;
    uint64_t metaSinceCkpt = 0;
    uint64_t lastCkptNs = 0;
    uint64_t mExtent = 0;            // zero-filled end of m-<mSeg>
    uint64_t dExtent = 0;
    uint64_t lExtent = 0;
    uint32_t lanesOverflowSeg = 0;   // last LANE_CKPT (more than 32 live lanes)
    uint64_t lanesOverflowOff = 0;
    uint32_t intentSeg = 0, intentGen = 0;
    uint64_t intentROff = 0, intentAOff = 0, intentThrough = 0;
    uint32_t nL0 = 0;
    L0DirEntry l0[kMaxL0Dir];
    std::unique_ptr<L0Accel[]> acc;  // kMaxL0Dir accelerators, only while warm
    SlabChain chain;
    std::vector<SegmentInfo> segs;   // every segment with merged rows (+ active)
    std::vector<Lane> lanes;         // index = position; lane ids in Lane::id
    std::unordered_multimap<uint64_t, uint32_t> laneByHash;  // lane hash -> index
    bool lanesLoaded = false;
    bool warm = false;
    bool sealPending = false;
    bool precreated = false;         // d/m of nextSeg created this incarnation
    bool quarantined = false;
    uint64_t consumePos = 0;         // ring read cursor (>= ring->head)
    uint64_t lastActivityNs = 0;
    ReconcileState rec;
    std::vector<PendingKill> kills;  // type-level kills from the mailbox
    uint8_t mergePhase = kMergeIdle;
    MergePlan mplan;
    uint8_t pendingCtl[256];         // ctl records for the next batch
    uint32_t pendingCtlBytes = 0;
    uint32_t nPendingCtl = 0;
    uint32_t mapAheadPages = 2;      // adaptive ring page supply
    uint32_t ownerEpoch = 1;
    FileRef h, m, d, l;              // owner handles
    FileRef rA, aA;                  // active segment r/a (merge)

    // published (cross-thread)
    SeqLock pubLock;
    PublishedPart pub;
    std::atomic<uint64_t> durablePseqHi{0};
    std::atomic<uint64_t> durableCommitSeq{0};
    std::atomic<uint64_t> durableMEnd{0};     // A4 in-process recovery point
    std::atomic<uint64_t> labeledThrough{0};  // written by the type owner
    std::atomic<uint32_t> pendingWork{0};     // mailbox items queued for this partition

    // staging (valid during one writer iteration)
    Staged* st = nullptr;
};

// ---- type owner ---------------------------------------------------------------
struct TypeOwner {
    uint8_t fid[4] = {0, 0, 0, 0};
    std::shared_ptr<TypeConfig> cfg;
    std::atomic<uint32_t> ownerWriter{0};
    // Append-only partition list, readable without a lock: fixed chunk table,
    // chunks never move (index < nParts is published with release).
    static constexpr uint32_t kChunk = 256;
    static constexpr uint32_t kMaxChunks = 1024;
    std::atomic<Partition**> partChunks[kMaxChunks] = {};
    std::atomic<uint32_t> nParts{0};
    Partition* partAt(uint32_t i) const {
        return partChunks[i / kChunk].load(std::memory_order_acquire)[i % kChunk];
    }

    // committed state (owner writer only)
    uint64_t commitSeq = 0;          // == tcs of the last type commit
    uint64_t gseqHi = 0;
    uint32_t gSeg = 0;
    uint64_t gLen = 0;
    uint64_t gSegFirstGseq = 0;
    uint32_t mSeg = 0;
    uint64_t mEnd = 0;
    uint32_t nextSeg = 1;
    uint64_t arrivalsCount = 0;
    uint64_t firstLiveCount = 0;
    uint64_t firstLiveBytes = 0;
    uint32_t incarnation = 0;
    uint64_t headGen = 0;
    uint32_t nextGen = 1;
    uint64_t metaSinceCkpt = 0;
    uint64_t lastCkptNs = 0;
    uint64_t mExtent = 0;
    uint64_t gExtent = 0;
    uint32_t nL0 = 0;
    TypeL0DirEntry l0[kMaxTypeL0Dir];
    L0Accel acc[kMaxTypeL0Dir];
    SlabChain chain;
    std::vector<SegRun> runs;        // L1 runs of the catalog (x-<gen>.fsx)
    uint8_t mergePhase = 0;          // 0 idle, 1 outputs written (DONE rides the next batch)
    uint32_t mergeGen = 0;
    uint32_t mergeK = 0;
    uint32_t mergeFold = 0;          // newest catalog runs folded into the new one
    std::vector<TypeL0DirEntry> mergeBatches;
    std::vector<MergeRunInput> mergeFoldRuns;
    std::vector<std::pair<uint32_t, uint64_t>> mergeKeepRuns;  // (gen, fileLen) kept in the manifest
    std::atomic<int32_t> mergeResult{0};
    uint64_t mergeThroughCommit = 0;
    SegRun mergeRun;
    FileRef mergeMf;
    std::unordered_map<uint32_t, uint64_t> labeled;  // pid -> labeled_through
    bool warm = false;
    FileRef h, m, g;
    std::unordered_map<uint64_t, FileRef> partM;  // read handles keyed (pid << 32 | mSeg)
    uint32_t manifestGenLoaded = 0;  // catalog manifest (live runs)
    uint64_t labelCkptOff = 0;       // A10 full-label checkpoint batch (> 128 pids)
    uint32_t labelCkptSeg = 0;
    std::atomic<uint64_t> publishedGseqHi{0};
    std::atomic<uint64_t> publishedArrivals{0};

    // notices (A25): bounded MPSC of pids; a full queue drops the notice
    std::unique_ptr<std::atomic<uint32_t>[]> notices;
    uint32_t noticeCap = 0;
    std::atomic<uint64_t> noticeHead{0};
    std::atomic<uint64_t> noticeTail{0};
    std::atomic<uint64_t> noticesDropped{0};
    std::atomic<uint32_t> dirty{0};  // any notice since the last pass

    // pending type-level deletes (mailbox)
    struct Delete {
        uint8_t cid[kCidLen];
        std::atomic<int32_t>* remaining;
    };
    std::vector<Delete> deletes;

    StagedType* st = nullptr;
};

// ---- writer mailbox --------------------------------------------------------------
enum CmdKind : uint32_t {
    kCmdNone = 0,
    kCmdAdoptPartition = 1,   // a = pid, b = epoch
    kCmdRebalance = 2,        // a = pid, b = target writer
    kCmdAdoptType = 3,        // ptr = TypeOwner*
    kCmdKillCid = 4,          // a = pid, ptr = PendingKill*
    kCmdTypeDelete = 5,       // ptr = TypeOwner*, cid in data, remaining ticket
    kCmdStop = 6,
};

struct Cmd {
    uint32_t kind = kCmdNone;
    uint32_t pad = 0;
    uint64_t a = 0;
    uint64_t b = 0;
    void* ptr = nullptr;
    std::atomic<int32_t>* ticket = nullptr;
    uint8_t data[kCidLen];
};

// Bounded MPMC queue (Vyukov). Allocation-free after construction.
class CmdQueue {
public:
    explicit CmdQueue(uint32_t capPow2 = 1024);
    bool push(const Cmd& c);
    bool pop(Cmd* c);
    bool empty() const;

private:
    struct Cell {
        std::atomic<uint64_t> seq;
        Cmd cmd;
    };
    std::unique_ptr<Cell[]> cells_;
    uint64_t mask_;
    alignas(64) std::atomic<uint64_t> enq_{0};
    alignas(64) std::atomic<uint64_t> deq_{0};
};

// ---- sync pool (A8) ---------------------------------------------------------------
struct SyncJob {
    Io* io = nullptr;
    int32_t handle = -1;
    int32_t* result = nullptr;
    std::atomic<uint32_t>* remaining = nullptr;
};

// Fair sync pool (A8): each writer publishes its round's jobs in its own
// slot and always runs its own jobs; pool threads help any writer,
// round-robin, so one writer's slow syncs never queue another's.
class SyncPool {
public:
    static constexpr uint32_t kMaxWriters = 64;
    ~SyncPool();
    void start(uint32_t threads);
    void stop();
    // Runs every job of `writer`'s round; returns when all finished.
    void runAll(uint32_t writer, SyncJob* jobs, size_t n);
    uint32_t threads() const { return uint32_t(threads_.size()); }

private:
    struct alignas(64) Slot {
        std::atomic<uint64_t> claim{0};     // (generation << 32) | next index
        std::atomic<uint32_t> n{0};
        std::atomic<uint32_t> remaining{0};
        SyncJob* jobs = nullptr;
    };
    bool helpOne(Slot& s);
    Slot slots_[kMaxWriters];
    std::atomic<uint32_t> work_{0};
    std::atomic<bool> stop_{false};
    std::vector<std::thread> threads_;
};

// ---- writer -------------------------------------------------------------------------
class Writer {
public:
    Writer(Engine* eng, uint8_t id);
    ~Writer();
    uint8_t id() const { return id_; }
    Engine* engine() const { return eng_; }
    const char* eng_root() const;
    void ring();                      // doorbell (A24)
    bool iterate(bool mayWait);       // one loop iteration; false when stopping
    void threadMain();
    CmdQueue& mailbox() { return mailbox_; }
    std::atomic<uint32_t>& doorbellSeq() { return seq_; }
    std::atomic<uint32_t>& sleeping() { return sleeping_; }
    uint32_t ownedCount() const { return ownedCount_.load(std::memory_order_relaxed); }
    uint64_t heartbeat() const { return heartbeat_.load(std::memory_order_relaxed); }
    Arena& arena() { return arena_; }
    IoCtx& io() { return io_; }
    IoStats& ioStats() { return ioStats_; }
    uint64_t commits() const { return commits_.load(std::memory_order_relaxed); }
    uint64_t syncRounds() const { return syncRounds_.load(std::memory_order_relaxed); }
    uint64_t iterationsWithCommit() const { return iterCommit_.load(std::memory_order_relaxed); }
    uint64_t commitSyncRounds() const { return commitSyncRounds_.load(std::memory_order_relaxed); }
    uint64_t partitionCommitsWithFrames() const { return framedCommits_.load(std::memory_order_relaxed); }
    uint64_t partitionBatches() const { return batches_.load(std::memory_order_relaxed); }
    uint8_t* lookupScratch() { return lookupScratch_.data(); }

private:
    friend class Engine;
    void processMailbox();
    bool stagePartition(Partition* p);
    bool stageTypeOwner(TypeOwner* t);
    void commitRound();
    void maintenance();
    void releaseOwnership(Partition* p, uint8_t target);
    void runJobs();
    void queueSync(const FileRef& f, void* owner, uint8_t ownerKind);
    void flushHeadSyncs();

    Engine* eng_;
    uint8_t id_;
    IoStats ioStats_;
    IoCtx io_;
    Arena arena_;
    CmdQueue mailbox_;
    std::vector<Partition*> owned_;
    std::vector<TypeOwner*> types_;
    std::vector<Partition*> dirty_;
    std::vector<TypeOwner*> dirtyTypes_;
    std::vector<SyncJob> jobs_;
    std::vector<int32_t> jobResults_;
    struct JobOwner {
        void* owner;
        uint8_t kind;  // 1 partition, 2 type
    };
    std::vector<JobOwner> jobOwners_;
    std::vector<std::pair<void*, uint8_t>> headSyncs_;  // checkpoint heads to sync next round
    uint64_t lastCommitNs_ = 0;
    StageScratch* sc_ = nullptr;
    Arena framesArena_;
    std::vector<uint8_t> lookupScratch_;
    size_t rr_ = 0;
    size_t maintRr_ = 0;
    uint64_t lastWorkNs_ = 0;
    std::atomic<uint32_t> seq_{0};
    std::atomic<uint32_t> sleeping_{0};
    std::atomic<uint32_t> ownedCount_{0};
    std::atomic<uint32_t> pinned_{0};   // partitions + types assigned (pinning)
    std::atomic<uint64_t> heartbeat_{0};
    std::atomic<uint64_t> commits_{0};
    std::atomic<uint64_t> syncRounds_{0};
    std::atomic<uint64_t> iterCommit_{0};
    std::atomic<uint64_t> commitSyncRounds_{0};
    std::atomic<uint64_t> framedCommits_{0};
    std::atomic<uint64_t> batches_{0};
    std::thread thread_;
    uint32_t osTid_ = 0;
};

// ---- engine -------------------------------------------------------------------------
struct EngineStats {
    uint64_t commits = 0;
    uint64_t syncRounds = 0;
    uint64_t iterationsWithCommit = 0;
    uint64_t commitSyncRounds = 0;       // sync rounds issued by commit rounds
    uint64_t partitionCommitsWithFrames = 0;
    uint64_t partitionBatches = 0;       // committed partition meta batches
    uint64_t rowsAppended = 0;
    uint64_t dedupeHits = 0;
    uint64_t retags = 0;
    uint64_t tombs = 0;
    uint64_t rejects = 0;
    uint64_t merges = 0;
    uint64_t seals = 0;
    uint64_t typeCommits = 0;
    uint64_t firstLabels = 0;
    uint64_t repeatLabels = 0;
    uint64_t promotions = 0;
    uint64_t mergeNotOwner = 0;          // helper found its epoch revoked (A26)
    uint64_t helperStalls = 0;           // injected helper stalls (tests)
    uint64_t handoffHelperWaits = 0;     // aborts that waited for an in-flight helper
    uint64_t noticesDropped = 0;
    uint64_t framesParsedAtOpen = 0;
    uint64_t openReadBytes = 0;
    uint64_t openDataBytes = 0;
    uint64_t openMetaBytes = 0;
    uint64_t adoptedBatches = 0;
    uint32_t poolSlabsInUse = 0;
    uint32_t poolSlabsPeak = 0;
    uint64_t poolCommittedBytes = 0;
    uint64_t arenaHighWater = 0;
    uint64_t descriptorBytes = 0;
    uint64_t acceleratorBytes = 0;
    uint64_t committedBytes = 0;   // engine-accounted writer memory
};

class Engine {
public:
    static int32_t open(const EngineConfig& cfg, std::unique_ptr<Engine>* out, std::string* err);
    ~Engine();

    int32_t start();                       // threads (not in cooperative mode)
    int32_t stop(uint64_t deadlineMs = 10000);
    // Abandons the engine without any further I/O (crash simulation: the
    // fault host freezes first). Threads are joined; nothing is flushed.
    void abandon();
    // Cooperative mode: one iteration of every writer.
    int32_t pump(uint64_t budgetUs);

    // Registration (durable on return).
    int32_t registerType(const std::vector<uint8_t>& config, std::string* err);
    int32_t registerPartition(const uint8_t* peer, size_t peerLen, const uint8_t fid[4],
                              uint32_t* pid);

    RingDesc* ring(uint32_t pid) const;
    Partition* partition(uint32_t pid) const;
    TypeOwner* type(const uint8_t fid[4]) const;
    uint32_t partitionCount() const { return uint32_t(nParts_.load()); }

    // Ownership (A26): request that pid moves to writer `to`.
    int32_t rebalance(uint32_t pid, uint8_t to);
    // Type-level kill of every live copy (A14). `remaining` reaches 0 once
    // every partition's TOMB is durable (or there were none).
    int32_t deleteCid(const uint8_t fid[4], const uint8_t cid[kCidLen],
                      std::atomic<int32_t>* remaining);

    // Doorbell for a partition's owner (and HANDOFF target, A24).
    void ringOwner(uint32_t pid);

    EngineStats stats() const;
    const EngineConfig& config() const { return cfg_; }
    IoStats& openStats() { return openIoStats_; }
    void totalIo(IoStats* out) const;
    SlabPool& pool() { return pool_; }
    uint32_t writerCount() const { return uint32_t(writers_.size()); }
    Writer* writer(uint32_t i) { return writers_[i].get(); }
    // Partition list of a type (registration lock held).
    void typeAddPartition(TypeOwner* t, Partition* p);
    const std::string& root() const { return cfg_.root; }
    uint32_t incarnation() const { return incarnation_; }
    int64_t nowMs() const { return cfg_.clockMs ? cfg_.clockMs(cfg_.clockCtx) : wallMsNow(); }
    uint64_t allocGseq(uint32_t n) { return gseqNext_.fetch_add(n, std::memory_order_relaxed); }
    uint64_t gseqNext() const { return gseqNext_.load(); }
    bool stopping() const { return stop_.load(std::memory_order_acquire); }
    uint32_t reserveSlabs() const { return reserveSlabs_; }

    // Instrumentation
    LockHist& lockHist() { return lockHist_; }        // registration lock holds
    LockHist& seqlockHist() { return seqlockHist_; }  // partition seqlock writer sections
    LockHist& maintHist() { return maintHist_; }      // writer maintenance step durations
    LockHist& commitHist() { return commitHist_; }    // commit round durations
    std::vector<AuditRecord> auditLog() const;
    void auditAppend(const AuditRecord& r);

    // Counters (relaxed)
    std::atomic<uint64_t> cRows{0}, cDedupe{0}, cRetags{0}, cTombs{0}, cRejects{0}, cMerges{0},
        cSeals{0}, cTypeCommits{0}, cFirst{0}, cRepeat{0}, cPromotions{0}, cMergeNotOwner{0},
        cHelperStalls{0}, cHelperJobs{0}, cHandoffHelperWaits{0};
    uint64_t framesParsedAtOpen = 0;
    uint64_t adoptedBatches = 0;

    // Internals shared by the implementation files.
    SyncPool& syncPool() { return syncPool_; }
    // Maintenance helper: builds merge outputs off the writer threads.
    void submitMaintenance(std::function<void(IoCtx*)> job);
    IoCtx* helperIo() { return helperIo_.get(); }
    std::mutex& regMutex() { return regMutex_; }
    Registry& registry() { return registry_; }
    uint8_t leastLoadedWriter() const;

private:
    friend class Writer;
    Engine() = default;
    static int64_t wallMsNow();
    int32_t openStore(std::string* err);
    int32_t openPartitions(std::string* err);
    int32_t openTypes(std::string* err);
    int32_t loadTypeConfig(const TypeEntry& t, std::string* err);
    Partition* makePartition(const PartitionEntry& e, uint8_t writer);

    EngineConfig cfg_;
    IoStats openIoStats_;
    IoCtx* openIo_ = nullptr;
    std::unique_ptr<IoCtx> openIoHolder_;
    SlabPool pool_;
    uint32_t reserveSlabs_ = 0;
    Registry registry_;
    std::mutex regMutex_;
    std::unordered_map<std::string, uint32_t> pidByKey_;   // token + fid -> pid
    std::unique_ptr<std::atomic<Partition*>[]> parts_;
    uint32_t partsCap_ = 0;
    std::atomic<uint32_t> nParts_{0};
    std::vector<std::unique_ptr<Partition>> partStore_;
    std::vector<std::unique_ptr<TypeOwner>> typeStore_;
    std::unordered_map<uint32_t, TypeOwner*> typeByFid_;
    std::vector<std::unique_ptr<Writer>> writers_;
    SyncPool syncPool_;
    IoStats helperIoStats_;
    std::unique_ptr<IoCtx> helperIo_;
    std::mutex helperMu_;               // maintenance queue (never on a record path)
    std::condition_variable helperCv_;
    std::deque<std::function<void(IoCtx*)>> helperJobs_;
    std::vector<std::thread> helpers_;
    bool helperStop_ = false;
    std::atomic<uint64_t> gseqNext_{1};
    uint32_t incarnation_ = 0;
    std::atomic<bool> stop_{false};
    bool started_ = false;
    bool closed_ = false;
    LockHist lockHist_;
    LockHist seqlockHist_;
    LockHist maintHist_;
    LockHist commitHist_;
    mutable std::mutex auditMutex_;
    std::vector<AuditRecord> audit_;
};

// ---- producer (the Go router's half, natively) -------------------------------
class Producer {
public:
    Producer(Engine* eng, uint32_t pid);
    // Enqueues one entry. Blocks for credits when wait is true, else returns
    // FLATSQL_IO_ERR_BUSY. `sealed` (optional) is the sealed frame for a
    // kEntSealed entry. Returns 0 and the entry's rseq.
    int32_t enqueue(uint16_t kind, uint16_t flags, int64_t arrivalMs, const uint8_t* cid,
                    const uint8_t* attr, uint32_t attrLen, const uint8_t* frame,
                    uint32_t frameLen, uint64_t* rseq, bool wait = true,
                    const uint8_t* sealed = nullptr, uint32_t sealedLen = 0);
    bool acked(uint64_t rseq) const;
    // Waits until rseq is acked. Returns 0, or FLATSQL_IO_ERR_BUSY on timeout.
    int32_t waitAcked(uint64_t rseq, uint64_t timeoutNs);
    // Reject code for rseq if the engine rejected it (0 otherwise).
    int32_t rejectCode(uint64_t rseq);
    uint64_t credits() const;
    RingDesc* ringDesc() const { return ring_; }
    // Times an entry found no credit (ring cap, pool, or unmapped pages).
    uint64_t creditWaits() const { return creditWaits_; }

private:
    Engine* eng_;
    uint32_t pid_;
    RingDesc* ring_;
    std::vector<uint8_t> scratch_;
    std::unordered_map<uint64_t, int32_t> rejects_;
    uint64_t creditWaits_ = 0;
};

// RecordAttr helpers (flatsql_attr.fbs), router side and engine side.
std::vector<uint8_t> buildRecordAttr(const std::string& peerId, const std::string& provider,
                                     const std::string& source, const std::string& batch,
                                     const std::string& contentKey = "",
                                     const std::string& producerPeer = "",
                                     const std::string& producerKey = "",
                                     const std::string& supersedeKey = "",
                                     int64_t sourceTimestamp = 0,
                                     const std::string& licenceKey = "");

}  // namespace ps
}  // namespace flatsql

#endif
