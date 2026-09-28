// FlatSQL partition store: reader instances and lanes (design §5.3, §8, §9,
// amended by A12, A18, A21, A28, A30).
//
// A reader instance runs L lane threads. Each lane owns a SQLite connection
// (:memory:, multi-thread mode, the null VFS, temp_store=MEMORY), its own
// arena (SQLITE_CONFIG_MALLOC dispatch), lookaside, file handles and caches.
// Readers share no memory and no lock with the writer instance: they learn
// committed state from files only (ps/snapshot.h).
//
// The mailbox ABI (what the Go router programs against, and what the native
// client below uses) lives in the instance's memory:
//   - a pool of request slots, each a SlotHeader followed by a request area
//     (SQL text, then RB1-encoded parameters) and a result ring (an SPSC
//     byte ring: the lane produces RB1 or raw-stream bytes, the client
//     consumes them and returns space);
//   - a bounded MPMC submission queue of slot indices;
//   - per lane: a doorbell word, a state word, a heartbeat, and the A12
//     announce word (the start time of its oldest running statement).
// A lane never blocks on output: when a statement's ring is full the lane
// parks it (the sqlite3_stmt stays resumable) and serves other slots, up to
// maxParked parked statements (A28). Cancellation is the slot's cancel word,
// polled by the progress handler and every vtab loop every 4 K entries.
#ifndef FLATSQL_PS_LANE_H
#define FLATSQL_PS_LANE_H

#include <pthread.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "flatsql/ps/lane_arena.h"
#include "flatsql/ps/snapshot.h"

struct sqlite3;
struct sqlite3_stmt;

namespace flatsql {
namespace ps {

enum class LaneClass : uint8_t {
    Interactive = 1,  // index-bounded plans only (FLATSQL_NEEDS_BULK otherwise)
    Bulk = 2,         // any plan; OS nice +10; large arenas
    Sandbox = 3,      // untrusted SQL on a dedicated capped lane (A28)
};

struct ReaderConfig {
    std::string root;
    Io* io = nullptr;                    // default: the seven imports
    LaneClass cls = LaneClass::Interactive;
    uint32_t lanes = 2;
    uint64_t arenaBytes = 0;             // per lane; 0 = 8 MiB (interactive/sandbox), 128 MiB (bulk)
    uint64_t cacheBytes = 16ull << 20;   // per instance, split across its lanes
    uint32_t maxParked = 8;              // parked statements per lane (A28)
    uint32_t slots = 0;                  // request slots; 0 = lanes * (maxParked + 1) + 16
    uint32_t reqBytes = 64u << 10;       // per slot: SQL + parameters
    uint32_t ringBytes = 256u << 10;     // per slot: result ring
    uint32_t maxHandlesPerLane = 512;
    uint64_t stackBytes = 2ull << 20;    // lane thread stacks (A30: >= 1 MiB)
    uint32_t lookasideSlotBytes = 256;
    uint32_t lookasideSlots = 512;
    bool verifyFrameCrc = true;
    // A18: untrusted SQL sees <TYPE> bounded to its newest N FIRST-live
    // records (by gseq). Per type name, with a generic default.
    std::unordered_map<std::string, uint64_t> hotWindow = {{"OMM", 400000}};
    uint64_t genericHotWindow = 10000;
    // Sandbox work budget defaults (A28 / §22.4-7: an admission limit that
    // returns the SandboxCaps `timeout` code), per statement.
    uint64_t sandboxMaxRowsExamined = 1000000;
    uint64_t sandboxMaxBytesRead = 256ull << 20;
    uint64_t sandboxMaxVmSteps = 200000000;  // CPU work without reads (e.g. recursive CTEs)
    // The public allow-list (A28): record vtab names only; flatsql_* meta
    // vtabs are never public. Empty = every record vtab.
    std::unordered_set<std::string> sandboxAllowed;
    // Tests: stall each lane's statement start (0 = none).
    uint64_t testStallNs = 0;
};

// ---- the mailbox ABI -------------------------------------------------------------
enum SlotState : uint32_t {
    kSlotFree = 0,
    kSlotClaimed = 1,   // client is writing the request
    kSlotQueued = 2,
    kSlotRunning = 3,
    kSlotParked = 4,    // result ring full: resumable
    kSlotDone = 5,      // status valid; remaining ring bytes are readable
};

enum ReqFlag : uint32_t {
    kReqRawStream = 1,   // output [u32le size][bytes] per BLOB cell, not RB1
    kReqSandbox = 2,     // untrusted SQL: authorizer, single SELECT, work budget
    kReqNoAdmission = 4, // trusted maintenance reads on bulk lanes (never on interactive)
};

struct alignas(64) SlotHeader {
    std::atomic<uint32_t> state;
    std::atomic<uint32_t> cancel;
    std::atomic<uint32_t> outSeq;     // lane -> client doorbell
    std::atomic<uint32_t> spaceSeq;   // client -> lane doorbell
    uint32_t flags;
    uint32_t lane;
    uint64_t reqId;
    uint32_t sqlLen;
    uint32_t paramsLen;
    uint32_t reqCap;
    uint32_t ringCap;
    uint64_t maxRowsExamined;         // 0 = default for the class
    uint64_t maxBytesRead;
    uint64_t maxResultRows;           // 0 = none
    uint64_t maxResultBytes;
    std::atomic<uint64_t> ringHead;   // consumed (client)
    std::atomic<uint64_t> ringTail;   // produced (lane)
    int32_t status;
    uint32_t errLen;
    uint64_t rowsOut;
    uint64_t rowsExamined;
    uint64_t bytesRead;
    uint64_t indexEntries;
    uint64_t submitNs, startNs, endNs;
    char err[256];
};

struct alignas(64) LaneShared {
    std::atomic<uint32_t> doorbell{0};
    std::atomic<uint32_t> state{0};      // 0 idle, 1 busy, 2 stopped
    std::atomic<uint64_t> heartbeat{0};
    std::atomic<uint64_t> announce{0};   // A12: oldest running statement's start (monoNs), 0 = none
    std::atomic<uint32_t> canary{0};     // A30: stack canary check failures
};

// ---- statement context (shared with the vtabs) --------------------------------------
struct StmtCtx {
    uint32_t slot = 0;
    uint32_t flags = 0;
    LaneClass cls = LaneClass::Interactive;
    uint64_t startNs = 0;
    ReadStats stats;
    WorkGuard guard;
    std::shared_ptr<const RegistryView> reg;
    // Snapshots, taken once per statement (§8.9). Type-level reads use
    // partition snapshots taken after their type's snapshot (typeParts),
    // so V_p = min(pseq_hi, labeled_through) never hides a promotion.
    uint64_t snapSeq = 0;
    struct PartEntry {
        uint64_t seq = 0;
        std::shared_ptr<PartSnap> snap;
    };
    struct TypeEntry {
        uint64_t seq = 0;
        std::shared_ptr<TypeSnap> snap;
    };
    std::unordered_map<uint32_t, PartEntry> parts;
    std::unordered_map<uint32_t, PartEntry> typeParts;
    std::unordered_map<uint32_t, TypeEntry> types;
    int32_t vtabStatus = 0;        // a ReaderStatus raised inside a vtab (reported instead of SQLITE_*)
    std::string vtabMessage;
    bool rowEmitted = false;       // §8.8: re-snapshot only before the first row
    uint64_t vmSteps = 0;          // sandbox: VM steps (progress handler, 4 K granularity)
    bool sandbox() const { return (flags & kReqSandbox) || cls == LaneClass::Sandbox; }
};

class ReaderInstance;

// ---- a lane --------------------------------------------------------------------------
class ReaderLane {
public:
    ReaderLane(ReaderInstance* inst, uint32_t id);
    ~ReaderLane();
    uint32_t id() const { return id_; }
    ReaderInstance* instance() const { return inst_; }
    LaneStore& store() { return *store_; }
    StmtCtx* current() { return cur_; }
    LaneClass cls() const;
    sqlite3* db() const { return db_; }
    const ReaderConfig& config() const;
    // Snapshot helpers used by the vtabs (statement-scoped, taken once).
    int32_t part(StmtCtx* s, uint32_t pid, PartSnap** out);
    int32_t type(StmtCtx* s, const uint8_t fid[4], TypeSnap** out);
    // A partition snapshot for type-level reads (taken after the type's).
    int32_t partForType(StmtCtx* s, uint32_t pid, const uint8_t fid[4], PartSnap** out);
    // §8.8: before the first row, a vanished file re-snapshots once.
    void dropSnapshots(StmtCtx* s);
    uint64_t hotWindow(const std::string& typeName) const;
    LaneArena& arena() { return arena_; }
    void runThread();
    // Virtual tables this lane's connection has created (lower case).
    bool hasTable(const char* name) const;
    void noteTable(const std::string& name);

private:
    friend class ReaderInstance;
    struct Active;
    void threadMain();
    int32_t initConnection(std::string* err);
    void closeConnection();
    bool startStatement(uint32_t slot);
    void runStatement(Active* a);       // steps until done or parked
    void finishStatement(Active* a, int32_t status, const std::string& msg);
    bool flushPending(Active* a);       // copies pending output into the ring
    int32_t prepare(Active* a, const char* sql, size_t n, std::string* msg);
    void updateAnnounce();

    ReaderInstance* inst_;
    uint32_t id_;
    std::unique_ptr<LaneStore> store_;
    LaneArena arena_;
    sqlite3* db_ = nullptr;
    StmtCtx* cur_ = nullptr;
    std::vector<std::unique_ptr<Active>> active_;
    std::unordered_set<std::string> tables_;
    std::vector<std::shared_ptr<PartSnap>> retired_;       // dropped before the first row (§8.8)
    std::vector<std::shared_ptr<TypeSnap>> retiredTypes_;
    bool storeOpened_ = false;
    void* stack_ = nullptr;
    size_t stackBytes_ = 0;
    volatile uint64_t* canary_ = nullptr;
    pthread_t thread_{};
    bool started_ = false;
};

// ---- the instance ----------------------------------------------------------------------
struct ReaderStats {
    uint64_t statements = 0;
    uint64_t parks = 0;
    uint64_t needsBulk = 0;
    uint64_t noMem = 0;
    uint64_t snapshotGone = 0;
    uint64_t cancelled = 0;
    uint64_t timeouts = 0;
    uint64_t errors = 0;
};

class ReaderInstance {
public:
    static int32_t open(const ReaderConfig& cfg, std::unique_ptr<ReaderInstance>* out, std::string* err);
    ~ReaderInstance();
    int32_t start();
    // Sets the stop word, wakes every lane, joins them (bounded).
    int32_t stop(uint64_t deadlineMs = 10000);

    const ReaderConfig& config() const { return cfg_; }
    uint32_t laneCount() const { return uint32_t(lanes_.size()); }
    ReaderLane* lane(uint32_t i) { return lanes_[i].get(); }
    LaneShared& laneShared(uint32_t i) { return shared_[i]; }
    std::atomic<uint32_t>& stopWord() { return stop_; }

    // Slots (the mailbox ABI).
    uint32_t slotCount() const { return nSlots_; }
    SlotHeader* slot(uint32_t i) const {
        return reinterpret_cast<SlotHeader*>(slotBase_ + size_t(i) * slotStride_);
    }
    uint8_t* slotReq(uint32_t i) const { return reinterpret_cast<uint8_t*>(slot(i)) + sizeof(SlotHeader); }
    uint8_t* slotRing(uint32_t i) const { return slotReq(i) + cfg_.reqBytes; }
    size_t slotStride() const { return slotStride_; }
    // Queues a CLAIMED slot and rings an idle lane.
    int32_t enqueue(uint32_t slot);
    bool dequeue(uint32_t* slot);
    bool queued() const;
    void wakeIdleLane();
    void wakeLane(uint32_t lane);
    std::atomic<uint32_t>& workSeq() { return workSeq_; }

    // A12: the oldest running statement's start time across every lane
    // (monoNs), or UINT64_MAX when none runs. A file retired by a SWAP at
    // time t may be unlinked once this is > t (checked twice, 60 s apart).
    uint64_t oldestActiveStart() const;

    ReaderStats stats() const;
    std::atomic<uint64_t> cStatements{0}, cParks{0}, cNeedsBulk{0}, cNoMem{0}, cSnapshotGone{0}, cCancelled{0},
        cTimeouts{0}, cErrors{0};

private:
    friend class ReaderLane;
    friend class ReaderClient;
    ReaderInstance() = default;
    ReaderConfig cfg_;
    std::vector<std::unique_ptr<ReaderLane>> lanes_;
    std::unique_ptr<LaneShared[]> shared_;
    uint8_t* slotBase_ = nullptr;
    size_t slotStride_ = 0;
    size_t slotBytes_ = 0;
    uint32_t nSlots_ = 0;
    // Submission queue (Vyukov MPMC of slot indices).
    struct QCell {
        std::atomic<uint64_t> seq;
        uint32_t value;
    };
    std::unique_ptr<QCell[]> q_;
    uint64_t qMask_ = 0;
    alignas(64) std::atomic<uint64_t> qEnq_{0};
    alignas(64) std::atomic<uint64_t> qDeq_{0};
    alignas(64) std::atomic<uint32_t> workSeq_{0};
    std::atomic<uint32_t> stop_{0};
    std::atomic<uint32_t> claimHint_{0};
    bool started_ = false;
};

// ---- the native client (the Go router's half) ------------------------------------------
struct Param {
    enum Type : uint8_t { kNull = 0, kInt = 1, kReal = 2, kText = 3, kBlob = 4 } type = kNull;
    int64_t i = 0;
    double d = 0;
    std::string s;
    static Param null() { return Param(); }
    static Param i64(int64_t v) { Param p; p.type = kInt; p.i = v; return p; }
    static Param real(double v) { Param p; p.type = kReal; p.d = v; return p; }
    static Param text(const std::string& v) { Param p; p.type = kText; p.s = v; return p; }
    static Param blob(const std::string& v) { Param p; p.type = kBlob; p.s = v; return p; }
};

struct Request {
    std::string sql;
    std::vector<Param> params;
    uint32_t flags = 0;
    uint64_t maxRowsExamined = 0;
    uint64_t maxBytesRead = 0;
    uint64_t maxResultRows = 0;
    uint64_t maxResultBytes = 0;
};

struct Outcome {
    int32_t status = 0;
    std::string error;
    uint64_t rowsOut = 0;
    uint64_t rowsExamined = 0;
    uint64_t bytesRead = 0;
    uint64_t indexEntries = 0;
    uint64_t queueNs = 0;   // submit -> start
    uint64_t runNs = 0;     // start -> done
};

class ReaderClient {
public:
    explicit ReaderClient(ReaderInstance* inst) : inst_(inst) {}
    // Claims a slot and queues the request. Waits up to waitNs for a free slot.
    int32_t submit(const Request& req, uint32_t* slot, uint64_t waitNs = 5000000000ull);
    // Reads result bytes: > 0 bytes, 0 at the end, < 0 on timeout (kRsBusy).
    int64_t read(uint32_t slot, uint8_t* dst, size_t cap, uint64_t waitNs);
    void cancel(uint32_t slot);
    // After read() returned 0: the outcome; frees the slot.
    Outcome finish(uint32_t slot);
    // Convenience: runs a statement and returns every output byte.
    Outcome run(const Request& req, std::vector<uint8_t>* out, uint64_t readDelayNs = 0,
                size_t readChunk = 1u << 20);

private:
    ReaderInstance* inst_;
};

// A28: counter reads without a lane, straight from heads (for the Go
// completion poller and the dashboard). Not thread-safe: one per caller.
class CounterReader {
public:
    CounterReader(const std::string& root, Io* io);
    struct PartitionCounters {
        uint32_t pid = 0;
        std::string token, sqlName, typeName;
        uint64_t pseqHi = 0, commitSeq = 0;
        Counters c{};
        bool quarantined = false;
        bool empty = true;
    };
    int32_t partitions(std::vector<PartitionCounters>* out);
    LaneStore& store() { return store_; }

private:
    LaneStore store_;
};

}  // namespace ps
}  // namespace flatsql

#endif
