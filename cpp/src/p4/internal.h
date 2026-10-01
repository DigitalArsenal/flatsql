// FlatSQL store format 4 ("p4"): engine internals.
//
// One SQLite file per partition (producer x type) x UTC content month, SQLite
// 3.53.4 unmodified, every file through FlatSQL's own VFS (flatsql_io). The
// design is stack docs/architecture/flatsql-sqlite-partitions.md; the
// interfaces are CONTRACT.md (v2) §1-§4; docs/STORE-FORMAT-4.md is the
// as-built description.
//
// Layout under the root (<data>/fsql4):
//   STORE, MIGRATED                      markers (§2.2)
//   T/TYPES                              registered type names (append-only records with a crc)
//   T/<TYPE>.spec                        the registered spec TLV, plus a crc tag
//   T/<TYPE>.idx                         type index: derived, rebuildable (c, ident, obj, part, file, lanes, srcs, lanecnt, meta)
//   T/<TYPE>.jnl                         intent journal (synchronous=FULL)
//   T/<TYPE>.fts                         FTS5 (maintenance thread)
//   P/<TYPE>/<pid>/<YYYYMM>.<gen>.db     partition file; 0.<gen>.db without a bucket time
//
// Threads: writers (partitions pinned to them), interactive / bulk / sandbox
// lanes, one maintenance thread. Lock order, outermost first: Engine::typesMu,
// Type::dmu, Type::jmu, Type::mu, Engine::wconnMu. Type::flushMu is held only
// by the maintenance thread (and stop) and is never taken under a writer lock.
// No lock is held across a partition file's I/O except by its one writer.
#ifndef FLATSQL_P4_INTERNAL_H
#define FLATSQL_P4_INTERNAL_H

#include <sqlite3.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "flatsql/p4/flatsql_p4.h"
#include "flatsql/p4/p4_reader.h"
#include "flatsql/ps/platform.h"
#include "flatsql/ps/result_block.h"
#include "flatsql/typecfg/extract.h"

struct P4Engine;
struct P4Lane;

namespace flatsql {
namespace p4 {

using Engine = ::P4Engine;
using ps::monoNs;
using ps::wallMs;

// ---- byte order ------------------------------------------------------------------
inline uint16_t ld16(const uint8_t* p) { return uint16_t(p[0] | p[1] << 8); }
inline uint32_t ld32(const uint8_t* p) {
    return uint32_t(p[0]) | uint32_t(p[1]) << 8 | uint32_t(p[2]) << 16 | uint32_t(p[3]) << 24;
}
inline uint64_t ld64(const uint8_t* p) { return uint64_t(ld32(p)) | uint64_t(ld32(p + 4)) << 32; }
inline void st16(uint8_t* p, uint16_t v) { p[0] = uint8_t(v); p[1] = uint8_t(v >> 8); }
inline void st32(uint8_t* p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = uint8_t(v >> (8 * i)); }
inline void st64(uint8_t* p, uint64_t v) { for (int i = 0; i < 8; i++) p[i] = uint8_t(v >> (8 * i)); }

// ---- TLV ([u16 tag][u32 len][value]) ------------------------------------------------
struct Tlv {
    uint16_t tag;
    uint32_t n;
    const uint8_t* v;
};
bool tlvParse(const uint8_t* p, size_t n, std::vector<Tlv>* out);  // false: malformed
const Tlv* tlvFind(const std::vector<Tlv>& v, uint16_t tag);
// Typed reads: false when absent; *bad is set when present with the wrong length.
bool tlvU8(const std::vector<Tlv>& v, uint16_t tag, uint8_t* out, bool* bad);
bool tlvU32(const std::vector<Tlv>& v, uint16_t tag, uint32_t* out, bool* bad);
bool tlvU64(const std::vector<Tlv>& v, uint16_t tag, uint64_t* out, bool* bad);
bool tlvI64(const std::vector<Tlv>& v, uint16_t tag, int64_t* out, bool* bad);
bool tlvText(const std::vector<Tlv>& v, uint16_t tag, std::string* out);
void tlvPut(std::vector<uint8_t>& out, uint16_t tag, const void* p, size_t n);

// ---- CIDs (CIDv1 raw sha2-256 only: 01 55 12 20 + digest) ---------------------------
constexpr size_t kCidBin = 36;
constexpr size_t kCidText = 59;
bool cidBinValid(const uint8_t* cid36);
// The stored 32-byte key: a bijection of the digest whose memcmp order equals
// the base32 text order of the CID (A17).
void cidKeyFromDigest(const uint8_t d[32], uint8_t k[32]);
void cidDigestFromKey(const uint8_t k[32], uint8_t d[32]);
void cidTextFromDigest(const uint8_t d[32], char out[60]);
void cidTextFromKey(const uint8_t k[32], char out[60]);
bool cidDigestFromText(const char* s, size_t n, uint8_t d[32]);

// ---- time ------------------------------------------------------------------------------
inline int64_t floorDiv(int64_t a, int64_t b) { return a >= 0 ? a / b : -((-a + b - 1) / b); }
int64_t monthOfSec(int64_t sec);  // UTC YYYYMM
int64_t nowSec();
void dayText(int64_t sec, char out[11]);  // "YYYY-MM-DD"

// ---- statistics (§3.10; entries only ever appended) ----------------------------------
enum Stat : int {
    kStPuts, kStPutRecords, kStNew, kStCopies, kStRetags, kStDups, kStIdentDups, kStRejects,
    kStCatSuperseded, kStSupersedeTags, kStSupersedeRecords, kStDeletes, kStGroupCommits,
    kStJournalSyncs, kStIndexFlushes, kStIndexFlushEntries, kStPassive, kStRestart, kStTruncate,
    kStWalBytes, kStRebuilds, kStUnlinked, kStQuotaFiles, kStReads, kStReadErrors, kStBusy,
    kStWriterConns, kStReaderConns, kStTwoPhase, kStHeap, kStHeapPeak, kStPendingBytes,
    kStLiveFiles, kStTypes, kStPartitions, kStQuarantined, kStSqlStatements, kStSqlBudgetTrips,
    kStMaxInflight, kStFtsRows, kStCount
};

// ---- config (§3.3) -------------------------------------------------------------------------
struct Config {
    std::string root;
    uint8_t createMode = 0;
    uint32_t writers = 0, interactive = 4, bulk = 1, sandbox = 1;
    uint32_t writeSlots = 16, readSlots = 64;
    uint32_t writeReqBytes = 8u << 20, readReqBytes = 64u << 10, ringBytes = 256u << 10;
    uint64_t gseqFloor = 1;
    uint32_t cores = 4;
    uint64_t engineBytes = 1ull << 30;
    uint32_t writerConns = 64, writerCacheKiB = 4096, readerConns = 256, readerCacheKiB = 512;
    uint64_t pendingBytes = 64ull << 20, softHeap = 512ull << 20, hardHeap = 640ull << 20;
    uint32_t raStreams = 2, raBytes = 1u << 20, passivePages = 65536;
    uint64_t restartBytes = 256ull << 20, walTotal = 1ull << 30, journalSizeLimit = 64ull << 20;
    uint32_t groupRecords = 4096, groupMs = 50, flushEntries = 131072, seqBlock = 1u << 20;
    uint32_t backlogCredit = 16384, rebuildFreePermille = 150;
    uint64_t rebuildMinBytes = 64ull << 20;
    uint8_t quotaMode = 1;
    uint64_t sandboxHeap = 64ull << 20, sandboxRows = 1000000, sandboxBytes = 256ull << 20;
};
int32_t parseConfig(const uint8_t* p, size_t n, Config* c, std::string* err);

// ---- markers (§2.2) ---------------------------------------------------------------------
struct Markers {
    bool storePresent = false, storeValid = false, migratedPresent = false, migratedValid = false;
    uint16_t format = 0, layout = 0, migratedFormat = 0;
    uint8_t uuid[16] = {}, migratedUuid[16] = {};
    int64_t createdMs = 0, writtenMs = 0;
    uint64_t gseqFloor = 0;
    uint32_t migratedFrom = 0;
};
void encodeStore(uint8_t out[64], const uint8_t uuid[16], int64_t createdMs, uint64_t floor, uint32_t from);
void encodeMigrated(uint8_t out[40], const uint8_t uuid[16], int64_t writtenMs);
void decodeMarkers(const uint8_t* store, size_t storeLen, const uint8_t* mig, size_t migLen, Markers* m);

// ---- host file helpers (flatsql_io) -------------------------------------------------------
bool ioExists(const std::string& path);
int32_t ioReadAll(const std::string& path, std::vector<uint8_t>* out);  // P4_E_IO when absent
int32_t ioWriteNew(const std::string& path, const uint8_t* p, size_t n);  // CREATE|TRUNC|CREATE_PARENTS, write, sync
int32_t ioAppend(const std::string& path, const uint8_t* p, size_t n);    // append, sync
int32_t ioTouch(const std::string& path);                                  // create (and parents)
int32_t ioUnlink(const std::string& path);                                 // absent is fine
int64_t ioSize(const std::string& path);                                   // -1 absent

// ---- SQLite connections --------------------------------------------------------------------
enum StmtId : int {
    // partition file
    S_INS, S_RL_INS, S_RL_ONE, S_RL_URL, S_RL_OF, S_RL_DEL_SEQ, S_R_DEL, S_R_ROW, S_R_GET, S_R_LEN, S_R_HOLDER,
    S_ENT_UP, S_ENT_DEC, S_ENT_GONE, S_ENT_ONE, S_SUP_K, S_LANE_UP, S_LANE_GET, S_LANE_DEL, S_SRC_INS,
    S_META_SET, S_META_GET, S_RL_SID_DESC, S_RL_SID_ASC,
    // type index
    S_C_GET, S_C_INS, S_C_DEL, S_IDENT_GET, S_IDENT_INS, S_IDENT_DEL,
    // journal
    S_J_INS, S_J_DEL, S_JM_SET,
    S_COUNT
};
struct Conn {
    sqlite3* db = nullptr;
    sqlite3_stmt* st[S_COUNT] = {};
    std::unordered_map<std::string, sqlite3_stmt*> dyn;  // other statements, by SQL text
    std::string path;
    ~Conn();
    sqlite3_stmt* get(StmtId id);             // reset and cleared; nullptr on a prepare error
    sqlite3_stmt* sql(const std::string& s);  // cached by text; reset and cleared
    int exec(const char* s);
};
const char* stmtSql(StmtId id);
// Every connection is opened through flatsql_io with share=1. Readers add ra=1
// and query_only and never create (a missing file is an error, never empty);
// writers, the journal and the type index add dsync=1.
enum class OpenKind { Writer, Reader, IndexReader, Index, Journal, Maint };
int openConn(const std::string& path, OpenKind kind, uint32_t cacheKiB, uint32_t pageSize, Conn** out,
             std::string* err);
int32_t statusOfSqlite(int rc);  // SQLite result -> P4 status (BUSY and I/O errors are errors, never misses)

// ---- engine objects ------------------------------------------------------------------------
struct Type;
struct Part;
struct File;

struct SrcDef {
    uint32_t id = 0;
    std::string provider, source;
};
struct LaneDef {
    uint32_t id = 0;
    uint32_t sid = 0;
    uint64_t h = 0;  // 63 bits of SHA-256 over the six identity fields (lanecnt key)
    std::string provider, source, batch, ckey, ppeer, pkey;
};
// A lane's counters in one file (the file's lane row), mirrored in memory.
struct LaneCount {
    int64_t n = 0, bytes = 0, minw = INT64_MAX, maxw = INT64_MIN, maxseq = 0;
    int64_t created = 0, updated = 0, maxat = 0, maxts = 0;
    std::string url;   // the latest write's url (SUMMARY 3)
    std::string url0;  // an instance's url when its rl.u is NULL
};

struct File {
    Part* part = nullptr;
    int64_t tb = 0;  // YYYYMM, or 0 without a bucket time
    int32_t gen = 0;
    std::string path;
    // counters (Type::mu); the file's meta and lane rows are the durable copy
    int64_t n = 0, bytes = 0, ncopy = 0, minseq = INT64_MAX, maxseq = 0, minw = INT64_MAX, maxw = INT64_MIN,
            maxts = 0, nnull = 0, mints = INT64_MAX, mine = INT64_MAX, maxe = INT64_MIN;
    std::map<uint32_t, LaneCount> lanes;  // live lanes (n > 0) by lane id
    bool created = false;      // exists on disk with its schema (readers skip it until then)
    bool retired = false;      // dropped or replaced: readers skip it, writers never use it again
    bool quarantined = false;  // corrupt: P4_E_CORRUPT for ops that need it
    bool indexed = true;       // secondary indexes present (false between a migration's append and REBUILD 1)
    bool touched = false;      // changed since the last flush
    bool objRefresh = false;   // rewrite all of its obj rows at the next flush (journal replay)
    // writer connection (Engine::wconnMu)
    Conn* w = nullptr;
    int wPins = 0;
    std::list<File*>::iterator lru;
    bool inLru = false;
    std::atomic<int> users{0};  // reader connections checked out on it now
};

struct WriteTask;

struct Part {
    Type* type = nullptr;
    uint32_t pid = 0;
    std::string producer;  // the producer token (C-13)
    std::string peer;      // the first writer's peer (REC.peer when r.p is NULL, C-2)
    uint32_t owner = 0;    // writer thread index
    std::map<int64_t, File*> files;          // live files by tb (Type::mu)
    std::vector<std::unique_ptr<File>> all;  // every File (live and retired; never freed while the engine runs)
    int64_t n = 0, bytes = 0;
    bool journaled = false;  // its J_PART row is durable
    // backlog (WriterState::mu of the owner)
    std::deque<WriteTask*> backlog;
    uint64_t backlogRecords = 0;
    bool ready = false;  // on the owner's ready list
};

// Pending type-index c entries, in memory until a flush writes them. st: 1
// insert (committed), 2 delete (committed), 3 dead, 4 in flight (assigned,
// its partition file not yet committed: visible to dedupe, never flushed).
struct CEnt {
    uint8_t key[32];
    int64_t tb;
    int64_t seq;
    uint32_t pid;
    uint8_t st;
};
class PMap {
public:
    void init(uint32_t want);
    void clear() {
        a_.clear();
        used_ = live_ = 0;
    }
    void put(const uint8_t* key, int64_t tb, uint32_t pid, int64_t seq, uint8_t st);
    // f(const CEnt&) for each pid's newest state of (tb, key).
    template <class F>
    void each(int64_t tb, const uint8_t* key, F f) const {
        if (a_.empty()) return;
        const uint32_t mask = uint32_t(a_.size() - 1);
        for (uint32_t i = hashOf(key) & mask;; i = (i + 1) & mask) {
            const CEnt& x = a_[i];
            if (x.st == 0) return;
            if (x.st != 3 && x.tb == tb && std::memcmp(x.key, key, 32) == 0) f(x);
        }
    }
    void kill(int64_t tb, const uint8_t* key, uint32_t pid);
    uint32_t live() const { return live_; }
    size_t bytes() const { return a_.size() * sizeof(CEnt); }
    std::vector<CEnt>& raw() { return a_; }
    const std::vector<CEnt>& raw() const { return a_; }

private:
    static uint32_t hashOf(const uint8_t* key) {
        uint32_t h;
        std::memcpy(&h, key + 8, 4);
        return h * 2654435761u;
    }
    void grow();
    std::vector<CEnt> a_;
    uint32_t used_ = 0, live_ = 0;
};

// Pending ingest identities (IQC): (tb, src, h) -> holder (st as CEnt).
struct IdentEnt {
    int64_t tb = 0;
    uint64_t src = 0;
    uint8_t h[32] = {};
    int64_t seq = 0;
    uint8_t cid[32] = {};
    uint8_t st = 0;
};
std::string identMapKey(int64_t tb, uint64_t src, const uint8_t h[32]);

struct Holder {
    uint32_t pid;
    int64_t seq;
};

// A registered spec and what the engine derives from it. Immutable once
// built; a re-registration swaps in a new one (Type::spec, under Type::mu).
struct Spec {
    std::string name;
    std::vector<uint8_t> bytes;  // the registered TLV
    ps::TypeConfig tc;
    uint32_t pageSize = 4096;
    bool identity = false;
    uint64_t a18Bound = 10000;
    uint8_t epochProfile = 0;
    bool fullText = false;
    bool hasBucket = false;     // month files (a bucket time rule)
    bool hasEpochRule = false;
    bool hasObject = false;     // an object rule: r.k
    bool hasSupersede = false;  // supersede-on-ingest (CAT): r.s scope
    bool ek = false;            // epoch + object: r_dk and ent
    std::string timeRules;      // the epoch and bucket lines (C-5)
    std::string rules;
};
int32_t buildSpec(const uint8_t* p, size_t n, std::shared_ptr<Spec>* out, std::string* err);

struct Type {
    Engine* e = nullptr;
    std::string name;
    std::string pIdx, pJnl, pFts, pSpec, pDir;
    std::shared_ptr<const Spec> spec_;  // mu
    std::shared_ptr<const Spec> spec() {
        std::lock_guard<std::mutex> g(mu);
        return spec_;
    }

    // registry and counters (mu)
    std::mutex mu;
    std::vector<std::unique_ptr<Part>> parts;  // index pid-1
    std::unordered_map<std::string, uint32_t> partByProducer;
    std::vector<std::unique_ptr<LaneDef>> lanes;  // index id-1
    std::unordered_map<std::string, uint32_t> laneByIdentity;
    std::vector<std::unique_ptr<SrcDef>> srcs;  // index id-1
    std::unordered_map<std::string, uint32_t> srcByName;
    std::vector<uint8_t> laneJournaled, srcJournaled;  // by id-1
    std::vector<int64_t> tbs;  // bucket months that hold entries (newest first)
    int64_t uniq = 0, uniqBytes = 0, copies = 0;
    int64_t nextSeq = 1;
    int64_t seqReserved = 0;
    std::atomic<int64_t> vis{0};  // visible-through (B2)
    std::vector<std::pair<int64_t, int64_t>> inflight;  // (lo, hi) of calls in flight
    PMap pend, flushing;
    std::unordered_map<std::string, IdentEnt> identPend, identFlushing;
    std::unordered_set<std::string> touchedObj;  // pid|tb|k whose obj rows to refresh
    int64_t lastFlushMs = 0;
    bool overQuota = false;

    std::mutex dmu;  // writers of this type: the recheck and seq assignment (M7)
    std::mutex jmu;  // the journal connection
    Conn* jdb = nullptr;
    int64_t jlast = 0;
    std::vector<int64_t> jinflight;  // first journal id of each group journaled and not yet committed (mu)
    std::mutex flushMu;              // the type index writer (maintenance)
    Conn* idx = nullptr;
    bool migrateSeqsLoaded = false;  // dmu
    std::unordered_set<int64_t> migrateSeqs;
    std::mutex ftsMu;
    Conn* fts = nullptr;
    int64_t ftsThrough = 0;
    uint8_t ftsState = 0;  // 0 off, 1 building, 2 ready

    Part* partById(uint32_t pid) { return pid >= 1 && pid <= parts.size() ? parts[pid - 1].get() : nullptr; }
    LaneDef* laneById(uint32_t id) { return id >= 1 && id <= lanes.size() ? lanes[id - 1].get() : nullptr; }
    SrcDef* srcById(uint32_t id) { return id >= 1 && id <= srcs.size() ? srcs[id - 1].get() : nullptr; }
    void visRecompute();  // mu held
    std::string filePath(uint32_t pid, int64_t tb, int32_t gen) const;
};

// ---- mailbox (§3.4) ---------------------------------------------------------------------------
struct alignas(64) SlotHeader {
    std::atomic<uint32_t> state;
    std::atomic<uint32_t> cancel;
    std::atomic<uint32_t> outSeq;
    std::atomic<uint32_t> spaceSeq;
    uint32_t op;
    uint32_t cls;
    uint32_t flags;
    uint32_t reqLen;
    uint64_t reqId;
    uint64_t maxRowsExamined, maxBytesRead, maxResultRows, maxResultBytes;
    std::atomic<uint64_t> ringHead;
    std::atomic<uint64_t> ringTail;
    int32_t status;
    uint32_t errLen;
    uint64_t rowsOut, rowsExamined, bytesRead;
    uint64_t submitNs, startNs, endNs;
    char err[256];
    uint32_t thread;
    uint8_t zero[44];
};
static_assert(sizeof(SlotHeader) == FLATSQL_P4_SLOT_HEADER, "slot header is 448 bytes");

struct alignas(16) QCell {
    std::atomic<uint64_t> seq;
    uint32_t value;
    uint32_t pad;
};
struct alignas(64) QueueWords {
    std::atomic<uint64_t> enq;
    uint8_t pad0[56];
    std::atomic<uint64_t> deq;
    uint8_t pad1[56];
};
struct Queue {
    QueueWords* w = nullptr;
    QCell* cells = nullptr;
    uint32_t mask = 0;
    bool push(uint32_t v);
    bool pop(uint32_t* v);
    bool empty() const {
        return w->enq.load(std::memory_order_acquire) == w->deq.load(std::memory_order_acquire);
    }
};
struct alignas(64) Bell {
    std::atomic<uint32_t> doorbell;
    std::atomic<uint32_t> state;  // 0 idle, 1 busy, 2 stopped
    uint8_t pad[56];
};

// A write op queued for a partition's owner writer.
struct Shared;
struct WriteTask {
    uint32_t slot = 0;
    int op = 0;
    int mode = 0;  // PUT: 0 ingest, 1 migrate (a group never mixes them)
    uint64_t records = 0;
    Part* part = nullptr;
    Shared* shared = nullptr;                       // fan-out ops (SUPERSEDE, DELETE)
    std::vector<std::pair<int64_t, int64_t>> dels;  // DELETE: (tb, seq) of this partition's copies
};
// The common state of a fan-out op: the slot completes when every partition's
// part has committed.
struct Shared {
    enum Kind { kSupersede, kDelete } kind = kSupersede;
    std::atomic<int> remaining{0};
    std::atomic<int64_t> a{0}, b{0}, c{0};
    std::atomic<int32_t> status{0};
    std::mutex errMu;
    std::string err;
    uint32_t slot = 0;
    Type* type = nullptr;
    std::string provider, source, keep;  // SUPERSEDE
    bool apply = false;
    std::vector<std::array<uint8_t, 32>> keys;  // DELETE: CID keys
};

struct WriterState {
    std::mutex mu;
    std::deque<Part*> ready;  // partitions with a backlog
};

// Reader connections: one capped pool for every thread. A connection is
// checked out for one page (one short read transaction) and returned.
class ReaderPool {
public:
    void configure(uint32_t cap, uint32_t cacheKiB) {
        cap_ = cap;
        cacheKiB_ = cacheKiB;
    }
    // A reader connection on path (an idle one, or a new one). nullptr: *rc set.
    Conn* acquire(const std::string& path, OpenKind kind, int* rc, std::string* err);
    void release(Conn* c);
    void dropPath(const std::string& path);  // closes idle connections to path
    void closeAll();
    uint32_t open() const { return open_.load(std::memory_order_relaxed); }

private:
    std::mutex mu_;
    std::unordered_multimap<std::string, Conn*> idle_;
    std::list<Conn*> lru_;  // idle, oldest at the back
    std::unordered_map<Conn*, std::list<Conn*>::iterator> pos_;
    uint32_t cap_ = 256, cacheKiB_ = 512;
    std::atomic<uint32_t> open_{0};
};

// Maintenance work items.
struct MaintTask {
    enum Kind { kSlot, kCheckpoint, kClose, kUnlink } kind = kSlot;
    uint32_t slot = 0;
    Conn* conn = nullptr;
    File* file = nullptr;
    std::string path;
};

}  // namespace p4
}  // namespace flatsql

// ---- the engine (opaque P4Engine in p4_reader.h) ------------------------------------------
struct P4Engine {
    flatsql::p4::Config cfg;
    flatsql::p4::Markers markers;
    std::atomic<uint64_t> stat[flatsql::p4::kStCount];
    std::atomic<uint64_t> quota{0};
    std::atomic<uint64_t> heapPeak{0};
    std::atomic<bool> stopping{false};
    std::atomic<uint32_t>* stopWord = nullptr;

    // types (typesMu; never removed while the engine runs)
    std::mutex typesMu;
    std::vector<std::unique_ptr<flatsql::p4::Type>> types;
    std::unordered_map<std::string, flatsql::p4::Type*> typeByName;

    // mailbox memory
    uint8_t* mem = nullptr;
    size_t memBytes = 0;
    uint32_t nSlots[2] = {0, 0};
    uint8_t* slotBase[2] = {nullptr, nullptr};
    uint32_t slotStride[2] = {0, 0};
    uint32_t reqBytes[2] = {0, 0};
    uint32_t ringBytes[2] = {0, 0};
    flatsql::p4::Queue queues[4];
    flatsql::p4::Bell* bells = nullptr;
    uint32_t nThreads = 0;
    uint32_t threadClass[64] = {};
    uint32_t firstOfClass[6] = {}, countOfClass[6] = {};
    std::vector<std::thread> threads;
    bool started = false;

    // writers
    std::vector<std::unique_ptr<flatsql::p4::WriterState>> writers;
    std::atomic<uint32_t> nextOwner{0};
    // writer connections (wconnMu): LRU over files with an open writer connection
    std::mutex wconnMu;
    std::list<flatsql::p4::File*> wlru;
    uint32_t nWConn = 0;
    // reader connections
    flatsql::p4::ReaderPool rpool;
    // maintenance
    std::mutex maintMu;
    std::deque<flatsql::p4::MaintTask> maintQ;
    uint32_t maintThread = 0;

    flatsql::p4::SlotHeader* slot(uint32_t i) {
        const int p = i < nSlots[0] ? 0 : 1;
        const uint32_t k = p == 0 ? i : i - nSlots[0];
        return reinterpret_cast<flatsql::p4::SlotHeader*>(slotBase[p] + size_t(k) * slotStride[p]);
    }
    uint32_t poolOf(uint32_t i) const { return i < nSlots[0] ? 0 : 1; }
    uint8_t* slotReq(uint32_t i) { return reinterpret_cast<uint8_t*>(slot(i)) + FLATSQL_P4_SLOT_HEADER; }
    uint8_t* slotRing(uint32_t i) { return slotReq(i) + reqBytes[poolOf(i)]; }
    void bump(int s, uint64_t n = 1) { stat[s].fetch_add(n, std::memory_order_relaxed); }
    void wake(uint32_t thread);
    void kickMaintenance();
    flatsql::p4::Type* findType(const std::string& name);
};

// ---- a service thread's op context (opaque P4Lane in p4_reader.h) ---------------------------
struct P4Lane {
    P4Engine* e = nullptr;
    uint32_t thread = 0;
    uint32_t cls = 0;
    void* sqlState = nullptr;
    // the slot being run
    uint32_t slot = 0;
    flatsql::p4::SlotHeader* h = nullptr;
    const uint8_t* req = nullptr;
    uint32_t reqLen = 0;
    uint8_t* ring = nullptr;
    uint32_t ringBytes = 0;
    uint64_t maxRows = 0, maxBytes = 0, maxResultRows = 0, maxResultBytes = 0, heapCap = 0;
    uint64_t rowsExamined = 0, bytesRead = 0, rowsOut = 0, outBytes = 0;
    int32_t trip = 0;  // a cap or cancel that tripped (sticky for the op)
    // per-thread caches
    std::vector<std::shared_ptr<const flatsql::p4::Spec>> specRefs;  // keep p4_types' pointers valid
    std::vector<std::string> typeNames;
    std::vector<P4TypeInfo> typeInfos;
    std::vector<std::string> srcStore;
    std::vector<const char*> srcPtrs;
    std::unordered_map<flatsql::p4::Type*, flatsql::p4::Conn*> idx;  // type-index readers
    std::vector<uint8_t> out;  // pending output bytes (RB1)
    ~P4Lane();
};

namespace flatsql {
namespace p4 {

// ---- store.cpp ----------------------------------------------------------------------------------
int32_t engineInit(P4Engine* e, const uint8_t* cfg, size_t n, std::string* err);
int32_t engineRegisterType(P4Engine* e, const uint8_t* spec, size_t n, std::string* err);
int32_t engineActivate(P4Engine* e);
int32_t engineStop(P4Engine* e, double deadlineMs);
int32_t engineStats(P4Engine* e, uint8_t* out, int32_t cap);
Part* partFor(Type* t, const std::string& producer, const std::string& peer, bool create);  // mu held
LaneDef* laneFor(Type* t, const std::string* f6, bool create);  // mu held; f6: provider, source, batch, ckey, ppeer, pkey
SrcDef* srcFor(Type* t, const std::string& provider, const std::string& source, bool create);  // mu held
uint64_t laneHash(const std::string* f6);

// ---- journal.cpp ------------------------------------------------------------------------------
enum JOp : int { J_PART = 1, J_LANE = 2, J_SRC = 3, J_FILE = 4, J_C = 5, J_IDENT = 6, J_DEL = 7, J_DROP = 8 };
int32_t journalOpen(Type* t, std::string* err);
// The journal's last-id high water and the durable seq reservation.
int32_t journalReserve(Type* t, int64_t through);  // jmu held by caller? no: takes jmu
int32_t journalReplay(Type* t, std::string* err);  // at open, before any read (M8)

// ---- type_index.cpp -----------------------------------------------------------------------------
int32_t typeIndexOpen(Type* t, std::string* err);  // load the registry and counters
// Every copy of (tb, key): pending layers over the type index.
int32_t holdersOf(P4Lane* L, Type* t, int64_t tb, const uint8_t* key, std::vector<Holder>* out);
// The live holder of an ingest identity (0 when none).
int32_t identHolder(P4Lane* L, Type* t, int64_t tb, uint64_t src, const uint8_t h[32], int64_t* seq,
                    uint8_t cid[32]);
Conn* indexReader(P4Lane* L, Type* t, int32_t* rc);
int32_t typeIndexFlush(Type* t, bool force);  // maintenance thread
int32_t typeIndexRebuild(Type* t, bool verifyOnly, int64_t* entries, int64_t* mismatches);
void noteTb(Type* t, int64_t tb);  // mu held

// ---- partition.cpp -------------------------------------------------------------------------------
// The writer connection of a file (created on first use), pinned for the caller.
Conn* writerPin(P4Engine* e, File* f, int32_t* rc, std::string* err);
void writerUnpin(P4Engine* e, File* f);
int32_t fileSchema(Type* t, Conn* c, File* f, bool indexes);
int32_t fileCreateIndexes(Type* t, Conn* c);
File* fileFor(Type* t, Part* p, int64_t tb, bool create);  // mu held
void putGroup(P4Engine* e, uint32_t writer, Part* p, std::vector<WriteTask*>& tasks);
void supersedePart(P4Engine* e, Part* p, WriteTask* task);
void deletePart(P4Engine* e, Part* p, WriteTask* task);
void finishShared(P4Engine* e, Shared* s);
int32_t retireFile(P4Engine* e, File* f);

// ---- reader.cpp -----------------------------------------------------------------------------------
int32_t runRead(P4Lane* L, uint32_t op);  // ops 10-17 on a lane

// ---- maintain.cpp ----------------------------------------------------------------------------------
void maintenanceLoop(P4Engine* e, uint32_t thread);
int32_t quotaGc(P4Engine* e, uint64_t maxBytes, int64_t* files, int64_t* records, int64_t* bytes);
int32_t rebuildOp(P4Engine* e, Type* only, uint32_t what, std::vector<std::array<int64_t, 2>>* rows,
                  std::vector<Type*>* rowTypes);
int32_t ftsCatchUp(P4Engine* e, Type* t, bool all);
int walHook(void* arg, sqlite3* db, const char* zDb, int nPages);

// ---- mailbox.cpp --------------------------------------------------------------------------------------
extern thread_local uint32_t tThread;  // the running service thread's index
int32_t mailboxInit(P4Engine* e, std::string* err);
void mailboxLayout(P4Engine* e, FlatsqlP4Layout* out);
int32_t startThreads(P4Engine* e);
// The slot's output: append bytes to its ring (waits for space; checks cancel).
int32_t emitBytes(P4Lane* L, const uint8_t* p, size_t n);
int32_t flushOut(P4Lane* L);  // L->out to the ring
void slotDone(P4Engine* e, uint32_t slot, int32_t status, const std::string& err, uint64_t rows);
void slotDoneLane(P4Lane* L, int32_t status, const std::string& err);
// A response with fixed columns and a status only (an op that failed before its first row).
void respondEmpty(P4Lane* L, const std::vector<std::string>& cols, int32_t status, const std::string& err);
// Write-path output to a slot that no lane owns (writers, maintenance).
struct SlotOut {
    P4Engine* e;
    uint32_t slot;
    std::vector<uint8_t> buf;
    ps::rb1::Encoder enc{&buf};
    uint64_t rows = 0;
    explicit SlotOut(P4Engine* eng, uint32_t s) : e(eng), slot(s) {}
    int32_t flush();  // buffered bytes to the ring
    int32_t end(int32_t status, const std::string& err);
};
std::string pathJoin(const std::string& a, const std::string& b);

}  // namespace p4
}  // namespace flatsql

#endif
