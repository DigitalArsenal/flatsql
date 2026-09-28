// FlatSQL partition store: the reader's view of committed state (design §8,
// amended by A12, A14, A15, A16, A17, A20).
//
// A reader lane learns everything from files: the registry head and frames,
// each partition's A/B head, immutable manifests, L1 runs, the meta-log L0
// blocks a head lists, row files, data segments, and the type logs. It takes
// no lock shared with a writer and never waits on one: every structure it
// reads is either immutable once named (manifests, runs, sealed files,
// committed batch ranges) or double-buffered (heads).
//
// Snapshots:
//   - a statement snapshots each partition it touches once (PartSnap: the
//     head's committed HWM, L0 directory, counters, manifest), and each type
//     it reads at type level once (TypeSnap), taken BEFORE its partitions;
//   - partition-level visibility is the head's pseq_hi; type-level
//     visibility is V_p = min(pseq_hi, labeled_through[p]), so every
//     type-level row has a durable FIRST/REPEAT label;
//   - a row is dead when a DEAD posting's killer pseq is <= the bound.
//
// Everything here is per lane (LaneStore): caches are private to the lane
// thread, and the immutable objects they hand out (manifests, L1 run
// accelerators, parsed L0 blocks) are shared_ptrs safe to read from any
// thread.
#ifndef FLATSQL_PS_SNAPSHOT_H
#define FLATSQL_PS_SNAPSHOT_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "flatsql/ps/extract.h"
#include "flatsql/ps/format.h"
#include "flatsql/ps/index.h"
#include "flatsql/ps/io.h"

namespace flatsql {
namespace ps {

// Reader status codes (C ABI values in flatsql_ps.h).
enum ReaderStatus : int32_t {
    kRsOk = 0,
    kRsNeedsBulk = -20,      // FLATSQL_NEEDS_BULK: unbounded plan on an interactive lane
    kRsSnapshotGone = -21,   // FLATSQL_SNAPSHOT_GONE: a file named by the snapshot vanished (retryable)
    kRsTimeout = -22,        // sandbox work budget exhausted (the SandboxCaps `timeout` code)
    kRsCancelled = -23,
    kRsNoMem = -24,          // lane arena exhausted (SQLITE_NOMEM)
    kRsSqlError = -25,
    kRsNotAuthorized = -26,
    kRsStopped = -27,
    kRsRetryable = -28,      // lane failure: the statement can be resubmitted
    kRsCorrupt = -29,        // a CRC or structural check failed
    kRsNotMigrated = -30,    // STORE without MIGRATED (§10 step 1)
    kRsBusy = -31,           // no free request slot
    kRsTooLarge = -32,       // request larger than a slot
};

// Per-statement work accounting (§9: rows_examined and bytes_read per
// statement; the sandbox work budget is an admission limit on them, A28).
struct ReadStats {
    uint64_t rowsExamined = 0;   // record rows read (candidates, dead or alive)
    uint64_t bytesRead = 0;      // bytes read from any store file
    uint64_t indexEntries = 0;   // posting entries stepped
    uint64_t fenceReads = 0;     // L1 fence / arrivals fence probes
    uint64_t headReads = 0;
    uint64_t preads = 0;
};

// Cancellation, stop word and budgets, polled by long loops every 4 K steps
// (A21, A28). Returns a ReaderStatus (< 0) to abort, 0 to continue.
struct WorkGuard {
    std::atomic<uint32_t>* cancel = nullptr;
    std::atomic<uint32_t>* stop = nullptr;
    uint64_t maxRowsExamined = 0;  // 0 = unlimited
    uint64_t maxBytesRead = 0;
    ReadStats* stats = nullptr;
    int32_t poll() const {
        if (stop && stop->load(std::memory_order_relaxed)) return kRsStopped;
        if (cancel && cancel->load(std::memory_order_relaxed)) return kRsCancelled;
        if (stats) {
            if (maxRowsExamined && stats->rowsExamined > maxRowsExamined) return kRsTimeout;
            if (maxBytesRead && stats->bytesRead > maxBytesRead) return kRsTimeout;
        }
        return 0;
    }
};

// ---- per-lane file handles ----------------------------------------------------
// Files are named by a numeric key; the path is built only on a miss.
struct FileKey {
    enum Space : uint8_t { kStore = 0, kPart = 1, kType = 2 };
    uint8_t space = kStore;
    char letter = 0;     // 'h' head, 'm' meta, 'd' data, 'r' rows, 'a' attrs, 'x' run,
                         // 'f' manifest, 'l' lanes, 'g' arrivals, 'F' fence, 's' config,
                         // 'R' registry log, 'H' registry head, 'S' STORE, 'M' MIGRATED
    uint32_t id = 0;     // pid, or the fid as u32
    uint32_t seg = 0;
    uint32_t gen = 0;    // run gen, manifest gen, or cgen (compacted d/r/a)
    uint64_t extra = 0;  // config fingerprint
    bool operator==(const FileKey& o) const {
        return space == o.space && letter == o.letter && id == o.id && seg == o.seg && gen == o.gen &&
               extra == o.extra;
    }
};
struct FileKeyHash {
    size_t operator()(const FileKey& k) const {
        uint64_t h = (uint64_t(k.space) << 56) ^ (uint64_t(uint8_t(k.letter)) << 48) ^ (uint64_t(k.id) << 16);
        h ^= (uint64_t(k.seg) * 0x9E3779B97F4A7C15ull) ^ (uint64_t(k.gen) * 0xC2B2AE3D27D4EB4Full) ^ k.extra;
        h ^= h >> 29;
        return size_t(h * 0xBF58476D1CE4E5B9ull);
    }
};
void filePath(const std::string& root, const FileKey& k, PathBuf* out);
FileClass fileClassOf(const FileKey& k);

// LRU of read-only handles. A handle used by the running statement is never
// evicted before the statement ends; files a SWAP retires stay on disk while
// any statement older than the SWAP runs (A12, via the announce slots).
class LaneIo {
public:
    LaneIo(Io* io, const std::string& root, uint32_t maxHandles);
    ~LaneIo();
    // Opens (or finds) a read-only handle. NOENT is returned as is.
    int32_t get(const FileKey& key, FileRef* out);
    int64_t read(const FileRef& f, void* dst, size_t len, uint64_t off);
    int64_t size(const FileRef& f);
    void setStats(ReadStats* s) { stats_ = s; }
    ReadStats* stats() const { return stats_; }
    void beginStatement() { stmtSeq_++; }
    void closeAll();
    // Drops a cached handle (the file was replaced, e.g. a torn-read retry).
    void forget(const FileKey& key);
    IoStats& ioStats() { return ioStats_; }
    IoCtx& ctx() { return ctx_; }
    uint32_t openHandles() const { return uint32_t(map_.size()); }

private:
    struct Entry {
        FileRef ref;
        uint64_t lastSeq = 0;
        std::list<FileKey>::iterator lru;
    };
    std::string root_;
    IoStats ioStats_;
    IoCtx ctx_;
    uint32_t max_;
    uint64_t stmtSeq_ = 1;
    ReadStats* stats_ = nullptr;
    std::unordered_map<FileKey, Entry, FileKeyHash> map_;
    std::list<FileKey> lru_;  // front = most recent
};

// ---- registry view --------------------------------------------------------------
struct ColumnDef {
    std::string name;
    uint16_t voffset = 0;
    uint8_t baseType = 0;       // reflection::BaseType
    uint8_t element = 0;        // vector element type
    int64_t defInt = 0;
    double defReal = 0;
    const char* sqlType() const;
};

struct TypeInfo {
    uint8_t fid[4] = {0, 0, 0, 0};
    std::string schemaName;     // "OMM.fbs"
    std::string typeName;       // "OMM"
    uint64_t configFp = 0;
    std::shared_ptr<TypeConfig> cfg;
    std::vector<ColumnDef> cols;   // root table fields in vtable-slot order
    std::vector<uint32_t> pids;    // registered partitions of the type
};

struct PartInfo {
    uint32_t pid = 0;
    uint8_t fid[4] = {0, 0, 0, 0};
    std::string token;
    std::string sqlName;
    bool quarantined = false;
    bool dropped = false;
};

struct RegistryView {
    uint32_t incarnation = 0;
    uint64_t frames = 0;
    uint64_t fslEnd = 0;
    std::vector<PartInfo> parts;              // index = pid (0 unused)
    std::vector<std::shared_ptr<TypeInfo>> types;
    const PartInfo* part(uint32_t pid) const {
        return pid < parts.size() && parts[pid].pid ? &parts[pid] : nullptr;
    }
    const TypeInfo* typeByFid(const uint8_t fid[4]) const;
    const TypeInfo* typeByName(const std::string& name) const;   // case-insensitive
    const PartInfo* partBySqlName(const std::string& name) const;  // case-insensitive
};

// ---- manifests ----------------------------------------------------------------
struct SegRunRef {
    uint32_t gen = 0;
    uint64_t nEntries = 0;
    uint64_t fileLen = 0;
};
struct ManifestSegRef {
    uint32_t seg = 0;
    bool sealed = false;
    uint32_t cgen = 0;          // T3 SWAP seam: rows/data/attrs in c-<seg>-<cgen>.*
    uint64_t firstPseq = 0, endPseq = 0, mergedEnd = 0;
    uint64_t dLen = 0, rLen = 0, aLen = 0;
    int64_t minEpoch = INT64_MAX, maxEpoch = INT64_MIN;
    std::vector<SegRunRef> runs;
};
struct Manifest {
    uint32_t gen = 0;
    std::vector<ManifestSegRef> segs;   // ascending seg
    const ManifestSegRef* segFor(uint32_t seg) const;
    const ManifestSegRef* segForPseq(uint64_t pseq) const;  // merged range
};

// ---- partition snapshot -----------------------------------------------------------
struct PartSnap {
    uint32_t pid = 0;
    uint8_t fid[4] = {0, 0, 0, 0};
    bool empty = true;               // registered, never committed
    PartitionHeadFixed head{};
    std::vector<L0DirEntry> l0;
    std::vector<LaneCounter> lanes;  // inline lanes (overflow: laneCounters())
    bool lanesOverflow = false;
    std::shared_ptr<const Manifest> manifest;
    uint64_t visible = 0;            // row visibility bound (pseq_hi or V_p)
    uint64_t pseqHi() const { return empty ? 0 : head.pseqHi; }
};

// ---- type snapshot ----------------------------------------------------------------
enum TypeLabel : uint8_t { kLblFirst = 1, kLblRepeat = 2, kLblDead = 3, kLblPromoted = 4 };

struct TypeSnap {
    uint8_t fid[4] = {0, 0, 0, 0};
    bool empty = true;
    TypeHeadFixed head{};
    std::vector<TypeL0DirEntry> l0;
    std::vector<SegRunRef> runs;              // catalog runs (type manifest)
    // pid -> labeled_through (shared: > 128 pids rebuild it from the log)
    std::shared_ptr<const std::unordered_map<uint32_t, uint64_t>> labeled;
    // Sealed arrivals segments (fence index) and cumulative entry counts.
    std::shared_ptr<const std::vector<ArrivalFence>> fence;
    std::vector<uint64_t> segStart;           // position of each segment's first entry
    uint64_t labeledThrough(uint32_t pid) const {
        if (!labeled) return 0;
        auto it = labeled->find(pid);
        return it == labeled->end() ? 0 : it->second;
    }
    uint64_t gseqHi() const { return empty ? 0 : head.gseqHi; }
    // Arrivals positions: [0, total) over sealed segments then the active one.
    uint64_t arrivalsTotal() const {
        return empty ? 0 : (segStart.empty() ? 0 : segStart.back()) + head.gLen / kArrivalBytes;
    }
};

struct CatalogCopy {
    uint32_t pid = 0;
    uint64_t pseq = 0;
    uint64_t tcs = 0;
    uint8_t label = 0;
    uint64_t gseq = 0;
    uint32_t len = 0;
};

// ---- postings ----------------------------------------------------------------------
struct L0Parsed;  // a parsed L0 block (cached)

// Ordered stream of one index kind over a snapshot: every L0 block the head
// lists and every L1 run the manifest lists, merged in (key, value) order,
// ascending or descending, restricted to [lo, hi) (either end open).
class PostingScan {
public:
    struct Source;
    PostingScan();
    ~PostingScan();
    PostingScan(PostingScan&&) noexcept;
    PostingScan& operator=(PostingScan&&) noexcept;
    bool valid() const { return cur_ >= 0; }
    const uint8_t* key() const;
    uint16_t klen() const;
    const uint8_t* val() const;
    uint8_t vlen() const;
    // Advances; returns false at the end or on error (see err()).
    bool next();
    int32_t err() const { return err_; }

private:
    friend class LaneStore;
    void pick();
    std::vector<std::unique_ptr<Source>> srcs_;
    int cur_ = -1;
    bool desc_ = false;
    int32_t err_ = 0;
    class LaneStore* store_ = nullptr;
};

// ---- the lane's store view -------------------------------------------------------------
struct LaneStoreConfig {
    std::string root;
    Io* io = nullptr;
    uint32_t maxHandles = 512;
    uint64_t cacheBytes = 16ull << 20;   // L0 blocks + L1 accelerators + manifests
    bool verifyFrameCrc = true;          // check RecRow.dataCrc on every payload read
};

class LaneStore {
public:
    explicit LaneStore(const LaneStoreConfig& cfg);
    ~LaneStore();

    // §10 steps 1-2 (lazily): STORE and MIGRATED, then the registry.
    int32_t open();
    // §8 step 1: pread the registry head; parse new frames (copy on write).
    int32_t refreshRegistry();
    std::shared_ptr<const RegistryView> registry() const { return reg_; }

    // §8 step 3: head (valid slot, highest gen) + manifest.
    int32_t loadPart(uint32_t pid, PartSnap* out, bool withManifest = true);
    // §8 step 4 (taken before the partitions it bounds).
    int32_t loadType(const uint8_t fid[4], TypeSnap* out);

    // Rows, payloads and attributes of committed rows.
    int32_t readRow(const PartSnap& s, uint64_t pseq, RecRow* out);
    int32_t readRows(const PartSnap& s, uint64_t first, uint32_t n, RecRow* out);
    // Payload bytes [0, len) of the stored frame (size prefix included).
    int32_t readFrame(const PartSnap& s, const RecRow& r, uint8_t* dst);
    int32_t readFramePart(const PartSnap& s, const RecRow& r, uint64_t off, uint32_t n, uint8_t* dst);
    int32_t readAttr(const PartSnap& s, const RecRow& r, std::vector<uint8_t>* out);

    // Postings of one kind (partition or type kinds).
    PostingScan scan(const PartSnap& s, uint16_t kind, const uint8_t* lo, size_t lol, const uint8_t* hi,
                     size_t hil, bool desc);
    PostingScan scanType(const TypeSnap& t, uint16_t kind, const uint8_t* lo, size_t lol, const uint8_t* hi,
                         size_t hil, bool desc);
    // Every value of an exact key (bloom-gated). visit(val) -> continue?
    int32_t lookup(const PartSnap& s, uint16_t kind, const uint8_t* key, size_t klen,
                   const std::function<bool(const uint8_t*)>& visit);
    int32_t lookupType(const TypeSnap& t, uint16_t kind, const uint8_t* key, size_t klen,
                       const std::function<bool(const uint8_t*)>& visit);

    // Liveness: a DEAD posting whose killer pseq <= bound.
    int32_t isDead(const PartSnap& s, uint64_t pseq, uint64_t bound, bool* dead);
    int32_t isTagDead(const PartSnap& s, uint64_t inst, uint64_t bound, bool* dead);
    // Type level (A14, A16): the latest LABEL of (pid, pseq); the latest
    // REHOME of a gseq; every catalog copy of a cid (latest tcs per copy).
    int32_t labelOf(const TypeSnap& t, uint32_t pid, uint64_t pseq, uint8_t* label, uint64_t* gseq,
                    bool* found);
    int32_t rehomeOf(const TypeSnap& t, uint64_t gseq, std::vector<std::pair<uint32_t, uint64_t>>* candidates);
    int32_t catalog(const TypeSnap& t, const uint8_t cid[kCidLen], std::vector<CatalogCopy>* out);

    // Arrivals (A15): entry at a position, and the first position whose
    // gseq > after (binary search over the fence index and one segment).
    int32_t arrivalAt(const TypeSnap& t, uint64_t pos, ArrivalEntry* out);
    int32_t arrivalsRead(const TypeSnap& t, uint64_t pos, uint32_t n, ArrivalEntry* out);
    int32_t arrivalsUpperBound(const TypeSnap& t, uint64_t gseq, uint64_t* pos);

    // Lane tuples of a partition (l.fsl), by lane id.
    struct LaneTuple {
        uint32_t id = 0;
        std::string provider, source, batch, peer, pubkey;
    };
    int32_t laneTuples(const PartSnap& s, std::vector<LaneTuple>* out);
    // Lane counters of a snapshot (inline, or the head's LANE_CKPT record).
    int32_t laneCounters(const PartSnap& s, std::vector<LaneCounter>* out);

    // Statement boundary: accounting target, handle pinning, work guard.
    void beginStatement(ReadStats* stats, const WorkGuard* guard);
    void endStatement();
    const WorkGuard* guard() const { return guard_; }
    ReadStats* stats() const { return stats_; }
    LaneIo& io() { return io_; }
    const std::string& root() const { return cfg_.root; }
    const LaneStoreConfig& config() const { return cfg_; }
    uint64_t cacheBytes() const { return cacheUsed_; }
    // Cooperative polling from long loops (every 4 K steps).
    int32_t poll() const { return guard_ ? guard_->poll() : 0; }

    // Internals shared with PostingScan sources.
    struct Cached;
    std::shared_ptr<const L0Parsed> l0Block(uint32_t pid, uint32_t mSeg, uint64_t off, uint32_t len,
                                            int32_t* rc);
    std::shared_ptr<const L0Parsed> typeL0Block(const uint8_t fid[4], const TypeL0DirEntry& e, int32_t* rc);
    // Sources keep file KEYS, never handles: a handle may be evicted and its
    // number reused between two reads; a key is reopened by name.
    std::shared_ptr<const L1Run> run(uint32_t pid, uint32_t seg, uint32_t gen, uint64_t fileLen, FileKey* key,
                                     int32_t* rc);
    std::shared_ptr<const L1Run> typeRun(const uint8_t fid[4], uint32_t gen, uint64_t fileLen, FileKey* key,
                                         int32_t* rc);
    int32_t readBlock(const FileKey& key, uint64_t off, uint8_t* dst);  // one 4 KiB L1 block

private:
    int32_t readHead(const FileKey& key, uint16_t kind, std::vector<uint8_t>* slot, bool* missing);
    int32_t loadManifest(uint32_t pid, uint32_t gen, std::shared_ptr<const Manifest>* out);
    int32_t loadTypeConfig(TypeInfo* t);
    int32_t loadTypeLabels(const uint8_t fid[4], TypeSnap* t);
    // The file holding a merged segment's rows/attrs/data ('r', 'a', 'd'),
    // honoring a SWAP to compaction outputs (cgen).
    FileKey segKey(const PartSnap& s, char letter, uint32_t seg) const;
    void cachePut(const FileKey& key, std::shared_ptr<const void> obj, uint64_t bytes);
    std::shared_ptr<const void> cacheGet(const FileKey& key);

    LaneStoreConfig cfg_;
    LaneIo io_;
    bool opened_ = false;
    std::shared_ptr<const RegistryView> reg_;
    uint64_t regFslRead_ = 0;
    ReadStats* stats_ = nullptr;
    ReadStats scratchStats_;
    const WorkGuard* guard_ = nullptr;
    // Immutable-object cache (LRU by bytes).
    struct CacheEntry {
        std::shared_ptr<const void> obj;
        uint64_t bytes = 0;
        std::list<FileKey>::iterator lru;
    };
    std::unordered_map<FileKey, CacheEntry, FileKeyHash> cache_;
    std::list<FileKey> cacheLru_;
    uint64_t cacheUsed_ = 0;
    // Type configs by (fid, fp).
    std::map<std::pair<uint32_t, uint64_t>, std::shared_ptr<TypeInfo>> typeInfos_;
    // Label maps of types with > 128 partitions: (fid) -> (commitSeq, labels).
    struct LabelCache {
        uint64_t ckptOff = 0;
        uint64_t scannedTo = 0;
        uint64_t commitSeq = 0;
        std::unordered_map<uint32_t, uint64_t> labels;
    };
    std::unordered_map<uint32_t, LabelCache> labelCache_;
};

// CID text ("b" + base32 lower) <-> binary, and the column value helpers the
// vtabs share.
bool cidFromText(const char* s, size_t n, uint8_t cid[kCidLen]);
std::string cidToText(const uint8_t cid[kCidLen]);
// Columns of a type's root table (reflection over its BFBS).
std::vector<ColumnDef> columnsOf(const TypeConfig& cfg);

}  // namespace ps
}  // namespace flatsql

#endif
