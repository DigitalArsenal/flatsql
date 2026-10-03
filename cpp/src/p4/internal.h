// FlatSQL store format 4 ("p4"): engine internals.
//
// One SQLite file per SOURCE FEED x standard (CONTRACT C-37, C-38, owner
// layout of 2026-10-02): P/<TYPE>/<feed>.db, where a feed is a record's
// (provider, source) tag pair; a record with no source lives in
// P/<TYPE>/local.db. SQLite 3.53.4 unmodified, every file through FlatSQL's
// own VFS (flatsql_io). The interfaces are CONTRACT.md §1-§4;
// docs/STORE-FORMAT-4.md is the as-built description.
//
// A feed file is its own table: a record is identified within it by its CID
// (the file's one CID index) and its seq; there is no cross-feed identity
// (C-38). A record that arrives through a second feed is a new row set in
// that feed's file with its own seq (ingest); a migrated record keeps format
// 1's rowid as its seq in every feed holding it. Within a feed file the rows
// of a record are its copies x its instances (every copy, the publishing
// node, with every instance, a batch and content key of the feed); local
// rows carry no instance. A row keeps the record bytes verbatim, its
// signature, epoch, object key, ts, the delivery's `at`, and small per-file
// ids for the node, the batch, the content key and the url. No provider or
// source string is in a row: the file is the feed.
//
// Layout under the root (<data>/fsql4):
//   STORE, MIGRATED                      markers (§2.2)
//   T/TYPES                              registered type names (append-only records with a crc)
//   T/<TYPE>.spec                        the registered spec TLV, plus a crc tag
//   T/<TYPE>.idx                         type index: the feed and token registries, each feed's counters, next seq
//   T/<TYPE>.jnl                         intent journal (synchronous=FULL)
//   T/<TYPE>.fts                         FTS5 (background)
//   P/<TYPE>/<feed>.db                   one file per source feed of the type
//
// Threads: writers (every feed file of a type is written by the type's one
// writer thread; a file has one writer connection), interactive / bulk /
// sandbox lanes, one maintenance thread and one long-work thread. Lock order,
// outermost first: Engine::typesMu, Type::mu, Feed::dictMu, Engine::wconnMu.
// No lock is held across a feed file's I/O except by its one writer.
#ifndef FLATSQL_P4_INTERNAL_H
#define FLATSQL_P4_INTERNAL_H

#include <sqlite3.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
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
int64_t nowSec();
void dayText(int64_t sec, char out[11]);  // "YYYY-MM-DD"

// ---- statistics (§3.10; entries only ever appended) ----------------------------------
// kStUnlinked, kStQuotaFiles and kStTwoPhase are always 0 in this layout (a
// feed keeps its one file; there are no merges): the frozen ABI keeps their
// slots, as it keeps SUPERSEDE's files_deleted and QUOTA_GC's files_dropped
// columns (always 0). kStPartitions counts feed files.
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
    uint32_t backlogCredit = 16384;
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
    // feed file
    S_R_SEQ, S_R_SEQD, S_R_D, S_R_INS, S_R_DEL, S_R_URL, S_R_MAXRID, S_META_SET,
    S_NODE_INS, S_BATCH_INS, S_CKEY_GET, S_CKEY_INS, S_URL_GET, S_URL_TEXT, S_URL_INS, S_CKEY_TEXT,
    S_INST_PUT, S_INST_DEL, S_R_CID, S_R_CIDSCAN, S_IDENT_GET, S_IDENT_INS, S_TOKC_PUT, S_TOKC_DEL,
    // journal
    S_J_INS, S_J_DEL, S_JM_SET,
    S_COUNT
};
struct Conn {
    sqlite3* db = nullptr;
    sqlite3_stmt* st[S_COUNT] = {};
    std::unordered_map<std::string, sqlite3_stmt*> dyn;  // other statements, by SQL text
    std::string path;
    uint32_t cacheKiB = 0;  // a pooled reader's page cache (its share of the pool's budget)
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
extern thread_local char tSqlLog[200];  // this thread's last SQLite error log line (SQLITE_CONFIG_LOG)
// A connection's database bytes (pages x page size) and free bytes, from its
// header (no file I/O on a connection that just committed).
int64_t dbBytesOf(Conn* c, int64_t* freeBytes);

// ---- keys --------------------------------------------------------------------------------------
// An object key value (r.k / x.k): none, an integer or text.
struct KVal {
    int type = 0;  // 0 none, 1 integer, 3 text
    int64_t i = 0;
    std::string s;
    void bind(sqlite3_stmt* q, int at) const;
    void from(sqlite3_stmt* q, int col);
    bool operator==(const KVal& o) const { return type == o.type && i == o.i && s == o.s; }
    std::string text() const { return type == 1 ? std::to_string(i) : type == 3 ? s : std::string(); }
};

// ---- engine objects ------------------------------------------------------------------------
struct Type;
struct Feed;

// A feed file's small id tables. Nodes (the copy: producer token and peer)
// and batches (format 1's summary key inside the feed: batch, producer peer,
// producer key) are few and cached whole; content keys and urls are looked up
// in the file (a url may be per record).
struct NodeDef {
    std::string producer, peer;
};
struct BatchDef {
    std::string batch, ppeer, pkey;
};
struct Dict {
    std::vector<NodeDef> node;    // index id-1
    std::vector<BatchDef> batch;  // index id-1
    std::unordered_map<std::string, uint32_t> nodeId, batchId;
    bool loaded = false;
};
std::string nodeKey(const std::string& producer, const std::string& peer);
std::string batchKey(const std::string& batch, const std::string& ppeer, const std::string& pkey);

// An instance (a batch number and content key) in one feed file: the records
// holding it (each once), their bytes (each record's smallest copy), and
// format 1's summary times. The strings are the instance's identity.
struct InstCount {
    std::string batch, ppeer, pkey, ckey;
    int64_t n = 0, bytes = 0, minw = INT64_MAX, maxw = INT64_MIN, minseq = INT64_MAX, maxseq = 0;
    int64_t first = 0, updated = 0, maxat = 0, maxts = 0;
    std::string url;  // the latest write's url (SUMMARY 3)
};
using InstId = std::pair<uint32_t, uint32_t>;  // (batch id, ckey id; 0 = "")

// A feed file's counters, committed in its meta with its rows. Exact: rows,
// recs (records), bytes (over rows), nnull (records without an epoch), rbytes
// (each record's smallest copy), copies (record x copy) and cbytes (each
// copy's bytes). Bounds: the rest.
struct Counters {
    int64_t rows = 0, recs = 0, bytes = 0, nnull = 0, rbytes = 0, copies = 0, cbytes = 0;
    int64_t minseq = INT64_MAX, maxseq = 0, minw = INT64_MAX, maxw = INT64_MIN;
    int64_t mints = INT64_MAX, maxts = 0, mine = INT64_MAX, maxe = INT64_MIN;
};

// A producer token's copies in one feed file (exact: n, bytes; bounds: the
// rest), committed in the file's tokc table with its rows.
struct TokCount {
    int64_t n = 0, bytes = 0, mints = INT64_MAX, maxts = 0, maxseq = 0;
};

struct Feed {
    Type* type = nullptr;
    uint32_t fid = 0;
    std::string provider, source;  // both "" for local
    std::string name, path;
    bool local = false;
    // Type::mu
    Counters k;
    std::map<InstId, InstCount> inst;   // live instances (n > 0)
    std::map<uint32_t, TokCount> tokc;  // type token id -> its copies in this file (n > 0)
    bool created = false;      // exists on disk with its schema (readers skip it until then)
    bool quarantined = false;  // corrupt: P4_E_CORRUPT for ops that need it
    bool indexed = true;       // secondary indexes present (false between a migration's append and REBUILD 1)
    bool registered = false;   // in the journal or the type index (a write journals it before its first use)
    int64_t dbBytes = -1, freeBytes = 0;  // pages and free pages after its writer's last commit (-1: not read)
    // dictMu
    std::mutex dictMu;
    Dict dict;
    // writer connection (Engine::wconnMu)
    struct Conn* w = nullptr;
    int wPins = 0;
    std::list<Feed*>::iterator lru;
    bool inLru = false;
};

// A producer token of the type (a copy's identity). Type-wide ids, in
// registration order: the lowest id is the first copy (C-12).
struct TokDef {
    std::string token, peer;
    bool registered = false;  // in the journal or the type index
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
    bool hasEpochRule = false;
    bool hasObject = false;     // an object rule: r.k, indexed r_ke(k, e)
    bool hasSupersede = false;  // supersede-on-ingest (CAT)
    bool ek = false;            // epoch + object: EPOCH points by one seek per object
    std::string epochRule;      // the epoch line (C-5)
    std::string keyRules;       // the object and col lines: fixed in a type's rows once it has data (C-5)
    std::string rules;
};
int32_t buildSpec(const uint8_t* p, size_t n, std::shared_ptr<Spec>* out, std::string* err);

struct WriteTask;

// The type's totals over its feed files (C-38 (5): a record held by N feeds
// counts once per feed).
struct TypeTotals {
    int64_t recs = 0, rbytes = 0, copies = 0, cbytes = 0;
    int64_t mine = INT64_MAX, maxe = INT64_MIN, mints = INT64_MAX, maxts = 0, maxseq = 0;
    std::vector<TokCount> toks;  // index token id-1, summed over the feeds
};

struct Type {
    Engine* e = nullptr;
    std::string name;
    std::string pIdx, pJnl, pFts, pSpec, pDir;
    // The type index and journal exist (the type has had a write, or they
    // were on disk at open). Set once, under openMu; readers check it first.
    std::atomic<bool> hasFiles{false};
    std::mutex openMu;
    std::shared_ptr<const Spec> spec_;  // mu
    std::shared_ptr<const Spec> spec() {
        std::lock_guard<std::mutex> g(mu);
        return spec_;
    }

    // registry and counters (mu)
    std::mutex mu;
    std::vector<std::unique_ptr<Feed>> feeds;  // index fid-1
    std::unordered_map<std::string, uint32_t> feedByKey;  // provider \x1f source -> fid
    std::vector<TokDef> toks;                              // index id-1
    std::unordered_map<std::string, uint32_t> tokByToken;
    int64_t nextSeq = 1;
    int64_t seqReserved = 0;
    std::atomic<int64_t> vis{0};  // visible-through (B2)
    std::vector<std::pair<int64_t, int64_t>> inflight;  // (lo, hi) of new seqs in flight
    bool overQuota = false;
    bool broken = false;  // the type index could not be brought in line with the files: writes refuse
    std::string brokenWhy;

    // The type's writer thread owns these (and init / replay before threads run).
    uint32_t owner = 0;
    struct Conn* idx = nullptr;  // type index (writer)
    struct Conn* jdb = nullptr;  // journal
    int64_t jcut = 0;            // journal rows <= jcut are applied to the index

    std::atomic<int64_t> idxBytes{0}, ftsBytes{0};  // T/ files, set by the maintenance thread
    std::mutex ftsMu;
    struct Conn* fts = nullptr;
    int64_t ftsThrough = 0;
    uint8_t ftsState = 0;  // 0 off, 1 building, 2 ready
    // Seqs whose row set left a feed file since the full text last caught up:
    // their full-text rows are deleted by the long-work thread (a migrated
    // seq only once no feed file holds it).
    std::mutex ftsGoneMu;
    std::vector<int64_t> ftsGone;

    // the writer backlog (WriterState::mu of the owner)
    std::deque<WriteTask*> backlog;
    uint64_t backlogRecords = 0;
    bool ready = false;

    Feed* feedById(uint32_t fid) { return fid >= 1 && fid <= feeds.size() ? feeds[fid - 1].get() : nullptr; }
    TokDef* tokById(uint32_t id) { return id >= 1 && id <= toks.size() ? &toks[id - 1] : nullptr; }
    void visRecompute();   // mu held
    TypeTotals totals();   // mu held
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

// Work for a type's writer thread: a slot's write op, or internal work
// (quota, REBUILD) whose waiter (the long-work thread) watches `done`.
struct Internal {
    std::atomic<bool> done{false};
    int32_t status = P4_OK;
    std::string err;
    int64_t a = 0, b = 0;  // QUOTA: records, bytes; REBUILD: entries, mismatches
    std::vector<std::pair<uint32_t, int64_t>> seqs;  // QUOTA: the (feed, seq) row sets to delete
    uint32_t what = 0;          // REBUILD
    std::string firstBad;       // REBUILD 8: the first damaged file
};
struct WriteTask {
    uint32_t slot = 0;
    int op = 0;
    int mode = 0;  // PUT: 0 ingest, 1 migrate (a group never mixes them)
    uint64_t records = 0;
    Type* type = nullptr;
    Internal* internal = nullptr;  // quota and REBUILD from the long-work thread
};

struct WriterState {
    std::mutex mu;
    std::deque<Type*> ready;  // types with a backlog
};

// Reader connections: one pool for every thread, inside the engine's one
// memory budget whatever the number of feed files. A connection is checked
// out for one page (one short read transaction) and returned. Idle
// connections close, least recently used first, while the pool has more than
// `cap` open, while their page caches together pass cap x cacheKiB (a larger
// cache counts for more), or while the engine's heap is past `pressure` bytes.
class ReaderPool {
public:
    void configure(uint32_t cap, uint32_t cacheKiB, uint64_t pressure) {
        cap_ = cap ? cap : 1;
        cacheKiB_ = cacheKiB;
        budgetKiB_ = uint64_t(cap_) * cacheKiB;
        pressure_ = pressure;
    }
    // A reader connection on path (an idle one, or a new one). nullptr: *rc set.
    Conn* acquire(const std::string& path, OpenKind kind, int* rc, std::string* err);
    void release(Conn* c);
    void dropPath(const std::string& path);  // closes idle connections to path
    void closeAll();                         // closes every idle connection (also: memory relief)
    uint32_t open() const { return open_.load(std::memory_order_relaxed); }

private:
    void trim(std::vector<Conn*>* close);  // mu_ held
    void unlinkIdle(Conn* v);              // mu_ held
    std::mutex mu_;
    std::unordered_multimap<std::string, Conn*> idle_;
    std::list<Conn*> lru_;  // idle, oldest at the back
    std::unordered_map<Conn*, std::list<Conn*>::iterator> pos_;
    uint32_t cap_ = 256, cacheKiB_ = 512;
    uint64_t budgetKiB_ = 256ull * 512, usedKiB_ = 0, pressure_ = 0;
    std::atomic<uint32_t> open_{0};
};

// Maintenance work items.
struct MaintTask {
    enum Kind { kSlot, kCheckpoint, kClose } kind = kSlot;
    uint32_t slot = 0;
    Conn* conn = nullptr;
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
    std::atomic<bool> stopping{false};
    // A migration target (create mode 2) holds full-text indexing until
    // activation: migrated rows keep format 1's seqs, below the watermark.
    std::atomic<bool> ftsHold{false};
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
    // writer connections (wconnMu): LRU over feed files with an open writer connection
    std::mutex wconnMu;
    std::list<flatsql::p4::Feed*> wlru;
    uint32_t nWConn = 0;
    // reader connections
    flatsql::p4::ReaderPool rpool;
    // WAL accounting (walMu): frames not yet checkpointed, by path
    std::mutex walMu;
    std::unordered_map<std::string, int64_t> walPages;
    int64_t walSum = 0;
    // and each WAL's frames since it last started over (its file's extent)
    std::unordered_map<std::string, int64_t> walExt;
    int64_t walExtSum = 0;
    std::unordered_set<std::string> ckptQueued;
    // maintenance
    std::mutex maintMu;
    std::deque<flatsql::p4::MaintTask> maintQ;
    uint32_t maintThread = 0;
    // Long maintenance (REBUILD and QUOTA_GC slots, the configured quota, the
    // full-text index) runs on its own thread, so WAL checkpoints never wait
    // behind it. Its type work runs on each type's writer thread.
    std::thread slowThread;
    std::mutex slowMu;
    std::condition_variable slowCv;
    std::deque<uint32_t> slowQ;
    bool slowStop = false;

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
    std::string err;   // the op's error text (p4_lane_set_error, C-19)
    // per-thread caches
    std::vector<std::shared_ptr<const flatsql::p4::Spec>> specRefs;  // keep p4_types' pointers valid
    std::vector<std::string> typeNames;
    std::vector<P4TypeInfo> typeInfos;
    std::vector<std::string> srcStore;
    std::vector<const char*> srcPtrs;
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
// The type index and journal, made on the type's first write (lazy T/ files).
int32_t typeFilesEnsure(Type* t, std::string* err);
// The feed of (provider, source); ("", "") is local. mu held. create: register
// it (its file is made by its first write); *made is set when it is new.
Feed* feedFor(Type* t, const std::string& provider, const std::string& source, bool create, bool* made = nullptr);
Feed* feedRestore(Type* t, uint32_t fid, const std::string& provider, const std::string& source, const std::string& name);  // mu held
uint32_t tokFor(Type* t, const std::string& token, const std::string& peer, bool create, bool* made = nullptr);  // mu held
std::string pathJoin(const std::string& a, const std::string& b);

// ---- journal.cpp ------------------------------------------------------------------------------
// J_FEED and J_TOK register ids; J_TOUCH names a feed file a write changes;
// J_MOVE a record whose rows leave a file (local) for another file in the
// same write. A write journals them (synchronous=FULL) with the seq
// reservation before its first file commit; open replays the tail: the ids
// are registered, a move cut between its two files is finished (the source
// file's rows of the seq go once another file holds it), and every touched
// feed file's counters (committed with its rows) are mirrored into the type
// index.
enum JOp : int { J_FEED = 1, J_TOK = 2, J_TOUCH = 3, J_MOVE = 4 };
int32_t journalOpen(Type* t, std::string* err);
int32_t journalReplay(Type* t, std::string* err);  // at open, before any read (M8); also after a failed index commit

// ---- type_index.cpp -----------------------------------------------------------------------------
int32_t typeIndexOpen(Type* t, std::string* err);  // load the registries and the feeds' counters
// A feed's mirror in the type index (its counters, live instances and tokens).
struct FeedSnap {
    uint32_t fid = 0;
    std::string provider, source, name;
    Counters k;
    bool indexed = true;
    std::map<InstId, InstCount> inst;
    std::map<uint32_t, TokCount> tokc;
};
FeedSnap feedSnapOf(Feed* f);  // Type::mu held
int indexPutFeed(Conn* idx, const FeedSnap& f);           // SQLite rc
int indexPutTok(Conn* idx, uint32_t id, const TokDef& d);  // the registry row
int indexPutNextSeq(Conn* idx, int64_t nextSeq);

// ---- partition.cpp (feed files) -------------------------------------------------------------------
// The writer connection of a feed file (made with its schema on first use), pinned for the caller.
Conn* writerPin(P4Engine* e, Feed* f, int32_t* rc, std::string* err);
void writerUnpin(P4Engine* e, Feed* f);
int32_t fileCreateIndexes(Type* t, Conn* c, bool local);
int32_t fileSchema(Type* t, Conn* c, Feed* f, bool indexes);
int writeMeta(Conn* c, const Counters& k, int64_t now);  // in the caller's write transaction; SQLite rc
int readMeta(Conn* c, Counters* k, bool* indexed);       // SQLite rc
int readInst(Feed* f, Conn* c, std::map<InstId, InstCount>* out);  // SQLite rc; strings from the file's dictionary
int readTokc(Type* t, Conn* c, std::map<uint32_t, TokCount>* out);  // SQLite rc; tokens registered as needed
int writeTokc(Type* t, Conn* c, const std::map<uint32_t, TokCount>& before, const std::map<uint32_t, TokCount>& after);
// The dictionary of a feed file, loaded through c (any connection of the
// file) when needed. Returns false on a read error.
bool dictEnsure(Feed* f, Conn* c);
bool dictNode(Feed* f, Conn* c, uint32_t id, NodeDef* out);
bool dictBatch(Feed* f, Conn* c, uint32_t id, BatchDef* out);
std::string dictCkey(Conn* c, uint32_t id);  // "" for 0
std::string dictUrl(Conn* c, uint32_t id);   // "" for 0
// The seq of a CID in a feed file (0: none; through r_c, or a walk before REBUILD 1).
int32_t seqOfCid(Conn* c, bool indexed, const uint8_t key[32], int64_t* seq);
// Whether a feed file holds rows of seq (its rid range).
int32_t fileHoldsSeq(Conn* c, int64_t seq, bool* held);
void putGroup(P4Engine* e, uint32_t writer, Type* t, std::vector<WriteTask*>& tasks);
// The supersede identity of a stored record (unsealed bytes only).
std::string identityOf(const ps::TypeConfig& tc, const uint8_t* d, size_t n);

// ---- record.cpp: a record's rows in one feed file, changed and committed --------------------------
struct RowR {
    int64_t rid = 0;  // 0: a new row (its rid is assigned at the commit)
    uint32_t tok = 0;          // the copy (type token id)
    std::string peer;          // the copy's peer
    bool inst = false;         // an instance (feed rows) or none (local rows)
    std::string batch, ppeer, pkey, ckey;
    std::string url;
    int64_t at = 0;
    int64_t ts = 0, len = 0;   // ts: this copy's (a record's copies share the record's, except a COPY that keeps its own, C-39 E6)
    std::string sig, fcols;
    bool sealed = false;
    bool hasD = false;
    std::string d;
    uint32_t srcFid = 0;   // a new row whose bytes are another row's (fetched at the commit)
    int64_t srcRid = 0;
    bool del = false;      // to delete
    bool urlSet = false;   // the url changed (an existing row)
    bool loaded = false;   // read from its file (an existing row)
    // A new row of an instance this write's tag delivered: the instance's
    // summary times follow it. A copy joining instances the record already
    // has (an untagged write, a filled row) leaves them alone (C-39 E4).
    bool stamp = false;
    uint32_t nId = 0, bId = 0, cId = 0, uId = 0;  // its file's ids (an existing row)
    bool live() const { return !del; }
    bool sameInst(const RowR& o) const {
        return inst == o.inst && batch == o.batch && ppeer == o.ppeer && pkey == o.pkey && ckey == o.ckey;
    }
};
// A record in one feed file: its rows there (one rid range).
struct RecState {
    uint32_t fid = 0;
    uint8_t key[32] = {};
    int64_t seq = 0;
    bool keyed = false;    // key holds the record's CID (a seq probed absent has none until a caller names it, C-39 B1)
    bool existed = false;  // rows in the file before this write
    bool restamp = false;  // its rows leave as a DELETE or a CAT supersede: the instances it leaves are restamped (C-39 E5)
    bool hasE = false;
    int64_t e = 0, ts = 0, w = 0;
    KVal k;
    std::vector<RowR> rows;  // loaded, then changed
    bool touched = false;    // this write changes its rows
};
// An ingest identity a write registers (IQC) in a feed file: h -> the record.
struct IdentNew {
    uint8_t h[32];
    RecState* rec;
};
// One type's write context (the type's writer thread).
class WriteCtx {
public:
    WriteCtx(Engine* e, Type* t);
    ~WriteCtx();
    Engine* e;
    Type* t;
    std::shared_ptr<const Spec> sp;
    std::string err;
    // A record of a feed file by CID key / by seq, loaded with its rows
    // (nullptr with *rc on an error). A key or seq with no rows is a new
    // record of that file.
    RecState* byKey(uint32_t fid, const uint8_t key[32], int32_t* rc);
    RecState* bySeq(uint32_t fid, int64_t seq, int32_t* rc, const uint8_t* keyIfNew = nullptr);
    RecState* known(uint32_t fid, int64_t seq);  // already loaded by this write
    int32_t loadD(uint32_t fid, RowR& r);         // the stored bytes of an existing row
    // A delivery to the record's file: copy `copy.tok` (its data in `copy`,
    // used when the copy is new) with the given instances of the file's feed.
    // Every copy of the record then appears with every instance (local rows
    // carry none).
    struct Inst {
        std::string batch, ppeer, pkey, ckey, url;
        int64_t at;
    };
    struct Out {
        bool copyNew = false, instNew = false, urlChanged = false;
    };
    void deliver(RecState* r, const RowR& copy, const std::vector<Inst>& insts, Out* out);
    void dropAll(RecState* r);
    const std::vector<RecState*>& records() const { return order_; }
    // Journal, the feed files (feeds before local), then the type index.
    int32_t commit();
    bool migrate = false;  // migrate mode: instance times are the caller's (C-36), new files without indexes
    bool replaying = false;  // journal replay's own repair: no journal rows, no replay on a failed index commit
    int64_t now = 0;
    std::vector<IdentNew> idents;
    std::set<uint32_t> committedFids, failedFids;
    int64_t jlast = 0;  // the journal id this write ended at
private:
    RecState* make(uint32_t fid, int64_t seq, const uint8_t* key);
    int32_t loadRows(RecState* r);
    int32_t journalWrite();
    int32_t commitFile(Feed* f, const std::vector<RecState*>& recs);
    int32_t indexCommit();
    void fill(RecState* r);
    std::deque<RecState> store_;
    std::vector<RecState*> order_;
    std::map<std::pair<uint32_t, std::string>, RecState*> byKey_;
    std::map<std::pair<uint32_t, int64_t>, RecState*> bySeq_;
};

// ---- remove.cpp ----------------------------------------------------------------------------------
void supersedeOp(P4Engine* e, Type* t, WriteTask* task);
void deleteOp(P4Engine* e, Type* t, WriteTask* task);
void quotaWork(P4Engine* e, Type* t, Internal* in);    // in->seqs: delete these records
void rebuildWork(P4Engine* e, Type* t, Internal* in);  // in->what: 1, 2, 8

// ---- reader.cpp -----------------------------------------------------------------------------------
int32_t runRead(P4Lane* L, uint32_t op);  // ops 10-17 on a lane

// ---- maintain.cpp ----------------------------------------------------------------------------------
void maintenanceLoop(P4Engine* e, uint32_t thread);
void slowLoop(P4Engine* e);
int32_t quotaGc(P4Engine* e, uint64_t maxBytes, int64_t* records, int64_t* bytes, bool enforce);
int32_t ftsCatchUp(P4Engine* e, Type* t, bool all);
int walHook(void* arg, sqlite3* db, const char* zDb, int nPages);
// Each WAL's frames not yet checkpointed (4 KiB pages), per engine; their sum
// is the WAL stat, refreshed on every change (commits and checkpoints).
int64_t walBytesOf(P4Engine* e, const std::string& path);
void walNote(P4Engine* e, const std::string& path, int64_t frames);  // 0 forgets the path
void walExtNote(P4Engine* e, const std::string& path, int64_t frames);  // a WAL's extent (0: started over / gone)
void typeFileBytes(Type* t);                  // refreshes idxBytes and ftsBytes (maintenance thread)
// Runs internal work on the type's writer thread and waits for it.
int32_t runOnWriter(P4Engine* e, Type* t, int op, Internal* in);

// ---- mailbox.cpp --------------------------------------------------------------------------------------
extern thread_local uint32_t tThread;  // the running service thread's index
int32_t mailboxInit(P4Engine* e, std::string* err);
void mailboxLayout(P4Engine* e, FlatsqlP4Layout* out);
int32_t startThreads(P4Engine* e);
// Queues a write task on its type's writer (internal tasks too).
void pushTask(P4Engine* e, Type* t, WriteTask* wt);
// The slot's output: append bytes to its ring (waits for space; checks cancel).
int32_t emitBytes(P4Lane* L, const uint8_t* p, size_t n);
int32_t flushOut(P4Lane* L);    // L->out to the ring (honours cancel and the caps)
int32_t flushFinal(P4Lane* L);  // the op's last bytes, whatever tripped
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

}  // namespace p4
}  // namespace flatsql

#endif
