// FlatSQL store format 4 ("p4"): engine internals.
//
// One source feed x standard is one feed (CONTRACT C-37, C-38, owner layout of
// 2026-10-02; BRIEF4, owner 2026-10-03: "use flatbuffers as the backing tech
// and stream them directly while indices and btrees (SQLite database
// metadata) were created"). A feed is two files:
//   P/<TYPE>/<feed>.fsdata  the feed's records as received, a pure FlatBuffer
//                           stream: [u32 LE size][FlatBuffer] ... and nothing
//                           else (no header, tag, CRC, padding or trailer), so
//                           a stock FlatBuffers reader walks it;
//   P/<TYPE>/<feed>.db      SQLite 3.53.4 unmodified, through FlatSQL's own VFS
//                           (flatsql_io): the index rows and per-row metadata,
//                           never record bytes.
// A feed is a record's (provider, source) tag pair; a record with no source
// lives in the local feed (P/<TYPE>/local.*). The interfaces are CONTRACT.md
// §1-§4; docs/STORE-FORMAT-4.md is the as-built description.
//
// A feed file is its own table: a record is identified within it by its CID
// (the file's one CID index) and its seq; there is no cross-feed identity
// (C-38). A record that arrives through a second feed is a new row set in
// that feed's file with its own seq (ingest); a migrated record keeps format
// 1's rowid as its seq in every feed holding it. Within a feed file the rows
// of a record are its copies x its instances (every copy, the publishing
// node, with every instance, a batch and content key of the feed); local
// rows carry no instance. A row points at its record's bytes in the feed's
// stream (off, len: the frame at off is [u32 len][bytes]; the rows of a
// record share a frame when their bytes are the same) and keeps its
// signature, epoch, object key, ts, the delivery's `at`, and small per-file
// ids for the node, the batch, the content key and the url. No provider or
// source string is in a row: the file is the feed.
//
// Layout under the root (<data>/fsql4):
//   STORE, MIGRATED                      markers (§2.2)
//   T/TYPES                              registered type names (append-only records with a crc)
//   T/<TYPE>.spec                        the registered spec TLV, plus a crc tag
//   T/<TYPE>.idx                         type index: the feed and token registries
//   T/<TYPE>.fts                         FTS5 (background)
//   P/<TYPE>/<feed>.fsdata, <feed>.db    one stream and one index per source feed of the type
//
// A write appends its new frames to each feed's stream, makes them durable
// (sync) and only then commits the rows that point at them with the stream's
// mark (its indexed end) in one SQLite transaction: the index never claims
// bytes the stream cannot back. Open cuts each stream to its mark.
//
// The ack writes no random index page (BRIEF4 ruling (B), owner: "stream
// them directly while indices and btrees ... were created in a separate
// thread"). A committed row is staged: in table r with m=0, outside the
// feed's random-keyed indexes (r_c CID, r_ke object + epoch, r_w epoch: partial
// indexes WHERE m=1), listed by r_m; the ordered indexes (r_s seq, r_b batch,
// r_a / r_t delivery time) take it at once (their pages are appends). The
// writer's indexer thread later merges a feed's staged rows into those
// indexes in one large transaction (m=1). Until then the reads that use them
// take the staged rows from the feed's in-memory view (Staged), which each
// commit and each merge replace and a read pins before its SQL runs.
//
// Threads: writers (every feed of a type is written by the type's one writer
// thread; a file has one writer connection), each with its indexer thread
// (commits, acks and merges), interactive / bulk / sandbox lanes, one
// maintenance thread and one long-work thread. Lock order, outermost first:
// Engine::typesMu, Type::mu, Feed::dictMu, Feed::smu, Engine::wconnMu. No lock
// is held across a feed file's I/O except by its one writer.
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
#include <initializer_list>
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
// kStQuotaFiles and kStTwoPhase are always 0 in this layout (a feed keeps its
// files; there are no merges): the frozen ABI keeps their slots, as it keeps
// SUPERSEDE's files_deleted and QUOTA_GC's files_dropped columns (always 0).
// kStPartitions counts feeds. With the streams the frozen names read:
// kStJournalSyncs = stream syncs (the stream is the record journal: a write
// syncs it before its index commit), kStIndexFlushes / kStIndexFlushEntries
// = type-index registry commits and their rows, kStUnlinked = stream
// generations a compaction retired and unlinked, kStRebuilds also counts
// compactions and indexes rebuilt from a stream.
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
    uint64_t restartBytes = 256ull << 20, walTotal = 4ull << 30, journalSizeLimit = 64ull << 20;
    // flushEntries (tag 42, "index flush entries"): a feed's staged rows are
    // merged into its indexes once it holds this many (Staged).
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
    S_R_SEQ, S_R_FRAME, S_R_INS, S_R_DEL, S_R_URL, S_R_OFF, S_R_MAXRID, S_META_SET, S_META_GEN,
    S_NODE_INS, S_BATCH_INS, S_CKEY_GET, S_CKEY_INS, S_URL_GET, S_URL_TEXT, S_URL_INS, S_CKEY_TEXT,
    S_INST_PUT, S_INST_DEL, S_R_CID, S_R_CIDSCAN, S_IDENT_GET, S_IDENT_INS, S_TOKC_PUT, S_TOKC_DEL,
    S_MOVED_INS, S_R_STAGE, S_IDST_INS, S_OKEY_INS,
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
// writers and the type index add dsync=1.
enum class OpenKind { Writer, Reader, IndexReader, Index, Maint };
// A feed index writer's spill threshold (PRAGMA cache_spill, KiB): a
// transaction's dirty pages stay in memory up to this before SQLite spills
// any to the WAL (a spilled page touched again is written again). A merge
// raises it to its own budget for its transaction.
constexpr uint32_t kWriterSpillKiB = 32768;
// A feed index file's page size, whatever the spec's tag 8 says: the file
// holds index rows only (no record bytes), every commit writes each page it
// dirties whole, and a page smaller than the VFS's 4 KiB sector drags its
// sector-mates along (the VFS claims no power-safe overwrite). WRITE-AMP:
// IQC's 16 KiB pages wrote 30% more.
constexpr uint32_t kIndexPageSize = 4096;
int openConn(const std::string& path, OpenKind kind, uint32_t cacheKiB, uint32_t pageSize, Conn** out,
             std::string* err);
int32_t statusOfSqlite(int rc);  // SQLite result -> P4 status (BUSY and I/O errors are errors, never misses)
extern thread_local char tSqlLog[200];  // this thread's last SQLite error log line (SQLITE_CONFIG_LOG)
// A connection's database bytes (pages x page size) and free bytes, from its
// header (no file I/O on a connection that just committed).
int64_t dbBytesOf(Conn* c, int64_t* freeBytes);
// A SQLite file's page size, recorded when a writing connection opens it
// (4096 until then): WAL frames are accounted in bytes.
void notePageSize(const std::string& path, uint32_t pageSize);
uint32_t pageSizeOf(const std::string& path);

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
// SQLite's order of k values: none < integers < text (BINARY).
int kCmp(const KVal& a, const KVal& b);

// ---- staged rows (staged.cpp) ---------------------------------------------------------------------
// A feed's committed rows that are not in its random-keyed indexes yet (r
// rows with m=0): what the reads that use those indexes need of each row.
struct StRow {
    int64_t rid = 0, w = 0, e = 0, at = 0, ts = 0;
    bool hasE = false;
    KVal k;
    uint8_t cid[32] = {};
    int64_t seq() const { return rid >> 16; }
};
// Rows in a few runs, each sorted four ways; the rows stay where they were
// made (chunks shared by successive views).
struct StRun {
    std::vector<std::shared_ptr<const std::vector<StRow>>> chunks;
    std::vector<const StRow*> byRid, byCid, byKe, byW;  // rid; (cid, rid); (k, e, rid); (w, rid)
};
using IdentKey = std::array<uint8_t, 32>;
// A feed's staged rows and staged ingest identities (idst), as one commit or
// merge left them. Immutable: the indexer publishes a new one (Feed::staged,
// Type::mu); a read pins the one it starts with, before its SQL runs, so a
// row merged after the pin is in both (and collapses: same key, same seq),
// one merged before it in the index only.
class Staged {
public:
    std::vector<std::shared_ptr<const StRun>> runs;
    std::vector<std::pair<IdentKey, int64_t>> idents;  // by hash, the latest seq of each
    size_t rows = 0;
    // base (may be null) less the rows `drop` names (and its identities when
    // dropIdents), plus `add` and `ids`; null when nothing is left.
    static std::shared_ptr<const Staged> make(const Staged* base, std::vector<StRow> add, const std::vector<int64_t>& drop,
                                              const std::vector<std::pair<IdentKey, int64_t>>& ids, bool dropIdents);
    void all(std::vector<const StRow*>* out) const;                           // rid order
    void ofCid(const uint8_t key[32], std::vector<const StRow*>* out) const;  // rid order
    void ofK(const KVal& k, std::vector<const StRow*>* out) const;           // (e, rid) order
    int64_t identSeq(const uint8_t h[32]) const;                              // 0: none
    // In w order (desc: descending), w from `from` (fromIncl: inclusive) to
    // `to` (inclusive), rid in [ridLo, ridHi]; past `limit` rows only the
    // last w group is completed. *more: rows are left in the range.
    void wRange(bool desc, int64_t from, bool fromIncl, int64_t to, int64_t ridLo, int64_t ridHi, size_t limit,
                std::vector<const StRow*>* out, bool* more) const;
    // In (cid, rid) order after (aCid, aRid) (aCid empty: from the start), up
    // to (bCid, bRid) inclusive (bCid null: to the end), rid in [ridLo, ridHi],
    // at most limit rows. *more: rows are left in the range.
    void cidRange(const std::string& aCid, int64_t aRid, const uint8_t* bCid, int64_t bRid, int64_t ridLo, int64_t ridHi,
                  size_t limit, std::vector<const StRow*>* out, bool* more) const;
    // The rows grouped by object key, keys in order (none first).
    void byObject(std::vector<std::pair<KVal, std::vector<const StRow*>>>* out) const;
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
// Strings joined by 0x1F, each escaped (0x1E and 0x1F behind a 0x1E), so a
// joined key names exactly one tuple whatever bytes its strings hold
// (GATES-feed-r2 N-A); a string with neither byte joins unchanged.
std::string unitJoin(std::initializer_list<const std::string*> parts);
// The strings of a unitJoin, `want` of them (missing ones empty).
std::vector<std::string> unitSplit(const std::string& s, size_t want);
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
// (each record's smallest copy), copies (record x copy), cbytes (each copy's
// bytes) and fbytes (the stream's live frames, size prefixes included: the
// stream's end less fbytes is what a compaction gives back). Bounds: the rest.
struct Counters {
    int64_t rows = 0, recs = 0, bytes = 0, nnull = 0, rbytes = 0, copies = 0, cbytes = 0, fbytes = 0;
    int64_t minseq = INT64_MAX, maxseq = 0, minw = INT64_MAX, maxw = INT64_MIN;
    int64_t mints = INT64_MAX, maxts = 0, mine = INT64_MAX, maxe = INT64_MIN;
};

// A producer token's copies in one feed file (exact: n, bytes; bounds: the
// rest), committed in the file's tokc table with its rows.
struct TokCount {
    int64_t n = 0, bytes = 0, mints = INT64_MAX, maxts = 0, maxseq = 0;
};

// One generation of a feed's record stream: P/<TYPE>/<name>.fsdata
// (generation 0) or <name>.<gen>.fsdata. A pure FlatBuffer stream:
// [u32 LE size][record bytes] ..., nothing else. One flatsql_io handle,
// offset-addressed, shared by the feed's writer and its readers. A generation
// a compaction replaced is unlinked when its last holder lets it go.
struct Stream {
    std::string path;
    uint32_t gen = 0;
    int32_t h = -1;
    std::atomic<bool> drop{false};
    ~Stream();
};

struct Feed {
    Type* type = nullptr;
    uint32_t fid = 0;
    std::string provider, source;  // both "" for local
    std::string name, path;        // path: the index (.db)
    bool local = false;
    // Type::mu
    Counters k;
    std::map<InstId, InstCount> inst;   // live instances (n > 0)
    std::map<uint32_t, TokCount> tokc;  // type token id -> its copies in this file (n > 0)
    bool created = false;      // exists on disk with its schema (readers skip it until then)
    bool quarantined = false;  // corrupt: P4_E_CORRUPT for ops that need it
    bool indexed = true;       // secondary indexes present (false between a migration's append and REBUILD 1)
    bool registered = false;   // in the type index (a write registers it before its first commit)
    int64_t dbBytes = -1, freeBytes = 0;  // pages and free pages after its writer's last commit (-1: not read)
    int64_t streamBytes = 0;   // the stream's committed end (its mark)
    uint32_t regGen = 0;       // the stream generation the type index records (a missing index's rebuild reads it)
    // dictMu
    std::mutex dictMu;
    Dict dict;
    // the record stream (smu): the current generation, and the generation a
    // compaction replaced, kept for readers on an older snapshot until retireAt
    std::mutex smu;
    std::shared_ptr<Stream> stream;
    std::shared_ptr<Stream> retired;
    int64_t retiredBytes = 0;  // its size (DiskUsage counts it until it goes)
    uint64_t retireAt = 0;
    // The writer's (and the open's): the append offset (past the frames of a
    // unit its indexer has not committed yet), and the current generation.
    int64_t end = 0;
    uint32_t gen = 0;
    // The committed mark (Type::mu; the indexer sets it with each commit).
    int64_t mark = 0;
    // Moves (a local record taken into this feed): the file's `moved` table
    // may hold rows, and they are all finished (their local rows are gone).
    bool movedRows = false, movedDone = true;
    // The staged rows and identities (Type::mu; replaced by commits and
    // merges on the type's indexer, never changed in place), and when rows
    // were last staged (monoNs).
    std::shared_ptr<const Staged> staged;
    uint64_t stagedAt = 0;
    uint64_t mergeAfter = 0;  // a failed merge is tried again after this (monoNs)
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
    bool registered = false;  // in the type index
};

// A registered spec and what the engine derives from it. Immutable once
// built; a re-registration swaps in a new one (Type::spec, under Type::mu).
struct Spec {
    std::string name;
    std::vector<uint8_t> bytes;  // the registered TLV
    ps::TypeConfig tc;
    uint32_t pageSize = 4096;  // spec tag 8: accepted and kept; a feed index uses kIndexPageSize
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
class WriteCtx;
class PutUnit;

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
    std::string pIdx, pFts, pSpec, pDir;
    // The type index exists (the type has had a write, or it was on disk at
    // open). Set once, under openMu; readers check it first.
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
    // The next seq: above every seq any feed file's meta holds (seqs never go
    // back: a file keeps its max seq after deletes), read at open.
    int64_t nextSeq = 1;
    std::atomic<int64_t> vis{0};  // visible-through (B2)
    std::vector<std::pair<int64_t, int64_t>> inflight;  // (lo, hi) of new seqs in flight
    bool overQuota = false;

    // The type's writer thread owns these (and init before threads run).
    uint32_t owner = 0;
    struct Conn* idx = nullptr;  // type index (writer)

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
// Internal ops (never a slot's): a feed's stream compaction.
constexpr int kOpCompact = 1000;
struct Internal {
    std::atomic<bool> done{false};
    int32_t status = P4_OK;
    std::string err;
    int64_t a = 0, b = 0;  // QUOTA: records, bytes; REBUILD: entries, mismatches; COMPACT: bytes before, after
    std::vector<std::pair<uint32_t, int64_t>> seqs;  // QUOTA: the (feed, seq) row sets to delete
    uint32_t what = 0;          // REBUILD; COMPACT: the feed id
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
    // The writer's indexer thread and its queue (imu): units in plan order.
    // A round takes every queued unit. After a failed round the indexer fails
    // every unit it takes (poison) until the writer has cut the streams back.
    std::mutex imu;
    std::condition_variable icv, dcv;  // work for the indexer; a unit done
    std::deque<PutUnit*> iq;
    bool istop = false;
    int32_t poison = P4_OK;
    std::thread indexer;
    // Merges (the indexer's, between rounds) never run while the writer
    // reads and writes the feed files itself (hold, set by drain): the
    // writer waits out a running merge (merging) first.
    bool hold = false, merging = false;
    // The writer's: its units not yet collected, oldest first.
    std::deque<std::unique_ptr<PutUnit>> pending;
    bool failing = false;          // a collected unit failed: recover before planning
    std::set<Feed*> resetFeeds;    // the feeds the failed units appended to
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
    // Staged rows over every feed (merges start with the largest feed past 4x flushEntries).
    std::atomic<int64_t> stagedRows{0};
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
int32_t engineActivate(P4Engine* e, std::string* err);
int32_t engineStop(P4Engine* e, double deadlineMs);
int32_t engineStats(P4Engine* e, uint8_t* out, int32_t cap);
// The type index, made on the type's first write (lazy T/ files).
int32_t typeFilesEnsure(Type* t, std::string* err);
// The feed of (provider, source); ("", "") is local. mu held. create: register
// it (its files are made by its first write); *made is set when it is new.
Feed* feedFor(Type* t, const std::string& provider, const std::string& source, bool create, bool* made = nullptr);
Feed* feedRestore(Type* t, uint32_t fid, const std::string& provider, const std::string& source, const std::string& name);  // mu held
uint32_t tokFor(Type* t, const std::string& token, const std::string& peer, bool create, bool* made = nullptr);  // mu held
std::string pathJoin(const std::string& a, const std::string& b);

// ---- stream.cpp: a feed's record stream ----------------------------------------------------------------
// P/<TYPE>/<name>.fsdata for generation 0, <name>.<gen>.fsdata after.
std::string streamPath(const Feed* f, uint32_t gen);
// The feed's current stream, opened (made when absent) on first use. The
// writer's and the open's (readers use streamAt).
std::shared_ptr<Stream> streamCur(Feed* f, int32_t* rc);
// The stream of generation gen for a reader whose snapshot names gen: the
// current one, or the one a compaction just replaced while it is kept. nullptr
// with *rc == P4_OK: that generation is gone (the snapshot is older than the
// grace: the caller starts a new read transaction).
std::shared_ptr<Stream> streamAt(Feed* f, uint32_t gen, int32_t* rc);
// The record bytes of the frame at off: its size prefix must be len.
int32_t streamRead(Stream* s, int64_t off, int64_t len, std::string* out);
// Appends bytes at off (the caller's end); syncs when sync is set.
int32_t streamWrite(Stream* s, int64_t off, const uint8_t* p, size_t n);
int32_t streamSync(Stream* s);
int32_t streamTruncate(Stream* s, int64_t size, bool sync);
int64_t streamSize(Stream* s);
// The stream generation a read transaction sees (the file's meta 'gen').
int32_t snapGen(Conn* c, uint32_t* gen);
// Closes the feed's streams (engine stop).
void streamClose(Feed* f);

// ---- type_index.cpp -----------------------------------------------------------------------------
// The registries: feed(fid, provider, source, name, gen) and tok(id, token,
// peer). Nothing else: a feed file's counters are in the file (read at open).
int32_t typeIndexOpen(Type* t, std::string* err);
int indexPutFeed(Conn* idx, const Feed& f, uint32_t gen);  // SQLite rc (Type::mu not needed: the strings are fixed)
int indexPutTok(Conn* idx, uint32_t id, const TokDef& d);   // SQLite rc

// ---- partition.cpp (feed files) -------------------------------------------------------------------
// The writer connection of a feed file (made with its schema on first use), pinned for the caller.
Conn* writerPin(P4Engine* e, Feed* f, int32_t* rc, std::string* err);
void writerUnpin(P4Engine* e, Feed* f);
void writerDrop(P4Engine* e, Feed* f);  // closes the feed's unpinned writer connection (before its file goes)
int32_t fileCreateIndexes(Type* t, Conn* c, bool local);
int32_t fileSchema(Type* t, Conn* c, Feed* f, bool indexes);
// A feed file's meta: its counters, the stream's committed mark and generation.
struct FileMeta {
    Counters k;
    bool indexed = true;
    int64_t mark = 0;
    uint32_t gen = 0;
};
int writeMeta(Conn* c, const Counters& k, int64_t now, int64_t mark, uint32_t gen);  // in the caller's write transaction; SQLite rc
int readMeta(Conn* c, FileMeta* m);                                                    // SQLite rc
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
// The seq of a CID in a feed file (0: none): its staged rows (st, pinned before
// c's read), then r_c (or a walk before REBUILD 1).
int32_t seqOfCid(Conn* c, bool indexed, const Staged* st, const uint8_t key[32], int64_t* seq);
// Whether a feed file holds rows of seq (its rid range).
int32_t fileHoldsSeq(Conn* c, int64_t seq, bool* held);
// A feed at open (before any read, M8): its counters, instances and tokens
// from its file, its stream cut to the committed mark, a missing or damaged
// index rebuilt from the stream, a crashed compaction's leftovers unlinked.
// *moved: the seqs its `moved` table names (moves into it from local).
int32_t feedOpen(Type* t, Feed* f, std::vector<int64_t>* moved, std::string* err);
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
    int64_t off = -1;          // its frame in the file's stream (-1: a new row whose frame the commit appends)
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
// One feed file's index transaction in a commit round (applyRound): the
// round's writes apply their rows to it in plan order, then it commits once,
// its stream synced first.
struct FileTxn {
    Feed* f = nullptr;
    struct Conn* c = nullptr;
    Dict dict;
    Counters k;
    std::map<InstId, InstCount> inst;
    std::map<uint32_t, TokCount> tokc, tokcBefore;
    std::set<InstId> changedInst;
    std::vector<int64_t> ftsDrop;  // full-text rows to delete after the commit
    int64_t newEnd = 0;            // the stream's mark after the round's frames
    // Staged rows: the feed's view at the transaction's start, the rows it
    // stages (by rid) and the staged rows it deletes, the identities it
    // stages; then the view it publishes.
    std::shared_ptr<const Staged> stBase, stNew;
    std::map<int64_t, StRow> stAdd;
    std::vector<int64_t> stDrop;
    std::vector<std::pair<IdentKey, int64_t>> idAdd;
    bool indexed = true, clearMoved = false, deleted = false, epochEdge = false, moves = false, open = false;
    int rc = 0;
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
    int32_t loadD(uint32_t fid, RowR& r);         // the stored bytes of an existing row (its frame)
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
    // The writer: what this write commits (its new rows' rids and frames, the
    // new feeds and tokens registered, the frames appended to each feed's
    // stream, the moves), and the state it leaves its records in (the next
    // unit's seed).
    int32_t prepare();
    // The indexer: each feed file (feeds before local): its stream synced,
    // then its rows committed with the stream's new mark in one transaction.
    int32_t apply();
    // prepare + apply on the caller's thread (the writer's synchronous work,
    // its indexer idle); a failure cuts the streams back to their marks.
    int32_t commit();
    // After a failed apply: the feeds this write appended to go back to their
    // committed marks.
    void resetStreams();
    // The type's units still being applied, oldest first: their records'
    // state after them is this write's starting point (planning reads see
    // only committed rows).
    void seed(const std::vector<const WriteCtx*>& prevs);
    bool seededFeed(uint32_t fid) const;
    void seededWithK(uint32_t fid, const KVal& k, std::vector<RecState*>* out);  // seeded records of an object key
    RecState* seededIdent(uint32_t fid, const uint8_t h[32]);                     // a seeded ingest identity
    bool migrate = false;  // migrate mode: instance times are the caller's (C-36), new files without indexes
    int64_t now = 0;
    std::vector<IdentNew> idents;
    // New feeds and tokens into the type index (prepare, the writer). The
    // commit round's steps (applyRound): the feed files this write touches
    // (feeds before local) and whether it changes one; its rows applied to a
    // feed's round transaction.
    int32_t registryWrite();
    const std::vector<uint32_t>& files() const { return fileOrder_; }
    bool fileChanges(uint32_t fid) const;
    int32_t fileCreate(Feed* f);  // the writer: a new feed's index file, before its first frame
    int32_t fileBegin(FileTxn& x, Feed* f);
    int32_t fileApply(FileTxn& x);
    bool pending() const { return any_; }  // prepared with something to commit
private:
    RecState* make(uint32_t fid, int64_t seq, const uint8_t* key);
    int32_t loadRows(RecState* r);
    int32_t frames(Feed* f, const std::vector<RecState*>& recs, std::string* out, int64_t end);
    void fill(RecState* r);
    void snapshot();
    std::deque<RecState> store_;
    std::vector<RecState*> order_;
    std::map<std::pair<uint32_t, std::string>, RecState*> byKey_;
    std::map<std::pair<uint32_t, int64_t>, RecState*> bySeq_;
    // prepare()'s plan for apply()
    bool any_ = false;
    std::vector<uint32_t> fileOrder_, locals_;
    std::map<uint32_t, std::vector<int64_t>> movesIn_;
    std::map<uint32_t, int64_t> newEnd_, oldEnd_;  // each feed's stream end after / before this write's frames
    // the state after this write (snapshot), with the lookups a later unit
    // makes into it; and this write's seeds (pending units, newest first,
    // used while planning only)
    std::vector<RecState> post_;
    std::map<std::pair<uint32_t, int64_t>, size_t> postSeq_;
    std::map<std::pair<uint32_t, std::string>, size_t> postKey_, postIdent_;
    std::map<std::pair<uint32_t, std::string>, std::vector<size_t>> postK_;
    std::set<uint32_t> postFids_;
    std::vector<const WriteCtx*> seeds_;
    RecState* materialize(const RecState& s);
};
// A commit round over several prepared writes (the indexer's), or one
// (WriteCtx::apply). A failure fails the round; files committed before it stay.
int32_t applyRound(P4Engine* e, const std::vector<WriteCtx*>& units, std::string* err);
int32_t fileCommit(P4Engine* e, Type* t, FileTxn& x, std::string* err);
// Planning reads go through the reader pool (committed rows): the writer
// connection belongs to the indexer (the writer pins it only to create a new
// feed's index file, before any unit of that feed is queued).
Conn* planAcquire(P4Engine* e, Feed* f, int32_t* rc, std::string* err);
void planRelease(P4Engine* e, Conn* c);

// ---- the PUT pipeline (partition.cpp plans, mailbox.cpp runs it) --------------------------------------
// A PUT group: planned on the type's writer thread (checks, delivery, seqs,
// rids and frames, the frames appended to the feeds' streams), then applied
// and answered on the writer's indexer thread (each stream synced, then the
// index rows committed with its mark, then the calls acked) while the writer
// plans the next group. Units are applied and answered in the order they were
// planned; the next group of the same type starts from the state the unit
// still being applied leaves its records in (its seed).
class PutUnit {
public:
    virtual ~PutUnit() = default;
    virtual WriteCtx* applyCtx() = 0;      // the indexer: the prepared write to commit, or nullptr
    virtual void finish(int32_t rc, const std::string& why) = 0;  // the indexer: in-flight seqs released, calls answered
    virtual void resetStreams() = 0;       // the writer: after a failed round
    virtual const WriteCtx* ctx() const = 0;
    Type* type = nullptr;
    bool seeded = false;  // planned on pending units' state
    std::atomic<bool> done{false};
    int32_t status = P4_OK;
};
// prevs: the writer's pending units, oldest first (those of the type seed it).
std::unique_ptr<PutUnit> putPlan(P4Engine* e, Type* t, std::vector<WriteTask*>& tasks, const std::vector<const PutUnit*>& prevs);

// ---- remove.cpp ----------------------------------------------------------------------------------
void supersedeOp(P4Engine* e, Type* t, WriteTask* task);
void deleteOp(P4Engine* e, Type* t, WriteTask* task);
void quotaWork(P4Engine* e, Type* t, Internal* in);    // in->seqs: delete these records
void rebuildWork(P4Engine* e, Type* t, Internal* in);  // in->what: 1, 2, 8
// A feed's stream rewritten with its live frames only, as the next generation
// (a pure stream again), its rows repointed in one transaction; the old
// generation is unlinked once no reader holds it. in->what: the feed id.
void compactWork(P4Engine* e, Type* t, Internal* in);
// Whether a feed's stream is worth compacting (its dead share). Type::mu held.
bool compactDue(const Feed* f);
// A missing or damaged index made again from the feed's stream (at open,
// after every other feed of the type is open: fresh seqs). *indexed: records.
int32_t rebuildFeed(P4Engine* e, Type* t, Feed* f, int64_t* indexed, std::string* err);

// ---- staged.cpp: the merge -------------------------------------------------------------------------
// One merge transaction of a feed: its staged rows (CID order, at most
// 2 x flushEntries) set m=1, which puts them in r_c, r_ke and r_w, and its
// staged identities into ident; then the view without them. On the type's
// indexer (or its writer, the indexer held). A failure changes nothing.
int32_t mergeFeed(P4Engine* e, Type* t, Feed* f, std::string* err);
// Every staged row of the type merged (REBUILD 1; the writer, indexer held).
int32_t mergeAll(P4Engine* e, Type* t, std::string* err);
// The indexer's merge step between rounds: the most due feed of the
// writer's types, if any (writer not holding the files).
void mergeStep(P4Engine* e, uint32_t writer);
// A feed's view rebuilt from its file at open (r_m and idst).
int32_t stagedLoad(P4Engine* e, Feed* f, Conn* c, std::string* err);
// REBUILD 8: whether a feed's view holds exactly its file's staged rows and identities (0: yes).
int64_t stagedMismatches(Feed* f, Conn* c);
// Publishes a feed's new view (Type::mu held).
void stagedPublish(P4Engine* e, Feed* f, std::shared_ptr<const Staged> v, bool added);

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
