// FlatSQL store format 4, SQL surface: shared internals (CONTRACT.md §3.9).
//
//   heap.cpp      the per-lane allocator (SQLITE_CONFIG_MALLOC dispatch, the
//                 sandbox heap cap, the hard heap counter of C-30),
//                 copy-adapted from ps/lane_arena.cpp
//   columns.cpp   format 1's relation columns and cell values, from the BFBS
//   vtab.cpp      the relation module over the p4 reader (from ps/vtab_*)
//   lane.cpp      p4sql_lane_init/exec/surface/lane_free (from ps/lane.cpp
//                 and ps/admission.cpp)
#ifndef FLATSQL_P4SQL_INTERNAL_H
#define FLATSQL_P4SQL_INTERNAL_H

#include <sqlite3.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <vector>

#include "flatsql/p4/flatsql_p4.h"
#include "flatsql/p4sql/p4sql.h"

namespace flatsql {
namespace p4sql {

// ---------------------------------------------------------------------------
// Per-lane arena (heap.cpp), copy-adapted from ps/lane_arena.h
// ---------------------------------------------------------------------------
// A TLSF allocator over one fixed region, used only by its lane's thread.
class LaneArena {
public:
    LaneArena() = default;
    ~LaneArena();
    LaneArena(const LaneArena&) = delete;
    LaneArena& operator=(const LaneArena&) = delete;

    // Reserves `bytes` (rounded to 64 KiB). Natively the region is an
    // anonymous mapping: untouched pages cost no resident memory.
    bool init(size_t bytes);
    void* alloc(size_t n);
    void free(void* p);
    void* realloc(void* p, size_t n);
    size_t usable(const void* p) const;
    bool owns(const void* p) const {
        return reinterpret_cast<uintptr_t>(p) >= base_ && reinterpret_cast<uintptr_t>(p) < end_;
    }

    size_t capacity() const { return cap_; }
    size_t used() const { return used_; }
    size_t highWater() const { return high_.load(std::memory_order_relaxed); }
    uint64_t failures() const { return failures_.load(std::memory_order_relaxed); }
    void resetHighWater() { high_.store(used_, std::memory_order_relaxed); }
    // Self-check of every block and free list (tests).
    bool check(std::string* why) const;

private:
    static constexpr int kSlLog2 = 4;
    static constexpr int kSlCount = 1 << kSlLog2;
    static constexpr int kFlShift = kSlLog2 + 4;  // small blocks: < 256 bytes, 16-byte classes
    static constexpr int kFlCount = 40 - kFlShift + 1;
    struct Block;
    void insertFree(Block* b);
    void removeFree(Block* b);
    Block* findFree(size_t size);
    static void mapping(size_t size, int* fl, int* sl);

    uintptr_t base_ = 0;
    uintptr_t end_ = 0;
    size_t cap_ = 0;
    size_t mapped_ = 0;
    size_t used_ = 0;
    std::atomic<size_t> high_{0};
    std::atomic<uint64_t> failures_{0};
    uint64_t flBitmap_ = 0;
    uint32_t slBitmap_[kFlCount] = {};
    Block* heads_[kFlCount][kSlCount] = {};
};

int32_t heapInstall();               // SQLITE_CONFIG_MALLOC; before sqlite3_initialize
void heapLimitRefresh();             // reads sqlite3_hard_heap_limit64 into the allocator (C-30)
void arenaBind(LaneArena* a);         // the calling thread's arena (nullptr = the system allocator)
LaneArena* arenaBound();
bool arenaRegister(LaneArena* a);     // so a pointer finds its arena from any thread; false: no slot
void arenaUnregister(LaneArena* a);

// Runs an engine call on the system allocator: the engine's reader
// connections are shared by every lane and never live in a lane's arena.
class EngineCall {
public:
    EngineCall() : saved_(arenaBound()) {
        if (saved_) arenaBind(nullptr);
    }
    ~EngineCall() {
        if (saved_) arenaBind(saved_);
    }
    EngineCall(const EngineCall&) = delete;
    EngineCall& operator=(const EngineCall&) = delete;

private:
    LaneArena* saved_;
};

// Sandbox CPU work without reads (recursive CTEs, cross joins of constants):
// VM steps, counted by the progress handler (format 2's default).
constexpr uint64_t kSandboxMaxVmSteps = 200000000ull;

// ---------------------------------------------------------------------------
// Columns (columns.cpp)
// ---------------------------------------------------------------------------
// Format 1's column kinds: how its engine reads a declared field.
enum ColKind : uint8_t {
    kColNull = 0,   // a type its engine cannot read (table, struct, non-byte vector,
                    // an enum its pinned table graph names unresolved): always NULL
    kColBool, kColI8, kColU8, kColI16, kColU16, kColI32, kColU32, kColI64, kColU64,
    kColF32, kColF64,
    kColText,
    kColBlob,
};

struct Column {
    std::string name;
    uint16_t slot = 0;          // vtable slot = the field's id (positional)
    ColKind kind = kColNull;
    bool placeholder = false;   // the >= 1 column fallback: a one-byte read of slot 0
    const char* sqlType() const;
};

// Format 1's declared columns of a type (SDN engineDeclaredColumns): the
// leading run of representable root fields in declaration order, a union's
// discriminator ending the run, slot 0 as a one-byte placeholder when none is
// representable; OMM and TBS keep their pinned table graphs (columns.cpp).
bool format1Columns(const std::string& type, const uint8_t* bfbs, size_t n, std::vector<Column>* out,
                    std::string* err);
// Format 1's value of column `c` in a record payload (no size prefix).
void resultColumn(sqlite3_context* ctx, const Column& c, const uint8_t* data, size_t n);
// The record payload format 1 stores: `d` without a 4-byte size prefix when
// it carries one in front of the type's file identifier.
void payloadOf(const uint8_t fid[4], const uint8_t* d, size_t n, const uint8_t** p, size_t* len);

// ---------------------------------------------------------------------------
// Lane state
// ---------------------------------------------------------------------------
// One registered type, as the lane's relations see it.
struct TypeEntry {
    std::string name;                 // "OMM"
    uint8_t fid[4] = {0, 0, 0, 0};
    uint64_t bound = 0;               // A18
    std::vector<Column> cols;
    int col0 = -1, col1 = -1;         // schema column mapped to COL0 (int) / COL1 (text), or -1
};

// The statement a lane is running.
struct Stmt {
    bool sandbox = false;
    int32_t status = 0;               // a P4 status raised inside a vtab or the progress handler
    std::string message;
    uint64_t vmSteps = 0;
    LaneArena* arena = nullptr;         // bound for a sandboxed statement
    uint64_t arenaFailures = 0;         // the arena's failures when it was bound
    // An allocation of this statement failed against the sandbox heap cap.
    bool capTripped() const { return arena && arena->failures() > arenaFailures; }
    void raise(int32_t st, const std::string& msg) {
        if (status == 0) {
            status = st;
            message = msg;
        }
    }
};

enum RelKind : uint8_t { kRelType = 1, kRelAlias = 2 };

// A relation created on the lane's connection.
struct RelSpec {
    RelKind kind = kRelType;
    std::string type;     // canonical type name
    std::string source;   // canonical source (kRelAlias)
    bool local = false;   // kRelAlias "<TYPE>@local": the type's local file (its records no feed holds, C-39 S1)
};

struct LaneState {
    P4Lane* lane = nullptr;
    LaneArena* arena = nullptr;   // sandboxed statements' heap, made at the first one
    sqlite3* db = nullptr;
    std::map<std::string, TypeEntry> types;      // key: lower-case name
    std::map<std::string, RelSpec> relations;    // key: lower-case relation name
    Stmt* cur = nullptr;
};

LaneState* stateOf(P4Lane* lane);

// ---------------------------------------------------------------------------
// Relations (vtab.cpp)
// ---------------------------------------------------------------------------
// A reader predicate a scan carries (CONTRACT §3.5 tag 17 / P4Pred), one value.
struct ScanPred {
    uint8_t field = 0, op = 0;
    bool isText = false;
    int64_t i = 0;
    std::string text;
};

// What a relation scan is asked for (the pushed-down constraints).
struct ScanArgs {
    int64_t seqAfter = 0, seqThrough = 0;   // exclusive / inclusive; 0 = none
    std::vector<ScanPred> preds;
    bool desc = false;                      // seq descending, else ascending
    bool hydrate = true;
    bool tags = true;                       // _source is read
};

// The rows of one relation through one reader cursor with the type's A18
// bound N (C-31): "<TYPE>@<source>" passes lane.source, so the reader walks
// that source's newest N; <TYPE> passes no lane filter, so it reads the
// type's newest N, each record once. The one implementation of the relation
// semantics, for the virtual tables and the raw-stream fast path.
class RelScan {
public:
    RelScan() = default;
    ~RelScan() { close(); }
    RelScan(const RelScan&) = delete;
    RelScan& operator=(const RelScan&) = delete;
    // Positions on the first row. P4_OK or a status.
    int32_t open(LaneState* ls, const TypeEntry& t, const RelSpec& rel, const ScanArgs& a);
    // A plan that reads no column: n rows without reading them (COUNT(*)).
    void openCounted(int64_t n);
    int32_t next();   // P4_OK or a status
    bool eof() const { return eof_; }
    const P4Row& row() const { return row_; }
    // The row's _source: the relation's "<TYPE>@<source>"; for <TYPE>,
    // "<TYPE>@" + the matched (earliest live) tag's source, or
    // "<TYPE>@local" for a record with no live tag.
    const std::string& sourceText();
    void close();

private:
    P4Cursor* c_ = nullptr;
    int64_t counted_ = -1;   // rows left of a counted plan (-1: a cursor)
    P4Row row_{};
    bool eof_ = true;
    bool alias_ = false;
    std::string prefix_;   // "<TYPE>@"
    std::string source_;   // the row's _source (fixed for an alias)
    // The spec's storage: deques keep every element in place while the spec
    // points into them, for the cursor's lifetime.
    std::deque<P4Value> vals_;
    std::vector<P4Pred> preds_;
    std::deque<std::string> strs_;
    P4ScanSpec spec_{};
};

// The relation `name` names (case-insensitive): 1 found, 0 not a relation,
// < 0 status. Creates nothing.
int32_t resolveRelation(LaneState* ls, const std::string& name, RelSpec* out, std::string* err);
int registerModule(LaneState* ls);
// Refreshes the registered types from the engine (p4_types).
int32_t loadTypes(LaneState* ls, std::string* err);
const TypeEntry* typeByName(LaneState* ls, const std::string& name);   // case-insensitive
// Creates the relation `name` when it names one: 1 created, 0 not a
// relation, < 0 status.
int32_t ensureRelation(LaneState* ls, const std::string& name, std::string* err);
bool isRelation(const LaneState* ls, const char* name);
// The sources of a type with >= 1 live tag, sorted (byte order).
int32_t sourcesOf(LaneState* ls, const std::string& type, std::vector<std::string>* out);

std::string lower(const std::string& s);

}  // namespace p4sql
}  // namespace flatsql

#endif
