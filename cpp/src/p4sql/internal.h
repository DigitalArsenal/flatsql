// FlatSQL store format 4, SQL surface: shared internals (CONTRACT.md §3.9).
//
//   heap.cpp      the per-lane allocator (SQLITE_CONFIG_MALLOC dispatch, the
//                 sandbox heap cap), copy-adapted from ps/lane_arena.cpp
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
#include <map>
#include <string>
#include <vector>

#include "flatsql/p4/flatsql_p4.h"
#include "flatsql/p4sql/p4sql.h"

namespace flatsql {
namespace p4sql {

// ---------------------------------------------------------------------------
// Per-lane heap (heap.cpp)
// ---------------------------------------------------------------------------
// Every SQLite allocation made while a lane's heap is bound to the calling
// thread is accounted to that lane; a limit (set for sandboxed statements)
// fails the allocation (SQLITE_NOMEM) instead of taking memory the writers
// need. Allocations of unbound threads (writers, the engine's own reader
// connections) are unaccounted. A pointer records its owner, so it may be
// freed from any thread.
struct LaneHeap {
    std::atomic<int64_t> used{0};
    std::atomic<int64_t> limit{0};      // bytes this lane may hold; 0 = none
    std::atomic<bool> tripped{false};   // an allocation failed against the limit
};
int32_t heapInstall();                  // SQLITE_CONFIG_MALLOC; before sqlite3_initialize
void heapBind(LaneHeap* h);             // the calling thread's heap (nullptr = none)
LaneHeap* heapBound();

// Unbinds the lane heap for a call into the engine: the engine's reader
// connections are shared by every lane and must not count against one.
class EngineCall {
public:
    EngineCall() : saved_(heapBound()) { heapBind(nullptr); }
    ~EngineCall() { heapBind(saved_); }
    EngineCall(const EngineCall&) = delete;
    EngineCall& operator=(const EngineCall&) = delete;

private:
    LaneHeap* saved_;
};

// Default cap of a sandboxed statement's lane heap (config tag 48's default;
// p4_reader.h does not expose the configured value).
constexpr int64_t kSandboxHeapCap = 64ll << 20;
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
    uint64_t rowsExamined = 0;        // rows taken from reader cursors
    uint64_t bytesRead = 0;           // hydrated record bytes
    uint64_t vmSteps = 0;
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
};

struct LaneState {
    P4Lane* lane = nullptr;
    LaneHeap heap;
    sqlite3* db = nullptr;
    std::map<std::string, TypeEntry> types;      // key: lower-case name
    std::map<std::string, RelSpec> relations;    // key: lower-case relation name
    Stmt* cur = nullptr;
    // The last statement's error text. CONTRACT §3.9 has no way yet to put
    // it in the slot's err (requested: p4_lane_set_error).
    std::string lastError;
};

LaneState* stateOf(P4Lane* lane);

// ---------------------------------------------------------------------------
// Relations (vtab.cpp)
// ---------------------------------------------------------------------------
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
