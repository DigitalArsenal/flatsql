#ifndef FLATSQL_SQLITE_VTAB_H
#define FLATSQL_SQLITE_VTAB_H

#include "flatsql/types.h"
#include "flatsql/storage.h"
#include "flatsql/sqlite_index.h"
#include <sqlite3.h>
#include <functional>
#include <unordered_set>

namespace flatbuffers { class EncryptionContext; }

namespace flatsql {

// Forward declarations
class FlatBufferVTab;
class FlatBufferCursor;

/**
 * A database's record key (FlatSQLDatabase::setEncryptionKey). The database
 * owns it at a stable address and a table reads it at query time, so a key set
 * or replaced after the table was registered is the key the table uses.
 */
struct RecordKey {
    const flatbuffers::EncryptionContext* ctx = nullptr;
};

struct VTabCreateInfo;

// Field extractor function type - extracts field values from raw FlatBuffer
using FieldExtractor = std::function<Value(const uint8_t* data, size_t length, const std::string& fieldName)>;

// Fast extractor that writes directly to SQLite - bypasses Value construction
// Returns true if the column was set, false to fall back to FieldExtractor
// Using raw function pointer instead of std::function to eliminate type erasure overhead
using FastFieldExtractor = bool(*)(const uint8_t* data, size_t length, int columnIndex, sqlite3_context* ctx);

// Batch extractor that extracts all columns at once - more efficient than per-column extraction
// Fills the output vector with all column values
using BatchExtractor = void(*)(const uint8_t* data, size_t length, std::vector<Value>& output);

// Scan type for cursor
enum class ScanType {
    FullScan,           // Iterate all records
    IndexEquality,      // Lazy index walk for `col = ?`
    IndexRange,         // Lazy index walk for a bounded key range
    RowidLookup         // Lookup by rowid (sequence)
};

/**
 * What the virtual tables of one connection actually did. These are
 * computable outcomes for tests and benchmarks: a point lookup that reads one
 * record, a `LIMIT 1` that stops after one index entry, a range that touches
 * only its keys. Read through SQLiteEngine::scanStats() or, from any lane, the
 * SQL function flatsql_scan_stats() (JSON text).
 */
struct VTabScanStats {
    uint64_t fullScans = 0;          // xFilter calls that walked the table
    uint64_t rowidLookups = 0;       // xFilter calls answered by rowid
    uint64_t indexEqualityScans = 0; // xFilter calls answered by `col = ?`
    uint64_t indexRangeScans = 0;    // xFilter calls answered by a key range
    uint64_t indexEntriesRead = 0;   // index entries stepped (tombstoned too)
    uint64_t rowsVisited = 0;        // records positioned for SQLite to read
};

// Index info for optimization
struct VTabIndexInfo {
    std::string columnName;
    SqliteIndex* index;
};

/**
 * Virtual table structure for FlatBuffer storage.
 * Extends sqlite3_vtab with FlatBuffer-specific data.
 */
struct FlatBufferVTab : public sqlite3_vtab {
    StreamingFlatBufferStore* store = nullptr;        // FlatBuffer storage (not owned)
    const TableDef* tableDef = nullptr;               // Schema (not owned)
    std::string sourceName;                 // Source name for _source column
    std::string fileId;                     // File identifier for routing
    FieldExtractor extractor;               // Extracts values from FlatBuffers
    FastFieldExtractor fastExtractor = nullptr;       // Optional fast path that writes directly to SQLite
    std::unordered_map<std::string, SqliteIndex*> indexes;  // Column name -> SQLite index (not owned)
    std::unordered_set<uint64_t>* tombstones = nullptr; // Deleted sequences (not owned, may be nullptr)

    // Column index for virtual _source column (-1 if not enabled)
    int sourceColumnIndex = -1;

    // Source-specific record infos (for multi-source routing)
    // When set, uses these instead of store->getRecordInfoVector(fileId)
    const std::vector<StreamingFlatBufferStore::FileRecordInfo>* sourceRecordInfos = nullptr;

    // The registration this table was created from (not owned). The record
    // key is read through it (VTabCreateInfo::recordKey).
    const VTabCreateInfo* createInfo = nullptr;

    // True when a column of this table is (encrypted): its records are read
    // from a decrypted copy while the database has a key.
    bool hasEncryptedColumns = false;

    // Per-connection scan counters (not owned, may be nullptr)
    VTabScanStats* stats = nullptr;
};

/**
 * Lightweight record reference (no data copy)
 */
struct RecordRef {
    uint64_t offset;
    uint64_t sequence;
    const uint8_t* data;
    uint32_t length;
};

/**
 * Cursor structure for iterating over FlatBuffer records.
 * Extends sqlite3_vtab_cursor with scan state.
 */
struct FlatBufferCursor : public sqlite3_vtab_cursor {
    FlatBufferVTab* vtab;

    // Current record state
    uint64_t currentOffset;
    uint64_t currentSequence;       // rowid
    const uint8_t* currentData;
    uint32_t currentLength;
    bool atEof;

    // Scan configuration
    ScanType scanType;

    // For index-based scans: one index entry per xNext, never materialized
    SqliteIndexScan indexScan;

    // For full scan iteration (O(1) per record)
    size_t scanFileIndex;
    size_t scanFileCount;
    const std::vector<StreamingFlatBufferStore::FileRecordInfo>* scanRecordInfos;
    const uint8_t* scanDataBuffer;  // Cached data buffer pointer for inline access

    // Counted locally on the hot path, folded into vtab->stats on reset/close
    uint64_t rowsVisited;
    uint64_t indexEntriesRead;

    // Column value cache - avoids re-extracting values for same row
    std::vector<Value> columnCache;
    bool cacheValid;

    // The current record with its (encrypted) columns decrypted, when the
    // table has any and the database has a key (valid with cacheValid)
    std::vector<uint8_t> plainRecord;

    // Cached column count to avoid size() calls
    int numRealColumns;

    // Cached fast extractor to avoid vtab pointer chase
    FastFieldExtractor cachedFastExtractor;

    // Cached tombstone flag - true if there are tombstones to check
    bool hasTombstones;
};

/**
 * SQLite virtual table module definition.
 * Contains all the callback functions for the virtual table.
 */
class FlatBufferVTabModule {
public:
    // Get the SQLite module structure
    static sqlite3_module* getModule();

    // Module callbacks
    static int xCreate(sqlite3* db, void* pAux, int argc, const char* const* argv,
                       sqlite3_vtab** ppVTab, char** pzErr);
    static int xConnect(sqlite3* db, void* pAux, int argc, const char* const* argv,
                        sqlite3_vtab** ppVTab, char** pzErr);
    static int xDisconnect(sqlite3_vtab* pVTab);
    static int xDestroy(sqlite3_vtab* pVTab);
    static int xBestIndex(sqlite3_vtab* pVTab, sqlite3_index_info* pIdxInfo);
    static int xOpen(sqlite3_vtab* pVTab, sqlite3_vtab_cursor** ppCursor);
    static int xClose(sqlite3_vtab_cursor* pCursor);
    static int xFilter(sqlite3_vtab_cursor* pCursor, int idxNum, const char* idxStr,
                       int argc, sqlite3_value** argv);
    static int xNext(sqlite3_vtab_cursor* pCursor);
    static int xEof(sqlite3_vtab_cursor* pCursor);
    static int xColumn(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int N);
    static int xRowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid);

private:
    static sqlite3_module module_;

    // Helper to build column declaration for sqlite3_declare_vtab
    static std::string buildColumnDecl(const ColumnDef& col);
    static std::string valueTypeToSQLite(ValueType type);

    // Helper to set SQLite result from Value
    static void setResultFromValue(sqlite3_context* ctx, const Value& value);

    // Helper to get Value from sqlite3_value
    static Value valueFromSqlite(sqlite3_value* val);
};

/**
 * Auxiliary data passed to xCreate/xConnect.
 * Contains all information needed to set up a virtual table.
 */
struct VTabCreateInfo {
    StreamingFlatBufferStore* store = nullptr;
    const TableDef* tableDef = nullptr;
    std::string sourceName;
    std::string fileId;
    FieldExtractor extractor;
    FastFieldExtractor fastExtractor = nullptr;
    std::unordered_map<std::string, SqliteIndex*> indexes;
    std::unordered_set<uint64_t>* tombstones = nullptr;
    // Source-specific record infos (for multi-source routing)
    // When set, uses these instead of store->getRecordInfoVector(fileId)
    const std::vector<StreamingFlatBufferStore::FileRecordInfo>* sourceRecordInfos = nullptr;
    // The database's record key (not owned, may be nullptr)
    const RecordKey* recordKey = nullptr;
    // Per-connection scan counters (not owned)
    VTabScanStats* stats = nullptr;
};

}  // namespace flatsql

#endif  // FLATSQL_SQLITE_VTAB_H
