#ifndef FLATSQL_SQLITE_ENGINE_H
#define FLATSQL_SQLITE_ENGINE_H

#include "flatsql/types.h"
#include "flatsql/storage.h"
#include "flatsql/sqlite_index.h"
#include "flatsql/sqlite_vtab.h"
#include <sqlite3.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace flatsql {

struct SQLiteConnectionOptions {
    std::string path = ":memory:";
    bool enableWal = false;
    int busyTimeoutMs = 250;
    int maxBusyRetries = 8;
    int busyBackoffMs = 1;
};

/**
 * Source registration for multi-source queries.
 * Holds all data for a single FlatBuffer data source.
 */
struct SourceInfo {
    std::string name;
    StreamingFlatBufferStore* store = nullptr;    // Not owned
    const TableDef* tableDef = nullptr;           // Not owned
    std::string fileId;
    FieldExtractor extractor;
    FastFieldExtractor fastExtractor = nullptr;
    BatchExtractor batchExtractor = nullptr;      // Optional batch extractor
    std::unordered_map<std::string, SqliteIndex*> indexes;  // Not owned
    std::unordered_set<uint64_t> tombstones;      // Owned - deleted sequences
    VTabCreateInfo vtabInfo;                      // Info passed to xCreate
    // Source-specific record infos pointer (for multi-source routing)
    const std::vector<StreamingFlatBufferStore::FileRecordInfo>* sourceRecordInfos = nullptr;
    // Encryption context (not owned)
    const flatbuffers::EncryptionContext* encryptionCtx = nullptr;
};

/**
 * High-level SQLite wrapper for FlatBuffer queries.
 *
 * Manages an in-memory SQLite database with virtual tables that
 * expose FlatBuffer storage. Supports:
 * - Multiple sources with same or different schemas
 * - Unified views for cross-source queries
 * - Tombstone-based deletes with compaction
 */
class SQLiteEngine {
public:
    explicit SQLiteEngine(SQLiteConnectionOptions options = {});
    ~SQLiteEngine();

    // Non-copyable
    SQLiteEngine(const SQLiteEngine&) = delete;
    SQLiteEngine& operator=(const SQLiteEngine&) = delete;

    // Move semantics
    SQLiteEngine(SQLiteEngine&& other) noexcept;
    SQLiteEngine& operator=(SQLiteEngine&& other) noexcept;

    /**
     * Register a data source with automatic _source column tagging.
     *
     * @param sourceName  Unique identifier for this source (becomes table name)
     * @param store       Pointer to this source's FlatBuffer storage
     * @param tableDef    Schema definition
     * @param fileId      File identifier for routing FlatBuffers
     * @param extractor   Callback to extract field values from FlatBuffers
     * @param indexes     Map of column name -> B-tree index
     * @param fastExtractor Optional fast field extractor
     * @param batchExtractor Optional batch extractor
     * @param sourceRecordInfos Optional source-specific record infos (for multi-source routing)
     */
    void registerSource(
        const std::string& sourceName,
        StreamingFlatBufferStore* store,
        const TableDef* tableDef,
        const std::string& fileId,
        FieldExtractor extractor,
        const std::unordered_map<std::string, SqliteIndex*>& indexes = {},
        FastFieldExtractor fastExtractor = nullptr,
        BatchExtractor batchExtractor = nullptr,
        const std::vector<StreamingFlatBufferStore::FileRecordInfo>* sourceRecordInfos = nullptr
    );

    /**
     * Create a unified view that combines multiple sources with the same schema.
     * Generates a UNION ALL view with _source column.
     *
     * @param viewName     Name for the unified view
     * @param sourceNames  List of registered source names to include
     */
    void createUnifiedView(
        const std::string& viewName,
        const std::vector<std::string>& sourceNames
    );

    /**
     * Validate SQL without executing it. Never throws.
     *
     * Prepares every statement in the (possibly multi-statement) SQL with
     * sqlite3_prepare_v2 and finalizes it immediately. No statement is executed.
     *
     * @param sql            SQL text (may contain multiple statements)
     * @param paramCountOut  Optional out: total bind-parameter count across all statements
     * @param errOut         Optional out: error message ("SQL error: <sqlite message>")
     * @return true if all statements compiled successfully
     */
    bool validateSQL(const std::string& sql, int* paramCountOut, std::string* errOut) noexcept;

    // ==================== Sandboxed public query (gateway loop G.5) ====================
    // A single-statement, read-only, resource-capped execution path for
    // UNTRUSTED SQL (the public /api/v1/query surface). Never throws — every
    // rejection lands in *errOut with a stable "sandbox: <code>: ..." prefix
    // so hosts can map violations to typed errors. Defense layers:
    //   1. sqlite3_set_authorizer during prepare: only SQLITE_SELECT /
    //      SQLITE_FUNCTION / SQLITE_RECURSIVE / SQLITE_READ-on-allowlisted
    //      tables are permitted; PRAGMA, ATTACH/DETACH, every DDL/DML verb
    //      (incl. temp objects), TRANSACTION/SAVEPOINT and reads outside
    //      `allowedTables` are denied (prepare fails).
    //   2. single-statement: any non-whitespace prepare tail is rejected.
    //   3. sqlite3_stmt_readonly must be true and the statement must return
    //      result columns (SELECT-shaped).
    //   4. sqlite3_progress_handler timeout (steady-clock deadline) —
    //      runaway statements abort with SQLITE_INTERRUPT.
    //   5. row / byte caps enforced inside the step loop (reject, not
    //      truncate).
    // The sandbox never touches the statement cache or the query/raw-stream
    // caches, and never invalidates anything (it is structurally read-only).

    enum class SandboxMode {
        RecordStream,  // all cells BLOB -> aligned [u32le size][bytes] frames
        JsonRows       // bare JSON array of {"<column>": value} objects
    };

    struct SandboxLimits {
        uint64_t maxRows = 0;    // 0 = unlimited
        uint64_t maxBytes = 0;   // 0 = unlimited (output payload bytes)
        uint32_t timeoutMs = 0;  // 0 = no deadline
    };

    struct SandboxOutput {
        std::vector<uint8_t> payload;  // stream frames or UTF-8 JSON array
        size_t rowCount = 0;
        size_t columnCount = 0;
    };

    /**
     * Execute untrusted SQL under the sandbox contract above. Never throws.
     *
     * @param allowedTables table/view names SQLITE_READ may touch
     * @return true on success; false with *errOut set ("sandbox: <code>: ..."
     *         for sandbox rejections, "SQL error: ..." for plain SQL errors)
     */
    bool executeSandboxed(const std::string& sql,
                          const std::vector<Value>& params,
                          const std::unordered_set<std::string>& allowedTables,
                          SandboxMode mode,
                          const SandboxLimits& limits,
                          SandboxOutput* out,
                          std::string* errOut) noexcept;

    /**
     * Execute a SQL query and return results.
     *
     * @param sql  SQL query string
     * @return QueryResult with columns and rows
     * @throws std::runtime_error on SQL error
     */
    QueryResult execute(const std::string& sql);

    /**
     * Execute a parameterized SQL query with bound values.
     * Much faster for repeated queries with different parameters.
     *
     * @param sql     SQL query string with ? placeholders
     * @param params  Values to bind to placeholders (in order)
     * @return QueryResult with columns and rows
     * @throws std::runtime_error on SQL error
     */
    QueryResult execute(const std::string& sql, const std::vector<Value>& params);

    /**
     * Mark a record as deleted in a source.
     * The record will be skipped in future queries.
     *
     * @param sourceName  Source to delete from
     * @param sequence    Sequence number (rowid) of record to delete
     */
    void markDeleted(const std::string& sourceName, uint64_t sequence);

    /**
     * Get count of deleted records for a source.
     */
    size_t getDeletedCount(const std::string& sourceName) const;

    /**
     * Clear tombstones for a source (call after compaction).
     */
    void clearTombstones(const std::string& sourceName);

    /**
     * Get list of registered source names.
     */
    std::vector<std::string> listSources() const;

    /**
     * Check if a source is registered.
     */
    bool hasSource(const std::string& sourceName) const;

    /**
     * Get the underlying SQLite database handle.
     * Use with caution - primarily for advanced use cases.
     */
    sqlite3* getDb() const { return db_; }

    /**
     * Execute a query and just step through results without building QueryResult.
     * For performance testing to isolate virtual table overhead.
     * Returns the number of rows.
     */
    size_t executeAndCount(const std::string& sql, const std::vector<Value>& params = {});

    /**
     * True when the prepared statement for sql cannot modify the database
     * (sqlite3_stmt_readonly). Prepares (and caches) the statement on first
     * use; throws std::runtime_error for unparseable SQL like execute().
     */
    bool statementIsReadOnly(const std::string& sql) const;

    /**
     * Get last error message.
     */
    std::string getLastError() const;

    /**
     * Get source info for direct access.
     */
    SourceInfo* getSource(const std::string& sourceName);
    const SourceInfo* getSource(const std::string& sourceName) const;

    /**
     * Optimized query that returns raw FlatBuffer data for point lookups.
     * Bypasses Value construction entirely.
     * Returns true if query was intercepted and data pointers are set.
     */
    bool tryFastPathMinimal(const std::string& sql, const std::vector<Value>& params,
                            const uint8_t** outData, uint32_t* outLen,
                            uint64_t* outSequence = nullptr);

private:
    struct ParsedQuery {
        std::string tableName;
        std::string columnName;
        bool isPointQuery = false;
        bool isFullScan = false;
    };

    // Try to intercept simple queries and use direct API instead of VTable
    // Returns true if query was intercepted and result is populated
    bool tryFastPath(const std::string& sql, const std::vector<Value>& params, QueryResult& result);

    // Fast path for executeAndCount - returns true if intercepted
    bool tryFastPathCount(const std::string& sql, const std::vector<Value>& params, size_t& count);

    // Helper to find source with case-insensitive matching
    SourceInfo* findSourceCaseInsensitive(const std::string& lowerTableName);

    // Helper to get cached column names for a source
    const std::vector<std::string>& getCachedColumnNames(const SourceInfo* source);

    sqlite3* db_;
    SQLiteConnectionOptions options_;
    std::map<std::string, std::unique_ptr<SourceInfo>> sources_;

    // Statement cache for frequently executed queries
    mutable std::unordered_map<std::string, sqlite3_stmt*> stmtCache_;
    static constexpr size_t MAX_STMT_CACHE_SIZE = 100;

    // Get or create a prepared statement (cached)
    sqlite3_stmt* getOrPrepareStmt(const std::string& sql) const;

    // Bind a Value to a prepared statement parameter
    void bindValue(sqlite3_stmt* stmt, int idx, const Value& value) const;

    // Clear statement cache
    void clearStmtCache();

    // Clear parsed fast-path/source caches when source/schema state changes
    void clearFastPathCaches();

    mutable std::unordered_map<std::string, SourceInfo*> sourceNameCache_;
    mutable std::unordered_map<std::string, ParsedQuery> parsedQueryCache_;
    mutable std::unordered_map<std::string, std::vector<std::string>> columnNamesCache_;

    // Helper to build column list for CREATE VIEW
    std::string buildColumnList(const TableDef* tableDef) const;
};

}  // namespace flatsql

#endif  // FLATSQL_SQLITE_ENGINE_H
