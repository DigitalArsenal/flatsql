#ifndef FLATSQL_SQLITE_ENGINE_H
#define FLATSQL_SQLITE_ENGINE_H

#include "flatsql/types.h"
#include "flatsql/storage.h"
#include "flatsql/sqlite_index.h"
#include "flatsql/sqlite_vtab.h"
#include <sqlite3.h>
#include <memory>
#include <unordered_set>

namespace flatsql {

/**
 * Source registration for multi-source queries.
 * Holds all data for a single FlatBuffer data source.
 */
struct SourceInfo {
    std::string name;
    StreamingFlatBufferStore* store;              // Not owned
    const TableDef* tableDef;                     // Not owned
    std::string fileId;
    FieldExtractor extractor;
    FastFieldExtractor fastExtractor;
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
    SQLiteEngine();
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
    // Try to intercept simple queries and use direct API instead of VTable
    // Returns true if query was intercepted and result is populated
    bool tryFastPath(const std::string& sql, const std::vector<Value>& params, QueryResult& result);

    // Fast path for executeAndCount - returns true if intercepted
    bool tryFastPathCount(const std::string& sql, const std::vector<Value>& params, size_t& count);

    // Helper to find source with case-insensitive matching
    SourceInfo* findSourceCaseInsensitive(const std::string& lowerTableName);

    sqlite3* db_;
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

    // Helper to build column list for CREATE VIEW
    std::string buildColumnList(const TableDef* tableDef) const;
};

}  // namespace flatsql

#endif  // FLATSQL_SQLITE_ENGINE_H
