#ifndef FLATSQL_DATABASE_H
#define FLATSQL_DATABASE_H

#include "flatsql/types.h"
#include "flatsql/storage.h"
#include "flatsql/sqlite_index.h"
#include "flatsql/schema_parser.h"
#include "flatsql/sqlite_engine.h"
#include "flatbuffers/encryption.h"
#include <atomic>
#include <list>
#include <set>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace flatsql {

/**
 * Table store: manages records and indexes for a single table.
 * Works with streaming ingest - indexes are built as records arrive.
 * Uses SQLite's optimized B-tree for indexes (via SqliteIndex).
 */
class TableStore {
public:
    TableStore(const TableDef& tableDef, StreamingFlatBufferStore& storage, sqlite3* indexDb);

    // Called during streaming ingest to index a record
    // This is the streaming index builder - called for each FlatBuffer as it arrives
    void onIngest(const uint8_t* data, size_t length, uint64_t sequence, uint64_t offset);

    // Find by indexed column
    std::vector<StoredRecord> findByIndex(const std::string& column, const Value& value);

    // Find by range on indexed column
    std::vector<StoredRecord> findByRange(const std::string& column,
                                          const Value& minValue, const Value& maxValue);

    // Full table scan (by file_id)
    std::vector<StoredRecord> scanAll();

    // Get table definition
    const TableDef& getTableDef() const { return tableDef_; }

    // Get file identifier for this table
    const std::string& getFileId() const { return fileId_; }

    // Set file identifier (4 bytes, e.g., "USER")
    void setFileId(const std::string& fileId) { fileId_ = fileId; }

    // Get record count
    uint64_t getRecordCount() const { return recordCount_; }

    // Get index names
    std::vector<std::string> getIndexNames() const;

    // Field extractor function type - extracts field values from raw FlatBuffer
    using FieldExtractor = std::function<Value(const uint8_t* data, size_t length, const std::string& fieldName)>;
    using FastFieldExtractor = flatsql::FastFieldExtractor;
    using BatchExtractor = flatsql::BatchExtractor;

    // Set field extractor (required for indexing and queries)
    void setFieldExtractor(FieldExtractor extractor) { fieldExtractor_ = extractor; }

    // Set fast field extractor (optional, for bypassing Value construction)
    void setFastFieldExtractor(FastFieldExtractor extractor) { fastFieldExtractor_ = extractor; }

    // Set batch extractor (optional, for efficient batch extraction)
    void setBatchExtractor(BatchExtractor extractor) { batchExtractor_ = extractor; }

    // Get field extractor
    FieldExtractor getFieldExtractor() const { return fieldExtractor_; }

    // Get fast field extractor
    FastFieldExtractor getFastFieldExtractor() const { return fastFieldExtractor_; }

    // Get batch extractor
    BatchExtractor getBatchExtractor() const { return batchExtractor_; }

    // Get index for a column (returns nullptr if not indexed)
    SqliteIndex* getIndex(const std::string& columnName) {
        auto it = indexes_.find(columnName);
        return it != indexes_.end() ? it->second.get() : nullptr;
    }

    // Get record infos for this specific table (for source-specific iteration)
    const std::vector<StreamingFlatBufferStore::FileRecordInfo>& getRecordInfos() const {
        return recordInfos_;
    }

private:
    TableDef tableDef_;
    std::string fileId_;  // 4-byte file identifier for routing
    StreamingFlatBufferStore& storage_;
    sqlite3* indexDb_;    // SQLite database for indexes (not owned)
    std::map<std::string, std::unique_ptr<SqliteIndex>> indexes_;
    uint64_t recordCount_ = 0;
    FieldExtractor fieldExtractor_;
    FastFieldExtractor fastFieldExtractor_ = nullptr;
    BatchExtractor batchExtractor_ = nullptr;

    // Per-table record tracking (for source-specific tables)
    std::vector<StreamingFlatBufferStore::FileRecordInfo> recordInfos_;

    // R-Tree spatial index support
    struct SpatialIndexDef {
        std::string latColumn;   // latitude / y column
        std::string lonColumn;   // longitude / x column
        std::string rtreeName;   // name of R-Tree virtual table
    };
    std::vector<SpatialIndexDef> spatialIndexes_;
    sqlite3_stmt* rtreeInsertStmt_ = nullptr;   // cached insert statement

    void createSpatialIndexes();
    void insertIntoRTree(const SpatialIndexDef& si, const uint8_t* data, size_t length, uint64_t sequence);
};

/**
 * FlatSQL Database: SQL interface over FlatBuffer storage.
 *
 * Uses SQLite virtual tables for mature SQL support while keeping
 * FlatBuffers as the storage/transfer format.
 *
 * Supports:
 * - Streaming ingest of raw size-prefixed FlatBuffers
 * - File identifier routing to tables
 * - Multiple sources with same schema (multi-source queries)
 * - Unified views for cross-source queries
 * - Tombstone-based deletes with compaction
 */
class FlatSQLDatabase {
public:
    struct RuntimeOptions {
        std::shared_ptr<StreamingFlatBufferStore> sharedStore;
        std::shared_ptr<std::shared_mutex> accessMutex;
        SQLiteConnectionOptions sqlite;
    };

    // Create from schema
    explicit FlatSQLDatabase(const DatabaseSchema& schema, RuntimeOptions options = {});

    // Create from schema source (IDL or JSON)
    static FlatSQLDatabase fromSchema(const std::string& source,
                                      const std::string& dbName = "default",
                                      RuntimeOptions options = {});

    // Register a file identifier -> table mapping
    // Call this before ingesting to enable routing
    void registerFileId(const std::string& fileId, const std::string& tableName);

    // Stream raw size-prefixed FlatBuffers
    // Format: [4-byte size][FlatBuffer][4-byte size][FlatBuffer]...
    // Returns number of bytes consumed (for buffer management)
    // Sets recordsIngested to number of records ingested (optional)
    size_t ingest(const uint8_t* data, size_t length, size_t* recordsIngested = nullptr);

    // Ingest a single FlatBuffer (without size prefix)
    // File identifier is read from bytes 4-7
    uint64_t ingestOne(const uint8_t* flatbuffer, size_t length);

    // Load existing stream data and rebuild indexes
    void loadAndRebuild(const uint8_t* data, size_t length);

    // Validate SQL without executing it. Never throws.
    // Initializes the SQLite engine (registering pending tables) if needed so
    // validation sees the same schema the subsequent query would.
    // paramCountOut receives the total bind-parameter count across statements.
    bool validateSQL(const std::string& sql, int* paramCountOut, std::string* errOut) noexcept;

    // Check whether a query template is registered. Never throws.
    bool hasQueryTemplate(const std::string& id) const noexcept;

    // Get the SQL text of a registered query template (nullptr if unknown). Never throws.
    const std::string* queryTemplateSQL(const std::string& id) const noexcept;

    // Check whether a named ingest source is registered. Never throws.
    bool hasSource(const std::string& name) const noexcept;

    // Execute SQL query (uses SQLite virtual tables)
    QueryResult query(const std::string& sql);

    // Execute SQL query with parameters (faster for repeated queries)
    QueryResult query(const std::string& sql, const std::vector<Value>& params);

    // Execute SQL query with a single integer parameter (most optimized for int key lookups)
    QueryResult query(const std::string& sql, int64_t param);

    struct QueryCacheStats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        size_t size = 0;
        uint64_t generation = 0;
        size_t maxEntries = 0;
        size_t maxRows = 0;
    };

    // Register a named SQL template for native cached execution.
    void registerQueryTemplate(const std::string& queryId,
                               const std::string& sql,
                               bool cacheable = true);

    // Execute a registered SQL template through the native result cache.
    QueryResult queryTemplate(const std::string& queryId,
                              const std::vector<Value>& params = {});

    // Clear cached query results without unregistering templates.
    void clearQueryResultCache();

    // Configure bounded native query-result caching.
    void configureQueryResultCache(size_t maxEntries, size_t maxRows);

    QueryCacheStats getQueryCacheStats() const;

    // ==================== Raw-stream response artifact cache ====================
    // Materialized aligned response streams ([u32le size][bytes] frames, all
    // cells BLOB) cached by (sql, params) so repeated raw-stream requests
    // skip SQL re-execution entirely. Keys embed queryCacheGeneration_, so
    // EVERY existing invalidation point (ingest, ingestOne, loadAndRebuild,
    // registerFileId, registerSource, createUnifiedViews, markDeleted,
    // clearTombstones, setEncryptionKey, DML through query(), ...) also
    // invalidates cached raw streams; the entries themselves are dropped on
    // invalidation to release memory. Only read-only statements
    // (sqlite3_stmt_readonly) are cached.

    struct RawStreamResult {
        // Aligned size-prefixed stream; shared so cache hits are zero-copy
        // and callers keep a valid buffer even across eviction.
        std::shared_ptr<const std::vector<uint8_t>> stream;
        size_t rowCount = 0;
        size_t columnCount = 0;
        bool cacheHit = false;
    };

    struct RawStreamCacheStats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        size_t entries = 0;
        size_t totalBytes = 0;
        size_t maxEntries = 0;
        size_t maxTotalBytes = 0;
    };

    // Execute a raw FlatBuffer-stream query through the response-artifact
    // cache. Returns false with *errorMessage set for user-level failures
    // (non-BLOB cell, oversize record) WITHOUT throwing — no-throw contract
    // for the no-exceptions build (SQL itself must be pre-validated by the
    // caller, matching the C API).
    bool queryRawFlatBufferStream(const std::string& sql,
                                  const std::vector<Value>& params,
                                  RawStreamResult* result,
                                  std::string* errorMessage);

    // Configure bounded raw-stream artifact caching (entry count + total
    // payload byte budget; a single stream larger than the byte budget is
    // returned uncached).
    void configureRawStreamCache(size_t maxEntries, size_t maxTotalBytes);

    RawStreamCacheStats getRawStreamCacheStats() const;

    // Execute and count without building QueryResult (for benchmarking)
    size_t queryCount(const std::string& sql, const std::vector<Value>& params = {});

    // Direct point lookup - bypasses SQLite for maximum speed
    // Returns records matching the given column value
    std::vector<StoredRecord> findByIndex(const std::string& tableName,
                                          const std::string& column,
                                          const Value& value);

    // Direct point lookup for unique keys - returns true if found
    // Most efficient for primary key lookups
    bool findOneByIndex(const std::string& tableName,
                        const std::string& column,
                        const Value& value,
                        StoredRecord& result);

    // Zero-copy point lookup - returns pointer to FlatBuffer data
    // Most efficient when you just need to read the FlatBuffer
    // Returns nullptr if not found
    const uint8_t* findRawByIndex(const std::string& tableName,
                                  const std::string& column,
                                  const Value& value,
                                  uint32_t* outLength,
                                  uint64_t* outSequence = nullptr);

    // Direct iteration over all records - bypasses SQLite completely
    // Callback receives raw FlatBuffer data for zero-copy access
    // Returns count of records iterated
    template<typename Callback>
    size_t iterateAll(const std::string& tableName, Callback&& callback) const {
        std::shared_lock lock(*accessMutex_);
        auto it = tables_.find(tableName);
        if (it == tables_.end()) {
            return 0;
        }

        const std::string& fileId = it->second->getFileId();
        size_t count = 0;
        storage_->iterateRefsByFileId(fileId, [&](const StreamingFlatBufferStore::RecordRef& ref) {
            callback(ref.data, ref.length, ref.sequence);
            count++;
            return true;
        });
        return count;
    }

    // Get storage for direct access
    const StreamingFlatBufferStore& getStorage() const { return *storage_; }

    // Preallocate storage for known large ingest targets.
    void reserveStorage(size_t bytes);

    // Set field extractor for a table (required for indexing and queries)
    void setFieldExtractor(const std::string& tableName, TableStore::FieldExtractor extractor);

    // Set fast field extractor for a table (optional, for bypassing Value construction)
    void setFastFieldExtractor(const std::string& tableName, TableStore::FastFieldExtractor extractor);

    // Set batch extractor for a table (optional, for efficient batch extraction)
    void setBatchExtractor(const std::string& tableName, TableStore::BatchExtractor extractor);

    // Get raw storage data (for export)
    std::vector<uint8_t> exportData() const {
        std::shared_lock lock(*accessMutex_);
        return storage_->exportData();
    }

    // Get schema
    const DatabaseSchema& getSchema() const { return schema_; }

    // List tables
    std::vector<std::string> listTables() const;

    // Get table definition
    const TableDef* getTableDef(const std::string& tableName) const;

    // Get statistics
    struct TableStats {
        std::string tableName;
        std::string fileId;
        uint64_t recordCount;
        std::vector<std::string> indexes;
    };
    std::vector<TableStats> getStats() const;

    // Get/reset internal ingest profile counters.
    IngestProfile getIngestProfile() {
        ingestProfileEnabled_ = false;
        return ingestProfile_;
    }
    void resetIngestProfile() {
        ingestProfile_.reset();
        ingestProfileEnabled_ = true;
    }

    // ==================== Multi-Source API ====================

    /**
     * Register a named data source for source-aware ingestion.
     *
     * Creates source-specific tables: User@siteA, Post@siteA, etc.
     * Source tables have the same schema as base tables plus a virtual _source column.
     *
     * @param sourceName  Unique identifier (e.g., "siteA", "satellite-1")
     */
    void registerSource(const std::string& sourceName);

    /**
     * Get list of registered source names.
     */
    std::vector<std::string> listSources() const;

    /**
     * Create unified views for cross-source queries.
     *
     * Creates views like "User" that UNION ALL User@siteA, User@siteB, etc.
     * Call this after registering all sources and before querying.
     */
    void createUnifiedViews();

    /**
     * Ingest data with explicit source tagging.
     *
     * Routes to source-specific tables (e.g., User@siteA).
     * Source must be registered with registerSource() first.
     *
     * @param data   Size-prefixed FlatBuffer stream
     * @param length Stream length
     * @param source Source name (must be registered)
     * @param recordsIngested Optional output for record count
     * @return Bytes consumed
     */
    size_t ingestWithSource(const uint8_t* data, size_t length,
                            const std::string& source,
                            size_t* recordsIngested = nullptr);

    /**
     * Ingest a single FlatBuffer with source tagging.
     *
     * @param flatbuffer FlatBuffer data (no size prefix)
     * @param length Data length
     * @param source Source name (must be registered)
     * @return Sequence number
     */
    uint64_t ingestOneWithSource(const uint8_t* flatbuffer, size_t length,
                                  const std::string& source);

    // Legacy multi-source API (external storage)
    void registerExternalSource(
        const std::string& sourceName,
        StreamingFlatBufferStore* store,
        const TableDef& schema,
        const std::string& fileId,
        TableStore::FieldExtractor extractor
    );

    void createUnifiedView(
        const std::string& viewName,
        const std::vector<std::string>& sourceNames
    );

    // ==================== Delete Support ====================

    /**
     * Mark a record as deleted (tombstone).
     * Record will be skipped in queries until compaction.
     *
     * @param tableName  Table name or source name
     * @param sequence   Sequence number (rowid) to delete
     */
    void markDeleted(const std::string& tableName, uint64_t sequence);

    /**
     * Get count of deleted records for a table.
     */
    size_t getDeletedCount(const std::string& tableName) const;

    /**
     * Clear tombstones after compaction.
     */
    void clearTombstones(const std::string& tableName);

    // ==================== Encryption API ====================

    /**
     * Set the encryption key for field-level FlatBuffer decryption.
     * Fields marked with (encrypted) in the schema will be transparently
     * decrypted when read through SQL queries.
     *
     * @param key     32-byte AES-256 key
     * @param keySize Must be 32
     */
    void setEncryptionKey(const uint8_t* key, size_t keySize);

    /**
     * Check if encryption is enabled.
     */
    bool isEncrypted() const { return encryptionCtx_ != nullptr; }

    /**
     * Get the encryption context (for vtab layer).
     */
    const flatbuffers::EncryptionContext* getEncryptionContext() const {
        return encryptionCtx_.get();
    }

    /**
     * Check if any table has encrypted fields.
     */
    bool hasEncryptedFields() const;

    // ==================== HMAC Authentication ====================

    /**
     * Enable HMAC verification on ingest.
     * When enabled, ingestOne() will reject buffers that fail HMAC verification.
     * Requires an encryption key to be set first.
     *
     * @param enabled  true to enable, false to disable
     */
    void setHMACVerification(bool enabled);

    /**
     * Check if HMAC verification is enabled.
     */
    bool isHMACVerificationEnabled() const { return hmacEnabled_; }

    /**
     * Compute HMAC-SHA256 for a FlatBuffer.
     * @param buffer    FlatBuffer data
     * @param length    Buffer length
     * @param outMAC    Output: 32-byte HMAC (caller must provide 32 bytes)
     * @return true on success
     */
    bool computeHMAC(const uint8_t* buffer, size_t length, uint8_t* outMAC) const;

    /**
     * Verify HMAC-SHA256 for a FlatBuffer.
     * @param buffer    FlatBuffer data
     * @param length    Buffer length
     * @param mac       32-byte HMAC to verify
     * @return true if MAC is valid
     */
    bool verifyHMAC(const uint8_t* buffer, size_t length, const uint8_t* mac) const;

private:
    // Callback for streaming ingest - routes to correct table and builds indexes
    void onIngest(std::string_view fileId, const uint8_t* data, size_t length,
                  uint64_t sequence, uint64_t offset);

    // Callback for source-aware ingest - routes to source-specific table
    void onIngestWithSource(std::string_view fileId, const uint8_t* data, size_t length,
                            uint64_t sequence, uint64_t offset, const std::string& source);

    // Initialize SQLite engine with registered tables
    void initializeSQLiteEngine();

    // Re-register a table with SQLite after extractor is set
    void updateSQLiteTable(const std::string& tableName);

    // Create a source-specific table (e.g., User@siteA)
    void createSourceTable(const std::string& baseTableName, const std::string& source);

    // Get source table name (e.g., "User" + "siteA" -> "User@siteA")
    static std::string getSourceTableName(const std::string& baseTable, const std::string& source) {
        return baseTable + "@" + source;
    }

    // Parse source from table name (e.g., "User@siteA" -> "siteA")
    static std::string parseSourceFromTableName(const std::string& tableName) {
        auto pos = tableName.find('@');
        return pos != std::string::npos ? tableName.substr(pos + 1) : "";
    }

    struct QueryTemplateDef {
        std::string sql;
        bool cacheable = true;
    };

    struct CachedQueryResult {
        QueryResult result;
        std::list<std::string>::iterator lruIt;
    };

    struct CachedRawStream {
        RawStreamResult result;
        std::list<std::string>::iterator lruIt;
    };

    void invalidateQueryResultCacheUnlocked();
    void invalidateCachesIfStatementWritesUnlocked(const std::string& sql);
    void storeCachedQueryResultUnlocked(const std::string& key, const QueryResult& result);
    void storeRawStreamResultUnlocked(const std::string& key, const RawStreamResult& result);
    std::string buildTemplateCacheKeyUnlocked(const std::string& queryId,
                                              const std::string& sql,
                                              const std::vector<Value>& params) const;

    DatabaseSchema schema_;
    std::shared_ptr<StreamingFlatBufferStore> storage_;
    std::shared_ptr<std::shared_mutex> accessMutex_;
    std::map<std::string, std::unique_ptr<TableStore>> tables_;
    std::map<std::string, std::string> fileIdToTable_;  // file_id -> table name

    // Source tracking
    std::vector<std::string> registeredSources_;        // List of registered source names
    std::map<std::string, std::string> sourceFileIdToTable_;  // "source:fileId" -> "table@source"

    // SQLite engine for query execution
    std::unique_ptr<SQLiteEngine> sqliteEngine_;
    std::atomic<bool> sqliteInitialized_{false};

    // Track which tables have been registered with SQLite
    std::set<std::string> sqliteRegisteredTables_;

    std::unordered_map<std::string, QueryTemplateDef> queryTemplates_;
    std::list<std::string> queryResultCacheLru_;
    std::unordered_map<std::string, CachedQueryResult> queryResultCache_;
    uint64_t queryCacheGeneration_ = 0;
    uint64_t queryCacheHits_ = 0;
    uint64_t queryCacheMisses_ = 0;

    size_t queryResultCacheMaxEntries_ = 1024;
    size_t queryResultCacheMaxRows_ = 1000;

    // Raw-stream response artifact cache (see public section above).
    std::list<std::string> rawStreamCacheLru_;
    std::unordered_map<std::string, CachedRawStream> rawStreamCache_;
    size_t rawStreamCacheTotalBytes_ = 0;
    uint64_t rawStreamCacheHits_ = 0;
    uint64_t rawStreamCacheMisses_ = 0;
    size_t rawStreamCacheMaxEntries_ = 64;
    size_t rawStreamCacheMaxTotalBytes_ = 256 * 1024 * 1024;

    // Encryption
    std::unique_ptr<flatbuffers::EncryptionContext> encryptionCtx_;

    // HMAC verification
    bool hmacEnabled_ = false;

    IngestProfile ingestProfile_;
    bool ingestProfileEnabled_ = false;
};

}  // namespace flatsql

#endif  // FLATSQL_DATABASE_H
