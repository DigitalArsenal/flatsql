#ifndef FLATSQL_DATABASE_H
#define FLATSQL_DATABASE_H

#include "flatsql/types.h"
#include "flatsql/storage.h"
#include "flatsql/btree.h"
#include "flatsql/schema_parser.h"
#include "flatsql/sqlite_engine.h"
#include <set>

namespace flatsql {

/**
 * Table store: manages records and indexes for a single table.
 * Works with streaming ingest - indexes are built as records arrive.
 */
class TableStore {
public:
    TableStore(const TableDef& tableDef, StreamingFlatBufferStore& storage);

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
    BTree* getIndex(const std::string& columnName) {
        auto it = indexes_.find(columnName);
        return it != indexes_.end() ? it->second.get() : nullptr;
    }

private:
    TableDef tableDef_;
    std::string fileId_;  // 4-byte file identifier for routing
    StreamingFlatBufferStore& storage_;
    std::map<std::string, std::unique_ptr<BTree>> indexes_;
    uint64_t recordCount_ = 0;
    FieldExtractor fieldExtractor_;
    FastFieldExtractor fastFieldExtractor_;
    BatchExtractor batchExtractor_ = nullptr;
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
    // Create from schema
    explicit FlatSQLDatabase(const DatabaseSchema& schema);

    // Create from schema source (IDL or JSON)
    static FlatSQLDatabase fromSchema(const std::string& source, const std::string& dbName = "default");

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

    // Execute SQL query (uses SQLite virtual tables)
    QueryResult query(const std::string& sql);

    // Execute SQL query with parameters (faster for repeated queries)
    QueryResult query(const std::string& sql, const std::vector<Value>& params);

    // Execute SQL query with a single integer parameter (most optimized for int key lookups)
    QueryResult query(const std::string& sql, int64_t param);

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
        auto it = tables_.find(tableName);
        if (it == tables_.end()) {
            return 0;
        }

        const std::string& fileId = it->second->getFileId();
        size_t count = 0;
        storage_.iterateRefsByFileId(fileId, [&](const StreamingFlatBufferStore::RecordRef& ref) {
            callback(ref.data, ref.length, ref.sequence);
            count++;
            return true;
        });
        return count;
    }

    // Get storage for direct access
    const StreamingFlatBufferStore& getStorage() const { return storage_; }

    // Set field extractor for a table (required for indexing and queries)
    void setFieldExtractor(const std::string& tableName, TableStore::FieldExtractor extractor);

    // Set fast field extractor for a table (optional, for bypassing Value construction)
    void setFastFieldExtractor(const std::string& tableName, TableStore::FastFieldExtractor extractor);

    // Set batch extractor for a table (optional, for efficient batch extraction)
    void setBatchExtractor(const std::string& tableName, TableStore::BatchExtractor extractor);

    // Get raw storage data (for export)
    std::vector<uint8_t> exportData() const { return storage_.exportData(); }

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

    DatabaseSchema schema_;
    StreamingFlatBufferStore storage_;
    std::map<std::string, std::unique_ptr<TableStore>> tables_;
    std::map<std::string, std::string> fileIdToTable_;  // file_id -> table name

    // Source tracking
    std::vector<std::string> registeredSources_;        // List of registered source names
    std::map<std::string, std::string> sourceFileIdToTable_;  // "source:fileId" -> "table@source"

    // SQLite engine for query execution
    std::unique_ptr<SQLiteEngine> sqliteEngine_;
    bool sqliteInitialized_ = false;

    // Track which tables have been registered with SQLite
    std::set<std::string> sqliteRegisteredTables_;
};

}  // namespace flatsql

#endif  // FLATSQL_DATABASE_H
