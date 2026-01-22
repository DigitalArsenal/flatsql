#ifndef FLATSQL_DATABASE_H
#define FLATSQL_DATABASE_H

#include "flatsql/types.h"
#include "flatsql/storage.h"
#include "flatsql/btree.h"
#include "flatsql/schema_parser.h"
#include "flatsql/sql_parser.h"

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

    // Set field extractor (required for indexing and queries)
    void setFieldExtractor(FieldExtractor extractor) { fieldExtractor_ = extractor; }

    // Get field extractor
    FieldExtractor getFieldExtractor() const { return fieldExtractor_; }

private:
    TableDef tableDef_;
    std::string fileId_;  // 4-byte file identifier for routing
    StreamingFlatBufferStore& storage_;
    std::map<std::string, std::unique_ptr<BTree>> indexes_;
    uint64_t recordCount_ = 0;
    FieldExtractor fieldExtractor_;
};

/**
 * FlatSQL Database: SQL interface over FlatBuffer storage.
 *
 * Supports streaming ingest of raw size-prefixed FlatBuffers.
 * File identifiers route records to the correct table for indexing.
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

    // Execute SQL query
    QueryResult query(const std::string& sql);

    // Set field extractor for a table (required for indexing)
    void setFieldExtractor(const std::string& tableName, TableStore::FieldExtractor extractor);

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

private:
    // Callback for streaming ingest - routes to correct table and builds indexes
    void onIngest(std::string_view fileId, const uint8_t* data, size_t length,
                  uint64_t sequence, uint64_t offset);

    QueryResult executeSelect(const ParsedSQL& parsed);
    bool evaluateCondition(const Value& fieldValue, const WhereCondition& cond);

    DatabaseSchema schema_;
    StreamingFlatBufferStore storage_;
    std::map<std::string, std::unique_ptr<TableStore>> tables_;
    std::map<std::string, std::string> fileIdToTable_;  // file_id -> table name
};

}  // namespace flatsql

#endif  // FLATSQL_DATABASE_H
