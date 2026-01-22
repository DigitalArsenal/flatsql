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
 */
class TableStore {
public:
    TableStore(const TableDef& tableDef, StackedFlatBufferStore& storage);

    // Insert a record (raw FlatBuffer data)
    uint64_t insert(const std::vector<uint8_t>& flatbufferData);

    // Find by indexed column
    std::vector<StoredRecord> findByIndex(const std::string& column, const Value& value);

    // Find by range on indexed column
    std::vector<StoredRecord> findByRange(const std::string& column,
                                          const Value& minValue, const Value& maxValue);

    // Full table scan
    std::vector<StoredRecord> scanAll();

    // Get table definition
    const TableDef& getTableDef() const { return tableDef_; }

    // Get record count
    uint64_t getRecordCount() const { return recordCount_; }

    // Get index names
    std::vector<std::string> getIndexNames() const;

    // Field extractor function type
    using FieldExtractor = std::function<Value(const std::vector<uint8_t>& data, const std::string& fieldName)>;

    // Set field extractor (for indexing)
    void setFieldExtractor(FieldExtractor extractor) { fieldExtractor_ = extractor; }

private:
    void updateIndexes(const std::vector<uint8_t>& data, uint64_t offset, uint64_t sequence);

    TableDef tableDef_;
    StackedFlatBufferStore& storage_;
    std::map<std::string, std::unique_ptr<BTree>> indexes_;
    uint64_t recordCount_ = 0;
    uint64_t nextRowId_ = 1;
    FieldExtractor fieldExtractor_;
};

/**
 * FlatSQL Database: SQL interface over FlatBuffer storage.
 */
class FlatSQLDatabase {
public:
    // Create from schema
    explicit FlatSQLDatabase(const DatabaseSchema& schema);

    // Create from schema source (IDL or JSON)
    static FlatSQLDatabase fromSchema(const std::string& source, const std::string& dbName = "default");

    // Execute SQL query
    QueryResult query(const std::string& sql);

    // Insert raw FlatBuffer
    uint64_t insertRaw(const std::string& tableName, const std::vector<uint8_t>& flatbufferData);

    // Stream multiple FlatBuffers
    std::vector<uint64_t> stream(const std::string& tableName,
                                  const std::vector<std::vector<uint8_t>>& flatbuffers);

    // Set field extractor for a table
    void setFieldExtractor(const std::string& tableName, TableStore::FieldExtractor extractor);

    // Get raw storage data (for export)
    std::vector<uint8_t> exportData() const { return storage_.getDataCopy(); }

    // Get schema
    const DatabaseSchema& getSchema() const { return schema_; }

    // List tables
    std::vector<std::string> listTables() const;

    // Get table definition
    const TableDef* getTableDef(const std::string& tableName) const;

    // Get statistics
    struct TableStats {
        std::string tableName;
        uint64_t recordCount;
        std::vector<std::string> indexes;
    };
    std::vector<TableStats> getStats() const;

private:
    QueryResult executeSelect(const ParsedSQL& parsed);
    bool evaluateCondition(const Value& fieldValue, const WhereCondition& cond);

    DatabaseSchema schema_;
    StackedFlatBufferStore storage_;
    std::map<std::string, std::unique_ptr<TableStore>> tables_;
};

}  // namespace flatsql

#endif  // FLATSQL_DATABASE_H
