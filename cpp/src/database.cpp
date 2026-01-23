#include "flatsql/database.h"
#include <algorithm>
#include <stdexcept>

namespace flatsql {

// ==================== TableStore ====================

TableStore::TableStore(const TableDef& tableDef, StreamingFlatBufferStore& storage)
    : tableDef_(tableDef), storage_(storage) {

    // Create indexes for indexed columns
    for (const auto& col : tableDef_.columns) {
        if (col.indexed || col.primaryKey) {
            indexes_[col.name] = std::make_unique<BTree>(col.type);
        }
    }
}

void TableStore::onIngest(const uint8_t* data, size_t length, uint64_t sequence, uint64_t offset) {
    // This is the streaming index builder
    // Called for each FlatBuffer as it arrives

    recordCount_++;

    if (!fieldExtractor_) {
        return;  // No extractor, can't index
    }

    // Extract and index each indexed column
    for (auto& [colName, btree] : indexes_) {
        Value key = fieldExtractor_(data, length, colName);
        btree->insert(key, offset, static_cast<uint32_t>(length), sequence);
    }
}

std::vector<StoredRecord> TableStore::findByIndex(const std::string& column, const Value& value) {
    std::vector<StoredRecord> results;

    auto it = indexes_.find(column);
    if (it == indexes_.end()) {
        // No index - fall back to scan
        auto all = scanAll();
        for (auto& record : all) {
            if (fieldExtractor_) {
                Value fieldValue = fieldExtractor_(record.data.data(), record.data.size(), column);
                if (compareValues(fieldValue, value) == 0) {
                    results.push_back(std::move(record));
                }
            }
        }
        return results;
    }

    // Try fast path for single result (common for primary key lookups)
    IndexEntry entry;
    if (it->second->searchFirst(value, entry)) {
        // Minimal record - just offset and sequence, no data copy
        StoredRecord record;
        record.offset = entry.dataOffset;
        record.header.sequence = entry.sequence;
        record.header.dataLength = entry.dataLength;
        // Data is left empty - caller can use offset to read if needed
        results.push_back(std::move(record));
    }

    return results;
}

std::vector<StoredRecord> TableStore::findByRange(const std::string& column,
                                                   const Value& minValue, const Value& maxValue) {
    std::vector<StoredRecord> results;

    auto it = indexes_.find(column);
    if (it == indexes_.end()) {
        // No index - fall back to scan
        auto all = scanAll();
        for (auto& record : all) {
            if (fieldExtractor_) {
                Value fieldValue = fieldExtractor_(record.data.data(), record.data.size(), column);
                if (compareValues(fieldValue, minValue) >= 0 &&
                    compareValues(fieldValue, maxValue) <= 0) {
                    results.push_back(std::move(record));
                }
            }
        }
        return results;
    }

    auto entries = it->second->range(minValue, maxValue);
    for (const auto& entry : entries) {
        results.push_back(storage_.readRecordAtOffset(entry.dataOffset));
    }

    return results;
}

std::vector<StoredRecord> TableStore::scanAll() {
    std::vector<StoredRecord> results;

    storage_.iterateByFileId(fileId_, [&](const StoredRecord& record) {
        results.push_back(record);
        return true;
    });

    return results;
}

std::vector<std::string> TableStore::getIndexNames() const {
    std::vector<std::string> names;
    for (const auto& [name, _] : indexes_) {
        names.push_back(name);
    }
    return names;
}

// ==================== FlatSQLDatabase ====================

FlatSQLDatabase::FlatSQLDatabase(const DatabaseSchema& schema)
    : schema_(schema) {

    // Initialize table stores
    for (const auto& tableDef : schema_.tables) {
        tables_[tableDef.name] = std::make_unique<TableStore>(tableDef, storage_);
    }

    // Initialize SQLite engine
    sqliteEngine_ = std::make_unique<SQLiteEngine>();
}

FlatSQLDatabase FlatSQLDatabase::fromSchema(const std::string& source, const std::string& dbName) {
    DatabaseSchema schema = SchemaParser::parse(source, dbName);
    return FlatSQLDatabase(schema);
}

void FlatSQLDatabase::registerFileId(const std::string& fileId, const std::string& tableName) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    fileIdToTable_[fileId] = tableName;
    it->second->setFileId(fileId);
}

void FlatSQLDatabase::onIngest(std::string_view fileId, const uint8_t* data, size_t length,
                                uint64_t sequence, uint64_t offset) {
    // Route to the correct table based on file identifier
    std::string fileIdStr(fileId);
    auto mapIt = fileIdToTable_.find(fileIdStr);
    if (mapIt == fileIdToTable_.end()) {
        // Unknown file identifier - skip (or could throw)
        return;
    }

    auto tableIt = tables_.find(mapIt->second);
    if (tableIt != tables_.end()) {
        tableIt->second->onIngest(data, length, sequence, offset);
    }
}

size_t FlatSQLDatabase::ingest(const uint8_t* data, size_t length, size_t* recordsIngested) {
    return storage_.ingest(data, length,
        [this](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngest(fileId, data, len, seq, offset);
        }, recordsIngested);
}

uint64_t FlatSQLDatabase::ingestOne(const uint8_t* flatbuffer, size_t length) {
    return storage_.ingestFlatBuffer(flatbuffer, length,
        [this](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngest(fileId, data, len, seq, offset);
        });
}

void FlatSQLDatabase::loadAndRebuild(const uint8_t* data, size_t length) {
    storage_.loadAndRebuild(data, length,
        [this](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngest(fileId, data, len, seq, offset);
        });
}

void FlatSQLDatabase::initializeSQLiteEngine() {
    if (sqliteInitialized_) return;

    // Register all tables that have file IDs registered
    // Tables without extractors will return NULL for field values
    for (const auto& [tableName, tableStore] : tables_) {
        if (!tableStore->getFileId().empty()) {
            updateSQLiteTable(tableName);
        }
    }

    sqliteInitialized_ = true;
}

void FlatSQLDatabase::updateSQLiteTable(const std::string& tableName) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        return;
    }

    TableStore* tableStore = it->second.get();

    // Skip if already registered
    if (sqliteRegisteredTables_.count(tableName)) {
        return;
    }

    // Build index map (BTree* pointers)
    std::unordered_map<std::string, BTree*> indexes;
    for (const auto& col : tableStore->getTableDef().columns) {
        if (col.indexed || col.primaryKey) {
            BTree* btree = tableStore->getIndex(col.name);
            if (btree) {
                indexes[col.name] = btree;
            }
        }
    }

    // Register with SQLite engine
    sqliteEngine_->registerSource(
        tableName,
        &storage_,
        &tableStore->getTableDef(),
        tableStore->getFileId(),
        tableStore->getFieldExtractor(),
        indexes,
        tableStore->getFastFieldExtractor(),
        tableStore->getBatchExtractor()
    );

    sqliteRegisteredTables_.insert(tableName);
}

QueryResult FlatSQLDatabase::query(const std::string& sql) {
    // Ensure SQLite engine is initialized
    initializeSQLiteEngine();

    // Execute via SQLite
    return sqliteEngine_->execute(sql);
}

QueryResult FlatSQLDatabase::query(const std::string& sql, const std::vector<Value>& params) {
    // Ensure SQLite engine is initialized
    initializeSQLiteEngine();

    // Execute via SQLite with parameters
    return sqliteEngine_->execute(sql, params);
}

QueryResult FlatSQLDatabase::query(const std::string& sql, int64_t param) {
    // Ensure SQLite engine is initialized
    initializeSQLiteEngine();

    // Use thread-local reusable vector for single-param queries
    static thread_local std::vector<Value> singleParam(1);
    singleParam[0] = param;

    // Execute via SQLite with single parameter
    return sqliteEngine_->execute(sql, singleParam);
}

size_t FlatSQLDatabase::queryCount(const std::string& sql, const std::vector<Value>& params) {
    // Ensure SQLite engine is initialized
    initializeSQLiteEngine();

    // Execute via SQLite without building QueryResult
    return sqliteEngine_->executeAndCount(sql, params);
}

std::vector<StoredRecord> FlatSQLDatabase::findByIndex(const std::string& tableName,
                                                        const std::string& column,
                                                        const Value& value) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        return {};
    }
    return it->second->findByIndex(column, value);
}

bool FlatSQLDatabase::findOneByIndex(const std::string& tableName,
                                      const std::string& column,
                                      const Value& value,
                                      StoredRecord& result) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        return false;
    }

    BTree* btree = it->second->getIndex(column);
    if (!btree) {
        return false;
    }

    IndexEntry entry;
    if (btree->searchFirst(value, entry)) {
        // Minimal record info - avoid data copy
        result.offset = entry.dataOffset;
        result.header.dataLength = entry.dataLength;
        result.header.sequence = entry.sequence;
        // Clear data vector without deallocating (reuse memory)
        result.data.clear();
        return true;
    }

    return false;
}

const uint8_t* FlatSQLDatabase::findRawByIndex(const std::string& tableName,
                                                const std::string& column,
                                                const Value& value,
                                                uint32_t* outLength,
                                                uint64_t* outSequence) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        return nullptr;
    }

    BTree* btree = it->second->getIndex(column);
    if (!btree) {
        return nullptr;
    }

    IndexEntry entry;
    if (btree->searchFirst(value, entry)) {
        if (outSequence) {
            *outSequence = entry.sequence;
        }
        return storage_.getDataAtOffset(entry.dataOffset, outLength);
    }

    return nullptr;
}

void FlatSQLDatabase::setFieldExtractor(const std::string& tableName, TableStore::FieldExtractor extractor) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    it->second->setFieldExtractor(extractor);

    // If table has a file ID registered, update SQLite registration
    if (!it->second->getFileId().empty()) {
        updateSQLiteTable(tableName);
    }
}

void FlatSQLDatabase::setFastFieldExtractor(const std::string& tableName, TableStore::FastFieldExtractor extractor) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    it->second->setFastFieldExtractor(extractor);

    // If table has a file ID registered, update SQLite registration
    if (!it->second->getFileId().empty()) {
        updateSQLiteTable(tableName);
    }
}

void FlatSQLDatabase::setBatchExtractor(const std::string& tableName, TableStore::BatchExtractor extractor) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    it->second->setBatchExtractor(extractor);

    // If table has a file ID registered, update SQLite registration
    if (!it->second->getFileId().empty()) {
        updateSQLiteTable(tableName);
    }
}

std::vector<std::string> FlatSQLDatabase::listTables() const {
    std::vector<std::string> names;
    for (const auto& [name, _] : tables_) {
        names.push_back(name);
    }
    return names;
}

const TableDef* FlatSQLDatabase::getTableDef(const std::string& tableName) const {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        return nullptr;
    }
    return &it->second->getTableDef();
}

std::vector<FlatSQLDatabase::TableStats> FlatSQLDatabase::getStats() const {
    std::vector<TableStats> stats;
    for (const auto& [name, store] : tables_) {
        TableStats ts;
        ts.tableName = name;
        ts.fileId = store->getFileId();
        ts.recordCount = store->getRecordCount();
        ts.indexes = store->getIndexNames();
        stats.push_back(ts);
    }
    return stats;
}

// ==================== Multi-Source API ====================

void FlatSQLDatabase::registerSource(const std::string& sourceName) {
    // Check if source already registered
    for (const auto& s : registeredSources_) {
        if (s == sourceName) {
            throw std::runtime_error("Source already registered: " + sourceName);
        }
    }

    registeredSources_.push_back(sourceName);

    // Create source-specific tables for each base table
    for (const auto& tableDef : schema_.tables) {
        createSourceTable(tableDef.name, sourceName);
    }
}

void FlatSQLDatabase::createSourceTable(const std::string& baseTableName, const std::string& source) {
    std::string sourceTableName = getSourceTableName(baseTableName, source);

    // Get base table definition
    auto baseIt = tables_.find(baseTableName);
    if (baseIt == tables_.end()) {
        return;
    }

    // Get base table def
    const TableDef& baseDef = baseIt->second->getTableDef();

    // Create source table with same schema
    tables_[sourceTableName] = std::make_unique<TableStore>(baseDef, storage_);

    // Copy file ID registration for source-specific routing
    std::string fileId = baseIt->second->getFileId();
    if (!fileId.empty()) {
        std::string sourceKey = source + ":" + fileId;
        sourceFileIdToTable_[sourceKey] = sourceTableName;
        tables_[sourceTableName]->setFileId(fileId);

        // Copy field extractor from base table
        auto extractor = baseIt->second->getFieldExtractor();
        if (extractor) {
            tables_[sourceTableName]->setFieldExtractor(extractor);
        }

        auto fastExtractor = baseIt->second->getFastFieldExtractor();
        if (fastExtractor) {
            tables_[sourceTableName]->setFastFieldExtractor(fastExtractor);
        }

        auto batchExtractor = baseIt->second->getBatchExtractor();
        if (batchExtractor) {
            tables_[sourceTableName]->setBatchExtractor(batchExtractor);
        }
    }
}

std::vector<std::string> FlatSQLDatabase::listSources() const {
    return registeredSources_;
}

void FlatSQLDatabase::createUnifiedViews() {
    if (registeredSources_.empty()) {
        return;
    }

    // For each base table, create a unified view
    for (const auto& tableDef : schema_.tables) {
        std::vector<std::string> sourceTableNames;
        for (const auto& source : registeredSources_) {
            std::string sourceTableName = getSourceTableName(tableDef.name, source);
            if (tables_.count(sourceTableName)) {
                // Make sure source table is registered with SQLite
                updateSQLiteTable(sourceTableName);
                sourceTableNames.push_back(sourceTableName);
            }
        }

        if (!sourceTableNames.empty()) {
            // Create unified view with base table name
            sqliteEngine_->createUnifiedView(tableDef.name, sourceTableNames);
        }
    }
}

void FlatSQLDatabase::onIngestWithSource(std::string_view fileId, const uint8_t* data, size_t length,
                                          uint64_t sequence, uint64_t offset, const std::string& source) {
    // Route to source-specific table
    std::string sourceKey = source + ":" + std::string(fileId);
    auto mapIt = sourceFileIdToTable_.find(sourceKey);
    if (mapIt == sourceFileIdToTable_.end()) {
        // Unknown source:fileId combination - skip
        return;
    }

    auto tableIt = tables_.find(mapIt->second);
    if (tableIt != tables_.end()) {
        tableIt->second->onIngest(data, length, sequence, offset);
    }
}

size_t FlatSQLDatabase::ingestWithSource(const uint8_t* data, size_t length,
                                          const std::string& source,
                                          size_t* recordsIngested) {
    return storage_.ingest(data, length,
        [this, &source](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngestWithSource(fileId, data, len, seq, offset, source);
        }, recordsIngested);
}

uint64_t FlatSQLDatabase::ingestOneWithSource(const uint8_t* flatbuffer, size_t length,
                                               const std::string& source) {
    return storage_.ingestFlatBuffer(flatbuffer, length,
        [this, &source](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngestWithSource(fileId, data, len, seq, offset, source);
        });
}

// Legacy multi-source API (external storage)
void FlatSQLDatabase::registerExternalSource(
    const std::string& sourceName,
    StreamingFlatBufferStore* store,
    const TableDef& schema,
    const std::string& fileId,
    TableStore::FieldExtractor extractor
) {
    // Build index map (empty for external sources)
    std::unordered_map<std::string, BTree*> indexes;

    sqliteEngine_->registerSource(
        sourceName,
        store,
        &schema,
        fileId,
        extractor,
        indexes
    );
}

void FlatSQLDatabase::createUnifiedView(
    const std::string& viewName,
    const std::vector<std::string>& sourceNames
) {
    sqliteEngine_->createUnifiedView(viewName, sourceNames);
}

// ==================== Delete Support ====================

void FlatSQLDatabase::markDeleted(const std::string& tableName, uint64_t sequence) {
    sqliteEngine_->markDeleted(tableName, sequence);
}

size_t FlatSQLDatabase::getDeletedCount(const std::string& tableName) const {
    return sqliteEngine_->getDeletedCount(tableName);
}

void FlatSQLDatabase::clearTombstones(const std::string& tableName) {
    sqliteEngine_->clearTombstones(tableName);
}

}  // namespace flatsql
