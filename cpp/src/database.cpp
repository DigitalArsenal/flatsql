#include "flatsql/database.h"
#include <algorithm>
#include <stdexcept>

#ifdef FLATSQL_HAVE_OPENSSL
#include <openssl/hmac.h>
#endif

namespace flatsql {

// ==================== TableStore ====================

TableStore::TableStore(const TableDef& tableDef, StreamingFlatBufferStore& storage, sqlite3* indexDb)
    : tableDef_(tableDef), storage_(storage), indexDb_(indexDb) {

    // Create indexes for indexed columns using SQLite's optimized B-tree
    for (const auto& col : tableDef_.columns) {
        if (col.indexed || col.primaryKey) {
            indexes_[col.name] = std::make_unique<SqliteIndex>(
                indexDb_, tableDef_.name, col.name, col.type);
        }
    }
}

void TableStore::onIngest(const uint8_t* data, size_t length, uint64_t sequence, uint64_t offset) {
    // This is the streaming index builder
    // Called for each FlatBuffer as it arrives

    recordCount_++;

    // Track this record for source-specific iteration
    recordInfos_.push_back({offset, sequence});

    if (!fieldExtractor_) {
        return;  // No extractor, can't index
    }

    // Extract and index each indexed column
    for (auto& [colName, index] : indexes_) {
        Value key = fieldExtractor_(data, length, colName);
        index->insert(key, offset, static_cast<uint32_t>(length), sequence);
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

    // Initialize SQLite engine first (we need its db handle for indexes)
    sqliteEngine_ = std::make_unique<SQLiteEngine>();

    // Initialize table stores with SQLite db handle for indexes
    for (const auto& tableDef : schema_.tables) {
        tables_[tableDef.name] = std::make_unique<TableStore>(
            tableDef, storage_, sqliteEngine_->getDb());
    }
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

    // Build index map (SqliteIndex* pointers)
    std::unordered_map<std::string, SqliteIndex*> indexes;
    for (const auto& col : tableStore->getTableDef().columns) {
        if (col.indexed || col.primaryKey) {
            SqliteIndex* index = tableStore->getIndex(col.name);
            if (index) {
                indexes[col.name] = index;
            }
        }
    }

    // Register with SQLite engine
    // Pass source-specific record infos for multi-source routing
    sqliteEngine_->registerSource(
        tableName,
        &storage_,
        &tableStore->getTableDef(),
        tableStore->getFileId(),
        tableStore->getFieldExtractor(),
        indexes,
        tableStore->getFastFieldExtractor(),
        tableStore->getBatchExtractor(),
        &tableStore->getRecordInfos()
    );

    // Propagate encryption context to the registered source
    if (encryptionCtx_) {
        auto* sourceInfo = sqliteEngine_->getSource(tableName);
        if (sourceInfo) {
            sourceInfo->encryptionCtx = encryptionCtx_.get();
            sourceInfo->vtabInfo.encryptionCtx = encryptionCtx_.get();
        }
    }

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

    SqliteIndex* index = it->second->getIndex(column);
    if (!index) {
        return false;
    }

    IndexEntry entry;
    if (index->searchFirst(value, entry)) {
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

    SqliteIndex* index = it->second->getIndex(column);
    if (!index) {
        return nullptr;
    }

    // Fast path for string keys - avoid Value construction overhead
    if (auto* strKey = std::get_if<std::string>(&value)) {
        uint64_t offset, seq;
        uint32_t len;
        if (index->searchFirstString(*strKey, offset, len, seq)) {
            if (outSequence) {
                *outSequence = seq;
            }
            return storage_.getDataAtOffset(offset, outLength);
        }
        return nullptr;
    }

    // Fast path for int64 keys
    if (auto* intKey = std::get_if<int64_t>(&value)) {
        uint64_t offset, seq;
        uint32_t len;
        if (index->searchFirstInt64(*intKey, offset, len, seq)) {
            if (outSequence) {
                *outSequence = seq;
            }
            return storage_.getDataAtOffset(offset, outLength);
        }
        return nullptr;
    }

    // Fallback for other types
    IndexEntry entry;
    if (index->searchFirst(value, entry)) {
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

    // Create source table with same schema (share the same sqlite db for indexes)
    tables_[sourceTableName] = std::make_unique<TableStore>(
        baseDef, storage_, sqliteEngine_->getDb());

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
    std::unordered_map<std::string, SqliteIndex*> indexes;

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

// ==================== Encryption ====================

void FlatSQLDatabase::setEncryptionKey(const uint8_t* key, size_t keySize) {
    encryptionCtx_ = std::make_unique<flatbuffers::EncryptionContext>(key, keySize);
}

bool FlatSQLDatabase::hasEncryptedFields() const {
    for (const auto& table : schema_.tables) {
        for (const auto& col : table.columns) {
            if (col.encrypted) return true;
        }
    }
    return false;
}

// ==================== HMAC Authentication ====================

void FlatSQLDatabase::setHMACVerification(bool enabled) {
    if (enabled && !encryptionCtx_) {
        throw std::runtime_error("Cannot enable HMAC verification without an encryption key");
    }
    hmacEnabled_ = enabled;
}

bool FlatSQLDatabase::computeHMAC(const uint8_t* buffer, size_t length, uint8_t* outMAC) const {
    if (!encryptionCtx_) return false;
#ifdef FLATSQL_HAVE_OPENSSL
    const uint8_t* key = encryptionCtx_->GetKey();
    unsigned int macLen = 32;
    HMAC(EVP_sha256(), key, 32, buffer, length, outMAC, &macLen);
    return true;
#else
    (void)buffer; (void)length; (void)outMAC;
    return false;
#endif
}

bool FlatSQLDatabase::verifyHMAC(const uint8_t* buffer, size_t length, const uint8_t* mac) const {
    if (!encryptionCtx_) return false;
#ifdef FLATSQL_HAVE_OPENSSL
    uint8_t computed[32];
    computeHMAC(buffer, length, computed);
    // Constant-time comparison to prevent timing attacks
    uint8_t diff = 0;
    for (int i = 0; i < 32; i++) {
        diff |= computed[i] ^ mac[i];
    }
    return diff == 0;
#else
    (void)buffer; (void)length; (void)mac;
    return false;
#endif
}

}  // namespace flatsql
