#include "flatsql/database.h"
#include "flatsql/query_cache.h"
#include <flatbuffers/flatbuffers.h>
#include <algorithm>
#include <mutex>
#include <stdexcept>

#ifdef FLATSQL_HAVE_OPENSSL
#include <openssl/hmac.h>
#endif

namespace flatsql {

namespace {

bool readFlatBufferLayout(const uint8_t* data,
                          size_t length,
                          size_t& rootOffset,
                          size_t& vtableOffset,
                          uint16_t& vtableSize) {
    if (!data || length < sizeof(uint32_t)) {
        return false;
    }

    const uint32_t root = flatbuffers::ReadScalar<uint32_t>(data);
    if (root > length || length - root < sizeof(int32_t)) {
        return false;
    }

    const int32_t vtableDistance = flatbuffers::ReadScalar<int32_t>(data + root);
    if (vtableDistance <= 0 || static_cast<size_t>(vtableDistance) > root) {
        return false;
    }

    const size_t vtable = root - static_cast<size_t>(vtableDistance);
    if (vtable > length || length - vtable < sizeof(uint16_t)) {
        return false;
    }

    const uint16_t size = flatbuffers::ReadScalar<uint16_t>(data + vtable);
    if (size < 4 || size > length - vtable) {
        return false;
    }

    rootOffset = root;
    vtableOffset = vtable;
    vtableSize = size;
    return true;
}

uint16_t readFieldOffset(const uint8_t* data,
                         size_t length,
                         size_t vtableOffset,
                         uint16_t vtableSize,
                         size_t fieldIndex) {
    const size_t entryOffset = vtableOffset + 4 + fieldIndex * sizeof(uint16_t);
    if (entryOffset > length || sizeof(uint16_t) > length - entryOffset) {
        return 0;
    }
    if (entryOffset + sizeof(uint16_t) > vtableOffset + vtableSize) {
        return 0;
    }
    return flatbuffers::ReadScalar<uint16_t>(data + entryOffset);
}

template <typename T>
Value readScalarField(const uint8_t* data, size_t length, size_t fieldOffset) {
    if (fieldOffset > length || sizeof(T) > length - fieldOffset) {
        return std::monostate{};
    }
    return flatbuffers::ReadScalar<T>(data + fieldOffset);
}

Value readStringField(const uint8_t* data, size_t length, size_t fieldOffset) {
    if (fieldOffset > length || sizeof(uint32_t) > length - fieldOffset) {
        return std::monostate{};
    }

    const uint32_t relativeOffset = flatbuffers::ReadScalar<uint32_t>(data + fieldOffset);
    if (relativeOffset > length || fieldOffset > length - relativeOffset) {
        return std::monostate{};
    }

    const size_t stringOffset = fieldOffset + relativeOffset;
    if (stringOffset > length || sizeof(uint32_t) > length - stringOffset) {
        return std::monostate{};
    }

    const uint32_t stringLength = flatbuffers::ReadScalar<uint32_t>(data + stringOffset);
    if (stringOffset + sizeof(uint32_t) > length || stringLength > length - stringOffset - sizeof(uint32_t)) {
        return std::monostate{};
    }

    const char* text = reinterpret_cast<const char*>(data + stringOffset + sizeof(uint32_t));
    return std::string(text, stringLength);
}

Value readBytesField(const uint8_t* data, size_t length, size_t fieldOffset) {
    if (fieldOffset > length || sizeof(uint32_t) > length - fieldOffset) {
        return std::monostate{};
    }

    const uint32_t relativeOffset = flatbuffers::ReadScalar<uint32_t>(data + fieldOffset);
    if (relativeOffset > length || fieldOffset > length - relativeOffset) {
        return std::monostate{};
    }

    const size_t vectorOffset = fieldOffset + relativeOffset;
    if (vectorOffset > length || sizeof(uint32_t) > length - vectorOffset) {
        return std::monostate{};
    }

    const uint32_t vectorLength = flatbuffers::ReadScalar<uint32_t>(data + vectorOffset);
    if (vectorOffset + sizeof(uint32_t) > length || vectorLength > length - vectorOffset - sizeof(uint32_t)) {
        return std::monostate{};
    }

    const uint8_t* begin = data + vectorOffset + sizeof(uint32_t);
    return std::vector<uint8_t>(begin, begin + vectorLength);
}

Value readGenericColumnValue(const uint8_t* data,
                             size_t length,
                             const ColumnDef& column,
                             size_t fieldIndex) {
    size_t rootOffset = 0;
    size_t vtableOffset = 0;
    uint16_t vtableSize = 0;
    if (!readFlatBufferLayout(data, length, rootOffset, vtableOffset, vtableSize)) {
        return std::monostate{};
    }

    const uint16_t fieldObjectOffset = readFieldOffset(data, length, vtableOffset, vtableSize, fieldIndex);
    if (fieldObjectOffset == 0) {
        return std::monostate{};
    }

    const size_t fieldOffset = rootOffset + fieldObjectOffset;
    if (fieldOffset >= length) {
        return std::monostate{};
    }

    switch (column.type) {
        case ValueType::Bool: {
            Value raw = readScalarField<uint8_t>(data, length, fieldOffset);
            if (auto* value = std::get_if<uint8_t>(&raw)) {
                return *value != 0;
            }
            return std::monostate{};
        }
        case ValueType::Int8:
            return readScalarField<int8_t>(data, length, fieldOffset);
        case ValueType::Int16:
            return readScalarField<int16_t>(data, length, fieldOffset);
        case ValueType::Int32:
            return readScalarField<int32_t>(data, length, fieldOffset);
        case ValueType::Int64:
            return readScalarField<int64_t>(data, length, fieldOffset);
        case ValueType::UInt8:
            return readScalarField<uint8_t>(data, length, fieldOffset);
        case ValueType::UInt16:
            return readScalarField<uint16_t>(data, length, fieldOffset);
        case ValueType::UInt32:
            return readScalarField<uint32_t>(data, length, fieldOffset);
        case ValueType::UInt64:
            return readScalarField<uint64_t>(data, length, fieldOffset);
        case ValueType::Float32:
            return readScalarField<float>(data, length, fieldOffset);
        case ValueType::Float64:
            return readScalarField<double>(data, length, fieldOffset);
        case ValueType::String:
            return readStringField(data, length, fieldOffset);
        case ValueType::Bytes:
            return readBytesField(data, length, fieldOffset);
        case ValueType::Null:
        default:
            return std::monostate{};
    }
}

TableStore::FieldExtractor makeGenericFieldExtractor(TableDef tableDef) {
    return [tableDef = std::move(tableDef)](const uint8_t* data,
                                            size_t length,
                                            const std::string& fieldName) -> Value {
        for (size_t index = 0; index < tableDef.columns.size(); index++) {
            if (tableDef.columns[index].name == fieldName) {
                return readGenericColumnValue(data, length, tableDef.columns[index], index);
            }
        }
        return std::monostate{};
    };
}

}  // namespace

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

    // Create R-Tree spatial indexes
    createSpatialIndexes();
}

void TableStore::createSpatialIndexes() {
    // Strategy 1: Explicit (spatial) attribute with pairing
    // Strategy 2: Convention-based detection (lat/latitude + lon/longitude)

    // Collect spatial columns marked with attribute
    std::vector<size_t> spatialCols;
    for (size_t i = 0; i < tableDef_.columns.size(); i++) {
        if (tableDef_.columns[i].spatial) {
            spatialCols.push_back(i);
        }
    }

    // If explicit spatial columns found, pair them
    if (spatialCols.size() >= 2) {
        // Take first two spatial columns as lat/lon pair
        SpatialIndexDef si;
        si.latColumn = tableDef_.columns[spatialCols[0]].name;
        si.lonColumn = tableDef_.columns[spatialCols[1]].name;
        si.rtreeName = "_rtree_" + tableDef_.name;
        spatialIndexes_.push_back(si);
    }

    // Convention-based: detect lat/lon column pairs
    if (spatialIndexes_.empty()) {
        auto findCol = [&](const std::vector<std::string>& names) -> std::string {
            for (const auto& col : tableDef_.columns) {
                std::string lower = col.name;
                for (auto& c : lower) c = std::tolower(c);
                for (const auto& n : names) {
                    if (lower == n) return col.name;
                }
            }
            return "";
        };

        std::string latCol = findCol({"latitude", "lat"});
        std::string lonCol = findCol({"longitude", "lon", "lng"});
        if (!latCol.empty() && !lonCol.empty()) {
            SpatialIndexDef si;
            si.latColumn = latCol;
            si.lonColumn = lonCol;
            si.rtreeName = "_rtree_" + tableDef_.name;
            spatialIndexes_.push_back(si);
        }
    }

    // Create R-Tree virtual tables in SQLite
    for (const auto& si : spatialIndexes_) {
        std::string sql = "CREATE VIRTUAL TABLE IF NOT EXISTS \"" + si.rtreeName +
                          "\" USING rtree(id, minLat, maxLat, minLon, maxLon)";
        char* errMsg = nullptr;
        int rc = sqlite3_exec(indexDb_, sql.c_str(), nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            // Non-fatal: log and continue without R-Tree
            sqlite3_free(errMsg);
            continue;
        }
    }
}

void TableStore::insertIntoRTree(const SpatialIndexDef& si, const uint8_t* data, size_t length, uint64_t sequence) {
    if (!fieldExtractor_) return;

    Value latVal = fieldExtractor_(data, length, si.latColumn);
    Value lonVal = fieldExtractor_(data, length, si.lonColumn);

    // Extract doubles from Value variant
    double lat = 0, lon = 0;
    bool valid = true;

    auto toDouble = [](const Value& v, double& out) -> bool {
        return std::visit([&out](const auto& val) -> bool {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, double>) { out = val; return true; }
            else if constexpr (std::is_same_v<T, float>) { out = static_cast<double>(val); return true; }
            else if constexpr (std::is_same_v<T, int32_t>) { out = static_cast<double>(val); return true; }
            else if constexpr (std::is_same_v<T, int64_t>) { out = static_cast<double>(val); return true; }
            else if constexpr (std::is_same_v<T, std::monostate>) { return false; }
            else { return false; }
        }, v);
    };

    valid = toDouble(latVal, lat) && toDouble(lonVal, lon);
    if (!valid) return;

    std::string sql = "INSERT INTO \"" + si.rtreeName + "\" VALUES(?, ?, ?, ?, ?)";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(indexDb_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return;

    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(sequence));
    sqlite3_bind_double(stmt, 2, lat);  // minLat
    sqlite3_bind_double(stmt, 3, lat);  // maxLat (point = equal)
    sqlite3_bind_double(stmt, 4, lon);  // minLon
    sqlite3_bind_double(stmt, 5, lon);  // maxLon

    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
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

    // Populate R-Tree spatial indexes
    for (const auto& si : spatialIndexes_) {
        insertIntoRTree(si, data, length, sequence);
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

FlatSQLDatabase::FlatSQLDatabase(const DatabaseSchema& schema, RuntimeOptions options)
    : schema_(schema)
    , storage_(options.sharedStore ? std::move(options.sharedStore)
                                   : std::make_shared<StreamingFlatBufferStore>())
    , accessMutex_(options.accessMutex ? std::move(options.accessMutex)
                                       : std::make_shared<std::shared_mutex>()) {

    // Initialize SQLite engine first (we need its db handle for indexes)
    sqliteEngine_ = std::make_unique<SQLiteEngine>(options.sqlite);

    // Initialize table stores with SQLite db handle for indexes
    for (const auto& tableDef : schema_.tables) {
        tables_[tableDef.name] = std::make_unique<TableStore>(
            tableDef, *storage_, sqliteEngine_->getDb());
        tables_[tableDef.name]->setFieldExtractor(makeGenericFieldExtractor(tableDef));
    }
}

FlatSQLDatabase FlatSQLDatabase::fromSchema(const std::string& source,
                                            const std::string& dbName,
                                            RuntimeOptions options) {
    DatabaseSchema schema = SchemaParser::parse(source, dbName);
    return FlatSQLDatabase(schema, std::move(options));
}

void FlatSQLDatabase::registerFileId(const std::string& fileId, const std::string& tableName) {
    std::unique_lock lock(*accessMutex_);
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    fileIdToTable_[fileId] = tableName;
    it->second->setFileId(fileId);
    invalidateQueryResultCacheUnlocked();
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
    std::unique_lock lock(*accessMutex_);
    invalidateQueryResultCacheUnlocked();
    IngestProfile* profile = ingestProfileEnabled_ ? &ingestProfile_ : nullptr;
    return storage_->ingest(data, length,
        [this](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngest(fileId, data, len, seq, offset);
        }, recordsIngested, profile);
}

uint64_t FlatSQLDatabase::ingestOne(const uint8_t* flatbuffer, size_t length) {
    std::unique_lock lock(*accessMutex_);
    invalidateQueryResultCacheUnlocked();
    IngestProfile* profile = ingestProfileEnabled_ ? &ingestProfile_ : nullptr;
    return storage_->ingestFlatBuffer(flatbuffer, length,
        [this](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngest(fileId, data, len, seq, offset);
        }, profile);
}

void FlatSQLDatabase::loadAndRebuild(const uint8_t* data, size_t length) {
    std::unique_lock lock(*accessMutex_);
    invalidateQueryResultCacheUnlocked();
    IngestProfile* profile = ingestProfileEnabled_ ? &ingestProfile_ : nullptr;
    storage_->loadAndRebuild(data, length,
        [this](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngest(fileId, data, len, seq, offset);
        }, profile);
}

void FlatSQLDatabase::reserveStorage(size_t bytes) {
    std::unique_lock lock(*accessMutex_);
    storage_->reserveCapacity(bytes);
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
    // Base tables should read record infos from the shared store so that
    // independent reader connections observe writer progress.
    const bool isSourceTable = tableName.find('@') != std::string::npos;
    sqliteEngine_->registerSource(
        tableName,
        storage_.get(),
        &tableStore->getTableDef(),
        tableStore->getFileId(),
        tableStore->getFieldExtractor(),
        indexes,
        tableStore->getFastFieldExtractor(),
        tableStore->getBatchExtractor(),
        isSourceTable ? &tableStore->getRecordInfos() : nullptr
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

bool FlatSQLDatabase::validateSQL(const std::string& sql, int* paramCountOut, std::string* errOut) noexcept {
    // Never throws for user-triggerable failures; the try/catch guards the
    // exceptions build against unexpected internal errors (mutex/alloc/engine
    // initialization) so noexcept never terminates there.
    try {
        std::unique_lock lock(*accessMutex_);
        if (!sqliteInitialized_) {
            initializeSQLiteEngine();
        }
        return sqliteEngine_->validateSQL(sql, paramCountOut, errOut);
    } catch (const std::exception& e) {
        if (paramCountOut) *paramCountOut = 0;
        if (errOut) {
            try { *errOut = e.what(); } catch (...) {}
        }
        return false;
    } catch (...) {
        if (paramCountOut) *paramCountOut = 0;
        if (errOut) {
            try { *errOut = "SQL error: validation failed"; } catch (...) {}
        }
        return false;
    }
}

bool FlatSQLDatabase::hasQueryTemplate(const std::string& id) const noexcept {
    try {
        std::shared_lock lock(*accessMutex_);
        return queryTemplates_.find(id) != queryTemplates_.end();
    } catch (...) {
        return false;
    }
}

const std::string* FlatSQLDatabase::queryTemplateSQL(const std::string& id) const noexcept {
    try {
        std::shared_lock lock(*accessMutex_);
        auto it = queryTemplates_.find(id);
        return it == queryTemplates_.end() ? nullptr : &it->second.sql;
    } catch (...) {
        return nullptr;
    }
}

bool FlatSQLDatabase::hasSource(const std::string& name) const noexcept {
    try {
        std::shared_lock lock(*accessMutex_);
        for (const auto& s : registeredSources_) {
            if (s == name) {
                return true;
            }
        }
        return false;
    } catch (...) {
        return false;
    }
}

QueryResult FlatSQLDatabase::query(const std::string& sql) {
    if (!sqliteInitialized_) {
        std::unique_lock initLock(*accessMutex_);
        initializeSQLiteEngine();
        return sqliteEngine_->execute(sql);
    }

    std::unique_lock lock(*accessMutex_);
    return sqliteEngine_->execute(sql);
}

QueryResult FlatSQLDatabase::query(const std::string& sql, const std::vector<Value>& params) {
    if (!sqliteInitialized_) {
        std::unique_lock initLock(*accessMutex_);
        initializeSQLiteEngine();
        return sqliteEngine_->execute(sql, params);
    }

    std::unique_lock lock(*accessMutex_);
    return sqliteEngine_->execute(sql, params);
}

QueryResult FlatSQLDatabase::query(const std::string& sql, int64_t param) {
    // Use thread-local reusable vector for single-param queries
    static thread_local std::vector<Value> singleParam(1);
    singleParam[0] = param;

    if (!sqliteInitialized_) {
        std::unique_lock initLock(*accessMutex_);
        initializeSQLiteEngine();
        return sqliteEngine_->execute(sql, singleParam);
    }

    std::unique_lock lock(*accessMutex_);
    return sqliteEngine_->execute(sql, singleParam);
}

void FlatSQLDatabase::registerQueryTemplate(const std::string& queryId,
                                            const std::string& sql,
                                            bool cacheable) {
    std::unique_lock lock(*accessMutex_);
    auto existing = queryTemplates_.find(queryId);
    const bool changed = existing != queryTemplates_.end() &&
        (existing->second.sql != sql || existing->second.cacheable != cacheable);

    queryTemplates_[queryId] = QueryTemplateDef{sql, cacheable};

    if (changed) {
        invalidateQueryResultCacheUnlocked();
    }
}

QueryResult FlatSQLDatabase::queryTemplate(const std::string& queryId,
                                           const std::vector<Value>& params) {
    std::unique_lock lock(*accessMutex_);

    auto templateIt = queryTemplates_.find(queryId);
    if (templateIt == queryTemplates_.end()) {
        throw std::runtime_error("Query template not found: " + queryId);
    }

    const QueryTemplateDef& tmpl = templateIt->second;
    if (tmpl.cacheable) {
        const std::string cacheKey = buildTemplateCacheKeyUnlocked(queryId, tmpl.sql, params);
        auto cached = queryResultCache_.find(cacheKey);
        if (cached != queryResultCache_.end()) {
            queryCacheHits_++;
            queryResultCacheLru_.splice(
                queryResultCacheLru_.begin(),
                queryResultCacheLru_,
                cached->second.lruIt
            );
            cached->second.lruIt = queryResultCacheLru_.begin();
            return cached->second.result;
        }

        queryCacheMisses_++;
        if (!sqliteInitialized_) {
            initializeSQLiteEngine();
        }
        QueryResult result = sqliteEngine_->execute(tmpl.sql, params);
        storeCachedQueryResultUnlocked(cacheKey, result);
        return result;
    }

    if (!sqliteInitialized_) {
        initializeSQLiteEngine();
    }
    return sqliteEngine_->execute(tmpl.sql, params);
}

void FlatSQLDatabase::clearQueryResultCache() {
    std::unique_lock lock(*accessMutex_);
    invalidateQueryResultCacheUnlocked();
}

void FlatSQLDatabase::configureQueryResultCache(size_t maxEntries, size_t maxRows) {
    if (maxEntries == 0) {
        throw std::runtime_error("query cache maxEntries must be greater than zero");
    }
    if (maxRows == 0) {
        throw std::runtime_error("query cache maxRows must be greater than zero");
    }

    std::unique_lock lock(*accessMutex_);
    queryResultCacheMaxEntries_ = maxEntries;
    queryResultCacheMaxRows_ = maxRows;
    invalidateQueryResultCacheUnlocked();
}

FlatSQLDatabase::QueryCacheStats FlatSQLDatabase::getQueryCacheStats() const {
    std::shared_lock lock(*accessMutex_);
    return QueryCacheStats{
        queryCacheHits_,
        queryCacheMisses_,
        queryResultCache_.size(),
        queryCacheGeneration_,
        queryResultCacheMaxEntries_,
        queryResultCacheMaxRows_
    };
}

void FlatSQLDatabase::invalidateQueryResultCacheUnlocked() {
    queryResultCache_.clear();
    queryResultCacheLru_.clear();
    queryCacheGeneration_++;
}

void FlatSQLDatabase::storeCachedQueryResultUnlocked(const std::string& key,
                                                     const QueryResult& result) {
    if (result.rows.size() > queryResultCacheMaxRows_) {
        return;
    }

    auto existing = queryResultCache_.find(key);
    if (existing != queryResultCache_.end()) {
        existing->second.result = result;
        queryResultCacheLru_.splice(
            queryResultCacheLru_.begin(),
            queryResultCacheLru_,
            existing->second.lruIt
        );
        existing->second.lruIt = queryResultCacheLru_.begin();
        return;
    }

    queryResultCacheLru_.push_front(key);
    queryResultCache_.emplace(key, CachedQueryResult{result, queryResultCacheLru_.begin()});

    while (queryResultCache_.size() > queryResultCacheMaxEntries_) {
        const std::string& evictedKey = queryResultCacheLru_.back();
        queryResultCache_.erase(evictedKey);
        queryResultCacheLru_.pop_back();
    }
}

std::string FlatSQLDatabase::buildTemplateCacheKeyUnlocked(const std::string& queryId,
                                                           const std::string& sql,
                                                           const std::vector<Value>& params) const {
    (void)sql;
    return buildQueryCacheKey(schema_.name, std::to_string(queryCacheGeneration_), queryId, params);
}

size_t FlatSQLDatabase::queryCount(const std::string& sql, const std::vector<Value>& params) {
    if (!sqliteInitialized_) {
        std::unique_lock initLock(*accessMutex_);
        initializeSQLiteEngine();
        return sqliteEngine_->executeAndCount(sql, params);
    }

    std::unique_lock lock(*accessMutex_);
    return sqliteEngine_->executeAndCount(sql, params);
}

std::vector<StoredRecord> FlatSQLDatabase::findByIndex(const std::string& tableName,
                                                        const std::string& column,
                                                        const Value& value) {
    std::unique_lock lock(*accessMutex_);
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
    std::unique_lock lock(*accessMutex_);
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
    std::unique_lock lock(*accessMutex_);
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
            return storage_->getDataAtOffset(offset, outLength);
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
            return storage_->getDataAtOffset(offset, outLength);
        }
        return nullptr;
    }

    // Fallback for other types
    IndexEntry entry;
    if (index->searchFirst(value, entry)) {
        if (outSequence) {
            *outSequence = entry.sequence;
        }
        return storage_->getDataAtOffset(entry.dataOffset, outLength);
    }

    return nullptr;
}

void FlatSQLDatabase::setFieldExtractor(const std::string& tableName, TableStore::FieldExtractor extractor) {
    std::unique_lock lock(*accessMutex_);
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    it->second->setFieldExtractor(extractor);
    invalidateQueryResultCacheUnlocked();

    // If table has a file ID registered, update SQLite registration
    if (!it->second->getFileId().empty()) {
        updateSQLiteTable(tableName);
    }
}

void FlatSQLDatabase::setFastFieldExtractor(const std::string& tableName, TableStore::FastFieldExtractor extractor) {
    std::unique_lock lock(*accessMutex_);
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    it->second->setFastFieldExtractor(extractor);
    invalidateQueryResultCacheUnlocked();

    // If table has a file ID registered, update SQLite registration
    if (!it->second->getFileId().empty()) {
        updateSQLiteTable(tableName);
    }
}

void FlatSQLDatabase::setBatchExtractor(const std::string& tableName, TableStore::BatchExtractor extractor) {
    std::unique_lock lock(*accessMutex_);
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    it->second->setBatchExtractor(extractor);
    invalidateQueryResultCacheUnlocked();

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
    std::shared_lock lock(*accessMutex_);
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
    std::unique_lock lock(*accessMutex_);
    // Check if source already registered
    for (const auto& s : registeredSources_) {
        if (s == sourceName) {
            throw std::runtime_error("Source already registered: " + sourceName);
        }
    }

    invalidateQueryResultCacheUnlocked();
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
        baseDef, *storage_, sqliteEngine_->getDb());

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
    std::unique_lock lock(*accessMutex_);
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
    invalidateQueryResultCacheUnlocked();
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
    std::unique_lock lock(*accessMutex_);
    invalidateQueryResultCacheUnlocked();
    IngestProfile* profile = ingestProfileEnabled_ ? &ingestProfile_ : nullptr;
    return storage_->ingest(data, length,
        [this, &source](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngestWithSource(fileId, data, len, seq, offset, source);
        }, recordsIngested, profile);
}

uint64_t FlatSQLDatabase::ingestOneWithSource(const uint8_t* flatbuffer, size_t length,
                                               const std::string& source) {
    std::unique_lock lock(*accessMutex_);
    invalidateQueryResultCacheUnlocked();
    IngestProfile* profile = ingestProfileEnabled_ ? &ingestProfile_ : nullptr;
    return storage_->ingestFlatBuffer(flatbuffer, length,
        [this, &source](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngestWithSource(fileId, data, len, seq, offset, source);
        }, profile);
}

// Legacy multi-source API (external storage)
void FlatSQLDatabase::registerExternalSource(
    const std::string& sourceName,
    StreamingFlatBufferStore* store,
    const TableDef& schema,
    const std::string& fileId,
    TableStore::FieldExtractor extractor
) {
    std::unique_lock lock(*accessMutex_);
    invalidateQueryResultCacheUnlocked();
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
    std::unique_lock lock(*accessMutex_);
    sqliteEngine_->createUnifiedView(viewName, sourceNames);
    invalidateQueryResultCacheUnlocked();
}

// ==================== Delete Support ====================

void FlatSQLDatabase::markDeleted(const std::string& tableName, uint64_t sequence) {
    std::unique_lock lock(*accessMutex_);
    sqliteEngine_->markDeleted(tableName, sequence);
    invalidateQueryResultCacheUnlocked();
}

size_t FlatSQLDatabase::getDeletedCount(const std::string& tableName) const {
    std::shared_lock lock(*accessMutex_);
    return sqliteEngine_->getDeletedCount(tableName);
}

void FlatSQLDatabase::clearTombstones(const std::string& tableName) {
    std::unique_lock lock(*accessMutex_);
    sqliteEngine_->clearTombstones(tableName);
    invalidateQueryResultCacheUnlocked();
}

// ==================== Encryption ====================

void FlatSQLDatabase::setEncryptionKey(const uint8_t* key, size_t keySize) {
    std::unique_lock lock(*accessMutex_);
    encryptionCtx_ = std::make_unique<flatbuffers::EncryptionContext>(key, keySize);
    invalidateQueryResultCacheUnlocked();
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
