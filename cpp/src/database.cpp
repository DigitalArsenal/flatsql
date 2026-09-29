#include "flatsql/database.h"
#include "flatsql/query_cache.h"
#include <flatbuffers/flatbuffers.h>
#include <algorithm>
#include <cstring>
#include <limits>
#include <sstream>
#include <locale>
#include <type_traits>
#include <mutex>
#include <stdexcept>
#include <unordered_set>

#if defined(__EMSCRIPTEN__)
#include <malloc.h>
#endif

namespace flatsql {

namespace {

#if defined(__EMSCRIPTEN__)
// dlmalloc extends the heap 64 KiB at a time, and on wasm every extension is
// a memory.grow. A 90k-row result (~30 MB) therefore cost ~450 grows, which
// V8 hosts (node, browsers) charged at ~1 ms each: a 550-800 ms stall the
// first time a large result was materialized. Growing in 4 MiB steps makes it
// a handful of grows; the most a heap can over-reserve is one step.
const int kHeapGrowthStep = mallopt(M_GRANULARITY, 4 * 1024 * 1024);
#endif

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
    const int64_t vtablePosition = static_cast<int64_t>(root) - vtableDistance;
    if (vtablePosition < 0 || static_cast<uint64_t>(vtablePosition) > length) {
        return false;
    }

    const size_t vtable = static_cast<size_t>(vtablePosition);
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

bool recordSearchText(const TableDef& table, const uint8_t* data, size_t length,
                      std::string& output, std::string* error) {
    output.clear();
    const auto fail = [&](const char* reason) {
        if (error) *error = reason;
        return false;
    };
    if (length > 16 * 1024 * 1024) return fail("Record exceeds search extraction limit");
    size_t root = 0, vtable = 0;
    uint16_t vtableSize = 0;
    if (!readFlatBufferLayout(data, length, root, vtable, vtableSize))
        return fail("Invalid FlatBuffer search record");
    std::ostringstream text;
    text.imbue(std::locale::classic());
    text.precision(std::numeric_limits<double>::max_digits10);
    for (size_t index = 0; index < table.columns.size(); ++index) {
        const auto& column = table.columns[index];
        if (column.encrypted || column.type == ValueType::Bytes || column.type == ValueType::Null ||
            (!column.name.empty() && column.name[0] == '_')) continue;
        if (!readFieldOffset(data, length, vtable, vtableSize, index)) continue;
        const Value value = readGenericColumnValue(data, length, column, index);
        if (std::holds_alternative<std::monostate>(value)) {
            if (error) *error = "Invalid FlatBuffer search field: " + column.name;
            return false;
        }
        std::visit([&](const auto& item) {
            using T = std::decay_t<decltype(item)>;
            if constexpr (std::is_same_v<T, std::string>) text << item << '\n';
            else if constexpr (std::is_same_v<T, bool>) text << (item ? "true" : "false") << '\n';
            else if constexpr (std::is_arithmetic_v<T>) text << +item << '\n';
        }, value);
        if (text.tellp() > 16 * 1024 * 1024) return fail("Search text exceeds extraction limit");
    }
    output = text.str();
    return true;
}

// ==================== TableStore ====================

TableStore::TableStore(const TableDef& tableDef, StreamingFlatBufferStore& storage, sqlite3* indexDb)
    : tableDef_(tableDef), storage_(storage), indexDb_(indexDb) {

    // Create indexes for indexed columns using SQLite's optimized B-tree.
    // An (encrypted) column is not indexed: its stored bytes are ciphertext
    // under a key of their own record, which no plaintext predicate matches.
    for (const auto& col : tableDef_.columns) {
        if ((col.indexed || col.primaryKey) && !col.encrypted) {
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
        if (tableDef_.columns[i].spatial && !tableDef_.columns[i].encrypted) {
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
                if (col.encrypted) continue;  // ciphertext is not a coordinate
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

void TableStore::clearDerived() {
    // Direct DELETE rather than SqliteIndex::clear(): that path throws, and a
    // throw on the -fignore-exceptions WASI artifact is a guest abort. A full
    // re-derivation is a RECOVERY path — it may never be the thing that kills
    // the instance (b26ed45).
    for (auto& [name, index] : indexes_) {
        const std::string sql =
            "DELETE FROM \"" + index->getIndexTableName() + "\"";
        sqlite3_exec(indexDb_, sql.c_str(), nullptr, nullptr, nullptr);
    }
    for (const auto& si : spatialIndexes_) {
        const std::string sql = "DELETE FROM \"" + si.rtreeName + "\"";
        sqlite3_exec(indexDb_, sql.c_str(), nullptr, nullptr, nullptr);
    }
    recordInfos_.clear();
    recordCount_ = 0;
}

// Replay of a record whose index rows are already on disk. Restores the record
// info the vtab scans need and nothing else — no extraction, no index insert.
void TableStore::onIngestReplay(uint64_t sequence, uint64_t offset) {
    recordCount_++;
    recordInfos_.push_back({offset, sequence});
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

    diskBacked_ = !options.sqlite.path.empty() && options.sqlite.path != ":memory:";
    if (diskBacked_) {
        // One name, one pair: the stream that backs this database is derived
        // from the database path, so a host cannot pair an index with a stream
        // that was never meant to go with it.
        sqlitePath_ = options.sqlite.path;
        streamPath_ = options.sqlite.path + ".fsdata";
    }

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
    sqliteEngine_->registerRecordSchema(tableName, &it->second->getTableDef(), fileId);
    const auto refreshIdentity = [&](const std::string& name) {
        // Initialization skips tables without an identifier. A later identity
        // must register the table as well as refresh an existing source.
        if (sqliteInitialized_) updateSQLiteTable(name);
        if (sqliteEngine_) {
            if (auto* source = sqliteEngine_->getSource(name)) {
                source->fileId = fileId;
                source->vtabInfo.fileId = fileId;
            }
        }
    };
    refreshIdentity(tableName);

    // Sources registered BEFORE the file id would otherwise hold partition
    // tables that route nothing — a silent empty partition that looks exactly
    // like the restore defect this file exists to prevent. Order must not
    // matter, so late file ids reach the partitions too.
    for (const auto& source : registeredSources_) {
        const std::string sourceTableName = getSourceTableName(tableName, source);
        auto sourceIt = tables_.find(sourceTableName);
        if (sourceIt == tables_.end()) continue;
        if (!sourceIt->second->getFileId().empty()) continue;
        sourceIt->second->setFileId(fileId);
        refreshIdentity(sourceTableName);
        sourceFileIdToTable_[source + ":" + fileId] = sourceTableName;
        if (auto extractor = it->second->getFieldExtractor()) {
            sourceIt->second->setFieldExtractor(extractor);
        }
        if (auto fastExtractor = it->second->getFastFieldExtractor()) {
            sourceIt->second->setFastFieldExtractor(fastExtractor);
        }
        if (auto batchExtractor = it->second->getBatchExtractor()) {
            sourceIt->second->setBatchExtractor(batchExtractor);
        }
    }

    invalidateQueryResultCacheUnlocked();
}

void FlatSQLDatabase::onIngest(std::string_view fileId, const uint8_t* data, size_t length,
                                uint64_t sequence, uint64_t offset, bool buildIndexes) {
    // Route to the correct table based on file identifier
    std::string fileIdStr(fileId);
    auto mapIt = fileIdToTable_.find(fileIdStr);
    if (mapIt == fileIdToTable_.end()) {
        // Unknown file identifier - skip (or could throw)
        return;
    }

    auto tableIt = tables_.find(mapIt->second);
    if (tableIt != tables_.end()) {
        if (buildIndexes) {
            tableIt->second->onIngest(data, length, sequence, offset);
        } else {
            tableIt->second->onIngestReplay(sequence, offset);
        }
    }
}

size_t FlatSQLDatabase::ingest(const uint8_t* data, size_t length, size_t* recordsIngested) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
    SQLiteWriteBatch batch(*sqliteEngine_);
    if (!batch.ok()) throw std::runtime_error(batch.error());
    invalidateQueryResultCacheUnlocked();
    IngestProfile* profile = ingestProfileEnabled_ ? &ingestProfile_ : nullptr;
    // If extraction or commit fails, SQL rolls back but the arena has already
    // appended bytes. Refuse reads until recovery rather than serving indexes
    // that disagree with that arena. The successful path clears the latch.
    reindexUnavailable_ = true;
    const auto consumed = storage_->ingest(data, length,
        [this](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngest(fileId, data, len, seq, offset);
        }, recordsIngested, profile);
    if (!batch.commit()) throw std::runtime_error(batch.error());
    reindexUnavailable_ = false;
    return consumed;
}

void FlatSQLDatabase::setArenaLimit(size_t bytes) {
    std::unique_lock lock(*accessMutex_);
    storage_->setArenaLimit(bytes);
}

uint64_t FlatSQLDatabase::ingestOne(const uint8_t* flatbuffer, size_t length) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
    return ingestOneUnlocked(flatbuffer, length, nullptr);
}

uint64_t FlatSQLDatabase::ingestOneUnlocked(const uint8_t* flatbuffer, size_t length,
                                            const std::string* source) {
    if (!storage_->canAppend(SIZE_PREFIX_LENGTH + length)) {
        return kIngestRefused;
    }
    invalidateQueryResultCacheUnlocked();
    IngestProfile* profile = ingestProfileEnabled_ ? &ingestProfile_ : nullptr;
    if (!source) {
        return storage_->ingestFlatBuffer(flatbuffer, length,
            [this](std::string_view fileId, const uint8_t* data, size_t len,
                   uint64_t seq, uint64_t offset) {
                onIngest(fileId, data, len, seq, offset);
            }, profile);
    }
    const uint64_t begin = storage_->getWriteOffset();
    const uint64_t seq = storage_->ingestFlatBuffer(flatbuffer, length,
        [this, source](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngestWithSource(fileId, data, len, seq, offset, *source);
        }, profile);
    recordSourceRangeUnlocked(*source, begin, storage_->getWriteOffset());
    return seq;
}

void FlatSQLDatabase::loadAndRebuild(const uint8_t* data, size_t length) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
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
    std::string err;
    if (!updateSQLiteTableNoThrow(tableName, &err)) {
        throw std::runtime_error(err);
    }
}

bool FlatSQLDatabase::updateSQLiteTableNoThrow(const std::string& tableName,
                                               std::string* errOut) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        return true;
    }

    TableStore* tableStore = it->second.get();

    // Skip if already registered
    if (sqliteRegisteredTables_.count(tableName)) {
        return true;
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
    if (!sqliteEngine_->registerSourceNoThrow(
            tableName,
            storage_.get(),
            &tableStore->getTableDef(),
            tableStore->getFileId(),
            tableStore->getFieldExtractor(),
            indexes,
            tableStore->getFastFieldExtractor(),
            tableStore->getBatchExtractor(),
            isSourceTable ? &tableStore->getRecordInfos() : nullptr,
            errOut)) {
        return false;
    }

    // The table reads the record key through the database's holder at query
    // time, so a key set after this registration still reaches it.
    if (auto* sourceInfo = sqliteEngine_->getSource(tableName)) {
        sourceInfo->vtabInfo.recordKey = recordKey_.get();
    }

    sqliteRegisteredTables_.insert(tableName);
    return true;
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

void FlatSQLDatabase::requireReadyUnlocked() const {
    if (reindexUnavailable_) throw std::runtime_error("state: reindex incomplete");
}

QueryResult FlatSQLDatabase::query(const std::string& sql) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
    if (!sqliteInitialized_) initializeSQLiteEngine();
    QueryResult result = sqliteEngine_->execute(sql);
    invalidateCachesIfStatementWritesUnlocked(sql);
    return result;
}

QueryResult FlatSQLDatabase::query(const std::string& sql, const std::vector<Value>& params) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
    if (!sqliteInitialized_) initializeSQLiteEngine();
    QueryResult result = sqliteEngine_->execute(sql, params);
    invalidateCachesIfStatementWritesUnlocked(sql);
    return result;
}

// DML/DDL executed through plain query() invalidates the native result
// caches: cached template results and raw-stream artifacts may read control
// tables that plain SQL just mutated. The statement is already prepared and
// cached by execute(), so the readonly check is a map lookup.
void FlatSQLDatabase::invalidateCachesIfStatementWritesUnlocked(const std::string& sql) {
    if (!sqliteEngine_->statementIsReadOnly(sql)) {
        invalidateQueryResultCacheUnlocked();
    }
}

bool FlatSQLDatabase::queryNoThrow(const std::string& sql, const std::vector<Value>& params,
                                   QueryResult& out, std::string* errOut) noexcept try {
    std::unique_lock lock(*accessMutex_);
    if (reindexUnavailable_) {
        out = {};
        if (errOut) *errOut = "state: reindex incomplete";
        return false;
    }
    if (!sqliteInitialized_) {
        initializeSQLiteEngine();
    }
    if (!sqliteEngine_->executeNoThrow(sql, params, out, errOut)) {
        return false;
    }
    // Same cache invalidation as query(), through the no-throw readonly probe.
    if (!sqliteEngine_->statementIsReadOnlyNoThrow(sql)) {
        invalidateQueryResultCacheUnlocked();
    }
    return true;
} catch (const std::exception& e) {
    if (errOut) {
        try { *errOut = e.what(); } catch (...) {}
    }
    return false;
} catch (...) {
    if (errOut) {
        try { *errOut = "SQL execution error: internal failure"; } catch (...) {}
    }
    return false;
}

QueryResult FlatSQLDatabase::query(const std::string& sql, int64_t param) {
    // Use thread-local reusable vector for single-param queries
    static thread_local std::vector<Value> singleParam(1);
    singleParam[0] = param;

    return query(sql, singleParam);
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
    requireReadyUnlocked();

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

bool FlatSQLDatabase::queryTemplateNoThrow(const std::string& queryId, const std::vector<Value>& params,
                                           QueryResult& out, std::string* errOut) noexcept try {
    std::unique_lock lock(*accessMutex_);
    if (reindexUnavailable_) {
        out = {};
        if (errOut) *errOut = "state: reindex incomplete";
        return false;
    }

    auto templateIt = queryTemplates_.find(queryId);
    if (templateIt == queryTemplates_.end()) {
        if (errOut) {
            try { *errOut = "Query template not found: " + queryId; } catch (...) {}
        }
        return false;
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
            out = cached->second.result;
            return true;
        }

        queryCacheMisses_++;
        if (!sqliteInitialized_) {
            initializeSQLiteEngine();
        }
        QueryResult result;
        if (!sqliteEngine_->executeNoThrow(tmpl.sql, params, result, errOut)) {
            return false;
        }
        storeCachedQueryResultUnlocked(cacheKey, result);
        out = std::move(result);
        return true;
    }

    if (!sqliteInitialized_) {
        initializeSQLiteEngine();
    }
    return sqliteEngine_->executeNoThrow(tmpl.sql, params, out, errOut);
} catch (const std::exception& e) {
    if (errOut) {
        try { *errOut = e.what(); } catch (...) {}
    }
    return false;
} catch (...) {
    if (errOut) {
        try { *errOut = "SQL execution error: internal failure"; } catch (...) {}
    }
    return false;
}

void FlatSQLDatabase::clearQueryResultCache() {
    std::unique_lock lock(*accessMutex_);
    invalidateQueryResultCacheUnlocked();
}

void FlatSQLDatabase::configureQueryResultCache(size_t maxEntries, size_t maxRows) {
    size_t maxBytes;
    {
        std::shared_lock lock(*accessMutex_);
        maxBytes = queryResultCacheMaxBytes_;
    }
    configureQueryResultCache(maxEntries, maxRows, maxBytes);
}

void FlatSQLDatabase::configureQueryResultCache(size_t maxEntries, size_t maxRows, size_t maxBytes) {
    if (maxBytes == 0) {
        throw std::runtime_error("query cache maxBytes must be greater than zero");
    }
    if (maxEntries == 0) {
        throw std::runtime_error("query cache maxEntries must be greater than zero");
    }
    if (maxRows == 0) {
        throw std::runtime_error("query cache maxRows must be greater than zero");
    }

    std::unique_lock lock(*accessMutex_);
    queryResultCacheMaxEntries_ = maxEntries;
    queryResultCacheMaxRows_ = maxRows;
    queryResultCacheMaxBytes_ = maxBytes;
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
        queryResultCacheMaxRows_,
        queryResultCacheMaxBytes_,
        queryResultCacheTotalBytes_
    };
}

void FlatSQLDatabase::invalidateQueryResultCacheUnlocked() {
    queryResultCache_.clear();
    queryResultCacheLru_.clear();
    queryResultCacheTotalBytes_ = 0;
    rawStreamCache_.clear();
    rawStreamCacheLru_.clear();
    rawStreamCacheTotalBytes_ = 0;
    queryCacheGeneration_++;
}

namespace {

// Heap bytes a cached QueryResult holds: the row vectors, every cell, and the
// string/blob payloads. An estimate of allocator reality, but a monotone one:
// twice the payload is always counted as more.
size_t estimateQueryResultBytes(const QueryResult& result) {
    size_t bytes = sizeof(QueryResult);
    for (const auto& column : result.columns) {
        bytes += sizeof(std::string) + column.size();
    }
    bytes += result.rows.capacity() * sizeof(std::vector<Value>);
    for (const auto& row : result.rows) {
        bytes += row.capacity() * sizeof(Value);
        for (const auto& cell : row) {
            if (const auto* text = std::get_if<std::string>(&cell)) {
                bytes += text->capacity();
            } else if (const auto* blob = std::get_if<std::vector<uint8_t>>(&cell)) {
                bytes += blob->capacity();
            }
        }
    }
    return bytes;
}

}  // namespace

void FlatSQLDatabase::storeCachedQueryResultUnlocked(const std::string& key,
                                                     const QueryResult& result) {
    if (result.rows.size() > queryResultCacheMaxRows_) {
        return;
    }
    const size_t bytes = estimateQueryResultBytes(result);
    if (bytes > queryResultCacheMaxBytes_) {
        return;
    }

    auto existing = queryResultCache_.find(key);
    if (existing != queryResultCache_.end()) {
        queryResultCacheTotalBytes_ -= existing->second.bytes;
        existing->second.result = result;
        existing->second.bytes = bytes;
        queryResultCacheTotalBytes_ += bytes;
        queryResultCacheLru_.splice(
            queryResultCacheLru_.begin(),
            queryResultCacheLru_,
            existing->second.lruIt
        );
        existing->second.lruIt = queryResultCacheLru_.begin();
    } else {
        queryResultCacheLru_.push_front(key);
        queryResultCache_.emplace(key, CachedQueryResult{result, queryResultCacheLru_.begin(), bytes});
        queryResultCacheTotalBytes_ += bytes;
    }

    // Evict least-recently-used entries until both bounds hold. The entry
    // just stored is at the front and fits the budget by itself, so it stays.
    while (queryResultCache_.size() > queryResultCacheMaxEntries_ ||
           queryResultCacheTotalBytes_ > queryResultCacheMaxBytes_) {
        const std::string& evictedKey = queryResultCacheLru_.back();
        auto evicted = queryResultCache_.find(evictedKey);
        if (evicted != queryResultCache_.end()) {
            queryResultCacheTotalBytes_ -= evicted->second.bytes;
            queryResultCache_.erase(evicted);
        }
        queryResultCacheLru_.pop_back();
    }
}

std::string FlatSQLDatabase::buildTemplateCacheKeyUnlocked(const std::string& queryId,
                                                           const std::string& sql,
                                                           const std::vector<Value>& params) const {
    (void)sql;
    return buildQueryCacheKey(schema_.name, std::to_string(queryCacheGeneration_), queryId, params);
}

bool FlatSQLDatabase::queryRawFlatBufferStream(const std::string& sql,
                                               const std::vector<Value>& params,
                                               RawStreamResult* result,
                                               std::string* errorMessage) {
    std::unique_lock lock(*accessMutex_);
    if (reindexUnavailable_) {
        if (result) *result = {};
        if (errorMessage) *errorMessage = "state: reindex incomplete";
        return false;
    }
    if (!sqliteInitialized_) {
        initializeSQLiteEngine();
    }

    // Only read-only statements are cacheable; the raw-stream contract is
    // SELECT-shaped anyway (all cells BLOB). SQL is pre-validated by the C
    // API, so the prepare inside statementIsReadOnly cannot throw for
    // user-supplied errors.
    const bool cacheable = sqliteEngine_->statementIsReadOnly(sql);
    std::string cacheKey;
    if (cacheable) {
        cacheKey = buildQueryCacheKey(schema_.name,
                                      std::to_string(queryCacheGeneration_),
                                      "raw-stream:" + sql,
                                      params);
        auto cached = rawStreamCache_.find(cacheKey);
        if (cached != rawStreamCache_.end()) {
            rawStreamCacheHits_++;
            rawStreamCacheLru_.splice(
                rawStreamCacheLru_.begin(),
                rawStreamCacheLru_,
                cached->second.lruIt
            );
            cached->second.lruIt = rawStreamCacheLru_.begin();
            *result = cached->second.result;
            result->cacheHit = true;
            return true;
        }
        rawStreamCacheMisses_++;
    }

    QueryResult queryResult = sqliteEngine_->execute(sql, params);
    if (!cacheable) {
        // A raw-stream request that writes (e.g. RETURNING) must invalidate
        // like any other DML through the engine.
        invalidateQueryResultCacheUnlocked();
    }

    size_t totalBytes = 0;
    for (const auto& row : queryResult.rows) {
        for (const auto& value : row) {
            const auto* bytes = std::get_if<std::vector<uint8_t>>(&value);
            if (!bytes) {
                if (errorMessage) {
                    *errorMessage = "raw response stream queries must return only BLOB cells";
                }
                return false;
            }
            if (bytes->size() > std::numeric_limits<uint32_t>::max()) {
                if (errorMessage) {
                    *errorMessage = "raw response stream record exceeds size-prefix capacity";
                }
                return false;
            }
            totalBytes += 4 + bytes->size();
        }
    }

    auto stream = std::make_shared<std::vector<uint8_t>>();
    stream->reserve(totalBytes);
    for (const auto& row : queryResult.rows) {
        for (const auto& value : row) {
            const auto& bytes = std::get<std::vector<uint8_t>>(value);
            const uint32_t size = static_cast<uint32_t>(bytes.size());
            stream->push_back(static_cast<uint8_t>(size & 0xFF));
            stream->push_back(static_cast<uint8_t>((size >> 8) & 0xFF));
            stream->push_back(static_cast<uint8_t>((size >> 16) & 0xFF));
            stream->push_back(static_cast<uint8_t>((size >> 24) & 0xFF));
            stream->insert(stream->end(), bytes.begin(), bytes.end());
        }
    }

    result->stream = std::move(stream);
    result->rowCount = queryResult.rows.size();
    result->columnCount = queryResult.columns.size();
    result->cacheHit = false;

    if (cacheable) {
        storeRawStreamResultUnlocked(cacheKey, *result);
    }
    return true;
}

bool FlatSQLDatabase::querySandboxed(const std::string& sql,
                                     const std::vector<Value>& params,
                                     SQLiteEngine::SandboxMode mode,
                                     const SQLiteEngine::SandboxLimits& limits,
                                     SQLiteEngine::SandboxOutput* out,
                                     std::string* errorMessage) noexcept {
    try {
        std::unique_lock lock(*accessMutex_);
        if (reindexUnavailable_) {
            if (out) *out = {};
            if (errorMessage) *errorMessage = "state: reindex incomplete";
            return false;
        }
        if (!sqliteInitialized_) {
            initializeSQLiteEngine();
        }

        // The public query surface: record base tables (schema names),
        // per-source shadow tables ("OMM@celestrak-gp"), and the unified
        // views (which reuse the base table name). Control tables created
        // through plain SQL DDL are deliberately absent — the authorizer
        // denies reading them.
        std::unordered_set<std::string> allowed;
        for (const auto& tableDef : schema_.tables) {
            allowed.insert(tableDef.name);
        }
        for (const auto& entry : tables_) {
            allowed.insert(entry.first);
        }

        return sqliteEngine_->executeSandboxed(sql, params, allowed, mode, limits, out,
                                               errorMessage);
    } catch (...) {
        if (errorMessage) {
            try { *errorMessage = "sandbox: internal: sandboxed execution failed"; } catch (...) {}
        }
        return false;
    }
}

void FlatSQLDatabase::storeRawStreamResultUnlocked(const std::string& key,
                                                   const RawStreamResult& result) {
    const size_t streamBytes = result.stream ? result.stream->size() : 0;
    if (streamBytes > rawStreamCacheMaxTotalBytes_) {
        return;  // single stream over the byte budget — serve uncached
    }

    auto existing = rawStreamCache_.find(key);
    if (existing != rawStreamCache_.end()) {
        const size_t oldBytes =
            existing->second.result.stream ? existing->second.result.stream->size() : 0;
        rawStreamCacheTotalBytes_ = rawStreamCacheTotalBytes_ - oldBytes + streamBytes;
        existing->second.result = result;
        rawStreamCacheLru_.splice(
            rawStreamCacheLru_.begin(),
            rawStreamCacheLru_,
            existing->second.lruIt
        );
        existing->second.lruIt = rawStreamCacheLru_.begin();
    } else {
        rawStreamCacheLru_.push_front(key);
        rawStreamCache_.emplace(key, CachedRawStream{result, rawStreamCacheLru_.begin()});
        rawStreamCacheTotalBytes_ += streamBytes;
    }

    while (!rawStreamCacheLru_.empty() &&
           (rawStreamCache_.size() > rawStreamCacheMaxEntries_ ||
            rawStreamCacheTotalBytes_ > rawStreamCacheMaxTotalBytes_)) {
        const std::string& evictedKey = rawStreamCacheLru_.back();
        auto evicted = rawStreamCache_.find(evictedKey);
        if (evicted != rawStreamCache_.end()) {
            const size_t evictedBytes =
                evicted->second.result.stream ? evicted->second.result.stream->size() : 0;
            rawStreamCacheTotalBytes_ -= evictedBytes;
            rawStreamCache_.erase(evicted);
        }
        rawStreamCacheLru_.pop_back();
    }
}

void FlatSQLDatabase::configureRawStreamCache(size_t maxEntries, size_t maxTotalBytes) {
    std::unique_lock lock(*accessMutex_);
    rawStreamCacheMaxEntries_ = maxEntries;
    rawStreamCacheMaxTotalBytes_ = maxTotalBytes;
    rawStreamCache_.clear();
    rawStreamCacheLru_.clear();
    rawStreamCacheTotalBytes_ = 0;
}

FlatSQLDatabase::RawStreamCacheStats FlatSQLDatabase::getRawStreamCacheStats() const {
    std::shared_lock lock(*accessMutex_);
    return RawStreamCacheStats{
        rawStreamCacheHits_,
        rawStreamCacheMisses_,
        rawStreamCache_.size(),
        rawStreamCacheTotalBytes_,
        rawStreamCacheMaxEntries_,
        rawStreamCacheMaxTotalBytes_
    };
}

size_t FlatSQLDatabase::queryCount(const std::string& sql, const std::vector<Value>& params) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
    if (!sqliteInitialized_) initializeSQLiteEngine();
    return sqliteEngine_->executeAndCount(sql, params);
}

std::vector<StoredRecord> FlatSQLDatabase::findByIndex(const std::string& tableName,
                                                        const std::string& column,
                                                        const Value& value) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
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
    requireReadyUnlocked();
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
    requireReadyUnlocked();
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
    requireReadyUnlocked();
    if (!ensureSourceRegisteredUnlocked(sourceName)) {
        throw std::runtime_error("Source already registered: " + sourceName);
    }
}

// The registration itself, without the throw: durable-state restore reuses it
// (a boot may not raise), and registerSource() supplies the throw for callers
// that ask for a source twice.
bool FlatSQLDatabase::ensureSourceRegisteredUnlocked(const std::string& sourceName) {
    for (const auto& s : registeredSources_) {
        if (s == sourceName) return false;
    }

    invalidateQueryResultCacheUnlocked();
    registeredSources_.push_back(sourceName);

    // Create source-specific tables for each base table
    for (const auto& tableDef : schema_.tables) {
        createSourceTable(tableDef.name, sourceName);
    }
    return true;
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
            // The view now OWNS that name in the schema. Without this, a later
            // lazy initializeSQLiteEngine() would try to CREATE VIRTUAL TABLE
            // over a view and raise — a throw that aborts the no-exceptions
            // artifact outright. Registration order must not decide whether
            // the engine survives its first query.
            sqliteRegisteredTables_.insert(tableDef.name);
        }
    }
    invalidateQueryResultCacheUnlocked();
}

void FlatSQLDatabase::onIngestWithSource(std::string_view fileId, const uint8_t* data, size_t length,
                                          uint64_t sequence, uint64_t offset, const std::string& source,
                                          bool buildIndexes) {
    // Route to source-specific table
    std::string sourceKey = source + ":" + std::string(fileId);
    auto mapIt = sourceFileIdToTable_.find(sourceKey);
    if (mapIt == sourceFileIdToTable_.end()) {
        // Unknown source:fileId combination - skip
        return;
    }

    auto tableIt = tables_.find(mapIt->second);
    if (tableIt != tables_.end()) {
        if (buildIndexes) {
            tableIt->second->onIngest(data, length, sequence, offset);
        } else {
            tableIt->second->onIngestReplay(sequence, offset);
        }
    }
}

// A source's records are the frames it appended. Both entry points below note
// the arena span they produced, which is what makes the partition survivable
// without touching a single byte of the stream.
void FlatSQLDatabase::recordSourceRangeUnlocked(const std::string& source,
                                                 uint64_t start, uint64_t end) {
    if (end <= start) return;
    if (!sourceRanges_.empty()) {
        SourceRange& back = sourceRanges_.back();
        if (back.source == source && back.end == start) {
            back.end = end;  // consecutive batches from one source are ONE range
            return;
        }
    }
    sourceRanges_.push_back(SourceRange{start, end, source});
}

const std::string* FlatSQLDatabase::sourceForOffset(uint64_t offset, size_t* cursor) const {
    size_t i = cursor ? *cursor : 0;
    while (i < sourceRanges_.size() && sourceRanges_[i].end <= offset) i++;
    if (cursor) *cursor = i;
    if (i < sourceRanges_.size() && offset >= sourceRanges_[i].start) {
        return &sourceRanges_[i].source;
    }
    return nullptr;
}

size_t FlatSQLDatabase::ingestWithSource(const uint8_t* data, size_t length,
                                          const std::string& source,
                                          size_t* recordsIngested) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
    SQLiteWriteBatch batch(*sqliteEngine_);
    if (!batch.ok()) throw std::runtime_error(batch.error());
    invalidateQueryResultCacheUnlocked();
    IngestProfile* profile = ingestProfileEnabled_ ? &ingestProfile_ : nullptr;
    const uint64_t begin = storage_->getWriteOffset();
    reindexUnavailable_ = true;
    const size_t consumed = storage_->ingest(data, length,
        [this, &source](std::string_view fileId, const uint8_t* data, size_t len,
               uint64_t seq, uint64_t offset) {
            onIngestWithSource(fileId, data, len, seq, offset, source);
        }, recordsIngested, profile);
    recordSourceRangeUnlocked(source, begin, storage_->getWriteOffset());
    if (!batch.commit()) throw std::runtime_error(batch.error());
    reindexUnavailable_ = false;
    return consumed;
}

uint64_t FlatSQLDatabase::ingestOneWithSource(const uint8_t* flatbuffer, size_t length,
                                               const std::string& source) {
    std::unique_lock lock(*accessMutex_);
    requireReadyUnlocked();
    return ingestOneUnlocked(flatbuffer, length, &source);
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

namespace {

// HKDF-SHA256(key 00 01 .. 1f, no salt, "flatbuffers-buffer-v3" || BE32(1)),
// 32 bytes (RFC 5869; node:crypto hkdfSync gives the same).
constexpr uint8_t kBufferKeyVector[32] = {
    0xf0, 0x33, 0x7d, 0x19, 0x4c, 0x59, 0x63, 0xe2, 0x8a, 0xd4, 0xb2, 0xf4,
    0x7c, 0x75, 0x51, 0x9a, 0x73, 0xbc, 0xcb, 0x9e, 0x6d, 0x75, 0x64, 0xa1,
    0xd6, 0x6e, 0x4a, 0x9d, 0x5e, 0x64, 0xee, 0x01};

const char* const kNoRecordEncryption =
    "this build cannot encrypt (encrypted) columns: its FlatBuffers crypto backend does "
    "not derive keys with HKDF-SHA256 (format 3 needs it; build against FlatBuffers "
    "8af3053e or later, see README \"Database-key encryption\")";

const char* const kFormat2Refused =
    "field-encryption format 2 (records encrypted by flatsql 3.2.0 or earlier, all under "
    "the field keys of record 0) is not supported; migrate the database to format 3 "
    "(README \"Database-key encryption\")";

bool hasEncryptedColumn(const TableDef& table) {
    for (const auto& col : table.columns) {
        if (col.encrypted) return true;
    }
    return false;
}

// The binary schema's root table must mark (encrypted) exactly the fields the
// database schema does, field id by field id, or the fields encrypted are not
// the columns SQL decrypts. A .bfbs serialized without builtin attributes
// marks none.
bool checkBinarySchema(const TableDef& table, const uint8_t* schema, size_t schemaSize,
                       std::string* error) {
    if (!schema || schemaSize == 0) {
        *error = "the schema is not a valid binary schema (.bfbs)";
        return false;
    }
    flatbuffers::Verifier verifier(schema, schemaSize);
    if (!reflection::VerifySchemaBuffer(verifier)) {
        *error = "the schema is not a valid binary schema (.bfbs)";
        return false;
    }
    const reflection::Object* root = reflection::GetSchema(schema)->root_table();
    if (!root || !root->fields()) {
        *error = "the binary schema has no root_type";
        return false;
    }
    const auto* fields = root->fields();
    for (const auto& column : table.columns) {
        const reflection::Field* field = nullptr;
        for (const auto* candidate : *fields) {
            if (candidate && candidate->id() == column.fieldId) {
                field = candidate;
                break;
            }
        }
        if (!field) {
            if (column.encrypted) {
                *error = "column " + column.name + " is (encrypted), but the binary schema's "
                         "root table has no field " + std::to_string(column.fieldId);
                return false;
            }
            continue;
        }
        if (!field->name() || field->name()->str() != column.name) {
            *error = "field " + std::to_string(column.fieldId) + " of the binary schema's root "
                     "table is not column " + column.name + ": the schemas differ";
            return false;
        }
        if (flatbuffers::IsFieldEncrypted(field) != column.encrypted) {
            *error = column.encrypted
                ? "column " + column.name + " is (encrypted), but the binary schema does not "
                  "mark it: serialize the .bfbs with builtin attributes (flatc --bfbs-builtins)"
                : "the binary schema marks field " + column.name +
                  " (encrypted), the database schema does not";
            return false;
        }
    }
    for (const auto* field : *fields) {
        if (!field || !flatbuffers::IsFieldEncrypted(field)) continue;
        bool known = false;
        for (const auto& column : table.columns) {
            if (column.fieldId == field->id() && column.encrypted) known = true;
        }
        if (!known) {
            *error = "the binary schema marks field " +
                     (field->name() ? field->name()->str() : std::to_string(field->id())) +
                     " (encrypted), the database schema does not";
            return false;
        }
    }
    return true;
}

bool checkRecordIndex(uint64_t recordIndex, std::string* error) {
    if (recordIndex == 0) {
        *error = "record index 0: a record's index is its sequence (rowid), numbered from 1. " +
                 std::string(kFormat2Refused);
        return false;
    }
    if (recordIndex > UINT32_MAX) {
        *error = "record index " + std::to_string(recordIndex) +
                 ": field-encryption format 3 numbers records 1 to 4294967295";
        return false;
    }
    return true;
}

}  // namespace

bool FlatSQLDatabase::recordEncryptionAvailable() noexcept {
    static const bool available = [] {
        uint8_t key[flatbuffers::kEncryptionKeySize];
        for (size_t i = 0; i < sizeof(key); i++) key[i] = static_cast<uint8_t>(i);
        flatbuffers::EncryptionContext ctx(key, sizeof(key));
        uint8_t derived[flatbuffers::kEncryptionKeySize];
        ctx.DeriveBufferKey(1, derived);
        return std::memcmp(derived, kBufferKeyVector, sizeof(derived)) == 0;
    }();
    return available;
}

bool FlatSQLDatabase::setEncryptionKey(const uint8_t* key, size_t keySize,
                                       uint8_t storedFormat, std::string* error) noexcept {
    const auto fail = [&](std::string text) {
        if (error) *error = std::move(text);
        return false;
    };
    if (!key || keySize != flatbuffers::kEncryptionKeySize) {
        return fail("the encryption key must be 32 bytes");
    }
    if (storedFormat == flatbuffers::kFieldEncryptionV2) return fail(kFormat2Refused);
    if (storedFormat != kRecordFormatUndeclared &&
        storedFormat != flatbuffers::kFieldEncryptionV3) {
        return fail("unknown field-encryption format " + std::to_string(storedFormat));
    }

    std::unique_lock lock(*accessMutex_);
    if (hasEncryptedFields()) {
        if (!recordEncryptionAvailable()) return fail(kNoRecordEncryption);
        if (storedFormat == kRecordFormatUndeclared) {
            uint64_t stored = 0;
            for (const auto& [name, table] : tables_) {
                if (hasEncryptedColumn(table->getTableDef())) stored += table->getRecordCount();
            }
            if (stored > 0) {
                return fail("this database holds " + std::to_string(stored) +
                            " records in tables with (encrypted) columns: declare their "
                            "field-encryption format (3 for records written by "
                            "ingestOneEncrypted or encryptRecord). " + kFormat2Refused);
            }
        }
    }
    auto next = std::make_unique<flatbuffers::EncryptionContext>(key, keySize);
    recordKey_->ctx = next.get();
    encryptionCtx_ = std::move(next);
    invalidateQueryResultCacheUnlocked();
    return true;
}

void FlatSQLDatabase::setEncryptionKey(const uint8_t* key, size_t keySize) {
    std::string error;
    if (!setEncryptionKey(key, keySize, kRecordFormatUndeclared, &error)) {
        throw std::runtime_error(error);
    }
}

bool FlatSQLDatabase::hasEncryptedFields() const {
    for (const auto& table : schema_.tables) {
        if (hasEncryptedColumn(table)) return true;
    }
    return false;
}

const TableStore* FlatSQLDatabase::tableForRecordUnlocked(const uint8_t* record, size_t length,
                                                          const std::string& source) const {
    if (!record || length < FILE_IDENTIFIER_OFFSET + FILE_IDENTIFIER_LENGTH) return nullptr;
    const std::string fileId = StreamingFlatBufferStore::extractFileId(record, length);
    const auto& routes = source.empty() ? fileIdToTable_ : sourceFileIdToTable_;
    const auto route = routes.find(source.empty() ? fileId : source + ":" + fileId);
    if (route == routes.end()) return nullptr;
    const auto table = tables_.find(route->second);
    return table == tables_.end() ? nullptr : table->second.get();
}

bool FlatSQLDatabase::cipherRecordUnlocked(uint8_t* record, size_t length,
                                           const uint8_t* schema, size_t schemaSize,
                                           uint64_t recordIndex, bool encrypt,
                                           std::string* error) const {
    if (!encryptionCtx_) {
        *error = "No encryption key set";
        return false;
    }
    if (!recordEncryptionAvailable()) {
        *error = kNoRecordEncryption;
        return false;
    }
    if (!checkRecordIndex(recordIndex, error)) return false;
    const TableStore* table = tableForRecordUnlocked(record, length, "");
    if (!table) {
        *error = "the record's file identifier is not registered to a table";
        return false;
    }
    if (!checkBinarySchema(table->getTableDef(), schema, schemaSize, error)) return false;
    const auto result = encrypt
        ? flatbuffers::EncryptBuffer(record, length, schema, schemaSize, *encryptionCtx_,
                                     static_cast<uint32_t>(recordIndex),
                                     flatbuffers::kFieldEncryptionV3)
        : flatbuffers::DecryptBuffer(record, length, schema, schemaSize, *encryptionCtx_,
                                     static_cast<uint32_t>(recordIndex),
                                     flatbuffers::kFieldEncryptionV3);
    if (!result.ok()) {
        *error = result.message;
        return false;
    }
    return true;
}

bool FlatSQLDatabase::encryptRecord(uint8_t* record, size_t length,
                                    const uint8_t* schema, size_t schemaSize,
                                    uint64_t recordIndex, std::string* error) const noexcept {
    std::shared_lock lock(*accessMutex_);
    std::string message;
    if (cipherRecordUnlocked(record, length, schema, schemaSize, recordIndex, true, &message)) {
        return true;
    }
    if (error) *error = std::move(message);
    return false;
}

bool FlatSQLDatabase::decryptRecord(uint8_t* record, size_t length,
                                    const uint8_t* schema, size_t schemaSize,
                                    uint64_t recordIndex, std::string* error) const noexcept {
    std::shared_lock lock(*accessMutex_);
    std::string message;
    if (cipherRecordUnlocked(record, length, schema, schemaSize, recordIndex, false, &message)) {
        return true;
    }
    if (error) *error = std::move(message);
    return false;
}

uint64_t FlatSQLDatabase::ingestOneEncrypted(const uint8_t* flatbuffer, size_t length,
                                             const uint8_t* schema, size_t schemaSize,
                                             const std::string& source, std::string* error) {
    std::unique_lock lock(*accessMutex_);
    std::string message;
    const auto fail = [&](std::string text) {
        if (error) *error = std::move(text);
        return kIngestRefused;
    };
    if (reindexUnavailable_) return fail("state: reindex incomplete");
    if (!storage_->canAppend(SIZE_PREFIX_LENGTH + length)) {
        return fail("arena capacity exhausted");
    }
    if (!source.empty() &&
        std::find(registeredSources_.begin(), registeredSources_.end(), source) ==
            registeredSources_.end()) {
        return fail("Source not registered: " + source);
    }
    if (!tableForRecordUnlocked(flatbuffer, length, source)) {
        return fail("the record's file identifier is not registered to a table" +
                    (source.empty() ? std::string() : " of source " + source));
    }
    // The sequence this record gets is its record index: both are decided
    // here, under the lock, so no other record can take either.
    const uint64_t sequence = storage_->nextSequence();
    std::vector<uint8_t> record(flatbuffer, flatbuffer + length);
    if (!cipherRecordUnlocked(record.data(), record.size(), schema, schemaSize, sequence,
                              true, &message)) {
        return fail(std::move(message));
    }
    const uint64_t assigned =
        ingestOneUnlocked(record.data(), record.size(), source.empty() ? nullptr : &source);
    if (assigned == kIngestRefused) return fail("arena capacity exhausted");
    if (assigned != sequence) {
        return fail("record " + std::to_string(assigned) + " was encrypted as record " +
                    std::to_string(sequence));
    }
    return sequence;
}

// ==================== HMAC Authentication ====================

void FlatSQLDatabase::setHMACVerification(bool enabled) {
    if (enabled && !encryptionCtx_) {
        throw std::runtime_error("Cannot enable HMAC verification without an encryption key");
    }
    hmacEnabled_ = enabled;
}

// HMAC-SHA256 under the database key, from the FlatBuffers crypto backend
// (OpenSSL natively, the fallback backend in the wasm builds).
bool FlatSQLDatabase::computeHMAC(const uint8_t* buffer, size_t length, uint8_t* outMAC) const {
    if (!encryptionCtx_ || !outMAC || (!buffer && length > 0)) return false;
    flatbuffers::HMACSha256(encryptionCtx_->GetKey(), flatbuffers::kEncryptionKeySize,
                            buffer, length, outMAC);
    return true;
}

bool FlatSQLDatabase::verifyHMAC(const uint8_t* buffer, size_t length, const uint8_t* mac) const {
    if (!encryptionCtx_ || !mac || (!buffer && length > 0)) return false;
    // Constant-time comparison.
    return flatbuffers::HMACSha256Verify(encryptionCtx_->GetKey(), flatbuffers::kEncryptionKeySize,
                                         buffer, length, mac);
}

}  // namespace flatsql
