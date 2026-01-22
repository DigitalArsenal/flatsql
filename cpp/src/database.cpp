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

    auto entries = it->second->search(value);
    for (const auto& entry : entries) {
        results.push_back(storage_.readRecordAtOffset(entry.dataOffset));
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

QueryResult FlatSQLDatabase::query(const std::string& sql) {
    ParsedSQL parsed = SQLParser::parse(sql);

    switch (parsed.type) {
        case SQLStatementType::Select:
            return executeSelect(parsed);
        default:
            throw std::runtime_error("Unsupported query type");
    }
}

QueryResult FlatSQLDatabase::executeSelect(const ParsedSQL& parsed) {
    QueryResult result;

    auto it = tables_.find(parsed.tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + parsed.tableName);
    }

    TableStore& tableStore = *it->second;
    const TableDef& tableDef = tableStore.getTableDef();

    // Determine columns
    if (parsed.columns.size() == 1 && parsed.columns[0] == "*") {
        for (const auto& col : tableDef.columns) {
            result.columns.push_back(col.name);
        }
    } else {
        result.columns = parsed.columns;
    }

    // Get records
    std::vector<StoredRecord> records;
    auto extractor = tableStore.getFieldExtractor();
    bool needsFilter = false;

    if (parsed.where.has_value()) {
        const WhereCondition& cond = parsed.where.value();

        if (cond.op == "=" || cond.op == "==") {
            records = tableStore.findByIndex(cond.column, cond.value);
        } else if (cond.hasBetween) {
            records = tableStore.findByRange(cond.column, cond.value, cond.value2);
        } else {
            // Full scan with filter for <, >, <=, >=, !=
            records = tableStore.scanAll();
            needsFilter = true;
        }
    } else {
        records = tableStore.scanAll();
    }

    // Build result rows using field extractor
    for (const auto& record : records) {
        // Apply WHERE filter for non-index operators
        if (needsFilter && parsed.where.has_value() && extractor) {
            const WhereCondition& cond = parsed.where.value();
            Value fieldValue = extractor(record.data.data(), record.data.size(), cond.column);
            if (!evaluateCondition(fieldValue, cond)) {
                continue;  // Skip this record
            }
        }

        std::vector<Value> row;
        for (const auto& colName : result.columns) {
            if (colName == "_rowid") {
                row.push_back(static_cast<int64_t>(record.header.sequence));
            } else if (colName == "_offset") {
                row.push_back(static_cast<int64_t>(record.offset));
            } else if (colName == "_data") {
                // Return raw FlatBuffer data
                row.push_back(record.data);
            } else if (extractor) {
                // Use field extractor to get actual value
                row.push_back(extractor(record.data.data(), record.data.size(), colName));
            } else {
                row.push_back(std::monostate{});  // null if no extractor
            }
        }
        result.rows.push_back(std::move(row));
    }

    // Apply LIMIT
    if (parsed.limit.has_value() && result.rows.size() > static_cast<size_t>(parsed.limit.value())) {
        result.rows.resize(parsed.limit.value());
    }

    return result;
}

bool FlatSQLDatabase::evaluateCondition(const Value& fieldValue, const WhereCondition& cond) {
    int cmp = compareValues(fieldValue, cond.value);

    if (cond.op == "=" || cond.op == "==") return cmp == 0;
    if (cond.op == "!=" || cond.op == "<>") return cmp != 0;
    if (cond.op == "<") return cmp < 0;
    if (cond.op == ">") return cmp > 0;
    if (cond.op == "<=") return cmp <= 0;
    if (cond.op == ">=") return cmp >= 0;

    return false;
}

void FlatSQLDatabase::setFieldExtractor(const std::string& tableName, TableStore::FieldExtractor extractor) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    it->second->setFieldExtractor(extractor);
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

}  // namespace flatsql
