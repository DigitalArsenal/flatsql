#include "flatsql/database.h"
#include <algorithm>
#include <stdexcept>

namespace flatsql {

// ==================== TableStore ====================

TableStore::TableStore(const TableDef& tableDef, StackedFlatBufferStore& storage)
    : tableDef_(tableDef), storage_(storage) {

    // Create indexes for indexed columns
    for (const auto& col : tableDef_.columns) {
        if (col.indexed || col.primaryKey) {
            indexes_[col.name] = std::make_unique<BTree>(col.type);
        }
    }
}

uint64_t TableStore::insert(const std::vector<uint8_t>& flatbufferData) {
    uint64_t offset = storage_.append(tableDef_.name, flatbufferData);
    uint64_t sequence = recordCount_++;

    updateIndexes(flatbufferData, offset, sequence);

    return nextRowId_++;
}

void TableStore::updateIndexes(const std::vector<uint8_t>& data, uint64_t offset, uint64_t sequence) {
    if (!fieldExtractor_) return;

    for (auto& [colName, btree] : indexes_) {
        Value key = fieldExtractor_(data, colName);
        btree->insert(key, offset, static_cast<uint32_t>(data.size()), sequence);
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
                Value fieldValue = fieldExtractor_(record.data, column);
                if (compareValues(fieldValue, value) == 0) {
                    results.push_back(std::move(record));
                }
            }
        }
        return results;
    }

    auto entries = it->second->search(value);
    for (const auto& entry : entries) {
        results.push_back(storage_.readRecord(entry.dataOffset));
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
                Value fieldValue = fieldExtractor_(record.data, column);
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
        results.push_back(storage_.readRecord(entry.dataOffset));
    }

    return results;
}

std::vector<StoredRecord> TableStore::scanAll() {
    std::vector<StoredRecord> results;

    storage_.iterateTableRecords(tableDef_.name, [&](const StoredRecord& record) {
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
    : schema_(schema), storage_(schema.name) {

    // Initialize table stores
    for (const auto& tableDef : schema_.tables) {
        tables_[tableDef.name] = std::make_unique<TableStore>(tableDef, storage_);
    }
}

FlatSQLDatabase FlatSQLDatabase::fromSchema(const std::string& source, const std::string& dbName) {
    DatabaseSchema schema = SchemaParser::parse(source, dbName);
    return FlatSQLDatabase(schema);
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

    if (parsed.where.has_value()) {
        const WhereCondition& cond = parsed.where.value();

        if (cond.op == "=" || cond.op == "==") {
            records = tableStore.findByIndex(cond.column, cond.value);
        } else if (cond.hasBetween) {
            records = tableStore.findByRange(cond.column, cond.value, cond.value2);
        } else {
            // Full scan with filter
            records = tableStore.scanAll();
            records.erase(std::remove_if(records.begin(), records.end(),
                [this, &cond, &tableStore](const StoredRecord& record) {
                    // Get field value - need a way to extract
                    // For now, just keep all records if no extractor
                    return false;
                }), records.end());
        }
    } else {
        records = tableStore.scanAll();
    }

    // Build result rows
    // Note: Without field extractor we can't populate field values
    // This would normally be done via FlatBuffer reflection or generated code
    for (const auto& record : records) {
        std::vector<Value> row;
        for (const auto& colName : result.columns) {
            if (colName == "_rowid") {
                row.push_back(static_cast<int64_t>(record.header.sequence));
            } else if (colName == "_offset") {
                row.push_back(static_cast<int64_t>(record.offset));
            } else {
                // Would need field extractor to get actual values
                row.push_back(std::monostate{});  // null placeholder
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

uint64_t FlatSQLDatabase::insertRaw(const std::string& tableName, const std::vector<uint8_t>& flatbufferData) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        throw std::runtime_error("Table not found: " + tableName);
    }

    return it->second->insert(flatbufferData);
}

std::vector<uint64_t> FlatSQLDatabase::stream(const std::string& tableName,
                                               const std::vector<std::vector<uint8_t>>& flatbuffers) {
    std::vector<uint64_t> rowids;
    rowids.reserve(flatbuffers.size());

    for (const auto& fb : flatbuffers) {
        rowids.push_back(insertRaw(tableName, fb));
    }

    return rowids;
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
        ts.recordCount = store->getRecordCount();
        ts.indexes = store->getIndexNames();
        stats.push_back(ts);
    }
    return stats;
}

}  // namespace flatsql
