#include "flatsql/sqlite_vtab.h"
#include "flatbuffers/encryption.h"
#include <cstring>
#include <sstream>

namespace flatsql {

// Decrypt a column value in-place using the FlatBuffer field-level encryption scheme
static void decryptColumnValue(Value& value,
                                const flatbuffers::EncryptionContext& ctx,
                                uint16_t fieldId) {
    if (auto* s = std::get_if<std::string>(&value)) {
        if (!s->empty()) {
            flatbuffers::DecryptString(
                reinterpret_cast<uint8_t*>(s->data()),
                s->size(), ctx, fieldId);
        }
    } else if (auto* i64 = std::get_if<int64_t>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(i64), sizeof(int64_t), ctx, fieldId);
    } else if (auto* i32 = std::get_if<int32_t>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(i32), sizeof(int32_t), ctx, fieldId);
    } else if (auto* d = std::get_if<double>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(d), sizeof(double), ctx, fieldId);
    } else if (auto* f = std::get_if<float>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(f), sizeof(float), ctx, fieldId);
    } else if (auto* u64 = std::get_if<uint64_t>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(u64), sizeof(uint64_t), ctx, fieldId);
    } else if (auto* u32 = std::get_if<uint32_t>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(u32), sizeof(uint32_t), ctx, fieldId);
    } else if (auto* i16 = std::get_if<int16_t>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(i16), sizeof(int16_t), ctx, fieldId);
    } else if (auto* u16 = std::get_if<uint16_t>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(u16), sizeof(uint16_t), ctx, fieldId);
    } else if (auto* i8 = std::get_if<int8_t>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(i8), sizeof(int8_t), ctx, fieldId);
    } else if (auto* u8val = std::get_if<uint8_t>(&value)) {
        flatbuffers::DecryptScalar(
            reinterpret_cast<uint8_t*>(u8val), sizeof(uint8_t), ctx, fieldId);
    } else if (auto* blob = std::get_if<std::vector<uint8_t>>(&value)) {
        if (!blob->empty()) {
            uint8_t key[flatbuffers::kEncryptionKeySize];
            uint8_t iv[flatbuffers::kEncryptionIVSize];
            ctx.DeriveFieldKey(fieldId, key);
            ctx.DeriveFieldIV(fieldId, iv);
            flatbuffers::DecryptBytes(blob->data(), blob->size(), key, iv);
        }
    }
}

// Static module instance
sqlite3_module FlatBufferVTabModule::module_ = {
    0,                          // iVersion
    xCreate,                    // xCreate
    xConnect,                   // xConnect
    xBestIndex,                 // xBestIndex
    xDisconnect,                // xDisconnect
    xDestroy,                   // xDestroy
    xOpen,                      // xOpen
    xClose,                     // xClose
    xFilter,                    // xFilter
    xNext,                      // xNext
    xEof,                       // xEof
    xColumn,                    // xColumn
    xRowid,                     // xRowid
    nullptr,                    // xUpdate (read-only)
    nullptr,                    // xBegin
    nullptr,                    // xSync
    nullptr,                    // xCommit
    nullptr,                    // xRollback
    nullptr,                    // xFindFunction
    nullptr,                    // xRename
    nullptr,                    // xSavepoint
    nullptr,                    // xRelease
    nullptr,                    // xRollbackTo
    nullptr,                    // xShadowName
    nullptr                     // xIntegrity
};

sqlite3_module* FlatBufferVTabModule::getModule() {
    return &module_;
}

std::string FlatBufferVTabModule::valueTypeToSQLite(ValueType type) {
    switch (type) {
        case ValueType::Null:    return "NULL";
        case ValueType::Bool:    return "INTEGER";
        case ValueType::Int8:
        case ValueType::Int16:
        case ValueType::Int32:
        case ValueType::Int64:
        case ValueType::UInt8:
        case ValueType::UInt16:
        case ValueType::UInt32:
        case ValueType::UInt64:  return "INTEGER";
        case ValueType::Float32:
        case ValueType::Float64: return "REAL";
        case ValueType::String:  return "TEXT";
        case ValueType::Bytes:   return "BLOB";
        default:                 return "TEXT";
    }
}

std::string FlatBufferVTabModule::buildColumnDecl(const ColumnDef& col) {
    std::string decl = "\"" + col.name + "\" " + valueTypeToSQLite(col.type);
    if (!col.nullable) {
        decl += " NOT NULL";
    }
    return decl;
}

int FlatBufferVTabModule::xCreate(sqlite3* db, void* pAux, int argc, const char* const* argv,
                                   sqlite3_vtab** ppVTab, char** pzErr) {
    return xConnect(db, pAux, argc, argv, ppVTab, pzErr);
}

int FlatBufferVTabModule::xConnect(sqlite3* db, void* pAux, int argc, const char* const* argv,
                                    sqlite3_vtab** ppVTab, char** pzErr) {
    (void)argc;
    (void)argv;

    VTabCreateInfo* info = static_cast<VTabCreateInfo*>(pAux);
    if (!info || !info->tableDef) {
        if (pzErr) {
            *pzErr = sqlite3_mprintf("Missing table definition");
        }
        return SQLITE_ERROR;
    }

    // Build CREATE TABLE statement for schema declaration
    std::ostringstream sql;
    sql << "CREATE TABLE x(";

    const TableDef& tableDef = *info->tableDef;
    bool first = true;

    for (const auto& col : tableDef.columns) {
        if (!first) sql << ", ";
        first = false;
        sql << buildColumnDecl(col);
    }

    // Add virtual _source column
    sql << ", \"_source\" TEXT";

    // Add virtual metadata columns
    sql << ", \"_rowid\" INTEGER";
    sql << ", \"_offset\" INTEGER";
    sql << ", \"_data\" BLOB";

    sql << ")";

    int rc = sqlite3_declare_vtab(db, sql.str().c_str());
    if (rc != SQLITE_OK) {
        if (pzErr) {
            *pzErr = sqlite3_mprintf("Failed to declare vtab: %s", sqlite3_errmsg(db));
        }
        return rc;
    }

    // Allocate our custom vtab structure
    FlatBufferVTab* vtab = new FlatBufferVTab();
    memset(static_cast<sqlite3_vtab*>(vtab), 0, sizeof(sqlite3_vtab));

    vtab->store = info->store;
    vtab->tableDef = info->tableDef;
    vtab->sourceName = info->sourceName;
    vtab->fileId = info->fileId;
    vtab->extractor = info->extractor;
    vtab->fastExtractor = info->fastExtractor;
    vtab->indexes = info->indexes;
    vtab->tombstones = info->tombstones;
    vtab->sourceRecordInfos = info->sourceRecordInfos;
    vtab->encryptionCtx = info->encryptionCtx;
    vtab->sourceColumnIndex = static_cast<int>(tableDef.columns.size());  // _source is first virtual column

    *ppVTab = vtab;
    return SQLITE_OK;
}

int FlatBufferVTabModule::xDisconnect(sqlite3_vtab* pVTab) {
    FlatBufferVTab* vtab = static_cast<FlatBufferVTab*>(pVTab);
    delete vtab;
    return SQLITE_OK;
}

int FlatBufferVTabModule::xDestroy(sqlite3_vtab* pVTab) {
    return xDisconnect(pVTab);
}

int FlatBufferVTabModule::xBestIndex(sqlite3_vtab* pVTab, sqlite3_index_info* pIdxInfo) {
    FlatBufferVTab* vtab = static_cast<FlatBufferVTab*>(pVTab);

    // Analyze constraints to find best index strategy
    // idxNum encoding (high bits = column index, low bits = strategy):
    //   0 = full scan
    //   1 = rowid equality
    //   2 + (colIdx << 8) = index equality on column colIdx
    //   3 + (colIdx << 8) = index range on column colIdx

    int idxNum = 0;
    double estimatedCost = 1000000.0;  // Full scan cost
    int argvIndex = 1;
    int usableConstraints = 0;

    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        const auto& constraint = pIdxInfo->aConstraint[i];
        if (!constraint.usable) continue;

        int colIdx = constraint.iColumn;

        // Check for rowid lookup (column -1 is rowid)
        if (colIdx == -1 && constraint.op == SQLITE_INDEX_CONSTRAINT_EQ) {
            idxNum = 1;
            pIdxInfo->aConstraintUsage[i].argvIndex = argvIndex++;
            pIdxInfo->aConstraintUsage[i].omit = 1;
            estimatedCost = 1.0;
            usableConstraints++;
            continue;
        }

        // Skip virtual columns for index optimization
        if (colIdx >= static_cast<int>(vtab->tableDef->columns.size())) {
            // Check if filtering by _source
            if (colIdx == vtab->sourceColumnIndex && constraint.op == SQLITE_INDEX_CONSTRAINT_EQ) {
                // _source filter - we can use this to skip the table entirely
                // if it doesn't match our source name (handled at xFilter time)
                pIdxInfo->aConstraintUsage[i].argvIndex = argvIndex++;
                pIdxInfo->aConstraintUsage[i].omit = 1;
                usableConstraints++;
            }
            continue;
        }

        // Get column name
        const std::string& colName = vtab->tableDef->columns[colIdx].name;

        // Check if we have an index for this column
        auto indexIt = vtab->indexes.find(colName);
        if (indexIt != vtab->indexes.end() && indexIt->second != nullptr) {
            if (constraint.op == SQLITE_INDEX_CONSTRAINT_EQ) {
                // Encode column index in idxNum (strategy 2 + column << 8)
                idxNum = 2 + (colIdx << 8);
                pIdxInfo->aConstraintUsage[i].argvIndex = argvIndex++;
                pIdxInfo->aConstraintUsage[i].omit = 1;
                estimatedCost = 10.0;  // Index lookup cost
                usableConstraints++;
            } else if (constraint.op == SQLITE_INDEX_CONSTRAINT_GE ||
                       constraint.op == SQLITE_INDEX_CONSTRAINT_GT ||
                       constraint.op == SQLITE_INDEX_CONSTRAINT_LE ||
                       constraint.op == SQLITE_INDEX_CONSTRAINT_LT) {
                // Range query - encode column index in idxNum (strategy 3 + column << 8)
                if ((idxNum & 0xFF) < 2) {  // Don't override equality
                    idxNum = 3 + (colIdx << 8);
                }
                pIdxInfo->aConstraintUsage[i].argvIndex = argvIndex++;
                pIdxInfo->aConstraintUsage[i].omit = 0;  // Don't omit - SQLite will double-check
                estimatedCost = 100.0;  // Range scan cost
                usableConstraints++;
            }
        }
    }

    pIdxInfo->idxNum = idxNum;
    pIdxInfo->estimatedCost = estimatedCost;

    // If we have an index, indicate row count estimate
    int strategy = idxNum & 0xFF;
    if (vtab->store) {
        if (strategy == 0) {
            pIdxInfo->estimatedRows = vtab->store->getRecordCount();
        } else if (strategy == 1) {
            pIdxInfo->estimatedRows = 1;
        } else if (strategy == 2) {
            pIdxInfo->estimatedRows = 10;  // Estimate for equality lookup
        } else {
            pIdxInfo->estimatedRows = vtab->store->getRecordCount() / 10;  // Estimate for range
        }
    }

    return SQLITE_OK;
}

int FlatBufferVTabModule::xOpen(sqlite3_vtab* pVTab, sqlite3_vtab_cursor** ppCursor) {
    FlatBufferVTab* vtab = static_cast<FlatBufferVTab*>(pVTab);

    FlatBufferCursor* cursor = new FlatBufferCursor();
    memset(static_cast<sqlite3_vtab_cursor*>(cursor), 0, sizeof(sqlite3_vtab_cursor));

    cursor->vtab = vtab;
    cursor->currentOffset = 0;
    cursor->currentSequence = 0;
    cursor->currentData = nullptr;
    cursor->currentLength = 0;
    cursor->atEof = true;
    cursor->scanType = ScanType::FullScan;
    cursor->indexPosition = 0;
    cursor->scanPosition = 0;
    cursor->cacheValid = false;
    cursor->numRealColumns = static_cast<int>(vtab->tableDef->columns.size());

    // Pre-allocate column cache
    cursor->columnCache.resize(cursor->numRealColumns);

    // Cache the fast extractor to avoid vtab pointer chase in hot path
    cursor->cachedFastExtractor = vtab->fastExtractor;

    *ppCursor = cursor;
    return SQLITE_OK;
}

int FlatBufferVTabModule::xClose(sqlite3_vtab_cursor* pCursor) {
    FlatBufferCursor* cursor = static_cast<FlatBufferCursor*>(pCursor);
    delete cursor;
    return SQLITE_OK;
}

Value FlatBufferVTabModule::valueFromSqlite(sqlite3_value* val) {
    switch (sqlite3_value_type(val)) {
        case SQLITE_INTEGER:
            return static_cast<int64_t>(sqlite3_value_int64(val));
        case SQLITE_FLOAT:
            return sqlite3_value_double(val);
        case SQLITE_TEXT: {
            const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
            return std::string(text ? text : "");
        }
        case SQLITE_BLOB: {
            const uint8_t* blob = static_cast<const uint8_t*>(sqlite3_value_blob(val));
            int len = sqlite3_value_bytes(val);
            return std::vector<uint8_t>(blob, blob + len);
        }
        case SQLITE_NULL:
        default:
            return std::monostate{};
    }
}

int FlatBufferVTabModule::xFilter(sqlite3_vtab_cursor* pCursor, int idxNum, const char* idxStr,
                                   int argc, sqlite3_value** argv) {
    (void)idxStr;  // No longer used - column index encoded in idxNum
    FlatBufferCursor* cursor = static_cast<FlatBufferCursor*>(pCursor);
    FlatBufferVTab* vtab = cursor->vtab;

    // Reset cursor state
    cursor->atEof = false;
    cursor->indexResults.clear();
    cursor->indexPosition = 0;
    cursor->scanRefs.clear();
    cursor->scanPosition = 0;
    cursor->currentData = nullptr;
    cursor->currentLength = 0;
    cursor->cacheValid = false;

    if (!vtab->store) {
        cursor->atEof = true;
        return SQLITE_OK;
    }

    int argIdx = 0;

    // Decode idxNum: low byte = strategy, high bytes = column index
    int strategy = idxNum & 0xFF;
    int colIdx = idxNum >> 8;

    switch (strategy) {
        case 0: {
            // Full scan - use indexed iteration with cached vector and buffer pointers
            cursor->scanType = ScanType::FullScan;
            cursor->useLazyScan = false;
            cursor->scanFileIndex = 0;
            // Prefer source-specific record infos if available (for multi-source routing)
            if (vtab->sourceRecordInfos) {
                cursor->scanRecordInfos = vtab->sourceRecordInfos;
            } else {
                cursor->scanRecordInfos = vtab->store->getRecordInfoVector(vtab->fileId);
            }
            cursor->scanFileCount = cursor->scanRecordInfos ? cursor->scanRecordInfos->size() : 0;
            cursor->scanDataBuffer = vtab->store->getDataBuffer();
            cursor->hasTombstones = vtab->tombstones && !vtab->tombstones->empty();

            // Find first non-tombstoned record
            while (cursor->scanFileIndex < cursor->scanFileCount) {
                const auto& info = (*cursor->scanRecordInfos)[cursor->scanFileIndex];
                // Check tombstone
                if (!vtab->tombstones || !vtab->tombstones->count(info.sequence)) {
                    // Inline data access - read size prefix and compute pointer
                    const uint8_t* ptr = cursor->scanDataBuffer + info.offset;
                    uint32_t len = static_cast<uint32_t>(ptr[0]) |
                                   (static_cast<uint32_t>(ptr[1]) << 8) |
                                   (static_cast<uint32_t>(ptr[2]) << 16) |
                                   (static_cast<uint32_t>(ptr[3]) << 24);
                    cursor->currentOffset = info.offset;
                    cursor->currentSequence = info.sequence;
                    cursor->currentData = ptr + 4;  // Skip size prefix
                    cursor->currentLength = len;
                    break;
                }
                cursor->scanFileIndex++;
            }

            if (cursor->scanFileIndex >= cursor->scanFileCount) {
                cursor->atEof = true;
            }
            break;
        }

        case 1: {
            // Rowid lookup - direct pointer, no copy
            cursor->scanType = ScanType::RowidLookup;
            if (argc < 1) {
                cursor->atEof = true;
                return SQLITE_OK;
            }

            int64_t rowid = sqlite3_value_int64(argv[argIdx]);

            // Check tombstone
            if (vtab->tombstones && vtab->tombstones->count(static_cast<uint64_t>(rowid))) {
                cursor->atEof = true;
                return SQLITE_OK;
            }

            // Look up by sequence
            auto offsetOpt = vtab->store->getOffsetForSequence(static_cast<uint64_t>(rowid));
            if (!offsetOpt.has_value()) {
                cursor->atEof = true;
            } else {
                uint32_t len = 0;
                const uint8_t* data = vtab->store->getDataAtOffset(offsetOpt.value(), &len);
                if (data) {
                    cursor->currentOffset = offsetOpt.value();
                    cursor->currentSequence = static_cast<uint64_t>(rowid);
                    cursor->currentData = data;
                    cursor->currentLength = len;
                } else {
                    cursor->atEof = true;
                }
            }
            break;
        }

        case 2: {
            // Index equality lookup - optimized fast path
            if (argc < 1 || colIdx < 0 || colIdx >= static_cast<int>(vtab->tableDef->columns.size())) {
                cursor->atEof = true;
                return SQLITE_OK;
            }

            // Get column name and look up index directly using column index
            const std::string& colName = vtab->tableDef->columns[colIdx].name;
            auto indexIt = vtab->indexes.find(colName);
            if (indexIt == vtab->indexes.end() || !indexIt->second) {
                cursor->atEof = true;
                return SQLITE_OK;
            }

            // Convert SQLite value to our Value type
            Value searchValue = valueFromSqlite(argv[argIdx]);

            // Check if this is a primary key column (unique index)
            bool isPrimaryKey = vtab->tableDef->columns[colIdx].primaryKey;

            // For primary key (unique) columns, use fast single-result path
            // For non-unique indexed columns, must use search() to get all matches
            if (isPrimaryKey && indexIt->second->searchFirst(searchValue, cursor->singleResult)) {
                // Fast path for primary key: single result expected
                if (!vtab->tombstones || !vtab->tombstones->count(cursor->singleResult.sequence)) {
                    cursor->scanType = ScanType::IndexSingleLookup;
                    cursor->singleResultReturned = false;

                    uint32_t len = 0;
                    const uint8_t* data = vtab->store->getDataAtOffset(cursor->singleResult.dataOffset, &len);
                    if (data) {
                        cursor->currentOffset = cursor->singleResult.dataOffset;
                        cursor->currentSequence = cursor->singleResult.sequence;
                        cursor->currentData = data;
                        cursor->currentLength = len;
                    } else {
                        cursor->atEof = true;
                    }
                } else {
                    // Tombstoned primary key - no match
                    cursor->atEof = true;
                }
            } else {
                // Non-unique index OR primary key with tombstone: search for all matches
                cursor->scanType = ScanType::IndexEquality;
                cursor->indexResults = indexIt->second->search(searchValue);

                // Filter out tombstoned entries if needed
                if (vtab->tombstones && !vtab->tombstones->empty()) {
                    std::vector<IndexEntry> filtered;
                    for (const auto& entry : cursor->indexResults) {
                        if (!vtab->tombstones->count(entry.sequence)) {
                            filtered.push_back(entry);
                        }
                    }
                    cursor->indexResults = std::move(filtered);
                }

                cursor->indexPosition = 0;
                if (cursor->indexResults.empty()) {
                    cursor->atEof = true;
                } else {
                    const IndexEntry& entry = cursor->indexResults[0];
                    uint32_t len = 0;
                    const uint8_t* data = vtab->store->getDataAtOffset(entry.dataOffset, &len);
                    if (data) {
                        cursor->currentOffset = entry.dataOffset;
                        cursor->currentSequence = entry.sequence;
                        cursor->currentData = data;
                        cursor->currentLength = len;
                    } else {
                        cursor->atEof = true;
                    }
                }
            }
            break;
        }

        case 3: {
            // Index range query
            cursor->scanType = ScanType::IndexRange;
            if (colIdx < 0 || colIdx >= static_cast<int>(vtab->tableDef->columns.size())) {
                cursor->atEof = true;
                return SQLITE_OK;
            }

            const std::string& colName = vtab->tableDef->columns[colIdx].name;
            cursor->constraintColumn = colName;

            auto indexIt = vtab->indexes.find(colName);
            if (indexIt == vtab->indexes.end() || !indexIt->second) {
                cursor->atEof = true;
                return SQLITE_OK;
            }

            cursor->indexResults = indexIt->second->all();

            // Filter out tombstoned entries
            if (vtab->tombstones) {
                std::vector<IndexEntry> filtered;
                filtered.reserve(cursor->indexResults.size());
                for (const auto& entry : cursor->indexResults) {
                    if (!vtab->tombstones->count(entry.sequence)) {
                        filtered.push_back(entry);
                    }
                }
                cursor->indexResults = std::move(filtered);
            }

            cursor->indexPosition = 0;
            if (cursor->indexResults.empty()) {
                cursor->atEof = true;
            } else {
                const IndexEntry& entry = cursor->indexResults[0];
                uint32_t len = 0;
                const uint8_t* data = vtab->store->getDataAtOffset(entry.dataOffset, &len);
                if (data) {
                    cursor->currentOffset = entry.dataOffset;
                    cursor->currentSequence = entry.sequence;
                    cursor->currentData = data;
                    cursor->currentLength = len;
                } else {
                    cursor->atEof = true;
                }
            }
            break;
        }

        default:
            cursor->atEof = true;
            break;
    }

    return SQLITE_OK;
}

int FlatBufferVTabModule::xNext(sqlite3_vtab_cursor* pCursor) {
    FlatBufferCursor* cursor = static_cast<FlatBufferCursor*>(pCursor);

    // Invalidate column cache on row change
    cursor->cacheValid = false;

    switch (cursor->scanType) {
        case ScanType::FullScan: {
            // Use indexed iteration with inlined buffer access
            cursor->scanFileIndex++;

            // Fast path: no tombstones (common case, cached check)
            if (__builtin_expect(!cursor->hasTombstones, 1)) {
                if (__builtin_expect(cursor->scanFileIndex < cursor->scanFileCount, 1)) {
                    const auto& info = (*cursor->scanRecordInfos)[cursor->scanFileIndex];
                    const uint8_t* ptr = cursor->scanDataBuffer + info.offset;
                    uint32_t len = static_cast<uint32_t>(ptr[0]) |
                                   (static_cast<uint32_t>(ptr[1]) << 8) |
                                   (static_cast<uint32_t>(ptr[2]) << 16) |
                                   (static_cast<uint32_t>(ptr[3]) << 24);
                    cursor->currentOffset = info.offset;
                    cursor->currentSequence = info.sequence;
                    cursor->currentData = ptr + 4;
                    cursor->currentLength = len;
                    return SQLITE_OK;
                }
                cursor->atEof = true;
                return SQLITE_OK;
            }

            // Slow path: has tombstones, need to check each record
            while (cursor->scanFileIndex < cursor->scanFileCount) {
                const auto& info = (*cursor->scanRecordInfos)[cursor->scanFileIndex];
                if (!cursor->vtab->tombstones->count(info.sequence)) {
                    const uint8_t* ptr = cursor->scanDataBuffer + info.offset;
                    uint32_t len = static_cast<uint32_t>(ptr[0]) |
                                   (static_cast<uint32_t>(ptr[1]) << 8) |
                                   (static_cast<uint32_t>(ptr[2]) << 16) |
                                   (static_cast<uint32_t>(ptr[3]) << 24);
                    cursor->currentOffset = info.offset;
                    cursor->currentSequence = info.sequence;
                    cursor->currentData = ptr + 4;
                    cursor->currentLength = len;
                    return SQLITE_OK;
                }
                cursor->scanFileIndex++;
            }

            cursor->atEof = true;
            break;
        }

        case ScanType::RowidLookup:
        case ScanType::IndexSingleLookup:
            // Only one result for these lookups
            cursor->atEof = true;
            break;

        case ScanType::IndexEquality:
        case ScanType::IndexRange: {
            cursor->indexPosition++;
            if (cursor->indexPosition >= cursor->indexResults.size()) {
                cursor->atEof = true;
            } else {
                const IndexEntry& entry = cursor->indexResults[cursor->indexPosition];
                uint32_t len = 0;
                const uint8_t* data = cursor->vtab->store->getDataAtOffset(entry.dataOffset, &len);
                if (data) {
                    cursor->currentOffset = entry.dataOffset;
                    cursor->currentSequence = entry.sequence;
                    cursor->currentData = data;
                    cursor->currentLength = len;
                } else {
                    cursor->atEof = true;
                }
            }
            break;
        }
    }

    return SQLITE_OK;
}

int FlatBufferVTabModule::xEof(sqlite3_vtab_cursor* pCursor) {
    FlatBufferCursor* cursor = static_cast<FlatBufferCursor*>(pCursor);
    return cursor->atEof ? 1 : 0;
}

void FlatBufferVTabModule::setResultFromValue(sqlite3_context* ctx, const Value& value) {
    // Optimized result setting - the Value contains copied data that lives in cursor cache,
    // so we must use SQLITE_TRANSIENT for strings/blobs
    switch (value.index()) {
        case 0:  // monostate (null)
            sqlite3_result_null(ctx);
            break;
        case 1:  // bool
            sqlite3_result_int(ctx, std::get<bool>(value) ? 1 : 0);
            break;
        case 2:  // int8_t
            sqlite3_result_int(ctx, std::get<int8_t>(value));
            break;
        case 3:  // int16_t
            sqlite3_result_int(ctx, std::get<int16_t>(value));
            break;
        case 4:  // int32_t
            sqlite3_result_int(ctx, std::get<int32_t>(value));
            break;
        case 5:  // int64_t
            sqlite3_result_int64(ctx, std::get<int64_t>(value));
            break;
        case 6:  // uint8_t
            sqlite3_result_int(ctx, std::get<uint8_t>(value));
            break;
        case 7:  // uint16_t
            sqlite3_result_int(ctx, std::get<uint16_t>(value));
            break;
        case 8:  // uint32_t
            sqlite3_result_int(ctx, static_cast<int>(std::get<uint32_t>(value)));
            break;
        case 9:  // uint64_t
            sqlite3_result_int64(ctx, static_cast<sqlite3_int64>(std::get<uint64_t>(value)));
            break;
        case 10: // float
            sqlite3_result_double(ctx, std::get<float>(value));
            break;
        case 11: // double
            sqlite3_result_double(ctx, std::get<double>(value));
            break;
        case 12: { // string
            const std::string& s = std::get<std::string>(value);
            sqlite3_result_text(ctx, s.c_str(), static_cast<int>(s.size()), SQLITE_TRANSIENT);
            break;
        }
        case 13: { // vector<uint8_t>
            const auto& v = std::get<std::vector<uint8_t>>(value);
            sqlite3_result_blob(ctx, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
            break;
        }
        default:
            sqlite3_result_null(ctx);
            break;
    }
}

int FlatBufferVTabModule::xColumn(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int N) {
    FlatBufferCursor* cursor = static_cast<FlatBufferCursor*>(pCursor);

    // Fast path: regular column with fast extractor (most common case)
    // Skip fast path when encryption is active - must go through cache for decryption
    if (N >= 0 && N < cursor->numRealColumns && cursor->currentData
        && cursor->cachedFastExtractor && !cursor->vtab->encryptionCtx) {
        if (cursor->cachedFastExtractor(cursor->currentData, cursor->currentLength, N, ctx)) {
            return SQLITE_OK;
        }
    }

    FlatBufferVTab* vtab = cursor->vtab;
    int numRealColumns = cursor->numRealColumns;

    // Slow path: virtual columns or fallback extraction
    if (N == vtab->sourceColumnIndex) {
        sqlite3_result_text(ctx, vtab->sourceName.c_str(),
                           static_cast<int>(vtab->sourceName.size()), SQLITE_STATIC);
        return SQLITE_OK;
    }
    if (N == numRealColumns + 1) {
        sqlite3_result_int64(ctx, static_cast<sqlite3_int64>(cursor->currentSequence));
        return SQLITE_OK;
    }
    if (N == numRealColumns + 2) {
        sqlite3_result_int64(ctx, static_cast<sqlite3_int64>(cursor->currentOffset));
        return SQLITE_OK;
    }
    if (N == numRealColumns + 3) {
        if (cursor->currentData && cursor->currentLength > 0) {
            sqlite3_result_blob(ctx, cursor->currentData, cursor->currentLength, SQLITE_TRANSIENT);
        } else {
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    if (N < 0 || N >= numRealColumns || !cursor->currentData) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    // Fallback: regular extractor with caching
    if (!vtab->extractor) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    if (!cursor->cacheValid) {
        for (int i = 0; i < numRealColumns; i++) {
            cursor->columnCache[i] = vtab->extractor(cursor->currentData, cursor->currentLength,
                                                      vtab->tableDef->columns[i].name);
        }

        // Decrypt encrypted columns if encryption context is present
        if (vtab->encryptionCtx) {
            for (int i = 0; i < numRealColumns; i++) {
                if (vtab->tableDef->columns[i].encrypted) {
                    decryptColumnValue(cursor->columnCache[i],
                                       *vtab->encryptionCtx,
                                       vtab->tableDef->columns[i].fieldId);
                }
            }
        }

        cursor->cacheValid = true;
    }

    setResultFromValue(ctx, cursor->columnCache[N]);
    return SQLITE_OK;
}

int FlatBufferVTabModule::xRowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid) {
    FlatBufferCursor* cursor = static_cast<FlatBufferCursor*>(pCursor);
    *pRowid = static_cast<sqlite3_int64>(cursor->currentSequence);
    return SQLITE_OK;
}

}  // namespace flatsql
