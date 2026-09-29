#include "flatsql/sqlite_vtab.h"
#include "flatbuffers/encryption.h"
#include <algorithm>
#include <cstring>
#include <sstream>

namespace flatsql {

namespace {

size_t scalarWidth(ValueType type) {
    switch (type) {
        case ValueType::Bool:
        case ValueType::Int8:
        case ValueType::UInt8:   return 1;
        case ValueType::Int16:
        case ValueType::UInt16:  return 2;
        case ValueType::Int32:
        case ValueType::UInt32:
        case ValueType::Float32: return 4;
        case ValueType::Int64:
        case ValueType::UInt64:
        case ValueType::Float64: return 8;
        default:                 return 0;
    }
}

// The [start, start + length) bytes a column's value occupies in one record,
// found the way the generic extractor finds them: the root table's vtable
// slot `fieldId`, then the scalar in place or the string/vector it refers to.
// False when the field is absent or does not fit in the record.
bool columnInstance(const uint8_t* data, size_t size, const ColumnDef& column,
                    uint64_t* start, uint64_t* length) {
    using flatbuffers::ReadScalar;
    if (size < sizeof(uint32_t)) return false;
    const uint64_t root = ReadScalar<uint32_t>(data);
    if (root > size || size - root < sizeof(int32_t)) return false;
    const int64_t vtable = static_cast<int64_t>(root) - ReadScalar<int32_t>(data + root);
    if (vtable < 0 || static_cast<uint64_t>(vtable) > size ||
        size - static_cast<uint64_t>(vtable) < 2 * sizeof(uint16_t)) {
        return false;
    }
    const uint64_t vt = static_cast<uint64_t>(vtable);
    const uint16_t vtableSize = ReadScalar<uint16_t>(data + vt);
    const uint64_t slot = 4 + 2 * static_cast<uint64_t>(column.fieldId);
    if (slot + sizeof(uint16_t) > vtableSize || vt + vtableSize > size) return false;
    const uint16_t fieldOffset = ReadScalar<uint16_t>(data + vt + slot);
    if (fieldOffset == 0) return false;
    const uint64_t loc = root + fieldOffset;

    if (const size_t width = scalarWidth(column.type)) {
        if (loc > size || size - loc < width) return false;
        *start = loc;
        *length = width;
        return true;
    }
    if (column.type != ValueType::String && column.type != ValueType::Bytes) {
        return false;  // opaque (a table, struct or non-byte vector): read as NULL
    }
    if (loc > size || size - loc < sizeof(uint32_t)) return false;
    const uint64_t target = loc + ReadScalar<uint32_t>(data + loc);
    if (target > size || size - target < sizeof(uint32_t)) return false;
    const uint64_t count = ReadScalar<uint32_t>(data + target);  // bytes: 1-byte elements
    if (count > size - target - sizeof(uint32_t)) return false;
    *start = target + sizeof(uint32_t);
    *length = count;
    return true;
}

// Decrypts, in `record` (a copy of one stored record, without its size
// prefix), the (encrypted) columns of the table under FlatBuffers field
// encryption format 3 (flatbuffers/encryption.h): the record's key is
// DeriveBufferKey(record index) and each instance's IV is
// FieldInstanceIV(offset of its first byte in the record). The record index
// is the record's sequence (rowid). An instance two columns share (a shared
// string) is decrypted once, as format 3 encrypted it once. A column that is
// absent, or does not fit in the record, is left alone: it reads as NULL.
bool decryptRecordColumns(std::vector<uint8_t>& record, const TableDef& table,
                          const flatbuffers::EncryptionContext& ctx,
                          uint64_t sequence, std::string* error) {
    if (sequence == 0 || sequence > UINT32_MAX) {
        *error = "record " + std::to_string(sequence) +
                 " has no field-encryption record index (records 1 to 4294967295 have one)";
        return false;
    }
    uint8_t key[flatbuffers::kEncryptionKeySize];
    bool derived = false;
    std::vector<uint64_t> done;
    for (const auto& column : table.columns) {
        if (!column.encrypted) continue;
        uint64_t start = 0;
        uint64_t length = 0;
        if (!columnInstance(record.data(), record.size(), column, &start, &length) ||
            length == 0 || std::find(done.begin(), done.end(), start) != done.end()) {
            continue;
        }
        done.push_back(start);
        if (!derived) {
            ctx.DeriveBufferKey(static_cast<uint32_t>(sequence), key);
            derived = true;
        }
        uint8_t iv[flatbuffers::kEncryptionIVSize];
        flatbuffers::FieldInstanceIV(static_cast<uint32_t>(start), iv);
        flatbuffers::DecryptBytes(record.data() + start, static_cast<size_t>(length), key, iv);
    }
    if (derived) {
        volatile uint8_t* wipe = key;
        for (size_t i = 0; i < sizeof(key); i++) wipe[i] = 0;
    }
    return true;
}

// The key a table's records are decrypted with: the database's record key
// when the table has (encrypted) columns, else none.
const flatbuffers::EncryptionContext* recordKeyOf(const FlatBufferVTab* vtab) {
    if (!vtab->hasEncryptedColumns || !vtab->createInfo || !vtab->createInfo->recordKey) {
        return nullptr;
    }
    return vtab->createInfo->recordKey->ctx;
}

}  // namespace

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
    vtab->createInfo = info;
    vtab->hasEncryptedColumns = std::any_of(
        tableDef.columns.begin(), tableDef.columns.end(),
        [](const ColumnDef& col) { return col.encrypted; });
    vtab->stats = info->stats;
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

// ---------------------------------------------------------------------------
// Plan encoding shared by xBestIndex and xFilter.
//
//   idxNum bits 0-3  strategy: 0 full scan, 1 rowid =, 2 index =, 3 index range
//   idxNum bits 4-7  range flags: lower present, lower inclusive,
//                                 upper present, upper inclusive
//   idxNum bits 8+   column index of the index used (strategies 2 and 3)
//
// argv carries exactly the values of the constraints the plan uses, in this
// order: the rowid or equality key; or the lower bound then the upper bound.
// Every other constraint keeps argvIndex 0 and is evaluated by SQLite, so a
// WHERE clause with several indexed terms is always answered correctly: the
// index narrows, SQLite filters.
// ---------------------------------------------------------------------------
namespace {

constexpr int kStrategyScan = 0;
constexpr int kStrategyRowid = 1;
constexpr int kStrategyEquality = 2;
constexpr int kStrategyRange = 3;

constexpr int kRangeLower = 1 << 4;
constexpr int kRangeLowerInclusive = 1 << 5;
constexpr int kRangeUpper = 1 << 6;
constexpr int kRangeUpperInclusive = 1 << 7;

bool isRangeOp(unsigned char op) {
    return op == SQLITE_INDEX_CONSTRAINT_GT || op == SQLITE_INDEX_CONSTRAINT_GE ||
           op == SQLITE_INDEX_CONSTRAINT_LT || op == SQLITE_INDEX_CONSTRAINT_LE;
}

bool isLowerOp(unsigned char op) {
    return op == SQLITE_INDEX_CONSTRAINT_GT || op == SQLITE_INDEX_CONSTRAINT_GE;
}

// The index table compares keys with BINARY collation. A constraint under any
// other collation (e.g. `COLLATE NOCASE`) must not be answered from it.
bool usesBinaryCollation(sqlite3_index_info* info, int constraint) {
    const char* coll = sqlite3_vtab_collation(info, constraint);
    return coll == nullptr || sqlite3_stricmp(coll, "BINARY") == 0;
}

uint64_t tableRowCount(const FlatBufferVTab* vtab) {
    if (vtab->sourceRecordInfos) {
        return vtab->sourceRecordInfos->size();
    }
    if (vtab->store) {
        const auto* infos = vtab->store->getRecordInfoVector(vtab->fileId);
        if (infos) return infos->size();
    }
    return 0;
}

double log2Rows(uint64_t rows) {
    double cost = 1.0;
    while (rows > 1) {
        rows >>= 1;
        cost += 1.0;
    }
    return cost;
}

}  // namespace

int FlatBufferVTabModule::xBestIndex(sqlite3_vtab* pVTab, sqlite3_index_info* pIdxInfo) {
    FlatBufferVTab* vtab = static_cast<FlatBufferVTab*>(pVTab);
    const int realColumns = static_cast<int>(vtab->tableDef->columns.size());

    auto indexedColumn = [&](int colIdx) -> bool {
        if (colIdx < 0 || colIdx >= realColumns) return false;
        auto it = vtab->indexes.find(vtab->tableDef->columns[colIdx].name);
        return it != vtab->indexes.end() && it->second != nullptr;
    };

    // Candidate plans. Constraints on the virtual columns (_source, _rowid,
    // _offset, _data) are never claimed: SQLite evaluates them against
    // xColumn, which is always correct (claiming _source with omit=1 once
    // returned rows from every source partition).
    int rowidEq = -1;
    int pkEq = -1;
    int anyEq = -1;
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        const auto& c = pIdxInfo->aConstraint[i];
        if (!c.usable || c.op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
        if (c.iColumn == -1) {
            if (rowidEq < 0) rowidEq = i;
            continue;
        }
        if (!indexedColumn(c.iColumn) || !usesBinaryCollation(pIdxInfo, i)) continue;
        if (vtab->tableDef->columns[c.iColumn].primaryKey) {
            if (pkEq < 0) pkEq = i;
        } else if (anyEq < 0) {
            anyEq = i;
        }
    }

    // Best range: prefer a column bounded on both sides.
    int rangeCol = -1;
    int rangeLower = -1;
    int rangeUpper = -1;
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        const auto& c = pIdxInfo->aConstraint[i];
        if (!c.usable || !isRangeOp(c.op) || !indexedColumn(c.iColumn) ||
            !usesBinaryCollation(pIdxInfo, i)) {
            continue;
        }
        int lower = -1;
        int upper = -1;
        for (int j = 0; j < pIdxInfo->nConstraint; j++) {
            const auto& d = pIdxInfo->aConstraint[j];
            if (!d.usable || d.iColumn != c.iColumn || !isRangeOp(d.op) ||
                !usesBinaryCollation(pIdxInfo, j)) {
                continue;
            }
            if (isLowerOp(d.op)) {
                if (lower < 0) lower = j;
            } else if (upper < 0) {
                upper = j;
            }
        }
        const int bounds = (lower >= 0) + (upper >= 0);
        const int bestBounds = rangeCol < 0 ? 0 : (rangeLower >= 0) + (rangeUpper >= 0);
        if (bounds > bestBounds) {
            rangeCol = c.iColumn;
            rangeLower = lower;
            rangeUpper = upper;
        }
    }

    const uint64_t rows = tableRowCount(vtab);
    const double logRows = log2Rows(rows);
    // Costs are structural, not data-driven: a statement is prepared once and
    // cached, possibly while the table is still empty, so the ranking of
    // plans must not flip with the row count. Rows feed join ordering only.
    int idxNum = kStrategyScan;
    double estimatedCost = 1000000.0;
    sqlite3_int64 estimatedRows = static_cast<sqlite3_int64>(rows > 0 ? rows : 1);
    int planColumn = -1;
    bool planIsInList = false;
    char* idxStr = nullptr;

    auto claim = [&](int constraint, int argvIndex, bool omit) {
        pIdxInfo->aConstraintUsage[constraint].argvIndex = argvIndex;
        pIdxInfo->aConstraintUsage[constraint].omit = omit ? 1 : 0;
    };

    if (rowidEq >= 0) {
        idxNum = kStrategyRowid;
        claim(rowidEq, 1, true);
        estimatedCost = 1.0;
        estimatedRows = 1;
        pIdxInfo->idxFlags |= SQLITE_INDEX_SCAN_UNIQUE;
        idxStr = sqlite3_mprintf("rowid");
    } else if (pkEq >= 0 || anyEq >= 0) {
        const int chosen = pkEq >= 0 ? pkEq : anyEq;
        planColumn = pIdxInfo->aConstraint[chosen].iColumn;
        idxNum = kStrategyEquality | (planColumn << 8);
        // Equality through the index uses SQLite's comparison on the bound
        // value, exactly what the row-by-row check would do: omit it.
        claim(chosen, 1, true);
        // `col IN (...)` arrives as an equality that SQLite answers with one
        // xFilter per list value; its order across values is SQLite's, so an
        // ORDER BY is not ours to claim. sqlite3_vtab_in() only reports on
        // the first 32 constraints, so beyond that assume the worst.
        planIsInList = chosen >= 32 || sqlite3_vtab_in(pIdxInfo, chosen, -1) != 0;
        estimatedCost = 2.0 + logRows;
        estimatedRows = pkEq >= 0 ? 1 : 10;
        idxStr = sqlite3_mprintf("eq:%s", vtab->tableDef->columns[planColumn].name.c_str());
    } else if (rangeCol >= 0) {
        planColumn = rangeCol;
        int flags = 0;
        int argv = 1;
        const char* lowerOp = "";
        const char* upperOp = "";
        if (rangeLower >= 0) {
            flags |= kRangeLower;
            const bool inclusive =
                pIdxInfo->aConstraint[rangeLower].op == SQLITE_INDEX_CONSTRAINT_GE;
            if (inclusive) flags |= kRangeLowerInclusive;
            lowerOp = inclusive ? ">=" : ">";
            // Range bounds narrow the walk; SQLite re-checks every row.
            claim(rangeLower, argv++, false);
        }
        if (rangeUpper >= 0) {
            flags |= kRangeUpper;
            const bool inclusive =
                pIdxInfo->aConstraint[rangeUpper].op == SQLITE_INDEX_CONSTRAINT_LE;
            if (inclusive) flags |= kRangeUpperInclusive;
            upperOp = inclusive ? "<=" : "<";
            claim(rangeUpper, argv++, false);
        }
        idxNum = kStrategyRange | flags | (planColumn << 8);
        const bool bothBounds = rangeLower >= 0 && rangeUpper >= 0;
        estimatedRows = static_cast<sqlite3_int64>(std::max<uint64_t>(1, rows / (bothBounds ? 16 : 4)));
        estimatedCost = bothBounds ? 100.0 + logRows : 1000.0 + logRows;
        idxStr = sqlite3_mprintf("range:%s:%s:%s",
                                 vtab->tableDef->columns[planColumn].name.c_str(),
                                 lowerOp, upperOp);
    } else {
        idxStr = sqlite3_mprintf("scan");
    }

    // The index yields (key, sequence) order. Under an equality every key is
    // the same, so any ORDER BY on that column alone is already satisfied;
    // under a range, a single ascending ORDER BY on the column is.
    if (planColumn >= 0 && !planIsInList && pIdxInfo->nOrderBy > 0) {
        bool consumed = true;
        for (int i = 0; i < pIdxInfo->nOrderBy; i++) {
            if (pIdxInfo->aOrderBy[i].iColumn != planColumn) consumed = false;
        }
        if ((idxNum & 0x0F) == kStrategyRange &&
            (pIdxInfo->nOrderBy != 1 || pIdxInfo->aOrderBy[0].desc)) {
            consumed = false;
        }
        if (consumed) pIdxInfo->orderByConsumed = 1;
    }

    pIdxInfo->idxNum = idxNum;
    pIdxInfo->estimatedCost = estimatedCost;
    pIdxInfo->estimatedRows = estimatedRows;
    if (idxStr) {
        pIdxInfo->idxStr = idxStr;
        pIdxInfo->needToFreeIdxStr = 1;
    }
    return SQLITE_OK;
}

namespace {

void foldCursorStats(FlatBufferCursor* cursor) {
    if (cursor->vtab && cursor->vtab->stats) {
        cursor->vtab->stats->rowsVisited += cursor->rowsVisited;
        cursor->vtab->stats->indexEntriesRead += cursor->indexEntriesRead;
    }
    cursor->rowsVisited = 0;
    cursor->indexEntriesRead = 0;
}

// Whether the row with this sequence is one of this table's rows. A
// partition ("OMM@celestrak") holds only the rows routed to it, but it shares
// its index tables (and the sequence space) with every other partition of the
// same base table, so an index entry or a rowid can name another partition's
// row — or, by rowid, another table's. A base table that is still a virtual
// table reads every frame of its file identifier.
bool tableHoldsRow(const FlatBufferVTab* vtab, uint64_t sequence, const uint8_t* data, uint32_t length) {
    if (vtab->sourceRecordInfos) {
        const auto& infos = *vtab->sourceRecordInfos;  // ascending by sequence
        const auto it = std::lower_bound(
            infos.begin(), infos.end(), sequence,
            [](const StreamingFlatBufferStore::FileRecordInfo& info, uint64_t seq) {
                return info.sequence < seq;
            });
        return it != infos.end() && it->sequence == sequence;
    }
    if (vtab->fileId.size() != FILE_IDENTIFIER_LENGTH) return true;
    return length >= FILE_IDENTIFIER_OFFSET + FILE_IDENTIFIER_LENGTH &&
           std::memcmp(data + FILE_IDENTIFIER_OFFSET, vtab->fileId.data(), FILE_IDENTIFIER_LENGTH) == 0;
}

// Position the cursor on the next live index entry, or at EOF.
int advanceIndexScan(FlatBufferCursor* cursor) {
    FlatBufferVTab* vtab = cursor->vtab;
    uint64_t offset = 0;
    uint64_t sequence = 0;
    uint32_t indexedLength = 0;
    for (;;) {
        const int step = cursor->indexScan.next(offset, indexedLength, sequence);
        if (step == 0) {
            cursor->atEof = true;
            return SQLITE_OK;
        }
        if (step < 0) {
            cursor->atEof = true;
            sqlite3_free(vtab->zErrMsg);
            vtab->zErrMsg = sqlite3_mprintf("FlatSQL index scan failed");
            cursor->indexScan.release();
            return SQLITE_ERROR;
        }
        cursor->indexEntriesRead++;
        if (cursor->hasTombstones && vtab->tombstones->count(sequence)) {
            continue;
        }
        uint32_t len = 0;
        const uint8_t* data = vtab->store->getDataAtOffset(offset, &len);
        if (!data || !tableHoldsRow(vtab, sequence, data, len)) {
            continue;
        }
        cursor->currentOffset = offset;
        cursor->currentSequence = sequence;
        cursor->currentData = data;
        cursor->currentLength = len;
        cursor->rowsVisited++;
        return SQLITE_OK;
    }
}

}  // namespace

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
    cursor->scanFileIndex = 0;
    cursor->scanFileCount = 0;
    cursor->scanRecordInfos = nullptr;
    cursor->scanDataBuffer = nullptr;
    cursor->rowsVisited = 0;
    cursor->indexEntriesRead = 0;
    cursor->cacheValid = false;
    cursor->hasTombstones = false;
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
    cursor->indexScan.release();
    foldCursorStats(cursor);
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
    (void)idxStr;  // Diagnostic only (EXPLAIN QUERY PLAN); the plan is idxNum
    FlatBufferCursor* cursor = static_cast<FlatBufferCursor*>(pCursor);
    FlatBufferVTab* vtab = cursor->vtab;

    // Reset cursor state
    cursor->indexScan.release();
    foldCursorStats(cursor);
    cursor->atEof = false;
    cursor->currentData = nullptr;
    cursor->currentLength = 0;
    cursor->cacheValid = false;
    cursor->hasTombstones = vtab->tombstones && !vtab->tombstones->empty();

    if (!vtab->store) {
        cursor->atEof = true;
        return SQLITE_OK;
    }

    const int strategy = idxNum & 0x0F;
    const int flags = idxNum & 0xF0;
    const int colIdx = idxNum >> 8;
    VTabScanStats* stats = vtab->stats;

    switch (strategy) {
        case kStrategyScan: {
            if (stats) stats->fullScans++;
            cursor->scanType = ScanType::FullScan;
            cursor->scanFileIndex = 0;
            // Prefer source-specific record infos if available (for multi-source routing)
            if (vtab->sourceRecordInfos) {
                cursor->scanRecordInfos = vtab->sourceRecordInfos;
            } else {
                cursor->scanRecordInfos = vtab->store->getRecordInfoVector(vtab->fileId);
            }
            cursor->scanFileCount = cursor->scanRecordInfos ? cursor->scanRecordInfos->size() : 0;
            cursor->scanDataBuffer = vtab->store->getDataBuffer();

            // Find first non-tombstoned record
            while (cursor->scanFileIndex < cursor->scanFileCount) {
                const auto& info = (*cursor->scanRecordInfos)[cursor->scanFileIndex];
                if (!cursor->hasTombstones || !vtab->tombstones->count(info.sequence)) {
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
                    cursor->rowsVisited++;
                    break;
                }
                cursor->scanFileIndex++;
            }

            if (cursor->scanFileIndex >= cursor->scanFileCount) {
                cursor->atEof = true;
            }
            return SQLITE_OK;
        }

        case kStrategyRowid: {
            if (stats) stats->rowidLookups++;
            cursor->scanType = ScanType::RowidLookup;
            if (argc < 1) {
                cursor->atEof = true;
                return SQLITE_OK;
            }

            int64_t rowid = sqlite3_value_int64(argv[0]);

            if (cursor->hasTombstones && vtab->tombstones->count(static_cast<uint64_t>(rowid))) {
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
                if (data && tableHoldsRow(vtab, static_cast<uint64_t>(rowid), data, len)) {
                    cursor->currentOffset = offsetOpt.value();
                    cursor->currentSequence = static_cast<uint64_t>(rowid);
                    cursor->currentData = data;
                    cursor->currentLength = len;
                    cursor->rowsVisited++;
                } else {
                    cursor->atEof = true;
                }
            }
            return SQLITE_OK;
        }

        case kStrategyEquality:
        case kStrategyRange: {
            if (colIdx < 0 || colIdx >= static_cast<int>(vtab->tableDef->columns.size())) {
                cursor->atEof = true;
                return SQLITE_OK;
            }
            auto indexIt = vtab->indexes.find(vtab->tableDef->columns[colIdx].name);
            if (indexIt == vtab->indexes.end() || !indexIt->second) {
                cursor->atEof = true;
                return SQLITE_OK;
            }
            SqliteIndex* index = indexIt->second;

            bool opened = false;
            if (strategy == kStrategyEquality) {
                if (stats) stats->indexEqualityScans++;
                cursor->scanType = ScanType::IndexEquality;
                if (argc < 1) {
                    cursor->atEof = true;
                    return SQLITE_OK;
                }
                opened = index->openEqual(argv[0], cursor->indexScan);
            } else {
                if (stats) stats->indexRangeScans++;
                cursor->scanType = ScanType::IndexRange;
                int arg = 0;
                sqlite3_value* lower = nullptr;
                sqlite3_value* upper = nullptr;
                if (flags & kRangeLower) {
                    lower = arg < argc ? argv[arg] : nullptr;
                    arg++;
                }
                if (flags & kRangeUpper) {
                    upper = arg < argc ? argv[arg] : nullptr;
                    arg++;
                }
                if (arg > argc) {
                    cursor->atEof = true;
                    return SQLITE_OK;
                }
                opened = index->openRange(lower, (flags & kRangeLowerInclusive) != 0,
                                          upper, (flags & kRangeUpperInclusive) != 0,
                                          cursor->indexScan);
            }
            if (!opened) {
                cursor->atEof = true;
                sqlite3_free(vtab->zErrMsg);
                vtab->zErrMsg = sqlite3_mprintf("FlatSQL index scan could not start: %s",
                                                index->lastError());
                return SQLITE_ERROR;
            }
            if (!cursor->indexScan.active()) {
                cursor->atEof = true;  // NULL key or bound: nothing can match
                return SQLITE_OK;
            }
            return advanceIndexScan(cursor);
        }

        default:
            cursor->atEof = true;
            return SQLITE_OK;
    }
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
                    cursor->rowsVisited++;
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
                    cursor->rowsVisited++;
                    return SQLITE_OK;
                }
                cursor->scanFileIndex++;
            }

            cursor->atEof = true;
            break;
        }

        case ScanType::RowidLookup:
            // Only one result for these lookups
            cursor->atEof = true;
            break;

        case ScanType::IndexEquality:
        case ScanType::IndexRange:
            return advanceIndexScan(cursor);
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

    // (encrypted) columns are read from a decrypted copy of the record
    const flatbuffers::EncryptionContext* recordKey = recordKeyOf(cursor->vtab);

    // Fast path: regular column with fast extractor (most common case)
    if (N >= 0 && N < cursor->numRealColumns && cursor->currentData
        && cursor->cachedFastExtractor && !recordKey) {
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
        const uint8_t* data = cursor->currentData;
        if (recordKey) {
            cursor->plainRecord.assign(data, data + cursor->currentLength);
            std::string error;
            if (!decryptRecordColumns(cursor->plainRecord, *vtab->tableDef, *recordKey,
                                      cursor->currentSequence, &error)) {
                sqlite3_result_error(ctx, error.c_str(), static_cast<int>(error.size()));
                return SQLITE_ERROR;
            }
            data = cursor->plainRecord.data();
        }
        for (int i = 0; i < numRealColumns; i++) {
            cursor->columnCache[i] = vtab->extractor(data, cursor->currentLength,
                                                      vtab->tableDef->columns[i].name);
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
