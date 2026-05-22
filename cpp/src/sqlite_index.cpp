#include "flatsql/sqlite_index.h"
#include <chrono>
#include <stdexcept>
#include <cstring>
#include <algorithm>
#include <thread>

namespace flatsql {

static bool isBusyResult(int rc) {
    return rc == SQLITE_BUSY || rc == SQLITE_LOCKED;
}

static int stepWithRetry(sqlite3* db, sqlite3_stmt* stmt) {
    int rc = SQLITE_OK;
    int delayMs = 1;

    for (int attempt = 0; attempt <= 8; attempt++) {
        rc = sqlite3_step(stmt);
        if (!isBusyResult(rc)) {
            return rc;
        }
        if (attempt == 8) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        delayMs = std::min(delayMs * 2, 32);
    }

    throw std::runtime_error("SQLite index busy/locked after retries: " + std::string(sqlite3_errmsg(db)));
}

// Helper to convert Value to int64 for comparison (optimized with get_if)
// Order by frequency: int32_t most common in FlatBuffers, then int64_t
static bool tryGetInt64(const Value& v, int64_t& out) {
    // Fast path for common types using get_if (faster than visit)
    if (auto* p = std::get_if<int32_t>(&v)) { out = *p; return true; }  // Most common
    if (auto* p = std::get_if<int64_t>(&v)) { out = *p; return true; }
    if (auto* p = std::get_if<uint32_t>(&v)) { out = *p; return true; }
    if (auto* p = std::get_if<uint64_t>(&v)) { out = static_cast<int64_t>(*p); return true; }
    if (auto* p = std::get_if<int16_t>(&v)) { out = *p; return true; }
    if (auto* p = std::get_if<uint16_t>(&v)) { out = *p; return true; }
    if (auto* p = std::get_if<int8_t>(&v)) { out = *p; return true; }
    if (auto* p = std::get_if<uint8_t>(&v)) { out = *p; return true; }
    return false;
}

// Helper to convert Value to double for comparison
static bool tryGetDouble(const Value& v, double& out) {
    return std::visit([&out](const auto& val) -> bool {
        using T = std::decay_t<decltype(val)>;
        if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
            out = static_cast<double>(val);
            return true;
        } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                            std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                            std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                            std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>) {
            out = static_cast<double>(val);
            return true;
        }
        return false;
    }, v);
}

// Compare two Values with numeric type coercion
int compareValues(const Value& a, const Value& b) {
    // Handle null comparisons
    if (std::holds_alternative<std::monostate>(a)) {
        return std::holds_alternative<std::monostate>(b) ? 0 : -1;
    }
    if (std::holds_alternative<std::monostate>(b)) {
        return 1;
    }

    // Try integer comparison first (handles int32 vs int64 etc.)
    int64_t aInt, bInt;
    if (tryGetInt64(a, aInt) && tryGetInt64(b, bInt)) {
        if (aInt < bInt) return -1;
        if (aInt > bInt) return 1;
        return 0;
    }

    // Try floating point comparison
    double aDouble, bDouble;
    if (tryGetDouble(a, aDouble) && tryGetDouble(b, bDouble)) {
        if (aDouble < bDouble) return -1;
        if (aDouble > bDouble) return 1;
        return 0;
    }

    // String comparison
    if (std::holds_alternative<std::string>(a) && std::holds_alternative<std::string>(b)) {
        return std::get<std::string>(a).compare(std::get<std::string>(b));
    }

    // Blob comparison
    if (std::holds_alternative<std::vector<uint8_t>>(a) && std::holds_alternative<std::vector<uint8_t>>(b)) {
        const auto& aVec = std::get<std::vector<uint8_t>>(a);
        const auto& bVec = std::get<std::vector<uint8_t>>(b);
        size_t minLen = std::min(aVec.size(), bVec.size());
        for (size_t i = 0; i < minLen; i++) {
            if (aVec[i] < bVec[i]) return -1;
            if (aVec[i] > bVec[i]) return 1;
        }
        if (aVec.size() < bVec.size()) return -1;
        if (aVec.size() > bVec.size()) return 1;
        return 0;
    }

    // Bool comparison
    if (std::holds_alternative<bool>(a) && std::holds_alternative<bool>(b)) {
        bool aVal = std::get<bool>(a);
        bool bVal = std::get<bool>(b);
        return aVal == bVal ? 0 : (aVal < bVal ? -1 : 1);
    }

    // Different incompatible types - compare by type index
    return a.index() < b.index() ? -1 : 1;
}

SqliteIndex::SqliteIndex(sqlite3* db, const std::string& tableName,
                         const std::string& columnName, ValueType keyType)
    : db_(db), keyType_(keyType) {

    // Create unique index table name: _idx_{table}_{column}
    indexTableName_ = "_idx_" + tableName + "_" + columnName;

    // Create the index table with appropriate type
    // Use (key, sequence) as composite primary key to support non-unique indexes
    // This allows multiple records with the same key (e.g., posts by same user_id)
    std::string sqlType = getSqliteType();
    std::string createSql =
        "CREATE TABLE IF NOT EXISTS \"" + indexTableName_ + "\" ("
        "key " + sqlType + " NOT NULL, "
        "data_offset INTEGER NOT NULL, "
        "data_length INTEGER NOT NULL, "
        "sequence INTEGER NOT NULL, "
        "PRIMARY KEY (key, sequence)"
        ") WITHOUT ROWID";

    char* errMsg = nullptr;
    int rc = sqlite3_exec(db_, createSql.c_str(), nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        std::string err = errMsg ? errMsg : "Unknown error";
        sqlite3_free(errMsg);
        throw std::runtime_error("Failed to create index table: " + err);
    }

    // Prepare statements
    std::string insertSql = "INSERT INTO \"" + indexTableName_ +
        "\" (key, data_offset, data_length, sequence) VALUES (?, ?, ?, ?)";
    rc = sqlite3_prepare_v2(db_, insertSql.c_str(), -1, &insertStmt_, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare insert statement");
    }

    std::string searchSql = "SELECT key, data_offset, data_length, sequence FROM \"" +
        indexTableName_ + "\" WHERE key = ?";
    rc = sqlite3_prepare_v2(db_, searchSql.c_str(), -1, &searchStmt_, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare search statement");
    }

    // searchFirst returns just the first match (with LIMIT 1 for efficiency)
    std::string searchFirstSql = "SELECT key, data_offset, data_length, sequence FROM \"" +
        indexTableName_ + "\" WHERE key = ? LIMIT 1";
    rc = sqlite3_prepare_v2(db_, searchFirstSql.c_str(), -1, &searchFirstStmt_, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare searchFirst statement");
    }

    std::string rangeSql = "SELECT key, data_offset, data_length, sequence FROM \"" +
        indexTableName_ + "\" WHERE key >= ? AND key <= ? ORDER BY key";
    rc = sqlite3_prepare_v2(db_, rangeSql.c_str(), -1, &rangeStmt_, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare range statement");
    }

    std::string allSql = "SELECT key, data_offset, data_length, sequence FROM \"" +
        indexTableName_ + "\" ORDER BY key";
    rc = sqlite3_prepare_v2(db_, allSql.c_str(), -1, &allStmt_, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare all statement");
    }

    std::string countSql = "SELECT COUNT(*) FROM \"" + indexTableName_ + "\"";
    rc = sqlite3_prepare_v2(db_, countSql.c_str(), -1, &countStmt_, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare count statement");
    }

    std::string clearSql = "DELETE FROM \"" + indexTableName_ + "\"";
    rc = sqlite3_prepare_v2(db_, clearSql.c_str(), -1, &clearStmt_, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare clear statement");
    }
}

SqliteIndex::~SqliteIndex() {
    if (insertStmt_) sqlite3_finalize(insertStmt_);
    if (searchStmt_) sqlite3_finalize(searchStmt_);
    if (searchFirstStmt_) sqlite3_finalize(searchFirstStmt_);
    if (rangeStmt_) sqlite3_finalize(rangeStmt_);
    if (allStmt_) sqlite3_finalize(allStmt_);
    if (countStmt_) sqlite3_finalize(countStmt_);
    if (clearStmt_) sqlite3_finalize(clearStmt_);
}

SqliteIndex::SqliteIndex(SqliteIndex&& other) noexcept
    : db_(other.db_)
    , indexTableName_(std::move(other.indexTableName_))
    , keyType_(other.keyType_)
    , entryCount_(other.entryCount_)
    , insertStmt_(other.insertStmt_)
    , searchStmt_(other.searchStmt_)
    , searchFirstStmt_(other.searchFirstStmt_)
    , rangeStmt_(other.rangeStmt_)
    , allStmt_(other.allStmt_)
    , countStmt_(other.countStmt_)
    , clearStmt_(other.clearStmt_)
{
    other.db_ = nullptr;
    other.insertStmt_ = nullptr;
    other.searchStmt_ = nullptr;
    other.searchFirstStmt_ = nullptr;
    other.rangeStmt_ = nullptr;
    other.allStmt_ = nullptr;
    other.countStmt_ = nullptr;
    other.clearStmt_ = nullptr;
}

SqliteIndex& SqliteIndex::operator=(SqliteIndex&& other) noexcept {
    if (this != &other) {
        // Clean up existing statements
        if (insertStmt_) sqlite3_finalize(insertStmt_);
        if (searchStmt_) sqlite3_finalize(searchStmt_);
        if (searchFirstStmt_) sqlite3_finalize(searchFirstStmt_);
        if (rangeStmt_) sqlite3_finalize(rangeStmt_);
        if (allStmt_) sqlite3_finalize(allStmt_);
        if (countStmt_) sqlite3_finalize(countStmt_);
        if (clearStmt_) sqlite3_finalize(clearStmt_);

        // Move from other
        db_ = other.db_;
        indexTableName_ = std::move(other.indexTableName_);
        keyType_ = other.keyType_;
        entryCount_ = other.entryCount_;
        insertStmt_ = other.insertStmt_;
        searchStmt_ = other.searchStmt_;
        searchFirstStmt_ = other.searchFirstStmt_;
        rangeStmt_ = other.rangeStmt_;
        allStmt_ = other.allStmt_;
        countStmt_ = other.countStmt_;
        clearStmt_ = other.clearStmt_;

        other.db_ = nullptr;
        other.insertStmt_ = nullptr;
        other.searchStmt_ = nullptr;
        other.searchFirstStmt_ = nullptr;
        other.rangeStmt_ = nullptr;
        other.allStmt_ = nullptr;
        other.countStmt_ = nullptr;
        other.clearStmt_ = nullptr;
    }
    return *this;
}

std::string SqliteIndex::getSqliteType() const {
    switch (keyType_) {
        case ValueType::Int8:
        case ValueType::Int16:
        case ValueType::Int32:
        case ValueType::Int64:
        case ValueType::UInt8:
        case ValueType::UInt16:
        case ValueType::UInt32:
        case ValueType::UInt64:
            return "INTEGER";
        case ValueType::Float32:
        case ValueType::Float64:
            return "REAL";
        case ValueType::String:
            return "TEXT";
        case ValueType::Bool:
            return "INTEGER";
        case ValueType::Bytes:
            return "BLOB";
        default:
            return "BLOB";
    }
}

void SqliteIndex::bindKey(sqlite3_stmt* stmt, int index, const Value& key) const {
    std::visit([stmt, index](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            sqlite3_bind_null(stmt, index);
        } else if constexpr (std::is_same_v<T, bool>) {
            sqlite3_bind_int(stmt, index, arg ? 1 : 0);
        } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                           std::is_same_v<T, int32_t>) {
            sqlite3_bind_int(stmt, index, static_cast<int>(arg));
        } else if constexpr (std::is_same_v<T, int64_t>) {
            sqlite3_bind_int64(stmt, index, arg);
        } else if constexpr (std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                           std::is_same_v<T, uint32_t>) {
            sqlite3_bind_int(stmt, index, static_cast<int>(arg));
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            sqlite3_bind_int64(stmt, index, static_cast<int64_t>(arg));
        } else if constexpr (std::is_same_v<T, float>) {
            sqlite3_bind_double(stmt, index, static_cast<double>(arg));
        } else if constexpr (std::is_same_v<T, double>) {
            sqlite3_bind_double(stmt, index, arg);
        } else if constexpr (std::is_same_v<T, std::string>) {
            sqlite3_bind_text(stmt, index, arg.c_str(), static_cast<int>(arg.size()), SQLITE_TRANSIENT);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            sqlite3_bind_blob(stmt, index, arg.data(), static_cast<int>(arg.size()), SQLITE_TRANSIENT);
        }
    }, key);
}

Value SqliteIndex::extractKey(sqlite3_stmt* stmt, int column) const {
    int type = sqlite3_column_type(stmt, column);

    switch (keyType_) {
        case ValueType::Int8:
            return static_cast<int8_t>(sqlite3_column_int(stmt, column));
        case ValueType::Int16:
            return static_cast<int16_t>(sqlite3_column_int(stmt, column));
        case ValueType::Int32:
            return static_cast<int32_t>(sqlite3_column_int(stmt, column));
        case ValueType::Int64:
            return sqlite3_column_int64(stmt, column);
        case ValueType::UInt8:
            return static_cast<uint8_t>(sqlite3_column_int(stmt, column));
        case ValueType::UInt16:
            return static_cast<uint16_t>(sqlite3_column_int(stmt, column));
        case ValueType::UInt32:
            return static_cast<uint32_t>(sqlite3_column_int(stmt, column));
        case ValueType::UInt64:
            return static_cast<uint64_t>(sqlite3_column_int64(stmt, column));
        case ValueType::Float32:
            return static_cast<float>(sqlite3_column_double(stmt, column));
        case ValueType::Float64:
            return sqlite3_column_double(stmt, column);
        case ValueType::String: {
            const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, column));
            return text ? std::string(text) : std::string();
        }
        case ValueType::Bool:
            return sqlite3_column_int(stmt, column) != 0;
        case ValueType::Bytes: {
            const void* blob = sqlite3_column_blob(stmt, column);
            int size = sqlite3_column_bytes(stmt, column);
            if (blob && size > 0) {
                const uint8_t* data = static_cast<const uint8_t*>(blob);
                return std::vector<uint8_t>(data, data + size);
            }
            return std::vector<uint8_t>();
        }
        case ValueType::Null:
        default:
            return std::monostate{};
    }
}

IndexEntry SqliteIndex::extractEntry(sqlite3_stmt* stmt) const {
    IndexEntry entry;
    entry.key = extractKey(stmt, 0);
    entry.dataOffset = static_cast<uint64_t>(sqlite3_column_int64(stmt, 1));
    entry.dataLength = static_cast<uint32_t>(sqlite3_column_int(stmt, 2));
    entry.sequence = static_cast<uint64_t>(sqlite3_column_int64(stmt, 3));
    return entry;
}

void SqliteIndex::insert(const Value& key, uint64_t dataOffset, uint32_t dataLength, uint64_t sequence) {
    if (std::holds_alternative<std::monostate>(key)) {
        return;
    }

    sqlite3_reset(insertStmt_);
    sqlite3_clear_bindings(insertStmt_);

    bindKey(insertStmt_, 1, key);
    sqlite3_bind_int64(insertStmt_, 2, static_cast<int64_t>(dataOffset));
    sqlite3_bind_int(insertStmt_, 3, static_cast<int>(dataLength));
    sqlite3_bind_int64(insertStmt_, 4, static_cast<int64_t>(sequence));

    int rc = stepWithRetry(db_, insertStmt_);
    if (rc != SQLITE_DONE) {
        throw std::runtime_error("Failed to insert index entry: " +
            std::string(sqlite3_errmsg(db_)));
    }

    entryCount_++;
}

std::vector<IndexEntry> SqliteIndex::search(const Value& key) const {
    std::vector<IndexEntry> results;

    sqlite3_reset(searchStmt_);
    sqlite3_clear_bindings(searchStmt_);
    bindKey(searchStmt_, 1, key);

    int rc;
    while ((rc = stepWithRetry(db_, searchStmt_)) == SQLITE_ROW) {
        results.push_back(extractEntry(searchStmt_));
    }

    if (rc != SQLITE_DONE) {
        throw std::runtime_error("Failed to search index: " + std::string(sqlite3_errmsg(db_)));
    }

    return results;
}

bool SqliteIndex::searchFirst(const Value& key, IndexEntry& result) const {
    sqlite3_reset(searchFirstStmt_);
    sqlite3_clear_bindings(searchFirstStmt_);
    bindKey(searchFirstStmt_, 1, key);

    int rc = stepWithRetry(db_, searchFirstStmt_);
    if (rc == SQLITE_ROW) {
        result = extractEntry(searchFirstStmt_);
        return true;
    }
    if (rc != SQLITE_DONE) {
        throw std::runtime_error("Failed to search first index entry: " + std::string(sqlite3_errmsg(db_)));
    }

    return false;
}

bool SqliteIndex::searchFirstString(const std::string& key, uint64_t& outOffset, uint32_t& outLength, uint64_t& outSequence) const {
    sqlite3_reset(searchFirstStmt_);
    // Bind string directly - no variant dispatch
    sqlite3_bind_text(searchFirstStmt_, 1, key.c_str(), static_cast<int>(key.size()), SQLITE_STATIC);

    int rc = stepWithRetry(db_, searchFirstStmt_);
    if (rc == SQLITE_ROW) {
        // Extract only what we need - skip key extraction entirely
        outOffset = static_cast<uint64_t>(sqlite3_column_int64(searchFirstStmt_, 1));
        outLength = static_cast<uint32_t>(sqlite3_column_int(searchFirstStmt_, 2));
        outSequence = static_cast<uint64_t>(sqlite3_column_int64(searchFirstStmt_, 3));
        return true;
    }
    if (rc != SQLITE_DONE) {
        throw std::runtime_error("Failed to search string index entry: " + std::string(sqlite3_errmsg(db_)));
    }

    return false;
}

bool SqliteIndex::searchFirstInt64(int64_t key, uint64_t& outOffset, uint32_t& outLength, uint64_t& outSequence) const {
    sqlite3_reset(searchFirstStmt_);
    // Bind int64 directly - no variant dispatch
    sqlite3_bind_int64(searchFirstStmt_, 1, key);

    int rc = stepWithRetry(db_, searchFirstStmt_);
    if (rc == SQLITE_ROW) {
        // Extract only what we need - skip key extraction entirely
        outOffset = static_cast<uint64_t>(sqlite3_column_int64(searchFirstStmt_, 1));
        outLength = static_cast<uint32_t>(sqlite3_column_int(searchFirstStmt_, 2));
        outSequence = static_cast<uint64_t>(sqlite3_column_int64(searchFirstStmt_, 3));
        return true;
    }
    if (rc != SQLITE_DONE) {
        throw std::runtime_error("Failed to search int64 index entry: " + std::string(sqlite3_errmsg(db_)));
    }

    return false;
}

std::vector<IndexEntry> SqliteIndex::range(const Value& minKey, const Value& maxKey) const {
    std::vector<IndexEntry> results;

    sqlite3_reset(rangeStmt_);
    sqlite3_clear_bindings(rangeStmt_);
    bindKey(rangeStmt_, 1, minKey);
    bindKey(rangeStmt_, 2, maxKey);

    int rc;
    while ((rc = stepWithRetry(db_, rangeStmt_)) == SQLITE_ROW) {
        results.push_back(extractEntry(rangeStmt_));
    }

    if (rc != SQLITE_DONE) {
        throw std::runtime_error("Failed to range-scan index: " + std::string(sqlite3_errmsg(db_)));
    }

    return results;
}

std::vector<IndexEntry> SqliteIndex::all() const {
    std::vector<IndexEntry> results;

    sqlite3_reset(allStmt_);

    int rc;
    while ((rc = stepWithRetry(db_, allStmt_)) == SQLITE_ROW) {
        results.push_back(extractEntry(allStmt_));
    }

    if (rc != SQLITE_DONE) {
        throw std::runtime_error("Failed to scan index: " + std::string(sqlite3_errmsg(db_)));
    }

    return results;
}

void SqliteIndex::clear() {
    sqlite3_reset(clearStmt_);

    int rc = stepWithRetry(db_, clearStmt_);
    if (rc != SQLITE_DONE) {
        throw std::runtime_error("Failed to clear index: " +
            std::string(sqlite3_errmsg(db_)));
    }

    entryCount_ = 0;
}

}  // namespace flatsql
