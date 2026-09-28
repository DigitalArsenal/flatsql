#ifndef FLATSQL_SQLITE_INDEX_H
#define FLATSQL_SQLITE_INDEX_H

#include "flatsql/types.h"
#include <sqlite3.h>
#include <array>
#include <cstdint>
#include <string>
#include <vector>
#include <memory>

namespace flatsql {

class SqliteIndex;

/**
 * A lazy walk over one key range of a SqliteIndex, in (key, sequence) order.
 *
 * The scan borrows a prepared statement from the index's pool for as long as
 * it is open, so a virtual-table cursor reads one index entry per xNext:
 * `LIMIT 1` over a key with 90k entries reads one entry, not 90k, and nothing
 * is materialized. Never throws (the no-exceptions WASI artifact turns a throw
 * into a guest abort).
 */
class SqliteIndexScan {
public:
    SqliteIndexScan() = default;
    ~SqliteIndexScan() { release(); }
    SqliteIndexScan(const SqliteIndexScan&) = delete;
    SqliteIndexScan& operator=(const SqliteIndexScan&) = delete;

    bool active() const { return stmt_ != nullptr; }

    // 1 = an entry was read, 0 = the range is exhausted (the statement goes
    // back to the pool), -1 = SQLite error (see SqliteIndex::lastError()).
    int next(uint64_t& dataOffset, uint32_t& dataLength, uint64_t& sequence) noexcept;

    // Return the statement to the pool. Idempotent.
    void release() noexcept;

private:
    friend class SqliteIndex;
    SqliteIndex* owner_ = nullptr;
    sqlite3_stmt* stmt_ = nullptr;
    int shape_ = 0;
};

/**
 * SQLite-backed index for FlatBuffer records.
 * Uses SQLite's highly optimized B-tree for fast lookups.
 * Keys point to offsets in the stacked FlatBuffer storage.
 */
class SqliteIndex {
public:
    /**
     * Create an index backed by the given SQLite database.
     *
     * @param db        SQLite database connection (must remain valid for index lifetime)
     * @param tableName Base table name (used to create unique index table)
     * @param columnName Column being indexed
     * @param keyType   Type of the key (determines SQLite column affinity)
     */
    SqliteIndex(sqlite3* db, const std::string& tableName,
                const std::string& columnName, ValueType keyType);

    ~SqliteIndex();

    // Disable copy (SQLite statements can't be copied)
    SqliteIndex(const SqliteIndex&) = delete;
    SqliteIndex& operator=(const SqliteIndex&) = delete;

    // Allow move
    SqliteIndex(SqliteIndex&& other) noexcept;
    SqliteIndex& operator=(SqliteIndex&& other) noexcept;

    // Insert an entry
    void insert(const Value& key, uint64_t dataOffset, uint32_t dataLength, uint64_t sequence);

    // Search for entries with exact key match
    std::vector<IndexEntry> search(const Value& key) const;

    // Search for first entry with exact key match (optimized for unique keys)
    // Returns true if found, false otherwise
    bool searchFirst(const Value& key, IndexEntry& result) const;

    // Fast path for string key lookups (avoids Value/variant overhead)
    // Returns true if found, sets outOffset and outLength
    bool searchFirstString(const std::string& key, uint64_t& outOffset, uint32_t& outLength, uint64_t& outSequence) const;

    // Fast path for int64 key lookups (avoids Value/variant overhead)
    bool searchFirstInt64(int64_t key, uint64_t& outOffset, uint32_t& outLength, uint64_t& outSequence) const;

    // Range query: minKey <= key <= maxKey
    std::vector<IndexEntry> range(const Value& minKey, const Value& maxKey) const;

    // Get all entries (full scan)
    std::vector<IndexEntry> all() const;

    // Lazy scans (see SqliteIndexScan). Keys are bound as the caller's
    // sqlite3_value, so comparisons follow SQLite's own affinity rules for the
    // key column: exactly what the query would have done row by row. A NULL
    // key or bound matches nothing (SQL comparison with NULL is never true);
    // the scan is then left inactive and the call still succeeds.
    // Return false only when a statement cannot be prepared or bound.
    bool openEqual(sqlite3_value* key, SqliteIndexScan& scan) noexcept;
    bool openRange(sqlite3_value* lower, bool lowerInclusive,
                   sqlite3_value* upper, bool upperInclusive,
                   SqliteIndexScan& scan) noexcept;

    const char* lastError() const noexcept;

    // Statistics
    uint64_t getEntryCount() const { return entryCount_; }

    // Clear all entries
    void clear();

    // Get the index table name
    const std::string& getIndexTableName() const { return indexTableName_; }

private:
    friend class SqliteIndexScan;

    // Scan shapes: 0 = equality; 1 + lower*3 + upper for ranges, where each
    // bound is 0 absent, 1 exclusive, 2 inclusive.
    static constexpr int kScanShapes = 10;
    static constexpr size_t kPooledPerShape = 4;
    sqlite3_stmt* acquireScanStmt(int shape) noexcept;
    void returnScanStmt(int shape, sqlite3_stmt* stmt) noexcept;
    void finalizeScanPool() noexcept;
    std::array<std::vector<sqlite3_stmt*>, kScanShapes> scanPool_;

    void bindKey(sqlite3_stmt* stmt, int index, const Value& key) const;
    Value extractKey(sqlite3_stmt* stmt, int column) const;
    IndexEntry extractEntry(sqlite3_stmt* stmt) const;
    std::string getSqliteType() const;

    sqlite3* db_;
    std::string indexTableName_;
    ValueType keyType_;
    uint64_t entryCount_ = 0;

    // Prepared statements for performance
    mutable sqlite3_stmt* insertStmt_ = nullptr;
    mutable sqlite3_stmt* searchStmt_ = nullptr;
    mutable sqlite3_stmt* searchFirstStmt_ = nullptr;
    mutable sqlite3_stmt* rangeStmt_ = nullptr;
    mutable sqlite3_stmt* allStmt_ = nullptr;
    mutable sqlite3_stmt* countStmt_ = nullptr;
    mutable sqlite3_stmt* clearStmt_ = nullptr;
};

}  // namespace flatsql

#endif  // FLATSQL_SQLITE_INDEX_H
