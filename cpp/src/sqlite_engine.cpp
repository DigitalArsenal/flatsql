#include "flatsql/sqlite_engine.h"
#include "flatsql/geo_functions.h"
#include <sstream>
#include <stdexcept>
#include <cctype>

// sqlean extension init functions (C linkage)
extern "C" {
    int math_init(sqlite3* db);
    int stats_init(sqlite3* db);
    int text_init(sqlite3* db);
    int uuid_init(sqlite3* db);
    int fuzzy_init(sqlite3* db);
}

namespace flatsql {

// Helper to trim whitespace and convert to lowercase for comparison
static std::string normalizeSQL(const std::string& sql) {
    std::string result;
    result.reserve(sql.size());
    bool inSpace = true;
    for (char c : sql) {
        if (std::isspace(c)) {
            if (!inSpace && !result.empty()) {
                result += ' ';
                inSpace = true;
            }
        } else {
            result += std::tolower(c);
            inSpace = false;
        }
    }
    // Trim trailing space
    if (!result.empty() && result.back() == ' ') {
        result.pop_back();
    }
    return result;
}

SQLiteEngine::SQLiteEngine() : db_(nullptr) {
    int rc = sqlite3_open(":memory:", &db_);
    if (rc != SQLITE_OK) {
        std::string error = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        db_ = nullptr;
        throw std::runtime_error("Failed to open SQLite database: " + error);
    }

    // Register custom geo/spatial functions
    registerGeoFunctions(db_);

    // Register sqlean extensions
    math_init(db_);
    stats_init(db_);
    text_init(db_);
    uuid_init(db_);
    fuzzy_init(db_);
}

SQLiteEngine::~SQLiteEngine() {
    clearStmtCache();
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

void SQLiteEngine::clearStmtCache() {
    for (auto& [sql, stmt] : stmtCache_) {
        if (stmt) {
            sqlite3_finalize(stmt);
        }
    }
    stmtCache_.clear();
}

sqlite3_stmt* SQLiteEngine::getOrPrepareStmt(const std::string& sql) const {
    auto it = stmtCache_.find(sql);
    if (it != stmtCache_.end()) {
        sqlite3_reset(it->second);
        return it->second;
    }

    // Evict old entries if cache is full
    if (stmtCache_.size() >= MAX_STMT_CACHE_SIZE) {
        // Simple eviction: clear entire cache
        for (auto& [s, stmt] : stmtCache_) {
            if (stmt) {
                sqlite3_finalize(stmt);
            }
        }
        stmtCache_.clear();
    }

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("SQL error: " + std::string(sqlite3_errmsg(db_)));
    }

    stmtCache_[sql] = stmt;
    return stmt;
}

SQLiteEngine::SQLiteEngine(SQLiteEngine&& other) noexcept
    : db_(other.db_), sources_(std::move(other.sources_)) {
    other.db_ = nullptr;
}

SQLiteEngine& SQLiteEngine::operator=(SQLiteEngine&& other) noexcept {
    if (this != &other) {
        if (db_) {
            sqlite3_close(db_);
        }
        db_ = other.db_;
        sources_ = std::move(other.sources_);
        other.db_ = nullptr;
    }
    return *this;
}

void SQLiteEngine::registerSource(
    const std::string& sourceName,
    StreamingFlatBufferStore* store,
    const TableDef* tableDef,
    const std::string& fileId,
    FieldExtractor extractor,
    const std::unordered_map<std::string, SqliteIndex*>& indexes,
    FastFieldExtractor fastExtractor,
    BatchExtractor batchExtractor,
    const std::vector<StreamingFlatBufferStore::FileRecordInfo>* sourceRecordInfos
) {
    if (sources_.count(sourceName)) {
        throw std::runtime_error("Source already registered: " + sourceName);
    }

    // Create source info
    auto sourceInfo = std::make_unique<SourceInfo>();
    sourceInfo->name = sourceName;
    sourceInfo->store = store;
    sourceInfo->tableDef = tableDef;
    sourceInfo->fileId = fileId;
    sourceInfo->extractor = extractor;
    sourceInfo->batchExtractor = batchExtractor;
    sourceInfo->indexes = indexes;
    sourceInfo->sourceRecordInfos = sourceRecordInfos;

    // Set up VTabCreateInfo (pointer will be stable after insert)
    sourceInfo->vtabInfo.store = store;
    sourceInfo->vtabInfo.tableDef = tableDef;
    sourceInfo->vtabInfo.sourceName = sourceName;
    sourceInfo->vtabInfo.fastExtractor = fastExtractor;
    sourceInfo->vtabInfo.fileId = fileId;
    sourceInfo->vtabInfo.extractor = extractor;
    sourceInfo->vtabInfo.indexes = indexes;
    sourceInfo->vtabInfo.tombstones = &sourceInfo->tombstones;
    sourceInfo->vtabInfo.sourceRecordInfos = sourceRecordInfos;

    // Store before registering (so pointers are stable)
    SourceInfo* infoPtr = sourceInfo.get();
    sources_[sourceName] = std::move(sourceInfo);

    // Register the virtual table module with this source's info
    int rc = sqlite3_create_module_v2(
        db_,
        sourceName.c_str(),
        FlatBufferVTabModule::getModule(),
        &infoPtr->vtabInfo,
        nullptr  // No destructor - we manage lifetime
    );

    if (rc != SQLITE_OK) {
        sources_.erase(sourceName);
        throw std::runtime_error("Failed to create SQLite module: " + std::string(sqlite3_errmsg(db_)));
    }

    // Create the virtual table
    std::ostringstream sql;
    sql << "CREATE VIRTUAL TABLE \"" << sourceName << "\" USING \"" << sourceName << "\"()";

    char* errMsg = nullptr;
    rc = sqlite3_exec(db_, sql.str().c_str(), nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        std::string error = errMsg ? errMsg : "Unknown error";
        sqlite3_free(errMsg);
        sources_.erase(sourceName);
        throw std::runtime_error("Failed to create virtual table: " + error);
    }
}

std::string SQLiteEngine::buildColumnList(const TableDef* tableDef) const {
    std::ostringstream ss;
    bool first = true;

    for (const auto& col : tableDef->columns) {
        if (!first) ss << ", ";
        first = false;
        ss << "\"" << col.name << "\"";
    }

    // Add virtual columns
    ss << ", \"_source\", \"_rowid\", \"_offset\", \"_data\"";

    return ss.str();
}

void SQLiteEngine::createUnifiedView(
    const std::string& viewName,
    const std::vector<std::string>& sourceNames
) {
    if (sourceNames.empty()) {
        throw std::runtime_error("Cannot create unified view with no sources");
    }

    // Verify all sources exist and have same schema
    const TableDef* baseSchema = nullptr;
    for (const auto& name : sourceNames) {
        auto it = sources_.find(name);
        if (it == sources_.end()) {
            throw std::runtime_error("Source not found: " + name);
        }
        if (!baseSchema) {
            baseSchema = it->second->tableDef;
        } else {
            // Verify compatible schema (same columns)
            if (it->second->tableDef->columns.size() != baseSchema->columns.size()) {
                throw std::runtime_error("Incompatible schemas for unified view");
            }
        }
    }

    // Drop existing table/view if it exists (base virtual table or old view)
    {
        std::string dropSql = "DROP TABLE IF EXISTS \"" + viewName + "\"";
        char* errMsg = nullptr;
        sqlite3_exec(db_, dropSql.c_str(), nullptr, nullptr, &errMsg);
        sqlite3_free(errMsg);  // Ignore errors
    }
    {
        std::string dropSql = "DROP VIEW IF EXISTS \"" + viewName + "\"";
        char* errMsg = nullptr;
        sqlite3_exec(db_, dropSql.c_str(), nullptr, nullptr, &errMsg);
        sqlite3_free(errMsg);  // Ignore errors
    }

    // Build UNION ALL view with _source column
    std::ostringstream sql;
    sql << "CREATE VIEW \"" << viewName << "\" AS ";

    bool first = true;
    for (const auto& name : sourceNames) {
        if (!first) sql << " UNION ALL ";
        first = false;
        sql << "SELECT " << buildColumnList(baseSchema) << " FROM \"" << name << "\"";
    }

    char* errMsg = nullptr;
    int rc = sqlite3_exec(db_, sql.str().c_str(), nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        std::string error = errMsg ? errMsg : "Unknown error";
        sqlite3_free(errMsg);
        throw std::runtime_error("Failed to create unified view: " + error);
    }
}

void SQLiteEngine::bindValue(sqlite3_stmt* stmt, int idx, const Value& value) const {
    std::visit([&](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            sqlite3_bind_null(stmt, idx);
        } else if constexpr (std::is_same_v<T, bool>) {
            sqlite3_bind_int(stmt, idx, v ? 1 : 0);
        } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t>) {
            sqlite3_bind_int(stmt, idx, static_cast<int>(v));
        } else if constexpr (std::is_same_v<T, int64_t>) {
            sqlite3_bind_int64(stmt, idx, v);
        } else if constexpr (std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t>) {
            sqlite3_bind_int(stmt, idx, static_cast<int>(v));
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            sqlite3_bind_int64(stmt, idx, static_cast<sqlite3_int64>(v));
        } else if constexpr (std::is_same_v<T, float>) {
            sqlite3_bind_double(stmt, idx, static_cast<double>(v));
        } else if constexpr (std::is_same_v<T, double>) {
            sqlite3_bind_double(stmt, idx, v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            sqlite3_bind_text(stmt, idx, v.c_str(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            sqlite3_bind_blob(stmt, idx, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
        } else {
            sqlite3_bind_null(stmt, idx);
        }
    }, value);
}

QueryResult SQLiteEngine::execute(const std::string& sql) {
    return execute(sql, {});
}

QueryResult SQLiteEngine::execute(const std::string& sql, const std::vector<Value>& params) {
    QueryResult result;

    // Try fast path for simple queries
    if (tryFastPath(sql, params, result)) {
        return result;
    }

    // Use cached prepared statement
    sqlite3_stmt* stmt = getOrPrepareStmt(sql);

    // Bind parameters
    for (size_t i = 0; i < params.size(); i++) {
        bindValue(stmt, static_cast<int>(i + 1), params[i]);
    }

    // Get column names
    int numCols = sqlite3_column_count(stmt);
    result.columns.reserve(numCols);
    for (int i = 0; i < numCols; i++) {
        const char* name = sqlite3_column_name(stmt, i);
        result.columns.push_back(name ? name : "");
    }

    // Fetch rows - optimized to reduce allocations
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        result.rows.emplace_back();
        std::vector<Value>& row = result.rows.back();
        row.resize(numCols);

        for (int i = 0; i < numCols; i++) {
            int colType = sqlite3_column_type(stmt, i);

            switch (colType) {
                case SQLITE_NULL:
                    row[i] = std::monostate{};
                    break;

                case SQLITE_INTEGER:
                    row[i] = static_cast<int64_t>(sqlite3_column_int64(stmt, i));
                    break;

                case SQLITE_FLOAT:
                    row[i] = sqlite3_column_double(stmt, i);
                    break;

                case SQLITE_TEXT: {
                    const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
                    int len = sqlite3_column_bytes(stmt, i);
                    row[i] = std::string(text ? text : "", len);
                    break;
                }

                case SQLITE_BLOB: {
                    const uint8_t* blob = static_cast<const uint8_t*>(sqlite3_column_blob(stmt, i));
                    int len = sqlite3_column_bytes(stmt, i);
                    row[i] = std::vector<uint8_t>(blob, blob + len);
                    break;
                }

                default:
                    row[i] = std::monostate{};
                    break;
            }
        }
    }

    // Don't finalize - statement is cached
    // sqlite3_reset is called by getOrPrepareStmt on next use

    if (rc != SQLITE_DONE) {
        throw std::runtime_error("SQL execution error: " + std::string(sqlite3_errmsg(db_)));
    }

    return result;
}

size_t SQLiteEngine::executeAndCount(const std::string& sql, const std::vector<Value>& params) {
    // Try fast path for simple queries - bypass VTable entirely
    size_t fastCount = 0;
    if (tryFastPathCount(sql, params, fastCount)) {
        return fastCount;
    }

    sqlite3_stmt* stmt = getOrPrepareStmt(sql);

    // Bind parameters
    for (size_t i = 0; i < params.size(); i++) {
        bindValue(stmt, static_cast<int>(i + 1), params[i]);
    }

    // Just step through - read columns but don't copy to QueryResult
    size_t rowCount = 0;
    int numCols = sqlite3_column_count(stmt);
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        rowCount++;
        // Read all columns to trigger xColumn callbacks
        for (int i = 0; i < numCols; i++) {
            int colType = sqlite3_column_type(stmt, i);
            switch (colType) {
                case SQLITE_INTEGER:
                    (void)sqlite3_column_int64(stmt, i);
                    break;
                case SQLITE_FLOAT:
                    (void)sqlite3_column_double(stmt, i);
                    break;
                case SQLITE_TEXT:
                    (void)sqlite3_column_text(stmt, i);
                    break;
                case SQLITE_BLOB:
                    (void)sqlite3_column_blob(stmt, i);
                    break;
                default:
                    break;
            }
        }
    }

    return rowCount;
}

static int fastPathCountHits = 0;

// Cache for case-insensitive lookups (table name -> actual source name)
static thread_local std::unordered_map<std::string, SourceInfo*> sourceNameCache_;

// Cache for parsed SQL queries
struct ParsedQuery {
    std::string tableName;
    std::string columnName;
    bool isPointQuery;
    bool isFullScan;
};
static thread_local std::unordered_map<std::string, ParsedQuery> parsedQueryCache_;

// Cache for column names (avoids rebuilding for each query)
static thread_local std::unordered_map<std::string, std::vector<std::string>> columnNamesCache_;

// Helper to get cached column names for a source
static const std::vector<std::string>& getCachedColumnNames(const SourceInfo* source) {
    auto it = columnNamesCache_.find(source->name);
    if (it != columnNamesCache_.end()) {
        return it->second;
    }

    std::vector<std::string> cols;
    cols.reserve(source->tableDef->columns.size() + 4);  // +4 for virtual columns
    for (const auto& col : source->tableDef->columns) {
        cols.push_back(col.name);
    }
    cols.push_back("_source");
    cols.push_back("_rowid");
    cols.push_back("_offset");
    cols.push_back("_data");

    columnNamesCache_[source->name] = std::move(cols);
    return columnNamesCache_[source->name];
}

// Helper to find source with case-insensitive matching (with caching)
SourceInfo* SQLiteEngine::findSourceCaseInsensitive(const std::string& lowerTableName) {
    // Check cache first
    auto cacheIt = sourceNameCache_.find(lowerTableName);
    if (cacheIt != sourceNameCache_.end()) {
        return cacheIt->second;
    }
    // Try exact match first
    auto it = sources_.find(lowerTableName);
    if (it != sources_.end()) {
        return it->second.get();
    }

    // Try case-insensitive match
    for (const auto& [name, src] : sources_) {
        std::string lowerName = name;
        for (char& c : lowerName) c = std::tolower(c);
        if (lowerName == lowerTableName) {
            // Cache the result
            sourceNameCache_[lowerTableName] = src.get();
            return src.get();
        }
    }
    sourceNameCache_[lowerTableName] = nullptr;
    return nullptr;
}

bool SQLiteEngine::tryFastPathCount(const std::string& sql, const std::vector<Value>& params, size_t& count) {
    // Check cache first
    auto cacheIt = parsedQueryCache_.find(sql);
    ParsedQuery* parsed = nullptr;

    if (cacheIt != parsedQueryCache_.end()) {
        parsed = &cacheIt->second;
    } else {
        // Parse and cache the query
        std::string normalized = normalizeSQL(sql);

        // Check for "select * from"
        if (normalized.size() < 14 || normalized.substr(0, 14) != "select * from ") {
            parsedQueryCache_[sql] = {.tableName = "", .columnName = "", .isPointQuery = false, .isFullScan = false};
            return false;
        }

        size_t wherePos = normalized.find(" where ", 14);
        ParsedQuery pq;
        pq.isFullScan = (wherePos == std::string::npos);
        pq.isPointQuery = !pq.isFullScan;

        if (pq.isFullScan) {
            pq.tableName = normalized.substr(14);
            while (!pq.tableName.empty() && (pq.tableName.back() == ' ' || pq.tableName.back() == ';')) {
                pq.tableName.pop_back();
            }
            if (pq.tableName.size() >= 2 && pq.tableName.front() == '"' && pq.tableName.back() == '"') {
                pq.tableName = pq.tableName.substr(1, pq.tableName.size() - 2);
            }
        } else {
            pq.tableName = normalized.substr(14, wherePos - 14);
            if (pq.tableName.size() >= 2 && pq.tableName.front() == '"' && pq.tableName.back() == '"') {
                pq.tableName = pq.tableName.substr(1, pq.tableName.size() - 2);
            }

            // Parse column name
            std::string whereClause = normalized.substr(wherePos + 7);
            size_t eqPos = whereClause.find(" = ?");
            if (eqPos == std::string::npos) {
                eqPos = whereClause.find("= ?");
            }
            if (eqPos != std::string::npos) {
                pq.columnName = whereClause.substr(0, eqPos);
                while (!pq.columnName.empty() && pq.columnName.back() == ' ') {
                    pq.columnName.pop_back();
                }
                if (pq.columnName.size() >= 2 && pq.columnName.front() == '"' && pq.columnName.back() == '"') {
                    pq.columnName = pq.columnName.substr(1, pq.columnName.size() - 2);
                }
            } else {
                pq.isPointQuery = false;
            }
        }

        parsedQueryCache_[sql] = pq;
        parsed = &parsedQueryCache_[sql];
    }

    // Fast path: full scan
    if (parsed->isFullScan && params.empty()) {
        auto* source = findSourceCaseInsensitive(parsed->tableName);
        if (source && source->store && source->tableDef) {
            const auto* recordInfos = source->store->getRecordInfoVector(source->fileId);
            if (recordInfos) {
                const auto* tombstones = &source->tombstones;
                if (tombstones->empty()) {
                    // Fast path: no tombstones
                    count = recordInfos->size();
                } else {
                    count = 0;
                    for (const auto& info : *recordInfos) {
                        if (!tombstones->count(info.sequence)) {
                            count++;
                        }
                    }
                }
                return true;
            }
        }
    }

    // Fast path: point query
    if (parsed->isPointQuery && params.size() == 1 && !parsed->columnName.empty()) {
        auto* source = findSourceCaseInsensitive(parsed->tableName);
        if (!source || !source->store || !source->tableDef) {
            return false;
        }

        auto indexIt = source->indexes.find(parsed->columnName);
        if (indexIt == source->indexes.end() || !indexIt->second) {
            return false;
        }

        IndexEntry entry;
        if (!indexIt->second->searchFirst(params[0], entry)) {
            count = 0;
            return true;
        }

        if (!source->tombstones.empty() && source->tombstones.count(entry.sequence)) {
            count = 0;
            return true;
        }

        count = 1;
        return true;
    }

    return false;
}

void SQLiteEngine::markDeleted(const std::string& sourceName, uint64_t sequence) {
    auto it = sources_.find(sourceName);
    if (it == sources_.end()) {
        throw std::runtime_error("Source not found: " + sourceName);
    }
    it->second->tombstones.insert(sequence);
}

size_t SQLiteEngine::getDeletedCount(const std::string& sourceName) const {
    auto it = sources_.find(sourceName);
    if (it == sources_.end()) {
        return 0;
    }
    return it->second->tombstones.size();
}

void SQLiteEngine::clearTombstones(const std::string& sourceName) {
    auto it = sources_.find(sourceName);
    if (it != sources_.end()) {
        it->second->tombstones.clear();
    }
}

std::vector<std::string> SQLiteEngine::listSources() const {
    std::vector<std::string> names;
    names.reserve(sources_.size());
    for (const auto& [name, _] : sources_) {
        names.push_back(name);
    }
    return names;
}

bool SQLiteEngine::hasSource(const std::string& sourceName) const {
    return sources_.count(sourceName) > 0;
}

std::string SQLiteEngine::getLastError() const {
    if (db_) {
        return sqlite3_errmsg(db_);
    }
    return "Database not initialized";
}

SourceInfo* SQLiteEngine::getSource(const std::string& sourceName) {
    auto it = sources_.find(sourceName);
    return it != sources_.end() ? it->second.get() : nullptr;
}

const SourceInfo* SQLiteEngine::getSource(const std::string& sourceName) const {
    auto it = sources_.find(sourceName);
    return it != sources_.end() ? it->second.get() : nullptr;
}

static int fastPathHits = 0;
static int fastPathFullScanHits = 0;

// Debug counters exposed for testing
int getFastPathHits() { return fastPathHits; }
int getFastPathFullScanHits() { return fastPathFullScanHits; }

bool SQLiteEngine::tryFastPath(const std::string& sql, const std::vector<Value>& params, QueryResult& result) {
    // Check cache first
    auto cacheIt = parsedQueryCache_.find(sql);
    ParsedQuery* parsed = nullptr;

    if (cacheIt != parsedQueryCache_.end()) {
        parsed = &cacheIt->second;
        // Early exit for non-optimizable queries
        if (!parsed->isPointQuery && !parsed->isFullScan) {
            return false;
        }
    } else {
        // Parse and cache the query (same logic as tryFastPathCount)
        std::string normalized = normalizeSQL(sql);

        if (normalized.size() < 14 || normalized.substr(0, 14) != "select * from ") {
            parsedQueryCache_[sql] = {.tableName = "", .columnName = "", .isPointQuery = false, .isFullScan = false};
            return false;
        }

        size_t wherePos = normalized.find(" where ", 14);
        ParsedQuery pq;
        pq.isFullScan = (wherePos == std::string::npos);
        pq.isPointQuery = !pq.isFullScan;

        if (pq.isFullScan) {
            pq.tableName = normalized.substr(14);
            while (!pq.tableName.empty() && (pq.tableName.back() == ' ' || pq.tableName.back() == ';')) {
                pq.tableName.pop_back();
            }
            if (pq.tableName.size() >= 2 && pq.tableName.front() == '"' && pq.tableName.back() == '"') {
                pq.tableName = pq.tableName.substr(1, pq.tableName.size() - 2);
            }
        } else {
            pq.tableName = normalized.substr(14, wherePos - 14);
            if (pq.tableName.size() >= 2 && pq.tableName.front() == '"' && pq.tableName.back() == '"') {
                pq.tableName = pq.tableName.substr(1, pq.tableName.size() - 2);
            }

            std::string whereClause = normalized.substr(wherePos + 7);
            size_t eqPos = whereClause.find(" = ?");
            if (eqPos == std::string::npos) {
                eqPos = whereClause.find("= ?");
            }
            if (eqPos != std::string::npos) {
                pq.columnName = whereClause.substr(0, eqPos);
                while (!pq.columnName.empty() && pq.columnName.back() == ' ') {
                    pq.columnName.pop_back();
                }
                if (pq.columnName.size() >= 2 && pq.columnName.front() == '"' && pq.columnName.back() == '"') {
                    pq.columnName = pq.columnName.substr(1, pq.columnName.size() - 2);
                }
            } else {
                pq.isPointQuery = false;
            }
        }

        parsedQueryCache_[sql] = pq;
        parsed = &parsedQueryCache_[sql];
    }

    // Full scan fast path
    if (parsed->isFullScan && params.empty()) {
        auto* source = findSourceCaseInsensitive(parsed->tableName);
        if (source && source->store && source->tableDef && source->extractor) {
            fastPathFullScanHits++;

            // Build column names
            for (const auto& col : source->tableDef->columns) {
                result.columns.push_back(col.name);
            }
            result.columns.push_back("_source");
            result.columns.push_back("_rowid");
            result.columns.push_back("_offset");
            result.columns.push_back("_data");

            const auto* recordInfos = source->store->getRecordInfoVector(source->fileId);
            if (recordInfos) {
                const uint8_t* dataBuffer = source->store->getDataBuffer();
                const auto* tombstones = &source->tombstones;
                result.rows.reserve(recordInfos->size());

                // Use batch extractor if available
                if (source->batchExtractor) {
                    for (const auto& info : *recordInfos) {
                        if (!tombstones->empty() && tombstones->count(info.sequence)) {
                            continue;
                        }

                        const uint8_t* ptr = dataBuffer + info.offset;
                        uint32_t len = static_cast<uint32_t>(ptr[0]) |
                                       (static_cast<uint32_t>(ptr[1]) << 8) |
                                       (static_cast<uint32_t>(ptr[2]) << 16) |
                                       (static_cast<uint32_t>(ptr[3]) << 24);
                        const uint8_t* data = ptr + 4;

                        std::vector<Value> row;
                        source->batchExtractor(data, len, row);

                        // Add virtual columns
                        row.push_back(source->name);
                        row.push_back(static_cast<int64_t>(info.sequence));
                        row.push_back(static_cast<int64_t>(info.offset));
                        row.push_back(std::monostate{});

                        result.rows.push_back(std::move(row));
                    }
                } else {
                    for (const auto& info : *recordInfos) {
                        if (!tombstones->empty() && tombstones->count(info.sequence)) {
                            continue;
                        }

                        const uint8_t* ptr = dataBuffer + info.offset;
                        uint32_t len = static_cast<uint32_t>(ptr[0]) |
                                       (static_cast<uint32_t>(ptr[1]) << 8) |
                                       (static_cast<uint32_t>(ptr[2]) << 16) |
                                       (static_cast<uint32_t>(ptr[3]) << 24);
                        const uint8_t* data = ptr + 4;

                        std::vector<Value> row;
                        row.reserve(result.columns.size());

                        for (const auto& col : source->tableDef->columns) {
                            row.push_back(source->extractor(data, len, col.name));
                        }

                        row.push_back(source->name);
                        row.push_back(static_cast<int64_t>(info.sequence));
                        row.push_back(static_cast<int64_t>(info.offset));
                        row.push_back(std::monostate{});

                        result.rows.push_back(std::move(row));
                    }
                }
            }
            return true;
        }
    }

    // Point query fast path
    if (!parsed->isPointQuery || params.size() != 1 || parsed->columnName.empty()) {
        return false;
    }

    auto* source = findSourceCaseInsensitive(parsed->tableName);
    if (!source || !source->store || !source->tableDef) {
        return false;
    }

    auto indexIt = source->indexes.find(parsed->columnName);
    if (indexIt == source->indexes.end() || !indexIt->second) {
        return false;
    }

    fastPathHits++;

    SqliteIndex* index = indexIt->second;
    const Value& searchValue = params[0];

    // Do the lookup first - avoid work if no match
    IndexEntry entry;
    if (!index->searchFirst(searchValue, entry)) {
        // No match found - return empty result with cached column names (avoid copy with move)
        result.columns = getCachedColumnNames(source);
        return true;
    }

    // Check tombstone only if there are any
    if (!source->tombstones.empty() && source->tombstones.count(entry.sequence)) {
        // Tombstoned - return empty result
        result.columns = getCachedColumnNames(source);
        return true;
    }

    // Get the data
    uint32_t dataLen = 0;
    const uint8_t* data = source->store->getDataAtOffset(entry.dataOffset, &dataLen);
    if (!data) {
        return false;  // Error, fall back to VTable
    }

    // Get cached column names (copy is necessary, but columns are cached)
    result.columns = getCachedColumnNames(source);

    // Use thread-local row buffer to avoid allocation
    static thread_local std::vector<Value> row;
    row.clear();
    row.reserve(result.columns.size());

    if (source->batchExtractor) {
        source->batchExtractor(data, dataLen, row);
    } else if (source->extractor) {
        for (const auto& col : source->tableDef->columns) {
            row.push_back(source->extractor(data, dataLen, col.name));
        }
    } else {
        // No extractor, fill with nulls
        row.resize(source->tableDef->columns.size(), std::monostate{});
    }

    // Virtual columns - use string_view-like approach where possible
    row.emplace_back(source->name);  // _source (move if possible)
    row.emplace_back(static_cast<int64_t>(entry.sequence));  // _rowid
    row.emplace_back(static_cast<int64_t>(entry.dataOffset));  // _offset
    row.emplace_back(std::monostate{});  // _data - null for performance

    result.rows.emplace_back(std::move(row));
    return true;
}

// Optimized query that returns minimal data for point lookups
bool SQLiteEngine::tryFastPathMinimal(const std::string& sql, const std::vector<Value>& params,
                                       const uint8_t** outData, uint32_t* outLen,
                                       uint64_t* outSequence) {
    // Only intercept simple point queries with one parameter
    if (params.size() != 1) {
        return false;
    }

    // Normalize and check pattern: "select * from tablename where column = ?"
    std::string normalized = normalizeSQL(sql);

    // Check for "select * from"
    if (normalized.size() < 14 || normalized.substr(0, 14) != "select * from ") {
        return false;
    }

    // Find table name and "where column = ?"
    size_t fromEnd = normalized.find(" where ", 14);
    if (fromEnd == std::string::npos) {
        return false;
    }

    std::string tableName = normalized.substr(14, fromEnd - 14);

    // Remove quotes if present
    if (tableName.size() >= 2 && tableName.front() == '"' && tableName.back() == '"') {
        tableName = tableName.substr(1, tableName.size() - 2);
    }

    // Find the source
    auto* source = getSource(tableName);
    if (!source || !source->store || !source->tableDef) {
        return false;
    }

    // Parse "where column = ?"
    std::string whereClause = normalized.substr(fromEnd + 7);  // Skip " where "

    // Look for "columnname = ?" pattern
    size_t eqPos = whereClause.find(" = ?");
    if (eqPos == std::string::npos) {
        // Try without space: "columnname= ?"
        eqPos = whereClause.find("= ?");
        if (eqPos == std::string::npos) {
            return false;
        }
    }

    std::string columnName = whereClause.substr(0, eqPos);
    // Trim trailing spaces from column name
    while (!columnName.empty() && columnName.back() == ' ') {
        columnName.pop_back();
    }
    // Remove quotes if present
    if (columnName.size() >= 2 && columnName.front() == '"' && columnName.back() == '"') {
        columnName = columnName.substr(1, columnName.size() - 2);
    }

    // Check if we have an index on this column
    auto indexIt = source->indexes.find(columnName);
    if (indexIt == source->indexes.end() || !indexIt->second) {
        return false;  // No index, fall back to VTable
    }

    SqliteIndex* index = indexIt->second;
    const Value& searchValue = params[0];

    // Check tombstones set
    const auto* tombstones = &source->tombstones;

    // Do the lookup
    IndexEntry entry;
    if (!index->searchFirst(searchValue, entry)) {
        return false;  // No match
    }

    // Check tombstone
    if (!tombstones->empty() && tombstones->count(entry.sequence)) {
        return false;  // Tombstoned
    }

    // Get the data
    const uint8_t* data = source->store->getDataAtOffset(entry.dataOffset, outLen);
    if (!data) {
        return false;
    }

    *outData = data;
    if (outSequence) {
        *outSequence = entry.sequence;
    }
    return true;
}

}  // namespace flatsql
