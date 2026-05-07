#include "flatsql/sqlite_engine.h"
#include "flatsql/geo_functions.h"
#include <algorithm>
#include <chrono>
#include <sstream>
#include <stdexcept>
#include <thread>
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

static std::string sqliteModuleNameForSource(const std::string& sourceName) {
    std::string sanitized;
    sanitized.reserve(sourceName.size());
    for (unsigned char c : sourceName) {
        if (std::isalnum(c)) {
            sanitized.push_back(static_cast<char>(std::tolower(c)));
        } else {
            sanitized.push_back('_');
        }
    }
    if (sanitized.empty()) {
        sanitized = "source";
    }
    return "__flatsql_module_" + sanitized;
}

static bool isBusyResult(int rc) {
    return rc == SQLITE_BUSY || rc == SQLITE_LOCKED;
}

static int stepWithRetry(sqlite3* db, sqlite3_stmt* stmt, const SQLiteConnectionOptions& options) {
    int rc = SQLITE_OK;
    int delayMs = std::max(1, options.busyBackoffMs);

    for (int attempt = 0; attempt <= options.maxBusyRetries; attempt++) {
        rc = sqlite3_step(stmt);
        if (!isBusyResult(rc)) {
            return rc;
        }
        if (attempt == options.maxBusyRetries) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        delayMs = std::min(delayMs * 2, 32);
    }

    throw std::runtime_error("SQLite busy/locked after retries: " + std::string(sqlite3_errmsg(db)));
}

static void execOrThrow(sqlite3* db, const char* sql) {
    char* errMsg = nullptr;
    int rc = sqlite3_exec(db, sql, nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        std::string error = errMsg ? errMsg : sqlite3_errmsg(db);
        sqlite3_free(errMsg);
        throw std::runtime_error(error);
    }
}

static std::string trimCopy(std::string value) {
    auto isNotSpace = [](unsigned char c) { return !std::isspace(c); };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), isNotSpace));
    value.erase(std::find_if(value.rbegin(), value.rend(), isNotSpace).base(), value.end());
    return value;
}

static void stripIdentifierQuotes(std::string& identifier) {
    if (identifier.size() >= 2 && identifier.front() == '"' && identifier.back() == '"') {
        identifier = identifier.substr(1, identifier.size() - 2);
    }
}

static bool parsePointPredicate(const std::string& whereClause, std::string& columnName) {
    size_t eqPos = whereClause.find(" = ?");
    size_t tokenLength = 4;
    if (eqPos == std::string::npos) {
        eqPos = whereClause.find("= ?");
        tokenLength = 3;
    }
    if (eqPos == std::string::npos) {
        return false;
    }

    std::string trailing = trimCopy(whereClause.substr(eqPos + tokenLength));
    if (trailing == ";") {
        trailing.clear();
    }
    if (!trailing.empty()) {
        return false;
    }

    columnName = trimCopy(whereClause.substr(0, eqPos));
    stripIdentifierQuotes(columnName);
    return !columnName.empty();
}

SQLiteEngine::SQLiteEngine(SQLiteConnectionOptions options)
    : db_(nullptr)
    , options_(std::move(options)) {
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
    int rc = sqlite3_open_v2(options_.path.c_str(), &db_, flags, nullptr);
    if (rc != SQLITE_OK) {
        std::string error = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        db_ = nullptr;
        throw std::runtime_error("Failed to open SQLite database: " + error);
    }

    sqlite3_extended_result_codes(db_, 1);
    if (options_.busyTimeoutMs > 0) {
        sqlite3_busy_timeout(db_, options_.busyTimeoutMs);
    }

    if (options_.enableWal && options_.path != ":memory:") {
        execOrThrow(db_, "PRAGMA journal_mode=WAL");
        execOrThrow(db_, "PRAGMA synchronous=NORMAL");
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
    clearFastPathCaches();
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

void SQLiteEngine::clearFastPathCaches() {
    sourceNameCache_.clear();
    parsedQueryCache_.clear();
    columnNamesCache_.clear();
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
    : db_(other.db_)
    , options_(std::move(other.options_))
    , sources_(std::move(other.sources_))
    , stmtCache_(std::move(other.stmtCache_))
    , sourceNameCache_(std::move(other.sourceNameCache_))
    , parsedQueryCache_(std::move(other.parsedQueryCache_))
    , columnNamesCache_(std::move(other.columnNamesCache_)) {
    other.db_ = nullptr;
}

SQLiteEngine& SQLiteEngine::operator=(SQLiteEngine&& other) noexcept {
    if (this != &other) {
        clearStmtCache();
        clearFastPathCaches();
        if (db_) {
            sqlite3_close(db_);
        }
        db_ = other.db_;
        options_ = std::move(other.options_);
        sources_ = std::move(other.sources_);
        stmtCache_ = std::move(other.stmtCache_);
        sourceNameCache_ = std::move(other.sourceNameCache_);
        parsedQueryCache_ = std::move(other.parsedQueryCache_);
        columnNamesCache_ = std::move(other.columnNamesCache_);
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
    const std::string moduleName = sqliteModuleNameForSource(sourceName);
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
        moduleName.c_str(),
        FlatBufferVTabModule::getModule(),
        &infoPtr->vtabInfo,
        nullptr  // No destructor - we manage lifetime
    );

    if (rc != SQLITE_OK) {
        sources_.erase(sourceName);
        throw std::runtime_error("Failed to create SQLite module: " + std::string(sqlite3_errmsg(db_)));
    }

    // TEMP schema DDL can hang in the WASM/SQLite build, so keep virtual tables
    // in the main schema and reserve TEMP only for caller-managed objects.
    std::ostringstream sql;
    sql << "CREATE VIRTUAL TABLE IF NOT EXISTS \"" << sourceName << "\" USING \"" << moduleName << "\"()";

    char* errMsg = nullptr;
    rc = sqlite3_exec(db_, sql.str().c_str(), nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
        std::string error = errMsg ? errMsg : "Unknown error";
        sqlite3_free(errMsg);
        sources_.erase(sourceName);
        throw std::runtime_error("Failed to create virtual table: " + error);
    }

    clearFastPathCaches();
    clearStmtCache();
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

    // Replace the base virtual table name with a unified view in the main schema.
    // TEMP schema DDL can hang in the WASM/SQLite build, so keep all runtime objects
    // in the same schema.
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
    int rc = SQLITE_OK;
    std::visit([&](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            rc = sqlite3_bind_null(stmt, idx);
        } else if constexpr (std::is_same_v<T, bool>) {
            rc = sqlite3_bind_int(stmt, idx, v ? 1 : 0);
        } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t>) {
            rc = sqlite3_bind_int(stmt, idx, static_cast<int>(v));
        } else if constexpr (std::is_same_v<T, int64_t>) {
            rc = sqlite3_bind_int64(stmt, idx, v);
        } else if constexpr (std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t>) {
            rc = sqlite3_bind_int(stmt, idx, static_cast<int>(v));
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            rc = sqlite3_bind_int64(stmt, idx, static_cast<sqlite3_int64>(v));
        } else if constexpr (std::is_same_v<T, float>) {
            rc = sqlite3_bind_double(stmt, idx, static_cast<double>(v));
        } else if constexpr (std::is_same_v<T, double>) {
            rc = sqlite3_bind_double(stmt, idx, v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            rc = sqlite3_bind_text(stmt, idx, v.c_str(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            rc = sqlite3_bind_blob(stmt, idx, v.empty() ? nullptr : v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
        } else {
            rc = sqlite3_bind_null(stmt, idx);
        }
    }, value);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("SQLite bind error: " + std::string(sqlite3_errmsg(db_)));
    }
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
    int expectedParams = sqlite3_bind_parameter_count(stmt);
    if (expectedParams != static_cast<int>(params.size())) {
        throw std::runtime_error(
            "SQL statement expects " + std::to_string(expectedParams) +
            " parameters but received " + std::to_string(params.size())
        );
    }
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

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
    while ((rc = stepWithRetry(db_, stmt, options_)) == SQLITE_ROW) {
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
    int expectedParams = sqlite3_bind_parameter_count(stmt);
    if (expectedParams != static_cast<int>(params.size())) {
        throw std::runtime_error(
            "SQL statement expects " + std::to_string(expectedParams) +
            " parameters but received " + std::to_string(params.size())
        );
    }
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    // Bind parameters
    for (size_t i = 0; i < params.size(); i++) {
        bindValue(stmt, static_cast<int>(i + 1), params[i]);
    }

    // Just step through - read columns but don't copy to QueryResult
    size_t rowCount = 0;
    int numCols = sqlite3_column_count(stmt);
    int rc;
    while ((rc = stepWithRetry(db_, stmt, options_)) == SQLITE_ROW) {
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

    if (rc != SQLITE_DONE) {
        throw std::runtime_error("SQL execution error: " + std::string(sqlite3_errmsg(db_)));
    }

    return rowCount;
}

static int fastPathCountHits = 0;

// Helper to get cached column names for a source
const std::vector<std::string>& SQLiteEngine::getCachedColumnNames(const SourceInfo* source) {
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
            stripIdentifierQuotes(pq.tableName);
        } else {
            pq.tableName = normalized.substr(14, wherePos - 14);
            pq.tableName = trimCopy(pq.tableName);
            stripIdentifierQuotes(pq.tableName);

            // Parse column name
            std::string whereClause = normalized.substr(wherePos + 7);
            if (!parsePointPredicate(whereClause, pq.columnName)) {
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
            stripIdentifierQuotes(pq.tableName);
        } else {
            pq.tableName = normalized.substr(14, wherePos - 14);
            pq.tableName = trimCopy(pq.tableName);
            stripIdentifierQuotes(pq.tableName);

            std::string whereClause = normalized.substr(wherePos + 7);
            if (!parsePointPredicate(whereClause, pq.columnName)) {
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
    tableName = trimCopy(tableName);
    stripIdentifierQuotes(tableName);

    // Find the source
    auto* source = getSource(tableName);
    if (!source || !source->store || !source->tableDef) {
        return false;
    }

    // Parse "where column = ?"
    std::string whereClause = normalized.substr(fromEnd + 7);  // Skip " where "

    std::string columnName;
    if (!parsePointPredicate(whereClause, columnName)) {
        return false;
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
