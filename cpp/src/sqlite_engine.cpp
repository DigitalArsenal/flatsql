#include "flatsql/sqlite_engine.h"
#include "flatsql/geo_functions.h"
#include <algorithm>
#include <chrono>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <cctype>
#include <cstdio>
#include <cstring>

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

// setErr assigns an error message without ever letting the assignment itself
// escape. Used by the exception-free (`*NoThrow`) paths, which the C ABI calls
// on the `-fignore-exceptions` WASI artifact where a throw is an `unreachable`.
static void setErr(std::string* errOut, const std::string& message) noexcept {
    if (!errOut) {
        return;
    }
    try {
        *errOut = message;
    } catch (...) {
        // An error message that cannot be allocated is still an error; the
        // caller sees the false return, which is what it acts on.
    }
}

static bool isBusyResult(int rc) {
    return rc == SQLITE_BUSY || rc == SQLITE_LOCKED;
}

// Exception-free step. Returns the sqlite result code; a busy/locked result
// that survives the retry budget is returned AS a result code (never thrown),
// so no-eh callers can report it instead of trapping.
static int stepWithRetryNoThrow(sqlite3_stmt* stmt, const SQLiteConnectionOptions& options) noexcept {
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

    return rc;
}

static int stepWithRetry(sqlite3* db, sqlite3_stmt* stmt, const SQLiteConnectionOptions& options) {
    const int rc = stepWithRetryNoThrow(stmt, options);
    if (isBusyResult(rc)) {
        throw std::runtime_error("SQLite busy/locked after retries: " + std::string(sqlite3_errmsg(db)));
    }
    return rc;
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
    std::string err;
    sqlite3_stmt* stmt = getOrPrepareStmtNoThrow(sql, &err);
    if (!stmt) {
        throw std::runtime_error(err);
    }
    return stmt;
}

sqlite3_stmt* SQLiteEngine::getOrPrepareStmtNoThrow(const std::string& sql, std::string* errOut) const noexcept {
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
        setErr(errOut, "SQL error: " + std::string(sqlite3_errmsg(db_)));
        return nullptr;
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

bool SQLiteEngine::bindValueNoThrow(sqlite3_stmt* stmt, int idx, const Value& value,
                                    std::string* errOut) const noexcept {
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
        setErr(errOut, "SQLite bind error: " + std::string(sqlite3_errmsg(db_)));
        return false;
    }
    return true;
}

bool SQLiteEngine::validateSQL(const std::string& sql, int* paramCountOut, std::string* errOut) noexcept {
    // Exception-free validation path: plain sqlite3 C API only. The try/catch
    // exists solely to keep the noexcept contract on the exceptions build if a
    // string allocation fails; it compiles to dead code under -fignore-exceptions.
    try {
        if (paramCountOut) *paramCountOut = 0;
        if (errOut) errOut->clear();

        if (!db_) {
            if (errOut) *errOut = "SQL error: database handle is not open";
            return false;
        }

        int totalParams = 0;
        size_t statementCount = 0;
        const char* cursor = sql.c_str();
        const char* end = cursor + sql.size();

        while (cursor < end) {
            sqlite3_stmt* stmt = nullptr;
            const char* tail = nullptr;
            int rc = sqlite3_prepare_v2(db_, cursor, static_cast<int>(end - cursor), &stmt, &tail);
            if (rc != SQLITE_OK) {
                if (stmt) sqlite3_finalize(stmt);
                if (errOut) *errOut = "SQL error: " + std::string(sqlite3_errmsg(db_));
                return false;
            }

            if (stmt) {
                totalParams += sqlite3_bind_parameter_count(stmt);
                statementCount++;
                sqlite3_finalize(stmt);
            }

            if (!tail || tail == cursor) {
                break;
            }
            cursor = tail;
        }

        if (statementCount == 0) {
            if (errOut) *errOut = "SQL error: no SQL statement provided";
            return false;
        }

        if (paramCountOut) *paramCountOut = totalParams;
        return true;
    } catch (...) {
        if (errOut) {
            // Avoid allocating in this path; assign from a literal only if possible.
            try { *errOut = "SQL error: validation failed"; } catch (...) {}
        }
        return false;
    }
}

// ==================== Sandboxed public query (gateway loop G.5) ====================

namespace {

struct SandboxAuthCtx {
    const std::unordered_set<std::string>* allowedTables = nullptr;
    // Every real table/view name in the database (sqlite_master + temp
    // schema). SQLITE_READ on a name that is NOT a real schema object is a
    // CTE / subquery alias (e.g. the whole-table read SQLite issues for
    // `SELECT count(*) FROM cte`) and is safe to allow: if the name resolved
    // to nothing at all, prepare fails later anyway, and a CTE that SHADOWS
    // a real table name is still checked against the real name (false
    // rejection — the safe direction).
    const std::unordered_set<std::string>* existingObjects = nullptr;
    std::string violation;  // first denial, human-readable
};

// Human-readable names for denied authorizer actions.
const char* sandboxActionName(int action) {
    switch (action) {
        case SQLITE_PRAGMA: return "PRAGMA";
        case SQLITE_ATTACH: return "ATTACH";
        case SQLITE_DETACH: return "DETACH";
        case SQLITE_INSERT: return "INSERT";
        case SQLITE_UPDATE: return "UPDATE";
        case SQLITE_DELETE: return "DELETE";
        case SQLITE_TRANSACTION: return "TRANSACTION";
        case SQLITE_SAVEPOINT: return "SAVEPOINT";
        case SQLITE_ALTER_TABLE: return "ALTER TABLE";
        case SQLITE_REINDEX: return "REINDEX";
        case SQLITE_ANALYZE: return "ANALYZE";
        case SQLITE_CREATE_INDEX:
        case SQLITE_CREATE_TABLE:
        case SQLITE_CREATE_TRIGGER:
        case SQLITE_CREATE_VIEW:
        case SQLITE_CREATE_VTABLE: return "CREATE";
        case SQLITE_CREATE_TEMP_INDEX:
        case SQLITE_CREATE_TEMP_TABLE:
        case SQLITE_CREATE_TEMP_TRIGGER:
        case SQLITE_CREATE_TEMP_VIEW: return "CREATE TEMP";
        case SQLITE_DROP_INDEX:
        case SQLITE_DROP_TABLE:
        case SQLITE_DROP_TEMP_INDEX:
        case SQLITE_DROP_TEMP_TABLE:
        case SQLITE_DROP_TEMP_TRIGGER:
        case SQLITE_DROP_TEMP_VIEW:
        case SQLITE_DROP_TRIGGER:
        case SQLITE_DROP_VIEW:
        case SQLITE_DROP_VTABLE: return "DROP";
        default: return "this operation";
    }
}

int sandboxAuthorizer(void* userData, int action,
                      const char* arg1, const char* /*arg2*/,
                      const char* /*dbName*/, const char* /*trigger*/) {
    auto* ctx = static_cast<SandboxAuthCtx*>(userData);
    switch (action) {
        case SQLITE_SELECT:
        case SQLITE_FUNCTION:
        case SQLITE_RECURSIVE:
            return SQLITE_OK;
        case SQLITE_READ: {
            const char* table = arg1;
            if (table && ctx->allowedTables && ctx->allowedTables->count(table)) {
                return SQLITE_OK;
            }
            // sqlite_master / sqlite_temp_master / sqlite_sequence / ... are
            // reserved names that never appear in the schema enumeration —
            // deny them BEFORE the CTE allowance below.
            const bool reservedName = table && std::strncmp(table, "sqlite_", 7) == 0;
            if (!reservedName && table && ctx->existingObjects &&
                !ctx->existingObjects->count(table)) {
                return SQLITE_OK;  // CTE / subquery alias, not a real object
            }
            if (ctx->violation.empty()) {
                ctx->violation = std::string("table \"") + (table ? table : "?") +
                                 "\" is outside the public query surface";
            }
            return SQLITE_DENY;
        }
        default:
            if (ctx->violation.empty()) {
                ctx->violation = std::string(sandboxActionName(action)) +
                                 " is not permitted (read-only SELECT sandbox)";
            }
            return SQLITE_DENY;
    }
}

struct SandboxProgressCtx {
    std::chrono::steady_clock::time_point deadline;
    bool expired = false;
};

int sandboxProgress(void* userData) {
    auto* ctx = static_cast<SandboxProgressCtx*>(userData);
    if (std::chrono::steady_clock::now() >= ctx->deadline) {
        ctx->expired = true;
        return 1;  // abort with SQLITE_INTERRUPT
    }
    return 0;
}

// VDBE ops between progress-handler callbacks; small enough that a deadline
// overshoot is bounded, large enough to stay invisible on the hot path (the
// handler is only installed for sandboxed statements).
constexpr int kSandboxProgressOps = 4096;

void sandboxJsonEscapeInto(std::vector<uint8_t>* out, const char* s, size_t len) {
    static const char* hex = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        if (c == '"' || c == '\\') {
            out->push_back('\\');
            out->push_back(c);
        } else if (c == '\n') {
            out->push_back('\\'); out->push_back('n');
        } else if (c == '\r') {
            out->push_back('\\'); out->push_back('r');
        } else if (c == '\t') {
            out->push_back('\\'); out->push_back('t');
        } else if (c < 0x20) {
            const char buf[6] = {'\\', 'u', '0', '0', hex[(c >> 4) & 0xF], hex[c & 0xF]};
            out->insert(out->end(), buf, buf + 6);
        } else {
            out->push_back(c);
        }
    }
}

void sandboxBase64Into(std::vector<uint8_t>* out, const uint8_t* data, size_t len) {
    static const char* alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t i = 0;
    for (; i + 2 < len; i += 3) {
        const uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                           (static_cast<uint32_t>(data[i + 1]) << 8) |
                           static_cast<uint32_t>(data[i + 2]);
        out->push_back(alphabet[(n >> 18) & 63]);
        out->push_back(alphabet[(n >> 12) & 63]);
        out->push_back(alphabet[(n >> 6) & 63]);
        out->push_back(alphabet[n & 63]);
    }
    if (i + 1 == len) {
        const uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        out->push_back(alphabet[(n >> 18) & 63]);
        out->push_back(alphabet[(n >> 12) & 63]);
        out->push_back('=');
        out->push_back('=');
    } else if (i + 2 == len) {
        const uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                           (static_cast<uint32_t>(data[i + 1]) << 8);
        out->push_back(alphabet[(n >> 18) & 63]);
        out->push_back(alphabet[(n >> 12) & 63]);
        out->push_back(alphabet[(n >> 6) & 63]);
        out->push_back('=');
    }
}

void sandboxAppendLiteral(std::vector<uint8_t>* out, const char* s) {
    out->insert(out->end(), s, s + std::strlen(s));
}

}  // namespace

bool SQLiteEngine::executeSandboxed(const std::string& sql,
                                    const std::vector<Value>& params,
                                    const std::unordered_set<std::string>& allowedTables,
                                    SandboxMode mode,
                                    const SandboxLimits& limits,
                                    SandboxOutput* out,
                                    std::string* errOut) noexcept {
    // The try/catch keeps the noexcept contract on the exceptions build if a
    // string/vector allocation fails; under -fignore-exceptions it is dead
    // code (allocation failure aborts, like everywhere else in the engine).
    try {
        const auto fail = [&](const std::string& message) {
            if (errOut) *errOut = message;
            return false;
        };
        if (!db_) {
            return fail("SQL error: database handle is not open");
        }
        if (out) {
            out->payload.clear();
            out->rowCount = 0;
            out->columnCount = 0;
        }

        // Real schema objects (tables, views, indexes, vtabs), collected so
        // the authorizer can tell CTE/subquery aliases from real tables.
        // FAIL CLOSED: if enumeration fails the CTE allowance is disabled
        // entirely (unknown names are then denied) — never fail open.
        std::unordered_set<std::string> existingObjects;
        bool schemaEnumerated = false;
        const auto enumerateSchema = [&](const char* schemaSql) {
            sqlite3_stmt* schemaStmt = nullptr;
            bool clean = false;
            if (sqlite3_prepare_v2(db_, schemaSql, -1, &schemaStmt, nullptr) == SQLITE_OK) {
                int stepRc;
                while ((stepRc = sqlite3_step(schemaStmt)) == SQLITE_ROW) {
                    const char* name =
                        reinterpret_cast<const char*>(sqlite3_column_text(schemaStmt, 0));
                    if (name) existingObjects.insert(name);
                }
                clean = (stepRc == SQLITE_DONE);
            }
            if (schemaStmt) sqlite3_finalize(schemaStmt);
            return clean;
        };
        // The main schema must enumerate cleanly; the temp schema is
        // best-effort (it may not exist before first temp use, and temp
        // writes are denied by the authorizer anyway).
        schemaEnumerated = enumerateSchema("SELECT name FROM sqlite_master");
        enumerateSchema("SELECT name FROM sqlite_temp_master");

        // Layer 1: authorizer during prepare — deny everything but
        // SELECT/READ-on-allowlist/functions. Cleared before returning.
        SandboxAuthCtx auth;
        auth.allowedTables = &allowedTables;
        auth.existingObjects = schemaEnumerated ? &existingObjects : nullptr;
        sqlite3_set_authorizer(db_, sandboxAuthorizer, &auth);

        sqlite3_stmt* stmt = nullptr;
        const char* tail = nullptr;
        const int prepareRc =
            sqlite3_prepare_v2(db_, sql.c_str(), static_cast<int>(sql.size()), &stmt, &tail);
        sqlite3_set_authorizer(db_, nullptr, nullptr);

        if (prepareRc != SQLITE_OK) {
            if (stmt) sqlite3_finalize(stmt);
            if (!auth.violation.empty()) {
                return fail("sandbox: not-authorized: " + auth.violation);
            }
            return fail("SQL error: " + std::string(sqlite3_errmsg(db_)));
        }
        if (!stmt) {
            return fail("sandbox: empty-statement: no SQL statement provided");
        }

        // Layer 2: single statement — reject any non-whitespace tail
        // (including trailing comments; the public contract is one bare
        // SELECT).
        for (const char* p = tail; p && *p; p++) {
            if (!std::isspace(static_cast<unsigned char>(*p))) {
                sqlite3_finalize(stmt);
                return fail("sandbox: multi-statement: exactly one SELECT statement is allowed");
            }
        }

        // Layer 3: SELECT-shaped and structurally read-only.
        if (!sqlite3_stmt_readonly(stmt)) {
            sqlite3_finalize(stmt);
            return fail("sandbox: read-only: statement could modify the database");
        }
        const int numCols = sqlite3_column_count(stmt);
        if (numCols <= 0) {
            sqlite3_finalize(stmt);
            return fail("sandbox: not-select: statement returns no result columns");
        }

        const int expectedParams = sqlite3_bind_parameter_count(stmt);
        if (expectedParams != static_cast<int>(params.size())) {
            sqlite3_finalize(stmt);
            return fail("sandbox: params: SQL statement expects " +
                        std::to_string(expectedParams) + " parameters but received " +
                        std::to_string(params.size()));
        }
        for (size_t i = 0; i < params.size(); i++) {
            int rc = SQLITE_OK;
            const Value& value = params[i];
            const int idx = static_cast<int>(i + 1);
            std::visit([&](const auto& v) {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, std::monostate>) {
                    rc = sqlite3_bind_null(stmt, idx);
                } else if constexpr (std::is_same_v<T, bool>) {
                    rc = sqlite3_bind_int(stmt, idx, v ? 1 : 0);
                } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                                     std::is_same_v<T, int32_t> || std::is_same_v<T, uint8_t> ||
                                     std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t>) {
                    rc = sqlite3_bind_int(stmt, idx, static_cast<int>(v));
                } else if constexpr (std::is_same_v<T, int64_t>) {
                    rc = sqlite3_bind_int64(stmt, idx, v);
                } else if constexpr (std::is_same_v<T, uint64_t>) {
                    rc = sqlite3_bind_int64(stmt, idx, static_cast<sqlite3_int64>(v));
                } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
                    rc = sqlite3_bind_double(stmt, idx, static_cast<double>(v));
                } else if constexpr (std::is_same_v<T, std::string>) {
                    rc = sqlite3_bind_text(stmt, idx, v.c_str(), static_cast<int>(v.size()),
                                           SQLITE_TRANSIENT);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    rc = sqlite3_bind_blob(stmt, idx, v.empty() ? nullptr : v.data(),
                                           static_cast<int>(v.size()), SQLITE_TRANSIENT);
                } else {
                    rc = sqlite3_bind_null(stmt, idx);
                }
            }, value);
            if (rc != SQLITE_OK) {
                sqlite3_finalize(stmt);
                return fail("SQL error: " + std::string(sqlite3_errmsg(db_)));
            }
        }

        // Layer 4: statement deadline via the progress handler.
        SandboxProgressCtx progress;
        if (limits.timeoutMs > 0) {
            progress.deadline = std::chrono::steady_clock::now() +
                                std::chrono::milliseconds(limits.timeoutMs);
            sqlite3_progress_handler(db_, kSandboxProgressOps, sandboxProgress, &progress);
        }

        std::vector<uint8_t> payload;
        size_t rowCount = 0;
        std::string failure;

        // JSON column-name prefixes (escaped once), verbatim from
        // sqlite3_column_name — schema-exact capitalization by construction.
        std::vector<std::vector<uint8_t>> jsonKeys;
        if (mode == SandboxMode::JsonRows) {
            jsonKeys.resize(static_cast<size_t>(numCols));
            for (int i = 0; i < numCols; i++) {
                std::vector<uint8_t>& key = jsonKeys[static_cast<size_t>(i)];
                key.push_back(i == 0 ? '{' : ',');
                key.push_back('"');
                const char* name = sqlite3_column_name(stmt, i);
                sandboxJsonEscapeInto(&key, name ? name : "", name ? std::strlen(name) : 0);
                key.push_back('"');
                key.push_back(':');
            }
            payload.push_back('[');
        }

        int rc;
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            rowCount++;
            // Layer 5a: row cap (reject, never truncate).
            if (limits.maxRows > 0 && rowCount > limits.maxRows) {
                failure = "sandbox: row-cap: result exceeds " +
                          std::to_string(limits.maxRows) + " rows";
                break;
            }
            if (mode == SandboxMode::RecordStream) {
                for (int i = 0; i < numCols; i++) {
                    if (sqlite3_column_type(stmt, i) != SQLITE_BLOB) {
                        failure = "sandbox: not-a-record-stream: raw response stream queries "
                                  "must return only BLOB cells (projection results are "
                                  "JSON-only — request format=json)";
                        break;
                    }
                    const uint8_t* blob = static_cast<const uint8_t*>(sqlite3_column_blob(stmt, i));
                    const int len = sqlite3_column_bytes(stmt, i);
                    const uint32_t size = static_cast<uint32_t>(len);
                    payload.push_back(static_cast<uint8_t>(size & 0xFF));
                    payload.push_back(static_cast<uint8_t>((size >> 8) & 0xFF));
                    payload.push_back(static_cast<uint8_t>((size >> 16) & 0xFF));
                    payload.push_back(static_cast<uint8_t>((size >> 24) & 0xFF));
                    if (len > 0) payload.insert(payload.end(), blob, blob + len);
                }
            } else {
                if (rowCount > 1) payload.push_back(',');
                for (int i = 0; i < numCols; i++) {
                    const std::vector<uint8_t>& key = jsonKeys[static_cast<size_t>(i)];
                    payload.insert(payload.end(), key.begin(), key.end());
                    switch (sqlite3_column_type(stmt, i)) {
                        case SQLITE_NULL:
                            sandboxAppendLiteral(&payload, "null");
                            break;
                        case SQLITE_INTEGER: {
                            char buf[32];
                            std::snprintf(buf, sizeof(buf), "%lld",
                                          static_cast<long long>(sqlite3_column_int64(stmt, i)));
                            sandboxAppendLiteral(&payload, buf);
                            break;
                        }
                        case SQLITE_FLOAT: {
                            const double v = sqlite3_column_double(stmt, i);
                            if (v != v || v > 1.7976931348623157e308 ||
                                v < -1.7976931348623157e308) {
                                sandboxAppendLiteral(&payload, "null");  // NaN/Inf have no JSON
                            } else {
                                char buf[40];
                                std::snprintf(buf, sizeof(buf), "%.17g", v);
                                sandboxAppendLiteral(&payload, buf);
                            }
                            break;
                        }
                        case SQLITE_TEXT: {
                            const char* text =
                                reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
                            const int len = sqlite3_column_bytes(stmt, i);
                            payload.push_back('"');
                            sandboxJsonEscapeInto(&payload, text ? text : "",
                                                  static_cast<size_t>(len));
                            payload.push_back('"');
                            break;
                        }
                        case SQLITE_BLOB: {
                            const uint8_t* blob =
                                static_cast<const uint8_t*>(sqlite3_column_blob(stmt, i));
                            const int len = sqlite3_column_bytes(stmt, i);
                            payload.push_back('"');
                            sandboxBase64Into(&payload, blob, static_cast<size_t>(len));
                            payload.push_back('"');
                            break;
                        }
                        default:
                            sandboxAppendLiteral(&payload, "null");
                            break;
                    }
                }
                payload.push_back('}');
            }
            if (!failure.empty()) break;
            // Layer 5b: byte cap on the assembled payload.
            if (limits.maxBytes > 0 && payload.size() > limits.maxBytes) {
                failure = "sandbox: byte-cap: result exceeds " +
                          std::to_string(limits.maxBytes) + " bytes";
                break;
            }
        }

        if (limits.timeoutMs > 0) {
            sqlite3_progress_handler(db_, 0, nullptr, nullptr);
        }

        if (failure.empty() && rc != SQLITE_DONE && rc != SQLITE_ROW) {
            if (progress.expired || rc == SQLITE_INTERRUPT) {
                failure = "sandbox: timeout: statement exceeded " +
                          std::to_string(limits.timeoutMs) + " ms";
            } else {
                failure = "SQL error: " + std::string(sqlite3_errmsg(db_));
            }
        }
        sqlite3_finalize(stmt);
        if (!failure.empty()) {
            return fail(failure);
        }

        if (mode == SandboxMode::JsonRows) {
            payload.push_back(']');
            if (limits.maxBytes > 0 && payload.size() > limits.maxBytes) {
                return fail("sandbox: byte-cap: result exceeds " +
                            std::to_string(limits.maxBytes) + " bytes");
            }
        }

        if (out) {
            out->payload = std::move(payload);
            out->rowCount = rowCount;
            out->columnCount = static_cast<size_t>(numCols);
        }
        if (errOut) errOut->clear();
        return true;
    } catch (...) {
        if (errOut) {
            try { *errOut = "sandbox: internal: sandboxed execution failed"; } catch (...) {}
        }
        return false;
    }
}

QueryResult SQLiteEngine::execute(const std::string& sql) {
    return execute(sql, {});
}

QueryResult SQLiteEngine::execute(const std::string& sql, const std::vector<Value>& params) {
    QueryResult result;
    std::string error;
    if (!executeNoThrow(sql, params, result, &error)) {
        throw std::runtime_error(error);
    }
    return result;
}

bool SQLiteEngine::executeNoThrow(const std::string& sql, const std::vector<Value>& params,
                                  QueryResult& out, std::string* errOut) noexcept try {
    QueryResult result;

    // Try fast path for simple queries
    if (tryFastPath(sql, params, result)) {
        out = std::move(result);
        return true;
    }

    // Use cached prepared statement
    sqlite3_stmt* stmt = getOrPrepareStmtNoThrow(sql, errOut);
    if (!stmt) {
        return false;
    }
    int expectedParams = sqlite3_bind_parameter_count(stmt);
    if (expectedParams != static_cast<int>(params.size())) {
        setErr(errOut,
            "SQL statement expects " + std::to_string(expectedParams) +
            " parameters but received " + std::to_string(params.size())
        );
        return false;
    }
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    // Bind parameters
    for (size_t i = 0; i < params.size(); i++) {
        if (!bindValueNoThrow(stmt, static_cast<int>(i + 1), params[i], errOut)) {
            return false;
        }
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
    while ((rc = stepWithRetryNoThrow(stmt, options_)) == SQLITE_ROW) {
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
        // Constraint violations, busy/locked-after-retries, IO and corruption
        // errors all land here. This is the exact frame that used to trap the
        // guest under -fignore-exceptions (host-01 record-catalog hydration,
        // graph task mod-flatsql-query-params-unreachable-trap): a SQL error
        // is a RESULT, not an abort.
        setErr(errOut, "SQL execution error: " + std::string(sqlite3_errmsg(db_)));
        // Leave no bound state behind for the next user of the cached stmt.
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        return false;
    }

    out = std::move(result);
    return true;
} catch (const std::exception& e) {
    // Exceptions build only: a non-SQL internal failure still leaves through
    // the value channel so both artifacts behave identically.
    setErr(errOut, e.what());
    return false;
} catch (...) {
    setErr(errOut, "SQL execution error: internal failure");
    return false;
}

bool SQLiteEngine::statementIsReadOnly(const std::string& sql) const {
    sqlite3_stmt* stmt = getOrPrepareStmt(sql);
    return sqlite3_stmt_readonly(stmt) != 0;
}

bool SQLiteEngine::statementIsReadOnlyNoThrow(const std::string& sql) const noexcept {
    sqlite3_stmt* stmt = getOrPrepareStmtNoThrow(sql, nullptr);
    // Unpreparable statement: assume it wrote, so caches are invalidated rather
    // than left stale. Never a trap.
    return stmt != nullptr && sqlite3_stmt_readonly(stmt) != 0;
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
