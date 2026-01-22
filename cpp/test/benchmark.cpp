// Benchmark: FlatSQL vs SQLite
// Compares ingest speed, query performance, and memory usage

#include "flatsql/database.h"
#include "../schemas/test_schema_generated.h"
#include <sqlite3.h>
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <cstring>

using namespace flatsql;
using namespace std::chrono;

// Configuration
constexpr int RECORD_COUNT = 10000;
constexpr int QUERY_ITERATIONS = 500;
constexpr int WARMUP_ITERATIONS = 50;

// Timer helper
class Timer {
public:
    void start() { start_ = high_resolution_clock::now(); }
    void stop() { end_ = high_resolution_clock::now(); }
    double ms() const { return duration_cast<microseconds>(end_ - start_).count() / 1000.0; }
    double us() const { return duration_cast<microseconds>(end_ - start_).count(); }
private:
    high_resolution_clock::time_point start_, end_;
};

// Create FlatBuffer User
std::vector<uint8_t> createUserFlatBuffer(int32_t id, const std::string& name,
                                           const std::string& email, int32_t age) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto user = test::CreateUserDirect(builder, id, name.c_str(), email.c_str(), age);
    builder.Finish(user, "USER");
    const uint8_t* buf = builder.GetBufferPointer();
    size_t size = builder.GetSize();
    return std::vector<uint8_t>(buf, buf + size);
}

// Field extractor for FlatSQL
Value extractUserField(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    auto user = test::GetUser(data);
    if (!user) return std::monostate{};

    if (fieldName == "id") return user->id();
    if (fieldName == "name") return user->name() ? std::string(user->name()->c_str()) : std::string();
    if (fieldName == "email") return user->email() ? std::string(user->email()->c_str()) : std::string();
    if (fieldName == "age") return user->age();

    return std::monostate{};
}

// SQLite helper
class SQLiteDB {
public:
    SQLiteDB() {
        sqlite3_open(":memory:", &db_);
        exec("PRAGMA journal_mode = OFF");
        exec("PRAGMA synchronous = OFF");
        exec("PRAGMA cache_size = 10000");
        exec("PRAGMA temp_store = MEMORY");
    }

    ~SQLiteDB() {
        if (db_) sqlite3_close(db_);
    }

    void exec(const char* sql) {
        char* err = nullptr;
        sqlite3_exec(db_, sql, nullptr, nullptr, &err);
        if (err) {
            std::cerr << "SQLite error: " << err << std::endl;
            sqlite3_free(err);
        }
    }

    sqlite3_stmt* prepare(const char* sql) {
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        return stmt;
    }

    sqlite3* db() { return db_; }

private:
    sqlite3* db_ = nullptr;
};

// Generate test data
struct TestRecord {
    int32_t id;
    std::string name;
    std::string email;
    int32_t age;
};

std::vector<TestRecord> generateTestData(int count) {
    std::vector<TestRecord> records;
    records.reserve(count);

    std::mt19937 rng(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<int> ageDist(18, 80);

    for (int i = 0; i < count; i++) {
        TestRecord r;
        r.id = i;
        r.name = "User" + std::to_string(i);
        r.email = "user" + std::to_string(i) + "@example.com";
        r.age = ageDist(rng);
        records.push_back(r);
    }

    return records;
}

void printHeader(const char* title) {
    std::cout << "\n" << std::string(60, '=') << "\n";
    std::cout << title << "\n";
    std::cout << std::string(60, '=') << "\n";
}

void printResult(const char* label, double flatsqlMs, double sqliteMs) {
    double ratio = sqliteMs / flatsqlMs;
    const char* winner = ratio > 1.0 ? "FlatSQL" : "SQLite";
    double factor = ratio > 1.0 ? ratio : 1.0 / ratio;

    std::cout << std::left << std::setw(25) << label
              << std::right << std::setw(12) << std::fixed << std::setprecision(2) << flatsqlMs << " ms"
              << std::setw(12) << sqliteMs << " ms"
              << "  " << winner << " " << std::setprecision(1) << factor << "x faster\n";
}

int main() {
    std::cout << "FlatSQL vs SQLite Benchmark\n";
    std::cout << "Records: " << RECORD_COUNT << "\n";
    std::cout << "Query iterations: " << QUERY_ITERATIONS << "\n";

    // Generate test data
    std::cout << "\nGenerating test data...";
    auto testData = generateTestData(RECORD_COUNT);
    std::cout << " done\n";

    // Pre-build FlatBuffers (this is realistic - data arrives pre-serialized)
    std::cout << "Pre-building FlatBuffers...";
    std::vector<std::vector<uint8_t>> flatBuffers;
    flatBuffers.reserve(RECORD_COUNT);
    for (const auto& r : testData) {
        flatBuffers.push_back(createUserFlatBuffer(r.id, r.name, r.email, r.age));
    }
    std::cout << " done\n";

    Timer timer;

    // ==================== INGEST BENCHMARK ====================
    printHeader("INGEST BENCHMARK");
    std::cout << std::left << std::setw(25) << "Operation"
              << std::right << std::setw(15) << "FlatSQL"
              << std::setw(15) << "SQLite"
              << "  Winner\n";
    std::cout << std::string(60, '-') << "\n";

    // FlatSQL ingest
    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto flatsqlDb = FlatSQLDatabase::fromSchema(schema, "benchmark");
    flatsqlDb.registerFileId("USER", "User");
    flatsqlDb.setFieldExtractor("User", extractUserField);

    timer.start();
    for (const auto& fb : flatBuffers) {
        flatsqlDb.ingestOne(fb.data(), fb.size());
    }
    timer.stop();
    double flatsqlIngestMs = timer.ms();

    // SQLite ingest
    SQLiteDB sqliteDb;
    sqliteDb.exec("CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)");
    sqliteDb.exec("CREATE INDEX idx_email ON User(email)");

    timer.start();
    sqliteDb.exec("BEGIN TRANSACTION");
    auto insertStmt = sqliteDb.prepare("INSERT INTO User (id, name, email, age) VALUES (?, ?, ?, ?)");
    for (const auto& r : testData) {
        sqlite3_bind_int(insertStmt, 1, r.id);
        sqlite3_bind_text(insertStmt, 2, r.name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(insertStmt, 3, r.email.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(insertStmt, 4, r.age);
        sqlite3_step(insertStmt);
        sqlite3_reset(insertStmt);
    }
    sqlite3_finalize(insertStmt);
    sqliteDb.exec("COMMIT");
    timer.stop();
    double sqliteIngestMs = timer.ms();

    printResult("Ingest (indexed)", flatsqlIngestMs, sqliteIngestMs);

    double flatsqlRecordsPerSec = RECORD_COUNT / (flatsqlIngestMs / 1000.0);
    double sqliteRecordsPerSec = RECORD_COUNT / (sqliteIngestMs / 1000.0);
    std::cout << "\nThroughput:\n";
    std::cout << "  FlatSQL: " << std::fixed << std::setprecision(0) << flatsqlRecordsPerSec << " records/sec\n";
    std::cout << "  SQLite:  " << sqliteRecordsPerSec << " records/sec\n";

    // ==================== QUERY BENCHMARK ====================
    printHeader("QUERY BENCHMARK");
    std::cout << std::left << std::setw(25) << "Operation"
              << std::right << std::setw(15) << "FlatSQL"
              << std::setw(15) << "SQLite"
              << "  Winner\n";
    std::cout << std::string(60, '-') << "\n";

    std::mt19937 rng(123);
    std::uniform_int_distribution<int> idDist(0, RECORD_COUNT - 1);

    // Point query by ID (indexed)
    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        int id = idDist(rng);
        flatsqlDb.query("SELECT * FROM User WHERE id = " + std::to_string(id));
    }

    timer.start();
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        int id = idDist(rng);
        auto result = flatsqlDb.query("SELECT * FROM User WHERE id = " + std::to_string(id));
        (void)result;
    }
    timer.stop();
    double flatsqlPointQueryMs = timer.ms();

    auto selectByIdStmt = sqliteDb.prepare("SELECT * FROM User WHERE id = ?");
    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        int id = idDist(rng);
        sqlite3_bind_int(selectByIdStmt, 1, id);
        sqlite3_step(selectByIdStmt);
        sqlite3_reset(selectByIdStmt);
    }

    timer.start();
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        int id = idDist(rng);
        sqlite3_bind_int(selectByIdStmt, 1, id);
        sqlite3_step(selectByIdStmt);
        sqlite3_reset(selectByIdStmt);
    }
    timer.stop();
    sqlite3_finalize(selectByIdStmt);
    double sqlitePointQueryMs = timer.ms();

    printResult("Point query (by id)", flatsqlPointQueryMs, sqlitePointQueryMs);

    // Point query by email (indexed)
    timer.start();
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        int id = idDist(rng);
        std::string email = "user" + std::to_string(id) + "@example.com";
        auto result = flatsqlDb.query("SELECT * FROM User WHERE email = '" + email + "'");
        (void)result;
    }
    timer.stop();
    double flatsqlEmailQueryMs = timer.ms();

    auto selectByEmailStmt = sqliteDb.prepare("SELECT * FROM User WHERE email = ?");
    timer.start();
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        int id = idDist(rng);
        std::string email = "user" + std::to_string(id) + "@example.com";
        sqlite3_bind_text(selectByEmailStmt, 1, email.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(selectByEmailStmt);
        sqlite3_reset(selectByEmailStmt);
    }
    timer.stop();
    sqlite3_finalize(selectByEmailStmt);
    double sqliteEmailQueryMs = timer.ms();

    printResult("Point query (by email)", flatsqlEmailQueryMs, sqliteEmailQueryMs);

    // Full table scan
    timer.start();
    for (int i = 0; i < 10; i++) {  // Fewer iterations for full scan
        auto result = flatsqlDb.query("SELECT * FROM User");
        (void)result;
    }
    timer.stop();
    double flatsqlScanMs = timer.ms() / 10;

    auto selectAllStmt = sqliteDb.prepare("SELECT * FROM User");
    timer.start();
    for (int i = 0; i < 10; i++) {
        while (sqlite3_step(selectAllStmt) == SQLITE_ROW) {
            // Read all columns
            sqlite3_column_int(selectAllStmt, 0);
            sqlite3_column_text(selectAllStmt, 1);
            sqlite3_column_text(selectAllStmt, 2);
            sqlite3_column_int(selectAllStmt, 3);
        }
        sqlite3_reset(selectAllStmt);
    }
    timer.stop();
    sqlite3_finalize(selectAllStmt);
    double sqliteScanMs = timer.ms() / 10;

    printResult("Full table scan", flatsqlScanMs, sqliteScanMs);

    // ==================== MEMORY BENCHMARK ====================
    printHeader("STORAGE SIZE");

    auto exported = flatsqlDb.exportData();
    std::cout << "FlatSQL storage: " << exported.size() << " bytes ("
              << std::fixed << std::setprecision(2) << exported.size() / 1024.0 / 1024.0 << " MB)\n";

    // SQLite in-memory size estimate
    sqlite3_int64 pageCount = 0;
    sqlite3_int64 pageSize = 0;
    auto stmt = sqliteDb.prepare("PRAGMA page_count");
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        pageCount = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);

    stmt = sqliteDb.prepare("PRAGMA page_size");
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        pageSize = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);

    sqlite3_int64 sqliteSize = pageCount * pageSize;
    std::cout << "SQLite storage:  " << sqliteSize << " bytes ("
              << std::fixed << std::setprecision(2) << sqliteSize / 1024.0 / 1024.0 << " MB)\n";

    // ==================== SUMMARY ====================
    printHeader("SUMMARY");

    std::cout << "FlatSQL advantages:\n";
    std::cout << "  - Zero-copy reads from pre-serialized FlatBuffers\n";
    std::cout << "  - Streaming ingest without parsing/conversion\n";
    std::cout << "  - Data stays in original FlatBuffer format\n";
    std::cout << "  - Export/reload without re-serialization\n\n";

    std::cout << "SQLite advantages:\n";
    std::cout << "  - Mature query optimizer\n";
    std::cout << "  - Full SQL support (JOINs, aggregations, etc.)\n";
    std::cout << "  - ACID transactions\n";
    std::cout << "  - Rich ecosystem\n\n";

    std::cout << "Use FlatSQL when:\n";
    std::cout << "  - Data arrives as FlatBuffers (IPC, network, files)\n";
    std::cout << "  - You need simple indexed lookups\n";
    std::cout << "  - Memory efficiency is critical\n";
    std::cout << "  - You want to avoid serialization overhead\n";

    return 0;
}
