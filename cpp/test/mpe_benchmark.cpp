// Advanced Benchmark: FlatSQL vs SQLite - MPE (Minimum Propagatable Element) Lookups
//
// This is an ADVANCED benchmark that does NOT run as part of CI.
// It tests large-scale performance with realistic space catalog data (MPE records).
//
// Usage:
//   ./flatsql_mpe_benchmark [record_count]
//
//   Default: 1,000,000 records
//   Example: ./flatsql_mpe_benchmark 10000000  # 10M records
//
// The benchmark simulates looking up satellites by NORAD CAT ID (ENTITY_ID),
// which is a common operation in space situational awareness applications.

#include "flatsql/database.h"
#include "../schemas/mpe_schema_generated.h"
#include <sqlite3.h>
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <cstring>
#include <algorithm>
#include <numeric>

using namespace flatsql;
using namespace std::chrono;

// Default configuration - can be overridden via command line
static size_t RECORD_COUNT = 1'000'000;  // 1M records default
static constexpr int QUERY_ITERATIONS = 100'000;
static constexpr int WARMUP_ITERATIONS = 10'000;

// Timer helper
class Timer {
public:
    void start() { start_ = high_resolution_clock::now(); }
    void stop() { end_ = high_resolution_clock::now(); }
    double ms() const { return duration_cast<microseconds>(end_ - start_).count() / 1000.0; }
    double us() const { return static_cast<double>(duration_cast<nanoseconds>(end_ - start_).count()) / 1000.0; }
    double ns() const { return static_cast<double>(duration_cast<nanoseconds>(end_ - start_).count()); }
private:
    high_resolution_clock::time_point start_, end_;
};

// Create FlatBuffer MPE record
std::vector<uint8_t> createMPEFlatBuffer(
    const std::string& entityId,
    double epoch,
    double meanMotion,
    double eccentricity,
    double inclination,
    double raOfAscNode,
    double argOfPericenter,
    double meanAnomaly,
    double bstar,
    mpe::meanElementTheory theory = mpe::meanElementTheory_SGP4
) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto mpeRecord = mpe::CreateMPEDirect(
        builder,
        entityId.c_str(),
        epoch,
        meanMotion,
        eccentricity,
        inclination,
        raOfAscNode,
        argOfPericenter,
        meanAnomaly,
        bstar,
        theory
    );
    builder.Finish(mpeRecord, "$MPE");
    const uint8_t* buf = builder.GetBufferPointer();
    size_t size = builder.GetSize();
    return std::vector<uint8_t>(buf, buf + size);
}

// Field extractor for FlatSQL
Value extractMPEField(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    auto mpeRecord = mpe::GetMPE(data);
    if (!mpeRecord) return std::monostate{};

    if (fieldName == "ENTITY_ID") {
        if (mpeRecord->ENTITY_ID()) {
            return std::string(mpeRecord->ENTITY_ID()->c_str(), mpeRecord->ENTITY_ID()->size());
        }
        return std::string();
    }
    if (fieldName == "EPOCH") return mpeRecord->EPOCH();
    if (fieldName == "MEAN_MOTION") return mpeRecord->MEAN_MOTION();
    if (fieldName == "ECCENTRICITY") return mpeRecord->ECCENTRICITY();
    if (fieldName == "INCLINATION") return mpeRecord->INCLINATION();
    if (fieldName == "RA_OF_ASC_NODE") return mpeRecord->RA_OF_ASC_NODE();
    if (fieldName == "ARG_OF_PERICENTER") return mpeRecord->ARG_OF_PERICENTER();
    if (fieldName == "MEAN_ANOMALY") return mpeRecord->MEAN_ANOMALY();
    if (fieldName == "BSTAR") return mpeRecord->BSTAR();
    if (fieldName == "MEAN_ELEMENT_THEORY") return static_cast<int32_t>(mpeRecord->MEAN_ELEMENT_THEORY());

    return std::monostate{};
}

// Fast extractor - writes directly to SQLite context
bool fastExtractMPEField(const uint8_t* data, size_t length, int columnIndex, sqlite3_context* ctx) {
    (void)length;
    auto mpeRecord = mpe::GetMPE(data);
    if (!mpeRecord) {
        sqlite3_result_null(ctx);
        return true;
    }

    // Column order matches schema: ENTITY_ID, EPOCH, MEAN_MOTION, ECCENTRICITY, INCLINATION,
    //                              RA_OF_ASC_NODE, ARG_OF_PERICENTER, MEAN_ANOMALY, BSTAR, MEAN_ELEMENT_THEORY
    switch (columnIndex) {
        case 0:  // ENTITY_ID
            if (mpeRecord->ENTITY_ID()) {
                sqlite3_result_text(ctx, mpeRecord->ENTITY_ID()->c_str(),
                                   static_cast<int>(mpeRecord->ENTITY_ID()->size()), SQLITE_STATIC);
            } else {
                sqlite3_result_null(ctx);
            }
            return true;
        case 1:  // EPOCH
            sqlite3_result_double(ctx, mpeRecord->EPOCH());
            return true;
        case 2:  // MEAN_MOTION
            sqlite3_result_double(ctx, mpeRecord->MEAN_MOTION());
            return true;
        case 3:  // ECCENTRICITY
            sqlite3_result_double(ctx, mpeRecord->ECCENTRICITY());
            return true;
        case 4:  // INCLINATION
            sqlite3_result_double(ctx, mpeRecord->INCLINATION());
            return true;
        case 5:  // RA_OF_ASC_NODE
            sqlite3_result_double(ctx, mpeRecord->RA_OF_ASC_NODE());
            return true;
        case 6:  // ARG_OF_PERICENTER
            sqlite3_result_double(ctx, mpeRecord->ARG_OF_PERICENTER());
            return true;
        case 7:  // MEAN_ANOMALY
            sqlite3_result_double(ctx, mpeRecord->MEAN_ANOMALY());
            return true;
        case 8:  // BSTAR
            sqlite3_result_double(ctx, mpeRecord->BSTAR());
            return true;
        case 9:  // MEAN_ELEMENT_THEORY
            sqlite3_result_int(ctx, static_cast<int>(mpeRecord->MEAN_ELEMENT_THEORY()));
            return true;
        default:
            return false;
    }
}

// SQLite helper
class SQLiteDB {
public:
    SQLiteDB() {
        sqlite3_open(":memory:", &db_);
        exec("PRAGMA journal_mode = OFF");
        exec("PRAGMA synchronous = OFF");
        exec("PRAGMA cache_size = 100000");  // Larger cache for big datasets
        exec("PRAGMA temp_store = MEMORY");
        exec("PRAGMA mmap_size = 1073741824");  // 1GB mmap
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

// Generate realistic MPE test data
struct MPERecord {
    std::string entityId;  // NORAD CAT ID as string (e.g., "25544" for ISS)
    double epoch;
    double meanMotion;
    double eccentricity;
    double inclination;
    double raOfAscNode;
    double argOfPericenter;
    double meanAnomaly;
    double bstar;
};

std::vector<MPERecord> generateMPEData(size_t count) {
    std::vector<MPERecord> records;
    records.reserve(count);

    std::mt19937_64 rng(42);  // Fixed seed for reproducibility

    // Realistic orbital parameter distributions
    std::uniform_real_distribution<double> epochDist(2460000.0, 2460365.0);  // ~1 year range in JD
    std::uniform_real_distribution<double> meanMotionDist(0.5, 16.0);  // rev/day (GEO to LEO)
    std::uniform_real_distribution<double> eccDist(0.0, 0.25);  // Most orbits are near-circular
    std::uniform_real_distribution<double> incDist(0.0, 180.0);  // degrees
    std::uniform_real_distribution<double> angleDist(0.0, 360.0);  // degrees
    std::uniform_real_distribution<double> bstarDist(-1e-4, 1e-3);  // realistic BSTAR range

    for (size_t i = 0; i < count; i++) {
        MPERecord r;
        // NORAD CAT IDs typically range from 1 to ~60000+ currently
        // For benchmark, we use sequential IDs starting from 1
        r.entityId = std::to_string(i + 1);
        r.epoch = epochDist(rng);
        r.meanMotion = meanMotionDist(rng);
        r.eccentricity = eccDist(rng);
        r.inclination = incDist(rng);
        r.raOfAscNode = angleDist(rng);
        r.argOfPericenter = angleDist(rng);
        r.meanAnomaly = angleDist(rng);
        r.bstar = bstarDist(rng);
        records.push_back(r);
    }

    return records;
}

void printHeader(const char* title) {
    std::cout << "\n" << std::string(80, '=') << "\n";
    std::cout << title << "\n";
    std::cout << std::string(80, '=') << "\n";
}

void printResult(const char* label, double flatsqlUs, double sqliteUs) {
    double ratio = sqliteUs / flatsqlUs;
    const char* winner = ratio > 1.0 ? "FlatSQL" : "SQLite";
    double factor = ratio > 1.0 ? ratio : 1.0 / ratio;

    std::cout << std::left << std::setw(35) << label
              << std::right << std::setw(12) << std::fixed << std::setprecision(2) << flatsqlUs << " us"
              << std::setw(12) << sqliteUs << " us"
              << "  " << winner << " " << std::setprecision(1) << factor << "x faster\n";
}

void printLatencyStats(const char* label, std::vector<double>& latencies) {
    std::sort(latencies.begin(), latencies.end());

    double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double mean = sum / latencies.size();

    size_t p50_idx = latencies.size() / 2;
    size_t p95_idx = static_cast<size_t>(latencies.size() * 0.95);
    size_t p99_idx = static_cast<size_t>(latencies.size() * 0.99);

    std::cout << "  " << label << ":\n";
    std::cout << "    Mean:  " << std::fixed << std::setprecision(2) << mean << " ns\n";
    std::cout << "    P50:   " << latencies[p50_idx] << " ns\n";
    std::cout << "    P95:   " << latencies[p95_idx] << " ns\n";
    std::cout << "    P99:   " << latencies[p99_idx] << " ns\n";
    std::cout << "    Min:   " << latencies.front() << " ns\n";
    std::cout << "    Max:   " << latencies.back() << " ns\n";
}

int main(int argc, char* argv[]) {
    // Parse command line for record count
    if (argc > 1) {
        RECORD_COUNT = std::stoull(argv[1]);
    }

    std::cout << "╔══════════════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║         FlatSQL vs SQLite - MPE (Space Catalog) Benchmark                    ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════════════════════╝\n\n";

    std::cout << "Configuration:\n";
    std::cout << "  Records:          " << std::setw(15) << RECORD_COUNT << "\n";
    std::cout << "  Query iterations: " << std::setw(15) << QUERY_ITERATIONS << "\n";
    std::cout << "  Warmup iterations:" << std::setw(15) << WARMUP_ITERATIONS << "\n\n";

    // Generate test data
    std::cout << "Generating " << RECORD_COUNT << " MPE records...\n";
    auto startGen = high_resolution_clock::now();
    auto testData = generateMPEData(RECORD_COUNT);
    auto endGen = high_resolution_clock::now();
    std::cout << "  Generated in " << duration_cast<milliseconds>(endGen - startGen).count() << " ms\n";

    // Pre-build FlatBuffers
    std::cout << "Pre-building FlatBuffers...\n";
    auto startBuild = high_resolution_clock::now();
    std::vector<std::vector<uint8_t>> flatBuffers;
    flatBuffers.reserve(RECORD_COUNT);
    for (const auto& r : testData) {
        flatBuffers.push_back(createMPEFlatBuffer(
            r.entityId, r.epoch, r.meanMotion, r.eccentricity,
            r.inclination, r.raOfAscNode, r.argOfPericenter,
            r.meanAnomaly, r.bstar
        ));
    }
    auto endBuild = high_resolution_clock::now();
    std::cout << "  Built in " << duration_cast<milliseconds>(endBuild - startBuild).count() << " ms\n";

    // Calculate total FlatBuffer size
    size_t totalFBSize = 0;
    for (const auto& fb : flatBuffers) {
        totalFBSize += fb.size();
    }
    std::cout << "  Total FlatBuffer size: " << totalFBSize / (1024*1024) << " MB\n";

    Timer timer;

    // ==================== INGEST BENCHMARK ====================
    printHeader("INGEST BENCHMARK");

    // FlatSQL setup and ingest
    std::string schema = R"(
        table MPE {
            ENTITY_ID: string (key);
            EPOCH: double;
            MEAN_MOTION: double;
            ECCENTRICITY: double;
            INCLINATION: double;
            RA_OF_ASC_NODE: double;
            ARG_OF_PERICENTER: double;
            MEAN_ANOMALY: double;
            BSTAR: double;
            MEAN_ELEMENT_THEORY: int;
        }
    )";

    std::cout << "Setting up FlatSQL...\n";
    auto flatsqlDb = FlatSQLDatabase::fromSchema(schema, "mpe_benchmark");
    flatsqlDb.registerFileId("$MPE", "MPE");
    flatsqlDb.setFieldExtractor("MPE", extractMPEField);
    flatsqlDb.setFastFieldExtractor("MPE", fastExtractMPEField);

    std::cout << "Ingesting into FlatSQL...\n";
    timer.start();
    for (const auto& fb : flatBuffers) {
        flatsqlDb.ingestOne(fb.data(), fb.size());
    }
    timer.stop();
    double flatsqlIngestMs = timer.ms();
    std::cout << "  FlatSQL ingest: " << std::fixed << std::setprecision(2) << flatsqlIngestMs << " ms\n";
    std::cout << "  Throughput: " << static_cast<size_t>(RECORD_COUNT / (flatsqlIngestMs / 1000.0)) << " records/sec\n";

    // SQLite setup and ingest
    std::cout << "\nSetting up SQLite...\n";
    SQLiteDB sqliteDb;
    sqliteDb.exec(R"(
        CREATE TABLE MPE (
            ENTITY_ID TEXT PRIMARY KEY,
            EPOCH REAL,
            MEAN_MOTION REAL,
            ECCENTRICITY REAL,
            INCLINATION REAL,
            RA_OF_ASC_NODE REAL,
            ARG_OF_PERICENTER REAL,
            MEAN_ANOMALY REAL,
            BSTAR REAL,
            MEAN_ELEMENT_THEORY INTEGER
        )
    )");

    std::cout << "Ingesting into SQLite...\n";
    timer.start();
    sqliteDb.exec("BEGIN TRANSACTION");
    auto insertStmt = sqliteDb.prepare(
        "INSERT INTO MPE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );
    for (const auto& r : testData) {
        sqlite3_bind_text(insertStmt, 1, r.entityId.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(insertStmt, 2, r.epoch);
        sqlite3_bind_double(insertStmt, 3, r.meanMotion);
        sqlite3_bind_double(insertStmt, 4, r.eccentricity);
        sqlite3_bind_double(insertStmt, 5, r.inclination);
        sqlite3_bind_double(insertStmt, 6, r.raOfAscNode);
        sqlite3_bind_double(insertStmt, 7, r.argOfPericenter);
        sqlite3_bind_double(insertStmt, 8, r.meanAnomaly);
        sqlite3_bind_double(insertStmt, 9, r.bstar);
        sqlite3_bind_int(insertStmt, 10, 0);  // SGP4
        sqlite3_step(insertStmt);
        sqlite3_reset(insertStmt);
    }
    sqlite3_finalize(insertStmt);
    sqliteDb.exec("COMMIT");
    timer.stop();
    double sqliteIngestMs = timer.ms();
    std::cout << "  SQLite ingest: " << sqliteIngestMs << " ms\n";
    std::cout << "  Throughput: " << static_cast<size_t>(RECORD_COUNT / (sqliteIngestMs / 1000.0)) << " records/sec\n";

    double ingestRatio = sqliteIngestMs / flatsqlIngestMs;
    std::cout << "\n  Result: FlatSQL is " << std::setprecision(1) << ingestRatio << "x faster for ingest\n";

    // ==================== POINT QUERY BENCHMARK (ENTITY_ID Lookup) ====================
    printHeader("POINT QUERY BENCHMARK - ENTITY_ID (NORAD CAT ID) Lookup");
    std::cout << "Simulating satellite lookups by NORAD CAT ID\n\n";

    std::mt19937_64 rng(12345);
    std::uniform_int_distribution<size_t> idDist(1, RECORD_COUNT);

    // Pre-generate query IDs
    std::vector<std::string> queryIds(QUERY_ITERATIONS);
    std::vector<std::string> warmupIds(WARMUP_ITERATIONS);
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        warmupIds[i] = std::to_string(idDist(rng));
    }
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        queryIds[i] = std::to_string(idDist(rng));
    }

    std::cout << std::left << std::setw(35) << "Operation"
              << std::right << std::setw(15) << "FlatSQL"
              << std::setw(15) << "SQLite"
              << "  Winner\n";
    std::cout << std::string(80, '-') << "\n";

    // --- SQL Query via VTable ---
    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        flatsqlDb.query("SELECT * FROM MPE WHERE ENTITY_ID = ?", {warmupIds[i]});
    }

    timer.start();
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        auto result = flatsqlDb.query("SELECT * FROM MPE WHERE ENTITY_ID = ?", {queryIds[i]});
        (void)result;
    }
    timer.stop();
    double flatsqlVtabUs = timer.us() / QUERY_ITERATIONS;

    // SQLite prepared statement
    auto selectStmt = sqliteDb.prepare("SELECT * FROM MPE WHERE ENTITY_ID = ?");

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        sqlite3_bind_text(selectStmt, 1, warmupIds[i].c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(selectStmt);
        sqlite3_reset(selectStmt);
    }

    timer.start();
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        sqlite3_bind_text(selectStmt, 1, queryIds[i].c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(selectStmt) == SQLITE_ROW) {
            // Read all columns to match FlatSQL behavior
            volatile const char* eid = (const char*)sqlite3_column_text(selectStmt, 0);
            volatile double epoch = sqlite3_column_double(selectStmt, 1);
            volatile double mm = sqlite3_column_double(selectStmt, 2);
            (void)eid; (void)epoch; (void)mm;
        }
        sqlite3_reset(selectStmt);
    }
    timer.stop();
    sqlite3_finalize(selectStmt);
    double sqliteVtabUs = timer.us() / QUERY_ITERATIONS;

    printResult("SQL Query (VTable)", flatsqlVtabUs, sqliteVtabUs);

    // --- Direct Index Lookup (FlatSQL only) ---
    timer.start();
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        auto result = flatsqlDb.findByIndex("MPE", "ENTITY_ID", queryIds[i]);
        (void)result;
    }
    timer.stop();
    double flatsqlDirectUs = timer.us() / QUERY_ITERATIONS;
    printResult("Direct Index Lookup", flatsqlDirectUs, sqliteVtabUs);

    // --- Zero-Copy Lookup (FlatSQL only - fastest path) ---
    timer.start();
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        uint32_t len;
        const uint8_t* data = flatsqlDb.findRawByIndex("MPE", "ENTITY_ID", queryIds[i], &len);
        if (data) {
            // Access the FlatBuffer directly - zero copy!
            auto mpeRecord = mpe::GetMPE(data);
            volatile double epoch = mpeRecord->EPOCH();
            (void)epoch;
        }
    }
    timer.stop();
    double flatsqlZeroCopyUs = timer.us() / QUERY_ITERATIONS;
    printResult("Zero-Copy Lookup", flatsqlZeroCopyUs, sqliteVtabUs);

    // ==================== LATENCY DISTRIBUTION ====================
    printHeader("LATENCY DISTRIBUTION (nanoseconds)");

    std::vector<double> flatsqlLatencies(QUERY_ITERATIONS);
    std::vector<double> sqliteLatencies(QUERY_ITERATIONS);

    // FlatSQL zero-copy latencies
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        auto start = high_resolution_clock::now();
        uint32_t len;
        const uint8_t* data = flatsqlDb.findRawByIndex("MPE", "ENTITY_ID", queryIds[i], &len);
        if (data) {
            auto mpeRecord = mpe::GetMPE(data);
            volatile double epoch = mpeRecord->EPOCH();
            (void)epoch;
        }
        auto end = high_resolution_clock::now();
        flatsqlLatencies[i] = static_cast<double>(duration_cast<nanoseconds>(end - start).count());
    }

    // SQLite latencies
    selectStmt = sqliteDb.prepare("SELECT * FROM MPE WHERE ENTITY_ID = ?");
    for (int i = 0; i < QUERY_ITERATIONS; i++) {
        auto start = high_resolution_clock::now();
        sqlite3_bind_text(selectStmt, 1, queryIds[i].c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(selectStmt) == SQLITE_ROW) {
            volatile double epoch = sqlite3_column_double(selectStmt, 1);
            (void)epoch;
        }
        sqlite3_reset(selectStmt);
        auto end = high_resolution_clock::now();
        sqliteLatencies[i] = static_cast<double>(duration_cast<nanoseconds>(end - start).count());
    }
    sqlite3_finalize(selectStmt);

    printLatencyStats("FlatSQL (zero-copy)", flatsqlLatencies);
    std::cout << "\n";
    printLatencyStats("SQLite", sqliteLatencies);

    // ==================== RANGE QUERY BENCHMARK ====================
    printHeader("RANGE QUERY BENCHMARK - EPOCH Range");
    std::cout << "Simulating time-range queries (e.g., \"all satellites with epoch > X\")\n\n";

    // Query for records in a specific epoch range (should return ~10% of data)
    double epochThreshold = 2460330.0;  // ~90% through the year

    timer.start();
    auto flatsqlRangeResult = flatsqlDb.query(
        "SELECT COUNT(*) FROM MPE WHERE EPOCH > ?",
        std::vector<Value>{epochThreshold}
    );
    timer.stop();
    double flatsqlRangeMs = timer.ms();

    auto countStmt = sqliteDb.prepare("SELECT COUNT(*) FROM MPE WHERE EPOCH > ?");
    timer.start();
    sqlite3_bind_double(countStmt, 1, epochThreshold);
    int sqliteCount = 0;
    if (sqlite3_step(countStmt) == SQLITE_ROW) {
        sqliteCount = sqlite3_column_int(countStmt, 0);
    }
    sqlite3_finalize(countStmt);
    timer.stop();
    double sqliteRangeMs = timer.ms();

    // Extract the count value from FlatSQL result
    int64_t flatsqlCount = 0;
    if (flatsqlRangeResult.rowCount() > 0 && flatsqlRangeResult.columnCount() > 0) {
        const auto& row = flatsqlRangeResult.rows[0];
        if (!row.empty()) {
            if (auto* val = std::get_if<int64_t>(&row[0])) {
                flatsqlCount = *val;
            } else if (auto* val32 = std::get_if<int32_t>(&row[0])) {
                flatsqlCount = *val32;
            }
        }
    }

    std::cout << "Query: SELECT COUNT(*) FROM MPE WHERE EPOCH > " << epochThreshold << "\n";
    std::cout << "  FlatSQL: " << flatsqlRangeMs << " ms (count: " << flatsqlCount << ")\n";
    std::cout << "  SQLite:  " << sqliteRangeMs << " ms (count: " << sqliteCount << ")\n";

    // ==================== STORAGE SIZE ====================
    printHeader("STORAGE SIZE");

    auto exported = flatsqlDb.exportData();
    std::cout << "FlatSQL storage: " << exported.size() / (1024*1024) << " MB ("
              << exported.size() << " bytes)\n";

    sqlite3_int64 pageCount = 0, pageSize = 0;
    auto stmt = sqliteDb.prepare("PRAGMA page_count");
    if (sqlite3_step(stmt) == SQLITE_ROW) pageCount = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    stmt = sqliteDb.prepare("PRAGMA page_size");
    if (sqlite3_step(stmt) == SQLITE_ROW) pageSize = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    sqlite3_int64 sqliteSize = pageCount * pageSize;
    std::cout << "SQLite storage:  " << sqliteSize / (1024*1024) << " MB ("
              << sqliteSize << " bytes)\n";

    std::cout << "\nNote: FlatSQL stores full FlatBuffers for zero-copy access.\n";
    std::cout << "      This is an intentional trade-off: speed > storage.\n";

    // ==================== SUMMARY ====================
    printHeader("SUMMARY");

    std::cout << "For " << RECORD_COUNT << " MPE (satellite) records:\n\n";

    double speedup = sqliteVtabUs / flatsqlZeroCopyUs;
    std::cout << "  ENTITY_ID (NORAD CAT ID) Lookup:\n";
    std::cout << "    FlatSQL zero-copy: " << std::fixed << std::setprecision(2) << flatsqlZeroCopyUs << " us/query\n";
    std::cout << "    SQLite indexed:    " << sqliteVtabUs << " us/query\n";
    std::cout << "    Speedup:           " << std::setprecision(1) << speedup << "x faster\n\n";

    std::cout << "  At this rate, FlatSQL can handle:\n";
    std::cout << "    " << static_cast<size_t>(1'000'000.0 / flatsqlZeroCopyUs) << " lookups/second\n\n";

    std::cout << "Key Insight:\n";
    std::cout << "  SQLite is faster for pure indexed lookups (highly optimized B-tree).\n";
    std::cout << "  FlatSQL trades some lookup speed for zero-copy FlatBuffer access.\n\n";

    std::cout << "Use FlatSQL when:\n";
    std::cout << "  - Data arrives as FlatBuffers (skip deserialization entirely)\n";
    std::cout << "  - You need zero-copy access to orbital elements\n";
    std::cout << "  - FlatBuffer format interoperability matters\n";
    std::cout << "  - You're streaming TLEs/OMMs and want to avoid parse overhead\n";
    std::cout << "  - Your bottleneck is serialization, not indexed lookup\n\n";

    std::cout << "Use SQLite when:\n";
    std::cout << "  - Data is already in-memory and needs fast lookups\n";
    std::cout << "  - You need complex SQL queries (JOINs, subqueries)\n";
    std::cout << "  - Storage efficiency is more important than format interop\n";

    return 0;
}
