// Query-path regression tests (graph task flatsql-query-regressions-20260928).
//
// Every check here is a computable outcome: the rows a query returns, and what
// the virtual table did to produce them, read from flatsql_scan_stats() (full
// scans, index lookups, index entries read, records visited). Nothing here
// depends on wall-clock time.
//
// Covered:
//   - an indexed point lookup visits one record, not the table
//   - `col = ? LIMIT 1` over a key with many entries reads one index entry
//   - a range (BETWEEN, >, <=, ...) reads only its keys, with exact bounds
//   - several indexed terms in one WHERE: the index narrows and SQLite filters
//     (the old planner handed xFilter the wrong constraint's value and dropped
//     the other term, returning wrong rows)
//   - COLLATE NOCASE and NULL comparisons are never answered from the index
//   - the `SELECT * ... WHERE col = ?` fast path returns every match for a
//     non-unique column, and a live duplicate behind a deleted primary key
//   - the query-result cache retains results by bytes, not by row count
//
// Checks do not stop at the first failure, so running this file against an
// older engine lists exactly which behaviours changed.

#include "flatsql/database.h"
#include <flatbuffers/flatbuffers.h>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

using namespace flatsql;

namespace {

int g_failures = 0;
int g_checks = 0;

#define CHECK(cond)                                                              \
    do {                                                                         \
        g_checks++;                                                              \
        if (!(cond)) {                                                           \
            g_failures++;                                                        \
            std::cerr << "  FAIL " << __FILE__ << ":" << __LINE__ << ": " #cond   \
                      << std::endl;                                              \
        }                                                                        \
    } while (0)

#define CHECK_EQ(actual, expected)                                               \
    do {                                                                         \
        g_checks++;                                                              \
        const auto actualValue = (actual);                                       \
        const auto expectedValue = (expected);                                   \
        if (!(actualValue == expectedValue)) {                                   \
            g_failures++;                                                        \
            std::cerr << "  FAIL " << __FILE__ << ":" << __LINE__ << ": " #actual \
                      << " == " << actualValue << ", expected " << expectedValue \
                      << std::endl;                                              \
        }                                                                        \
    } while (0)

const char* kSchema = R"(
    table PublishEventRecord {
        FILE_ID: string (key);
        RECORD_ID: string (index);
        EVENT_INDEX: int (index);
        PAYLOAD_SIZE: int;
    }
    root_type PublishEventRecord;
)";

constexpr int kRecords = 2000;
constexpr int kHotRecords = 1800;

std::vector<uint8_t> publishEvent(const std::string& fileId, const std::string& recordId,
                                  int32_t eventIndex, int32_t payloadSize) {
    flatbuffers::FlatBufferBuilder builder(128);
    builder.ForceDefaults(true);  // EVENT_INDEX 0 is a value, not an absent field
    const auto file = builder.CreateString(fileId);
    const auto record = builder.CreateString(recordId);
    const auto start = builder.StartTable();
    builder.AddOffset(4, file);
    builder.AddOffset(6, record);
    builder.AddElement<int32_t>(8, eventIndex, 0);
    builder.AddElement<int32_t>(10, payloadSize, 0);
    const auto root = flatbuffers::Offset<flatbuffers::Table>(builder.EndTable(start));
    builder.Finish(root, "PUBL");
    return std::vector<uint8_t>(builder.GetBufferPointer(),
                                builder.GetBufferPointer() + builder.GetSize());
}

// Records 0..1799 belong to the "hot" publish event, the rest to one cold
// event each. One extra record has FILE_ID "record-5": the same text as an
// existing RECORD_ID, which is what exposes a planner that feeds one term's
// value into another term's index.
void populate(FlatSQLDatabase& db) {
    db.registerFileId("PUBL", "PublishEventRecord");
    for (int i = 0; i < kRecords; i++) {
        const std::string fileId = i < kHotRecords ? "hot" : "cold-" + std::to_string(i);
        const auto buffer = publishEvent(fileId, "record-" + std::to_string(i), i, 96 + (i % 7) * 32);
        db.ingestOne(buffer.data(), buffer.size());
    }
    const auto decoy = publishEvent("record-5", "decoy", 100000, 1);
    db.ingestOne(decoy.data(), decoy.size());
}

struct ScanStats {
    uint64_t fullScans = 0;
    uint64_t rowidLookups = 0;
    uint64_t indexEqualityScans = 0;
    uint64_t indexRangeScans = 0;
    uint64_t indexEntriesRead = 0;
    uint64_t rowsVisited = 0;
    bool available = false;
};

uint64_t jsonNumber(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\":";
    const size_t at = json.find(needle);
    if (at == std::string::npos) return 0;
    return std::strtoull(json.c_str() + at + needle.size(), nullptr, 10);
}

ScanStats readStats(FlatSQLDatabase& db) {
    ScanStats stats;
    QueryResult result;
    std::string error;
    if (!db.queryNoThrow("SELECT flatsql_scan_stats()", {}, result, &error) ||
        result.rows.empty() || !std::holds_alternative<std::string>(result.rows[0][0])) {
        return stats;  // older engine: no counters
    }
    const std::string& json = std::get<std::string>(result.rows[0][0]);
    stats.available = true;
    stats.fullScans = jsonNumber(json, "fullScans");
    stats.rowidLookups = jsonNumber(json, "rowidLookups");
    stats.indexEqualityScans = jsonNumber(json, "indexEqualityScans");
    stats.indexRangeScans = jsonNumber(json, "indexRangeScans");
    stats.indexEntriesRead = jsonNumber(json, "indexEntriesRead");
    stats.rowsVisited = jsonNumber(json, "rowsVisited");
    return stats;
}

ScanStats delta(const ScanStats& before, const ScanStats& after) {
    ScanStats d;
    d.available = before.available && after.available;
    d.fullScans = after.fullScans - before.fullScans;
    d.rowidLookups = after.rowidLookups - before.rowidLookups;
    d.indexEqualityScans = after.indexEqualityScans - before.indexEqualityScans;
    d.indexRangeScans = after.indexRangeScans - before.indexRangeScans;
    d.indexEntriesRead = after.indexEntriesRead - before.indexEntriesRead;
    d.rowsVisited = after.rowsVisited - before.rowsVisited;
    return d;
}

// Run a query and report both its rows and what the vtab did for it.
QueryResult run(FlatSQLDatabase& db, const std::string& sql, const std::vector<Value>& params,
                ScanStats* work) {
    const ScanStats before = readStats(db);
    QueryResult result;
    std::string error;
    if (!db.queryNoThrow(sql, params, result, &error)) {
        std::cerr << "  query failed: " << sql << ": " << error << std::endl;
        g_failures++;
    }
    const ScanStats after = readStats(db);
    if (work) *work = delta(before, after);
    return result;
}

std::string text(const QueryResult& result, size_t row, size_t column) {
    if (row >= result.rows.size() || column >= result.rows[row].size()) return "<missing>";
    const Value& value = result.rows[row][column];
    if (const auto* s = std::get_if<std::string>(&value)) return *s;
    if (const auto* i = std::get_if<int64_t>(&value)) return std::to_string(*i);
    return "<non-text>";
}

int64_t integer(const QueryResult& result, size_t row, size_t column) {
    if (row >= result.rows.size() || column >= result.rows[row].size()) return -1;
    const Value& value = result.rows[row][column];
    if (const auto* i = std::get_if<int64_t>(&value)) return *i;
    return -1;
}

std::string plan(FlatSQLDatabase& db, const std::string& sql, const std::vector<Value>& params) {
    QueryResult result;
    std::string error;
    if (!db.queryNoThrow("EXPLAIN QUERY PLAN " + sql, params, result, &error)) return "<error>";
    std::string out;
    for (const auto& row : result.rows) {
        if (row.size() >= 4) out += text(result, &row - &result.rows[0], 3) + "\n";
    }
    return out;
}

// ---------------------------------------------------------------------------

void testPointLookupUsesIndex(FlatSQLDatabase& db) {
    std::cout << "point lookup on an indexed column" << std::endl;
    ScanStats work;
    const auto result = run(db,
        "SELECT FILE_ID, RECORD_ID, PAYLOAD_SIZE FROM PublishEventRecord WHERE RECORD_ID = ?",
        {std::string("record-1234")}, &work);
    CHECK_EQ(result.rows.size(), size_t{1});
    CHECK_EQ(text(result, 0, 1), std::string("record-1234"));
    CHECK_EQ(integer(result, 0, 2), int64_t{96 + (1234 % 7) * 32});
    CHECK(work.available);
    CHECK_EQ(work.fullScans, uint64_t{0});
    CHECK_EQ(work.indexEqualityScans, uint64_t{1});
    CHECK_EQ(work.rowsVisited, uint64_t{1});

    const std::string p = plan(db,
        "SELECT FILE_ID FROM PublishEventRecord WHERE RECORD_ID = ?", {std::string("x")});
    CHECK(p.find("eq:RECORD_ID") != std::string::npos);
}

void testLimitStopsEarly(FlatSQLDatabase& db) {
    std::cout << "LIMIT 1 over a key with 1800 entries" << std::endl;
    ScanStats work;
    const auto result = run(db,
        "SELECT RECORD_ID, EVENT_INDEX FROM PublishEventRecord WHERE FILE_ID = ? LIMIT 1",
        {std::string("hot")}, &work);
    CHECK_EQ(result.rows.size(), size_t{1});
    // (key, sequence) order: the first record of the hot event.
    CHECK_EQ(text(result, 0, 0), std::string("record-0"));
    CHECK(work.available);
    CHECK_EQ(work.fullScans, uint64_t{0});
    CHECK_EQ(work.indexEqualityScans, uint64_t{1});
    CHECK_EQ(work.indexEntriesRead, uint64_t{1});
    CHECK_EQ(work.rowsVisited, uint64_t{1});

    // Without LIMIT the same key yields every entry, each read once.
    const auto all = run(db,
        "SELECT RECORD_ID FROM PublishEventRecord WHERE FILE_ID = ?", {std::string("hot")}, &work);
    CHECK_EQ(all.rows.size(), size_t{kHotRecords});
    CHECK_EQ(work.indexEntriesRead, uint64_t{kHotRecords});
    CHECK_EQ(work.rowsVisited, uint64_t{kHotRecords});
}

void testRangeReadsOnlyItsKeys(FlatSQLDatabase& db) {
    std::cout << "index range scans" << std::endl;
    struct Case {
        const char* where;
        std::vector<Value> params;
        std::vector<int64_t> expected;
    };
    const std::vector<Case> cases = {
        {"EVENT_INDEX BETWEEN 0 AND ?", {int64_t{3}}, {0, 1, 2, 3}},
        {"EVENT_INDEX > ? AND EVENT_INDEX <= ?", {int64_t{5}, int64_t{9}}, {6, 7, 8, 9}},
        {"EVENT_INDEX >= ? AND EVENT_INDEX < ?", {int64_t{5}, int64_t{9}}, {5, 6, 7, 8}},
        {"EVENT_INDEX < ?", {int64_t{3}}, {0, 1, 2}},
        {"EVENT_INDEX >= ? AND EVENT_INDEX < ?", {int64_t{1998}, int64_t{5000}}, {1998, 1999}},
        {"EVENT_INDEX > ?", {int64_t{1998}}, {1999, 100000}},
    };
    for (const auto& c : cases) {
        ScanStats work;
        const std::string sql = std::string("SELECT EVENT_INDEX FROM PublishEventRecord WHERE ") +
                                c.where + " ORDER BY EVENT_INDEX";
        const auto result = run(db, sql, c.params, &work);
        CHECK_EQ(result.rows.size(), c.expected.size());
        for (size_t i = 0; i < c.expected.size() && i < result.rows.size(); i++) {
            CHECK_EQ(integer(result, i, 0), c.expected[i]);
        }
        CHECK(work.available);
        CHECK_EQ(work.fullScans, uint64_t{0});
        CHECK_EQ(work.indexRangeScans, uint64_t{1});
        CHECK_EQ(work.indexEntriesRead, uint64_t{c.expected.size()});
        CHECK_EQ(work.rowsVisited, uint64_t{c.expected.size()});
    }

    // The index already yields EVENT_INDEX order: no sort step.
    const std::string p = plan(db,
        "SELECT RECORD_ID FROM PublishEventRecord WHERE EVENT_INDEX BETWEEN ? AND ? ORDER BY EVENT_INDEX",
        {int64_t{10}, int64_t{20}});
    CHECK(p.find("range:EVENT_INDEX:>=:<=") != std::string::npos);
    CHECK(p.find("ORDER BY") == std::string::npos);
}

void testSeveralIndexedTermsReturnCorrectRows(FlatSQLDatabase& db) {
    std::cout << "several indexed terms in one WHERE (wrong-row regression)" << std::endl;
    // record-5 lives in the hot event. A record whose FILE_ID is the text
    // "record-5" also exists; the old planner looked "record-5" up in the
    // FILE_ID index and dropped both terms, returning that decoy row.
    auto result = run(db,
        "SELECT RECORD_ID FROM PublishEventRecord WHERE RECORD_ID = 'record-5' AND FILE_ID = 'hot'",
        {}, nullptr);
    CHECK_EQ(result.rows.size(), size_t{1});
    CHECK_EQ(text(result, 0, 0), std::string("record-5"));

    // Same terms, other order: the old planner searched the RECORD_ID index
    // for "hot" and returned nothing.
    result = run(db,
        "SELECT RECORD_ID FROM PublishEventRecord WHERE FILE_ID = ? AND RECORD_ID = ?",
        {std::string("hot"), std::string("record-5")}, nullptr);
    CHECK_EQ(result.rows.size(), size_t{1});
    CHECK_EQ(text(result, 0, 0), std::string("record-5"));

    // A range term before an equality term: the old planner fed the range
    // bound (an integer) to the equality lookup.
    result = run(db,
        "SELECT RECORD_ID FROM PublishEventRecord WHERE EVENT_INDEX >= ? AND RECORD_ID = ?",
        {int64_t{0}, std::string("record-42")}, nullptr);
    CHECK_EQ(result.rows.size(), size_t{1});
    CHECK_EQ(text(result, 0, 0), std::string("record-42"));

    // Equality narrows, the other term filters.
    ScanStats work;
    result = run(db,
        "SELECT RECORD_ID FROM PublishEventRecord WHERE RECORD_ID = ? AND EVENT_INDEX > ?",
        {std::string("record-42"), int64_t{100}}, &work);
    CHECK_EQ(result.rows.size(), size_t{0});
    CHECK_EQ(work.indexEqualityScans, uint64_t{1});
    CHECK_EQ(work.rowsVisited, uint64_t{1});

    // IN is answered one key at a time from the index.
    result = run(db,
        "SELECT RECORD_ID FROM PublishEventRecord WHERE RECORD_ID IN ('record-1', 'record-3', 'nope') "
        "ORDER BY RECORD_ID", {}, &work);
    CHECK_EQ(result.rows.size(), size_t{2});
    CHECK_EQ(text(result, 0, 0), std::string("record-1"));
    CHECK_EQ(text(result, 1, 0), std::string("record-3"));
    CHECK_EQ(work.fullScans, uint64_t{0});

    // Two cursors on the same table, each with its own index walk.
    result = run(db,
        "SELECT a.RECORD_ID, b.EVENT_INDEX FROM PublishEventRecord a "
        "JOIN PublishEventRecord b ON b.RECORD_ID = a.RECORD_ID "
        "WHERE a.EVENT_INDEX BETWEEN ? AND ? ORDER BY a.EVENT_INDEX",
        {int64_t{1990}, int64_t{1994}}, &work);
    CHECK_EQ(result.rows.size(), size_t{5});
    CHECK_EQ(text(result, 0, 0), std::string("record-1990"));
    CHECK_EQ(integer(result, 4, 1), int64_t{1994});
    CHECK_EQ(work.fullScans, uint64_t{0});
}

void testCollationAndNullAreNotIndexAnswers(FlatSQLDatabase& db) {
    std::cout << "COLLATE NOCASE and NULL comparisons" << std::endl;
    // The index compares BINARY. A NOCASE term must be checked row by row.
    auto result = run(db,
        "SELECT RECORD_ID FROM PublishEventRecord WHERE RECORD_ID = 'RECORD-77' COLLATE NOCASE",
        {}, nullptr);
    CHECK_EQ(result.rows.size(), size_t{1});
    CHECK_EQ(text(result, 0, 0), std::string("record-77"));

    result = run(db, "SELECT RECORD_ID FROM PublishEventRecord WHERE RECORD_ID = ?",
                 {std::monostate{}}, nullptr);
    CHECK_EQ(result.rows.size(), size_t{0});
    result = run(db, "SELECT RECORD_ID FROM PublishEventRecord WHERE EVENT_INDEX > ?",
                 {std::monostate{}}, nullptr);
    CHECK_EQ(result.rows.size(), size_t{0});

    // Text against an INTEGER key follows SQLite affinity, as a scan would.
    result = run(db, "SELECT RECORD_ID FROM PublishEventRecord WHERE EVENT_INDEX = '7'", {}, nullptr);
    CHECK_EQ(result.rows.size(), size_t{1});
    CHECK_EQ(text(result, 0, 0), std::string("record-7"));
}

void testFastPathReturnsEveryMatch() {
    std::cout << "SELECT * fast path on a non-unique column" << std::endl;
    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(R"(
        table ev {
            file_id: string (key);
            seq: int;
        }
        root_type ev;
    )", "fast_path_regression");
    db.registerFileId("EVNT", "ev");
    for (int i = 0; i < 5; i++) {
        flatbuffers::FlatBufferBuilder builder(64);
        const auto file = builder.CreateString(i < 3 ? "same" : "other");
        const auto start = builder.StartTable();
        builder.AddOffset(4, file);
        builder.AddElement<int32_t>(6, i, -1);
        builder.Finish(flatbuffers::Offset<flatbuffers::Table>(builder.EndTable(start)), "EVNT");
        db.ingestOne(builder.GetBufferPointer(), builder.GetSize());
    }
    const auto result = db.query("SELECT * FROM ev WHERE file_id = ?", std::vector<Value>{Value{std::string("same")}});
    CHECK_EQ(result.rows.size(), size_t{3});
}

void testLiveDuplicateBehindDeletedKey() {
    std::cout << "primary key whose first record is deleted" << std::endl;
    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(R"(
        table item {
            id: int (id);
            version: int;
        }
        root_type item;
    )", "tombstone_regression");
    db.registerFileId("ITEM", "item");
    std::vector<uint64_t> sequences;
    for (int version = 1; version <= 2; version++) {
        flatbuffers::FlatBufferBuilder builder(64);
        const auto start = builder.StartTable();
        builder.AddElement<int32_t>(4, 7, -1);
        builder.AddElement<int32_t>(6, version, -1);
        builder.Finish(flatbuffers::Offset<flatbuffers::Table>(builder.EndTable(start)), "ITEM");
        sequences.push_back(db.ingestOne(builder.GetBufferPointer(), builder.GetSize()));
    }
    // Tombstones live on the SQL side, which registers tables on first use.
    db.query("SELECT count(*) FROM item");
    db.markDeleted("item", sequences[0]);
    auto result = db.query("SELECT version FROM item WHERE id = ?", std::vector<Value>{Value{int64_t{7}}});
    CHECK_EQ(result.rows.size(), size_t{1});
    CHECK_EQ(integer(result, 0, 0), int64_t{2});
    result = db.query("SELECT * FROM item WHERE id = ?", std::vector<Value>{Value{int64_t{7}}});
    CHECK_EQ(result.rows.size(), size_t{1});
    CHECK_EQ(integer(result, 0, 1), int64_t{2});
}

void testQueryCacheRetainsByBytes(FlatSQLDatabase& db) {
    std::cout << "query-result cache retention" << std::endl;
    db.clearQueryResultCache();
    db.registerQueryTemplate("hot", "SELECT _data FROM PublishEventRecord WHERE FILE_ID = ?", true);

    const auto base = db.getQueryCacheStats();
    for (int i = 0; i < 4; i++) {
        const auto result = db.queryTemplate("hot", {std::string("hot")});
        CHECK_EQ(result.rows.size(), size_t{kHotRecords});
    }
    const auto after = db.getQueryCacheStats();
    // 1800 rows used to exceed the 1000-row default and never be retained.
    CHECK_EQ(after.misses - base.misses, uint64_t{1});
    CHECK_EQ(after.hits - base.hits, uint64_t{3});
    CHECK_EQ(after.size, size_t{1});
    CHECK(after.totalBytes > size_t{kHotRecords} * 100);
    CHECK(after.totalBytes <= after.maxBytes);

    // A budget smaller than the result: never retained, never counted.
    db.configureQueryResultCache(1024, 1000000, after.totalBytes / 2);
    db.queryTemplate("hot", {std::string("hot")});
    db.queryTemplate("hot", {std::string("hot")});
    auto small = db.getQueryCacheStats();
    CHECK_EQ(small.size, size_t{0});
    CHECK_EQ(small.totalBytes, size_t{0});

    // A budget for one hot result: the next distinct result evicts it.
    db.registerQueryTemplate("cold", "SELECT _data FROM PublishEventRecord WHERE FILE_ID = ?", true);
    db.configureQueryResultCache(1024, 1000000, after.totalBytes + after.totalBytes / 4);
    db.queryTemplate("hot", {std::string("hot")});
    db.queryTemplate("cold", {std::string("cold-1900")});
    auto both = db.getQueryCacheStats();
    CHECK_EQ(both.size, size_t{2});
    db.queryTemplate("cold", {std::string("hot")});  // a second large result
    auto evicted = db.getQueryCacheStats();
    CHECK(evicted.totalBytes <= evicted.maxBytes);
    CHECK_EQ(evicted.size, size_t{2});  // the other hot result went, cold-1900 stayed

    // Invalidation releases everything.
    const auto buffer = publishEvent("hot", "record-late", 5000, 1);
    db.ingestOne(buffer.data(), buffer.size());
    db.queryTemplate("cold", {std::string("cold-1900")});
    auto invalidated = db.getQueryCacheStats();
    CHECK_EQ(invalidated.size, size_t{1});
    CHECK(invalidated.totalBytes < after.totalBytes);

    db.configureQueryResultCache(FlatSQLDatabase::kDefaultQueryCacheMaxEntries,
                                 FlatSQLDatabase::kDefaultQueryCacheMaxRows,
                                 FlatSQLDatabase::kDefaultQueryCacheMaxBytes);
}

}  // namespace

int main() {
    std::cout << "FlatSQL query regression tests" << std::endl;
    {
        FlatSQLDatabase db = FlatSQLDatabase::fromSchema(kSchema, "query_regression");
        populate(db);
        testPointLookupUsesIndex(db);
        testLimitStopsEarly(db);
        testRangeReadsOnlyItsKeys(db);
        testSeveralIndexedTermsReturnCorrectRows(db);
        testCollationAndNullAreNotIndexAnswers(db);
        testQueryCacheRetainsByBytes(db);
    }
    testFastPathReturnsEveryMatch();
    testLiveDuplicateBehindDeletedKey();

    std::cout << (g_checks - g_failures) << "/" << g_checks << " checks passed" << std::endl;
    if (g_failures) {
        std::cerr << g_failures << " check(s) failed" << std::endl;
        return 1;
    }
    std::cout << "=== All query regression checks passed! ===" << std::endl;
    return 0;
}
