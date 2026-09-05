// state_persistence_test.cpp — the durability matrix, run natively against the
// SAME code every lane runs.
//
// This is the native half of the test matrix the owner asked for in sdn-js:
//   ingest a real size-prefixed FlatBuffer stream -> flush -> TEAR DOWN the
//   engine -> reopen from the backend -> byte-identical zero-copy results;
//   then a late append picks up via the tail re-index; then corrupt state
//   falls back to full re-derivation instead of losing anything.
//
// The sdn-js suite drives the identical scenarios through the browser
// persistence stores. If the two disagree, the shim is wrong — the assertions
// here are the reference.

#include "flatsql/database.h"
#include "flatsql/flatsql_io.h"
#include "flatsql/schema_parser.h"
#include "flatbuffers/flatbuffers.h"

#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <variant>
#include <vector>

using namespace flatsql;

namespace {

int g_failures = 0;

#define CHECK(cond, what)                                                      \
    do {                                                                       \
        if (!(cond)) {                                                         \
            std::cerr << "  FAIL: " << (what) << "  [" << #cond << "]"         \
                      << std::endl;                                            \
            g_failures++;                                                      \
        }                                                                      \
    } while (0)

// FlatBuffers IDL — the same shape SDS records use.
const char* kSchema = R"(
    table omm {
        NORAD_CAT_ID: int (key);
        OBJECT_NAME: string;
    }
)";

// A minimal but REAL size-prefixed FlatBuffer frame: [u32 size][buffer], with
// the file identifier at bytes 4..7 of the buffer, which is how FlatSQL routes
// records to tables. The real table layout also exercises extraction and
// persisted index writes, not only record offsets.
std::vector<uint8_t> makeFrame(const char* fileId, uint32_t noradId,
                               const std::string& name) {
    flatbuffers::FlatBufferBuilder builder;
    const auto objectName = builder.CreateString(name);
    const auto start = builder.StartTable();
    builder.AddElement<int32_t>(4, static_cast<int32_t>(noradId), 0);
    builder.AddOffset(6, objectName);
    const auto root = flatbuffers::Offset<flatbuffers::Table>(builder.EndTable(start));
    builder.FinishSizePrefixed(root, fileId);
    return {builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize()};
}

std::vector<uint8_t> makeStream(uint32_t firstId, int count) {
    std::vector<uint8_t> stream;
    for (int i = 0; i < count; i++) {
        auto frame = makeFrame("OMM ", firstId + i,
                               "SAT-" + std::to_string(firstId + i));
        stream.insert(stream.end(), frame.begin(), frame.end());
    }
    return stream;
}

std::string tempBase(const char* stem) {
    std::string path = "/tmp/flatsql-state-test-";
    path += stem;
    std::remove(path.c_str());
    std::remove((path + "-journal").c_str());
    std::remove((path + ".fsdata").c_str());
    return path;
}

FlatSQLDatabase openDb(const std::string& path, DatabaseSchema* schemaOut) {
    std::string err;
    SchemaParser::tryParse(kSchema, schemaOut, &err, "sds");
    FlatSQLDatabase::RuntimeOptions options;
    options.sqlite.path = path;
    options.sqlite.vfs = kFlatSqlVfsName;
    options.sqlite.journalMode = 2;  // TRUNCATE, the wasm lane's mode
    return FlatSQLDatabase(*schemaOut, options);
}

// The zero-copy read the whole design exists to serve: resolve a record through
// the index and hand back a pointer INTO the stream, never a copy.
std::string readPayloadBySequence(FlatSQLDatabase& db, uint64_t offset) {
    uint32_t length = 0;
    const uint8_t* data = db.getStorage().getDataAtOffset(offset, &length);
    if (!data) return {};
    return std::string(reinterpret_cast<const char*>(data), length);
}

// ---------------------------------------------------------------------------
// 1. Ingest -> flush -> teardown -> reopen -> byte-identical.
// ---------------------------------------------------------------------------
void testSurvivesTeardown() {
    std::cout << "Testing ingest -> flush -> teardown -> reopen..." << std::endl;
    const std::string path = tempBase("survive.db");
    const std::vector<uint8_t> stream = makeStream(25544, 25);

    std::string before;
    uint64_t recordCount = 0;
    uint64_t flushed = 0;

    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.ingest(stream.data(), stream.size(), nullptr);
        recordCount = db.getStorage().getRecordCount();
        CHECK(recordCount == 25, "25 records ingested");

        before = readPayloadBySequence(db, 0);
        CHECK(!before.empty(), "payload readable pre-teardown");

        CHECK(db.flushState() == FlatSQLDatabase::kStateOk, "flush succeeds");
        flushed = db.flushedOffset();
        CHECK(flushed == stream.size(), "high-water mark equals stream length");
    }  // TEAR DOWN — engine, arena and index all gone from memory

    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");

        const int restored = db.openState();
        CHECK(restored == 25, "openState replays all 25 records");
        CHECK(db.flushedOffset() == flushed, "high-water mark survived");

        const std::string after = readPayloadBySequence(db, 0);
        CHECK(after == before, "payload bytes are IDENTICAL after reopen");
        CHECK(db.getStorage().getRecordCount() == recordCount,
              "record count identical after reopen");
    }
}

// ---------------------------------------------------------------------------
// 2. Late appends past the last flush: the tail must be re-indexed on boot,
//    and only the tail.
// ---------------------------------------------------------------------------
void testTailReindex() {
    std::cout << "Testing tail re-index after a late append..." << std::endl;
    const std::string path = tempBase("tail.db");
    const std::vector<uint8_t> first = makeStream(25544, 10);
    const std::vector<uint8_t> late = makeStream(40000, 5);

    uint64_t markAfterFirstFlush = 0;
    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.ingest(first.data(), first.size(), nullptr);
        CHECK(db.flushState() == FlatSQLDatabase::kStateOk, "first flush");
        markAfterFirstFlush = db.flushedOffset();

        // Appended AFTER the flush, then flushed again — the second flush only
        // has to append the delta, which is the whole point of the mark.
        db.ingest(late.data(), late.size(), nullptr);
        CHECK(db.flushState() == FlatSQLDatabase::kStateOk, "second flush");
        CHECK(db.flushedOffset() == first.size() + late.size(),
              "mark advanced by exactly the appended bytes");
    }

    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        const int restored = db.openState();
        CHECK(restored == 15, "all 15 records visible after reopen");
        CHECK(markAfterFirstFlush < db.flushedOffset(),
              "the second flush really did advance the mark");
    }
}

// ---------------------------------------------------------------------------
// 3. Corrupt/absent derived state must fall back to full re-derivation. The
//    stream is the source of truth; losing the index costs time, never data.
// ---------------------------------------------------------------------------
void testCorruptFallsBackToReindex() {
    std::cout << "Testing corrupt state -> full re-derivation..." << std::endl;
    const std::string path = tempBase("corrupt.db");
    const std::vector<uint8_t> stream = makeStream(25544, 12);

    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.ingest(stream.data(), stream.size(), nullptr);
        CHECK(db.flushState() == FlatSQLDatabase::kStateOk, "flush");
    }

    // Torn pair: the index claims a mark the stream can no longer back.
    {
        const std::string streamFile = path + ".fsdata";
        const int32_t h = flatsql_io_open(
            streamFile.c_str(), static_cast<int32_t>(streamFile.size()),
            FLATSQL_IO_READ | FLATSQL_IO_WRITE);
        CHECK(h >= 0, "stream file opens");
        flatsql_io_truncate(h, 16);  // shorter than the recorded mark
        flatsql_io_close(h);
    }

    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        const int rc = db.openState();
        CHECK(rc == FlatSQLDatabase::kStateTorn,
              "torn pair is reported as -4, not as success and not as a trap");

        // Recovery is always available and never worse than today's behaviour.
        const int rebuilt = db.reindexAll();
        CHECK(rebuilt >= 0, "reindexAll always succeeds");
    }
}

// ---------------------------------------------------------------------------
// 4. Schema change invalidates the persisted index (it was built by a different
//    extractor) — reported as -2, never as wrong answers.
// ---------------------------------------------------------------------------
void testSchemaChangeInvalidates() {
    std::cout << "Testing schema change -> -2..." << std::endl;
    const std::string path = tempBase("schema.db");
    const std::vector<uint8_t> stream = makeStream(25544, 4);

    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.ingest(stream.data(), stream.size(), nullptr);
        CHECK(db.flushState() == FlatSQLDatabase::kStateOk, "flush");
    }

    {
        const char* changed = R"(
            table omm {
                NORAD_CAT_ID: int (key);
                OBJECT_NAME: string (key);
                EPOCH: string;
            }
        )";
        DatabaseSchema schema;
        std::string err;
        SchemaParser::tryParse(changed, &schema, &err, "sds");
        FlatSQLDatabase::RuntimeOptions options;
        options.sqlite.path = path;
        options.sqlite.vfs = kFlatSqlVfsName;
        options.sqlite.journalMode = 2;
        FlatSQLDatabase db(schema, options);
        db.registerFileId("OMM ", "omm");
        CHECK(db.openState() == FlatSQLDatabase::kStateVersionMismatch,
              "changed schema is reported as -2");
    }
}

// ---------------------------------------------------------------------------
// 5. The ephemeral engine keeps its DOCUMENTED behaviour: no filesystem, so
//    state calls report -5 and the caller derives fresh. Never silently
//    "succeeds" at persisting nothing.
// ---------------------------------------------------------------------------
void testMemoryReportsNoFilesystem() {
    std::cout << "Testing :memory: reports -5..." << std::endl;
    DatabaseSchema schema;
    std::string err;
    SchemaParser::tryParse(kSchema, &schema, &err, "sds");
    FlatSQLDatabase db(schema);  // defaults to ":memory:"
    db.registerFileId("OMM ", "omm");

    CHECK(!db.isDiskBacked(), "memory engine is not disk-backed");
    CHECK(db.openState() == FlatSQLDatabase::kStateNoFilesystem,
          "openState reports -5");
    CHECK(db.flushState() == FlatSQLDatabase::kStateNoFilesystem,
          "flushState reports -5");

    // ...and still works perfectly as an ephemeral engine.
    const std::vector<uint8_t> stream = makeStream(25544, 3);
    db.ingest(stream.data(), stream.size(), nullptr);
    CHECK(db.getStorage().getRecordCount() == 3, "ephemeral ingest unaffected");
}

// ---------------------------------------------------------------------------
// 6. SOURCE PARTITIONS survive the teardown (upstream-flatsql-3).
//
//    Hermes measured this against 1.4.4 through the chunked shim: alpha=60,
//    beta=20 before teardown, 0/0 after, and the persisted unified view then
//    answered "no such module: __flatsql_module_omm_alpha". sdn-js partitions
//    EVERY standard by source, so that made the browser lane unable to retire
//    its snapshot exports. The reopen below re-registers nothing.
// ---------------------------------------------------------------------------
uint64_t tableRows(FlatSQLDatabase& db, const std::string& tableName) {
    for (const auto& stat : db.getStats()) {
        if (stat.tableName == tableName) return stat.recordCount;
    }
    return 0;
}

int64_t queryCount(FlatSQLDatabase& db, const std::string& sql) {
    QueryResult result;
    std::string err;
    if (!db.queryNoThrow(sql, {}, result, &err)) return -1;
    if (result.rows.empty() || result.rows[0].empty()) return -1;
    if (const auto* n = std::get_if<int64_t>(&result.rows[0][0])) return *n;
    return -1;
}

void testSourcePartitionsSurviveTeardown() {
    std::cout << "Testing source partitions -> teardown -> reopen..." << std::endl;
    const std::string path = tempBase("sources.db");
    const std::vector<uint8_t> alpha = makeStream(25544, 60);
    const std::vector<uint8_t> beta = makeStream(40000, 20);

    std::string beforePayload;
    int64_t beforeUnified = 0;
    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.registerSource("alpha");
        db.registerSource("beta");
        db.ingestWithSource(alpha.data(), alpha.size(), "alpha", nullptr);
        db.ingestWithSource(beta.data(), beta.size(), "beta", nullptr);
        db.createUnifiedViews();

        CHECK(tableRows(db, "omm@alpha") == 60, "alpha holds 60 records pre-teardown");
        CHECK(tableRows(db, "omm@beta") == 20, "beta holds 20 records pre-teardown");
        beforeUnified = queryCount(db, "SELECT COUNT(*) FROM omm");
        CHECK(beforeUnified == 80, "unified view sees 80 records pre-teardown");
        CHECK(queryCount(db, "SELECT COUNT(*) FROM omm WHERE _source='omm@alpha'") == 60,
              "_source filter selects alpha's 60 pre-teardown");

        beforePayload = readPayloadBySequence(db, 0);
        CHECK(db.flushState() == FlatSQLDatabase::kStateOk, "flush succeeds");
    }  // TEAR DOWN

    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        // NOTHING is re-registered: no registerSource, no createUnifiedViews.
        const int restored = db.openState();
        CHECK(restored == 80, "openState replays all 80 records");

        CHECK(db.listSources().size() == 2, "both sources come back");
        CHECK(db.listSources()[0] == "alpha" && db.listSources()[1] == "beta",
              "sources come back in REGISTRATION order (view row order)");
        CHECK(tableRows(db, "omm@alpha") == 60, "alpha still holds 60 records");
        CHECK(tableRows(db, "omm@beta") == 20, "beta still holds 20 records");

        CHECK(queryCount(db, "SELECT COUNT(*) FROM omm") == beforeUnified,
              "the persisted unified view answers WITHOUT re-registration");
        CHECK(queryCount(db, "SELECT COUNT(*) FROM omm WHERE _source='omm@alpha'") == 60,
              "_source filter still selects alpha's 60");
        CHECK(readPayloadBySequence(db, 0) == beforePayload,
              "payload bytes are IDENTICAL after reopen");
    }

    // A late append to ONE source lands in that source, not in the base table.
    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        CHECK(db.openState() == 80, "second reopen replays 80");
        const std::vector<uint8_t> more = makeStream(50000, 5);
        db.ingestWithSource(more.data(), more.size(), "beta", nullptr);
        CHECK(db.flushState() == FlatSQLDatabase::kStateOk, "flush after late append");
    }
    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        CHECK(db.openState() == 85, "third reopen replays 85");
        CHECK(tableRows(db, "omm@alpha") == 60, "alpha unchanged by beta's append");
        CHECK(tableRows(db, "omm@beta") == 25, "beta grew by exactly the appended 5");
        CHECK(queryCount(db, "SELECT COUNT(*) FROM omm") == 85, "view sees 85");

        // Full re-derivation keeps the partition: the ranges are durable
        // metadata, not something the stream could ever re-derive.
        CHECK(db.reindexAll() == 85, "reindexAll replays 85");
        CHECK(tableRows(db, "omm@alpha") == 60, "alpha survives a full re-derivation");
        CHECK(tableRows(db, "omm@beta") == 25, "beta survives a full re-derivation");
    }
}

// ---------------------------------------------------------------------------
// 7. COMPAT: state written by a build with no partition tables (1.4.4 and
//    older, which is what is live on the fleet) must open unchanged — same
//    record count, same bytes, no -2, no re-derivation. Simulated by dropping
//    the two additive tables, which is byte-for-byte the older layout.
// ---------------------------------------------------------------------------
void testPrePartitionStateStillOpens() {
    std::cout << "Testing pre-1.4.5 state (no partition tables) -> opens..." << std::endl;
    const std::string path = tempBase("legacy.db");
    const std::vector<uint8_t> stream = makeStream(25544, 9);

    std::string before;
    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.ingest(stream.data(), stream.size(), nullptr);
        before = readPayloadBySequence(db, 0);
        CHECK(db.flushState() == FlatSQLDatabase::kStateOk, "flush");

        QueryResult ignored;
        std::string err;
        CHECK(db.queryNoThrow("DROP TABLE IF EXISTS _flatsql_sources", {}, ignored, &err),
              "drop _flatsql_sources (older layout)");
        CHECK(db.queryNoThrow("DROP TABLE IF EXISTS _flatsql_source_ranges", {}, ignored, &err),
              "drop _flatsql_source_ranges (older layout)");
        CHECK(db.queryNoThrow("DELETE FROM _flatsql_state WHERE k='source_index'",
                              {}, ignored, &err),
              "drop the source_index marker (older layout)");
    }

    {
        DatabaseSchema schema;
        FlatSQLDatabase db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        const int restored = db.openState();
        CHECK(restored == 9, "older state opens with every record, no -2");
        CHECK(readPayloadBySequence(db, 0) == before, "older state's bytes are identical");
        CHECK(db.listSources().empty(), "no partitions is 'no partitions', not an error");
    }
}

int g_commits = 0;
int observeCommit(void*) { ++g_commits; return 0; }
int observeConnection(sqlite3* db, char**, const sqlite3_api_routines*) {
    sqlite3_commit_hook(db, observeCommit, nullptr);
    return SQLITE_OK;
}

void testBulkCommitBound() {
    std::cout << "Testing bounded commits during bulk ingest and reindex..." << std::endl;
    const auto extension = reinterpret_cast<void (*)()>(observeConnection);
    CHECK(sqlite3_auto_extension(extension) == SQLITE_OK, "install commit observer");
    {
        DatabaseSchema schema;
        auto db = openDb(tempBase("bulk.db"), &schema);
        db.registerFileId("OMM ", "omm");
        db.registerSource("alpha");
        const auto stream = makeStream(1000, 128);
        g_commits = 0;
        CHECK(db.ingest(stream.data(), stream.size()) == stream.size(), "bulk base ingest");
        CHECK(g_commits <= 1, "base batch commits at most once, not per record");
        g_commits = 0;
        CHECK(db.ingestWithSource(stream.data(), stream.size(), "alpha") == stream.size(), "bulk source ingest");
        CHECK(g_commits <= 1, "source batch commits at most once");
        CHECK(db.flushState() == 0, "flush populated stream");
        g_commits = 0;
        CHECK(db.reindexAll() == 256, "reindex all records");
        CHECK(g_commits <= 1, "reindex commits index and mark together once");
        CHECK(tableRows(db, "omm@alpha") == 128, "partition preserved by batched reindex");

        QueryResult ignored;
        std::string err;
        CHECK(db.queryNoThrow("BEGIN IMMEDIATE", {}, ignored, &err), "caller transaction begins");
        CHECK(db.reindexAll() == 256, "reindex nests inside caller transaction");
        CHECK(db.queryNoThrow("COMMIT", {}, ignored, &err), "reindex leaves caller transaction open");
    }
    sqlite3_cancel_auto_extension(extension);
}

void testAdditiveSchemaPreservesIndexes() {
    std::cout << "Testing additive schema preserves existing indexes..." << std::endl;
    const auto path = tempBase("additive.db");
    const auto stream = makeStream(5000, 8);
    {
        DatabaseSchema schema;
        auto db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.ingest(stream.data(), stream.size());
        CHECK(db.flushState() == 0, "persist original schema");
    }
    DatabaseSchema schema;
    std::string err;
    CHECK(SchemaParser::tryParse(std::string(kSchema) + " table added { value: int; }",
                                &schema, &err, "sds"), "parse additive schema");
    FlatSQLDatabase::RuntimeOptions options;
    options.sqlite.path = path;
    options.sqlite.vfs = kFlatSqlVfsName;
    options.sqlite.journalMode = 2;
    FlatSQLDatabase db(schema, options);
    db.registerFileId("OMM ", "omm");
    int extractions = 0;
    db.setFieldExtractor("omm", [&](const uint8_t*, size_t, const std::string&) -> Value {
        ++extractions;
        return int32_t(0);
    });
    CHECK(db.openState() == 8, "adding an unrelated table keeps persisted state valid");
    CHECK(extractions == 0, "unchanged rows reuse persisted indexes without extraction");
    CHECK(db.getStorage().getRecordCount() == 8, "all existing records restored");
}

void testResumableReindex() {
    std::cout << "Testing resumable rebuild and interrupted transaction..." << std::endl;
    const auto path = tempBase("stepped.db");
    const auto stream = makeStream(100, 9);
    {
        DatabaseSchema schema;
        auto db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.ingest(stream.data(), stream.size());
        CHECK(db.flushState() == 0, "persist before stepped rebuild");
        CHECK(db.reindexStep(2) == 1, "first bounded step is pending");
        CHECK(db.getStorage().getRecordCount() == 2, "first step processes two records");
        QueryResult result;
        std::string error;
        CHECK(!db.queryNoThrow("SELECT COUNT(*) FROM omm", {}, result, &error), "partial index cannot answer a query");
        CHECK(error == "state: reindex incomplete", "pending state is explicit");
        CHECK(db.flushState() < 0, "partial rebuild cannot publish a checkpoint");
        CHECK(db.reindexStep(2) == 1, "second bounded step is pending");
        CHECK(db.getStorage().getRecordCount() == 4, "second step resumes from the cursor");
        // Teardown rolls back the pending index transaction. The stream and
        // previous complete checkpoint must still be usable.
    }
    {
        DatabaseSchema schema;
        auto db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        CHECK(db.openState() == 9, "interrupted rebuild preserves the complete checkpoint");
        int steps = 0;
        int rc;
        do { rc = db.reindexStep(2); ++steps; } while (rc == 1 && steps < 10);
        CHECK(rc == 0 && steps == 5, "bounded steps complete all nine records");
        CHECK(queryCount(db, "SELECT COUNT(*) FROM omm") == 9, "completed index answers all records");
        CHECK(db.exportData() == stream, "rebuild preserves canonical bytes exactly");
    }
}

void testNewlyRecognizedTable() {
    std::cout << "Testing existing wire records for an added table..." << std::endl;
    const auto path = tempBase("recognized.db");
    const auto frame = makeFrame("NEW ", 42, "newly recognized");
    {
        DatabaseSchema schema;
        auto db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.registerSource("alpha");
        db.ingestWithSource(frame.data(), frame.size(), "alpha");
        CHECK(db.flushState() == 0, "unrecognized record is durable");
    }
    DatabaseSchema schema;
    std::string error;
    CHECK(SchemaParser::tryParse(std::string(kSchema) +
        " table added { NORAD_CAT_ID: int (key); OBJECT_NAME: string; }", &schema, &error, "sds"), "parse added table");
    FlatSQLDatabase::RuntimeOptions options;
    options.sqlite.path = path;
    options.sqlite.vfs = kFlatSqlVfsName;
    options.sqlite.journalMode = 2;
    {
        FlatSQLDatabase db(schema, options);
        db.registerFileId("OMM ", "omm");
        db.registerFileId("NEW ", "added");
        CHECK(db.openState() == 1, "added table reuses the existing wire stream");
        CHECK(queryCount(db, "SELECT COUNT(*) FROM added WHERE NORAD_CAT_ID=42") == 1, "new table gets indexed and keeps its source partition");
    }
    {
        FlatSQLDatabase db(schema, options);
        db.registerFileId("NEW ", "added");
        CHECK(db.openState() == 1, "additive checkpoint survives a second reopen");
        CHECK(queryCount(db, "SELECT COUNT(*) FROM added WHERE NORAD_CAT_ID=42") == 1, "new index survives reopen");
    }
    // fieldId is deliberately absent from the legacy aggregate fingerprint.
    schema.tables[1].columns[0].fieldId = 7;
    FlatSQLDatabase changed(schema, options);
    CHECK(changed.openState() == FlatSQLDatabase::kStateVersionMismatch, "complete per-table contract detects a changed field ID");
}

void testFailedBatchRequiresRecovery() {
    std::cout << "Testing failed bulk extraction cannot expose divergent indexes..." << std::endl;
    const auto path = tempBase("failed-batch.db");
    const auto stream = makeStream(100, 4);
    {
        DatabaseSchema schema;
        auto db = openDb(path, &schema);
        db.registerFileId("OMM ", "omm");
        db.ingest(stream.data(), stream.size());
        CHECK(db.flushState() == 0, "checkpoint before failed batch");
        db.setFieldExtractor("omm", [](const uint8_t*, size_t, const std::string&) -> Value {
            throw std::runtime_error("injected extraction failure");
        });
        bool failed = false;
        try { db.ingest(stream.data(), stream.size()); }
        catch (const std::runtime_error&) { failed = true; }
        CHECK(failed, "failed extraction is reported");
        QueryResult result;
        std::string error;
        CHECK(!db.queryNoThrow("SELECT COUNT(*) FROM omm", {}, result, &error), "failed batch cannot expose rolled-back indexes with appended bytes");
        CHECK(db.flushState() < 0, "failed batch cannot advance the durable checkpoint");
    }
    DatabaseSchema schema;
    auto recovered = openDb(path, &schema);
    recovered.registerFileId("OMM ", "omm");
    CHECK(recovered.openState() == 4, "reopening recovers the last successful checkpoint");
    CHECK(queryCount(recovered, "SELECT COUNT(*) FROM omm") == 4, "failed batch did not alter durable indexes");
}

}  // namespace

int main() {
    std::cout << "=== FlatSQL durable state matrix ===" << std::endl;
    testSurvivesTeardown();
    testTailReindex();
    testCorruptFallsBackToReindex();
    testSchemaChangeInvalidates();
    testMemoryReportsNoFilesystem();
    testSourcePartitionsSurviveTeardown();
    testPrePartitionStateStillOpens();
    testBulkCommitBound();
    testAdditiveSchemaPreservesIndexes();
    testResumableReindex();
    testNewlyRecognizedTable();
    testFailedBatchRequiresRecovery();

    if (g_failures) {
        std::cerr << g_failures << " check(s) FAILED" << std::endl;
        return 1;
    }
    std::cout << "All durable-state checks passed." << std::endl;
    return 0;
}
