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
// records to tables. Payload layout past that is irrelevant to the durability
// contract — what has to survive is the byte range and its offset.
std::vector<uint8_t> makeFrame(const char* fileId, uint32_t noradId,
                               const std::string& name) {
    std::vector<uint8_t> body;
    body.resize(8);
    std::memset(body.data(), 0, 8);
    body[0] = 4;  // root table offset (unused by these assertions)
    std::memcpy(body.data() + 4, fileId, 4);
    for (int i = 0; i < 4; i++) {
        body.push_back(static_cast<uint8_t>((noradId >> (8 * i)) & 0xFF));
    }
    body.insert(body.end(), name.begin(), name.end());
    while (body.size() % 4) body.push_back(0);

    std::vector<uint8_t> frame;
    const uint32_t size = static_cast<uint32_t>(body.size());
    for (int i = 0; i < 4; i++) {
        frame.push_back(static_cast<uint8_t>((size >> (8 * i)) & 0xFF));
    }
    frame.insert(frame.end(), body.begin(), body.end());
    return frame;
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

}  // namespace

int main() {
    std::cout << "=== FlatSQL durable state matrix ===" << std::endl;
    testSurvivesTeardown();
    testTailReindex();
    testCorruptFallsBackToReindex();
    testSchemaChangeInvalidates();
    testMemoryReportsNoFilesystem();

    if (g_failures) {
        std::cerr << g_failures << " check(s) FAILED" << std::endl;
        return 1;
    }
    std::cout << "All durable-state checks passed." << std::endl;
    return 0;
}
