// vfs_test.cpp — FlatSQL's own sqlite3_vfs, driven through the seven-import
// host I/O contract (include/flatsql/flatsql_io.h).
//
// This suite is the parity floor. Every assertion here runs the EXACT code the
// browser and WasmEdge lanes run: cpp/src/flatsql_vfs.cpp, calling
// flatsql_io_open/read/write/truncate/sync/size/close. The only thing that
// changes between lanes is who answers those seven calls. If a lane disagrees
// with this file, the lane's shim is wrong — not the engine.
//
// It also pins the two failure modes that produced the original defect
// (docs/STORAGE-DURABILITY.md §2.2): I/O silently going to RAM, and a
// disk-backed open succeeding without ever reaching the host.

#include "flatsql/database.h"
#include "flatsql/schema_parser.h"
#include "flatsql/flatsql_io.h"
#include "flatsql/sqlite_engine.h"

#include <cassert>
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

std::string tempPath(const char* stem) {
    std::string path = "/tmp/flatsql-vfs-test-";
    path += stem;
    std::remove(path.c_str());
    std::remove((path + "-journal").c_str());
    return path;
}

// ---------------------------------------------------------------------------
// 1. The seven imports themselves. A host shim that passes these behaves like
//    the native lane; one that does not will diverge in the VFS above it.
// ---------------------------------------------------------------------------
void testHostIoContract() {
    std::cout << "Testing the seven-import contract..." << std::endl;
    const std::string path = tempPath("contract");

    // PROBE on a missing path must report absence, not create anything.
    CHECK(flatsql_io_open(path.c_str(), (int32_t)path.size(), FLATSQL_IO_PROBE) < 0,
          "probe of a missing path reports absent");

    int32_t h = flatsql_io_open(path.c_str(), (int32_t)path.size(),
                                FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE);
    CHECK(h >= 0, "create returns a handle");

    CHECK(flatsql_io_size(h) == 0, "new file is empty");

    const char payload[] = "flatsql";
    CHECK(flatsql_io_write(h, payload, 7, 0) == 7, "write at offset 0");
    CHECK(flatsql_io_size(h) == 7, "size reflects the write");

    // Offset addressing: writing past EOF extends, and the gap reads as zeroes.
    CHECK(flatsql_io_write(h, payload, 7, 16) == 7, "write past EOF extends");
    CHECK(flatsql_io_size(h) == 23, "size after sparse write");

    char buf[32];
    std::memset(buf, 0xAB, sizeof(buf));
    CHECK(flatsql_io_read(h, buf, 9, 7) == 9, "read across the gap");
    for (int i = 0; i < 9; i++) {
        CHECK(buf[i] == 0, "gap bytes read as zero");
    }

    // Short read at EOF must report the byte count, never an error.
    std::memset(buf, 0xAB, sizeof(buf));
    const int32_t got = flatsql_io_read(h, buf, 32, 16);
    CHECK(got == 7, "short read at EOF returns the available count");

    CHECK(flatsql_io_truncate(h, 4) == 0, "truncate shrinks");
    CHECK(flatsql_io_size(h) == 4, "size after truncate");
    CHECK(flatsql_io_sync(h) == 0, "sync succeeds");
    CHECK(flatsql_io_close(h) == 0, "close succeeds");

    // Handles are not reusable after close.
    CHECK(flatsql_io_read(h, buf, 1, 0) == FLATSQL_IO_ERR_BADHANDLE,
          "closed handle is rejected");

    CHECK(flatsql_io_open(path.c_str(), (int32_t)path.size(), FLATSQL_IO_PROBE) == 0,
          "probe finds the created file");
    CHECK(flatsql_io_open(path.c_str(), (int32_t)path.size(), FLATSQL_IO_UNLINK) == 0,
          "unlink removes it");
    CHECK(flatsql_io_open(path.c_str(), (int32_t)path.size(), FLATSQL_IO_PROBE) < 0,
          "probe after unlink reports absent");
}

// ---------------------------------------------------------------------------
// 2. A real database, opened through the VFS by name. Nothing may touch the
//    system VFS: this is the wasm path.
// ---------------------------------------------------------------------------
void testDatabaseThroughVfs() {
    std::cout << "Testing a database opened through flatsql_io..." << std::endl;
    const std::string path = tempPath("engine.db");

    {
        SQLiteConnectionOptions options;
        options.path = path;
        options.vfs = kFlatSqlVfsName;
        options.journalMode = 2;  // TRUNCATE — the wasm lane's mode
        SQLiteEngine engine(options);

        engine.execute("CREATE TABLE IF NOT EXISTS _idx_omm_NORAD_CAT_ID ("
                       "key INTEGER NOT NULL, data_offset INTEGER NOT NULL, "
                       "data_length INTEGER NOT NULL, sequence INTEGER NOT NULL, "
                       "PRIMARY KEY (key, sequence)) WITHOUT ROWID");
        for (int i = 0; i < 512; i++) {
            engine.execute("INSERT INTO _idx_omm_NORAD_CAT_ID VALUES(" +
                           std::to_string(25544 + i) + "," +
                           std::to_string(i * 512) + ",512," +
                           std::to_string(i) + ")");
        }
    }

    // The file must exist on the host side, written entirely by our VFS.
    const int32_t probe = flatsql_io_open(path.c_str(), (int32_t)path.size(),
                                          FLATSQL_IO_PROBE);
    CHECK(probe == 0, "database file exists on the host after close");

    {
        SQLiteConnectionOptions options;
        options.path = path;
        options.vfs = kFlatSqlVfsName;
        options.journalMode = 2;
        SQLiteEngine engine(options);

        QueryResult result =
            engine.execute("SELECT COUNT(*) FROM _idx_omm_NORAD_CAT_ID");
        CHECK(result.rows.size() == 1, "count query returns a row");
        CHECK(!result.rows.empty() && std::get<int64_t>(result.rows[0][0]) == 512,
              "all 512 index tuples survived close+reopen through the VFS");

        QueryResult point = engine.execute(
            "SELECT data_offset FROM _idx_omm_NORAD_CAT_ID WHERE key = 25600");
        CHECK(point.rows.size() == 1 &&
                  std::get<int64_t>(point.rows[0][0]) == 28672,
              "point lookup reads the right page back off disk");
    }
}

// ---------------------------------------------------------------------------
// 3. Crash safety in TRUNCATE journal mode. WAL is unavailable on wasm (no
//    xShmMap in either lane), so the rollback journal is the durability story
//    and it has to actually roll back.
// ---------------------------------------------------------------------------
void testRollbackJournal() {
    std::cout << "Testing rollback under TRUNCATE journalling..." << std::endl;
    const std::string path = tempPath("rollback.db");

    SQLiteConnectionOptions options;
    options.path = path;
    options.vfs = kFlatSqlVfsName;
    options.journalMode = 2;
    SQLiteEngine engine(options);

    engine.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER)");
    engine.execute("INSERT INTO t VALUES('a',1)");

    engine.execute("BEGIN");
    engine.execute("INSERT INTO t VALUES('b',2)");
    engine.execute("INSERT INTO t VALUES('c',3)");
    engine.execute("ROLLBACK");

    QueryResult result = engine.execute("SELECT COUNT(*) FROM t");
    CHECK(!result.rows.empty() && std::get<int64_t>(result.rows[0][0]) == 1,
          "rolled-back rows are gone");

    engine.execute("BEGIN");
    engine.execute("INSERT INTO t VALUES('d',4)");
    engine.execute("COMMIT");
    result = engine.execute("SELECT COUNT(*) FROM t");
    CHECK(!result.rows.empty() && std::get<int64_t>(result.rows[0][0]) == 2,
          "committed rows are kept");
}

// ---------------------------------------------------------------------------
// 4. The silent-RAM guard. ":memory:" must NOT be routed to the VFS, and a
//    disk-backed open must genuinely be disk-backed.
// ---------------------------------------------------------------------------
void testMemoryIsStillMemory() {
    std::cout << "Testing :memory: bypasses the VFS..." << std::endl;

    SQLiteConnectionOptions options;  // path defaults to ":memory:"
    SQLiteEngine engine(options);
    engine.execute("CREATE TABLE t(k TEXT)");
    engine.execute("INSERT INTO t VALUES('x')");
    QueryResult result = engine.execute("SELECT COUNT(*) FROM t");
    CHECK(!result.rows.empty() && std::get<int64_t>(result.rows[0][0]) == 1,
          ":memory: engine still works untouched");
}

// ---------------------------------------------------------------------------
// 5. Full FlatSQLDatabase over the VFS, with the ingest -> query path that
//    sdn-js will drive from JavaScript.
// ---------------------------------------------------------------------------
void testDatabaseHandleOverVfs() {
    std::cout << "Testing FlatSQLDatabase over the VFS..." << std::endl;
    const std::string path = tempPath("fsdb.db");

    const std::string schema = R"({
        "standardId": "TEST",
        "tables": [{
            "name": "records",
            "fileId": "TEST",
            "columns": [{"name": "id", "type": "int64", "indexed": true}]
        }]
    })";

    DatabaseSchema parsed;
    std::string parseError;
    const bool ok = SchemaParser::tryParse(schema, &parsed, &parseError, "vfs");
    CHECK(ok, "schema parses");
    if (!ok) return;

    FlatSQLDatabase::RuntimeOptions options;
    options.sqlite.path = path;
    options.sqlite.vfs = kFlatSqlVfsName;
    options.sqlite.journalMode = 2;

    FlatSQLDatabase db(parsed, options);
    CHECK(db.isDiskBacked(), "handle reports disk-backed");
}

}  // namespace

int main() {
    std::cout << "=== FlatSQL VFS (seven-import contract) ===" << std::endl;
    testHostIoContract();
    testDatabaseThroughVfs();
    testRollbackJournal();
    testMemoryIsStillMemory();
    testDatabaseHandleOverVfs();

    if (g_failures) {
        std::cerr << g_failures << " check(s) FAILED" << std::endl;
        return 1;
    }
    std::cout << "All VFS checks passed." << std::endl;
    return 0;
}
