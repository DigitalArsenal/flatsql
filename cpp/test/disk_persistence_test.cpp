// disk_persistence_test.cpp — the disk-backed engine path.
//
// Locks the fix for the defect recorded in docs/STORAGE-DURABILITY.md: the C++
// core could always open a real database file, but no wasm consumer could ask
// for one, so every engine everywhere was ephemeral. These tests exercise the
// path that flatsql_open_db() now reaches.
//
// Native build only. The wasm lane needs the shim-bridged VFS (see the doc,
// §3.5) before it can run the same assertions; until then this suite is what
// proves the engine half is correct.

#include "flatsql/database.h"
#include "flatsql/sqlite_engine.h"
#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

using namespace flatsql;

namespace {

std::string tempDbPath(const char* stem) {
    std::string path = "/tmp/flatsql-disk-test-";
    path += stem;
    path += ".db";
    std::remove(path.c_str());
    std::remove((path + "-journal").c_str());
    return path;
}

bool fileExistsNonEmpty(const std::string& path) {
    FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return false;
    std::fseek(f, 0, SEEK_END);
    const long size = std::ftell(f);
    std::fclose(f);
    return size > 0;
}

// A disk-backed engine must keep its btree/table data across a full close and
// reopen. This is the core of the owner's "save the btree / table data to disk
// and read it from there" requirement.
void testTableDataSurvivesReopen() {
    std::cout << "Testing table data survives close+reopen..." << std::endl;
    const std::string path = tempDbPath("reopen");

    {
        SQLiteConnectionOptions options;
        options.path = path;
        options.journalMode = 2;  // TRUNCATE — crash-safe, no shared memory
        SQLiteEngine engine(options);

        engine.execute("CREATE TABLE IF NOT EXISTS idx_demo ("
                       "key TEXT NOT NULL, data_offset INTEGER NOT NULL, "
                       "sequence INTEGER NOT NULL, PRIMARY KEY (key, sequence)) WITHOUT ROWID");
        engine.execute("INSERT INTO idx_demo VALUES ('SAT-10001', 0, 1)");
        engine.execute("INSERT INTO idx_demo VALUES ('SAT-10002', 512, 2)");
        engine.execute("INSERT INTO idx_demo VALUES ('SAT-10003', 1024, 3)");
    }  // engine closed — everything must now be on disk

    assert(fileExistsNonEmpty(path) && "database file must exist after close");

    {
        SQLiteConnectionOptions options;
        options.path = path;
        options.journalMode = 2;
        SQLiteEngine engine(options);

        QueryResult result = engine.execute(
            "SELECT key, data_offset, sequence FROM idx_demo ORDER BY sequence");
        assert(result.rows.size() == 3 && "all three rows must survive the reopen");

        // Byte-identical round trip of the (key, offset, sequence) tuple — the
        // exact shape a durable index over the FlatBuffer stream stores.
        assert(std::get<std::string>(result.rows[0][0]) == "SAT-10001");
        assert(std::get<int64_t>(result.rows[1][1]) == 512);
        assert(std::get<int64_t>(result.rows[2][2]) == 3);
    }

    std::remove(path.c_str());
    std::cout << "  PASS: 3/3 index tuples survived close+reopen" << std::endl;
}

// ":memory:" must remain byte-for-byte the historical behaviour, so every
// existing consumer is unaffected by the new option.
void testMemoryPathUnchanged() {
    std::cout << "Testing :memory: engine is unchanged..." << std::endl;

    SQLiteConnectionOptions options;  // defaults: path=":memory:"
    assert(options.path == ":memory:");
    assert(options.journalMode == 0);
    assert(options.enableWal == false);

    SQLiteEngine engine(options);
    engine.execute("CREATE TABLE t (a INTEGER)");
    engine.execute("INSERT INTO t VALUES (7)");
    QueryResult result = engine.execute("SELECT a FROM t");
    assert(result.rows.size() == 1);
    assert(std::get<int64_t>(result.rows[0][0]) == 7);

    std::cout << "  PASS: ephemeral engine unchanged" << std::endl;
}

// FlatSQLDatabase must report which mode it is in, and must actually create the
// file when handed a path. isDiskBacked() is what the host uses to decide
// whether a boot may trust persisted state.
void testDatabaseReportsDiskBacked() {
    std::cout << "Testing FlatSQLDatabase disk-backed reporting..." << std::endl;
    const std::string path = tempDbPath("dbflag");

    const std::string idl = R"(
        table Sat {
            id: int (id);
            name: string (key);
        }
    )";
    DatabaseSchema schema = SchemaParser::parseIDL(idl, "disk_db");

    {
        FlatSQLDatabase::RuntimeOptions options;
        options.sqlite.path = path;
        options.sqlite.journalMode = 2;
        FlatSQLDatabase db(schema, std::move(options));
        assert(db.isDiskBacked() && "path-backed database must report disk-backed");
    }
    assert(fileExistsNonEmpty(path) && "opening with a path must create the file");

    {
        FlatSQLDatabase db(schema);  // default options
        assert(!db.isDiskBacked() && "default database must report ephemeral");
    }

    std::remove(path.c_str());
    std::cout << "  PASS: disk-backed reporting correct both ways" << std::endl;
}

// The journal mode the host asks for must actually be the one SQLite adopts;
// silently falling back would make durability guarantees fiction.
void testJournalModeIsApplied() {
    std::cout << "Testing journal mode is really applied..." << std::endl;
    const std::string path = tempDbPath("journal");

    SQLiteConnectionOptions options;
    options.path = path;
    options.journalMode = 2;  // TRUNCATE
    SQLiteEngine engine(options);

    QueryResult result = engine.execute("PRAGMA journal_mode");
    assert(result.rows.size() == 1);
    const std::string mode = std::get<std::string>(result.rows[0][0]);
    assert(mode == "truncate" && "requested TRUNCATE must be the adopted mode");

    std::remove(path.c_str());
    std::cout << "  PASS: journal_mode=truncate adopted" << std::endl;
}

}  // namespace

int main() {
    std::cout << "=== FlatSQL disk-persistence tests ===" << std::endl;
    testMemoryPathUnchanged();
    testTableDataSurvivesReopen();
    testDatabaseReportsDiskBacked();
    testJournalModeIsApplied();
    std::cout << "=== ALL DISK-PERSISTENCE TESTS PASSED ===" << std::endl;
    return 0;
}
