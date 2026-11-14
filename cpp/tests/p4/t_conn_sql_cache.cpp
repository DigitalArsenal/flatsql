// Conn::sql's statement cache must not finalize a statement its caller still
// holds. Up to 70026dcb (3.12.1) a full cache (64 texts) finalized
// dyn.begin() for the next new text; in libc++ that is often the statement
// handed out just before, and appendConsolidated (frames.cpp:215-242) and
// checkpointAppend (checkpoint.cpp:232-235) hold one statement across the
// prepare of another. SUPERSEDE fills the cache with one-off
// "... WHERE b IN (...)" texts (remove.cpp:123, 155), so a node writing a
// supersede per object stepped finalized statements and corrupted its heap.
#include <sqlite3.h>

#include <string>

#include "internal.h"
#include "p4/p4_test.h"

using namespace p4t;

namespace {

bool held(flatsql::p4::Conn& c, const std::string& text) { return c.dyn.count(text) == 1; }

}  // namespace

FLATSQL_TEST(t_conn_sql_cache_keeps_held_statements) {
    flatsql::p4::Conn c;
    REQUIRE(sqlite3_open_v2(":memory:", &c.db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY,
                            nullptr) == SQLITE_OK,
            "open");
    REQUIRE(sqlite3_exec(c.db, "CREATE TABLE r(rid INTEGER PRIMARY KEY, b INTEGER); INSERT INTO r(b) VALUES(1),(2),(3)",
                         nullptr, nullptr, nullptr) == SQLITE_OK,
            "schema");
    // A full cache of one-off texts, as per-object supersedes leave it.
    for (int i = 0; i < 64; i++) REQUIRE(c.sql("SELECT rid FROM r WHERE b IN (" + std::to_string(i) + ")"), "fill");
    for (int round = 0; round < 64; round++) {
        const std::string n = std::to_string(round);
        // appendConsolidated: q is stepped, and a new text is prepared inside
        // the step loop before q is stepped again.
        const std::string qText = "SELECT rid, b FROM r ORDER BY rid /* q " + n + " */";
        sqlite3_stmt* q = c.sql(qText);
        REQUIRE(q, "prepare q");
        REQUIRE(sqlite3_step(q) == SQLITE_ROW, "first row");
        REQUIRE(c.sql("SELECT b FROM r WHERE rid=?1 /* iq " + n + " */"), "prepare iq");
        CHECK(held(c, qText), "a statement being stepped was finalized by a nested prepare (round " + n + ")");
        if (!held(c, qText)) return;  // stepping q now would use freed memory
        int rows = 1;
        while (sqlite3_step(q) == SQLITE_ROW) rows++;
        CHECK_EQ(rows, 3, "rows of the held statement");
        sqlite3_reset(q);
        // checkpointAppend: q is prepared, a second new text is prepared, then
        // q is bound and stepped.
        const std::string pText = "SELECT rid FROM r WHERE rid=?1 /* p " + n + " */";
        sqlite3_stmt* p = c.sql(pText);
        REQUIRE(p, "prepare p");
        REQUIRE(c.sql("SELECT b FROM r WHERE rid=?1 /* cid " + n + " */"), "prepare cid");
        CHECK(held(c, pText), "a prepared statement was finalized by the next prepare (round " + n + ")");
        if (!held(c, pText)) return;
        sqlite3_bind_int64(p, 1, 2);
        CHECK_EQ(sqlite3_step(p), SQLITE_ROW, "the held statement still answers");
        sqlite3_reset(p);
    }
    CHECK(c.dyn.size() <= 64, "the cache stays bounded");
}
