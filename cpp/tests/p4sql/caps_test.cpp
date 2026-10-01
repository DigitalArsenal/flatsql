// Caps, cancel, the sandbox and raw framing (CONTRACT.md §3.9, §6 "sql"):
// every limit ends the statement with a status (P4_E_BUDGET, P4_E_SQL,
// P4_E_CANCELLED, P4_E_ARG), never a trap, and the lane serves the next
// statement.
#ifdef FLATSQL_P4SQL_FAKE

#include <sqlite3.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "p4sql_test.h"

using namespace p4sqlt;

namespace {

void fill(Harness& h, int n) {
    static const std::vector<uint8_t> bfbs = readFile(vectorDir() + "/OMM.bfbs");
    h.addType("OMM", "$OMM", bfbs, 400000);
    for (int s = 1; s <= n; s++) {
        p4fake::Rec r;
        r.seq = s;
        r.cid = "bafkreicap" + std::to_string(s);
        r.producer = "source_celestrak";
        r.peer = "12D3KooWExample";
        r.ts = 1790000000 + s;
        r.data = buildRecord(bfbs, "{\"OBJECT_NAME\":\"SAT\",\"NORAD_CAT_ID\":" + std::to_string(25000 + s) + "}", false);
        p4fake::Tag t;
        t.provider = "space-data-network-02";
        t.source = "celestrak-gp";
        t.at = r.ts;
        r.tags.push_back(t);
        h.put("OMM", r);
    }
}

// The lane still answers after a refused statement.
void stillServes(Harness& h) {
    const Result r = h.sql("SELECT COUNT(*) FROM OMM");
    CHECK_EQ(r.status, 0);
    CHECK(!r.rows.empty());
}

}  // namespace

P4SQL_TEST(caps_result_rows_and_bytes) {
    Harness h;
    fill(h, 20);
    Caps c;
    c.maxResultRows = 5;
    Result r = h.sql("SELECT _seq FROM OMM", {}, 0, c);
    CHECK_EQ(r.status, P4_E_BUDGET);
    CHECK(r.error.find("row-cap") == 0);
    CHECK_EQ(r.rows.size(), size_t(5));   // the rows within the cap, then the end marker
    CHECK(r.ended && r.endStatus == P4_E_BUDGET);
    c.maxResultRows = 20;
    r = h.sql("SELECT _seq FROM OMM", {}, 0, c);
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(20));

    Caps b;
    b.maxResultBytes = 600;
    r = h.sql("SELECT _data FROM OMM", {}, P4_SLOT_RAW, b);
    CHECK_EQ(r.status, P4_E_BUDGET);
    CHECK(r.error.find("byte-cap") == 0);
    CHECK(r.raw.size() <= 600);
    r = h.sql("SELECT _data FROM OMM", {}, 0, b);
    CHECK_EQ(r.status, P4_E_BUDGET);
    // The slot's own result cap (the engine refuses ring bytes).
    Caps s;
    s.slotMaxResultBytes = 300;
    r = h.sql("SELECT _data FROM OMM", {}, P4_SLOT_RAW, s);
    CHECK_EQ(r.status, P4_E_BUDGET);
    stillServes(h);
}

P4SQL_TEST(caps_reader_rows_examined_and_bytes_read) {
    Harness h;
    fill(h, 50);
    Caps c;
    c.maxRowsExamined = 10;
    Result r = h.sql("SELECT COUNT(*) FROM OMM", {}, P4_SLOT_SANDBOX, c);
    CHECK_EQ(r.status, P4_E_BUDGET);
    CHECK(r.ended && r.endStatus == P4_E_BUDGET);
    Caps b;
    b.maxBytesRead = 500;
    r = h.sql("SELECT _data FROM OMM", {}, P4_SLOT_RAW | P4_SLOT_SANDBOX, b);
    CHECK_EQ(r.status, P4_E_BUDGET);
    stillServes(h);
}

// Cancel between rows: the reader's cursor and the progress handler both
// see the slot's cancel word.
P4SQL_TEST(cancel_between_rows) {
    Harness h;
    fill(h, 50);
    Caps c;
    c.cancelAfterRows = 7;
    Result r = h.sql("SELECT _seq FROM OMM", {}, 0, c);
    CHECK_EQ(r.status, P4_E_CANCELLED);
    CHECK(r.rowsExamined <= 7);
    // A statement that reads nothing: the progress handler.
    h.lane.resetSlot();
    std::atomic<bool> done{false};
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        h.lane.cancel = 1;
        done = true;
    });
    P4SqlRequest req;
    std::memset(&req, 0, sizeof(req));
    const std::string sql = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c";
    req.sql = sql.data();
    req.sqlLen = uint32_t(sql.size());
    const auto t0 = std::chrono::steady_clock::now();
    const int32_t st = p4sql_exec(&h.lane, &req);
    const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
    canceller.join();
    CHECK_EQ(st, P4_E_CANCELLED);
    CHECK(ms < 5000);
    report("cancel_latency_upper_bound_ms", ms - 50, "ms");
    stillServes(h);
}

// Untrusted SQL: one read-only SELECT over the record relations, the VM
// step budget and the lane heap cap.
P4SQL_TEST(sandbox_refusals) {
    Harness h;
    fill(h, 5);
    struct Case {
        const char* sql;
        const char* prefix;
    };
    const Case cases[] = {
        {"PRAGMA table_info(OMM)", "sandbox: not-authorized: PRAGMA"},
        {"SELECT * FROM sqlite_master", "sandbox: not-authorized: table \"sqlite_master\""},
        {"SELECT * FROM pragma_table_info('OMM')", "sandbox: not-authorized"},
        {"CREATE TABLE t(a)", "sandbox: not-authorized"},
        {"ATTACH ':memory:' AS x", "sandbox: not-authorized"},
        {"SELECT 1; SELECT 2", "sandbox: multi-statement"},
        {"BEGIN", "sandbox: not-authorized: TRANSACTION"},
        {"SELECT * FROM flatsql_partitions", "no such table"},
    };
    for (const Case& c : cases) {
        const Result r = h.sql(c.sql, {}, P4_SLOT_SANDBOX);
        CHECK_EQ(r.status, P4_E_SQL);
        if (r.error.find(c.prefix) != 0 && r.error.find(c.prefix) == std::string::npos)
            failAt(__FILE__, __LINE__, std::string(c.sql) + ": " + r.error);
        CHECK(r.ended && r.endStatus == P4_E_SQL);
    }
    // Allowed: relations, subqueries, CTEs, functions.
    const char* ok[] = {
        "SELECT COUNT(*) FROM OMM",
        "SELECT _source, COUNT(*) FROM \"OMM@celestrak-gp\" GROUP BY _source",
        "WITH x AS (SELECT NORAD_CAT_ID n FROM OMM) SELECT max(n) FROM x",
        "SELECT (SELECT COUNT(*) FROM OMM) + 1",
    };
    for (const char* q : ok) {
        const Result r = h.sql(q, {}, P4_SLOT_SANDBOX);
        CHECK_EQ(r.status, 0);
        if (r.status) std::fprintf(stderr, "    %s: %s\n", q, r.error.c_str());
    }
    // Parameters.
    Result r = h.sql("SELECT ?1 + ?2", {cInt(1)}, P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, P4_E_SQL);
    CHECK(r.error.find("parameter count mismatch") == 0);
    P4SqlRequest req;
    std::memset(&req, 0, sizeof(req));
    const std::string sql = "SELECT 1";
    const uint8_t bad[3] = {5, 0, 0};
    req.sql = sql.data();
    req.sqlLen = uint32_t(sql.size());
    req.params = bad;
    req.paramsLen = 3;
    h.lane.resetSlot();
    CHECK_EQ(p4sql_exec(&h.lane, &req), P4_E_ARG);
    stillServes(h);
}

P4SQL_TEST(sandbox_work_budget) {
    Harness h;
    fill(h, 2);
    const auto t0 = std::chrono::steady_clock::now();
    const Result r = h.sql("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c", {},
                           P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, P4_E_BUDGET);
    CHECK(r.error.find("VM steps") != std::string::npos);
    report("sandbox_vm_budget_trip_ms",
           std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count(), "ms");
    stillServes(h);
}

// The lane heap cap: a sandboxed statement past it fails with P4_E_BUDGET;
// the same statement trusted (no cap) completes.
P4SQL_TEST(sandbox_heap_cap) {
    Harness h;
    fill(h, 2);
    const std::string big = "SELECT length(randomblob(100000000))";
    Result r = h.sql(big, {}, P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, P4_E_BUDGET);
    CHECK(r.error.find("heap cap") == 0);
    r = h.sql(big);
    CHECK_EQ(r.status, 0);
    CHECK(!r.rows.empty() && r.rows[0][0] == cInt(100000000));
    // A sort that grows past the cap.
    const std::string sort =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 200000) "
        "SELECT count(*) FROM (SELECT x, randomblob(1000) b FROM c ORDER BY b)";
    r = h.sql(sort, {}, P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, P4_E_BUDGET);
    stillServes(h);
}

P4SQL_TEST(raw_framing) {
    Harness h;
    fill(h, 4);
    Result r = h.sql("SELECT _data, _data FROM OMM WHERE _seq <= 2", {}, P4_SLOT_RAW);
    CHECK_EQ(r.status, 0);
    std::vector<std::string> frames;
    CHECK(rb1::rawSplit(r.raw.data(), r.raw.size(), &frames));
    CHECK_EQ(frames.size(), size_t(4));
    const Result rows = h.sql("SELECT _data FROM OMM WHERE _seq <= 2");
    CHECK_EQ(rows.rows.size(), size_t(2));
    if (frames.size() == 4 && rows.rows.size() == 2) {
        CHECK(frames[0] == rows.rows[0][0].s && frames[1] == rows.rows[0][0].s);
        CHECK(frames[2] == rows.rows[1][0].s);
    }
    r = h.sql("SELECT 1", {}, P4_SLOT_RAW);
    CHECK_EQ(r.status, P4_E_SQL);
    CHECK(r.error.find("not-a-record-stream") == 0);
    CHECK(r.raw.empty());
    r = h.sql("SELECT _data FROM OMM WHERE _seq > 100", {}, P4_SLOT_RAW);
    CHECK_EQ(r.status, 0);
    CHECK(r.raw.empty());
}

// M2: a sandboxed statement at its lane heap cap cannot take the memory the
// writers need, nor slow them. The writer is shaped like the engine's
// partition writer (design §2.1, §4): a file-backed WAL database with
// synchronous=FULL, 4,096-record group commits of ~400-byte records with the
// window index, on its own thread, under the process-wide hard heap limit.
// A capped sandbox statement that churns its heap runs in a loop beside it.
// The writer's rate stays within 10% of its rate alone and none of its
// allocations fails. P4SQL_MEMSTATUS=0 runs it with SQLite's memory
// statistics (and their global mutex) off.
P4SQL_TEST(sandbox_cannot_starve_writers) {
    Harness h;
    fill(h, 2);
    std::string dir = env("TMPDIR");
    if (dir.empty()) dir = "/tmp";
    const std::string path = dir + "/p4sql-starve-" + std::to_string(uint64_t(std::chrono::steady_clock::now().time_since_epoch().count())) + ".db";
    const sqlite3_int64 prior = sqlite3_hard_heap_limit64(-1);
    sqlite3_hard_heap_limit64(640ll << 20);   // the engine's default hard heap (config tag 27)
    struct Writer {
        std::string path;
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> rows{0}, nomem{0};
        int firstRc = 0;
        std::string firstMsg;
        void failed(sqlite3* db, int rc) {
            if (!nomem++) {
                firstRc = rc;
                firstMsg = sqlite3_errmsg(db);
            }
        }
        void run() {
            std::remove(path.c_str());   // each run starts from an empty store
            std::remove((path + "-wal").c_str());
            std::remove((path + "-shm").c_str());
            sqlite3* db = nullptr;
            if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
                nomem++;
                return;
            }
            sqlite3_exec(db,
                         "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-4096;"
                         "CREATE TABLE IF NOT EXISTS r(seq INTEGER PRIMARY KEY, cid BLOB NOT NULL, e INTEGER, k,"
                         " ts INTEGER NOT NULL, x BLOB, d BLOB NOT NULL);"
                         "CREATE INDEX IF NOT EXISTS r_w ON r(coalesce(e, ts) DESC);",
                         nullptr, nullptr, nullptr);
            sqlite3_stmt* ins = nullptr;
            sqlite3_prepare_v2(db, "INSERT INTO r(seq, cid, e, k, ts, d) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", -1, &ins,
                               nullptr);
            std::vector<uint8_t> rec(400), cid(32);
            for (size_t i = 0; i < rec.size(); i++) rec[i] = uint8_t(i * 31);
            uint64_t n = 0;
            while (!stop.load()) {
                int rc = sqlite3_exec(db, "BEGIN", nullptr, nullptr, nullptr);
                if (rc != SQLITE_OK) failed(db, rc);
                for (int i = 0; i < 4096; i++, n++) {
                    std::memcpy(cid.data(), &n, sizeof(n));
                    rec[0] = uint8_t(n);
                    sqlite3_bind_int64(ins, 1, int64_t(n + 1));
                    sqlite3_bind_blob(ins, 2, cid.data(), int(cid.size()), SQLITE_STATIC);
                    sqlite3_bind_int64(ins, 3, int64_t(1789000000 + (n * 7919) % 2592000));
                    sqlite3_bind_int64(ins, 4, int64_t(n % 30000));
                    sqlite3_bind_int64(ins, 5, int64_t(1790000000 + n));
                    sqlite3_bind_blob(ins, 6, rec.data(), int(rec.size()), SQLITE_STATIC);
                    rc = sqlite3_step(ins);
                    if (rc != SQLITE_DONE) failed(db, rc);
                    sqlite3_reset(ins);
                }
                rc = sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr);
                if (rc != SQLITE_OK) failed(db, rc);
                rows = n;
            }
            sqlite3_finalize(ins);
            sqlite3_close(db);
        }
    };
    // A sort that grows its heap past the cap (no randomblob(): it serializes
    // on SQLite's global PRNG mutex, which no writer takes).
    const std::string hog =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 400000) "
        "SELECT count(*) FROM (SELECT x, printf('%01000d', x) b FROM c ORDER BY b DESC)";
    auto measure = [&](bool withHog, uint64_t* trips) {
        Writer w;
        w.path = path;
        std::thread t([&] { w.run(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(300));   // warm-up
        const uint64_t start = w.rows.load();
        const auto m0 = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - m0 < std::chrono::milliseconds(1000)) {
            if (withHog) {
                const Result r = h.sql(hog, {}, P4_SLOT_SANDBOX);
                if (r.status == P4_E_BUDGET) (*trips)++;
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
        const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - m0).count();
        const uint64_t end = w.rows.load();
        w.stop = true;
        t.join();
        CHECK_EQ(w.nomem.load(), uint64_t(0));
        if (w.nomem.load()) std::fprintf(stderr, "    writer: rc %d %s\n", w.firstRc, w.firstMsg.c_str());
        return double(end - start) / secs;
    };
    // Paired runs (alone, then beside the sandbox, back to back); the median
    // of the pairs' ratios (the box is shared and its load drifts).
    std::vector<double> alone, hogged, ratios;
    uint64_t trips = 0;
    for (int i = 0; i < 7; i++) {
        alone.push_back(measure(false, &trips));
        hogged.push_back(measure(true, &trips));
        ratios.push_back(hogged.back() / alone.back());
    }
    std::remove(path.c_str());
    std::remove((path + "-wal").c_str());
    std::remove((path + "-shm").c_str());
    std::sort(alone.begin(), alone.end());
    std::sort(hogged.begin(), hogged.end());
    std::sort(ratios.begin(), ratios.end());
    report("writer_rows_per_s_alone_median", alone[3], "rows/s");
    report("writer_rows_per_s_with_capped_sandbox_median", hogged[3], "rows/s");
    report("writer_rate_ratio_median_of_pairs", ratios[3], "x");
    report("sandbox_heap_cap_trips", double(trips), "statements");
    CHECK(trips > 0);
    CHECK(ratios[3] >= 0.9);
    sqlite3_hard_heap_limit64(prior);
}

#endif  // FLATSQL_P4SQL_FAKE
