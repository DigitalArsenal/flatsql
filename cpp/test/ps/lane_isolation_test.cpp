// Lane isolation (T2 acceptance #4, A30):
//   - a bulk statement past its 128 MiB arena fails with SQLITE_NOMEM
//     (kRsNoMem), interactive p99 stays within +/-10%, nothing is poisoned
//     (the same lane serves the next statement, its arena is consistent);
//   - an unbounded plan on an interactive lane returns FLATSQL_NEEDS_BULK in
//     <= 1 ms with 0 rows examined;
//   - A30: a maximum-depth SQL expression runs on a lane (explicit stacks,
//     canary intact), and one level deeper is an error, not a crash.
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <random>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {
std::string ep(int i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-06-%02dT%02d:%02d:%02d.%03dZ", 1 + (i / 86400) % 28, (i / 3600) % 24, (i / 60) % 60,
                  i % 60, i % 1000);
    return b;
}

void fill(Store& s, int parts, int perPart, std::vector<uint32_t>* pids) {
    s.registerTypes({&ommType()});
    for (int p = 0; p < parts; p++) pids->push_back(s.partition("prod" + std::to_string(p), ommType()));
    std::vector<std::unique_ptr<Producer>> prods;
    std::vector<uint64_t> last(size_t(parts), 0);
    for (int p = 0; p < parts; p++) prods.emplace_back(new Producer(s.e.get(), (*pids)[size_t(p)]));
    for (int i = 0; i < perPart; i++)
        for (int p = 0; p < parts; p++) {
            const int k = p * perPart + i;
            const auto attr = buildRecordAttr("prod" + std::to_string(p), "prov", "src" + std::to_string(p % 3), "b1");
            last[size_t(p)] = send(s.e.get(), *prods[size_t(p)], ommRecord(uint32_t(k + 1), "O" + std::to_string(k), ep(k * 7), 15.0),
                                   attr, 1780000000000ll + k);
        }
    for (int p = 0; p < parts; p++) CHECK_EQ(prods[size_t(p)]->waitAcked(last[size_t(p)], 60000000000ull), 0);
    CHECK(waitLabeledEngine(s.e.get(), *pids, 60000000000ull));
    CHECK(waitTypeVisible(s.fs.get(), s.root, ommType().fid, *pids, 60000000000ull));
}

double pct(std::vector<double> v, double q) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, size_t(q * double(v.size())))];
}

// One interactive statement: a LIMIT 100 window or a CID lookup.
double interactiveOnce(Reader& r, std::mt19937_64& rng, const std::vector<std::string>& cids) {
    const uint64_t t0 = monoNs();
    Rows q = (rng() % 2) ? r.q("SELECT _cid, NORAD_CAT_ID FROM OMM LIMIT 100")
                         : r.q("SELECT NORAD_CAT_ID FROM OMM WHERE _cid = ?", {Param::text(cids[rng() % cids.size()])});
    const double ms = double(monoNs() - t0) / 1e6;
    return q.status == 0 ? ms : -1;
}
}  // namespace

PS_TEST(lane_isolation_bulk_nomem_T2_4) {
    Store s(false, 2, true);
    REQUIRE(s.open() == 0);
    std::vector<uint32_t> pids;
    fill(s, 8, 1000, &pids);
    std::vector<std::string> cids;
    {
        Reader b(s, LaneClass::Bulk, 1);
        Rows all = b.q("SELECT _cid FROM OMM");
        REQUIRE(all.status == 0);
        for (auto& row : all.rows) cids.push_back(row[0].s);
    }
    Reader bulk(s, LaneClass::Bulk, 1);  // 128 MiB arena (default for the class)
    Reader inter(s, LaneClass::Interactive, 2);
    REQUIRE(bulk.inst && inter.inst);
    CHECK_EQ(bulk.inst->lane(0)->arena().capacity(), size_t(128) << 20);
    // A sort of ~300 MB of rows in memory: past the arena.
    const std::string hog =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3000000) "
        "SELECT x, randomblob(100) AS b FROM c ORDER BY b";
    std::mt19937_64 rng(3);
    const int rounds = int(argInt("rounds", 4));
    const int perRound = int(argInt("per_round", 250));
    std::vector<double> base, loaded;
    int nomem = 0, hogs = 0;
    // Alternate unloaded and loaded windows so background load on the box
    // affects both samples alike.
    for (int r = 0; r < rounds; r++) {
        for (int i = 0; i < perRound; i++) {
            const double ms = interactiveOnce(inter, rng, cids);
            CHECK(ms >= 0);
            base.push_back(ms);
        }
        std::atomic<bool> done{false};
        const bool withHog = argInt("hog", 1) != 0;  // 0: control run (noise floor)
        std::thread t([&] {
            if (!withHog) {
                sleepNs(300000000);
                done = true;
                return;
            }
            Rows h = bulk.q(hog);
            if (h.status == int32_t(kRsNoMem)) nomem++;
            hogs++;
            done = true;
        });
        // Measure while the hog is filling its arena.
        int n = 0;
        while (!done.load() || n < perRound / 4) {
            const double ms = interactiveOnce(inter, rng, cids);
            CHECK(ms >= 0);
            loaded.push_back(ms);
            if (++n >= perRound && done.load()) break;
        }
        t.join();
        if (argInt("verbose", 0)) {
            std::vector<double> lb(base.end() - perRound, base.end());
            std::printf("  round %d: base p99 %.3f loaded p99 %.3f (n=%d)\n", r, pct(lb, 0.99), pct(std::vector<double>(loaded.end() - n, loaded.end()), 0.99), n);
        }
    }
    CHECK_EQ(nomem, hogs);
    const double p99b = pct(base, 0.99), p99l = pct(loaded, 0.99);
    report("interactive_p99_baseline_ms", p99b, "ms");
    report("interactive_p99_during_bulk_nomem_ms", p99l, "ms");
    report("interactive_p99_ratio", p99l / p99b, "x");
    const unsigned hw = std::thread::hardware_concurrency();
    double load[3] = {0, 0, 0};
    getloadavg(load, 3);
    report("hardware_threads", double(hw), "threads");
    report("load_average_1m", load[0], "");
    // Linux-8 acceptance: +/-10%, a property of a dedicated box. The hog is
    // one busy core streaming ~300 MB through its caches; on a box whose
    // cores are already busy with other work it steals time from the lanes
    // (no lock is shared: see the lock report below). Enforced on a quiet box
    // with 8+ hardware threads, reported everywhere (PARTITION-STORE.md §9).
    const bool quiet = hw >= 8 && load[0] < double(hw) / 4;
    if (quiet && argInt("enforce_ratio", 1)) CHECK(p99l <= p99b * 1.10);
    if (!quiet) std::printf("  NOTE ratio not enforced: load %.1f on %u hardware threads (needs a quiet Linux-8 box)\n", load[0], hw);
    // Nothing poisoned: the same bulk lane runs the next statement, and its
    // arena is consistent and back near its idle footprint.
    Rows after = bulk.q("SELECT count(*) FROM OMM");
    CHECK_EQ(after.status, 0);
    REQUIRE(after.rows.size() == 1);
    CHECK_EQ(after.i(0, 0), int64_t(8000));
    std::string why;
    CHECK(bulk.inst->lane(0)->arena().check(&why));
    report("bulk_arena_used_after_nomem_kib", double(bulk.inst->lane(0)->arena().used()) / 1024.0, "KiB");
    CHECK(bulk.inst->lane(0)->arena().used() < (size_t(16) << 20));
    CHECK(bulk.inst->lane(0)->arena().failures() > 0);
    CHECK_EQ(inter.inst->stats().noMem, uint64_t(0));
    s.close();
}

PS_TEST(lane_needs_bulk_under_1ms_zero_rows_T2_4) {
    Store s(false, 2, true);
    REQUIRE(s.open() == 0);
    std::vector<uint32_t> pids;
    fill(s, 4, 500, &pids);
    Reader inter(s, LaneClass::Interactive, 2);
    REQUIRE(inter.inst);
    const char* unbounded[] = {
        "SELECT count(*) FROM OMM",
        "SELECT * FROM OMM WHERE MEAN_MOTION > 1",
        "SELECT a._cid FROM OMM a, OMM b WHERE a.NORAD_CAT_ID = b.NORAD_CAT_ID",
        "SELECT * FROM sds_p_prod1__OMM ORDER BY OBJECT_ID",
        "SELECT sum(_len) FROM sds_p_prod0__OMM",
    };
    std::vector<double> laneMs;
    for (int k = 0; k < 200; k++) {
        const char* sql = unbounded[k % 5];
        Rows r = inter.q(sql);
        CHECK_EQ(r.status, int32_t(kRsNeedsBulk));
        CHECK_EQ(r.outcome.rowsExamined, uint64_t(0));
        CHECK_EQ(r.rows.size(), size_t(0));
        laneMs.push_back(double(r.outcome.runNs) / 1e6);
    }
    report("needs_bulk_lane_ms_p50", pct(laneMs, 0.5), "ms");
    report("needs_bulk_lane_ms_p99", pct(laneMs, 0.99), "ms");
    report("needs_bulk_lane_ms_max", pct(laneMs, 1.0), "ms");
    CHECK(pct(laneMs, 0.99) <= 1.0);
    // The same statements are admitted on a bulk lane.
    Reader bulk(s, LaneClass::Bulk, 1);
    Rows c = bulk.q("SELECT count(*) FROM OMM");
    CHECK_EQ(c.status, 0);
    CHECK_EQ(c.i(0, 0), int64_t(2000));
    s.close();
}

PS_TEST(lane_max_depth_expression_A30) {
    Store s(false, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    Reader r(s, LaneClass::Interactive, 1);
    REQUIRE(r.inst);
    // SQLITE_MAX_EXPR_DEPTH is 1000: the deepest expression tree SQLite
    // accepts. A left-associative chain 1+1+...+1 of d terms is a tree of
    // depth d (code generation recurses once per level) that the LALR
    // parser takes without growing its own stack.
    auto nested = [](int depth) {
        std::string e = "1";
        for (int i = 1; i < depth; i++) e += "+1";
        return "SELECT " + e;
    };
    int deepest = 0;
    for (int d = 990; d <= 1000; d++) {
        Rows q = r.q(nested(d));
        if (q.status == 0 && q.rows.size() == 1 && q.i(0, 0) == d) deepest = d;
        else if (d == 990) std::fprintf(stderr, "  depth %d: status %d %s\n", d, q.status, q.error.c_str());
    }
    report("max_expression_depth_run", double(deepest), "levels");
    CHECK(deepest >= 995);
    Rows over = r.q(nested(1001));
    CHECK_EQ(over.status, int32_t(kRsSqlError));
    CHECK(over.error.find("too large") != std::string::npos);
    // The lane is alive, its stack canary intact.
    Rows ok = r.q("SELECT 42");
    CHECK_EQ(ok.status, 0);
    CHECK_EQ(r.inst->laneShared(0).canary.load(), uint32_t(0));
    s.close();
}
