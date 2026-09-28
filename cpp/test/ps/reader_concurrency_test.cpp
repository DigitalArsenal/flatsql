// Readers under saturating writers (T2 acceptance #1, A28, A29):
//   8 writers saturate 256 partitions while 16 lanes serve statements:
//   flatsql_partitions p99 <= 1 ms, index window LIMIT 1000 p99 <= 10 ms,
//   reader and writer lock sets disjoint (native instrumentation of every
//   SQLite mutex; A29 moves the acceptance itself under WasmEdge, T5/T6),
//   maximum lane lock wait <= 1 ms. A28: 8 clients reading at 10 KB/s:
//   counter p99 <= 10 ms, window p99 <= 50 ms.
// Linux-8 numbers; this test reports the machine it ran on (hardware
// threads, load average) with every figure.
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <mutex>
#include <random>
#include <thread>

#include "flatsql/ps/lane_arena.h"
#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {
double pct(std::vector<double> v, double q) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, size_t(q * double(v.size())))];
}

std::string ep(uint64_t i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-03-%02dT%02d:%02d:%02d.%03dZ", int(1 + (i / 86400) % 28), int((i / 3600) % 24),
                  int((i / 60) % 60), int(i % 60), int(i % 1000));
    return b;
}

struct Lat {
    std::mutex mu;
    std::vector<double> e2e, lane;
    void add(double a, double b) {
        std::lock_guard<std::mutex> g(mu);
        e2e.push_back(a);
        lane.push_back(b);
    }
};

void runConcurrency(uint64_t seconds, uint32_t parts, uint32_t lanes) {
    Store s(false, 8, true);
    s.cfg.poolBytes = 256ull << 20;
    // --dir=<path>: a real file system through the native seven-import host
    // (lock-free preads, real fsyncs); default: the in-memory test host.
    const std::string dir = argStr("dir", "");
    Io* io = s.fs.get();
    if (!dir.empty()) {
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        s.cfg.io = nullptr;
        s.cfg.root = dir;
        s.root = dir;
        io = importIo();
    }
    report("real_file_system", dir.empty() ? 0 : 1, "");
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids;
    for (uint32_t p = 0; p < parts; p++) pids.push_back(s.partition("cp" + std::to_string(p), ommType()));
    lockReportReset();
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> sent{0};
    // Writers: 8 producer threads, 32 partitions each, never waiting for acks
    // (credits pace them: saturation).
    std::vector<std::thread> producers;
    for (int t = 0; t < 8; t++)
        producers.emplace_back([&, t] {
            std::vector<std::unique_ptr<Producer>> ps;
            std::vector<uint32_t> mine;
            for (uint32_t p = uint32_t(t); p < parts; p += 8) {
                ps.emplace_back(new Producer(s.e.get(), pids[p]));
                mine.push_back(p);
            }
            std::vector<std::vector<uint8_t>> attrs;
            for (uint32_t p : mine)
                attrs.push_back(buildRecordAttr("cp" + std::to_string(p), "prov", "src" + std::to_string(p % 4), "b1"));
            uint64_t i = 0;
            while (!stop.load()) {
                for (size_t k = 0; k < ps.size() && !stop.load(); k++) {
                    const uint64_t id = (uint64_t(mine[k]) << 32) | i;
                    send(s.e.get(), *ps[k], ommRecord(uint32_t(id % 4000000000u), "C" + std::to_string(id), ep(i * 37 + k), 15.0),
                         attrs[k], 1780000000000ll + int64_t(i), true);
                    sent++;
                }
                i++;
            }
        });
    // Let the store fill before measuring.
    sleepNs(uint64_t(argInt("warmup_ms", 3000)) * 1000000ull);
    ReaderConfig rc;
    rc.root = s.root;
    rc.io = io;
    rc.cls = LaneClass::Interactive;
    rc.lanes = lanes;
    rc.cacheBytes = 64ull << 20;
    Reader inter(rc);
    REQUIRE(inter.inst);
    Lat counters, windows, sourceWindows;
    std::atomic<uint64_t> errors{0};
    std::vector<std::thread> clients;
    const uint64_t until = monoNs() + seconds * 1000000000ull;
    for (uint32_t c = 0; c < lanes; c++)
        clients.emplace_back([&, c] {
            std::mt19937_64 rng(c + 1);
            while (monoNs() < until) {
                const int kind = int(rng() % 3);
                const uint64_t t0 = monoNs();
                Rows r = kind == 0 ? inter.q("SELECT pid, live_count, live_bytes, max_epoch FROM flatsql_partitions")
                         : kind == 1 ? inter.q("SELECT _cid, _epoch, NORAD_CAT_ID FROM OMM LIMIT 1000")
                                     : inter.q("SELECT _cid, _epoch FROM OMM WHERE _source = ? ORDER BY _epoch DESC LIMIT 1000",
                                               {Param::text("OMM@src" + std::to_string(rng() % 4))});
                const double e2e = double(monoNs() - t0) / 1e6;
                if (r.status != 0) {
                    errors++;
                    continue;
                }
                Lat& l = kind == 0 ? counters : kind == 1 ? windows : sourceWindows;
                l.add(e2e, double(r.outcome.runNs) / 1e6);
            }
        });
    for (auto& t : clients) t.join();
    // A28: 8 clients reading slowly (10 KB/s: 4 KiB chunks, 400 ms apart).
    Lat slowCounters, slowWindows;
    {
        std::atomic<bool> slowStop{false};
        std::vector<std::thread> slow;
        for (int c = 0; c < 8; c++)
            slow.emplace_back([&, c] {
                while (!slowStop.load()) inter.q("SELECT _data FROM OMM LIMIT 1000", {}, 0, 400000000ull, 4096);
                (void)c;
            });
        const uint64_t slowUntil = monoNs() + uint64_t(argInt("slow_seconds", 10)) * 1000000000ull;
        std::mt19937_64 rng(99);
        while (monoNs() < slowUntil) {
            const bool counter = rng() % 2;
            const uint64_t t0 = monoNs();
            Rows r = counter ? inter.q("SELECT pid, live_count FROM flatsql_partitions")
                             : inter.q("SELECT _cid, _epoch FROM OMM LIMIT 1000");
            const double e2e = double(monoNs() - t0) / 1e6;
            if (r.status != 0) errors++;
            else (counter ? slowCounters : slowWindows).add(e2e, double(r.outcome.runNs) / 1e6);
        }
        slowStop = true;
        for (auto& t : slow) t.join();
    }
    stop = true;
    for (auto& t : producers) t.join();
    const LockReport lr = lockReport();
    // The intrinsic lane cost: one client, writers idle (their last L0
    // state, then after merges settle), so no CPU contention in the number.
    auto quietRun = [&](const char* tag) {
        std::vector<double> w, c;
        for (int i = 0; i < int(argInt("quiet_statements", 200)); i++) {
            Rows a = inter.q("SELECT _cid, _epoch, NORAD_CAT_ID FROM OMM LIMIT 1000");
            Rows b = inter.q("SELECT pid, live_count, live_bytes, max_epoch FROM flatsql_partitions");
            if (a.status || b.status) errors++;
            w.push_back(double(a.outcome.runNs) / 1e6);
            c.push_back(double(b.outcome.runNs) / 1e6);
        }
        char key[96];
        std::snprintf(key, sizeof(key), "quiet_%s_window_limit1000_p50_ms", tag);
        report(key, pct(w, 0.5), "ms");
        std::snprintf(key, sizeof(key), "quiet_%s_window_limit1000_p99_ms", tag);
        report(key, pct(w, 0.99), "ms");
        std::snprintf(key, sizeof(key), "quiet_%s_flatsql_partitions_p99_ms", tag);
        report(key, pct(c, 0.99), "ms");
    };
    uint64_t l0Total = 0;
    for (uint32_t pid : pids) l0Total += s.e->partition(pid)->nL0;
    report("unmerged_l0_blocks_per_partition", double(l0Total) / double(pids.size()), "blocks");
    quietRun("unmerged");
    sleepNs(uint64_t(argInt("settle_ms", 5000)) * 1000000ull);
    l0Total = 0;
    for (uint32_t pid : pids) l0Total += s.e->partition(pid)->nL0;
    report("settled_l0_blocks_per_partition", double(l0Total) / double(pids.size()), "blocks");
    quietRun("settled");
    const unsigned hw = std::thread::hardware_concurrency();
    double load[3] = {0, 0, 0};
    getloadavg(load, 3);
    report("hardware_threads", double(hw), "threads");
    report("load_average_1m", load[0], "");
    report("seconds", double(seconds), "s");
    report("records_sent", double(sent.load()), "records");
    report("records_per_second", double(sent.load()) / double(seconds + uint64_t(argInt("warmup_ms", 3000)) / 1000), "rec/s");
    report("statements_counters", double(counters.e2e.size()), "");
    report("statements_windows", double(windows.e2e.size()), "");
    report("flatsql_partitions_p99_ms", pct(counters.e2e, 0.99), "ms");
    report("flatsql_partitions_p99_lane_ms", pct(counters.lane, 0.99), "ms");
    report("window_limit1000_p50_ms", pct(windows.e2e, 0.5), "ms");
    report("window_limit1000_p99_ms", pct(windows.e2e, 0.99), "ms");
    report("window_limit1000_p99_lane_ms", pct(windows.lane, 0.99), "ms");
    report("source_window_limit1000_p99_ms", pct(sourceWindows.e2e, 0.99), "ms");
    report("a28_slow_clients_counter_p99_ms", pct(slowCounters.e2e, 0.99), "ms");
    report("a28_slow_clients_window_p99_ms", pct(slowWindows.e2e, 0.99), "ms");
    report("lane_max_lock_wait_ms", double(lr.laneMaxWaitNs) / 1e6, "ms");
    bool disjoint = true;
    for (const auto& e : lr.entries) {
        std::printf("  LOCK %-12s lane %llu writer %llu other %llu lane-contended %llu lane-max-wait %.3f ms\n",
                    e.name.c_str(), (unsigned long long)e.laneAcquires, (unsigned long long)e.writerAcquires,
                    (unsigned long long)e.otherAcquires, (unsigned long long)e.laneContended,
                    double(e.laneMaxWaitNs) / 1e6);
        if (e.laneAcquires && e.writerAcquires) disjoint = false;
    }
    CHECK(disjoint);
    CHECK_EQ(errors.load(), uint64_t(0));
    CHECK(double(lr.laneMaxWaitNs) / 1e6 <= 1.0);
    // Latency bounds are Linux-8 acceptance: enforced on a quiet 8+ thread
    // box, reported everywhere.
    const bool quiet = hw >= 8 && load[0] < double(hw) / 4;
    if (quiet && argInt("enforce", 1)) {
        CHECK(pct(counters.e2e, 0.99) <= 1.0);
        CHECK(pct(windows.e2e, 0.99) <= 10.0);
        CHECK(pct(slowCounters.e2e, 0.99) <= 10.0);
        CHECK(pct(slowWindows.e2e, 0.99) <= 50.0);
    } else {
        std::printf("  NOTE latency bounds not enforced: load %.1f on %u hardware threads\n", load[0], hw);
    }
    s.close();
    if (!dir.empty()) std::filesystem::remove_all(dir);
}
}  // namespace

PS_TEST(readers_under_saturating_writers_T2_1) {
    runConcurrency(uint64_t(argInt("seconds", 15)), uint32_t(argInt("partitions", 256)), uint32_t(argInt("lanes", 16)));
}
PS_SLOW_TEST(readers_under_saturating_writers_T2_1_full) {
    runConcurrency(uint64_t(argInt("seconds", 600)), uint32_t(argInt("partitions", 256)), uint32_t(argInt("lanes", 16)));
}
