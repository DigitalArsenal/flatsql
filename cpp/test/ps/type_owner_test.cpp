// Type owner tests: FIRST/REPEAT labels and store-global gseqs (A6),
// promotion keeps the gseq (A14), type-level deletes (A14), notices that
// never block writers (A25), label checkpoints past 128 pids (A10).
#include <algorithm>
#include <atomic>
#include <random>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/ps_test.h"

using namespace pst;

namespace {
std::string ep(int i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-08-%02dT00:00:%02dZ", 1 + i % 28, i % 60);
    return b;
}
bool waitLabeled(Engine* e, const std::vector<uint32_t>& pids, uint64_t timeoutNs) {
    const uint64_t deadline = monoNs() + timeoutNs;
    while (monoNs() < deadline) {
        bool all = true;
        for (uint32_t pid : pids) {
            Partition* p = e->partition(pid);
            if (p->labeledThrough.load() < p->durablePseqHi.load()) all = false;
        }
        if (all) return true;
        sleepNs(1000000);
    }
    return false;
}
}  // namespace

PS_TEST(type_first_repeat_labels_and_gseq) {
    Store s(true, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids = {s.partition("P1", ommType()), s.partition("P2", ommType()),
                                  s.partition("P3", ommType())};
    std::vector<std::vector<uint8_t>> recs;
    for (int i = 0; i < 200; i++) recs.push_back(ommRecord(uint32_t(30000 + i), "C", ep(i), 3.0 + i));
    const int ranges[3][2] = {{0, 100}, {50, 150}, {100, 200}};
    for (int k = 0; k < 3; k++) {
        Producer prod(s.e.get(), pids[k]);
        const auto attr = buildRecordAttr("P" + std::to_string(k + 1), "prov", "src", "b");
        uint64_t last = 0;
        for (int i = ranges[k][0]; i < ranges[k][1]; i++) last = send(s.e.get(), prod, recs[i], attr, i);
        CHECK_EQ(prod.waitAcked(last, 10000000000ull), 0);
        CHECK(waitLabeled(s.e.get(), pids, 5000000000ull));
    }
    CHECK_EQ(s.e->stats().firstLabels, uint64_t(200));
    CHECK_EQ(s.e->stats().repeatLabels, uint64_t(100));
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    const auto tv = ins.type(ommType().fid);
    REQUIRE(tv.ok);
    CHECK_EQ(tv.arrivals.size(), size_t(200));
    CHECK_EQ(tv.head.firstLiveCount, uint64_t(200));
    uint64_t prev = 0;
    int firstInP1 = 0;
    for (const auto& a : tv.arrivals) {
        CHECK(a.gseq > prev);
        prev = a.gseq;
        CHECK(a.flags & kArrivalFirst);
        if (a.pid == pids[0]) firstInP1++;
    }
    CHECK_EQ(firstInP1, 100);
    CHECK_EQ(tv.head.gseqHi, prev);
    for (uint32_t pid : pids) CHECK_EQ(tv.labeled.at(pid), uint64_t(100));
    s.close();
}

PS_TEST(type_promotion_keeps_gseq_A14_and_type_delete) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t p1 = s.partition("P1", ommType()), p2 = s.partition("P2", ommType()),
                   p3 = s.partition("P3", ommType());
    const auto x = ommRecord(1, "X", ep(1), 1.0);
    const auto y = ommRecord(2, "Y", ep(2), 2.0);
    uint8_t cidX[kCidLen], cidY[kCidLen];
    frameCid(x, cidX);
    frameCid(y, cidY);
    const auto attr = buildRecordAttr("p", "prov", "src", "b");
    for (uint32_t pid : {p1, p2, p3}) {
        Producer prod(s.e.get(), pid);
        send(s.e.get(), prod, x, attr, 1);
        const uint64_t r = send(s.e.get(), prod, y, attr, 1);
        CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
        CHECK(waitLabeled(s.e.get(), {p1, p2, p3}, 5000000000ull));
    }
    TypeOwner* t = s.e->type(ommType().fid);
    const uint64_t arrivals0 = t->publishedArrivals.load();
    CHECK_EQ(arrivals0, uint64_t(2));
    // Kill X's FIRST copy (P1) at partition level: P2's REPEAT is promoted.
    {
        Producer prod(s.e.get(), p1);
        uint64_t r = 0;
        prod.enqueue(kEntTombCid, 0, 2, cidX, nullptr, 0, nullptr, 0, &r);
        CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
        CHECK(waitLabeled(s.e.get(), {p1, p2, p3}, 5000000000ull));
    }
    CHECK_EQ(s.e->stats().promotions, uint64_t(1));
    CHECK_EQ(t->publishedArrivals.load(), arrivals0);  // no new arrivals entry
    CHECK_EQ(t->firstLiveCount, uint64_t(2));           // the gseq lives on
    // Type-level delete of Y: every copy dies.
    std::atomic<int32_t> remaining{1};
    CHECK_EQ(s.e->deleteCid(ommType().fid, cidY, &remaining), 0);
    const uint64_t deadline = monoNs() + 10000000000ull;
    while (remaining.load() != 0 && monoNs() < deadline) sleepNs(1000000);
    CHECK_EQ(remaining.load(), 0);
    CHECK(waitLabeled(s.e.get(), {p1, p2, p3}, 5000000000ull));
    CHECK_EQ(t->firstLiveCount, uint64_t(1));
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    for (uint32_t pid : {p1, p2, p3}) {
        PartView v = ins.partition(pid);
        REQUIRE(v.err.empty());
        int yTombs = 0;
        for (const auto& row : v.rows)
            if (row.kind == kRowTomb && std::memcmp(row.cid, cidY, kCidLen) == 0) yTombs++;
        CHECK_EQ(yTombs, 1);
    }
    const auto tv = ins.type(ommType().fid);
    CHECK_EQ(tv.arrivals.size(), size_t(2));
    CHECK_EQ(tv.head.firstLiveCount, uint64_t(1));
    s.close();
}

static void noticeRun(double seconds) {
    Store s(true, 2, true);
    s.cfg.noticeQueue = 1;  // A25: a full queue drops the notice
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &mpeType()});
    // Crossed ownership: each type owner lives on the other writer.
    TypeOwner* to = s.e->type(ommType().fid);
    TypeOwner* tm = s.e->type(mpeType().fid);
    std::vector<uint32_t> pids;
    for (int i = 0; i < 16; i++) pids.push_back(s.partition("n" + std::to_string(i), (i % 2) ? mpeType() : ommType()));
    for (uint32_t pid : pids) {
        Partition* p = s.e->partition(pid);
        const uint32_t typeWriter = p->type == to ? to->ownerWriter.load() : tm->ownerWriter.load();
        if (p->ownerWriter.load() == typeWriter) s.e->rebalance(pid, uint8_t(1 - typeWriter));
    }
    CHECK(to->ownerWriter.load() != tm->ownerWriter.load());
    sleepNs(100000000);
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> maxAckNs{0};
    std::vector<std::thread> prods;
    for (int t = 0; t < 4; t++) {
        prods.emplace_back([&, t] {
            std::mt19937 rng{uint32_t(t)};
            const auto attr = buildRecordAttr("x", "p", "s", "b");
            std::vector<Producer> ps;
            std::vector<uint32_t> idx;
            for (int i = t; i < 16; i += 4) {
                ps.emplace_back(s.e.get(), pids[i]);
                idx.push_back(uint32_t(i));
            }
            for (uint64_t n = 0; !stop.load(); n++) {
                const size_t k = rng() % ps.size();
                const int burst = 1 + int(rng() % 32);
                uint64_t last = 0;
                const uint64_t t0 = monoNs();
                for (int b = 0; b < burst; b++) {
                    const auto f = (idx[k] % 2) ? mpeRecord("E" + std::to_string(n * 64 + b), 1.6e9 + n, double(idx[k]))
                                                : ommRecord(uint32_t(n * 64 + b), "O", ep(int(n)), double(idx[k]) + n * 1e-3);
                    last = send(s.e.get(), ps[k], f, attr, int64_t(n));
                }
                ps[k].waitAcked(last, 30000000000ull);
                const uint64_t dt = monoNs() - t0;
                uint64_t m = maxAckNs.load();
                while (dt > m && !maxAckNs.compare_exchange_weak(m, dt)) {}
                if (rng() % 4 == 0) sleepNs(rng() % 2000000);
            }
        });
    }
    // Labeling lag: time from a partition's durable HWM to labeled_through.
    std::vector<double> lags;
    const uint64_t end = monoNs() + uint64_t(seconds * 1e9);
    while (monoNs() < end) {
        const uint32_t pid = pids[lags.size() % pids.size()];
        Partition* p = s.e->partition(pid);
        const uint64_t hi = p->durablePseqHi.load();
        const uint64_t t0 = monoNs();
        while (p->labeledThrough.load() < hi && monoNs() - t0 < 5000000000ull) cpuRelax();
        lags.push_back(double(monoNs() - t0) / 1e6);
        sleepNs(500000);
    }
    stop.store(true);
    for (auto& t : prods) t.join();
    CHECK(waitLabeled(s.e.get(), pids, 1000000000ull));
    std::sort(lags.begin(), lags.end());
    const double p99 = lags[size_t(lags.size() * 0.99)];
    report("a25_label_lag_p99", p99, "ms");
    report("a25_label_lag_max", lags.back(), "ms");
    report("a25_notices_dropped", double(s.e->stats().noticesDropped), "notices");
    report("a25_max_ack_wait", double(maxAckNs.load()) / 1e6, "ms");
    CHECK(s.e->stats().noticesDropped > 0);
    CHECK(p99 <= 2 * 5.0 + 1.0);                      // within 2 commit windows (5 ms each)
    CHECK(double(maxAckNs.load()) / 1e9 < 1.0);        // 0 stalls
    s.close();
}

PS_TEST(type_notices_never_block_A25) { noticeRun(double(argInt("a25-seconds", 5))); }
PS_SLOW_TEST(type_notices_never_block_A25_full) { noticeRun(600.0); }

PS_TEST(type_label_checkpoint_past_128_pids_A10) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids;
    for (int i = 0; i < 200; i++) pids.push_back(s.partition("many" + std::to_string(i), ommType()));
    const auto attr = buildRecordAttr("x", "p", "s", "b");
    for (int i = 0; i < 200; i++) {
        Producer prod(s.e.get(), pids[i]);
        const uint64_t r = send(s.e.get(), prod, ommRecord(uint32_t(40000 + i), "M", ep(i), 1.0), attr, i);
        CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
    }
    CHECK(waitLabeled(s.e.get(), pids, 10000000000ull));
    s.close();
    REQUIRE(s.open() == 0);
    // Every pid's labeled_through survived the reopen: no relabeling.
    for (uint32_t pid : pids) CHECK_EQ(s.e->partition(pid)->labeledThrough.load(), uint64_t(1));
    sleepNs(200000000);
    CHECK_EQ(s.e->stats().firstLabels, uint64_t(0));
    Inspector ins(s.fs.get(), s.root);
    const auto tv = ins.type(ommType().fid);
    CHECK_EQ(tv.arrivals.size(), size_t(200));
    CHECK_EQ(tv.head.nLabels, uint16_t(0xffff));
    s.close();
}
