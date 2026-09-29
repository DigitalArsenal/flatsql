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
    Store s(false, 2, true);  // no crash here: one in-memory copy per file
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
                // Random load below the owners' labeling throughput: A25 is
                // about dropped notices never stalling anyone, not saturation.
                sleepNs(rng() % 2000000);
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

PS_TEST(type_arrivals_segments_and_fence_A15) {
    // Arrivals seal every 40 entries; the fence index names each sealed
    // segment's gseq range; reopen resumes the active segment. A type batch
    // never splits across segments (22.3a-3), so a batch into an empty
    // segment may fill it past the seal size: type commits are capped at 40
    // rows here, which bounds every sealed segment by 40 on any machine (a
    // slow type owner once labeled more than 40 rows in one commit).
    Store s(true, 2, true);
    s.cfg.arrivalsSegBytes = 40 * kArrivalBytes;
    s.cfg.typeCommitRows = 40;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const std::vector<uint32_t> pids = {s.partition("A", ommType()), s.partition("B", ommType())};
    uint32_t norad = 50000;
    auto ingest = [&](int n) {
        for (int k = 0; k < 2; k++) {
            Producer prod(s.e.get(), pids[size_t(k)]);
            const auto attr = buildRecordAttr("P", "prov", "src", "b");
            uint64_t last = 0;
            for (int i = 0; i < n; i++) {
                last = send(s.e.get(), prod, ommRecord(norad, "S", ep(int(norad % 1000)), 1.0 + norad), attr, i);
                norad++;
                if (i % 16 == 15) CHECK_EQ(prod.waitAcked(last, 10000000000ull), 0);  // many small commits
            }
            CHECK_EQ(prod.waitAcked(last, 10000000000ull), 0);
        }
        CHECK(waitLabeled(s.e.get(), pids, 5000000000ull));
    };
    auto verify = [&](size_t expect) -> uint32_t {
        Inspector ins(s.fs.get(), s.root);
        const auto tv = ins.type(ommType().fid);
        CHECK(tv.ok);
        if (!tv.ok) return 0;
        if (!tv.fenceErr.empty()) std::fprintf(stderr, "  fence: %s\n", tv.fenceErr.c_str());
        CHECK(tv.fenceErr.empty());
        CHECK_EQ(tv.arrivals.size(), expect);
        CHECK_EQ(tv.fence.size(), size_t(tv.head.gSeg));
        uint64_t total = 0;
        for (const auto& f : tv.fence) {
            CHECK(f.count > 0 && f.count <= 40);
            total += f.count;
        }
        CHECK_EQ(total + tv.head.gLen / kArrivalBytes, uint64_t(expect));
        return tv.head.gSeg;
    };
    ingest(150);
    s.close();
    REQUIRE(s.open() == 0);
    const uint32_t segs = verify(300);
    CHECK(segs >= 7);
    report("a15_sealed_segments", double(segs), "segments");
    ingest(100);
    s.close();
    REQUIRE(s.open() == 0);
    CHECK(verify(500) > segs);
    s.close();
}

// ---- store-migrate gseqs (design §16.1-5; PARTITION-STORE.md §31) -----------
namespace {
std::vector<uint8_t> migAttr(const std::string& peer, uint64_t gseq) {
    return buildRecordAttr(peer, "prov", "src", "b", "", "", "", "", 0, "", gseq);
}
struct ArrivalAt {
    uint64_t gseq;
    uint32_t pid;
    uint64_t pseq;
};
void checkArrivals(const Inspector::TypeView& tv, const std::vector<ArrivalAt>& want) {
    CHECK_EQ(tv.arrivals.size(), want.size());
    for (size_t i = 0; i < want.size() && i < tv.arrivals.size(); i++) {
        CHECK_EQ(tv.arrivals[i].gseq, want[i].gseq);
        CHECK_EQ(tv.arrivals[i].pid, want[i].pid);
        CHECK_EQ(tv.arrivals[i].pseq, want[i].pseq);
        CHECK(tv.arrivals[i].flags & kArrivalFirst);
    }
}
}  // namespace

PS_TEST(type_migrated_gseq_preserved_fallback_and_stats) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pa = s.partition("MA", ommType()), pb = s.partition("MB", ommType());
    std::vector<std::vector<uint8_t>> recs;
    for (int i = 0; i < 12; i++) recs.push_back(ommRecord(uint32_t(40000 + i), "M", ep(i), 5.0 + i));
    // A: five FIRST copies with legacy rowids 100..500, fed in rowid order.
    {
        Producer prod(s.e.get(), pa);
        uint64_t last = 0;
        for (int i = 0; i < 5; i++) last = send(s.e.get(), prod, recs[size_t(i)], migAttr("MA", uint64_t(100 * (i + 1))), i);
        CHECK_EQ(prod.waitAcked(last, 10000000000ull), 0);
        CHECK(waitLabeled(s.e.get(), {pa, pb}, 5000000000ull));
    }
    TypeOwner* t = s.e->type(ommType().fid);
    CHECK_EQ(s.e->stats().migratedGseqs, uint64_t(5));
    CHECK_EQ(s.e->stats().migratedGseqFallbacks, uint64_t(0));
    CHECK_EQ(t->publishedGseqHi.load(), uint64_t(500));
    CHECK_EQ(s.e->gseqNext(), uint64_t(501));
    // B: a rowid at or below the type's gseq_hi falls back to a new gseq (and
    // is counted); a higher one is kept; a copy of A's first record is
    // labeled REPEAT and its rowid is not used (counted).
    {
        Producer prod(s.e.get(), pb);
        send(s.e.get(), prod, recs[5], migAttr("MB", 250), 5);
        send(s.e.get(), prod, recs[6], migAttr("MB", 600), 6);
        const uint64_t last = send(s.e.get(), prod, recs[0], migAttr("MB", 100), 7);
        CHECK_EQ(prod.waitAcked(last, 10000000000ull), 0);
        CHECK(waitLabeled(s.e.get(), {pa, pb}, 5000000000ull));
    }
    CHECK_EQ(s.e->stats().migratedGseqs, uint64_t(6));
    CHECK_EQ(s.e->stats().migratedGseqFallbacks, uint64_t(2));
    CHECK_EQ(s.e->stats().repeatLabels, uint64_t(1));
    // An ordinary record continues above everything.
    {
        Producer prod(s.e.get(), pa);
        const uint64_t last = send(s.e.get(), prod, recs[7], buildRecordAttr("MA", "prov", "src", "b"), 8);
        CHECK_EQ(prod.waitAcked(last, 10000000000ull), 0);
        CHECK(waitLabeled(s.e.get(), {pa, pb}, 5000000000ull));
    }
    s.close();
    REQUIRE(s.open() == 0);
    CHECK_EQ(s.e->gseqNext(), uint64_t(602));
    Inspector ins(s.fs.get(), s.root);
    const auto tv = ins.type(ommType().fid);
    REQUIRE(tv.ok);
    checkArrivals(tv, {{100, pa, 1}, {200, pa, 2}, {300, pa, 3}, {400, pa, 4}, {500, pa, 5},
                       {501, pb, 1}, {600, pb, 2}, {601, pa, 6}});
    CHECK_EQ(tv.head.gseqHi, uint64_t(601));
    CHECK_EQ(tv.head.firstLiveCount, uint64_t(8));
    // The flag rides only the PUT rows whose attribute carries a gseq.
    const PartView va = ins.partition(pa), vb = ins.partition(pb);
    REQUIRE(va.err.empty() && vb.err.empty());
    REQUIRE(va.rows.size() == 6 && vb.rows.size() == 3);
    for (size_t i = 0; i < 6; i++) CHECK_EQ(bool(va.rows[i].flags & kRowMigratedGseq), i < 5);
    for (size_t i = 0; i < 3; i++) CHECK(vb.rows[i].flags & kRowMigratedGseq);
    s.close();
}

// One type commit labels two partitions whose migrated gseqs interleave: its
// arrivals are appended in gseq order. Crashing after the partition commits
// (labels lost) or after the type commit (labels durable) gives the same
// arrivals after reopen, and allocation continues above them.
PS_TEST(type_migrated_gseq_one_commit_sorted_and_crash_reopen) {
    for (int mode = 0; mode < 2; mode++) {
        Store s(true, 1, false);  // cooperative: commits at explicit pumps
        REQUIRE(s.open() == 0);
        s.registerTypes({&ommType()});
        const uint32_t pa = s.partition("MA", ommType()), pb = s.partition("MB", ommType());
        uint64_t ra = 0, rb = 0;
        {
            Producer a(s.e.get(), pa), b(s.e.get(), pb);
            // Ring pages first: an idle ring holds none until a producer asks
            // and a pump maps them (§17), so all four entries land before
            // any commit.
            const auto r0 = ommRecord(41000, "S", ep(1), 1.0), r2 = ommRecord(41002, "S", ep(3), 3.0);
            uint64_t r0seq = send(s.e.get(), a, r0, migAttr("MA", 900), 1, false);
            uint64_t r2seq = send(s.e.get(), b, r2, migAttr("MB", 700), 3, false);
            s.e->pump(0);
            if (!r0seq) r0seq = send(s.e.get(), a, r0, migAttr("MA", 900), 1, false);
            if (!r2seq) r2seq = send(s.e.get(), b, r2, migAttr("MB", 700), 3, false);
            REQUIRE(r0seq && r2seq);
            REQUIRE((ra = send(s.e.get(), a, ommRecord(41001, "S", ep(2), 2.0), migAttr("MA", 950), 2, false)));
            REQUIRE((rb = send(s.e.get(), b, ommRecord(41003, "S", ep(4), 4.0), migAttr("MB", 920), 4, false)));
            s.e->pump(0);  // both partition batches commit (durable) in one round
            CHECK(a.acked(ra) && b.acked(rb));
        }
        TypeOwner* t = s.e->type(ommType().fid);
        CHECK_EQ(t->publishedArrivals.load(), uint64_t(0));
        if (mode == 1) {
            s.e->pump(0);  // one type commit labels both partitions
            CHECK_EQ(t->publishedArrivals.load(), uint64_t(4));
            CHECK_EQ(s.e->stats().migratedGseqs, uint64_t(4));
        }
        s.crash(FaultFs::kDropAll, uint64_t(mode + 1));
        REQUIRE(s.open() == 0);
        for (int i = 0; i < 4; i++) s.e->pump(0);
        t = s.e->type(ommType().fid);
        CHECK_EQ(t->publishedArrivals.load(), uint64_t(4));
        CHECK_EQ(s.e->stats().migratedGseqs, uint64_t(mode == 0 ? 4 : 0));
        CHECK_EQ(s.e->stats().migratedGseqFallbacks, uint64_t(0));
        CHECK_EQ(s.e->gseqNext(), uint64_t(951));
        {
            Producer a(s.e.get(), pa);
            const auto r4 = ommRecord(41004, "S", ep(5), 5.0);
            const auto attr = buildRecordAttr("MA", "prov", "src", "b");
            uint64_t r = send(s.e.get(), a, r4, attr, 5, false);
            if (!r) {
                s.e->pump(0);
                r = send(s.e.get(), a, r4, attr, 5, false);
            }
            REQUIRE(r);
            for (int i = 0; i < 4; i++) s.e->pump(0);
            CHECK(a.acked(r));
        }
        s.close();
        Inspector ins(s.fs.get(), s.root);
        const auto tv = ins.type(ommType().fid);
        REQUIRE(tv.ok);
        checkArrivals(tv, {{700, pb, 1}, {900, pa, 1}, {920, pb, 2}, {950, pa, 2}, {951, pa, 3}});
        CHECK_EQ(tv.head.gseqHi, uint64_t(951));
    }
}
