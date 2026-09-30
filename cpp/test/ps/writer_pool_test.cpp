// Writer pool tests: durable ingest, dedupe with TAG rows (A2), RECONCILE,
// supersede, backpressure (T1 #5), single writer under rebalancing (T1 #4,
// A26), memory (T1 #9), hot-path allocations (T1 #8), control transactions,
// fsync EIO quarantine and ENOSPC recovery.
#include <algorithm>
#include <cstdio>
#include <atomic>
#include <random>
#include <map>
#include <set>
#include <filesystem>
#include <thread>

#include "flatsql/ps/flatsql_ps.h"
#include "flatsql/ps/platform.h"
#include "ps/ps_test.h"

extern std::atomic<uint64_t> gHotAllocs;

using namespace pst;

namespace {

std::vector<uint8_t> reconcilePayload(const std::string& provider, const std::string& source,
                                      const std::string& keep) {
    std::vector<uint8_t> out;
    for (const std::string* s : {&provider, &source, &keep}) {
        const size_t at = out.size();
        out.resize(at + 2 + s->size());
        putU16(out.data() + at, uint16_t(s->size()));
        std::memcpy(out.data() + at + 2, s->data(), s->size());
    }
    return out;
}

uint64_t reconcile(Producer& prod, const std::string& provider, const std::string& source,
                   const std::string& keep) {
    const auto payload = reconcilePayload(provider, source, keep);
    uint64_t rseq = 0;
    prod.enqueue(kEntReconcile, 0, 0, nullptr, nullptr, 0, payload.data(), uint32_t(payload.size()), &rseq);
    return rseq;
}

std::string epochAt(int i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-09-%02dT%02d:%02d:%02d.%03dZ", 1 + (i / 86400) % 28, (i / 3600) % 24,
                  (i / 60) % 60, i % 60, i % 1000);
    return b;
}

struct KindCount {
    int puts = 0, tombs = 0, retags = 0, tagTombs = 0, ctl = 0, licences = 0;
};
KindCount kinds(const PartView& v) {
    KindCount k;
    for (const auto& r : v.rows) {
        if (r.kind == kRowPut) k.puts++;
        if (r.kind == kRowTomb) k.tombs++;
        if (r.kind == kRowRetag) k.retags++;
        if (r.kind == kRowTagTomb) k.tagTombs++;
        if (r.kind == kRowCtl) k.ctl++;
        if (r.kind == kRowLicence) k.licences++;
    }
    return k;
}

void checkCounters(const PartView& v) {
    const Recount rc = recount(v);
    CHECK_EQ(v.head.counters.totalCount, rc.total);
    CHECK_EQ(v.head.counters.totalBytes, rc.totalBytes);
    CHECK_EQ(v.head.counters.liveCount, rc.live);
    CHECK_EQ(v.head.counters.liveBytes, rc.liveBytes);
    CHECK_EQ(v.head.counters.tombCount, rc.tombs);
    if (rc.total) {
        CHECK_EQ(v.head.counters.minEpoch, rc.minEpoch);
        CHECK_EQ(v.head.counters.maxEpoch, rc.maxEpoch);
    }
    std::map<uint32_t, std::pair<int64_t, int64_t>> head;
    for (const auto& l : v.lanes)
        if (l.count) head[l.laneId] = {l.count, l.bytes};
    std::map<uint32_t, std::pair<int64_t, int64_t>> rec;
    for (const auto& kv : rc.lanes)
        if (kv.second.first) rec[kv.first] = kv.second;
    CHECK(head == rec);
}

}  // namespace

PS_TEST(writer_ingest_ack_reopen_identical_bytes) {
    Store s(true, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &mpeType(), &catType(), &iqcType()});
    const uint32_t pid = s.partition("12D3KooWalpha", ommType());
    const uint32_t pid2 = s.partition("12D3KooWalpha", mpeType());
    REQUIRE(pid && pid2 && pid != pid2);
    CHECK_EQ(s.partition(" 12D3KooWalpha ", ommType()), pid);  // same token (A3)
    Producer prod(s.e.get(), pid), prod2(s.e.get(), pid2);
    const auto attr = buildRecordAttr("12D3KooWalpha", "celestrak", "gp", "b1");
    std::vector<std::vector<uint8_t>> frames;
    uint64_t last = 0, last2 = 0;
    for (int i = 0; i < 500; i++) {
        frames.push_back(ommRecord(10000 + i, "OBJ-" + std::to_string(i), epochAt(i), 15.0 + i * 1e-3));
        last = send(s.e.get(), prod, frames.back(), attr, 1000 + i);
        REQUIRE(last);
        last2 = send(s.e.get(), prod2, mpeRecord("E" + std::to_string(i), 1.7e9 + i, i), attr, 1000 + i);
    }
    CHECK_EQ(prod.waitAcked(last, 10000000000ull), 0);
    CHECK_EQ(prod2.waitAcked(last2, 10000000000ull), 0);
    s.close();
    REQUIRE(s.open() == 0);
    CHECK_EQ(s.e->stats().openDataBytes, uint64_t(0));
    CHECK_EQ(s.e->stats().framesParsedAtOpen, uint64_t(0));
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(pid);
    REQUIRE(v.ok);
    if (!v.err.empty()) std::fprintf(stderr, "  inspector: %s\n", v.err.c_str());
    CHECK(v.err.empty());
    CHECK_EQ(v.rows.size(), size_t(500));
    int same = 0;
    for (size_t i = 0; i < v.rows.size() && i < frames.size(); i++)
        same += ins.frame(pid, v.rows[i]) == frames[i];
    CHECK_EQ(same, 500);
    checkCounters(v);
    s.close();
}

PS_TEST(writer_dedupe_retag_reconcile_A2) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("peer-celestrak", ommType());
    Producer prod(s.e.get(), pid);
    std::vector<std::vector<uint8_t>> recs;
    for (int i = 0; i < 100; i++) recs.push_back(ommRecord(20000 + i, "O" + std::to_string(i), epochAt(i), 14.1));
    auto ingest = [&](const std::string& batch, size_t n) {
        const auto attr = buildRecordAttr("peer-celestrak", "celestrak", "gp", batch);
        uint64_t last = 0;
        for (size_t i = 0; i < n; i++) last = send(s.e.get(), prod, recs[i], attr, 5000);
        prod.waitAcked(last, 10000000000ull);
    };
    ingest("b1", 100);
    uint64_t r = reconcile(prod, "celestrak", "gp", "b1");
    CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
    ingest("b1", 100);  // identical resend: no rows
    ingest("b2", 100);  // new tuple: RETAGs
    r = reconcile(prod, "celestrak", "gp", "b2");
    CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(pid);
    REQUIRE(v.ok && v.err.empty());
    KindCount k = kinds(v);
    CHECK_EQ(k.puts, 100);
    CHECK_EQ(k.retags, 100);
    CHECK_EQ(k.tagTombs, 100);
    CHECK_EQ(k.tombs, 0);  // A2: 0 TOMB rows
    checkCounters(v);
    // Only the b2 lane is live: 100 records.
    int64_t live = 0;
    for (const auto& l : v.lanes) live += l.count;
    CHECK_EQ(live, 100);
    Inspector::TypeView tv = ins.type(ommType().fid);
    CHECK_EQ(tv.arrivals.size(), size_t(100));  // A2: 0 new arrivals entries
    s.start();
    // B3 without record 42: exactly that record is tombstoned.
    const auto attr3 = buildRecordAttr("peer-celestrak", "celestrak", "gp", "b3");
    uint64_t last = 0;
    Producer prod3(s.e.get(), pid);
    for (int i = 0; i < 100; i++)
        if (i != 42) last = send(s.e.get(), prod3, recs[i], attr3, 6000);
    prod3.waitAcked(last, 10000000000ull);
    r = reconcile(prod3, "celestrak", "gp", "b3");
    CHECK_EQ(prod3.waitAcked(r, 10000000000ull), 0);
    s.close();
    REQUIRE(s.open() == 0);
    v = ins.partition(pid);
    REQUIRE(v.ok && v.err.empty());
    k = kinds(v);
    CHECK_EQ(k.tombs, 1);
    uint8_t cid42[kCidLen];
    frameCid(recs[42], cid42);
    for (const auto& row : v.rows)
        if (row.kind == kRowTomb) CHECK(std::memcmp(row.cid, cid42, kCidLen) == 0);
    checkCounters(v);
    live = 0;
    for (const auto& l : v.lanes) live += l.count;
    CHECK_EQ(live, 99);
    tv = ins.type(ommType().fid);
    CHECK_EQ(tv.arrivals.size(), size_t(100));
    s.close();
}

PS_TEST(writer_cat_supersede_by_source_lane) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&catType()});
    const uint32_t pid = s.partition("peer-cat", catType());
    Producer prod(s.e.get(), pid);
    const auto txt = buildRecordAttr("peer-cat", "celestrak", "satcat.txt", "b1");
    const auto csv = buildRecordAttr("peer-cat", "celestrak", "satcat.csv", "b1");
    const auto bare = buildRecordAttr("peer-cat", "", "", "");
    const auto v1 = catRecord(25544, "1998-067A", "", "", "ISS");
    const auto v2 = catRecord(25544, "1998-067A", "", "", "ISS (ZARYA)");
    uint64_t r = 0;
    r = send(s.e.get(), prod, v1, txt, 1);
    r = send(s.e.get(), prod, v1, txt, 2);   // unchanged: no-op
    r = send(s.e.get(), prod, v2, txt, 3);   // changed: supersedes v1 in the txt lane
    r = send(s.e.get(), prod, v1, csv, 4);   // v1 re-put under another source: own lane
    r = send(s.e.get(), prod, catRecord(7, "OBJ", "", "", "X"), bare, 5);
    CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(pid);
    REQUIRE(v.ok && v.err.empty());
    const KindCount k = kinds(v);
    CHECK_EQ(k.puts, 4);   // v1, v2, v1 again (dead copy is not a dedupe hit), obj 7
    CHECK_EQ(k.tombs, 1);  // v1 retired once by v2
    checkCounters(v);
    CHECK_EQ(v.head.counters.liveCount, uint64_t(3));
    s.close();
}

// Unique OMM frames at producer speed: one template, a counter patched into
// MEAN_MOTION (the FlatBuffer stays valid; each frame has its own CID).
struct FastFrames {
    std::vector<uint8_t> tmpl;
    size_t at = 0;
    explicit FastFrames(size_t pad) {
        const double sentinel = 123456.789012345;
        tmpl = ommRecord(7, "FAST", "2026-09-01T00:00:00Z", sentinel, pad);
        uint8_t b[8];
        std::memcpy(b, &sentinel, 8);
        for (size_t i = 0; i + 8 <= tmpl.size(); i++)
            if (std::memcmp(tmpl.data() + i, b, 8) == 0) at = i;
    }
    const std::vector<uint8_t>& next(uint64_t n) {
        const double v = 1.0 + double(n);
        std::memcpy(tmpl.data() + at, &v, 8);
        return tmpl;
    }
};

PS_TEST(writer_backpressure_flood_T1_5) {
    Store s(true, 2, true);
    s.cfg.poolBytes = 64ull << 20;
    s.cfg.reserveBytes = 4ull << 20;
    REQUIRE(s.open() == 0);
    s.fs->setSyncLatencyNs(uint64_t(argInt("bp-sync-us", 4000)) * 1000);  // a slow disk
    // The flood (OMM) and its type owner share one writer; the other
    // partitions (MPE) and theirs share the other writer.
    s.registerTypes({&ommType(), &mpeType()});
    const uint32_t flood = s.partition("flood", ommType());
    std::vector<uint32_t> others;
    for (int i = 0; i < 6; i++) others.push_back(s.partition("other" + std::to_string(i), mpeType()));
    const uint32_t floodOwner = s.e->type(ommType().fid)->ownerWriter.load();
    const uint32_t otherOwner = s.e->type(mpeType().fid)->ownerWriter.load();
    CHECK(floodOwner != otherOwner);
    if (s.e->partition(flood)->ownerWriter.load() != floodOwner) s.e->rebalance(flood, uint8_t(floodOwner));
    for (uint32_t pid : others)
        if (s.e->partition(pid)->ownerWriter.load() != otherOwner) s.e->rebalance(pid, uint8_t(otherOwner));
    sleepNs(100000000);
    const auto attr = buildRecordAttr("x", "p", "s", "b");
    uint64_t idBase = 1u << 30;
    auto measureOthers = [&](int n) {
        std::vector<double> lat;
        std::vector<Producer> prods;
        for (uint32_t pid : others) prods.emplace_back(s.e.get(), pid);
        for (int i = 0; i < n; i++) {
            for (size_t k = 0; k < prods.size(); k++) {
                const uint64_t t0 = monoNs();
                const uint64_t r = send(s.e.get(), prods[k],
                                        mpeRecord("o" + std::to_string(idBase++), 1.7e9 + i, 1.0 + i), attr, i);
                prods[k].waitAcked(r, 10000000000ull);
                lat.push_back(double(monoNs() - t0) / 1e6);
            }
        }
        std::sort(lat.begin(), lat.end());
        return lat[size_t(lat.size() * 0.99)];
    };
    // --bp-samples: acks timed per partition (default 150). The in-memory
    // host keeps every flooded byte twice (page cache and device image), and
    // the flood lasts as long as the flooded measurement: a wasm32 host (4 GiB)
    // runs this test with fewer samples (docs/PARTITION-STORE-WASM.md).
    const int samples = int(argInt("bp-samples", 150));
    const double base = measureOthers(samples);
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> maxUsed{0}, zeroCredits{0}, sent{0}, drainedAtStop{0}, floodWaits{0};
    const uint64_t cap = s.e->ring(flood)->cap;
    uint64_t floodStart = 0, floodEnd = 0;
    std::thread floodThread([&] {
        Producer p(s.e.get(), flood);
        FastFrames ff(800);
        const auto fa = buildRecordAttr("flood", "p", "s", "b");
        uint64_t last = 0;
        floodStart = monoNs();
        for (uint64_t i = 0; !stop.load(); i++) {
            const uint64_t r = send(s.e.get(), p, ff.next(i), fa, int64_t(i));
            if (r) {
                last = r;
                sent.fetch_add(1);
            }
        }
        floodEnd = monoNs();
        floodWaits.store(p.creditWaits());
        drainedAtStop.store(p.ringDesc()->ackedRseq.load());
        p.waitAcked(last, 120000000000ull);
    });
    std::thread sampler([&] {
        Producer p(s.e.get(), flood);
        while (!stop.load()) {
            const uint64_t u = s.e->ring(flood)->used();
            uint64_t m = maxUsed.load();
            while (u > m && !maxUsed.compare_exchange_weak(m, u)) {}
            // Zero credit = the next entry does not fit.
            if (p.credits() < 1024) zeroCredits.fetch_add(1);
            sleepNs(100000);
        }
    });
    sleepNs(500000000);
    const double flooded = measureOthers(samples);
    stop.store(true);
    floodThread.join();
    sampler.join();
    sleepNs(300000000);
    const uint64_t credits = Producer(s.e.get(), flood).credits();
    const double floodSecs = double(floodEnd - floodStart) / 1e9;
    report("backpressure_other_ack_p99_baseline_ms", base, "ms");
    report("backpressure_other_ack_p99_flooded_ms", flooded, "ms");
    report("backpressure_flood_ring_max_used", double(maxUsed.load()), "bytes");
    report("backpressure_flood_ring_cap", double(cap), "bytes");
    report("backpressure_zero_credit_samples", double(zeroCredits.load()), "samples");
    report("backpressure_flood_credit_waits", double(floodWaits.load()), "waits");
    report("backpressure_flood_accepted_rate", double(sent.load()) / floodSecs, "records/s");
    report("backpressure_flood_drain_rate", double(drainedAtStop.load()) / floodSecs, "records/s");
    report("backpressure_pool_peak_slabs", double(s.e->pool().peakInUse()), "slabs");
    CHECK(maxUsed.load() <= cap);                         // ring at or under its cap
    CHECK(zeroCredits.load() > 0 && floodWaits.load() > 0);  // credits went to 0
    CHECK(credits > 0);                                   // and recovered
    CHECK(s.e->pool().peakInUse() <= s.e->pool().total() - s.e->reserveSlabs());
    // The ack-p99 ratio is a Linux-8 acceptance number (design: acceptance is
    // measured on the machine each test names). On fewer than 8 hardware
    // threads the flood's writer, producer and merge helper compete with the
    // other partitions for cores, so the ratio is reported, not enforced; the
    // checks above and the frame count below hold on every machine.
    const unsigned threads = std::thread::hardware_concurrency();
    const double ratio = base > 0 ? flooded / base : 0;
    report("backpressure_other_ack_p99_ratio", ratio, "x");
    report("backpressure_hardware_threads", double(threads), "threads");
    if (threads >= 8) {
        CHECK(flooded <= base * 1.2);  // other partitions' commit p99 changes <= 20%
    } else {
        std::printf("  ratio %.3f reported, not enforced: %u hardware threads (< 8)\n", ratio, threads);
    }
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(flood);
    CHECK(v.err.empty());
    CHECK_EQ(uint64_t(v.rows.size()), sent.load());  // 0 frames lost
    s.close();
}

static void rebalanceRun(double seconds, uint32_t writers, uint32_t partitions, bool requireHelperOverlap,
                         uint64_t roundPauseNs = 0) {
    // No crash here: the in-memory host keeps one copy of each file, and the
    // 10-minute run paces its producers so that copy stays in memory.
    Store s(false, writers, true);
    s.cfg.audit = true;
    s.cfg.mergeL0Blocks = 4;  // merges during rebalancing (A26)
    s.cfg.mergeMinL0Bytes = 0;
    s.cfg.mergeHelpers = 1;
    s.cfg.testHelperStallNs = 100000000;  // A26: helpers stalled for 100 ms
    s.cfg.testHelperStallEvery = 8;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &mpeType()});
    std::vector<uint32_t> pids;
    for (uint32_t i = 0; i < partitions; i++)
        pids.push_back(s.partition("p" + std::to_string(i), (i % 2) ? mpeType() : ommType()));
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> moves{0};
    std::vector<std::thread> producers;
    std::vector<uint64_t> sentPer(partitions, 0);
    for (uint32_t t = 0; t < 4; t++) {
        producers.emplace_back([&, t] {
            std::vector<Producer> ps;
            for (uint32_t i = t; i < partitions; i += 4) ps.emplace_back(s.e.get(), pids[i]);
            const auto attr = buildRecordAttr("x", "prov", "src", "b");
            std::vector<uint64_t> last(ps.size(), 0);
            for (uint64_t n = 0; !stop.load(); n++) {
                for (size_t k = 0; k < ps.size(); k++) {
                    const uint32_t idx = t + uint32_t(k) * 4;
                    const auto f = (idx % 2) ? mpeRecord("E" + std::to_string(n), 1.7e9 + double(n), double(idx))
                                             : ommRecord(uint32_t(n), "O", epochAt(int(n % 100000)), double(idx) + n * 1e-6);
                    const uint64_t r = send(s.e.get(), ps[k], f, attr, int64_t(n));
                    if (r) {
                        last[k] = r;
                        sentPer[idx]++;
                    }
                }
                if (roundPauseNs) sleepNs(roundPauseNs);
            }
            for (size_t k = 0; k < ps.size(); k++) ps[k].waitAcked(last[k], 30000000000ull);
        });
    }
    std::thread rebalancer([&] {
        std::mt19937 rng(9);
        while (!stop.load()) {
            const uint32_t pid = pids[rng() % pids.size()];
            if (s.e->rebalance(pid, uint8_t(rng() % writers)) == 0) moves.fetch_add(1);
            sleepNs(50000000);
        }
    });
    sleepNs(uint64_t(seconds * 1e9));
    stop.store(true);
    for (auto& t : producers) t.join();
    rebalancer.join();
    const auto audit = s.e->auditLog();
    const EngineStats es = s.e->stats();
    s.close();
    report("rebalance_moves", double(moves.load()), "moves");
    report("rebalance_merges", double(es.merges), "merges");
    report("rebalance_helper_stalls_100ms", double(es.helperStalls), "stalls");
    report("rebalance_non_owner_helper_writes", double(es.mergeNotOwner), "writes");
    report("rebalance_handoffs_during_helper_merge", double(es.handoffHelperWaits), "handoffs");
    CHECK(es.merges > 0);
    CHECK(es.helperStalls > 0);
    CHECK_EQ(es.mergeNotOwner, uint64_t(0));
    if (requireHelperOverlap) CHECK(es.handoffHelperWaits > 0);
    report("rebalance_commits_audited", double(audit.size()), "commits");
    // Per partition: pseq ranges contiguous, one (writer, thread) per epoch,
    // epochs non-decreasing, commit intervals never overlap.
    std::map<uint32_t, std::vector<AuditRecord>> per;
    for (const auto& a : audit) per[a.pid].push_back(a);
    int violations = 0;
    uint32_t epochsSeen = 0;
    for (auto& kv : per) {
        auto& v = kv.second;
        std::sort(v.begin(), v.end(), [](const AuditRecord& a, const AuditRecord& b) { return a.firstPseq < b.firstPseq; });
        std::map<uint32_t, std::pair<uint32_t, uint32_t>> owner;
        for (size_t i = 0; i < v.size(); i++) {
            if (i > 0) {
                if (v[i].firstPseq != v[i - 1].lastPseq + 1) violations++;
                if (v[i].epoch < v[i - 1].epoch) violations++;
                if (v[i].startNs < v[i - 1].endNs) violations++;
            }
            auto it = owner.find(v[i].epoch);
            if (it == owner.end()) owner[v[i].epoch] = {v[i].writer, v[i].osTid};
            else if (it->second != std::make_pair(uint32_t(v[i].writer), v[i].osTid)) violations++;
        }
        epochsSeen += uint32_t(owner.size());
    }
    report("rebalance_ownership_intervals", double(epochsSeen), "intervals");
    CHECK_EQ(violations, 0);
    CHECK(moves.load() > 0);
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    uint64_t rows = 0, sentTotal = 0;
    for (uint32_t i = 0; i < partitions; i++) {
        PartView v = ins.partition(pids[i]);
        if (!v.err.empty()) {
            std::fprintf(stderr, "  pid %u: %s\n", pids[i], v.err.c_str());
            violations++;
        }
        rows += v.rows.size();
        sentTotal += sentPer[i];
        checkCounters(v);
    }
    CHECK_EQ(rows, sentTotal);
    s.close();
}

PS_TEST(writer_single_writer_under_rebalance_T1_4) { rebalanceRun(double(argInt("rebalance-seconds", 8)), 4, 16, true); }
PS_SLOW_TEST(writer_single_writer_under_rebalance_T1_4_full) { rebalanceRun(600.0, 8, 64, true, 10000000); }  // ~6,400 records/s

PS_TEST(writer_memory_2048_partitions_64_active_T1_9) {
    Store s(true, 1, true);
    s.cfg.poolBytes = 192ull << 20;   // §17 host-02 profile
    s.cfg.arenaBytes = 24ull << 20;
    s.cfg.reserveBytes = 16ull << 20;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids;
    for (int i = 0; i < 2048; i++) pids.push_back(s.partition("prod" + std::to_string(i), ommType()));
    const auto attr = buildRecordAttr("x", "p", "s", "b");
    std::vector<Producer> prods;
    for (int i = 0; i < 64; i++) prods.emplace_back(s.e.get(), pids[size_t(i) * 32]);
    std::vector<uint64_t> last(64, 0);
    for (int n = 0; n < 400; n++)
        for (int i = 0; i < 64; i++)
            last[i] = send(s.e.get(), prods[i], ommRecord(uint32_t(n), "O", epochAt(n), i + n * 1e-3, 200), attr, n);
    for (int i = 0; i < 64; i++) prods[i].waitAcked(last[i], 30000000000ull);
    const EngineStats st = s.e->stats();
    uint32_t idleWithSlabs = 0;
    for (size_t i = 0; i < pids.size(); i++) {
        if (i % 32 == 0 && i / 32 < 64) continue;
        Partition* p = s.e->partition(pids[i]);
        if (p->ring->mappedPages.load() || p->chain.slabs()) idleWithSlabs++;
    }
    report("memory_writer_committed", double(st.committedBytes) / (1 << 20), "MiB");
    report("memory_pool_committed", double(st.poolCommittedBytes) / (1 << 20), "MiB");
    report("memory_descriptors", double(st.descriptorBytes) / (1 << 20), "MiB");
    CHECK(st.committedBytes <= (288ull << 20));
    CHECK_EQ(idleWithSlabs, 0u);
    s.close();
}

static void hotPathRun(bool journal) {
    Store s(true, 1, true);
    s.cfg.commitJournal = journal;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids;
    for (int i = 0; i < 8; i++) pids.push_back(s.partition("hot" + std::to_string(i), ommType()));
    const auto attr = buildRecordAttr("x", "p", "s", "b");
    std::vector<Producer> prods;
    for (uint32_t pid : pids) prods.emplace_back(s.e.get(), pid);
    // Warm-up: partitions active, lane tuple interned, type owner warm.
    std::vector<uint64_t> last(pids.size(), 0);
    for (int n = 0; n < 200; n++)
        for (size_t i = 0; i < prods.size(); i++)
            last[i] = send(s.e.get(), prods[i], ommRecord(uint32_t(n), "W", epochAt(n), double(i) + n), attr, n);
    for (size_t i = 0; i < prods.size(); i++) prods[i].waitAcked(last[i], 30000000000ull);
    sleepNs(50000000);
    std::vector<std::vector<uint8_t>> frames;
    for (int n = 0; n < 4000; n++) frames.push_back(ommRecord(uint32_t(100000 + n), "H", epochAt(n), 7.0 + n));
    const uint64_t before = gHotAllocs.load();
    const uint64_t rows0 = s.e->stats().rowsAppended;
    for (int n = 0; n < 4000; n++) last[n % 8] = send(s.e.get(), prods[n % 8], frames[n], attr, n);
    for (size_t i = 0; i < prods.size(); i++) prods[i].waitAcked(last[i], 30000000000ull);
    const uint64_t hot = gHotAllocs.load() - before;
    const uint64_t rows = s.e->stats().rowsAppended - rows0;
    report(journal ? "hot_path_allocations_journal" : "hot_path_allocations", double(hot), "allocations");
    report(journal ? "hot_path_records_journal" : "hot_path_records", double(rows), "records");
    CHECK_EQ(rows, uint64_t(4000));
    CHECK_EQ(hot, uint64_t(0));
    s.close();
}

PS_TEST(writer_hot_path_zero_malloc_T1_8) {
    hotPathRun(false);
    hotPathRun(true);  // A8 commit journal
}

PS_TEST(writer_control_transactions_single_batch) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ctlType()});
    const uint32_t pid = s.partition("node-local", ctlType());
    Producer prod(s.e.get(), pid);
    uint64_t r = 0;
    prod.enqueue(kEntTxnBegin, 0, 1, nullptr, nullptr, 0, nullptr, 0, &r);
    for (int i = 0; i < 5; i++) {
        const auto attr = buildRecordAttr("node", "", "", "", "", "", "", "t" + std::to_string(i % 2) + std::string("\0k", 2) + std::to_string(i));
        std::vector<uint8_t> body(64, uint8_t(i));
        putU32(body.data(), 60);
        prod.enqueue(kEntCtl, 0, 1, nullptr, attr.data(), uint32_t(attr.size()), body.data(), uint32_t(body.size()), &r);
    }
    prod.enqueue(kEntTxnEnd, 0, 1, nullptr, nullptr, 0, nullptr, 0, &r);
    CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(pid);
    REQUIRE(v.ok && v.err.empty());
    CHECK_EQ(kinds(v).ctl, 5);
    CHECK_EQ(v.head.nL0, 1);  // the transaction is one batch
    for (const auto& row : v.rows) CHECK(row.flags & kRowTxn);
    s.close();
}

PS_TEST(writer_fsync_eio_quarantines_only_that_partition) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t a = s.partition("alpha", ommType());
    const uint32_t b = s.partition("beta", ommType());
    Producer pa(s.e.get(), a), pb(s.e.get(), b);
    const auto attr = buildRecordAttr("x", "p", "s", "b");
    uint64_t ra = send(s.e.get(), pa, ommRecord(1, "A", epochAt(1), 1), attr, 1);
    uint64_t rb = send(s.e.get(), pb, ommRecord(2, "B", epochAt(2), 2), attr, 1);
    CHECK_EQ(pa.waitAcked(ra, 10000000000ull), 0);
    CHECK_EQ(pb.waitAcked(rb, 10000000000ull), 0);
    char needle[32];
    std::snprintf(needle, sizeof(needle), "/p/%08x/m-", a);
    s.fs->failSyncs(needle, 1);
    ra = send(s.e.get(), pa, ommRecord(3, "A", epochAt(3), 3), attr, 2);
    CHECK(pa.waitAcked(ra, 3000000000ull) != 0);  // never acked
    CHECK_EQ(s.e->ring(a)->state.load(), uint32_t(kRingQuarantined));
    rb = send(s.e.get(), pb, ommRecord(4, "B", epochAt(4), 4), attr, 2);
    CHECK_EQ(pb.waitAcked(rb, 10000000000ull), 0);  // the other partition continues
    s.close();
}

PS_TEST(writer_enospc_pauses_then_recovers_without_loss) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t a = s.partition("alpha", ommType());
    Producer pa(s.e.get(), a);
    const auto attr = buildRecordAttr("x", "p", "s", "b");
    char needle[32];
    std::snprintf(needle, sizeof(needle), "/p/%08x/d-", a);
    s.fs->failWrites(needle, 3, FLATSQL_IO_ERR_NOSPACE);
    uint64_t last = 0;
    for (int i = 0; i < 50; i++) last = send(s.e.get(), pa, ommRecord(uint32_t(i), "A", epochAt(i), i), attr, i);
    CHECK_EQ(pa.waitAcked(last, 20000000000ull), 0);
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(a);
    CHECK(v.err.empty());
    CHECK_EQ(v.rows.size(), size_t(50));
    checkCounters(v);
    s.close();
}

PS_TEST(writer_tomb_range_spares_supersede_heads) {
    // TOMB_RANGE{seg, epoch < t} (§13, A24): only live PUTs below the bound in
    // that segment die; supersede-lane heads and later segments stay.
    Store s(true, 1, true);
    s.cfg.sealRecords = 40;
    s.cfg.reconcileStep = 7;  // several bounded steps
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &catType()});
    const uint32_t pid = s.partition("peer-q", ommType());
    Producer prod(s.e.get(), pid);
    const auto attr = buildRecordAttr("peer-q", "prov", "src", "b1");
    uint64_t r = 0;
    for (int i = 0; i < 100; i++) {
        r = send(s.e.get(), prod, ommRecord(40000 + i, "Q" + std::to_string(i), epochAt(i), 13.0), attr, i);
        if (i % 10 == 9) CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
    }
    CHECK_EQ(prod.waitAcked(r, 10000000000ull), 0);
    // Bound: the epoch of record 25; segment 0 holds records 0..39.
    int64_t bound = 0;
    {
        Inspector ins(s.fs.get(), s.root);
        PartView v = ins.partition(pid);
        REQUIRE(v.ok && v.rows.size() >= 26);
        bound = v.rows[25].epochMs;
    }
    std::atomic<int32_t> remaining{0};
    CHECK_EQ(s.e->tombRange(pid, 0, bound, &remaining), 0);
    const uint64_t deadline = monoNs() + 10000000000ull;
    while (remaining.load() != 0 && monoNs() < deadline) sleepNs(1000000);
    CHECK_EQ(remaining.load(), 0);
    // CAT: supersede-lane heads are never evicted; a superseded copy is
    // already dead; a record with no identity is.
    const uint32_t cpid = s.partition("peer-c", catType());
    Producer cprod(s.e.get(), cpid);
    const auto cattr = buildRecordAttr("peer-c", "celestrak", "satcat", "b1");
    r = send(s.e.get(), cprod, catRecord(25544, "1998-067A", "", "", "ISS"), cattr, 1);
    r = send(s.e.get(), cprod, catRecord(25544, "1998-067A", "", "", "ISS (ZARYA)"), cattr, 2);
    r = send(s.e.get(), cprod, catRecord(0, "", "", "", "NO IDENTITY"), cattr, 3);
    CHECK_EQ(cprod.waitAcked(r, 10000000000ull), 0);
    std::atomic<int32_t> remaining2{0};
    CHECK_EQ(s.e->tombRange(cpid, 0, INT64_MAX, &remaining2), 0);
    const uint64_t deadline2 = monoNs() + 10000000000ull;
    while (remaining2.load() != 0 && monoNs() < deadline2) sleepNs(1000000);
    CHECK_EQ(remaining2.load(), 0);
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(pid);
    REQUIRE(v.ok && v.err.empty());
    std::set<uint64_t> killed;
    for (const auto& row : v.rows)
        if (row.kind == kRowTomb) killed.insert(row.targetPseq);
    std::set<uint64_t> expect;
    for (const auto& row : v.rows)
        if (row.kind == kRowPut && row.pseq <= 40 && row.epochMs < bound) expect.insert(row.pseq);
    CHECK_EQ(expect.size(), size_t(25));
    CHECK(killed == expect);
    checkCounters(v);
    CHECK_EQ(v.head.counters.liveCount, uint64_t(75));
    PartView cv = ins.partition(cpid);
    REQUIRE(cv.ok && cv.err.empty());
    const KindCount ck = kinds(cv);
    CHECK_EQ(ck.puts, 3);
    CHECK_EQ(ck.tombs, 2);  // ISS v1 by supersede, the identity-less record by TOMB_RANGE
    checkCounters(cv);
    CHECK_EQ(cv.head.counters.liveCount, uint64_t(1));
    s.close();
}

PS_TEST(writer_commit_journal_one_sync_per_round_A8) {
    // A8 fallback (§22.4 ruling 5): one journal fsync per committing round,
    // files synced by asynchronous checkpoints; a crash dropping every
    // unsynced write keeps every acked record; a clean stop empties the
    // journals.
    Store s(true, 2, true);
    s.cfg.commitJournal = true;
    s.cfg.journalCkptBytes = 32u << 10;
    s.cfg.journalCkptMs = 20;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &mpeType()});
    std::vector<uint32_t> pids;
    for (int i = 0; i < 10; i++)
        pids.push_back(s.partition("jp" + std::to_string(i), (i % 2) ? mpeType() : ommType()));
    std::vector<std::unique_ptr<Producer>> prods;
    for (uint32_t pid : pids) prods.emplace_back(new Producer(s.e.get(), pid));
    const auto attr = buildRecordAttr("jp", "prov", "src", "b1");
    std::vector<std::vector<std::pair<uint64_t, std::vector<uint8_t>>>> sent(pids.size());
    auto wave = [&](int from, int n) {
        std::vector<uint64_t> last(pids.size(), 0);
        for (int i = from; i < from + n; i++) {
            for (size_t k = 0; k < pids.size(); k++) {
                auto f = (k % 2) ? mpeRecord("J" + std::to_string(i), 1.7e9 + i, double(k))
                                 : ommRecord(uint32_t(60000 + i), "J", epochAt(i), 12.0 + double(k));
                last[k] = send(s.e.get(), *prods[k], f, attr, i);
                sent[k].push_back({last[k], f});
            }
        }
        for (size_t k = 0; k < pids.size(); k++) CHECK_EQ(prods[k]->waitAcked(last[k], 10000000000ull), 0);
    };
    for (int w = 0; w < 20; w++) wave(w * 20, 20);
    // A consistent snapshot: the counters move at different points of a
    // round, so wait until a round is not in flight (no new work arrives).
    EngineStats st = s.e->stats();
    for (int i = 0; i < 1000; i++) {
        sleepNs(2000000);
        const EngineStats again = s.e->stats();
        const bool same = again.iterationsWithCommit == st.iterationsWithCommit &&
                          again.commitSyncRounds == st.commitSyncRounds && again.journalRecords == st.journalRecords;
        st = again;
        if (same) break;
    }
    report("journal_commit_rounds", double(st.iterationsWithCommit), "rounds");
    report("journal_sync_rounds", double(st.commitSyncRounds), "rounds");
    report("journal_records", double(st.journalRecords), "records");
    report("journal_checkpoints", double(st.journalCheckpoints), "checkpoints");
    CHECK(st.journalRecords > 0);
    CHECK_EQ(st.journalRecords, st.commitSyncRounds);          // one fsync per round
    CHECK(st.commitSyncRounds <= st.iterationsWithCommit);
    CHECK(st.journalCheckpoints > 0);
    // Acked just before a crash that drops every unsynced write.
    wave(400, 5);
    for (auto& p : prods) p.reset();
    s.crash(FaultFs::kDropAll, 7);
    REQUIRE(s.open() == 0);
    st = s.e->stats();
    report("journal_replayed_records", double(st.journalReplayRecords), "records");
    CHECK_EQ(st.openDataBytes, uint64_t(0));
    Inspector ins(s.fs.get(), s.root);
    int missing = 0, bad = 0;
    for (size_t k = 0; k < pids.size(); k++) {
        PartView v = ins.partition(pids[k]);
        REQUIRE(v.ok && v.err.empty());
        checkCounters(v);
        std::map<std::string, const RecRow*> puts;
        for (const auto& row : v.rows)
            if (row.kind == kRowPut) puts[std::string(reinterpret_cast<const char*>(row.cid), kCidLen)] = &row;
        for (const auto& sr : sent[k]) {
            uint8_t cid[kCidLen];
            frameCid(sr.second, cid);
            auto it = puts.find(std::string(reinterpret_cast<const char*>(cid), kCidLen));
            if (it == puts.end()) {
                missing++;
                continue;
            }
            if (ins.frame(pids[k], *it->second) != sr.second) bad++;
        }
    }
    CHECK_EQ(missing, 0);
    CHECK_EQ(bad, 0);
    s.close();
    // A clean stop checkpoints everything: the next open replays nothing.
    REQUIRE(s.open() == 0);
    CHECK_EQ(s.e->stats().journalReplayRecords, uint64_t(0));
    CHECK_EQ(s.e->stats().openJournalBytes, uint64_t(0));
    s.close();
}

// Writer TLV 19 / EngineConfig::maxEntryBytes: the largest ring entry. A 9 MiB
// frame is refused under the default (1 MiB + 4 KiB) and stored under a
// 10 MiB setting, the writer arena growing to hold two such entries.
PS_TEST(writer_max_entry_bytes_configurable) {
    const auto big = iqcRecord("E1", "2026-09-01T00:00:00Z", 9u << 20, 7);
    const auto attr = buildRecordAttr("big", "prov", "src", "b");
    {
        Store s(true, 1, true);
        REQUIRE(s.open() == 0);
        s.registerTypes({&iqcType()});
        Producer prod(s.e.get(), s.partition("big", iqcType()));
        CHECK_EQ(send(s.e.get(), prod, big, attr, 1, false), uint64_t(0));
        s.close();
    }
    Store s(true, 1, true);
    s.cfg.maxEntryBytes = 10u << 20;
    REQUIRE(s.open() == 0);
    CHECK(s.e->config().arenaBytes >= 4 * ((10ull << 20) + 4096));
    s.registerTypes({&iqcType()});
    const uint32_t pid = s.partition("big", iqcType());
    {
        Producer prod(s.e.get(), pid);
        CHECK_EQ(prod.ringDesc()->maxEntry, uint64_t(10u << 20));
        const uint64_t r = send(s.e.get(), prod, big, attr, 1);
        REQUIRE(r);
        CHECK_EQ(prod.waitAcked(r, 20000000000ull), 0);
    }
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    const PartView v = ins.partition(pid);
    REQUIRE(v.err.empty() && v.rows.size() == 1);
    CHECK(ins.frame(pid, v.rows[0]) == big);
    s.close();
}

// The same setting through the C ABI (flatsql_ps_init tag 19), and the two
// store-migrate counters at the end of flatsql_ps_stats.
PS_TEST(writer_capi_tlv19_max_entry_and_stats) {
    const std::string dir = std::filesystem::temp_directory_path().string() + "/flatsql-ps-tlv19-" +
                            std::to_string(monoNs());
    std::filesystem::create_directories(dir);
    std::vector<uint8_t> cfg;
    auto tlv = [&](uint16_t tag, const void* v, uint32_t n) {
        const size_t at = cfg.size();
        cfg.resize(at + 6 + n);
        putU16(cfg.data() + at, tag);
        putU32(cfg.data() + at + 2, n);
        std::memcpy(cfg.data() + at + 6, v, n);
    };
    tlv(1, dir.data(), uint32_t(dir.size()));
    const uint64_t maxEntry = 3u << 20;
    tlv(19, &maxEntry, 8);
    REQUIRE(flatsql_ps_init(FLATSQL_PS_ROLE_WRITER, cfg.data(), int32_t(cfg.size())) == 0);
    REQUIRE(flatsql_ps_start() == 0);
    const auto& type = ommType();
    CHECK(flatsql_ps_register_type(type.config.data(), int32_t(type.config.size())) >= 0);
    const int32_t pid = flatsql_ps_register_partition(reinterpret_cast<const uint8_t*>("tlv19"), 5, type.fid);
    CHECK(pid > 0);
    FlatsqlPsLayout L;
    REQUIRE(flatsql_ps_layout(&L) >= 0);
    const uintptr_t ring = uintptr_t(flatsql_ps_ring(pid));
    REQUIRE(ring != 0);
    uint64_t got = 0;
    std::memcpy(&got, reinterpret_cast<const uint8_t*>(ring) + L.offMaxEntry, 8);
    CHECK_EQ(got, maxEntry);
    const int32_t n = flatsql_ps_stats(nullptr, 0);
    CHECK_EQ(n, int32_t(41 * 8));
    std::vector<uint8_t> st(size_t(n > 0 ? n : 0));
    CHECK_EQ(flatsql_ps_stats(st.data(), n), n);
    if (n == 41 * 8) {
        CHECK_EQ(getU64(st.data() + 24 * 8), uint64_t(0));  // migrated gseqs used
        CHECK_EQ(getU64(st.data() + 25 * 8), uint64_t(0));  // migrated gseq fallbacks
        CHECK_EQ(getU64(st.data() + 26 * 8), uint64_t(0));  // hot splits
        CHECK_EQ(getU64(st.data() + 35 * 8), uint64_t(0));  // arrival entries dropped
        // TB03: a fresh store at this engine's level; no ratchet; no lane checkpoint.
        CHECK_EQ(getU64(st.data() + 36 * 8), uint64_t(kFormatMax));  // store format
        CHECK_EQ(getU64(st.data() + 37 * 8), uint64_t(kFormatMax));  // engine kFormatMax
        CHECK_EQ(getU64(st.data() + 38 * 8), uint64_t(0));           // ratcheted from
        CHECK_EQ(getU64(st.data() + 39 * 8), uint64_t(0));           // lane checkpoints cut
        CHECK_EQ(getU64(st.data() + 40 * 8), uint64_t(0));           // lane checkpoint bytes
    }
    CHECK_EQ(flatsql_ps_stop(5000), 0);
    std::filesystem::remove_all(dir);
}
