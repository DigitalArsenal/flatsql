// Terabyte unit gates (audit §4.5, task flatsql-ps-terabyte-harness).
//
// Four gates that must pass before format 2 can hold a terabyte on one node.
// Each asserts an audit pass criterion (the audit's §4 table). B2, M1 and B4
// fail on the engine as built (flatsql 14a3075) and are committed marked as
// expected failures (Gate.fails); the lanes that fix those blockers clear the
// marks. B1 passes since SDN reads labels from the type log. They are slow
// tests: run one by name (`flatsql_ps_test --test=tbgate_B2`), or all four,
// each classified PASS, FAIL, HANG or TRAP, with scripts/tb/run-gates.sh.
//
//   B1  129 partitions in one type: labeled_through of every partition is
//       readable from the head and type log, and a reader sees a write
//       labeled within 50 ms (p99).
//   B2  20,000 partitions written since boot: the virtual-handle high-water
//       stays at or below half of the SDN host's 16,384 handles.
//   M1  100,000 lifetime registrations with drops (M1, M2): every one
//       succeeds, and a registered cold partition costs at most 1 KiB of heap.
//   B4  5,000 segments in one partition: maintenance and commit p99 below
//       10 ms per step, and ingest at S = 5,000 at least 80% of the rate at
//       S = 500.
#include <algorithm>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"
#include "ps/tb_corpus.h"

using namespace pst;
using pst::tb::CountingIo;
using pst::tb::Gate;
using pst::tb::HistWindow;

namespace {

constexpr uint64_t kSec = 1000000000ull;

std::vector<uint8_t> u32Payload(uint32_t v) {
    std::vector<uint8_t> p(4);
    putU32(p.data(), v);
    return p;
}

const Gate kGateB1 = {
    "B1", "B1", false,
    "past 128 partitions a type keeps labeled_through in its type log (nLabels = 0xffff); SDN reads it there since "
    "4e69a9db7 (heads.go typeLabels.fold). The gate holds the engine to what that reader needs: every label durable "
    "in the head or log, a write readable as labeled within 50 ms p99"};
const Gate kGateB2 = {
    "B2", "B2", true,
    "a partition's head handle stays open after it cools (merge.cpp partitionCool) and the type owner keeps a "
    "handle per (pid, m-seg) (partM): open handles grow with every partition written since boot"};
const Gate kGateM1 = {
    "M1", "M1, M2", true,
    "pids are never reused and the partition table holds 65,535 (open.cpp partsCap_ = 1 << 16): lifetime "
    "registration 65,536 fails NOSPACE; a registered partition keeps about 7 KB resident (Partition, RingDesc)"};
const Gate kGateB4 = {
    "B4", "B4, M3", true,
    "the partition ledger is a vector scanned per sealed segment on every MERGE_DONE, SEAL, SWAP, RETIRE and "
    "UNLINKED commit (quota.cpp partitionPublishSummary) and by pickCandidate every 50 ms, and every merge "
    "rewrites the whole manifest: maintenance per merge grows as O(S^2)"};

}  // namespace

// ---- B1 ---------------------------------------------------------------------------------
// 129 producers of one type. Past 128 partitions the engine writes nLabels =
// 0xffff into the type head and keeps labeled_through in the type log. The
// audit's hang was the host's: SDN read only the inline table and WaitLabeled
// never returned; SDN 4e69a9db7 and 0de504531 fold the type log instead. This
// gate reads labels the way that reader does (tb::headLabeledThrough) and
// passes when every partition's labeled_through is readable from durable
// files once labels catch up, and a write is readable as labeled within 50 ms
// at p99 (audit: WaitLabeled p99 <= 50 ms with more than 128 partitions per
// type). --tb-b1-parts=128 exercises the inline table instead.
TB_GATE(tbgate_B1_129_partitions_one_type, kGateB1) {
    const uint32_t P = uint32_t(argInt("tb-b1-parts", 129));
    const uint32_t K = uint32_t(argInt("tb-b1-records", 4));
    const uint32_t rounds = uint32_t(argInt("tb-b1-rounds", 200));
    Store s(false, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids;
    for (uint32_t i = 0; i < P; i++) {
        const uint32_t pid = s.partition("b1p" + std::to_string(i), ommType());
        REQUIRE(pid != 0);
        pids.push_back(pid);
    }
    const auto attr = buildRecordAttr("12D3KooWb1peer", "prov", "src", "batch-1");
    uint32_t norad = 1;
    // 1. Every write is acknowledged (the engine side never stalls).
    for (uint32_t i = 0; i < P; i++) {
        Producer prod(s.e.get(), pids[i]);
        uint64_t last = 0;
        for (uint32_t k = 0; k < K; k++, norad++)
            last = send(s.e.get(), prod, ommRecord(norad, "2026-001A", tb::isoSecond(norad), 15.0 + k * 1e-3, 64),
                        attr, 1790000000000ll + norad);
        REQUIRE(last != 0);
        if (prod.waitAcked(last, 30 * kSec) != 0) {
            TB_HANG("partition " + std::to_string(pids[i]) + ": acks not released in 30 s");
            return;
        }
    }
    // 2. The engine labels every partition (in memory).
    if (!waitLabeledEngine(s.e.get(), pids, 60 * kSec)) {
        TB_HANG("the type owner did not label every partition in 60 s");
        return;
    }
    // 3. A reader of the durable head and type log learns labeled_through >=
    //    pseq_hi for every partition (poll for up to 10 s).
    Io* io = s.fs.get();
    uint32_t resolved = 0;
    for (const uint64_t until = monoNs() + 10 * kSec; resolved < P && monoNs() < until; sleepNs(5000000)) {
        resolved = 0;
        for (uint32_t pid : pids) {
            const uint64_t hi = tb::headPseqHi(io, s.root, pid);
            uint64_t lt = 0;
            if (hi && tb::headLabeledThrough(io, s.root, ommType().fid, pid, &lt) && lt >= hi) resolved++;
        }
    }
    report("b1_partitions", double(P), "n");
    {
        // Which path the reader took: the inline table (nLabels <= 128) or
        // the type-log fold (0xffff).
        PathBuf hp;
        pathType(&hp, s.root.c_str(), ommType().fid, "h.fsh");
        std::vector<uint8_t> head;
        TypeHeadFixed th{};
        if (tb::readDurableHead(io, hp, kHeadType, &head) && head.size() >= sizeof(th))
            std::memcpy(&th, head.data(), sizeof(th));
        report("b1_type_head_nlabels", double(th.nLabels), "n (65535: labels in the type log)");
    }
    report("b1_labels_resolved_from_files", double(resolved), "partitions");
    CHECK_EQ(resolved, P);
    // 4. WaitLabeled as SDN runs it: one write, its ack, then a reader of the
    //    durable files polls until the partition's labeled_through covers it.
    std::vector<double> ms;
    uint32_t unresolved = 0;
    uint64_t rng = 0xB1;
    for (uint32_t r = 0; r < rounds; r++, norad++) {
        const uint32_t pid = pids[size_t(tb::splitmix(&rng) % P)];
        Producer prod(s.e.get(), pid);
        const uint64_t rseq = send(s.e.get(), prod, ommRecord(norad, "2026-001A", tb::isoSecond(norad), 15.1, 64),
                                   attr, 1790000000000ll + norad);
        REQUIRE(rseq != 0);
        if (prod.waitAcked(rseq, 30 * kSec) != 0) {
            TB_HANG("round write not acknowledged in 30 s");
            return;
        }
        const uint64_t hi = tb::headPseqHi(io, s.root, pid);
        const uint64_t t0 = monoNs();
        bool ok = false;
        while (monoNs() - t0 < 2 * kSec) {
            uint64_t lt = 0;
            if (tb::headLabeledThrough(io, s.root, ommType().fid, pid, &lt) && lt >= hi) {
                ok = true;
                break;
            }
            sleepNs(200000);
        }
        if (ok) ms.push_back(double(monoNs() - t0) / 1e6);
        else unresolved++;
        // Ten rounds none of which resolves in 2 s is the answer: stop there.
        if (ms.empty() && unresolved >= 10) break;
    }
    report("b1_waitlabeled_rounds", double(rounds), "n");
    report("b1_waitlabeled_unresolved_2s", double(unresolved), "rounds");
    if (!ms.empty()) report("b1_waitlabeled_p99", tb::pct(ms, 0.99), "ms");
    CHECK_EQ(unresolved, 0u);
    CHECK(!ms.empty() && tb::pct(ms, 0.99) <= 50.0);
    s.close();
}

// ---- B2 ---------------------------------------------------------------------------------
// 20,000 producers each write two records, paced (1,000 new partitions a
// second) so that a partition idles into its cool well before the run ends.
// SDN gives each instance 16,384 virtual handles (psinstance.go MaxHandles)
// and a full table fails every open (hostio_native.c alloc_slot). Pass: the
// handle high-water stays at or below 50% of that table (audit: <= 50% of
// MaxHandles, flat as P grows past the LRU size).
TB_GATE(tbgate_B2_20k_partitions_since_boot, kGateB2) {
    const uint32_t P = uint32_t(argInt("tb-b2-parts", 20000));
    const uint32_t perSec = uint32_t(argInt("tb-b2-rate", 1000));
    const uint64_t maxHandles = uint64_t(argInt("tb-b2-max-handles", 16384));
    const uint32_t threads = 4;
    Store s(false, 4, true);
    CountingIo cio(s.fs.get());
    s.cfg.io = &cio;
    s.cfg.idleCloseMs = 250;    // cool quickly: the gate is about what cooling keeps
    s.cfg.idleReclaimMs = 100;
    s.cfg.poolBytes = 256ull << 20;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids(P);
    for (uint32_t i = 0; i < P; i++) {
        pids[i] = s.partition("b2p" + std::to_string(i), ommType());
        REQUIRE(pids[i] != 0);
    }
    const uint64_t afterReg = cio.highWater();
    std::atomic<uint32_t> hung{0};
    const uint64_t t0 = monoNs();
    std::vector<std::thread> ts;
    for (uint32_t t = 0; t < threads; t++)
        ts.emplace_back([&, t] {
            const auto attr = buildRecordAttr("12D3KooWb2peer", "prov", "src", "batch-1");
            for (uint32_t i = t; i < P && !hung.load(); i += threads) {
                // Pace: partition i starts no earlier than i / perSec seconds in.
                const uint64_t due = t0 + uint64_t(i) * kSec / perSec;
                const uint64_t now = monoNs();
                if (due > now) sleepNs(due - now);
                Producer prod(s.e.get(), pids[i]);
                uint64_t last = 0;
                for (uint32_t k = 0; k < 2; k++)
                    last = send(s.e.get(), prod,
                                ommRecord(i * 2 + k + 1, "2026-002A", tb::isoSecond(i * 2 + k), 15.0 + k, 64), attr,
                                1790000000000ll + i);
                if (!last || prod.waitAcked(last, 30 * kSec) != 0) hung.store(pids[i]);
            }
        });
    for (auto& th : ts) th.join();
    if (hung.load()) {
        TB_HANG("partition " + std::to_string(hung.load()) + ": acks not released in 30 s");
        return;
    }
    const double ingestS = double(monoNs() - t0) / 1e9;
    // Every partition idles past its cool.
    sleepNs(uint64_t(s.cfg.idleCloseMs) * 4 * 1000000ull + kSec);
    const uint64_t hw = cio.highWater(), after = cio.openNow();
    report("b2_partitions_written", double(P), "n");
    report("b2_ingest_seconds", ingestS, "s");
    report("b2_handles_high_water_after_registration", double(afterReg), "handles");
    report("b2_handles_high_water", double(hw), "handles");
    report("b2_handles_open_after_cool", double(after), "handles");
    report("b2_handles_per_partition_written", double(hw) / double(P), "handles");
    report("b2_handle_budget", double(maxHandles / 2), "handles");
    CHECK(hw <= maxHandles / 2);
    s.close();
}

// ---- M1 / M2 ------------------------------------------------------------------------------
// 100,000 lifetime registrations in rounds of 10,000: each round registers its
// partitions, writes one record to every 50th (staying far below B2's handle
// ceiling), then drops them all with registry DROP frames (format.h
// kRegDrop, the only drop the format has; the engine has no call for it) and
// reopens, so at most 10,000 partitions are ever live. Pass: all 100,000
// registrations succeed (M1: pids are never reused, so the cap is lifetime),
// and registering a round costs at most 1 KiB of heap per partition (M2: a
// cold core, the ring and active state allocated on warm).
TB_GATE(tbgate_M1_100k_registrations_with_drops, kGateM1) {
    const uint64_t total = uint64_t(argInt("tb-m1-registrations", 100000));
    const uint64_t perRound = uint64_t(argInt("tb-m1-round", 10000));
    const uint64_t budget = uint64_t(argInt("tb-m1-bytes-per-partition", 1024));
    Store s(false, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const auto attr = buildRecordAttr("12D3KooWm1peer", "prov", "src", "batch-1");
    uint64_t registered = 0, failAt = 0, worstHeap = 0, worstDesc = 0;
    int32_t failRc = 0;
    double reopenMsMax = 0;
    for (uint32_t round = 0; registered < total && !failAt; round++) {
        if (round) {
            s.close();
            const uint64_t o0 = monoNs();
            REQUIRE(s.open() == 0);
            reopenMsMax = std::max(reopenMsMax, double(monoNs() - o0) / 1e6);
        }
        // The fixture's file system is in memory: its bytes are heap too and
        // are subtracted (usedBytes), so the delta is the engine's.
        const uint64_t h0 = tb::heapInUse(), u0 = s.fs->usedBytes();
        const uint64_t d0 = s.e->stats().descriptorBytes;
        std::vector<uint32_t> pids;
        for (uint64_t i = 0; i < perRound && registered < total; i++) {
            const std::string peer = "12D3KooWm1r" + std::to_string(round) + "p" + std::to_string(i);
            uint32_t pid = 0;
            const int32_t rc = s.e->registerPartition(reinterpret_cast<const uint8_t*>(peer.data()), peer.size(),
                                                      ommType().fid, &pid);
            if (rc < 0) {
                failRc = rc;
                failAt = registered + 1;
                break;
            }
            registered++;
            pids.push_back(pid);
        }
        if (!pids.empty()) {
            const uint64_t h1 = tb::heapInUse(), u1 = s.fs->usedBytes();
            const uint64_t d1 = s.e->stats().descriptorBytes;
            const int64_t engine = int64_t(h1 - h0) - int64_t(u1 - u0);
            worstHeap = std::max<uint64_t>(worstHeap, engine > 0 ? uint64_t(engine) / pids.size() : 0);
            worstDesc = std::max<uint64_t>(worstDesc, d1 > d0 ? (d1 - d0) / pids.size() : 0);
        }
        for (size_t i = 0; i < pids.size(); i += 50) {
            Producer prod(s.e.get(), pids[i]);
            const uint64_t last = send(s.e.get(), prod, ommRecord(uint32_t(i + 1), "2026-003A", tb::isoSecond(i), 15.0, 64),
                                       attr, 1790000000000ll + int64_t(i));
            REQUIRE(last != 0);
            if (prod.waitAcked(last, 30 * kSec) != 0) {
                TB_HANG("partition " + std::to_string(pids[i]) + ": ack not released in 30 s");
                return;
            }
        }
        for (uint32_t pid : pids) REQUIRE(s.e->registry().writeFrame(kRegDrop, u32Payload(pid)) == 0);
        REQUIRE(s.e->registry().syncLog() == 0);
    }
    s.close();
    const uint64_t o0 = monoNs();
    REQUIRE(s.open() == 0);
    const double finalOpenMs = double(monoNs() - o0) / 1e6;
    report("m1_registrations_attempted", double(total), "n");
    report("m1_registrations_succeeded", double(registered), "n");
    if (failAt) {
        report("m1_first_failed_registration", double(failAt), "n");
        report("m1_first_failure_status", double(failRc), "status");
    }
    report("m1_live_partitions_after_final_reopen", double(s.e->partitionCount()), "n");
    report("m1_reopen_ms_max", reopenMsMax, "ms");
    report("m1_final_open_ms", finalOpenMs, "ms");
    report("m2_heap_per_registered_partition", double(worstHeap), "B");
    report("m2_descriptor_bytes_per_registered_partition", double(worstDesc), "B");
    report("m2_budget_per_partition", double(budget), "B");
    CHECK_EQ(registered, total);
#if defined(__wasm__)
    CHECK(worstDesc <= budget);  // no allocator statistics in the guest
#else
    CHECK(worstHeap <= budget);
#endif
    s.close();
}

// ---- B4 ---------------------------------------------------------------------------------
// One partition grows to 5,000 sealed segments (16 records a segment, so the
// segment count is what grows, not bytes). Compaction's candidate scan stays
// on (autoCompact); coalescing is off (compactSmallBytes = 0) so S keeps
// growing. At each checkpoint S = 500, 1,000, 2,000, 3,000, 4,000, 5,000 the
// window since the previous one is measured: records a second, writer
// maintenance-step p99, commit-round p99 and maintenance time per merge.
// Pass (audit: maintenance p99 < 10 ms at S = 6k; rate at 1 TB >= 80% of the
// rate at 10% fill): in the last window both p99s are under 10 ms and the rate
// is at least 80% of the first window's. --tb-b4-minutes bounds the run: a
// partition that cannot reach 5,000 segments in time is a HANG.
TB_GATE(tbgate_B4_5k_segments_one_partition, kGateB4) {
    const uint64_t targetS = uint64_t(argInt("tb-b4-segments", 5000));
    const uint32_t perSeg = uint32_t(argInt("tb-b4-seg-records", 16));
    const uint64_t deadline = uint64_t(argInt("tb-b4-minutes", 20)) * 60 * kSec;
    Store s(false, 1, true);
    s.cfg.sealRecords = perSeg;
    s.cfg.compactSmallBytes = 0;
    s.cfg.lockStats = true;  // commitHist and maintHist record only with it (writer_pool.cpp)
    s.cfg.poolBytes = 256ull << 20;
    s.cfg.reclaimGraceMs = 1000;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("b4big", ommType());
    REQUIRE(pid != 0);
    Producer prod(s.e.get(), pid);
    const auto attr = buildRecordAttr("12D3KooWb4peer", "prov", "src", "batch-1");
    std::vector<uint64_t> marks = {500, 1000, 2000, 3000, 4000, 5000};
    marks.erase(std::remove_if(marks.begin(), marks.end(), [&](uint64_t m) { return m > targetS; }), marks.end());
    if (marks.empty() || marks.back() != targetS) marks.push_back(targetS);
    HistWindow maint, commit;
    maint.take(s.e->maintHist());
    commit.take(s.e->commitHist());
    struct Win {
        uint64_t s, records;
        double secs, rate, maintP99, commitP99, maintPerMerge;
    };
    std::vector<Win> wins;
    const uint64_t t0 = monoNs();
    uint64_t n = 0, last = 0, winStart = t0, winRecords = 0, winMerges = 0;
    size_t mi = 0;
    while (mi < marks.size()) {
        if (monoNs() - t0 > deadline) {
            report("b4_segments_reached", double(s.e->stats().seals), "segments");
            TB_HANG("S = " + std::to_string(s.e->stats().seals) + " sealed segments after " +
                    std::to_string(deadline / (60 * kSec)) + " min (target " + std::to_string(targetS) + ")");
            break;
        }
        const uint32_t chunk = perSeg * 8;
        for (uint32_t k = 0; k < chunk; k++, n++)
            last = send(s.e.get(), prod, ommRecord(uint32_t(n % 60000 + 1), "2026-004A", tb::isoSecond(n), 15.0 + double(n) * 1e-7, 400),
                        attr, 1790000000000ll + int64_t(n));
        REQUIRE(last != 0);
        if (prod.waitAcked(last, 120 * kSec) != 0) {
            TB_HANG("a chunk was not acknowledged in 120 s at S = " + std::to_string(s.e->stats().seals));
            break;
        }
        winRecords += chunk;
        const EngineStats st = s.e->stats();
        if (st.seals >= marks[mi]) {
            const uint64_t now = monoNs();
            maint.take(s.e->maintHist());
            commit.take(s.e->commitHist());
            Win w;
            w.s = st.seals;
            w.records = winRecords;
            w.secs = double(now - winStart) / 1e9;
            w.rate = double(winRecords) / w.secs;
            w.maintP99 = double(maint.percentileNs(0.99)) / 1e6;
            w.commitP99 = double(commit.percentileNs(0.99)) / 1e6;
            const uint64_t dm = st.merges - winMerges;
            w.maintPerMerge = dm ? maint.sumNs() / 1e6 / double(dm) : 0;
            wins.push_back(w);
            std::printf("  B4 S=%llu records=%llu window=%.1fs rate=%.0f rec/s maint_p99=%.2fms commit_p99=%.2fms "
                        "maint_per_merge=%.2fms\n",
                        (unsigned long long)w.s, (unsigned long long)w.records, w.secs, w.rate, w.maintP99,
                        w.commitP99, w.maintPerMerge);
            std::fflush(stdout);
            winStart = now;
            winRecords = 0;
            winMerges = st.merges;
            mi++;
        }
    }
    for (const Win& w : wins) {
        const std::string k = "b4_S" + std::to_string(w.s);
        report((k + "_rate").c_str(), w.rate, "rec/s");
        report((k + "_maint_p99").c_str(), w.maintP99, "ms");
        report((k + "_commit_p99").c_str(), w.commitP99, "ms");
        report((k + "_maint_per_merge").c_str(), w.maintPerMerge, "ms");
    }
    if (tb::gateHangs()) return;
    REQUIRE(wins.size() >= 2);
    const Win& first = wins.front();
    const Win& lastW = wins.back();
    report("b4_rate_ratio_last_over_first", lastW.rate / first.rate, "x");
    CHECK(lastW.maintP99 < 10.0);
    CHECK(lastW.commitP99 < 10.0);
    CHECK(lastW.rate >= 0.8 * first.rate);
    s.close();
}
