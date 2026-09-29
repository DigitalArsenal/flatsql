// Hot-partition splitting (T3b, design §12, A26; PARTITION-STORE.md §32):
// stage-1 helpers prepare a split partition's entries, the owner stays the
// only appender.
#include <algorithm>
#include <atomic>
#include <map>
#include <random>
#include <set>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {
std::string ep(int i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-08-%02dT%02d:%02d:%02dZ", 1 + (i / 3600) % 28, (i / 60) % 24, i % 60, i % 60);
    return b;
}
bool waitHot(Engine* e, uint32_t pid, bool want, uint64_t timeoutNs) {
    const uint64_t until = monoNs() + timeoutNs;
    while (e->isHot(pid) != want && monoNs() < until) sleepNs(1000000);
    return e->isHot(pid) == want;
}

// One CAT workload: supersedes (a changed record per object), exact resends
// (dedupe), re-tags (RETAG rows), and type-level kills by CID.
struct Workload {
    struct Op {
        bool tomb = false;
        std::vector<uint8_t> frame;
        std::vector<uint8_t> attr;
        uint8_t cid[kCidLen];
    };
    std::vector<Op> ops;
    explicit Workload(int n, uint64_t seed) {
        std::mt19937_64 rng(seed);
        std::vector<std::vector<uint8_t>> sentCids;
        for (int i = 0; i < n; i++) {
            Op op;
            if (i % 53 == 52 && !sentCids.empty()) {
                op.tomb = true;
                std::memcpy(op.cid, sentCids[rng() % sentCids.size()].data(), kCidLen);
            } else {
                const uint32_t obj = uint32_t(rng() % 1500);
                const int version = int(rng() % 3);
                const int tag = int(rng() % 4);
                op.frame = catRecord(50000 + obj, "O-" + std::to_string(obj), "https://cat/x",
                                     "C" + std::to_string(obj), "N" + std::to_string(obj) + "v" + std::to_string(version),
                                     1 + int(obj % 3));
                op.attr = buildRecordAttr("hot", "prov" + std::to_string(tag % 2), "src" + std::to_string(tag),
                                          "b" + std::to_string(tag));
                frameCid(op.frame, op.cid);
                sentCids.emplace_back(op.cid, op.cid + kCidLen);
            }
            ops.push_back(std::move(op));
        }
    }
    // Sends everything in order on one producer; returns the last rseq.
    uint64_t send(Engine* e, uint32_t pid) const {
        Producer prod(e, pid);
        uint64_t last = 0;
        for (size_t i = 0; i < ops.size(); i++) {
            const Op& op = ops[i];
            uint64_t r = 0;
            if (op.tomb) prod.enqueue(kEntTombCid, 0, int64_t(i), op.cid, nullptr, 0, nullptr, 0, &r);
            else r = pst::send(e, prod, op.frame, op.attr, int64_t(i));
            if (r) last = r;
        }
        CHECK_EQ(prod.waitAcked(last, 60000000000ull), 0);
        return last;
    }
};

// Rows equal field by field, except where a row lives (offsets, the segment
// and ATTR_IN_M follow commit sizes and merge progress, which a split changes).
bool sameRow(const RecRow& a, const RecRow& b) {
    const uint8_t mask = uint8_t(~kRowAttrInM);
    return a.pseq == b.pseq && a.len == b.len && a.kind == b.kind && (a.flags & mask) == (b.flags & mask) &&
           std::memcmp(a.fid, b.fid, 4) == 0 && a.dataCrc == b.dataCrc && a.epochMs == b.epochMs &&
           a.arrivalMs == b.arrivalMs && a.targetPseq == b.targetPseq && a.attrLen == b.attrLen &&
           a.laneId == b.laneId && std::memcmp(a.cid, b.cid, kCidLen) == 0 && a.supersedeHash == b.supersedeHash &&
           a.tagHash == b.tagHash && a.aux == b.aux;
}

std::vector<std::vector<std::string>> query(Reader& r, const std::string& sql) {
    const Rows rows = r.q(sql);
    CHECK_EQ(rows.status, 0);
    if (rows.status) std::fprintf(stderr, "  %s: %s\n", sql.c_str(), rows.error.c_str());
    std::vector<std::vector<std::string>> out;
    for (const auto& row : rows.rows) {
        std::vector<std::string> cells;
        for (const auto& c : row) cells.push_back(c.s + "|" + std::to_string(c.i));
        out.push_back(std::move(cells));
    }
    return out;
}
}  // namespace

// T3 #5 (correctness half): a split partition stores exactly what an
// unsplit one stores for the same input, one appending thread per ownership
// interval, and SQL results equal the unsplit reference.
PS_TEST(hot_split_equals_unsplit_reference_T3_5) {
    const Workload wl(int(argInt("hot-ops", 12000)), 20260929);
    // One injected wall clock: tombstones take their epoch from it.
    auto clock = [](void*) -> int64_t { return 1790000000000ll; };
    Store ref(true, 5, true);
    ref.cfg.clockMs = clock;
    REQUIRE(ref.open() == 0);
    ref.registerTypes({&catType()});
    const uint32_t rp = ref.partition("hot", catType());
    wl.send(ref.e.get(), rp);
    CHECK(waitLabeledEngine(ref.e.get(), {rp}, 20000000000ull));

    Store sp(true, 5, true);
    sp.cfg.clockMs = clock;
    sp.cfg.audit = true;
    REQUIRE(sp.open() == 0);
    sp.registerTypes({&catType()});
    const uint32_t hp = sp.partition("hot", catType());
    REQUIRE(sp.e->setHotSplit(hp, true) == 0);
    REQUIRE(waitHot(sp.e.get(), hp, true, 5000000000ull));
    wl.send(sp.e.get(), hp);
    CHECK(waitLabeledEngine(sp.e.get(), {hp}, 20000000000ull));
    const EngineStats st = sp.e->stats();
    report("hot_split_ref_prepared", double(st.prepPrepared), "entries");
    report("hot_split_ref_used", double(st.prepUsed), "entries");
    report("hot_split_ref_hinted", double(st.prepHinted), "entries");
    CHECK(st.prepUsed > 0);  // entries really came prepared
    CHECK(sp.e->isHot(hp));
    // One appender: every commit of the partition by its owner's one thread.
    const auto audit = sp.e->auditLog();
    std::set<std::pair<uint32_t, uint32_t>> appenders;
    uint64_t next = 1;
    std::vector<AuditRecord> mine;
    for (const auto& a : audit)
        if (a.pid == hp) mine.push_back(a);
    std::sort(mine.begin(), mine.end(), [](const AuditRecord& a, const AuditRecord& b) { return a.firstPseq < b.firstPseq; });
    for (const auto& a : mine) {
        appenders.insert({a.writer, a.osTid});
        CHECK_EQ(a.firstPseq, next);  // gap-free
        next = a.lastPseq + 1;
    }
    CHECK_EQ(appenders.size(), size_t(1));
    // SQL at type level against the unsplit reference.
    {
        Reader rr(ref, LaneClass::Bulk, 1), rs(sp, LaneClass::Bulk, 1);
        REQUIRE(rr.inst && rs.inst);
        for (const char* sql : {"SELECT _cid, _epoch, _provider, _batch, OBJECT_NAME FROM CAT ORDER BY _cid",
                                "SELECT _cid, OBJECT_NAME FROM CAT_current ORDER BY _cid",
                                "SELECT count(*), sum(_len) FROM sds_p_hot__CAT",
                                "SELECT _cid FROM CAT WHERE _provider = 'prov1' ORDER BY _cid"}) {
            const auto a = query(rr, sql), b = query(rs, sql);
            CHECK(a == b);
            if (a != b) std::fprintf(stderr, "  differs: %s (%zu vs %zu rows)\n", sql, a.size(), b.size());
        }
    }
    ref.close();
    sp.close();
    // Rows, frames and attributes identical; pseqs gap-free.
    Inspector ia(ref.fs.get(), ref.root), ib(sp.fs.get(), sp.root);
    const PartView va = ia.partition(rp), vb = ib.partition(hp);
    REQUIRE(va.err.empty() && vb.err.empty());
    CHECK_EQ(va.rows.size(), vb.rows.size());
    size_t diff = 0;
    for (size_t i = 0; i < va.rows.size() && i < vb.rows.size(); i++) {
        CHECK_EQ(vb.rows[i].pseq, uint64_t(i + 1));
        if (!sameRow(va.rows[i], vb.rows[i]) || ia.frame(rp, va.rows[i]) != ib.frame(hp, vb.rows[i]) ||
            ia.attr(rp, va.rows[i]) != ib.attr(hp, vb.rows[i])) {
            if (diff < 3) {
                const RecRow &a = va.rows[i], &b = vb.rows[i];
                std::fprintf(stderr, "  row %zu: kind %u/%u flags %x/%x len %u/%u crc %x/%x epoch %lld/%lld target %llu/%llu lane %u/%u sh %llx/%llx th %llx/%llx aux %u/%u cid %d frame %d attr %d\n",
                             i, a.kind, b.kind, a.flags, b.flags, a.len, b.len, a.dataCrc, b.dataCrc, (long long)a.epochMs,
                             (long long)b.epochMs, (unsigned long long)a.targetPseq, (unsigned long long)b.targetPseq,
                             a.laneId, b.laneId, (unsigned long long)a.supersedeHash, (unsigned long long)b.supersedeHash,
                             (unsigned long long)a.tagHash, (unsigned long long)b.tagHash, a.aux, b.aux,
                             std::memcmp(a.cid, b.cid, kCidLen) == 0, ia.frame(rp, a) == ib.frame(hp, b),
                             ia.attr(rp, a) == ib.attr(hp, b));
            }
            diff++;
        }
    }
    CHECK_EQ(diff, size_t(0));
    report("hot_split_ref_rows", double(vb.rows.size()), "rows");
}

// A26: rebalances while helpers are stopped for 100 ms in the middle of a
// claim, with other partitions ingesting (ring slabs recycle across
// partitions): 0 non-owner writes, 0 cross-partition slab corruption, every
// acked record intact.
static void hotSplitRebalanceRun(double seconds) {
    Store s(false, 5, true);
    s.cfg.audit = true;
    s.cfg.testPrepStallNs = 100000000;
    s.cfg.testPrepStallEvery = 97;
    s.cfg.poolBytes = 48ull << 20;  // slabs recycle quickly
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &mpeType()});
    std::vector<uint32_t> pids;
    for (uint32_t i = 0; i < 6; i++) pids.push_back(s.partition("rb" + std::to_string(i), (i % 2) ? mpeType() : ommType()));
    const uint32_t hot = pids[0];
    REQUIRE(s.e->setHotSplit(hot, true) == 0);
    REQUIRE(waitHot(s.e.get(), hot, true, 5000000000ull));
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> moves{0};
    std::vector<std::set<std::string>> sentCids(pids.size());
    std::vector<std::thread> producers;
    for (uint32_t k = 0; k < pids.size(); k++) {
        producers.emplace_back([&, k] {
            Producer prod(s.e.get(), pids[k]);
            const auto attr = buildRecordAttr("rb", "prov", "src", "b");
            uint64_t last = 0;
            for (uint64_t n = 0; !stop.load(); n++) {
                const auto f = (k % 2) ? mpeRecord("E" + std::to_string(n), 1.7e9 + double(n), double(k))
                                       : ommRecord(uint32_t(n % 90000), "O" + std::to_string(k), ep(int(n % 80000)),
                                                   double(k) + double(n) * 1e-6, 64 + (n % 5) * 40);
                const uint64_t r = send(s.e.get(), prod, f, attr, int64_t(n));
                if (r) {
                    last = r;
                    uint8_t cid[kCidLen];
                    frameCid(f, cid);
                    sentCids[k].insert(std::string(reinterpret_cast<const char*>(cid), kCidLen));
                }
                if (k != 0 && (n & 63) == 0) sleepNs(200000);  // the hot one floods, the others trickle
            }
            CHECK_EQ(prod.waitAcked(last, 60000000000ull), 0);
        });
    }
    std::thread rebalancer([&] {
        std::mt19937 rng(11);
        while (!stop.load()) {
            const uint32_t pid = (rng() % 2) ? hot : pids[rng() % pids.size()];
            if (s.e->rebalance(pid, uint8_t(rng() % 5)) == 0) moves.fetch_add(1);
            sleepNs(50000000);
        }
    });
    sleepNs(uint64_t(seconds * 1e9));
    stop.store(true);
    for (auto& t : producers) t.join();
    rebalancer.join();
    const EngineStats es = s.e->stats();
    const auto audit = s.e->auditLog();
    report("hot_split_a26_moves", double(moves.load()), "moves");
    report("hot_split_a26_helper_stalls_100ms", double(es.prepHelperStalls), "stalls");
    report("hot_split_a26_prepared", double(es.prepPrepared), "entries");
    report("hot_split_a26_stolen", double(es.prepStolen), "entries");
    report("hot_split_a26_wasted", double(es.prepWasted), "entries");
    report("hot_split_a26_non_owner_helper_writes", double(es.mergeNotOwner), "writes");
    CHECK(es.prepHelperStalls > 0);
    CHECK(es.prepPrepared > 0);
    CHECK(moves.load() > 0);
    CHECK_EQ(es.mergeNotOwner, uint64_t(0));
    // One (writer, thread) per ownership epoch, contiguous pseqs, no overlap.
    std::map<uint32_t, std::vector<AuditRecord>> per;
    for (const auto& a : audit) per[a.pid].push_back(a);
    int violations = 0;
    uint64_t intervals = 0;
    for (auto& kv : per) {
        auto& v = kv.second;
        std::sort(v.begin(), v.end(), [](const AuditRecord& a, const AuditRecord& b) { return a.firstPseq < b.firstPseq; });
        std::map<uint32_t, std::pair<uint32_t, uint32_t>> owner;
        for (size_t i = 0; i < v.size(); i++) {
            if (i > 0 && (v[i].firstPseq != v[i - 1].lastPseq + 1 || v[i].epoch < v[i - 1].epoch ||
                          v[i].startNs < v[i - 1].endNs))
                violations++;
            auto it = owner.find(v[i].epoch);
            if (it == owner.end()) owner[v[i].epoch] = {v[i].writer, v[i].osTid};
            else if (it->second != std::make_pair(uint32_t(v[i].writer), v[i].osTid)) violations++;
        }
        intervals += owner.size();
    }
    report("hot_split_a26_ownership_intervals", double(intervals), "intervals");
    CHECK_EQ(violations, 0);
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    uint64_t corrupt = 0, rows = 0;
    for (uint32_t k = 0; k < pids.size(); k++) {
        const PartView v = ins.partition(pids[k]);
        REQUIRE(v.err.empty());
        std::set<std::string> got;
        for (const RecRow& r : v.rows) {
            if (r.kind != kRowPut) continue;
            rows++;
            const auto f = ins.frame(pids[k], r);
            uint8_t cid[kCidLen];
            if (f.size() < 4) {
                corrupt++;
                continue;
            }
            computeCid(f.data() + 4, f.size() - 4, cid);
            // The stored bytes hash to the row's CID and match its CRC: no
            // frame came from another partition's recycled slab.
            if (std::memcmp(cid, r.cid, kCidLen) != 0 || crc32c(f.data(), f.size()) != r.dataCrc) corrupt++;
            got.insert(std::string(reinterpret_cast<const char*>(r.cid), kCidLen));
        }
        CHECK(got == sentCids[k]);
    }
    report("hot_split_a26_rows_checked", double(rows), "rows");
    CHECK_EQ(corrupt, uint64_t(0));
    s.close();
}
#if defined(__wasm__)
constexpr long kA26Seconds = 3;       // the in-memory host keeps every byte in a 4 GiB space
constexpr long kTputRecords = 100000;
#else
constexpr long kA26Seconds = 6;
constexpr long kTputRecords = 200000;
#endif
PS_TEST(hot_split_rebalance_stalled_helpers_A26) { hotSplitRebalanceRun(double(argInt("hot-a26-seconds", kA26Seconds))); }
PS_SLOW_TEST(hot_split_rebalance_stalled_helpers_A26_full) { hotSplitRebalanceRun(600.0); }

// The §12 trigger: a backlog over half the ring for the configured time, on
// a worker serving that partition alone, splits; a drained ring merges back.
PS_TEST(hot_split_trigger_splits_and_merges_back) {
    Store s(false, 3, true);
    s.cfg.hotSplitAfterMs = 200;
    s.cfg.hotUnsplitAfterMs = 300;
    s.cfg.defaultRingCap = 1u << 20;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("trig", ommType());
    std::atomic<bool> stop{false};
    std::atomic<bool> sawHot{false};
    std::thread prodT([&] {
        Producer prod(s.e.get(), pid);
        const auto attr = buildRecordAttr("trig", "prov", "src", "b");
        uint64_t last = 0;
        for (uint64_t n = 0; !stop.load(); n++) {
            const uint64_t r = send(s.e.get(), prod, ommRecord(uint32_t(n), "T", ep(int(n % 80000)), double(n), 300), attr,
                                    int64_t(n));
            if (r) last = r;
            if (s.e->isHot(pid)) sawHot.store(true);
        }
        CHECK_EQ(prod.waitAcked(last, 60000000000ull), 0);
    });
    const uint64_t until = monoNs() + 20000000000ull;
    while (!sawHot.load() && monoNs() < until) sleepNs(5000000);
    stop.store(true);
    prodT.join();
    CHECK(sawHot.load());
    CHECK(waitHot(s.e.get(), pid, false, 10000000000ull));
    const EngineStats st = s.e->stats();
    report("hot_split_trigger_splits", double(st.splits), "splits");
    report("hot_split_trigger_unsplits", double(st.unsplits), "unsplits");
    CHECK(st.splits >= 1 && st.unsplits >= 1);
    s.close();
}

// T3 #5 (throughput half): one partition at many times one writer's
// capacity (the ring stays full), the same engine unsplit and split with 4
// helpers. Enforced (>= 2.5x) only on a quiet box of 8+ usable hardware
// threads; reported everywhere (bench: --mode=hotsplit).
static void hotSplitThroughput(uint64_t records) {
    double rates[2] = {0, 0};
    for (int split = 0; split < 2; split++) {
        Store s(false, 5, true);
        s.cfg.hotSplit = false;
        s.cfg.poolBytes = 192ull << 20;
        s.cfg.arenaBytes = 24ull << 20;
        REQUIRE(s.open() == 0);
        s.registerTypes({&ommType()});
        const uint32_t pid = s.partition("tput", ommType());
        if (split) {
            REQUIRE(s.e->setHotSplit(pid, true) == 0);
            REQUIRE(waitHot(s.e.get(), pid, true, 5000000000ull));
        }
        // Frames and CIDs first (the router computes CIDs), then the timed run.
        std::vector<std::vector<uint8_t>> frames(records);
        std::vector<uint8_t> cids(records * kCidLen);
        for (uint64_t i = 0; i < records; i++) {
            frames[i] = ommRecord(uint32_t(i % 90000), "TP", "2026-09-01T00:00:00Z", 1.0 + double(i), 200);
            computeCid(frames[i].data() + 4, frames[i].size() - 4, cids.data() + i * kCidLen);
        }
        const auto attr = buildRecordAttr("tput", "prov", "src", "b1");
        Producer prod(s.e.get(), pid);
        const uint64_t t0 = monoNs();
        uint64_t last = 0;
        for (uint64_t i = 0; i < records; i++) {
            uint64_t r = 0;
            prod.enqueue(kEntRecord, kEntCidPresent, int64_t(i), cids.data() + i * kCidLen, attr.data(),
                         uint32_t(attr.size()), frames[i].data(), uint32_t(frames[i].size()), &r, true);
            last = r;
        }
        CHECK_EQ(prod.waitAcked(last, 120000000000ull), 0);
        rates[split] = double(records) / (double(monoNs() - t0) / 1e9);
        s.close();
    }
    const double ratio = rates[1] / rates[0];
    report("hot_split_unsplit_records_per_s", rates[0], "records/s");
    report("hot_split_split_records_per_s", rates[1], "records/s");
    report("hot_split_ratio_4_helpers", ratio, "x");
    unsigned threads = 0;
    double load = 0;
    std::string why;
    if (latencyBoxQuiet(&threads, &load, &why)) CHECK(ratio >= 2.5);
    else std::printf("  ratio %.3f reported, not enforced: %s\n", ratio, why.c_str());
}
PS_TEST(hot_split_throughput_T3_5) { hotSplitThroughput(uint64_t(argInt("hot-records", kTputRecords))); }
PS_SLOW_TEST(hot_split_throughput_T3_5_full) { hotSplitThroughput(uint64_t(argInt("hot-records", 2000000))); }
