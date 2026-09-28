// Quota and the full disk (T3 acceptance #3, #6; design §13, A13, owner
// decision §22.4-3: arrival order, 0.85 low-water mark).
//
//   - T3 #3: a cap at 80% of the store's bytes: the planner evicts whole
//     sealed segments oldest arrival first (in each partition the evicted
//     records are exactly its oldest ones), never a supersede-lane head
//     (every CAT object's latest version survives), usage is at or below the
//     cap within 3 planning passes, and every eviction step (a bounded
//     TOMB_RANGE slice) takes at most 10 ms;
//   - T3 #6: the device fills under ingest: the engine releases its ballast,
//     stops consuming records (nothing is acked meanwhile), evicts, compacts,
//     reclaims, recreates the ballast and resumes, with no operator action
//     and 0 acked records lost (in each partition the surviving acked
//     records are exactly its newest ones: no holes).
#include <algorithm>
#include <atomic>
#include <deque>
#include <map>
#include <random>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {

struct QRec {
    std::vector<uint8_t> frame;
    int64_t arrival = 0;
    uint64_t rseq = 0;
    bool acked = false;
};

// Live PUTs of a partition by cid (Inspector: the files, not a reader).
std::map<std::string, uint64_t> livePuts(Store& s, uint32_t pid) {
    std::map<std::string, uint64_t> out;
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(pid);
    if (!v.ok) return out;
    const Recount rc = recount(v);
    for (const RecRow& r : v.rows)
        if (r.kind == kRowPut && !rc.dead.count(r.pseq))
            out[std::string(reinterpret_cast<const char*>(r.cid), kCidLen)] = r.pseq;
    return out;
}

std::string cidKey(const std::vector<uint8_t>& frame) {
    uint8_t cid[kCidLen];
    frameCid(frame, cid);
    return std::string(reinterpret_cast<const char*>(cid), kCidLen);
}

// The store's bytes (§13): the partitions and the type logs.
uint64_t storeBytes(Store& s, const std::vector<uint32_t>& pids, const std::vector<const TestType*>& types) {
    uint64_t b = 0;
    for (uint32_t pid : pids) b += checkPartitionDir(s.fs.get(), s.fs.get(), s.root, pid).bytes;
    for (const TestType* t : types) b += checkTypeDir(s.fs.get(), s.fs.get(), s.root, t->fid).bytes;
    return b;
}

// Waits until background work (merges, compactions, reclamation) has
// stopped changing the store: no compaction in flight and the same bytes on
// disk for a second, equal to the engine's disk_bytes.
uint64_t settle(Store& s, const std::vector<uint32_t>& pids, const std::vector<const TestType*>& types,
              uint64_t timeoutNs) {
    uint64_t last = 0;
    int same = 0;
    for (const uint64_t t0 = monoNs(); monoNs() - t0 < timeoutNs; sleepNs(50000000)) {
        const uint64_t b = storeBytes(s, pids, types);
        uint64_t d = 0;
        for (uint32_t pid : pids) d += s.e->partitionDiskBytes(pid);
        for (const TestType* t : types) d += s.e->typeDiskBytesOf(t->fid);
        if (b == last && b == d && s.e->stats().compactInFlight == 0) {
            if (++same >= 20) return b;
        } else {
            same = 0;
        }
        last = b;
    }
    return last;
}

}  // namespace

// ---------------------------------------------------------------------------
PS_TEST(quota_arrival_order_heads_spared_T3_3) {
    Store s(true, 2, true);
    s.cfg.sealBytes = 64u << 10;
    s.cfg.sealAgeMs = 50;
    s.cfg.mergeL0Blocks = 4;
    s.cfg.mergeMinL0Bytes = 0;
    s.cfg.reclaimGraceMs = 1;
    s.cfg.quotaIntervalMs = 20;
    // Several segments per partition once small ones coalesce: eviction is by
    // whole segment (A13), so its arrival order shows at this granularity.
    s.cfg.compactMaxOutputBytes = 512u << 10;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &catType()});
    const int nOmm = 3;
    std::vector<uint32_t> pids;
    for (int p = 0; p < nOmm; p++) pids.push_back(s.partition("q" + std::to_string(p), ommType()));
    const uint32_t catPid = s.partition("qcat", catType());
    pids.push_back(catPid);
    std::vector<std::vector<QRec>> recs(nOmm);
    const int perPart = int(argInt("records", 3000));
    const int objects = 300;
    std::vector<std::vector<uint8_t>> catLatest(objects);
    int64_t arrival = 1780000000000ll;
    {
        std::vector<std::unique_ptr<Producer>> prods;
        for (uint32_t pid : pids) prods.emplace_back(new Producer(s.e.get(), pid));
        std::vector<uint64_t> last(pids.size(), 0);
        std::mt19937_64 rng(5);
        // Interleaved in arrival order; CAT objects are updated along the way
        // (older versions die by supersede; the latest is each lane's head).
        for (int i = 0; i < perPart; i++) {
            for (int p = 0; p < nOmm; p++) {
                QRec r;
                r.frame = ommRecord(uint32_t(p * 1000000 + i + 1), "Q" + std::to_string(p) + "-" + std::to_string(i),
                                    "2026-06-01T00:00:00Z", double(i), 200);
                r.arrival = ++arrival;
                r.rseq = send(s.e.get(), *prods[size_t(p)], r.frame, buildRecordAttr("q", "prov", "src", "b1"), r.arrival);
                last[size_t(p)] = r.rseq;
                recs[size_t(p)].push_back(std::move(r));
            }
            if (i % 5 == 0) {
                const int o = int(rng() % objects);
                auto f = catRecord(uint32_t(o + 1), "CO" + std::to_string(o), "", "",
                                   "CAT-" + std::to_string(o) + "-" + std::to_string(i));
                last.back() = send(s.e.get(), *prods.back(), f, buildRecordAttr("q", "prov", "catsrc", "b1"), ++arrival);
                catLatest[size_t(o)] = std::move(f);
            }
        }
        for (size_t k = 0; k < pids.size(); k++)
            if (last[k]) REQUIRE(prods[k]->waitAcked(last[k], 60000000000ull) == 0);
    }
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 60000000000ull));
    // Seals by age, merges and compactions first: the cap applies to a store
    // at rest.
    const std::vector<const TestType*> types = {&ommType(), &catType()};
    const uint64_t usage0 = settle(s, pids, types, 60000000000ull);
    const uint64_t cap = usage0 * 8 / 10;
    s.e->setQuota(cap);
    // Disk at or below the cap: planner passes until then.
    uint64_t passesAtCap = 0;
    bool underCap = false;
    for (const uint64_t t0 = monoNs(); monoNs() - t0 < 60000000000ull; sleepNs(5000000)) {
        const QuotaStats q = s.e->quotaStats();
        if (storeBytes(s, pids, types) <= cap && q.passes) {
            passesAtCap = q.passes;
            underCap = true;
            break;
        }
    }
    const QuotaStats qs = s.e->quotaStats();
    const uint64_t usage1 = storeBytes(s, pids, types);
    // Arrival order within each partition: the evicted records are exactly
    // its oldest ones (no survivor is older than an evicted record).
    uint64_t evicted = 0, olderSurvivorsGlobal = 0;
    int64_t newestEvicted = INT64_MIN;
    std::vector<std::map<std::string, uint64_t>> live;
    for (int p = 0; p < nOmm; p++) live.push_back(livePuts(s, pids[size_t(p)]));
    for (int p = 0; p < nOmm; p++) {
        bool seenLive = false;
        int holes = 0;
        for (const QRec& r : recs[size_t(p)]) {
            const bool isLive = live[size_t(p)].count(cidKey(r.frame)) != 0;
            if (isLive) {
                seenLive = true;
            } else {
                evicted++;
                newestEvicted = std::max(newestEvicted, r.arrival);
                if (seenLive) holes++;
            }
        }
        CHECK_EQ(holes, 0);
    }
    for (int p = 0; p < nOmm; p++)
        for (const QRec& r : recs[size_t(p)])
            if (live[size_t(p)].count(cidKey(r.frame)) && r.arrival < newestEvicted) olderSurvivorsGlobal++;
    // Supersede-lane heads are never evicted.
    const auto catLive = livePuts(s, catPid);
    int headsLost = 0, heads = 0;
    for (const auto& f : catLatest) {
        if (f.empty()) continue;
        heads++;
        if (!catLive.count(cidKey(f))) headsLost++;
    }
    const LockHist& eh = s.e->evictHist();
    report("quota_usage_before", double(usage0), "bytes");
    report("quota_cap", double(cap), "bytes");
    report("quota_usage_after", double(usage1), "bytes");
    report("quota_passes_to_cap", double(passesAtCap), "passes");
    report("quota_segments_evicted", double(qs.segmentsEvicted), "segments");
    report("quota_records_evicted", double(evicted), "records");
    report("quota_survivors_older_than_newest_evicted", double(olderSurvivorsGlobal), "records");
    report("quota_cat_heads", double(heads), "heads");
    report("quota_cat_heads_evicted", double(headsLost), "heads");
    report("quota_eviction_steps", double(eh.count.load()), "steps");
    report("quota_eviction_step_max_ms", double(eh.maxNs.load()) / 1e6, "ms");
    // (the histogram's p99 is its bucket's upper bound: never above the max)
    report("quota_eviction_step_p99_ms", double(std::min(eh.percentileNs(0.99), eh.maxNs.load())) / 1e6, "ms");
    CHECK(underCap);
    CHECK(passesAtCap <= 3);
    CHECK(evicted > 0);
    CHECK_EQ(headsLost, 0);
    CHECK(double(eh.maxNs.load()) / 1e6 <= 10.0);
    s.close();
}

// ---------------------------------------------------------------------------
PS_TEST(quota_disk_full_resumes_without_operator_T3_6) {
    Store s(true, 2, true);
    s.cfg.sealBytes = 64u << 10;
    s.cfg.sealAgeMs = 50;
    s.cfg.mergeL0Blocks = 4;
    s.cfg.mergeMinL0Bytes = 0;
    s.cfg.reclaimGraceMs = 1;
    s.cfg.quotaIntervalMs = 20;
    s.cfg.zeroFillStep = 0;
    s.cfg.typeMetaSegBytes = 256u << 10;  // the type meta log rotates (and retires) at this size
    s.cfg.ballastBytes = 1u << 20;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids;
    for (int p = 0; p < 2; p++) pids.push_back(s.partition("full" + std::to_string(p), ommType()));
    // A small device: ingest fills it.
    s.fs->setCapacity(s.fs->usedBytes() + (uint64_t(argInt("device_mib", 16)) << 20));
    std::vector<std::vector<QRec>> recs(pids.size());
    std::atomic<bool> stop{false};
    std::atomic<int64_t> arrival{1780000000000ll};
    std::atomic<uint64_t> ackedAfterEmergency{0}, ackedAfterRestore{0};
    std::vector<std::thread> prods;
    for (size_t p = 0; p < pids.size(); p++)
        prods.emplace_back([&, p] {
            Producer prod(s.e.get(), pids[p]);
            std::deque<size_t> inflight;
            uint64_t i = 0;
            while (!stop.load()) {
                QRec r;
                r.frame = ommRecord(uint32_t(p * 10000000 + i + 1), "F" + std::to_string(p) + "-" + std::to_string(i),
                                    "2026-06-01T00:00:00Z", double(i), 300);
                r.arrival = ++arrival;
                r.rseq = send(s.e.get(), prod, r.frame, buildRecordAttr("full", "prov", "src", "b1"), r.arrival, false);
                if (!r.rseq) {
                    sleepNs(1000000);  // zero credits (the emergency, or backpressure)
                    continue;
                }
                i++;
                recs[p].push_back(std::move(r));
                inflight.push_back(recs[p].size() - 1);
                while (!inflight.empty() && prod.acked(recs[p][inflight.front()].rseq)) {
                    recs[p][inflight.front()].acked = true;
                    const QuotaStats qs = s.e->quotaStats();
                    if (qs.emergencies) ackedAfterEmergency++;
                    if (qs.ballastRestores) ackedAfterRestore++;
                    inflight.pop_front();
                }
            }
            // Settle what is still in flight (an emergency may hold it).
            if (!inflight.empty()) prod.waitAcked(recs[p][inflight.back()].rseq, 5000000000ull);
            for (size_t k : inflight)
                if (prod.acked(recs[p][k].rseq)) recs[p][k].acked = true;
        });
    const uint64_t seconds = uint64_t(argInt("seconds", 4));
    uint64_t emergencySeen = 0;
    // Run the full-disk cycle until ingest has resumed after a recovery
    // (at least `seconds`, at most a minute more).
    for (const uint64_t t0 = monoNs();; sleepNs(10000000)) {
        if (s.e->spaceEmergency()) emergencySeen++;
        const uint64_t el = monoNs() - t0;
        if (el >= seconds * 1000000000ull && ackedAfterRestore.load() >= 100) break;
        if (el >= (seconds + 60) * 1000000000ull) break;
    }
    stop = true;
    for (auto& t : prods) t.join();
    sleepNs(300000000);  // the last heads (written in an emergency too: they never grow)
    const QuotaStats q = s.e->quotaStats();
    const uint64_t resumed = ackedAfterRestore.load();
    if (getenv("PS_QDEBUG")) {
        std::map<std::string, uint64_t> by;
        for (const std::string& path : s.fs->list(s.root + "/")) {
            IoStats st2;
            IoCtx ctx(s.fs.get(), &st2);
            FileRef f;
            if (ctx.open(path.c_str(), path.size(), FLATSQL_IO_READ, FileClass::Store, &f) < 0) continue;
            std::string rel = path.substr(s.root.size() + 7);
            const std::string base = rel.substr(rel.rfind('/') + 1);
            const std::string dir = rel.substr(0, 1);
            by[dir + ":" + base.substr(0, base.find_first_of("-."))] += uint64_t(ctx.size(f));
            if (getenv("PS_QDEBUG_FILES") && dir == "t") std::fprintf(stderr, "    %s %llu\n", base.c_str(), (unsigned long long)ctx.size(f));
            ctx.close(&f);
        }
        std::fprintf(stderr, "  device used %llu emergency %d usage %llu cap %llu retired %llu unlinked %llu busy %llu:",
                     (unsigned long long)s.fs->usedBytes(), int(q.emergency), (unsigned long long)q.usageBytes,
                     (unsigned long long)q.capBytes, (unsigned long long)s.e->stats().retiredFiles,
                     (unsigned long long)s.e->stats().unlinkedFiles, (unsigned long long)s.e->stats().unlinkBusy);
        for (auto& kv : by) std::fprintf(stderr, " %s=%llu", kv.first.c_str(), (unsigned long long)kv.second);
        std::fprintf(stderr, "\n");
    }
    // No holes: the surviving acked records of a partition are its newest.
    uint64_t acked = 0, alive = 0;
    int holes = 0;
    for (size_t p = 0; p < pids.size(); p++) {
        const auto live = livePuts(s, pids[p]);
        if (getenv("PS_QDEBUG")) {
            std::string runs;
            int cur = -1, n = 0;
            for (const QRec& r : recs[p]) {
                if (!r.acked) continue;
                const int v = live.count(cidKey(r.frame)) ? 1 : 0;
                if (v != cur) {
                    if (cur >= 0) runs += (cur ? "L" : "D") + std::to_string(n) + " ";
                    cur = v;
                    n = 0;
                }
                n++;
            }
            runs += (cur ? "L" : "D") + std::to_string(n);
            std::fprintf(stderr, "  pid %u runs: %s\n", pids[p], runs.c_str());
        }
        bool seenLive = false;
        for (const QRec& r : recs[p]) {
            if (!r.acked) continue;
            acked++;
            const bool isLive = live.count(cidKey(r.frame)) != 0;
            if (isLive) {
                alive++;
                seenLive = true;
            } else if (seenLive) {
                holes++;
            }
        }
    }
    report("full_device_bytes", double(argInt("device_mib", 16) << 20), "bytes");
    report("full_emergencies", double(q.emergencies), "episodes");
    report("full_ballast_releases", double(q.ballastReleases), "releases");
    report("full_ballast_restores", double(q.ballastRestores), "restores");
    report("full_segments_evicted", double(q.segmentsEvicted), "segments");
    report("full_records_acked", double(acked), "records");
    report("full_records_alive", double(alive), "records");
    report("full_records_acked_after_first_emergency", double(ackedAfterEmergency.load()), "records");
    report("full_records_acked_after_recovery", double(resumed), "records");
    report("full_emergency_samples", double(emergencySeen), "samples");
    CHECK(q.emergencies >= 1);
    CHECK(q.ballastRestores >= 1);
    CHECK(resumed > 0);
    CHECK_EQ(holes, 0);
    // A reopen names exactly the files on disk (checked before the engine
    // starts: a merge it would plan at once is not an orphan).
    s.close();
    {
        std::string err;
        REQUIRE(Engine::open(s.cfg, &s.e, &err) == 0);
    }
    for (uint32_t pid : pids) {
        const DirCheck dc = checkPartitionDir(s.fs.get(), s.fs.get(), s.root, pid);
        if (!dc.ok) std::fprintf(stderr, "  dir: %s\n", dc.err.c_str());
        CHECK(dc.ok);
        CHECK_EQ(dc.bytes, s.e->partitionDiskBytes(pid));
    }
    const DirCheck tc = checkTypeDir(s.fs.get(), s.fs.get(), s.root, ommType().fid);
    if (!tc.ok) std::fprintf(stderr, "  type dir: %s\n", tc.err.c_str());
    CHECK(tc.ok);
    CHECK_EQ(tc.bytes, s.e->typeDiskBytesOf(ommType().fid));
    s.e->start();
    s.close();
}
