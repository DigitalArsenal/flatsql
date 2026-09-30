// TB03 (terabyte design §2.9 K0, §3, §10): lane caps, paged lane checkpoints,
// live-only candidate caps (N2) and format levels. PARTITION-STORE.md §41.
//
// Gates (§10 TB03):
//   - 5,000 live lanes in one partition, all acked, correct after a reopen
//     and across 1,000 crash trials: lanecap_5000_live_lanes,
//     lanecap_crash_trials(_full);
//   - FaultFs crash points at each lk step (written, synced, head written,
//     RETIRE): lane counts equal the oracle, no lk orphan after open:
//     lanecap_ckpt_crash_every_op(_full);
//   - 2,000 lanes with retire-bearing batches: lanecap_2000_lanes_retire_bearing;
//   - per-batch lane bytes proportional to changed lanes:
//     lanecap_batch_lane_bytes_proportional;
//   - tb_supersede_candidate_cap and _autocompact: 0 duplicate live versions;
//   - an older engine refuses a ratcheted store and creates no files:
//     format_older_engine_refuses_ratcheted_store (plus the 3.5.1 engine
//     itself, run out of tree: §41);
//   - an empty-registry store never ratchets: format_empty_registry_never_ratchets.
// Beyond the gates: the ratchet crashed at every I/O call, manifest v3 from
// level 3, a kill of a record with 300 tag instances (N2 and the lane-delta
// table), CID candidates past 64 (N2), a torn-head rebuild from LANE_REF.
#include <algorithm>
#include <map>
#include <random>
#include <set>
#include <thread>

#include "flatsql/ps/compaction.h"
#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {

std::string lcEpoch(uint64_t i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-08-%02dT%02d:%02d:%02dZ", int(1 + (i / 86400) % 28), int((i / 3600) % 24),
                  int((i / 60) % 60), int(i % 60));
    return b;
}

std::vector<uint8_t> laneAttr(uint64_t lane) { return buildRecordAttr("peer", "prov", "src", "b" + std::to_string(lane)); }

using LaneMap = std::map<uint32_t, std::pair<int64_t, int64_t>>;

LaneMap headLanes(const PartView& v) {
    LaneMap m;
    for (const auto& c : v.lanes)
        if (c.count != 0) m[c.laneId] = {c.count, c.bytes};
    return m;
}

LaneMap recountLanes(const PartView& v) {
    LaneMap m;
    for (const auto& kv : recount(v).lanes)
        if (kv.second.first != 0) m[kv.first] = kv.second;
    return m;
}

// The head's lane table (folded when it names a LaneRef) equals a recount
// from the rows, lane by lane.
bool lanesMatchRows(const PartView& v, std::string* why) {
    const LaneMap h = headLanes(v), r = recountLanes(v);
    if (h == r) return true;
    char b[200];
    size_t diff = 0;
    uint32_t first = 0;
    for (const auto& kv : r)
        if (!h.count(kv.first) || h.at(kv.first) != kv.second) {
            if (!diff++) first = kv.first;
        }
    for (const auto& kv : h)
        if (!r.count(kv.first)) {
            if (!diff++) first = kv.first;
        }
    std::snprintf(b, sizeof(b), "head %zu lanes, rows %zu lanes, %zu differ (first lane %u: head %lld/%lld rows %lld/%lld)",
                  h.size(), r.size(), diff, first, h.count(first) ? (long long)h.at(first).first : -1ll,
                  h.count(first) ? (long long)h.at(first).second : -1ll, r.count(first) ? (long long)r.at(first).first : -1ll,
                  r.count(first) ? (long long)r.at(first).second : -1ll);
    *why = b;
    return false;
}

// The engine's own lane table (what its next checkpoint or head is cut
// from) equals the on-disk table (the head's, folded when it names a
// LaneRef) in every counter, for every live lane: a lane that went to count 0
// and came back must not keep counters the disk dropped (TB03 review).
bool lanesMatchEngine(const PartView& v, Partition* p, std::string* why) {
    std::map<uint32_t, LaneCounter> disk, mem;
    for (const auto& c : v.lanes)
        if (c.count != 0) disk[c.laneId] = c;
    for (const auto& l : p->lanes)
        if (l.c.count != 0) {
            LaneCounter c = l.c;
            c.laneId = l.id;
            mem[l.id] = c;
        }
    auto same = [](const LaneCounter& a, const LaneCounter& b) {
        return a.count == b.count && a.bytes == b.bytes && a.maxPseq == b.maxPseq && a.maxGseq == b.maxGseq &&
               a.firstSeen == b.firstSeen && a.updated == b.updated;
    };
    for (const auto& kv : mem) {
        auto it = disk.find(kv.first);
        if (it != disk.end() && same(it->second, kv.second)) continue;
        char b[240];
        const LaneCounter& m = kv.second;
        if (it == disk.end()) {
            std::snprintf(b, sizeof(b), "lane %u live in the engine (count %lld), absent on disk", kv.first,
                          (long long)m.count);
        } else {
            const LaneCounter& d = it->second;
            std::snprintf(b, sizeof(b),
                          "lane %u engine/disk: count %lld/%lld bytes %lld/%lld max_pseq %llu/%llu first_seen %lld/%lld "
                          "updated %lld/%lld",
                          kv.first, (long long)m.count, (long long)d.count, (long long)m.bytes, (long long)d.bytes,
                          (unsigned long long)m.maxPseq, (unsigned long long)d.maxPseq, (long long)m.firstSeen,
                          (long long)d.firstSeen, (long long)m.updated, (long long)d.updated);
        }
        *why = b;
        return false;
    }
    if (disk.size() != mem.size()) {
        *why = "disk holds " + std::to_string(disk.size()) + " live lanes, the engine " + std::to_string(mem.size());
        return false;
    }
    return true;
}

// lk-* files present in a partition directory, by generation.
std::set<uint32_t> lkFiles(FaultFs* fs, const std::string& root, uint32_t pid) {
    PathBuf dp;
    pathPartitionDir(&dp, root.c_str(), pid);
    std::set<uint32_t> out;
    for (const std::string& path : fs->list(std::string(dp.c_str(), dp.len) + "/")) {
        const size_t at = path.rfind("/lk-");
        if (at == std::string::npos) continue;
        out.insert(uint32_t(std::strtoul(path.c_str() + at + 4, nullptr, 16)));
    }
    return out;
}

uint16_t storeFormatOnDisk(FaultFs* fs, const std::string& root) {
    const std::vector<uint8_t> b = fs->contents(root + "/fsql2/STORE");
    if (b.size() != sizeof(StoreFile)) return 0;
    StoreFile sf;
    std::memcpy(&sf, b.data(), sizeof(sf));
    return sf.crc == crc32c(&sf, offsetof(StoreFile, crc)) ? sf.format : 0;
}

// Every file of the store with its bytes (the "no file created or changed" oracle).
std::map<std::string, std::vector<uint8_t>> snapshotFiles(FaultFs* fs, const std::string& root) {
    std::map<std::string, std::vector<uint8_t>> out;
    for (const std::string& p : fs->list(root + "/")) out[p] = fs->contents(p);
    return out;
}

// Per-batch lane bytes of a partition's meta log: the lane-delta section and
// any LANE_CKPT record, per batch, walking m-<firstLiveMSeg>..m-<mSeg>.
struct BatchLaneBytes {
    uint64_t batches = 0, withDeltas = 0, ckptRecords = 0;
    uint64_t deltaBytes = 0, ckptBytes = 0;
    uint64_t maxLaneBytes = 0;
    bool proportional = true;  // every batch: lane bytes == pad8(4 + 48 x changed lanes) (+ one crossing table)
    std::vector<std::pair<uint32_t, uint64_t>> perBatch;  // (changed lanes, lane bytes)
};

BatchLaneBytes batchLaneBytes(FaultFs* fs, const std::string& root, uint32_t pid, uint32_t firstSeg, uint32_t lastSeg,
                              uint64_t crossingAllowance) {
    BatchLaneBytes r;
    for (uint32_t seg = firstSeg; seg <= lastSeg; seg++) {
        PathBuf mp;
        pathPartitionSeg(&mp, root.c_str(), pid, 'm', seg, "fsl");
        const std::vector<uint8_t> m = fs->contents(std::string(mp.c_str(), mp.len));
        uint64_t off = 0;
        while (off + sizeof(BatchHeader) <= m.size()) {
            BatchHeader h;
            std::memcpy(&h, m.data() + off, sizeof(h));
            if (h.magic != kMagicBatch || h.batchLen < sizeof(BatchHeader) + sizeof(BatchTrailer) ||
                off + h.batchLen > m.size())
                break;
            const uint8_t* b = m.data() + off;
            size_t q = sizeof(BatchHeader) + size_t(h.nRows) * sizeof(RecRow) + h.attrBytes + h.l0Bytes;
            const uint32_t nd = getU32(b + q);
            const uint64_t dsec = pad8(4 + size_t(nd) * sizeof(LaneDelta));
            q += dsec;
            const uint32_t nc = getU32(b + q);
            q += 4;
            uint64_t ck = 0;
            for (uint32_t i = 0; i < nc; i++) {
                const uint16_t kind = getU16(b + q), len = getU16(b + q + 2);
                if (kind == kCtlLaneCkpt) ck += 4 + len;
                q += 4 + len;
            }
            r.batches++;
            if (nd) r.withDeltas++;
            if (ck) r.ckptRecords++;
            r.deltaBytes += nd ? dsec : 0;
            r.ckptBytes += ck;
            const uint64_t lane = (nd ? dsec : 0) + ck;
            r.maxLaneBytes = std::max(r.maxLaneBytes, lane);
            if (ck > crossingAllowance) r.proportional = false;
            r.perBatch.push_back({nd, lane});
            off += h.batchLen;
        }
    }
    return r;
}

struct LcStore {
    Store s;
    uint32_t pid = 0;
    std::vector<std::vector<uint8_t>> frames;
    explicit LcStore(bool threads = true, uint32_t writers = 1) : s(true, writers, threads) {
        s.cfg.poolBytes = 96ull << 20;
    }
    bool setup(const std::string& producer = "lanes") {
        if (s.open() != 0) return false;
        s.registerTypes({&ommType()});
        pid = s.partition(producer, ommType());
        return pid != 0;
    }
    std::vector<uint8_t> frame(uint64_t i) {
        return ommRecord(uint32_t(i + 1), "L" + std::to_string(i), lcEpoch(i), double(i) * 1e-3, 120);
    }
};

bool waitAll(Producer& prod, uint64_t last, uint64_t timeoutNs = 120000000000ull) {
    return !last || prod.waitAcked(last, timeoutNs) == 0;
}

}  // namespace

// ---------------------------------------------------------------------------
// 5,000 live lanes in one partition
// ---------------------------------------------------------------------------
PS_TEST(lanecap_5000_live_lanes) {
    const uint64_t n = uint64_t(argInt("lanes", 5000));
    LcStore ls(true, 2);
    ls.s.cfg.sealRecords = 1500;  // seals, merges and meta retirement run beside the lanes
    ls.s.cfg.mergeL0Blocks = 4;
    ls.s.cfg.mergeMinL0Bytes = 0;
    ls.s.cfg.reclaimGraceMs = 10;
    ls.s.cfg.laneCkptBatches = 64;
    REQUIRE(ls.setup());
    CHECK_EQ(ls.s.e->storeFormat(), kFormatMax);
    Producer prod(ls.s.e.get(), ls.pid);
    uint64_t last = 0, acked = 0;
    // One PUT per lane.
    for (uint64_t i = 0; i < n; i++) {
        ls.frames.push_back(ls.frame(i));
        last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
        REQUIRE(last != 0);
    }
    REQUIRE(waitAll(prod, last));
    acked += n;
    // Re-tags of 500 of them into new lanes, and 200 kills (each kill takes a
    // record's every instance, so up to two lanes).
    for (uint64_t i = 0; i < 500; i++) {
        last = send(ls.s.e.get(), prod, ls.frames[i * 7 % n], laneAttr(n + i), int64_t(1780001000000ll + i));
        REQUIRE(last != 0);
    }
    REQUIRE(waitAll(prod, last));
    std::set<uint64_t> killed;
    for (uint64_t k = 0; k < 200; k++) {
        const uint64_t i = (k * 13 + 5) % n;
        killed.insert(i);
        uint8_t cid[kCidLen];
        frameCid(ls.frames[i], cid);
        REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &last) == 0);
    }
    REQUIRE(waitAll(prod, last));
    // Every lane but those whose only instances were killed.
    std::set<uint64_t> retagged;
    for (uint64_t i = 0; i < 500; i++) retagged.insert(i * 7 % n);
    uint64_t expectLive = 0;
    for (uint64_t i = 0; i < n; i++) expectLive += killed.count(i) ? 0 : 1;
    for (uint64_t i = 0; i < 500; i++) expectLive += killed.count(i * 7 % n) ? 0 : 1;
    // Settle, then the engine's table, a reader's flatsql_lanes, the on-disk
    // fold and a recount from rows agree.
    sleepNs(500000000);
    Partition* p = ls.s.e->partition(ls.pid);
    report("lanes_live_engine", double(p->laneLive), "lanes");
    report("lane_checkpoints_cut", double(ls.s.e->cLaneCuts.load()), "checkpoints");
    {
        Reader rd(ls.s, LaneClass::Bulk, 1);
        const Rows rows = rd.q("SELECT COUNT(*), SUM(count) FROM flatsql_lanes");
        REQUIRE(rows.status == 0 && rows.rows.size() == 1);
        CHECK_EQ(uint64_t(rows.i(0, 0)), expectLive);
        CHECK_EQ(uint64_t(rows.i(0, 1)), expectLive);
    }
    ls.s.close();
    Inspector ins(ls.s.fs.get(), ls.s.root);
    PartView v = ins.partition(ls.pid);
    REQUIRE(v.ok);
    CHECK(v.lanesRef);
    CHECK_EQ(uint64_t(headLanes(v).size()), expectLive);
    std::string why;
    if (!lanesMatchRows(v, &why)) {
        std::fprintf(stderr, "  lanes: %s\n", why.c_str());
        gFailures++;
    }
    CHECK_EQ(v.head.counters.liveCount, uint64_t(n - killed.size()));
    const DirCheck dc = checkPartitionDir(ls.s.fs.get(), ls.s.fs.get(), ls.s.root, ls.pid);
    if (!dc.ok) std::fprintf(stderr, "  dir (before reopen, retired files wait): %s\n", dc.err.c_str());
    // Reopen: the same table, and the engine carries on (all acked).
    REQUIRE(ls.s.open() == 0);
    CHECK_EQ(ls.s.e->partition(ls.pid)->laneLive, uint32_t(expectLive));
    {
        Producer p2(ls.s.e.get(), ls.pid);
        for (uint64_t i = 0; i < 100; i++) {
            ls.frames.push_back(ls.frame(n + 1000 + i));
            last = send(ls.s.e.get(), p2, ls.frames.back(), laneAttr(n + 1000 + i), int64_t(1780002000000ll + i));
        }
        REQUIRE(waitAll(p2, last));
        expectLive += 100;
    }
    ls.s.close();
    v = ins.partition(ls.pid);
    REQUIRE(v.ok);
    CHECK_EQ(uint64_t(headLanes(v).size()), expectLive);
    if (!lanesMatchRows(v, &why)) {
        std::fprintf(stderr, "  lanes after reopen: %s\n", why.c_str());
        gFailures++;
    }
    REQUIRE(ls.s.open() == 0);
    const DirCheck dc2 = checkPartitionDir(ls.s.fs.get(), ls.s.fs.get(), ls.s.root, ls.pid);
    if (!dc2.ok) {
        std::fprintf(stderr, "  dir after open: %s\n", dc2.err.c_str());
        gFailures++;
    }
    ls.s.close();
}

// ---------------------------------------------------------------------------
// Per-batch lane bytes
// ---------------------------------------------------------------------------
namespace {
// Builds `lanes` live lanes, then commits `singles` batches that each change
// one lane, and returns the lane bytes of those batches.
BatchLaneBytes singleLaneBatches(uint32_t writeFormat, uint64_t lanes, uint64_t singles, bool* allAcked,
                                 uint64_t buildTimeoutNs = 60000000000ull) {
    LcStore ls(true, 1);
    ls.s.cfg.writeFormat = writeFormat;
    ls.s.cfg.laneCkptBatches = 1u << 30;  // no cut in the measured window
    BatchLaneBytes out;
    *allAcked = false;
    if (!ls.setup()) return out;
    Producer prod(ls.s.e.get(), ls.pid);
    uint64_t last = 0;
    for (uint64_t i = 0; i < lanes; i++) {
        ls.frames.push_back(ls.frame(i));
        last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
    }
    bool ok = waitAll(prod, last, buildTimeoutNs);
    Partition* p = ls.s.e->partition(ls.pid);
    const uint32_t seg0 = p->mSeg;
    const uint64_t mark = p->mEnd;
    for (uint64_t i = 0; i < singles && ok; i++) {
        ls.frames.push_back(ls.frame(lanes + i));
        last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i % lanes), int64_t(1780001000000ll + i));
        ok = waitAll(prod, last, 20000000000ull);  // one record (one changed lane) per batch
    }
    *allAcked = ok;
    const uint32_t seg1 = p->mSeg;
    ls.s.close();
    // Only the batches after `mark` in seg0 (and any later segments).
    BatchLaneBytes all = batchLaneBytes(ls.s.fs.get(), ls.s.root, ls.pid, seg0, seg1, 0);
    // Drop the batches of the build phase (before mark) by walking seg0 again.
    BatchLaneBytes pre;
    {
        PathBuf mp;
        pathPartitionSeg(&mp, ls.s.root.c_str(), ls.pid, 'm', seg0, "fsl");
        const std::vector<uint8_t> m = ls.s.fs->contents(std::string(mp.c_str(), mp.len));
        uint64_t off = 0;
        while (off < mark && off + sizeof(BatchHeader) <= m.size()) {
            BatchHeader h;
            std::memcpy(&h, m.data() + off, sizeof(h));
            if (h.magic != kMagicBatch) break;
            pre.batches++;
            off += h.batchLen;
        }
    }
    out = all;
    out.perBatch.erase(out.perBatch.begin(), out.perBatch.begin() + std::min<size_t>(pre.batches, out.perBatch.size()));
    out.batches = out.perBatch.size();
    out.deltaBytes = out.ckptBytes = out.maxLaneBytes = 0;
    out.withDeltas = 0;
    for (const auto& pb : out.perBatch) {
        out.maxLaneBytes = std::max(out.maxLaneBytes, pb.second);
        if (pb.first) out.withDeltas++;
    }
    return out;
}
}  // namespace

PS_TEST(lanecap_batch_lane_bytes_proportional) {
    // Level 3 at 5,000 live lanes: a batch that changes one lane writes one
    // LaneDelta (pad8(4 + 48) = 56 bytes) and no table.
    bool acked = false;
    const BatchLaneBytes l3 = singleLaneBatches(0, uint64_t(argInt("lanes", 5000)), 50, &acked);
    CHECK(acked);
    uint64_t l3Sum = 0, l3Deltas = 0;
    for (const auto& pb : l3.perBatch) {
        l3Sum += pb.second;
        l3Deltas += pb.first;
        if (pb.second != (pb.first ? pad8(4 + size_t(pb.first) * sizeof(LaneDelta)) : 0)) {
            std::fprintf(stderr, "  level 3 batch: %u changed lanes, %llu lane bytes\n", pb.first,
                         (unsigned long long)pb.second);
            gFailures++;
        }
    }
    report("lane_bytes_per_single_lane_batch_level3_5000_lanes", l3.withDeltas ? double(l3Sum) / double(l3.withDeltas) : 0,
           "bytes");
    report("lane_bytes_max_batch_level3_5000_lanes", double(l3.maxLaneBytes), "bytes");
    CHECK(l3.withDeltas >= 40);
    CHECK_EQ(l3.maxLaneBytes, uint64_t(pad8(4 + sizeof(LaneDelta))));
    // Level 2 (writeFormat 2) at 1,100 live lanes: the same batches rewrite
    // the whole table (56 bytes a lane) beside the delta.
    const BatchLaneBytes l2 = singleLaneBatches(2, 1100, 50, &acked);
    CHECK(acked);
    uint64_t l2Sum = 0;
    for (const auto& pb : l2.perBatch) l2Sum += pb.second;
    report("lane_bytes_per_single_lane_batch_level2_1100_lanes", l2.withDeltas ? double(l2Sum) / double(l2.withDeltas) : 0,
           "bytes");
    CHECK(l2.withDeltas >= 40 && l2Sum / std::max<uint64_t>(1, l2.withDeltas) >= 1100 * sizeof(LaneCounter));
    // host-02's largest partition today (66 live lanes, MPE celestrak-gp):
    // both levels, per single-lane batch.
    for (uint32_t wf : {2u, 0u}) {
        const BatchLaneBytes h = singleLaneBatches(wf, 66, 30, &acked);
        CHECK(acked);
        uint64_t sum = 0;
        for (const auto& pb : h.perBatch) sum += pb.second;
        report(wf == 2 ? "lane_bytes_per_single_lane_batch_level2_66_lanes" : "lane_bytes_per_single_lane_batch_level3_66_lanes",
               h.withDeltas ? double(sum) / double(h.withDeltas) : 0, "bytes");
    }
    // Level 2 past 1,170 lanes: the defect (a table beyond one u16 ctl record
    // refuses every batch, so the partition never acks again). Level 3 at the
    // same count acks everything (above).
    const BatchLaneBytes cap = singleLaneBatches(2, 1180, 1, &acked, 5000000000ull);
    (void)cap;
    report("level2_1180_lanes_all_acked", acked ? 1 : 0, "bool");
    CHECK(!acked);
}

// ---------------------------------------------------------------------------
// 2,000 lanes with retire-bearing batches
// ---------------------------------------------------------------------------
PS_TEST(lanecap_2000_lanes_retire_bearing) {
    const uint64_t n = uint64_t(argInt("lanes", 2000));
    LcStore ls(true, 2);
    ls.s.cfg.sealRecords = 400;      // many seals: merges, MERGE_DONE and A9 retirements
    ls.s.cfg.sealBytes = 256u << 10;
    ls.s.cfg.mergeL0Blocks = 2;
    ls.s.cfg.mergeMinL0Bytes = 0;
    ls.s.cfg.reclaimGraceMs = 5;
    ls.s.cfg.compactSmallBytes = 128u << 10;  // coalescing SWAPs retire a dozen files each
    ls.s.cfg.laneCkptBatches = 16;
    REQUIRE(ls.setup());
    Producer prod(ls.s.e.get(), ls.pid);
    uint64_t last = 0;
    std::mt19937_64 rng(7);
    std::set<uint64_t> killed;
    for (uint64_t i = 0; i < n; i++) {
        ls.frames.push_back(ls.frame(i));
        last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
        REQUIRE(last != 0);
        if (i % 9 == 8) {  // kills spread through the run: dead bytes, compactions, RETIRE sets
            const uint64_t k = rng() % (i + 1);
            if (killed.insert(k).second) {
                uint8_t cid[kCidLen];
                frameCid(ls.frames[k], cid);
                REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &last) == 0);
            }
        }
        if (i % 250 == 249) REQUIRE(waitAll(prod, last));
    }
    REQUIRE(waitAll(prod, last));
    sleepNs(1500000000);
    const EngineStats st = ls.s.e->stats();
    report("retire_bearing_seals", double(st.seals), "seals");
    report("retire_bearing_merges", double(st.merges), "merges");
    report("retire_bearing_files_retired", double(st.retiredFiles), "files");
    report("retire_bearing_meta_segments_retired", double(st.metaSegsRetired), "segments");
    report("retire_bearing_compactions", double(st.compactions), "compactions");
    report("retire_bearing_lane_checkpoints", double(ls.s.e->cLaneCuts.load()), "checkpoints");
    Partition* p = ls.s.e->partition(ls.pid);
    report("retire_bearing_retire_overflows", double(p->ledger.retireOverflows), "batches");
    CHECK(st.retiredFiles > 0 && st.metaSegsRetired > 0);
    CHECK(ls.s.e->cLaneCuts.load() > 0);
    CHECK_EQ(p->ledger.retireOverflows, uint64_t(0));
    CHECK_EQ(uint64_t(p->laneLive), uint64_t(n - killed.size()));
    ls.s.close();
    REQUIRE(ls.s.open() == 0);  // unlinks the persisted RETIRE set
    ls.s.close();
    Inspector ins(ls.s.fs.get(), ls.s.root);
    const PartView v = ins.partition(ls.pid);
    REQUIRE(v.ok);
    CHECK_EQ(uint64_t(headLanes(v).size()), uint64_t(n - killed.size()));
    for (const auto& kv : headLanes(v))
        if (kv.second.first != 1) {
            std::fprintf(stderr, "  lane %u count %lld (want 1)\n", kv.first, (long long)kv.second.first);
            gFailures++;
            break;
        }
    std::string why;
    if (!lanesMatchRows(v, &why)) {
        std::fprintf(stderr, "  lanes: %s\n", why.c_str());
        gFailures++;
    }
}

// ---------------------------------------------------------------------------
// Crash at every mutating I/O call around lane checkpoints
// ---------------------------------------------------------------------------
namespace {

struct LkSweep {
    Store s{true, 1, false};  // cooperative: the same op sequence every run
    uint32_t pid = 0;
    std::vector<std::vector<uint8_t>> frames;
    uint64_t nextLane = 0;
    std::mt19937_64 rng{23};
    LkSweep() {
        s.cfg.sealBytes = 16u << 10;
        s.cfg.sealRecords = 48;
        s.cfg.mergeL0Blocks = 2;
        s.cfg.mergeMinL0Bytes = 0;
        s.cfg.mergeL0Bytes = 32u << 10;
        s.cfg.ckptIntervalMs = 0;
        s.cfg.ckptMetaBytes = 4u << 10;
        s.cfg.reclaimGraceMs = 0;
        s.cfg.autoCompact = false;
        s.cfg.zeroFillStep = 8u << 10;
        s.cfg.poolBytes = 32ull << 20;
        s.cfg.laneCkptBatches = 3;  // a checkpoint every few batches
    }
    bool setup() {
        if (s.open() != 0) return false;
        s.registerTypes({&ommType()});
        pid = s.partition("lk", ommType());
        return pid != 0;
    }
    void pump(int n) {
        for (int i = 0; i < n && !s.fs->frozen(); i++) s.e->pump(0);
    }
    // New lanes, re-tags into new lanes and kills, pumped as it goes: the
    // partition crosses 32 lanes, cuts checkpoints, seals and retires meta
    // segments and replaced checkpoints.
    void workload(int records) {
        Producer prod(s.e.get(), pid);
        for (int i = 0; i < records && !s.fs->frozen(); i++) {
            std::vector<uint8_t> f;
            const bool retag = i % 4 == 3 && !frames.empty();
            if (retag) f = frames[size_t(rng() % frames.size())];
            else f = ommRecord(uint32_t(frames.size() + 1), "K", lcEpoch(frames.size()), double(i), 100);
            uint64_t rs = 0;
            for (int t = 0; t < 64 && !rs && !s.fs->frozen(); t++) {
                rs = send(s.e.get(), prod, f, laneAttr(nextLane), int64_t(1780000000000ll + i), false);
                if (!rs) pump(1);
            }
            nextLane++;
            if (!retag) frames.push_back(std::move(f));
            if (i % 7 == 6) {
                uint8_t cid[kCidLen];
                frameCid(frames[size_t(rng() % frames.size())], cid);
                uint64_t ts = 0;
                prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &ts, false);
            }
            if (i % 2 == 1) pump(1);
        }
        pump(64);
    }
    bool verify(const char* phase, uint64_t point) {
        Inspector ins(s.fs.get(), s.root);
        const PartView v = ins.partition(pid);
        if (!v.ok) {
            std::fprintf(stderr, "  [%s @%llu] partition unreadable: %s\n", phase, (unsigned long long)point, v.err.c_str());
            return false;
        }
        std::string why;
        if (!lanesMatchRows(v, &why)) {
            std::fprintf(stderr, "  [%s @%llu] lanes: %s\n", phase, (unsigned long long)point, why.c_str());
            return false;
        }
        if (s.e && !lanesMatchEngine(v, s.e->partition(pid), &why)) {
            std::fprintf(stderr, "  [%s @%llu] engine lanes: %s\n", phase, (unsigned long long)point, why.c_str());
            return false;
        }
        const Recount rc = recount(v);
        if (v.head.counters.liveCount != rc.live || v.head.counters.totalCount != rc.total) {
            std::fprintf(stderr, "  [%s @%llu] counters differ from a recount\n", phase, (unsigned long long)point);
            return false;
        }
        // No lk orphan: the directory holds exactly the checkpoint the head names.
        const std::set<uint32_t> lk = lkFiles(s.fs.get(), s.root, pid);
        const std::set<uint32_t> want =
            v.lanesRef && v.laneRef.lkGen ? std::set<uint32_t>{v.laneRef.lkGen} : std::set<uint32_t>{};
        if (lk != want) {
            std::fprintf(stderr, "  [%s @%llu] lk files: %zu present, head names gen %u\n", phase,
                         (unsigned long long)point, lk.size(), v.lanesRef ? v.laneRef.lkGen : 0);
            return false;
        }
        const DirCheck dc = checkPartitionDir(s.fs.get(), s.fs.get(), s.root, pid);
        if (!dc.ok || dc.bytes != s.e->partitionDiskBytes(pid)) {
            std::fprintf(stderr, "  [%s @%llu] files: %s (dir %llu, disk_bytes %llu)\n", phase, (unsigned long long)point,
                         dc.ok ? "sizes differ" : dc.err.c_str(), (unsigned long long)dc.bytes,
                         (unsigned long long)s.e->partitionDiskBytes(pid));
            return false;
        }
        return true;
    }
};

// The checkpoint state a crash left, read before the open cleans up.
enum LkState { kLkNone = 0, kLkUnnamed = 1, kLkReplaced = 2, kLkClean = 3 };
LkState lkStateAfterCrash(LkSweep& w) {
    Inspector ins(w.s.fs.get(), w.s.root);
    const PartView v = ins.partition(w.pid);
    if (!v.ok || !v.lanesRef) return kLkNone;
    const std::set<uint32_t> lk = lkFiles(w.s.fs.get(), w.s.root, w.pid);
    if (lk.count(v.laneRef.lkGen + 1)) return kLkUnnamed;  // written (and maybe synced); no durable head names it
    if (v.laneRef.lkGen >= 2 && lk.count(v.laneRef.lkGen - 1)) return kLkReplaced;  // named; RETIRE or unlink pending
    return kLkClean;
}

void runLkSweep(int records, uint64_t stride) {
    uint64_t total = 0;
    {
        LkSweep w;
        REQUIRE(w.setup());
        const uint64_t ops0 = w.s.fs->mutatingOps();
        w.workload(records);
        total = w.s.fs->mutatingOps() - ops0;
        const EngineStats st = w.s.e->stats();
        report("lk_sweep_lane_checkpoints", double(w.s.e->cLaneCuts.load()), "checkpoints");
        report("lk_sweep_meta_segments_retired", double(st.metaSegsRetired), "segments");
        report("lk_sweep_files_unlinked", double(st.unlinkedFiles), "files");
        report("lk_sweep_lanes_live", double(w.s.e->partition(w.pid)->laneLive), "lanes");
        CHECK(w.s.e->cLaneCuts.load() >= 3 && st.metaSegsRetired > 0 && st.unlinkedFiles > 0);
        CHECK(w.s.e->partition(w.pid)->laneLive > kMaxInlineLanes);
        w.s.close();
        std::string err;
        REQUIRE(Engine::open(w.s.cfg, &w.s.e, &err) == 0);
        REQUIRE(w.verify("baseline", 0));
        w.s.close();
    }
    report("lk_sweep_mutating_ops", double(total), "ops");
    uint64_t trials = 0, fails = 0;
    std::map<int, uint64_t> states;
    for (uint64_t k = 1; k <= total; k += stride) {
        LkSweep w;
        REQUIRE(w.setup());
        w.s.fs->armCrashAtOp(w.s.fs->mutatingOps() + k);
        w.workload(records);
        const auto mode = FaultFs::CrashMode(k % FaultFs::kModeCount);
        w.s.crash(mode, k);
        states[int(lkStateAfterCrash(w))]++;
        trials++;
        std::string err;
        bool ok = Engine::open(w.s.cfg, &w.s.e, &err) == 0;
        if (!ok) std::fprintf(stderr, "  [crash @%llu] reopen failed: %s\n", (unsigned long long)k, err.c_str());
        ok = ok && w.verify("reopen", k);
        if (ok) w.workload(30);  // goes on: more lanes, cuts and retirements
        if (ok) {
            w.s.close();
            std::string err2;
            ok = Engine::open(w.s.cfg, &w.s.e, &err2) == 0 && w.verify("clean", k);
        }
        if (!ok) {
            fails++;
            gFailures++;
            if (fails > 5) break;
        }
        w.s.close();
    }
    report("lk_sweep_crash_points", double(trials), "trials");
    report("lk_sweep_failures", double(fails), "trials");
    report("lk_sweep_state_unnamed_checkpoint", double(states[kLkUnnamed]), "trials");
    report("lk_sweep_state_replaced_checkpoint_pending", double(states[kLkReplaced]), "trials");
    report("lk_sweep_state_clean", double(states[kLkClean]), "trials");
    CHECK_EQ(fails, uint64_t(0));
    CHECK(states[kLkUnnamed] > 0 && states[kLkReplaced] > 0);
}

}  // namespace

PS_TEST(lanecap_ckpt_crash_every_op) {
    runLkSweep(int(argInt("sweep-records", 220)), uint64_t(argInt("sweep-stride", 7)));
}
PS_SLOW_TEST(lanecap_ckpt_crash_every_op_full) {
    runLkSweep(int(argInt("sweep-records", 220)), uint64_t(argInt("sweep-stride", 1)));
}

// ---------------------------------------------------------------------------
// Crash trials with thousands of live lanes (threads, helper-written checkpoints)
// ---------------------------------------------------------------------------
namespace {
void runLaneCrashTrials(uint64_t trials, uint64_t lanes) {
    uint64_t fails = 0, unnamed = 0, replaced = 0;
    const uint64_t t0 = monoNs();
    for (uint64_t t = 0; t < trials; t++) {
        std::mt19937_64 rng(1000 + t);
        LcStore ls(true, 2);
        ls.s.cfg.sealRecords = 700;
        ls.s.cfg.mergeL0Blocks = 4;
        ls.s.cfg.mergeMinL0Bytes = 0;
        ls.s.cfg.reclaimGraceMs = 0;
        ls.s.cfg.ckptIntervalMs = 5;
        ls.s.cfg.autoCompact = false;
        ls.s.cfg.laneCkptBatches = 1 + uint32_t(rng() % 8);
        ls.s.cfg.mergeHelpers = (t & 1) ? 0 : 1;
        ls.s.cfg.commitJournal = (t & 2) != 0;
        ls.s.cfg.journalCkptMs = 20;
        REQUIRE(ls.setup());
        Producer prod(ls.s.e.get(), ls.pid);
        uint64_t last = 0;
        // `lanes` live lanes first; the crash lands in what follows (a quarter
        // more: new lanes, re-tags, kills), so every crash has at least
        // `lanes` live lanes behind it.
        const uint64_t before = lanes;
        for (uint64_t i = 0; i < before; i++) {
            ls.frames.push_back(ls.frame(i));
            last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
        }
        REQUIRE(waitAll(prod, last));
        // The crash lands somewhere in the rest: new lanes, re-tags, kills.
        ls.s.fs->armCrashAtOp(ls.s.fs->mutatingOps() + 1 + rng() % 600);
        for (uint64_t i = before; i < lanes + lanes / 4 && !ls.s.fs->frozen(); i++) {
            std::vector<uint8_t> f = (i % 5 == 4) ? ls.frames[size_t(rng() % ls.frames.size())] : ls.frame(i);
            const uint64_t rs = send(ls.s.e.get(), prod, f, laneAttr(i), int64_t(1780000000000ll + i), false);
            if (!rs) sleepNs(100000);
            if (i % 5 != 4) ls.frames.push_back(std::move(f));
            if (i % 11 == 10) {
                uint8_t cid[kCidLen];
                frameCid(ls.frames[size_t(rng() % ls.frames.size())], cid);
                uint64_t ts = 0;
                prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &ts, false);
            }
        }
        for (int k = 0; k < 200 && !ls.s.fs->frozen(); k++) sleepNs(1000000);
        ls.s.crash(FaultFs::CrashMode(rng() % FaultFs::kModeCount), rng());
        {
            Inspector ins(ls.s.fs.get(), ls.s.root);
            const PartView v0 = ins.partition(ls.pid);
            if (v0.ok && v0.lanesRef) {
                const std::set<uint32_t> lk = lkFiles(ls.s.fs.get(), ls.s.root, ls.pid);
                unnamed += lk.count(v0.laneRef.lkGen + 1);
                replaced += v0.laneRef.lkGen >= 2 && lk.count(v0.laneRef.lkGen - 1);
            }
        }
        // Reopen without threads: exactly what open leaves.
        ls.s.cfg.cooperative = true;
        std::string err;
        bool ok = Engine::open(ls.s.cfg, &ls.s.e, &err) == 0;
        if (!ok) std::fprintf(stderr, "  [trial %llu] reopen failed: %s\n", (unsigned long long)t, err.c_str());
        if (ok) {
            Inspector ins(ls.s.fs.get(), ls.s.root);
            const PartView v = ins.partition(ls.pid);
            std::string why;
            if (!v.ok || !lanesMatchRows(v, &why)) {
                std::fprintf(stderr, "  [trial %llu] %s\n", (unsigned long long)t, v.ok ? why.c_str() : v.err.c_str());
                ok = false;
            } else {
                const std::set<uint32_t> lk = lkFiles(ls.s.fs.get(), ls.s.root, ls.pid);
                const std::set<uint32_t> want =
                    v.lanesRef && v.laneRef.lkGen ? std::set<uint32_t>{v.laneRef.lkGen} : std::set<uint32_t>{};
                if (lk != want) {
                    std::fprintf(stderr, "  [trial %llu] lk orphan after open (%zu files, head gen %u)\n",
                                 (unsigned long long)t, lk.size(), v.lanesRef ? v.laneRef.lkGen : 0);
                    ok = false;
                }
                if (!lanesMatchEngine(v, ls.s.e->partition(ls.pid), &why)) {
                    std::fprintf(stderr, "  [trial %llu] engine lanes: %s\n", (unsigned long long)t, why.c_str());
                    ok = false;
                }
                if (ls.s.e->partition(ls.pid)->laneLive != uint32_t(headLanes(v).size())) {
                    std::fprintf(stderr, "  [trial %llu] engine lanes %u, on disk %zu\n", (unsigned long long)t,
                                 ls.s.e->partition(ls.pid)->laneLive, headLanes(v).size());
                    ok = false;
                }
            }
        }
        if (!ok) {
            fails++;
            gFailures++;
            if (fails > 5) break;
        }
        ls.s.close();
    }
    report("lane_crash_trials", double(trials), "trials");
    report("lane_crash_trial_lanes", double(lanes), "lanes");
    report("lane_crash_failures", double(fails), "trials");
    report("lane_crash_unnamed_checkpoint_found", double(unnamed), "trials");
    report("lane_crash_replaced_checkpoint_found", double(replaced), "trials");
    report("lane_crash_seconds", double(monoNs() - t0) / 1e9, "s");
    CHECK_EQ(fails, uint64_t(0));
}
}  // namespace

PS_TEST(lanecap_crash_trials) { runLaneCrashTrials(uint64_t(argInt("trials", 24)), uint64_t(argInt("lanes", 1400))); }
PS_SLOW_TEST(lanecap_crash_trials_full) {
    runLaneCrashTrials(uint64_t(argInt("trials", 1000)), uint64_t(argInt("lanes", 5000)));
}

// ---------------------------------------------------------------------------
// N2: live-only candidate caps
// ---------------------------------------------------------------------------
// A CAT object superseded 20 times, one version per segment, compaction off:
// every version's SUPERSEDE posting stays, and the live one comes after 8+
// dead ones. The terabyte audit measured 12 live versions at v = 19.
PS_TEST(tb_supersede_candidate_cap) {
    Store s(true, 2, true);
    s.cfg.autoCompact = false;
    s.cfg.sealRecords = 1;  // one version per segment
    s.cfg.mergeL0Blocks = 1;
    s.cfg.mergeMinL0Bytes = 0;
    REQUIRE(s.open() == 0);
    s.registerTypes({&catType()});
    const uint32_t pid = s.partition("cat0", catType());
    Producer prod(s.e.get(), pid);
    const auto attr = buildRecordAttr("cat0", "prov", "catsrc", "b1");
    const int versions = int(argInt("versions", 20));
    for (int v = 0; v < versions; v++) {
        auto f = catRecord(1, "O0", "", "", "NAME-v" + std::to_string(v) + std::string(64, 'x'));
        const uint64_t last = send(s.e.get(), prod, f, attr, 1780000000000ll + v);
        REQUIRE(prod.waitAcked(last, 60000000000ull) == 0);
        REQUIRE(waitLabeledEngine(s.e.get(), {pid}, 60000000000ull));
        sleepNs(100000000);  // the version's segment seals and merges into a run
    }
    s.close();
    Inspector ins(s.fs.get(), s.root);
    const PartView v = ins.partition(pid);
    REQUIRE(v.ok);
    report("supersede_cap_live_versions", double(v.head.counters.liveCount), "versions");
    CHECK_EQ(v.head.counters.liveCount, 1ull);
    CHECK_EQ(recount(v).live, 1ull);
}

// The same with default compaction on: each segment holds one version of a
// hot object among 19 static objects (5% dead, under the 15% trigger). The
// audit measured 392 live against 381.
PS_TEST(tb_supersede_candidate_cap_autocompact) {
    Store s(true, 2, true);
    s.cfg.sealRecords = 20;
    s.cfg.compactSmallBytes = 0;  // 64 MiB segments never coalesce
    s.cfg.mergeL0Blocks = 1;
    s.cfg.mergeMinL0Bytes = 0;
    REQUIRE(s.open() == 0);
    s.registerTypes({&catType()});
    const uint32_t pid = s.partition("cat0", catType());
    Producer prod(s.e.get(), pid);
    const auto attr = buildRecordAttr("cat0", "prov", "catsrc", "b1");
    const int versions = int(argInt("versions", 20));
    int stat = 1000;
    for (int v = 0; v < versions; v++) {
        uint64_t last = send(s.e.get(), prod, catRecord(1, "O0", "", "", "HOT-v" + std::to_string(v) + std::string(64, 'x')),
                             attr, 1780000000000ll + v * 100);
        for (int k = 0; k < 19; k++, stat++)
            last = send(s.e.get(), prod,
                        catRecord(uint32_t(stat), "S" + std::to_string(stat), "", "", "STATIC" + std::string(64, 'y')),
                        attr, 1780000000000ll + v * 100 + k + 1);
        REQUIRE(prod.waitAcked(last, 60000000000ull) == 0);
        REQUIRE(waitLabeledEngine(s.e.get(), {pid}, 60000000000ull));
        sleepNs(100000000);
    }
    sleepNs(500000000);
    s.close();
    Inspector ins(s.fs.get(), s.root);
    const PartView v = ins.partition(pid);
    REQUIRE(v.ok);
    const uint64_t expect = uint64_t(stat - 1000) + 1;
    report("supersede_cap_autocompact_live", double(v.head.counters.liveCount), "records");
    report("supersede_cap_autocompact_expected", double(expect), "records");
    CHECK_EQ(v.head.counters.liveCount, expect);
    CHECK_EQ(recount(v).live, expect);
}

// A record fetched in 300 batches has 300 live tag instances in 300 lanes; a
// kill takes all of them in one batch (forEachLiveInstance kept 256, and a
// batch held 256 lane deltas: the kill failed its batch for good).
PS_TEST(lanecap_kill_record_with_300_instances) {
    LcStore ls(true, 1);
    REQUIRE(ls.setup());
    Producer prod(ls.s.e.get(), ls.pid);
    const std::vector<uint8_t> f = ls.frame(1);
    uint64_t last = 0;
    for (uint64_t b = 0; b < 300; b++) last = send(ls.s.e.get(), prod, f, laneAttr(b), int64_t(1780000000000ll + b));
    REQUIRE(waitAll(prod, last));
    CHECK_EQ(ls.s.e->partition(ls.pid)->laneLive, 300u);
    uint8_t cid[kCidLen];
    frameCid(f, cid);
    REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &last) == 0);
    REQUIRE(waitAll(prod, last, 30000000000ull));
    CHECK_EQ(ls.s.e->partition(ls.pid)->laneLive, 0u);
    ls.s.close();
    Inspector ins(ls.s.fs.get(), ls.s.root);
    const PartView v = ins.partition(ls.pid);
    REQUIRE(v.ok);
    CHECK_EQ(v.head.counters.liveCount, 0ull);
    CHECK_EQ(uint64_t(headLanes(v).size()), uint64_t(0));
    std::string why;
    if (!lanesMatchRows(v, &why)) {
        std::fprintf(stderr, "  lanes: %s\n", why.c_str());
        gFailures++;
    }
}

// One CID put and killed 80 times: its CID postings outnumber the 64 the
// lookup kept, the live copy is the newest, and a resend must dedupe.
PS_TEST(lanecap_cid_candidates_past_64) {
    LcStore ls(true, 1);
    ls.s.cfg.autoCompact = false;
    ls.s.cfg.sealRecords = 16;
    ls.s.cfg.mergeL0Blocks = 1;
    ls.s.cfg.mergeMinL0Bytes = 0;
    REQUIRE(ls.setup());
    Producer prod(ls.s.e.get(), ls.pid);
    const std::vector<uint8_t> f = ls.frame(7);
    const auto attr = laneAttr(0);
    uint8_t cid[kCidLen];
    frameCid(f, cid);
    uint64_t last = 0;
    for (int k = 0; k < 80; k++) {
        last = send(ls.s.e.get(), prod, f, attr, int64_t(1780000000000ll + k));
        REQUIRE(waitAll(prod, last));
        if (k < 79) {
            REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &last) == 0);
            REQUIRE(waitAll(prod, last));
        }
        // Filler so the copies land in merged runs.
        for (int j = 0; j < 3; j++) last = send(ls.s.e.get(), prod, ls.frame(1000 + k * 3 + j), attr, int64_t(k));
        REQUIRE(waitAll(prod, last));
    }
    sleepNs(300000000);
    const uint64_t dedupe0 = ls.s.e->stats().dedupeHits;
    last = send(ls.s.e.get(), prod, f, attr, int64_t(1780000009999ll));
    REQUIRE(waitAll(prod, last));
    CHECK_EQ(ls.s.e->stats().dedupeHits, dedupe0 + 1);
    ls.s.close();
    Inspector ins(ls.s.fs.get(), ls.s.root);
    const PartView v = ins.partition(ls.pid);
    REQUIRE(v.ok);
    uint64_t liveCopies = 0;
    const Recount rc = recount(v);
    for (const RecRow& r : v.rows)
        if (r.kind == kRowPut && std::memcmp(r.cid, cid, kCidLen) == 0 && !rc.dead.count(r.pseq)) liveCopies++;
    CHECK_EQ(liveCopies, uint64_t(1));
}

// ---------------------------------------------------------------------------
// Torn heads: the rebuild folds from the LANE_REF of a RETIRE batch
// ---------------------------------------------------------------------------
PS_TEST(lanecap_torn_heads_rebuild_from_lane_ref) {
    LcStore ls(true, 1);
    ls.s.cfg.sealRecords = 60;
    ls.s.cfg.mergeL0Blocks = 2;
    ls.s.cfg.mergeMinL0Bytes = 0;
    ls.s.cfg.reclaimGraceMs = 0;
    ls.s.cfg.autoCompact = false;
    ls.s.cfg.laneCkptBatches = 4;
    REQUIRE(ls.setup());
    Producer prod(ls.s.e.get(), ls.pid);
    uint64_t last = 0;
    for (uint64_t i = 0; i < 900; i++) {
        ls.frames.push_back(ls.frame(i));
        last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
        if (i % 50 == 49) REQUIRE(waitAll(prod, last));
    }
    REQUIRE(waitAll(prod, last));
    sleepNs(500000000);
    const EngineStats st = ls.s.e->stats();
    CHECK(st.metaSegsRetired > 0);
    ls.s.close();
    Inspector ins(ls.s.fs.get(), ls.s.root);
    const PartView before = ins.partition(ls.pid);
    REQUIRE(before.ok && before.lanesRef && before.head.firstLiveMSeg > 0);
    const LaneMap want = headLanes(before);
    CHECK_EQ(uint64_t(want.size()), uint64_t(900));
    // Both head slots torn.
    PathBuf hp;
    pathPartition(&hp, ls.s.root.c_str(), ls.pid, "h.fsh");
    {
        IoStats ios;
        IoCtx io(ls.s.fs.get(), &ios);
        FileRef h;
        REQUIRE(io.open(hp.c_str(), hp.len, FLATSQL_IO_READ | FLATSQL_IO_WRITE, FileClass::Head, &h) == 0);
        std::vector<uint8_t> junk(2 * kHeadSlotBytes, 0x5a);
        REQUIRE(io.write(h, junk.data(), junk.size(), 0) >= 0);
        REQUIRE(io.sync(h) >= 0);
        io.close(&h);
    }
    REQUIRE(ls.s.open() == 0);
    CHECK_EQ(ls.s.e->partition(ls.pid)->laneLive, uint32_t(want.size()));
    ls.s.close();
    const PartView after = ins.partition(ls.pid);
    REQUIRE(after.ok);
    CHECK(headLanes(after) == want);
    std::string why;
    if (!lanesMatchRows(after, &why)) {
        std::fprintf(stderr, "  lanes after rebuild: %s\n", why.c_str());
        gFailures++;
    }
}

// ---------------------------------------------------------------------------
// Format levels (A-TB2)
// ---------------------------------------------------------------------------
PS_TEST(format_fresh_store_at_write_format) {
    {
        Store s(true, 1, true);
        REQUIRE(s.open() == 0);
        CHECK_EQ(s.e->storeFormat(), kFormatMax);
        s.close();
        CHECK_EQ(storeFormatOnDisk(s.fs.get(), s.root), kFormatMax);
    }
    {
        Store s(true, 1, true);
        s.cfg.writeFormat = 2;
        REQUIRE(s.open() == 0);
        CHECK_EQ(s.e->storeFormat(), uint16_t(2));
        s.close();
        CHECK_EQ(storeFormatOnDisk(s.fs.get(), s.root), uint16_t(2));
    }
}

PS_TEST(format_empty_registry_never_ratchets) {
    Store s(true, 1, true);
    s.cfg.writeFormat = 2;
    REQUIRE(s.open() == 0);
    s.close();
    // An engine at level 3 opens the empty level-2 store: no ratchet.
    s.cfg.writeFormat = 0;
    for (int i = 0; i < 3; i++) {
        REQUIRE(s.open() == 0);
        CHECK_EQ(s.e->storeFormat(), uint16_t(2));
        CHECK_EQ(s.e->ratchetedFrom(), uint16_t(0));
        s.close();
        CHECK_EQ(storeFormatOnDisk(s.fs.get(), s.root), uint16_t(2));
        CHECK(!s.fs->exists(s.root + "/fsql2/STORE.tmp"));
    }
    // Registered at runtime: the store stays at 2 until the next open, and the
    // partition writes level-2 lanes meanwhile.
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("p", ommType());
    CHECK(!s.e->partition(pid)->laneCkpt);
    s.close();
    CHECK_EQ(storeFormatOnDisk(s.fs.get(), s.root), uint16_t(2));
    REQUIRE(s.open() == 0);
    CHECK_EQ(s.e->storeFormat(), kFormatMax);
    CHECK_EQ(s.e->ratchetedFrom(), uint16_t(2));
    CHECK(s.e->partition(pid)->laneCkpt);
    s.close();
    CHECK_EQ(storeFormatOnDisk(s.fs.get(), s.root), kFormatMax);
}

PS_TEST(format_older_engine_refuses_ratcheted_store) {
    // A level-2 store with lanes past 32, ratcheted by the first level-3 open,
    // then a checkpoint cut: every level-3 structure is on disk.
    LcStore ls(true, 1);
    ls.s.cfg.writeFormat = 2;
    REQUIRE(ls.setup());
    {
        Producer prod(ls.s.e.get(), ls.pid);
        uint64_t last = 0;
        for (uint64_t i = 0; i < 100; i++) {
            ls.frames.push_back(ls.frame(i));
            last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
        }
        REQUIRE(waitAll(prod, last));
    }
    ls.s.close();
    CHECK_EQ(storeFormatOnDisk(ls.s.fs.get(), ls.s.root), uint16_t(2));
    ls.s.cfg.writeFormat = 0;
    ls.s.cfg.laneCkptBatches = 1;
    REQUIRE(ls.s.open() == 0);
    CHECK_EQ(ls.s.e->ratchetedFrom(), uint16_t(2));
    {
        Producer prod(ls.s.e.get(), ls.pid);
        uint64_t last = 0;
        for (uint64_t i = 100; i < 140; i++) {
            ls.frames.push_back(ls.frame(i));
            last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
            REQUIRE(waitAll(prod, last));
        }
    }
    sleepNs(300000000);
    CHECK(ls.s.e->cLaneCuts.load() > 0);
    ls.s.close();
    CHECK_EQ(storeFormatOnDisk(ls.s.fs.get(), ls.s.root), kFormatMax);
    CHECK(!lkFiles(ls.s.fs.get(), ls.s.root, ls.pid).empty());
    // An engine one level lower (kFormatMax 2): refused, no file created or changed.
    const auto files0 = snapshotFiles(ls.s.fs.get(), ls.s.root);
    ls.s.cfg.testFormatMax = 2;
    std::string err;
    CHECK(ls.s.open(&err) < 0);
    report("older_engine_refusal", 1, "bool");
    std::fprintf(stderr, "  older engine: %s\n", err.c_str());
    CHECK(err.find("newer than this engine") != std::string::npos);
    CHECK(snapshotFiles(ls.s.fs.get(), ls.s.root) == files0);
    // And a store from a later level (format 4) refuses this engine the same way.
    {
        std::vector<uint8_t> b = ls.s.fs->contents(ls.s.root + "/fsql2/STORE");
        StoreFile sf;
        std::memcpy(&sf, b.data(), sizeof(sf));
        sf.format = uint16_t(kFormatMax + 1);
        sf.crc = crc32c(&sf, offsetof(StoreFile, crc));
        IoStats ios;
        IoCtx io(ls.s.fs.get(), &ios);
        PathBuf sp;
        pathStore(&sp, ls.s.root.c_str(), "STORE");
        FileRef f;
        REQUIRE(io.open(sp.c_str(), sp.len, FLATSQL_IO_READ | FLATSQL_IO_WRITE, FileClass::Store, &f) == 0);
        REQUIRE(io.write(f, &sf, sizeof(sf), 0) >= 0);
        io.close(&f);
    }
    const auto files1 = snapshotFiles(ls.s.fs.get(), ls.s.root);
    ls.s.cfg.testFormatMax = 0;
    err.clear();
    CHECK(ls.s.open(&err) < 0);
    CHECK(err.find("newer than this engine") != std::string::npos);
    CHECK(snapshotFiles(ls.s.fs.get(), ls.s.root) == files1);
}

// The ratchet crashed at every mutating I/O call of the ratcheting open:
// afterwards an engine built with kFormatMax 2 (this code, testFormatMax 2;
// not the released 3.5.1, which never reads STORE.tmp: §41) opens the
// level-2 store (removing a STORE.tmp a ratchet left beside an intact STORE)
// or refuses it creating nothing, and a level-3 engine opens at level 3 with
// every record.
PS_TEST(format_ratchet_crash_every_io_call) {
    auto build = [](LcStore& ls) {
        ls.s.cfg.writeFormat = 2;
        if (!ls.setup()) return false;
        Producer prod(ls.s.e.get(), ls.pid);
        uint64_t last = 0;
        for (uint64_t i = 0; i < 60; i++) {
            ls.frames.push_back(ls.frame(i));
            last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
        }
        const bool ok = waitAll(prod, last);
        ls.s.close();
        return ok;
    };
    uint64_t total = 0;
    {
        LcStore ls(true, 1);
        REQUIRE(build(ls));
        ls.s.cfg.writeFormat = 0;
        ls.s.cfg.cooperative = true;
        const uint64_t ops0 = ls.s.fs->mutatingOps();
        std::string err;
        REQUIRE(Engine::open(ls.s.cfg, &ls.s.e, &err) == 0);
        total = ls.s.fs->mutatingOps() - ops0;
        ls.s.close();
    }
    report("ratchet_open_mutating_ops", double(total), "ops");
    uint64_t trials = 0, fails = 0, oldOpened = 0, oldRefused = 0;
    for (uint64_t k = 1; k <= total; k++) {
        for (int mode = 0; mode < FaultFs::kModeCount; mode++) {
            LcStore ls(true, 1);
            REQUIRE(build(ls));
            ls.s.cfg.writeFormat = 0;
            ls.s.cfg.cooperative = true;
            ls.s.fs->armCrashAtOp(ls.s.fs->mutatingOps() + k);
            std::string err;
            if (Engine::open(ls.s.cfg, &ls.s.e, &err) == 0) ls.s.e->abandon();
            ls.s.e.reset();
            ls.s.fs->crash(FaultFs::CrashMode(mode), k * 7 + uint64_t(mode));
            trials++;
            bool ok = true;
            // A level-2 engine.
            {
                const auto files0 = snapshotFiles(ls.s.fs.get(), ls.s.root);
                EngineConfig c2 = ls.s.cfg;
                c2.testFormatMax = 2;
                std::unique_ptr<Engine> e2;
                std::string err2;
                if (Engine::open(c2, &e2, &err2) == 0) {
                    oldOpened++;
                    Inspector ins(ls.s.fs.get(), ls.s.root);
                    ok = ok && ins.partition(ls.pid).head.counters.liveCount == 60;
                    e2->stop();
                } else {
                    oldRefused++;
                    ok = ok && snapshotFiles(ls.s.fs.get(), ls.s.root) == files0;
                    if (!ok) std::fprintf(stderr, "  [ratchet %llu/%d] level-2 refusal changed files\n", (unsigned long long)k, mode);
                }
            }
            // This engine.
            std::string err3;
            if (Engine::open(ls.s.cfg, &ls.s.e, &err3) != 0) {
                std::fprintf(stderr, "  [ratchet %llu/%d] reopen failed: %s\n", (unsigned long long)k, mode, err3.c_str());
                ok = false;
            } else {
                ok = ok && ls.s.e->storeFormat() == kFormatMax;
                ls.s.close();
                ok = ok && storeFormatOnDisk(ls.s.fs.get(), ls.s.root) == kFormatMax &&
                     !ls.s.fs->exists(ls.s.root + "/fsql2/STORE.tmp");
                Inspector ins(ls.s.fs.get(), ls.s.root);
                const PartView v = ins.partition(ls.pid);
                ok = ok && v.ok && v.head.counters.liveCount == 60;
            }
            if (!ok) {
                fails++;
                gFailures++;
                if (fails > 5) break;
            }
        }
        if (fails > 5) break;
    }
    report("ratchet_crash_trials", double(trials), "trials");
    report("ratchet_crash_level2_opened", double(oldOpened), "trials");
    report("ratchet_crash_level2_refused", double(oldRefused), "trials");
    CHECK_EQ(fails, uint64_t(0));
    CHECK(oldOpened > 0 && oldRefused > 0);
}

PS_TEST(format_manifest_v3_from_level_3) {
    for (uint32_t wf : {2u, 0u}) {
        LcStore ls(true, 1);
        ls.s.cfg.writeFormat = wf;
        ls.s.cfg.mergeL0Blocks = 1;
        ls.s.cfg.mergeMinL0Bytes = 0;
        REQUIRE(ls.setup());
        Producer prod(ls.s.e.get(), ls.pid);
        uint64_t last = 0;
        for (uint64_t i = 0; i < 40; i++) {
            last = send(ls.s.e.get(), prod, ls.frame(i), laneAttr(0), int64_t(i));
            REQUIRE(waitAll(prod, last));
        }
        sleepNs(200000000);
        const uint32_t gen = ls.s.e->partition(ls.pid)->manifestGen;
        ls.s.close();
        REQUIRE(gen != 0);
        PathBuf mp;
        pathPartitionManifest(&mp, ls.s.root.c_str(), ls.pid, gen);
        const std::vector<uint8_t> m = ls.s.fs->contents(std::string(mp.c_str(), mp.len));
        REQUIRE(m.size() >= sizeof(ManifestHeader));
        ManifestHeader h;
        std::memcpy(&h, m.data(), sizeof(h));
        CHECK_EQ(h.ver, wf == 2 ? kManifestVer : kManifestVerWide);
        ManifestDesc md;
        CHECK(decodeManifest(m.data(), m.size(), &md));
    }
}

// ---------------------------------------------------------------------------
// Review fixes (TB03 adversarial review)
// ---------------------------------------------------------------------------

// A cut waits until the RETIRE listing the checkpoint it replaced commits.
// Meta writes then fail NOSPACE for a long stretch (no RETIRE commits while
// maintenance keeps running): no cut starts while an 'L' item waits, and a
// crash in that stretch leaves exactly the checkpoint the head names after
// open. Before the wait, the next cut started in the step that queued the
// RETIRE, a later one installed, and the crash left lk-<g> named by nothing.
PS_TEST(lanecap_no_cut_while_lk_retire_pending) {
    LkSweep w;
    w.s.cfg.laneCkptBatches = 1;
    REQUIRE(w.setup());
    Producer prod(w.s.e.get(), w.pid);
    Partition* p = w.s.e->partition(w.pid);
    uint64_t lane = 0, pending = 0, cutWhilePending = 0;
    auto step = [&]() {
        std::vector<uint8_t> f = ommRecord(uint32_t(100000 + lane), "Z", lcEpoch(lane), double(lane), 100);
        send(w.s.e.get(), prod, f, laneAttr(50000 + lane), int64_t(1780000000000ll + lane), false);
        lane++;
        w.pump(1);
        bool waits = false;
        for (const auto& r : p->retiring)
            if (r.letter == 'L') waits = true;
        pending += waits;
        cutWhilePending += waits && p->laneCut;
    };
    for (int i = 0; i < 200; i++) step();
    REQUIRE(p->laneMode == kLaneModeRef);
    const uint32_t genBefore = p->laneRef.lkGen;
    w.s.fs->failWrites("/m-", 1 << 20, FLATSQL_IO_ERR_NOSPACE);
    for (int i = 0; i < 300; i++) {
        step();
        sleepNs(500000);  // a failed write pauses the ring for 100 ms
    }
    report("lk_retire_pending_steps", double(pending), "steps");
    report("lk_cut_while_retire_pending", double(cutWhilePending), "steps");
    report("lk_gens_during_nospace", double(p->laneRef.lkGen - genBefore), "gens");
    CHECK(pending > 0);
    CHECK_EQ(cutWhilePending, uint64_t(0));
    w.s.crash(FaultFs::kDropAll, 1);
    std::string err;
    REQUIRE(Engine::open(w.s.cfg, &w.s.e, &err) == 0);
    CHECK(w.verify("retire-pending-reopen", 0));
    w.workload(30);
    w.s.close();
    REQUIRE(Engine::open(w.s.cfg, &w.s.e, &err) == 0);
    CHECK(w.verify("retire-pending-clean", 0));
    w.s.close();
}

// A lane whose count went to 0, was dropped by a checkpoint and got a record
// again: its counters read the same before the next cut (a fold of the
// checkpoint that dropped it, plus the delta), after it (cut from the
// writer's memory), and after a reopen. A lane at count 0 has no history, so
// the revival starts it afresh everywhere (laneApplyDelta).
PS_TEST(lanecap_revived_lane_counters_stable) {
    Store s(true, 1, false);  // cooperative: cuts happen exactly when pumped
    s.cfg.poolBytes = 96ull << 20;
    s.cfg.laneCkptBatches = 1u << 28;  // cuts only when asked
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("revive", ommType());
    Producer prod(s.e.get(), pid);
    auto pumpUntil = [&](uint64_t rs) {
        for (int t = 0; t < 200000 && !prod.acked(rs); t++) s.e->pump(0);
        return prod.acked(rs);
    };
    auto sendCoop = [&](const std::vector<uint8_t>& f, const std::vector<uint8_t>& a, int64_t at) {
        uint64_t rs = 0;
        for (int t = 0; t < 100000 && !rs; t++) {
            rs = send(s.e.get(), prod, f, a, at, false);
            if (!rs) s.e->pump(0);
        }
        return rs;
    };
    std::vector<std::vector<uint8_t>> frames;
    uint64_t last = 0;
    for (uint64_t i = 0; i < 40; i++) {
        frames.push_back(ommRecord(uint32_t(i + 1), "R" + std::to_string(i), lcEpoch(i), double(i) * 1e-3, 120));
        last = sendCoop(frames.back(), laneAttr(i), int64_t(1780000000000ll + i * 1000));
        REQUIRE(last != 0);
        REQUIRE(pumpUntil(last));
    }
    Partition* p = s.e->partition(pid);
    auto cut = [&]() {
        p->laneCutWanted = true;
        for (int t = 0; t < 64; t++) s.e->pump(0);
        return p->laneRef.lkGen;
    };
    REQUIRE(p->laneMode == kLaneModeRef);
    cut();
    // Kill lane 1's only record: count 0; the next cut drops lane 1.
    uint8_t cid[kCidLen];
    frameCid(frames[0], cid);
    REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &last, false) == 0);
    REQUIRE(pumpUntil(last));
    const uint32_t g1 = cut();
    const int64_t later = 1790000000000ll;
    frames.push_back(ommRecord(1001, "R1000", lcEpoch(1000), 1.0, 120));
    last = sendCoop(frames.back(), laneAttr(0), later);
    REQUIRE(pumpUntil(last));
    auto lane1 = [&](int64_t* first, int64_t* updated, int64_t* count) {
        Reader rd(s.fs.get(), s.root, LaneClass::Bulk, 1);
        const Rows r = rd.q("SELECT first_seen, updated, count FROM flatsql_lanes WHERE lane_id = 1");
        if (r.status != 0 || r.rows.size() != 1) {
            std::fprintf(stderr, "  query: status %d rows %zu %s\n", r.status, r.rows.size(), r.error.c_str());
            return false;
        }
        *first = r.i(0, 0);
        *updated = r.i(0, 1);
        *count = r.i(0, 2);
        return true;
    };
    int64_t f0 = 0, u0 = 0, c0 = 0, f1 = 0, u1 = 0, c1 = 0, f2 = 0, u2 = 0, c2 = 0;
    REQUIRE(p->laneRef.lkGen == g1);
    REQUIRE(lane1(&f0, &u0, &c0));  // a fold: lk-g1 (lane 1 dropped) plus the delta
    const uint32_t g2 = cut();
    REQUIRE(g2 > g1);
    REQUIRE(lane1(&f1, &u1, &c1));  // lk-g2, cut from the writer's memory
    s.close();
    REQUIRE(s.open() == 0);
    REQUIRE(lane1(&f2, &u2, &c2));
    CHECK_EQ(c0, int64_t(1));
    CHECK_EQ(c1, int64_t(1));
    CHECK_EQ(c2, int64_t(1));
    CHECK_EQ(f0, later);
    CHECK_EQ(f1, later);
    CHECK_EQ(f2, later);
    CHECK_EQ(u0, u1);
    CHECK_EQ(u1, u2);
    {
        Inspector ins(s.fs.get(), s.root);
        const PartView v = ins.partition(pid);
        std::string why;
        CHECK(v.ok && lanesMatchEngine(v, s.e->partition(pid), &why));
        if (!why.empty()) std::fprintf(stderr, "  %s\n", why.c_str());
    }
    s.close();
}

// A fold (every flatsql_lanes statement, every open) walks at most
// laneCkptBatches (default 128) batches with lane deltas since the
// checkpoint, however many one-lane batches arrive: no seal here, so the
// cadence alone cuts (the review measured 1,032 batches walked, 1.05 ms per
// flatsql_lanes, at the old default of 1,024).
PS_TEST(lanecap_fold_walk_bounded) {
    Store s(true, 1, false);
    s.cfg.poolBytes = 96ull << 20;
    s.cfg.sealRecords = 1u << 30;
    s.cfg.sealBytes = 1ull << 40;
    s.cfg.sealAgeMs = 0;
    REQUIRE(s.open() == 0);
    REQUIRE(s.cfg.laneCkptBatches == 128);  // the default a host runs
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("fold", ommType());
    Producer prod(s.e.get(), pid);
    auto sendAcked = [&](uint64_t i, uint64_t laneNo) {
        uint64_t rs = 0;
        for (int t = 0; t < 100000 && !rs; t++) {
            rs = send(s.e.get(), prod, ommRecord(uint32_t(i + 1), "F" + std::to_string(i), lcEpoch(i), double(i), 100),
                      laneAttr(laneNo), int64_t(1780000000000ll + i), false);
            if (!rs) s.e->pump(0);
        }
        for (int t = 0; t < 200000 && rs && !prod.acked(rs); t++) s.e->pump(0);
        return rs && prod.acked(rs);
    };
    for (uint64_t i = 0; i < 100; i++) REQUIRE(sendAcked(i, i));
    Partition* p = s.e->partition(pid);
    REQUIRE(p->laneMode == kLaneModeRef);
    uint32_t maxDelta = 0, maxAll = 0;
    const uint64_t n = uint64_t(argInt("batches", 1500));
    for (uint64_t b = 0; b < n; b++) {
        REQUIRE(sendAcked(100 + b, b % 100));
        maxDelta = std::max(maxDelta, p->laneRef.deltaBatches);
        maxAll = std::max(maxAll, p->laneRef.batches);
    }
    report("fold_walk_max_delta_batches", double(maxDelta), "batches");
    report("fold_walk_max_batches", double(maxAll), "batches");
    report("fold_walk_checkpoints", double(s.e->cLaneCuts.load()), "checkpoints");
    // A cut is taken in the step after the cadence is reached and installed
    // one step later: a few batches of slack.
    CHECK(maxDelta <= 128 + 4);
    CHECK(maxAll <= 4 * 128 + 4);
    {
        const uint64_t t0 = monoNs();
        Reader rd(s.fs.get(), s.root, LaneClass::Bulk, 1);
        const Rows r = rd.q("SELECT COUNT(*), SUM(count) FROM flatsql_lanes");
        report("fold_walk_flatsql_lanes_ms", double(monoNs() - t0) / 1e6, "ms");
        CHECK(r.status == 0 && r.rows.size() == 1 && r.i(0, 0) == 100 && r.i(0, 1) == int64_t(100 + n));
    }
    s.close();
}

// The supersede walk (N2) stops at a key's newest live row: a PUT of a key
// with hundreds of dead versions costs about what a PUT of a new key does
// (the review measured 12.8x at 800 versions before the stop). Default
// compaction: the audit's shape, with the dead postings left in the runs.
PS_TEST(tb_supersede_hot_key_cost) {
    const int versions = int(argInt("versions", 400));
    Store s(true, 1, false);
    s.cfg.sealRecords = 20;
    s.cfg.compactSmallBytes = 0;
    s.cfg.mergeL0Blocks = 1;
    s.cfg.mergeMinL0Bytes = 0;
    s.cfg.poolBytes = 96ull << 20;
    REQUIRE(s.open() == 0);
    s.registerTypes({&catType()});
    const uint32_t pid = s.partition("cat0", catType());
    Producer prod(s.e.get(), pid);
    const auto attr = buildRecordAttr("cat0", "prov", "catsrc", "b1");
    auto put = [&](const std::vector<uint8_t>& f, int64_t at, double* us) {
        uint64_t rs = 0;
        for (int t = 0; t < 100000 && !rs; t++) {
            rs = send(s.e.get(), prod, f, attr, at, false);
            if (!rs) s.e->pump(0);
        }
        const uint64_t t0 = monoNs();
        for (int t = 0; t < 200000 && rs && !prod.acked(rs); t++) s.e->pump(0);
        *us = double(monoNs() - t0) / 1e3;
        return rs && prod.acked(rs);
    };
    int stat = 1000;
    std::vector<double> hot, cold;
    for (int v = 0; v < versions; v++) {
        double us = 0;
        REQUIRE(put(catRecord(1, "O0", "", "", "HOT-v" + std::to_string(v) + std::string(64, 'x')),
                    1780000000000ll + v * 100, &us));
        if (v >= versions - 40) hot.push_back(us);
        for (int k = 0; k < 19; k++, stat++) {
            REQUIRE(put(catRecord(uint32_t(stat), "S" + std::to_string(stat), "", "", "STATIC" + std::string(64, 'y')),
                        1780000000000ll + v * 100 + k + 1, &us));
            if (v >= versions - 40 && k == 9) cold.push_back(us);
        }
        for (int t = 0; t < 8; t++) s.e->pump(0);  // maintenance: seals, merges
    }
    std::sort(hot.begin(), hot.end());
    std::sort(cold.begin(), cold.end());
    const double hotP50 = hot[hot.size() / 2], coldP50 = cold[cold.size() / 2];
    report("supersede_hot_put_us_p50", hotP50, "us");
    report("supersede_static_put_us_p50", coldP50, "us");
    report("supersede_hot_over_static", hotP50 / coldP50, "x");
    CHECK(hotP50 <= 3.0 * coldP50);
    s.close();
    Inspector ins(s.fs.get(), s.root);
    const PartView pv = ins.partition(pid);
    REQUIRE(pv.ok);
    CHECK_EQ(pv.head.counters.liveCount, uint64_t(stat - 1000) + 1);  // one live version of the hot key
}

// A full device at the first level-3 open: the ratchet's 64-byte STORE.tmp
// finds no room, so the store opens at its level (as the level-2 open does)
// and the next open with room raises it.
PS_TEST(format_ratchet_waits_on_a_full_device) {
    LcStore ls(true, 1);
    ls.s.cfg.writeFormat = 2;
    REQUIRE(ls.setup());
    {
        Producer prod(ls.s.e.get(), ls.pid);
        uint64_t last = 0;
        for (uint64_t i = 0; i < 80; i++) {
            ls.frames.push_back(ls.frame(i));
            last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
        }
        REQUIRE(waitAll(prod, last));
    }
    ls.s.close();
    ls.s.fs->setCapacity(ls.s.fs->usedBytes());
    std::string err;
    REQUIRE(ls.s.open(&err) == 0);  // level 2 on a full device (the control)
    ls.s.close();
    ls.s.fs->setCapacity(ls.s.fs->usedBytes());
    ls.s.cfg.writeFormat = 0;
    err.clear();
    const int32_t rc = ls.s.open(&err);
    if (rc != 0) std::fprintf(stderr, "  level-3 open on a full device: %s\n", err.c_str());
    REQUIRE(rc == 0);
    CHECK_EQ(ls.s.e->storeFormat(), uint16_t(2));
    CHECK_EQ(ls.s.e->ratchetedFrom(), uint16_t(0));
    CHECK(!ls.s.e->partition(ls.pid)->laneCkpt);
    ls.s.close();
    CHECK_EQ(storeFormatOnDisk(ls.s.fs.get(), ls.s.root), uint16_t(2));
    CHECK(!ls.s.fs->exists(ls.s.root + "/fsql2/STORE.tmp"));
    ls.s.fs->setCapacity(0);
    REQUIRE(ls.s.open() == 0);
    CHECK_EQ(ls.s.e->storeFormat(), kFormatMax);
    CHECK_EQ(ls.s.e->ratchetedFrom(), uint16_t(2));
    ls.s.close();
    CHECK_EQ(storeFormatOnDisk(ls.s.fs.get(), ls.s.root), kFormatMax);
    Inspector ins(ls.s.fs.get(), ls.s.root);
    CHECK_EQ(ins.partition(ls.pid).head.counters.liveCount, uint64_t(80));
}

namespace {
// A level-2 store with records, then STORE.tmp as a ratchet to `tmpFormat`
// leaves it after step 1 (STORE intact), or after a torn step 2 (`tornStore`).
void ratchetLeftovers(LcStore& ls, uint16_t tmpFormat, bool tornStore, bool sameStore = true) {
    std::vector<uint8_t> b = ls.s.fs->contents(ls.s.root + "/fsql2/STORE");
    REQUIRE(b.size() == sizeof(StoreFile));
    StoreFile sf;
    std::memcpy(&sf, b.data(), sizeof(sf));
    sf.format = tmpFormat;
    if (!sameStore) sf.gseqFloor += 1;
    sf.crc = crc32c(&sf, offsetof(StoreFile, crc));
    IoStats ios;
    IoCtx io(ls.s.fs.get(), &ios);
    PathBuf tp, sp;
    pathStore(&tp, ls.s.root.c_str(), "STORE.tmp");
    pathStore(&sp, ls.s.root.c_str(), "STORE");
    FileRef f;
    REQUIRE(io.open(tp.c_str(), tp.len,
                    FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, FileClass::Store,
                    &f) == 0);
    REQUIRE(io.write(f, &sf, sizeof(sf), 0) >= 0);
    REQUIRE(io.sync(f) >= 0);
    io.close(&f);
    if (tornStore) {
        REQUIRE(io.open(sp.c_str(), sp.len, FLATSQL_IO_READ | FLATSQL_IO_WRITE, FileClass::Store, &f) == 0);
        const uint8_t junk[8] = {0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef};
        REQUIRE(io.write(f, junk, sizeof(junk), 24) >= 0);
        REQUIRE(io.sync(f) >= 0);
        io.close(&f);
    }
}

bool levelTwoStore(LcStore& ls, uint64_t records) {
    ls.s.cfg.writeFormat = 2;
    if (!ls.setup()) return false;
    Producer prod(ls.s.e.get(), ls.pid);
    uint64_t last = 0;
    for (uint64_t i = 0; i < records; i++) {
        ls.frames.push_back(ls.frame(i));
        last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i), int64_t(1780000000000ll + i));
    }
    const bool ok = waitAll(prod, last);
    ls.s.close();
    return ok;
}
}  // namespace

// A STORE.tmp beside an intact STORE is a ratchet that never reached step 2:
// nothing of its level exists. An engine that cannot open that level, or one
// pinned below it (writeFormat, O5), removes it and opens at STORE's level;
// an engine that writes it finishes it and says so (stats entry 38). With
// STORE torn, STORE.tmp is finished (the old level is unknown), or the store
// is refused, creating nothing, by an engine below its level. A STORE.tmp
// that differs from STORE in more than the format names no ratchet of this
// store: removed.
PS_TEST(format_ratchet_leftover_tmp_rules) {
    // Pinned at 2: STORE stays 2, the leftover goes, no raise reported.
    {
        LcStore ls(true, 1);
        REQUIRE(levelTwoStore(ls, 40));
        ratchetLeftovers(ls, 3, false);
        ls.s.cfg.writeFormat = 2;
        REQUIRE(ls.s.open() == 0);
        CHECK_EQ(ls.s.e->storeFormat(), uint16_t(2));
        CHECK_EQ(ls.s.e->ratchetedFrom(), uint16_t(0));
        ls.s.close();
        CHECK_EQ(storeFormatOnDisk(ls.s.fs.get(), ls.s.root), uint16_t(2));
        CHECK(!ls.s.fs->exists(ls.s.root + "/fsql2/STORE.tmp"));
    }
    // An engine whose kFormatMax is 2 (a level-N engine after a crashed
    // N -> N+1 ratchet): opens, the leftover goes.
    {
        LcStore ls(true, 1);
        REQUIRE(levelTwoStore(ls, 40));
        ratchetLeftovers(ls, 3, false);
        ls.s.cfg.writeFormat = 0;
        ls.s.cfg.testFormatMax = 2;
        std::string err;
        const int32_t rc = ls.s.open(&err);
        if (rc != 0) std::fprintf(stderr, "  kFormatMax-2 engine: %s\n", err.c_str());
        CHECK_EQ(rc, 0);
        if (rc == 0) {
            CHECK_EQ(ls.s.e->storeFormat(), uint16_t(2));
            ls.s.close();
        }
        CHECK_EQ(storeFormatOnDisk(ls.s.fs.get(), ls.s.root), uint16_t(2));
        CHECK(!ls.s.fs->exists(ls.s.root + "/fsql2/STORE.tmp"));
    }
    // This engine at its level: finishes the ratchet and reports it.
    {
        LcStore ls(true, 1);
        REQUIRE(levelTwoStore(ls, 40));
        ratchetLeftovers(ls, 3, false);
        ls.s.cfg.writeFormat = 0;
        REQUIRE(ls.s.open() == 0);
        CHECK_EQ(ls.s.e->storeFormat(), kFormatMax);
        CHECK_EQ(ls.s.e->ratchetedFrom(), uint16_t(2));
        ls.s.close();
        CHECK_EQ(storeFormatOnDisk(ls.s.fs.get(), ls.s.root), kFormatMax);
        CHECK(!ls.s.fs->exists(ls.s.root + "/fsql2/STORE.tmp"));
    }
    // A STORE.tmp that is not this store's (another gseq floor): removed.
    {
        LcStore ls(true, 1);
        REQUIRE(levelTwoStore(ls, 40));
        ratchetLeftovers(ls, 3, false, false);
        ls.s.cfg.writeFormat = 2;
        REQUIRE(ls.s.open() == 0);
        CHECK_EQ(ls.s.e->storeFormat(), uint16_t(2));
        ls.s.close();
        CHECK(!ls.s.fs->exists(ls.s.root + "/fsql2/STORE.tmp"));
    }
    // STORE torn: a kFormatMax-2 engine refuses and changes nothing; this
    // engine finishes from STORE.tmp, even pinned at 2 (it is all there is).
    {
        LcStore ls(true, 1);
        REQUIRE(levelTwoStore(ls, 40));
        ratchetLeftovers(ls, 3, true);
        const auto files0 = snapshotFiles(ls.s.fs.get(), ls.s.root);
        ls.s.cfg.testFormatMax = 2;
        std::string err;
        CHECK(ls.s.open(&err) < 0);
        CHECK(err.find("newer than this engine") != std::string::npos);
        CHECK(snapshotFiles(ls.s.fs.get(), ls.s.root) == files0);
        ls.s.cfg.testFormatMax = 0;
        ls.s.cfg.writeFormat = 2;
        REQUIRE(ls.s.open() == 0);
        CHECK_EQ(ls.s.e->storeFormat(), kFormatMax);
        CHECK_EQ(ls.s.e->ratchetedFrom(), kRatchetFromTorn);
        ls.s.close();
        CHECK_EQ(storeFormatOnDisk(ls.s.fs.get(), ls.s.root), kFormatMax);
        CHECK(!ls.s.fs->exists(ls.s.root + "/fsql2/STORE.tmp"));
        Inspector ins(ls.s.fs.get(), ls.s.root);
        CHECK_EQ(ins.partition(ls.pid).head.counters.liveCount, uint64_t(40));
    }
}

// Readers apply the writer's rules: an intact STORE decides alone (a newer
// one is refused whatever STORE.tmp says), and only a torn STORE reads
// through STORE.tmp, whose level must be one this engine opens.
PS_TEST(format_reader_store_rules) {
    LcStore ls(true, 1);
    REQUIRE(levelTwoStore(ls, 20));
    auto rewriteStore = [&](uint16_t format) {
        std::vector<uint8_t> b = ls.s.fs->contents(ls.s.root + "/fsql2/STORE");
        StoreFile sf;
        std::memcpy(&sf, b.data(), sizeof(sf));
        sf.format = format;
        sf.crc = crc32c(&sf, offsetof(StoreFile, crc));
        IoStats ios;
        IoCtx io(ls.s.fs.get(), &ios);
        PathBuf sp;
        pathStore(&sp, ls.s.root.c_str(), "STORE");
        FileRef f;
        REQUIRE(io.open(sp.c_str(), sp.len, FLATSQL_IO_READ | FLATSQL_IO_WRITE, FileClass::Store, &f) == 0);
        REQUIRE(io.write(f, &sf, sizeof(sf), 0) >= 0);
        io.close(&f);
    };
    auto query = [&]() {
        Reader rd(ls.s.fs.get(), ls.s.root, LaneClass::Bulk, 1);
        return rd.q("SELECT COUNT(*) FROM flatsql_lanes").status;
    };
    CHECK_EQ(query(), 0);  // the control
    ratchetLeftovers(ls, kFormatMax, false);
    rewriteStore(uint16_t(kFormatMax + 1));  // a newer STORE beside a STORE.tmp this engine opens
    CHECK(query() != 0);
    rewriteStore(kFormatMax);
    CHECK_EQ(query(), 0);
    // Torn STORE: through STORE.tmp at a level this engine opens, not above it.
    ratchetLeftovers(ls, kFormatMax, true);
    CHECK_EQ(query(), 0);
    ratchetLeftovers(ls, uint16_t(kFormatMax + 1), true);
    CHECK(query() != 0);
}

// Manifest v3 is the written version from level 3 on both manifest paths: a
// merge's (MERGE_DONE) and a compaction's (SWAP); level 2 writes v2.
PS_TEST(format_manifest_v3_on_swap) {
    for (uint32_t wf : {2u, 0u}) {
        LcStore ls(true, 1);
        ls.s.cfg.writeFormat = wf;
        ls.s.cfg.sealRecords = 30;
        ls.s.cfg.mergeL0Blocks = 1;
        ls.s.cfg.mergeMinL0Bytes = 0;
        ls.s.cfg.autoCompact = false;
        REQUIRE(ls.setup());
        Producer prod(ls.s.e.get(), ls.pid);
        uint64_t last = 0;
        for (uint64_t i = 0; i < 150; i++) {
            ls.frames.push_back(ls.frame(i));
            last = send(ls.s.e.get(), prod, ls.frames.back(), laneAttr(i % 8), int64_t(1780000000000ll + i));
        }
        REQUIRE(waitAll(prod, last));
        for (uint64_t i = 0; i < 150; i += 2) {  // dead rows for the compaction to drop
            uint8_t cid[kCidLen];
            frameCid(ls.frames[i], cid);
            REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &last) == 0);
        }
        REQUIRE(waitAll(prod, last));
        std::vector<uint8_t> m;  // the manifest the SWAP wrote, read as soon as it is durable
        int32_t st = -1;
        for (int attempt = 0; attempt < 100 && st != 0; attempt++) {
            sleepNs(50000000);  // sealed segments merge first
            SwapResult tr;
            tr.requestSeg = UINT32_MAX;
            if (ls.s.e->swapSegment(ls.pid, &tr) != 0) continue;
            const uint64_t t0 = monoNs();
            while (tr.remaining.load() != 0 && monoNs() - t0 < 30000000000ull) sleepNs(200000);
            st = tr.remaining.load() == 0 ? tr.status : -2;
            if (st == 0) {
                PathBuf mp;
                pathPartitionManifest(&mp, ls.s.root.c_str(), ls.pid, tr.gen);
                m = ls.s.fs->contents(std::string(mp.c_str(), mp.len));
            }
        }
        REQUIRE(st == 0);
        ls.s.close();
        REQUIRE(m.size() >= sizeof(ManifestHeader));
        ManifestHeader h;
        std::memcpy(&h, m.data(), sizeof(h));
        CHECK_EQ(h.ver, wf == 2 ? kManifestVer : kManifestVerWide);
        ManifestDesc md;
        CHECK(decodeManifest(m.data(), m.size(), &md));
    }
}

// A batch that minted a lane and failed (NOSPACE) rolls nextLaneId back to
// the id it minted from. The lane table is not in id order when l.fsl lost
// frames the head still counts (merge.cpp appends those lanes after the
// l.fsl ones), so "the last lane's id + 1" could fall below ids already
// live, and the retried lane would share one.
PS_TEST(lanecap_rollback_keeps_lane_ids_unique) {
    Store s(true, 1, false);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("rollback", ommType());
    uint64_t seq = 0;
    auto put = [&](uint64_t laneNo, bool wait) {
        Producer prod(s.e.get(), pid);
        uint64_t rs = 0;
        for (int t = 0; t < 100000 && !rs; t++) {
            rs = send(s.e.get(), prod, ommRecord(uint32_t(seq + 1), "B" + std::to_string(seq), lcEpoch(seq), double(seq), 100),
                      laneAttr(laneNo), int64_t(1780000000000ll + seq), false);
            if (!rs) s.e->pump(0);
        }
        seq++;
        const uint64_t t0 = monoNs();
        while (wait && rs && !prod.acked(rs) && monoNs() - t0 < 20000000000ull) {
            s.e->pump(0);
            sleepNs(100000);
        }
        return rs && prod.acked(rs);
    };
    for (uint64_t i = 0; i < 40; i++) REQUIRE(put(i, true));
    s.close();
    // l.fsl loses its last six frames (lanes 35..40); the head still counts them.
    {
        PathBuf lp;
        pathPartition(&lp, s.root.c_str(), pid, "l.fsl");
        const std::vector<uint8_t> l = s.fs->contents(std::string(lp.c_str(), lp.len));
        uint64_t off = 0;
        for (int k = 0; k < 34 && off + 8 <= l.size(); k++) off += 8 + getU32(l.data() + off);
        REQUIRE(off < l.size());
        IoStats ios;
        IoCtx io(s.fs.get(), &ios);
        FileRef f;
        REQUIRE(io.open(lp.c_str(), lp.len, FLATSQL_IO_READ | FLATSQL_IO_WRITE, FileClass::Lanes, &f) == 0);
        REQUIRE(io.truncate(f, off) >= 0);
        REQUIRE(io.sync(f) >= 0);
        io.close(&f);
    }
    REQUIRE(s.open() == 0);
    REQUIRE(put(0, true));  // warms the partition: l.fsl's lanes, then the six it lost
    Partition* p = s.e->partition(pid);
    const uint32_t next = p->nextLaneId;
    bool sorted = true;
    for (size_t i = 1; i < p->lanes.size(); i++) sorted = sorted && p->lanes[i - 1].id < p->lanes[i].id;
    report("rollback_lane_table_in_id_order", sorted ? 1 : 0, "bool");
    CHECK_EQ(next, uint32_t(41));
    s.fs->failWrites("/m-", 1, FLATSQL_IO_ERR_NOSPACE);
    REQUIRE(put(1000, true));  // fails once, rolls back, retries after the pause
    std::set<uint32_t> ids;
    for (const auto& l : p->lanes) ids.insert(l.id);
    CHECK_EQ(ids.size(), p->lanes.size());
    const Lane* fresh = p->lanes.empty() ? nullptr : &p->lanes.back();
    CHECK(fresh && fresh->id >= next);
    s.close();
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    const PartView v = ins.partition(pid);
    std::string why;
    CHECK(v.ok && lanesMatchRows(v, &why));
    if (!why.empty()) std::fprintf(stderr, "  %s\n", why.c_str());
    CHECK_EQ(uint64_t(headLanes(v).size()), uint64_t(41));
    s.close();
}

// The head write that names a newly installed checkpoint fails ENOSPC (A13:
// the next commit writes the head). The replaced checkpoint must wait for a
// durable head naming the new one; crashes at points after such installs,
// in every mode, reopen with the table, the recount and the engine agreeing
// and only the named checkpoint on disk.
PS_TEST(lanecap_install_head_nospace) {
    uint64_t failedInstalls = 0, trials = 0, fails = 0;
    for (int mode = 0; mode < FaultFs::kModeCount; mode++) {
        for (int extra : {0, 2, 5, 11}) {
            LkSweep w;
            w.s.cfg.laneCkptBatches = 1;
            REQUIRE(w.setup());
            Producer prod(w.s.e.get(), w.pid);
            Partition* p = w.s.e->partition(w.pid);
            uint64_t lane = 0;
            auto one = [&]() {  // one record, pumped until acked
                std::vector<uint8_t> f = ommRecord(uint32_t(200000 + lane), "H", lcEpoch(lane), double(lane), 100);
                uint64_t rs = 0;
                for (int t = 0; t < 64 && !rs && !w.s.fs->frozen(); t++) {
                    rs = send(w.s.e.get(), prod, f, laneAttr(70000 + lane), int64_t(1780000000000ll + lane), false);
                    if (!rs) w.pump(1);
                }
                lane++;
                for (int t = 0; t < 1000 && rs && !prod.acked(rs) && !w.s.fs->frozen(); t++) w.pump(1);
            };
            for (int i = 0; i < 100; i++) one();
            REQUIRE(p->laneMode == kLaneModeRef);
            int failed = 0;
            char headNeedle[32];  // this partition's head (type heads are h.fsh too)
            std::snprintf(headNeedle, sizeof(headNeedle), "/p/%08x/h.fsh", w.pid);
            for (int k = 0; k < 200 && failed < 3 && !w.s.fs->frozen(); k++) {
                one();
                if (p->laneCut && p->laneCut->result.load() > 0) {
                    // Written; the next step installs it and writes its head.
                    const uint32_t gen = p->laneRef.lkGen;
                    // The pump may commit a batch first (the RETIRE of the
                    // checkpoint before): its head write fails too.
                    w.s.fs->failWrites(headNeedle, 2, FLATSQL_IO_ERR_NOSPACE);
                    w.pump(1);
                    if (p->laneRef.lkGen != gen) failed++;
                    w.s.fs->failWrites(headNeedle, 0, 0);
                }
            }
            failedInstalls += uint64_t(failed);
            for (int k = 0; k < extra; k++) one();
            w.s.crash(FaultFs::CrashMode(mode), uint64_t(7 + extra));
            trials++;
            std::string err;
            bool ok = Engine::open(w.s.cfg, &w.s.e, &err) == 0;
            if (!ok) std::fprintf(stderr, "  [mode %d +%d] reopen failed: %s\n", mode, extra, err.c_str());
            ok = ok && w.verify("install-nospace", uint64_t(mode * 100 + extra));
            if (ok) w.workload(30);
            if (ok) {
                w.s.close();
                ok = Engine::open(w.s.cfg, &w.s.e, &err) == 0 && w.verify("install-nospace-clean", uint64_t(mode * 100 + extra));
            }
            if (!ok) {
                fails++;
                gFailures++;
            }
            w.s.close();
        }
    }
    report("install_head_nospace_installs", double(failedInstalls), "installs");
    report("install_head_nospace_trials", double(trials), "trials");
    report("install_head_nospace_failures", double(fails), "trials");
    CHECK(failedInstalls > 0);
    CHECK_EQ(fails, uint64_t(0));
}
