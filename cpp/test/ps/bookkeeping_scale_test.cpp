// Per-partition bookkeeping at scale (TB audit B4) and manifest v3 (M3).
//
//   - book_manifest_v3_codec: manifests round-trip exactly at and past the
//     u16 segment count (version 3 carries a u32), a version 2 image whose
//     count wrapped is refused instead of losing segments under a valid CRC,
//     any byte between the records and the trailer is refused, version 1
//     still decodes;
//   - book_retire_valve_keeps_ingest_flowing: at the default 60 s grace and
//     a retirement rate that fills a RETIRE record within it, no batch is
//     refused for its RETIRE set (the set used to outgrow one batch and fail
//     every commit of the partition until the oldest items aged out);
//   - book_equivalence_all_commit_kinds: with gBookCheck, every ledger
//     operation is compared with the linear ledger it replaced, and every
//     segment lookup, summary refresh and compaction pick with a walk of the
//     whole segment list, across seals, merges, SWAPs (dead-ratio,
//     coalescing, requested, quota evictions), RETIRE and UNLINKED commits,
//     meta-segment retirement, a hot split, a clean reopen and a kill -9;
//   - book_crash_every_op: a deterministic single-threaded workload crashed
//     at every mutating I/O call (every stride-th in the fast variant) in all
//     five crash modes; after each reopen the rebuilt ledger names exactly
//     the files on disk, counters equal a recount, and the engine goes on
//     under gBookCheck;
//   - bookkeeping_scale_S5k: one partition past 5,000 segments; the
//     bookkeeping each commit and each look does stays flat from S = 235 to
//     S >= 5,000, and on the in-memory host so do the writer's commit rounds
//     and maintenance steps (p99 under 10 ms; lockStats). With --dir they
//     wait on the disk too and are reported only. Open stays O(S).
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <map>
#include <random>
#include <set>
#include <thread>

#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <fstream>
#endif

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

extern std::atomic<int64_t> gAllLiveBytes;  // ps_test_main.cpp: live operator-new bytes

using namespace pst;

namespace {

// The process's memory: RSS natively, the linear memory in wasm (never
// shrinks). 0 where unknown.
uint64_t bookProcessBytes() {
#if defined(__wasm__)
    return uint64_t(__builtin_wasm_memory_size(0)) * 65536ull;
#elif defined(__APPLE__)
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t n = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &n) != KERN_SUCCESS)
        return 0;
    return uint64_t(info.resident_size);
#elif defined(__linux__)
    std::ifstream f("/proc/self/statm");
    uint64_t size = 0, rss = 0;
    if (!(f >> size >> rss)) return 0;
    return rss * 4096ull;
#else
    return 0;
#endif
}

struct BookCheckScope {
    int saved;
    BookCheckScope() : saved(gBookCheck.exchange(1)) {}
    ~BookCheckScope() { gBookCheck.store(saved); }
};

std::string bookEpoch(uint64_t i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-07-%02dT%02d:%02d:%02dZ", int(1 + (i / 86400) % 28), int((i / 3600) % 24),
                  int((i / 60) % 60), int(i % 60));
    return b;
}

// A LockHist's bucket counts at a point, and the p99 of what it recorded
// since (the bucket's upper bound, in ms).
struct HistSnap {
    uint64_t b[LockHist::kBuckets];
};
HistSnap histSnap(const LockHist& h) {
    HistSnap s;
    for (int i = 0; i < LockHist::kBuckets; i++) s.b[i] = h.buckets[i].load();
    return s;
}
double histP99MsSince(const LockHist& h, const HistSnap& s0, uint64_t* n = nullptr) {
    uint64_t d[LockHist::kBuckets], total = 0;
    for (int i = 0; i < LockHist::kBuckets; i++) {
        d[i] = h.buckets[i].load() - s0.b[i];
        total += d[i];
    }
    if (n) *n = total;
    if (!total) return 0;
    uint64_t seen = 0;
    for (int i = 0; i < LockHist::kBuckets; i++) {
        seen += d[i];
        if (double(seen) >= 0.99 * double(total)) return double(LockHist::upperNs(i) + 1) / 1e6;
    }
    return 0;
}

// ---- manifest codec -------------------------------------------------------------------
ManifestDesc manifestOf(uint32_t n) {
    ManifestDesc md;
    md.gen = 77;
    md.pid = 5;
    md.prevGen = 76;
    md.segs.resize(n);
    for (uint32_t i = 0; i < n; i++) {
        ManifestSegDesc& d = md.segs[i];
        d.seg = 2 * i + 1;
        d.lastSeg = (i % 9 == 0) ? d.seg + 1 : d.seg;
        d.sealed = (i % 11) != 0;
        d.empty = (i % 13) == 0;
        d.firstPseq = uint64_t(i) * 10 + 1;
        d.endPseq = d.firstPseq + 10;
        d.mergedEnd = d.endPseq - (i % 3);
        d.dLen = 4096 + i;
        d.rLen = 1280 + i;
        d.aLen = 640 + i;
        d.minEpoch = int64_t(i) - 5;
        d.maxEpoch = int64_t(i) + 5;
        d.minArrival = 1000 + int64_t(i);
        d.maxArrival = 2000 + int64_t(i);
        d.cgen = (i % 7 == 0) ? i + 100 : 0;
        d.killThrough = i % 3;
        d.prevGen = i % 5;
        for (uint32_t r = 0; r < i % 3; r++) {
            ManifestRun mr{};
            mr.gen = i * 3 + r + 200;
            mr.nEntries = 50 + r;
            mr.fileLen = 9000 + i + r;
            d.runs.push_back(mr);
        }
    }
    return md;
}

bool sameSeg(const ManifestSegDesc& a, const ManifestSegDesc& b) {
    if (a.seg != b.seg || a.lastSeg != b.lastSeg || a.sealed != b.sealed || a.empty != b.empty || a.cgen != b.cgen ||
        a.firstPseq != b.firstPseq || a.endPseq != b.endPseq || a.mergedEnd != b.mergedEnd || a.dLen != b.dLen ||
        a.rLen != b.rLen || a.aLen != b.aLen || a.minEpoch != b.minEpoch || a.maxEpoch != b.maxEpoch ||
        a.minArrival != b.minArrival || a.maxArrival != b.maxArrival || a.killThrough != b.killThrough ||
        a.prevGen != b.prevGen || a.runs.size() != b.runs.size())
        return false;
    for (size_t r = 0; r < a.runs.size(); r++)
        if (a.runs[r].gen != b.runs[r].gen || a.runs[r].nEntries != b.runs[r].nEntries ||
            a.runs[r].fileLen != b.runs[r].fileLen)
            return false;
    return true;
}

void reseal(std::vector<uint8_t>* buf) {
    const size_t body = buf->size() - 8;
    putU32(buf->data() + body, crc32c(buf->data(), body));
}

void setHeader(std::vector<uint8_t>* buf, uint16_t ver, uint16_t n16, uint32_t n32) {
    ManifestHeader h;
    std::memcpy(&h, buf->data(), sizeof(h));
    h.ver = ver;
    h.nSegs = n16;
    h.nSegs32 = n32;
    std::memcpy(buf->data(), &h, sizeof(h));
    reseal(buf);
}

}  // namespace

PS_TEST(book_manifest_v3_codec) {
    // Round trips at the u16 boundary and past it: version 2 while the count
    // fits, version 3 (u32 count, u16 = 0xFFFF) past it.
    for (const uint32_t n : {0u, 1u, 65535u, 65536u, 70001u}) {
        const ManifestDesc md = manifestOf(n);
        const std::vector<uint8_t> buf = encodeManifest(md);
        ManifestHeader h;
        std::memcpy(&h, buf.data(), sizeof(h));
        CHECK_EQ(h.ver, n > 65535 ? kManifestVerWide : kManifestVer);
        CHECK_EQ(h.nSegs, n > 65535 ? 0xFFFFu : n);
        CHECK_EQ(h.nSegs32, n > 65535 ? n : 0u);
        ManifestDesc out;
        CHECK(decodeManifest(buf.data(), buf.size(), &out));
        CHECK_EQ(out.segs.size(), size_t(n));
        CHECK_EQ(out.gen, md.gen);
        CHECK_EQ(out.pid, md.pid);
        CHECK_EQ(out.prevGen, md.prevGen);
        size_t diff = 0;
        for (size_t i = 0; i < std::min(out.segs.size(), md.segs.size()); i++)
            if (!sameSeg(out.segs[i], md.segs[i])) diff++;
        CHECK_EQ(diff, size_t(0));
    }
    // A version 2 manifest over 65,535 segments (its count wrapped: 65,537
    // written as 1) passes the CRC but names segments the count does not:
    // refused, not silently shortened.
    {
        std::vector<uint8_t> buf = encodeManifest(manifestOf(65537));
        setHeader(&buf, 2, uint16_t(65537), 0);
        ManifestDesc out;
        CHECK(!decodeManifest(buf.data(), buf.size(), &out));
    }
    // Any byte between the last record and the trailer is refused (v2, v3).
    for (const uint32_t n : {3u, 65536u}) {
        std::vector<uint8_t> buf = encodeManifest(manifestOf(n));
        buf.insert(buf.end() - 8, uint8_t(0));
        reseal(&buf);
        ManifestDesc out;
        CHECK(!decodeManifest(buf.data(), buf.size(), &out));
    }
    // A version 3 header must say 0xFFFF in the u16; its count must fit the body.
    {
        std::vector<uint8_t> buf = encodeManifest(manifestOf(65536));
        setHeader(&buf, 3, 7, 65536);
        ManifestDesc out;
        CHECK(!decodeManifest(buf.data(), buf.size(), &out));
        std::vector<uint8_t> big = encodeManifest(manifestOf(65536));
        setHeader(&big, 3, 0xFFFF, 0xFFFFFFFFu);
        CHECK(!decodeManifest(big.data(), big.size(), &out));
        std::vector<uint8_t> v4 = encodeManifest(manifestOf(2));
        setHeader(&v4, 4, 2, 0);
        CHECK(!decodeManifest(v4.data(), v4.size(), &out));
    }
    // Version 1 (T1/T2 stores) still decodes, records exactly consumed.
    {
        std::vector<uint8_t> buf(sizeof(ManifestHeader));
        ManifestHeader h{};
        h.magic = kMagicManifest;
        h.ver = 1;
        h.nSegs = 2;
        h.gen = 9;
        h.pid = 3;
        std::memcpy(buf.data(), &h, sizeof(h));
        for (uint32_t i = 0; i < 2; i++) {
            ManifestSegV1 ms{};
            ms.seg = 4 + i;
            ms.flags = kMsegSealed;
            ms.firstPseq = 1 + 100 * i;
            ms.endPseq = 101 + 100 * i;
            ms.mergedEnd = ms.endPseq;
            ms.dLen = 777;
            ms.nRuns = 1;
            ms.cgen = 0;
            const uint8_t* b = reinterpret_cast<const uint8_t*>(&ms);
            buf.insert(buf.end(), b, b + sizeof(ms));
            ManifestRun r{};
            r.gen = 20 + i;
            r.nEntries = 100;
            r.fileLen = 4321;
            const uint8_t* rb = reinterpret_cast<const uint8_t*>(&r);
            buf.insert(buf.end(), rb, rb + sizeof(r));
        }
        buf.resize(buf.size() + 8, 0);
        reseal(&buf);
        ManifestDesc out;
        CHECK(decodeManifest(buf.data(), buf.size(), &out));
        CHECK_EQ(out.segs.size(), size_t(2));
        if (out.segs.size() == 2) {
            CHECK_EQ(out.segs[1].seg, 5u);
            CHECK_EQ(out.segs[1].lastSeg, 5u);
            CHECK_EQ(out.segs[1].firstPseq, uint64_t(101));
            CHECK_EQ(out.segs[1].runs.size(), size_t(1));
            CHECK_EQ(out.prevGen, 0u);
        }
    }
}

// ---------------------------------------------------------------------------
// The RETIRE valve
// ---------------------------------------------------------------------------
PS_TEST(book_retire_valve_keeps_ingest_flowing) {
    Store s(false, 1, true);
    s.cfg.sealBytes = 16u << 10;           // a seal, a merge and a coalescing SWAP every few dozen records
    s.cfg.zeroFillStep = 0;
    s.cfg.reclaimGraceMs = 60000;          // the default: the grace unlinks nothing during the test
    s.cfg.poolBytes = 64ull << 20;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("valve", ommType());
    const auto attr = buildRecordAttr("valve", "prov", "src", "b1");
    Producer prod(s.e.get(), pid);
    // Enough retirements within the grace to pass the soft cap several times
    // (the in-memory host keeps every byte in a 4 GiB space on wasm).
#if defined(__wasm__)
    const uint64_t total = uint64_t(argInt("valve-records", 25000));
#else
    const uint64_t total = uint64_t(argInt("valve-records", 60000));
#endif
    uint64_t last = 0;
    double worstChunkMs = 0;
    // Memory as records arrive: the heap (engine and the in-memory store's
    // file vectors), the store's file bytes, the slab pool's high-water, and
    // the process. Every term is atomic (sampled while the engine runs).
    int64_t heapPeak = 0;
    uint64_t storePeak = 0, procPeak = 0;
    for (uint64_t n = 0; n < total;) {
        const uint64_t t0 = monoNs();
        for (uint64_t k = 0; k < 5000 && n < total; k++, n++)
            last = send(s.e.get(), prod, ommRecord(uint32_t(n + 1), "V", bookEpoch(n), double(n) * 1e-3, 400), attr,
                        int64_t(n));
        REQUIRE(prod.waitAcked(last, 120000000000ull) == 0);
        worstChunkMs = std::max(worstChunkMs, double(monoNs() - t0) / 1e6);
        const int64_t heap = gAllLiveBytes.load(std::memory_order_relaxed);
        const uint64_t store = s.fs->usedBytes(), proc = bookProcessBytes();
        heapPeak = std::max(heapPeak, heap);
        storePeak = std::max(storePeak, store);
        procPeak = std::max(procPeak, proc);
        std::printf("  valve n=%llu heap=%.1fMiB store=%.1fMiB heap-store=%.1fMiB pool=%.1fMiB process=%.1fMiB\n",
                    (unsigned long long)n, double(heap) / 1048576.0, double(store) / 1048576.0,
                    double(heap - int64_t(store)) / 1048576.0, double(s.e->pool().committedBytes()) / 1048576.0,
                    double(proc) / 1048576.0);
    }
    std::fflush(stdout);
    const EngineStats st = s.e->stats();
    s.e->stop();
    const Partition* p = s.e->partition(pid);
    REQUIRE(p != nullptr);
    const PartitionLedger& L = p->ledger;
    // The writer's per-partition terms, read after stop.
    uint64_t segBytes = uint64_t(p->segs.capacity()) * sizeof(SegmentInfo), runs = 0;
    for (const auto& si : p->segs) {
        segBytes += si.runs.capacity() * sizeof(si.runs[0]);
        runs += si.runs.size();
    }
    report("valve_heap_peak", double(heapPeak), "bytes");
    report("valve_store_bytes_peak", double(storePeak), "bytes");
    report("valve_process_peak", double(procPeak), "bytes");
    report("valve_pool_committed", double(s.e->pool().committedBytes()), "bytes");
    report("valve_ledger_files", double(L.files()), "files");
    report("valve_ledger_memory", double(L.memoryBytes()), "bytes");
    report("valve_retired_items", double(p->retired.size()), "items");
    report("valve_retired_memory", double(p->retired.capacity() * sizeof(p->retired[0])), "bytes");
    report("valve_segments", double(p->segs.size()), "segments");
    report("valve_segment_list_memory", double(segBytes), "bytes");
    report("valve_runs", double(runs), "runs");
    report("valve_summary_entries", double(p->summary.size()), "entries");
    report("valve_accel_bytes", double(p->accelBytes.load()), "bytes");
    report("valve_records", double(total), "records");
    report("valve_seals", double(st.seals), "seals");
    report("valve_compactions", double(st.compactions), "compactions");
    report("valve_files_retired", double(st.retiredFiles), "files");
    report("valve_items_forced", double(L.retireForced), "items");
    report("valve_encoded_set_peak", double(L.retireEncPeak), "items");
    report("valve_soft_cap", double(retireSetSoftCap(p)), "items");
    report("valve_batches_refused", double(L.retireOverflows), "batches");
    report("valve_merge_holds", double(L.retireHolds), "holds");
    report("valve_worst_5000_record_chunk", worstChunkMs, "ms");
    // More retired than the soft cap within the grace: the valve had to act,
    // and no batch was ever refused for its RETIRE set.
    CHECK(st.retiredFiles > retireSetSoftCap(p));
    CHECK(L.retireForced > 0);
    CHECK_EQ(L.retireOverflows, uint64_t(0));
    CHECK(retireSetEncodedItems(p) <= retireSetSoftCap(p) + 512);
    s.close();
}

// ---------------------------------------------------------------------------
// Equivalence with the walks and the linear ledger, every commit kind
// ---------------------------------------------------------------------------
namespace {

struct EqStore {
    Store s{true, 2, true};
    std::vector<uint32_t> pids;
    std::mt19937_64 rng{7};
    std::vector<std::vector<std::vector<uint8_t>>> frames;  // per partition, OMM frames sent (kill targets)
    int catObjects = 300;
    std::vector<int> catVersion;
    uint64_t arrival = 1780000000000ull;

    EqStore() {
        s.cfg.sealBytes = 16u << 10;
        s.cfg.sealRecords = 48;
        s.cfg.mergeL0Blocks = 3;
        s.cfg.mergeMinL0Bytes = 0;
        s.cfg.mergeL0Bytes = 64u << 10;
        s.cfg.ckptIntervalMs = 10;
        s.cfg.sealAgeMs = 40;
        s.cfg.reclaimGraceMs = 5;
        s.cfg.compactDeadRatio = 0.10;
        s.cfg.compactSmallBytes = 40u << 10;
        s.cfg.compactMaxInputs = 5;
        s.cfg.compactSliceBytes = 16u << 10;
        s.cfg.zeroFillStep = 16u << 10;
        s.cfg.poolBytes = 64ull << 20;
        catVersion.assign(size_t(catObjects), 0);
    }

    void setup() {
        REQUIRE(s.open() == 0);
        s.registerTypes({&ommType(), &catType()});
        pids.push_back(s.partition("eq-omm-0", ommType()));
        pids.push_back(s.partition("eq-omm-1", ommType()));
        pids.push_back(s.partition("eq-cat", catType()));
        frames.resize(2);
    }

    // OMM records with TOMB_CID kills, CAT supersedes, requested SWAPs.
    void workload(int ops) {
        Producer p0(s.e.get(), pids[0]), p1(s.e.get(), pids[1]), pc(s.e.get(), pids[2]);
        Producer* prods[3] = {&p0, &p1, &pc};
        uint64_t last[3] = {0, 0, 0};
        std::vector<SwapResult*> reqs;
        for (int op = 0; op < ops; op++) {
            const uint32_t r = uint32_t(rng() % 100);
            if (r < 55) {
                const int k = int(rng() % 2);
                auto f = ommRecord(uint32_t(k * 10000000 + frames[k].size() + 1), "E" + std::to_string(k),
                                   bookEpoch(frames[k].size()), double(rng() % 1000) * 0.01, 100 + size_t(rng() % 400));
                last[k] = send(s.e.get(), *prods[k], f, buildRecordAttr("eq", "prov", "src", "b1"), int64_t(arrival++));
                frames[k].push_back(std::move(f));
            } else if (r < 70) {
                const int k = int(rng() % 2);
                if (frames[k].empty()) continue;
                uint8_t cid[kCidLen];
                frameCid(frames[k][rng() % frames[k].size()], cid);
                uint64_t rs = 0;
                if (prods[k]->enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &rs, true) == 0) last[k] = rs;
            } else if (r < 98) {
                const int o = int(rng() % uint64_t(catObjects));
                catVersion[size_t(o)]++;
                auto f = catRecord(uint32_t(o + 1), "EO" + std::to_string(o), "", "",
                                   "N-" + std::to_string(o) + "-v" + std::to_string(catVersion[size_t(o)]) +
                                       std::string(80, 'x'));
                last[2] = send(s.e.get(), pc, f, buildRecordAttr("eq", "prov", "catsrc", "b1"), int64_t(arrival++));
            } else {
                auto* q = new SwapResult();
                if (s.e->swapSegment(pids[rng() % 3], q) == 0) reqs.push_back(q);
                else delete q;
            }
        }
        for (int k = 0; k < 3; k++)
            if (last[k]) REQUIRE(prods[k]->waitAcked(last[k], 120000000000ull) == 0);
        for (SwapResult* q : reqs) {
            const uint64_t t0 = monoNs();
            while (q->remaining.load() != 0 && monoNs() - t0 < 60000000000ull) sleepNs(1000000);
            REQUIRE(q->remaining.load() == 0);
            delete q;
        }
    }

    uint64_t storeBytes() {
        uint64_t b = 0;
        for (uint32_t pid : pids) b += s.e->partitionDiskBytes(pid);
        return b;
    }

    // At rest: nothing in flight and every partition's disk_bytes equal to
    // the files its head and manifest name (checkPartitionDir), steadily.
    bool settle(uint64_t timeoutNs) {
        const uint64_t deadline = monoNs() + timeoutNs;
        uint64_t since = 0, bytes0 = UINT64_MAX;
        while (monoNs() < deadline) {
            bool ok = s.e->stats().compactInFlight == 0;
            uint64_t bytes = 0;
            for (uint32_t pid : pids) {
                const DirCheck dc = checkPartitionDir(s.fs.get(), s.fs.get(), s.root, pid);
                if (!dc.ok || dc.bytes != s.e->partitionDiskBytes(pid)) ok = false;
                bytes += dc.bytes;
            }
            if (!ok || bytes != bytes0) {
                since = monoNs();
                bytes0 = ok ? bytes : UINT64_MAX;
            } else if (monoNs() - since > 200000000ull) {
                return true;
            }
            sleepNs(5000000);
        }
        for (uint32_t pid : pids) {
            const DirCheck dc = checkPartitionDir(s.fs.get(), s.fs.get(), s.root, pid);
            Inspector ins(s.fs.get(), s.root);
            const PartView v = ins.partition(pid);
            PathBuf mp;
            pathPartitionManifest(&mp, s.root.c_str(), pid, v.head.manifestGen);
            const std::string mpath(mp.c_str(), mp.len);
            std::fprintf(stderr,
                         "  unsettled pid %u: %s dir %llu disk_bytes %llu; head manifest %u %s (%zu bytes); "
                         "compactions in flight %llu\n",
                         pid, dc.ok ? "ok" : dc.err.c_str(), (unsigned long long)dc.bytes,
                         (unsigned long long)s.e->partitionDiskBytes(pid), v.head.manifestGen,
                         s.fs->exists(mpath) ? "present" : "MISSING", s.fs->contents(mpath).size(),
                         (unsigned long long)s.e->stats().compactInFlight);
        }
        return false;
    }

    void reopen() {
        std::string err;
        REQUIRE(Engine::open(s.cfg, &s.e, &err) == 0);
        s.e->start();
    }
};

}  // namespace

PS_TEST(book_equivalence_all_commit_kinds) {
    BookCheckScope check;  // every lookup, refresh, pick and ledger operation cross-checked
    EqStore q;
    q.setup();
    REQUIRE(q.s.e->setHotSplit(q.pids[0], true) == 0);  // stage-1 helpers: the run set signed by manifest
    const int ops = int(argInt("eq-ops", 12000));
    // Sanitizer builds run the quota waves many times slower.
    const uint64_t settleNs = uint64_t(argInt("eq-settle-s", 60)) * 1000000000ull;
    q.workload(ops);
    REQUIRE(waitLabeledEngine(q.s.e.get(), q.pids, 60000000000ull));
    REQUIRE(q.settle(settleNs));
    // Quota: evict to 70% of what is on disk (TOMB_RANGE, SWAP, RETIRE, UNLINKED).
    const uint64_t usage = q.storeBytes();
    q.s.e->setQuota(usage * 7 / 10);
    for (const uint64_t t0 = monoNs(); monoNs() - t0 < 60000000000ull && !q.s.e->quotaStats().segmentsEvicted;)
        sleepNs(5000000);
    q.workload(ops / 4);
    REQUIRE(q.settle(settleNs));
    const QuotaStats qs = q.s.e->quotaStats();
    q.s.e->setQuota(0);
    const EngineStats st1 = q.s.e->stats();
    // A clean stop and open: the ledger, summary and candidates rebuilt in
    // one walk, then kept incrementally again.
    q.s.close();
    q.reopen();
    q.workload(ops / 4);
    REQUIRE(q.settle(settleNs));
    const EngineStats st2 = q.s.e->stats();
    // kill -9 mid-flight, then the same.
    q.workload(ops / 8);
    q.s.crash(FaultFs::kKeepAll, 3);
    q.reopen();
    q.workload(ops / 8);
    REQUIRE(q.settle(settleNs));
    const EngineStats st3 = q.s.e->stats();
    q.s.close();
    // Every commit kind the bookkeeping follows happened (the checks ran on each).
    report("eq_seals", double(st1.seals + st2.seals + st3.seals), "seals");
    report("eq_merges", double(st1.merges + st2.merges + st3.merges), "merges");
    report("eq_swaps", double(st1.compactions + st2.compactions + st3.compactions), "compactions");
    report("eq_files_retired", double(st1.retiredFiles + st2.retiredFiles + st3.retiredFiles), "files");
    report("eq_files_unlinked", double(st1.unlinkedFiles + st2.unlinkedFiles + st3.unlinkedFiles), "files");
    report("eq_meta_segments_retired", double(st1.metaSegsRetired + st2.metaSegsRetired + st3.metaSegsRetired),
           "segments");
    report("eq_segments_evicted", double(qs.segmentsEvicted), "segments");
    report("eq_split_dedupe_hints_used", double(st1.prepHinted), "lookups");
    CHECK(st1.seals > 0 && st1.merges > 0 && st1.compactions > 0);
    CHECK(st1.retiredFiles > 0 && st1.unlinkedFiles > 0 && st1.metaSegsRetired > 0);
    CHECK(qs.segmentsEvicted > 0);
    CHECK(st2.merges > 0 && st3.merges > 0);
}

// ---------------------------------------------------------------------------
// Crash at every mutating I/O call
// ---------------------------------------------------------------------------
namespace {

struct SweepStore {
    Store s{true, 1, false};  // cooperative: one thread, the same op sequence every run
    uint32_t pid = 0;
    std::vector<std::vector<uint8_t>> frames;
    std::mt19937_64 rng{11};

    SweepStore() {
        s.cfg.sealBytes = 8u << 10;
        s.cfg.sealRecords = 24;
        s.cfg.mergeL0Blocks = 2;
        s.cfg.mergeMinL0Bytes = 0;
        s.cfg.mergeL0Bytes = 32u << 10;
        s.cfg.ckptIntervalMs = 0;
        s.cfg.ckptMetaBytes = 4u << 10;
        s.cfg.reclaimGraceMs = 0;
        s.cfg.compactSmallBytes = 24u << 10;
        s.cfg.compactMaxInputs = 4;
        s.cfg.zeroFillStep = 8u << 10;
        s.cfg.poolBytes = 32ull << 20;
    }
    bool setup() {
        if (s.open() != 0) return false;
        s.registerTypes({&ommType()});
        pid = s.partition("sweep", ommType());
        return pid != 0;
    }
    void pump(int n) {
        for (int i = 0; i < n && !s.fs->frozen(); i++) s.e->pump(0);
    }
    // Records, kills and requested SWAPs, pumped as it goes, until the host
    // freezes or the workload ends.
    void workload(int records) {
        Producer prod(s.e.get(), pid);
        std::vector<std::unique_ptr<SwapResult>> reqs;
        const auto attr = buildRecordAttr("sweep", "prov", "src", "b1");
        for (int i = 0; i < records && !s.fs->frozen(); i++) {
            auto f = ommRecord(uint32_t(frames.size() + 1), "S", bookEpoch(frames.size()), double(i), 150 + size_t(i % 7) * 40);
            uint64_t rs = 0;
            for (int t = 0; t < 64 && !rs && !s.fs->frozen(); t++) {
                rs = send(s.e.get(), prod, f, attr, int64_t(1780000000000ll + i), false);
                if (!rs) pump(1);
            }
            frames.push_back(std::move(f));
            if (i % 5 == 4) {
                uint8_t cid[kCidLen];
                frameCid(frames[size_t(rng() % frames.size())], cid);
                uint64_t ts = 0;
                prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &ts, false);
            }
            if (i % 97 == 96) {
                std::unique_ptr<SwapResult> q(new SwapResult());
                if (s.e->swapSegment(pid, q.get()) == 0) reqs.push_back(std::move(q));
            }
            if (i % 3 == 2) pump(1);
        }
        pump(64);
        // Requests still open belong to the engine until it goes: never
        // freed while it may complete them.
        abandoned.insert(abandoned.end(), std::make_move_iterator(reqs.begin()), std::make_move_iterator(reqs.end()));
    }
    std::vector<std::unique_ptr<SwapResult>> abandoned;

    bool verify(const char* phase, uint64_t point) {
        Inspector ins(s.fs.get(), s.root);
        const PartView v = ins.partition(pid);
        if (!v.ok) {
            std::fprintf(stderr, "  [%s @%llu] partition unreadable: %s\n", phase, (unsigned long long)point, v.err.c_str());
            return false;
        }
        const Recount rc = recount(v);
        if (v.head.counters.totalCount != rc.total || v.head.counters.liveCount != rc.live ||
            v.head.counters.totalBytes != rc.totalBytes || v.head.counters.liveBytes != rc.liveBytes) {
            std::fprintf(stderr, "  [%s @%llu] counters differ from a recount\n", phase, (unsigned long long)point);
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

void runCrashSweep(int records, uint64_t stride) {
    BookCheckScope check;
    // The workload's mutating calls, counted once without a crash.
    uint64_t total = 0;
    {
        SweepStore w;
        REQUIRE(w.setup());
        const uint64_t ops0 = w.s.fs->mutatingOps();
        w.workload(records);
        total = w.s.fs->mutatingOps() - ops0;
        const EngineStats st = w.s.e->stats();
        report("sweep_workload_seals", double(st.seals), "seals");
        report("sweep_workload_merges", double(st.merges), "merges");
        report("sweep_workload_swaps", double(st.compactions), "compactions");
        report("sweep_workload_files_retired", double(st.retiredFiles), "files");
        report("sweep_workload_files_unlinked", double(st.unlinkedFiles), "files");
        report("sweep_workload_meta_segments_retired", double(st.metaSegsRetired), "segments");
        CHECK(st.seals > 0 && st.merges > 0 && st.compactions > 0 && st.unlinkedFiles > 0);
        // Files retired and not yet unlinked are on disk until open unlinks
        // the persisted set: the directory is checked after an open.
        w.s.close();
        std::string err;
        REQUIRE(Engine::open(w.s.cfg, &w.s.e, &err) == 0);
        REQUIRE(w.verify("baseline", 0));
        w.s.close();
    }
    report("sweep_mutating_ops", double(total), "ops");
    uint64_t trials = 0, fails = 0;
    std::map<int, uint64_t> modes;
    for (uint64_t k = 1; k <= total; k += stride) {
        SweepStore w;
        REQUIRE(w.setup());
        w.s.fs->armCrashAtOp(w.s.fs->mutatingOps() + k);
        w.workload(records);
        const auto mode = FaultFs::CrashMode(k % FaultFs::kModeCount);
        w.s.crash(mode, k);
        w.abandoned.clear();
        modes[int(mode)]++;
        trials++;
        std::string err;
        bool ok = Engine::open(w.s.cfg, &w.s.e, &err) == 0;
        if (!ok) std::fprintf(stderr, "  [crash @%llu] reopen failed: %s\n", (unsigned long long)k, err.c_str());
        ok = ok && w.verify("reopen", k);
        // The engine goes on from the rebuilt state (checks still on), then
        // stops cleanly and opens again.
        if (ok) w.workload(40);
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
    report("sweep_crash_points", double(trials), "trials");
    report("sweep_failures", double(fails), "trials");
    const char* names[] = {"drop_all", "drop_subset", "tear_last", "reorder", "kill9_keep_all"};
    for (const auto& kv : modes) {
        char key[64];
        std::snprintf(key, sizeof(key), "sweep_mode_%s", names[kv.first]);
        report(key, double(kv.second), "trials");
    }
    CHECK_EQ(fails, uint64_t(0));
}

}  // namespace

PS_TEST(book_crash_every_op) {
    runCrashSweep(int(argInt("sweep-records", 400)), uint64_t(argInt("sweep-stride", 37)));
}
PS_SLOW_TEST(book_crash_every_op_full) {
    runCrashSweep(int(argInt("sweep-records", 1200)), uint64_t(argInt("sweep-stride", 1)));
}

// ---------------------------------------------------------------------------
// S >= 5,000 segments in one partition
// ---------------------------------------------------------------------------
namespace {

struct ScalePoint {
    uint64_t records = 0, segs = 0;
    double recPerSec = 0;
    double commitP99 = 0, maintP99 = 0, sumP99 = 0, pickP99 = 0;
    uint64_t commits = 0, maints = 0, sums = 0, picks = 0;
};

}  // namespace

PS_SLOW_TEST(bookkeeping_scale_S5k) {
    Store s(false, 1, true);
    const std::string dir = argStr("dir", "");
    if (!dir.empty()) {
        std::filesystem::remove_all(dir + "/scale");
        s.cfg.io = nullptr;
        s.cfg.root = dir + "/scale";
        s.root = s.cfg.root;
    }
    // The audit's harness (B4): 128 KiB seals, so S grows by one per ~210
    // records; coalescing off, so S is the seal count.
    s.cfg.sealBytes = uint64_t(argInt("scale-seal", 131072));
    s.cfg.compactSmallBytes = 0;
    s.cfg.reclaimGraceMs = 1000;
    s.cfg.poolBytes = 256ull << 20;
    s.cfg.lockStats = true;
    const uint64_t target = uint64_t(argInt("scale-segments", 5000));
    const uint64_t chunk = uint64_t(argInt("scale-chunk", 50000));
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("scale", ommType());
    const auto attr = buildRecordAttr("scale", "prov", "src", "b1");
    const int saveTime = gBookTime.exchange(1);
    std::vector<ScalePoint> pts;
    double reopenHalfMs = 0, reopenFullMs = 0;
    uint64_t reopenHalfSegs = 0, reopenFullSegs = 0;
    uint64_t n = 0;
    // S = segments sealed so far (engine stats restart with each open).
    uint64_t sealsBase = 0;
    auto segsNow = [&]() { return sealsBase + s.e->stats().seals; };
    // Open rebuilds the ledger, summary and candidates in one walk (O(S)).
    auto reopenTimed = [&](uint64_t* segs, double* ms) {
        *segs = segsNow();
        sealsBase = *segs;
        s.close();
        const uint64_t t0 = monoNs();
        const bool ok = s.open() == 0;
        *ms = double(monoNs() - t0) / 1e6;
        return ok;
    };
    while (true) {
        const uint64_t segs0 = segsNow();
        if (segs0 >= target) break;
        if (!reopenHalfMs && segs0 >= target / 2) REQUIRE(reopenTimed(&reopenHalfSegs, &reopenHalfMs));
        const HistSnap c0 = histSnap(s.e->commitHist()), m0 = histSnap(s.e->maintHist());
        const HistSnap b0 = histSnap(bookSummaryHist()), k0 = histSnap(bookPickHist());
        Producer prod(s.e.get(), pid);
        const uint64_t t0 = monoNs();
        uint64_t last = 0;
        for (uint64_t k = 0; k < chunk; k++, n++)
            last = send(s.e.get(), prod, ommRecord(uint32_t(n), "O", bookEpoch(n), double(n) * 1e-4, 400), attr,
                        int64_t(n));
        REQUIRE(prod.waitAcked(last, 1200000000000ull) == 0);
        ScalePoint pt;
        pt.records = n;
        pt.segs = segsNow();
        pt.recPerSec = double(chunk) / (double(monoNs() - t0) / 1e9);
        pt.commitP99 = histP99MsSince(s.e->commitHist(), c0, &pt.commits);
        pt.maintP99 = histP99MsSince(s.e->maintHist(), m0, &pt.maints);
        pt.sumP99 = histP99MsSince(bookSummaryHist(), b0, &pt.sums);
        pt.pickP99 = histP99MsSince(bookPickHist(), k0, &pt.picks);
        std::printf("  S=%llu records=%llu rate=%.0f rec/s commit_p99=%.3fms (%llu) maint_p99=%.3fms (%llu) "
                    "summary_p99=%.3fms (%llu) pick_p99=%.3fms (%llu)\n",
                    (unsigned long long)pt.segs, (unsigned long long)pt.records, pt.recPerSec, pt.commitP99,
                    (unsigned long long)pt.commits, pt.maintP99, (unsigned long long)pt.maints, pt.sumP99,
                    (unsigned long long)pt.sums, pt.pickP99, (unsigned long long)pt.picks);
        std::fflush(stdout);
        pts.push_back(pt);
    }
    REQUIRE(reopenTimed(&reopenFullSegs, &reopenFullMs));
    s.close();
    gBookTime.store(saveTime);
    if (!dir.empty()) std::filesystem::remove_all(dir + "/scale");
    REQUIRE(pts.size() >= 2);
    const ScalePoint& a = pts.front();  // S about 235 (the audit's first point)
    const ScalePoint& b = pts.back();   // S >= 5,000
    report("scale_first_segments", double(a.segs), "segments");
    report("scale_last_segments", double(b.segs), "segments");
    report("scale_first_rate", a.recPerSec, "rec/s");
    report("scale_last_rate", b.recPerSec, "rec/s");
    report("scale_rate_ratio_last_over_first", b.recPerSec / a.recPerSec, "x");
    report("scale_first_commit_p99", a.commitP99, "ms");
    report("scale_last_commit_p99", b.commitP99, "ms");
    report("scale_first_maintenance_p99", a.maintP99, "ms");
    report("scale_last_maintenance_p99", b.maintP99, "ms");
    report("scale_first_summary_refresh_p99", a.sumP99, "ms");
    report("scale_last_summary_refresh_p99", b.sumP99, "ms");
    report("scale_first_candidate_look_p99", a.pickP99, "ms");
    report("scale_last_candidate_look_p99", b.pickP99, "ms");
    report("scale_reopen_half_segments", double(reopenHalfSegs), "segments");
    report("scale_reopen_half", reopenHalfMs, "ms");
    report("scale_reopen_full_segments", double(reopenFullSegs), "segments");
    report("scale_reopen_full", reopenFullMs, "ms");
    CHECK(b.segs >= target);
    // Per-merge maintenance and commit rounds (MERGE_DONE, SEAL, SWAP,
    // RETIRE, UNLINKED ride them) under 10 ms at S >= 5,000. On the
    // in-memory host that is their bookkeeping; on a real directory (--dir)
    // they also wait on fsync and file writes, which the disk sets, so there
    // they are reported only.
    if (dir.empty()) {
        CHECK(b.commitP99 < 10.0);
        CHECK(b.maintP99 < 10.0);
    }
    // The bookkeeping itself is flat in S: at S >= 5,000 within 2x (plus
    // 50 us of timer and load noise) of what it costs at S = 235.
    CHECK(b.sumP99 <= 2.0 * a.sumP99 + 0.05);
    CHECK(b.pickP99 <= 2.0 * a.pickP99 + 0.05);
}

// ---------------------------------------------------------------------------
// Every fast test of the suite again, with gBookCheck: the whole T1-T3
// coverage (crash harnesses, compaction, quota, hot split, readers) runs with
// the ledger compared to the linear one and every lookup, summary refresh and
// candidate pick compared to a walk (a mismatch aborts).
// ---------------------------------------------------------------------------
PS_SLOW_TEST(book_check_fast_suite) {
    BookCheckScope check;
    const std::string only = argStr("book-only", "");
    int ran = 0;
    for (const auto& t : registry()) {
        if (t.slow || std::string(t.name).rfind("book_", 0) == 0) continue;
        if (!only.empty() && std::string(t.name).find(only) == std::string::npos) continue;
        const int before = gFailures;
        const uint64_t t0 = monoNs();
        t.fn();
        std::printf("  book-check %s %s (%.0f ms)\n", t.name, gFailures == before ? "ok" : "FAILED",
                    double(monoNs() - t0) / 1e6);
        std::fflush(stdout);
        ran++;
    }
    report("book_check_tests", double(ran), "tests");
}
