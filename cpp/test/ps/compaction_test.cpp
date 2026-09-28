// Compaction and reclamation (T3 acceptance #1, #2; design §11, A9, A11, A12).
//
//   - compaction_removes_dead_rows_keeps_live: a compaction keeps every live
//     row with identical bytes (pseqs preserved), removes the killed ones and
//     their postings, drops tombstones only after an earlier SWAP removed their
//     targets (minor 1), reclaims the retired files, and a reopen names
//     exactly the files on disk with disk_bytes equal to their total;
//   - T3 #1: a store with 50% dead bytes compacts to <= 55% of its prior disk
//     bytes, owner commit and interactive lane p99 during compaction are
//     measured against a baseline (enforced on a quiet Linux-8 box), and
//     statements that started before a SWAP complete with their rows;
//   - T3 #2 (+ A9): CAT supersede at 20% changed records per cycle: disk bytes
//     plateau at <= 1.3x the compacted live footprint, and meta segments stay
//     at one active plus one sealed.
#include <algorithm>
#include <atomic>
#include <filesystem>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {

std::string epochOf(int i) {
    char e[40];
    std::snprintf(e, sizeof(e), "2026-05-%02dT%02d:%02d:%02dZ", 1 + (i / 3600) % 28, (i / 60) % 24, i % 60, (i * 7) % 60);
    return e;
}

std::vector<uint8_t> ommFrame(int p, int i, size_t pad) {
    return ommRecord(uint32_t(p * 1000000 + i + 1), "K" + std::to_string(p) + "-" + std::to_string(i), epochOf(i),
                     15.0 + i, pad);
}

// Requests a compaction of [seg, segEnd] (UINT32_MAX: the engine's choice)
// and waits for its SWAP. Returns the status.
int32_t compactNow(Engine* e, uint32_t pid, uint32_t seg, uint32_t segEnd, SwapResult* r,
                   uint64_t timeoutNs = 120000000000ull) {
    r->requestSeg = seg;
    r->requestSegEnd = segEnd;
    if (e->swapSegment(pid, r) != 0) return -1;
    const uint64_t t0 = monoNs();
    while (r->remaining.load() != 0 && monoNs() - t0 < timeoutNs) sleepNs(200000);
    return r->remaining.load() == 0 ? r->status : -2;
}

// Sealed segments of the durable manifest, as (seg, lastSeg).
std::vector<std::pair<uint32_t, uint32_t>> sealedSegments(Io* io, const std::string& root, uint32_t pid) {
    std::vector<std::pair<uint32_t, uint32_t>> out;
    Inspector ins(io, root);
    PartView v = ins.partition(pid);
    if (!v.ok || !v.head.manifestGen) return out;
    PathBuf mp;
    pathPartitionManifest(&mp, root.c_str(), pid, v.head.manifestGen);
    IoStats st;
    IoCtx ctx(io, &st);
    FileRef f;
    if (ctx.open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Manifest, &f) < 0) return out;
    std::vector<uint8_t> buf(size_t(ctx.size(f)));
    ManifestDesc md;
    const bool ok = ctx.read(f, buf.data(), buf.size(), 0) == int64_t(buf.size()) &&
                    decodeManifest(buf.data(), buf.size(), &md);
    ctx.close(&f);
    if (!ok) return out;
    for (const auto& s : md.segs)
        if (s.sealed && s.mergedEnd == s.endPseq) out.push_back({s.seg, s.lastSeg ? s.lastSeg : s.seg});
    return out;
}

// Every sealed segment merged (the head's L0 blocks are all in the active one).
bool waitMerged(Io* io, const std::string& root, const std::vector<uint32_t>& pids, uint64_t timeoutNs) {
    const uint64_t deadline = monoNs() + timeoutNs;
    while (monoNs() < deadline) {
        bool all = true;
        Inspector ins(io, root);
        for (uint32_t pid : pids) {
            PartView v = ins.partition(pid);
            if (!v.ok) {
                all = false;
                break;
            }
            // Rows above merged_through live in L0 blocks; sealed segments are
            // merged when their end is at or below merged_through.
            if (v.head.segFirstPseq > v.head.mergedThrough + 1) all = false;
        }
        if (all) return true;
        sleepNs(2000000);
    }
    return false;
}

// Waits until the store is at rest: nothing in flight, each partition's
// disk_bytes equal to its directory, and the bytes unchanged for 150 ms
// (three of maintenance's 50 ms looks for a compaction candidate: a wave of
// compactions has gaps between its members). Returns the checks.
bool waitSettled(Store& s, const std::vector<uint32_t>& pids, uint64_t timeoutNs, std::vector<DirCheck>* out) {
    const uint64_t deadline = monoNs() + timeoutNs;
    Io* io = s.cfg.io ? s.cfg.io : importIo();
    FaultFs* fs = s.cfg.io ? s.fs.get() : nullptr;
    uint64_t restSince = 0, restBytes = UINT64_MAX;
    for (;;) {
        out->clear();
        bool all = s.e->stats().compactInFlight == 0;
        uint64_t bytes = 0;
        for (uint32_t pid : pids) {
            DirCheck dc = checkPartitionDir(io, fs, s.root, pid);
            if (!dc.ok || dc.bytes != s.e->partitionDiskBytes(pid)) all = false;
            bytes += dc.bytes;
            out->push_back(std::move(dc));
        }
        const uint64_t now = monoNs();
        if (!all || bytes != restBytes) {
            restSince = now;
            restBytes = all ? bytes : UINT64_MAX;
        } else if (now - restSince >= 150000000ull) {
            return true;
        }
        if (monoNs() >= deadline) {
            for (size_t i = 0; i < pids.size(); i++)
                std::fprintf(stderr, "  unsettled pid %u: %s bytes %llu disk_bytes %llu in-flight %llu\n", pids[i],
                             (*out)[i].ok ? "ok" : (*out)[i].err.c_str(), (unsigned long long)(*out)[i].bytes,
                             (unsigned long long)s.e->partitionDiskBytes(pids[i]),
                             (unsigned long long)s.e->stats().compactInFlight);
            return false;
        }
        sleepNs(5000000);
    }
}

uint64_t sumBytes(const std::vector<DirCheck>& v) {
    uint64_t b = 0;
    for (const auto& d : v) b += d.bytes;
    return b;
}

// A store on the fault host, or on a real file system with --dir.
struct CStore {
    Store s{true, 2, true};
    std::string dir;
    Io* io = nullptr;
    explicit CStore(const char* sub) {
        s.cfg.mergeL0Blocks = 4;
        s.cfg.mergeMinL0Bytes = 0;
        s.cfg.reclaimGraceMs = 1;
        s.cfg.sealAgeMs = 150;
        io = s.fs.get();
        dir = argStr("dir", "");
        if (!dir.empty()) {
            dir += std::string("/") + sub;
            std::filesystem::remove_all(dir);
            std::filesystem::create_directories(dir);
            s.cfg.io = nullptr;
            s.cfg.root = dir;
            s.root = dir;
            io = importIo();
        }
    }
    ~CStore() {
        s.close();
        if (!dir.empty()) std::filesystem::remove_all(dir);
    }
    FaultFs* fs() { return dir.empty() ? s.fs.get() : nullptr; }
};

}  // namespace

// ---------------------------------------------------------------------------
PS_TEST(compaction_removes_dead_rows_keeps_live) {
    CStore cs("basic");
    Store& s = cs.s;
    s.cfg.sealBytes = 128u << 10;
    s.cfg.autoCompact = false;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("cmp0", ommType());
    const int n = 2400;
    std::vector<std::vector<uint8_t>> frames;
    {
        Producer prod(s.e.get(), pid);
        const auto attr = buildRecordAttr("cmp0", "prov", "src", "b1");
        uint64_t last = 0;
        for (int i = 0; i < n; i++) {
            frames.push_back(ommFrame(0, i, 200));
            last = send(s.e.get(), prod, frames.back(), attr, 1780000000000ll + i);
        }
        REQUIRE(prod.waitAcked(last, 60000000000ull) == 0);
        sleepNs(400000000);  // the data segment seals by age: the tombstones start a new one
        // Kill every other record (partition-level TOMB by cid).
        for (int i = 1; i < n; i += 2) {
            uint8_t cid[kCidLen];
            frameCid(frames[size_t(i)], cid);
            uint64_t rs = 0;
            REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &rs) == 0);
            last = rs;
        }
        REQUIRE(prod.waitAcked(last, 60000000000ull) == 0);
    }
    REQUIRE(waitLabeledEngine(s.e.get(), {pid}, 60000000000ull));
    // The tombstones' segment seals by age; every sealed segment merges.
    sleepNs(400000000);
    REQUIRE(waitMerged(cs.io, s.root, {pid}, 60000000000ull));
    auto segs = sealedSegments(cs.io, s.root, pid);
    REQUIRE(segs.size() >= 4);
    std::vector<DirCheck> before;
    REQUIRE(waitSettled(s, {pid}, 30000000000ull, &before));
    // Compact every sealed segment, oldest first: the tombstones' segment is
    // last, after every SWAP that removed their targets, so they go too.
    uint64_t putsDropped = 0, tombsDropped = 0, rowsKept = 0;
    for (const auto& sg : segs) {
        SwapResult r;
        CHECK_EQ(compactNow(s.e.get(), pid, sg.first, sg.second, &r), 0);
        putsDropped += r.putsDropped;
        tombsDropped += r.tombsDropped;
        rowsKept += r.rowsKept;
    }
    report("compaction_puts_dropped", double(putsDropped), "rows");
    report("compaction_tombs_dropped", double(tombsDropped), "rows");
    CHECK_EQ(putsDropped, uint64_t(n / 2));
    CHECK_EQ(tombsDropped, uint64_t(n / 2));
    std::vector<DirCheck> after;
    CHECK(waitSettled(s, {pid}, 30000000000ull, &after));
    for (const auto& d : after)
        if (!d.ok) std::fprintf(stderr, "  dir: %s\n", d.err.c_str());
    report("compaction_disk_before", double(sumBytes(before)), "bytes");
    report("compaction_disk_after", double(sumBytes(after)), "bytes");
    CHECK(sumBytes(after) < sumBytes(before));
    auto verify = [&](const char* when) {
        // Independent view: live rows have their bytes; killed pseqs read VOID.
        Inspector ins(cs.io, s.root);
        PartView v = ins.partition(pid);
        REQUIRE(v.ok);
        const Recount rc = recount(v);
        CHECK_EQ(v.head.counters.liveCount, uint64_t(n / 2));
        CHECK_EQ(v.head.counters.totalCount, rc.total);
        CHECK_EQ(v.head.counters.liveBytes, rc.liveBytes);
        CHECK_EQ(v.head.counters.tombCount, rc.tombs);
        uint64_t live = 0, voids = 0;
        for (const RecRow& row : v.rows) {
            if (row.kind == kRowVoid) voids++;
            if (row.kind != kRowPut) continue;
            live++;
            const uint64_t i = row.pseq - 1;  // pseq i+1 carried frames[i]
            if (i >= frames.size() || (i & 1)) {
                CHECK(false);
                continue;
            }
            CHECK(ins.frame(pid, row) == frames[i]);
        }
        CHECK_EQ(live, uint64_t(n / 2));
        CHECK_EQ(voids, uint64_t(n));  // n/2 killed PUTs + n/2 dropped TOMBs
        Reader reader(cs.io, s.root, LaneClass::Bulk, 1);
        Rows pr = reader.q("SELECT count(*) FROM sds_p_cmp0__OMM");
        CHECK_EQ(pr.status, 0);
        if (pr.status == 0) CHECK_EQ(pr.i(0, 0), int64_t(n / 2));
        Rows tr = reader.q("SELECT count(*) FROM OMM");
        CHECK_EQ(tr.status, 0);
        if (tr.status == 0) CHECK_EQ(tr.i(0, 0), int64_t(n / 2));
        // Postings of removed rows are gone: a killed record's object id finds nothing.
        Rows gone = reader.q("SELECT count(*) FROM sds_p_cmp0__OMM WHERE OBJECT_ID = 'K0-1'");
        CHECK_EQ(gone.status, 0);
        if (gone.status == 0) CHECK_EQ(gone.i(0, 0), 0);
        Rows kept = reader.q("SELECT count(*) FROM sds_p_cmp0__OMM WHERE OBJECT_ID = 'K0-2'");
        CHECK_EQ(kept.status, 0);
        if (kept.status == 0) CHECK_EQ(kept.i(0, 0), 1);
        (void)when;
    };
    verify("after compaction");
    // A resend of a removed record is a new record (its cid is gone).
    {
        Producer prod(s.e.get(), pid);
        const uint64_t rs = send(s.e.get(), prod, frames[1], buildRecordAttr("cmp0", "prov", "src", "b1"),
                                 1790000000000ll);
        REQUIRE(prod.waitAcked(rs, 30000000000ull) == 0);
        Reader reader(cs.io, s.root, LaneClass::Bulk, 1);
        Rows r = reader.q("SELECT count(*) FROM sds_p_cmp0__OMM WHERE OBJECT_ID = 'K0-1'");
        CHECK_EQ(r.status, 0);
        if (r.status == 0) CHECK_EQ(r.i(0, 0), 1);
        // Kill it again so the verify below sees the same live set.
        uint8_t cid[kCidLen];
        frameCid(frames[1], cid);
        uint64_t k = 0;
        REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &k) == 0);
        REQUIRE(prod.waitAcked(k, 30000000000ull) == 0);
    }
    // Reopen: the writer reads the compacted files; exactly the named files exist.
    s.close();
    REQUIRE(s.open() == 0);
    std::vector<DirCheck> reopened;
    CHECK(waitSettled(s, {pid}, 30000000000ull, &reopened));
    for (const auto& d : reopened) {
        if (!d.ok) std::fprintf(stderr, "  dir after reopen: %s\n", d.err.c_str());
        CHECK_EQ(d.bytes, s.e->partitionDiskBytes(pid));
    }
    Inspector ins(cs.io, s.root);
    PartView v = ins.partition(pid);
    REQUIRE(v.ok);
    CHECK_EQ(v.head.counters.liveCount, uint64_t(n / 2));
    s.close();
}

// ---------------------------------------------------------------------------
namespace {

struct LatencySeries {
    std::mutex mu;
    std::vector<double> ms;
    void add(double v) {
        std::lock_guard<std::mutex> g(mu);
        ms.push_back(v);
    }
    double p99() {
        std::lock_guard<std::mutex> g(mu);
        if (ms.empty()) return 0;
        std::vector<double> v = ms;
        std::sort(v.begin(), v.end());
        return v[std::min(v.size() - 1, size_t(double(v.size()) * 0.99))];
    }
    void clear() {
        std::lock_guard<std::mutex> g(mu);
        ms.clear();
    }
};

void runHalfDead(int parts, int perPart, uint64_t minPreSwap, uint64_t maxSeconds) {
    CStore cs("halfdead");
    Store& s = cs.s;
    s.cfg.sealBytes = 512u << 10;
    s.cfg.autoCompact = false;
    s.cfg.lockStats = true;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &mpeType()});
    std::vector<uint32_t> pids;
    std::vector<std::string> tokens;
    std::vector<std::vector<std::vector<uint8_t>>> frames(static_cast<size_t>(parts));
    for (int p = 0; p < parts; p++) {
        tokens.push_back("hd" + std::to_string(p));
        pids.push_back(s.partition(tokens.back(), ommType()));
    }
    const uint32_t livePid = s.partition("hdlive", mpeType());  // the latency probe's partition
    for (int p = 0; p < parts; p++) {
        Producer prod(s.e.get(), pids[size_t(p)]);
        const auto attr = buildRecordAttr(tokens[size_t(p)], "prov", "src", "b1");
        uint64_t last = 0;
        for (int i = 0; i < perPart; i++) {
            frames[size_t(p)].push_back(ommFrame(p, i, 320));
            last = send(s.e.get(), prod, frames[size_t(p)].back(), attr, 1780000000000ll + i);
        }
        REQUIRE(prod.waitAcked(last, 120000000000ull) == 0);
    }
    sleepNs(400000000);  // data segments seal by age: the tombstones start new ones
    for (int p = 0; p < parts; p++) {
        Producer prod(s.e.get(), pids[size_t(p)]);
        uint64_t last = 0;
        for (int i = 1; i < perPart; i += 2) {
            uint8_t cid[kCidLen];
            frameCid(frames[size_t(p)][size_t(i)], cid);
            uint64_t rs = 0;
            REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &rs) == 0);
            last = rs;
        }
        REQUIRE(prod.waitAcked(last, 120000000000ull) == 0);
    }
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 120000000000ull));
    sleepNs(400000000);
    REQUIRE(waitMerged(cs.io, s.root, pids, 120000000000ull));
    std::vector<DirCheck> before;
    REQUIRE(waitSettled(s, pids, 60000000000ull, &before));
    uint64_t live = 0, total = 0;
    for (uint32_t pid : pids) {
        Inspector ins(cs.io, s.root);
        PartView v = ins.partition(pid);
        live += v.head.counters.liveBytes;
        total += v.head.counters.totalBytes;
    }
    report("halfdead_dead_share_of_record_bytes", 1.0 - double(live) / double(total), "fraction");
    // Load while compacting: a paced producer on another partition (owner
    // commit and ack latency) and interactive statements (lane latency);
    // bulk statements over the compacted partitions verify their rows.
    ReaderConfig ic;
    ic.root = s.root;
    ic.io = cs.io;
    ic.cls = LaneClass::Interactive;
    ic.lanes = 2;
    Reader interactive(ic);
    ReaderConfig bc = ic;
    bc.cls = LaneClass::Bulk;
    bc.lanes = 3;
    bc.ringBytes = 64u << 10;
    Reader bulk(bc);
    REQUIRE(interactive.inst && bulk.inst);
    // A12: retired files wait for the bulk statements that may still read them.
    struct Gates {
        ReaderInstance* a;
        ReaderInstance* b;
    } gates{interactive.inst.get(), bulk.inst.get()};
    s.e->setReaderGate(
        [](void* ctx) -> uint64_t {
            const Gates* g = static_cast<const Gates*>(ctx);
            return std::min(g->a->oldestActiveStart(), g->b->oldestActiveStart());
        },
        &gates);
    std::atomic<bool> stop{false};
    LatencySeries ack, lane;
    std::atomic<uint64_t> mpeSeq{0};
    std::thread producer([&] {
        Producer prod(s.e.get(), livePid);
        const auto attr = buildRecordAttr("hdlive", "prov", "src", "b1");
        while (!stop.load()) {
            const uint64_t i = mpeSeq++;
            const uint64_t t0 = monoNs();
            const uint64_t rs = send(s.e.get(), prod, mpeRecord("L" + std::to_string(i), 1.7e9 + double(i), 1.0),
                                     attr, 1790000000000ll + int64_t(i));
            if (rs && prod.waitAcked(rs, 30000000000ull) == 0) ack.add(double(monoNs() - t0) / 1e6);
            sleepNs(2000000);
        }
    });
    std::thread asker([&] {
        std::mt19937_64 rng(3);
        while (!stop.load()) {
            const int p = int(rng() % uint64_t(parts));
            const int i = int(rng() % uint64_t(perPart)) & ~1;
            const uint64_t t0 = monoNs();
            Rows r = interactive.q("SELECT _pseq FROM sds_p_" + tokens[size_t(p)] + "__OMM WHERE _pseq BETWEEN " +
                                   std::to_string(i + 1) + " AND " + std::to_string(i + 101));
            if (r.status == 0) lane.add(double(monoNs() - t0) / 1e6);
            sleepNs(1000000);
        }
    });
    std::atomic<uint64_t> statements{0}, errors{0}, mismatches{0};
    std::mutex spanMu;
    std::vector<std::pair<uint64_t, uint64_t>> spans;
    std::vector<uint64_t> swapTimes;
    std::string firstErr;
    std::vector<std::thread> readers;
    for (int t = 0; t < 3; t++)
        readers.emplace_back([&, t] {
            std::mt19937_64 rng(uint64_t(t) + 17);
            while (!stop.load()) {
                const int p = int(rng() % uint64_t(parts));
                const uint64_t t0 = monoNs();
                Rows r = bulk.q("SELECT _pseq, _data FROM sds_p_" + tokens[size_t(p)] + "__OMM", {}, 0,
                                (rng() % 3 == 0) ? 20000 : 0, 16u << 10);
                statements++;
                {
                    std::lock_guard<std::mutex> g(spanMu);
                    spans.push_back({t0, monoNs()});
                }
                if (r.status != 0) {
                    errors++;
                    std::lock_guard<std::mutex> g(spanMu);
                    if (firstErr.empty()) firstErr = r.error + " (" + std::to_string(r.status) + ")";
                    continue;
                }
                // Exactly the live half, in pseq order, bytes as sent.
                bool ok = r.rows.size() == size_t(perPart / 2);
                for (size_t k = 0; ok && k < r.rows.size(); k++) {
                    const int64_t ps = r.i(k, 0);
                    const auto& f = frames[size_t(p)][size_t(ps - 1)];
                    ok = ps == int64_t(2 * k + 1) &&
                         r.s(k, 1) == std::string(reinterpret_cast<const char*>(f.data()) + 4, f.size() - 4);
                }
                if (!ok) mismatches++;
            }
        });
    const double windowS = 3;
    sleepNs(uint64_t(windowS * 1e9));
    const double ackBase = ack.p99(), laneBase = lane.p99();
    const double commitBase = double(s.e->commitHist().percentileNs(0.99)) / 1e6;
    ack.clear();
    lane.clear();
    // Compact every sealed segment of every partition (oldest first).
    const uint64_t c0 = monoNs();
    uint64_t swaps = 0, dropped = 0;
    for (uint32_t pid : pids) {
        for (const auto& sg : sealedSegments(cs.io, s.root, pid)) {
            SwapResult r;
            if (compactNow(s.e.get(), pid, sg.first, sg.second, &r) == 0) {
                swaps++;
                dropped += r.putsDropped;
                std::lock_guard<std::mutex> g(spanMu);
                swapTimes.push_back(monoNs());
            }
        }
    }
    const double compactS = double(monoNs() - c0) / 1e9;
    std::vector<DirCheck> after;
    const bool settled = waitSettled(s, pids, 120000000000ull, &after);
    CHECK(settled);
    for (const auto& d : after)
        if (!d.ok) std::fprintf(stderr, "  dir: %s\n", d.err.c_str());
    const double ratio = double(sumBytes(after)) / double(sumBytes(before));
    // More SWAPs (re-compactions) until enough statements started before one.
    auto preSwap = [&]() -> uint64_t {
        std::lock_guard<std::mutex> g(spanMu);
        uint64_t n = 0;
        for (const auto& sp : spans)
            for (uint64_t t : swapTimes)
                if (t > sp.first && t < sp.second) {
                    n++;
                    break;
                }
        return n;
    };
    const uint64_t until = monoNs() + maxSeconds * 1000000000ull;
    size_t k = 0;
    while (preSwap() < minPreSwap && monoNs() < until) {
        SwapResult r;
        if (compactNow(s.e.get(), pids[k++ % pids.size()], UINT32_MAX, 0, &r) == 0) {
            swaps++;
            std::lock_guard<std::mutex> g(spanMu);
            swapTimes.push_back(monoNs());
        }
    }
    // Latency during compaction: the whole period SWAPs ran in.
    const double ackDuring = ack.p99(), laneDuring = lane.p99();
    const size_t ackSamples = ack.ms.size(), laneSamples = lane.ms.size();
    stop = true;
    producer.join();
    asker.join();
    for (auto& t : readers) t.join();
    s.e->setReaderGate(nullptr, nullptr);
    report("halfdead_latency_samples_compacting", double(ackSamples + laneSamples), "samples");
    report("halfdead_disk_before", double(sumBytes(before)), "bytes");
    report("halfdead_disk_after", double(sumBytes(after)), "bytes");
    report("halfdead_disk_ratio", ratio, "x");
    report("halfdead_compaction_seconds", compactS, "s");
    report("halfdead_swaps", double(swaps), "swaps");
    report("halfdead_puts_dropped", double(dropped), "rows");
    report("halfdead_ack_p99_baseline_ms", ackBase, "ms");
    report("halfdead_ack_p99_compacting_ms", ackDuring, "ms");
    report("halfdead_commit_round_p99_ms", commitBase, "ms");
    report("halfdead_lane_p99_baseline_ms", laneBase, "ms");
    report("halfdead_lane_p99_compacting_ms", laneDuring, "ms");
    report("halfdead_statements", double(statements.load()), "statements");
    report("halfdead_statements_started_before_a_swap", double(preSwap()), "statements");
    if (!firstErr.empty()) std::fprintf(stderr, "  first error: %s\n", firstErr.c_str());
    CHECK(ratio <= 0.55);
    CHECK_EQ(errors.load(), uint64_t(0));
    CHECK_EQ(mismatches.load(), uint64_t(0));
    CHECK(preSwap() >= minPreSwap);
    unsigned threads = 0;
    double load = 0;
    std::string why;
    if (latencyBoxQuiet(&threads, &load, &why)) {
        CHECK(ackDuring <= 1.5 * ackBase);
        CHECK(laneDuring <= 1.5 * laneBase);
    } else {
        std::fprintf(stderr, "  NOTE latency bounds not enforced: %s\n", why.c_str());
    }
    s.close();
}

}  // namespace

PS_TEST(compaction_half_dead_T3_1) {
    runHalfDead(int(argInt("parts", 3)), int(argInt("records", 6000)), uint64_t(argInt("pre_swap", 100)),
                uint64_t(argInt("max_seconds", 60)));
}
PS_SLOW_TEST(compaction_half_dead_T3_1_full) {
    runHalfDead(int(argInt("parts", 4)), int(argInt("records", 20000)), uint64_t(argInt("pre_swap", 1000)),
                uint64_t(argInt("max_seconds", 600)));
}

// ---------------------------------------------------------------------------
namespace {

void runCatPlateau(int objects, int cycles) {
    CStore cs("plateau");
    Store& s = cs.s;
    s.cfg.sealBytes = 256u << 10;
    s.cfg.autoCompact = true;
    if (argInt("dead-pct", 0) > 0) s.cfg.compactDeadRatio = double(argInt("dead-pct", 0)) / 100.0;
    REQUIRE(s.open() == 0);
    s.registerTypes({&catType()});
    const uint32_t pid = s.partition("cat0", catType());
    std::vector<int> version(static_cast<size_t>(objects), 0);
    auto record = [&](int o) {
        return catRecord(uint32_t(o + 1), "O" + std::to_string(o), "", "",
                         "NAME-" + std::to_string(o) + "-v" + std::to_string(version[size_t(o)]) + std::string(120, 'x'));
    };
    Producer prod(s.e.get(), pid);
    const auto attr = buildRecordAttr("cat0", "prov", "catsrc", "b1");
    auto sendAll = [&](const std::vector<int>& objs, int64_t at) {
        uint64_t last = 0;
        for (int o : objs) last = send(s.e.get(), prod, record(o), attr, at++);
        return last ? prod.waitAcked(last, 120000000000ull) == 0 : true;
    };
    std::vector<int> all(static_cast<size_t>(objects));
    for (int o = 0; o < objects; o++) all[size_t(o)] = o;
    REQUIRE(sendAll(all, 1780000000000ll));
    REQUIRE(waitLabeledEngine(s.e.get(), {pid}, 60000000000ull));
    sleepNs(400000000);
    REQUIRE(waitMerged(cs.io, s.root, {pid}, 60000000000ull));
    // The live set's compacted footprint: every sealed segment coalesced.
    {
        auto segs = sealedSegments(cs.io, s.root, pid);
        if (!segs.empty()) {
            SwapResult r;
            CHECK_EQ(compactNow(s.e.get(), pid, segs.front().first, segs.back().second, &r), 0);
        }
    }
    std::vector<DirCheck> d0;
    REQUIRE(waitSettled(s, {pid}, 60000000000ull, &d0));
    const uint64_t base = sumBytes(d0);
    uint64_t liveBytes0 = 0;
    {
        Inspector ins(cs.io, s.root);
        liveBytes0 = ins.partition(pid).head.counters.liveBytes;
    }
    std::mt19937_64 rng(11);
    double plateau = 0, plateauLive = 0;
    uint64_t maxMFiles = 0, maxMBytes = 0;
    int64_t at = 1781000000000ll;
    for (int c = 0; c < cycles; c++) {
        std::vector<int> pick;
        for (int o = 0; o < objects; o++)
            if (rng() % 5 == 0) pick.push_back(o);
        for (int o : pick) version[size_t(o)]++;
        REQUIRE(sendAll(pick, at));
        at += int64_t(pick.size());
        REQUIRE(waitLabeledEngine(s.e.get(), {pid}, 60000000000ull));
        std::vector<DirCheck> dc;
        waitSettled(s, {pid}, 10000000000ull, &dc);
        Inspector ins(cs.io, s.root);
        PartView v = ins.partition(pid);
        CHECK_EQ(v.head.counters.liveCount, uint64_t(objects));
        const double ratio = double(sumBytes(dc)) / double(base);
        const double ratioLive = double(sumBytes(dc)) / double(v.head.counters.liveBytes);
        // A9: meta segments on disk (the pre-created next one is empty).
        uint64_t mFiles = 0, mBytes = 0;
        for (uint32_t sg = v.head.firstLiveMSeg; sg <= v.head.mSeg; sg++) {
            PathBuf mp;
            pathPartitionSeg(&mp, s.root.c_str(), pid, 'm', sg, "fsl");
            IoStats st;
            IoCtx ctx(cs.io, &st);
            FileRef f;
            if (ctx.open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Meta, &f) < 0) continue;
            mFiles++;
            mBytes += uint64_t(ctx.size(f));
            ctx.close(&f);
        }
        if (getenv("PS_PLATEAU_TRACE")) {
            std::map<std::string, uint64_t> by;
            if (cs.fs())
                for (const std::string& path : cs.fs()->list(s.root + "/fsql2/p/")) {
                    IoStats st2;
                    IoCtx ctx(cs.io, &st2);
                    FileRef f;
                    if (ctx.open(path.c_str(), path.size(), FLATSQL_IO_READ, FileClass::Store, &f) < 0) continue;
                    const std::string base = path.substr(path.rfind('/') + 1);
                    by[base.substr(0, base.find_first_of("-."))] += uint64_t(ctx.size(f));
                    ctx.close(&f);
                }
            std::fprintf(stderr, "  cycle %d ratio %.3f dead %llu:", c, ratio,
                         (unsigned long long)(v.head.counters.totalBytes - v.head.counters.liveBytes));
            for (auto& kv : by) std::fprintf(stderr, " %s=%llu", kv.first.c_str(), (unsigned long long)kv.second);
            std::fprintf(stderr, "\n");
        }
        if (c >= cycles / 5) {
            plateau = std::max(plateau, ratio);
            plateauLive = std::max(plateauLive, ratioLive);
            maxMFiles = std::max(maxMFiles, mFiles);
            maxMBytes = std::max(maxMBytes, mBytes);
        }
    }
    const EngineStats st = s.e->stats();
    report("plateau_objects", double(objects), "objects");
    report("plateau_cycles", double(cycles), "cycles");
    report("plateau_live_footprint_bytes", double(base), "bytes");
    report("plateau_live_record_bytes", double(liveBytes0), "bytes");
    report("plateau_disk_over_live_footprint_max", plateau, "x");
    report("plateau_disk_over_live_record_bytes_max", plateauLive, "x");
    report("plateau_meta_segments_on_disk_max", double(maxMFiles), "files");
    report("plateau_meta_bytes_max", double(maxMBytes), "bytes");
    report("plateau_compactions", double(st.compactions), "compactions");
    report("plateau_meta_segments_retired", double(st.metaSegsRetired), "segments");
    report("plateau_files_unlinked", double(st.unlinkedFiles), "files");
    CHECK(plateau <= 1.3);
    CHECK(maxMFiles <= 2);
    s.close();
}

}  // namespace

PS_TEST(compaction_cat_supersede_plateau_T3_2) {
    runCatPlateau(int(argInt("objects", 3000)), int(argInt("cycles", 20)));
}
PS_SLOW_TEST(compaction_cat_supersede_plateau_T3_2_full) {
    runCatPlateau(int(argInt("objects", 20000)), int(argInt("cycles", 100)));
}

// ---------------------------------------------------------------------------
// Type logs (A9, A12, A15): under ingest and kills, with readers running
// type-level statements the whole time, the catalog runs a merge replaces,
// the manifests they were named in and the sealed type meta segments are
// unlinked behind the reader gate, copies killed within a fold leave the
// catalog, no statement fails or sees a wrong result, and the type directory
// holds exactly what the head and manifest name (also after a reopen).
PS_TEST(type_logs_reclaimed_under_readers) {
    CStore cs("typelogs");
    Store& s = cs.s;
    s.cfg.typeMetaSegBytes = 16u << 10;
    s.cfg.sealBytes = 64u << 10;
    s.cfg.sealAgeMs = 50;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const int parts = 3;
    std::vector<uint32_t> pids;
    std::vector<std::string> tokens;
    for (int p = 0; p < parts; p++) {
        tokens.push_back("tl" + std::to_string(p));
        pids.push_back(s.partition(tokens.back(), ommType()));
    }
    std::vector<std::unique_ptr<Producer>> prods;
    for (uint32_t pid : pids) prods.emplace_back(new Producer(s.e.get(), pid));
    const auto attr = buildRecordAttr("tl", "prov", "src", "b1");
    std::vector<std::vector<std::vector<uint8_t>>> live(parts);  // frames of live records
    uint64_t live0 = 0;
    std::mt19937_64 rng(11);
    int next = 0;
    // One round: `n` new records per partition, then kills of `killPct`% of
    // the live ones (by cid), all acked.
    auto round = [&](int n, int killPct, std::vector<std::vector<uint8_t>>* killed) {
        for (int p = 0; p < parts; p++) {
            uint64_t last = 0;
            for (int i = 0; i < n; i++) {
                auto f = ommFrame(p, next++, 120);
                last = send(s.e.get(), *prods[size_t(p)], f, attr, 1780000000000ll + next, false);
                if (last) live[size_t(p)].push_back(std::move(f));
            }
            std::vector<std::vector<uint8_t>> keep;
            for (auto& f : live[size_t(p)]) {
                if (int(rng() % 100) >= killPct) {
                    keep.push_back(std::move(f));
                    continue;
                }
                uint8_t cid[kCidLen];
                frameCid(f, cid);
                uint64_t rs = 0;
                if (prods[size_t(p)]->enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &rs, false) == 0) {
                    last = rs;
                    if (killed) killed->push_back(std::move(f));
                } else {
                    keep.push_back(std::move(f));
                }
            }
            live[size_t(p)].swap(keep);
            if (last) REQUIRE(prods[size_t(p)]->waitAcked(last, 60000000000ull) == 0);
        }
    };
    // Keepers (never killed) and victims (killed before any reader starts).
    std::vector<std::vector<uint8_t>> victims;
    round(300, 50, &victims);
    std::vector<std::vector<uint8_t>> keepers;
    for (auto& v : live) {
        for (auto& f : v) keepers.push_back(f);
        v.clear();  // keepers are never killed: out of the churn
    }
    live0 = keepers.size();
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 60000000000ull));
    REQUIRE(waitTypeVisible(cs.io, s.root, ommType().fid, pids, 60000000000ull));
    ReaderConfig ic;
    ic.root = s.root;
    ic.io = cs.io;
    ic.cls = LaneClass::Interactive;
    ic.lanes = 2;
    Reader rd(ic);
    REQUIRE(rd.inst);
    s.e->setReaderGate([](void* ctx) -> uint64_t { return static_cast<ReaderInstance*>(ctx)->oldestActiveStart(); },
                       rd.inst.get());
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> statements{0}, errors{0}, wrong{0};
    std::mutex errMu;
    std::string firstErr;
    std::vector<std::thread> readers;
    for (int t = 0; t < 2; t++)
        readers.emplace_back([&, t] {
            std::mt19937_64 r2(uint64_t(t) + 5);
            while (!stop.load()) {
                const bool wantLive = (r2() & 1) != 0;
                const auto& pool = wantLive ? keepers : victims;
                const auto& f = pool[r2() % pool.size()];
                const Rows c = rd.q("SELECT count(*) FROM OMM WHERE _cid = ?", {Param::text(cidTextOf(f))});
                statements++;
                if (c.status != 0 || c.rows.size() != 1) {
                    errors++;
                    std::lock_guard<std::mutex> g(errMu);
                    if (firstErr.empty()) firstErr = c.error + " (" + std::to_string(c.status) + ")";
                    continue;
                }
                if (c.i(0, 0) != (wantLive ? 1 : 0)) wrong++;
            }
        });
    // Churn: merges fold catalog runs, kills leave dead copies to fold out,
    // the type meta log rotates.
    const uint64_t t0 = monoNs();
    int rounds = 0;
    while (monoNs() - t0 < uint64_t(argInt("seconds", 3)) * 1000000000ull) {
        round(200, 60, nullptr);
        rounds++;
    }
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 60000000000ull));
    REQUIRE(waitTypeVisible(cs.io, s.root, ommType().fid, pids, 60000000000ull));
    uint64_t liveNow = live0;
    for (const auto& v : live) liveNow += v.size();
    // Every retired type file goes once the readers stop holding it.
    stop = true;
    for (auto& t : readers) t.join();
    DirCheck tc;
    for (const uint64_t t1 = monoNs(); monoNs() - t1 < 30000000000ull; sleepNs(20000000)) {
        tc = checkTypeDir(cs.io, cs.fs(), s.root, ommType().fid);
        if (tc.ok) break;
    }
    if (!tc.ok) std::fprintf(stderr, "  type dir: %s\n", tc.err.c_str());
    // The type's disk bytes are its directory's (maintenance publishes them).
    bool bytesMatch = false;
    for (const uint64_t t1 = monoNs(); !bytesMatch && monoNs() - t1 < 10000000000ull; sleepNs(20000000))
        bytesMatch = checkTypeDir(cs.io, cs.fs(), s.root, ommType().fid).bytes == s.e->typeDiskBytesOf(ommType().fid);
    const Rows cnt = rd.q("SELECT first_live_count FROM flatsql_types WHERE type = 'OMM'");
    s.e->setReaderGate(nullptr, nullptr);
    const EngineStats st = s.e->stats();
    Inspector ins(cs.io, s.root);
    const auto tv = ins.type(ommType().fid);
    uint64_t runBytes = 0;
    {
        PathBuf mp;
        if (tv.head.manifestGen) {
            pathTypeManifest(&mp, s.root.c_str(), ommType().fid, tv.head.manifestGen);
            IoStats ios;
            IoCtx ctx(cs.io, &ios);
            FileRef f;
            if (ctx.open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Manifest, &f) >= 0) {
                std::vector<uint8_t> man(size_t(ctx.size(f)));
                if (ctx.read(f, man.data(), man.size(), 0) == int64_t(man.size()) && man.size() >= 24)
                    for (uint32_t i = 0; i < getU32(man.data() + 4); i++) runBytes += getU64(man.data() + 24 + i * 16);
                ctx.close(&f);
            }
        }
    }
    report("typelogs_rounds", double(rounds), "rounds");
    report("typelogs_records_sent", double(next), "records");
    report("typelogs_records_live", double(liveNow), "records");
    report("typelogs_statements", double(statements.load()), "statements");
    report("typelogs_catalog_entries_dropped", double(st.catalogEntriesDropped), "entries");
    report("typelogs_catalog_run_bytes", double(runBytes), "bytes");
    report("typelogs_catalog_run_bytes_per_live_record", double(runBytes) / double(std::max<uint64_t>(liveNow, 1)),
           "bytes");
    report("typelogs_type_dir_bytes", double(tc.bytes), "bytes");
    report("typelogs_first_live_m_seg", double(tv.head.firstLiveMSeg), "segment");
    if (!firstErr.empty()) std::fprintf(stderr, "  first error: %s\n", firstErr.c_str());
    CHECK(tc.ok);
    if (!bytesMatch)
        std::fprintf(stderr, "  type dir %llu bytes, type disk bytes %llu\n",
                     (unsigned long long)checkTypeDir(cs.io, cs.fs(), s.root, ommType().fid).bytes,
                     (unsigned long long)s.e->typeDiskBytesOf(ommType().fid));
    CHECK(bytesMatch);
    CHECK_EQ(errors.load(), uint64_t(0));
    CHECK_EQ(wrong.load(), uint64_t(0));
    CHECK(statements.load() > 0);
    CHECK(cnt.status == 0 && cnt.rows.size() == 1 && uint64_t(cnt.i(0, 0)) == liveNow);
    CHECK(st.catalogEntriesDropped > 0);
    CHECK(tv.head.firstLiveMSeg > 0);  // sealed type meta segments were retired
    // A reopen finds the same files (checked before the engine starts: a
    // merge it would plan at once is not an orphan), and the catalog still
    // answers.
    s.close();
    {
        std::string err;
        REQUIRE(Engine::open(s.cfg, &s.e, &err) == 0);
    }
    const DirCheck tc2 = checkTypeDir(cs.io, cs.fs(), s.root, ommType().fid);
    if (!tc2.ok) std::fprintf(stderr, "  type dir after reopen: %s\n", tc2.err.c_str());
    CHECK(tc2.ok);
    CHECK_EQ(tc2.bytes, s.e->typeDiskBytesOf(ommType().fid));
    s.e->start();
    Reader rd2(ic);
    int wrong2 = 0;
    for (int i = 0; i < 64; i++) {
        const bool wantLive = (i & 1) != 0;
        const auto& pool = wantLive ? keepers : victims;
        const Rows c = rd2.q("SELECT count(*) FROM OMM WHERE _cid = ?",
                             {Param::text(cidTextOf(pool[rng() % pool.size()]))});
        if (c.status != 0 || c.rows.size() != 1 || c.i(0, 0) != (wantLive ? 1 : 0)) wrong2++;
    }
    CHECK_EQ(wrong2, 0);
}

// ---------------------------------------------------------------------------
// A12 throughput: a backlog of retired files goes about one grace after the
// reader gate passes, not one grace per file. Every file needs two passing
// gate checks a grace apart; each maintenance pass takes the first check of
// every file past the gate. (Taking one file's first check per pass made a
// backlog of N files wait N graces: 60 s each in production.)
PS_TEST(reclaim_backlog_goes_in_one_grace) {
    CStore cs("backlog");
    Store& s = cs.s;
    s.cfg.sealBytes = 32u << 10;  // many segments: many SWAPs, many files
    s.cfg.autoCompact = false;
    s.cfg.reclaimGraceMs = 300;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("backlog", ommType());
    {
        Producer prod(s.e.get(), pid);
        const auto attr = buildRecordAttr("backlog", "prov", "src", "b1");
        uint64_t last = 0;
        std::vector<std::vector<uint8_t>> frames;
        for (int i = 0; i < 2000; i++) {
            frames.push_back(ommFrame(0, i, 200));
            last = send(s.e.get(), prod, frames.back(), attr, 1780000000000ll + i);
        }
        for (int i = 1; i < 2000; i += 2) {
            uint8_t cid[kCidLen];
            frameCid(frames[size_t(i)], cid);
            uint64_t rs = 0;
            REQUIRE(prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &rs) == 0);
            last = rs;
        }
        REQUIRE(prod.waitAcked(last, 60000000000ull) == 0);
    }
    REQUIRE(waitLabeledEngine(s.e.get(), {pid}, 60000000000ull));
    sleepNs(400000000);
    REQUIRE(waitMerged(cs.io, s.root, {pid}, 60000000000ull));
    // A reader statement holds the gate while every segment is compacted.
    Reader rd(s, LaneClass::Bulk, 1);
    std::atomic<uint64_t> held{0};
    s.e->setReaderGate([](void* ctx) -> uint64_t { return static_cast<std::atomic<uint64_t>*>(ctx)->load(); }, &held);
    held = monoNs();
    uint64_t swaps = 0;
    for (const auto& sg : sealedSegments(cs.io, s.root, pid)) {
        SwapResult r;
        if (compactNow(s.e.get(), pid, sg.first, sg.second, &r) == 0) swaps++;
    }
    const EngineStats st0 = s.e->stats();
    const uint64_t backlog = st0.retiredFiles - st0.unlinkedFiles;
    // The statement ends: the whole backlog goes.
    const uint64_t t0 = monoNs();
    held = UINT64_MAX;
    uint64_t left = backlog;
    while (monoNs() - t0 < 60000000000ull) {
        const EngineStats st = s.e->stats();
        left = st.retiredFiles - st.unlinkedFiles;
        if (!left) break;
        sleepNs(5000000);
    }
    const double seconds = double(monoNs() - t0) / 1e9;
    s.e->setReaderGate(nullptr, nullptr);
    report("backlog_swaps", double(swaps), "swaps");
    report("backlog_files", double(backlog), "files");
    report("backlog_seconds_to_unlink", seconds, "s");
    CHECK(backlog >= 40);
    CHECK_EQ(left, uint64_t(0));
    // Two gate checks a grace apart (0.3 s), then 64 unlinks a pass: well
    // under the backlog's count of graces (the defect's cost).
    CHECK(seconds < double(backlog) * 0.3 / 4);
}
