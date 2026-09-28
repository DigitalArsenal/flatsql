// Snapshots (T2 acceptance #5, A12):
//   - 1,000 compaction SWAPs under load: statements that started before a
//     SWAP complete with their pre-SWAP rows, 0 errors. The compactor is the
//     writer's SWAP seam (Engine::swapSegment, a verbatim copier standing in
//     for T3); the reclaimer is the test, unlinking retired files only once
//     every reader announcement is newer than the SWAP (checked twice, A12).
//   - the negative control: unlinking at once breaks a statement that still
//     had to open a retired file (FLATSQL_SNAPSHOT_GONE, retryable) — the
//     announcement protocol is what prevents it;
//   - A12: a bulk export running continuously under continuous compaction
//     sees 0 SNAPSHOT_GONE (10 minutes in the slow variant).
#include <algorithm>
#include <atomic>
#include <deque>
#include <filesystem>
#include <mutex>
#include <random>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {

std::vector<uint8_t> rec(int p, int i) {
    char epoch[40];
    std::snprintf(epoch, sizeof(epoch), "2026-05-%02dT%02d:%02d:%02dZ", 1 + (i / 3600) % 28, (i / 60) % 24, i % 60,
                  (i * 7) % 60);
    return ommRecord(uint32_t(p * 1000000 + i + 1), "P" + std::to_string(p) + "-" + std::to_string(i), epoch, 15.0 + i,
                     256);
}

struct Harness {
    Store s{false, 2, true};
    std::vector<uint32_t> pids;
    std::vector<std::string> tokens;
    std::vector<std::atomic<int>> sent;  // records acked per partition
    Io* io = nullptr;
    std::string dir;
    // --dir=<path>: a real file system (the native host); retired files are
    // really unlinked, so long runs stay bounded on disk. Default: in memory.
    explicit Harness(int parts) : sent(size_t(parts)) {
        s.cfg.sealBytes = 256u << 10;  // many sealed segments
        s.cfg.mergeL0Blocks = 4;
        io = s.fs.get();
        dir = argStr("dir", "");
        if (!dir.empty()) {
            std::filesystem::remove_all(dir);
            std::filesystem::create_directories(dir);
            s.cfg.io = nullptr;
            s.cfg.root = dir;
            s.root = dir;
            io = importIo();
        }
    }
    ~Harness() {
        if (!dir.empty()) {
            s.close();
            std::filesystem::remove_all(dir);
        }
    }
    void fill(int perPart) {
        s.registerTypes({&ommType()});
        for (size_t p = 0; p < sent.size(); p++) {
            tokens.push_back("snap" + std::to_string(p));
            pids.push_back(s.partition(tokens.back(), ommType()));
        }
        for (size_t p = 0; p < sent.size(); p++) ingest(int(p), perPart);
    }
    void ingest(int p, int n) {
        Producer prod(s.e.get(), pids[size_t(p)]);
        const auto attr = buildRecordAttr(tokens[size_t(p)], "prov", "src", "b1");
        uint64_t last = 0;
        const int from = sent[size_t(p)].load();
        for (int i = from; i < from + n; i++) last = send(s.e.get(), prod, rec(p, i), attr, 1780000000000ll + i);
        if (last) CHECK_EQ(prod.waitAcked(last, 60000000000ull), 0);
        sent[size_t(p)] += n;
    }
};

struct Retired {
    uint32_t pid;
    uint32_t seg;
    uint32_t cgen;
    uint64_t publishedNs;
    uint64_t firstCheckNs = 0;
};

void unlinkRetired(Harness& h, const Retired& r) {
    IoStats st;
    IoCtx io(h.io, &st);
    const char* exts[3] = {"fsd", "fsr", "fsa"};
    const char letters[3] = {'d', 'r', 'a'};
    for (int k = 0; k < 3; k++) {
        PathBuf p;
        if (r.cgen) pathPartitionCompact(&p, h.s.root.c_str(), r.pid, r.seg, r.cgen, exts[k]);
        else pathPartitionSeg(&p, h.s.root.c_str(), r.pid, letters[k], r.seg, exts[k]);
        io.unlink(p.c_str(), p.len, false);
    }
}

// Verifies a partition-level scan: pseqs 1..n contiguous, frames as sent.
bool verifyScan(const Rows& r, int p, std::string* why) {
    for (size_t i = 0; i < r.rows.size(); i++) {
        if (r.i(i, 0) != int64_t(i + 1)) {
            *why = "pseq gap";
            return false;
        }
        const auto f = rec(p, int(i));
        if (r.s(i, 1) != std::string(reinterpret_cast<const char*>(f.data()) + 4, f.size() - 4)) {
            *why = "frame differs";
            return false;
        }
    }
    return true;
}

void runSwapsUnderLoad(int targetSwaps, uint64_t maxSeconds) {
    Harness h(4);
    REQUIRE(h.s.open() == 0);
    h.fill(4000);
    REQUIRE(waitLabeledEngine(h.s.e.get(), h.pids, 60000000000ull));
    ReaderConfig rc;
    rc.root = h.s.root;
    rc.io = h.io;
    rc.cls = LaneClass::Bulk;
    rc.lanes = 3;
    rc.ringBytes = 64u << 10;
    Reader reader(rc);
    REQUIRE(reader.inst);
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> statements{0}, errors{0}, gone{0}, mismatches{0};
    std::string firstErr;
    std::mutex errMu;
    std::vector<std::pair<uint64_t, uint64_t>> spans;  // statement [start, end]
    std::vector<uint64_t> swapTimes;
    std::mutex spanMu;
    // Readers: slow partition scans that span several SWAPs.
    std::vector<std::thread> readers;
    for (int t = 0; t < 3; t++)
        readers.emplace_back([&, t] {
            std::mt19937_64 rng(uint64_t(t) + 11);
            while (!stop.load()) {
                const int p = int(rng() % h.pids.size());
                const uint64_t s0 = monoNs();
                Rows r = reader.q("SELECT _pseq, _data FROM sds_p_" + h.tokens[size_t(p)] + "__OMM", {}, 0,
                                  (rng() % 4 == 0) ? 20000 : 0, 16u << 10);
                statements++;
                {
                    std::lock_guard<std::mutex> g(spanMu);
                    spans.push_back({s0, monoNs()});
                }
                std::string why;
                if (r.status == int32_t(kRsSnapshotGone)) gone++;
                if (r.status != 0) {
                    errors++;
                    std::lock_guard<std::mutex> g(errMu);
                    if (firstErr.empty()) firstErr = r.error + " (" + std::to_string(r.status) + ")";
                } else if (!verifyScan(r, p, &why)) {
                    mismatches++;
                    std::lock_guard<std::mutex> g(errMu);
                    if (firstErr.empty()) firstErr = why;
                }
            }
        });
    // Load: ingest continues (new segments seal and merge meanwhile).
    std::thread writer([&] {
        int k = 0;
        while (!stop.load()) {
            h.ingest(k % int(h.pids.size()), 200);
            k++;
            sleepNs(2000000);
        }
    });
    // Compactor + reclaimer.
    int swaps = 0, noCandidate = 0, unlinked = 0;
    std::deque<Retired> retired;
    const uint64_t until = monoNs() + maxSeconds * 1000000000ull;
    auto reclaim = [&](bool drain) {
        while (!retired.empty()) {
            Retired& r = retired.front();
            const uint64_t oldest = reader.inst->oldestActiveStart();
            if (oldest <= r.publishedNs) {
                if (!drain) return;
                sleepNs(1000000);
                continue;
            }
            const uint64_t now = monoNs();
            if (!r.firstCheckNs) r.firstCheckNs = now;
            // A12: two checks, a grace apart (60 s in production).
            if (now - r.firstCheckNs < 2000000ull) {
                if (!drain) return;
                sleepNs(500000);
                continue;
            }
            unlinkRetired(h, r);
            unlinked++;
            retired.pop_front();
        }
    };
    int k = 0;
    while (swaps < targetSwaps && monoNs() < until) {
        const uint32_t pid = h.pids[size_t(k++) % h.pids.size()];
        SwapResult res;
        REQUIRE(h.s.e->swapSegment(pid, &res) == 0);
        const uint64_t t0 = monoNs();
        while (res.remaining.load() != 0 && monoNs() - t0 < 30000000000ull) sleepNs(100000);
        REQUIRE(res.remaining.load() == 0);
        if (res.status < 0) {
            noCandidate++;
            continue;
        }
        retired.push_back({pid, res.seg, res.oldCgen, monoNs()});
        {
            std::lock_guard<std::mutex> g(spanMu);
            swapTimes.push_back(retired.back().publishedNs);
        }
        swaps++;
        reclaim(false);
    }
    stop = true;
    writer.join();
    for (auto& t : readers) t.join();
    reclaim(true);
    report("swaps", double(swaps), "swaps");
    report("swap_requests_without_candidate", double(noCandidate), "requests");
    report("retired_file_sets_unlinked", double(unlinked), "sets");
    report("reader_statements", double(statements.load()), "statements");
    uint64_t spanning = 0, maxSwapsInOne = 0;
    for (const auto& sp : spans) {
        const uint64_t n = uint64_t(std::count_if(swapTimes.begin(), swapTimes.end(), [&](uint64_t t) {
            return t > sp.first && t < sp.second;
        }));
        if (n) spanning++;
        maxSwapsInOne = std::max(maxSwapsInOne, n);
    }
    report("statements_spanning_a_swap", double(spanning), "statements");
    report("max_swaps_during_one_statement", double(maxSwapsInOne), "swaps");
    CHECK(spanning > 0);
    if (!firstErr.empty()) std::fprintf(stderr, "  first error: %s\n", firstErr.c_str());
    CHECK(swaps >= targetSwaps || maxSeconds > 0);
    CHECK_EQ(errors.load(), uint64_t(0));
    CHECK_EQ(gone.load(), uint64_t(0));
    CHECK_EQ(mismatches.load(), uint64_t(0));
    CHECK_EQ(unlinked, swaps);
    // After everything, a fresh statement reads the compacted files only.
    for (size_t p = 0; p < h.pids.size(); p++) {
        Rows r = reader.q("SELECT _pseq, _data FROM sds_p_" + h.tokens[p] + "__OMM");
        std::string why;
        CHECK_EQ(r.status, 0);
        CHECK_EQ(r.rows.size(), size_t(h.sent[p].load()));
        CHECK(verifyScan(r, int(p), &why));
    }
    h.s.close();
    // The writer reopens on the SWAPped manifests (the owner reads c-* files).
    REQUIRE(h.s.open() == 0);
    Reader again(h.io, h.s.root, LaneClass::Bulk, 1);
    for (size_t p = 0; p < h.pids.size(); p++) {
        Rows r = again.q("SELECT count(*) FROM sds_p_" + h.tokens[p] + "__OMM");
        CHECK_EQ(r.status, 0);
        CHECK_EQ(r.i(0, 0), int64_t(h.sent[p].load()));
    }
    h.s.close();
}

}  // namespace

PS_TEST(snapshot_swaps_under_load_T2_5) { runSwapsUnderLoad(int(argInt("swaps", 1000)), uint64_t(argInt("max_seconds", 300))); }

PS_TEST(snapshot_unlink_without_announce_is_snapshot_gone) {
    Harness h(1);
    REQUIRE(h.s.open() == 0);
    h.fill(6000);
    REQUIRE(waitLabeledEngine(h.s.e.get(), h.pids, 60000000000ull));
    ReaderConfig rc;
    rc.root = h.s.root;
    rc.io = h.io;
    rc.cls = LaneClass::Bulk;
    rc.lanes = 1;
    rc.ringBytes = 16u << 10;
    Reader reader(rc);
    REQUIRE(reader.inst);
    ReaderClient c(reader.inst.get());
    Request req;
    req.sql = "SELECT _pseq, _data FROM sds_p_snap0__OMM";
    uint32_t slot;
    REQUIRE(c.submit(req, &slot) == 0);
    // Read a little: the statement is running (parked) inside segment 0.
    std::vector<uint8_t> buf(4096);
    CHECK(c.read(slot, buf.data(), buf.size(), 5000000000ull) > 0);
    // SWAP segments 0 and 1, unlinking at once (ignoring announcements).
    int unlinked = 0;
    for (int i = 0; i < 2; i++) {
        SwapResult res;
        REQUIRE(h.s.e->swapSegment(h.pids[0], &res) == 0);
        while (res.remaining.load() != 0) sleepNs(100000);
        if (res.status < 0) continue;
        unlinkRetired(h, {h.pids[0], res.seg, res.oldCgen, 0});
        unlinked++;
    }
    CHECK_EQ(unlinked, 2);
    for (;;) {
        const int64_t n = c.read(slot, buf.data(), buf.size(), 5000000000ull);
        if (n <= 0) break;
    }
    Outcome o = c.finish(slot);
    CHECK_EQ(o.status, int32_t(kRsSnapshotGone));
    // Retryable: the same statement started now reads the new files.
    Rows r = reader.q("SELECT count(*) FROM sds_p_snap0__OMM");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.i(0, 0), 6000);
    h.s.close();
}

namespace {
void runExportUnderCompaction(uint64_t seconds) {
    Harness h(4);
    REQUIRE(h.s.open() == 0);
    h.fill(3000);
    REQUIRE(waitLabeledEngine(h.s.e.get(), h.pids, 60000000000ull));
    REQUIRE(waitTypeVisible(h.io, h.s.root, ommType().fid, h.pids, 60000000000ull));
    ReaderConfig rc;
    rc.root = h.s.root;
    rc.io = h.io;
    rc.cls = LaneClass::Bulk;
    rc.lanes = 2;
    rc.ringBytes = 64u << 10;
    Reader reader(rc);
    REQUIRE(reader.inst);
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> exports{0}, gone{0}, errors{0}, frames{0};
    std::thread exporter([&] {
        while (!stop.load()) {
            Outcome o;
            const auto fr = reader.raw("SELECT _data FROM OMM", {}, &o);
            exports++;
            frames += fr.size();
            if (o.status == int32_t(kRsSnapshotGone)) gone++;
            else if (o.status != 0) errors++;
        }
    });
    int swaps = 0, unlinked = 0;
    std::deque<Retired> retired;
    const uint64_t until = monoNs() + seconds * 1000000000ull;
    int k = 0;
    // The in-memory host keeps every file it ever held (unlinked ones too):
    // bound the SWAPs there; on a real file system (--dir) they run freely.
    const int maxSwaps = int(argInt("max_swaps", h.dir.empty() ? 300 : 1 << 30));
    while (monoNs() < until) {
        if (swaps >= maxSwaps) {
            sleepNs(1000000);
            continue;
        }
        const uint32_t pid = h.pids[size_t(k++) % h.pids.size()];
        SwapResult res;
        REQUIRE(h.s.e->swapSegment(pid, &res) == 0);
        while (res.remaining.load() != 0) sleepNs(100000);
        if (res.status >= 0) {
            retired.push_back({pid, res.seg, res.oldCgen, monoNs()});
            swaps++;
        }
        while (!retired.empty()) {
            Retired& r = retired.front();
            if (reader.inst->oldestActiveStart() <= r.publishedNs) break;
            const uint64_t now = monoNs();
            if (!r.firstCheckNs) r.firstCheckNs = now;
            if (now - r.firstCheckNs < 2000000ull) break;
            unlinkRetired(h, r);
            unlinked++;
            retired.pop_front();
        }
        if (k % 8 == 0) h.ingest(k % int(h.pids.size()), 100);
    }
    stop = true;
    exporter.join();
    report("export_seconds", double(seconds), "s");
    report("exports", double(exports.load()), "exports");
    report("exported_frames", double(frames.load()), "frames");
    report("swaps_during_export", double(swaps), "swaps");
    report("retired_sets_unlinked", double(unlinked), "sets");
    report("snapshot_gone", double(gone.load()), "statements");
    CHECK(exports.load() > 0);
    CHECK(swaps > 0);
    CHECK_EQ(gone.load(), uint64_t(0));
    CHECK_EQ(errors.load(), uint64_t(0));
    h.s.close();
}
}  // namespace

PS_TEST(snapshot_bulk_export_under_compaction_A12) { runExportUnderCompaction(uint64_t(argInt("seconds", 5))); }
PS_SLOW_TEST(snapshot_bulk_export_under_compaction_A12_full) { runExportUnderCompaction(uint64_t(argInt("seconds", 600))); }
