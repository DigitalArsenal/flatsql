// Reader instance memory under a sustained point-statement mix while a writer
// commits (flatsql-reader-instance-memory-growth-20260929; docs
// PARTITION-STORE.md §37).
//
// ps_test_main.cpp counts the live bytes that reader lane threads allocate
// (gLaneLiveBytes): everything a reader instance holds beyond its fixed
// arenas, its caches included. A reader's index state must stay under a
// stated bound however many statements run and however large the store
// grows:
//
//   instance cache + lanes x (private cache + resident runs + front)
//     = cacheBytes + lanes x 3 x laneCacheBytes
//
// plus bookkeeping independent of the store's size: per lane its open
// handles (maxHandlesPerLane, at most 256 bytes each) and the evicted runs
// it remembers verifying (1,024, at most 128 bytes each). With the
// defaults (16 MiB, 4 MiB, 2 lanes, 4,096 handles) that is 42.25 MiB.
//
//   - reader_point_memory_plateaus_under_commits: in-memory store, 120,000
//     statements while producers commit and merges run, tight caches;
//   - reader_memory_soak_writer / reader_memory_soak_reader (slow, named):
//     two processes on one directory. The writer grows the store past
//     --store-mib; the reader (native, or the wasm command under the Node
//     host) runs the mix in rounds and reports its live bytes and its
//     process memory (RSS natively, linear memory in wasm).
#include <algorithm>
#include <cstring>
#include <atomic>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>

#if defined(__APPLE__)
#include <mach/mach.h>
#endif
#if !defined(__wasm__)
#include <sys/stat.h>
#endif

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

extern std::atomic<int64_t> gLaneLiveBytes;
extern std::atomic<int64_t> gAllLiveBytes;

using namespace pst;

namespace {

// The process's memory: RSS natively, the linear memory in wasm (never
// shrinks). 0 where unknown.
uint64_t processBytes() {
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

double mib(int64_t b) { return double(b) / double(1u << 20); }

std::string epochOf(uint64_t i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-09-%02dT%02d:%02d:%02d.%03dZ", int(1 + (i / 86400) % 28), int((i / 3600) % 24),
                  int((i / 60) % 60), int(i % 60), int(i % 1000));
    return b;
}

// Record i of partition p (deterministic: a second process rebuilds it).
std::vector<uint8_t> soakRecord(uint32_t p, uint64_t i, size_t pad) {
    const uint64_t id = (uint64_t(p) << 32) | i;
    return ommRecord(uint32_t(100000 + id % 4000000000u), "S" + std::to_string(p) + "-" + std::to_string(i),
                     epochOf(i), 15.0 + double(i % 1000) / 1000.0, pad);
}

// RECONCILE{provider, source, keep batch} (the lane's other batches die).
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

std::string cidBin(const std::vector<uint8_t>& frame) {
    uint8_t c[kCidLen];
    frameCid(frame, c);
    return std::string(reinterpret_cast<const char*>(c), kCidLen);
}

// The stated bound (see the top of this file), plus `slack`.
int64_t readerBound(const ReaderConfig& rc, int64_t slack) {
    const int64_t perLane = int64_t(3 * rc.laneCacheBytes) + int64_t(rc.maxHandlesPerLane) * 256 + 1024 * 128;
    return int64_t(rc.cacheBytes) + int64_t(rc.lanes) * perLane + slack;
}

// Each lane's store within its budgets, the instance cache within its own.
bool withinBudgets(ReaderInstance* inst, const ReaderConfig& rc) {
    bool ok = inst->lane(0)->store().config().shared->used() <= rc.cacheBytes;
    for (uint32_t l = 0; l < inst->laneCount(); l++) {
        flatsql::ps::LaneStore& st = inst->lane(l)->store();
        const flatsql::ps::LaneStoreUsage u = st.usage();
        ok = ok && u.cacheBytes <= rc.laneCacheBytes && u.frontBytes <= rc.laneCacheBytes &&
             u.verifiedRuns <= 1024 && st.io().openHandles() <= rc.maxHandlesPerLane;
        // Resident runs: the budget, or one run over it (the one in use).
        ok = ok && (u.runBytes <= rc.laneCacheBytes || u.runs <= 1);
    }
    return ok;
}

// What each lane's store holds, one line.
std::string laneDetail(ReaderInstance* inst) {
    std::string out;
    for (uint32_t l = 0; l < inst->laneCount(); l++) {
        flatsql::ps::LaneStore& st = inst->lane(l)->store();
        const flatsql::ps::LaneStoreUsage u = st.usage();
        char b[200];
        std::snprintf(b, sizeof(b), " lane%u[cache %.2f/%zu runs %.2f/%zu front %.2f/%zu verified %zu handles %u]", l,
                      mib(int64_t(u.cacheBytes)), u.cacheEntries, mib(int64_t(u.runBytes)), u.runs,
                      mib(int64_t(u.frontBytes)), u.frontEntries, u.verifiedRuns, st.io().openHandles());
        out += b;
    }
    return out;
}

// The point mix SDN's router runs: GetRecord by CID at type level, a CID IN
// at type level, and a partition-level point read.
struct PointMix {
    Reader& r;
    std::vector<std::string> partTables;  // per partition
    uint64_t errors = 0, rowsMissing = 0, gone = 0;
    uint64_t missingBy[3] = {0, 0, 0};  // by statement kind
    int32_t run(uint64_t i, uint32_t p, const std::string& cid, bool mustExist) {
        Rows q;
        switch (i % 3) {
            case 0:
                q = r.q("SELECT _cid_bin, _gseq, _pseq, _data FROM \"OMM\" WHERE _cid_bin = ?1", {Param::blob(cid)});
                break;
            case 1:
                q = r.q("SELECT _cid_bin FROM \"OMM\" WHERE _cid_bin IN (?1, ?2)",
                        {Param::blob(cid), Param::blob(std::string(kCidLen, '\1'))});
                break;
            default:
                q = r.q("SELECT _pseq, _data FROM \"" + partTables[p] + "\" WHERE _cid_bin = ?1", {Param::blob(cid)});
                break;
        }
        if (q.status == kRsSnapshotGone) {
            gone++;  // retryable (a second process holds no reader gate)
        } else if (q.status != 0) {
            if (errors++ < 5) std::fprintf(stderr, "  statement %llu: %d %s\n", (unsigned long long)i, q.status, q.error.c_str());
        } else if (mustExist && q.rows.size() != 1) {
            rowsMissing++;
            missingBy[i % 3]++;
        }
        return q.status;
    }
};

}  // namespace

// ---------------------------------------------------------------------------
PS_TEST(reader_point_memory_plateaus_under_commits) {
    Store s(false, 2, true);  // no crash tracking: the in-memory store stays small (wasm)
    s.cfg.mergeL0Blocks = 4;
    s.cfg.mergeMinL0Bytes = 0;
    s.cfg.sealBytes = 1u << 20;  // segments seal and merge as ingest goes
    s.cfg.typeCommitRows = 256;  // many small type commits: type L0 blocks and merges every round
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t parts = uint32_t(argInt("mem-parts", 8));
    std::vector<uint32_t> pids;
    std::vector<std::string> tables;
    for (uint32_t p = 0; p < parts; p++) {
        pids.push_back(s.partition("mem" + std::to_string(p), ommType()));
        tables.push_back("sds_p_mem" + std::to_string(p) + "__OMM");
    }
    // Producers commit continuously, paced (the in-memory store holds every
    // byte): 4 threads, a batch per partition, then a pause.
    std::atomic<bool> stop{false};
    std::mutex mu;
    std::vector<std::pair<uint32_t, std::string>> known;  // (partition, cid) of acked records
    std::atomic<uint64_t> sent{0};
    const uint32_t nProd = 4;
    const uint64_t pauseNs = uint64_t(argInt("mem-pause-us", 20000)) * 1000ull;
    std::vector<std::thread> prod;
    for (uint32_t t = 0; t < nProd; t++)
        prod.emplace_back([&, t] {
            std::vector<std::unique_ptr<Producer>> ps;
            std::vector<uint32_t> mine;
            for (uint32_t p = t; p < parts; p += nProd) {
                ps.emplace_back(new Producer(s.e.get(), pids[p]));
                mine.push_back(p);
            }
            std::vector<uint64_t> next(mine.size(), 0);
            while (!stop.load()) {
                for (size_t k = 0; k < mine.size() && !stop.load(); k++) {
                    uint64_t last = 0;
                    std::vector<std::pair<uint32_t, std::string>> batch;
                    for (int j = 0; j < 8; j++) {
                        auto f = soakRecord(mine[k], next[k]++, 100);
                        last = send(s.e.get(), *ps[k], f, buildRecordAttr("mem", "prov", "src", "b1"),
                                    1790000000000ll + int64_t(next[k]), false);
                        if (last) batch.push_back({mine[k], cidBin(f)});
                    }
                    if (last && ps[k]->waitAcked(last, 10000000000ull) == 0) {
                        std::lock_guard<std::mutex> g(mu);
                        known.insert(known.end(), batch.begin(), batch.end());
                    }
                    sent += batch.size();
                }
                sleepNs(pauseNs);
            }
        });
    // Wait for the first labeled records, then start the point reader.
    for (const uint64_t t0 = monoNs(); monoNs() - t0 < 30000000000ull; sleepNs(10000000)) {
        std::lock_guard<std::mutex> g(mu);
        if (known.size() >= 1000) break;
    }
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 60000000000ull));
    ReaderConfig rc;
    rc.root = s.root;
    rc.io = s.fs.get();
    rc.cls = LaneClass::Interactive;
    rc.lanes = 2;
    rc.cacheBytes = uint64_t(argInt("mem-cache-kib", 1024)) << 10;
    rc.laneCacheBytes = uint64_t(argInt("mem-lane-cache-kib", 256)) << 10;
    rc.maxHandlesPerLane = 256;  // its bookkeeping at its cap within a round
    Reader r(rc);
    REQUIRE(r.inst);
    // Two clients (the router's concurrent statements): both lanes work.
    const int nClients = 2;
    std::vector<PointMix> mixes(nClients, PointMix{r, tables});
    // Fixed state: the lanes' connections and first statements.
    for (int i = 0; i < 64; i++) REQUIRE(r.q("SELECT 1").status == 0);
    const int64_t fixed = gLaneLiveBytes.load();
    std::atomic<uint64_t> stmt{0};
    auto round = [&](uint64_t n, bool mustExist) {
        std::vector<std::thread> cl;
        for (int c = 0; c < nClients; c++)
            cl.emplace_back([&, c] {
                std::mt19937_64 rng(uint64_t(7 + c) * 1000003u + stmt.load());
                for (uint64_t k = c; k < n; k += nClients) {
                    uint32_t p;
                    std::string cid;
                    {
                        std::lock_guard<std::mutex> g(mu);
                        const auto& e = known[size_t(rng() % known.size())];
                        p = e.first;
                        cid = e.second;
                    }
                    mixes[size_t(c)].run(stmt.fetch_add(1), p, cid, mustExist);
                }
            });
        for (auto& t : cl) t.join();
    };
    round(2000, false);  // warm
    const uint64_t perRound = uint64_t(argInt("mem-stmts", 20000));
    const int rounds = int(argInt("mem-rounds", 6));
    std::vector<int64_t> live;
    bool budgets = true;
    std::printf("  %-7s %5s %9s %12s %12s %12s\n", "phase", "round", "records", "lane_live", "shared_used", "stmts/s");
    for (int k = 1; k <= rounds; k++) {
        const uint64_t t0 = monoNs();
        round(perRound, false);
        live.push_back(gLaneLiveBytes.load() - fixed);
        budgets = budgets && withinBudgets(r.inst.get(), rc);
        std::printf("  %-7s %5d %9llu %10.2f MiB %10.2f MiB %12.0f\n    %s\n", "ingest", k,
                    (unsigned long long)sent.load(), mib(live.back()),
                    mib(int64_t(r.inst->lane(0)->store().config().shared->used())),
                    double(perRound) / (double(monoNs() - t0) / 1e9), laneDetail(r.inst.get()).c_str());
    }
    stop = true;
    for (auto& t : prod) t.join();
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 60000000000ull));
    sleepNs(500000000);  // merges settle
    round(2000, true);   // the caches meet the settled store
    const int64_t idle0 = gLaneLiveBytes.load() - fixed;
    std::vector<int64_t> idle;
    for (int k = 1; k <= 2; k++) {
        round(perRound, true);
        idle.push_back(gLaneLiveBytes.load() - fixed);
        std::printf("  %-7s %5d %9llu %10.2f MiB %10.2f MiB\n", "idle", k, (unsigned long long)sent.load(), mib(idle.back()),
                    mib(int64_t(r.inst->lane(0)->store().config().shared->used())));
    }
    const int64_t peak = *std::max_element(live.begin(), live.end());
    const int64_t bound = readerBound(rc, 1 << 20);
    PointMix mix{r, tables};
    for (const auto& m : mixes) {
        mix.errors += m.errors;
        mix.rowsMissing += m.rowsMissing;
    }
    report("reader_mem_statements", double(stmt.load()), "statements");
    report("reader_mem_records", double(sent.load()), "records");
    report("reader_mem_fixed", mib(fixed), "MiB");
    report("reader_mem_lane_live_round1", mib(live.front()), "MiB");
    report("reader_mem_lane_live_last_ingest_round", mib(live.back()), "MiB");
    report("reader_mem_lane_live_peak", mib(peak), "MiB");
    report("reader_mem_bound", mib(bound), "MiB");
    report("reader_mem_idle_delta", mib(idle.back() - idle0), "MiB");
    report("reader_mem_errors", double(mix.errors), "statements");
    report("reader_mem_rows_missing", double(mix.rowsMissing), "statements");
    CHECK_EQ(mix.errors, uint64_t(0));
    CHECK_EQ(mix.rowsMissing, uint64_t(0));
    CHECK(budgets);
    CHECK(peak <= bound);
    // Flat: the last half of the rounds adds at most one lane cache's worth.
    CHECK(live.back() <= live[size_t(rounds / 2)] + int64_t(rc.laneCacheBytes));
    CHECK(idle.back() - idle0 <= int64_t(64u << 10));
    r.inst->stop();
    s.close();
}

// ---------------------------------------------------------------------------
// The two-process soak. The writer grows a store in --dir past --store-mib.
// Every 200 ms it appends the CIDs of acked records (one in 16, as
// [u32 partition][36-byte CID]) to <dir>.cids and writes <dir>.progress:
// "<store bytes> <CIDs written>", then " done". The reader looks CIDs up
// from that file: it never rebuilds a frame, whose bytes may differ between
// builds (a multiply-add contracted to FMA on one target and not another).
PS_SLOW_TEST(reader_memory_soak_writer) {
    const std::string dir = argStr("dir", "");
    REQUIRE(!dir.empty());
    Store s(false, uint32_t(argInt("writers", 2)), true);
    s.cfg.io = nullptr;
    s.cfg.root = dir;
    s.root = dir;
    s.cfg.sealBytes = uint64_t(argInt("seal-mib", 16)) << 20;
    s.cfg.typeCommitRows = uint32_t(argInt("type-commit-rows", 2048));
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t parts = uint32_t(argInt("parts", 16));
    std::vector<uint32_t> pids;
    for (uint32_t p = 0; p < parts; p++) pids.push_back(s.partition("soak" + std::to_string(p), ommType()));
    const uint64_t target = uint64_t(argInt("store-mib", 1200)) << 20;
    const size_t pad = size_t(argInt("pad", 1600));
    const double mibPerS = double(argInt("mib-per-s", 16));
    std::mutex cidMu;
    std::vector<std::pair<uint32_t, std::string>> cids;  // acked, not yet written
    uint64_t cidsWritten = 0;
    std::atomic<bool> stop{false};
    auto storeBytes = [&] {
        uint64_t b = 0;
        for (uint32_t pid : pids) b += s.e->partitionDiskBytes(pid);
        return b + s.e->typeDiskBytesOf(ommType().fid);
    };
    auto writeProgress = [&](bool done) {
        std::vector<std::pair<uint32_t, std::string>> batch;
        {
            std::lock_guard<std::mutex> g(cidMu);
            batch.swap(cids);
        }
        if (!batch.empty()) {
            std::ofstream f(dir + ".cids", std::ios::app | std::ios::binary);
            for (const auto& e : batch) {
                uint8_t pb[4];
                putU32(pb, e.first);
                f.write(reinterpret_cast<const char*>(pb), 4);
                f.write(e.second.data(), std::streamsize(e.second.size()));
            }
            cidsWritten += batch.size();
        }
        std::ostringstream o;
        o << storeBytes() << ' ' << cidsWritten;
        if (done) o << " done";
        o << '\n';
        const std::string tmp = dir + ".progress.tmp";
        {
            std::ofstream f(tmp, std::ios::trunc);
            f << o.str();
        }
        std::rename(tmp.c_str(), (dir + ".progress").c_str());
    };
    const uint32_t nProd = 4;
    const uint64_t t0 = monoNs();
    std::atomic<uint64_t> bytesSent{0};
    std::vector<std::thread> prod;
    for (uint32_t t = 0; t < nProd; t++)
        prod.emplace_back([&, t] {
            std::vector<std::unique_ptr<Producer>> ps;
            std::vector<uint32_t> mine;
            for (uint32_t p = t; p < parts; p += nProd) {
                ps.emplace_back(new Producer(s.e.get(), pids[p]));
                mine.push_back(p);
            }
            std::vector<uint64_t> next(mine.size(), 0);
            while (!stop.load()) {
                for (size_t k = 0; k < mine.size() && !stop.load(); k++) {
                    uint64_t last = 0;
                    std::vector<std::pair<uint32_t, std::string>> sample;
                    for (int j = 0; j < 64; j++) {
                        auto f = soakRecord(mine[k], next[k], pad);
                        last = send(s.e.get(), *ps[k], f, buildRecordAttr("soak", "prov", "src", "b1"),
                                    1790000000000ll + int64_t(next[k]), true);
                        if (next[k] % 16 == 0) sample.push_back({mine[k], cidBin(f)});
                        next[k]++;
                        bytesSent += f.size();
                    }
                    if (last && ps[k]->waitAcked(last, 60000000000ull) == 0) {
                        std::lock_guard<std::mutex> g(cidMu);
                        cids.insert(cids.end(), sample.begin(), sample.end());
                    }
                }
                // Pace to mib-per-s of record bytes.
                const double el = double(monoNs() - t0) / 1e9;
                const double ahead = double(bytesSent.load()) / (mibPerS * 1048576.0) - el;
                if (ahead > 0) sleepNs(uint64_t(ahead * 1e9));
            }
        });
    uint64_t last = 0;
    while (storeBytes() < target) {
        sleepNs(200000000);
        writeProgress(false);
        const uint64_t b = storeBytes();
        if (b >> 28 != last >> 28) {
            std::printf("  store %.0f MiB after %.0f s\n", double(b) / 1048576.0, double(monoNs() - t0) / 1e9);
            std::fflush(stdout);
        }
        last = b;
    }
    stop = true;
    for (auto& t : prod) t.join();
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 120000000000ull));
    writeProgress(true);
    report("soak_store_bytes", double(storeBytes()), "bytes");
    report("soak_writer_seconds", double(monoNs() - t0) / 1e9, "s");
    // Keep the files the reader may still read until it is done (a marker).
    for (const uint64_t w0 = monoNs(); monoNs() - w0 < uint64_t(argInt("linger-s", 600)) * 1000000000ull;
         sleepNs(200000000)) {
        std::ifstream f(dir + ".reader-done");
        if (f.good()) break;
    }
    s.close();
}

// The reader half: waits for the store, then runs the point mix in rounds
// of --round statements until the writer is done, plus --idle-rounds.
PS_SLOW_TEST(reader_memory_soak_reader) {
    const std::string dir = argStr("dir", "");
    REQUIRE(!dir.empty());
    struct Progress {
        uint64_t bytes = 0;
        uint64_t nCids = 0;
        bool done = false;
    };
    std::vector<std::pair<uint32_t, std::string>> cids;  // (partition, CID) acked by the writer
    auto readProgress = [&](Progress* pr) {
        std::ifstream f(dir + ".progress");
        if (!f.good()) return false;
        std::string line;
        if (!std::getline(f, line)) return false;
        std::istringstream in(line);
        Progress x;
        std::string tok;
        if (!(in >> x.bytes >> x.nCids) || !x.nCids) return false;
        while (in >> tok) x.done = x.done || tok == "done";
        std::ifstream cf(dir + ".cids", std::ios::binary);
        cf.seekg(std::streamoff(cids.size() * (4 + kCidLen)));
        while (cids.size() < x.nCids) {
            char e[4 + kCidLen];
            if (!cf.read(e, sizeof(e))) return false;
            cids.push_back({getU32(reinterpret_cast<const uint8_t*>(e)), std::string(e + 4, kCidLen)});
        }
        *pr = x;
        return true;
    };
    Progress pr;
    for (const uint64_t t0 = monoNs(); !readProgress(&pr) || pr.bytes < (64u << 20);) {
        REQUIRE(monoNs() - t0 < 120000000000ull);
        sleepNs(200000000);
    }
    ReaderConfig rc;
    rc.root = dir;
    rc.io = importIo();
    rc.cls = LaneClass::Interactive;
    rc.lanes = 2;
    rc.cacheBytes = uint64_t(argInt("cache-kib", long(rc.cacheBytes >> 10))) << 10;
    rc.laneCacheBytes = uint64_t(argInt("lane-cache-kib", long(rc.laneCacheBytes >> 10))) << 10;
    Reader r(rc);
    REQUIRE(r.inst);
    std::vector<std::string> tables;
    for (uint32_t p = 0; p < uint32_t(argInt("parts", 16)); p++) tables.push_back("sds_p_soak" + std::to_string(p) + "__OMM");
    PointMix mix{r, tables};
    std::mt19937_64 rng(11);
    uint64_t stmt = 0;
    auto round = [&](uint64_t n) {
        for (uint64_t k = 0; k < n; k++, stmt++) {
            const auto& e = cids[size_t(rng() % pr.nCids)];
            mix.run(stmt, e.first, e.second, pr.done);
        }
    };
    for (int i = 0; i < 64; i++) REQUIRE(r.q("SELECT 1").status == 0);
    const int64_t fixed = gLaneLiveBytes.load();
    const uint64_t perRound = uint64_t(argInt("round", 20000));
    const int idleRounds = int(argInt("idle-rounds", 2));
    const int warmRounds = int(argInt("warm-rounds", 2));
    std::printf("  %-6s %5s %10s %12s %12s %12s %10s\n", "phase", "round", "store_MiB", "lane_live", "process", "all_live",
                "stmts/s");
    std::vector<int64_t> live, proc;
    int idleLeft = idleRounds;
    for (int k = 1;; k++) {
        const bool idle = pr.done;
        const uint64_t t0 = monoNs();
        round(perRound);
        const double rate = double(perRound) / (double(monoNs() - t0) / 1e9);
        live.push_back(gLaneLiveBytes.load() - fixed);
        proc.push_back(int64_t(processBytes()));
        std::printf("  %-6s %5d %10.0f %8.2f MiB %8.1f MiB %8.2f MiB %10.0f\n", idle ? "idle" : "ingest", k,
                    double(pr.bytes) / 1048576.0, mib(live.back()), mib(proc.back()), mib(gAllLiveBytes.load()), rate);
        if (argInt("detail", 0))
            std::printf("    shared %.2f MiB%s\n", mib(int64_t(r.inst->lane(0)->store().config().shared->used())),
                        laneDetail(r.inst.get()).c_str());
        std::fflush(stdout);
        Progress next;
        if (readProgress(&next)) pr = next;
        if (idle && --idleLeft <= 0) break;
    }
    {
        std::ofstream f(dir + ".reader-done");
        f << "done\n";
    }
    const int64_t bound = readerBound(rc, 2 << 20);
    int64_t peak = 0, procFirst = 0, procPeak = 0;
    for (size_t k = 0; k < live.size(); k++) {
        if (int(k) < warmRounds) continue;
        peak = std::max(peak, live[k]);
        if (!procFirst) procFirst = proc[k];
        procPeak = std::max(procPeak, proc[k]);
    }
    report("soak_reader_statements", double(stmt), "statements");
    report("soak_reader_store_mib", double(pr.bytes) / 1048576.0, "MiB");
    report("soak_reader_lane_live_peak_after_warmup", mib(peak), "MiB");
    report("soak_reader_lane_live_last", mib(live.back()), "MiB");
    report("soak_reader_bound", mib(bound), "MiB");
    report("soak_reader_process_after_warmup", mib(procFirst), "MiB");
    report("soak_reader_process_peak", mib(procPeak), "MiB");
    report("soak_reader_process_growth_after_warmup", mib(procPeak - procFirst), "MiB");
    report("soak_reader_snapshot_gone", double(mix.gone), "statements");
    report("soak_reader_errors", double(mix.errors), "statements");
    report("soak_reader_rows_missing", double(mix.rowsMissing), "statements");
    if (mix.rowsMissing)
        std::printf("  missing by kind: type GetRecord %llu, type CID IN %llu, partition point %llu\n",
                    (unsigned long long)mix.missingBy[0], (unsigned long long)mix.missingBy[1],
                    (unsigned long long)mix.missingBy[2]);
    CHECK_EQ(mix.errors, uint64_t(0));
    CHECK_EQ(mix.rowsMissing, uint64_t(0));
    CHECK(peak <= bound);
    r.inst->stop();
}

// ---------------------------------------------------------------------------
// Fences are read a page at a time (FenceView). With two fences per page
// every L1 lookup and scan crosses pages: the results equal a reader with
// the default pages and the records written, point lookups at type and
// partition level, index range scans both ways, and offset paging after
// deaths (GONE counts summed from fences).
PS_TEST(reader_lookups_and_scans_across_fence_pages) {
    Store s(true, 2, true);
    s.cfg.mergeL0Blocks = 4;
    s.cfg.mergeMinL0Bytes = 0;
    s.cfg.sealBytes = 512u << 10;
    s.cfg.sealAgeMs = 100;
    s.cfg.typeCommitRows = 512;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const int parts = 2, n = 12000;
    std::vector<uint32_t> pids;
    std::vector<std::unique_ptr<Producer>> prods;
    for (int p = 0; p < parts; p++) {
        pids.push_back(s.partition("fp" + std::to_string(p), ommType()));
        prods.emplace_back(new Producer(s.e.get(), pids.back()));
    }
    std::vector<std::vector<uint8_t>> frames;
    std::vector<std::string> objectIds;
    const auto keepAttr = buildRecordAttr("fp", "prov", "src", "keep");
    const auto dropAttr = buildRecordAttr("fp", "prov", "src", "drop");
    std::vector<uint64_t> last(parts, 0);
    std::vector<bool> keep(n);
    std::mt19937_64 rng(3);
    for (int k = 0; k < n; k++) {
        const int p = k % parts;
        char oid[32];
        std::snprintf(oid, sizeof(oid), "OID-%06d", (k * 7919) % n);  // index order differs from arrival order
        objectIds.push_back(oid);
        frames.push_back(ommRecord(uint32_t(500000 + k), oid, epochOf(uint64_t(k)), double(k), 40));
        keep[size_t(k)] = rng() % 10 < 8;
        last[size_t(p)] = send(s.e.get(), *prods[size_t(p)], frames.back(), keep[size_t(k)] ? keepAttr : dropAttr,
                               1780000000000ll + k);
        REQUIRE(last[size_t(p)]);
    }
    for (int p = 0; p < parts; p++) REQUIRE(prods[size_t(p)]->waitAcked(last[size_t(p)], 120000000000ull) == 0);
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 120000000000ull));
    // Deaths: RECONCILE the "drop" batch away (GONE postings at type level).
    for (int p = 0; p < parts; p++) {
        const auto payload = reconcilePayload("prov", "src", "keep");
        uint64_t rseq = 0;
        REQUIRE(prods[size_t(p)]->enqueue(kEntReconcile, 0, 0, nullptr, nullptr, 0, payload.data(),
                                          uint32_t(payload.size()), &rseq) == 0);
        REQUIRE(prods[size_t(p)]->waitAcked(rseq, 120000000000ull) == 0);
    }
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 120000000000ull));
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, pids, 120000000000ull));
    // Merged: partitions' sealed segments and most type L0 blocks are in L1 runs.
    bool merged = false;
    for (const uint64_t t0 = monoNs(); !merged && monoNs() - t0 < 120000000000ull; sleepNs(20000000)) {
        Inspector ins(s.fs.get(), s.root);
        merged = true;
        for (uint32_t pid : pids) {
            const PartView v = ins.partition(pid);
            merged = merged && v.ok && v.head.manifestGen && v.head.mergedThrough + 1 >= v.head.segFirstPseq;
        }
        const auto tv = ins.type(ommType().fid);
        merged = merged && tv.ok && tv.head.manifestGen && tv.head.nL0 < 4;
    }
    REQUIRE(merged);
    auto cfgFor = [&](uint32_t page) {
        ReaderConfig rc;
        rc.root = s.root;
        rc.io = s.fs.get();
        rc.cls = LaneClass::Bulk;
        rc.lanes = 1;
        rc.fencePageEntries = page;
        return rc;
    };
    Reader small(cfgFor(2)), dflt(cfgFor(128));
    REQUIRE(small.inst && dflt.inst);
    uint64_t smallFence = 0, dfltFence = 0, lookups = 0, scans = 0, pages = 0;
    int bad = 0;
    auto same = [&](const Rows& a, const Rows& b, const char* what) {
        bool eq = a.status == 0 && b.status == 0 && a.rows.size() == b.rows.size();
        for (size_t i = 0; eq && i < a.rows.size(); i++)
            for (size_t c = 0; eq && c < a.rows[i].size(); c++)
                eq = a.rows[i][c].i == b.rows[i][c].i && a.rows[i][c].s == b.rows[i][c].s;
        if (!eq && bad++ < 5)
            std::fprintf(stderr, "  %s: %d/%zu rows vs %d/%zu rows\n", what, a.status, a.rows.size(), b.status, b.rows.size());
        smallFence += a.outcome.fenceReads;
        dfltFence += b.outcome.fenceReads;
        return eq;
    };
    // Point lookups (type level and partition level), live and dead.
    for (int k = 0; k < n; k += 37) {
        const std::string cid = cidBin(frames[size_t(k)]);
        const std::string tbl = "sds_p_fp" + std::to_string(k % parts) + "__OMM";
        const std::string q1 = "SELECT _cid_bin, _gseq, _pseq FROM \"OMM\" WHERE _cid_bin = ?1";
        const std::string q2 = "SELECT _pseq, OBJECT_ID FROM \"" + tbl + "\" WHERE _cid_bin = ?1";
        Rows a = small.q(q1, {Param::blob(cid)}), b = dflt.q(q1, {Param::blob(cid)});
        same(a, b, "type lookup");
        if (a.rows.size() != (keep[size_t(k)] ? 1u : 0u) && bad++ < 5)
            std::fprintf(stderr, "  record %d: %zu rows, keep %d\n", k, a.rows.size(), int(keep[size_t(k)]));
        same(small.q(q2, {Param::blob(cid)}), dflt.q(q2, {Param::blob(cid)}), "partition lookup");
        lookups += 2;
    }
    // Index range scans on OBJECT_ID, ascending and descending.
    for (int t = 0; t < 40; t++) {
        const int a0 = int(rng() % n), len = 1 + int(rng() % 600);
        char lo[32], hi[32];
        std::snprintf(lo, sizeof(lo), "OID-%06d", a0);
        std::snprintf(hi, sizeof(hi), "OID-%06d", a0 + len);
        const std::string tbl = "sds_p_fp" + std::to_string(t % parts) + "__OMM";
        const std::string q = "SELECT OBJECT_ID, _pseq FROM \"" + tbl + "\" WHERE OBJECT_ID >= ?1 AND OBJECT_ID < ?2 ORDER BY OBJECT_ID" +
                              (t % 2 ? " DESC" : "");
        const std::vector<Param> ps = {Param::text(lo), Param::text(hi)};
        Rows a = small.q(q, ps);
        same(a, dflt.q(q, ps), "range scan");
        // Expected: the partition's records in [lo, hi), in index order.
        std::vector<std::string> want;
        for (int k = t % parts; k < n; k += parts)
            if (objectIds[size_t(k)] >= lo && objectIds[size_t(k)] < hi && keep[size_t(k)]) want.push_back(objectIds[size_t(k)]);
        std::sort(want.begin(), want.end());
        if (t % 2) std::reverse(want.begin(), want.end());
        std::vector<std::string> got;
        for (const auto& r : a.rows) got.push_back(r[0].s);
        if (got != want && bad++ < 5) std::fprintf(stderr, "  range %s..%s: %zu rows, want %zu\n", lo, hi, got.size(), want.size());
        scans++;
    }
    // Offset paging over arrivals with deaths (GONE counts from fences).
    Rows all = dflt.q("SELECT _gseq FROM OMM ORDER BY _gseq");
    REQUIRE(all.status == 0);
    size_t live = 0;
    for (bool k : keep) live += k;
    CHECK_EQ(all.rows.size(), live);
    for (int t = 0; t < 40; t++) {
        const size_t off = size_t(rng() % (all.rows.size() + 20));
        const std::string q = std::string("SELECT _gseq FROM OMM ORDER BY _gseq ") + (t % 2 ? "DESC " : "") +
                              "LIMIT 50 OFFSET " + std::to_string(off);
        Rows a = small.q(q);
        same(a, dflt.q(q), "offset page");
        std::vector<int64_t> want, got;
        for (size_t i = 0; i < 50 && off + i < all.rows.size(); i++)
            want.push_back(t % 2 ? all.i(all.rows.size() - 1 - off - i, 0) : all.i(off + i, 0));
        for (const auto& r : a.rows) got.push_back(r[0].i);
        if (got != want && bad++ < 5) std::fprintf(stderr, "  offset %zu: %zu rows, want %zu\n", off, got.size(), want.size());
        pages++;
    }
    report("fence_pages_lookups", double(lookups), "statements");
    report("fence_pages_scans", double(scans), "statements");
    report("fence_pages_offset_pages", double(pages), "statements");
    report("fence_pages_fence_reads_page2", double(smallFence), "reads");
    report("fence_pages_fence_reads_page128", double(dfltFence), "reads");
    CHECK_EQ(bad, 0);
    // Two fences per page read more pages: the lookups crossed pages.
    CHECK(smallFence > dfltFence);
    s.close();
}
