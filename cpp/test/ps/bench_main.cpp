// Partition store benchmark driver (acceptance T1 #6, #7 with A8, #8).
//
//   flatsql_ps_bench --mode=scaling --writers=N --io=mem|fs [--dir=D]
//                    [--partitions=64] [--records=2000000] [--producers=8]
//   flatsql_ps_bench --mode=host02 [--dir=D] [--partitions=50] [--seconds=20]
//                    [--rate=R] (open loop; 0 = find the sustained maximum)
//   flatsql_ps_bench --mode=dirty --rate=R [--dir=D]   (A8: ack p99 at 1/10/50
//                    dirty partitions)
//   Any mode: --journal=1 commits through the per-writer journal (A8 fallback).
//   flatsql_ps_bench --mode=soak [--seconds=1800] [--dir=D]   (lock holds,
//                    hot-path allocations)
// Every number is printed as "MEASURED <key> = <value> <unit>" together with
// the machine it ran on (uname, CPU count). Benchmarks are only meaningful on
// the machine the acceptance names (Linux-8, host-02 profile); the driver
// never decides that for you.
#include <sys/utsname.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <mutex>
#include <new>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/ps_test.h"

std::atomic<uint64_t> gHotAllocs{0};
void* operator new(std::size_t n) {
    if (flatsql::ps::tHotPathDepth > 0) gHotAllocs.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(n ? n : 1);
    if (!p) throw std::bad_alloc();
    return p;
}
void* operator new[](std::size_t n) {
    if (flatsql::ps::tHotPathDepth > 0) gHotAllocs.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(n ? n : 1);
    if (!p) throw std::bad_alloc();
    return p;
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace pst {
int gFailures = 0;
static std::vector<std::string> gArgs;
std::vector<Test>& registry() {
    static std::vector<Test> r;
    return r;
}
long argInt(const char* name, long def) {
    const std::string key = std::string("--") + name + "=";
    for (const auto& a : gArgs)
        if (a.compare(0, key.size(), key) == 0) return std::atol(a.c_str() + key.size());
    return def;
}
std::string argStr(const char* name, const std::string& def) {
    const std::string key = std::string("--") + name + "=";
    for (const auto& a : gArgs)
        if (a.compare(0, key.size(), key) == 0) return a.substr(key.size());
    return def;
}
void report(const char* key, double value, const char* unit) {
    std::printf("MEASURED %s = %.3f %s\n", key, value, unit);
    std::fflush(stdout);
}
}  // namespace pst

using namespace pst;

namespace {

void machine() {
    struct utsname u;
    uname(&u);
    std::printf("MACHINE %s %s %s cpus=%ld\n", u.sysname, u.release, u.machine, sysconf(_SC_NPROCESSORS_ONLN));
}

// Unique frames at producer speed (template + patched counter).
struct FastFrames {
    std::vector<uint8_t> tmpl;
    size_t at = 0;
    explicit FastFrames(size_t pad, uint32_t norad) {
        const double sentinel = 123456.789012345;
        tmpl = ommRecord(norad, "BENCH", "2026-09-01T00:00:00Z", sentinel, pad);
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
    const std::vector<uint8_t>& next(uint64_t n, const uint8_t fid[4]) {
        std::memcpy(tmpl.data() + 8, fid, 4);
        return next(n);
    }
};

struct Bench {
    std::unique_ptr<FaultFs> mem;
    EngineConfig cfg;
    std::unique_ptr<Engine> e;
    std::string dir;
    std::vector<uint32_t> pids;
    std::vector<uint32_t> pidType;
    std::vector<TestType> types;

    Bench(uint32_t writers, bool memIo) {
        cfg.writers = writers;
        cfg.syncThreads = uint32_t(argInt("sync-threads", 0));
        cfg.poolBytes = uint64_t(argInt("pool-mb", 192)) << 20;
        cfg.arenaBytes = 24ull << 20;
        cfg.zeroFillStep = uint64_t(argInt("zero-fill-kb", 1024)) << 10;
        cfg.commitJournal = argInt("journal", 0) != 0;  // A8 fallback mode
        cfg.journalCkptMs = uint32_t(argInt("journal-ckpt-ms", 1000));
        cfg.journalCkptBytes = uint64_t(argInt("journal-ckpt-mb", 8)) << 20;
        cfg.journalCkptPaced = argInt("journal-paced", 1) != 0;
        cfg.lockStats = true;
        if (memIo) {
            mem.reset(new FaultFs(false));
            cfg.io = mem.get();
            cfg.root = "/mem/bench";
        } else {
            dir = argStr("dir", "/tmp/flatsql-ps-bench");
            std::filesystem::remove_all(dir);
            cfg.root = dir;
        }
    }
    ~Bench() {
        if (e) e->stop();
        e.reset();
        if (!dir.empty() && !argInt("keep", 0)) std::filesystem::remove_all(dir);
    }
    bool open(uint32_t partitions) {
        std::string err;
        if (Engine::open(cfg, &e, &err) < 0) {
            std::fprintf(stderr, "open: %s\n", err.c_str());
            return false;
        }
        e->start();
        // Types spread over the writers like a production mix (one type owner
        // per type): OMM-shaped variants with distinct file identifiers.
        const uint32_t nTypes = uint32_t(argInt("types", 8));
        types.clear();
        for (uint32_t k = 0; k < nTypes; k++) {
            char fid[5] = {'B', char('0' + (k / 100) % 10), char('0' + (k / 10) % 10), char('0' + k % 10), 0};
            types.push_back(makeTypeVariant(0, fid, std::string(fid) + ".fbs"));
            e->registerType(types.back().config, &err);
        }
        for (uint32_t i = 0; i < partitions; i++) {
            const std::string prod = "bench-producer-" + std::to_string(i);
            uint32_t pid = 0;
            e->registerPartition(reinterpret_cast<const uint8_t*>(prod.data()), prod.size(),
                                 types[i % nTypes].fid, &pid);
            pids.push_back(pid);
            pidType.push_back(i % nTypes);
        }
        return true;
    }
};

// Closed-loop ingest: producers push as fast as credits allow; returns
// durable records/s (first enqueue to last ack).
double closedLoop(Bench& b, uint64_t records, uint32_t producers, size_t pad) {
    // The router computes CIDs before a record reaches the engine; precompute
    // them (untimed) so the measurement is the writer instance's rate.
    const uint64_t perThread = records / producers;
    std::vector<std::vector<uint8_t>> cids(producers);
    {
        std::vector<std::thread> gen;
        for (uint32_t t = 0; t < producers; t++)
            gen.emplace_back([&, t] {
                FastFrames ff(pad, 1000 + t);
                cids[t].resize(perThread * kCidLen);
                std::vector<uint32_t> mine;
                for (size_t i = t; i < b.pids.size(); i += producers) mine.push_back(uint32_t(i));
                for (uint64_t n = 0; n < perThread; n++) {
                    const auto& f = ff.next(uint64_t(t) << 40 | n, b.types[b.pidType[mine[n % mine.size()]]].fid);
                    computeCid(f.data() + 4, f.size() - 4, cids[t].data() + n * kCidLen);
                }
            });
        for (auto& g : gen) g.join();
    }
    std::vector<std::thread> ts;
    std::atomic<uint32_t> ready{0};
    std::atomic<bool> go{false};
    uint64_t t0 = 0;
    for (uint32_t t = 0; t < producers; t++) {
        ts.emplace_back([&, t] {
            std::vector<Producer> ps;
            std::vector<uint32_t> mine;
            for (size_t i = t; i < b.pids.size(); i += producers) {
                ps.emplace_back(b.e.get(), b.pids[i]);
                mine.push_back(uint32_t(i));
            }
            FastFrames ff(pad, 1000 + t);
            const auto attr = buildRecordAttr("bench", "prov", "src", "b1");
            std::vector<uint64_t> last(ps.size(), 0);
            ready.fetch_add(1);
            while (!go.load()) cpuRelax();
            for (uint64_t n = 0; n < perThread; n++) {
                const size_t k = n % ps.size();
                const auto& f = ff.next(uint64_t(t) << 40 | n, b.types[b.pidType[mine[k]]].fid);
                uint64_t r = 0;
                ps[k].enqueue(kEntRecord, kEntCidPresent, int64_t(n), cids[t].data() + n * kCidLen, attr.data(),
                              uint32_t(attr.size()), f.data(), uint32_t(f.size()), &r, true);
                last[k] = r;
            }
            for (size_t k = 0; k < ps.size(); k++) ps[k].waitAcked(last[k], 600000000000ull);
        });
    }
    while (ready.load() < producers) sleepNs(100000);
    t0 = monoNs();
    go.store(true);
    for (auto& t : ts) t.join();
    const double secs = double(monoNs() - t0) / 1e9;
    return double(perThread * producers) / secs;
}

struct Sample {
    uint32_t pidx;
    uint64_t rseq;
    uint64_t t;
};

// Open-loop ingest at `rate` records/s over `nDirty` partitions for `secs`;
// a completion poller (250 us, like the Go poller) timestamps acks.
std::vector<double> openLoop(Bench& b, double rate, uint32_t nDirty, double secs, size_t pad, uint64_t* sentOut) {
    std::vector<std::unique_ptr<Producer>> ps;
    for (uint32_t i = 0; i < nDirty; i++) ps.emplace_back(new Producer(b.e.get(), b.pids[i]));
    std::vector<std::deque<Sample>> pending(nDirty);
    std::mutex mu;
    std::vector<double> lat;
    lat.reserve(size_t(rate * secs) + 16);
    std::atomic<bool> done{false};
    std::thread poller([&] {
        uint64_t doneAt = 0;
        while (true) {
            const bool finish = done.load();
            if (finish && !doneAt) doneAt = monoNs();
            if (doneAt && monoNs() - doneAt > 10000000000ull) {
                // Diagnostics for a stall: ring positions and writer liveness.
                std::lock_guard<std::mutex> g(mu);
                for (uint32_t i = 0; i < nDirty; i++) {
                    RingDesc* r = ps[i]->ringDesc();
                    if (pending[i].empty()) continue;
                    std::fprintf(stderr, "STALL pid=%u tail=%llu head=%llu acked=%llu pendingFront=%llu state=%u mapped=%u want=%u\n",
                                 b.pids[i], (unsigned long long)r->tail.load(), (unsigned long long)r->head.load(),
                                 (unsigned long long)r->ackedRseq.load(), (unsigned long long)pending[i].front().rseq,
                                 r->state.load(), r->mappedPages.load(), r->wantPage.load());
                }
                for (uint32_t w = 0; w < b.e->writerCount(); w++)
                    std::fprintf(stderr, "STALL writer %u heartbeat=%llu\n", w,
                                 (unsigned long long)b.e->writer(w)->heartbeat());
                doneAt = monoNs();
            }
            {
                std::lock_guard<std::mutex> g(mu);
                const uint64_t now = monoNs();
                bool any = false;
                for (uint32_t i = 0; i < nDirty; i++) {
                    const uint64_t acked = ps[i]->ringDesc()->ackedRseq.load(std::memory_order_acquire);
                    while (!pending[i].empty() && pending[i].front().rseq <= acked) {
                        lat.push_back(double(now - pending[i].front().t) / 1e6);
                        pending[i].pop_front();
                    }
                    any = any || !pending[i].empty();
                }
                if (finish && !any) break;
            }
            sleepNs(250000);
        }
    });
    FastFrames ff(pad, 77);
    const auto attr = buildRecordAttr("bench", "prov", "src", "b1");
    const uint64_t start = monoNs();
    const uint64_t total = uint64_t(rate * secs);
    const double interval = 1e9 / rate;
    uint64_t sent = 0;
    uint64_t lastReport = start;
    for (uint64_t n = 0; n < total; n++) {
        if (monoNs() - lastReport > 2000000000ull) {
            lastReport = monoNs();
            std::fprintf(stderr, "PROGRESS n=%llu of %llu\n", (unsigned long long)n, (unsigned long long)total);
        }
        const uint64_t due = start + uint64_t(double(n) * interval);
        for (uint64_t now = monoNs(); now < due; now = monoNs()) {
            const uint64_t left = due - now;
            if (left > 200000) sleepNs(left - 100000);
            else cpuRelax();
        }
        const uint32_t k = uint32_t(n % nDirty);
        const auto& f = ff.next((uint64_t(0x5a) << 48) | n, b.types[b.pidType[k]].fid);
        uint8_t cid[kCidLen];
        computeCid(f.data() + 4, f.size() - 4, cid);
        uint64_t r = 0;
        const uint64_t t = monoNs();
        if (ps[k]->enqueue(kEntRecord, kEntCidPresent, int64_t(n), cid, attr.data(), uint32_t(attr.size()), f.data(),
                           uint32_t(f.size()), &r, true) == 0) {
            std::lock_guard<std::mutex> g(mu);
            pending[k].push_back({k, r, t});
            sent++;
        }
    }
    done.store(true);
    poller.join();
    if (sentOut) *sentOut = sent;
    std::sort(lat.begin(), lat.end());
    return lat;
}

double pct(const std::vector<double>& v, double q) {
    if (v.empty()) return 0;
    return v[std::min(v.size() - 1, size_t(double(v.size()) * q))];
}

void reportSyncs(Engine* e, const char* prefix) {
    e->stop();  // quiesce: counters from one consistent moment
    const EngineStats st = e->stats();
    IoStats io;
    e->totalIo(&io);
    char key[128];
    std::snprintf(key, sizeof(key), "%s_sync_rounds_per_committing_iteration", prefix);
    report(key, st.iterationsWithCommit ? double(st.commitSyncRounds) / double(st.iterationsWithCommit) : 0, "rounds");
    // A partition commit with frames syncs d once and m once (2); a batch
    // without frames (merge intents) syncs m only.
    std::snprintf(key, sizeof(key), "%s_d_fsyncs_per_framed_partition_commit", prefix);
    report(key, st.partitionCommitsWithFrames ? double(io.syncs(FileClass::Data)) / double(st.partitionCommitsWithFrames) : 0,
           "fsyncs");
    std::snprintf(key, sizeof(key), "%s_m_fsyncs_per_partition_batch", prefix);
    report(key, st.partitionBatches ? double(io.syncs(FileClass::Meta)) / double(st.partitionBatches) : 0, "fsyncs");
    std::snprintf(key, sizeof(key), "%s_records_per_framed_commit", prefix);
    report(key, st.partitionCommitsWithFrames ? double(st.rowsAppended) / double(st.partitionCommitsWithFrames) : 0,
           "records");
    std::snprintf(key, sizeof(key), "%s_total_fsyncs", prefix);
    report(key, double(io.totalSyncs()), "fsyncs");
    if (st.journalRecords) {
        std::snprintf(key, sizeof(key), "%s_journal_fsyncs_per_committing_iteration", prefix);
        report(key, st.iterationsWithCommit ? double(st.journalRecords) / double(st.iterationsWithCommit) : 0, "fsyncs");
        std::snprintf(key, sizeof(key), "%s_journal_checkpoints", prefix);
        report(key, double(st.journalCheckpoints), "checkpoints");
        std::snprintf(key, sizeof(key), "%s_journal_bytes", prefix);
        report(key, double(st.journalBytes), "bytes");
    }
    std::snprintf(key, sizeof(key), "%s_maintenance_step_max_ms", prefix);
    report(key, double(e->maintHist().maxNs.load()) / 1e6, "ms");
    std::snprintf(key, sizeof(key), "%s_maintenance_step_p99_ms", prefix);
    report(key, double(e->maintHist().percentileNs(0.99)) / 1e6, "ms");
    std::snprintf(key, sizeof(key), "%s_commit_round_max_ms", prefix);
    report(key, double(e->commitHist().maxNs.load()) / 1e6, "ms");
    std::snprintf(key, sizeof(key), "%s_commit_round_p99_ms", prefix);
    report(key, double(e->commitHist().percentileNs(0.99)) / 1e6, "ms");
}

int modeScaling() {
    const bool memIo = argStr("io", "mem") == "mem";
    const uint32_t partitions = uint32_t(argInt("partitions", 64));
    const uint64_t records = uint64_t(argInt("records", 2000000));
    const uint32_t producers = uint32_t(argInt("producers", 8));
    const size_t pad = size_t(argInt("pad", 200));
    std::vector<uint32_t> ns = {1, uint32_t(argInt("writers", 8))};
    double rates[2] = {0, 0};
    for (int i = 0; i < 2; i++) {
        Bench b(ns[size_t(i)], memIo);
        if (!b.open(partitions)) return 1;
        rates[i] = closedLoop(b, records, producers, pad);
        char key[96];
        std::snprintf(key, sizeof(key), "scaling_%s_N%u_records_per_s", memIo ? "mem" : "fs", ns[size_t(i)]);
        report(key, rates[i], "records/s");
        std::snprintf(key, sizeof(key), "scaling_%s_N%u", memIo ? "mem" : "fs", ns[size_t(i)]);
        reportSyncs(b.e.get(), key);
    }
    report(memIo ? "scaling_mem_speedup_N8_over_N1" : "scaling_fs_speedup_N8_over_N1", rates[1] / rates[0], "x");
    return 0;
}

int modeHost02() {
    const uint32_t partitions = uint32_t(argInt("partitions", 50));
    const double secs = double(argInt("seconds", 20));
    const size_t pad = size_t(argInt("pad", 200));
    // 1. Sustained maximum (closed loop, 50 partitions, N = 1).
    double maxRate;
    {
        Bench b(1, false);
        if (!b.open(partitions)) return 1;
        maxRate = closedLoop(b, uint64_t(argInt("records", 200000)), uint32_t(argInt("producers", 2)), pad);
        report("host02_sustained_records_per_s", maxRate, "records/s");
        reportSyncs(b.e.get(), "host02");
    }
    // 2. Durable-ack latency at 50% of 5,000 records/s (the acceptance rate).
    const double rate = double(argInt("rate", 2500));
    {
        Bench b(1, false);
        if (!b.open(partitions)) return 1;
        uint64_t sent = 0;
        const auto lat = openLoop(b, rate, partitions, secs, pad, &sent);
        report("host02_open_loop_rate", rate, "records/s");
        report("host02_ack_p50_ms", pct(lat, 0.50), "ms");
        report("host02_ack_p99_ms", pct(lat, 0.99), "ms");
        report("host02_ack_max_ms", lat.empty() ? 0 : lat.back(), "ms");
        report("host02_acked", double(lat.size()), "records");
        report("host02_sent", double(sent), "records");
    }
    return 0;
}

int modeDirty() {
    const double rate = double(argInt("rate", 2500));
    const double secs = double(argInt("seconds", 10));
    for (uint32_t dirty : {1u, 10u, 50u}) {
        Bench b(1, false);
        if (!b.open(50)) return 1;
        uint64_t sent = 0;
        const auto lat = openLoop(b, rate, dirty, secs, 200, &sent);
        char key[96];
        std::snprintf(key, sizeof(key), "a8_ack_p99_ms_dirty_%u", dirty);
        report(key, pct(lat, 0.99), "ms");
        std::snprintf(key, sizeof(key), "a8_ack_p50_ms_dirty_%u", dirty);
        report(key, pct(lat, 0.50), "ms");
        std::snprintf(key, sizeof(key), "a8_dirty_%u", dirty);
        reportSyncs(b.e.get(), key);
    }
    return 0;
}

int modeSoak() {
    const double secs = double(argInt("seconds", 1800));
    const bool memIo = argStr("io", "fs") == "mem";
    Bench b(uint32_t(argInt("writers", 2)), memIo);
    if (!b.open(50)) return 1;
    // Warm-up (interns lanes, warms partitions and the type owner).
    closedLoop(b, 20000, 2, 200);
    const uint64_t rows0 = b.e->stats().rowsAppended;
    const uint64_t hot0 = gHotAllocs.load();
    uint64_t sent = 0;
    const auto lat = openLoop(b, double(argInt("rate", 2000)), 50, secs, 200, &sent);
    const uint64_t rows = b.e->stats().rowsAppended - rows0;
    const uint64_t hot = gHotAllocs.load() - hot0;
    LockHist& sq = b.e->seqlockHist();
    LockHist& rg = b.e->lockHist();
    report("soak_seconds", secs, "s");
    report("soak_records", double(rows), "records");
    report("soak_hot_path_allocations_per_record", rows ? double(hot) / double(rows) : 0, "allocations");
    report("soak_seqlock_writer_max_ms", double(sq.maxNs.load()) / 1e6, "ms");
    report("soak_seqlock_writer_p999_ms", double(sq.percentileNs(0.999)) / 1e6, "ms");
    report("soak_seqlock_writer_sections", double(sq.count.load()), "sections");
    report("soak_registration_lock_max_ms", double(rg.maxNs.load()) / 1e6, "ms");
    report("soak_ack_p99_ms", pct(lat, 0.99), "ms");
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    for (int i = 1; i < argc; i++) gArgs.push_back(argv[i]);
    machine();
    const std::string mode = argStr("mode", "scaling");
    report("commit_journal", double(argInt("journal", 0) != 0), "mode");
    if (mode == "scaling") return modeScaling();
    if (mode == "host02") return modeHost02();
    if (mode == "dirty") return modeDirty();
    if (mode == "soak") return modeSoak();
    std::fprintf(stderr, "unknown --mode\n");
    return 2;
}
