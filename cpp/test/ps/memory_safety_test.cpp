// Memory safety and race tests (flatsql-ps-memory-safety-races-20260929).
//
//   memory_safety_seqlock_publication_consistent
//       The partition publish seqlock (writer.h publishPartition /
//       readPublishedPartition): readers racing a publishing owner only ever
//       copy out a state the owner published whole. The protected words are
//       relaxed atomics, so ThreadSanitizer sees no race either.
//   memory_safety_sync_pool_rounds_run_each_job_once
//       SyncPool (A8): writers publishing rounds back to back while pool
//       threads help: every job of every round runs exactly once and its
//       result reaches its writer. The slot's jobs pointer is read by helpers
//       while the next round is published (atomic).
//   memory_safety_sleep_and_notify_at_folded_offsets
//       Host conformance for wait/notify. sleepNs sleeps, and a notify whose
//       address the compiler folded into the instruction's memarg offset (a
//       global: `i32.const 0; memory.atomic.notify offset=<addr>`, as
//       wasi-libc's thread-list lock does) wakes its waiter. WasmEdge 0.16.4's
//       AOT compiler dropped that offset: sleeps returned at once and thread
//       exit/join lost wakeups (docs/PARTITION-STORE-WASM.md §1).
#include <algorithm>
#include <atomic>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

#include "flatsql/ps/platform.h"
#include "flatsql/ps/writer.h"
#include "ps/ps_test.h"

using namespace flatsql::ps;
using namespace pst;

namespace {

// The state published for version v: every field derives from v.
void fillVersion(uint64_t v, uint64_t* commitSeq, uint64_t* pseqHi, uint32_t* nL0, L0DirEntry* l0) {
    *commitSeq = v;
    *pseqHi = v * 1000 + 7;
    *nL0 = uint32_t(v % (kMaxL0Dir + 1));
    for (uint32_t i = 0; i < *nL0; i++) {
        L0DirEntry& e = l0[i];
        e.mSeg = uint32_t(v);
        e.nRows = i + 1;
        e.mOff = v * 131 + i;
        e.firstPseq = v * 1000 + i * 10;
        e.batchLen = uint32_t(v ^ i);
        e.l0Off = i * 3;
    }
}

bool consistent(const PublishedPart& p) {
    const uint64_t v = p.commitSeq;
    if (p.pseqHi != v * 1000 + 7 || p.nL0 != uint32_t(v % (kMaxL0Dir + 1))) return false;
    for (uint32_t i = 0; i < p.nL0; i++) {
        const L0DirEntry& e = p.l0[i];
        if (e.mSeg != uint32_t(v) || e.nRows != i + 1 || e.mOff != v * 131 + i || e.firstPseq != v * 1000 + i * 10 ||
            e.batchLen != uint32_t(v ^ i) || e.l0Off != i * 3)
            return false;
    }
    return true;
}

// The writer thread running (none: -1), so a sync can tell whether a pool
// thread helped.
thread_local int tWriter = -1;

// A sync-only Io: result = handle * 3 + 1, a count per handle, and how many
// ran on a thread other than their writer's. Each sync spins ~2 us so rounds
// last long enough for the pool threads to join in.
class CountingIo : public Io {
public:
    CountingIo(size_t handles, size_t perWriter) : calls_(handles), perWriter_(perWriter) {}
    int32_t open(const char*, int32_t, int32_t) override { return -1; }
    int32_t read(int32_t, void*, int32_t, double) override { return -1; }
    int32_t write(int32_t, const void*, int32_t, double) override { return -1; }
    int32_t truncate(int32_t, double) override { return -1; }
    int32_t sync(int32_t handle) override {
        calls_[size_t(handle)].fetch_add(1, std::memory_order_relaxed);
        if (tWriter != int(size_t(handle) / perWriter_)) helped_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t t0 = monoNs();
        while (monoNs() - t0 < 2000) cpuRelax();
        return handle * 3 + 1;
    }
    double size(int32_t) override { return -1; }
    int32_t close(int32_t) override { return -1; }
    uint32_t calls(size_t handle) const { return calls_[handle].load(std::memory_order_relaxed); }
    uint64_t helped() const { return helped_.load(std::memory_order_relaxed); }

private:
    std::vector<std::atomic<uint32_t>> calls_;
    size_t perWriter_;
    std::atomic<uint64_t> helped_{0};
};

#if defined(__wasm__)
// Globals: the compiler folds their addresses into the memarg offset of the
// wait and notify below (`i32.const 0` plus offset=<address>).
std::atomic<uint32_t> gWord{0};
std::atomic<uint32_t> gWaiting{0};
#endif

}  // namespace

PS_TEST(memory_safety_seqlock_publication_consistent) {
    SeqLock lock;
    PublishedPart pub;
    const uint64_t ms = uint64_t(argInt("seconds", 1)) * 1000;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> published{0};
    std::thread owner([&] {
        L0DirEntry l0[kMaxL0Dir];
        uint64_t commitSeq, pseqHi;
        uint32_t nL0;
        for (uint64_t v = 1; !stop.load(std::memory_order_relaxed); v++) {
            fillVersion(v, &commitSeq, &pseqHi, &nL0, l0);
            publishPartition(lock, pub, commitSeq, pseqHi, nL0, l0);
            published.store(v, std::memory_order_relaxed);
        }
    });
    constexpr int kReaders = 3;
    std::atomic<uint64_t> reads{0}, torn{0}, backwards{0};
    std::vector<std::thread> readers;
    for (int r = 0; r < kReaders; r++)
        readers.emplace_back([&] {
            uint64_t last = 0, n = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                PublishedPart p;
                readPublishedPartition(lock, pub, &p);
                n++;
                if (p.commitSeq == 0) continue;  // nothing published yet
                if (!consistent(p)) torn.fetch_add(1);
                if (p.commitSeq < last) backwards.fetch_add(1);
                last = p.commitSeq;
            }
            reads.fetch_add(n);
        });
    sleepNs(ms * 1000000ull);
    stop = true;
    owner.join();
    for (auto& t : readers) t.join();
    report("seqlock_versions_published", double(published.load()), "versions");
    report("seqlock_reads", double(reads.load()), "reads");
    CHECK(published.load() > 0);
    CHECK(reads.load() > 0);
    CHECK_EQ(torn.load(), 0ull);
    CHECK_EQ(backwards.load(), 0ull);
}

PS_TEST(memory_safety_sync_pool_rounds_run_each_job_once) {
    constexpr uint32_t kWriters = 6;
    const uint32_t rounds = uint32_t(argInt("rounds", 400));
    constexpr uint32_t kMaxJobs = 40;
    // One handle per (writer, round, job): every job is counted on its own.
    const size_t handles = size_t(kWriters) * rounds * kMaxJobs;
    CountingIo io(handles, size_t(rounds) * kMaxJobs);
    SyncPool pool;
    pool.start(4);
    std::atomic<uint64_t> wrong{0}, jobsRun{0};
    std::vector<std::thread> writers;
    for (uint32_t w = 0; w < kWriters; w++)
        writers.emplace_back([&, w] {
            tWriter = int(w);
            std::mt19937 rng(w * 7919u + 1);
            std::vector<SyncJob> jobs;
            std::vector<int32_t> results;
            for (uint32_t r = 0; r < rounds; r++) {
                const uint32_t n = 2 + rng() % (kMaxJobs - 1);
                jobs.assign(n, SyncJob());
                results.assign(n, -12345);
                for (uint32_t j = 0; j < n; j++) {
                    jobs[j].io = &io;
                    jobs[j].handle = int32_t((size_t(w) * rounds + r) * kMaxJobs + j);
                    jobs[j].result = &results[j];
                }
                pool.runAll(w, jobs.data(), n);
                for (uint32_t j = 0; j < n; j++)
                    if (results[j] != jobs[j].handle * 3 + 1) wrong.fetch_add(1);
                jobsRun.fetch_add(n);
            }
        });
    for (auto& t : writers) t.join();
    pool.stop();
    uint64_t once = 0, other = 0;
    for (size_t h = 0; h < handles; h++) {
        const uint32_t c = io.calls(h);
        if (c == 1) once++;
        else if (c > 1) other++;
    }
    report("sync_pool_jobs", double(jobsRun.load()), "jobs");
    report("sync_pool_jobs_helped", double(io.helped()), "jobs");
    CHECK(io.helped() > 0);  // the pool threads took part
    CHECK_EQ(wrong.load(), 0ull);
    CHECK_EQ(once, jobsRun.load());
    CHECK_EQ(other, 0ull);
}

PS_TEST(memory_safety_sleep_and_notify_at_folded_offsets) {
    // sleepNs sleeps (a wait that returns at once turns every backoff into a
    // busy spin).
    const uint64_t t0 = monoNs();
    for (int i = 0; i < 5; i++) sleepNs(20000000);
    const double sleptMs = double(monoNs() - t0) / 1e6;
    report("sleep_5x20ms", sleptMs, "ms");
    CHECK(sleptMs >= 90.0);
#if defined(__wasm__)
    // A waiter on a global, reached through a volatile pointer (the address
    // is the operand, memarg offset 0: right on every runtime), woken by a
    // notify on the global itself (the compiler folds its address into the
    // memarg offset: `i32.const 0; memory.atomic.notify offset=<addr>`). The
    // wait is bounded: a lost wakeup reads as a timeout (2), not a hang.
    int woken = 0, timedOut = 0;
    for (int round = 0; round < 20; round++) {
        gWord.store(0);
        gWaiting.store(0);
        int result = -1;
        std::thread waiter([&] {
            int* volatile wp = reinterpret_cast<int*>(&gWord);
            gWaiting.store(1);
            result = __builtin_wasm_memory_atomic_wait32(wp, 0, 2000000000ll);
        });
        while (!gWaiting.load()) sleepNs(100000);
        sleepNs(5000000);  // the waiter is asleep in the wait by now
        gWord.store(1);
        __builtin_wasm_memory_atomic_notify(reinterpret_cast<int*>(&gWord), 1);
        waiter.join();
        // 0: woken; 1: the word had changed already (fine); 2: timed out.
        if (result == 2) timedOut++;
        else woken++;
    }
    report("folded_offset_notify_woken", double(woken), "waits");
    CHECK_EQ(timedOut, 0);
    // A wait on the global itself (folded) compares the global: 7 there, so
    // waiting for 7 times out (2); comparing another word returns 1 at once.
    gWord.store(7);
    const int folded = __builtin_wasm_memory_atomic_wait32(reinterpret_cast<int*>(&gWord), 7, 20000000ll);
    CHECK_EQ(folded, 2);
#else
    std::printf("  NOTE the wait/notify memarg check runs in the wasm commands only\n");
#endif
}
