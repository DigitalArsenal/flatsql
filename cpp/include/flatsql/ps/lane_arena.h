// FlatSQL partition store: per-lane memory arenas (design §5.3, T2).
//
// Every reader lane owns one arena: a fixed region carved by a two-level
// segregated-fit allocator (TLSF: O(1) alloc and free, immediate coalescing,
// 16-byte alignment). SQLite's allocator is installed process-wide with
// SQLITE_CONFIG_MALLOC and dispatches on the calling thread's bound arena, so
// a lane's connection, its statements, sorters, lookaside and result scratch
// all live in that lane's arena and a statement that exhausts it fails with
// SQLITE_NOMEM without touching any other lane (acceptance T2 #4).
//
// An arena is never shared: only its lane thread allocates from it or frees
// into it. Threads without a bound arena (process start-up, SQLite's own
// global initialization) use the system allocator through a tagged header,
// so a pointer always finds its owner.
#ifndef FLATSQL_PS_LANE_ARENA_H
#define FLATSQL_PS_LANE_ARENA_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace flatsql {
namespace ps {

class LaneArena {
public:
    LaneArena() = default;
    ~LaneArena();
    LaneArena(const LaneArena&) = delete;
    LaneArena& operator=(const LaneArena&) = delete;

    // Reserves `bytes` (rounded to 64 KiB). Natively the region is an
    // anonymous mapping: untouched pages cost no resident memory.
    bool init(size_t bytes);
    void* alloc(size_t n);
    void free(void* p);
    void* realloc(void* p, size_t n);
    size_t usable(const void* p) const;
    bool owns(const void* p) const {
        return reinterpret_cast<uintptr_t>(p) >= base_ && reinterpret_cast<uintptr_t>(p) < end_;
    }

    size_t capacity() const { return cap_; }
    size_t used() const { return used_; }
    size_t highWater() const { return high_.load(std::memory_order_relaxed); }
    uint64_t failures() const { return failures_.load(std::memory_order_relaxed); }
    void resetHighWater() { high_.store(used_, std::memory_order_relaxed); }
    // Self-check of every block and free list (tests).
    bool check(std::string* why) const;

private:
    static constexpr int kSlLog2 = 4;
    static constexpr int kSlCount = 1 << kSlLog2;
    static constexpr int kFlShift = kSlLog2 + 4;  // small blocks: < 256 bytes, 16-byte classes
    static constexpr int kFlCount = 40 - kFlShift + 1;
    struct Block;
    void insertFree(Block* b);
    void removeFree(Block* b);
    Block* findFree(size_t size);
    static void mapping(size_t size, int* fl, int* sl);

    uintptr_t base_ = 0;
    uintptr_t end_ = 0;
    size_t cap_ = 0;
    size_t mapped_ = 0;
    size_t used_ = 0;
    std::atomic<size_t> high_{0};
    std::atomic<uint64_t> failures_{0};
    uint64_t flBitmap_ = 0;
    uint32_t slBitmap_[kFlCount] = {};
    Block* heads_[kFlCount][kSlCount] = {};
};

// Binds (or unbinds, nullptr) the calling thread's arena for SQLite.
void laneArenaBind(LaneArena* a);
LaneArena* laneArenaCurrent();

// One-time SQLite configuration for the reader instances of this process:
// SQLITE_CONFIG_MALLOC (arena dispatch), SQLITE_CONFIG_MUTEX (instrumented
// wrappers over the default mutexes), MEMSTATUS off, and the null VFS
// "flatsql_ps_null" that refuses every file open (lanes are :memory: with
// temp_store=MEMORY). Safe to call from any thread, any number of times.
int32_t readerSqliteInit(std::string* err);
constexpr const char* kNullVfsName = "flatsql_ps_null";

// Lock instrumentation (T2 #1, A29: kept natively; the disjointness
// acceptance itself is measured under WasmEdge in T5/T6). Every SQLite mutex
// acquisition is timed; waits are recorded per thread class.
enum class ThreadClass : uint8_t { Other = 0, Lane = 1, Writer = 2 };
void setThreadClass(ThreadClass c);
struct LockReport {
    struct Entry {
        std::string name;       // SQLite mutex id ("static_main", "recursive", ...)
        uint64_t laneAcquires = 0;
        uint64_t writerAcquires = 0;
        uint64_t otherAcquires = 0;
        uint64_t laneMaxWaitNs = 0;
        uint64_t laneContended = 0;   // lane acquisitions that had to wait
    };
    std::vector<Entry> entries;
    uint64_t laneMaxWaitNs = 0;
};
LockReport lockReport();
void lockReportReset();

}  // namespace ps
}  // namespace flatsql

#endif
