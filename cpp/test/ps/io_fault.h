// Fault-injecting in-memory host I/O for the partition store (design §19.3).
//
// Every file keeps two images: what reads see now (the page cache) and what
// survives a crash (the device). A write lands in the page cache and on a
// pending list; flatsql_io_sync makes that file's pending list durable. A
// crash applies one of the modes below to the pending lists of every file,
// then closes every handle:
//   kDropAll     all unsynced writes vanish
//   kDropSubset  each unsynced op survives with p = 1/2, in order
//   kTearLast    everything survives except the last write, cut at 512 B
//   kReorder     a random subset survives, applied in a random order
//   kKeepAll     kill -9: the page cache survives whole
// Directory entries: a file created with CREATE_PARENTS (or unlinked with
// UNLINK_IF_UNUSED) has a durable entry at once; otherwise the entry becomes
// durable on a DIRECTORY sync of its parent. Crashing drops non-durable
// entries and restores non-durable unlinks.
//
// The same class is the "in-memory I/O shim" of the scaling benchmark with
// tracking off (no pending lists, sync is free).
#ifndef FLATSQL_PS_TEST_IO_FAULT_H
#define FLATSQL_PS_TEST_IO_FAULT_H

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "flatsql/ps/io.h"

namespace flatsql {
namespace ps {
namespace test {

class FaultFs final : public Io {
public:
    enum CrashMode { kDropAll = 0, kDropSubset, kTearLast, kReorder, kKeepAll, kModeCount };

    explicit FaultFs(bool tracking = true) : tracking_(tracking) {}

    int32_t open(const char* path, int32_t pathLen, int32_t flags) override;
    int32_t read(int32_t handle, void* dst, int32_t len, double offset) override;
    int32_t write(int32_t handle, const void* src, int32_t len, double offset) override;
    int32_t truncate(int32_t handle, double size) override;
    int32_t sync(int32_t handle) override;
    double size(int32_t handle) override;
    int32_t close(int32_t handle) override;

    // Freeze: every later mutating call fails with FLATSQL_IO_ERR_IO and has
    // no effect (the process is dying). armCrashAtOp freezes automatically at
    // the Nth mutating call.
    void freeze() { frozen_.store(true); }
    bool frozen() const { return frozen_.load(); }
    void armCrashAtOp(uint64_t n) { crashAt_.store(n); }
    uint64_t mutatingOps() const { return ops_.load(); }
    // Applies a crash, closes all handles, unfreezes.
    void crash(CrashMode mode, uint64_t seed);

    // Fault injection: the next `count` syncs of paths containing `needle`
    // fail with EIO (or writes with NOSPACE).
    void failSyncs(const std::string& needle, int count);
    void failWrites(const std::string& needle, int count, int32_t code);
    // Latency injection for syncs (ns), e.g. to model a disk.
    void setSyncLatencyNs(uint64_t ns) { syncLatencyNs_ = ns; }

    // Test-only introspection.
    bool exists(const std::string& path);
    std::vector<uint8_t> contents(const std::string& path);
    std::vector<std::string> list(const std::string& prefix);
    uint64_t totalBytes(const std::string& prefix);
    uint64_t syncCount() const { return syncs_.load(); }
    uint64_t openHandles();

private:
    struct Op {
        uint8_t kind;  // 0 write, 1 truncate
        uint64_t seq;
        uint64_t off;
        uint64_t size;
        std::vector<uint8_t> data;
    };
    struct File {
        std::mutex mu;
        std::vector<uint8_t> cur;
        std::vector<uint8_t> durable;
        std::vector<Op> pending;
    };
    struct Entry {
        std::shared_ptr<File> file;
        bool durableEntry = false;
    };
    // Handle slots never move and files are never freed while the FaultFs
    // lives, so data calls look a handle up without the namespace lock.
    struct Handle {
        std::atomic<File*> file{nullptr};
        std::string path;
        bool isDir = false;
        std::atomic<bool> inUse{false};
    };
    static constexpr size_t kMaxHandles = 1u << 18;
    bool countOp();
    File* lookupHandle(int32_t h, std::string* path, bool* isDir);
    int32_t allocHandle(File* f, const std::string& path, bool isDir);

    bool tracking_;
    std::mutex mu_;  // namespace + handle table
    std::map<std::string, Entry> ns_;                       // current namespace
    std::map<std::string, std::shared_ptr<File>> unlinked_;  // non-durable unlinks
    std::unique_ptr<Handle[]> handles_{new Handle[kMaxHandles]};
    std::atomic<size_t> highWater_{0};
    size_t nextFree_ = 0;
    std::vector<std::shared_ptr<File>> keepAlive_;
    std::atomic<bool> frozen_{false};
    std::atomic<uint64_t> ops_{0};
    std::atomic<uint64_t> crashAt_{0};
    std::atomic<uint64_t> seq_{0};
    std::atomic<uint64_t> syncs_{0};
    uint64_t syncLatencyNs_ = 0;
    std::string failSyncNeedle_;
    std::atomic<int> failSyncCount_{0};
    std::string failWriteNeedle_;
    std::atomic<int> failWriteCount_{0};
    int32_t failWriteCode_ = FLATSQL_IO_ERR_NOSPACE;
};

}  // namespace test
}  // namespace ps
}  // namespace flatsql

#endif
