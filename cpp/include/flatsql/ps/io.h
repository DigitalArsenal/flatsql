// FlatSQL partition store: the engine's view of host I/O.
//
// The engine never names a host. It calls an Io, whose default instance
// forwards to the seven flatsql_io_* imports (flatsql_io.h). Tests substitute
// a fault-injecting in-memory Io (cpp/test/io_fault.cpp); the wasm artifact
// only ever uses the imports. Every call is accounted per file class so
// acceptance can prove, for example, that open read zero data-segment bytes.
#ifndef FLATSQL_PS_IO_H
#define FLATSQL_PS_IO_H

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "flatsql/flatsql_io.h"

namespace flatsql {
namespace ps {

class Io {
public:
    virtual ~Io() = default;
    virtual int32_t open(const char* path, int32_t pathLen, int32_t flags) = 0;
    virtual int32_t read(int32_t handle, void* dst, int32_t len, double offset) = 0;
    virtual int32_t write(int32_t handle, const void* src, int32_t len, double offset) = 0;
    virtual int32_t truncate(int32_t handle, double size) = 0;
    virtual int32_t sync(int32_t handle) = 0;
    virtual double size(int32_t handle) = 0;
    virtual int32_t close(int32_t handle) = 0;
};

// The seven imports (natively: flatsql_io_native.cpp).
Io* importIo();

enum class FileClass : uint8_t {
    Store = 0,
    Registry,
    Head,
    Meta,       // m-<seg>.fsl (partition)
    Data,       // d-<seg>.fsd
    Rows,       // r-<seg>.fsr
    Attrs,      // a-<seg>.fsa
    Index,      // x-*.fsx
    Manifest,   // mf-*.fsm
    Lanes,      // l.fsl
    TypeHead,
    TypeMeta,
    Arrivals,   // g-<seg>.fsg
    Config,     // t/<fid>/s-<fp>.fsc
    Directory,
    Journal,    // j/<writer>-<a|b>.fsj (A8 commit journal)
    Count
};

struct IoCounters {
    std::atomic<uint64_t> readBytes{0};
    std::atomic<uint64_t> readCalls{0};
    std::atomic<uint64_t> writeBytes{0};
    std::atomic<uint64_t> writeCalls{0};
    std::atomic<uint64_t> syncs{0};
    std::atomic<uint64_t> opens{0};
    std::atomic<uint64_t> truncates{0};
};

struct IoStats {
    IoCounters c[size_t(FileClass::Count)];
    uint64_t readBytes(FileClass k) const { return c[size_t(k)].readBytes.load(); }
    uint64_t writeBytes(FileClass k) const { return c[size_t(k)].writeBytes.load(); }
    uint64_t syncs(FileClass k) const { return c[size_t(k)].syncs.load(); }
    uint64_t totalReadBytes() const {
        uint64_t s = 0;
        for (const auto& x : c) s += x.readBytes.load();
        return s;
    }
    uint64_t totalSyncs() const {
        uint64_t s = 0;
        for (const auto& x : c) s += x.syncs.load();
        return s;
    }
    void reset() {
        for (auto& x : c) {
            x.readBytes = 0;
            x.readCalls = 0;
            x.writeBytes = 0;
            x.writeCalls = 0;
            x.syncs = 0;
            x.opens = 0;
            x.truncates = 0;
        }
    }
};

// One open file as the engine holds it.
struct FileRef {
    int32_t handle = -1;
    FileClass cls = FileClass::Store;
    bool valid() const { return handle >= 0; }
};

// Thin accounted wrappers. All return the host's status convention.
class IoCtx {
public:
    IoCtx(Io* io, IoStats* stats) : io_(io), stats_(stats) {}
    Io* io() const { return io_; }
    IoStats* stats() const { return stats_; }

    int32_t open(const char* path, size_t len, int32_t flags, FileClass cls, FileRef* out);
    int32_t probe(const char* path, size_t len);
    int32_t unlink(const char* path, size_t len, bool durableIfUnused);
    // Reads exactly len bytes or fails (short read at EOF => returns bytes read).
    int64_t read(const FileRef& f, void* dst, size_t len, uint64_t off);
    // Writes all bytes or fails. Splits into <= 64 MiB host calls.
    int32_t write(const FileRef& f, const void* src, size_t len, uint64_t off);
    int32_t writeZeros(const FileRef& f, uint64_t off, uint64_t len);
    int32_t truncate(const FileRef& f, uint64_t size);
    int32_t sync(const FileRef& f);
    int64_t size(const FileRef& f);
    int32_t close(FileRef* f);

private:
    Io* io_;
    IoStats* stats_;
};

}  // namespace ps
}  // namespace flatsql

#endif
