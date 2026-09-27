// FlatSQL partition store: accounted host I/O (see ps/io.h).
#include "flatsql/ps/io.h"

#include <cstring>

namespace flatsql {
namespace ps {

namespace {

class ImportIo final : public Io {
public:
    int32_t open(const char* path, int32_t pathLen, int32_t flags) override {
        return flatsql_io_open(path, pathLen, flags);
    }
    int32_t read(int32_t handle, void* dst, int32_t len, double offset) override {
        return flatsql_io_read(handle, dst, len, offset);
    }
    int32_t write(int32_t handle, const void* src, int32_t len, double offset) override {
        return flatsql_io_write(handle, src, len, offset);
    }
    int32_t truncate(int32_t handle, double size) override {
        return flatsql_io_truncate(handle, size);
    }
    int32_t sync(int32_t handle) override { return flatsql_io_sync(handle); }
    double size(int32_t handle) override { return flatsql_io_size(handle); }
    int32_t close(int32_t handle) override { return flatsql_io_close(handle); }
};

constexpr size_t kMaxCall = size_t(64) << 20;
alignas(64) const uint8_t kZeros[64 * 1024] = {0};

inline IoCounters& ctr(IoStats* s, FileClass c) { return s->c[size_t(c)]; }

}  // namespace

Io* importIo() {
    static ImportIo io;
    return &io;
}

int32_t IoCtx::open(const char* path, size_t len, int32_t flags, FileClass cls, FileRef* out) {
    const int32_t h = io_->open(path, int32_t(len), flags);
    ctr(stats_, cls).opens.fetch_add(1, std::memory_order_relaxed);
    if (h < 0) return h;
    out->handle = h;
    out->cls = cls;
    return 0;
}

int32_t IoCtx::probe(const char* path, size_t len) {
    return io_->open(path, int32_t(len), FLATSQL_IO_PROBE);
}

int32_t IoCtx::unlink(const char* path, size_t len, bool durableIfUnused) {
    return io_->open(path, int32_t(len),
                     FLATSQL_IO_UNLINK | (durableIfUnused ? FLATSQL_IO_UNLINK_IF_UNUSED : 0));
}

int64_t IoCtx::read(const FileRef& f, void* dst, size_t len, uint64_t off) {
    size_t done = 0;
    while (done < len) {
        const size_t chunk = (len - done) > kMaxCall ? kMaxCall : (len - done);
        const int32_t n = io_->read(f.handle, static_cast<uint8_t*>(dst) + done, int32_t(chunk),
                                    double(off + done));
        if (n < 0) return n;
        auto& c = ctr(stats_, f.cls);
        c.readCalls.fetch_add(1, std::memory_order_relaxed);
        c.readBytes.fetch_add(uint64_t(n), std::memory_order_relaxed);
        done += size_t(n);
        if (size_t(n) < chunk) break;
    }
    return int64_t(done);
}

int32_t IoCtx::write(const FileRef& f, const void* src, size_t len, uint64_t off) {
    size_t done = 0;
    while (done < len) {
        const size_t chunk = (len - done) > kMaxCall ? kMaxCall : (len - done);
        const int32_t n = io_->write(f.handle, static_cast<const uint8_t*>(src) + done,
                                     int32_t(chunk), double(off + done));
        if (n < 0) return n;
        if (n == 0) return FLATSQL_IO_ERR_IO;
        auto& c = ctr(stats_, f.cls);
        c.writeCalls.fetch_add(1, std::memory_order_relaxed);
        c.writeBytes.fetch_add(uint64_t(n), std::memory_order_relaxed);
        done += size_t(n);
    }
    return 0;
}

int32_t IoCtx::writeZeros(const FileRef& f, uint64_t off, uint64_t len) {
    while (len) {
        const size_t chunk = len > sizeof(kZeros) ? sizeof(kZeros) : size_t(len);
        const int32_t rc = write(f, kZeros, chunk, off);
        if (rc < 0) return rc;
        off += chunk;
        len -= chunk;
    }
    return 0;
}

int32_t IoCtx::truncate(const FileRef& f, uint64_t size) {
    ctr(stats_, f.cls).truncates.fetch_add(1, std::memory_order_relaxed);
    return io_->truncate(f.handle, double(size));
}

int32_t IoCtx::sync(const FileRef& f) {
    ctr(stats_, f.cls).syncs.fetch_add(1, std::memory_order_relaxed);
    return io_->sync(f.handle);
}

int64_t IoCtx::size(const FileRef& f) {
    const double s = io_->size(f.handle);
    return int64_t(s);
}

int32_t IoCtx::close(FileRef* f) {
    if (!f->valid()) return 0;
    const int32_t rc = io_->close(f->handle);
    f->handle = -1;
    return rc;
}

}  // namespace ps
}  // namespace flatsql
