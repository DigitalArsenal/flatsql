// Store format 4: a feed's record stream (BRIEF4, owner 2026-10-03: "the
// flatbuffers written to disk should be readable by a flatbuffer reader
// directly").
//
// P/<TYPE>/<name>.fsdata holds the feed's records exactly as they arrived:
// [u32 LE size][FlatBuffer] frames back to back, with no file header, magic,
// per-frame tag, CRC, padding or trailer (format 1's .fsdata, flatsql commit
// 645f86c). A sealed record's frame holds its sealed bytes. Everything else
// (CID, seq, provenance, the frame's place) is in the feed's index file, whose
// rows point at frames by (off, len).
//
// The writer appends at its end offset and syncs the stream before the index
// transaction that commits the rows and the stream's mark (its indexed end):
// the index never claims bytes the stream cannot back, and open cuts the
// stream back to the mark (bytes past it were never acknowledged).
//
// A compaction writes the live frames into the next generation
// (<name>.<gen>.fsdata, a pure stream again) and repoints the rows in one
// transaction; the replaced generation stays open for readers whose snapshot
// still names it, and is unlinked when the last of them lets it go.
#include "flatsql/flatsql_io.h"
#include "internal.h"

namespace flatsql {
namespace p4 {

Stream::~Stream() {
    if (h >= 0) flatsql_io_close(h);
    if (drop.load(std::memory_order_acquire)) ioUnlink(path);
}

std::string streamPath(const Feed* f, uint32_t gen) {
    const std::string base = pathJoin(f->type->pDir, f->name);
    return gen == 0 ? base + ".fsdata" : base + "." + std::to_string(gen) + ".fsdata";
}

namespace {
int32_t openStream(const std::string& path, uint32_t gen, bool create, std::shared_ptr<Stream>* out) {
    int32_t flags = FLATSQL_IO_READ | FLATSQL_IO_WRITE;
    if (create) flags |= FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS;
    const int32_t h = flatsql_io_open(path.data(), int32_t(path.size()), flags);
    if (h < 0) return h == FLATSQL_IO_ERR_NOSPACE ? P4_E_NOSPACE : P4_E_IO;
    auto s = std::make_shared<Stream>();
    s->path = path;
    s->gen = gen;
    s->h = h;
    *out = std::move(s);
    return P4_OK;
}
}  // namespace

std::shared_ptr<Stream> streamCur(Feed* f, int32_t* rc) {
    *rc = P4_OK;
    std::lock_guard<std::mutex> g(f->smu);
    if (f->stream && f->stream->gen == f->gen) return f->stream;
    std::shared_ptr<Stream> s;
    *rc = openStream(streamPath(f, f->gen), f->gen, true, &s);
    if (*rc != P4_OK) return nullptr;
    f->stream = s;
    return s;
}

std::shared_ptr<Stream> streamAt(Feed* f, uint32_t gen, int32_t* rc) {
    *rc = P4_OK;
    std::lock_guard<std::mutex> g(f->smu);
    if (f->stream && f->stream->gen == gen) return f->stream;
    if (f->retired && f->retired->gen == gen) return f->retired;
    if (f->stream || gen != f->gen) return nullptr;
    // Not opened since the engine opened: the snapshot's rows name it, so it exists.
    std::shared_ptr<Stream> s;
    *rc = openStream(streamPath(f, gen), gen, false, &s);
    if (*rc != P4_OK) return nullptr;
    f->stream = s;
    return s;
}

int32_t streamRead(Stream* s, int64_t off, int64_t len, std::string* out, bool frame) {
    if (off < 0 || len < 0 || len > (int64_t(1) << 31) - 8) return P4_E_CORRUPT;
    out->resize(size_t(len) + 4);
    size_t got = 0;
    while (got < out->size()) {
        const int32_t n = flatsql_io_read(s->h, &(*out)[got], int32_t(out->size() - got), double(off) + double(got));
        if (n < 0) return P4_E_IO;
        if (n == 0) return P4_E_CORRUPT;  // the frame runs past the stream's end
        got += size_t(n);
    }
    if (ld32(reinterpret_cast<const uint8_t*>(out->data())) != uint32_t(len)) return P4_E_CORRUPT;
    if (!frame) out->erase(0, 4);
    return P4_OK;
}

int32_t streamWrite(Stream* s, int64_t off, const uint8_t* p, size_t n) {
    size_t done = 0;
    while (done < n) {
        const int32_t w = flatsql_io_write(s->h, p + done, int32_t(std::min<size_t>(n - done, size_t(1) << 30)),
                                           double(off) + double(done));
        if (w <= 0) return w == FLATSQL_IO_ERR_NOSPACE ? P4_E_NOSPACE : P4_E_IO;
        done += size_t(w);
    }
    return P4_OK;
}

int32_t streamSync(Stream* s) { return flatsql_io_sync(s->h) == 0 ? P4_OK : P4_E_IO; }

int32_t streamTruncate(Stream* s, int64_t size, bool sync) {
    if (flatsql_io_truncate(s->h, double(size)) < 0) return P4_E_IO;
    return sync ? streamSync(s) : P4_OK;
}

int64_t streamSize(Stream* s) {
    const double n = flatsql_io_size(s->h);
    return n < 0 ? -1 : int64_t(n);
}

int32_t snapGen(Conn* c, uint32_t* gen) {
    *gen = 0;
    sqlite3_stmt* q = c->get(S_META_GEN);
    if (!q) return P4_E_INTERNAL;
    const int r = sqlite3_step(q);
    if (r == SQLITE_ROW) *gen = uint32_t(sqlite3_column_int64(q, 0));
    sqlite3_reset(q);
    return r == SQLITE_ROW || r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}

void streamClose(Feed* f) {
    std::shared_ptr<Stream> a, b;
    {
        std::lock_guard<std::mutex> g(f->smu);
        a.swap(f->stream);
        b.swap(f->retired);
    }
}

}  // namespace p4
}  // namespace flatsql
