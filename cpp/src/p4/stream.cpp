// Store format 5: the authoritative feed stream (FORMAT.md sections 2 and 9).
// P/<TYPE>/<name>.fsdata contains back-to-back [u32 LE size][FlatBuffer]
// frames: verbatim records, genesis checkpoints, and sibling metadata.
// The writer syncs the stream before committing its disposable SQLite cache.
// Recovery verifies and applies complete commits after the cache's mark and
// truncates only the remaining uncommitted or damaged tail.
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

// Unexposed feeds are released implicitly. The IPFS integration supplies
// this decision when a node must acknowledge registrations in the new file.
__attribute__((weak)) bool streamGenerationReleased(const Feed*, uint32_t) { return true; }

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
    if (f->mark == 0 && streamSize(s.get()) == 0) {
        int64_t end = 0;
        *rc = streamGenesis(f, s.get(), &end);
        if (*rc == P4_OK) *rc = streamSync(s.get());
        if (*rc != P4_OK) {
            // A failed genesis was never acknowledged. Leave an empty stream
            // so a later attempt cannot append records after a torn checkpoint.
            streamTruncate(s.get(), 0, false);
            return nullptr;
        }
        f->end = end;
        f->lastCommit = 0;
        f->metadataBytes = end;
        f->mark = end;
    }
    f->stream = s;
    return s;
}

std::shared_ptr<Stream> streamAt(Feed* f, uint32_t gen, int32_t* rc) {
    *rc = P4_OK;
    std::lock_guard<std::mutex> g(f->smu);
    if (f->stream && f->stream->gen == gen) return f->stream;
    if (f->retired && f->retired->gen == gen) return f->retired;
    for (auto& retired : f->olderRetired) {
        if (retired.stream->gen != gen) continue;
        // Startup retirement tracks paths without spending an I/O handle on
        // each one. Open lazily if an existing snapshot still names one.
        if (retired.stream->h < 0) {
            std::shared_ptr<Stream> opened;
            *rc = openStream(retired.stream->path, gen, false, &opened);
            if (*rc != P4_OK) return nullptr;
            retired.stream = std::move(opened);
        }
        return retired.stream;
    }
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

int32_t streamReadRange(Stream* s, int64_t off, size_t n, std::string* out) {
    if (off < 0) return P4_E_CORRUPT;
    out->resize(n);
    size_t got = 0;
    while (got < n) {
        const int32_t r = flatsql_io_read(s->h, &(*out)[got], int32_t(std::min<size_t>(n - got, size_t(1) << 30)),
                                          double(off) + double(got));
        if (r < 0) return P4_E_IO;
        if (r == 0) return P4_E_CORRUPT;  // past the stream's end
        got += size_t(r);
    }
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
    std::vector<RetiredGeneration> older;
    {
        std::lock_guard<std::mutex> g(f->smu);
        a.swap(f->stream);
        b.swap(f->retired);
        older.swap(f->olderRetired);
    }
}

}  // namespace p4
}  // namespace flatsql
