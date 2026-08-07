// flatsql_state.cpp — durable FlatSQL state over the seven-import host I/O
// contract. Implements the owner's three rulings as one mechanism:
//
//   (1) FlatSQL only          — no second engine, no second process, no host
//                               storage format. Everything here is FlatSQL's.
//   (2) btree/table data on   — the index lives in the database file, which is
//       disk via the FS layer   written page by page through FlatSQL's own VFS
//                               (flatsql_vfs.cpp), never through emscripten's
//                               filesystem and never through WASI fds.
//   (3) streaming SDS         — the DATA is an append-only stream of
//       flatbuffers             size-prefixed SDS FlatBuffer records in wire
//                               form. It is appended to, never rewritten, and
//                               the index holds offsets into it. Payload bytes
//                               exist in exactly one place.
//
// Boot is therefore: read the stream, restore record positions, replay ONLY
// the tail past the recorded high-water mark, and trust the index rows already
// on disk for everything below it. A verification failure at any step is not a
// data-loss event — it costs exactly one full re-derivation, which is today's
// behaviour and the worst case by construction.
//
// docs/STORAGE-DURABILITY.md §3.

#include "flatsql/database.h"
#include "flatsql/flatsql_io.h"

#include <cstring>
#include <string>
#include <vector>

namespace flatsql {

namespace {

constexpr int kFormatVersion = 1;

// Chosen so a browser shim backed by a key->bytes store moves whole page
// groups, and a POSIX host does one pread. Both lanes read the same stream in
// the same order; only the transport differs.
constexpr int32_t kIoChunk = 1 << 20;  // 1 MiB

// Cheap, stable fingerprint of the schema the index rows were built against.
// A schema change alters extraction, so the persisted index is meaningless and
// must be rebuilt; this is what turns that into a -2 instead of wrong answers.
std::string schemaFingerprint(const DatabaseSchema& schema) {
    uint64_t h = 1469598103934665603ULL;  // FNV-1a
    auto mix = [&h](const std::string& s) {
        for (unsigned char c : s) {
            h ^= c;
            h *= 1099511628211ULL;
        }
        h ^= '|';
        h *= 1099511628211ULL;
    };
    mix(schema.name);
    for (const auto& table : schema.tables) {
        mix(table.name);
        for (const auto& col : table.columns) {
            mix(col.name);
            mix(std::to_string(static_cast<int>(col.type)));
            mix(col.indexed ? "1" : "0");
        }
    }
    char buf[24];
    std::snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(h));
    return std::string(buf);
}

}  // namespace

#if !defined(FLATSQL_ENABLE_IO_VFS)

// Targets built without the I/O VFS (space-data-module-sdk MODULES) have no
// filesystem of any kind and must never pretend otherwise. Returning -5 keeps
// the host contract identical: a negative code, recoverable, never a trap.
int FlatSQLDatabase::openState() { return kStateNoFilesystem; }
int FlatSQLDatabase::reindexAll() { return kStateNoFilesystem; }
int FlatSQLDatabase::flushState() { return kStateNoFilesystem; }
int FlatSQLDatabase::loadStreamFromDisk(uint64_t) { return kStateNoFilesystem; }
int FlatSQLDatabase::readWholeStream(std::vector<uint8_t>*) { return kStateNoFilesystem; }
void FlatSQLDatabase::clearDerivedState() {}

#else

namespace {

struct IoHandle {
    int32_t h = -1;
    explicit IoHandle(int32_t handle) : h(handle) {}
    ~IoHandle() {
        if (h >= 0) flatsql_io_close(h);
    }
    IoHandle(const IoHandle&) = delete;
    IoHandle& operator=(const IoHandle&) = delete;
    bool ok() const { return h >= 0; }
};

}  // namespace

// Read the entire stream file into `out`. The stream is the source of truth;
// nothing else is ever consulted for payload bytes.
int FlatSQLDatabase::readWholeStream(std::vector<uint8_t>* out) {
    out->clear();
    if (streamPath_.empty()) return kStateNoFilesystem;

    IoHandle file(flatsql_io_open(streamPath_.c_str(),
                                  static_cast<int32_t>(streamPath_.size()),
                                  FLATSQL_IO_READ));
    if (!file.ok()) return kStateAbsent;

    const double sizeD = flatsql_io_size(file.h);
    if (sizeD < 0) return kStateCorrupt;
    const size_t size = static_cast<size_t>(sizeD);
    if (size == 0) return kStateOk;

    out->resize(size);
    size_t done = 0;
    while (done < size) {
        const int32_t want = static_cast<int32_t>(
            (size - done) > static_cast<size_t>(kIoChunk) ? kIoChunk : (size - done));
        const int32_t got = flatsql_io_read(file.h, out->data() + done, want,
                                            static_cast<double>(done));
        if (got < 0) return kStateCorrupt;
        if (got == 0) break;  // truncated under us; the caller's checks catch it
        done += static_cast<size_t>(got);
    }
    out->resize(done);
    return kStateOk;
}

// Drive the store's rebuild once, indexing only records at or past
// `indexFromOffset`. Records below it already have their index rows on disk.
int FlatSQLDatabase::loadStreamFromDisk(uint64_t indexFromOffset) {
    std::vector<uint8_t> stream;
    const int rc = readWholeStream(&stream);
    if (rc < 0) return rc;
    if (stream.empty()) return 0;

    int replayed = 0;
    storage_->loadAndRebuild(
        stream.data(), stream.size(),
        [this, indexFromOffset, &replayed](std::string_view fileId,
                                           const uint8_t* data, size_t len,
                                           uint64_t seq, uint64_t offset) {
            onIngest(fileId, data, len, seq, offset, offset >= indexFromOffset);
            replayed++;
        },
        nullptr);
    return replayed;
}

void FlatSQLDatabase::clearDerivedState() {
    for (auto& [name, table] : tables_) {
        table->clearDerived();
    }
}

int FlatSQLDatabase::openState() {
    if (!diskBacked_ || !sqliteEngine_) return kStateNoFilesystem;

    // 1. Is there persisted state at all? Every statement below goes through
    //    executeNoThrow: on the -fignore-exceptions WASI artifact a throw is
    //    lowered to `unreachable`, which would poison the whole instance for
    //    what is merely "no state yet" (b26ed45).
    QueryResult state;
    std::string err;
    if (!sqliteEngine_->executeNoThrow(
            "CREATE TABLE IF NOT EXISTS _flatsql_state("
            "k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID",
            {}, state, &err)) {
        return kStateCorrupt;
    }
    if (!sqliteEngine_->executeNoThrow("SELECT k, v FROM _flatsql_state", {},
                                       state, &err)) {
        return kStateCorrupt;
    }
    if (state.rows.empty()) return kStateAbsent;

    std::string version, fingerprint, flushed;
    for (const auto& row : state.rows) {
        if (row.size() < 2) continue;
        const auto* key = std::get_if<std::string>(&row[0]);
        const auto* val = std::get_if<std::string>(&row[1]);
        if (!key || !val) continue;
        if (*key == "format_version")     version = *val;
        else if (*key == "schema")        fingerprint = *val;
        else if (*key == "flushed_offset") flushed = *val;
    }

    // 2. Format and schema must match, or the index rows mean something else.
    if (version != std::to_string(kFormatVersion)) return kStateVersionMismatch;
    if (fingerprint != schemaFingerprint(schema_)) return kStateVersionMismatch;
    if (flushed.empty()) return kStateAbsent;

    const uint64_t mark = std::strtoull(flushed.c_str(), nullptr, 10);

    // 3. Torn pair: the index claims a high-water mark the stream cannot back.
    //    Offsets are stable forever (streams are append-only), so a stream that
    //    is SHORTER than the mark can only mean a partial write. Recoverable.
    IoHandle probe(flatsql_io_open(streamPath_.c_str(),
                                   static_cast<int32_t>(streamPath_.size()),
                                   FLATSQL_IO_READ));
    if (!probe.ok()) return mark == 0 ? kStateAbsent : kStateTorn;
    const double streamSize = flatsql_io_size(probe.h);
    if (streamSize < 0) return kStateCorrupt;
    if (static_cast<uint64_t>(streamSize) < mark) return kStateTorn;

    // 4. Restore. Everything below the mark keeps its on-disk index rows; the
    //    tail is re-indexed. This is the entire win: a boot pays for a scan of
    //    the stream, not for rebuilding every index.
    const int replayed = loadStreamFromDisk(mark);
    if (replayed < 0) return replayed;

    flushedOffset_ = mark;
    return replayed;
}

int FlatSQLDatabase::reindexAll() {
    if (!diskBacked_ || !sqliteEngine_) return kStateNoFilesystem;

    // Derived state only. The stream is never touched by a re-derivation —
    // that is what makes every negative code above survivable.
    clearDerivedState();
    storage_->reset();

    const int replayed = loadStreamFromDisk(0);
    if (replayed < 0) return replayed;

    flushedOffset_ = 0;
    const int rc = flushState();
    if (rc < 0) return rc;
    return replayed;
}

int FlatSQLDatabase::flushState() {
    if (!diskBacked_ || !sqliteEngine_) return kStateNoFilesystem;

    const uint64_t writeOffset = storage_->getWriteOffset();
    if (writeOffset < flushedOffset_) return kStateCorrupt;  // arena went backwards

    // 1. Append the new stream bytes FIRST and fsync them. Order matters: the
    //    index may only ever claim a mark the stream can already back. The
    //    reverse order is what produces torn pairs.
    if (writeOffset > flushedOffset_) {
        IoHandle file(flatsql_io_open(
            streamPath_.c_str(), static_cast<int32_t>(streamPath_.size()),
            FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE));
        if (!file.ok()) return kStateNoFilesystem;

        const std::vector<uint8_t>& arena = storage_->getData();
        uint64_t at = flushedOffset_;
        while (at < writeOffset) {
            const uint64_t remain = writeOffset - at;
            const int32_t want = static_cast<int32_t>(
                remain > static_cast<uint64_t>(kIoChunk) ? kIoChunk : remain);
            const int32_t wrote = flatsql_io_write(file.h, arena.data() + at, want,
                                                   static_cast<double>(at));
            if (wrote <= 0) return kStateCorrupt;
            at += static_cast<uint64_t>(wrote);
        }
        if (flatsql_io_sync(file.h) < 0) return kStateCorrupt;
    }

    // 2. Now record the mark, in the same transaction as the index pages the
    //    pager is about to commit through the VFS.
    QueryResult ignored;
    std::string err;
    const bool ok =
        sqliteEngine_->executeNoThrow(
            "CREATE TABLE IF NOT EXISTS _flatsql_state("
            "k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID",
            {}, ignored, &err) &&
        sqliteEngine_->executeNoThrow("BEGIN IMMEDIATE", {}, ignored, &err) &&
        sqliteEngine_->executeNoThrow(
            "INSERT OR REPLACE INTO _flatsql_state(k,v) VALUES"
            "('format_version',?),('schema',?),('stream',?),('flushed_offset',?)",
            {Value(std::to_string(kFormatVersion)),
             Value(schemaFingerprint(schema_)),
             Value(streamPath_),
             Value(std::to_string(writeOffset))},
            ignored, &err) &&
        sqliteEngine_->executeNoThrow("COMMIT", {}, ignored, &err);
    if (!ok) {
        sqliteEngine_->executeNoThrow("ROLLBACK", {}, ignored, &err);
        return kStateCorrupt;
    }

    flushedOffset_ = writeOffset;
    return kStateOk;
}

#endif  // FLATSQL_ENABLE_IO_VFS

}  // namespace flatsql
