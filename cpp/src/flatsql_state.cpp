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

#include <algorithm>
#include <cstring>
#include <map>
#include <mutex>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace flatsql {

namespace {

constexpr int kFormatVersion = 1;

// The source-partition layout, versioned SEPARATELY from kFormatVersion on
// purpose. Bumping kFormatVersion invalidates every persisted index row on
// every deployed node; the partition tables are additive and cost nothing to
// miss, so their absence must read as "no partitions", never as "-2, throw the
// index away". Old state stays valid; new state stays readable by old builds.
constexpr int kSourceIndexVersion = 1;

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

std::string tableFingerprint(const DatabaseSchema& schema, const TableDef& table) {
    // Keep the legacy aggregate fingerprint readable, but record the complete
    // extraction/index contract for new checkpoints, including field IDs and
    // defaults. Length-prefix strings to avoid delimiter ambiguities.
    std::ostringstream out;
    out << std::setprecision(17);
    auto text = [&out](const std::string& value) { out << value.size() << ':' << value; };
    text(schema.name);
    text(table.name);
    for (const auto& col : table.columns) {
        text(col.name);
        out << ':' << int(col.type) << ':' << col.nullable << col.indexed
            << col.primaryKey << col.encrypted << col.spatial << ':' << col.fieldId << ':';
        text(col.spatialPair);
        out << ':' << col.defaultValue.has_value() << ':';
        if (col.defaultValue) {
            out << col.defaultValue->index() << ':';
            std::visit([&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, std::string>) text(value);
                else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    out << value.size() << ':';
                    for (auto byte : value) out << unsigned(byte) << ',';
                } else if constexpr (!std::is_same_v<T, std::monostate>) out << +value;
            }, *col.defaultValue);
        }
        out << ';';
    }
    for (const auto& key : table.primaryKeyColumns) text(key);
    return out.str();
}

}  // namespace

#if !defined(FLATSQL_ENABLE_IO_VFS)

// Targets built without the I/O VFS (space-data-module-sdk MODULES) have no
// filesystem of any kind and must never pretend otherwise. Returning -5 keeps
// the host contract identical: a negative code, recoverable, never a trap.
int FlatSQLDatabase::openState() { return kStateNoFilesystem; }
int FlatSQLDatabase::reindexAll() { return kStateNoFilesystem; }
int FlatSQLDatabase::reindexStep(size_t) { return kStateNoFilesystem; }
int FlatSQLDatabase::flushState() { return kStateNoFilesystem; }
int FlatSQLDatabase::flushStateUnlocked() { return kStateNoFilesystem; }
int FlatSQLDatabase::loadStreamFromDisk(uint64_t, const std::unordered_set<std::string>&) { return kStateNoFilesystem; }
int FlatSQLDatabase::readWholeStream(std::vector<uint8_t>*) { return kStateNoFilesystem; }
void FlatSQLDatabase::clearDerivedState() {}
int FlatSQLDatabase::restoreSourceIndex() { return kStateNoFilesystem; }
int FlatSQLDatabase::persistSourceIndex(uint64_t) { return kStateNoFilesystem; }
void FlatSQLDatabase::rebindSourceViews() {}

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

// The source partition, persisted next to the mark (docs/STORAGE-DURABILITY.md
// §3.4). PURELY ADDITIVE: neither table changes the stream, the index rows or
// kFormatVersion, so a database written by 1.4.5 opens under 1.4.4 (which
// ignores tables it does not read) and a database written by 1.4.4 opens here
// (both SELECTs return zero rows, which IS "no partitions" — the pre-1.4.5
// behaviour, reached without invalidating anything).
constexpr const char* kSourcesDDL =
    "CREATE TABLE IF NOT EXISTS _flatsql_sources("
    "name TEXT PRIMARY KEY, ord TEXT NOT NULL) WITHOUT ROWID";

// Offsets are decimal TEXT for the same reason flushed_offset is: one canonical
// spelling per offset, no integer-affinity surprises across engines.
constexpr const char* kSourceRangesDDL =
    "CREATE TABLE IF NOT EXISTS _flatsql_source_ranges("
    "\"start\" TEXT PRIMARY KEY, \"stop\" TEXT NOT NULL, source TEXT NOT NULL)"
    " WITHOUT ROWID";

// Cells arrive as int64 or TEXT depending on how SQLite typed the column.
// Accept both; anything else is a corrupt row, reported as 0 by the caller.
uint64_t cellToU64(const Value& v, bool* ok) {
    if (const auto* s = std::get_if<std::string>(&v)) {
        *ok = !s->empty();
        return *ok ? std::strtoull(s->c_str(), nullptr, 10) : 0;
    }
    if (const auto* i = std::get_if<int64_t>(&v)) {
        *ok = *i >= 0;
        return static_cast<uint64_t>(*i);
    }
    *ok = false;
    return 0;
}

std::string cellToString(const Value& v, bool* ok) {
    if (const auto* s = std::get_if<std::string>(&v)) {
        *ok = true;
        return *s;
    }
    *ok = false;
    return {};
}

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
//
// Routing is the fix for upstream-flatsql-3: a frame inside a restored source
// range belongs to that source's partition, exactly as it did at ingest. A
// replay that only ever reached the base tables is what made every
// registerSource() partition come back empty (alpha=60/beta=20 -> 0/0).
int FlatSQLDatabase::loadStreamFromDisk(uint64_t indexFromOffset,
                                      const std::unordered_set<std::string>& reindexTables) {
    std::vector<uint8_t> stream;
    const int rc = readWholeStream(&stream);
    if (rc < 0) return rc;
    if (stream.empty()) return 0;

    int replayed = 0;
    size_t rangeCursor = 0;  // frames arrive in ascending offset order
    storage_->loadAndRebuild(
        stream.data(), stream.size(),
        [this, indexFromOffset, &replayed, &rangeCursor, &reindexTables](std::string_view fileId,
                                                         const uint8_t* data, size_t len,
                                                         uint64_t seq, uint64_t offset) {
            const auto table = fileIdToTable_.find(std::string(fileId));
            const bool buildIndexes = offset >= indexFromOffset ||
                (table != fileIdToTable_.end() && reindexTables.count(table->second));
            const std::string* source = sourceForOffset(offset, &rangeCursor);
            if (source) {
                onIngestWithSource(fileId, data, len, seq, offset, *source, buildIndexes);
            } else {
                onIngest(fileId, data, len, seq, offset, buildIndexes);
            }
            replayed++;
        },
        nullptr);
    return replayed;
}

// Read the registered sources and their stream ranges back, and recreate the
// partition tables so the replay above has somewhere to route to.
//
// Absent tables are NOT an error: that is a pre-1.4.5 database, which had no
// partitions to lose. Every statement goes through executeNoThrow for the
// reason at the top of openState — a boot may not raise.
int FlatSQLDatabase::restoreSourceIndex() {
    if (!sqliteEngine_) return kStateNoFilesystem;

    QueryResult rows;
    std::string err;
    if (!sqliteEngine_->executeNoThrow(kSourcesDDL, {}, rows, &err) ||
        !sqliteEngine_->executeNoThrow(kSourceRangesDDL, {}, rows, &err)) {
        return kStateCorrupt;
    }

    // 1. Sources, in their original registration order. Order is not cosmetic:
    //    it is the UNION ALL order of every unified view, and therefore the row
    //    order of every query that reads one. Restoring it out of order would
    //    return the same rows in a different sequence — a byte-identity break.
    if (!sqliteEngine_->executeNoThrow("SELECT name, ord FROM _flatsql_sources",
                                       {}, rows, &err)) {
        return kStateCorrupt;
    }
    std::vector<std::pair<uint64_t, std::string>> ordered;
    for (const auto& row : rows.rows) {
        if (row.size() < 2) continue;
        bool okName = false, okOrd = false;
        const std::string name = cellToString(row[0], &okName);
        const uint64_t ord = cellToU64(row[1], &okOrd);
        if (!okName || !okOrd) continue;
        ordered.push_back({ord, name});
    }
    std::sort(ordered.begin(), ordered.end());
    for (const auto& [ord, name] : ordered) {
        (void)ord;
        ensureSourceRegisteredUnlocked(name);
    }

    // 2. The ranges. In-memory entries win over persisted ones (a re-flush of
    //    the same start offset is an extension of that run, never a conflict).
    if (!sqliteEngine_->executeNoThrow(
            "SELECT \"start\", \"stop\", source FROM _flatsql_source_ranges", {},
            rows, &err)) {
        return kStateCorrupt;
    }
    std::map<uint64_t, SourceRange> merged;
    for (const auto& row : rows.rows) {
        if (row.size() < 3) continue;
        bool okStart = false, okStop = false, okSource = false;
        const uint64_t start = cellToU64(row[0], &okStart);
        const uint64_t stop = cellToU64(row[1], &okStop);
        const std::string source = cellToString(row[2], &okSource);
        if (!okStart || !okStop || !okSource || stop <= start) continue;
        merged[start] = SourceRange{start, stop, source};
    }
    for (const auto& range : sourceRanges_) merged[range.start] = range;

    sourceRanges_.clear();
    for (const auto& [start, range] : merged) {
        (void)start;
        if (!sourceRanges_.empty()) {
            SourceRange& back = sourceRanges_.back();
            if (back.source == range.source && back.end == range.start) {
                back.end = range.end;
                continue;
            }
        }
        sourceRanges_.push_back(range);
    }

    return kStateOk;
}

// Write the partition delta. Called INSIDE flushState's transaction so the
// ranges and the high-water mark commit together: a range may never claim a
// stretch of stream the mark cannot back.
int FlatSQLDatabase::persistSourceIndex(uint64_t upToOffset) {
    if (!sqliteEngine_) return kStateNoFilesystem;

    QueryResult ignored;
    std::string err;
    if (!sqliteEngine_->executeNoThrow(kSourcesDDL, {}, ignored, &err) ||
        !sqliteEngine_->executeNoThrow(kSourceRangesDDL, {}, ignored, &err)) {
        return kStateCorrupt;
    }

    for (size_t i = 0; i < registeredSources_.size(); i++) {
        if (!sqliteEngine_->executeNoThrow(
                "INSERT OR REPLACE INTO _flatsql_sources(name, ord) VALUES(?,?)",
                {Value(registeredSources_[i]), Value(std::to_string(i))}, ignored,
                &err)) {
            return kStateCorrupt;
        }
    }

    // Only the ranges the last flush could not already have written. A range
    // that merely EXTENDED keeps its start offset, so INSERT OR REPLACE on the
    // start key updates it in place instead of accumulating duplicates.
    for (const auto& range : sourceRanges_) {
        if (range.end <= flushedOffset_) continue;
        const uint64_t stop = range.end < upToOffset ? range.end : upToOffset;
        if (stop <= range.start) continue;
        if (!sqliteEngine_->executeNoThrow(
                "INSERT OR REPLACE INTO _flatsql_source_ranges(\"start\",\"stop\",source)"
                " VALUES(?,?,?)",
                {Value(std::to_string(range.start)), Value(std::to_string(stop)),
                 Value(range.source)},
                ignored, &err)) {
            return kStateCorrupt;
        }
    }
    return kStateOk;
}

// A previous run left `omm@alpha` virtual tables and an `omm` unified view in
// the schema. Those objects are durable; the MODULES behind them are not, and
// a module that is not re-registered is the "no such module:
// __flatsql_module_omm_alpha" the reopened database used to answer with.
// Re-bind them here so a reopen answers the same query the same way, with no
// re-registration by the caller.
void FlatSQLDatabase::rebindSourceViews() {
    if (registeredSources_.empty()) return;

    for (const auto& tableDef : schema_.tables) {
        std::vector<std::string> sourceTableNames;
        for (const auto& source : registeredSources_) {
            const std::string sourceTableName = getSourceTableName(tableDef.name, source);
            if (!tables_.count(sourceTableName)) continue;
            if (!updateSQLiteTableNoThrow(sourceTableName)) continue;
            sourceTableNames.push_back(sourceTableName);
        }
        if (sourceTableNames.empty()) continue;
        if (sqliteEngine_->createUnifiedViewNoThrow(tableDef.name, sourceTableNames)) {
            sqliteRegisteredTables_.insert(tableDef.name);
        }
    }
    invalidateQueryResultCacheUnlocked();
}

void FlatSQLDatabase::clearDerivedState() {
    for (auto& [name, table] : tables_) {
        table->clearDerived();
    }
}

int FlatSQLDatabase::openState() {
    std::unique_lock lock(*accessMutex_);
    if (!diskBacked_ || !sqliteEngine_) return kStateNoFilesystem;
    if (reindexUnavailable_) return kStateCorrupt;

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

    // 1a. Source partitions come back BEFORE anything else, and before every
    //     early return below: a -2/-4 boot hands the caller to reindexAll(),
    //     which replays from zero and needs the same routing. Offsets are
    //     schema-independent, so a schema change never invalidates them.
    const int sourceRc = restoreSourceIndex();
    if (sourceRc < 0) return sourceRc;

    std::string version, fingerprint, flushed, tableCount;
    std::map<std::string, std::string> persistedTables;
    for (const auto& row : state.rows) {
        if (row.size() < 2) continue;
        const auto* key = std::get_if<std::string>(&row[0]);
        const auto* val = std::get_if<std::string>(&row[1]);
        if (!key || !val) continue;
        if (*key == "format_version")     version = *val;
        else if (*key == "schema")        fingerprint = *val;
        else if (*key == "flushed_offset") flushed = *val;
        else if (*key == "schema_table_count") tableCount = *val;
        else if (key->compare(0, 13, "schema_table:") == 0)
            persistedTables.emplace(key->substr(13), *val);
    }

    // 2. Format and schema must match, or the index rows mean something else.
    if (version != std::to_string(kFormatVersion)) return kStateVersionMismatch;
    std::unordered_set<std::string> addedTables;
    if (!tableCount.empty() || fingerprint != schemaFingerprint(schema_)) {
        // Older state has only an aggregate fingerprint and cannot prove an
        // additive change. New state records every table in the same commit
        // as the aggregate; a missing/changed/removed table still fails closed.
        if (tableCount.empty() || tableCount != std::to_string(persistedTables.size()))
            return kStateVersionMismatch;
        for (const auto& [name, hash] : persistedTables) {
            const auto* table = schema_.getTable(name);
            if (!table || tableFingerprint(schema_, *table) != hash) return kStateVersionMismatch;
        }
        for (const auto& table : schema_.tables) {
            if (!persistedTables.count(table.name)) addedTables.insert(table.name);
        }
    }
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
    SQLiteWriteBatch batch(*sqliteEngine_);
    if (!batch.ok()) return kStateCorrupt;
    // A newly recognised table may have records already present in the wire
    // stream. Rebuild just its indexes; unchanged tables keep their rows.
    for (const auto& name : addedTables) {
        for (auto& [tableName, table] : tables_) {
            if (tableName == name || tableName.compare(0, name.size() + 1, name + "@") == 0)
                table->clearDerived();
        }
    }
    const int replayed = loadStreamFromDisk(mark, addedTables);
    if (replayed < 0) return replayed;

    flushedOffset_ = mark;

    // 5. The partition tables now hold their records again; give them back the
    //    vtab modules and the unified views the schema already names.
    rebindSourceViews();
    if (!addedTables.empty() && flushStateUnlocked() < 0) return kStateCorrupt;
    if (!batch.commit()) return kStateCorrupt;
    reindexUnavailable_ = false;
    return replayed;
}

int FlatSQLDatabase::reindexAll() {
    int rc;
    do { rc = reindexStep(4096); } while (rc == 1);
    return rc < 0 ? rc : static_cast<int>(storage_->getRecordCount());
}

int FlatSQLDatabase::reindexStep(size_t maxRecords) {
    std::unique_lock lock(*accessMutex_);
    if (!diskBacked_ || !sqliteEngine_) return kStateNoFilesystem;
    if (maxRecords == 0) return kStateCorrupt;
    auto fail = [this](int code) {
        reindexBatch_.reset(); // rolls back only the rebuild's own savepoint
        return code;
    };
    IoHandle file(flatsql_io_open(streamPath_.c_str(),
        static_cast<int32_t>(streamPath_.size()), FLATSQL_IO_READ));
    if (!file.ok()) return fail(kStateAbsent);

    if (!reindexBatch_) {
        const double size = flatsql_io_size(file.h);
        if (size < 0) return kStateCorrupt;
        reindexBatch_ = std::make_unique<SQLiteWriteBatch>(*sqliteEngine_);
        if (!reindexBatch_->ok()) return fail(kStateCorrupt);
        reindexUnavailable_ = true;
        invalidateQueryResultCacheUnlocked();
        reindexReadOffset_ = 0;
        reindexStreamSize_ = static_cast<uint64_t>(size);
        reindexSourceCursor_ = 0;
        clearDerivedState();
        storage_->reset();
        const int sourceRc = restoreSourceIndex();
        if (sourceRc < 0) return fail(sourceRc);
    }

    auto readExact = [&file](uint8_t* dst, size_t size, uint64_t offset) {
        size_t done = 0;
        while (done < size) {
            const int32_t want = static_cast<int32_t>(std::min<size_t>(size - done, kIoChunk));
            const int32_t got = flatsql_io_read(file.h, dst + done, want, static_cast<double>(offset + done));
            if (got <= 0) return false;
            done += static_cast<size_t>(got);
        }
        return true;
    };
    // Frames are read individually. Peak temporary memory is one record,
    // instead of a second copy of the entire durable stream.
    for (size_t count = 0; count < maxRecords && reindexReadOffset_ + 4 <= reindexStreamSize_; ++count) {
        uint8_t prefix[4];
        if (!readExact(prefix, 4, reindexReadOffset_)) return fail(kStateCorrupt);
        const uint32_t length = uint32_t(prefix[0]) | (uint32_t(prefix[1]) << 8) |
            (uint32_t(prefix[2]) << 16) | (uint32_t(prefix[3]) << 24);
        const uint64_t framedLength = uint64_t(length) + 4;
        if (framedLength > reindexStreamSize_ - reindexReadOffset_) {
            // Preserve the legacy recovery contract: keep the complete
            // prefix of a stream with a torn final record, never rewrite it.
            reindexReadOffset_ = reindexStreamSize_;
            break;
        }
        std::vector<uint8_t> frame(static_cast<size_t>(framedLength));
        std::memcpy(frame.data(), prefix, 4);
        if (!readExact(frame.data() + 4, length, reindexReadOffset_ + 4)) return fail(kStateCorrupt);
        storage_->ingest(frame.data(), frame.size(),
            [this](std::string_view fileId, const uint8_t* data, size_t len, uint64_t seq, uint64_t offset) {
                const auto* source = sourceForOffset(offset, &reindexSourceCursor_);
                if (source) onIngestWithSource(fileId, data, len, seq, offset, *source);
                else onIngest(fileId, data, len, seq, offset);
            });
        reindexReadOffset_ += framedLength;
    }
    if (reindexReadOffset_ + 4 <= reindexStreamSize_) return 1;

    // These bytes came from the durable stream. Rebuild only the index and
    // its checkpoint; do not rewrite the source stream during recovery.
    flushedOffset_ = storage_->getWriteOffset();
    const int rc = flushStateUnlocked();
    if (rc < 0) return fail(rc);
    rebindSourceViews();
    if (!reindexBatch_->commit()) return fail(kStateCorrupt);
    reindexBatch_.reset();
    reindexUnavailable_ = false;
    return 0;
}

int FlatSQLDatabase::flushState() {
    std::unique_lock lock(*accessMutex_);
    if (reindexUnavailable_) return kStateCorrupt;
    return flushStateUnlocked();
}

int FlatSQLDatabase::flushStateUnlocked() {
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
    SQLiteWriteBatch batch(*sqliteEngine_);
    if (!batch.ok()) return kStateCorrupt;
    if (!sqliteEngine_->executeNoThrow(
            "CREATE TABLE IF NOT EXISTS _flatsql_state("
            "k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID",
            {}, ignored, &err)) return kStateCorrupt;
    std::string tableManifest;
    for (const auto& table : schema_.tables) {
        const auto value = tableFingerprint(schema_, table);
        tableManifest += std::to_string(value.size()) + ':' + value;
    }
    QueryResult previous;
    if (!sqliteEngine_->executeNoThrow(
            "SELECT v FROM _flatsql_state WHERE k='schema_tables'", {}, previous, &err))
        return kStateCorrupt;
    const bool tablesChanged = previous.rows.empty() ||
        previous.rows[0][0] != Value(tableManifest);
    bool ok = sqliteEngine_->executeNoThrow(
            "INSERT OR REPLACE INTO _flatsql_state(k,v) VALUES"
            "('format_version',?),('schema',?),('stream',?),('flushed_offset',?),"
            "('source_index',?)",
            {Value(std::to_string(kFormatVersion)),
             Value(schemaFingerprint(schema_)),
             Value(streamPath_),
             Value(std::to_string(writeOffset)),
             Value(std::to_string(kSourceIndexVersion))},
            ignored, &err) &&
        // Same transaction, same mark: the partition can never describe more
        // stream than the mark admits, in either direction.
        persistSourceIndex(writeOffset) == kStateOk;
    if (ok && tablesChanged) {
        ok = sqliteEngine_->executeNoThrow(
            "DELETE FROM _flatsql_state WHERE substr(k,1,13)='schema_table:'", {}, ignored, &err);
        for (const auto& table : schema_.tables) {
            if (!ok) break;
            ok = sqliteEngine_->executeNoThrow(
                "INSERT OR REPLACE INTO _flatsql_state(k,v) VALUES(?,?)",
                {Value("schema_table:" + table.name), Value(tableFingerprint(schema_, table))},
                ignored, &err);
        }
        ok = ok && sqliteEngine_->executeNoThrow(
            "INSERT OR REPLACE INTO _flatsql_state(k,v) VALUES('schema_table_count',?)",
            {Value(std::to_string(schema_.tables.size()))}, ignored, &err) &&
            sqliteEngine_->executeNoThrow(
                "INSERT OR REPLACE INTO _flatsql_state(k,v) VALUES('schema_tables',?)",
                {Value(tableManifest)}, ignored, &err);
    }
    ok = ok && batch.commit();
    if (!ok) {
        return kStateCorrupt;
    }

    flushedOffset_ = writeOffset;
    return kStateOk;
}

#endif  // FLATSQL_ENABLE_IO_VFS

}  // namespace flatsql
