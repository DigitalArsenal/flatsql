// arena_compaction_test.cpp — FlatSQLDatabase::compactArena
// (docs/STORAGE-DURABILITY.md §6.4.2).
//
// Computable outcomes only:
//   1. equivalence: every query answers identically before and after a
//      compaction, sequences included (in-memory and disk-backed);
//   2. between steps, reads answer from the model while ingests, tombstones
//      and flushes land; after a reopen the persisted arena is exactly the
//      rows the compaction kept plus what was written since;
//   3. crash injection at EVERY mutating host-I/O call of a compaction, in two
//      modes (kill -9: the page cache survives; power loss: only fsynced bytes
//      survive): the reopened state is the old one or the new one, never a
//      mixture, and sequences are never handed out twice;
//   4. a churn soak (ingest plus tombstone, per-source windows): the arena
//      stays under its budget, compactions keep it there, and the final
//      answers equal the model. FLATSQL_SOAK_MIB sets the cumulative ingest
//      (default 96 MiB; the host-02-shape run uses 2600).
//
// The host I/O here is an in-memory filesystem that keeps, per file, what
// reads see (the page cache) and what survives a power loss (the last fsynced
// image). Directory entries are durable at once (a simplification: the
// engine asks for durable entries with CREATE_PARENTS / UNLINK_IF_UNUSED).
// This executable links the engine sources without flatsql_io_native.cpp and
// supplies the seven flatsql_io_* functions itself.

#include "flatsql/database.h"
#include "flatsql/flatsql_io.h"
#include "flatbuffers/flatbuffers.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

using namespace flatsql;

// ==================== In-memory host I/O ====================

namespace memfs {

struct File {
    std::vector<uint8_t> cur;      // what reads see
    std::vector<uint8_t> durable;  // what a power loss leaves
};

struct Handle {
    std::shared_ptr<File> file;
    std::string path;
    bool deleteOnClose = false;
    bool open = false;
};

struct Fs {
    std::map<std::string, std::shared_ptr<File>> ns;
    std::vector<Handle> handles;
    bool tracking = true;  // false: no durable images (the soak)
    uint64_t ops = 0;
    uint64_t crashAt = 0;
    bool frozen = false;
};

Fs& fs() {
    static Fs instance;
    return instance;
}

// Counts a mutating call; false once the "process" is dying.
bool mutate() {
    Fs& f = fs();
    f.ops++;
    if (f.crashAt && f.ops >= f.crashAt) f.frozen = true;
    return !f.frozen;
}

void reset(bool tracking = true) {
    Fs& f = fs();
    f.ns.clear();
    f.handles.clear();
    f.tracking = tracking;
    f.ops = 0;
    f.crashAt = 0;
    f.frozen = false;
}

enum class Crash { KeepAll, DropAll };

void crash(Crash mode) {
    Fs& f = fs();
    if (mode == Crash::DropAll) {
        for (auto& [path, file] : f.ns) {
            (void)path;
            file->cur = file->durable;
        }
    }
    for (auto& h : f.handles) h.open = false;
    f.crashAt = 0;
    f.frozen = false;
}

Handle* handle(int32_t h) {
    Fs& f = fs();
    if (h < 0 || size_t(h) >= f.handles.size() || !f.handles[h].open) return nullptr;
    return &f.handles[h];
}

bool exists(const std::string& path) { return fs().ns.count(path) > 0; }
uint64_t size(const std::string& path) {
    auto it = fs().ns.find(path);
    return it == fs().ns.end() ? 0 : it->second->cur.size();
}

}  // namespace memfs

extern "C" {

int32_t flatsql_io_open(const char* path, int32_t pathLen, int32_t flags) {
    if (!path || pathLen <= 0) return FLATSQL_IO_ERR_GENERIC;
    memfs::Fs& f = memfs::fs();
    const std::string name(path, size_t(pathLen));
    auto it = f.ns.find(name);
    if (flags & FLATSQL_IO_PROBE) return it == f.ns.end() ? FLATSQL_IO_ERR_NOENT : 0;
    if (flags & FLATSQL_IO_UNLINK) {
        if (it == f.ns.end()) return FLATSQL_IO_ERR_NOENT;
        if (!memfs::mutate()) return FLATSQL_IO_ERR_IO;
        f.ns.erase(it);
        return 0;
    }
    if (it == f.ns.end()) {
        if (!(flags & FLATSQL_IO_CREATE)) return FLATSQL_IO_ERR_NOENT;
        if (!memfs::mutate()) return FLATSQL_IO_ERR_IO;
        it = f.ns.emplace(name, std::make_shared<memfs::File>()).first;
    } else if ((flags & FLATSQL_IO_CREATE) && (flags & FLATSQL_IO_EXCL)) {
        return FLATSQL_IO_ERR_GENERIC;
    }
    if (flags & FLATSQL_IO_TRUNC) {
        if (!memfs::mutate()) return FLATSQL_IO_ERR_IO;
        it->second->cur.clear();
    }
    memfs::Handle h;
    h.file = it->second;
    h.path = name;
    h.deleteOnClose = (flags & FLATSQL_IO_DELETE_ON_CLOSE) != 0;
    h.open = true;
    for (size_t i = 0; i < f.handles.size(); i++) {
        if (!f.handles[i].open) {
            f.handles[i] = h;
            return int32_t(i);
        }
    }
    f.handles.push_back(h);
    return int32_t(f.handles.size() - 1);
}

int32_t flatsql_io_read(int32_t handle, void* dst, int32_t len, double offset) {
    memfs::Handle* h = memfs::handle(handle);
    if (!h) return FLATSQL_IO_ERR_BADHANDLE;
    const auto& cur = h->file->cur;
    const uint64_t off = uint64_t(offset);
    if (off >= cur.size() || len <= 0) return 0;
    const size_t n = std::min<size_t>(size_t(len), cur.size() - size_t(off));
    std::memcpy(dst, cur.data() + off, n);
    return int32_t(n);
}

int32_t flatsql_io_write(int32_t handle, const void* src, int32_t len, double offset) {
    memfs::Handle* h = memfs::handle(handle);
    if (!h) return FLATSQL_IO_ERR_BADHANDLE;
    if (!memfs::mutate()) return FLATSQL_IO_ERR_IO;
    auto& cur = h->file->cur;
    const size_t off = size_t(offset);
    if (off + size_t(len) > cur.size()) cur.resize(off + size_t(len), 0);
    std::memcpy(cur.data() + off, src, size_t(len));
    return len;
}

int32_t flatsql_io_truncate(int32_t handle, double size) {
    memfs::Handle* h = memfs::handle(handle);
    if (!h) return FLATSQL_IO_ERR_BADHANDLE;
    if (!memfs::mutate()) return FLATSQL_IO_ERR_IO;
    h->file->cur.resize(size_t(size), 0);
    return 0;
}

int32_t flatsql_io_sync(int32_t handle) {
    memfs::Handle* h = memfs::handle(handle);
    if (!h) return FLATSQL_IO_ERR_BADHANDLE;
    if (!memfs::mutate()) return FLATSQL_IO_ERR_IO;
    if (memfs::fs().tracking) h->file->durable = h->file->cur;
    return 0;
}

double flatsql_io_size(int32_t handle) {
    memfs::Handle* h = memfs::handle(handle);
    if (!h) return FLATSQL_IO_ERR_BADHANDLE;
    return double(h->file->cur.size());
}

int32_t flatsql_io_close(int32_t handle) {
    memfs::Handle* h = memfs::handle(handle);
    if (!h) return FLATSQL_IO_ERR_BADHANDLE;
    h->open = false;
    if (h->deleteOnClose) memfs::fs().ns.erase(h->path);
    h->file.reset();
    return 0;
}

}  // extern "C"

// ==================== Records and the database ====================

namespace {

int g_failures = 0;

#define CHECK(cond, what)                                                          \
    do {                                                                           \
        if (!(cond)) {                                                             \
            std::cerr << "  FAIL " << __LINE__ << ": " << (what) << "  [" #cond "]" \
                      << std::endl;                                                \
            g_failures++;                                                          \
        }                                                                          \
    } while (0)

// Two tables: OBS has an indexed key (index rows carry arena offsets) and a
// lat/lon pair (an R-Tree keyed by sequence); CAT has neither.
const char* kSchema = R"(
    table Obs {
        NORAD: int (key);
        NAME: string;
        LAT: double;
        LON: double;
        EPOCH: long;
    }
    table Cat {
        ID: string;
        SIZE: long;
    }
)";

// SDN's shape: no index tables (the only kind a compaction inside an open
// transaction accepts).
const char* kSchemaNoIndex = R"(
    table Obs {
        NORAD: int;
        NAME: string;
        LAT: double;
        LON: double;
        EPOCH: long;
    }
    table Cat {
        ID: string;
        SIZE: long;
    }
)";

const std::vector<std::string> kSources = {"alpha", "beta", "gamma"};

std::vector<uint8_t> obs(int32_t norad, const std::string& name, double lat, double lon, int64_t epoch) {
    flatbuffers::FlatBufferBuilder b(256 + name.size());
    const auto n = b.CreateString(name);
    const auto start = b.StartTable();
    b.AddElement<int32_t>(4, norad, 0);
    b.AddOffset(6, n);
    b.AddElement<double>(8, lat, 0);
    b.AddElement<double>(10, lon, 0);
    b.AddElement<int64_t>(12, epoch, 0);
    b.Finish(flatbuffers::Offset<flatbuffers::Table>(b.EndTable(start)), "OBS ");
    return {b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize()};
}

std::vector<uint8_t> cat(const std::string& id, int64_t size) {
    flatbuffers::FlatBufferBuilder b(128 + id.size());
    const auto i = b.CreateString(id);
    const auto start = b.StartTable();
    b.AddOffset(4, i);
    b.AddElement<int64_t>(6, size, 0);
    b.Finish(flatbuffers::Offset<flatbuffers::Table>(b.EndTable(start)), "CAT ");
    return {b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize()};
}

struct Row {
    std::string partition;  // "Obs@alpha"
    std::vector<uint8_t> bytes;
};

// A deterministic record generator.
struct Gen {
    std::mt19937_64 rng;
    explicit Gen(uint64_t seed) : rng(seed) {}
    uint64_t next(uint64_t n) { return rng() % n; }
    Row make(int serial, size_t nameBytes = 0) {
        const std::string& source = kSources[next(kSources.size())];
        if (next(4) == 0) {
            const std::string id = "CAT-" + std::to_string(serial) + std::string(next(64), 'c');
            return Row{"Cat@" + source, cat(id, int64_t(serial) * 7)};
        }
        const size_t len = nameBytes ? nameBytes : 8 + next(400);
        std::string name = "SAT-" + std::to_string(serial) + "-";
        name.resize(len, char('a' + serial % 26));
        return Row{"Obs@" + source,
                   obs(int32_t(serial % 97), name, double(serial % 180) - 90.0,
                       double(serial % 360) - 180.0, int64_t(serial) * 1000)};
    }
};

std::string sourceOf(const std::string& partition) {
    return partition.substr(partition.find('@') + 1);
}

std::unique_ptr<FlatSQLDatabase> openDb(const std::string& path, int journalMode = 1,
                                        const char* schema = kSchema) {
    FlatSQLDatabase::RuntimeOptions options;
    if (!path.empty()) {
        options.sqlite.path = path;
        options.sqlite.vfs = kFlatSqlVfsName;  // SQLite's pages go through memfs too
        options.sqlite.journalMode = journalMode;
        options.sqlite.enableWal = journalMode == 1;
    }
    auto db = std::make_unique<FlatSQLDatabase>(SchemaParser::parse(schema, "arena"), std::move(options));
    db->registerFileId("OBS ", "Obs");
    db->registerFileId("CAT ", "Cat");
    // What SQLITE_TEMP_STORE=3 gives every wasm build: the flatsql_io VFS has
    // no anonymous temp files.
    QueryResult ignored;
    db->queryNoThrow("PRAGMA temp_store=MEMORY", {}, ignored, nullptr);
    // SDN's setting: WAL at synchronous=NORMAL, where an ordinary commit may
    // roll back on power loss. The compaction's own commits must not.
    if (!path.empty() && journalMode == 1) db->queryNoThrow("PRAGMA synchronous=NORMAL", {}, ignored, nullptr);
    return db;
}

// A fresh database's sources and views, the way SDN sets them up.
void setUpSources(FlatSQLDatabase& db) {
    for (const auto& source : kSources) db.registerSource(source);
    db.createUnifiedViews();
}

uint64_t ingest(FlatSQLDatabase& db, const Row& row) {
    return db.ingestOneWithSource(row.bytes.data(), row.bytes.size(), sourceOf(row.partition));
}

std::string cellText(const Value& v) {
    return std::visit([](const auto& x) -> std::string {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::monostate>) return "NULL";
        else if constexpr (std::is_same_v<T, std::string>) return "'" + x + "'";
        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            static const char* hex = "0123456789abcdef";
            std::string out = "x";
            for (uint8_t c : x) { out += hex[c >> 4]; out += hex[c & 15]; }
            return out;
        } else if constexpr (std::is_same_v<T, bool>) return x ? "true" : "false";
        else {
            std::ostringstream s;
            s.precision(17);
            s << +x;
            return s.str();
        }
    }, v);
}

std::string run(FlatSQLDatabase& db, const std::string& sql, const std::vector<Value>& params = {}) {
    QueryResult result;
    std::string err;
    if (!db.queryNoThrow(sql, params, result, &err)) return "ERROR " + err;
    std::string out;
    for (const auto& row : result.rows) {
        for (const auto& cell : row) out += cellText(cell) + "|";
        out += "\n";
    }
    return out;
}

std::vector<std::string> partitions() {
    std::vector<std::string> out;
    for (const char* table : {"Obs", "Cat"}) {
        for (const auto& source : kSources) out.push_back(std::string(table) + "@" + source);
    }
    return out;
}

// The query battery: every plan shape the vtab has (full scan, index
// equality, index range, rowid lookup, the unified views, aggregates),
// never the _offset column (the one thing a compaction is meant to change).
std::vector<std::string> battery(FlatSQLDatabase& db, const std::vector<uint64_t>& probeSeqs) {
    std::vector<std::string> out;
    for (const auto& p : partitions()) {
        const std::string q = "\"" + p + "\"";
        const bool isObs = p.rfind("Obs", 0) == 0;
        out.push_back(run(db, "SELECT _rowid, _source, _data FROM " + q + " ORDER BY _rowid"));
        out.push_back(run(db, "SELECT COUNT(*) FROM " + q));
        if (isObs) {
            out.push_back(run(db, "SELECT _rowid, NORAD, NAME, LAT, LON, EPOCH FROM " + q + " ORDER BY _rowid"));
            for (int key : {0, 5, 13, 42, 96}) {
                out.push_back(run(db, "SELECT _rowid, NAME FROM " + q + " WHERE NORAD = ?", {Value(int64_t(key))}));
            }
            out.push_back(run(db, "SELECT _rowid, NORAD FROM " + q + " WHERE NORAD BETWEEN ? AND ? ORDER BY NORAD, _rowid",
                              {Value(int64_t(10)), Value(int64_t(30))}));
            out.push_back(run(db, "SELECT _rowid FROM " + q + " WHERE NORAD > ? ORDER BY NORAD, _rowid LIMIT 7",
                              {Value(int64_t(50))}));
        } else {
            out.push_back(run(db, "SELECT _rowid, ID, SIZE FROM " + q + " ORDER BY ID"));
        }
    }
    out.push_back(run(db, "SELECT _rowid, _source, NAME FROM Obs ORDER BY _rowid"));
    out.push_back(run(db, "SELECT _source, COUNT(*), SUM(EPOCH), MIN(NAME), MAX(LAT) FROM Obs GROUP BY _source ORDER BY _source"));
    out.push_back(run(db, "SELECT COUNT(*), SUM(SIZE) FROM Cat"));
    out.push_back(run(db, "SELECT _rowid, ID FROM Cat ORDER BY SIZE DESC LIMIT 11"));
    // Rowid lookups inside the partition that owns the row (live or dead).
    for (const uint64_t seq : probeSeqs) {
        for (const auto& p : partitions()) {
            out.push_back(run(db, "SELECT _rowid, _data FROM \"" + p + "\" WHERE _rowid = ? AND _rowid IN "
                                  "(SELECT _rowid FROM \"" + p + "\")", {Value(int64_t(seq))}));
        }
    }
    return out;
}

// What a partition holds: sequence -> bytes.
using Contents = std::map<std::string, std::map<uint64_t, std::vector<uint8_t>>>;

Contents contents(FlatSQLDatabase& db) {
    Contents out;
    for (const auto& p : partitions()) {
        QueryResult result;
        std::string err;
        if (!db.queryNoThrow("SELECT _rowid, _data FROM \"" + p + "\"", {}, result, &err)) {
            out[p][0] = std::vector<uint8_t>(err.begin(), err.end());
            continue;
        }
        auto& part = out[p];
        for (const auto& row : result.rows) {
            const uint64_t seq = uint64_t(std::get<int64_t>(row[0]));
            part[seq] = std::get<std::vector<uint8_t>>(row[1]);
        }
    }
    return out;
}

uint64_t maxSequence(const Contents& c) {
    uint64_t m = 0;
    for (const auto& [p, rows] : c) {
        (void)p;
        if (!rows.empty()) m = std::max(m, rows.rbegin()->first);
    }
    return m;
}

size_t rowCount(const Contents& c) {
    size_t n = 0;
    for (const auto& [p, rows] : c) {
        (void)p;
        n += rows.size();
    }
    return n;
}

int compactAll(FlatSQLDatabase& db, uint64_t step, int* steps = nullptr) {
    int rc;
    int n = 0;
    do {
        rc = db.compactArena(step);
        n++;
    } while (rc == FlatSQLDatabase::kCompactPending && n < 1000000);
    if (steps) *steps = n;
    return rc;
}

// Everything the arena holds, tombstoned or not: what a reopen restores
// (tombstones are not persisted).
Contents arenaContents(FlatSQLDatabase& db) {
    for (const auto& p : partitions()) db.clearTombstones(p);
    return contents(db);
}

// ==================== 1. Equivalence ====================

void testEquivalence(bool diskBacked, uint64_t step) {
    const std::string label = std::string(diskBacked ? "disk" : "memory") + " step " + std::to_string(step);
    memfs::reset();
    auto db = openDb(diskBacked ? "/mem/eq.db" : "");
    setUpSources(*db);
    Gen gen(7 + step);
    std::vector<std::pair<std::string, uint64_t>> rows;
    for (int i = 1; i <= 1500; i++) {
        const Row row = gen.make(i);
        const uint64_t seq = ingest(*db, row);
        CHECK(seq == uint64_t(i), label + ": sequences count from 1");
        rows.push_back({row.partition, seq});
        if (diskBacked && i == 700) CHECK(db->flushState() == 0, label + ": flush");
    }
    // Tombstone ~60%: runs at the start, the end and scattered in between,
    // so the kept rows need sequence runs everywhere.
    std::vector<uint64_t> probes;
    for (size_t i = 0; i < rows.size(); i++) {
        const bool dead = i < 40 || i >= rows.size() - 25 || gen.next(10) < 6;
        if (dead) db->markDeleted(rows[i].first, rows[i].second);
        if (i % 97 == 0) probes.push_back(rows[i].second);
    }
    const auto before = battery(*db, probes);
    const auto statsBefore = db->arenaStats();
    CHECK(statsBefore.deadBytes > statsBefore.size / 2, label + ": dead bytes counted");

    int steps = 0;
    CHECK(compactAll(*db, step, &steps) == 0, label + ": compaction completes: " + db->lastCompactionError());
    const auto statsAfter = db->arenaStats();
    const auto report = db->lastArenaCompaction();
    CHECK(report.completed, label + ": report completed");
    CHECK(statsAfter.size < statsBefore.size / 2, label + ": arena shrank");
    CHECK(statsAfter.size == report.afterBytes, label + ": report after bytes");
    CHECK(statsAfter.deadBytes == 0, label + ": no dead bytes left");
    // In place: nothing allocated for the bytes, the capacity kept.
    CHECK(statsAfter.capacity == statsBefore.capacity, label + ": packed in place");
    CHECK(report.sequenceRuns > 2, label + ": gaps became runs");
    CHECK(report.droppedRecords + report.keptRecords == rows.size(), label + ": every row kept or dropped");
    if (step) CHECK(steps > 3, label + ": ran in steps");

    const auto after = battery(*db, probes);
    CHECK(before.size() == after.size(), label + ": battery size");
    size_t differing = 0;
    for (size_t i = 0; i < before.size() && i < after.size(); i++) {
        if (before[i] != after[i]) {
            if (differing++ < 3) {
                std::cerr << "  " << label << " query " << i << " differs\n  before: "
                          << before[i].substr(0, 300) << "\n  after:  " << after[i].substr(0, 300) << std::endl;
            }
        }
    }
    CHECK(differing == 0, label + ": every query answers identically");

    // The next row gets the next sequence, not a reused one.
    const Row more = gen.make(5000);
    CHECK(ingest(*db, more) == rows.size() + 1, label + ": next sequence unchanged");
    // Nothing left to reclaim: a second call is a no-op.
    CHECK(db->compactArena(0) == 0, label + ": second compaction is a no-op");
    CHECK(db->arenaStats().records == report.keptRecords + 1, label + ": records");

    if (diskBacked) {
        const auto live = battery(*db, probes);
        CHECK(db->flushState() == 0, label + ": flush after");
        db.reset();
        auto reopened = openDb("/mem/eq.db");
        const int rc = reopened->openState();
        CHECK(rc == int(report.keptRecords + 1), label + ": reopen restores the kept rows, rc " + std::to_string(rc));
        const auto again = battery(*reopened, probes);
        CHECK(again == live, label + ": a reopened compacted state answers identically");
        const Row later = gen.make(6000);
        CHECK(ingest(*reopened, later) == rows.size() + 2, label + ": sequences continue after reopen");
        CHECK(!memfs::exists("/mem/eq.db.fsdata.compact"), label + ": temp stream removed");
    }
}

// ==================== 2. Between steps ====================

void testInterleaved() {
    const std::string label = "interleaved";
    memfs::reset();
    auto db = openDb("/mem/mix.db");
    setUpSources(*db);
    Gen gen(99);
    Contents model;  // visible rows
    std::vector<std::pair<std::string, uint64_t>> live;
    int serial = 0;
    auto add = [&](size_t nameBytes = 0) {
        const Row row = gen.make(++serial, nameBytes);
        const uint64_t seq = ingest(*db, row);
        model[row.partition][seq] = row.bytes;
        live.push_back({row.partition, seq});
    };
    auto kill = [&](size_t index) {
        auto [p, seq] = live[index];
        db->markDeleted(p, seq);
        model[p].erase(seq);
        live.erase(live.begin() + long(index));
    };
    for (int i = 0; i < 2000; i++) add(i % 50 == 0 ? 3000 : 0);
    CHECK(db->flushState() == 0, "flush");
    for (int i = 0; i < 1200; i++) kill(gen.next(live.size()));
    for (const auto& p : partitions()) model[p];  // every partition, even empty

    int steps = 0;
    int rc;
    bool sawCopying = false;
    do {
        rc = db->compactArena(2048);
        steps++;
        if (db->arenaStats().compacting && db->lastArenaCompaction().afterBytes) sawCopying = true;
        // Between steps: writes land, reads answer from the model.
        if (steps % 3 == 0) add();
        if (steps % 4 == 0 && !live.empty()) kill(gen.next(live.size()));
        if (steps % 11 == 0) CHECK(db->flushState() == 0, "flush between steps");
        if (steps % 5 == 0) CHECK(contents(*db) == model, "reads between steps equal the model (step " + std::to_string(steps) + ")");
    } while (rc == FlatSQLDatabase::kCompactPending);
    CHECK(rc == 0, "compaction completes: " + db->lastCompactionError());
    CHECK(sawCopying, "reads and writes landed while the stream was being rewritten");
    CHECK(contents(*db) == model, "after compaction the reads equal the model");

    // A reopen restores the arena: the kept rows (tombstones set between
    // steps are forgotten, as every tombstone is) plus what landed since.
    for (int i = 0; i < 10; i++) add();
    CHECK(db->flushState() == 0, "flush");
    const Contents arena = arenaContents(*db);
    const uint64_t next = db->getStorage().nextSequence();
    db.reset();
    auto reopened = openDb("/mem/mix.db");
    CHECK(reopened->openState() >= 0, "reopen");
    CHECK(contents(*reopened) == arena, "reopened arena equals the arena that was flushed");
    const Row row = gen.make(++serial);
    CHECK(ingest(*reopened, row) == next, "sequences continue");
}

// ==================== 2b. Inside a transaction ====================

void testInTransaction() {
    const std::string label = "in a transaction";
    memfs::reset();
    auto db = openDb("/mem/tx.db", 1, kSchemaNoIndex);
    setUpSources(*db);
    Gen gen(5);
    std::vector<std::pair<std::string, uint64_t>> rows;
    for (int i = 1; i <= 1200; i++) {
        const Row row = gen.make(i);
        rows.push_back({row.partition, ingest(*db, row)});
    }
    CHECK(db->flushState() == 0, label + ": flush");
    for (size_t i = 0; i < rows.size(); i++) {
        if (gen.next(10) < 7) db->markDeleted(rows[i].first, rows[i].second);
    }
    const auto before = battery(*db, {});
    const uint64_t sizeBefore = db->arenaStats().size;
    QueryResult ignored;
    std::string err;
    CHECK(db->queryNoThrow("BEGIN", {}, ignored, &err), label + ": BEGIN " + err);
    CHECK(db->compactArena(0) == FlatSQLDatabase::kCompactDeferred, label + ": swapped, persist deferred: " + db->lastCompactionError());
    CHECK(db->arenaStats().size < sizeBefore / 2, label + ": the room is there at once");
    CHECK(db->arenaStats().compacting, label + ": the persist is pending");
    CHECK(battery(*db, {}) == before, label + ": reads answer identically inside the transaction");
    // More writes in the same transaction, then a second in-memory pass.
    for (int i = 0; i < 50; i++) {
        const Row row = gen.make(5000 + i);
        rows.push_back({row.partition, ingest(*db, row)});
        db->markDeleted(row.partition, rows.back().second);
    }
    CHECK(db->compactArena(0) == FlatSQLDatabase::kCompactDeferred, label + ": a second pass in the transaction");
    CHECK(db->queryNoThrow("COMMIT", {}, ignored, &err), label + ": COMMIT " + err);
    CHECK(battery(*db, {}) == before, label + ": reads answer identically after the commit");
    // A flush persists the new layout first (the old stream is not appended to).
    CHECK(db->flushState() == 0, label + ": flush persists the swapped layout");
    CHECK(!db->arenaStats().compacting, label + ": nothing pending after the flush");
    CHECK(db->lastArenaCompaction().completed, label + ": the swap completed");
    CHECK(!memfs::exists("/mem/tx.db.fsdata.compact"), label + ": temp stream removed");
    CHECK(memfs::size("/mem/tx.db.fsdata") == db->arenaStats().size, label + ": the stream is the arena");
    const Contents arena = arenaContents(*db);
    db.reset();
    auto reopened = openDb("/mem/tx.db", 1, kSchemaNoIndex);
    CHECK(reopened->openState() >= 0, label + ": reopen");
    CHECK(contents(*reopened) == arena, label + ": reopened state is the swapped layout");
    // With index tables, a transaction refuses (their rows carry offsets).
    memfs::reset();
    auto indexed = openDb("/mem/tx2.db");
    setUpSources(*indexed);
    for (int i = 1; i <= 50; i++) {
        const Row row = gen.make(i);
        indexed->markDeleted(row.partition, ingest(*indexed, row));
    }
    CHECK(indexed->queryNoThrow("BEGIN", {}, ignored, &err), label + ": BEGIN");
    CHECK(indexed->compactArena(0) < 0, label + ": refused with index tables in a transaction");
    CHECK(indexed->queryNoThrow("COMMIT", {}, ignored, &err), label + ": COMMIT");
    CHECK(indexed->compactArena(0) == 0, label + ": outside the transaction it runs");
}

// ==================== 3. Crash injection ====================

struct CrashScenario {
    Contents oldPersisted;   // persisted before the compaction was called
    Contents oldComplete;    // the whole arena at the call (its first flush)
    Contents compacted;      // the kept rows
    uint64_t maxSeqGiven = 0;
};

// Builds the pre-compaction state on a fresh filesystem and returns the
// database, ready to compact.
std::unique_ptr<FlatSQLDatabase> crashSetup(Gen& gen, int journalMode, CrashScenario* s,
                                            const char* schema = kSchema) {
    memfs::reset(true);
    auto db = openDb("/mem/crash.db", journalMode, schema);
    setUpSources(*db);
    std::vector<std::pair<std::string, uint64_t>> rows;
    for (int i = 1; i <= 240; i++) {
        const Row row = gen.make(i, i % 40 == 0 ? 2500 : 0);
        rows.push_back({row.partition, ingest(*db, row)});
    }
    db->flushState();
    // The baseline is durable: a checkpoint syncs the WAL and the database
    // (SDN's WAL runs at synchronous=NORMAL, where commits alone are not).
    {
        QueryResult ignored;
        db->queryNoThrow("PRAGMA wal_checkpoint(FULL)", {}, ignored, nullptr);
    }
    for (int i = 241; i <= 260; i++) {  // unflushed tail
        const Row row = gen.make(i);
        rows.push_back({row.partition, ingest(*db, row)});
    }
    if (s) {
        // Persisted before the call: the flushed 240 rows.
        Contents all = contents(*db);
        for (auto& [p, part] : all) {
            for (auto it = part.begin(); it != part.end();) it = it->first > 240 ? part.erase(it) : std::next(it);
        }
        s->oldPersisted = all;
    }
    for (size_t i = 0; i < rows.size(); i++) {
        if (i < 10 || i % 3 != 0 || i >= rows.size() - 4) db->markDeleted(rows[i].first, rows[i].second);
    }
    if (s) {
        s->compacted = contents(*db);
        s->maxSeqGiven = rows.back().second;
        for (const auto& p : partitions()) db->clearTombstones(p);
        s->oldComplete = contents(*db);
        for (size_t i = 0; i < rows.size(); i++) {
            if (i < 10 || i % 3 != 0 || i >= rows.size() - 4) db->markDeleted(rows[i].first, rows[i].second);
        }
    }
    return db;
}

// A compaction swapped in memory inside a transaction, then persisted in
// steps once the transaction commits (SDN's mirror path).
int compactDeferred(FlatSQLDatabase& db, uint64_t step) {
    QueryResult ignored;
    std::string err;
    db.queryNoThrow("BEGIN", {}, ignored, &err);
    const int rc = db.compactArena(step);
    db.queryNoThrow("COMMIT", {}, ignored, &err);
    if (rc != FlatSQLDatabase::kCompactDeferred) return rc < 0 ? rc : -100;
    return compactAll(db, step);
}

void testCrashes(int journalMode, bool deferred = false) {
    const std::string mode = std::string(journalMode == 1 ? "WAL" : "rollback journal") +
                             (deferred ? ", swapped in a transaction" : "");
    const char* schema = deferred ? kSchemaNoIndex : kSchema;
    auto compactNow = [&](FlatSQLDatabase& db) {
        return deferred ? compactDeferred(db, 1500) : compactAll(db, 1500);
    };
    // Count the mutating calls of one complete compaction.
    uint64_t total = 0;
    {
        Gen gen(1234);
        auto db = crashSetup(gen, journalMode, nullptr, schema);
        const uint64_t before = memfs::fs().ops;
        CHECK(compactNow(*db) == 0, mode + ": reference compaction: " + db->lastCompactionError());
        total = memfs::fs().ops - before;
    }
    CHECK(total > 20, mode + ": a compaction does real I/O");
    int outcomes[4] = {0, 0, 0, 0};  // old-persisted, old-complete, compacted, reindexed-empty
    for (const memfs::Crash crashMode : {memfs::Crash::KeepAll, memfs::Crash::DropAll}) {
        const std::string cm = crashMode == memfs::Crash::KeepAll ? "kill -9" : "power loss";
        for (uint64_t at = 1; at <= total + 1; at++) {
            Gen gen(1234);
            CrashScenario s;
            auto db = crashSetup(gen, journalMode, &s, schema);
            memfs::fs().crashAt = memfs::fs().ops + at;
            compactNow(*db);  // fails once frozen, never traps
            db.reset();       // the process dies
            memfs::crash(crashMode);
            auto reopened = openDb("/mem/crash.db", journalMode, schema);
            int rc = reopened->openState();
            if (rc < 0 && rc != FlatSQLDatabase::kStateAbsent) {
                reopened.reset();
                reopened = openDb("/mem/crash.db", journalMode, schema);
                rc = reopened->reindexAll();
            }
            const std::string where = mode + ", " + cm + ", crash at I/O call " + std::to_string(at) +
                                      " of " + std::to_string(total);
            CHECK(rc >= 0, where + ": the state opens (rc " + std::to_string(rc) + ")");
            if (rc < 0) continue;
            const Contents got = contents(*reopened);
            int which = -1;
            if (got == s.oldPersisted) which = 0;
            else if (got == s.oldComplete) which = 1;
            else if (got == s.compacted) which = 2;
            else if (rowCount(got) == 0) which = 3;
            CHECK(which >= 0, where + ": the reopened state is the old one or the new one (" +
                                  std::to_string(rowCount(got)) + " rows; old " +
                                  std::to_string(rowCount(s.oldPersisted)) + "/" +
                                  std::to_string(rowCount(s.oldComplete)) + ", new " +
                                  std::to_string(rowCount(s.compacted)) + ")");
            if (which >= 0) outcomes[which]++;
            // Sequences are never handed out twice: a fresh row is numbered
            // past every row the reopened state holds.
            const Row row = gen.make(9999);
            const uint64_t seq = ingest(*reopened, row);
            CHECK(seq > maxSequence(got), where + ": a new row gets a fresh sequence");
            if (which == 2) CHECK(seq > s.maxSeqGiven, where + ": the compacted state keeps the next sequence");
            // And the state it settled on is stable across another reopen.
            CHECK(reopened->flushState() == 0, where + ": flush after reopen");
            const Contents settled = contents(*reopened);
            reopened.reset();
            auto third = openDb("/mem/crash.db", journalMode, schema);
            CHECK(third->openState() >= 0, where + ": third open");
            CHECK(contents(*third) == settled, where + ": stable across reopen");
            CHECK(!memfs::exists("/mem/crash.db.fsdata.compact"), where + ": temp stream gone after open");
        }
    }
    std::cout << "  crash injection (" << mode << "): " << total << " I/O calls x 2 modes; outcomes old-persisted "
              << outcomes[0] << ", old-complete " << outcomes[1] << ", compacted " << outcomes[2]
              << ", emptied " << outcomes[3] << std::endl;
    CHECK(outcomes[2] > 0 && (outcomes[0] + outcomes[1]) > 0, mode + ": both outcomes reached");
}

// ==================== 4. Churn soak ====================

void testSoak() {
    const char* env = std::getenv("FLATSQL_SOAK_MIB");
    const uint64_t totalMiB = env ? std::strtoull(env, nullptr, 10) : 96;
    // host-02 shape, scaled: budget 512 MiB and compaction mark 256 MiB for
    // runs past 1 GiB of ingest; 1/8 of that for the default CI run.
    const bool full = totalMiB >= 1024;
    const uint64_t budget = full ? (uint64_t(512) << 20) : (uint64_t(64) << 20);
    const uint64_t mark = budget / 2;
    // Live rows per source: host-02's hot window is ~150 MB of a 512 MiB
    // budget; three sources of ~1.7 KB rows.
    const size_t window = full ? 30000 : 3700;
    memfs::reset(false);
    auto db = openDb("/mem/soak.db");
    setUpSources(*db);
    db->setArenaLimit(budget);
    Gen gen(42);
    std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> windows;  // per source
    std::map<uint64_t, std::string> liveRows;  // seq -> partition
    uint64_t ingested = 0, refused = 0, compactions = 0, maxSize = 0, maxCapacity = 0;
    double longestStepMs = 0, compactMs = 0;
    int serial = 0;
    const auto started = std::chrono::steady_clock::now();
    while (ingested < (totalMiB << 20)) {
        // IQC-like captures: ~1.7 KB average, some 20 KB.
        const size_t nameBytes = gen.next(50) == 0 ? 20000 : 600 + gen.next(2200);
        const Row row = gen.make(++serial, nameBytes);
        // SDN's second trigger: a record that would not fit compacts first
        // when anything is dead, instead of being refused.
        if (db->arenaStats().size + row.bytes.size() + 4 > budget && db->arenaStats().deadBytes > 0) {
            CHECK(compactAll(*db, uint64_t(16) << 20) == 0, "soak compaction before a refusal");
            compactions++;
        }
        const uint64_t seq = ingest(*db, row);
        if (seq == FlatSQLDatabase::kIngestRefused) {
            refused++;
            if (refused > 1000) {
                CHECK(false, "soak: the arena stays full");
                break;
            }
            continue;
        }
        ingested += row.bytes.size() + 4;
        const std::string source = sourceOf(row.partition);
        windows[source].push_back({row.partition, seq});
        liveRows[seq] = row.partition;
        if (windows[source].size() > window) {
            const auto [p, old] = windows[source].front();
            windows[source].erase(windows[source].begin());
            db->markDeleted(p, old);
            liveRows.erase(old);
        }
        if (serial % 20000 == 0) db->flushState();
        const auto stats = db->arenaStats();
        maxSize = std::max<uint64_t>(maxSize, stats.size);
        maxCapacity = std::max<uint64_t>(maxCapacity, stats.capacity);
        // SDN's trigger: past the mark with more than half of it dead.
        if (stats.size > mark && stats.deadBytes * 2 > stats.size) {
            const auto t0 = std::chrono::steady_clock::now();
            int rc;
            do {
                const auto s0 = std::chrono::steady_clock::now();
                rc = db->compactArena(uint64_t(16) << 20);
                longestStepMs = std::max(longestStepMs, std::chrono::duration<double, std::milli>(
                                                            std::chrono::steady_clock::now() - s0).count());
            } while (rc == FlatSQLDatabase::kCompactPending);
            CHECK(rc == 0, "soak compaction: " + db->lastCompactionError());
            compactMs += std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
            compactions++;
        }
    }
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    CHECK(refused == 0, "no ingest refused: the arena never reached its budget");
    CHECK(compactions > 0, "the soak compacted");
    CHECK(maxCapacity <= budget, "capacity stayed under the budget");
    // Final answers equal the model: exactly the window rows, by sequence.
    std::set<uint64_t> expected;
    for (const auto& [seq, p] : liveRows) {
        (void)p;
        expected.insert(seq);
    }
    std::set<uint64_t> got;
    for (const auto& p : partitions()) {
        QueryResult result;
        std::string err;
        CHECK(db->queryNoThrow("SELECT _rowid FROM \"" + p + "\"", {}, result, &err), "soak query " + err);
        for (const auto& r : result.rows) got.insert(uint64_t(std::get<int64_t>(r[0])));
    }
    CHECK(got == expected, "soak: the partitions hold exactly the window");
    std::cout << "  soak: " << (ingested >> 20) << " MiB ingested (" << serial << " rows) in " << seconds
              << " s; " << compactions << " compactions, " << compactMs << " ms total, longest step "
              << longestStepMs << " ms; peak arena " << (maxSize >> 20) << " MiB, peak capacity "
              << (maxCapacity >> 20) << " MiB, budget " << (budget >> 20) << " MiB; live rows "
              << expected.size() << std::endl;
}

}  // namespace

int main() {
    std::cout << "equivalence" << std::endl;
    testEquivalence(false, 0);
    testEquivalence(true, 0);
    testEquivalence(true, 4096);
    std::cout << "between steps" << std::endl;
    testInterleaved();
    std::cout << "in a transaction" << std::endl;
    testInTransaction();
    std::cout << "crash injection" << std::endl;
    testCrashes(1);
    testCrashes(0);
    testCrashes(1, true);
    std::cout << "soak" << std::endl;
    testSoak();
    if (g_failures) {
        std::cerr << g_failures << " failure(s)" << std::endl;
        return 1;
    }
    std::cout << "arena compaction: equivalence, steps, crash injection and soak passed" << std::endl;
    return 0;
}
