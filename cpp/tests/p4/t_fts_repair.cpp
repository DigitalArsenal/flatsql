// Synthetic upgrade, restart, and concurrency coverage for the derived index.
#include "p4/p4_test.h"
#include "internal.h"
#include <chrono>
#include <thread>
#if !defined(__wasm__)
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace flatsql { namespace p4 { P4Engine* currentEngine(); } }
namespace fp = flatsql::p4;
using namespace p4t;

namespace {
TestType textType() { auto t = mpeType(); t.fullText = true; return t; }
Batch records(int n, int from = 0) {
    Batch b; b.type = "MPE"; b.peer = "synthetic-peer";
    for (int i = from; i < from + n; ++i) {
        In r;
        r.frame = buildFrame(textType(), {Field::str("ENTITY_ID", "violet comet " + std::to_string(i)),
                                          Field::f64("EPOCH", 1789000000.0)});
        if (i % 2) {
            const uint32_t n = uint32_t(r.frame.size());
            r.frame.insert(r.frame.begin(), 4, 0);
            fp::st32(r.frame.data(), n);
        }
        r.ts = 1790000000;
        b.recs.push_back(std::move(r));
    }
    return b;
}
Result findText(uint64_t timeout = 5000) {
    TlvW q; q.text(1, "MPE").text(18, "violet").u64(3, 100000);
    CallOpts o; o.timeoutMs = timeout;
    return call(FLATSQL_OPC_SCAN, q.b, o);
}
std::string state() {
    TlvW q; q.u8(45, 5).text(1, "MPE");
    const auto r = call(FLATSQL_OPC_SUMMARY, q.b);
    return r.rows.empty() ? r.err : r.s(0, "state");
}
bool ready() {
    for (int i = 0; i < 200; ++i) {
        if (state() == "ready") return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}
size_t matches() {
    for (int i = 0; i < 200; ++i) {
        const auto r = findText();
        if (r.status == FLATSQL_OK) return r.rows.size();
        if (r.status != FLATSQL_E_BUSY) return SIZE_MAX;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return SIZE_MAX;
}
uint64_t extracted() { return fp::currentEngine()->stat[fp::kStFtsRows].load(); }
// Edit only the closed derived index: reproduce missing prefixed entries and
// the absent or old extractor version without a binary fixture.
bool editIndex(const std::string& root, const std::string& sql) {
    sqlite3* db = nullptr;
    int rc = sqlite3_open_v2((root + "/T/MPE.fts").c_str(), &db, SQLITE_OPEN_READWRITE, "unix");
    if (rc == SQLITE_OK) rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) std::fprintf(stderr, "FTS fixture: %s\n", db ? sqlite3_errmsg(db) : "open failed");
    if (db) sqlite3_close(db);
    return rc == SQLITE_OK;
}
std::string oldIndex(bool absent) {
    return "DELETE FROM fts WHERE rowid%2=0; DELETE FROM ftsmeta WHERE k IN ('extractor_version','extractor_target');" +
           std::string(absent ? "" : "INSERT INTO ftsmeta(k,v) VALUES('extractor_version',0);");
}
bool seed(const std::string& root, int n) {
    if (openEngine(root) != FLATSQL_OK || registerType(textType()) != FLATSQL_OK || put(records(n)).status != FLATSQL_OK)
        return false;
    const bool ok = ready() && matches() == size_t(n);
    return closeEngine() == FLATSQL_OK && ok;
}
Result rebuild() {
    TlvW w; w.text(1, "MPE").u32(63, 4);
    return call(FLATSQL_OPC_REBUILD, w.b);
}
void eraseRecord(const In& r) {
    uint8_t cid[36]; cidOf(r.frame, cid);
    std::vector<uint8_t> ids{1, 0, 0, 0}; ids.insert(ids.end(), cid, cid + 36);
    TlvW w; w.text(1, "MPE").raw(40, ids.data(), ids.size());
    CHECK_EQ(call(FLATSQL_OPC_DELETE, w.b).status, FLATSQL_OK, "delete during repair");
}
}

FLATSQL_TEST(t_fts_upgrade) {
    for (int n : {1, 2}) for (bool absent : {false, true}) {
        const std::string root = scratchDir("fts-upgrade");
        REQUIRE(seed(root, n), "seed bare-only or mixed current index");
        REQUIRE(editIndex(root, oldIndex(absent)), "install old extractor state");
        REQUIRE(openEngine(root) == FLATSQL_OK, "reopen old index");
        // SUMMARY does not build FTS; the slow loop repairs without a search.
        REQUIRE(ready(), "background upgrade without search or rebuild");
        CHECK_EQ(extracted(), uint64_t(n), "each stored envelope re-extracted once");
        CHECK_EQ(matches(), size_t(n), "all records repaired");
        REQUIRE(closeEngine() == FLATSQL_OK, "close repaired store");
        REQUIRE(openEngine(root) == FLATSQL_OK, "reopen current store");
        REQUIRE(ready(), "current index ready");
        CHECK_EQ(matches(), size_t(n), "current matches");
        CHECK_EQ(extracted(), uint64_t(0), "current index does not re-extract");
        closeEngine(); removeTree(root);
    }
}

FLATSQL_TEST(t_fts_rebuild_immediate) {
    const std::string root = scratchDir("fts-immediate");
    REQUIRE(seed(root, 2), "seed");
    for (int attempt = 0; attempt < 3; ++attempt) {
        // Keep the current version: only the explicit operation can repair it.
        REQUIRE(editIndex(root, "DELETE FROM fts WHERE rowid%2=0"), "remove prefixed row");
        REQUIRE(openEngine(root, {}, false) == FLATSQL_OK, "init before threads");
        auto* e = fp::currentEngine();
        REQUIRE(e->findType("MPE")->fts == nullptr, "connection absent when rebuild submitted");
        TlvW w; w.text(1, "MPE").u32(63, 4);
        const uint32_t slot = submit(FLATSQL_OPC_REBUILD, w.b);
        REQUIRE(flatsql_start() >= 0, "start");
        REQUIRE(wait(slot).status == FLATSQL_OK, "immediate rebuild");
        CHECK_EQ(matches(), size_t(2), "both matches after immediate rebuild");
        CHECK_EQ(extracted(), uint64_t(2), "explicit rebuild extracted both records");
        closeEngine();
    }
    removeTree(root);
}

FLATSQL_TEST(t_fts_repair_pages) {
    const std::string root = scratchDir("fts-pages");
    constexpr int n = 4100;
    REQUIRE(seed(root, n), "seed more than one page");
    REQUIRE(editIndex(root, oldIndex(true)), "old mixed index");
    REQUIRE(openEngine(root, {}, false) == FLATSQL_OK, "init");
    auto* e = fp::currentEngine(); auto* t = e->findType("MPE");
    // Keep page scheduling deterministic; the readers and writers still run.
    e->slowStop = true;
    REQUIRE(flatsql_start() >= 0, "start readers and writers");
    REQUIRE(fp::ftsCatchUp(e, t, false) == FLATSQL_OK, "one repair page");
    CHECK_EQ(extracted(), uint64_t(4096), "bounded extraction page");
    CHECK(state() == "building", "partial repair is not ready");
    CHECK_EQ(findText().status, FLATSQL_E_BUSY, "partial index returns BUSY");
    {
        std::lock_guard<std::mutex> lock(t->ftsMu);
        // Waiting on ftsMu here would time out: BUSY must arrive while held.
        CHECK_EQ(findText(1000).status, FLATSQL_E_BUSY, "busy lock returns immediately");
    }
    const auto b = records(n);
    eraseRecord(b.recs.front());   // already extracted
    eraseRecord(b.recs.back());    // not extracted
    REQUIRE(fp::ftsCatchUp(e, t, true) == FLATSQL_OK, "finish repair");
    CHECK(state() == "ready", "repair completed");
    CHECK_EQ(matches(), size_t(n - 2), "both deletions respected");
    closeEngine();
    REQUIRE(openEngine(root) == FLATSQL_OK, "reopen completed repair");
    REQUIRE(ready(), "ready after reopen");
    CHECK_EQ(matches(), size_t(n - 2), "removals persist");
    CHECK_EQ(extracted(), uint64_t(0), "completed pages not repeated");
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_fts_rebuild_held) {
    const std::string root = scratchDir("fts-held");
    REQUIRE(seed(root, 2), "seed");
    REQUIRE(openEngine(root, {}, false) == FLATSQL_OK, "init");
    fp::currentEngine()->ftsHold = true;
    REQUIRE(flatsql_start() >= 0, "start");
    CHECK_EQ(rebuild().status, FLATSQL_OK, "committed reset succeeds while extraction is held");
    auto* t = fp::currentEngine()->findType("MPE");
    const auto generation = t->ftsReset.load();
    CHECK_EQ(rebuild().status, FLATSQL_OK, "duplicate pending rebuild is already accepted");
    CHECK_EQ(t->ftsReset.load(), generation, "duplicate request does not reset again");
    CHECK(state() == "building", "reset persists incomplete state");
    CHECK_EQ(findText().status, FLATSQL_E_BUSY, "held reset cannot answer empty success");
    closeEngine();
    REQUIRE(openEngine(root) == FLATSQL_OK, "reopen after reset before first page");
    REQUIRE(ready(), "repair resumes");
    CHECK_EQ(matches(), size_t(2), "both records recovered");
    closeEngine(); removeTree(root);
}

#if !defined(__wasm__)
FLATSQL_TEST(t_fts_repair_crash) {
    const std::string root = scratchDir("fts-crash");
    constexpr int n = 4100;
    REQUIRE(seed(root, n), "seed");
    for (int pages : {0, 1}) {
        REQUIRE(editIndex(root, oldIndex(true)), "old index");
        const pid_t child = fork();
        REQUIRE(child >= 0, "fork");
        if (child == 0) {
            if (openEngine(root, {}, false) != FLATSQL_OK) _exit(2);
            auto* e = fp::currentEngine(); auto* t = e->findType("MPE");
            e->ftsHold = pages == 0;
            const int32_t rc = fp::ftsCatchUp(e, t, false);
            if (rc != (pages ? FLATSQL_OK : FLATSQL_E_BUSY) || t->ftsThrough != (pages ? 4096 : 0) ||
                t->ftsVersion != 0 || extracted() != uint64_t(pages * 4096)) _exit(3);
            // No engine close or SQLite cleanup: terminate at a committed
            // reset or page boundary, leaving the journal for recovery.
            kill(getpid(), SIGKILL);
            _exit(4);
        }
        int status = 0;
        REQUIRE(waitpid(child, &status, 0) == child, "wait exact child");
        REQUIRE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL, "crash landed after committed boundary");
        REQUIRE(openEngine(root) == FLATSQL_OK, "reopen killed repair");
        REQUIRE(ready(), "background repair resumes");
        CHECK_EQ(matches(), size_t(n), "all records recovered");
        CHECK_EQ(extracted(), uint64_t(n - pages * 4096), "committed pages are resumed, not restarted");
        closeEngine();
    }
    removeTree(root);
}
#endif


FLATSQL_TEST(t_fts_concurrent_readers) {
    const std::string root = scratchDir("fts-concurrent");
    constexpr int n = 20000;
    REQUIRE(seed(root, n), "seed idle index");
    REQUIRE(openEngine(root) == FLATSQL_OK && ready(), "reopen ready");
    for (auto count : {std::make_pair(4, 25), std::make_pair(8, 50)}) {
        std::atomic<int> ok{0}, busy{0}, other{0};
        std::vector<std::thread> threads;
        for (int k = 0; k < count.first; ++k) threads.emplace_back([&] {
            for (int j = 0; j < count.second; ++j) {
                const auto r = findText(30000);
                if (r.status == FLATSQL_OK && r.rows.size() == n) ++ok;
                else if (r.status == FLATSQL_E_BUSY) ++busy;
                else ++other;
            }
        });
        for (auto& t : threads) t.join();
        std::printf("  %d x %d: ok=%d busy=%d other=%d\n", count.first, count.second,
                    ok.load(), busy.load(), other.load());
        CHECK_EQ(ok.load(), count.first * count.second, "every complete search succeeds");
        CHECK_EQ(busy.load(), 0, "readers do not contend");
        CHECK_EQ(other.load(), 0, "no other errors");
    }
    // A maintenance page's mutex cannot block a reader of a complete snapshot.
    {
        std::lock_guard<std::mutex> lock(fp::currentEngine()->findType("MPE")->ftsMu);
        CHECK_EQ(findText().status, FLATSQL_OK, "search does not take maintenance mutex");
    }
    closeEngine(); removeTree(root);
}

namespace {
uint32_t pausedSearch() {
    TlvW q; q.text(1, "MPE").text(18, "violet").u64(3, 100000);
    const uint32_t slot = submit(FLATSQL_OPC_SCAN, q.b);
    auto* e = fp::currentEngine(); auto* h = e->slot(slot);
    for (int i = 0; i < 2000; ++i) {
        // An undrained ring pauses a multi-page query after its snapshot opens.
        if (h->ringTail.load() >= e->ringBytes[e->poolOf(slot)] / 2) return slot;
        if (h->state.load() == FLATSQL_SLOT_DONE) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return slot;
}
}

FLATSQL_TEST(t_fts_snapshot_ingest) {
    const std::string root = scratchDir("fts-snapshot");
    constexpr int n = 20000;
    REQUIRE(seed(root, n), "seed");
    REQUIRE(openEngine(root) == FLATSQL_OK && ready(), "ready");
    const uint32_t slot = pausedSearch();
    auto* e = fp::currentEngine(); auto* t = e->findType("MPE");
    e->ftsHold = true;
    REQUIRE(put(records(100, n)).status == FLATSQL_OK, "ingest beyond search snapshot");
    for (int i = 0; i < 200 && state() != "building"; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK(state() == "building", "new records await extraction");
    const auto r = wait(slot);
    CHECK_EQ(r.status, FLATSQL_OK, "indexed snapshot succeeds during ingest");
    CHECK_EQ(r.rows.size(), size_t(n), "snapshot excludes later records");
    e->ftsHold = false;
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_fts_snapshot_reset) {
    const std::string root = scratchDir("fts-reset-search");
    constexpr int n = 20000;
    REQUIRE(seed(root, n), "seed");
    REQUIRE(openEngine(root) == FLATSQL_OK && ready(), "ready");
    const uint32_t slot = pausedSearch();
    REQUIRE(rebuild().status == FLATSQL_OK, "reset accepted");
    REQUIRE(ready(), "rebuild completes before old search resumes");
    const auto r = wait(slot);
    CHECK_EQ(r.status, FLATSQL_E_BUSY, "reset invalidates old search even after catch-up");
    closeEngine(); removeTree(root);
}

// Explicitly selected million-record comparison; ordinary test runs skip it.
FLATSQL_TEST(t_fts_reextract_timing) {
    const std::string root = argStr("fts-timing-store", "");
    if (root.empty()) return;
    constexpr int n = 1000000;
    if (argInt("fts-timing-seed", 0)) {
        REQUIRE(openEngine(root) == FLATSQL_OK, "seed open");
        fp::currentEngine()->ftsHold = true;
        REQUIRE(registerType(textType()) == FLATSQL_OK, "seed register");
        for (int i = 0; i < n; i += 2000)
            REQUIRE(put(records(2000, i)).status == FLATSQL_OK, "synthetic seed batch");
        REQUIRE(closeEngine() == FLATSQL_OK, "seed close");
        return;
    }
    REQUIRE(editIndex(root, "DELETE FROM fts; DELETE FROM ftsmeta;"), "reset closed derived index");
    REQUIRE(openEngine(root, {}, false) == FLATSQL_OK, "open before timing");
    const auto start = std::chrono::steady_clock::now();
    REQUIRE(flatsql_start() >= 0, "start background repair");
    auto* e = fp::currentEngine(); auto* t = e->findType("MPE");
    bool complete = false;
    for (int i = 0; i < 120000; ++i) {
        {
            std::lock_guard<std::mutex> lock(t->ftsMu);
            complete = t->ftsState == 2 && t->ftsThrough >= t->vis.load();
        }
        if (complete) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    CHECK(complete, "million-record repair completed");
    CHECK_EQ(extracted(), uint64_t(n), "each record extracted once");
    std::printf("  TIMING records=%d seconds=%.6f records_per_second=%.3f\n", n, seconds, n / seconds);
    REQUIRE(closeEngine() == FLATSQL_OK, "timed close");
}


FLATSQL_TEST(t_fts_version_range) {
    const std::string root = scratchDir("fts-version-range");
    REQUIRE(seed(root, 2), "seed certified prefix");
    REQUIRE(openEngine(root) == FLATSQL_OK && ready(), "current open");
    REQUIRE(put(records(4, 2)).status == FLATSQL_OK && ready(), "append suffix");
    REQUIRE(closeEngine() == FLATSQL_OK, "close before simulating older engine");
    // The older engine keeps unknown keys, skips prefixed rows, and advances
    // through. The two-record certified prefix remains intact.
    REQUIRE(editIndex(root, "DELETE FROM fts WHERE rowid>2 AND rowid%2=0;"
                            "INSERT OR REPLACE INTO ftsmeta(k,v) VALUES('extractor_through',2)"), "older append");
    REQUIRE(openEngine(root) == FLATSQL_OK && ready(), "repair suffix automatically");
    CHECK_EQ(extracted(), uint64_t(4), "only uncovered suffix extracted");
    CHECK_EQ(matches(), size_t(6), "prefix and suffix both complete");
    REQUIRE(closeEngine() == FLATSQL_OK, "close repaired range");
    REQUIRE(openEngine(root) == FLATSQL_OK && ready(), "current reopen");
    CHECK_EQ(extracted(), uint64_t(0), "current range re-extracts nothing");
    CHECK_EQ(matches(), size_t(6), "all matches persist");
    closeEngine(); removeTree(root);
}


FLATSQL_TEST(t_fts_rebuild_retry) {
    const std::string root = scratchDir("fts-rebuild-retry");
    REQUIRE(seed(root, 4100), "seed");
    REQUIRE(editIndex(root, oldIndex(true)), "request automatic repair");
    REQUIRE(openEngine(root, {}, false) == FLATSQL_OK, "init without background thread");
    auto* e = fp::currentEngine(); auto* t = e->findType("MPE");
    REQUIRE(fp::ftsCatchUp(e, t, false) == FLATSQL_OK, "commit first repair page");
    const auto generation = t->ftsReset.load();
    REQUIRE(t->ftsThrough == 4096, "nonzero partial progress");
    e->ftsHold = true;
    TlvW w; w.text(1, "MPE").u32(63, 4);
    const uint32_t slot = submit(FLATSQL_OPC_REBUILD, w.b);
    REQUIRE(flatsql_start() >= 0, "start");
    CHECK_EQ(wait(slot).status, FLATSQL_OK, "pending repair is already accepted");
    CHECK_EQ(t->ftsThrough.load(), int64_t(4096), "retry preserves committed progress");
    CHECK_EQ(t->ftsReset.load(), generation, "retry does not reset index");
    e->ftsHold = false;
    REQUIRE(ready(), "finish background repair");
    CHECK_EQ(extracted(), uint64_t(4100), "no page repeated");
    CHECK_EQ(matches(), size_t(4100), "all matches restored");
    closeEngine(); removeTree(root);
}
