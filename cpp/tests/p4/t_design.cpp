// The design's regression tests (design §14): t_close (B1), t_gap (B2),
// t_crash and t_kill (B3, M8).
//
// A "crash" is a real process death: the engine runs in a forked child that
// _exits (or is killed with SIGKILL) without stopping; the parent reopens the
// store and checks it. The kill loop's two halves (t_kill_run, t_kill_check)
// are also separate tests, so the wasm build's host can drive the same loop.
#include <unistd.h>
#if !defined(__wasm__)
#include <signal.h>
#include <sys/wait.h>
#endif

#include <atomic>
#include <cstring>
#include <filesystem>
#include <random>
#include <set>
#include <thread>
#include <unordered_set>

#include "flatsql/flatsql_io.h"
#include "internal.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;

namespace {

TestType& pnm() {
    static TestType t = pnmLikeType("PNM");
    return t;
}

std::vector<uint8_t> pnmFrame(uint64_t id, size_t pad = 160) {
    return buildFrame(pnm(), {Field::str("FILE_ID", "file-" + std::to_string(id)), Field::str("NAME", "n" + std::to_string(id)),
                              Field::raw("BODY", std::vector<uint8_t>(pad, uint8_t(id)))});
}

Batch pnmBatch(const std::string& peer, const std::string& batch, uint64_t from, int n, const std::string& source = "src") {
    Batch b;
    b.type = "PNM";
    b.peer = peer;
    b.tags.push_back(Tag{"prov", source, "", batch, "", "", ""});
    b.at = 1790000000;
    for (int i = 0; i < n; i++) {
        In in;
        in.frame = pnmFrame(from + uint64_t(i));
        in.ts = 1790000000;
        b.recs.push_back(std::move(in));
    }
    return b;
}

#if !defined(__wasm__)
// Runs fn in a child process that dies without stopping the engine.
bool inChild(const std::function<void()>& fn) {
    std::fflush(stdout);
    std::fflush(stderr);
    const pid_t pid = fork();
    if (pid == 0) {
        fn();
        std::fflush(stdout);
        std::fflush(stderr);
        _exit(0);
    }
    int st = 0;
    waitpid(pid, &st, 0);
    return WIFEXITED(st) && WEXITSTATUS(st) == 0;
}
#endif

int64_t countRows(const std::string& path) {
    sqlite3* db = nullptr;
    if (sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
        sqlite3_close(db);
        return -1;
    }
    sqlite3_stmt* s = nullptr;
    int64_t n = -1;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM r", -1, &s, nullptr) == SQLITE_OK && sqlite3_step(s) == SQLITE_ROW)
        n = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    sqlite3_close(db);
    return n;
}

std::string integrity(const std::string& path) {
    sqlite3* db = nullptr;
    if (sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
        sqlite3_close(db);
        return "open failed";
    }
    sqlite3_stmt* s = nullptr;
    std::string out = "?";
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &s, nullptr) == SQLITE_OK && sqlite3_step(s) == SQLITE_ROW)
        out = reinterpret_cast<const char*>(sqlite3_column_text(s, 0));
    sqlite3_finalize(s);
    sqlite3_close(db);
    return out;
}

std::vector<std::string> partitionFiles(const std::string& root, const std::string& type) {
    std::vector<std::string> out;
    std::error_code ec;
    for (auto& d : std::filesystem::recursive_directory_iterator(root + "/P/" + type, ec)) {
        const std::string p = d.path().string();
        if (p.size() > 3 && p.compare(p.size() - 3, 3, ".db") == 0) out.push_back(p);
    }
    return out;
}

Result summary1(const std::string& type) {
    TlvW w;
    w.u8(45, 1).text(1, type);
    return call(P4_OPC_SUMMARY, w.b);
}

Result scanAfter(const std::string& type, int64_t after, uint64_t limit) {
    TlvW w;
    w.text(1, type).i64(6, after).u64(3, limit);
    return call(P4_OPC_SCAN, w.b);
}

}  // namespace

#if !defined(__wasm__)
// B1 at the VFS: closing one connection must not delete the WAL under the
// others; after a crash every committed row is there.
P4_TEST(t_close) {
    const std::string dir = scratchDir("close");
    const std::string path = dir + "/f.db";
    flatsql::registerFlatSqlIoVfs(false);
    const std::string uri = "file:" + path + "?share=1";
    const bool ok = inChild([&] {
        sqlite3 *w = nullptr, *r = nullptr, *r2 = nullptr;
        sqlite3_open_v2(uri.c_str(), &w, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, "flatsql_io");
        sqlite3_exec(w, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;"
                        "CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)", nullptr, nullptr, nullptr);
        auto ins = [&](int from, int to) {
            sqlite3_exec(w, "BEGIN", nullptr, nullptr, nullptr);
            sqlite3_stmt* s;
            sqlite3_prepare_v2(w, "INSERT INTO t(x,y) VALUES(?1, randomblob(400))", -1, &s, nullptr);
            for (int i = from; i < to; i++) {
                sqlite3_bind_int(s, 1, i);
                sqlite3_step(s);
                sqlite3_reset(s);
            }
            sqlite3_finalize(s);
            sqlite3_exec(w, "COMMIT", nullptr, nullptr, nullptr);
        };
        ins(0, 1000);
        sqlite3_open_v2((uri + "&ra=1").c_str(), &r, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "flatsql_io");
        sqlite3_exec(r, "PRAGMA query_only=1; SELECT count(*) FROM t", nullptr, nullptr, nullptr);
        sqlite3_close(r);  // a reader-pool eviction while the writer is open
        if (!fp::ioExists(path + "-wal")) std::_Exit(3);
        ins(1000, 2000);
        sqlite3_open_v2((uri + "&ra=1").c_str(), &r2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "flatsql_io");
        sqlite3_stmt* s;
        sqlite3_prepare_v2(r2, "SELECT count(*) FROM t", -1, &s, nullptr);
        int64_t n = sqlite3_step(s) == SQLITE_ROW ? sqlite3_column_int64(s, 0) : -1;
        sqlite3_finalize(s);
        if (n != 2000) std::_Exit(4);
        _exit(0);  // crash: W never closes
    });
    CHECK(ok, "child: the WAL survived the reader's close and a new reader saw 2,000 rows");
    sqlite3* d = nullptr;
    sqlite3_open_v2(path.c_str(), &d, SQLITE_OPEN_READWRITE, nullptr);
    sqlite3_stmt* s;
    sqlite3_prepare_v2(d, "SELECT count(*) FROM t", -1, &s, nullptr);
    const int64_t n = sqlite3_step(s) == SQLITE_ROW ? sqlite3_column_int64(s, 0) : -1;
    sqlite3_finalize(s);
    sqlite3_close(d);
    CHECK_EQ(n, int64_t(2000), "after the crash every committed row");
    CHECK(integrity(path) == "ok", "integrity");
}

#endif

// B2: a datasync follower never skips a committed record: a writer that took
// lower seqs and commits late holds the visible-through mark below them.
P4_TEST(t_gap) {
    const std::string root = scratchDir("gap") + "/fsql4";
    EngineOpts o;
    o.writers = 2;
    REQUIRE(openEngine(root, o) == P4_OK, "open");
    REQUIRE(registerType(pnm()) == P4_OK, "register");
    // both partitions exist (pinned to different writers) before the race
    REQUIRE(put(pnmBatch("peerA", "b0", 1, 1)).status == P4_OK, "seed A");
    REQUIRE(put(pnmBatch("peerB", "b0", 2, 1)).status == P4_OK, "seed B");
    const int nx = int(argInt("gap_records", 40000));
    std::atomic<bool> xDone{false}, yDone{false};
    std::atomic<long> yStored{0};
    std::thread x([&] {
        for (int c = 0; c < 4; c++) put(pnmBatch("peerA", "bx", 1000000 + uint64_t(c) * nx, nx / 4));
        xDone = true;
    });
    std::thread y([&] {
        for (int k = 0; !xDone || k < 50; k++) {
            if (put(pnmBatch("peerB", "by", 2000000 + uint64_t(k), 1)).status == P4_OK) yStored++;
            flatsql::ps::sleepNs(300000);
        }
        yDone = true;
    });
    int64_t cursor = 0;
    long received = 0;
    std::set<int64_t> seen;
    auto drain = [&] {
        for (;;) {
            Result r = scanAfter("PNM", cursor, 5000);
            if (r.status != P4_OK || r.rows.empty()) break;
            for (size_t i = 0; i < r.rows.size(); i++) {
                seen.insert(r.i(i, "seq"));
                cursor = r.i(i, "seq");
            }
            received += long(r.rows.size());
        }
    };
    while (!(xDone && yDone)) {
        drain();
        flatsql::ps::sleepNs(2000000);
    }
    x.join();
    y.join();
    drain();
    Result all = summary1("PNM");
    const int64_t total = all.rows.size() == 1 ? all.i(0, "records") : -1;
    CHECK_EQ(int64_t(seen.size()), total, "the follower received every record");
    CHECK_EQ(int64_t(received), total, "exactly once");
    report("t_gap.records", double(total), "records");
    closeEngine();
}

#if !defined(__wasm__)
// B3: a crash between partition commits and the deferred type-index flush.
P4_TEST(t_crash) {
    const std::string root = scratchDir("crash") + "/fsql4";
    EngineOpts o;
    o.flushEntries = 5000;
    const bool ok = inChild([&] {
        if (openEngine(root, o) != P4_OK || registerType(pnm()) != P4_OK) std::_Exit(2);
        if (put(pnmBatch("peerA", "b1", 1000, 10000)).status != P4_OK) std::_Exit(3);
        flatsql::ps::sleepNs(300000000);  // the maintenance thread flushes past 5,000 entries
        if (put(pnmBatch("peerA", "b1", 50000, 10000)).status != P4_OK) std::_Exit(4);
        _exit(0);  // crash: no stop, no final flush
    });
    REQUIRE(ok, "child stored 20,000 records");
    REQUIRE(openEngine(root, o) == P4_OK, "reopen");
    REQUIRE(registerType(pnm()) == P4_OK, "re-register (identical: a no-op)");
    Result s = summary1("PNM");
    if (s.rows.size() == 1) CHECK_EQ(s.i(0, "records"), int64_t(20000), "count 20,000");
    uint8_t cid[36];
    cidOf(pnmFrame(50007), cid);
    Result g = get("PNM", {std::vector<uint8_t>(cid, cid + 36)});
    CHECK_EQ(g.rows.size(), size_t(1), "a record of the unflushed half is found");
    Result retry = put(pnmBatch("peerA", "b1", 50000, 10000));
    CHECK_EQ(retry.status, P4_OK, retry.err);
    size_t dups = 0;
    for (size_t i = 0; i < retry.rows.size(); i++) dups += retry.i(i, "action") == P4_ACT_DUP;
    CHECK_EQ(dups, size_t(10000), "the retried half is all DUP (no UNIQUE failure, nothing new)");
    int64_t maxBefore = 0;
    for (auto& f : partitionFiles(root, "PNM")) {
        sqlite3* db = nullptr;
        sqlite3_open_v2(f.c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
        sqlite3_stmt* q;
        sqlite3_prepare_v2(db, "SELECT max(seq) FROM r", -1, &q, nullptr);
        if (sqlite3_step(q) == SQLITE_ROW) maxBefore = std::max<int64_t>(maxBefore, sqlite3_column_int64(q, 0));
        sqlite3_finalize(q);
        sqlite3_close(db);
    }
    Result fresh = put(pnmBatch("peerA", "b2", 90000, 100));
    bool above = fresh.rows.size() == 100;
    for (size_t i = 0; i < fresh.rows.size(); i++) above = above && fresh.i(i, "seq") > maxBefore;
    CHECK(above, "new seqs are above every seq on disk");
    TlvW rb;
    rb.u32(63, 8);
    Result v = call(P4_OPC_REBUILD, rb.b);
    if (v.rows.size() == 1) CHECK_EQ(v.i(0, "mismatches"), int64_t(0), "verify after the crash");
    closeEngine();
}

#endif

// ---- the kill loop -------------------------------------------------------------------------------
namespace {
// The kill loop's type: PNM, one file per partition.
TestType& killType() {
    static TestType t = pnmLikeType("PNM");
    return t;
}

// A repeat of earlier ids is the same bytes.
std::vector<uint8_t> killFrame(uint64_t id) {
    return buildFrame(killType(), {Field::str("FILE_ID", "file-" + std::to_string(id)), Field::str("NAME", "n" + std::to_string(id)),
                                   Field::raw("BODY", std::vector<uint8_t>(160, uint8_t(id)))});
}

Batch killBatch(const std::string& peer, const std::string& batch, uint64_t from, int n) {
    Batch b = pnmBatch(peer, batch, from, 0);
    for (int i = 0; i < n; i++) {
        In in;
        in.frame = killFrame(from + uint64_t(i));
        in.ts = 1790000000;
        b.recs.push_back(std::move(in));
    }
    return b;
}

// One step of the workload the kill lands in: 3 producers; every 5th call
// repeats earlier records under another producer (copies); every 13th call
// supersedes a batch and every 11th deletes the oldest arrivals down to a
// 4 MiB quota.
void killWorkStep(int c, uint64_t* id) {
    Batch b = killBatch("producer" + std::to_string(c % 3), "b" + std::to_string(c / 7), *id, 500);
    if (c % 5 == 4)
        for (int i = 0; i < 500; i++) b.recs[size_t(i)].frame = killFrame(*id - 1000 + uint64_t(i));
    *id += 500;
    Result r = put(b);
    if (r.status != P4_OK) {
        std::fprintf(stderr, "  put failed: %d %s\n", r.status, r.err.c_str());
        std::_Exit(1);
    }
    if (c % 13 == 12) {
        TlvW s;
        s.text(1, "PNM").text(11, "prov").text(12, "src").text(60, "b" + std::to_string(c / 7)).u8(61, 1);
        call(P4_OPC_SUPERSEDE, s.b);
    }
    if (c % 11 == 10) {
        TlvW q;
        q.u64(62, 4ull << 20);
        call(P4_OPC_QUOTA_GC, q.b);
    }
}
}  // namespace

// t_kill_run ingests until killed (killWorkStep; a small flush threshold so
// flushes and journal cleanup happen often); t_kill_check reopens and checks.
P4_SLOW_TEST(t_kill_run) {
    const std::string root = argStr("store", "");
    REQUIRE(!root.empty(), "--store");
    EngineOpts o;
    o.flushEntries = 5000;
    o.writers = 3;
    REQUIRE(openEngine(root, o) == P4_OK, "open");
    REQUIRE(registerType(killType()) == P4_OK, "register");
    uint64_t id = uint64_t(argInt("base", 1)) * 100000000ull;
    for (int c = 0;; c++) killWorkStep(c, &id);
}

namespace {
bool killCheck(const std::string& root, std::string* why) {
    EngineOpts o;
    o.flushEntries = 5000;
    if (openEngine(root, o) != P4_OK) {
        *why = "reopen failed";
        return false;
    }
    if (registerType(killType()) != P4_OK) {
        *why = "register failed";
        closeEngine();
        return false;
    }
    std::unordered_set<std::string> distinct;
    int64_t rows = 0, maxSeq = 0, missing = 0;
    std::vector<std::vector<uint8_t>> sample;
    for (auto& f : partitionFiles(root, "PNM")) {
        if (integrity(f) != "ok") {
            *why = "integrity_check " + f;
            closeEngine();
            return false;
        }
        sqlite3* db = nullptr;
        sqlite3_open_v2(f.c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
        sqlite3_stmt* q;
        sqlite3_prepare_v2(db, "SELECT seq, cid FROM r", -1, &q, nullptr);
        while (sqlite3_step(q) == SQLITE_ROW) {
            rows++;
            maxSeq = std::max<int64_t>(maxSeq, sqlite3_column_int64(q, 0));
            const std::string key(static_cast<const char*>(sqlite3_column_blob(q, 1)), 32);
            if (distinct.insert(key).second) {
                uint8_t d[32], c[36] = {0x01, 0x55, 0x12, 0x20};
                fp::cidDigestFromKey(reinterpret_cast<const uint8_t*>(key.data()), d);
                std::memcpy(c + 4, d, 32);
                sample.push_back(std::vector<uint8_t>(c, c + 36));
            }
        }
        sqlite3_finalize(q);
        sqlite3_close(db);
    }
    // every row on disk is found by CID
    for (size_t i = 0; i < sample.size(); i += 512) {
        std::vector<std::vector<uint8_t>> part(sample.begin() + long(i), sample.begin() + long(std::min(sample.size(), i + 512)));
        Result g = get("PNM", part, false);
        missing += int64_t(part.size()) - int64_t(g.rows.size());
    }
    Result s = summary1("PNM");
    const int64_t count = s.rows.size() == 1 ? s.i(0, "records") : (distinct.empty() ? 0 : -1);
    TlvW rb;
    rb.u32(63, 8);
    Result v = call(P4_OPC_REBUILD, rb.b);
    const int64_t mism = v.rows.size() == 1 ? v.i(0, "mismatches") : -1;
    if (mism < 0) std::fprintf(stderr, "  check: REBUILD 8 failed: %d %s\n", v.status, v.err.c_str());
    static uint64_t nextFresh = 9000000000ull;
    nextFresh += 1000;
    Result fresh = put(killBatch("producer0", "after", nextFresh + flatsql::ps::monoNs() % 1000000000ull * 1000, 100));
    bool above = fresh.status == P4_OK && fresh.rows.size() == 100;
    if (fresh.status != P4_OK) std::fprintf(stderr, "  check: fresh PUT failed: %d %s\n", fresh.status, fresh.err.c_str());
    for (size_t i = 0; i < fresh.rows.size(); i++) above = above && fresh.i(i, "seq") > maxSeq;
    closeEngine();
    // A closed engine leaves no file open: no VFS node (a forked child would
    // inherit its locks and WAL index).
    const int64_t nodesLeft = flatsql::flatSqlIoVfsStats().nodes;
    if (nodesLeft) std::fprintf(stderr, "  check: %lld VFS nodes left open after close\n", (long long)nodesLeft);
    char buf[256];
    std::snprintf(buf, sizeof buf, "rows %lld distinct %zu count %lld missing %lld mismatches %lld above %d nodes %lld",
                  (long long)rows, distinct.size(), (long long)count, (long long)missing, (long long)mism, int(above),
                  (long long)nodesLeft);
    *why = buf;
    return missing == 0 && count == int64_t(distinct.size()) && mism == 0 && above && nodesLeft == 0;
}
}  // namespace

P4_SLOW_TEST(t_kill_check) {
    const std::string root = argStr("store", "");
    REQUIRE(!root.empty(), "--store");
    std::string why;
    const bool ok = killCheck(root, &why);
    std::printf("  %s %s\n", ok ? "PASS" : "FAIL", why.c_str());
    CHECK(ok, why);
}

#if !defined(__wasm__)
// The native loop: kill -9 an ingesting engine at a random point, check, repeat.
P4_SLOW_TEST(t_kill) {
    const int rounds = int(argInt("rounds", 1000));
    const std::string root = scratchDir("kill") + "/fsql4";
    std::mt19937 rng(uint32_t(argInt("seed", 1)));
    int pass = 0, fail = 0;
    for (int round = 1; round <= rounds; round++) {
        std::fflush(stdout);
        const pid_t pid = fork();
        if (pid == 0) {
            EngineOpts o;
            o.flushEntries = 5000;
            o.writers = 3;
            if (openEngine(root, o) != P4_OK || registerType(killType()) != P4_OK) {
                std::fprintf(stderr, "  child: open failed\n");
                _exit(2);
            }
            uint64_t id = uint64_t(round) * 100000000ull;
            for (int c = 0;; c++) killWorkStep(c, &id);
        }
        flatsql::ps::sleepNs(uint64_t(100 + rng() % 900) * 1000000ull + uint64_t(rng() % 3) * 1000000000ull / 4);
        kill(pid, SIGKILL);
        int st = 0;
        waitpid(pid, &st, 0);
        std::string why;
        if (killCheck(root, &why)) {
            pass++;
        } else {
            fail++;
            std::printf("  round %d FAIL: %s\n", round, why.c_str());
            if (argInt("stop-on-fail", 0)) {
                std::printf("  store kept: %s\n", root.c_str());
                CHECK_EQ(fail, 0, "every round");
                return;
            }
        }
        if (round % 50 == 0) std::printf("  t_kill: %d rounds, %d pass, %d fail (load %.1f)\n", round, pass, fail, loadAvg());
    }
    std::printf("  t_kill: %d pass, %d fail of %d rounds\n", pass, fail, rounds);
    CHECK_EQ(fail, 0, "every round");
    removeTree(root);
}
#endif
