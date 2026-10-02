// The crash proof (C-33: end to end, few): t_kill, a kill -9 loop. A forked
// engine ingests (three producers, three feeds: two source feeds and the
// local file, the same records in more than one feed, copies, batch
// supersede, quota) until it is killed at a random point; the parent reopens
// the store and checks it: integrity_check on every file, one seq per CID
// across the feed files, every row found by CID, the record count, REBUILD 8
// (the type index against the files) and seqs above the old ones.
// The loop's two halves (t_kill_run, t_kill_check) are also tests of their
// own, so the wasm build's host drives the same loop (scripts/p4-wasm-suite.mjs).
#include <unistd.h>
#if !defined(__wasm__)
#include <signal.h>
#include <sys/wait.h>
#endif

#include <cstring>
#include <filesystem>
#include <map>
#include <random>
#include <unordered_set>

#include "flatsql/flatsql_io.h"
#include "internal.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;

namespace {

// A read-only connection of the check's own, beside the engine's: through
// FlatSQL's VFS with share=1, as every format-4 connection (it attaches to
// the path's node: the engine's locks and WAL index). Without share=1 a
// connection gets a private node (every lock granted, its own WAL index),
// believes it is the file's last connection at close and may delete the WAL
// under the engine's connections.
int openSide(const std::string& path, sqlite3** db) {
    const std::string uri = "file:" + path + "?share=1";
    return sqlite3_open_v2(uri.c_str(), db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, flatsql::kFlatSqlVfsName);
}

std::string integrity(const std::string& path) {
    sqlite3* db = nullptr;
    if (openSide(path, &db) != SQLITE_OK) {
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

}  // namespace

namespace {
// The kill loop's type: PNM with full text (catching up and the rows of
// superseded and evicted records deleted, in the background).
TestType& killType() {
    static TestType t = [] {
        TestType x = pnmLikeType("PNM");
        x.fullText = true;
        return x;
    }();
    return t;
}

// A repeat of earlier ids is the same bytes.
std::vector<uint8_t> killFrame(uint64_t id) {
    return buildFrame(killType(), {Field::str("FILE_ID", "file-" + std::to_string(id)), Field::str("NAME", "n" + std::to_string(id)),
                                   Field::raw("BODY", std::vector<uint8_t>(160, uint8_t(id)))});
}

// The feeds a call writes to (C-37): 0 prov@src, 1 prov@src2, 2 the local
// file (no tag).
Batch killBatch(const std::string& peer, const std::string& batch, uint64_t from, int n, int feed = 0) {
    Batch b;
    b.type = "PNM";
    b.peer = peer;
    if (feed == 0) b.tags.push_back(Tag{"prov", "src", "", batch, "", "", ""});
    if (feed == 1) b.tags.push_back(Tag{"prov", "src2", "", batch, "", "", ""});
    b.at = 1790000000;
    for (int i = 0; i < n; i++) {
        In in;
        in.frame = killFrame(from + uint64_t(i));
        in.ts = 1790000000;
        b.recs.push_back(std::move(in));
    }
    return b;
}

// One step of the workload the kill lands in: 3 producers, call c to feed
// c % 3; every 5th call repeats earlier records under another producer and
// often another feed (copies; the same record in two feed files); every 4th
// call (from the second) sends the same new records from all three
// producers at once, each to its own feed (concurrent copies across feed
// files: one seq per CID, §3.8.2; 50-record calls); every 13th call
// supersedes a batch of prov@src and every 11th deletes the oldest arrivals
// down to a 4 MiB quota.
void killWorkStep(int c, uint64_t* id) {
    Batch b = killBatch("producer" + std::to_string(c % 3), "b" + std::to_string(c / 7), *id, 500, c % 3);
    if (c % 5 == 4)
        for (int i = 0; i < 500; i++) b.recs[size_t(i)].frame = killFrame(*id - 1000 + uint64_t(i));
    *id += 500;
    Result r;
    if (c % 4 == 1) {
        // The same records from all three producers at once, in 50-record calls.
        std::vector<uint32_t> slots;
        for (int at = 0; at < 500; at += 50)
            for (int pi = 0; pi < 3; pi++) {
                Batch part = killBatch("producer" + std::to_string(pi), "b" + std::to_string(c / 7), 0, 0, pi);
                part.recs.assign(b.recs.begin() + at, b.recs.begin() + at + 50);
                slots.push_back(submit(P4_OPC_PUT, encodePut(part)));
            }
        for (uint32_t sl : slots) {
            Result x = wait(sl);
            if (r.status == P4_OK && x.status != P4_OK) r = x;
        }
    } else {
        r = put(b);
    }
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
    std::map<std::string, int64_t> diskSeq;  // one seq per CID in every feed file
    int64_t rows = 0, maxSeq = 0, missing = 0, diskSplit = 0;
    size_t files = 0;
    std::vector<std::vector<uint8_t>> sample;
    for (auto& f : partitionFiles(root, "PNM")) {
        if (integrity(f) != "ok") {
            *why = "integrity_check " + f;
            closeEngine();
            return false;
        }
        files++;
        sqlite3* db = nullptr;
        openSide(f, &db);
        sqlite3_stmt* q;
        sqlite3_prepare_v2(db, "SELECT seq, cid FROM r", -1, &q, nullptr);
        while (sqlite3_step(q) == SQLITE_ROW) {
            rows++;
            maxSeq = std::max<int64_t>(maxSeq, sqlite3_column_int64(q, 0));
            const std::string key(static_cast<const char*>(sqlite3_column_blob(q, 1)), 32);
            const auto ds = diskSeq.emplace(key, sqlite3_column_int64(q, 0));
            if (ds.first->second != sqlite3_column_int64(q, 0)) diskSplit++;
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
    // every row on disk is found by CID, and every copy of a CID has its one seq
    int64_t split = 0;
    for (size_t i = 0; i < sample.size(); i += 512) {
        std::vector<std::vector<uint8_t>> part(sample.begin() + long(i), sample.begin() + long(std::min(sample.size(), i + 512)));
        Result g = get("PNM", part, false, true);
        std::map<std::string, int64_t> seqOf;
        for (size_t k = 0; k < g.rows.size(); k++) {
            auto it = seqOf.emplace(g.s(k, "cid"), g.i(k, "seq")).first;
            if (it->second != g.i(k, "seq")) split++;
        }
        missing += int64_t(part.size()) - int64_t(seqOf.size());
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
    std::snprintf(buf, sizeof buf,
                  "files %zu rows %lld distinct %zu count %lld missing %lld split %lld disk-split %lld mismatches %lld above %d nodes %lld",
                  files, (long long)rows, distinct.size(), (long long)count, (long long)missing, (long long)split, (long long)diskSplit,
                  (long long)mism, int(above), (long long)nodesLeft);
    *why = buf;
    return missing == 0 && split == 0 && diskSplit == 0 && count == int64_t(distinct.size()) && mism == 0 && above && nodesLeft == 0;
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
            if (round == 1 || round == rounds) std::printf("  round %d: %s\n", round, why.c_str());
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
