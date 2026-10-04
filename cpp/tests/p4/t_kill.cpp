// The crash proof (C-33: end to end, few): t_kill, a kill -9 loop. A forked
// engine ingests (three producers, three feeds: two source feeds and the
// local file, the same records in more than one feed, copies, batch
// supersede, quota) until it is killed at a random point; the parent reopens
// the store and checks it: integrity_check on every index file, one seq per
// CID in each feed file (C-38: a record of two feeds is a row set in each,
// with its own seq), no CID both in local and in a feed file (an untagged
// write of a held record is a copy in its feeds; a tagged write takes a local
// record into its feed), every CID found by GET, the record count (once per
// feed), REBUILD 8 (each file's counters against the rows, every row's frame
// in its stream), the bytes GET returns (each record's CID recomputed from
// them) and seqs above the old ones. Every stream (BRIEF4) is
// walked from byte 0 to its end with no FlatSQL code: back-to-back
// [u32 LE size][FlatBuffer] frames, each a PNM table with its file
// identifier, ending exactly at the end of the file, and every index row
// names one of its frames (off, len). Every 4th step also sends four calls
// at once, each to a feed of its own, new to the store, so kills land between
// new feeds' first frames and their first commits: no row of a source feed
// may come back without its batch (an unacknowledged record is absent or
// present with its tag, never a record with blank provenance;
// GATES-stream-r1 B1).
// The loop's two halves (t_kill_run, t_kill_check) are also tests of their
// own, so the wasm build's host drives the same loop (scripts/p4-wasm-suite.mjs).
// t_kill_merge kills inside the indexer's merges of staged rows (BRIEF4
// ruling (B)): the kill follows the start of a merge transaction, and every
// call acknowledged before it must be there with its batch; the same checks
// as t_kill run on the store.
#include <unistd.h>
#if !defined(__wasm__)
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#endif

#include <cstring>
#include <filesystem>
#include <map>
#include <random>
#include <set>
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
// The URI names exactly `path` (feed file names carry %HH escapes; SQLite
// decodes %HH in a URI path and ends it at '?' or '#').
int openSide(const std::string& path, sqlite3** db) {
    std::string uri = "file://";
    for (char c : path) uri += c == '%' ? std::string("%25") : c == '?' ? std::string("%3F") : c == '#' ? std::string("%23") : std::string(1, c);
    uri += "?share=1";
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

// A stream walked as a stock reader walks it: [u32 LE size][FlatBuffer]
// frames from byte 0, each buffer's root offset inside it and its file
// identifier "$PNM", the last frame ending exactly at the end of the file.
// *frames: off -> len. "" when it walks clean, else why not.
std::string walkStream(const std::string& path, std::map<int64_t, int64_t>* frames) {
    frames->clear();
    FILE* fp = std::fopen(path.c_str(), "rb");
    if (!fp) return "absent";
    std::vector<uint8_t> b;
    uint8_t chunk[65536];
    size_t n;
    while ((n = std::fread(chunk, 1, sizeof chunk, fp)) > 0) b.insert(b.end(), chunk, chunk + n);
    std::fclose(fp);
    size_t at = 0;
    while (at < b.size()) {
        if (b.size() - at < 4) return "a torn size prefix at " + std::to_string(at);
        const uint32_t len = fp::ld32(&b[at]);
        if (b.size() - at - 4 < len) return "a frame past the end at " + std::to_string(at);
        const uint8_t* fb = &b[at + 4];
        if (len < 8 || fp::ld32(fb) + 4 > len || std::memcmp(fb + 4, "$PNM", 4) != 0)
            return "not a $PNM FlatBuffer at " + std::to_string(at);
        (*frames)[int64_t(at)] = len;
        at += 4 + len;
    }
    return "";
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

// The second source feed's source needs escaping in a file name: a space,
// '/', "/../", '%' before hex digits, '?', '#' and non-ASCII (C-37 (1): the
// file name is a safe encoding of the feed). Its file is exactly
// kOddFeedFile, flat in P/<TYPE>.
const char* const kOddSource = "s rc/../2 %41?#\xC3\xA9";
const char* const kOddFeedFile = "prov@s%20rc%2F..%2F2%20%2541%3F%23%C3%A9.db";

// A feed's file name without its kind: "<feed>.db" (and its -wal, -shm,
// -journal) and "<feed>.fsdata" / "<feed>.<generation>.fsdata" give "<feed>";
// anything else gives "".
std::string feedOfFile(std::string n) {
    for (const char* sfx : {"-wal", "-shm", "-journal"}) {
        const size_t k = std::strlen(sfx);
        if (n.size() > k && n.compare(n.size() - k, k, sfx) == 0) n.resize(n.size() - k);
    }
    if (n.size() > 3 && n.compare(n.size() - 3, 3, ".db") == 0) return n.substr(0, n.size() - 3);
    if (n.size() > 7 && n.compare(n.size() - 7, 7, ".fsdata") == 0) {
        n.resize(n.size() - 7);
        const size_t dot = n.rfind('.');
        if (dot != std::string::npos && dot + 1 < n.size() && n.find_first_not_of("0123456789", dot + 1) == std::string::npos)
            n.resize(dot);
        return n;
    }
    return "";
}

// What is in P/<type> other than the feeds the workload writes (prov@src,
// the odd feed, local, the new feeds prov@fresh-<id>): their index files with
// -wal/-shm/-journal and their streams. A directory, or another file. Empty
// when the layout is right.
std::string strayEntries(const std::string& root, const std::string& type) {
    std::string out;
    std::error_code ec;
    const std::string odd = std::string(kOddFeedFile).substr(0, std::strlen(kOddFeedFile) - 3);
    for (auto& d : std::filesystem::directory_iterator(root + "/P/" + type, ec)) {
        const std::string n = feedOfFile(d.path().filename().string());
        const bool fresh = n.rfind("prov@fresh-", 0) == 0 && n.size() > 11 && n.find_first_not_of("0123456789", 11) == std::string::npos;
        if (d.is_directory() || (n != "prov@src" && n != "local" && n != odd && !fresh)) out += " " + d.path().filename().string();
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

// The feeds a call writes to (C-37, C-38): 0 prov@src, 1 prov@<kOddSource>,
// 2 the local file (no tag), 3 a feed of its own, prov@fresh-<from>.
Batch killBatch(const std::string& peer, const std::string& batch, uint64_t from, int n, int feed = 0) {
    Batch b;
    b.type = "PNM";
    b.peer = peer;
    if (feed == 0) b.tags.push_back(Tag{"prov", "src", "", batch, "", "", ""});
    if (feed == 1) b.tags.push_back(Tag{"prov", kOddSource, "", batch, "", "", ""});
    if (feed == 3) b.tags.push_back(Tag{"prov", "fresh-" + std::to_string(from), "", batch, "", "", ""});
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
// often another feed (copies; the same record in two feed files, a row set
// in each); every 4th call (from the second) sends the same new records from
// all three producers at once, each to its own feed (50-record calls);
// every 13th call supersedes a batch of prov@src and every 11th deletes the
// oldest arrivals down to a 4 MiB quota; every 4th first sends four calls at
// once, each with 64 new records to a feed new to the store.
void killWorkStep(int c, uint64_t* id) {
    if (c % 4 == 3) {
        std::vector<uint32_t> slots;
        for (int k = 0; k < 4; k++) {
            slots.push_back(submit(P4_OPC_PUT, encodePut(killBatch("producer1", "f" + std::to_string(*id), *id, 64, 3))));
            *id += 64;
        }
        for (uint32_t sl : slots) {
            Result r = wait(sl);
            if (r.status != P4_OK) {
                std::fprintf(stderr, "  put failed: %d %s\n", r.status, r.err.c_str());
                std::_Exit(1);
            }
        }
    }
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
    std::unordered_set<std::string> distinct;   // CIDs over every feed file
    std::map<std::string, int64_t> diskSeq;      // (file, CID) -> its one seq in that file
    std::map<std::pair<std::string, int64_t>, std::string> seqCid;  // (file, seq) -> its one CID
    std::unordered_set<std::string> localCids, feedCids;  // a record is local only while no feed holds it
    int64_t rows = 0, maxSeq = 0, missing = 0, diskSplit = 0, frames = 0, unframed = 0, unbatched = 0;
    size_t files = 0;
    std::vector<std::vector<uint8_t>> sample;
    for (auto& f : partitionFiles(root, "PNM")) {
        if (integrity(f) != "ok") {
            *why = "integrity_check " + f;
            closeEngine();
            return false;
        }
        files++;
        // The feed's stream, as a stock reader walks it (the generation the
        // index's meta names).
        sqlite3* db = nullptr;
        openSide(f, &db);
        sqlite3_stmt* q;
        int64_t gen = 0, mark = 0;
        sqlite3_prepare_v2(db, "SELECT k, v FROM meta WHERE k IN ('gen', 'mark')", -1, &q, nullptr);
        while (sqlite3_step(q) == SQLITE_ROW)
            (std::strcmp(reinterpret_cast<const char*>(sqlite3_column_text(q, 0)), "gen") == 0 ? gen : mark) = sqlite3_column_int64(q, 1);
        sqlite3_finalize(q);
        const std::string base = f.substr(0, f.size() - 3);
        const std::string sp = gen == 0 ? base + ".fsdata" : base + "." + std::to_string(gen) + ".fsdata";
        std::map<int64_t, int64_t> fr;
        std::string walk = mark == 0 && !std::filesystem::exists(sp) ? std::string() : walkStream(sp, &fr);
        std::error_code sec;
        if (walk.empty() && mark != 0 && int64_t(std::filesystem::file_size(sp, sec)) != mark) walk = "size != mark";
        if (!walk.empty()) {
            *why = "stream " + sp + ": " + walk;
            sqlite3_close(db);
            closeEngine();
            return false;
        }
        frames += int64_t(fr.size());
        sqlite3_prepare_v2(db, "SELECT off, len FROM r", -1, &q, nullptr);
        while (sqlite3_step(q) == SQLITE_ROW) {
            auto it = fr.find(sqlite3_column_int64(q, 0));
            if (it == fr.end() || it->second != sqlite3_column_int64(q, 1)) unframed++;
        }
        sqlite3_finalize(q);
        // Every row of a source feed carries its batch: the workload tags
        // every call to a feed with a non-empty batch.
        const bool isLocal = f.size() >= 9 && f.compare(f.size() - 9, 9, "/local.db") == 0;
        if (!isLocal) {
            sqlite3_prepare_v2(db, "SELECT count(*) FROM r LEFT JOIN batch ON batch.id = r.b WHERE coalesce(batch.batch, '') = ''", -1, &q,
                               nullptr);
            if (sqlite3_step(q) == SQLITE_ROW) unbatched += sqlite3_column_int64(q, 0);
            sqlite3_finalize(q);
        }
        sqlite3_prepare_v2(db, "SELECT seq, cid FROM r", -1, &q, nullptr);
        while (sqlite3_step(q) == SQLITE_ROW) {
            rows++;
            const int64_t seq = sqlite3_column_int64(q, 0);
            maxSeq = std::max<int64_t>(maxSeq, seq);
            const std::string key(static_cast<const char*>(sqlite3_column_blob(q, 1)), 32);
            const auto ds = diskSeq.emplace(f + "|" + key, seq);
            if (ds.first->second != seq) diskSplit++;
            const auto sc = seqCid.emplace(std::make_pair(f, seq), key);
            if (sc.first->second != key) diskSplit++;
            (isLocal ? localCids : feedCids).insert(key);
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
    int64_t localAndFeed = 0;
    for (const std::string& k : localCids) localAndFeed += feedCids.count(k);
    // P/PNM holds the three feed files (and sidecars) only, flat: the odd
    // source's file is its escaped name, not a decoded path.
    const std::string stray = strayEntries(root, "PNM");
    // every CID on disk is found by GET (its first feed file's row set: one
    // seq), with the bytes it arrived with (its CID recomputed from them)
    int64_t split = 0, badBytes = 0;
    for (size_t i = 0; i < sample.size(); i += 512) {
        std::vector<std::vector<uint8_t>> part(sample.begin() + long(i), sample.begin() + long(std::min(sample.size(), i + 512)));
        Result g = get("PNM", part, true, true);
        std::map<std::string, int64_t> seqOf;
        for (size_t k = 0; k < g.rows.size(); k++) {
            auto it = seqOf.emplace(g.s(k, "cid"), g.i(k, "seq")).first;
            if (it->second != g.i(k, "seq")) split++;
            const std::string cid = g.s(k, "cid"), data = g.s(k, "data");
            uint8_t want[32], got[32];
            flatsql::ps::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size(), got);
            if (!fp::cidDigestFromText(cid.data(), cid.size(), want) || std::memcmp(want, got, 32) != 0) badBytes++;
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
    char buf[384];
    std::snprintf(buf, sizeof buf,
                  "files %zu rows %lld frames %lld unframed %lld unbatched %lld cids %zu records %zu local %zu local+feed %lld count %lld"
                  " missing %lld bad-bytes %lld split %lld disk-split %lld mismatches %lld above %d nodes %lld",
                  files, (long long)rows, (long long)frames, (long long)unframed, (long long)unbatched, distinct.size(), diskSeq.size(),
                  localCids.size(),
                  (long long)localAndFeed, (long long)count, (long long)missing, (long long)badBytes, (long long)split, (long long)diskSplit,
                  (long long)mism, int(above), (long long)nodesLeft);
    *why = buf;
    if (!stray.empty()) *why += "; stray entries in P/PNM:" + stray;
    return stray.empty() && missing == 0 && split == 0 && diskSplit == 0 && localAndFeed == 0 && count == int64_t(diskSeq.size()) && mism == 0 &&
           above && nodesLeft == 0 && unframed == 0 && unbatched == 0 && badBytes == 0;
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

#if !defined(__wasm__)
namespace {
// Every acknowledged call of a t_kill_merge child is there: each record (GET
// finds its CID) and, for a source feed's call, its delivery (TAGS lists
// the feed's source with the call's batch).
struct Acked {
    int c = 0, n = 0, feed = 0;
    uint64_t from = 0;
    std::string batch;
};
bool ackedCheck(const std::string& root, const std::vector<Acked>& acked, std::string* why) {
    EngineOpts o;
    if (openEngine(root, o) != P4_OK || registerType(killType()) != P4_OK) {
        *why = "reopen failed";
        closeEngine();
        return false;
    }
    int64_t lost = 0, untagged = 0, checked = 0;
    for (const Acked& a : acked) {
        std::vector<std::vector<uint8_t>> cids;
        for (int i = 0; i < a.n; i++) {
            uint8_t c[36];
            cidOf(killFrame(a.from + uint64_t(i)), c);
            cids.push_back(std::vector<uint8_t>(c, c + 36));
        }
        Result g = get("PNM", cids, false);
        std::set<std::string> have;
        for (size_t k = 0; k < g.rows.size(); k++) have.insert(g.s(k, "cid"));
        for (auto& c : cids) lost += have.count(cidText(c.data())) ? 0 : 1;
        checked += a.n;
        if (a.feed == 2) continue;
        TlvW tw;
        tw.text(1, "PNM");
        std::vector<uint8_t> cl(4);
        fp::st32(cl.data(), uint32_t(cids.size()));
        for (auto& c : cids) cl.insert(cl.end(), c.begin(), c.end());
        tw.raw(40, cl.data(), cl.size());
        Result tg = call(P4_OPC_TAGS, tw.b);
        std::set<std::string> tagged;
        const std::string src = a.feed == 0 ? "src" : kOddSource;
        for (size_t k = 0; k < tg.rows.size(); k++)
            if (tg.s(k, "source") == src && tg.s(k, "batch") == a.batch) tagged.insert(tg.s(k, "cid"));
        for (auto& c : cids) untagged += tagged.count(cidText(c.data())) ? 0 : 1;
    }
    closeEngine();
    char buf[160];
    std::snprintf(buf, sizeof buf, "acked calls %zu records %lld lost %lld untagged %lld", acked.size(), (long long)checked, (long long)lost,
                  (long long)untagged);
    *why = buf;
    return lost == 0 && untagged == 0;
}
}  // namespace

// The kill -9 loop timed into merges: the child ingests with a small index
// flush threshold (a merge per 2,000 staged rows of a feed) and logs each
// merge's start and end (P4_MERGE_DEBUG) and each acknowledged call; the
// parent kills it 0-4 ms after the k-th merge start, then checks the store
// (t_kill's checks) and every acknowledged call.
P4_SLOW_TEST(t_kill_merge) {
    const int rounds = int(argInt("rounds", 100));
    const std::string root = scratchDir("killmerge") + "/fsql4";
    std::mt19937 rng(uint32_t(argInt("seed", 1)));
    int pass = 0, fail = 0, inMerge = 0;
    std::vector<Acked> acked;  // over every round: the store keeps them all
    for (int round = 1; round <= rounds; round++) {
        std::fflush(stdout);
        std::fflush(stderr);
        int po[2], pe[2];
        if (pipe(po) != 0 || pipe(pe) != 0) {
            CHECK(false, "pipe");
            return;
        }
        const pid_t pid = fork();
        if (pid == 0) {
            setenv("P4_MERGE_DEBUG", "1", 1);
            dup2(po[1], 1);
            dup2(pe[1], 2);
            close(po[0]);
            close(pe[0]);
            EngineOpts o;
            o.flushEntries = 2000;
            o.writers = 2;
            if (openEngine(root, o) != P4_OK || registerType(killType()) != P4_OK) _exit(2);
            uint64_t id = uint64_t(round) * 100000000ull;
            for (int c = 0;; c++) {
                const int feed = c % 3;
                const std::string batch = "m" + std::to_string(round) + "-" + std::to_string(c);
                // every 5th call delivers 400 earlier records again under the
                // other producer: copies and new instances of staged and
                // merged records
                const uint64_t from = c % 5 == 4 ? id - 1600 : id;
                Batch b = killBatch("producer" + std::to_string(c % 2), batch, from, 400, feed);
                Result r = put(b);
                if (r.status != P4_OK) _exit(1);
                std::printf("ACK %d %llu %d %d %s\n", c, (unsigned long long)from, 400, feed, batch.c_str());
                std::fflush(stdout);
                if (c % 5 != 4) id += 400;
            }
        }
        close(po[1]);
        close(pe[1]);
        const int want = 1 + int(rng() % 12);
        const uint64_t delayUs = rng() % 4000;
        int begins = 0;
        bool killed = false;
        std::string outBuf, errBuf;
        std::map<std::string, int> open;  // merges begun and not ended, by file
        std::vector<Acked> got;
        auto lines = [&](std::string& buf, bool isErr) {
            size_t nl;
            while ((nl = buf.find('\n')) != std::string::npos) {
                const std::string line = buf.substr(0, nl);
                buf.erase(0, nl + 1);
                if (isErr) {
                    const bool b = line.rfind("merge-begin ", 0) == 0, e = line.rfind("merge-end ", 0) == 0;
                    if (b || e) {
                        const std::string path = line.substr(line.find(' ') + 1, line.find(" rows ") - line.find(' ') - 1);
                        open[path] += b ? 1 : -1;
                        if (b) begins++;
                    }
                } else if (line.rfind("ACK ", 0) == 0) {
                    Acked a;
                    char bt[64] = {};
                    unsigned long long from = 0;
                    if (std::sscanf(line.c_str(), "ACK %d %llu %d %d %63s", &a.c, &from, &a.n, &a.feed, bt) == 5) {
                        a.from = from;
                        a.batch = bt;
                        got.push_back(a);
                    }
                }
            }
        };
        const uint64_t until = flatsql::ps::monoNs() + 30000000000ull;
        bool eofO = false, eofE = false;
        while (!eofO || !eofE) {
            if (!killed && (begins >= want || flatsql::ps::monoNs() > until)) {
                flatsql::ps::sleepNs(delayUs * 1000);
                kill(pid, SIGKILL);
                killed = true;
            }
            pollfd fds[2] = {{po[0], POLLIN, 0}, {pe[0], POLLIN, 0}};
            if (poll(fds, 2, 50) <= 0) continue;
            char buf[65536];
            for (int k = 0; k < 2; k++) {
                if (!(fds[k].revents & (POLLIN | POLLHUP))) continue;
                const ssize_t n = read(fds[k].fd, buf, sizeof buf);
                if (n <= 0) {
                    (k ? eofE : eofO) = true;
                    continue;
                }
                (k ? errBuf : outBuf).append(buf, size_t(n));
                lines(k ? errBuf : outBuf, k == 1);
                // the kill follows the k-th merge start as closely as the delay says
                if (!killed && begins >= want) break;
            }
        }
        close(po[0]);
        close(pe[0]);
        int st = 0;
        waitpid(pid, &st, 0);
        bool merging = false;
        for (auto& kv : open) merging = merging || kv.second > 0;
        if (merging) inMerge++;
        acked.insert(acked.end(), got.begin(), got.end());
        std::string why, why2;
        const bool ok1 = killCheck(root, &why);
        const bool ok2 = ackedCheck(root, acked, &why2);
        if (ok1 && ok2) {
            pass++;
            if (round == 1 || round == rounds)
                std::printf("  round %d: %s; %s; merges begun %d, killed inside one: %d\n", round, why.c_str(), why2.c_str(), begins,
                            int(merging));
        } else {
            fail++;
            std::printf("  round %d FAIL: %s; %s; merges begun %d, killed inside one: %d\n", round, why.c_str(), why2.c_str(), begins,
                        int(merging));
            if (argInt("stop-on-fail", 0)) {
                std::printf("  store kept: %s\n", root.c_str());
                CHECK_EQ(fail, 0, "every round");
                return;
            }
        }
        if (round % 25 == 0)
            std::printf("  t_kill_merge: %d rounds, %d pass, %d fail, %d killed inside a merge (load %.1f)\n", round, pass, fail, inMerge,
                        loadAvg());
    }
    std::printf("  t_kill_merge: %d pass, %d fail of %d rounds; %d killed inside a merge transaction\n", pass, fail, rounds, inMerge);
    CHECK_EQ(fail, 0, "every round");
    CHECK(inMerge > rounds / 4, "kills land inside merges");
    removeTree(root);
}
#endif
