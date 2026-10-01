// Store format 4 under power loss (flatsql_p4_fault_test only).
//
// The seven host I/O functions are served by FaultFs (cpp/test/ps/io_fault.h):
// every file keeps the image reads see and the image a crash leaves, and a
// crash applies one of five modes to the unsynced writes (all vanish, a
// random subset survives in order, the last write is torn, a random subset
// survives reordered, or everything survives: kill -9). The engine runs a
// workload until the device freezes at a random mutating call, is stopped
// (its I/O failing, as a dying machine's), the crash is applied, and the
// store is reopened and checked through the engine:
//   - every record whose PUT was acknowledged is there (C-4: the ack follows
//     the commit, so a power loss cannot take it);
//   - REBUILD 8 finds no mismatch (the type index against the files, and
//     PRAGMA integrity_check on every file, C-27);
//   - the count equals the distinct CIDs a full scan returns, and no VFS
//     node is left open after the close.
#include <algorithm>
#include <atomic>
#include <cstring>
#include <random>
#include <set>
#include <thread>

#include "flatsql/flatsql_io.h"
#include "flatsql/p4/p4_reader.h"
#include "flatsql/ps/platform.h"
#include "internal.h"
#include "p4/p4_test.h"
#include "ps/io_fault.h"

using namespace p4t;
namespace fp = flatsql::p4;
using flatsql::ps::test::FaultFs;

namespace flatsql {
namespace p4 {
const std::string& lastError();
}
}  // namespace flatsql

namespace {
FaultFs& faultFs() {
    static FaultFs* fs = new FaultFs(true);
    return *fs;
}
}  // namespace

extern "C" {
int32_t flatsql_io_open(const char* path, int32_t pathLen, int32_t flags) { return faultFs().open(path, pathLen, flags); }
int32_t flatsql_io_read(int32_t h, void* dst, int32_t len, double off) { return faultFs().read(h, dst, len, off); }
int32_t flatsql_io_write(int32_t h, const void* src, int32_t len, double off) { return faultFs().write(h, src, len, off); }
int32_t flatsql_io_truncate(int32_t h, double size) { return faultFs().truncate(h, size); }
int32_t flatsql_io_sync(int32_t h) { return faultFs().sync(h); }
double flatsql_io_size(int32_t h) { return faultFs().size(h); }
int32_t flatsql_io_close(int32_t h) { return faultFs().close(h); }
}

namespace {

TestType& faultType() {
    static TestType t = [] {
        TestType x = pnmLikeType("PNM");
        x.rules += "bucket str:NAME\n";
        return x;
    }();
    return t;
}

std::vector<uint8_t> faultFrame(uint64_t id) {
    char name[32];
    std::snprintf(name, sizeof name, "2026-%02d-15T00:00:00", 7 + int((id / 200) % 2));
    return buildFrame(faultType(), {Field::str("FILE_ID", "file-" + std::to_string(id)), Field::str("NAME", name),
                                    Field::raw("BODY", std::vector<uint8_t>(96, uint8_t(id)))});
}

std::string cidKeyOf(const std::vector<uint8_t>& frame) {
    uint8_t c[36];
    cidOf(frame, c);
    return std::string(reinterpret_cast<const char*>(c), 36);
}

struct Check {
    bool ok = true;
    std::string why;
    void fail(const std::string& w) {
        if (ok) why = w;
        ok = false;
    }
};

// The store after a crash, through the engine only (the files are in memory).
Check checkStore(const std::string& root, const std::vector<std::string>& acked) {
    Check ck;
    EngineOpts o;
    o.flushEntries = 2000;
    const int32_t orc = openEngine(root, o);
    if (orc != P4_OK) {
        ck.fail("reopen failed: " + std::to_string(orc) + " " + fp::lastError());
        return ck;
    }
    if (registerType(faultType()) != P4_OK) ck.fail("register failed");
    // every acknowledged record is there
    std::vector<std::vector<uint8_t>> want;
    for (const std::string& c : acked) want.emplace_back(c.begin(), c.end());
    size_t found = 0;
    for (size_t i = 0; i < want.size(); i += 512) {
        std::vector<std::vector<uint8_t>> part(want.begin() + long(i), want.begin() + long(std::min(want.size(), i + 512)));
        Result g = get("PNM", part, false);
        if (g.status != P4_OK) ck.fail("GET failed: " + g.err);
        found += g.rows.size();
    }
    if (found != want.size()) ck.fail("acknowledged records lost: " + std::to_string(want.size() - found));
    // count = distinct CIDs of a full scan
    std::set<std::string> distinct;
    int64_t after = 0;
    for (;;) {
        TlvW sc;
        sc.text(1, "PNM").u8(5, P4_ORDER_SEQ_ASC).i64(6, after).u64(3, 5000);
        Result r = call(P4_OPC_SCAN, sc.b);
        if (r.status != P4_OK) {
            ck.fail("SCAN failed: " + r.err);
            break;
        }
        for (size_t i = 0; i < r.rows.size(); i++) {
            distinct.insert(r.s(i, "cid"));
            after = std::max(after, r.i(i, "seq"));
        }
        if (r.rows.size() < 5000) break;
    }
    TlvW s1;
    s1.u8(45, 1).text(1, "PNM");
    Result s = call(P4_OPC_SUMMARY, s1.b);
    const int64_t count = s.rows.size() == 1 ? s.i(0, "records") : (distinct.empty() ? 0 : -1);
    if (count != int64_t(distinct.size()))
        ck.fail("count " + std::to_string(count) + " vs distinct " + std::to_string(distinct.size()));
    TlvW rb;
    rb.u32(63, 8);
    Result v = call(P4_OPC_REBUILD, rb.b);
    if (v.status != P4_OK || v.rows.size() != 1) ck.fail("REBUILD 8 failed: " + std::to_string(v.status) + " " + v.err);
    else if (v.i(0, "mismatches") != 0) ck.fail("mismatches " + std::to_string(v.i(0, "mismatches")) + " " + v.err);
    closeEngine();
    if (flatsql::flatSqlIoVfsStats().nodes != 0) ck.fail("VFS nodes left open");
    return ck;
}

}  // namespace

P4_SLOW_TEST(t_power_loss) {
    FaultFs& fs = faultFs();
    const int rounds = int(argInt("rounds", 200));
    // A fresh store every 50 rounds keeps each round's check (a full scan and
    // REBUILD 8) bounded.
    std::string root;
    std::mt19937_64 rng(uint64_t(argInt("seed", 1)));
    std::set<std::string> acked;
    uint64_t id = 1;
    int pass = 0, fail = 0;
    const char* modes[] = {"drop-all", "drop-subset", "tear-last", "reorder", "keep-all"};
    for (int round = 1; round <= rounds; round++) {
        if ((round - 1) % 50 == 0) {
            root = "/p4fault/s" + std::to_string((round - 1) / 50) + "/fsql4";
            acked.clear();
        }
        EngineOpts o;
        o.writers = 3;
        o.flushEntries = 2000;
        if (openEngine(root, o) != P4_OK || registerType(faultType()) != P4_OK) {
            std::printf("  round %d: open failed before the workload\n", round);
            fail++;
            break;
        }
        // The device freezes at a random mutating call ahead.
        fs.armCrashAtOp(fs.mutatingOps() + 50 + rng() % 4000);
        std::atomic<bool> stop{false};
        std::mutex mu;
        std::set<std::string> newAcks;
        std::vector<std::thread> producers;
        for (int pi = 0; pi < 3; pi++)
            producers.emplace_back([&, pi] {
                std::mt19937 r(uint32_t(round * 7 + pi));
                for (int c = 0; !stop.load() && !fs.frozen() && c < 400; c++) {
                    Batch b;
                    b.type = "PNM";
                    b.peer = "12D3KooWFault" + std::to_string(pi);
                    b.tags.push_back(Tag{"prov", "src", "", "b" + std::to_string(c % 5), "", "", ""});
                    b.at = 1790000000;
                    uint64_t base;
                    {
                        std::lock_guard<std::mutex> g(mu);
                        base = id;
                        id += 100;
                    }
                    for (int i = 0; i < 100; i++) {
                        In in;
                        // a fifth of the calls repeat earlier records (copies, retags)
                        const uint64_t rid = (c % 5 == 4 && base > 1000) ? base - 1 - (r() % 900) : base + uint64_t(i);
                        in.frame = faultFrame(rid);
                        in.ts = 1790000000;
                        b.recs.push_back(std::move(in));
                    }
                    Result res = put(b);
                    if (res.status != P4_OK) continue;
                    std::lock_guard<std::mutex> g(mu);
                    for (auto& in : b.recs) newAcks.insert(cidKeyOf(in.frame));
                }
            });
        for (int i = 0; i < 3000 && !fs.frozen(); i++) flatsql::ps::sleepNs(1000000);
        stop = true;
        for (auto& t : producers) t.join();
        closeEngine(10000);  // the device is frozen: the stop's I/O fails, as on a dying machine
        // This round's acknowledged records, all of them, and a sample of
        // the earlier rounds' (each was checked whole in its own round).
        std::vector<std::string> want(newAcks.begin(), newAcks.end());
        {
            std::vector<std::string> older(acked.begin(), acked.end());
            std::shuffle(older.begin(), older.end(), rng);
            if (older.size() > 5000) older.resize(5000);
            want.insert(want.end(), older.begin(), older.end());
        }
        acked.insert(newAcks.begin(), newAcks.end());
        const int mode = int(round % FaultFs::kModeCount);
        fs.crash(FaultFs::CrashMode(mode), rng());
        Check ck = checkStore(root, want);
        if (ck.ok) pass++;
        else {
            fail++;
            std::printf("  round %d (%s) FAIL: %s\n", round, modes[mode], ck.why.c_str());
            if (argInt("list", 0))
                for (const std::string& p : fs.list("/p4fault"))
                    std::printf("    %s %zu\n", p.c_str(), fs.contents(p).size());
            if (argInt("stop-on-fail", 0)) break;
        }
        if (round % 25 == 0)
            std::printf("  t_power_loss: %d rounds, %d pass, %d fail, %zu acknowledged records (load %.1f)\n", round, pass, fail,
                        acked.size(), loadAvg());
    }
    std::printf("  t_power_loss: %d pass, %d fail of %d rounds\n", pass, fail, pass + fail);
    CHECK_EQ(fail, 0, "every round");
}
