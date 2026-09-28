// Parity vectors (T4 #3, docs/PARTITION-STORE-WASM.md).
//
// The same workload runs natively, under the Node wasi-threads host and under
// the SDK WasmEdge C runner; scripts/ps-parity.mjs compares what they print.
//
//   parity_canonical_dump [--out=FILE] [--dir=D]
//     threaded (4 writers): the logical store, sorted by (pid, pseq) (every
//     row field except file offsets, which follow commit batching), head and
//     lane counters, and the results of partition- and type-level queries
//     run on reader lanes. Identical on every host.
//   parity_deterministic_bytes [--out=FILE] [--dir=D]
//     deterministic mode: one thread (cooperative pump), the injected clock
//     (setTestClock), explicit commit and seal points: the size and SHA-256 of
//     every file the store wrote. Byte-identical on every host.
//
// Each prints "PARITY <name> lines=<n> sha256=<hex>" and writes the full
// vector to --out. --dir runs on the host's files (the flatsql_io imports)
// instead of the in-memory host. PS_SLOW_TEST: they run only when named.
#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>

#include "flatsql/ps/flatsql_attr_generated.h"
#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"
#include "ps/ps_test.h"

using namespace pst;

namespace {

std::string hex(const uint8_t* p, size_t n) {
    static const char* d = "0123456789abcdef";
    std::string s(n * 2, '0');
    for (size_t i = 0; i < n; i++) {
        s[2 * i] = d[p[i] >> 4];
        s[2 * i + 1] = d[p[i] & 15];
    }
    return s;
}

std::string sha(const void* p, size_t n) {
    uint8_t h[32];
    sha256(p, n, h);
    return hex(h, 32);
}

std::string parityEpoch(uint64_t i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-08-%02dT%02d:%02d:%02dZ", int(1 + (i / 86400) % 28), int((i / 3600) % 24),
                  int((i / 60) % 60), int(i % 60));
    return b;
}

// The workload: 3 OMM partitions (resends, re-tags across sources), 2 MPE
// partitions and one CAT partition (supersedes); record ids are unique per
// partition and CIDs never repeat across partitions, so type-level FIRST
// copies do not depend on writer timing.
struct Plan {
    std::string producer;
    TestType* type;
    uint32_t count;
};
std::vector<Plan> plans() {
    return {{"par-omm-0", &ommType(), 2500}, {"par-omm-1", &ommType(), 2500}, {"par-omm-2", &ommType(), 1200},
            {"par-mpe-0", &mpeType(), 1800}, {"par-mpe-1", &mpeType(), 900},  {"par-cat-0", &catType(), 600}};
}

// Frames are built from exact values only: a fixture expression such as
// a + b * c may be contracted to a fused multiply-add natively (arm64) and not
// in wasm, which would change the input bytes, not the engine's output.
std::vector<uint8_t> parityOmm(uint32_t norad, const std::string& objectId, const std::string& epoch, uint32_t v) {
    std::vector<Field> f = {Field::str("OBJECT_NAME", "SAT-" + std::to_string(norad)),
                            Field::str("OBJECT_ID", objectId),
                            Field::u64("NORAD_CAT_ID", norad),
                            Field::str("EPOCH", epoch),
                            Field::str("CREATION_DATE", "2026-09-27T00:00:00"),
                            Field::f64("MEAN_MOTION", 14.0 + double(v % 64) / 64.0),
                            Field::f64("ECCENTRICITY", double(v % 128) / 1024.0),
                            Field::f64("INCLINATION", 51.5),
                            Field::f64("RA_OF_ASC_NODE", 120.5),
                            Field::f64("ARG_OF_PERICENTER", 90.25),
                            Field::f64("MEAN_ANOMALY", 270.125),
                            Field::f64("BSTAR", 0.0009765625),
                            Field::str("COMMENT", std::string(40, 'x'))};
    return buildRecord(ommType(), f);
}

std::vector<uint8_t> planFrame(size_t k, const Plan& p, uint32_t i) {
    const uint32_t id = uint32_t(k) * 100000 + i;
    if (p.type == &ommType()) {
        // Every 9th record repeats the previous one (a resend: dedupe).
        const uint32_t j = (i % 9 == 8) ? i - 1 : i;
        const uint32_t jid = uint32_t(k) * 100000 + j;
        return parityOmm(jid + 1, "P" + std::to_string(jid), parityEpoch(j * 37), j);
    }
    if (p.type == &mpeType()) return mpeRecord("E" + std::to_string(id), double(1780000000u + i * 60u), double(i % 500));
    // CAT: 150 objects updated four times each (supersede by object).
    const uint32_t obj = i % 150;
    return catRecord(700000 + obj, "C" + std::to_string(obj), "", "",
                     "N" + std::to_string(obj) + "-v" + std::to_string(i / 150));
}

// RecordAttr bytes with a fixed build order. buildRecordAttr creates its
// strings inside one call's argument list, whose evaluation order C++ leaves
// unspecified (GCC on x86_64 builds them in the opposite order from clang and
// from GCC on arm64), so the same attributes serialize differently.
std::vector<uint8_t> parityAttr(const std::string& peerId, const std::string& provider, const std::string& source,
                                const std::string& batch) {
    flatbuffers::FlatBufferBuilder b(256);
    const auto p = b.CreateString(provider);
    const auto s = b.CreateString(source);
    const auto bt = b.CreateString(batch);
    const auto ck = b.CreateString("");
    const auto pp = b.CreateString("");
    const auto pk = b.CreateString("");
    const auto tag = fb::CreateSourceTag(b, p, s, 0, bt, ck, pp, pk);
    const auto tags = b.CreateVector(&tag, 1);
    const auto peer = b.CreateVector(reinterpret_cast<const uint8_t*>(peerId.data()), peerId.size());
    const auto ra = fb::CreateRecordAttr(b, peer, 0, 0, 0, 0, tags);
    fb::FinishRecordAttrBuffer(b, ra);
    return std::vector<uint8_t>(b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize());
}

std::vector<uint8_t> planAttr(size_t k, uint32_t i) {
    // Resent OMM records arrive from another source (a re-tag, A2).
    return parityAttr("peer" + std::to_string(k), "prov", "src" + std::to_string(i % 3),
                      "batch" + std::to_string(i / 250));
}

struct Registered {
    std::vector<uint32_t> pids;
};

Registered registerAll(Store& s) {
    s.registerTypes({&ommType(), &mpeType(), &catType()});
    Registered r;
    for (const Plan& p : plans()) r.pids.push_back(s.partition(p.producer, *p.type));
    return r;
}

void emit(const std::string& name, const std::vector<std::string>& lines) {
    std::string all;
    for (const auto& l : lines) {
        all += l;
        all += '\n';
    }
    std::printf("PARITY %s lines=%zu sha256=%s\n", name.c_str(), lines.size(), sha(all.data(), all.size()).c_str());
    const std::string out = argStr("out", "");
    if (!out.empty()) {
        std::ofstream f(out, std::ios::binary | std::ios::trunc);
        f.write(all.data(), std::streamsize(all.size()));
        CHECK(f.good());
    }
}

std::string cell(const rb1::Cell& c) {
    switch (c.type) {
        case rb1::kNull: return "null";
        case rb1::kInt: return "i" + std::to_string(c.i);
        case rb1::kReal: {
            char b[40];
            std::snprintf(b, sizeof(b), "r%.17g", c.d);
            return b;
        }
        case rb1::kText: return "t" + c.s;
        default: return "b" + hex(reinterpret_cast<const uint8_t*>(c.s.data()), c.s.size());
    }
}

void useHostDir(Store& s, const std::string& dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir, ec);
    s.cfg.io = nullptr;
    s.cfg.root = dir;
    s.root = dir;
}

}  // namespace

PS_SLOW_TEST(parity_canonical_dump) {
    Store s(false, 4, true);
    // The engine's wall clock (TOMB arrival times) is injected; the threads
    // run on the host's monotonic clock.
    s.cfg.clockMs = [](void*) -> int64_t { return 1790000000000ll; };
    const std::string dir = argStr("dir", "");
    if (!dir.empty()) useHostDir(s, dir);
    REQUIRE(s.open() == 0);
    const Registered reg = registerAll(s);
    const auto ps = plans();
    std::vector<std::thread> ts;
    std::atomic<int> failed{0};
    for (size_t k = 0; k < ps.size(); k++) {
        ts.emplace_back([&, k] {
            Producer p(s.e.get(), reg.pids[k]);
            uint64_t last = 0;
            for (uint32_t i = 0; i < ps[k].count; i++)
                last = send(s.e.get(), p, planFrame(k, ps[k], i), planAttr(k, i), int64_t(1780000000000ll + i));
            if (!last || p.waitAcked(last, 120000000000ull) != 0) failed++;
        });
    }
    for (auto& t : ts) t.join();
    REQUIRE(failed.load() == 0);
    // Type-level kills (A14): the first ten OMM records of partition 0.
    for (uint32_t i = 0; i < 10; i++) {
        uint8_t cid[kCidLen];
        frameCid(planFrame(0, ps[0], i), cid);
        std::atomic<int32_t> remaining{0};
        REQUIRE(s.e->deleteCid(ommType().fid, cid, &remaining) == 0);
        const uint64_t until = monoNs() + 60000000000ull;
        while (remaining.load() > 0 && monoNs() < until) sleepNs(1000000);
        REQUIRE(remaining.load() == 0);
    }
    REQUIRE(waitLabeledEngine(s.e.get(), reg.pids, 120000000000ull));

    std::vector<std::string> lines;
    Inspector ins(s.cfg.io ? s.cfg.io : importIo(), s.root);
    for (size_t k = 0; k < reg.pids.size(); k++) {
        const uint32_t pid = reg.pids[k];
        PartView v = ins.partition(pid);
        REQUIRE(v.ok && v.err.empty());
        const auto& c = v.head.counters;
        lines.push_back("P " + std::to_string(pid) + " " + ps[k].producer + " total=" + std::to_string(c.totalCount) +
                        " live=" + std::to_string(c.liveCount) + " liveBytes=" + std::to_string(c.liveBytes) +
                        " tombs=" + std::to_string(c.tombCount) + " totalBytes=" + std::to_string(c.totalBytes));
        for (const auto& r : v.rows) {
            std::ostringstream o;
            // ATTR_IN_M says where the attributes are stored (the meta batch
            // until a merge moves them to a-<seg>): physical, like offsets.
            o << "R " << pid << " " << r.pseq << " k" << int(r.kind) << " f" << int(r.flags & ~kRowAttrInM) << " "
              << hex(r.cid, r.cidLen) << " e" << r.epochMs << " a" << r.arrivalMs << " n" << r.len << " t"
              << r.targetPseq << " l" << r.laneId << " s" << r.supersedeHash << " g" << r.tagHash << " c"
              << r.dataCrc;
            lines.push_back(o.str());
        }
        std::vector<std::pair<uint32_t, std::string>> lanes;
        for (const auto& l : v.lanes)
            if (l.count) lanes.emplace_back(l.laneId, std::to_string(l.count) + "/" + std::to_string(l.bytes));
        std::sort(lanes.begin(), lanes.end());
        for (const auto& l : lanes) lines.push_back("L " + std::to_string(pid) + " " + std::to_string(l.first) + " " + l.second);
    }
    Reader r(s, LaneClass::Bulk, 2);
    const std::vector<std::string> queries = {
        "SELECT count(*), sum(_len) FROM OMM",
        "SELECT _cid, _epoch, _producer FROM OMM ORDER BY _epoch DESC, _cid LIMIT 400",
        "SELECT _cid, _epoch FROM MPE ORDER BY _epoch, _cid LIMIT 400 OFFSET 700",
        "SELECT _cid, _epoch, _source FROM CAT_current ORDER BY _cid",
        "SELECT _pseq, _cid, _epoch FROM sds_p_par_omm_1__OMM WHERE _pseq BETWEEN 100 AND 400 ORDER BY _pseq",
        "SELECT producer, live_count, live_bytes, total_count FROM flatsql_partitions ORDER BY producer",
    };
    for (const auto& q : queries) {
        Rows x = r.q(q);
        if (x.status != 0) std::fprintf(stderr, "  query failed (%d): %s: %s\n", x.status, q.c_str(), x.error.c_str());
        CHECK_EQ(x.status, 0);
        lines.push_back("Q " + q + " rows=" + std::to_string(x.rows.size()));
        for (const auto& row : x.rows) {
            std::string l = "  ";
            for (const auto& c : row) l += cell(c) + " ";
            lines.push_back(l);
        }
    }
    emit("canonical_dump", lines);
    s.close();
}

namespace {
uint64_t detMono(void*) { return 5000000000000ull; }
int64_t detWall(void*) { return 1790000000000ll; }
}  // namespace

PS_SLOW_TEST(parity_deterministic_bytes) {
    // One thread, a frozen injected clock, seals every 700 records, merges
    // built on the writer, commits at explicit pump points.
    setTestClock(detMono, detWall, nullptr);
    Store s(false, 1, false);
    s.cfg.mergeHelpers = 0;
    s.cfg.syncThreads = 0;
    s.cfg.sealRecords = 700;
    s.cfg.commitFrames = 256;
    s.cfg.clockMs = [](void*) -> int64_t { return 1790000000000ll; };
    const std::string dir = argStr("dir", "");
    if (!dir.empty()) useHostDir(s, dir);
    REQUIRE(s.open() == 0);
    const Registered reg = registerAll(s);
    const auto ps = plans();
    std::vector<std::unique_ptr<Producer>> prods;
    for (uint32_t pid : reg.pids) prods.emplace_back(new Producer(s.e.get(), pid));
    // Round-robin batches of 50 records per partition, each followed by pumps
    // until every entry sent so far is acked: the commit points are the pumps.
    std::vector<uint32_t> next(ps.size(), 0);
    std::vector<uint64_t> last(ps.size(), 0);
    bool more = true;
    uint64_t pumps = 0;
    while (more) {
        more = false;
        for (size_t k = 0; k < ps.size(); k++) {
            for (int b = 0; b < 50 && next[k] < ps[k].count; b++, next[k]++) {
                uint64_t rseq = 0;
                for (int tries = 0; !(rseq = send(s.e.get(), *prods[k], planFrame(k, ps[k], next[k]),
                                                  planAttr(k, next[k]), int64_t(1780000000000ll + next[k]), false));
                     tries++) {
                    REQUIRE(tries < 1000);
                    s.e->pump(0);
                    pumps++;
                }
                last[k] = rseq;
            }
            if (next[k] < ps[k].count) more = true;
        }
        for (int guard = 0;; guard++) {
            bool all = true;
            for (size_t k = 0; k < ps.size(); k++)
                if (last[k] && !prods[k]->acked(last[k])) all = false;
            if (all) break;
            REQUIRE(guard < 100000);
            s.e->pump(0);
            pumps++;
        }
    }
    for (uint32_t i = 0; i < 10; i++) {
        uint8_t cid[kCidLen];
        frameCid(planFrame(0, ps[0], i), cid);
        std::atomic<int32_t> remaining{0};
        REQUIRE(s.e->deleteCid(ommType().fid, cid, &remaining) == 0);
        for (int guard = 0; remaining.load() > 0; guard++) {
            REQUIRE(guard < 100000);
            s.e->pump(0);
            pumps++;
        }
    }
    // Let the type owners label everything and merges settle: a fixed number
    // of further pumps.
    for (int i = 0; i < 64; i++) s.e->pump(0);
    prods.clear();
    s.close();
    report("parity_pumps", double(pumps), "pumps");

    std::vector<std::pair<std::string, std::vector<uint8_t>>> files;
    if (dir.empty()) {
        for (const auto& p : s.fs->list(s.root + "/")) files.emplace_back(p.substr(s.root.size()), s.fs->contents(p));
    } else {
        std::error_code ec;
        for (auto it = std::filesystem::recursive_directory_iterator(dir, ec);
             !ec && it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
            if (!it->is_regular_file()) continue;
            const std::string p = it->path().string();
            std::ifstream f(p, std::ios::binary);
            std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
            files.emplace_back(p.substr(dir.size()), std::move(bytes));
        }
    }
    std::sort(files.begin(), files.end());
    std::vector<std::string> lines;
    for (const auto& f : files)
        lines.push_back(f.first + " " + std::to_string(f.second.size()) + " " + sha(f.second.data(), f.second.size()));
    REQUIRE(!lines.empty());
    emit("deterministic_bytes", lines);
    setTestClock(nullptr, nullptr, nullptr);
}
