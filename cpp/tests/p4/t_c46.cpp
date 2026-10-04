// CONTRACT C-46 (BRIEF5): the follow-ups after 3.7.0, each an answer format 1
// gives (or a refusal), driven through the mailbox as a host drives it.
#include <flatbuffers/reflection.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <thread>

#include "p4/p4_test.h"

namespace flatsql {
namespace p4 {
const std::string& lastError();
}  // namespace p4
}  // namespace flatsql

using namespace p4t;

namespace {

namespace fs = std::filesystem;

// cpp/tests/p4 (P4_SCHEMA_DIR names cpp/tests/p4/schemas): the checkout is
// visible at its own path natively and under the wasm host.
std::string testsDir() {
    const std::string d = P4_SCHEMA_DIR;
    return d.substr(0, d.rfind('/'));
}

std::map<std::string, std::string> filesUnder(const std::string& root) {
    std::map<std::string, std::string> out;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(root, ec); !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        if (!it->is_regular_file()) continue;
        std::ifstream in(it->path(), std::ios::binary);
        out[fs::relative(it->path(), root).string()] = std::string(std::istreambuf_iterator<char>(in), {});
    }
    return out;
}

}  // namespace

namespace {

// SUMMARY 5: the type's full-text state (the last pass's) and the seq it
// has indexed through.
std::string ftsState(const std::string& type, int64_t* through = nullptr) {
    TlvW w;
    w.u8(45, 5).text(1, type);
    const Result r = call(P4_OPC_SUMMARY, w.b);
    for (size_t i = 0; i < r.rows.size(); i++)
        if (r.s(i, "type") == type) {
            if (through) *through = r.i(i, "through");
            return r.s(i, "state");
        }
    return "status " + std::to_string(r.status) + " " + r.err;
}
// Ready with every record through seq indexed.
bool ftsCaughtUp(const std::string& type, int64_t seq) {
    for (int i = 0; i < 600; i++) {
        int64_t through = 0;
        if (ftsState(type, &through) == "ready" && through >= seq) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}
Result search(const std::string& type, const std::string& text) {
    TlvW w;
    w.text(1, type).text(18, text).u64(3, 1000);
    return call(P4_OPC_SCAN, w.b);
}
Result epochCall(int profile, int64_t at, int64_t maxDelta, bool count, uint64_t limit = 1000) {
    TlvW w;
    w.text(1, "MPE").u8(30, uint8_t(profile)).i64(31, at).i64(32, maxDelta);
    if (count) w.u8(33, 1);
    else w.u64(3, limit);
    return call(P4_OPC_EPOCH, w.b);
}
// A page's (key, epoch) pairs in answer order.
std::vector<std::pair<std::string, int64_t>> keyEpochs(const Result& r) {
    std::vector<std::pair<std::string, int64_t>> out;
    for (size_t i = 0; i < r.rows.size(); i++) out.push_back({r.s(i, "key"), r.i(i, "epoch")});
    return out;
}

}  // namespace

// C-46 (3): full text indexes a record whose 8-byte fields align to a size
// prefix the producer cut off (a size-prefixed build plus [4:], as SDN's sds
// builders send) as format 1 does, and a start-aligned one as before.
P4_TEST(t_fts_prefix_aligned) {
    const std::string root = scratchDir("t_fts_prefix_aligned");
    REQUIRE(openEngine(root) == P4_OK, "open");
    TestType mpe = mpeType();
    mpe.fullText = true;
    REQUIRE(registerType(mpe) == P4_OK, "register");
    Batch b;
    b.type = "MPE";
    b.peer = "12D3KooWExample";
    b.tags.push_back(Tag{"space-data-network-02", "celestrak-gp", "", "b1", "", "", ""});
    for (int i = 0; i < 8; i++) {
        In r;
        const bool start = i >= 4;
        r.frame = buildFrame(mpe, {Field::str("ENTITY_ID", "ALIGN-" + std::to_string(i) + (start ? " startaligned" : " prefixaligned")),
                                   Field::f64("EPOCH", 1789000000.0 + i), Field::f64("MEAN_MOTION", 15.5 + i)},
                             start);
        // The records are what the case says: the bare record verifies only
        // from its own start when start-aligned, only from the prefix when not.
        const uint8_t* rec = r.frame.data() + 4;
        const size_t n = r.frame.size() - 4;
        CHECK_EQ(flatbuffers::Verify(*mpe.schema, *mpe.schema->root_table(), rec, n), start, "bare verify " + std::to_string(i));
        r.ts = 1790000000 + i;
        b.recs.push_back(std::move(r));
    }
    const Result pr = put(b);
    REQUIRE(pr.status == P4_OK, "put");
    int64_t last = 0;
    for (size_t i = 0; i < pr.rows.size(); i++) last = std::max(last, pr.i(i, "seq"));
    REQUIRE(last >= 8, "the records' seqs");
    REQUIRE(ftsCaughtUp("MPE", last), "full text caught up: " + ftsState("MPE"));
    CHECK_EQ(search("MPE", "prefixaligned").rows.size(), size_t(4), "size-prefix-aligned records found");
    CHECK_EQ(search("MPE", "startaligned").rows.size(), size_t(4), "start-aligned records found");
    CHECK_EQ(search("MPE", "align").rows.size(), size_t(8), "every record found");
    closeEngine();
    removeTree(root);
}

// C-46 (2): full text on a type with no records answers like format 1: no
// rows (its index has nothing to build), not "building".
P4_TEST(t_fts_empty_type) {
    const std::string root = scratchDir("t_fts_empty_type");
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(catType()) == P4_OK, "register");
    CHECK(ftsState("CAT") == "ready", ftsState("CAT"));
    const Result r = search("CAT", "lorem");
    CHECK(r.status == P4_OK && r.rows.empty(), std::to_string(r.status) + " " + r.err);
    closeEngine();
    removeTree(root);
}

// C-46 (1): an epoch outside the int64 range (a double of magnitude >= 2^63
// seconds, infinities included) is indexed clamped to it, as format 1 on
// arm64 indexes it; NaN and 0 stay absent. The EPOCH points rank the clamped
// epochs by their exact distance (no overflow at INT64_MIN), staged and
// merged alike.
P4_TEST(t_epoch_clamped) {
    const std::string root = scratchDir("t_epoch_clamped");
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(mpeType()) == P4_OK, "register");
    const double inf = std::numeric_limits<double>::infinity();
    const std::vector<std::pair<std::string, double>> in = {
        {"E-A", 1789000000.0}, {"E-A", 1e19},   {"E-B", -1e19},    {"E-C", inf},          {"E-D", -inf},
        {"E-E", std::nan("")}, {"E-F", 9223372036854775808.0}, {"E-G", 9.21e18}, {"E-H", -9223372036854775808.0}};
    Batch b;
    b.type = "MPE";
    b.peer = "12D3KooWExample";
    b.tags.push_back(Tag{"space-data-network-02", "celestrak-gp", "", "b1", "", "", ""});
    for (size_t i = 0; i < in.size(); i++) {
        In r;
        r.frame = mpeFrame(in[i].first, in[i].second, 1.0 + double(i));
        r.ts = 1790000000 + int64_t(i);
        b.recs.push_back(std::move(r));
    }
    REQUIRE(put(b).status == P4_OK, "put");
    const int64_t mx = INT64_MAX, mn = INT64_MIN;
    for (int pass = 0; pass < 2; pass++) {   // the rows staged, then merged (REBUILD 1)
        const std::string P = pass ? "merged " : "staged ";
        TlvW sc;
        sc.text(1, "MPE").u64(3, 100);
        const Result all = call(P4_OPC_SCAN, sc.b);
        REQUIRE(all.rows.size() == in.size(), P + "scan " + all.err);
        std::map<std::string, std::vector<int64_t>> byKey;
        int nulls = 0;
        for (size_t i = 0; i < all.rows.size(); i++) {
            if (all.null(i, "epoch")) nulls++;
            else byKey[all.s(i, "key")].push_back(all.i(i, "epoch"));
        }
        CHECK_EQ(nulls, 1, P + "NaN: no epoch");
        CHECK(byKey["E-A"].size() == 2 && byKey["E-A"][1] == mx, P + "1e19 -> INT64_MAX");
        CHECK(byKey["E-B"] == std::vector<int64_t>{mn}, P + "-1e19 -> INT64_MIN");
        CHECK(byKey["E-C"] == std::vector<int64_t>{mx}, P + "+inf -> INT64_MAX");
        CHECK(byKey["E-D"] == std::vector<int64_t>{mn}, P + "-inf -> INT64_MIN");
        CHECK(byKey["E-F"] == std::vector<int64_t>{mx}, P + "2^63 -> INT64_MAX");
        CHECK(byKey["E-G"] == std::vector<int64_t>{int64_t(9210000000000000000)}, P + "9.21e18 exact");
        CHECK(byKey["E-H"] == std::vector<int64_t>{mn}, P + "-2^63 exact");
        // nearest at a normal instant: E-A's normal record; every other
        // entity's only record, at its exact distance.
        const std::vector<std::pair<std::string, int64_t>> nearest = {
            {"E-A", 1789000000}, {"E-B", mn}, {"E-C", mx}, {"E-D", mn}, {"E-F", mx}, {"E-G", int64_t(9210000000000000000)}, {"E-H", mn}};
        CHECK(keyEpochs(epochCall(2, 1789000500, 0, false)) == nearest, P + "nearest");
        CHECK_EQ(epochCall(2, 1789000500, 0, true).rows[0][0].i, int64_t(7), P + "nearest count");
        CHECK(keyEpochs(epochCall(2, 1789000500, 0, false, 2)) ==
                  (std::vector<std::pair<std::string, int64_t>>(nearest.begin(), nearest.begin() + 2)),
              P + "nearest page of 2");
        // a max delta keeps the near ones only (the distance never overflows)
        CHECK(keyEpochs(epochCall(2, 1789000500, 86400, false)) == (std::vector<std::pair<std::string, int64_t>>{{"E-A", 1789000000}}),
              P + "nearest within a day");
        CHECK_EQ(epochCall(2, 1789000500, 86400, true).rows[0][0].i, int64_t(1), P + "count within a day");
        // forward after every normal epoch, as_of at it
        CHECK(keyEpochs(epochCall(4, 1789100000, 0, false)) ==
                  (std::vector<std::pair<std::string, int64_t>>{{"E-A", mx}, {"E-C", mx}, {"E-F", mx}, {"E-G", int64_t(9210000000000000000)}}),
              P + "forward");
        CHECK(keyEpochs(epochCall(3, 1789100000, 0, false)) ==
                  (std::vector<std::pair<std::string, int64_t>>{{"E-A", 1789000000}, {"E-B", mn}, {"E-D", mn}, {"E-H", mn}}),
              P + "as_of");
        TlvW s1;
        s1.u8(45, 1).text(1, "MPE");
        const Result sum = call(P4_OPC_SUMMARY, s1.b);
        CHECK(sum.rows.size() == 1 && sum.i(0, "min_epoch") == mn && sum.i(0, "max_epoch") == mx, P + "summary epoch range");
        TlvW rb;
        rb.text(1, "MPE").u32(63, 1);
        if (!pass) REQUIRE(call(P4_OPC_REBUILD, rb.b).status == P4_OK, "REBUILD 1");
    }
    closeEngine();
    removeTree(root);
}

// C-46 (5): EPOCH pages with the limit pushed down while deletes take the
// records they pick: a pick gone before its answer sends the page to the full
// pass. Every page answers (no error, no stall), in entity order within its
// limit; once the deletes end, the page is the first rows of the answer with
// no limit, and the count is its size.
P4_TEST(t_epoch_page_under_deletes) {
    const std::string root = scratchDir("t_epoch_page_deletes");
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(mpeType()) == P4_OK, "register");
    const int64_t at = 1789000000;
    Batch b;
    b.type = "MPE";
    b.peer = "12D3KooWExample";
    b.tags.push_back(Tag{"space-data-network-02", "celestrak-gp", "", "b1", "", "", ""});
    // 40 entities x 5 records; record j of an entity is j+1 minutes from at
    std::vector<std::vector<std::vector<uint8_t>>> cids(40);
    for (int k = 0; k < 40; k++)
        for (int j = 0; j < 5; j++) {
            In r;
            char ent[16];
            std::snprintf(ent, sizeof ent, "ENT-%02d", k);
            r.frame = mpeFrame(ent, double(at + 60 * (j + 1) * (k % 2 ? 1 : -1)), 1.0 + j);
            r.ts = 1790000000 + k * 5 + j;
            uint8_t c[36];
            cidOf(r.frame, c);
            cids[size_t(k)].push_back(std::vector<uint8_t>(c, c + 36));
            b.recs.push_back(std::move(r));
        }
    REQUIRE(put(b).status == P4_OK, "put");
    auto page = [&](uint64_t limit) {
        TlvW w;
        w.text(1, "MPE").u8(30, 2).i64(31, at).u64(3, limit).u8(2, 1);
        return call(P4_OPC_EPOCH, w.b);
    };
    // The deleter takes, one call each, the nearest record of the page's
    // entities (ENT-00..ENT-03), their first four records in rank order.
    std::atomic<bool> done{false};
    int deleteErrors = 0;
    std::thread deleter([&]() {
        for (int j = 0; j < 4; j++)
            for (int k = 0; k < 4; k++) {
                TlvW d;
                d.text(1, "MPE");
                std::vector<uint8_t> cl = {1, 0, 0, 0};  // one CID
                cl.insert(cl.end(), cids[size_t(k)][size_t(j)].begin(), cids[size_t(k)][size_t(j)].end());
                d.raw(40, cl.data(), cl.size());
                if (call(P4_OPC_DELETE, d.b).status != P4_OK) deleteErrors++;
            }
        done = true;
    });
    int pages = 0, bad = 0;
    while (!done.load() || pages < 20) {
        const Result r = page(3);
        bool ok = r.status == P4_OK && r.rows.size() == 3;
        for (size_t i = 1; ok && i < r.rows.size(); i++) ok = r.s(i - 1, "key") < r.s(i, "key");
        if (!ok) bad++;
        pages++;
    }
    deleter.join();
    CHECK_EQ(deleteErrors, 0, "deletes");
    CHECK_EQ(bad, 0, std::to_string(pages) + " pages under deletes");
    const Result full = page(250000), three = page(3);
    REQUIRE(full.status == P4_OK && full.rows.size() == 40 && three.status == P4_OK && three.rows.size() == 3, full.err + three.err);
    for (size_t i = 0; i < 3; i++)
        CHECK(three.s(i, "key") == full.s(i, "key") && three.i(i, "epoch") == full.i(i, "epoch") && three.s(i, "cid") == full.s(i, "cid"),
              "row " + std::to_string(i));
    // ENT-00's last record is 5 minutes before at, ENT-01's 5 minutes after
    CHECK(three.s(0, "key") == "ENT-00" && three.i(0, "epoch") == at - 300, "ENT-00 keeps its last record");
    CHECK(three.s(1, "key") == "ENT-01" && three.i(1, "epoch") == at + 300, "ENT-01 keeps its last record");
    TlvW c;
    c.text(1, "MPE").u8(30, 2).i64(31, at).u8(33, 1);
    const Result n = call(P4_OPC_EPOCH, c.b);
    CHECK(n.status == P4_OK && n.rows.size() == 1 && n.rows[0][0].i == 40, "count");
    closeEngine();
    removeTree(root);
}

// C-46 (6): a store an earlier format-4 build wrote is refused with a format
// error and never crashes. The fixture is a store 74b2f0d wrote (feed index
// r_c on the CID, r_ke on (k, e), no staged rows): one feed and local, OMM.
P4_TEST(t_earlier_layout_refused) {
    const std::string dir = scratchDir("t_earlier_layout");
    std::error_code ec;
    fs::copy(testsDir() + "/fixtures/store-74b2f0d", dir, fs::copy_options::recursive, ec);
    REQUIRE(!ec, "copy the fixture: " + ec.message());
    const std::map<std::string, std::string> before = filesUnder(dir + "/fsql4");
    REQUIRE(before.size() == 9, "the fixture's 9 files");
    for (uint8_t mode : {uint8_t(0), uint8_t(1)}) {
        EngineOpts o;
        o.createMode = mode;
        CHECK_EQ(openEngine(dir + "/fsql4", o), P4_E_FORMAT, "create mode " + std::to_string(mode));
        CHECK(flatsql::p4::lastError().find("an earlier format-4 build wrote") != std::string::npos, flatsql::p4::lastError());
    }
    // Refused as it was: every file of the store has its bytes.
    const std::map<std::string, std::string> after = filesUnder(dir + "/fsql4");
    for (const auto& f : before) {
        auto it = after.find(f.first);
        CHECK(it != after.end() && it->second == f.second, f.first + " unchanged");
    }
    // The engine starts again in this process: a fresh store opens and serves.
    const std::string fresh = scratchDir("t_earlier_layout_fresh");
    REQUIRE(openEngine(fresh) == P4_OK, "a fresh store opens");
    REQUIRE(registerType(ommType()) == P4_OK, "register");
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWExample";
    In r;
    r.frame = ommFrame(25544, "1998-067A", isoTime(1789000000));
    r.ts = 1790000000;
    b.recs.push_back(r);
    CHECK_EQ(put(b).status, P4_OK, "put");
    uint8_t cid[36];
    cidOf(r.frame, cid);
    const Result g = get("OMM", {std::vector<uint8_t>(cid, cid + 36)});
    CHECK(g.status == P4_OK && g.rows.size() == 1, g.err);
    closeEngine();
    removeTree(dir);
    removeTree(fresh);
}
