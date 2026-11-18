// A feed's DAG answers are pinned to fixed values.
//
// The commits root goes into every checkpoint (COMMITS_ROOT_CID) and the
// feed root, ranges and drops are what a node publishes and unpins, so a
// change of the DAG cache must not change any of them. t_dag_roots writes one
// feed through the engine's PUT, DELETE and SUPERSEDE calls: ten PUT rounds
// (re-puts of earlier records' bytes, new versions of earlier objects, three
// records near 1 MiB, two just over a 1 MiB chunk and one just under), a
// DELETE of 25 records after every third round, supersedes after rounds 1
// and 2, then one
// DELETE of 200 records scattered over every round and of one of the records
// over 1 MiB. It stops the engine,
// opens it again and, each from an empty DAG cache (as after a reopen),
// reads the commits root at the committed point, FEED_ROOT, and hashes of
// RANGES and DROPS, and checks them against the values the engine computed
// before its DAG cache kept digest prefixes (FlatSQL 3.12.10).
//
// flatsql_p4_collide_test runs the same test with every frame's digest
// prefix equal (FLATSQL_TEST_PREFIX_COLLIDE): the answers are the same.
#include <array>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "internal.h"
#include "dag.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;
namespace flatsql { namespace p4 { P4Engine* currentEngine(); } }

namespace {
const char* kProvider = "dag";
const char* kSource = "roots";

fp::Feed* rootsFeed() {
    auto* t = fp::currentEngine()->findType("OMM");
    if (!t) return nullptr;
    std::lock_guard<std::mutex> lock(t->mu);
    for (const auto& f : t->feeds)
        if (f && f->created && f->provider == kProvider && f->source == kSource) return f.get();
    return nullptr;
}
TlvW feedRequest() {
    TlvW w;
    w.text(1, "OMM").text(3, kProvider).text(4, kSource);
    return w;
}
Result bigCall(uint32_t op, const TlvW& w) {
    CallOpts o;
    o.maxResultRows = 10000000;
    o.maxResultBytes = uint64_t(1) << 30;
    return call(op, w.b, o);
}
std::vector<uint8_t> object(long i, int version, size_t pad = 0) {
    return ommFrame(uint32_t(10000 + i), "2026-" + std::to_string(100000 + i), isoTime(1789000000 + version),
                    15.5 + double(i % 997) * 1e-6, pad);
}
std::string cidHex(const std::string& cid) {
    return hex(reinterpret_cast<const uint8_t*>(cid.data()), cid.size());
}
// Row count and the first 16 bytes of the SHA-256 of the rows' columns.
std::string hashRows(const Result& r, const std::vector<std::string>& cols) {
    std::string all;
    for (size_t i = 0; i < r.rows.size(); i++) {
        for (const auto& c : cols) {
            if (c == "cid" || c == "block") all += cidHex(r.s(i, c));
            else all += std::to_string(r.i(i, c));
            all += "|";
        }
        all += "\n";
    }
    uint8_t d[32];
    flatsql::ps::sha256(reinterpret_cast<const uint8_t*>(all.data()), all.size(), d);
    return std::to_string(r.rows.size()) + ":" + hex(d, 16);
}
int64_t fixedWallMs(void*) { return int64_t(1790000000) * 1000; }
bool deleteCids(const std::vector<std::array<uint8_t, 36>>& cids) {
    const uint32_t n = uint32_t(cids.size());
    std::vector<uint8_t> body{uint8_t(n), uint8_t(n >> 8), uint8_t(n >> 16), uint8_t(n >> 24)};
    for (const auto& c : cids) body.insert(body.end(), c.begin(), c.end());
    TlvW del;
    del.text(1, "OMM").raw(40, body.data(), body.size());
    return call(FLATSQL_OPC_DELETE, del.b).status == FLATSQL_OK;
}
}  // namespace

FLATSQL_TEST(t_dag_roots) {
    // Values from FlatSQL 3.12.10 (main before the 24-byte frame entries).
    const std::string kCommitsRoot = "01701220f631ed8ed4703c1106e9c1f8dca2eb0e36eb40d26dc92ddb8a0d29581c50bfcf";
    const std::string kFeedRoot = "01701220287180aef19d17fe3073a3948cd256d5c5d19b46bc80a52e75e31e438d52019d";
    const long long kFeedRootUnit = 3751192, kGeneration = 0;
    const std::string kRanges = "1171:57833bf96e0112942b1888017889ee6e";
    const std::string kDrops = "479:3c810c327203751a550aed3061126781";

    // A fixed wall clock: an FMB holds its round's clock (COMMITTED_AT), so
    // the stream's bytes, and every CID over them, depend on it.
    flatsql::ps::setTestClock(nullptr, fixedWallMs, nullptr);
    struct ClockReset { ~ClockReset() { flatsql::ps::setTestClock(nullptr, nullptr, nullptr); } } clockReset;
    const std::string dir = scratchDir("t_dag_roots");
    REQUIRE(openEngine(dir) == FLATSQL_OK, "open");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");
    std::mt19937 rng(818);
    // Round 3's first three records: two just over a 1 MiB chunk (the first
    // deleted by the last DELETE), one just under. Random deletes leave them
    // alone and the supersedes come before them, so the dead bytes stay
    // under half of the stream and no compaction starts during the test.
    const size_t kLarge[3] = {(size_t(1) << 20) + 4096, (size_t(1) << 20) - 4096, (size_t(1) << 20) + 8192};
    std::vector<std::array<uint8_t, 36>> written, large;
    const long rounds = 10, per = 150;
    for (long r = 0; r < rounds; r++) {
        Batch b;
        b.type = "OMM";
        b.peer = "12D3KooWDagRoots";
        b.at = 1790000000 + r;
        b.tags.push_back(Tag{kProvider, kSource, "", "b" + std::to_string(r % 3), "", "", ""});
        for (long i = 0; i < per; i++) {
            In in;
            in.frame = object(r * per + i, 0, r == 3 && i < 3 ? kLarge[i] : 0);
            in.ts = 1790000000 + r;
            b.recs.push_back(std::move(in));
        }
        for (long i = 0; i < 15 && r > 0; i++) {  // the same bytes again: a second row on an earlier frame
            In in;
            in.frame = object(long(rng() % uint32_t(r * per)), 0);
            in.ts = 1790000000 + r;
            b.recs.push_back(std::move(in));
        }
        for (long i = 0; i < 10 && r > 0; i++) {  // a new version of an earlier object
            In in;
            in.frame = object(long(rng() % uint32_t(r * per)), int(r));
            in.ts = 1790000000 + r;
            b.recs.push_back(std::move(in));
        }
        for (size_t i = 0; i < b.recs.size(); i++) {
            std::array<uint8_t, 36> c;
            cidOf(b.recs[i].frame, c.data());
            if (r == 3 && i < 3) large.push_back(c);
            else written.push_back(c);
        }
        const Result w = put(b);
        REQUIRE(w.status == FLATSQL_OK, "put: " + w.err);
        if (r % 3 == 2) {
            std::vector<std::array<uint8_t, 36>> d;
            for (int k = 0; k < 25; k++) d.push_back(written[rng() % written.size()]);
            REQUIRE(deleteCids(d), "delete");
        }
        if (r == 1 || r == 2) {  // keeps this round's batch: the source's other batches' rows go
            TlvW s;
            s.text(1, "OMM").text(11, kProvider).text(12, kSource).text(60, "b" + std::to_string(r % 3)).u8(61, 1);
            const Result x = call(FLATSQL_OPC_SUPERSEDE, s.b);
            REQUIRE(x.status == FLATSQL_OK, "supersede: " + x.err);
        }
    }
    {
        std::vector<std::array<uint8_t, 36>> d;
        for (int k = 0; k < 200; k++) d.push_back(written[rng() % written.size()]);
        d.push_back(large[0]);
        REQUIRE(deleteCids(d), "scattered delete");
    }
    REQUIRE(closeEngine() == FLATSQL_OK, "close");

    REQUIRE(openEngine(dir) == FLATSQL_OK, "reopen");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register again");
    fp::Feed* feed = rootsFeed();
    REQUIRE(feed, "feed");
    fp::feedDagReset(feed);
    const Result root = bigCall(FLATSQL_OPC_FEED_ROOT, feedRequest());
    REQUIRE(root.status == FLATSQL_OK && root.rows.size() == 1, "feed root: " + root.err);
    const long long unit = root.i(0, "unit");
    int32_t rc = FLATSQL_OK;
    auto stream = fp::streamCur(feed, &rc);
    REQUIRE(stream, "stream");
    fp::feedDagReset(feed);
    std::string commits, err;
    rc = fp::feedDagCommitRoot(feed, stream.get(), uint64_t(unit), uint64_t(fp::streamSize(stream.get())), &commits, &err);
    REQUIRE(rc == FLATSQL_OK, "commits root: " + err);
    fp::feedDagReset(feed);
    const Result ranges = bigCall(FLATSQL_OPC_RANGES, feedRequest());
    REQUIRE(ranges.status == FLATSQL_OK, "ranges: " + ranges.err);
    fp::feedDagReset(feed);
    TlvW dw = feedRequest();
    dw.u64(80, 0);
    const Result drops = bigCall(FLATSQL_OPC_DROPS, dw);
    REQUIRE(drops.status == FLATSQL_OK, "drops: " + drops.err);

    const std::string gotCommits = cidHex(commits), gotRoot = cidHex(root.s(0, "cid"));
    const std::string gotRanges = hashRows(ranges, {"cid", "unit", "kind", "offset", "length", "block", "generation"});
    const std::string gotDrops = hashRows(drops, {"cid", "unit", "generation"});
    const long long generation = root.i(0, "generation");
    std::printf("commits root %s\nfeed root %s unit %lld generation %lld\nranges %s\ndrops %s\n", gotCommits.c_str(),
                gotRoot.c_str(), unit, generation, gotRanges.c_str(), gotDrops.c_str());
    CHECK(gotCommits == kCommitsRoot, "commits root " + gotCommits);
    CHECK(gotRoot == kFeedRoot, "feed root " + gotRoot);
    CHECK_EQ(unit, kFeedRootUnit, "feed root unit");
    CHECK_EQ(generation, kGeneration, "the feed's first generation (no compaction)");
    CHECK(gotRanges == kRanges, "ranges " + gotRanges);
    CHECK(gotDrops == kDrops, "drops " + gotDrops);
    REQUIRE(closeEngine() == FLATSQL_OK, "close again");
    removeTree(dir);
}
