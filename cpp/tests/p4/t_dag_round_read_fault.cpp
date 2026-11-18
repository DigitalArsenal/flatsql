// A failed stream read in the middle of a DAG round does not stop the feed's
// later DAG builds.
//
// Folding a round that removes records reads each removed frame's digest
// back from its unit's FMB, after earlier rows of the round have changed the
// DAG cache. t_dag_round_read_fault writes three PUT rounds into one feed,
// builds the commits root at that commit point, deletes records of all three
// rounds, and builds the commits root at the delete's commit point with the
// first read of an earlier unit failing (the test target's flatsql_io_read).
// That build fails; the next build at the same point succeeds and gives the
// commits root a build from an empty cache gives.
#include <array>
#include <string>
#include <vector>

#include "internal.h"
#include "dag.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;
namespace flatsql { namespace p4 { P4Engine* currentEngine(); } }
void p4ReplayFailNextRead(int32_t handle, uint64_t atOrBelow);
uint64_t p4ReplayFailedReads();

namespace {
fp::Feed* faultFeed() {
    auto* t = fp::currentEngine()->findType("OMM");
    if (!t) return nullptr;
    std::lock_guard<std::mutex> lock(t->mu);
    for (const auto& f : t->feeds)
        if (f && f->created && f->provider == "dag" && f->source == "fault") return f.get();
    return nullptr;
}
long long commitPoint() {
    TlvW w;
    w.text(1, "OMM").text(3, "dag").text(4, "fault");
    const Result r = call(FLATSQL_OPC_FEED_ROOT, w.b);
    return r.status == FLATSQL_OK && r.rows.size() == 1 ? r.i(0, "unit") : -1;
}
std::string cidHex(const std::string& cid) {
    return hex(reinterpret_cast<const uint8_t*>(cid.data()), cid.size());
}
}  // namespace

FLATSQL_TEST(t_dag_round_read_fault) {
    const std::string dir = scratchDir("t_dag_round_read_fault");
    REQUIRE(openEngine(dir) == FLATSQL_OK, "open");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");
    std::vector<std::array<uint8_t, 36>> removed;
    for (long r = 0; r < 3; r++) {
        Batch b;
        b.type = "OMM";
        b.peer = "12D3KooWDagFault";
        b.at = 1790000000 + r;
        b.tags.push_back(Tag{"dag", "fault", "", "b" + std::to_string(r), "", "", ""});
        for (long i = 0; i < 60; i++) {
            In in;
            const long n = r * 60 + i;
            in.frame = ommFrame(uint32_t(20000 + n), "2026-" + std::to_string(200000 + n), isoTime(1789000000));
            in.ts = 1790000000 + r;
            if (i % 4 == 1) {
                std::array<uint8_t, 36> c;
                cidOf(in.frame, c.data());
                removed.push_back(c);
            }
            b.recs.push_back(std::move(in));
        }
        const Result w = put(b);
        REQUIRE(w.status == FLATSQL_OK, "put: " + w.err);
    }
    const long long before = commitPoint();
    REQUIRE(before > 0, "commit point after the puts");
    {
        const uint32_t n = uint32_t(removed.size());
        std::vector<uint8_t> body{uint8_t(n), uint8_t(n >> 8), uint8_t(n >> 16), uint8_t(n >> 24)};
        for (const auto& c : removed) body.insert(body.end(), c.begin(), c.end());
        TlvW del;
        del.text(1, "OMM").raw(40, body.data(), body.size());
        const Result d = call(FLATSQL_OPC_DELETE, del.b);
        REQUIRE(d.status == FLATSQL_OK, "delete: " + d.err);
    }
    const long long after = commitPoint();
    REQUIRE(after > before, "commit point after the delete");

    fp::Feed* feed = faultFeed();
    REQUIRE(feed, "feed");
    int32_t rc = FLATSQL_OK;
    auto stream = fp::streamCur(feed, &rc);
    REQUIRE(stream, "stream");
    const uint64_t end = uint64_t(fp::streamSize(stream.get()));
    std::string first, failed, retried, fresh, err;
    int32_t rcFirst, rcFailed, rcRetried, rcFresh;
    uint64_t faults;
    {
        // dagMu held throughout: no checkpoint build moves the cache between these.
        std::lock_guard<std::mutex> lock(feed->dagMu);
        feed->dagHistory.reset();
        rcFirst = fp::feedDagCommitRootHeld(feed, stream.get(), uint64_t(before), end, &first, &err);
        // Reads up to the cache's last commit point are only the folded
        // round's reads of earlier units' FMBs.
        const uint64_t faultsBefore = p4ReplayFailedReads();
        p4ReplayFailNextRead(stream->h, uint64_t(before));
        rcFailed = fp::feedDagCommitRootHeld(feed, stream.get(), uint64_t(after), end, &failed, &err);
        faults = p4ReplayFailedReads() - faultsBefore;
        rcRetried = fp::feedDagCommitRootHeld(feed, stream.get(), uint64_t(after), end, &retried, &err);
        feed->dagHistory.reset();
        rcFresh = fp::feedDagCommitRootHeld(feed, stream.get(), uint64_t(after), end, &fresh, &err);
    }
    CHECK_EQ(rcFirst, FLATSQL_OK, "build at the puts' commit point");
    CHECK_EQ(faults, uint64_t(1), "the armed read failed");
    CHECK_EQ(rcFailed, FLATSQL_E_IO, "the build with the failed read");
    CHECK_EQ(rcRetried, FLATSQL_OK, "the next build at the delete's commit point");
    CHECK_EQ(rcFresh, FLATSQL_OK, "a build from an empty cache");
    CHECK(!fresh.empty() && retried == fresh, "commits root after the failure " + cidHex(retried) + " from an empty cache " + cidHex(fresh));
    REQUIRE(closeEngine() == FLATSQL_OK, "close");
    removeTree(dir);
}
