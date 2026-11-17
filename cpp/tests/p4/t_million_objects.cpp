// A feed of more than a million objects survives a stop and a start.
//
// A checkpoint's dictionary segment holds one FlatBuffer table per entry (an
// object key is one). Once a feed's dictionary segment passed 1,000,000
// entries, FlatBuffers' default verifier bound (1,000,000 tables) refused it:
// the next checkpoint and the stop failed with P4_E_CORRUPT, and the open
// replayed the stream, stopped at that segment and cut the stream there.
//
// t_million_objects writes --records distinct OMM objects (default
// 1,100,000) into one feed in 4,096-record PUTs, compacts the feed (a fresh
// generation, as a format-4 conversion writes it: the whole dictionary in one
// segment), writes 4,096 more objects, stops the engine, opens it again and
// reads every record back: both stops answer OK and every record is held with
// its bytes.
#include <map>
#include <string>
#include <vector>

#include "internal.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;
namespace flatsql { namespace p4 { P4Engine* currentEngine(); } }

namespace {
std::vector<uint8_t> objectFrame(long i) {
    return ommFrame(uint32_t(100000 + i), "2026-" + std::to_string(1000000 + i), isoTime(1789000000));
}
Result putObjects(long from, long to) {
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWMillion";
    b.tags.push_back(Tag{"million", "objects", "", "b1", "", "", ""});
    for (long i = from; i < to; i++) {
        In r;
        r.frame = objectFrame(i);
        r.ts = 1790000000;
        b.recs.push_back(std::move(r));
    }
    return put(b);
}
}  // namespace

FLATSQL_SLOW_TEST(t_million_objects) {
    const long n = argInt("records", 1100000);
    const std::string dir = scratchDir("t_million_objects");
    REQUIRE(openEngine(dir) == FLATSQL_OK, "open a fresh store");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register OMM");
    for (long at = 0; at < n; at += 4096) {
        const Result w = putObjects(at, std::min(n, at + 4096));
        REQUIRE(w.status == FLATSQL_OK, "put at " + std::to_string(at) + ": " + w.err);
    }
    {
        auto* e = fp::currentEngine();
        auto* t = e->findType("OMM");
        REQUIRE(t, "the OMM type");
        fp::Feed* feed = nullptr;
        {
            std::lock_guard<std::mutex> lock(t->mu);
            for (auto& f : t->feeds)
                if (f && f->provider == "million") feed = f.get();
        }
        REQUIRE(feed, "the feed");
        fp::Internal in;
        in.what = feed->fid;
        in.force = true;
        REQUIRE(fp::runOnWriter(e, t, fp::kOpCompact, &in) == FLATSQL_OK && in.b > 0, "compaction: " + in.err);
    }
    const long total = n + 4096;
    const Result w = putObjects(n, total);
    REQUIRE(w.status == FLATSQL_OK, "put after the compaction: " + w.err);
    CHECK_EQ(closeEngine(300000), FLATSQL_OK, "the stop checkpoints the feed");

    REQUIRE(openEngine(dir, [] { EngineOpts o; o.createMode = 0; return o; }()) == FLATSQL_OK, "open again");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register OMM again");
    long held = 0, wrong = 0;
    for (long at = 0; at < total; at += 512) {
        std::vector<std::vector<uint8_t>> cids;
        std::map<std::string, std::string> want;  // CID text -> record bytes
        for (long i = at; i < std::min(total, at + 512); i++) {
            const std::vector<uint8_t> frame = objectFrame(i);
            uint8_t cid[36];
            cidOf(frame, cid);
            cids.emplace_back(cid, cid + 36);
            want[cidText(cid)] = std::string(reinterpret_cast<const char*>(frame.data() + 4), frame.size() - 4);
        }
        const Result g = get("OMM", cids);
        REQUIRE(g.status == FLATSQL_OK, "get at " + std::to_string(at) + ": " + g.err);
        held += long(g.rows.size());
        for (size_t r = 0; r < g.rows.size(); r++) {
            auto it = want.find(g.s(r, "cid"));
            if (it == want.end() || it->second != g.s(r, "data")) wrong++;
        }
    }
    CHECK_EQ(held, total, "every record is held after the reopen");
    CHECK_EQ(wrong, 0, "every record has its bytes");
    CHECK_EQ(closeEngine(300000), FLATSQL_OK, "the second stop");
    removeTree(dir);
}
