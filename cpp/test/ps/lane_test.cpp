// Reader lanes (T2): the snapshot protocol end to end through SQL, the
// partition and fan-out vtabs, meta vtabs, the mailbox ABI (parking,
// cancellation), admission, and the sandbox.
#include <algorithm>
#include <map>
#include <set>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {
std::string ep(int i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-08-%02dT%02d:%02d:%02dZ", 1 + (i / 3600) % 28, (i / 60) % 24, i % 60, i % 60);
    return b;
}

struct Fixture {
    Store s{true, 2, true};
    std::vector<uint32_t> pids;
    std::vector<std::vector<uint8_t>> recs;
    std::map<std::string, int> cidIndex;
    Fixture() {}
};
}  // namespace

PS_TEST(lane_partition_and_type_queries) {
    Store s(true, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &catType()});
    const uint32_t p1 = s.partition("peerA", ommType());
    const uint32_t p2 = s.partition("peerB", ommType());
    std::vector<std::vector<uint8_t>> recs;
    for (int i = 0; i < 300; i++) recs.push_back(ommRecord(uint32_t(40000 + i), "OBJ-" + std::to_string(i), ep(i), 15.0 + i));
    {
        Producer a(s.e.get(), p1), b(s.e.get(), p2);
        const auto attrA = buildRecordAttr("peerA", "prov", "srcA", "b1");
        const auto attrB = buildRecordAttr("peerB", "prov", "srcB", "b1");
        uint64_t la = 0, lb = 0;
        // peerA first (labeled), so records 100..199 are FIRST in peerA.
        for (int i = 0; i < 200; i++) la = send(s.e.get(), a, recs[size_t(i)], attrA, 1000 + i);
        CHECK_EQ(a.waitAcked(la, 10000000000ull), 0);
        CHECK(waitLabeledEngine(s.e.get(), {p1}, 10000000000ull));
        for (int i = 100; i < 300; i++) lb = send(s.e.get(), b, recs[size_t(i)], attrB, 2000 + i);
        CHECK_EQ(b.waitAcked(lb, 10000000000ull), 0);
    }
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, {p1, p2}, 10000000000ull));
    Reader bulk(s, LaneClass::Bulk, 2);
    REQUIRE(bulk.inst);
    // Partition level: every row of the partition.
    Rows r = bulk.q("SELECT count(*), min(_pseq), max(_pseq) FROM sds_p_peerA__OMM");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK_EQ(r.i(0, 0), 200);
    CHECK_EQ(r.i(0, 1), 1);
    CHECK_EQ(r.i(0, 2), 200);
    // Type level: one row per cid (300 distinct), FIRST copies only.
    r = bulk.q("SELECT count(*), count(DISTINCT _cid) FROM OMM");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK_EQ(r.i(0, 0), 300);
    CHECK_EQ(r.i(0, 1), 300);
    // CID lookup through the catalog; schema columns project.
    const std::string cid = cidTextOf(recs[150]);
    r = bulk.q("SELECT NORAD_CAT_ID, OBJECT_ID, _producer, _source, _pid FROM OMM WHERE _cid = ?", {Param::text(cid)});
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK_EQ(r.i(0, 0), 40150);
    CHECK(r.s(0, 1) == "OBJ-150");
    CHECK(r.s(0, 2) == "peerA");  // FIRST copy: peerA acked it first
    CHECK(r.s(0, 3) == "OMM@srcA");
    CHECK_EQ(r.i(0, 4), int64_t(p1));
    // Default order (A19): seconds DESC, CID ASC; LIMIT stops the merge.
    Reader inter(s, LaneClass::Interactive, 2);
    r = inter.q("SELECT _epoch, _cid FROM OMM LIMIT 20");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(20));
    for (size_t i = 1; i < r.rows.size(); i++) {
        const int64_t a = epochSecFloor(r.i(i - 1, 0)), b = epochSecFloor(r.i(i, 0));
        CHECK(a > b || (a == b && r.s(i - 1, 1) < r.s(i, 1)));
    }
    CHECK(r.outcome.rowsExamined <= 20 * 3);
    // Unbounded on an interactive lane: NEEDS_BULK before any row.
    r = inter.q("SELECT count(*) FROM OMM");
    CHECK_EQ(r.status, int32_t(kRsNeedsBulk));
    CHECK_EQ(r.outcome.rowsExamined, uint64_t(0));
    // Column index (COL 0 = NORAD_CAT_ID) is bounded.
    r = inter.q("SELECT _cid FROM OMM WHERE NORAD_CAT_ID = ?", {Param::i64(40042)});
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK(r.s(0, 0) == cidTextOf(recs[42]));
    // Source window (the IQC incident shape).
    r = inter.q("SELECT _cid, _epoch FROM OMM WHERE _source = 'OMM@srcB' ORDER BY _epoch DESC LIMIT 5");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(5));
    // Alias vtab: the FIRST copies of srcB are records 200..299.
    r = bulk.q("SELECT count(*) FROM \"OMM@srcB\"");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK_EQ(r.i(0, 0), 100);
    // Meta: counters from heads.
    r = inter.q("SELECT pid, live_count, total_count FROM flatsql_partitions ORDER BY pid");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 2);
    CHECK_EQ(r.i(0, 1), 200);
    CHECK_EQ(r.i(1, 1), 200);
    r = inter.q("SELECT source, count, bytes FROM flatsql_lanes ORDER BY pid");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 2);
    CHECK(r.s(0, 0) == "srcA");
    CHECK_EQ(r.i(0, 1), 200);
    // Arrivals: 300 FIRST entries in gseq order.
    r = bulk.q("SELECT gseq FROM flatsql_arrivals WHERE type = 'OMM'");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(300));
    for (size_t i = 1; i < r.rows.size(); i++) CHECK(r.i(i, 0) > r.i(i - 1, 0));
    // _rowid = _gseq at type level; gseq order is arrivals order.
    r = inter.q("SELECT _rowid, _gseq FROM OMM ORDER BY _rowid LIMIT 10");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 10);
    for (size_t i = 0; i < r.rows.size(); i++) CHECK_EQ(r.i(i, 0), r.i(i, 1));
    // Raw stream: the stored frames verbatim.
    Outcome o;
    const auto frames = bulk.raw("SELECT _data FROM sds_p_peerA__OMM WHERE _pseq BETWEEN 1 AND 3", {}, &o);
    CHECK_EQ(o.status, 0);
    REQUIRE(frames.size() == 3);
    for (int i = 0; i < 3; i++)
        CHECK(frames[size_t(i)] == std::string(reinterpret_cast<const char*>(recs[size_t(i)].data()) + 4, recs[size_t(i)].size() - 4));
    s.close();
}

PS_TEST(lane_parking_and_cancellation) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t p1 = s.partition("peerA", ommType());
    {
        Producer a(s.e.get(), p1);
        const auto attr = buildRecordAttr("peerA", "prov", "src", "b1");
        uint64_t last = 0;
        for (int i = 0; i < 2000; i++) last = send(s.e.get(), a, ommRecord(uint32_t(i + 1), "X", ep(i), 1.0 + i, 200), attr, i);
        CHECK_EQ(a.waitAcked(last, 10000000000ull), 0);
    }
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, {p1}, 10000000000ull));
    // One lane, a 16 KiB result ring: the first statement parks, the lane
    // serves others meanwhile (A28: never blocks on output).
    ReaderConfig cfg;
    cfg.root = s.root;
    cfg.io = s.fs.get();
    cfg.cls = LaneClass::Bulk;
    cfg.lanes = 1;
    cfg.ringBytes = 16u << 10;
    Reader r(cfg);
    REQUIRE(r.inst);
    ReaderClient c(r.inst.get());
    Request big;
    big.sql = "SELECT _data FROM sds_p_peerA__OMM";
    uint32_t slot;
    REQUIRE(c.submit(big, &slot) == 0);
    // Wait until it parks.
    const uint64_t until = monoNs() + 5000000000ull;
    while (r.inst->slot(slot)->state.load() != kSlotParked && monoNs() < until) sleepNs(100000);
    CHECK_EQ(r.inst->slot(slot)->state.load(), uint32_t(kSlotParked));
    // Another statement completes on the same lane while the first is parked.
    Rows small = r.q("SELECT count(*) FROM flatsql_partitions");
    CHECK_EQ(small.status, 0);
    // Drain the parked statement fully and decode it.
    std::vector<uint8_t> out, buf(4096);
    for (;;) {
        const int64_t n = c.read(slot, buf.data(), buf.size(), 5000000000ull);
        if (n <= 0) break;
        out.insert(out.end(), buf.begin(), buf.begin() + n);
    }
    Outcome o = c.finish(slot);
    CHECK_EQ(o.status, 0);
    rb1::Decoder d;
    CHECK(d.feed(out.data(), out.size()) && d.done());
    CHECK_EQ(d.rows().size(), size_t(2000));
    CHECK(r.inst->stats().parks > 0);
    // Cancellation of a parked statement.
    REQUIRE(c.submit(big, &slot) == 0);
    while (r.inst->slot(slot)->state.load() != kSlotParked && monoNs() < until + 5000000000ull) sleepNs(100000);
    c.cancel(slot);
    for (;;) {
        const int64_t n = c.read(slot, buf.data(), buf.size(), 5000000000ull);
        if (n <= 0) break;
    }
    o = c.finish(slot);
    CHECK_EQ(o.status, int32_t(kRsCancelled));
    s.close();
}

PS_TEST(lane_sandbox_contract_A28) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t p1 = s.partition("peerA", ommType());
    {
        Producer a(s.e.get(), p1);
        const auto attr = buildRecordAttr("peerA", "prov", "src", "b1");
        uint64_t last = 0;
        for (int i = 0; i < 3000; i++) last = send(s.e.get(), a, ommRecord(uint32_t(i + 1), "X", ep(i), 1.0 + i), attr, i);
        CHECK_EQ(a.waitAcked(last, 10000000000ull), 0);
    }
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, {p1}, 10000000000ull));
    ReaderConfig cfg;
    cfg.root = s.root;
    cfg.io = s.fs.get();
    cfg.cls = LaneClass::Sandbox;
    cfg.lanes = 1;
    cfg.sandboxMaxRowsExamined = 20000;
    Reader sb(cfg);
    Reader bulk(s, LaneClass::Bulk, 1);
    const uint32_t kSb = flatsql::ps::kReqSandbox;
    // A bounded public read works.
    Rows r = sb.q("SELECT NORAD_CAT_ID FROM OMM WHERE NORAD_CAT_ID = 7", {}, kSb);
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(1));
    // Meta tables are not public; writes and multiple statements are refused.
    r = sb.q("SELECT * FROM flatsql_partitions", {}, kSb);
    CHECK(r.status != 0);
    r = sb.q("SELECT 1; SELECT 2", {}, kSb);
    CHECK_EQ(r.status, int32_t(kRsNotAuthorized));
    r = sb.q("CREATE TABLE t(x)", {}, kSb);
    CHECK(r.status != 0);
    // A cartesian join returns `timeout` (the work budget) on the sandbox lane,
    // while the bulk lane stays free.
    std::atomic<int32_t> sbStatus{1};
    std::thread t([&] { sbStatus = sb.q("SELECT count(*) FROM OMM a, OMM b", {}, kSb).status; });
    const uint64_t t0 = monoNs();
    Rows br = bulk.q("SELECT count(*) FROM sds_p_peerA__OMM");
    const uint64_t bulkNs = monoNs() - t0;
    t.join();
    CHECK_EQ(sbStatus.load(), int32_t(kRsTimeout));
    CHECK_EQ(br.status, 0);
    report("bulk_statement_during_sandbox_join_ms", double(bulkNs) / 1e6, "ms");
    // Sandbox SQL is refused on the shared bulk lane.
    r = bulk.q("SELECT NORAD_CAT_ID FROM OMM WHERE NORAD_CAT_ID = 7", {}, kSb);
    CHECK_EQ(r.status, int32_t(kRsNeedsBulk));
    s.close();
}
