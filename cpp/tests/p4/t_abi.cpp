// t_markers, t_abi: the §2.2 marker bytes and create modes, the §3.5/§3.7
// golden vectors, and every op of §3.5 through the mailbox (CONTRACT v2).
#include <cstring>

#include "internal.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;

namespace {

const char* kStoreGolden =
    "4653513404000100000102030405060708090a0b0c0d0e0f006c50c4a00100003a3010000000000001000000000000000000000000000000dd6684d300000000";
const char* kMigratedGolden = "4653514d04000000000102030405060708090a0b0c0d0e0f7b6c50c4a001000086bbeba000000000";
const char* kPutGolden =
    "0100030000004f4d4d32000f000000313244334b6f6f574578616d706c6533002900000001000900000063656c65737472616b02000c00000063"
    "656c65737472616b2d67700400020000006231340001000000003500430000000100000000000000015512202cf24dba5fb0a30e26e83b2ac5"
    "b9e29e1b161e5c1fa7425e73043362938b9824803bb16a00000000090000000500000068656c6c6f0000360008000000803bb16a00000000";
const char* kGetGolden =
    "0100030000004f4d4d0200010000000128002800000001000000015512202cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e7304336"
    "2938b9824";
const char* kPredGolden = "0a01010001c863000000000000";

const Tag kGp{"space-data-network-02", "celestrak-gp", "https://celestrak.org/gp.php", "b1", "", "", ""};

}  // namespace

P4_TEST(t_markers_golden_and_create_modes) {
    uint8_t uuid[16];
    for (int i = 0; i < 16; i++) uuid[i] = uint8_t(i);
    uint8_t st[64], mg[40];
    fp::encodeStore(st, uuid, 1790000000000, 1060922, 1);
    fp::encodeMigrated(mg, uuid, 1790000000123);
    CHECK(hex(st, 64) == kStoreGolden, hex(st, 64));
    CHECK(hex(mg, 40) == kMigratedGolden, hex(mg, 40));
    fp::Markers m;
    const auto sb = unhex(kStoreGolden), mb = unhex(kMigratedGolden);
    fp::decodeMarkers(sb.data(), sb.size(), mb.data(), mb.size(), &m);
    CHECK(m.storeValid && m.migratedValid && m.format == 4 && m.layout == 1 && m.gseqFloor == 1060922 && m.migratedFrom == 1,
          "decode");
    CHECK_EQ(m.createdMs, int64_t(1790000000000), "created");
    CHECK_EQ(m.writtenMs, int64_t(1790000000123), "written");
    // A torn STORE is present but not valid.
    auto torn = sb;
    torn[40] ^= 1;
    fp::Markers t2;
    fp::decodeMarkers(torn.data(), torn.size(), nullptr, 0, &t2);
    CHECK(t2.storePresent && !t2.storeValid, "torn STORE");

    // Create modes.
    const std::string root = scratchDir("markers") + "/fsql4";
    EngineOpts o;
    o.createMode = 0;
    CHECK_EQ(openEngine(root, o, false), P4_E_FORMAT, "mode 0 on an empty dir");
    o.createMode = 2;
    REQUIRE(openEngine(root, o, false) == P4_OK, "mode 2 creates a migration target");
    CHECK(!fp::ioExists(root + "/STORE") && !fp::ioExists(root + "/MIGRATED"), "mode 2 writes no marker before activate");
    CHECK_EQ(registerType(ommType()), P4_OK, "register");
    CHECK_EQ(flatsql_p4_activate(), P4_OK, "activate");
    CHECK_EQ(flatsql_p4_activate(), P4_OK, "activate is idempotent");
    closeEngine();
    std::vector<uint8_t> s2, m2;
    fp::ioReadAll(root + "/STORE", &s2);
    fp::ioReadAll(root + "/MIGRATED", &m2);
    fp::Markers a;
    fp::decodeMarkers(s2.data(), s2.size(), m2.data(), m2.size(), &a);
    CHECK(a.storeValid && a.migratedValid && std::memcmp(a.uuid, a.migratedUuid, 16) == 0 && a.migratedFrom == 1,
          "activated markers");
    o.createMode = 2;
    CHECK_EQ(openEngine(root, o, false), P4_E_FORMAT, "mode 2 on an activated store");
    o.createMode = 0;
    CHECK_EQ(openEngine(root, o, false), P4_OK, "mode 0 opens the activated store");
    closeEngine();
    // Mode 1: fresh store writes MIGRATED then STORE.
    const std::string root1 = scratchDir("markers1") + "/fsql4";
    o.createMode = 1;
    o.gseqFloor = 777;
    CHECK_EQ(openEngine(root1, o, false), P4_OK, "mode 1 creates");
    closeEngine();
    fp::ioReadAll(root1 + "/STORE", &s2);
    fp::ioReadAll(root1 + "/MIGRATED", &m2);
    fp::Markers b;
    fp::decodeMarkers(s2.data(), s2.size(), m2.data(), m2.size(), &b);
    CHECK(b.storeValid && b.migratedValid && b.gseqFloor == 777 && b.migratedFrom == 0, "fresh markers");
    // A newer format is refused before any write.
    uint8_t newer[64];
    fp::encodeStore(newer, b.uuid, 1, 1, 0);
    newer[4] = 5;
    fp::st32(newer + 56, flatsql::ps::crc32c(newer, 56));
    fp::ioWriteNew(root1 + "/STORE", newer, 64);
    o.createMode = 1;
    CHECK_EQ(openEngine(root1, o, false), P4_E_FORMAT, "format 5 refused");
}

P4_TEST(t_abi_golden_vectors) {
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWExample";
    b.tags.push_back(Tag{"celestrak", "celestrak-gp", "", "b1", "", "", ""});
    b.at = 1790000000;
    In in;
    in.frame = {5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'};
    in.ts = 1790000000;
    b.recs.push_back(in);
    const auto put = encodePut(b);
    CHECK_EQ(put.size(), size_t(171), "171 bytes");
    CHECK(hex(put.data(), put.size()) == kPutGolden, hex(put.data(), put.size()));
    uint8_t cid[36];
    cidOf(in.frame, cid);
    CHECK(cidText(cid) == "bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq", cidText(cid));
    TlvW g;
    g.text(1, "OMM").u8(2, 1);
    std::vector<uint8_t> c(4);
    fp::st32(c.data(), 1);
    c.insert(c.end(), cid, cid + 36);
    g.raw(40, c.data(), c.size());
    CHECK(hex(g.b.data(), g.b.size()) == kGetGolden, hex(g.b.data(), g.b.size()));
    // The predicate golden (COL0 = 25544).
    std::vector<uint8_t> p = {10, 1, 1, 0, 1};
    uint8_t v[8];
    fp::st64(v, 25544);
    p.insert(p.end(), v, v + 8);
    CHECK(hex(p.data(), p.size()) == kPredGolden, hex(p.data(), p.size()));

    // The golden PUT through the engine: "hello" is not a FlatBuffer, so the
    // record is rejected by frame size, per record, and the call succeeds.
    const std::string root = scratchDir("golden") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    TestType nb = ommType();
    REQUIRE(registerType(nb) == P4_OK, "register");
    Result r = call(P4_OPC_PUT, put);
    CHECK_EQ(r.status, P4_OK, r.err);
    REQUIRE(r.rows.size() == 1, "one row");
    CHECK_EQ(r.i(0, "action"), int64_t(P4_ACT_REJECTED), "rejected");
    CHECK_EQ(r.i(0, "reject"), int64_t(P4_REJ_FRAME_SIZE), "frame size");
    Result gr = call(P4_OPC_GET, g.b);
    CHECK_EQ(gr.status, P4_OK, gr.err);
    CHECK_EQ(gr.rows.size(), size_t(0), "a miss is no row");
    CHECK(gr.cols.size() == 18 && gr.cols[0] == "seq" && gr.cols[17] == "at", "REC columns");
    closeEngine();
}

P4_TEST(t_abi_put_get_copy_retag) {
    const std::string root = scratchDir("putget") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(ommType()) == P4_OK, "register OMM");
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWProducerA";
    b.tags.push_back(kGp);
    b.at = 1790000100;
    for (uint32_t i = 0; i < 5; i++) {
        In in;
        in.frame = ommFrame(25544 + i, "1998-067A", "2026-09-14T12:00:00", 15.5 + i);
        in.ts = 1790000000 + i;
        in.sig = {1, 2, 3};
        b.recs.push_back(in);
    }
    Result r = put(b);
    REQUIRE(r.status == P4_OK, r.err);
    REQUIRE(r.rows.size() == 5, "five rows");
    for (size_t i = 0; i < 5; i++) CHECK_EQ(r.i(i, "action"), int64_t(P4_ACT_NEW), "NEW");
    std::vector<int64_t> seqs;
    for (size_t i = 0; i < 5; i++) seqs.push_back(r.i(i, "seq"));
    for (size_t i = 1; i < 5; i++) CHECK(seqs[i] != seqs[i - 1], "distinct seqs");
    // GET each, hydrated: bytes verbatim.
    std::vector<std::vector<uint8_t>> cids;
    for (auto& in : b.recs) {
        uint8_t c[36];
        cidOf(in.frame, c);
        cids.push_back(std::vector<uint8_t>(c, c + 36));
    }
    Result g = get("OMM", cids);
    REQUIRE(g.status == P4_OK, g.err);
    REQUIRE(g.rows.size() == 5, "five hits");
    for (size_t i = 0; i < 5; i++) {
        const auto& f = b.recs[i].frame;
        CHECK(g.s(i, "data") == std::string(f.begin() + 4, f.end()), "data verbatim");
        CHECK_EQ(g.i(i, "seq"), seqs[i], "seq");
        CHECK(g.s(i, "cid") == cidText(cids[i].data()), "cid text");
        CHECK(g.s(i, "peer") == b.peer, "peer");
        CHECK_EQ(g.i(i, "ts"), int64_t(1790000000 + i), "ts");
        CHECK(g.null(i, "provider"), "GET has no tag columns");
        CHECK_EQ(g.i(i, "epoch"), unixOf(2026, 9, 14, 12), "epoch");
        CHECK_EQ(g.i(i, "key"), int64_t(25544 + i), "object key = NORAD");
        CHECK(g.s(i, "sig") == std::string("\x01\x02\x03", 3), "sig");
    }
    // Same records again: DUP; with a new tag: RETAG; from another producer: COPY.
    Result d = put(b);
    REQUIRE(d.rows.size() == 5, "dup rows");
    for (size_t i = 0; i < 5; i++) {
        CHECK_EQ(d.i(i, "action"), int64_t(P4_ACT_DUP), "DUP");
        CHECK_EQ(d.i(i, "seq"), seqs[i], "DUP keeps the seq");
    }
    Batch b2 = b;
    b2.tags[0].batch = "b2";
    Result rt = put(b2);
    for (size_t i = 0; i < 5; i++) CHECK_EQ(rt.i(i, "action"), int64_t(P4_ACT_RETAG), "RETAG");
    Batch b3 = b;
    b3.peer = "12D3KooWProducerB";
    for (auto& in : b3.recs) in.ts = 1;
    Result cp = put(b3);
    for (size_t i = 0; i < 5; i++) {
        CHECK_EQ(cp.i(i, "action"), int64_t(P4_ACT_COPY), "COPY");
        CHECK_EQ(cp.i(i, "seq"), seqs[i], "COPY keeps the holder's seq");
    }
    Result g2 = get("OMM", cids, true, true);
    CHECK_EQ(g2.rows.size(), size_t(10), "every copy");
    if (g2.rows.size() == 10) {
        CHECK_EQ(g2.i(1, "ts"), int64_t(1790000000), "COPY stores the holder's ts");
        CHECK(g2.s(1, "peer") == "12D3KooWProducerB", "COPY keeps this write's peer");
    }
    // TAGS: per (cid, tag identity), merged over copies.
    TlvW tw;
    tw.text(1, "OMM");
    std::vector<uint8_t> cl(4);
    fp::st32(cl.data(), 1);
    cl.insert(cl.end(), cids[0].begin(), cids[0].end());
    tw.raw(40, cl.data(), cl.size());
    Result tg = call(P4_OPC_TAGS, tw.b);
    CHECK_EQ(tg.status, P4_OK, tg.err);
    CHECK_EQ(tg.rows.size(), size_t(2), "two tag identities (b1, b2)");
    if (tg.rows.size() == 2) {
        CHECK(tg.s(0, "batch") == "b1" && tg.s(1, "batch") == "b2", "ordered by at");
        CHECK(tg.s(0, "source_url") == "https://celestrak.org/gp.php", "url");
    }
    // SUMMARY 1: records and copies.
    TlvW sw;
    sw.u8(45, 1);
    Result s1 = call(P4_OPC_SUMMARY, sw.b);
    CHECK_EQ(s1.status, P4_OK, s1.err);
    if (s1.rows.size() == 1) {
        CHECK_EQ(s1.i(0, "records"), int64_t(5), "records");
        CHECK_EQ(s1.i(0, "copies"), int64_t(10), "copies");
    }
    closeEngine();
    // Reopen: everything is still there (the journal tail or the flushed index).
    REQUIRE(openEngine(root) == P4_OK, "reopen");
    Result g3 = get("OMM", cids, true, true);
    CHECK_EQ(g3.rows.size(), size_t(10), "after reopen");
    Result d2 = put(b);
    for (size_t i = 0; i < 5 && i < d2.rows.size(); i++) CHECK_EQ(d2.i(i, "action"), int64_t(P4_ACT_DUP), "DUP after reopen");
    closeEngine();
}

P4_TEST(t_abi_reads) {
    const std::string root = scratchDir("reads") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(ommType()) == P4_OK, "register OMM");
    // 3 objects x 4 epochs over two months.
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWProducerA";
    b.tags.push_back(kGp);
    b.at = 1790000000;
    const int64_t days[4] = {unixOf(2026, 8, 30, 6), unixOf(2026, 8, 31, 6), unixOf(2026, 9, 1, 6), unixOf(2026, 9, 2, 6)};
    for (uint32_t o = 0; o < 3; o++)
        for (int d = 0; d < 4; d++) {
            In in;
            in.frame = ommFrame(100 + o, "OBJ" + std::to_string(o), isoTime(days[d]), 15.0 + d);
            in.ts = 1790000000;
            b.recs.push_back(in);
        }
    Result r = put(b);
    REQUIRE(r.status == P4_OK && r.rows.size() == 12, r.err);
    // SCAN asc: 12 rows in seq order; desc reversed; limit/offset.
    TlvW sc;
    sc.text(1, "OMM").u8(2, 1);
    Result s = call(P4_OPC_SCAN, sc.b);
    CHECK_EQ(s.status, P4_OK, s.err);
    CHECK_EQ(s.rows.size(), size_t(12), "scan rows");
    for (size_t i = 1; i < s.rows.size(); i++) CHECK(s.i(i, "seq") > s.i(i - 1, "seq"), "seq asc");
    if (!s.rows.empty()) CHECK(s.s(0, "source") == "celestrak-gp" && s.s(0, "batch") == "b1", "matched tag");
    TlvW sd;
    sd.text(1, "OMM").u8(5, 2).u64(3, 5).u64(4, 2);
    Result s2 = call(P4_OPC_SCAN, sd.b);
    CHECK_EQ(s2.rows.size(), size_t(5), "limit");
    if (s2.rows.size() == 5 && s.rows.size() == 12) CHECK_EQ(s2.i(0, "seq"), s.i(9, "seq"), "desc + offset");
    // HEAD: no filter, and with a COL0 predicate.
    TlvW h;
    h.text(1, "OMM");
    Result hd = call(P4_OPC_HEAD, h.b);
    CHECK_EQ(hd.status, P4_OK, hd.err);
    if (hd.rows.size() == 1) {
        CHECK_EQ(hd.i(0, "n"), int64_t(12), "n");
        CHECK_EQ(hd.i(0, "through"), s.rows.empty() ? 0 : s.i(11, "seq"), "through");
    }
    TlvW hp;
    hp.text(1, "OMM");
    std::vector<uint8_t> pred = {10, 1, 1, 0, 1};
    uint8_t v8[8];
    fp::st64(v8, 101);
    pred.insert(pred.end(), v8, v8 + 8);
    hp.raw(17, pred.data(), pred.size());
    Result hd2 = call(P4_OPC_HEAD, hp.b);
    if (hd2.rows.size() == 1) CHECK_EQ(hd2.i(0, "n"), int64_t(4), "COL0 = 101");
    // WINDOW: epoch desc, cid asc.
    TlvW wn;
    wn.text(1, "OMM").u64(3, 100);
    Result w = call(P4_OPC_WINDOW, wn.b);
    CHECK_EQ(w.status, P4_OK, w.err);
    CHECK_EQ(w.rows.size(), size_t(12), "window rows");
    for (size_t i = 1; i < w.rows.size(); i++) {
        const int64_t a = w.i(i - 1, "epoch"), c = w.i(i, "epoch");
        CHECK(a > c || (a == c && w.s(i - 1, "cid") < w.s(i, "cid")), "epoch desc, cid asc");
    }
    // WINDOW in CID order.
    TlvW wc;
    wc.text(1, "OMM").u8(5, 4);
    Result wcr = call(P4_OPC_WINDOW, wc.b);
    CHECK_EQ(wcr.rows.size(), size_t(12), "cid window");
    for (size_t i = 1; i < wcr.rows.size(); i++) CHECK(wcr.s(i - 1, "cid") < wcr.s(i, "cid"), "cid asc");
    // INDEX_PAGE: c0, epoch, cid.
    TlvW ip;
    ip.text(1, "OMM").u64(3, 5).u64(4, 1);
    Result ipr = call(P4_OPC_INDEX_PAGE, ip.b);
    CHECK_EQ(ipr.status, P4_OK, ipr.err);
    CHECK_EQ(ipr.rows.size(), size_t(5), "index page");
    if (ipr.rows.size() == 5 && w.rows.size() == 12) {
        CHECK(ipr.s(0, "cid") == w.s(1, "cid"), "offset 1 of the window order");
        CHECK(ipr.i(0, "c0") >= 100 && ipr.i(0, "c0") <= 102, "c0");
    }
    // EPOCH: nearest / as_of / forward / coverage / count.
    const int64_t at = unixOf(2026, 8, 31, 18);
    TlvW ep;
    ep.text(1, "OMM").u8(30, 2).i64(31, at);
    Result en = call(P4_OPC_EPOCH, ep.b);
    CHECK_EQ(en.status, P4_OK, en.err);
    CHECK_EQ(en.rows.size(), size_t(3), "one per entity");
    // Aug 31 06:00 and Sep 1 06:00 are both 12 h away: the tie goes to e <= at.
    for (size_t i = 0; i < en.rows.size(); i++) CHECK_EQ(en.i(i, "epoch"), days[1], "nearest");
    TlvW ea;
    ea.text(1, "OMM").u8(30, 3).i64(31, at);
    Result eas = call(P4_OPC_EPOCH, ea.b);
    for (size_t i = 0; i < eas.rows.size(); i++) CHECK_EQ(eas.i(i, "epoch"), days[1], "as_of");
    TlvW ef;
    ef.text(1, "OMM").u8(30, 4).i64(31, at);
    Result efs = call(P4_OPC_EPOCH, ef.b);
    for (size_t i = 0; i < efs.rows.size(); i++) CHECK_EQ(efs.i(i, "epoch"), days[2], "forward");
    TlvW cov;
    cov.text(1, "OMM").u8(30, 5);
    Result cv = call(P4_OPC_EPOCH, cov.b);
    CHECK_EQ(cv.rows.size(), size_t(4), "four days");
    if (cv.rows.size() == 4) CHECK(cv.s(0, "day") == "2026-08-30" && cv.i(0, "n") == 3, "coverage");
    TlvW cnt;
    cnt.text(1, "OMM").u8(30, 1).u8(33, 1);
    Result cn = call(P4_OPC_EPOCH, cnt.b);
    if (cn.rows.size() == 1) CHECK_EQ(cn.i(0, "n"), int64_t(12), "window count");
    // SUMMARY 2-5.
    for (uint8_t k = 2; k <= 5; k++) {
        TlvW sw;
        sw.u8(45, k);
        Result sm = call(P4_OPC_SUMMARY, sw.b);
        CHECK_EQ(sm.status, P4_OK, sm.err);
        CHECK(!sm.rows.empty(), "summary rows");
    }
    // Error paths: unknown type, bad request: statuses, never traps.
    TlvW nt;
    nt.text(1, "NOPE");
    CHECK_EQ(call(P4_OPC_SCAN, nt.b).status, P4_E_NOTYPE, "NOTYPE");
    std::vector<uint8_t> junk = {1, 2, 3};
    CHECK_EQ(call(P4_OPC_SCAN, junk).status, P4_E_ARG, "malformed");
#if __has_include("flatsql/p4sql/p4sql.h")
    {
        // The SQL surface is in the tree, so it is linked (never the defaults).
        Result one = call(P4_OPC_SQL, TlvW().text(70, "SELECT 1").b);
        CHECK_EQ(one.status, P4_OK, "SQL with the surface: " + one.err);
        CHECK_EQ(one.rows.size(), size_t(1), "SELECT 1 is one row");
    }
#else
    CHECK_EQ(call(P4_OPC_SQL, TlvW().text(70, "SELECT 1").b).status, P4_E_UNSUPPORTED, "SQL without the surface");
#endif
    // REBUILD verify: 0 mismatches.
    TlvW rb;
    rb.u32(63, 8);
    Result rv = call(P4_OPC_REBUILD, rb.b);
    CHECK_EQ(rv.status, P4_OK, rv.err);
    if (rv.rows.size() == 1) CHECK_EQ(rv.i(0, "mismatches"), int64_t(0), "verify");
    closeEngine();
}
