#include "QRP/main_generated.h"
// The SQL surface over the real engine (CONTRACT.md §3.5 ops 30/31, §3.9):
// records PUT through the mailbox, then op 30 (SQL) and op 31 (SURFACE)
// answered by p4sql on the engine's lanes, cursors from the engine's reader.
// Built into the engine's test binaries (native and wasm, the engine's CMake
// globs this directory).

#include "flatsql/p4/p4_reader.h"
#include "p4/p4_test.h"

using namespace p4t;

namespace {

const char* kSrc = "celestrak-gp";

Batch ommBatch(int from, int n, const std::string& source, const std::string& batch) {
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWExample";
    b.tags.push_back(Tag{"space-data-network-02", source, "", batch, "", "", ""});
    b.at = 1790000000;
    for (int i = from; i < from + n; i++) {
        In r;
        r.frame = ommFrame(uint32_t(25000 + i), "1998-0" + std::to_string(i) + "A", isoTime(1789000000 + i * 3600));
        r.ts = 1790000000 + i;
        b.recs.push_back(std::move(r));
    }
    return b;
}

std::vector<uint8_t> sqlReq(const std::string& sql, const std::vector<Cell>& params = {}) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<QRPValue>> values;
    for (const auto& c : params) {
        flatbuffers::Offset<QRPValue> v;
        switch (c.type) {
            case flatsql::ps::rb1::kInt: v = CreateQRPValue(builder, qrpValueKind_Int, false, c.i); break;
            case flatsql::ps::rb1::kReal: v = CreateQRPValue(builder, qrpValueKind_Float, false, 0, c.d); break;
            case flatsql::ps::rb1::kText: v = CreateQRPValue(builder, qrpValueKind_Text, false, 0, 0, builder.CreateString(c.s)); break;
            case flatsql::ps::rb1::kBlob: v = CreateQRPValue(builder, qrpValueKind_Bytes, false, 0, 0, 0,
                builder.CreateVector(reinterpret_cast<const uint8_t*>(c.s.data()), c.s.size())); break;
            default: v = CreateQRPValue(builder); break;
        }
        values.push_back(v);
    }
    auto text = builder.CreateString(sql);
    auto bindings = builder.CreateVector(values);
    QRPBuilder q(builder);
    q.add_SQL(text);
    q.add_PARAMETERS(bindings);
    q.add_REPRESENTATION(qrpRepresentation_FlatBuffer);
    FinishSizePrefixedQRPBuffer(builder, q.Finish());
    std::vector<uint8_t> bytes(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    return bytes;
}

Result sql(const std::string& q, uint32_t flags = 0, const std::vector<Cell>& params = {}) {
    CallOpts o;
    o.flags = flags;
    return call(FLATSQL_OPC_SQL, sqlReq(q, params), o);
}

Cell intCell(int64_t v) {
    Cell c;
    c.type = flatsql::ps::rb1::kInt;
    c.i = v;
    return c;
}

}  // namespace

// Characterizes the bounded relation before explicit unlimited registration.
FLATSQL_TEST(t_sql_bounded_visibility) {
    const auto root=scratchDir("bounded_visibility");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=2;
    REQUIRE(registerType(type)==FLATSQL_OK,"register");
    const auto batch=ommBatch(1,3,kSrc,"synthetic");
    REQUIRE(put(batch).status==FLATSQL_OK,"put");
    uint8_t cid[36]; cidOf(batch.recs[0].frame,cid);
    auto count=sql("SELECT COUNT(*) AS n FROM OMM");
    CHECK_EQ(count.status,FLATSQL_OK,count.err);
    CHECK_EQ(count.i(0,"n"),2,"bounded count");
    auto old=sql("SELECT _seq FROM OMM WHERE _cid='"+cidText(cid)+"'");
    CHECK_EQ(old.status,FLATSQL_OK,old.err);
    CHECK_EQ(old.rows.size(),size_t(0),"old CID outside relation");
    auto seq=sql("SELECT _seq FROM OMM WHERE _seq=1");
    CHECK_EQ(seq.status,FLATSQL_OK,seq.err);
    CHECK_EQ(seq.rows.size(),size_t(1),"existing sequence cutoff inconsistency");
    CHECK_EQ(closeEngine(),FLATSQL_OK,"close");
    removeTree(root);
}

// Statuses, never traps: result caps, the sandbox, malformed parameters.
FLATSQL_TEST(t_sql_unlimited_visibility) {
    const auto root=scratchDir("unlimited_visibility");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=2;
    REQUIRE(registerType(type)==FLATSQL_OK,"register bounded");
    const auto batch=ommBatch(1,8,kSrc,"synthetic");
    REQUIRE(put(batch).status==FLATSQL_OK,"put");
    CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),2,"warm old policy");
    type.a18=0;
    REQUIRE(registerType(type)==FLATSQL_OK,"promote unlimited");
    CHECK_EQ(flatsql_capabilities()&1,1,"capability");
    for(int pass=0;pass<2;++pass) {
        auto count=sql("SELECT COUNT(*) AS n FROM OMM");
        CHECK_EQ(count.status,FLATSQL_OK,count.err);
        CHECK_EQ(count.i(0,"n"),8,"all records counted");
        for(size_t i=0;i<batch.recs.size();++i) {
            uint8_t cid[36]; cidOf(batch.recs[i].frame,cid);
            auto r=sql("SELECT _seq,_data FROM OMM WHERE _cid='"+cidText(cid)+"'");
            CHECK_EQ(r.status,FLATSQL_OK,r.err);
            REQUIRE(r.rows.size()==1,"CID visible");
            CHECK_EQ(r.i(0,"_seq"),int64_t(i+1),"sequence");
            const auto& frame=batch.recs[i].frame;
            CHECK(r.s(0,"_data")==std::string(reinterpret_cast<const char*>(frame.data()+4),frame.size()-4),"exact payload");
        }
        auto scan=sql("SELECT _seq FROM OMM ORDER BY _seq");
        CHECK_EQ(scan.rows.size(),size_t(8),"complete scan");
        for(size_t i=0;i<scan.rows.size();++i) CHECK_EQ(scan.i(i,"_seq"),int64_t(i+1),"no gap or duplicate");
        auto raw=sql("SELECT _data FROM OMM",FLATSQL_SLOT_RAW);
        CHECK_EQ(raw.status,FLATSQL_OK,raw.err);
        std::set<std::vector<uint8_t>> expected;
        for(const auto& r:batch.recs) expected.insert(r.frame);
        size_t at=0;
        while(at+4<=raw.raw.size()) {
            const size_t n=flatbuffers::ReadScalar<uint32_t>(raw.raw.data()+at)+4;
            REQUIRE(n>=4 && n<=raw.raw.size()-at,"complete raw frame");
            CHECK_EQ(expected.erase(std::vector<uint8_t>(raw.raw.begin()+at,raw.raw.begin()+at+n)),size_t(1),"exact raw frame once");
            at+=n;
        }
        CHECK(at==raw.raw.size() && expected.empty(),"complete raw stream");
        CallOpts limit; limit.maxResultRows=3;
        CHECK_EQ(call(FLATSQL_OPC_SQL,sqlReq("SELECT _seq FROM OMM"),limit).status,FLATSQL_E_BUDGET,"explicit budget failure");
        CHECK_EQ(closeEngine(),FLATSQL_OK,"close");
        if(pass==0) REQUIRE(openEngine(root)==FLATSQL_OK,"reopen guarded store");
    }
    REQUIRE(openEngine(root)==FLATSQL_OK,"reopen for downgrade refusal");
    type.a18=2;
    CHECK_EQ(registerType(type),FLATSQL_E_FORMAT,"bounded downgrade refused");
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_metadata_only_visibility) {
    const auto root=scratchDir("metadata_only_visibility");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.bfbs.clear(); type.rules.clear(); type.flags=2; type.a18=0;
    REQUIRE(registerType(type)==FLATSQL_OK,"opaque registration");
    auto batch=ommBatch(1,3,kSrc,"synthetic");
    REQUIRE(put(batch).status==FLATSQL_OK,"put");
    auto r=sql("SELECT _seq,_cid,_data FROM OMM ORDER BY _seq");
    CHECK_EQ(r.status,FLATSQL_OK,r.err);
    CHECK_EQ(r.rows.size(),size_t(3),"metadata relation");
    CHECK_EQ(sql("SELECT OBJECT_NAME FROM OMM").status,FLATSQL_E_SQL,"no reflected plaintext fields");
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_caps_and_sandbox) {
    const std::string root = scratchDir("t_sql_caps");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");
    REQUIRE(put(ommBatch(1, 20, kSrc, "b1")).status == FLATSQL_OK, "put");
    CallOpts o;
    o.maxResultRows = 5;
    Result r = call(FLATSQL_OPC_SQL, sqlReq("SELECT _seq FROM OMM"), o);
    CHECK_EQ(r.status, FLATSQL_E_BUDGET, r.err);
    r = sql("PRAGMA table_info(OMM)", FLATSQL_SLOT_SANDBOX);
    CHECK_EQ(r.status, FLATSQL_E_SQL, r.err);
    r = sql("SELECT 1; SELECT 2", FLATSQL_SLOT_SANDBOX);
    CHECK_EQ(r.status, FLATSQL_E_SQL, r.err);
    r = sql("SELECT length(randomblob(100000000))", FLATSQL_SLOT_SANDBOX);   // past the lane heap cap
    CHECK_EQ(r.status, FLATSQL_E_BUDGET, r.err);
    r = sql("SELECT ?1 + ?2", 0, {intCell(1)});
    CHECK_EQ(r.status, FLATSQL_E_SQL, r.err);
    r = sql("SELECT COUNT(*) FROM OMM");   // the lane serves on
    CHECK(r.status == FLATSQL_OK && r.rows.size() == 1 && r.rows[0][0].i == 20, r.err);
    closeEngine();
    removeTree(root);
}

// SURFACE: the relation per type and per source, format 1's columns.
FLATSQL_TEST(t_sql_surface) {
    const std::string root = scratchDir("t_sql_surface");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");
    REQUIRE(put(ommBatch(1, 3, kSrc, "b1")).status == FLATSQL_OK, "put");
    const Result r = call(FLATSQL_OPC_SURFACE, {});
    CHECK_EQ(r.status, FLATSQL_OK, r.err);
    size_t base = 0, alias = 0;
    for (size_t i = 0; i < r.rows.size(); i++) {
        if (r.s(i, "name") == "OMM") {
            base++;
            CHECK(r.s(i, "kind") == "view", "OMM is a view");
            CHECK_EQ(r.i(i, "bound"), int64_t(400000), "bound");
        }
        if (r.s(i, "name") == "OMM@celestrak-gp") {
            alias++;
            CHECK(r.s(i, "kind") == "table" && r.s(i, "source") == kSrc, "per-source table");
        }
    }
    // The test schema's 9 fields, then _source, _rowid, _offset, _data.
    CHECK_EQ(base, size_t(13), "OMM columns");
    CHECK_EQ(alias, size_t(13), "OMM@celestrak-gp columns");
    closeEngine();
    removeTree(root);
}

