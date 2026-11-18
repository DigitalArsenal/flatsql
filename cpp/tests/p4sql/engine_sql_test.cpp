#include "QRP/main_generated.h"
#include "flatbuffers/reflection_generated.h"
// The SQL surface over the real engine (CONTRACT.md §3.5 ops 30/31, §3.9):
// records PUT through the mailbox, then op 30 (SQL) and op 31 (SURFACE)
// answered by p4sql on the engine's lanes, cursors from the engine's reader.
// Built into the engine's test binaries (native and wasm, the engine's CMake
// globs this directory).

#include <algorithm>
#include <chrono>
#include <cmath>
#include <set>

#include "flatsql/p4/p4_reader.h"
#include "p4/p4_test.h"
#include "internal.h"
#include "checkpoint.h"
#include <fstream>

namespace flatsql { namespace p4 { P4Engine* currentEngine(); } }

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

std::string joined(const std::vector<int64_t>& ids, const char* sep, const char* each = "") {
    std::string s;
    for (size_t i = 0; i < ids.size(); i++) {
        if (i) s += sep;
        s += each;
        s += std::to_string(ids[i]);
    }
    return s;
}

// `_rowid = a OR _rowid = b ...` as a balanced tree: SQLite refuses a
// left-deep chain past its expression depth (1,000), whatever the relation.
std::string orTree(const std::vector<int64_t>& ids, size_t lo, size_t hi) {
    if (hi - lo == 1) return "_rowid = " + std::to_string(ids[lo]);
    const size_t mid = lo + (hi - lo) / 2;
    return "(" + orTree(ids, lo, mid) + " OR " + orTree(ids, mid, hi) + ")";
}

// Closes the engine and removes its root however the test returns: a REQUIRE
// returns early, and an engine left open fails every later test's open.
struct OpenedEngine {
    std::string root;
    ~OpenedEngine() {
        closeEngine();
        removeTree(root);
    }
};

}  // namespace

// Characterizes the bounded relation before explicit unlimited registration.
FLATSQL_TEST(t_sql_bounded_visibility) {
    // Characterization: bounded relations retain the shipped visibility policy.
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

FLATSQL_TEST(t_sql_unlimited_promotion_recovery) {
    namespace p4 = flatsql::p4;
    const auto root=scratchDir("promotion_recovery");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=2;
    REQUIRE(registerType(type)==FLATSQL_OK,"bounded registration");
    const auto batch=ommBatch(1,8,kSrc,"synthetic");
    REQUIRE(put(batch).status==FLATSQL_OK,"acknowledged rows");
    REQUIRE(closeEngine()==FLATSQL_OK,"close");
    std::vector<uint8_t> original;
    REQUIRE(p4::ioReadAll(root+"/STORE",&original)==FLATSQL_OK,"original marker");
    auto promoted=original;
    p4::st16(promoted.data()+6,2);
    p4::st32(promoted.data()+56,flatsql::ps::crc32c(promoted.data(),56));
    // Intent, authoritative marker, and recovery's marker rewrite.
    for(int phase=0;phase<3;phase++) for(size_t cut=0;cut<=64;cut++) {
        REQUIRE(p4::ioWriteNew(root+"/STORE",original.data(),original.size())==FLATSQL_OK,"restore baseline");
        REQUIRE(p4::ioWriteNew(root+"/STORE.unlimited",promoted.data(),phase?64:cut)==FLATSQL_OK,"intent prefix");
        if(phase) REQUIRE(p4::ioWriteNew(root+"/STORE",promoted.data(),cut)==FLATSQL_OK,"torn overwrite");
        REQUIRE(openEngine(root)==FLATSQL_OK,"automatic reopen");
        auto scan=sql("SELECT _seq,_data FROM OMM ORDER BY _seq");
        // The persisted type policy is still bounded: promotion precedes it.
        CHECK_EQ(scan.status,FLATSQL_OK,scan.err);
        CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),2,"no unlimited spec persisted");
        REQUIRE(closeEngine()==FLATSQL_OK,"close recovered store");
        std::vector<uint8_t> actual;
        REQUIRE(p4::ioReadAll(root+"/STORE",&actual)==FLATSQL_OK,"recovered marker");
        CHECK(actual==(phase||cut==64?promoted:original),"exact recovery marker");
        CHECK(!p4::ioExists(root+"/STORE.unlimited"),"intent retired");
    }
    REQUIRE(openEngine(root)==FLATSQL_OK,"reopen recovered store");
    type.a18=0; REQUIRE(registerType(type)==FLATSQL_OK,"unlimited registration after recovery");
    auto scan=sql("SELECT _seq,_data FROM OMM ORDER BY _seq");
    REQUIRE(scan.status==FLATSQL_OK && scan.rows.size()==8,"all acknowledged records survived");
    for(size_t i=0;i<8;i++) {
        CHECK_EQ(scan.i(i,"_seq"),int64_t(i+1),"no gap or duplicate");
        const auto& f=batch.recs[i].frame;
        CHECK(scan.s(i,"_data")==std::string(reinterpret_cast<const char*>(f.data()+4),f.size()-4),"exact payload");
    }
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_metadata_count_consistency) {
    namespace p4 = flatsql::p4;
    const auto root=scratchDir("metadata_count");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=0;
    REQUIRE(registerType(type)==FLATSQL_OK,"unlimited registration");
    REQUIRE(put(ommBatch(1,8,kSrc,"synthetic")).status==FLATSQL_OK,"records");
    CallOpts caps; caps.flags=FLATSQL_SLOT_SANDBOX; caps.maxBytesRead=1; caps.maxRowsExamined=1;
    for(const auto* expr:{"COUNT(*)","COUNT(_cid)"}) {
        const auto result=call(FLATSQL_OPC_SQL,sqlReq(std::string("SELECT ")+expr+" AS n FROM OMM"),caps);
        CHECK_EQ(result.status,FLATSQL_OK,result.err);
        CHECK_EQ(result.i(0,"n"),8,"unfiltered count uses metadata");
    }
    auto* t=p4::currentEngine()->findType("OMM");
    t->vis.store(3);
    CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),3,"count respects visibility ceiling");
    CHECK_EQ(sql("SELECT COUNT(_cid) AS n FROM OMM").i(0,"n"),3,"CID count respects visibility ceiling");
    CHECK_EQ(sql("SELECT _cid FROM OMM").rows.size(),size_t(3),"same scan ceiling");
    t->vis.store(8);
    { std::lock_guard<std::mutex> g(t->mu); for(auto& f:t->feeds) if(f->created) f->quarantined=true; }
    CHECK_EQ(sql("SELECT COUNT(*) FROM OMM").status,FLATSQL_E_CORRUPT,"count rejects quarantine");
    CHECK_EQ(sql("SELECT COUNT(_cid) FROM OMM").status,FLATSQL_E_CORRUPT,"CID count rejects quarantine");
    CHECK_EQ(sql("SELECT _cid FROM OMM").status,FLATSQL_E_CORRUPT,"scan rejects quarantine");
    { std::lock_guard<std::mutex> g(t->mu); for(auto& f:t->feeds) f->quarantined=false; }
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_empty_root_isolation) {
    const auto root=scratchDir("empty_root_isolation");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto ordinary=ommType();
    REQUIRE(registerType(ordinary)==FLATSQL_OK,"ordinary registration");
    REQUIRE(put(ommBatch(1,3,kSrc,"synthetic")).status==FLATSQL_OK,"ordinary records");
    auto empty=ommType(); empty.name="EMPTY"; empty.rules.clear();
    const auto* schema=reflection::GetSchema(empty.bfbs.data());
    auto* fields=const_cast<flatbuffers::Vector<flatbuffers::Offset<reflection::Field>>*>(schema->root_table()->fields());
    flatbuffers::WriteScalar(reinterpret_cast<uint32_t*>(fields),uint32_t(0));
    REQUIRE(registerType(empty)==FLATSQL_OK,"legal ingest-only empty root");
    CHECK_EQ(sql("SELECT 1 AS n").i(0,"n"),1,"unrelated statement works");
    auto result=sql("SELECT _seq FROM OMM ORDER BY _seq");
    CHECK_EQ(result.status,FLATSQL_OK,result.err);
    CHECK_EQ(result.rows.size(),size_t(3),"normal relation unchanged");
    CHECK_EQ(sql("SELECT * FROM EMPTY").status,FLATSQL_E_SQL,"ingest-only relation hidden");
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_legacy_zero_and_missing_bound) {
    namespace p4 = flatsql::p4;
    const auto root=scratchDir("legacy_zero_bound");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=0;
    auto encoded=type.spec();
    std::vector<p4::Tlv> tags;
    REQUIRE(p4::tlvParse(encoded.data(),encoded.size(),&tags),"parse test spec");
    for(bool omit:{false,true}) {
        std::vector<uint8_t> legacy;
        for(const auto& tag:tags) if(tag.tag!=13 && !(omit && tag.tag==10)) p4::tlvPut(legacy,tag.tag,tag.v,tag.n);
        std::string err;
        REQUIRE(p4::engineRegisterType(p4::currentEngine(),legacy.data(),legacy.size(),&err)==FLATSQL_OK,err);
        CHECK_EQ(p4::currentEngine()->findType("OMM")->spec()->a18Bound,uint32_t(10000),"legacy default");
        CHECK_EQ(closeEngine(),FLATSQL_OK,"close");
        REQUIRE(openEngine(root)==FLATSQL_OK,"reopen explicit zero or missing bound");
        CHECK_EQ(p4::currentEngine()->markers.layout,1,"legacy zero keeps layout 1");
        CHECK_EQ(p4::currentEngine()->findType("OMM")->spec()->a18Bound,uint32_t(10000),"persisted legacy default");
    }
    REQUIRE(registerType(type)==FLATSQL_OK,"explicit unlimited flag");
    CHECK_EQ(p4::currentEngine()->markers.layout,2,"flag promotes layout");
    CHECK_EQ(p4::currentEngine()->findType("OMM")->spec()->a18Bound,uint32_t(0),"flag means unlimited");
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_registration_recovery_and_validation) {
    namespace p4 = flatsql::p4;
    const auto root=scratchDir("registration_recovery");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=2;
    REQUIRE(registerType(type)==FLATSQL_OK,"bounded registration");
    REQUIRE(put(ommBatch(1,8,kSrc,"synthetic")).status==FLATSQL_OK,"acknowledged rows");
    auto invalid=type; invalid.a18=0; invalid.rules += "col 1 str:OBJECT_NAME\n";
    CHECK_EQ(registerType(invalid),FLATSQL_E_FORMAT,"reject changed key rules");
    CHECK_EQ(p4::currentEngine()->markers.layout,1,"failed registration does not promote");
    CHECK(!p4::currentEngine()->unlimitedRelations.load(),"failed registration does not enable unlimited");
    std::vector<uint8_t> before,after;
    const auto path=root+"/T/OMM.spec";
    REQUIRE(p4::ioReadAll(path,&before)==FLATSQL_OK,"read original spec");
    type.a18=3; REQUIRE(registerType(type)==FLATSQL_OK,"new bound");
    REQUIRE(p4::ioReadAll(path,&after)==FLATSQL_OK,"read new spec");
    REQUIRE(closeEngine()==FLATSQL_OK,"close");
    const size_t cuts[]={0,1,6,after.size()/2,after.size()-1,after.size()};
    for(int phase=0;phase<2;phase++) for(size_t cut:cuts) {
        REQUIRE(p4::ioWriteNew(path,before.data(),before.size())==FLATSQL_OK,"restore original");
        REQUIRE(p4::ioWriteNew(path+".intent",after.data(),phase?after.size():cut)==FLATSQL_OK,"intent prefix");
        if(phase) REQUIRE(p4::ioWriteNew(path,after.data(),cut)==FLATSQL_OK,"spec prefix");
        REQUIRE(openEngine(root)==FLATSQL_OK,"automatic spec recovery");
        CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),phase||cut==after.size()?3:2,"old or complete new policy");
        CHECK(!p4::ioExists(path+".intent"),"intent removed");
        REQUIRE(closeEngine()==FLATSQL_OK,"close recovered store");
    }
    removeTree(root);
}

FLATSQL_TEST(t_sql_bounded_spec_recovery) {
    namespace p4 = flatsql::p4;
    const auto root=scratchDir("bounded_spec_recovery");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=2;
    REQUIRE(registerType(type)==FLATSQL_OK,"bounded registration");
    REQUIRE(put(ommBatch(1,3,kSrc,"synthetic")).status==FLATSQL_OK,"durable data");
    REQUIRE(closeEngine()==FLATSQL_OK,"close");
    const auto path=root+"/T/OMM.spec";
    const uint8_t torn=0;
    REQUIRE(p4::ioWriteNew(path,&torn,1)==FLATSQL_OK,"torn spec");
    REQUIRE(openEngine(root)==FLATSQL_OK,"bounded store skips torn spec");
    REQUIRE(registerType(type)==FLATSQL_OK,"host restores registration");
    CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),2,"bounded data preserved");
    type.a18=0;
    REQUIRE(registerType(type)==FLATSQL_OK,"promote");
    REQUIRE(closeEngine()==FLATSQL_OK,"close promoted");
    REQUIRE(p4::ioWriteNew(path,&torn,1)==FLATSQL_OK,"torn unlimited spec");
    CHECK_EQ(openEngine(root),FLATSQL_E_CORRUPT,"layout 2 refuses hidden unlimited type");
    removeTree(root);
}

FLATSQL_TEST(t_sql_registration_preflight) {
    namespace p4 = flatsql::p4;
    const auto root=scratchDir("registration_preflight");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=0;
    auto bytes=type.spec();
    REQUIRE(flatsql_validate_type(bytes.data(),int32_t(bytes.size()))==FLATSQL_OK,"validate unlimited");
    CHECK(!p4::currentEngine()->findType("OMM"),"validation creates no type");
    CHECK(!p4::ioExists(root+"/T/OMM.spec"),"validation writes no spec");
    CHECK_EQ(p4::currentEngine()->markers.layout,1,"validation keeps layout");
    type.rules="invalid rule syntax\n";
    CHECK_EQ(registerType(type),FLATSQL_E_ARG,"new invalid rules rejected before openType");
    CHECK(!p4::ioExists(root+"/T/OMM.spec"),"invalid registration writes no spec");
    CHECK_EQ(p4::currentEngine()->markers.layout,1,"invalid registration keeps layout");
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_unlimited_keyset_with_ingest) {
    const auto root=scratchDir("unlimited_keyset_ingest");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=2;
    REQUIRE(registerType(type)==FLATSQL_OK,"bounded registration");
    REQUIRE(put(ommBatch(1,512,kSrc,"initial")).status==FLATSQL_OK,"initial rows");
    CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),2,"warm readers under bounded policy");
    auto* registered=flatsql::p4::currentEngine()->findType("OMM");
    { std::lock_guard<std::mutex> g(registered->mu); for(auto& f:registered->feeds) f->quarantined=true; }
    CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),2,"bounded quarantine count preserves main");
    { std::lock_guard<std::mutex> g(registered->mu); for(auto& f:registered->feeds) f->quarantined=false; }
    type.a18=0; REQUIRE(registerType(type)==FLATSQL_OK,"promote with readers already used");
    CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),512,"cached readers see new policy");
    REQUIRE(closeEngine()==FLATSQL_OK,"checkpoint close");
    REQUIRE(openEngine(root)==FLATSQL_OK,"reopen");
    REQUIRE(put(ommBatch(513,40,kSrc,"tail")).status==FLATSQL_OK,"uncheckpointed staged rows");
    int64_t last=0;
    for(;;) {
        auto page=sql("SELECT _seq FROM OMM WHERE _seq>"+std::to_string(last)+" ORDER BY _seq LIMIT 17");
        REQUIRE(page.status==FLATSQL_OK,page.err);
        if(page.rows.empty()) break;
        for(size_t i=0;i<page.rows.size();i++) { CHECK_EQ(page.i(i,"_seq"),last+1,"no keyset gap or duplicate"); last++; }
    }
    CHECK_EQ(last,553-1,"all initial and staged rows");
    CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),last,"keyset equals count");
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_spec_all_prefixes) {
    namespace p4 = flatsql::p4;
    const auto root=scratchDir("spec_all_prefixes");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.bfbs.clear(); type.rules.clear(); type.flags=2; type.a18=2;
    REQUIRE(registerType(type)==FLATSQL_OK,"original registration");
    const auto path=root+"/T/OMM.spec";
    std::vector<uint8_t> before,after;
    REQUIRE(p4::ioReadAll(path,&before)==FLATSQL_OK,"original bytes");
    type.a18=3; REQUIRE(registerType(type)==FLATSQL_OK,"replacement");
    REQUIRE(p4::ioReadAll(path,&after)==FLATSQL_OK,"replacement bytes");
    REQUIRE(closeEngine()==FLATSQL_OK,"close");
    // All prefixes of intent, authoritative spec, and recovery spec rewrite.
    for(int phase=0;phase<3;phase++) for(size_t cut=0;cut<=after.size();cut++) {
        REQUIRE(p4::ioWriteNew(path,before.data(),before.size())==FLATSQL_OK,"baseline");
        REQUIRE(p4::ioWriteNew(path+".intent",after.data(),phase?after.size():cut)==FLATSQL_OK,"intent prefix");
        if(phase) REQUIRE(p4::ioWriteNew(path,after.data(),cut)==FLATSQL_OK,"spec prefix");
        REQUIRE(openEngine(root)==FLATSQL_OK,"recovery");
        REQUIRE(p4::currentEngine()->findType("OMM"),"registered type survives");
        CHECK_EQ(p4::currentEngine()->findType("OMM")->spec()->a18Bound,phase||cut==after.size()?3u:2u,"complete policy");
        REQUIRE(closeEngine()==FLATSQL_OK,"close recovered");
    }
    std::printf("spec prefix crash points: %zu; spec bytes: %zu\n",3*(after.size()+1),after.size());
    removeTree(root);
}

FLATSQL_TEST(t_sql_cid_only_not_queryable) {
    const auto root=scratchDir("metadata_only_visibility");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.bfbs.clear(); type.rules.clear(); type.flags=2; type.a18=0;
    REQUIRE(registerType(type)==FLATSQL_OK,"opaque registration");
    auto batch=ommBatch(1,3,kSrc,"synthetic");
    REQUIRE(put(batch).status==FLATSQL_OK,"put");
    for(uint32_t flags:{0u,uint32_t(FLATSQL_SLOT_SANDBOX),uint32_t(FLATSQL_SLOT_RAW),uint32_t(FLATSQL_SLOT_SANDBOX|FLATSQL_SLOT_RAW)}) {
        for(const auto* query:{"SELECT COUNT(*) FROM OMM","SELECT COUNT(_cid) FROM OMM","SELECT _cid FROM OMM","SELECT _data FROM OMM","SELECT * FROM OMM"}) {
            auto r=sql(query,flags);
            CHECK_EQ(r.status,FLATSQL_E_SQL,"CID-only registration has no SQL relation");
            CHECK(r.err.find("no such table")!=std::string::npos,r.err);
        }
    }
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_unlimited_reader_mutations) {
    namespace p4 = flatsql::p4;
    const auto root=scratchDir("unlimited_reader_mutations");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=0;
    REQUIRE(registerType(type)==FLATSQL_OK,"unlimited registration");
    auto batch=ommBatch(1,2048,kSrc,"old");
    REQUIRE(put(batch).status==FLATSQL_OK,"put initial records");
    auto* e=p4::currentEngine(); auto* t=e->findType("OMM");
    p4::Feed* feed=nullptr;
    { std::lock_guard<std::mutex> g(t->mu); for(auto& f:t->feeds) if(f->created && f->source==kSrc) feed=f.get(); }
    REQUIRE(feed,"feed");
    int32_t status=FLATSQL_OK;
    auto generation=p4::streamCur(feed,&status);
    REQUIRE(generation && status==FLATSQL_OK,"pin generation");
    const auto oldGen=generation->gen;
    P4Lane lane; lane.e=e;
    P4ScanSpec spec{}; spec.type="OMM"; spec.order=FLATSQL_ORDER_SEQ_ASC; spec.hydrate=1;
    P4Cursor* cursor=nullptr;
    REQUIRE(p4_cursor_open(&lane,&spec,&cursor)==FLATSQL_OK,"open cursor");
    P4Row row{};
    REQUIRE(p4_cursor_next(cursor,&row)==1 && row.seq==1,"first row");
    // Delete one buffered row and one beyond the first page. Add a retained
    // delivery before superseding the old batch in the same feed.
    uint8_t ids[4+72]; p4::st32(ids,2);
    cidOf(batch.recs[1].frame,ids+4); cidOf(batch.recs[1535].frame,ids+40);
    TlvW del; del.text(1,"OMM").raw(40,ids,sizeof ids);
    REQUIRE(call(FLATSQL_OPC_DELETE,del.b).status==FLATSQL_OK,"delete unreached records");
    auto replacement=ommBatch(3000,1,kSrc,"keep");
    REQUIRE(put(replacement).status==FLATSQL_OK,"replacement delivery");
    TlvW supersede; supersede.text(1,"OMM").text(11,"space-data-network-02").text(12,kSrc).text(60,"keep").u8(61,1);
    REQUIRE(call(FLATSQL_OPC_SUPERSEDE,supersede.b).status==FLATSQL_OK,"supersede old batch");
    p4::Internal in; in.what=feed->fid; in.force=true;
    REQUIRE(p4::runOnWriter(e,t,p4::kOpCompact,&in)==FLATSQL_OK,"compact during scan");
    auto newGeneration=p4::streamCur(feed,&status);
    REQUIRE(newGeneration && newGeneration->gen!=oldGen,"generation changed");
    std::string header;
    CHECK_EQ(p4::streamReadRange(generation.get(),0,12,&header),FLATSQL_OK,"pinned generation still readable");
    const std::string retiredPath=generation->path;
    { std::lock_guard<std::mutex> g(feed->smu);
      CHECK(feed->retiredBytes>0,"retired generation remains charged while pinned");
      feed->retireAt=0; // exercise eligibility without waiting through the grace interval
    }
    CHECK(std::ifstream(retiredPath).good(),"pinned retired file remains present");
    int64_t previous=1; size_t seen=1;
    while((status=p4_cursor_next(cursor,&row))==1) {
        CHECK(row.seq>previous && row.seq<=512,"only already buffered rows survive mutations");
        const auto& f=batch.recs[size_t(row.seq-1)].frame;
        CHECK(row.dataLen==f.size()-4 && std::memcmp(row.data,f.data()+4,row.dataLen)==0,"buffered bytes unchanged");
        previous=row.seq; seen++;
    }
    CHECK_EQ(status,0,"cursor completed without freed-generation access");
    CHECK(seen>1 && seen<=512,"page snapshot retained");
    p4_cursor_close(cursor); generation.reset(); newGeneration.reset();
    e->rowCache->trim(std::chrono::seconds(0));
    bool reclaimed=false;
    const auto reclaimDeadline=flatsql::ps::monoNs()+5000000000ull;
    while(flatsql::ps::monoNs()<reclaimDeadline) {
        { std::lock_guard<std::mutex> g(feed->smu); reclaimed=!feed->retired && feed->retiredBytes==0; }
        if(reclaimed) break;
        flatsql::ps::sleepNs(10000000);
    }
    CHECK(reclaimed,"retired byte charge clears after the last pin");
    CHECK(!std::ifstream(retiredPath).good(),"retired file unlinked before charge clears");
    for(int pass=0;pass<2;pass++) {
        auto fresh=sql("SELECT _seq,_data FROM OMM ORDER BY _seq");
        REQUIRE(fresh.status==FLATSQL_OK && fresh.rows.size()==1,"new reader sees only retained batch");
        CHECK_EQ(sql("SELECT COUNT(*) AS n FROM OMM").i(0,"n"),1,"count agrees");
        for(size_t i:{size_t(0),size_t(1),size_t(1535)}) {
            uint8_t cid[36]; cidOf(batch.recs[i].frame,cid);
            CHECK(sql("SELECT _seq FROM OMM WHERE _cid='"+cidText(cid)+"'").rows.empty(),"deleted or superseded record stays invisible");
        }
        REQUIRE(closeEngine()==FLATSQL_OK,"close");
        if(!pass) REQUIRE(openEngine(root)==FLATSQL_OK,"reopen");
    }
    removeTree(root);
}

FLATSQL_TEST(t_sql_cid_equality_semantics) {
    const auto root=scratchDir("cid_equality_semantics");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=0;
    REQUIRE(registerType(type)==FLATSQL_OK,"register");
    auto batch=ommBatch(1,4,kSrc,"synthetic");
    REQUIRE(put(batch).status==FLATSQL_OK,"put");
    auto other=ommBatch(20,1,"another-feed","synthetic");
    REQUIRE(put(other).status==FLATSQL_OK,"other feed");
    auto foreignType=type; foreignType.name="OTHER"; foreignType.rules.clear();
    REQUIRE(registerType(foreignType)==FLATSQL_OK,"register other type");
    auto foreign=ommBatch(9000,1,"other-type","synthetic"); foreign.type="OTHER";
    REQUIRE(put(foreign).status==FLATSQL_OK,"put other type");
    uint8_t foreignCid[36]; cidOf(foreign.recs[0].frame,foreignCid);
    CHECK_EQ(sql("SELECT _cid FROM OTHER").rows.size(),size_t(1),"other type has record");
    CHECK(sql("SELECT _seq FROM OMM WHERE _cid='"+cidText(foreignCid)+"'").rows.empty(),"CID pushdown excludes other type");
    uint8_t cid[36]; cidOf(batch.recs[0].frame,cid);
    const auto text=cidText(cid); std::string upper=text;
    for(auto& c:upper) if(c>='a' && c<='z') c=char(c-'a'+'A');
    std::vector<Cell> values;
    for(const auto& value:{text,upper,text+"x",std::string("invalid")}) {
        Cell c; c.type=flatsql::ps::rb1::kText; c.s=value; values.push_back(c);
    }
    values.push_back(intCell(7)); values.push_back(Cell{});
    Cell blob; blob.type=flatsql::ps::rb1::kBlob; blob.s=text; values.push_back(blob);
    for(const auto& value:values) for(const auto* coll:{"BINARY","NOCASE"}) {
        const std::string suffix=std::string("=?1 COLLATE ")+coll+" ORDER BY _seq";
        const auto indexed=sql("SELECT _seq FROM OMM WHERE _cid"+suffix,0,{value});
        const auto scanned=sql("SELECT _seq FROM OMM WHERE (_cid||'')"+suffix,0,{value});
        REQUIRE(indexed.status==FLATSQL_OK && scanned.status==FLATSQL_OK,"both equality forms succeed");
        CHECK_EQ(indexed.rows.size(),scanned.rows.size(),"pushdown matches SQLite equality");
        for(size_t i=0;i<std::min(indexed.rows.size(),scanned.rows.size());i++) CHECK_EQ(indexed.i(i,"_seq"),scanned.i(i,"_seq"),"same rows");
    }
    cidOf(other.recs[0].frame,cid);
    const auto otherText=cidText(cid);
    CHECK_EQ(sql("SELECT _seq FROM OMM WHERE _cid='"+otherText+"'").rows.size(),size_t(1),"other feed reachable");
    CHECK(sql("SELECT _seq FROM \"OMM@"+std::string(kSrc)+"\" WHERE _cid='"+otherText+"'").rows.empty(),"source relation excludes other feed");
    closeEngine(); removeTree(root);
}

FLATSQL_TEST(t_sql_cache_pin_budget) {
    namespace p4=flatsql::p4;
    const auto root=scratchDir("sql_cache_pin_budget");
    EngineOpts tooSmall; TlvW tiny; tiny.u64(52,16383); tooSmall.extra=tiny.b;
    CHECK_EQ(openEngine(root,tooSmall),FLATSQL_E_ARG,"reject nonzero cache below minimum");
    EngineOpts convTagCheck; convTagCheck.createMode=3; TlvW convTags; convTags.u64(51,3).u64(52,65536); convTagCheck.extra=convTags.b;
    CHECK_EQ(openEngine(root,convTagCheck),FLATSQL_E_FORMAT,"conversion tag 51 coexists with rowCacheBytes tag 52");
    REQUIRE(openEngine(root)==FLATSQL_OK,"open");
    auto type=ommType(); type.a18=0;
    REQUIRE(registerType(type)==FLATSQL_OK,"register");
    REQUIRE(put(ommBatch(1,2048,kSrc,"synthetic")).status==FLATSQL_OK,"put");
    REQUIRE(closeEngine()==FLATSQL_OK,"checkpoint on close");
    REQUIRE(openEngine(root)==FLATSQL_OK,"reopen");
    auto* e=p4::currentEngine(); auto* t=e->findType("OMM");
    p4::Feed* feed=nullptr;
    { std::lock_guard<std::mutex> g(t->mu); for(auto& f:t->feeds) if(f->created) feed=f.get(); }
    REQUIRE(feed && feed->checkpoint,"checkpoint");
    auto ck=feed->checkpoint; int32_t status=0; auto stream=p4::streamCur(feed,&status);
    REQUIRE(stream && status==FLATSQL_OK,"generation");
    std::vector<std::shared_ptr<const p4::FsgVerified>> verified;
    for(const auto& d:ck->segments) if(d.kind==p4::kFsgSeq) {
        std::shared_ptr<const p4::FsgVerified> v;
        REQUIRE(p4::checkpointVerifyPairs(feed,stream.get(),d,&v)==FLATSQL_OK,"verify before pressure");
        verified.push_back(v);
    }
    REQUIRE(!verified.empty(),"row order exists");
    REQUIRE(sql("SELECT _seq FROM OMM ORDER BY _seq LIMIT 1").status==FLATSQL_OK,"prepare cached view");
    auto view=feed->fsg[0].lock(); REQUIRE(view,"view is cache-owned");
    e->rowCache->trim(std::chrono::seconds(0));
    const size_t limit=1u<<20; e->rowCache->configure(limit);
    const auto used=e->rowCache->used(); REQUIRE(used+2048<limit,"space for pressure pin");
    auto pin=e->rowCache->load<int>("test/sql-pressure",limit-used-2048,[]{ return std::make_shared<int>(1); });
    REQUIRE(pin,"hold cache pressure");
    const auto failed=sql("SELECT _seq FROM OMM ORDER BY _seq LIMIT 1");
    CHECK_EQ(failed.status,FLATSQL_E_CACHE_BUDGET,"page admission has distinct SQL status");
    pin.reset(); e->rowCache->trim(std::chrono::seconds(0));
    const auto retried=sql("SELECT _seq FROM OMM ORDER BY _seq LIMIT 1");
    CHECK_EQ(retried.status,FLATSQL_OK,retried.err);
    CHECK_EQ(retried.rows.size(),size_t(1),"query succeeds after releasing pin");
    view.reset(); e->rowCache->trim(std::chrono::seconds(0));
    CHECK(feed->fsg[0].expired(),"feed does not retain an evicted row-order view");
    CHECK(e->rowCache->used()<=limit,"views and pages share the admission budget");
    verified.clear(); stream.reset(); closeEngine(); removeTree(root);
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

// Rowid lookups spread over a large type (stack issue 676). SQLite answers
// `_rowid IN (...)`, and an OR of `_rowid =` terms, with one xFilter, so one
// reader cursor, per value. Each cursor's A18 cut walked the type's seqs from
// its rowid down to the bound N, below the range it reads, and counted every
// seq walked as a row examined: 16 rowids spread over 150,000 records walked
// over 1,000,000 seqs and tripped the sandbox's rows-examined cap ("reader
// cursor failed"), and 7 near the newest end did too.
FLATSQL_TEST(t_sql_scattered_rowid_lookups) {
    const std::string root = scratchDir("t_sql_rowid_lookups");
    EngineOpts eo;
    TlvW req;
    req.u32(10, 1u << 20);   // read requests up to 1 MiB (the JS host gives 8 MiB): a 10,000-value IN list
    eo.extra = req.b;
    REQUIRE(openEngine(root, eo) == FLATSQL_OK, "open");
    const OpenedEngine opened{root};
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");   // A18 bound 400,000: every record is in the window
    const int kRecords = 150000, kBatch = 5000;
    for (int from = 1; from <= kRecords; from += kBatch)
        REQUIRE(put(ommBatch(from, kBatch, kSrc, "b" + std::to_string(from))).status == FLATSQL_OK, "put");
    // Every record's rowid and NORAD id, in 10,000-row pages.
    std::vector<int64_t> rowids;
    std::map<int64_t, int64_t> norad;
    for (int64_t after = 0;;) {
        const Result r = sql("SELECT _rowid AS RID, NORAD_CAT_ID AS N FROM OMM WHERE _rowid > ?1 ORDER BY _rowid LIMIT 10000",
                             FLATSQL_SLOT_SANDBOX, {intCell(after)});
        REQUIRE(r.status == FLATSQL_OK, r.err);
        if (r.rows.empty()) break;
        for (size_t i = 0; i < r.rows.size(); i++) {
            rowids.push_back(r.i(i, "RID"));
            norad[r.i(i, "RID")] = r.i(i, "N");
        }
        after = rowids.back();
    }
    REQUIRE(rowids.size() == size_t(kRecords), "every record has a rowid");
    auto at = [&](std::initializer_list<size_t> positions) {
        std::vector<int64_t> v;
        for (size_t p : positions) v.push_back(rowids[p]);
        return v;
    };
    // n rowids spread over the whole type, out of order (a stride prime to the count).
    auto spread = [&](size_t n) {
        std::vector<int64_t> v;
        for (size_t j = 0; j < n; j++) v.push_back(rowids[(j * 7919 + 16 * 7919) % rowids.size()]);
        return v;
    };
    auto lookup = [&](const std::string& what, const std::string& where, const std::vector<int64_t>& want,
                      const std::string& from = "OMM") {
        const auto t0 = std::chrono::steady_clock::now();
        const Result r = sql("SELECT _rowid AS RID, NORAD_CAT_ID AS N FROM " + from + " WHERE " + where, FLATSQL_SLOT_SANDBOX);
        if (want.size() >= 10000) {
            report(("rowid lookups." + what + ".ms").c_str(),
                   std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count(), "ms");
            report(("rowid lookups." + what + ".rows_examined").c_str(), double(r.rowsExamined), "rows");
        }
        CHECK_EQ(r.status, FLATSQL_OK, what + ": " + r.err);
        CHECK_EQ(r.rows.size(), want.size(), what);
        std::set<int64_t> got;
        bool own = true;
        for (size_t i = 0; i < r.rows.size(); i++) {
            got.insert(r.i(i, "RID"));
            own = own && norad[r.i(i, "RID")] == r.i(i, "N");
        }
        CHECK(got == std::set<int64_t>(want.begin(), want.end()), what + ": the rows asked for");
        CHECK(own, what + ": each row from its own record");
        // A lookup examines its own record, not the window below it: 3 rows on
        // this type of one feed file (the cut's query and the seq it takes, then
        // the row).
        CHECK(r.rowsExamined <= 4 * want.size(), what + ": " + std::to_string(r.rowsExamined) + " rows examined");
    };
    // The issue's rowids: 15 answered, the 16th (in any order) failed.
    std::vector<int64_t> issue;
    for (size_t i = 0; i < 16; i++) issue.push_back(rowids[((16 + i) * 7919) % 147000]);
    lookup("the issue's 15", "_rowid IN (" + joined({issue.begin(), issue.end() - 1}, ",") + ")",
           {issue.begin(), issue.end() - 1});
    std::vector<int64_t> reversed(issue.rbegin(), issue.rend());
    lookup("the issue's 16", "_rowid IN (" + joined(reversed, ",") + ")", issue);
    // Not a count: 7 rowids among the newest failed the same way.
    const std::vector<int64_t> newest = at({149999, 149000, 148000, 147000, 146000, 145000, 144000});
    lookup("7 newest", "_rowid IN (" + joined(newest, ",") + ")", newest);
    for (size_t n : {size_t(16), size_t(64), size_t(1024), size_t(10000)}) {
        const std::vector<int64_t> ids = spread(n);
        lookup(std::to_string(n) + " IN", "_rowid IN (" + joined(ids, ",") + ")", ids);
        lookup(std::to_string(n) + " OR", orTree(ids, 0, ids.size()), ids);
        if (n <= 64) lookup(std::to_string(n) + " OR chain", joined(ids, " OR ", "_rowid = "), ids);
    }
    // The per-source relation reads the same cursors (its lane filter's files).
    const std::vector<int64_t> some = spread(1024);
    lookup("1024 IN, per source", "_rowid IN (" + joined(some, ",") + ")", some, "\"OMM@" + std::string(kSrc) + "\"");
    // The store serves on: a count, a contiguous list, a write and its read-back.
    Result r = sql("SELECT COUNT(*) FROM OMM", FLATSQL_SLOT_SANDBOX);
    CHECK(r.status == FLATSQL_OK && r.rows.size() == 1 && r.rows[0][0].i == kRecords, r.err);
    lookup("256 contiguous", "_rowid IN (" + joined({rowids.begin(), rowids.begin() + 256}, ",") + ")",
           {rowids.begin(), rowids.begin() + 256});
    REQUIRE(put(ommBatch(kRecords + 1, 16, kSrc, "after")).status == FLATSQL_OK, "put after");
    r = sql("SELECT _rowid AS RID FROM OMM WHERE _rowid > ?1 ORDER BY _rowid", FLATSQL_SLOT_SANDBOX, {intCell(rowids.back())});
    CHECK(r.status == FLATSQL_OK && r.rows.size() == 16, r.err);
    std::vector<int64_t> fresh;
    for (size_t i = 0; i < r.rows.size(); i++) fresh.push_back(r.i(i, "RID"));
    std::vector<int64_t> mixed = fresh;
    for (int64_t id : spread(16)) mixed.push_back(id);
    for (size_t i = 0; i < r.rows.size(); i++) norad[fresh[i]] = kRecords + 1 + int64_t(i) + 25000;
    lookup("16 new and 16 old", "_rowid IN (" + joined(mixed, ",") + ")", mixed);
}

// Every reader cursor a statement opens is charged against the rows-examined
// cap for its work. The A18 cut runs one query in each feed file of its bound
// on every open (the type's, or the source's through "<TYPE>@<source>"),
// found or not, and each query counts one row; an open past the newest rowid
// reads no file and counts one. Charged only the rows they found (2 for a
// hit, 0 for a miss) whatever the feed files, lookups on a type of many feed
// files ran for minutes under the cap.
FLATSQL_TEST(t_sql_lookup_opens_charged) {
    const std::string root = scratchDir("t_sql_lookup_opens");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    const OpenedEngine opened{root};
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");   // A18 bound 400,000: every record is in the window
    const int kFeeds = 16, kEach = 64;   // one feed file per source
    auto source = [](int k) { return "feed-" + std::to_string(k); };
    for (int k = 0; k < kFeeds; k++)
        REQUIRE(put(ommBatch(1 + k * kEach, kEach, source(k), "b" + std::to_string(k))).status == FLATSQL_OK, "put");
    const Result all = sql("SELECT _rowid AS RID, _source AS S FROM OMM ORDER BY _rowid", FLATSQL_SLOT_SANDBOX);
    REQUIRE(all.status == FLATSQL_OK && all.rows.size() == size_t(kFeeds * kEach), all.err);
    std::vector<int64_t> rowids;
    std::map<std::string, std::vector<int64_t>> bySource;
    for (size_t i = 0; i < all.rows.size(); i++) {
        rowids.push_back(all.i(i, "RID"));
        bySource[all.s(i, "S")].push_back(all.i(i, "RID"));
    }
    REQUIRE(bySource.size() == size_t(kFeeds), "one feed file per source");
    const int64_t first = rowids.front(), newest = rowids.back();
    REQUIRE(newest - first == int64_t(rowids.size()) - 1, "the records' rowids are contiguous");
    const uint64_t n = 64;
    auto examined = [&](const std::string& what, const std::string& q, size_t rows, uint64_t lo, uint64_t hi) {
        const Result r = sql(q, FLATSQL_SLOT_SANDBOX);
        CHECK_EQ(r.status, FLATSQL_OK, what + ": " + r.err);
        CHECK_EQ(r.rows.size(), rows, what);
        CHECK(r.rowsExamined >= lo && r.rowsExamined <= hi,
              what + ": " + std::to_string(r.rowsExamined) + " rows examined, want " + std::to_string(lo) + ".." +
                  std::to_string(hi));
    };
    // Hits on the type: each probes the type's 16 feed files, then takes and reads its record (18).
    std::vector<int64_t> hits;
    for (uint64_t j = 0; j < n; j++) hits.push_back(rowids[(j * 97) % rowids.size()]);
    examined("64 hits", "SELECT _rowid FROM OMM WHERE _rowid IN (" + joined(hits, ",") + ")", n, n * kFeeds,
             n * (kFeeds + 4));
    // Misses through a source's relation (feed-0's rowids looked up in feed-1): each probes feed-1's one file.
    const std::string other = "\"OMM@" + source(1) + "\"";
    examined("64 misses, per source",
             "SELECT _rowid FROM " + other + " WHERE _rowid IN (" + joined(bySource["OMM@" + source(0)], ",") + ")", 0,
             n, 4 * n);
    // Past the newest rowid: no file is read; each open counts one.
    std::vector<int64_t> past;
    for (uint64_t j = 1; j <= n; j++) past.push_back(newest + 1000000000 + int64_t(j));
    examined("64 past the newest", "SELECT _rowid FROM OMM WHERE _rowid IN (" + joined(past, ",") + ")", 0, n, 4 * n);
    examined("64 past the newest, per source",
             "SELECT _rowid FROM " + other + " WHERE _rowid IN (" + joined(past, ",") + ")", 0, n, 4 * n);
    // A statement of 400,000 hits stops at the sandbox's cap (1,000,000), 18
    // each: after about 55,000 opens, within seconds and one open past the
    // cap at most. Charged 2 each, all of them answered, in about 40 s.
    const uint64_t cap = 1000000;
    const auto t0 = std::chrono::steady_clock::now();
    const Result many = sql("WITH RECURSIVE k(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM k WHERE i < 399999) "
                            "SELECT COUNT(*) AS C FROM k CROSS JOIN OMM WHERE OMM._rowid = ?1 + k.i % ?2",
                            FLATSQL_SLOT_SANDBOX, {intCell(first), intCell(int64_t(rowids.size()))});
    const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
    report("lookup opens.400k hits on 16 feed files.ms", ms, "ms");
    report("lookup opens.400k hits on 16 feed files.rows_examined", double(many.rowsExamined), "rows");
    CHECK_EQ(many.status, FLATSQL_E_BUDGET, "400,000 hits on 16 feed files: " + std::to_string(many.rows.size()) +
                                                " rows, " + std::to_string(many.rowsExamined) + " examined, " + many.err);
    CHECK(many.rowsExamined > cap && many.rowsExamined <= cap + kFeeds + 2,
          "stopped at the cap: " + std::to_string(many.rowsExamined) + " rows examined");
    // The store serves on.
    const Result count = sql("SELECT COUNT(*) FROM OMM", FLATSQL_SLOT_SANDBOX);
    CHECK(count.status == FLATSQL_OK && count.rows.size() == 1 && count.rows[0][0].i == kFeeds * kEach, count.err);
}

// The A18 window of a range read stays what it was: the newest N records at
// or below the range's top, then the range (a point lookup always finds its
// record). The lookups' fix only stops the cut's walk at the range's floor.
FLATSQL_TEST(t_sql_window_cut_ranges) {
    const std::string root = scratchDir("t_sql_window_cut");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    const OpenedEngine opened{root};
    TestType small = ommType();
    small.a18 = 100;
    REQUIRE(registerType(small) == FLATSQL_OK, "register");
    REQUIRE(put(ommBatch(1, 1000, kSrc, "b1")).status == FLATSQL_OK, "put");
    const Result first = sql("SELECT _rowid AS RID FROM OMM ORDER BY _rowid");
    REQUIRE(first.status == FLATSQL_OK && first.rows.size() == 100, first.err);   // the newest 100
    const int64_t top = first.i(99, "RID"), base = top - 999;   // rowids base..top, one per record
    CHECK_EQ(first.i(0, "RID"), top - 99, "the window is the newest 100");
    auto count = [&](const std::string& where) {
        const Result r = sql("SELECT COUNT(_rowid) AS C, MIN(_rowid) AS LO, MAX(_rowid) AS HI FROM OMM WHERE " + where);
        return r.status == FLATSQL_OK && r.rows.size() == 1
                   ? std::vector<int64_t>{r.i(0, "C"), r.i(0, "LO"), r.i(0, "HI")}
                   : std::vector<int64_t>{-1, -1, -1};
    };
    auto expect = [&](const std::string& where, int64_t n, int64_t lo, int64_t hi) {
        const std::vector<int64_t> got = count(where);
        CHECK(got == (std::vector<int64_t>{n, lo, hi}),
              where + ": " + std::to_string(got[0]) + " rows " + std::to_string(got[1]) + ".." + std::to_string(got[2]));
    };
    const std::string b = std::to_string(base);
    auto at = [&](int64_t k) { return std::to_string(base + k); };   // the (k+1)-th record's rowid
    expect("_rowid <= " + at(499), 100, base + 400, base + 499);                              // newest 100 at or below
    expect("_rowid > " + at(299) + " AND _rowid <= " + at(499), 100, base + 400, base + 499);  // 200 in range, cut to 100
    expect("_rowid BETWEEN " + at(449) + " AND " + at(499), 51, base + 449, base + 499);      // fewer than 100 in range
    expect("_rowid BETWEEN " + at(400) + " AND " + at(499), 100, base + 400, base + 499);     // exactly 100
    expect("_rowid BETWEEN " + at(399) + " AND " + at(499), 100, base + 400, base + 499);     // 101: the oldest is cut
    expect("_rowid IN (" + b + ", " + at(499) + ", " + at(999) + ")", 3, base, base + 999);    // each lookup finds its record
    expect("_rowid > " + at(899), 100, base + 900, base + 999);
    expect("_rowid > " + at(949), 50, base + 950, base + 999);
    expect("_rowid >= " + b + " AND _rowid <= " + at(0), 1, base, base);   // the oldest record, by its own range
}

// SQLite's built-in math functions (SQLITE_ENABLE_MATH_FUNCTIONS; stack
// issue 676: "no such function: pow"), in the sandbox and over a relation's
// columns.
FLATSQL_TEST(t_sql_math_functions) {
    const std::string root = scratchDir("t_sql_math");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    const OpenedEngine opened{root};
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");
    REQUIRE(put(ommBatch(1, 3, kSrc, "b1")).status == FLATSQL_OK, "put");
    auto real = [](const Result& r, size_t row, const std::string& c) {
        const int k = r.col(c);
        return k < 0 || row >= r.rows.size() || r.rows[row][size_t(k)].type != flatsql::ps::rb1::kReal
                   ? std::nan("")
                   : r.rows[row][size_t(k)].d;
    };
    auto near = [](double a, double b) { return std::fabs(a - b) <= 1e-12 * std::max(1.0, std::fabs(b)); };
    const Result r = sql("SELECT pow(2, 10) AS P, power(1.5, 2) AS PW, sqrt(2.25) AS Q, sin(0.5) AS S, cos(0.5) AS C, "
                         "atan2(1, 2) AS A, ln(10) AS L, log10(1000) AS G, log2(8) AS L2, exp(1) AS E, pi() AS PI, "
                         "degrees(pi()) AS D, radians(180) AS R, mod(7.5, 2) AS M, floor(-1.5) AS F, ceil(1.25) AS CE",
                         FLATSQL_SLOT_SANDBOX);
    REQUIRE(r.status == FLATSQL_OK && r.rows.size() == 1, r.err);
    const double pi = std::acos(-1.0);
    const std::pair<const char*, double> want[] = {
        {"P", 1024.0},        {"PW", 2.25},       {"Q", 1.5},    {"S", std::sin(0.5)},
        {"C", std::cos(0.5)}, {"A", std::atan2(1.0, 2.0)},     {"L", std::log(10.0)}, {"G", 3.0},
        {"L2", 3.0},          {"E", std::exp(1.0)}, {"PI", pi},  {"D", 180.0},
        {"R", pi},            {"M", 1.5},         {"F", -2.0},   {"CE", 2.0},
    };
    for (const auto& w : want)
        CHECK(near(real(r, 0, w.first), w.second), std::string(w.first) + " = " + std::to_string(real(r, 0, w.first)));
    // Over a relation: MEAN_MOTION is 15.5 in every test OMM.
    const Result o = sql("SELECT pow(MEAN_MOTION, 2) AS MM2, sqrt(MEAN_MOTION * 4) AS S FROM OMM ORDER BY _rowid", FLATSQL_SLOT_SANDBOX);
    REQUIRE(o.status == FLATSQL_OK && o.rows.size() == 3, o.err);
    for (size_t i = 0; i < o.rows.size(); i++) {
        CHECK(near(real(o, i, "MM2"), 240.25), "pow over a column");
        CHECK(near(real(o, i, "S"), std::sqrt(62.0)), "sqrt over a column");
    }
    // Out of a function's domain: NULL, as SQLite documents.
    const Result d = sql("SELECT sqrt(-1) AS N, ln(0) AS Z", FLATSQL_SLOT_SANDBOX);
    CHECK(d.status == FLATSQL_OK && d.rows.size() == 1 && d.null(0, "N") && d.null(0, "Z"), d.err);
}
