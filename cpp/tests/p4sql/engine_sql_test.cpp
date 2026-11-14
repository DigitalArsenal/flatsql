#include "QRP/main_generated.h"
#include "flatbuffers/reflection_generated.h"
// The SQL surface over the real engine (CONTRACT.md §3.5 ops 30/31, §3.9):
// records PUT through the mailbox, then op 30 (SQL) and op 31 (SURFACE)
// answered by p4sql on the engine's lanes, cursors from the engine's reader.
// Built into the engine's test binaries (native and wasm, the engine's CMake
// globs this directory).

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
    for(int phase=0;phase<2;phase++) for(size_t cut=0;cut<=64;cut++) {
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
    for(const auto* query:{"SELECT COUNT(*) FROM OMM","SELECT COUNT(_cid) FROM OMM","SELECT _cid FROM OMM","SELECT _data FROM OMM","SELECT * FROM OMM"}) {
        CHECK_EQ(sql(query,FLATSQL_SLOT_SANDBOX).status,FLATSQL_E_SQL,"metadata-only relation excluded from sandbox");
    }
    CHECK_EQ(sql("SELECT _data FROM OMM",FLATSQL_SLOT_SANDBOX|FLATSQL_SLOT_RAW).status,FLATSQL_E_SQL,"raw shortcut cannot bypass authorizer");
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
    auto foreignType=type; foreignType.name="OTHER"; foreignType.bfbs.clear(); foreignType.rules.clear(); foreignType.flags=2;
    std::memcpy(foreignType.fid,"$OTH",4);
    REQUIRE(registerType(foreignType)==FLATSQL_OK,"register other type");
    auto foreign=ommBatch(9000,1,"other-type","synthetic"); foreign.type="OTHER";
    std::memcpy(foreign.recs[0].frame.data()+8,"$OTH",4);
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
    EngineOpts tooSmall; TlvW tiny; tiny.u64(51,16383); tooSmall.extra=tiny.b;
    CHECK_EQ(openEngine(root,tooSmall),FLATSQL_E_ARG,"reject nonzero cache below minimum");
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

