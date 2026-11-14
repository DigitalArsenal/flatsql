#include "p4_test.h"
#include "checkpoint.h"
#include "flatsql/flatsql_io.h"
#include <fstream>
#include <thread>
#include "FSG/main_generated.h"
using namespace p4t;
using namespace flatsql::p4;

FLATSQL_TEST(t_row_cache_large_segment) {
    const auto root=scratchDir("large_segment");
    const auto path=root+"/pairs.fsdata";
    Engine engine; Type type; type.e=&engine; Feed feed; feed.type=&type; feed.fid=1;
    constexpr size_t count=2100000;
    std::vector<FSGPair> pairs; pairs.reserve(count);
    for(size_t i=0;i<count;i++) pairs.emplace_back(i+1,0,i+1,0);
    flatbuffers::FlatBufferBuilder builder;
    auto vector=builder.CreateVectorOfStructs(pairs);
    FSGBuilder table(builder); table.add_FEED_ID(1); table.add_KIND(fckSegmentKind(kFsgSeq)); table.add_PAIRS(vector);
    FinishFSGBuffer(builder,table.Finish());
    CheckpointSegment desc; desc.kind=kFsgSeq; desc.entries=count; desc.length=uint32_t(builder.GetSize());
    flatsql::ps::sha256(builder.GetBufferPointer(),builder.GetSize(),desc.digest.data());
    uint8_t length[4]; st32(length,desc.length);
    { std::ofstream file(path,std::ios::binary); file.write(reinterpret_cast<char*>(length),4); file.write(reinterpret_cast<const char*>(builder.GetBufferPointer()),builder.GetSize()); }
    pairs.clear(); pairs.shrink_to_fit(); builder.Clear();
    auto stream=std::make_shared<Stream>(); stream->path=path;
    stream->h=flatsql_io_open(path.data(),int32_t(path.size()),FLATSQL_IO_READ);
    REQUIRE(stream->h>=0,"open large segment");
    std::shared_ptr<const FsgVerified> verified;
    REQUIRE(checkpointVerifyPairs(&feed,stream.get(),desc,&verified)==FLATSQL_OK,"large segment verified within default cache");
    CHECK_EQ(verified->n,uint64_t(count),"complete row order");
    CHECK(engine.rowCache->peak()<=32u<<20,"verification admission bounded");
    FsgRun run; REQUIRE(checkpointReadPairs(&feed,stream,desc,&run)==FLATSQL_OK,"reader gets acceleration");
    int32_t status=FLATSQL_OK;
    CHECK_EQ(run.lowerBound(FsgPairV{count,0,count,0},&status),count-1,"last row found");
    CHECK_EQ(status,FLATSQL_OK,"page read");
    verified.reset(); engine.rowCache->trim(std::chrono::seconds(0));
    const auto hashes=fsgVerifyStats().hashed;
    FsgRun absent;
    CHECK_EQ(checkpointReadPairs(&feed,stream,desc,&absent),FLATSQL_E_BUSY,"evicted verification never hashes on reader");
    CHECK_EQ(fsgVerifyStats().hashed,hashes,"reader hash count unchanged");
    auto corrupt=desc; corrupt.digest[0]^=1;
    CHECK_EQ(checkpointVerifyPairs(&feed,stream.get(),corrupt,&verified),FLATSQL_E_CORRUPT,"bad digest");
    flatsql_io_close(stream->h); stream->h=-1;
    CHECK_EQ(checkpointVerifyPairs(&feed,stream.get(),corrupt,&verified),FLATSQL_E_CORRUPT,"negative cache avoids rereading closed stream");
    stream.reset(); run.stream.reset(); engine.rowCache->trim(std::chrono::seconds(0));
    removeTree(root);
}

FLATSQL_TEST(t_row_cache_verification_touch) {
    auto cache=std::make_shared<PageCache>();
    auto value=std::make_shared<FsgVerified>(); value->fences.push_back(FsgPairV{1,0,1,0});
    auto pinned=cache->load<FsgVerified>("verification",sizeof(FsgVerified)+sizeof(FsgPairV),[&] {return value;});
    FsgRun run; run.cache=cache; run.cacheKey="verification"; run.verified=pinned;
    value.reset(); pinned.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(650));
    int32_t status=FLATSQL_OK;
    CHECK_EQ(run.lowerBound(FsgPairV{},&status),size_t(0),"lookup touches verification");
    std::this_thread::sleep_for(std::chrono::milliseconds(650));
    cache->trim(std::chrono::seconds(1));
    CHECK(!run.verified.expired(),"recent verification survives idle trim");
}

FLATSQL_TEST(t_row_cache_admission) {
    PageCache cache; cache.configure(4096);
    auto pinned=cache.load<int>("one",2000,[] { return std::make_shared<const int>(7); });
    REQUIRE(pinned,"first allocation");
    int loads=0;
    auto refused=cache.load<int>("two",2000,[&] { ++loads; return std::make_shared<const int>(9); });
    CHECK(!refused && loads==0,"pinned budget rejects before loading");
    CHECK_EQ(*pinned,7,"pin remains valid");
    pinned.reset();
    auto admitted=cache.load<int>("two",2000,[&] { ++loads; return std::make_shared<const int>(9); });
    REQUIRE(admitted,"unpin allows eviction");
    CHECK_EQ(loads,1,"one load");
    CHECK(cache.used()<=4096 && cache.peak()<=4096,"bounded allocations");
    admitted.reset(); cache.trim(std::chrono::seconds(0));
    CHECK_EQ(cache.used(),size_t(0),"idle release");
}

FLATSQL_TEST(t_row_cache_destruction_unlocked) {
    PageCache cache; cache.configure(4096);
    bool destroyed=false;
    auto value=cache.load<int>("generation/page/1",2000,[&] {
        return std::shared_ptr<const int>(new int(7),[&](const int* p) {
            // Reentering the cache deadlocks if destruction holds its mutex.
            CHECK(cache.used()<=4096,"destructor runs outside cache mutex");
            destroyed=true; delete p;
        });
    });
    value.reset(); cache.dropPrefix("generation/");
    CHECK(destroyed,"retirement drops unpinned generation pages");
    CHECK_EQ(cache.used(),size_t(0),"retirement accounting");
}

FLATSQL_TEST(t_row_cache_scan_and_generation_pin) {
    const auto root=scratchDir("row_cache");
    const auto path=root+"/pairs.bin";
    std::vector<FsgPairV> pairs(16384);
    for(size_t i=0;i<pairs.size();++i) pairs[i]={i+1,0,i+1,0};
    { std::ofstream out(path,std::ios::binary); out.write(reinterpret_cast<const char*>(pairs.data()),pairs.size()*sizeof(FsgPairV)); }
    auto stream=std::make_shared<Stream>(); stream->path=path;
    stream->h=flatsql_io_open(path.data(),int32_t(path.size()),FLATSQL_IO_READ);
    REQUIRE(stream->h>=0,"open synthetic row order");
    std::weak_ptr<Stream> generation=stream;
    auto cache=std::make_shared<PageCache>(); cache->configure(16384);
    auto run=std::make_unique<FsgRun>(); run->n=pairs.size(); run->stream=stream; run->cache=cache; run->cacheKey="synthetic-generation-1";
    for(size_t pg=0;pg<pairs.size()/FsgRun::kPage;pg++) {
        size_t count=0; const auto page=run->page(pg,&count);
        REQUIRE(page,"scan page");
        CHECK_EQ(count,FsgRun::kPage,"page length");
        CHECK_EQ(page[0].rid,pg*FsgRun::kPage+1,"no gap");
        CHECK(cache->used()<=16384,"scan memory bound");
    }
    size_t count=0; auto pinned=run->page(0,&count);
    REQUIRE(pinned,"reload evicted first page");
    auto second=run->page(1,&count);
    REQUIRE(second,"second live pin");
    auto refused=run->page(2,&count);
    CHECK(!refused && refused.status==FLATSQL_E_CACHE_BUDGET,"row page distinguishes exhausted cache pins");
    second={};
    auto retried=run->page(2,&count);
    REQUIRE(retried,"budget failure does not poison a later read");
    retried={};
    run.reset(); stream.reset(); cache->trim(std::chrono::seconds(0));
    CHECK(!generation.expired(),"owning page retains generation");
    CHECK_EQ(pinned[0].rid,uint64_t(1),"pinned bytes survive trim");
    pinned={}; cache->trim(std::chrono::seconds(0));
    CHECK(generation.expired(),"generation released after final pin");
    CHECK_EQ(cache->used(),size_t(0),"idle release after scan");
    removeTree(root);
}

FLATSQL_TEST(t_row_cache_concurrent_miss) {
    PageCache cache; std::atomic<int> loads{0};
    std::shared_ptr<const int> a,b;
    auto load=[&] { return cache.load<int>("shared",sizeof(int),[&] {
        ++loads;
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(!cache.coalesced() && loads.load()==1 && std::chrono::steady_clock::now()<deadline) std::this_thread::yield();
        return std::make_shared<const int>(3);
    }); };
    std::thread t([&] { a=load(); }); b=load(); t.join();
    REQUIRE(a && b,"both readers");
    CHECK_EQ(loads.load(),1,"miss coalesced");
    CHECK_EQ(cache.coalesced(),size_t(1),"second caller waited on an in-flight load");
    CHECK(a==b,"same owned allocation");
}

FLATSQL_TEST(t_row_cache_failed_load_waiters) {
    PageCache cache; std::atomic<int> entered{0},loads{0};
    std::shared_ptr<const int> a,b;
    int32_t firstStatus=-15,secondStatus=-15;
    auto load=[&](int32_t& status) {
        entered.fetch_add(1);
        while(entered.load()!=2) std::this_thread::yield();
        return cache.load<int>("failure",sizeof(int),[&]() -> std::shared_ptr<const int> {
            ++loads;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            status=-3;
            return {};
        },0,&status);
    };
    std::thread first([&] { a=load(firstStatus); }); std::thread second([&] { b=load(secondStatus); });
    first.join(); second.join();
    CHECK(!a && !b,"all waiters receive failed load");
    CHECK_EQ(loads.load(),1,"failed miss coalesced");
    CHECK_EQ(firstStatus,-3,"loader reports I/O failure");
    CHECK_EQ(secondStatus,-3,"waiter preserves the same I/O failure");
    auto retry=cache.load<int>("failure",sizeof(int),[] { return std::make_shared<const int>(5); });
    REQUIRE(retry,"later request retries failed admission");
}

FLATSQL_TEST(t_row_cache_view_ownership) {
    auto cache=std::make_shared<PageCache>(); std::weak_ptr<PageCache> weak=cache;
    auto view=cache->load<FsgView>("view",sizeof(FsgView)+sizeof(FsgRun)+128,[&] {
        auto v=std::make_shared<FsgView>(); auto run=std::make_shared<FsgRun>();
        run->cache=cache; run->n=1; v->runs[0].push_back(run); return v;
    });
    REQUIRE(view,"cached view");
    cache.reset();
    CHECK(weak.expired(),"cached runs do not form a cache ownership cycle");
    size_t count=0; const auto page=view->runs[0][0]->page(0,&count);
    CHECK(!page && page.status==FLATSQL_E_STOPPED,"view cannot use a destroyed cache");
}

FLATSQL_TEST(t_row_cache_sha256_vectors) {
    auto hex=[](const uint8_t* digest) { const char* digits="0123456789abcdef"; std::string out;
        for(size_t i=0;i<32;i++) { out.push_back(digits[digest[i]>>4]); out.push_back(digits[digest[i]&15]); } return out; };
    const std::vector<std::pair<std::string,std::string>> vectors={
        {"","e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
        {"abc","ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
        {std::string(1000000,'a'),"cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"}
    };
    for(const auto& vector:vectors) {
        const auto* bytes=reinterpret_cast<const uint8_t*>(vector.first.data()); uint8_t digest[32];
        flatsql::ps::sha256(bytes,vector.first.size(),digest);
        CHECK(hex(digest)==vector.second,"standard one-shot SHA-256 vector");
        for(size_t chunk:{size_t(1),size_t(63),size_t(64),size_t(65),size_t(4096)}) {
            flatsql::ps::Sha256 hash;
            for(size_t i=0;i<vector.first.size();i+=chunk) hash.update(bytes+i,std::min(chunk,vector.first.size()-i));
            hash.finish(digest); CHECK(hex(digest)==vector.second,"standard incremental SHA-256 vector");
        }
    }
}
