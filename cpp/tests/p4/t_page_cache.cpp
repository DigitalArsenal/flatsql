#include "p4_test.h"
#include "checkpoint.h"
#include "flatsql/flatsql_io.h"
#include <fstream>
#include <thread>
using namespace p4t;
using namespace flatsql::p4;

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

FLATSQL_TEST(t_row_cache_scan_and_generation_pin) {
    const auto root=scratchDir("row_cache");
    const auto path=root+"/pairs.bin";
    std::vector<FsgPairV> pairs(16384);
    for(size_t i=0;i<pairs.size();++i) pairs[i]={i+1,0,i+1,0};
    { std::ofstream out(path,std::ios::binary); out.write(reinterpret_cast<const char*>(pairs.data()),pairs.size()*sizeof(FsgPairV)); }
    auto stream=std::make_shared<Stream>(); stream->path=path;
    stream->h=flatsql_io_open(path.data(),int32_t(path.size()),FLATSQL_IO_READONLY);
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
        ++loads; std::this_thread::sleep_for(std::chrono::milliseconds(10)); return std::make_shared<const int>(3);
    }); };
    std::thread t([&] { a=load(); }); b=load(); t.join();
    REQUIRE(a && b,"both readers");
    CHECK_EQ(loads.load(),1,"miss coalesced");
    CHECK(a==b,"same owned allocation");
}
