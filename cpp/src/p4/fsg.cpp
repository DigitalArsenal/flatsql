// FlatBuffer row orders (store format 5, FORMAT.md §3.3, §7): a feed's
// ROWS_BY_SEQ and ROWS_BY_OBJECT_EPOCH as immutable sorted FSGPair runs, the
// checkpoint's segments (read from the stream a page at a time) plus the
// tail the $FMB chain added since.
#include "checkpoint.h"
#include <algorithm>
namespace flatsql { namespace p4 {

FsgPage FsgRun::page(size_t pg,size_t* count,int ahead) const {
    (void)ahead;
    if(pg>=n/kPage+(n%kPage!=0)) { *count=0; return {nullptr,{},FLATSQL_E_CORRUPT}; }
    const size_t first=pg*kPage; *count=std::min(kPage,n-first);
    if(p) return {p+first,keep};
    struct Page { std::vector<FsgPairV> pairs; std::shared_ptr<Stream> generation; };
    auto sharedCache=cache.lock();
    if(!sharedCache) return {nullptr,{},FLATSQL_E_STOPPED};
    int32_t status=FLATSQL_E_CACHE_BUDGET;
    auto value=sharedCache->load<Page>(cacheKey+"/page/"+std::to_string(pg),*count*sizeof(FsgPairV)+sizeof(Page),[&]() -> std::shared_ptr<const Page> {
        std::string buf;
        status=streamReadRange(stream.get(),int64_t(base+first*sizeof(FsgPairV)),*count*sizeof(FsgPairV),&buf);
        if(status!=FLATSQL_OK) return {};
        if(buf.size()!=*count*sizeof(FsgPairV)) { status=FLATSQL_E_CORRUPT; return {}; }
        auto v=std::make_shared<Page>(); v->generation=stream; v->pairs.resize(*count);
        std::memcpy(v->pairs.data(),buf.data(),buf.size());
        if(!std::is_sorted(v->pairs.begin(),v->pairs.end())) { status=FLATSQL_E_CORRUPT; return {}; }
        return v;
    },*count*sizeof(FsgPairV),&status);
    return value?FsgPage{value->pairs.data(),value}:FsgPage{nullptr,{},status};
}

FsgPairV FsgRun::at(size_t i,int32_t* status) const {
    size_t count=0; const auto q=page(i/kPage,&count);
    if(!q) *status=q.status;
    return q?q[i%kPage]:FsgPairV{UINT64_MAX,UINT64_MAX,UINT64_MAX,0};
}

// Interpolation on KEY_HI (rids and object ids are dense), one page a step,
// then a binary search inside the page it lands in.
size_t FsgRun::lowerBound(const FsgPairV& key,int32_t* status) const {
    const auto sharedCache=cache.lock();
    const auto fences=sharedCache ? sharedCache->find<FsgVerified>(cacheKey) : verified.lock();
    if(fences && !fences->fences.empty()) {
        // The fence keys (in memory) name the one page the answer lies in.
        const auto& fc=fences->fences;
        const size_t L=size_t(std::lower_bound(fc.begin(),fc.end(),key)-fc.begin());
        if(L==0) return 0;
        size_t count=0; const auto q=page(L-1,&count); if(!q) { *status=q.status; return n; }
        return (L-1)*kPage+size_t(std::lower_bound(q.data,q.data+count,key)-q.data);
    }
    size_t lo=0,hi=n;  // the answer lies in [lo, hi]
    for(int step=0;hi-lo>kPage && step<6 && *status==FLATSQL_OK;step++) {
        const FsgPairV a=at(lo,status),b=at(hi-1,status);
        if(!(a<key)) return lo;
        if(b<key) return hi;
        const double fr=b.hi>a.hi && key.hi>=a.hi?double(key.hi-a.hi)/double(b.hi-a.hi):0.5;
        const size_t g=lo+std::min(hi-1-lo,size_t(fr*double(hi-1-lo)));
        const size_t p0=std::max(lo,g/kPage*kPage),p1=std::min(hi,g/kPage*kPage+kPage);
        if(at(p1-1,status)<key) lo=p1;
        else if(!(at(p0,status)<key)) hi=p0;
        else { lo=p0; hi=p1; break; }
    }
    while(lo<hi && *status==FLATSQL_OK) { const size_t mid=lo+(hi-lo)/2; if(at(mid,status)<key) lo=mid+1; else hi=mid; }
    return lo;
}

std::shared_ptr<const FsgView> fsgView(Feed* f,Conn* c,uint8_t kind,int64_t vis) {
    const int k=kind==kFsgSeq?0:1;
    std::shared_ptr<const CheckpointState> ck; uint64_t previous=0;
    { std::lock_guard<std::mutex> g(f->type->mu); ck=f->checkpoint; previous=f->lastCommit; }
    if(!ck) return nullptr;
    // Tail materialization grows with ingest between checkpoints. The SQLite
    // index already serves that snapshot without retaining a second row order.
    if(previous!=ck->offset || !ck->current) return nullptr;
    // A checkpoint whose live rows have no row orders (written before they
    // existed): the SQLite index answers until the next checkpoint folds them.
    if(ck->counters.rows>0 && !checkpointHasRowOrders(*ck,kind)) return nullptr;
    std::unique_lock<std::mutex> g(f->fsgMu);
    int32_t st=FLATSQL_OK; auto stream=streamCur(f,&st);
    if(!stream || st!=FLATSQL_OK) return nullptr;
    // No reader hashes: a segment not yet checked in this process is checked
    // on the checkpoint worker, and until then the SQLite index answers.
    for(const auto& d:ck->segments)
        if(d.kind==kind && !checkpointPairsVerified(f,stream.get(),d)) {
            const bool queue=!f->fsgVerifying; g.unlock();
            if(queue) fsgVerifySchedule(f);
            return nullptr;
        }
    const auto key=stream->path+"/"+std::to_string(stream->gen)+"/view/"+
        std::to_string(kind)+"/"+std::to_string(ck->offset)+"/"+std::to_string(previous);
    auto cache=f->type->e->rowCache;
    if(auto ready=cache->find<FsgView>(key)) { f->fsg[k]=ready; return ready; }
    const size_t count=size_t(std::count_if(ck->segments.begin(),ck->segments.end(),[&](const CheckpointSegment& d){return d.kind==kind;}));
    // Include run objects, control blocks, key capacities and vector slots.
    // Feeds hold weak references so inactive views remain globally evictable.
    const size_t perRun=sizeof(FsgRun)+128+2*(stream->path.size()+96)+2*sizeof(std::shared_ptr<const FsgRun>);
    if(count>(SIZE_MAX-sizeof(FsgView))/perRun) return nullptr;
    bool lostVerification=false;
    auto out=cache->load<FsgView>(key,sizeof(FsgView)+count*perRun,[&]() -> std::shared_ptr<const FsgView> {
        auto view=std::make_shared<FsgView>(); view->runs[k].reserve(count);
        for(const auto& d:ck->segments) {
            if(d.kind!=kind) continue;
            auto run=std::make_shared<FsgRun>();
            if(checkpointReadPairs(f,stream,d,run.get())!=FLATSQL_OK) { lostVerification=true; return {}; }
            view->runs[k].push_back(std::move(run));
        }
        return view;
    });
    f->fsg[k]=out;
    if(lostVerification) { const bool queue=!f->fsgVerifying; g.unlock(); if(queue) fsgVerifySchedule(f); }
    return out;
}

int32_t fsgSeqs(const FsgView& v,bool desc,int64_t from,int64_t lo,int64_t hi,size_t n,std::vector<int64_t>* out) {
    out->clear();
    if(desc) hi=std::min(hi,from-1); else lo=std::max(lo,from);
    if(lo>=hi || !n) return FLATSQL_OK;
    const uint64_t ridLo=uint64_t(lo+1)<<16, ridHi=(uint64_t(hi)<<16)|0xffff;
    for(const auto& run:v.runs[0]) {
        const FsgRun& x=*run; size_t got=0; int64_t last=-1; bool end=false;
        int32_t status=FLATSQL_OK;
        size_t i=desc?x.lowerBound(FsgPairV{ridHi+1,0,0,0},&status):x.lowerBound(FsgPairV{ridLo,0,0,0},&status);
        if(status!=FLATSQL_OK) return status;
        // asc: pairs i, i+1, ...; desc: pairs i-1, i-2, ...
        while(!end && got<n && (desc?i>0:i<x.n)) {
            const size_t at=desc?i-1:i, pg=at/FsgRun::kPage; size_t cnt=0;
            const auto q=x.page(pg,&cnt,desc?-16:16); if(!q) return q.status;
            size_t j=at%FsgRun::kPage;
            for(;;) {
                const uint64_t r=q[j].hi;
                if(desc?r<ridLo:r>ridHi) { end=true; break; }
                const int64_t s=int64_t(r>>16); if(s!=last) { out->push_back(s); last=s; if(++got>=n) break; }
                if(desc) { i--; if(j==0) break; j--; } else { i++; if(++j>=cnt) break; }
            }
        }
        if(desc) std::sort(out->begin(),out->end(),std::greater<int64_t>());
        else std::sort(out->begin(),out->end());
        out->erase(std::unique(out->begin(),out->end()),out->end());
        if(out->size()>n) out->resize(n);
    }
    if(desc) std::sort(out->begin(),out->end(),std::greater<int64_t>()); else std::sort(out->begin(),out->end());
    out->erase(std::unique(out->begin(),out->end()),out->end());
    if(out->size()>n) out->resize(n);
    return FLATSQL_OK;
}

int32_t fsgObject(const FsgView& v,uint64_t kk,const std::function<bool(uint64_t)>& visit) {
    for(const auto& run:v.runs[1]) {
        const FsgRun& x=*run;
        int32_t status=FLATSQL_OK;
        size_t i=x.lowerBound(FsgPairV{kk,0,0,0},&status);
        bool end=false;
        while(i<x.n && status==FLATSQL_OK && !end) {
            size_t count=0; const auto page=x.page(i/FsgRun::kPage,&count);
            if(!page) { status=page.status; break; }
            for(size_t j=i%FsgRun::kPage;j<count && i<x.n;j++,i++) {
                const auto& pair=page[j];
                if(pair.hi!=kk) { end=true; break; }
                if(!visit(pair.rid)) return FLATSQL_OK;
            }
        }
        if(status!=FLATSQL_OK) return status;
    }
    return FLATSQL_OK;
}
} }
