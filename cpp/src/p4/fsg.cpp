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
    int32_t status=FLATSQL_E_BUDGET;
    auto value=cache->load<Page>(cacheKey+"/page/"+std::to_string(pg),*count*sizeof(FsgPairV)+sizeof(Page),[&]() -> std::shared_ptr<const Page> {
        std::string buf;
        status=streamReadRange(stream.get(),int64_t(base+first*sizeof(FsgPairV)),*count*sizeof(FsgPairV),&buf);
        if(status!=FLATSQL_OK) return {};
        if(buf.size()!=*count*sizeof(FsgPairV)) { status=FLATSQL_E_CORRUPT; return {}; }
        auto v=std::make_shared<Page>(); v->generation=stream; v->pairs.resize(*count);
        std::memcpy(v->pairs.data(),buf.data(),buf.size());
        if(!std::is_sorted(v->pairs.begin(),v->pairs.end())) { status=FLATSQL_E_CORRUPT; return {}; }
        return v;
    },*count*sizeof(FsgPairV));
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
    const auto fences=verified.lock();
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
    // A checkpoint whose live rows have no row orders (written before they
    // existed): the SQLite index answers until the next checkpoint folds them.
    if(ck->counters.rows>0 && !checkpointHasRowOrders(*ck,kind)) return nullptr;
    std::unique_lock<std::mutex> g(f->fsgMu);
    if(f->fsg[k] && f->fsg[k]->ck==ck && f->fsg[k]->previous==previous) return f->fsg[k];
    int32_t st=FLATSQL_OK; auto stream=streamCur(f,&st);
    if(!stream || st!=FLATSQL_OK) return nullptr;
    // No reader hashes: a segment not yet checked in this process is checked
    // on the checkpoint worker, and until then the SQLite index answers.
    for(const auto& d:ck->segments)
        if(d.kind==kind && !f->fsgSegments.count({stream->gen,d.offset}) && !checkpointPairsVerified(f,stream.get(),d)) {
            const bool queue=!f->fsgVerifying; g.unlock();
            if(queue) fsgVerifySchedule(f);
            return nullptr;
        }
    auto v=std::make_shared<FsgView>(); v->ck=ck; v->previous=previous;
    for(const auto& d:ck->segments) {
        if(d.kind!=kind) continue;
        auto& slot=f->fsgSegments[{stream->gen,d.offset}];
        if(!slot) { auto run=std::make_shared<FsgRun>(); if(checkpointReadPairs(f,stream,d,run.get())!=FLATSQL_OK) { f->fsgSegments.erase({stream->gen,d.offset}); return nullptr; } slot=std::move(run); }
        v->runs[k].push_back(slot);
    }
    bool complete=true;
    // A view of the same checkpoint at an earlier commit: its tail runs, and
    // the rows the commits since then added.
    const auto& old=f->fsg[k];
    const bool extend=old && old->ck==ck && old->previous!=ck->offset;
    if(extend) {
        for(const auto& r:old->runs[k]) if(r->p) v->runs[k].push_back(r);
        // Many small tail runs: one.
        if(v->runs[k].size()-std::count_if(v->runs[k].begin(),v->runs[k].end(),[](const std::shared_ptr<const FsgRun>& r){return !r->p;})>8) {
            auto all=std::make_shared<std::vector<FsgPairV>>();
            std::vector<std::shared_ptr<const FsgRun>> keep;
            for(const auto& r:v->runs[k]) { if(r->p) all->insert(all->end(),r->begin(),r->end()); else keep.push_back(r); }
            std::sort(all->begin(),all->end()); all->erase(std::unique(all->begin(),all->end(),[](const FsgPairV& a,const FsgPairV& b){return a.rid==b.rid && a.hi==b.hi && a.lo==b.lo;}),all->end());
            auto run=std::make_shared<FsgRun>(); run->p=all->data(); run->n=all->size(); run->keep=std::move(all);
            keep.push_back(std::move(run)); v->runs[k]=std::move(keep);
        }
    }
    if(previous!=ck->offset || !ck->current) {
        std::vector<uint64_t> rids;
        if(checkpointTailRows(f,stream.get(),*ck,previous,&rids,nullptr,nullptr,extend?old->previous:UINT64_MAX)!=FLATSQL_OK) return nullptr;
        auto tail=std::make_shared<std::vector<FsgPairV>>(); tail->reserve(rids.size());
        if(kind==kFsgSeq) { for(uint64_t rid:rids) tail->push_back({rid,0,rid,0}); }
        else {
            sqlite3_stmt* q=c->sql("SELECT kk,e FROM r WHERE rid=?1"); if(!q) return nullptr;
            for(uint64_t rid:rids) {
                sqlite3_bind_int64(q,1,sqlite3_int64(rid)); const int r=sqlite3_step(q);
                if(r==SQLITE_ROW) {
                    const uint64_t hi=sqlite3_column_type(q,0)==SQLITE_NULL?0:uint64_t(sqlite3_column_int64(q,0));
                    const uint64_t lo=sqlite3_column_type(q,1)==SQLITE_NULL?0:uint64_t(sqlite3_column_int64(q,1))^(uint64_t(1)<<63);
                    tail->push_back({hi,lo,rid,0});
                } else if(r!=SQLITE_DONE) { sqlite3_reset(q); return nullptr; }
                else if(int64_t(rid>>16)>vis) complete=false;  // committed to the stream, not yet in this snapshot
                sqlite3_reset(q);
            }
            std::sort(tail->begin(),tail->end());
        }
        if(!tail->empty()) { auto run=std::make_shared<FsgRun>(); run->p=tail->data(); run->n=tail->size(); run->keep=std::move(tail); v->runs[k].push_back(std::move(run)); }
    }
    // Segments the checkpoint no longer lists are let go.
    for(auto it=f->fsgSegments.begin();it!=f->fsgSegments.end();) {
        const bool live=it->first.first==stream->gen && std::any_of(ck->segments.begin(),ck->segments.end(),[&](const CheckpointSegment& d){return d.offset==it->first.second;});
        it=live?std::next(it):f->fsgSegments.erase(it);
    }
    std::shared_ptr<const FsgView> out=std::move(v);
    if(complete) f->fsg[k]=out;
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
    }
    if(desc) std::sort(out->begin(),out->end(),std::greater<int64_t>()); else std::sort(out->begin(),out->end());
    out->erase(std::unique(out->begin(),out->end()),out->end());
    if(out->size()>n) out->resize(n);
    return FLATSQL_OK;
}

int32_t fsgObject(const FsgView& v,uint64_t kk,std::vector<uint64_t>* rids) {
    rids->clear();
    for(const auto& run:v.runs[1]) {
        const FsgRun& x=*run;
        int32_t status=FLATSQL_OK;
        for(size_t i=x.lowerBound(FsgPairV{kk,0,0,0},&status);i<x.n && status==FLATSQL_OK;i++) { const FsgPairV q=x.at(i,&status); if(q.hi!=kk) break; rids->push_back(q.rid); }
        if(status!=FLATSQL_OK) return status;
    }
    std::sort(rids->begin(),rids->end());
    rids->erase(std::unique(rids->begin(),rids->end()),rids->end());
    return FLATSQL_OK;
}
} }
