// FlatBuffer row orders (store format 5, FORMAT.md §3.3, §7): a feed's
// ROWS_BY_SEQ and ROWS_BY_OBJECT_EPOCH as immutable sorted FSGPair runs, the
// checkpoint's segments (read from the stream a page at a time) plus the
// tail the $FMB chain added since.
#include "checkpoint.h"
#include <algorithm>
namespace flatsql { namespace p4 {

const FsgPairV* FsgRun::page(size_t pg,size_t* count,int ahead) const {
    const size_t first=pg*kPage; *count=std::min(kPage,n-first);
    if(p) return p+first;
    std::lock_guard<std::mutex> g(mu);
    if(!pages[pg]) {
        // The missing pages from pg on in the walk's direction, one read.
        size_t a=pg,b=pg+1; const size_t np=pages.size();
        if(++faults>kFaults) { a=0; b=np; }  // a run read this often: all of it, once
        else if(ahead>0) while(b<np && b-pg<size_t(ahead) && !pages[b]) b++;
        if(ahead<0) while(a>0 && pg-a<size_t(-ahead) && !pages[a-1]) a--;
        const size_t from=a*kPage,to=std::min(n,b*kPage);
        std::string buf; const int32_t rc=streamReadRange(stream.get(),int64_t(base+from*sizeof(FsgPairV)),(to-from)*sizeof(FsgPairV),&buf);
        if(rc!=P4_OK || buf.size()!=(to-from)*sizeof(FsgPairV)) { failed=true; return nullptr; }
        for(size_t k=a;k<b;k++) {
            if(pages[k]) continue;
            const size_t f0=k*kPage,c=std::min(kPage,n-f0);
            auto v=std::make_shared<std::vector<FsgPairV>>(c); std::memcpy(v->data(),buf.data()+(f0-from)*sizeof(FsgPairV),c*sizeof(FsgPairV));
            if(!std::is_sorted(v->begin(),v->end())) { failed=true; return nullptr; }
            pages[k]=std::move(v);
        }
    }
    return pages[pg]->data();  // pages are never replaced: the pointer outlives the lock
}

FsgPairV FsgRun::at(size_t i) const {
    size_t count=0; const FsgPairV* q=page(i/kPage,&count);
    return q?q[i%kPage]:FsgPairV{UINT64_MAX,UINT64_MAX,UINT64_MAX,0};
}

// Interpolation on KEY_HI (rids and object ids are dense), one page a step,
// then a binary search inside the page it lands in.
size_t FsgRun::lowerBound(const FsgPairV& key) const {
    if(verified && !verified->fences.empty()) {
        // The fence keys (in memory) name the one page the answer lies in.
        const auto& fc=verified->fences;
        const size_t L=size_t(std::lower_bound(fc.begin(),fc.end(),key)-fc.begin());
        if(L==0) return 0;
        size_t count=0; const FsgPairV* q=page(L-1,&count); if(!q) return n;
        return (L-1)*kPage+size_t(std::lower_bound(q,q+count,key)-q);
    }
    size_t lo=0,hi=n;  // the answer lies in [lo, hi]
    for(int step=0;hi-lo>kPage && step<6 && !failed;step++) {
        const FsgPairV a=at(lo),b=at(hi-1);
        if(!(a<key)) return lo;
        if(b<key) return hi;
        const double fr=b.hi>a.hi && key.hi>=a.hi?double(key.hi-a.hi)/double(b.hi-a.hi):0.5;
        const size_t g=lo+std::min(hi-1-lo,size_t(fr*double(hi-1-lo)));
        const size_t p0=std::max(lo,g/kPage*kPage),p1=std::min(hi,g/kPage*kPage+kPage);
        if(at(p1-1)<key) lo=p1;
        else if(!(at(p0)<key)) hi=p0;
        else { lo=p0; hi=p1; break; }
    }
    while(lo<hi && !failed) { const size_t mid=lo+(hi-lo)/2; if(at(mid)<key) lo=mid+1; else hi=mid; }
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
    int32_t st=P4_OK; auto stream=streamCur(f,&st);
    if(!stream || st!=P4_OK) return nullptr;
    // No reader hashes: a segment not yet checked in this process is checked
    // on the checkpoint worker, and until then the SQLite index answers.
    for(const auto& d:ck->segments)
        if(d.kind==kind && !f->fsgSegments.count({stream->gen,d.offset}) && !checkpointPairsVerified(stream.get(),d)) {
            const bool queue=!f->fsgVerifying; g.unlock();
            if(queue) fsgVerifySchedule(f);
            return nullptr;
        }
    auto v=std::make_shared<FsgView>(); v->ck=ck; v->previous=previous;
    for(const auto& d:ck->segments) {
        if(d.kind!=kind) continue;
        auto& slot=f->fsgSegments[{stream->gen,d.offset}];
        if(!slot) { auto run=std::make_shared<FsgRun>(); if(checkpointReadPairs(f,stream,d,run.get())!=P4_OK) { f->fsgSegments.erase({stream->gen,d.offset}); return nullptr; } slot=std::move(run); }
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
        if(checkpointTailRows(f,stream.get(),*ck,previous,&rids,nullptr,nullptr,extend?old->previous:UINT64_MAX)!=P4_OK) return nullptr;
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
    if(!extend) checkpointPairsRetain(stream.get(),*ck);
    std::shared_ptr<const FsgView> out=std::move(v);
    if(complete) f->fsg[k]=out;
    return out;
}

int32_t fsgSeqs(const FsgView& v,bool desc,int64_t from,int64_t lo,int64_t hi,size_t n,std::vector<int64_t>* out) {
    out->clear();
    if(desc) hi=std::min(hi,from-1); else lo=std::max(lo,from);
    if(lo>=hi || !n) return P4_OK;
    const uint64_t ridLo=uint64_t(lo+1)<<16, ridHi=(uint64_t(hi)<<16)|0xffff;
    for(const auto& run:v.runs[0]) {
        const FsgRun& x=*run; size_t got=0; int64_t last=-1; bool end=false;
        size_t i=desc?x.lowerBound(FsgPairV{ridHi+1,0,0,0}):x.lowerBound(FsgPairV{ridLo,0,0,0});
        // asc: pairs i, i+1, ...; desc: pairs i-1, i-2, ...
        while(!end && got<n && (desc?i>0:i<x.n)) {
            const size_t at=desc?i-1:i, pg=at/FsgRun::kPage; size_t cnt=0;
            const FsgPairV* q=x.page(pg,&cnt,desc?-16:16); if(!q) return P4_E_CORRUPT;
            size_t j=at%FsgRun::kPage;
            for(;;) {
                const uint64_t r=q[j].hi;
                if(desc?r<ridLo:r>ridHi) { end=true; break; }
                const int64_t s=int64_t(r>>16); if(s!=last) { out->push_back(s); last=s; if(++got>=n) break; }
                if(desc) { i--; if(j==0) break; j--; } else { i++; if(++j>=cnt) break; }
            }
        }
        if(x.failed) return P4_E_CORRUPT;
    }
    if(desc) std::sort(out->begin(),out->end(),std::greater<int64_t>()); else std::sort(out->begin(),out->end());
    out->erase(std::unique(out->begin(),out->end()),out->end());
    if(out->size()>n) out->resize(n);
    return P4_OK;
}

int32_t fsgObject(const FsgView& v,uint64_t kk,std::vector<uint64_t>* rids) {
    rids->clear();
    for(const auto& run:v.runs[1]) {
        const FsgRun& x=*run;
        for(size_t i=x.lowerBound(FsgPairV{kk,0,0,0});i<x.n;i++) { const FsgPairV q=x.at(i); if(q.hi!=kk) break; rids->push_back(q.rid); }
        if(x.failed) return P4_E_CORRUPT;
    }
    std::sort(rids->begin(),rids->end());
    rids->erase(std::unique(rids->begin(),rids->end()),rids->end());
    return P4_OK;
}
} }
