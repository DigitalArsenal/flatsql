#include "checkpoint.h"
#include "dag.h"
#include "sds_frames.h"
#include <algorithm>
#include <cstdio>
#include <cerrno>

namespace flatsql { namespace p4 {
namespace {
using DictKey=std::pair<uint8_t,uint32_t>;
struct SegmentData {
    std::map<DictKey,FmbEntry> dictionary;
    std::map<uint32_t,CheckpointLatest> latest;
    std::map<InstId,InstCount> instances;
    std::map<uint32_t,FmbEntry> tokens;
    std::set<uint64_t> tombstones;
    uint64_t size(uint8_t k) const { switch(k) { case 0:return dictionary.size(); case 1:return latest.size(); case 2:return instances.size(); case 3:return tokens.size(); default:return tombstones.size(); } }
};
std::string text(sqlite3_stmt* q,int i) {
    auto* p=reinterpret_cast<const char*>(sqlite3_column_text(q,i));
    return p?std::string(p,size_t(sqlite3_column_bytes(q,i))):std::string();
}
int32_t frame(Stream* stream,uint64_t at,std::vector<uint64_t>* storage,uint32_t* length) {
    if(at>uint64_t(INT64_MAX)) return P4_E_CORRUPT;
    std::string prefix; int32_t rc=streamReadRange(stream,int64_t(at),4,&prefix);
    if(rc!=P4_OK || prefix.size()!=4) return P4_E_CORRUPT;
    const uint32_t n=ld32(reinterpret_cast<const uint8_t*>(prefix.data()));
    if(n<8 || n>(128u<<20)) return P4_E_CORRUPT;
    std::string data; rc=streamRead(stream,int64_t(at),n,&data);
    if(rc!=P4_OK || data.size()!=n) return P4_E_CORRUPT;
    storage->resize((n+7)/8); std::memcpy(storage->data(),data.data(),n); *length=n; return P4_OK;
}
// Recovery may end at a complete FSG whose following FCK was torn. Coverage
// still names the last folded FMB end, never the segment's end.
int32_t appliedEnd(Stream* stream,uint64_t previous,uint64_t* end) {
    uint64_t at=previous;
    while(at!=UINT64_MAX) {
        std::vector<uint64_t> storage; uint32_t n=0;
        const int32_t rc=frame(stream,at,&storage,&n); if(rc!=P4_OK) return rc;
        const auto* p=reinterpret_cast<const uint8_t*>(storage.data()); flatbuffers::Verifier v(p,n);
        if(flatbuffers::BufferHasIdentifier(p,"$FMB")) { if(!VerifyFMBBuffer(v)) return P4_E_CORRUPT; *end=at+4+n; return P4_OK; }
        if(flatbuffers::BufferHasIdentifier(p,"$FCK")) { if(!VerifyFCKBuffer(v)) return P4_E_CORRUPT; *end=GetFCK(p)->APPLIED_THROUGH(); return P4_OK; }
        if(!VerifyFSGBuffer(v)) return P4_E_CORRUPT;
        const uint64_t next=GetFSG(p)->PREVIOUS(); if(next>=at) return P4_E_CORRUPT; at=next;
    }
    *end=0; return P4_OK;
}
int32_t append(Stream* stream,const uint8_t* data,size_t size,int64_t* end,const char* kind) {
    std::vector<uint8_t> bytes(size+4); st32(bytes.data(),uint32_t(size));
    std::memcpy(bytes.data()+4,data,size);
#ifndef __wasm__
    if(const char* pause=std::getenv("P4_STATE_PAUSE_MS")) {
        const size_t half=bytes.size()/2; int32_t rc=streamWrite(stream,*end,bytes.data(),half);
        if(rc!=P4_OK) return rc;
        if(std::getenv("P4_STATE_DEBUG")) { std::fprintf(stderr,"state-write-begin %s offset=%lld bytes=%zu\n",kind,(long long)*end,bytes.size()); std::fflush(stderr); }
        ps::sleepNs(uint64_t(std::max(0,std::atoi(pause)))*1000000ull);
        rc=streamWrite(stream,*end+int64_t(half),bytes.data()+half,bytes.size()-half);
        if(rc!=P4_OK) return rc;
    } else
#endif
    { const int32_t rc=streamWrite(stream,*end,bytes.data(),bytes.size()); if(rc!=P4_OK) return rc; }
    *end+=int64_t(bytes.size());
#ifndef __wasm__
    if(std::getenv("P4_STATE_DEBUG")) { std::fprintf(stderr,"state-write-end %s\n",kind); std::fflush(stderr); }
#endif
    return P4_OK;
}
FmbEntry entry(const ::FMBEntry* d) {
    FmbEntry out; out.kind=uint8_t(d->KIND()); out.id=d->ID();
    if(d->TEXT()) for(const auto* s:*d->TEXT()) out.text.push_back(s->str());
    return out;
}
flatbuffers::Offset<::FMBEntry> entry(flatbuffers::FlatBufferBuilder& b,const FmbEntry& d) {
    std::vector<flatbuffers::Offset<flatbuffers::String>> v;
    for(const auto& s:d.text) v.push_back(b.CreateString(s));
    return CreateFMBEntry(b,fmbDictKind(d.kind),d.id,b.CreateVector(v));
}
FmbEntry tokenEntry(uint32_t id,const TokCount& k,const std::string& token) {
    return {0,id,{token,std::to_string(k.n),std::to_string(k.bytes),std::to_string(k.mints),std::to_string(k.maxts),std::to_string(k.maxseq)}};
}
bool number(const std::string& s,int64_t* out) {
    if(s.empty()) return false; char* end=nullptr; errno=0; long long value=std::strtoll(s.c_str(),&end,10);
    if(errno || !end || *end) return false; *out=int64_t(value); return true;
}
bool instanceEqual(const InstCount& a,const InstCount& b) {
    return a.n==b.n && a.bytes==b.bytes && a.minw==b.minw && a.maxw==b.maxw && a.minseq==b.minseq && a.maxseq==b.maxseq &&
      a.first==b.first && a.updated==b.updated && a.maxat==b.maxat && a.maxts==b.maxts;
}
bool tokenEqual(const TokCount& a,const TokCount& b) {
    return a.n==b.n && a.bytes==b.bytes && a.mints==b.mints && a.maxts==b.maxts && a.maxseq==b.maxseq;
}
int32_t readSegment(Feed* f,Stream* stream,const CheckpointSegment& desc,SegmentData* out) {
    std::vector<uint64_t> storage; uint32_t n=0;
    int32_t rc=frame(stream,desc.offset,&storage,&n); if(rc!=P4_OK || n!=desc.length) return P4_E_CORRUPT;
    const auto* p=reinterpret_cast<const uint8_t*>(storage.data());
    std::array<uint8_t,32> digest{}; ps::sha256(p,n,digest.data()); if(digest!=desc.digest) return P4_E_CORRUPT;
    flatbuffers::Verifier v(p,n); if(!VerifyFSGBuffer(v)) return P4_E_CORRUPT;
    const auto* s=GetFSG(p);
    if(s->SELF_OFFSET()!=desc.offset || s->FEED_ID()!=f->fid || s->KIND()!=desc.kind || s->LEVEL()!=desc.level ||
       s->COVERS_THROUGH()!=desc.coversThrough || s->MIN_SEQ()!=desc.minSeq || s->MAX_SEQ()!=desc.maxSeq || desc.kind>4) return P4_E_CORRUPT;
    if(s->ENTRIES()) for(const auto* e:*s->ENTRIES()) { auto d=entry(e); if(!d.id) return P4_E_CORRUPT; out->dictionary[{d.kind,d.id}]=std::move(d); }
    if(s->LATEST()) for(const auto* e:*s->LATEST()) {
        if(!e->OBJECT_KEY_ID() || e->RESERVED()) return P4_E_CORRUPT;
        out->latest[e->OBJECT_KEY_ID()]={e->OBJECT_KEY_ID(),e->EPOCH(),e->RID(),e->ROW_REF()};
    }
    if(s->INSTANCES()) for(const auto* e:*s->INSTANCES()) {
        InstCount x; x.n=int64_t(e->RECORDS()); x.bytes=int64_t(e->BYTES()); x.minw=e->MIN_W(); x.maxw=e->MAX_W();
        x.minseq=int64_t(e->MIN_SEQ()); x.maxseq=int64_t(e->MAX_SEQ()); x.first=e->FIRST(); x.updated=e->UPDATED(); x.maxat=e->MAX_AT(); x.maxts=e->MAX_TS();
        out->instances[{e->BATCH_ID(),e->CONTENT_KEY_ID()}]=std::move(x);
    }
    if(s->TOKENS()) for(const auto* e:*s->TOKENS()) { auto d=entry(e); if(!d.id || d.text.size()!=6) return P4_E_CORRUPT; out->tokens[d.id]=std::move(d); }
    if(s->PAIRS()) for(const auto* e:*s->PAIRS()) out->tombstones.insert(e->KEY_HI());
    return out->size(desc.kind)==desc.entries?P4_OK:P4_E_CORRUPT;
}
void merge(SegmentData* dst,const SegmentData& src) {
    for(const auto& x:src.dictionary) dst->dictionary[x.first]=x.second;
    for(const auto& x:src.latest) dst->latest[x.first]=x.second;
    for(const auto& x:src.instances) dst->instances[x.first]=x.second;
    for(const auto& x:src.tokens) dst->tokens[x.first]=x.second;
    dst->tombstones.insert(src.tombstones.begin(),src.tombstones.end());
}
// Encodes one segment at the provisional offset *cursor. Defaults are forced
// so SELF_OFFSET, PREVIOUS and every REPLACES entry exist to be patched.
void encodeSegment(Feed* f,uint8_t kind,uint8_t level,const SegmentData& data,const std::vector<uint64_t>& replaces,
    uint64_t applied,const Counters& counters,uint64_t* cursor,CheckpointPlan* plan,CheckpointSegment* desc) {
    flatbuffers::FlatBufferBuilder b; b.ForceDefaults(true);
    std::vector<flatbuffers::Offset<::FMBEntry>> entries,tokens;
    std::vector<::FSGLatest> latest; std::vector<::FSGInstance> instances; std::vector<::FSGPair> pairs;
    for(const auto& x:data.dictionary) entries.push_back(entry(b,x.second));
    for(const auto& x:data.tokens) tokens.push_back(entry(b,x.second));
    for(const auto& x:data.latest) { const auto& l=x.second; latest.emplace_back(l.objectKey,0,l.epoch,l.rid,l.rowRef); }
    for(const auto& item:data.instances) { const auto& x=item.second;
        instances.emplace_back(item.first.first,item.first.second,uint64_t(x.n),uint64_t(x.bytes),x.minw,x.maxw,uint64_t(x.minseq),uint64_t(x.maxseq),x.first,x.updated,x.maxat,x.maxts); }
    for(auto rid:data.tombstones) pairs.emplace_back(rid,0,rid,0);
    auto ev=b.CreateVector(entries),tv=b.CreateVector(tokens); auto lv=b.CreateVectorOfStructs(latest);
    auto iv=b.CreateVectorOfStructs(instances); auto pv=b.CreateVectorOfStructs(pairs); auto rv=b.CreateVector(replaces);
    desc->offset=*cursor; desc->kind=kind; desc->level=level; desc->coversThrough=applied; desc->entries=data.size(kind);
    desc->minSeq=counters.minseq==INT64_MAX?0:uint64_t(counters.minseq); desc->maxSeq=uint64_t(counters.maxseq);
    FSGBuilder root(b); root.add_SELF_OFFSET(desc->offset); root.add_PREVIOUS(0); root.add_FEED_ID(f->fid);
    root.add_KIND(fckSegmentKind(kind)); root.add_LEVEL(level); root.add_MIN_SEQ(desc->minSeq); root.add_MAX_SEQ(desc->maxSeq);
    root.add_COVERS_THROUGH(applied); root.add_REPLACES(rv);
    if(kind==0) root.add_ENTRIES(ev); else if(kind==1) root.add_LATEST(lv); else if(kind==2) root.add_INSTANCES(iv); else if(kind==3) root.add_TOKENS(tv); else root.add_PAIRS(pv);
    auto x=root.Finish(); FinishFSGBuffer(b,x); desc->length=uint32_t(b.GetSize());
    std::vector<uint64_t> frame((b.GetSize()+7)/8); std::memcpy(frame.data(),b.GetBufferPointer(),b.GetSize());
    plan->frames.push_back(std::move(frame)); plan->lengths.push_back(desc->length); *cursor+=4+uint64_t(desc->length);
}
// Whether only $FSG frames lie in [from,to): size prefixes only.
bool onlySegments(Stream* stream,uint64_t from,uint64_t to) {
    while(from<to) {
        if(to-from<12) return false;
        std::string prefix; if(streamReadRange(stream,int64_t(from),12,&prefix)!=P4_OK || prefix.size()!=12) return false;
        const uint32_t n=ld32(reinterpret_cast<const uint8_t*>(prefix.data()));
        if(n<8 || uint64_t(n)>to-from-4 || std::memcmp(prefix.data()+8,"$FSG",4)) return false;
        from+=4+uint64_t(n);
    }
    return from==to;
}
// Metadata-only backward chain; unchanged latest entries retain their ROW_REF.
struct TailChanges { std::map<uint64_t,uint64_t> rows; std::set<uint64_t> removed,removedSeqs; };
int32_t changes(Stream* stream,uint64_t previous,uint64_t after,TailChanges* changes,CheckpointState* state,bool allRefs) {
    std::map<uint64_t,uint32_t> wanted;
    for(const auto& item:state->latest) if(!item.second.rowRef) wanted[item.second.rid]=item.first;
    uint64_t at=previous;
    while(at!=UINT64_MAX) {
        std::vector<uint64_t> storage; uint32_t n=0; int32_t rc=frame(stream,at,&storage,&n); if(rc!=P4_OK) return rc;
        const auto* p=reinterpret_cast<const uint8_t*>(storage.data()); flatbuffers::Verifier v(p,n); uint64_t prev;
        const bool tail=at+4+n>after;
        if(flatbuffers::BufferHasIdentifier(p,"$FMB")) {
            if(!VerifyFMBBuffer(v)) return P4_E_CORRUPT; const auto* m=GetFMB(p); prev=m->PREVIOUS();
            if(tail && m->REMOVALS()) for(const auto* r:*m->REMOVALS()) {
                if(r->ROW_J()==UINT32_MAX) changes->removedSeqs.insert(r->SEQ()); else changes->removed.insert((r->SEQ()<<16)|r->ROW_J());
            }
            if(m->ROWS()) for(size_t i=0;i<m->ROWS()->size();i++) {
                const auto* r=m->ROWS()->Get(flatbuffers::uoffset_t(i)); const uint64_t rid=(r->SEQ()<<16)|r->ROW_J();
                auto wi=wanted.find(rid); if(wi!=wanted.end()) { state->latest[wi->second].rowRef=(at<<16)|uint64_t(i); wanted.erase(wi); }
                if(tail && allRefs) changes->rows.emplace(rid,(at<<16)|uint64_t(i));
                if(changes->removedSeqs.count(r->SEQ())) changes->removed.insert(rid);
            }
        } else if(flatbuffers::BufferHasIdentifier(p,"$FSG")) { if(!VerifyFSGBuffer(v)) return P4_E_CORRUPT; prev=GetFSG(p)->PREVIOUS(); }
        else if(flatbuffers::BufferHasIdentifier(p,"$FCK")) { if(!VerifyFCKBuffer(v)) return P4_E_CORRUPT; prev=GetFCK(p)->PREVIOUS(); }
        else return P4_E_CORRUPT;
        if(at==0 || (!tail && wanted.empty() && changes->removedSeqs.empty())) break;
        if(prev>=at) return P4_E_CORRUPT; at=prev;
    }
    state->tombstones.insert(changes->removed.begin(),changes->removed.end());
    return wanted.empty()?P4_OK:P4_E_CORRUPT;
}
int32_t latestSnapshot(Feed* f,Conn* c,Stream* stream,uint64_t previous,const CheckpointState& old,CheckpointState* state) {
    const bool hasObject=f->type->spec()->hasObject;
    if(!old.appliedThrough) {
        if(hasObject) {
            auto* q=c->sql("SELECT kk,e,rid FROM r WHERE kk IS NOT NULL AND e IS NOT NULL ORDER BY kk,e DESC,cid,rid"); if(!q) return P4_E_IO; int rc;
            while((rc=sqlite3_step(q))==SQLITE_ROW) { uint32_t key=uint32_t(sqlite3_column_int64(q,0)); if(!state->latest.count(key)) state->latest[key]={key,sqlite3_column_int64(q,1),uint64_t(sqlite3_column_int64(q,2)),0}; }
            sqlite3_reset(q); if(rc!=SQLITE_DONE) return statusOfSqlite(rc);
        }
        TailChanges tail; return changes(stream,previous,0,&tail,state,false);
    }
    state->latest=old.latest; TailChanges tail;
    int32_t rc=changes(stream,previous,old.appliedThrough,&tail,state,hasObject); if(rc!=P4_OK || !hasObject) return rc;
    std::set<uint32_t> refresh;
    for(const auto& item:state->latest) if(tail.removed.count(item.second.rid) || tail.removedSeqs.count(item.second.rid>>16)) refresh.insert(item.first);
    // Only a removed winner needs a new object seek. Ordinary checkpoints read
    // precisely the tail's rows by their integer primary keys.
    auto* best=c->sql("SELECT e,rid FROM r WHERE kk=?1 AND e IS NOT NULL ORDER BY e DESC,cid,rid LIMIT 1");
    if(!best) return P4_E_IO;
    for(uint32_t key:refresh) { sqlite3_bind_int64(best,1,key); const int sr=sqlite3_step(best);
        if(sr==SQLITE_ROW) state->latest[key]={key,sqlite3_column_int64(best,0),uint64_t(sqlite3_column_int64(best,1)),0};
        else if(sr==SQLITE_DONE) state->latest.erase(key); else { sqlite3_reset(best); return statusOfSqlite(sr); } sqlite3_reset(best);
    }
    auto* q=c->sql("SELECT kk,e,rid,cid FROM r WHERE rid=?1 AND kk IS NOT NULL AND e IS NOT NULL");
    auto* cid=c->sql("SELECT cid FROM r WHERE rid=?1"); if(!q || !cid) return P4_E_IO;
    for(const auto& item:tail.rows) {
        sqlite3_bind_int64(q,1,sqlite3_int64(item.first)); const int sr=sqlite3_step(q);
        if(sr==SQLITE_ROW) {
            const uint32_t key=uint32_t(sqlite3_column_int64(q,0)); const int64_t epoch=sqlite3_column_int64(q,1); auto oldWinner=state->latest.find(key);
            bool better=oldWinner==state->latest.end() || epoch>oldWinner->second.epoch;
            if(!better && oldWinner!=state->latest.end() && epoch==oldWinner->second.epoch) {
                sqlite3_bind_int64(cid,1,sqlite3_int64(oldWinner->second.rid)); const int cr=sqlite3_step(cid);
                if(cr!=SQLITE_ROW) better=true;
                else if(sqlite3_column_bytes(q,3)==32 && sqlite3_column_bytes(cid,0)==32) { const int order=std::memcmp(sqlite3_column_blob(q,3),sqlite3_column_blob(cid,0),32); better=order<0 || (order==0 && item.first<oldWinner->second.rid); }
                sqlite3_reset(cid);
            }
            if(better) state->latest[key]={key,epoch,item.first,item.second};
        } else if(sr!=SQLITE_DONE) { sqlite3_reset(q); return statusOfSqlite(sr); }
        sqlite3_reset(q);
    }
    if(!refresh.empty()) { TailChanges refs; return changes(stream,previous,old.appliedThrough,&refs,state,false); }
    return P4_OK;
}
int32_t snapshot(Feed* f,Conn* c,Stream* stream,uint64_t previous,const CheckpointState& old,const Counters& counters,CheckpointState* state) {
    state->counters=counters;
    { std::lock_guard<std::mutex> g(f->type->mu);
        state->typeSeqHigh=uint64_t(f->type->nextSeq-1);
        for(size_t i=0;i<f->type->toks.size();i++) { const auto& t=f->type->toks[i]; state->dictionary[{0,uint32_t(i+1)}]={0,uint32_t(i+1),{t.token,t.peer}}; }
    }
    const char* dict[]={"SELECT id,producer,peer FROM node ORDER BY id","SELECT id,batch,ppeer,pkey FROM batch ORDER BY id","SELECT id,ckey FROM ckey ORDER BY id","SELECT id,url FROM url ORDER BY id","SELECT id,k FROM okey ORDER BY id"};
    for(uint8_t kind=1;kind<=5;kind++) {
        auto* q=c->sql(dict[kind-1]); if(!q) return P4_E_IO; int rc;
        while((rc=sqlite3_step(q))==SQLITE_ROW) {
            FmbEntry d; d.kind=kind; d.id=uint32_t(sqlite3_column_int64(q,0));
            for(int i=1;i<sqlite3_column_count(q);i++) d.text.push_back(text(q,i));
            if(kind==5) { KVal k; k.from(q,1); d.text={std::string(k.type==1?"i":"s")+k.text()}; }
            state->dictionary[{kind,d.id}]=std::move(d);
        }
        sqlite3_reset(q); if(rc!=SQLITE_DONE) return statusOfSqlite(rc);
    }
    // Names come from this snapshot's own dictionary: the feed's shared
    // dictionary cache belongs to its writer.
    std::vector<std::pair<InstId,InstCount>> rows; int sql=readInstRows(c,&rows); if(sql!=SQLITE_OK) return statusOfSqlite(sql);
    for(auto& row:rows) {
        auto b=state->dictionary.find({2,row.first.first}); if(b==state->dictionary.end() || b->second.text.size()!=3) return P4_E_CORRUPT;
        row.second.batch=b->second.text[0]; row.second.ppeer=b->second.text[1]; row.second.pkey=b->second.text[2];
        auto k=state->dictionary.find({3,row.first.second}); if(k!=state->dictionary.end() && k->second.text.size()==1) row.second.ckey=k->second.text[0];
        state->instances[row.first]=std::move(row.second);
    }
    sql=readTokc(f->type,c,&state->tokens); if(sql!=SQLITE_OK) return statusOfSqlite(sql);
    return latestSnapshot(f,c,stream,previous,old,state);
}
} // namespace

int32_t checkpointReadAt(Feed* f,Stream* stream,uint64_t offset,CheckpointState* state,std::string* err) {
    std::vector<uint64_t> storage; uint32_t n=0; int32_t rc=frame(stream,offset,&storage,&n);
    auto fail=[&]() { if(err) *err="invalid checkpoint or segment in "+stream->path+" at "+std::to_string(offset); return P4_E_CORRUPT; };
    if(rc!=P4_OK) return fail(); const auto* p=reinterpret_cast<const uint8_t*>(storage.data()); flatbuffers::Verifier v(p,n);
    if(!VerifyFCKBuffer(v)) return fail(); const auto* ck=GetFCK(p);
    if(ck->SELF_OFFSET()!=offset || ck->FEED_ID()!=f->fid || ck->GENERATION()!=stream->gen || !ck->COUNTERS() || ck->APPLIED_THROUGH()>offset) return fail();
    CheckpointState out; out.offset=offset; out.end=offset+4+n; out.appliedThrough=ck->APPLIED_THROUGH(); out.typeSeqHigh=ck->TYPE_SEQ_HIGH(); out.checkpointBytes=n+4;
    const auto* k=ck->COUNTERS(); out.counters.rows=int64_t(k->ROWS()); out.counters.recs=int64_t(k->RECORDS()); out.counters.bytes=int64_t(k->BYTES()); out.counters.nnull=int64_t(k->RECORDS_NO_EPOCH());
    out.counters.copies=int64_t(k->COPIES()); out.counters.cbytes=int64_t(k->COPY_BYTES()); out.counters.fbytes=int64_t(k->LIVE_FRAME_BYTES());
    out.counters.maxseq=int64_t(k->MAX_SEQ());
    if(k->ROWS()) { out.counters.minseq=int64_t(k->MIN_SEQ()); out.counters.mints=k->MIN_TS(); out.counters.maxts=k->MAX_TS(); }
    if(k->RECORDS()>k->RECORDS_NO_EPOCH()) { out.counters.mine=k->MIN_EPOCH(); out.counters.maxe=k->MAX_EPOCH(); }
    const size_t count=ck->SEGMENTS()?ck->SEGMENTS()->size():0;
    if((ck->SEGMENT_DIGESTS()?ck->SEGMENT_DIGESTS()->size():0)!=count*32) return fail();
    std::set<uint64_t> offsets;
    for(size_t i=0;i<count;i++) { const auto* s=ck->SEGMENTS()->Get(flatbuffers::uoffset_t(i)); CheckpointSegment d;
        d.offset=s->OFFSET(); d.length=s->LENGTH(); d.kind=uint8_t(s->KIND()); d.level=s->LEVEL(); d.minSeq=s->MIN_SEQ(); d.maxSeq=s->MAX_SEQ(); d.coversThrough=s->COVERS_THROUGH(); d.entries=s->ENTRIES();
        if(s->RESERVED() || d.kind>4 || d.offset>=offset || d.length>offset-d.offset-4 || d.coversThrough>out.appliedThrough || !offsets.insert(d.offset).second) return fail();
        std::memcpy(d.digest.data(),ck->SEGMENT_DIGESTS()->data()+i*32,32); out.segmentBytes+=d.length+4; out.segments.push_back(d);
    }
    std::sort(out.segments.begin(),out.segments.end(),[](const CheckpointSegment& a,const CheckpointSegment& b){return a.offset<b.offset;});
    SegmentData data;
    for(const auto& d:out.segments) { SegmentData part; if(readSegment(f,stream,d,&part)!=P4_OK) return fail(); merge(&data,part); }
    out.dictionary=std::move(data.dictionary); out.latest=std::move(data.latest); out.instances=std::move(data.instances); out.tombstones=std::move(data.tombstones);
    for(auto it=out.latest.begin();it!=out.latest.end();) { if(!it->second.rid) it=out.latest.erase(it); else ++it; }
    for(auto it=out.instances.begin();it!=out.instances.end();) {
        if(!it->second.n) { it=out.instances.erase(it); continue; }
        auto b=out.dictionary.find({2,it->first.first}); if(b!=out.dictionary.end() && b->second.text.size()==3) { it->second.batch=b->second.text[0]; it->second.ppeer=b->second.text[1]; it->second.pkey=b->second.text[2]; }
        auto c=out.dictionary.find({3,it->first.second}); if(c!=out.dictionary.end() && c->second.text.size()==1) it->second.ckey=c->second.text[0]; ++it;
    }
    for(const auto& item:data.tokens) { const auto& t=item.second.text; TokCount x;
        if(t.size()!=6 || !number(t[1],&x.n) || !number(t[2],&x.bytes) || !number(t[3],&x.mints) || !number(t[4],&x.maxts) || !number(t[5],&x.maxseq)) return fail(); if(x.n) out.tokens[item.first]=x;
    }
    out.current=onlySegments(stream,out.appliedThrough,offset);
    out.feedRoot=feedDagCheckpointCid(p,n);
    if(ck->COMMITS_ROOT_CID()) out.commitsRoot.assign(reinterpret_cast<const char*>(ck->COMMITS_ROOT_CID()->data()),ck->COMMITS_ROOT_CID()->size());
    *state=std::move(out); return P4_OK;
}

int32_t checkpointPrepare(Feed* f,Conn* c,Stream* stream,uint64_t previous,uint64_t mark,const Counters& counters,const CheckpointState& old,
                          bool fresh,const std::string& commitsRoot,CheckpointPlan* plan,std::string* err) {
    plan->at=mark; plan->previous=previous; plan->frames.clear(); plan->lengths.clear();
    CheckpointState& out=plan->state; out=CheckpointState();
    out.tombstones=old.tombstones;
    int32_t rc=appliedEnd(stream,previous,&out.appliedThrough); if(rc!=P4_OK) return rc;
    rc=snapshot(f,c,stream,previous,old,counters,&out); if(rc!=P4_OK) { if(err) *err="checkpoint snapshot "+f->path; return rc; }
    out.segments=old.segments; out.checkpointBytes=old.checkpointBytes; out.commitsRoot=commitsRoot;
    SegmentData deltas[5];
    for(const auto& x:out.dictionary) { auto it=old.dictionary.find(x.first); if(it==old.dictionary.end() || it->second.text!=x.second.text) deltas[0].dictionary[x.first]=x.second; }
    for(const auto& x:out.latest) { auto it=old.latest.find(x.first); if(it==old.latest.end() || !(it->second==x.second)) deltas[1].latest[x.first]=x.second; }
    for(const auto& x:old.latest) if(!out.latest.count(x.first)) deltas[1].latest[x.first]={x.first,INT64_MIN,0,0};
    for(const auto& x:out.instances) { auto it=old.instances.find(x.first); if(it==old.instances.end() || !instanceEqual(it->second,x.second)) deltas[2].instances[x.first]=x.second; }
    for(const auto& x:old.instances) if(!out.instances.count(x.first)) deltas[2].instances[x.first]=InstCount{};
    for(const auto& x:out.tokens) { auto it=old.tokens.find(x.first); if(it==old.tokens.end() || !tokenEqual(it->second,x.second)) deltas[3].tokens[x.first]=tokenEntry(x.first,x.second,out.dictionary.at({0,x.first}).text[0]); }
    for(const auto& x:old.tokens) if(!out.tokens.count(x.first)) deltas[3].tokens[x.first]=tokenEntry(x.first,TokCount{},out.dictionary.at({0,x.first}).text[0]);
    for(auto rid:out.tombstones) if(!old.tombstones.count(rid)) deltas[4].tombstones.insert(rid);
    // This plan's own segments (offsets >= mark) are merged from memory.
    uint64_t cursor=mark; std::map<uint64_t,SegmentData> made;
    for(uint8_t kind=0;kind<5;kind++) if(deltas[kind].size(kind)) {
        CheckpointSegment desc; encodeSegment(f,kind,0,deltas[kind],{},out.appliedThrough,counters,&cursor,plan,&desc);
        made[desc.offset]=std::move(deltas[kind]); out.segments.push_back(desc);
        for(uint8_t level=0;level<63;level++) {
            std::vector<CheckpointSegment> inputs; for(const auto& d:out.segments) if(d.kind==kind && d.level==level) inputs.push_back(d);
            if(inputs.size()<4) break;
            std::sort(inputs.begin(),inputs.end(),[](const CheckpointSegment& a,const CheckpointSegment& b){return a.offset<b.offset;}); inputs.resize(4);
            SegmentData combined; std::vector<uint64_t> replaces;
            for(const auto& d:inputs) {
                if(d.offset>=mark) merge(&combined,made.at(d.offset));
                else { SegmentData part; rc=readSegment(f,stream,d,&part); if(rc!=P4_OK) return rc; merge(&combined,part); }
                replaces.push_back(d.offset);
            }
            // LATEST deltas already include zero-rid deletion markers. Applying
            // the tombstone union again would erase a later reuse of ROW_J.
            encodeSegment(f,kind,uint8_t(level+1),combined,replaces,out.appliedThrough,counters,&cursor,plan,&desc);
            made[desc.offset]=std::move(combined);
            out.segments.erase(std::remove_if(out.segments.begin(),out.segments.end(),[&](const CheckpointSegment& d){return std::find(replaces.begin(),replaces.end(),d.offset)!=replaces.end();}),out.segments.end()); out.segments.push_back(desc);
        }
    }
    std::sort(out.segments.begin(),out.segments.end(),[](const CheckpointSegment& a,const CheckpointSegment& b){return a.offset<b.offset;});
    plan->previousRoot=old.feedRoot;
    if(plan->previousRoot.empty() && fresh) { std::lock_guard<std::mutex> lock(f->type->mu); if(f->checkpoint) plan->previousRoot=f->checkpoint->feedRoot; }
    if(plan->previousRoot.empty()) {
        std::vector<uint64_t> genesis; uint32_t genesisSize=0;
        rc=frame(stream,0,&genesis,&genesisSize); if(rc!=P4_OK) return rc;
        plan->previousRoot=feedDagCheckpointCid(genesis.data(),genesisSize);
    }
    return P4_OK;
}

int32_t checkpointInstall(Feed* f,Stream* stream,CheckpointPlan& plan,int64_t* end,uint64_t* previous,CheckpointState* state) {
    if(*end<0 || uint64_t(*end)<plan.at || plan.frames.size()!=plan.lengths.size()) return P4_E_INTERNAL;
    const uint64_t shift=uint64_t(*end)-plan.at;
    auto place=[&](uint64_t offset) { return offset>=plan.at?offset+shift:offset; };
    CheckpointState out=std::move(plan.state); const Counters& counters=out.counters;
    out.current=*previous==plan.previous;
    std::map<uint64_t,std::array<uint8_t,32>> digests;
    for(size_t i=0;i<plan.frames.size();i++) {
        auto* p=reinterpret_cast<uint8_t*>(plan.frames[i].data()); const uint32_t n=plan.lengths[i]; const uint64_t at=uint64_t(*end);
        auto* table=const_cast<flatbuffers::Table*>(reinterpret_cast<const flatbuffers::Table*>(GetFSG(p)));
        if(!table->SetField<uint64_t>(FSG::VT_SELF_OFFSET,at,0) || !table->SetField<uint64_t>(FSG::VT_PREVIOUS,*previous,0)) return P4_E_INTERNAL;
        if(auto* replaces=const_cast<flatbuffers::Vector<uint64_t>*>(GetFSG(p)->REPLACES()))
            for(flatbuffers::uoffset_t j=0;j<replaces->size();j++) replaces->Mutate(j,place(replaces->Get(j)));
        ps::sha256(p,n,digests[at].data());
        const int32_t rc=append(stream,p,n,end,"FSG"); if(rc!=P4_OK) return rc; *previous=at;
    }
    for(auto& s:out.segments) if(s.offset>=plan.at) {
        s.offset=place(s.offset); auto d=digests.find(s.offset); if(d==digests.end()) return P4_E_INTERNAL; s.digest=d->second;
    }
    flatbuffers::FlatBufferBuilder b; auto type=b.CreateString(f->type->name),provider=b.CreateString(f->provider),source=b.CreateString(f->source);
    int64_t minAt=INT64_MAX,maxAt=0; for(const auto& x:out.instances) { minAt=std::min(minAt,x.second.first); maxAt=std::max(maxAt,x.second.maxat); }
    FCKCounters k(uint64_t(counters.rows),uint64_t(counters.recs),uint64_t(counters.bytes),uint64_t(counters.nnull),uint64_t(counters.copies),uint64_t(counters.cbytes),uint64_t(counters.fbytes),counters.minseq==INT64_MAX?0:uint64_t(counters.minseq),uint64_t(counters.maxseq),counters.mine==INT64_MAX?0:counters.mine,counters.maxe==INT64_MIN?0:counters.maxe,counters.mints==INT64_MAX?0:counters.mints,counters.maxts,minAt==INT64_MAX?0:minAt,maxAt);
    std::vector<::FCKSegment> segments; std::vector<uint8_t> digestBytes;
    for(const auto& s:out.segments) { segments.emplace_back(s.offset,s.length,fckSegmentKind(s.kind),s.level,0,s.minSeq,s.maxSeq,s.coversThrough,s.entries); digestBytes.insert(digestBytes.end(),s.digest.begin(),s.digest.end()); out.segmentBytes+=s.length+4; }
    auto sv=b.CreateVectorOfStructs(segments); auto dv=b.CreateVector(digestBytes);
    auto cv=b.CreateVector(reinterpret_cast<const uint8_t*>(out.commitsRoot.data()),out.commitsRoot.size());
    auto pv=b.CreateVector(reinterpret_cast<const uint8_t*>(plan.previousRoot.data()),plan.previousRoot.size());
    out.offset=uint64_t(*end); FCKBuilder root(b); root.add_SELF_OFFSET(out.offset); root.add_PREVIOUS(*previous); root.add_FEED_ID(f->fid); root.add_TYPE(type); root.add_PROVIDER(provider); root.add_SOURCE(source); root.add_GENERATION(stream->gen); root.add_COMMITTED_AT(nowSec()); root.add_APPLIED_THROUGH(out.appliedThrough); root.add_TYPE_SEQ_HIGH(out.typeSeqHigh); root.add_COUNTERS(&k); root.add_SEGMENTS(sv); root.add_SEGMENT_DIGESTS(dv); root.add_COMMITS_ROOT_CID(cv); if(!plan.previousRoot.empty()) root.add_PREVIOUS_FEED_ROOT_CID(pv);
    auto x=root.Finish(); FinishFCKBuffer(b,x); out.checkpointBytes+=b.GetSize()+4;
    out.feedRoot=feedDagCheckpointCid(b.GetBufferPointer(),b.GetSize());
    const int32_t rc=append(stream,b.GetBufferPointer(),b.GetSize(),end,"FCK"); if(rc!=P4_OK) return rc;
    *previous=out.offset; out.end=uint64_t(*end); *state=std::move(out); return P4_OK;
}

int32_t checkpointAppend(Feed* f,Conn* c,Stream* stream,const Counters& counters,int64_t* end,uint64_t* previous,CheckpointState* state,bool fresh,std::string* err) {
    std::string commitsRoot; const CheckpointState empty; CheckpointPlan plan;
    int32_t rc=feedDagCommitRoot(f,stream,*previous,uint64_t(*end),&commitsRoot,err); if(rc!=P4_OK) return rc;
    rc=checkpointPrepare(f,c,stream,*previous,uint64_t(*end),counters,fresh?empty:*state,fresh,commitsRoot,&plan,err); if(rc!=P4_OK) return rc;
    return checkpointInstall(f,stream,plan,end,previous,state);
}

bool checkpointDue(const Feed* f,bool idle) {
    if(!f->created || f->quarantined || !f->mark) return false;
    if(f->pendingCheckpoint) return idle;
    const uint64_t through=f->checkpoint?f->checkpoint->appliedThrough:0;
    if(uint64_t(f->mark)<=through || (f->checkpoint && uint64_t(f->mark)==f->checkpoint->end)) return false;
    const uint64_t segmentBytes=f->checkpoint?f->checkpoint->segmentBytes:0;
    const uint64_t lastEnd=f->checkpoint?f->checkpoint->end:0;
    return uint64_t(f->mark)-lastEnd>=std::max<uint64_t>(64ull<<20,4*segmentBytes) || f->checkpointRows>=65536 ||
        (idle && f->checkpointAt && monoNs()-f->checkpointAt>=30000000000ull);
}
int32_t checkpointFeed(Feed* f,bool force,std::string* err) {
    Engine* e=f->type->e; Counters counters; CheckpointState state; int64_t end,start; uint64_t previous; bool pending=false;
    { std::lock_guard<std::mutex> g(f->type->mu);
        if(!f->created || f->quarantined || !f->mark || (!force && !checkpointDue(f,true))) return P4_OK;
        if(f->checkpoint && uint64_t(f->mark)==f->checkpoint->end && !f->pendingCheckpoint && !f->checkpoint->commitsRoot.empty()) return P4_OK;
        pending=bool(f->pendingCheckpoint);
        if(pending) state=*f->pendingCheckpoint; else if(f->checkpoint) state=*f->checkpoint;
        counters=f->k; start=f->mark; end=pending?int64_t(state.end):start; previous=pending?state.offset:f->lastCommit;
    }
    int32_t rc=P4_OK; Conn* c=writerPin(e,f,&rc,err); if(!c) return rc;
    std::shared_ptr<Stream> stream=streamCur(f,&rc);
    if(rc==P4_OK && f->end!=end) { writerUnpin(e,f); return P4_E_BUSY; }
    if(rc==P4_OK && !pending) rc=checkpointAppend(f,c,stream.get(),counters,&end,&previous,&state,false,err);
    if(rc==P4_OK) rc=streamSync(stream.get());
    if(rc==P4_OK) {
        int sql=c->exec("BEGIN IMMEDIATE");
        if(sql==SQLITE_OK) sql=writeMeta(c,counters,nowSec(),end,f->gen);
        if(sql==SQLITE_OK) {
            auto* q=c->sql("INSERT OR REPLACE INTO meta(k,v) VALUES('commit',?1)");
            if(!q) sql=SQLITE_ERROR; else { sqlite3_bind_int64(q,1,sqlite3_int64(previous)); int step=sqlite3_step(q); sqlite3_reset(q); if(step!=SQLITE_DONE) sql=step; }
        }
        if(sql==SQLITE_OK) sql=writeStreamCache(c,f,end,previous,f->metadataBytes+end-start,state.typeSeqHigh,f->streamTokens,f->streamObjects,f->streamMoves);
        if(sql==SQLITE_OK) sql=c->exec("COMMIT"); if(sql!=SQLITE_OK) { c->exec("ROLLBACK"); rc=statusOfSqlite(sql); }
    }
    if(rc==P4_OK) { std::lock_guard<std::mutex> g(f->type->mu); f->checkpoint=std::make_shared<CheckpointState>(std::move(state)); f->pendingCheckpoint.reset(); f->checkpointRows=0; f->end=f->mark=f->streamBytes=end; f->lastCommit=previous; f->metadataBytes+=end-start; }
    else if(stream) { streamTruncate(stream.get(),start,false); std::lock_guard<std::mutex> g(f->type->mu); f->pendingCheckpoint.reset(); f->end=start; }
    writerUnpin(e,f); return rc;
}
} }
