// Recovery applies the authoritative SDS commit chain; SQLite is only its cache.
#include "internal.h"
#include "sds_frames.h"
#include "checkpoint.h"
#include "flatsql/flatsql_io.h"
#include <algorithm>
#include <limits>

namespace flatsql { namespace p4 {
namespace {
using Digest=std::array<uint8_t,32>;
struct Frame { uint32_t size=0; std::string bytes; };
struct Replay {
    Feed* f; Conn* c; std::shared_ptr<const Spec> spec;
    std::unordered_map<uint64_t,Digest> digests;
    std::map<InstId,InstCount> inst;
    std::map<uint32_t,TokCount> tokc;
    uint64_t high=0, feedHigh=0;
    Counters history;
    std::unordered_map<uint32_t,uint32_t> nodes;
    std::map<uint32_t,KVal> objects;
    bool catHistoryPending=false;
    std::set<int64_t> moved;
};
int step(sqlite3_stmt* s) {
    if(!s) return SQLITE_ERROR;
    int rc=sqlite3_step(s); sqlite3_reset(s); return rc==SQLITE_DONE ? SQLITE_OK:rc;
}
void bindText(sqlite3_stmt* s,int i,const std::string& v) { sqlite3_bind_text(s,i,v.data(),int(v.size()),SQLITE_TRANSIENT); }
KVal decodeObject(const std::string& value) {
    KVal k;
    if(value.empty()) return k;
    if(value[0]=='i') { k.type=1; k.i=std::strtoll(value.c_str()+1,nullptr,10); }
    else { k.type=3; k.s=value[0]=='s' ? value.substr(1):value; }
    return k;
}
int applyEntries(Replay& r,const std::vector<FmbEntry>& entries) {
    for(const auto& e:entries) {
        if(!e.id || e.text.empty()) return SQLITE_CORRUPT;
        sqlite3_stmt* q=nullptr;
        const std::string a=e.text[0],b=e.text.size()>1?e.text[1]:"",d=e.text.size()>2?e.text[2]:"";
        switch(e.kind) {
            case fmbDictKind_TOKEN: {
                if(e.id>10000000) return SQLITE_CORRUPT;
                Type* t=r.f->type;
                std::lock_guard<std::mutex> g(t->mu);
                while(t->toks.size()<e.id) { TokDef ph; ph.token="\x1f#"+std::to_string(t->toks.size()+1); t->toks.push_back(ph); }
                TokDef td; td.token=a; td.peer=b; td.registered=true;
                t->toks[e.id-1]=td; t->tokByToken[a]=e.id; r.f->streamTokens.insert(e.id);
                int rc=indexPutTok(t->idx,e.id,td); if(rc!=SQLITE_OK) return rc;
                break;
            }
            case fmbDictKind_NODE:
                r.nodes[e.id]=r.f->type->tokByToken[a];
                q=r.c->sql("INSERT OR REPLACE INTO node(id,producer,peer) VALUES(?1,?2,?3)");
                if(!q) return SQLITE_ERROR; sqlite3_bind_int64(q,1,e.id); bindText(q,2,a); bindText(q,3,b); break;
            case fmbDictKind_BATCH:
                q=r.c->sql("INSERT OR REPLACE INTO batch(id,batch,ppeer,pkey) VALUES(?1,?2,?3,?4)");
                if(!q) return SQLITE_ERROR; sqlite3_bind_int64(q,1,e.id); bindText(q,2,a); bindText(q,3,b); bindText(q,4,d); break;
            case fmbDictKind_CONTENT_KEY: case fmbDictKind_URL:
                q=r.c->sql(e.kind==fmbDictKind_URL ? "INSERT OR REPLACE INTO url(id,url) VALUES(?1,?2)":"INSERT OR REPLACE INTO ckey(id,ckey) VALUES(?1,?2)");
                if(!q) return SQLITE_ERROR; sqlite3_bind_int64(q,1,e.id); bindText(q,2,a); break;
            case fmbDictKind_OBJECT_KEY: {
                r.f->streamObjects.insert(e.id);
                r.objects[e.id]=decodeObject(a);
                q=r.c->sql("INSERT OR IGNORE INTO okey(id,k) VALUES(?1,?2)");
                if(!q) return SQLITE_ERROR; sqlite3_bind_int64(q,1,e.id); r.objects[e.id].bind(q,2); break;
            }
            default:return SQLITE_CORRUPT;
        }
        if(q) { int rc=step(q); if(rc!=SQLITE_OK) return rc; }
    }
    return SQLITE_OK;
}
int applyDictionary(Replay& r,const FMB* m) {
    std::vector<FmbEntry> entries;
    if(m->DICTIONARY()) for(const auto* x:*m->DICTIONARY()) {
        FmbEntry entry; entry.kind=uint8_t(x->KIND()); entry.id=x->ID();
        if(x->TEXT()) for(const auto* value:*x->TEXT()) entry.text.push_back(value->str());
        entries.push_back(std::move(entry));
    }
    std::stable_sort(entries.begin(),entries.end(),[](const FmbEntry& a,const FmbEntry& b){ return a.kind<b.kind; });
    return applyEntries(r,entries);
}
// Install the checkpoint's historical state only after the row walk reaches
// APPLIED_THROUGH. Earlier rows rebuild the row index without restamping it.
void checkpointHistory(Replay& r,const CheckpointState& ck) {
    auto history=ck.instances;
    for(auto& item:history) {
        const auto old=r.inst.find(item.first);
        if(item.second.url.empty() && old!=r.inst.end()) item.second.url=old->second.url;
    }
    r.inst=std::move(history); r.tokc=ck.tokens; r.history=ck.counters;
    r.feedHigh=uint64_t(ck.counters.maxseq); r.high=std::max(r.high,ck.typeSeqHigh);
}
const FMBSealed* sealedAt(const FMB* m,size_t row) {
    if(m->SEALED()) for(const auto* s:*m->SEALED()) if(s->ROW()==row) return s;
    return nullptr;
}
int applyCommit(Replay& r,Stream* stream,const FMB* m,const std::map<uint64_t,Frame>& pending) {
    int rc=applyDictionary(r,m); if(rc!=SQLITE_OK) return rc;
    r.high=std::max(r.high,m->TYPE_SEQ_HIGH());
    size_t dg=0;
    if(m->ROWS()) for(size_t j=0;j<m->ROWS()->size();j++) {
        const auto* row=m->ROWS()->Get(flatbuffers::uoffset_t(j));
        const auto* g=m->GROUPS()->Get(row->GROUP());
        r.feedHigh=std::max(r.feedHigh,row->SEQ());
        r.history.mints=std::min(r.history.mints,row->SOURCE_TS());
        r.history.maxts=std::max(r.history.maxts,row->SOURCE_TS());
        const uint32_t token=r.nodes[g->NODE_ID()];
        if(token) {
            auto& tc=r.tokc[token];
            tc.mints=std::min(tc.mints,row->SOURCE_TS()); tc.maxts=std::max(tc.maxts,row->SOURCE_TS()); tc.maxseq=std::max(tc.maxseq,int64_t(row->SEQ()));
        }
        Digest digest{};
        if(row->FLAGS()&FmbNewFrame) { std::memcpy(digest.data(),m->DIGESTS()->data()+dg,32); dg+=32; r.digests[row->OFFSET()]=digest; }
        else {
            auto it=r.digests.find(row->OFFSET());
            if(it!=r.digests.end()) digest=it->second;
            else {
                auto* q=r.c->sql("SELECT cid FROM r WHERE off=?1 LIMIT 1");
                if(!q) return SQLITE_ERROR;
                sqlite3_bind_int64(q,1,sqlite3_int64(row->OFFSET()));
                const int qr=sqlite3_step(q);
                if(qr!=SQLITE_ROW || sqlite3_column_bytes(q,0)!=32) { sqlite3_reset(q); return SQLITE_CORRUPT; }
                cidDigestFromKey(static_cast<const uint8_t*>(sqlite3_column_blob(q,0)),digest.data()); sqlite3_reset(q);
            }
        }
        bool hasE=false; int64_t epoch=0; KVal key; std::string columns;
        if(row->FLAGS()&FmbSealed) {
            const auto* sealed=sealedAt(m,j); if(!sealed) return SQLITE_CORRUPT;
            hasE=sealed->EPOCH()!=INT64_MIN; epoch=sealed->EPOCH();
            if(sealed->COLUMNS()) columns.assign(reinterpret_cast<const char*>(sealed->COLUMNS()->data()),sealed->COLUMNS()->size());
            if(sealed->OBJECT_KEY_ID()) {
                auto oi=r.objects.find(sealed->OBJECT_KEY_ID());
                if(oi!=r.objects.end()) {
                    auto* oq=r.c->sql("INSERT OR IGNORE INTO okey(id,k) VALUES(?1,?2)"); if(!oq) return SQLITE_ERROR;
                    sqlite3_bind_int64(oq,1,sealed->OBJECT_KEY_ID()); oi->second.bind(oq,2);
                    if((rc=step(oq))!=SQLITE_OK) return rc;
                }
                auto* q=r.c->sql("SELECT k FROM okey WHERE id=?1"); if(!q) return SQLITE_ERROR;
                sqlite3_bind_int64(q,1,sealed->OBJECT_KEY_ID());
                if(sqlite3_step(q)!=SQLITE_ROW) { sqlite3_reset(q); return SQLITE_CORRUPT; }
                key.from(q,0); sqlite3_reset(q);
            }
        } else {
            std::string data; const std::string* bytes=nullptr;
            auto it=pending.find(row->OFFSET());
            if(it!=pending.end()) bytes=&it->second.bytes;
            else { if(streamRead(stream,int64_t(row->OFFSET()),row->LENGTH(),&data)!=P4_OK) return SQLITE_CORRUPT; bytes=&data; }
            std::vector<uint8_t> frame(bytes->size()+4); st32(frame.data(),uint32_t(bytes->size())); std::memcpy(frame.data()+4,bytes->data(),bytes->size());
            if(r.spec->tc.checkFrame(frame.data(),uint32_t(frame.size()))) return SQLITE_CORRUPT;
            ps::Extracted x; uint8_t scratch[2048]; r.spec->tc.extract(frame.data(),frame.size(),&x,scratch,sizeof scratch);
            hasE=x.hasEpoch; epoch=x.epochSec;
            if(x.objectCol>=0 && x.objectCol<int(ps::kMaxCols) && x.cols[x.objectCol].present) {
                const auto& v=x.cols[x.objectCol];
                if(v.isU64) { key.type=1; key.i=int64_t(v.u); }
                else { key.type=3; key.s.assign(reinterpret_cast<const char*>(v.s),v.n); }
            }
        }
        if(key.type) { auto* q=r.c->get(S_OKEY_INS); if(!q) return SQLITE_ERROR; key.bind(q,1); if((rc=step(q))!=SQLITE_OK) return rc; }
        const int64_t rid=int64_t(row->SEQ()<<16|row->ROW_J());
        auto* q=r.c->get(S_R_INS); if(!q) return SQLITE_ERROR;
        uint8_t cid[32]; cidKeyFromDigest(digest.data(),cid);
        sqlite3_bind_int64(q,1,rid); sqlite3_bind_int64(q,2,sqlite3_int64(row->SEQ())); sqlite3_bind_int64(q,3,g->NODE_ID());
        sqlite3_bind_int64(q,4,g->BATCH_ID()); sqlite3_bind_int64(q,5,g->CONTENT_KEY_ID()); sqlite3_bind_int64(q,6,g->URL_ID());
        sqlite3_bind_int64(q,7,g->DELIVERED_AT()); sqlite3_bind_blob(q,8,cid,32,SQLITE_TRANSIENT);
        if(hasE) sqlite3_bind_int64(q,9,epoch); else sqlite3_bind_null(q,9);
        key.bind(q,10); sqlite3_bind_int64(q,11,row->SOURCE_TS());
        if(row->FLAGS()&FmbSealed) sqlite3_bind_blob(q,12,columns.data(),int(columns.size()),SQLITE_TRANSIENT);
        if(m->SIGNATURES()) for(const auto* s:*m->SIGNATURES()) if(s->ROW()==j && s->BYTES()) sqlite3_bind_blob(q,13,s->BYTES()->data(),int(s->BYTES()->size()),SQLITE_TRANSIENT);
        sqlite3_bind_int64(q,14,sqlite3_int64(row->OFFSET())); sqlite3_bind_int64(q,15,row->LENGTH());
        if((rc=step(q))!=SQLITE_OK) return rc;
        if(m->IDENTITIES()) for(const auto* id:*m->IDENTITIES()) if(id->ROW()==j) {
            if(!id->HASH() || id->HASH()->size()!=32) return SQLITE_CORRUPT;
            q=r.c->get(S_IDENT_INS); if(!q) return SQLITE_ERROR;
            sqlite3_bind_blob(q,1,id->HASH()->data(),32,SQLITE_TRANSIENT); sqlite3_bind_int64(q,2,sqlite3_int64(row->SEQ()));
            if((rc=step(q))!=SQLITE_OK) return rc;
        }
        if(row->FLAGS()&FmbMovedIn) r.moved.insert(int64_t(row->SEQ()));
        if(g->BATCH_ID() && (!(row->FLAGS()&FmbCopy) || (row->FLAGS()&FmbRetag))) {
            auto& ic=r.inst[{g->BATCH_ID(),g->CONTENT_KEY_ID()}];
            if(!ic.first) ic.first=g->DELIVERED_AT();
            ic.updated=std::max(ic.updated,m->COMMITTED_AT()); ic.maxat=std::max(ic.maxat,g->DELIVERED_AT());
            ic.maxts=std::max(ic.maxts,row->SOURCE_TS());
            int64_t w=hasE ? epoch:row->SOURCE_TS(); ic.minw=std::min(ic.minw,w); ic.maxw=std::max(ic.maxw,w);
            ic.minseq=std::min(ic.minseq,int64_t(row->SEQ())); ic.maxseq=std::max(ic.maxseq,int64_t(row->SEQ()));
            ic.url=dictUrl(r.c,g->URL_ID());
        }
    }
    if(m->URLS()) for(const auto* u:*m->URLS()) {
        auto* q=r.c->get(S_R_URL); if(!q) return SQLITE_ERROR;
        sqlite3_bind_int64(q,1,sqlite3_int64(u->SEQ()<<16|u->ROW_J())); sqlite3_bind_int64(q,2,u->URL_ID());
        if((rc=step(q))!=SQLITE_OK) return rc;
        q=r.c->sql("SELECT b,c,at FROM r WHERE rid=?1"); if(!q) return SQLITE_ERROR;
        sqlite3_bind_int64(q,1,sqlite3_int64(u->SEQ()<<16|u->ROW_J()));
        if(sqlite3_step(q)==SQLITE_ROW) {
            auto& ic=r.inst[{uint32_t(sqlite3_column_int64(q,0)),uint32_t(sqlite3_column_int64(q,1))}];
            ic.updated=std::max(ic.updated,m->COMMITTED_AT()); ic.url=dictUrl(r.c,u->URL_ID());
        } sqlite3_reset(q);
    }
    if(m->REMOVALS()) for(const auto* rm:*m->REMOVALS()) {
        const int64_t lo=int64_t(rm->SEQ()<<16 | (rm->ROW_J()==UINT32_MAX ? 0:rm->ROW_J()));
        const int64_t hi=rm->ROW_J()==UINT32_MAX ? lo|65535:lo;
        if(rm->REASON()==fmbRemoveReason_DELETE || rm->REASON()==fmbRemoveReason_CAT_SUPERSEDE) {
            auto* q=r.c->sql("SELECT DISTINCT b,c FROM r WHERE rid BETWEEN ?1 AND ?2"); if(!q) return SQLITE_ERROR;
            sqlite3_bind_int64(q,1,lo); sqlite3_bind_int64(q,2,hi);
            while(sqlite3_step(q)==SQLITE_ROW) r.inst[{uint32_t(sqlite3_column_int64(q,0)),uint32_t(sqlite3_column_int64(q,1))}].updated=m->COMMITTED_AT();
            sqlite3_reset(q);
        }
        auto* q=r.c->sql("DELETE FROM r WHERE rid BETWEEN ?1 AND ?2"); if(!q) return SQLITE_ERROR;
        sqlite3_bind_int64(q,1,lo); sqlite3_bind_int64(q,2,hi); if((rc=step(q))!=SQLITE_OK) return rc;
        q=r.c->sql("DELETE FROM ident WHERE seq=?1 AND NOT EXISTS(SELECT 1 FROM r WHERE seq=?1)"); if(!q) return SQLITE_ERROR;
        sqlite3_bind_int64(q,1,sqlite3_int64(rm->SEQ())); if((rc=step(q))!=SQLITE_OK) return rc;
        q=r.c->sql("DELETE FROM idst WHERE seq=?1 AND NOT EXISTS(SELECT 1 FROM r WHERE seq=?1)"); if(!q) return SQLITE_ERROR;
        sqlite3_bind_int64(q,1,sqlite3_int64(rm->SEQ())); if((rc=step(q))!=SQLITE_OK) return rc;
    }
    bool removals=m->REMOVALS() && m->REMOVALS()->size();
    bool catOnly=removals;
    if(removals) for(const auto* rm:*m->REMOVALS()) if(rm->REASON()!=fmbRemoveReason_CAT_SUPERSEDE) catOnly=false;
    if(catOnly) r.catHistoryPending=true;
    // Explicit removal calls publish their zero-count instances before a
    // later call can recreate them. CAT's removal frame precedes its own rows;
    // keep that round's history until its following rows have been applied.
    const bool finishCat=r.catHistoryPending && m->ROWS() && m->ROWS()->size();
    if((removals && !catOnly) || finishCat) {
        auto* q=r.c->sql("SELECT DISTINCT node.producer FROM r JOIN node ON node.id=r.n"); if(!q) return SQLITE_ERROR;
        std::set<uint32_t> live;
        while((rc=sqlite3_step(q))==SQLITE_ROW) {
            const char* token=reinterpret_cast<const char*>(sqlite3_column_text(q,0));
            auto it=r.f->type->tokByToken.find(token?token:""); if(it!=r.f->type->tokByToken.end()) live.insert(it->second);
        }
        sqlite3_reset(q); if(rc!=SQLITE_DONE) return rc;
        for(auto it=r.tokc.begin();it!=r.tokc.end();) if(!live.count(it->first)) it=r.tokc.erase(it); else ++it;
        if(live.empty()) r.history=Counters();
        q=r.c->sql("SELECT DISTINCT b,c FROM r WHERE b<>0"); if(!q) return SQLITE_ERROR;
        std::set<InstId> instances;
        while((rc=sqlite3_step(q))==SQLITE_ROW) instances.emplace(uint32_t(sqlite3_column_int64(q,0)),uint32_t(sqlite3_column_int64(q,1)));
        sqlite3_reset(q); if(rc!=SQLITE_DONE) return rc;
        for(auto it=r.inst.begin();it!=r.inst.end();) if(!instances.count(it->first)) it=r.inst.erase(it); else ++it;
        r.catHistoryPending=false;
    }
    return SQLITE_OK;
}
int writeFold(Replay& r,Stream* stream,int64_t mark,uint64_t previous) {
    {
        std::lock_guard<std::mutex> g(r.f->dictMu); r.f->dict=Dict();
    }
    Counters counts;
    std::map<InstId,InstCount> exactInst; std::map<uint32_t,TokCount> exactTok;
    int32_t st=recountFeed(r.f,r.c,stream,mark,&counts,&exactInst,&exactTok);
    if(st!=P4_OK) return SQLITE_CORRUPT;
    counts.maxseq=std::max(counts.maxseq,int64_t(r.feedHigh));
    if(counts.rows) { counts.mints=std::min(counts.mints,r.history.mints); counts.maxts=std::max(counts.maxts,r.history.maxts); }
    for(auto& p:exactInst) {
        auto history=r.inst.find(p.first);
        if(history!=r.inst.end()) {
            const int64_t n=p.second.n, bytes=p.second.bytes;
            InstCount saved=history->second;
            saved.batch=p.second.batch; saved.ppeer=p.second.ppeer; saved.pkey=p.second.pkey; saved.ckey=p.second.ckey;
            saved.n=n; saved.bytes=bytes; p.second=std::move(saved);
        }
    }
    int rc=r.c->exec("DELETE FROM inst; DELETE FROM tokc"); if(rc!=SQLITE_OK) return rc;
    for(const auto& p:exactInst) {
        const auto& ic=p.second; auto* q=r.c->get(S_INST_PUT); if(!q) return SQLITE_ERROR;
        sqlite3_bind_int64(q,1,p.first.first); sqlite3_bind_int64(q,2,p.first.second);
        sqlite3_bind_int64(q,3,ic.n); sqlite3_bind_int64(q,4,ic.bytes);
        if(ic.minw!=INT64_MAX) sqlite3_bind_int64(q,5,ic.minw);
        if(ic.maxw!=INT64_MIN) sqlite3_bind_int64(q,6,ic.maxw);
        if(ic.minseq!=INT64_MAX) sqlite3_bind_int64(q,7,ic.minseq);
        sqlite3_bind_int64(q,8,ic.maxseq); sqlite3_bind_int64(q,9,ic.first); sqlite3_bind_int64(q,10,ic.updated);
        sqlite3_bind_int64(q,11,ic.maxat); sqlite3_bind_int64(q,12,ic.maxts); bindText(q,13,ic.url);
        if((rc=step(q))!=SQLITE_OK) return rc;
    }
    // Token bounds are historical while a token still has copies. Keep the
    // cached history, extending it with every surviving copy after replay.
    auto* q=r.c->sql("SELECT node.producer,min(r.ts),max(r.ts),max(r.seq) FROM r JOIN node ON node.id=r.n GROUP BY node.producer");
    if(!q) return SQLITE_ERROR;
    while((rc=sqlite3_step(q))==SQLITE_ROW) {
        const char* token=reinterpret_cast<const char*>(sqlite3_column_text(q,0));
        auto it=r.f->type->tokByToken.find(token?token:"");
        if(it==r.f->type->tokByToken.end()) continue;
        TokCount& tc=exactTok[it->second];
        auto hist=r.tokc.find(it->second);
        if(hist!=r.tokc.end()) { tc.mints=hist->second.mints; tc.maxts=hist->second.maxts; tc.maxseq=hist->second.maxseq; }
        tc.mints=std::min(tc.mints,sqlite3_column_int64(q,1)); tc.maxts=std::max(tc.maxts,sqlite3_column_int64(q,2)); tc.maxseq=std::max(tc.maxseq,sqlite3_column_int64(q,3));
    }
    sqlite3_reset(q); if(rc!=SQLITE_DONE) return rc;
    std::map<uint32_t,TokCount> empty;
    if((rc=writeTokc(r.f->type,r.c,empty,exactTok))!=SQLITE_OK) return rc;
    if((rc=writeMeta(r.c,counts,nowSec(),mark,r.f->gen))!=SQLITE_OK) return rc;
    q=r.c->get(S_META_SET); if(!q) return SQLITE_ERROR;
    sqlite3_bind_text(q,1,"commit",-1,SQLITE_STATIC); sqlite3_bind_int64(q,2,sqlite3_int64(previous));
    return step(q);
}
bool loadAligned(Stream* stream,uint64_t at,std::vector<uint64_t>* aligned,uint32_t* size) {
    std::string prefix;
    if(streamReadRange(stream,int64_t(at),4,&prefix)!=P4_OK) return false;
    *size=ld32(reinterpret_cast<const uint8_t*>(prefix.data()));
    if(*size<8 || *size>(128u<<20)) return false;
    std::string bytes;
    if(streamRead(stream,int64_t(at),*size,&bytes)!=P4_OK) return false;
    aligned->resize((*size+7)/8); std::memcpy(aligned->data(),bytes.data(),*size); return true;
}
void collectMoves(Stream* stream,uint64_t previous,Feed* f,std::set<int64_t>* moved) {
    f->metadataBytes=0; f->streamHigh=0; f->streamTokens.clear(); f->streamObjects.clear();
    std::string prefix;
    if(streamReadRange(stream,0,4,&prefix)==P4_OK) f->metadataBytes=int64_t(ld32(reinterpret_cast<const uint8_t*>(prefix.data())))+4;
    while(previous) {
        std::vector<uint64_t> aligned; uint32_t size;
        if(!loadAligned(stream,previous,&aligned,&size)) return;
        const uint8_t* p=reinterpret_cast<const uint8_t*>(aligned.data());
        flatbuffers::Verifier verifier(p,size);
        if(!std::memcmp(p+4,"$FCK",4)) {
            if(!VerifyFCKBuffer(verifier)) return;
            const auto* ck=GetFCK(p);
            if(ck->SELF_OFFSET()!=previous || ck->FEED_ID()!=f->fid || ck->PREVIOUS()>=previous) return;
            f->metadataBytes+=int64_t(size)+4;
            f->streamHigh=std::max(f->streamHigh,ck->TYPE_SEQ_HIGH());
            f->type->nextSeq=std::max(f->type->nextSeq,int64_t(ck->TYPE_SEQ_HIGH())+1);
            previous=ck->PREVIOUS(); continue;
        }
        if(!std::memcmp(p+4,"$FSG",4)) {
            if(!VerifyFSGBuffer(verifier)) return;
            const auto* sg=GetFSG(p);
            if(sg->SELF_OFFSET()!=previous || sg->FEED_ID()!=f->fid || sg->PREVIOUS()>=previous) return;
            f->metadataBytes+=int64_t(size)+4; previous=sg->PREVIOUS(); continue;
        }
        if(!VerifyFMBBuffer(verifier)) return;
        const auto* m=GetFMB(p);
        if(m->SELF_OFFSET()!=previous || m->FEED_ID()!=f->fid || m->PREVIOUS()>=previous) return;
        f->metadataBytes+=int64_t(size)+4;
        f->streamHigh=std::max(f->streamHigh,m->TYPE_SEQ_HIGH());
        f->type->nextSeq=std::max(f->type->nextSeq,int64_t(m->TYPE_SEQ_HIGH())+1);
        if(m->DICTIONARY()) for(const auto* entry:*m->DICTIONARY()) if(entry->KIND()==fmbDictKind_OBJECT_KEY) f->streamObjects.insert(entry->ID());
        if(m->DICTIONARY()) for(const auto* entry:*m->DICTIONARY()) if(entry->KIND()==fmbDictKind_TOKEN && entry->ID() && entry->ID()<=10000000 && entry->TEXT() && entry->TEXT()->size()==2) {
            f->streamTokens.insert(entry->ID());
            Type* t=f->type;
            while(t->toks.size()<entry->ID()) { TokDef ph; ph.token="\x1f#"+std::to_string(t->toks.size()+1); t->toks.push_back(ph); }
            TokDef td; td.token=entry->TEXT()->Get(0)->str(); td.peer=entry->TEXT()->Get(1)->str(); td.registered=true;
            t->toks[entry->ID()-1]=td; t->tokByToken[td.token]=entry->ID();
            indexPutTok(t->idx,entry->ID(),td);
        }
        if(m->ROWS()) for(const auto* row:*m->ROWS()) if(row->FLAGS()&FmbMovedIn) moved->insert(int64_t(row->SEQ()));
        previous=m->PREVIOUS();
    }
}
}

int32_t recoverFeed(Type* t,Feed* f,Conn* c,std::vector<int64_t>* moved,std::string* err) {
    FileMeta meta; int rc=readMeta(c,&meta);
    if(rc!=SQLITE_OK) return statusOfSqlite(rc);
    f->gen=meta.gen; f->mark=meta.mark;
    int32_t st=P4_OK; auto stream=streamCur(f,&st); if(!stream) return st;
    const int64_t size=streamSize(stream.get()); if(size<0) return P4_E_IO;
    if(size<meta.mark) {
        f->quarantined=true;
        if(err) *err="stream shorter than committed mark: "+stream->path;
        return P4_OK;
    }
    uint64_t previous=0;
    auto* q=c->sql("SELECT v FROM meta WHERE k='commit'");
    if(q && sqlite3_step(q)==SQLITE_ROW) previous=uint64_t(sqlite3_column_int64(q,0));
    if(q) sqlite3_reset(q);
    const uint64_t openStart=monoNs();
    auto checkpoint=std::make_shared<CheckpointState>(); bool haveCheckpoint=false;
    // A durable index mark is an already-verified anchor. Follow only its
    // metadata links to the checkpoint; missing indexes use the tail search.
    if(meta.mark) {
        uint64_t point=previous;
        for(;;) {
            std::vector<uint64_t> aligned; uint32_t n=0;
            if(!loadAligned(stream.get(),point,&aligned,&n) || point+4+n>uint64_t(meta.mark)) break;
            const auto* p=reinterpret_cast<const uint8_t*>(aligned.data()); flatbuffers::Verifier v(p,n);
            uint64_t prior=UINT64_MAX;
            if(!std::memcmp(p+4,"$FCK",4)) {
                if(!VerifyFCKBuffer(v) || GetFCK(p)->SELF_OFFSET()!=point || GetFCK(p)->FEED_ID()!=f->fid) break;
                if(checkpointReadAt(f,stream.get(),point,checkpoint.get(),err)==P4_OK) haveCheckpoint=true;
                break;
            } else if(!std::memcmp(p+4,"$FSG",4)) {
                if(!VerifyFSGBuffer(v) || GetFSG(p)->SELF_OFFSET()!=point || GetFSG(p)->FEED_ID()!=f->fid) break;
                prior=GetFSG(p)->PREVIOUS();
            } else if(!std::memcmp(p+4,"$FMB",4)) {
                if(!VerifyFMBBuffer(v) || GetFMB(p)->SELF_OFFSET()!=point || GetFMB(p)->FEED_ID()!=f->fid) break;
                prior=GetFMB(p)->PREVIOUS();
            } else break;
            if(prior>=point) break; point=prior;
        }
    }
    if(!haveCheckpoint) {
        st=checkpointFindNewest(f,stream.get(),checkpoint.get(),&haveCheckpoint,err);
        if(st!=P4_OK) return st;
    }
    f->checkpointOpenNs+=monoNs()-openStart;
    if(haveCheckpoint) f->checkpoint=checkpoint;
    Replay replay{}; replay.f=f; replay.c=c; replay.spec=t->spec();
    replay.high=replay.feedHigh=uint64_t(meta.k.maxseq); replay.history=meta.k;
    bool cached=false;
    rc=readStreamCache(c,f,meta.mark,previous,&replay.high,&cached);
    if(rc!=SQLITE_OK) return statusOfSqlite(rc);
    if(cached) replay.moved=f->streamMoves;
    else { collectMoves(stream.get(),previous,f,&replay.moved); replay.high=std::max(replay.high,f->streamHigh); }
    if(cached && size==meta.mark) {
        f->lastCommit=previous; f->end=meta.mark; f->streamBytes=meta.mark;
        st=checkpointRestoreCadence(f,stream.get()); if(st!=P4_OK) return st;
        { std::lock_guard<std::mutex> lock(t->mu); t->nextSeq=std::max(t->nextSeq,int64_t(replay.high)+1); }
        moved->assign(replay.moved.begin(),replay.moved.end());
        return P4_OK;
    }
    if(haveCheckpoint && checkpoint->holdsStateAt(uint64_t(meta.mark))) checkpointHistory(replay,*checkpoint);
    else if(readInst(f,c,&replay.inst)!=SQLITE_OK || readTokc(t,c,&replay.tokc)!=SQLITE_OK) return P4_E_CORRUPT;
    auto* nodes=c->sql("SELECT id,producer FROM node"); if(!nodes) return P4_E_IO;
    while(sqlite3_step(nodes)==SQLITE_ROW) {
        const char* token=reinterpret_cast<const char*>(sqlite3_column_text(nodes,1));
        auto it=t->tokByToken.find(token?token:""); if(it!=t->tokByToken.end()) replay.nodes[uint32_t(sqlite3_column_int64(nodes,0))]=it->second;
    }
    sqlite3_reset(nodes);
    int64_t at=meta.mark,lastEnd=meta.mark;
    bool changed=false;
    std::map<uint64_t,Frame> pending;
    rc=c->exec("BEGIN IMMEDIATE"); if(rc!=SQLITE_OK) return statusOfSqlite(rc);
    auto fail=[&](int status) { c->exec("ROLLBACK"); if(err) *err="replay "+f->path+": "+sqlite3_errmsg(c->db); return status; };
    if(haveCheckpoint) {
        std::vector<FmbEntry> entries;
        for(const auto& entry:checkpoint->dictionary) entries.push_back(entry.second);
        rc=applyEntries(replay,entries); if(rc!=SQLITE_OK) return fail(statusOfSqlite(rc));
    }
    bool restoredHistory=haveCheckpoint && uint64_t(at)>=checkpoint->appliedThrough;
    if(restoredHistory && (uint64_t(at)==checkpoint->appliedThrough || checkpoint->holdsStateAt(uint64_t(at))))
        checkpointHistory(replay,*checkpoint);
    while(at+12<=size) {
        std::string prefix;
        if(streamReadRange(stream.get(),at,12,&prefix)!=P4_OK) break;
        const auto* p=reinterpret_cast<const uint8_t*>(prefix.data());
        const uint32_t n=ld32(p);
        if(n<8 || n>(128u<<20) || int64_t(n)>size-at-4) break;
        const bool batch=std::memcmp(p+8,"$FMB",4)==0, isCheckpoint=std::memcmp(p+8,"$FCK",4)==0, segment=std::memcmp(p+8,"$FSG",4)==0;
        if(!batch && !isCheckpoint && !segment) {
            Frame frame; frame.size=n;
            if(streamRead(stream.get(),at,n,&frame.bytes)!=P4_OK) break;
            pending.emplace(uint64_t(at),std::move(frame)); at+=int64_t(n)+4; continue;
        }
        std::vector<uint64_t> aligned;
        uint32_t actual;
        if(!loadAligned(stream.get(),uint64_t(at),&aligned,&actual)) break;
        const auto* bytes=reinterpret_cast<const uint8_t*>(aligned.data());
        flatbuffers::Verifier verifier(bytes,n);
        if(batch) {
            if(!VerifyFMBBuffer(verifier)) break;
            const auto* m=GetFMB(bytes);
            std::map<uint64_t,FmbFrameProof> proofs;
            // A validated checkpoint commits the historical prefix. Rebuild
            // its SQLite rows from record bytes without rehashing that whole
            // prefix; only new tail records require a fresh digest.
            const bool checkpointPrefix=haveCheckpoint && uint64_t(at)+4+n<=checkpoint->appliedThrough;
            size_t digestAt=0;
            if(m->ROWS()) for(size_t j=0;j<m->ROWS()->size();++j) {
                const auto* row=m->ROWS()->Get(flatbuffers::uoffset_t(j));
                if(!(row->FLAGS()&FmbNewFrame)) continue;
                auto it=pending.find(row->OFFSET());
                if(it!=pending.end() && !proofs.count(row->OFFSET())) {
                    FmbFrameProof proof; proof.size=it->second.size;
                    if(checkpointPrefix) {
                        if(!m->DIGESTS() || digestAt>m->DIGESTS()->size() || m->DIGESTS()->size()-digestAt<32) break;
                        const uint8_t* digest=m->DIGESTS()->data()+digestAt;
                        if(row->FLAGS()&FmbSealed) {
                            const auto* sealed=sealedAt(m,j);
                            if(!sealed || !sealed->LEAF_DIGEST() || sealed->LEAF_DIGEST()->size()!=32) break;
                            digest=sealed->LEAF_DIGEST()->data();
                        }
                        std::memcpy(proof.digest.data(),digest,32);
                    } else ps::sha256(reinterpret_cast<const uint8_t*>(it->second.bytes.data()),it->second.bytes.size(),proof.digest.data());
                    proofs.emplace(row->OFFSET(),proof);
                }
                digestAt+=32;
            }
            if(!verifyFmbCommit(m,uint64_t(at),previous,f->fid,proofs)) break;
            rc=applyCommit(replay,stream.get(),m,pending);
            if(rc!=SQLITE_OK) return fail(statusOfSqlite(rc));
        } else if(isCheckpoint) {
            if(!VerifyFCKBuffer(verifier)) break;
            const auto* ck=GetFCK(bytes);
            if(ck->SELF_OFFSET()!=uint64_t(at) || ck->FEED_ID()!=f->fid || ck->PREVIOUS()!=(at==0 ? UINT64_MAX:previous)) break;
            if(at==0 && (!ck->TYPE() || ck->TYPE()->str()!=t->name || !ck->PROVIDER() || ck->PROVIDER()->str()!=f->provider || !ck->SOURCE() || ck->SOURCE()->str()!=f->source || ck->GENERATION()!=f->gen)) break;
            replay.high=std::max(replay.high,ck->TYPE_SEQ_HIGH());
            if(at && (!haveCheckpoint || uint64_t(at)>checkpoint->offset)) {
                auto newer=std::make_shared<CheckpointState>();
                if(checkpointReadAt(f,stream.get(),uint64_t(at),newer.get(),err)!=P4_OK) break;
                checkpoint=newer; f->checkpoint=newer; haveCheckpoint=true;
                // The walk already applied any $FMB between its APPLIED_THROUGH
                // and here: only a checkpoint with none replaces the walk's history.
                if(checkpoint->current) checkpointHistory(replay,*checkpoint);
                restoredHistory=true;
            }
            if(at && ck->COUNTERS()) {
                const auto* counters=ck->COUNTERS();
                replay.feedHigh=std::max(replay.feedHigh,counters->MAX_SEQ());
                if(counters->ROWS()) {
                    replay.history.mints=std::min(replay.history.mints,counters->MIN_TS());
                    replay.history.maxts=std::max(replay.history.maxts,counters->MAX_TS());
                }
            }
        } else {
            if(!VerifyFSGBuffer(verifier)) break;
            const auto* sg=GetFSG(bytes);
            if(sg->SELF_OFFSET()!=uint64_t(at) || sg->PREVIOUS()!=previous || sg->FEED_ID()!=f->fid) break;
        }
        if(at!=0) f->metadataBytes+=int64_t(n)+4;
        previous=uint64_t(at); at+=int64_t(n)+4; lastEnd=at; pending.clear(); changed=true;
        if(haveCheckpoint && !restoredHistory && uint64_t(at)>=checkpoint->appliedThrough) {
            checkpointHistory(replay,*checkpoint); restoredHistory=true;
        }
    }
    if(lastEnd<size) { st=streamTruncate(stream.get(),lastEnd,true); if(st!=P4_OK) return fail(st); }
    if(changed) {
        rc=writeFold(replay,stream.get(),lastEnd,previous); if(rc!=SQLITE_OK) return fail(statusOfSqlite(rc));
    }
    f->streamHigh=replay.high;
    rc=writeStreamCache(c,f,lastEnd,previous,f->metadataBytes,replay.high,f->streamTokens,f->streamObjects,replay.moved);
    if(rc!=SQLITE_OK) return fail(statusOfSqlite(rc));
    rc=c->exec("COMMIT"); if(rc!=SQLITE_OK) return fail(statusOfSqlite(rc));
    f->streamMoves=replay.moved;
    f->lastCommit=previous; f->end=lastEnd; f->mark=lastEnd; f->streamBytes=lastEnd;
    st=checkpointRestoreCadence(f,stream.get()); if(st!=P4_OK) return st;
    { std::lock_guard<std::mutex> lock(t->mu); t->nextSeq=std::max(t->nextSeq,int64_t(replay.high)+1); }
    moved->assign(replay.moved.begin(),replay.moved.end());
    return P4_OK;
}

int32_t replayRebuildFeed(Engine* e,Type* t,Feed* f,int64_t* indexed,std::string* err) {
    *indexed=0; writerDrop(e,f); e->rpool.dropPath(f->path);
    for(const char* suffix:{"","-wal","-shm","-journal"}) ioUnlink(f->path+suffix);
    { std::lock_guard<std::mutex> lock(t->mu); f->created=false; f->indexed=false; f->quarantined=false; f->k=Counters(); f->inst.clear(); f->tokc.clear(); }
    { std::lock_guard<std::mutex> lock(f->dictMu); f->dict=Dict(); }
    int32_t st=P4_OK; Conn* c=writerPin(e,f,&st,err); if(!c) return st;
    std::vector<int64_t> moved;
    const uint64_t rebuildStart=monoNs(),priorCheckpointNs=f->checkpointOpenNs;
    st=recoverFeed(t,f,c,&moved,err);
    if(st==P4_OK) st=fileCreateIndexes(t,c,f->local);
    f->rowIndexRebuildNs+=monoNs()-rebuildStart-(f->checkpointOpenNs-priorCheckpointNs);
    writerUnpin(e,f);
    if(st!=P4_OK) return st;
    st=feedOpen(t,f,&moved,err);
    if(st==P4_OK) { *indexed=f->k.recs; e->bump(kStRebuilds); }
    return st;
}

} }
