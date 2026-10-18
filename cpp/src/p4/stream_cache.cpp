// A disposable summary of already-applied stream metadata. It lives in SQLite
// meta, in the same transaction as mark; recovery still verifies every frame
// after mark. No bytes here are needed when either SQLite cache disappears.
#include "internal.h"
#include <limits>

namespace flatsql { namespace p4 {
int writeStreamCache(Conn* c,Feed* f,int64_t mark,uint64_t previous,int64_t metadataBytes,uint64_t high,
                     const std::set<uint32_t>& tokens,const std::set<uint32_t>& objects,const std::set<int64_t>& moves) {
    std::vector<uint8_t> b(48);
    st32(b.data(),1); st32(b.data()+4,f->gen); st64(b.data()+8,uint64_t(mark)); st64(b.data()+16,previous);
    st64(b.data()+24,uint64_t(metadataBytes)); st64(b.data()+32,high);
    st32(b.data()+40,uint32_t(tokens.size())); st32(b.data()+44,uint32_t(objects.size()));
    auto put32=[&](uint32_t x) { size_t at=b.size(); b.resize(at+4); st32(b.data()+at,x); };
    auto put64=[&](uint64_t x) { size_t at=b.size(); b.resize(at+8); st64(b.data()+at,x); };
    put32(uint32_t(moves.size()));
    {
        std::lock_guard<std::mutex> lock(f->type->mu);
        for(uint32_t id:tokens) {
            const TokDef* tok=f->type->tokById(id); if(!tok) return SQLITE_CORRUPT;
            put32(id); put32(uint32_t(tok->token.size())); put32(uint32_t(tok->peer.size()));
            b.insert(b.end(),tok->token.begin(),tok->token.end()); b.insert(b.end(),tok->peer.begin(),tok->peer.end());
        }
    }
    for(uint32_t id:objects) put32(id);
    for(int64_t seq:moves) put64(uint64_t(seq));
    sqlite3_stmt* q=c->get(S_META_SET); if(!q) return SQLITE_ERROR;
    sqlite3_bind_text(q,1,"stream_state",-1,SQLITE_STATIC);
    sqlite3_bind_blob(q,2,b.data(),int(b.size()),SQLITE_TRANSIENT);
    int rc=sqlite3_step(q); sqlite3_reset(q); return rc==SQLITE_DONE?SQLITE_OK:rc;
}
int readStreamCache(Conn* c,Feed* f,int64_t mark,uint64_t previous,uint64_t* high,bool* found) {
    *found=false;
    sqlite3_stmt* q=c->sql("SELECT v FROM meta WHERE k='stream_state'"); if(!q) return SQLITE_ERROR;
    int rc=sqlite3_step(q);
    std::vector<uint8_t> b;
    if(rc==SQLITE_ROW && sqlite3_column_type(q,0)==SQLITE_BLOB) {
        const auto* p=static_cast<const uint8_t*>(sqlite3_column_blob(q,0));
        const int n=sqlite3_column_bytes(q,0); if(n) b.assign(p,p+n);
    }
    sqlite3_reset(q);
    if(rc!=SQLITE_ROW && rc!=SQLITE_DONE) return rc;
    if(b.size()<52 || ld32(b.data())!=1 || ld32(b.data()+4)!=f->gen || ld64(b.data()+8)!=uint64_t(mark) ||
       ld64(b.data()+16)!=previous || ld64(b.data()+24)>uint64_t(mark) || ld64(b.data()+32)>uint64_t(INT64_MAX>>16)) return SQLITE_OK;
    const uint32_t nt=ld32(b.data()+40), no=ld32(b.data()+44), nm=ld32(b.data()+48);
    if(uint64_t(nt)*12+uint64_t(no)*4+uint64_t(nm)*8>b.size()-52) return SQLITE_OK;
    std::map<uint32_t,TokDef> tokens;
    std::set<uint32_t> objects; std::set<int64_t> moves;
    size_t at=52;
    for(uint32_t i=0;i<nt;i++) {
        if(at>b.size() || b.size()-at<12) return SQLITE_OK;
        uint32_t id=ld32(b.data()+at), n=ld32(b.data()+at+4), p=ld32(b.data()+at+8); at+=12;
        if(!id || id>10000000 || uint64_t(n)+p>b.size()-at || tokens.count(id)) return SQLITE_OK;
        TokDef td; td.token.assign(reinterpret_cast<const char*>(b.data()+at),n); at+=n;
        td.peer.assign(reinterpret_cast<const char*>(b.data()+at),p); at+=p; td.registered=true;
        tokens.emplace(id,std::move(td));
    }
    if(uint64_t(no)*4+uint64_t(nm)*8!=b.size()-at) return SQLITE_OK;
    for(uint32_t i=0;i<no;i++,at+=4) { uint32_t id=ld32(b.data()+at); if(!id || !objects.insert(id).second) return SQLITE_OK; }
    for(uint32_t i=0;i<nm;i++,at+=8) { uint64_t seq=ld64(b.data()+at); if(!seq || seq>uint64_t(INT64_MAX>>16) || !moves.insert(int64_t(seq)).second) return SQLITE_OK; }
    Type* t=f->type;
    std::lock_guard<std::mutex> lock(t->mu);
    for(const auto& kv:tokens) {
        const TokDef* existing=t->tokById(kv.first);
        if(existing && existing->registered && (existing->token!=kv.second.token || existing->peer!=kv.second.peer)) return SQLITE_OK;
    }
    for(const auto& kv:tokens) {
        const TokDef* existing=t->tokById(kv.first);
        if(existing && existing->registered) continue;
        rc=indexPutTok(t->idx,kv.first,kv.second); if(rc!=SQLITE_OK) return rc;
        while(t->toks.size()<kv.first) { TokDef ph; ph.token="\x1f#"+std::to_string(t->toks.size()+1); t->toks.push_back(ph); }
        t->toks[kv.first-1]=kv.second; t->tokByToken[kv.second.token]=kv.first;
    }
    f->streamTokens.clear(); for(const auto& kv:tokens) f->streamTokens.insert(kv.first);
    f->streamObjects=std::move(objects); f->streamMoves=std::move(moves);
    f->metadataBytes=int64_t(ld64(b.data()+24)); *high=ld64(b.data()+32); f->streamHigh=*high; *found=true;
    return SQLITE_OK;
}
} }
