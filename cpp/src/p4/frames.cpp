// The format-5 stream codec. Generated SDS tables are confined to persistence.
#include "internal.h"
#include "checkpoint.h"
#include "sds_frames.h"
#include "flatsql/flatsql_io.h"
#include <algorithm>
#ifndef __wasm__
#include <dirent.h>
#endif

namespace flatsql { namespace p4 {
namespace {
int32_t appendBuffer(Stream* s, flatbuffers::FlatBufferBuilder& b, int64_t* end) {
    std::vector<uint8_t> frame(b.GetSize()+4);
    st32(frame.data(), uint32_t(b.GetSize()));
    std::memcpy(frame.data()+4,b.GetBufferPointer(),b.GetSize());
    const int32_t rc=streamWrite(s,*end,frame.data(),frame.size());
    if(rc==P4_OK) *end+=int64_t(frame.size());
    return rc;
}
}
bool verifyFmbCommit(const FMB* m,uint64_t at,uint64_t previous,uint32_t fid,
                  const std::map<uint64_t,FmbFrameProof>& pending) {
    if(m->SELF_OFFSET()!=at || m->PREVIOUS()!=previous || m->FEED_ID()!=fid) return false;
    if(m->ROWS() && m->ROWS()->size()>65536) return false;
    if(m->TYPE_SEQ_HIGH()>uint64_t(INT64_MAX>>16) || (m->GROUPS() && m->GROUPS()->size()>65536)) return false;
    const size_t rowCount=m->ROWS()?m->ROWS()->size():0;
    std::set<std::pair<uint8_t,uint32_t>> entries;
    if(m->DICTIONARY()) for(const auto* e:*m->DICTIONARY()) {
        if(!e->ID() || e->KIND()>fmbDictKind_OBJECT_KEY || !e->TEXT() ||
           !entries.emplace(uint8_t(e->KIND()),e->ID()).second) return false;
        const size_t fields=e->KIND()==fmbDictKind_BATCH?3:
            (e->KIND()==fmbDictKind_TOKEN || e->KIND()==fmbDictKind_NODE?2:1);
        if(e->TEXT()->size()!=fields || (e->KIND()==fmbDictKind_TOKEN && e->ID()>10000000)) return false;
    }
    std::set<uint32_t> sealedRows,signatureRows,identityRows;
    if(m->SEALED()) for(const auto* s:*m->SEALED()) {
        if(s->ROW()>=rowCount || !(m->ROWS()->Get(s->ROW())->FLAGS()&FmbSealed) ||
           !s->LEAF_DIGEST() || s->LEAF_DIGEST()->size()!=32 || !sealedRows.insert(s->ROW()).second) return false;
    }
    if(m->SIGNATURES()) for(const auto* s:*m->SIGNATURES())
        if(s->ROW()>=rowCount || !s->BYTES() || !signatureRows.insert(s->ROW()).second) return false;
    if(m->IDENTITIES()) for(const auto* i:*m->IDENTITIES())
        if(i->ROW()>=rowCount || !i->HASH() || i->HASH()->size()!=32 || !identityRows.insert(i->ROW()).second) return false;
    if(m->REMOVALS()) for(const auto* rm:*m->REMOVALS()) if(!rm->SEQ() || rm->SEQ()>uint64_t(INT64_MAX>>16) || (rm->ROW_J()!=UINT32_MAX && rm->ROW_J()>65535) || rm->REASON()>fmbRemoveReason_MOVED_OUT) return false;
    if(m->URLS()) for(const auto* u:*m->URLS()) if(!u->SEQ() || u->SEQ()>uint64_t(INT64_MAX>>16) || u->ROW_J()>65535) return false;
    size_t dg=0;
    if(m->ROWS()) for(size_t j=0;j<m->ROWS()->size();j++) {
        const auto* row=m->ROWS()->Get(flatbuffers::uoffset_t(j));
        if(row->RESERVED()!=0 || (row->FLAGS()&~uint32_t(127)) || !row->SEQ() || row->SEQ()>uint64_t(INT64_MAX>>16) || row->OFFSET()>=at || row->LENGTH()<8 ||
           !m->GROUPS() || row->GROUP()>=m->GROUPS()->size()) return false;
        if((row->FLAGS()&FmbSealed) && !sealedRows.count(uint32_t(j))) return false;
        if(row->FLAGS()&FmbNewFrame) {
            auto it=pending.find(row->OFFSET());
            if(it==pending.end() || it->second.size!=row->LENGTH() || row->OFFSET()<=previous || row->OFFSET()+4+row->LENGTH()>at ||
               !m->DIGESTS() || dg+32>m->DIGESTS()->size()) return false;
            const uint8_t* expected=m->DIGESTS()->data()+dg;
            if(row->FLAGS()&FmbSealed) {
                const ::FMBSealed* s=nullptr;
                if(m->SEALED()) for(const auto* candidate:*m->SEALED()) if(candidate->ROW()==j) { s=candidate; break; }
                if(!s || !s->LEAF_DIGEST() || s->LEAF_DIGEST()->size()!=32) return false;
                expected=s->LEAF_DIGEST()->data();
            }
            if(std::memcmp(it->second.digest.data(),expected,32)) return false;
            dg+=32;
        }
    }
    return dg==(m->DIGESTS()?m->DIGESTS()->size():0);
}

int32_t streamGenesis(Feed* f, Stream* s, int64_t* end) {
    if(*end != 0) return P4_E_CORRUPT;
    flatbuffers::FlatBufferBuilder b;
    auto type=b.CreateString(f->type->name), provider=b.CreateString(f->provider), source=b.CreateString(f->source);
    FCKCounters counters(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
    FCKBuilder root(b);
    root.add_SELF_OFFSET(0); root.add_PREVIOUS(UINT64_MAX); root.add_FEED_ID(f->fid);
    root.add_TYPE(type); root.add_PROVIDER(provider); root.add_SOURCE(source);
    root.add_GENERATION(s->gen); root.add_COUNTERS(&counters);
    auto x=root.Finish(); FinishFCKBuffer(b,x);
    return appendBuffer(s,b,end);
}

int32_t streamAppendCommit(Feed* f, Stream* stream, const FmbCommit& m, int64_t* end, uint64_t* previous) {
    size_t at=0;
    do {
        const size_t count=std::min<size_t>(65536,m.rows.size()-at);
        flatbuffers::FlatBufferBuilder b;
        std::vector<::FMBRow> rows; std::vector<uint8_t> digests;
        std::vector<flatbuffers::Offset<FMBSignature>> signatures;
        std::vector<flatbuffers::Offset<FMBSealed>> sealed;
        std::vector<flatbuffers::Offset<FMBIdentity>> identities;
        for(size_t j=0;j<count;j++) {
            const auto& r=m.rows[at+j];
            rows.emplace_back(r.offset,r.seq,r.sourceTs,r.length,r.group,r.rowJ,r.flags,0);
            if(r.flags&FmbNewFrame) digests.insert(digests.end(),r.digest.begin(),r.digest.end());
            if(!r.signature.empty()) signatures.push_back(CreateFMBSignature(b,uint32_t(j),b.CreateVector(reinterpret_cast<const uint8_t*>(r.signature.data()),r.signature.size())));
            if(r.flags&FmbSealed) sealed.push_back(CreateFMBSealed(b,uint32_t(j),r.epoch,r.objectKey,b.CreateVector(r.leafDigest.data(),32),b.CreateVector(reinterpret_cast<const uint8_t*>(r.columns.data()),r.columns.size())));
            if(!r.identity.empty()) identities.push_back(CreateFMBIdentity(b,uint32_t(j),b.CreateVector(reinterpret_cast<const uint8_t*>(r.identity.data()),r.identity.size())));
        }
        std::vector<::FMBGroup> groups;
        for(const auto& g:m.groups) groups.emplace_back(g.deliveredAt,g.node,g.batch,g.contentKey,g.url);
        std::vector<flatbuffers::Offset<FMBEntry>> dictionary;
        if(at==0) for(const auto& d:m.dictionary) {
            std::vector<flatbuffers::Offset<flatbuffers::String>> text;
            for(const auto& v:d.text) text.push_back(b.CreateString(v));
            dictionary.push_back(CreateFMBEntry(b,fmbDictKind(d.kind),d.id,b.CreateVector(text)));
        }
        std::vector<::FMBRemove> removals; std::vector<::FMBUrl> urls;
        if(at==0) {
            for(const auto& d:m.removals) removals.emplace_back(d.seq,d.rowJ,fmbRemoveReason(d.reason));
            for(const auto& d:m.urls) urls.emplace_back(d.seq,d.rowJ,d.url);
        }
        const auto rv=b.CreateVectorOfStructs(rows); const auto gv=b.CreateVectorOfStructs(groups);
        const auto dv=b.CreateVector(digests); const auto dict=b.CreateVector(dictionary); const auto rm=b.CreateVectorOfStructs(removals); const auto uv=b.CreateVectorOfStructs(urls);
        const auto sv=b.CreateVector(signatures); const auto ev=b.CreateVector(sealed); const auto iv=b.CreateVector(identities);
        FMBBuilder root(b);
        root.add_SELF_OFFSET(uint64_t(*end)); root.add_PREVIOUS(*previous); root.add_FEED_ID(f->fid);
        root.add_COMMITTED_AT(m.committedAt); root.add_TYPE_SEQ_HIGH(m.typeSeqHigh);
        root.add_ROWS(rv); root.add_DIGESTS(dv); root.add_GROUPS(gv); root.add_DICTIONARY(dict);
        root.add_REMOVALS(rm); root.add_URLS(uv); root.add_SIGNATURES(sv); root.add_SEALED(ev); root.add_IDENTITIES(iv);
        auto x=root.Finish(); FinishFMBBuffer(b,x);
        const uint64_t offset=uint64_t(*end);
        int32_t rc=appendBuffer(stream,b,end);
        if(rc!=P4_OK) return rc;
        *previous=offset; at+=count;
    } while(at<m.rows.size());
    return P4_OK;
}

} }

namespace flatsql { namespace p4 {
namespace {
std::string colText(sqlite3_stmt* q,int col) {
    const char* p=reinterpret_cast<const char*>(sqlite3_column_text(q,col));
    return p?std::string(p,size_t(sqlite3_column_bytes(q,col))):std::string();
}
std::string colBlob(sqlite3_stmt* q,int col) {
    const char* p=static_cast<const char*>(sqlite3_column_blob(q,col));
    return p?std::string(p,size_t(sqlite3_column_bytes(q,col))):std::string();
}
}
int32_t appendConsolidated(Feed* f,Conn* c,Stream* stream,int64_t* end,uint64_t* previous,std::string* err,
                          const std::vector<int64_t>* rids,bool includeDictionary) {
    FmbCommit m; m.committedAt=nowSec(); m.typeSeqHigh=uint64_t(f->type->nextSeq-1);
    if(includeDictionary) {
        for(size_t i=0;i<f->type->toks.size();i++) {
            const auto& tok=f->type->toks[i]; m.dictionary.push_back({0,uint32_t(i+1),{tok.token,tok.peer}});
        }
        const char* sql[]={"SELECT id,producer,peer FROM node ORDER BY id","SELECT id,batch,ppeer,pkey FROM batch ORDER BY id","SELECT id,ckey FROM ckey ORDER BY id","SELECT id,url FROM url ORDER BY id"};
        for(uint8_t k=1;k<=4;k++) {
            auto* q=c->sql(sql[k-1]); if(!q) return P4_E_IO; int rc;
            while((rc=sqlite3_step(q))==SQLITE_ROW) {
                FmbEntry d; d.kind=k; d.id=uint32_t(sqlite3_column_int64(q,0));
                for(int j=1;j<sqlite3_column_count(q);j++) d.text.push_back(colText(q,j));
                m.dictionary.push_back(std::move(d));
            }
            sqlite3_reset(q); if(rc!=SQLITE_DONE) return statusOfSqlite(rc);
        }
    }
    // A compaction visits the feed in record-frame order, in bounded chunks.
    // Materialize just this chunk's keys so each FMB does indexed row seeks;
    // scanning all rows for every chunk makes a large rewrite quadratic.
    if(rids) {
        int rc=c->exec("CREATE TEMP TABLE IF NOT EXISTS compact_rid(rid INTEGER PRIMARY KEY) WITHOUT ROWID; DELETE FROM compact_rid");
        if(rc!=SQLITE_OK) return statusOfSqlite(rc);
        auto* insert=c->sql("INSERT OR IGNORE INTO temp.compact_rid(rid) VALUES(?1)");
        if(!insert) return P4_E_IO;
        for(int64_t rid:*rids) {
            sqlite3_bind_int64(insert,1,rid); rc=sqlite3_step(insert); sqlite3_reset(insert);
            if(rc!=SQLITE_DONE) return statusOfSqlite(rc);
        }
    }
    std::unordered_set<uint64_t> frames;
    std::unordered_set<uint32_t> keys;
    auto* q=c->sql(rids?
        "SELECT r.rid,r.seq,r.n,r.b,r.c,r.u,r.at,r.cid,r.e,r.k,r.ts,r.f,r.x,r.off,r.len,r.kk "
        "FROM temp.compact_rid AS chosen CROSS JOIN r ON r.rid=chosen.rid ORDER BY chosen.rid":
        "SELECT rid,seq,n,b,c,u,at,cid,e,k,ts,f,x,off,len,kk FROM r ORDER BY rid");
    if(!q) return P4_E_IO; int rc;
    while((rc=sqlite3_step(q))==SQLITE_ROW) {
        const int64_t rid=sqlite3_column_int64(q,0);
        FmbRow row; row.seq=uint64_t(sqlite3_column_int64(q,1)); row.rowJ=uint16_t(rid&65535);
        row.offset=uint64_t(sqlite3_column_int64(q,13)); row.length=uint32_t(sqlite3_column_int64(q,14));
        row.sourceTs=sqlite3_column_int64(q,10); row.flags=FmbCompacted;
        if(frames.insert(row.offset).second) row.flags|=FmbNewFrame;
        if(sqlite3_column_bytes(q,7)!=32) { sqlite3_reset(q); return P4_E_CORRUPT; }
        cidDigestFromKey(static_cast<const uint8_t*>(sqlite3_column_blob(q,7)),row.digest.data());
        row.signature=colBlob(q,12);
        row.group=uint32_t(m.groups.size());
        m.groups.push_back({sqlite3_column_int64(q,6),uint32_t(sqlite3_column_int64(q,2)),uint32_t(sqlite3_column_int64(q,3)),uint32_t(sqlite3_column_int64(q,4)),uint32_t(sqlite3_column_int64(q,5))});
        if(sqlite3_column_type(q,11)!=SQLITE_NULL) {
            row.flags|=FmbSealed; row.columns=colBlob(q,11);
            row.epoch=sqlite3_column_type(q,8)==SQLITE_NULL?INT64_MIN:sqlite3_column_int64(q,8);
            row.objectKey=uint32_t(sqlite3_column_int64(q,15));
            if(row.objectKey && keys.insert(row.objectKey).second) {
                KVal key; key.from(q,9); m.dictionary.push_back({5,row.objectKey,{std::string(key.type==1?"i":"s")+key.text()}});
            }
            std::string bytes;
            if(streamRead(stream,int64_t(row.offset),row.length,&bytes)!=P4_OK) { sqlite3_reset(q); return P4_E_IO; }
            ps::sha256(reinterpret_cast<const uint8_t*>(bytes.data()),bytes.size(),row.leafDigest.data());
        }
        auto* iq=c->sql("SELECT h FROM ident WHERE seq=?1 UNION ALL SELECT h FROM idst WHERE seq=?1 LIMIT 1");
        if(!iq) { sqlite3_reset(q); return P4_E_IO; }
        sqlite3_bind_int64(iq,1,sqlite3_int64(row.seq));
        if(sqlite3_step(iq)==SQLITE_ROW) row.identity=colBlob(iq,0);
        sqlite3_reset(iq);
        m.rows.push_back(std::move(row));
    }
    sqlite3_reset(q); if(rc!=SQLITE_DONE) return statusOfSqlite(rc);
    if(rids && c->exec("DELETE FROM temp.compact_rid")!=SQLITE_OK) return P4_E_IO;
    if(m.rows.size()>65536) { if(err) *err="compaction chunk exceeds 65536 rows"; return P4_E_INTERNAL; }
    return streamAppendCommit(f,stream,m,end,previous);
}

__attribute__((weak)) bool p4ListStreams(const std::string& directory,std::vector<std::string>* names) {
    names->clear();
#ifndef __wasm__
    DIR* dir=opendir(directory.c_str());
    if(!dir) return !ioExists(directory);
    while(const dirent* d=readdir(dir)) {
        std::string name=d->d_name;
        if(name.size()>7 && name.compare(name.size()-7,7,".fsdata")==0) names->push_back(name);
    }
    closedir(dir); std::sort(names->begin(),names->end()); return true;
#else
    // The seven-import I/O ABI has no enumeration operation. A host must
    // supply this primitive before an absent type registry can be discovered.
    return false;
#endif
}
namespace {
// A compaction generation is authoritative only after its final checkpoint.
// This distinguishes a complete rewrite from a crash between its chunks when
// both SQLite caches have disappeared.
bool completedGeneration(Stream* stream,uint32_t fid,uint32_t gen) {
    const int64_t size=streamSize(stream); int64_t at=0; uint64_t previous=UINT64_MAX;
    bool complete=false;
    std::map<uint64_t,FmbFrameProof> pending;
    while(at+12<=size) {
        std::string prefix; if(streamReadRange(stream,at,12,&prefix)!=P4_OK) break;
        uint32_t n=ld32(reinterpret_cast<const uint8_t*>(prefix.data()));
        if(n<8 || n>(128u<<20) || int64_t(n)>size-at-4) break;
        const bool fmb=!std::memcmp(prefix.data()+8,"$FMB",4), fck=!std::memcmp(prefix.data()+8,"$FCK",4), fsg=!std::memcmp(prefix.data()+8,"$FSG",4);
        if(fmb || fck || fsg) {
            std::string data; if(streamRead(stream,at,n,&data)!=P4_OK) break;
            std::vector<uint64_t> aligned((n+7)/8); std::memcpy(aligned.data(),data.data(),n);
            auto* p=reinterpret_cast<const uint8_t*>(aligned.data()); flatbuffers::Verifier verifier(p,n);
            if(fmb) {
                if(!VerifyFMBBuffer(verifier)) break;
                const auto* m=GetFMB(p);
                if(!verifyFmbCommit(m,uint64_t(at),previous,fid,pending)) break;
            } else if(fsg) {
                if(!VerifyFSGBuffer(verifier)) break;
                const auto* segment=GetFSG(p);
                if(segment->SELF_OFFSET()!=uint64_t(at) || segment->PREVIOUS()!=previous || segment->FEED_ID()!=fid) break;
            } else {
                if(!VerifyFCKBuffer(verifier)) break;
                const auto* c=GetFCK(p);
                if(c->SELF_OFFSET()!=uint64_t(at) || c->PREVIOUS()!=previous || c->FEED_ID()!=fid || c->GENERATION()!=gen) break;
                if(at && c->APPLIED_THROUGH()<=uint64_t(at) && c->COUNTERS()) {
                    Feed feed; feed.fid=fid; CheckpointState state; stream->gen=gen;
                    if(checkpointReadAt(&feed,stream,uint64_t(at),&state,nullptr)!=P4_OK) break;
                    complete=true;
                }
            }
            previous=uint64_t(at); pending.clear();
        } else {
            std::string data; if(streamRead(stream,at,n,&data)!=P4_OK) break;
            FmbFrameProof proof; proof.size=n;
            ps::sha256(reinterpret_cast<const uint8_t*>(data.data()),data.size(),proof.digest.data());
            pending.emplace(uint64_t(at),proof);
        }
        at+=int64_t(n)+4;
    }
    return complete;
}
}
bool streamGenerationComplete(Feed* f,uint32_t gen) {
    if(!gen) return ioExists(streamPath(f,gen));
    Stream stream; stream.path=streamPath(f,gen);
    stream.h=flatsql_io_open(stream.path.data(),int32_t(stream.path.size()),FLATSQL_IO_READ);
    return stream.h>=0 && completedGeneration(&stream,f->fid,gen);
}
int32_t streamDiscover(Type* t,Conn* idx,std::string* err) {
    std::vector<std::string> names;
    if(!p4ListStreams(t->pDir,&names)) {
        if(err) *err="stream discovery requires directory enumeration: "+t->pDir;
        return P4_E_UNSUPPORTED;
    }
    std::set<uint32_t> registered;
    auto* registry=idx->sql("SELECT fid,provider,source,name,gen FROM feed ORDER BY fid");
    if(!registry) return P4_E_IO;
    int rr;
    while((rr=sqlite3_step(registry))==SQLITE_ROW) {
        Feed* f=feedRestore(t,uint32_t(sqlite3_column_int64(registry,0)),colText(registry,1),colText(registry,2),colText(registry,3));
        f->regGen=f->gen=uint32_t(sqlite3_column_int64(registry,4)); registered.insert(f->fid);
    }
    sqlite3_reset(registry); if(rr!=SQLITE_DONE) return statusOfSqlite(rr);
    for(const auto& name:names) {
        Stream stream; stream.path=pathJoin(t->pDir,name);
        stream.h=flatsql_io_open(stream.path.data(),int32_t(stream.path.size()),FLATSQL_IO_READ);
        if(stream.h<0) continue;
        const int64_t size=streamSize(&stream); if(size<12) continue;
        std::string prefix;
        if(streamReadRange(&stream,0,12,&prefix)!=P4_OK || std::memcmp(prefix.data()+8,"$FCK",4)) continue;
        uint32_t n=ld32(reinterpret_cast<const uint8_t*>(prefix.data())); if(n<8 || n>(1u<<20) || int64_t(n)+4>size) continue;
        std::string bytes; if(streamRead(&stream,0,n,&bytes)!=P4_OK) continue;
        std::vector<uint64_t> aligned((n+7)/8); std::memcpy(aligned.data(),bytes.data(),n);
        const auto* p=reinterpret_cast<const uint8_t*>(aligned.data()); flatbuffers::Verifier v(p,n);
        if(!VerifyFCKBuffer(v)) continue;
        const auto* ck=GetFCK(p);
        if(ck->SELF_OFFSET()!=0 || ck->PREVIOUS()!=UINT64_MAX || !ck->FEED_ID() || ck->FEED_ID()>1000000 || !ck->TYPE() || ck->TYPE()->str()!=t->name || !ck->PROVIDER() || !ck->SOURCE()) continue;
        if(registered.count(ck->FEED_ID())) continue;
        const uint32_t gen=ck->GENERATION();
        if(gen && !completedGeneration(&stream,ck->FEED_ID(),gen)) continue;
        std::string base=name.substr(0,name.size()-7);
        if(gen) {
            const std::string suffix="."+std::to_string(gen);
            if(base.size()<suffix.size() || base.compare(base.size()-suffix.size(),suffix.size(),suffix)!=0) continue;
            base.resize(base.size()-suffix.size());
        }
        Feed* f=t->feedById(ck->FEED_ID());
        // The newest complete generation wins when the registry was lost.
        // Existing registry generations remain authoritative until recovery.
        if(!f || gen>=f->gen) {
            f=feedRestore(t,ck->FEED_ID(),ck->PROVIDER()->str(),ck->SOURCE()->str(),base);
            f->gen=gen; f->regGen=gen;
            const int rc=indexPutFeed(idx,*f,gen); if(rc!=SQLITE_OK) return statusOfSqlite(rc);
        }
    }
    // Each feed recovery restores TOKEN entries while following its committed
    // metadata chain. Record frames need no scan during a normal cache open.
    return P4_OK;
}
} }
