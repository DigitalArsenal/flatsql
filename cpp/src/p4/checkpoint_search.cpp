// Checkpoint discovery is independent of state decoding and row-index replay.
#include "checkpoint.h"
#include "sds_frames.h"
#include <algorithm>

namespace flatsql { namespace p4 {
namespace {
struct Frame {
    uint64_t at=0,end=0,previous=UINT64_MAX;
    uint32_t length=0;
    std::vector<uint64_t> aligned;
    const uint8_t* data() const { return reinterpret_cast<const uint8_t*>(aligned.data()); }
};
bool readFrame(Stream* s,uint64_t at,uint64_t size,Frame* f) {
    if(at>size || size-at<12) return false;
    std::string prefix;
    if(streamReadRange(s,int64_t(at),4,&prefix)!=P4_OK) return false;
    uint32_t n=ld32(reinterpret_cast<const uint8_t*>(prefix.data()));
    if(n<8 || n>(128u<<20) || n>size-at-4) return false;
    std::string bytes;
    if(streamRead(s,int64_t(at),n,&bytes)!=P4_OK) return false;
    f->at=at; f->end=at+4+n; f->length=n; f->aligned.resize((size_t(n)+7)/8);
    std::memcpy(f->aligned.data(),bytes.data(),n); return true;
}
bool commitHeader(Frame* f,Feed* feed) {
    flatbuffers::Verifier v(f->data(),f->length);
    if(!std::memcmp(f->data()+4,"$FCK",4)) {
        if(!VerifyFCKBuffer(v)) return false;
        const auto* x=GetFCK(f->data());
        if(x->SELF_OFFSET()!=f->at || x->FEED_ID()!=feed->fid || x->GENERATION()!=feed->gen) return false;
        f->previous=x->PREVIOUS();
    } else if(!std::memcmp(f->data()+4,"$FSG",4)) {
        if(!VerifyFSGBuffer(v)) return false;
        const auto* x=GetFSG(f->data());
        if(x->SELF_OFFSET()!=f->at || x->FEED_ID()!=feed->fid) return false;
        f->previous=x->PREVIOUS();
    } else if(!std::memcmp(f->data()+4,"$FMB",4)) {
        if(!VerifyFMBBuffer(v)) return false;
        const auto* x=GetFMB(f->data());
        if(x->SELF_OFFSET()!=f->at || x->FEED_ID()!=feed->fid) return false;
        f->previous=x->PREVIOUS();
    } else return false;
    return f->at ? f->previous<f->at : f->previous==UINT64_MAX;
}
bool verifyBatch(Stream* s,const Frame& f,uint64_t size,Feed* feed) {
    const auto* m=GetFMB(f.data());
    std::map<uint64_t,FmbFrameProof> proofs;
    if(m->ROWS()) for(const auto* row:*m->ROWS()) if((row->FLAGS()&FmbNewFrame) && !proofs.count(row->OFFSET())) {
        Frame record;
        if(!readFrame(s,row->OFFSET(),size,&record)) return false;
        FmbFrameProof proof; proof.size=record.length;
        ps::sha256(record.data(),record.length,proof.digest.data()); proofs.emplace(record.at,proof);
    }
    return verifyFmbCommit(m,f.at,f.previous,feed->fid,proofs);
}
// A verifier-valid table inside a record's payload is not a commit point.
// Anchor candidate headers to genesis using frame boundaries. Historical
// record ranges are skipped by size prefix here; only the candidate's tail
// needs their bytes and digests read again.
bool anchored(Stream* s,Feed* feed,Frame current,uint64_t size) {
    while(current.at) {
        Frame previous;
        if(!readFrame(s,current.previous,size,&previous) || !commitHeader(&previous,feed) || previous.end>current.at) return false;
        if(std::memcmp(current.data()+4,"$FMB",4)) {
            if(previous.end!=current.at) return false;
        } else {
            std::map<uint64_t,FmbFrameProof> records;
            uint64_t pos=previous.end;
            while(pos<current.at) {
                if(current.at-pos<12) return false;
                std::string prefix;
                if(streamReadRange(s,int64_t(pos),12,&prefix)!=P4_OK) return false;
                const uint32_t n=ld32(reinterpret_cast<const uint8_t*>(prefix.data()));
                if(n<8 || n>(128u<<20) || uint64_t(n)>current.at-pos-4 ||
                   !std::memcmp(prefix.data()+8,"$FCK",4) || !std::memcmp(prefix.data()+8,"$FMB",4) ||
                   !std::memcmp(prefix.data()+8,"$FSG",4)) return false;
                FmbFrameProof proof; proof.size=n; records.emplace(pos,proof); pos+=uint64_t(n)+4;
            }
            if(pos!=current.at) return false;
            const auto* batch=GetFMB(current.data()); size_t digest=0;
            if(batch->ROWS()) for(size_t i=0;i<batch->ROWS()->size();i++) {
                const auto* row=batch->ROWS()->Get(flatbuffers::uoffset_t(i));
                if(!(row->FLAGS()&FmbNewFrame)) continue;
                auto record=records.find(row->OFFSET());
                if(record==records.end() || !batch->DIGESTS() || digest>batch->DIGESTS()->size() || batch->DIGESTS()->size()-digest<32) return false;
                const uint8_t* expected=batch->DIGESTS()->data()+digest;
                if(row->FLAGS()&FmbSealed) {
                    const ::FMBSealed* sealed=nullptr;
                    if(batch->SEALED()) for(const auto* candidate:*batch->SEALED()) if(candidate->ROW()==i) { sealed=candidate; break; }
                    if(!sealed || !sealed->LEAF_DIGEST() || sealed->LEAF_DIGEST()->size()!=32) return false;
                    expected=sealed->LEAF_DIGEST()->data();
                }
                std::memcpy(record->second.digest.data(),expected,32); digest+=32;
            }
            if(!verifyFmbCommit(batch,current.at,previous.at,feed->fid,records)) return false;
        }
        current=std::move(previous);
    }
    if(std::memcmp(current.data()+4,"$FCK",4)) return false;
    const auto* genesis=GetFCK(current.data());
    return genesis->TYPE() && genesis->TYPE()->str()==feed->type->name &&
        genesis->PROVIDER() && genesis->PROVIDER()->str()==feed->provider &&
        genesis->SOURCE() && genesis->SOURCE()->str()==feed->source;
}
bool candidate(Stream* s,Feed* feed,uint64_t at,uint64_t size,CheckpointState* out,std::string* err) {
    Frame frame;
    if(!readFrame(s,at,size,&frame) || std::memcmp(frame.data()+4,"$FCK",4) || !commitHeader(&frame,feed)) return false;
    if(at) {
        Frame previous;
        if(!readFrame(s,frame.previous,size,&previous) || previous.end!=at || !commitHeader(&previous,feed)) return false;
        if(!std::memcmp(previous.data()+4,"$FMB",4) && !verifyBatch(s,previous,size,feed)) return false;
    }
    if(!anchored(s,feed,frame,size)) return false;
    CheckpointState state;
    if(checkpointReadAt(feed,s,at,&state,err)!=P4_OK) return false;
    // Every complete tail commit must link back to the candidate. A partial
    // frame is a torn tail; a complete invalid commit requires the fallback.
    uint64_t pos=frame.end,prev=at;
    std::map<uint64_t,FmbFrameProof> pending;
    while(pos<size) {
        Frame next;
        if(!readFrame(s,pos,size,&next)) break;
        const bool special=!std::memcmp(next.data()+4,"$FMB",4) || !std::memcmp(next.data()+4,"$FCK",4) || !std::memcmp(next.data()+4,"$FSG",4);
        if(special) {
            if(!commitHeader(&next,feed) || next.previous!=prev) return false;
            if(!std::memcmp(next.data()+4,"$FMB",4) && !verifyFmbCommit(GetFMB(next.data()),next.at,prev,feed->fid,pending)) return false;
            prev=pos; pending.clear();
        } else {
            FmbFrameProof proof; proof.size=next.length;
            ps::sha256(next.data(),next.length,proof.digest.data()); pending.emplace(pos,proof);
        }
        pos=next.end;
    }
    *out=std::move(state); return true;
}
}
// stop: a commit already read (the walk ends there: the rows after it only).
int32_t checkpointTailRows(Feed* feed,Stream* stream,const CheckpointState& checkpoint,uint64_t previous,std::vector<uint64_t>* rids,
                          uint64_t* rowCount,int64_t* lastCommitAt,uint64_t stop) {
    rids->clear();
    if(rowCount) *rowCount=0;
    if(lastCommitAt) *lastCommitAt=0;
    const int64_t extent=streamSize(stream);
    if(extent<0) return P4_E_IO;
    const uint64_t size=uint64_t(extent);
    if(!checkpoint.end || checkpoint.end>size || previous>=size || previous<checkpoint.offset) return P4_E_CORRUPT;
    // Every $FMB that ends past APPLIED_THROUGH: those after the $FCK, and
    // (prepared off the writer) those between APPLIED_THROUGH and the $FCK.
    bool seen=false;
    for(uint64_t at=previous;at!=UINT64_MAX;) {
        if(at==stop) { seen=true; break; }
        Frame f;
        if(!readFrame(stream,at,size,&f) || !commitHeader(&f,feed)) return P4_E_CORRUPT;
        if(at==checkpoint.offset) {
            if(f.end!=checkpoint.end || std::memcmp(f.data()+4,"$FCK",4)) return P4_E_CORRUPT;
            seen=true; if(checkpoint.current) break;
        } else if(!std::memcmp(f.data()+4,"$FMB",4)) {
            if(f.end<=checkpoint.appliedThrough) break;
            const auto* batch=GetFMB(f.data());
            if(rowCount && batch->ROWS()) *rowCount+=batch->ROWS()->size();
            if(lastCommitAt) *lastCommitAt=std::max(*lastCommitAt,batch->COMMITTED_AT());
            if(batch->ROWS()) for(const auto* row:*batch->ROWS()) {
                if(!row->SEQ() || row->SEQ()>uint64_t(INT64_MAX>>16)) return P4_E_CORRUPT;
                rids->push_back((row->SEQ()<<16)|row->ROW_J());
            }
        } else if(at<checkpoint.offset && !std::memcmp(f.data()+4,"$FCK",4)) break;
        at=f.previous;
    }
    if(!seen) return P4_E_CORRUPT;
    std::sort(rids->begin(),rids->end());
    rids->erase(std::unique(rids->begin(),rids->end()),rids->end());
    return P4_OK;
}
int32_t checkpointRestoreCadence(Feed* feed,Stream* stream) {
    std::shared_ptr<const CheckpointState> checkpoint;
    uint64_t previous=0;
    {
        std::lock_guard<std::mutex> lock(feed->type->mu);
        checkpoint=feed->checkpoint; previous=feed->lastCommit;
        feed->checkpointRows=0; feed->checkpointAt=0;
    }
    if(!checkpoint || (previous==checkpoint->offset && checkpoint->current)) return P4_OK;
    uint64_t rows=0; int64_t committedAt=0;
    std::vector<uint64_t> rids;
    const int32_t rc=checkpointTailRows(feed,stream,*checkpoint,previous,&rids,&rows,&committedAt);
    if(rc!=P4_OK) return rc;
    const uint64_t now=monoNs();
    const int64_t wall=nowSec();
    const uint64_t seconds=committedAt>0 && committedAt<wall?uint64_t(wall-committedAt):0;
    const uint64_t age=seconds>UINT64_MAX/1000000000ull?UINT64_MAX:seconds*1000000000ull;
    std::lock_guard<std::mutex> lock(feed->type->mu);
    feed->checkpointRows=rows;
    // Reserve zero for "no commits" and keep a wall-clock age from wrapping
    // the monotonic clock after a reboot or wall-clock adjustment.
    if(committedAt) feed->checkpointAt=now>age?now-age:1;
    return P4_OK;
}
int32_t checkpointFindNewest(Feed* feed,Stream* stream,CheckpointState* out,bool* found,std::string* err) {
    *found=false;
    const int64_t signedSize=streamSize(stream); if(signedSize<0) return P4_E_IO;
    const uint64_t size=uint64_t(signedSize),chunk=uint64_t(1)<<20;
    bool doubt=false;
    for(uint64_t end=size;end>0;) {
        const uint64_t begin=end>chunk?end-chunk:0;
        // Three overlapping bytes include identifiers crossing chunk edges.
        std::string bytes;
        if(streamReadRange(stream,int64_t(begin),int64_t(std::min(size,end+3)-begin),&bytes)!=P4_OK) { doubt=true; break; }
        size_t at=bytes.size();
        while(at>=4) {
            const size_t p=bytes.rfind("$FCK",at-4);
            if(p==std::string::npos) break;
            if(begin+p>=8 && begin+p<end) {
                CheckpointState state;
                if(candidate(stream,feed,begin+p-8,size,&state,err)) { *out=std::move(state); *found=true; return P4_OK; }
                doubt=true;
            }
            if(p<4) break;
            at=p+3;
        }
        if(doubt) break;
        end=begin;
    }
    // Any false identifier, invalid reference or unreadable candidate takes
    // the conservative size-prefix path; no guessed boundary is authoritative.
    uint64_t pos=0,previous=UINT64_MAX;
    std::map<uint64_t,FmbFrameProof> pending;
    while(pos<size) {
        Frame f; if(!readFrame(stream,pos,size,&f)) break;
        const bool ck=!std::memcmp(f.data()+4,"$FCK",4),mb=!std::memcmp(f.data()+4,"$FMB",4),sg=!std::memcmp(f.data()+4,"$FSG",4);
        if(ck||mb||sg) {
            if(!commitHeader(&f,feed) || f.previous!=previous) break;
            if(mb && !verifyFmbCommit(GetFMB(f.data()),pos,previous,feed->fid,pending)) break;
            if(ck) { CheckpointState state; if(checkpointReadAt(feed,stream,pos,&state,err)==P4_OK) { *out=std::move(state); *found=true; } }
            previous=pos; pending.clear();
        } else {
            FmbFrameProof proof; proof.size=f.length;
            ps::sha256(f.data(),f.length,proof.digest.data()); pending.emplace(pos,proof);
        }
        pos=f.end;
    }
    return P4_OK;
}
} }
