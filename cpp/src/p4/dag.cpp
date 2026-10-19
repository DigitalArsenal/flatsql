#include "dag.h"
#include "ipld.h"
#include "sds_frames.h"
#include "checkpoint.h"
#include <algorithm>
#include <array>
#include <unordered_set>

namespace flatsql { namespace p4 {
namespace {
using Digest=std::array<uint8_t,32>;
// A raw stream block: a metadata frame (FMB/FCK/FSG). Its unit is its offset.
struct Raw { uint64_t at=0; uint32_t length=0; Digest digest{}; };
// A record frame. Frames sit in one offset-sorted vector (stream order); a
// unit's own frames are one contiguous run of it. CIDs are built on demand.
struct Frame { uint64_t at=0; uint32_t length=0,owner=0,refs=0; bool large=false; Digest digest{}; };
struct Large { std::string cid,node; std::vector<Digest> leaves; };
// A derived dag-pb node: only its CID, encoded size and unit stay resident;
// its bytes are rebuilt from its children when a closure is read.
struct Link { std::string cid; uint64_t length=0,unit=0; };
struct Unit {
    Raw metadata; Link node;
    uint32_t firstFrame=0,frameCount=0,liveFrames=0;
    std::set<size_t> targets;
    uint64_t live=0,removedAt=0,droppedAt=0,changedAt=0;
    bool linked=false;
};
// Live rows by rid: appends arrive in rid order (a sorted vector); the few
// out-of-order rids (moves keep their seq) go to a side map.
class LiveRows {
public:
    struct Row { uint64_t rid=0; uint32_t frame=UINT32_MAX,unit=0; };
    const Row* find(uint64_t rid) const {
        auto it=std::lower_bound(sorted_.begin(),sorted_.end(),rid,[](const Row& r,uint64_t x){ return r.rid<x; });
        if(it!=sorted_.end() && it->rid==rid) return it->frame==UINT32_MAX?nullptr:&*it;
        auto late=late_.find(rid); return late==late_.end()?nullptr:&late->second;
    }
    void put(uint64_t rid,uint32_t frame,uint32_t unit) {
        if(sorted_.empty() || rid>sorted_.back().rid) { sorted_.push_back({rid,frame,unit}); return; }
        auto it=std::lower_bound(sorted_.begin(),sorted_.end(),rid,[](const Row& r,uint64_t x){ return r.rid<x; });
        if(it!=sorted_.end() && it->rid==rid) { if(it->frame==UINT32_MAX) --dead_; *it={rid,frame,unit}; return; }
        late_[rid]={rid,frame,unit};
    }
    void erase(uint64_t rid) {
        auto it=std::lower_bound(sorted_.begin(),sorted_.end(),rid,[](const Row& r,uint64_t x){ return r.rid<x; });
        if(it!=sorted_.end() && it->rid==rid) {
            if(it->frame!=UINT32_MAX) { it->frame=UINT32_MAX; ++dead_; }
            if(dead_>sorted_.size()/2) { sorted_.erase(std::remove_if(sorted_.begin(),sorted_.end(),[](const Row& r){ return r.frame==UINT32_MAX; }),sorted_.end()); dead_=0; }
            return;
        }
        late_.erase(rid);
    }
    // The live rids of one seq (rid = SEQ << 16 | ROW_J), ascending.
    std::vector<uint64_t> ofSeq(uint64_t seq) const {
        std::vector<uint64_t> out; const uint64_t lo=seq<<16,hi=lo|0xffff;
        for(auto it=std::lower_bound(sorted_.begin(),sorted_.end(),lo,[](const Row& r,uint64_t x){ return r.rid<x; });it!=sorted_.end() && it->rid<=hi;++it) if(it->frame!=UINT32_MAX) out.push_back(it->rid);
        for(auto it=late_.lower_bound(lo);it!=late_.end() && it->first<=hi;++it) out.push_back(it->first);
        std::sort(out.begin(),out.end()); return out;
    }
private:
    std::vector<Row> sorted_; std::map<uint64_t,Row> late_; size_t dead_=0;
};
struct Meta { uint64_t offset=0,previous=0; uint32_t kind=0; std::string bytes; };
int32_t readMeta(Stream* stream,uint64_t at,uint64_t end,Meta* m) {
    if(at>end || end-at<12) return P4_E_CORRUPT;
    std::string prefix; int32_t rc=streamReadRange(stream,int64_t(at),4,&prefix); if(rc!=P4_OK) return rc;
    const uint32_t n=ld32(reinterpret_cast<const uint8_t*>(prefix.data())); if(n<8 || uint64_t(n)>end-at-4) return P4_E_CORRUPT;
    rc=streamReadRange(stream,int64_t(at+4),n,&m->bytes); if(rc!=P4_OK) return rc;
    std::vector<uint64_t> aligned((n+7)/8); std::memcpy(aligned.data(),m->bytes.data(),n);
    const auto* p=reinterpret_cast<const uint8_t*>(aligned.data()); flatbuffers::Verifier v(p,n);
    m->offset=at;
    if(!std::memcmp(p+4,"$FMB",4) && VerifyFMBBuffer(v)) { const auto* x=GetFMB(p); m->previous=x->PREVIOUS(); m->kind=P4_RANGE_FMB; if(x->SELF_OFFSET()!=at) return P4_E_CORRUPT; }
    else if(!std::memcmp(p+4,"$FCK",4) && VerifyFCKBuffer(v)) { const auto* x=GetFCK(p); m->previous=x->PREVIOUS(); m->kind=P4_RANGE_FCK; if(x->SELF_OFFSET()!=at) return P4_E_CORRUPT; }
    else if(!std::memcmp(p+4,"$FSG",4) && VerifyFSGBuffer(v)) { const auto* x=GetFSG(p); m->previous=x->PREVIOUS(); m->kind=P4_RANGE_FSG; if(x->SELF_OFFSET()!=at) return P4_E_CORRUPT; }
    else return P4_E_CORRUPT;
    if(at && m->previous>=at) return P4_E_CORRUPT;
    return P4_OK;
}
std::string rawCid(const Digest& d) { return ipldCidDigest(IpldRaw,d.data()); }
Digest digestOf(const uint8_t* p) { Digest d; std::memcpy(d.data(),p,32); return d; }
Link linkOf(const std::vector<IpldLink>& links,uint64_t unit) {
    const std::string bytes=ipldDagPb(links); return Link{ipldCid(IpldDagPb,bytes.data(),bytes.size()),bytes.size(),unit};
}
}
struct DagState {
    uint32_t gen=0; uint64_t end=0,last=UINT64_MAX,firstCheckpoint=UINT64_MAX;
    std::string path;                 // the generation's stream, relative to the store root
    std::vector<Unit> units;
    std::vector<Frame> frames;        // offset-sorted
    std::map<uint64_t,Large> large;   // by frame offset
    LiveRows live;
    std::vector<DagDrop> drops;
    Raw checkpoint; bool hasCheckpoint=false;
    std::vector<Raw> segments;
    // Fixed 1,024-wide positions let an append or removal replace only its
    // unit and the nodes on its right/affected spine. Empty positions vanish.
    std::vector<std::vector<Link>> tree;
    std::set<size_t> dirty;
    // Commits roots by the commit points builds stopped at (most recent kept).
    std::map<uint64_t,std::string> commitRoots;
};
struct DagHistory {
    // Caches remain generation-scoped. A compaction must not erase drops
    // that the node has not yet read; after restart retained streams rebuild
    // these same entries without an additional authoritative file.
    std::map<uint32_t,std::shared_ptr<DagState>> generations;
};
namespace {
DagBlock rawBlock(const DagState& s,const Raw& r,uint32_t kind) {
    DagBlock b; b.cid=rawCid(r.digest); b.path=s.path; b.offset=r.at+4; b.length=r.length; b.unit=r.at; b.kind=kind; return b;
}
std::string frameCid(const DagState& s,const Frame& f) { return f.large?s.large.at(f.at).cid:rawCid(f.digest); }
uint64_t frameSize(const DagState& s,const Frame& f) { return f.large?s.large.at(f.at).node.size():f.length; }
// A unit node's links: its live frames in stream order, then its FMB leaf.
std::vector<IpldLink> unitLinks(const DagState& s,const Unit& u) {
    std::vector<IpldLink> links;
    for(uint32_t i=u.firstFrame;i<u.firstFrame+u.frameCount;i++) { const Frame& f=s.frames[i]; if(f.refs) links.push_back({frameCid(s,f),"",frameSize(s,f)}); }
    links.push_back({rawCid(u.metadata.digest),"",u.metadata.length}); return links;
}
std::vector<IpldLink> treeLinks(const DagState& s,size_t level,size_t parent) {
    std::vector<IpldLink> links; const size_t count=level?s.tree[level-1].size():s.units.size();
    for(size_t j=parent*1024;j<std::min(count,(parent+1)*1024);j++) { const Link& child=level?s.tree[level-1][j]:s.units[j].node; if(!child.cid.empty()) links.push_back({child.cid,"",child.length}); }
    return links;
}
std::vector<IpldLink> segmentLinks(const DagState& s) {
    std::vector<IpldLink> links; for(const auto& r:s.segments) links.push_back({rawCid(r.digest),"",r.length}); return links;
}
uint32_t frameAt(const DagState& s,uint64_t at) {
    auto it=std::lower_bound(s.frames.begin(),s.frames.end(),at,[](const Frame& f,uint64_t x){ return f.at<x; });
    return it!=s.frames.end() && it->at==at?uint32_t(it-s.frames.begin()):UINT32_MAX;
}
void eraseRow(DagState& s,const LiveRows::Row row,uint64_t removal,Unit* remover) {
    if(remover) remover->targets.insert(row.unit);
    Unit& owner=s.units[row.unit]; if(owner.live) --owner.live; owner.removedAt=std::max(owner.removedAt,removal); owner.changedAt=std::max(owner.changedAt,removal); s.dirty.insert(row.unit);
    Frame& frame=s.frames[row.frame];
    if(frame.refs && !--frame.refs) {
        Unit& home=s.units[frame.owner]; --home.liveFrames; s.dirty.insert(frame.owner);
        home.removedAt=std::max(home.removedAt,removal); home.changedAt=std::max(home.changedAt,removal);
        s.drops.push_back({frameCid(s,frame),removal,s.gen});
        if(frame.large) for(const auto& leaf:s.large.at(frame.at).leaves) s.drops.push_back({rawCid(leaf),removal,s.gen});
    }
    s.live.erase(row.rid);
}
int32_t apply(DagState& s,const Meta& m) {
    std::vector<uint64_t> aligned((m.bytes.size()+7)/8); std::memcpy(aligned.data(),m.bytes.data(),m.bytes.size());
    const auto* p=reinterpret_cast<const uint8_t*>(aligned.data());
    Raw self; self.at=m.offset; self.length=uint32_t(m.bytes.size()); ps::sha256(p,m.bytes.size(),self.digest.data());
    if(m.kind==P4_RANGE_FCK) {
        if(m.offset && s.firstCheckpoint==UINT64_MAX) s.firstCheckpoint=m.offset;
        if(s.hasCheckpoint && s.checkpoint.digest!=self.digest) s.drops.push_back({rawCid(s.checkpoint.digest),m.offset,s.gen});
        const auto oldSegments=std::move(s.segments);
        s.checkpoint=self; s.hasCheckpoint=true; s.segments.clear(); const auto* ck=GetFCK(p);
        if(ck->SEGMENTS()) {
            if(!ck->SEGMENT_DIGESTS() || ck->SEGMENT_DIGESTS()->size()!=ck->SEGMENTS()->size()*32) return P4_E_CORRUPT;
            for(size_t i=0;i<ck->SEGMENTS()->size();i++) { const auto* x=ck->SEGMENTS()->Get(flatbuffers::uoffset_t(i)); s.segments.push_back({x->OFFSET(),x->LENGTH(),digestOf(ck->SEGMENT_DIGESTS()->data()+i*32)}); }
        }
        std::set<Digest> liveSegments; for(const auto& r:s.segments) liveSegments.insert(r.digest);
        for(const auto& r:oldSegments) if(!liveSegments.count(r.digest)) s.drops.push_back({rawCid(r.digest),m.offset,s.gen});
    } else if(m.kind==P4_RANGE_FMB) {
        const auto* mb=GetFMB(p); const size_t rows=mb->ROWS()?mb->ROWS()->size():0;
        std::vector<const ::FMBSealed*> sealed; std::vector<const ::FMBLarge*> larges;
        if(!fmbByRow(mb->SEALED(),rows,&sealed) || !fmbByRow(mb->LARGE(),rows,&larges)) return P4_E_CORRUPT;
        const uint32_t index=uint32_t(s.units.size()); s.units.emplace_back(); Unit& u=s.units.back();
        u.metadata=self; u.changedAt=m.offset; u.firstFrame=uint32_t(s.frames.size());
        // The round's new frames, in stream order after every earlier frame.
        size_t dig=0; std::vector<std::pair<Frame,Large>> added;
        for(size_t i=0;i<rows;i++) {
            const auto* r=mb->ROWS()->Get(flatbuffers::uoffset_t(i)); if(!(r->FLAGS()&FmbNewFrame)) continue;
            if(!mb->DIGESTS() || dig+32>mb->DIGESTS()->size()) return P4_E_CORRUPT;
            Frame f; f.at=r->OFFSET(); f.length=r->LENGTH(); f.owner=index; f.digest=digestOf(mb->DIGESTS()->data()+dig); dig+=32;
            if(r->FLAGS()&FmbSealed) { const auto* x=fmbRow(sealed,i); if(!x || !x->LEAF_DIGEST() || x->LEAF_DIGEST()->size()!=32) return P4_E_CORRUPT; f.digest=digestOf(x->LEAF_DIGEST()->data()); }
            Large big;
            if(r->FLAGS()&FmbLarge) {
                const auto* x=fmbRow(larges,i); const size_t leaves=(uint64_t(r->LENGTH())+IpldChunkSize-1)/IpldChunkSize;
                if(!x || !x->ROOT_CID() || x->ROOT_CID()->size()!=36 || !x->ROOT_NODE() || !x->LEAF_DIGESTS() || x->LEAF_DIGESTS()->size()!=leaves*32) return P4_E_CORRUPT;
                f.large=true; big.cid.assign(reinterpret_cast<const char*>(x->ROOT_CID()->data()),36);
                big.node.assign(reinterpret_cast<const char*>(x->ROOT_NODE()->data()),x->ROOT_NODE()->size());
                for(size_t j=0;j<leaves;j++) big.leaves.push_back(digestOf(x->LEAF_DIGESTS()->data()+j*32));
            }
            added.emplace_back(f,std::move(big));
        }
        std::sort(added.begin(),added.end(),[](const auto& a,const auto& b){ return a.first.at<b.first.at; });
        for(auto& item:added) {
            if(!s.frames.empty() && item.first.at<=s.frames.back().at) return P4_E_CORRUPT;
            if(item.first.large) s.large[item.first.at]=std::move(item.second);
            s.frames.push_back(item.first);
        }
        u.frameCount=uint32_t(added.size());
        for(size_t i=0;i<rows;i++) {
            const auto* r=mb->ROWS()->Get(flatbuffers::uoffset_t(i)); const uint64_t rid=(r->SEQ()<<16)|r->ROW_J();
            if(const auto* old=s.live.find(rid)) eraseRow(s,*old,m.offset,nullptr);
            const uint32_t fi=frameAt(s,r->OFFSET()); if(fi==UINT32_MAX) return P4_E_CORRUPT;
            Frame& frame=s.frames[fi];
            if(!frame.refs) { s.units[frame.owner].changedAt=m.offset; ++s.units[frame.owner].liveFrames; }
            ++frame.refs; ++s.units[index].live; s.live.put(rid,fi,index); s.dirty.insert(frame.owner);
        }
        if(mb->URLS()) for(const auto* url:*mb->URLS()) if(const auto* row=s.live.find((url->SEQ()<<16)|url->ROW_J())) s.units[index].targets.insert(row->unit);
        // Match the recovery writer's authoritative apply order.
        if(mb->REMOVALS()) for(const auto* r:*mb->REMOVALS()) {
            if(r->ROW_J()!=UINT32_MAX) { if(const auto* row=s.live.find((r->SEQ()<<16)|r->ROW_J())) eraseRow(s,*row,m.offset,&s.units[index]); }
            else for(uint64_t rid:s.live.ofSeq(r->SEQ())) eraseRow(s,*s.live.find(rid),m.offset,&s.units[index]);
        }
        s.dirty.insert(index);
    }
    s.end=m.offset+4+m.bytes.size(); s.last=m.offset; return P4_OK;
}
void updateTree(DagState& s,uint64_t through) {
    // Removal units and URL updates are live while a unit they affect is live.
    std::vector<bool> linked(s.units.size(),false);
    for(size_t i=0;i<s.units.size();i++) linked[i]=s.units[i].live>0 || s.units[i].liveFrames>0;
    bool changed=true;
    while(changed) { changed=false; for(size_t i=0;i<s.units.size();i++) if(!linked[i]) for(size_t target:s.units[i].targets) if(target<linked.size() && linked[target]) { linked[i]=true; changed=true; break; } }
    // Removal dependencies always point backward in stream order. This fold
    // reconstructs the exact commit that retired a metadata leaf even when
    // the DAG cache was absent while those removals happened.
    std::vector<uint64_t> inactiveAt(s.units.size(),0);
    for(size_t i=0;i<s.units.size();i++) {
        auto& u=s.units[i]; uint64_t at=u.removedAt;
        for(size_t target:u.targets) if(target<i) at=std::max(at,inactiveAt[target]);
        if(linked[i]) { u.droppedAt=0; continue; }
        inactiveAt[i]=at;
        if(at>u.metadata.at && u.droppedAt!=at) { s.drops.push_back({rawCid(u.metadata.digest),at,s.gen}); u.droppedAt=at; }
    }
    for(size_t i=0;i<s.units.size();i++) if(s.units[i].linked!=linked[i]) { s.units[i].linked=linked[i]; s.dirty.insert(i); }
    // A rewritten unit node is a new block published by the change's
    // commit. Keep the FMB leaf's original unit, but let incremental range
    // pulls discover this new link block after a removal in a later unit.
    for(size_t i:s.dirty) { auto& u=s.units[i]; u.node=u.linked?linkOf(unitLinks(s,u),u.changedAt):Link{}; }
    if(s.tree.empty()) s.tree.emplace_back();
    std::set<size_t> dirty=s.dirty; s.dirty.clear(); size_t count=s.units.size(),level=0;
    do {
        const size_t groups=std::max<size_t>(1,(count+1023)/1024); if(s.tree.size()<=level) s.tree.emplace_back();
        auto& nodes=s.tree[level]; const size_t oldSize=nodes.size(); nodes.resize(groups);
        std::set<size_t> parents; for(size_t i:dirty) parents.insert(i/1024); for(size_t i=oldSize;i<groups;i++) parents.insert(i);
        for(size_t parent:parents) nodes[parent]=linkOf(treeLinks(s,level,parent),through);
        dirty=std::move(parents); count=groups; ++level;
    } while(count>1);
    s.tree.resize(level);
}
// The feed root's closure in deterministic DFS pre-order, first occurrence of
// each CID only. Derived node bytes are rebuilt here and must hash to the
// CIDs the cache holds.
class Closure {
public:
    // Only blocks with unit >= from are output. A node's unit is the commit
    // that last rewrote it, never earlier than any block below it, so a node
    // before `from` is pruned with its subtree.
    Closure(const DagState& s,uint64_t end,std::vector<DagBlock>* out,uint64_t from=0):s_(s),end_(end),from_(from),out_(out) {}
    bool root(const std::vector<IpldLink>& links,const std::string& dir,uint64_t unit) {
        if(!node(ipldDagPb(links,&dir),unit,nullptr)) return false;
        if(!raw(s_.checkpoint,P4_RANGE_FCK) || !tree(s_.tree.size()-1,0)) return false;
        const std::string segments=ipldDagPb(segmentLinks(s_)); bool fresh=false;
        if(!node(segments,unit,&fresh)) return false;
        if(fresh) for(const auto& r:s_.segments) if(!raw(r,P4_RANGE_FSG)) return false;
        return true;
    }
private:
    bool first(const std::string& cid) { return seen_.insert(cid).second; }
    bool node(const std::string& bytes,uint64_t unit,bool* fresh,const std::string* expect=nullptr) {
        DagBlock b; b.kind=P4_RANGE_LINK; b.unit=unit; b.cid=ipldCid(IpldDagPb,bytes.data(),bytes.size());
        if(expect && b.cid!=*expect) return false;
        const bool isNew=first(b.cid); if(fresh) *fresh=isNew;
        if(isNew && unit>=from_) { b.bytes=bytes; b.length=bytes.size(); out_->push_back(std::move(b)); }
        return true;
    }
    bool raw(const Raw& r,uint32_t kind,uint64_t unit=UINT64_MAX) {
        DagBlock b=rawBlock(s_,r,kind); if(unit!=UINT64_MAX) b.unit=unit; if(!first(b.cid)) return true;
        if(b.offset>end_ || b.length>end_-b.offset) return false;
        if(b.unit>=from_) out_->push_back(std::move(b));
        return true;
    }
    bool tree(size_t level,size_t index) {
        const Link& self=s_.tree[level][index]; bool fresh=false;
        if(self.unit<from_) return true;
        if(!node(ipldDagPb(treeLinks(s_,level,index)),self.unit,&fresh,&self.cid)) return false;
        if(!fresh) return true;
        const size_t count=level?s_.tree[level-1].size():s_.units.size();
        for(size_t j=index*1024;j<std::min(count,(index+1)*1024);j++) {
            if(level) { if(!s_.tree[level-1][j].cid.empty() && !tree(level-1,j)) return false; }
            else if(!s_.units[j].node.cid.empty() && !unit(s_.units[j])) return false;
        }
        return true;
    }
    bool unit(const Unit& u) {
        if(u.node.unit<from_) return true;
        bool fresh=false; if(!node(ipldDagPb(unitLinks(s_,u)),u.node.unit,&fresh,&u.node.cid)) return false;
        if(!fresh) return true;
        for(uint32_t i=u.firstFrame;i<u.firstFrame+u.frameCount;i++) {
            const Frame& f=s_.frames[i]; if(!f.refs) continue;
            if(!f.large) { if(!raw({f.at,f.length,f.digest},P4_RANGE_RECORD,u.metadata.at)) return false; continue; }
            const Large& big=s_.large.at(f.at); if(!first(big.cid)) continue;
            if(u.metadata.at>=from_) { DagBlock root; root.cid=big.cid; root.bytes=big.node; root.length=big.node.size(); root.unit=u.metadata.at; root.kind=P4_RANGE_LARGE_ROOT; out_->push_back(std::move(root)); }
            for(size_t j=0;j<big.leaves.size();j++) {
                const uint64_t at=f.at+j*IpldChunkSize;
                if(!raw({at,uint32_t(std::min<uint64_t>(IpldChunkSize,f.length-j*IpldChunkSize)),big.leaves[j]},P4_RANGE_LARGE_LEAF,u.metadata.at)) return false;
            }
        }
        return raw(u.metadata,P4_RANGE_FMB);
    }
    const DagState& s_; uint64_t end_,from_; std::vector<DagBlock>* out_; std::unordered_set<std::string> seen_;
};
int32_t build(Feed* feed,Stream* stream,uint64_t previous,uint64_t end,DagSnapshot* out,std::string* err,bool closure=true,uint64_t from=0) {
    if(!feed->dagHistory) feed->dagHistory=std::make_shared<DagHistory>();
    auto& ptr=feed->dagHistory->generations[stream->gen];
    if(ptr && ptr->last!=UINT64_MAX && previous<ptr->last) {
        // The cache never goes back. An earlier point's commits root is one
        // it recorded when a build stopped there; nothing replays the feed.
        const auto it=ptr->commitRoots.find(previous);
        if(closure || it==ptr->commitRoots.end()) { if(err) *err="DAG cache is past commit point "+std::to_string(previous); return P4_E_BUSY; }
        DagSnapshot result; result.through=previous; result.generation=stream->gen; result.commitsRoot=it->second;
        *out=std::move(result); return P4_OK;
    }
    if(!ptr) {
        ptr=std::make_shared<DagState>(); ptr->gen=stream->gen;
        const auto& root=feed->type->e->cfg.root; ptr->path=stream->path;
        if(ptr->path.compare(0,root.size(),root)==0 && ptr->path.size()>root.size() && ptr->path[root.size()]=='/') ptr->path.erase(0,root.size()+1);
    }
    DagState& s=*ptr; std::vector<Meta> tail; uint64_t at=previous;
    while(at!=UINT64_MAX && (s.last==UINT64_MAX || at>s.last)) {
        Meta m; const int32_t rc=readMeta(stream,at,end,&m); if(rc!=P4_OK) { if(err) *err="invalid acknowledged DAG chain: "+stream->path; return rc; }
        at=m.previous; tail.push_back(std::move(m));
    }
    if(s.last!=UINT64_MAX && at!=s.last) return P4_E_CORRUPT;
    for(auto it=tail.rbegin();it!=tail.rend();++it) { const int32_t rc=apply(s,*it); if(rc!=P4_OK) return rc; }
    if(!s.hasCheckpoint) return P4_E_CORRUPT;
    updateTree(s,previous);
    DagSnapshot result; result.through=previous; result.generation=stream->gen;
    const Link& commits=s.tree.back().front(); const Link segments=linkOf(segmentLinks(s),previous);
    s.commitRoots[previous]=commits.cid; while(s.commitRoots.size()>4096) s.commitRoots.erase(s.commitRoots.begin());
    const std::string dir=ipldUnixfsDirectory();
    const std::vector<IpldLink> rootLinks={{rawCid(s.checkpoint.digest),"checkpoint",0},{commits.cid,"commits",0},{segments.cid,"segments",0}};
    const std::string rootBytes=ipldDagPb(rootLinks,&dir);
    result.root=ipldCid(IpldDagPb,rootBytes.data(),rootBytes.size()); result.commitsRoot=commits.cid;
    if(closure && !Closure(s,end,&result.blocks,from).root(rootLinks,dir,previous)) return P4_E_CORRUPT;
    *out=std::move(result); return P4_OK;
}
// Read the three cache values in one SQLite snapshot, then pin that stream
// before releasing the snapshot. Compaction can prepare and swap another
// generation concurrently, but cannot mix its offsets with this one.
int32_t committedStream(Feed* f,std::shared_ptr<Stream>* stream,uint64_t* previous,uint64_t* end,std::string* err) {
    { std::lock_guard<std::mutex> lock(f->type->mu); if(f->quarantined) return P4_E_CORRUPT; }
    int sql=SQLITE_OK; Conn* c=f->type->e->rpool.acquire(f->path,OpenKind::Reader,&sql,err);
    if(!c) return statusOfSqlite(sql);
    sql=c->exec("BEGIN"); uint32_t gen=0; unsigned seen=0;
    if(sql==SQLITE_OK) {
        auto* q=c->sql("SELECT k,v FROM meta WHERE k IN ('gen','mark','commit')");
        if(!q) sql=SQLITE_ERROR;
        else {
            while((sql=sqlite3_step(q))==SQLITE_ROW) {
                const char* key=reinterpret_cast<const char*>(sqlite3_column_text(q,0));
                const int64_t value=sqlite3_column_int64(q,1);
                if(!key || value<0) { sql=SQLITE_CORRUPT; break; }
                if(!std::strcmp(key,"gen")) { gen=uint32_t(value); seen|=1; }
                else if(!std::strcmp(key,"mark")) { *end=uint64_t(value); seen|=2; }
                else if(!std::strcmp(key,"commit")) { *previous=uint64_t(value); seen|=4; }
            }
            sqlite3_reset(q); if(sql==SQLITE_DONE) sql=seen==7?SQLITE_OK:SQLITE_CORRUPT;
        }
    }
    int32_t rc=sql==SQLITE_OK?P4_OK:statusOfSqlite(sql);
    if(rc==P4_OK) { *stream=streamAt(f,gen,&rc); if(!*stream && rc==P4_OK) rc=P4_E_BUSY; }
    c->exec("ROLLBACK"); f->type->e->rpool.release(c);
    if(rc==P4_OK && (*previous>=*end || *end<12)) return P4_E_CORRUPT;
    return rc;
}
// Retired streams are immutable, and can be rediscovered after a restart.
// Skip record payloads; only size prefixes and metadata headers are read.
int32_t retiredBounds(Stream* stream,uint64_t* previous,uint64_t* end,uint64_t* firstCheckpoint=nullptr,uint64_t limit=0) {
    const int64_t length=limit?int64_t(limit):streamSize(stream); if(length<12) return P4_E_CORRUPT;
    *previous=UINT64_MAX; *end=0; if(firstCheckpoint) *firstCheckpoint=UINT64_MAX;
    uint64_t at=0;
    while(at<uint64_t(length)) {
        if(uint64_t(length)-at<12) return P4_E_CORRUPT;
        std::string prefix; const int32_t rc=streamReadRange(stream,int64_t(at),12,&prefix); if(rc!=P4_OK) return rc;
        const uint32_t n=ld32(reinterpret_cast<const uint8_t*>(prefix.data()));
        if(n<8 || uint64_t(n)>uint64_t(length)-at-4) return P4_E_CORRUPT;
        if(!std::memcmp(prefix.data()+8,"$FMB",4) || !std::memcmp(prefix.data()+8,"$FSG",4) || !std::memcmp(prefix.data()+8,"$FCK",4)) {
            *previous=at; *end=at+4+n;
            if(firstCheckpoint && at && *firstCheckpoint==UINT64_MAX && !std::memcmp(prefix.data()+8,"$FCK",4)) *firstCheckpoint=at;
        }
        at+=4+n;
    }
    return *previous==UINT64_MAX?P4_E_CORRUPT:P4_OK;
}
}
int32_t feedDagAt(Feed* f,Stream* stream,uint64_t previous,uint64_t end,DagSnapshot* out,std::string* err) {
    std::lock_guard<std::mutex> lock(f->dagMu); return build(f,stream,previous,end,out,err);
}
int32_t feedDagCommitRootHeld(Feed* f,Stream* stream,uint64_t previous,uint64_t end,std::string* cid,std::string* err) {
    DagSnapshot result;
    const int32_t rc=build(f,stream,previous,end,&result,err,false); if(rc==P4_OK) *cid=std::move(result.commitsRoot); return rc;
}
int32_t feedDagCommitRoot(Feed* f,Stream* stream,uint64_t previous,uint64_t end,std::string* cid,std::string* err) {
    std::lock_guard<std::mutex> lock(f->dagMu); return feedDagCommitRootHeld(f,stream,previous,end,cid,err);
}
std::string feedDagCheckpointCid(const void* bytes,size_t length) {
    std::vector<uint64_t> aligned((length+7)/8); std::memcpy(aligned.data(),bytes,length);
    const auto* ck=GetFCK(aligned.data()); std::vector<IpldLink> links;
    if(ck->SEGMENTS() && ck->SEGMENT_DIGESTS()) for(size_t i=0;i<ck->SEGMENTS()->size();i++) {
        links.push_back({ipldCidDigest(0x55,ck->SEGMENT_DIGESTS()->data()+i*32),"",ck->SEGMENTS()->Get(flatbuffers::uoffset_t(i))->LENGTH()});
    }
    const std::string segments=ipldDagPb(links); links.clear();
    const std::string empty=ipldDagPb({});
    const std::string commits=ck->COMMITS_ROOT_CID()?std::string(reinterpret_cast<const char*>(ck->COMMITS_ROOT_CID()->data()),ck->COMMITS_ROOT_CID()->size()):ipldCid(0x70,empty.data(),empty.size());
    // Link Tsize is the target block's encoded size. COMMITS_ROOT_CID alone
    // does not carry that size, so directory links encode Tsize=0 consistently.
    links.push_back({ipldCid(0x55,bytes,length),"checkpoint",0});
    links.push_back({commits,"commits",0});
    links.push_back({ipldCid(0x70,segments.data(),segments.size()),"segments",0});
    const std::string data=ipldUnixfsDirectory(),root=ipldDagPb(links,&data);
    return ipldCid(0x70,root.data(),root.size());
}
int32_t feedDagSnapshot(Feed* f,DagSnapshot* out,std::string* err,uint64_t from,bool closure) {
    uint64_t previous=0,end=0; std::shared_ptr<Stream> stream;
    int32_t rc;
    {
        // The snapshot's committed point is read under dagMu, as every
        // builder's is: the cache only moves forward, never resets for a
        // point another builder already passed (smu holders never take dagMu).
        std::lock_guard<std::mutex> lock(f->dagMu);
        rc=committedStream(f,&stream,&previous,&end,err); if(rc!=P4_OK) return rc;
        rc=build(f,stream.get(),previous,end,out,err,closure,from); if(rc!=P4_OK) return rc;
    }
    out->stream=std::move(stream);
    // Released generations whose files have left the retained set no longer
    // need an in-memory replay cache. Never nest the stream and DAG locks.
    std::set<uint32_t> retained;
    { std::lock_guard<std::mutex> lock(f->smu); retained.insert(f->gen); if(f->retired) retained.insert(f->retired->gen); for(const auto& old:f->olderRetired) retained.insert(old.stream->gen); }
    retained.insert(out->generation);
    { std::lock_guard<std::mutex> lock(f->dagMu); for(auto it=f->dagHistory->generations.begin();it!=f->dagHistory->generations.end();) { if(!retained.count(it->first)) it=f->dagHistory->generations.erase(it); else ++it; } }
    return P4_OK;
}
namespace {
struct DigestHash { size_t operator()(const Digest& d) const { size_t h; std::memcpy(&h,d.data(),sizeof h); return h; } };
// Which of the candidate CIDs the generation's closure at its committed
// point holds, from the cache's live entries (the closure's leaves: the
// checkpoint, the segments, each linked unit's FMB and live frames, a large
// frame's root and leaves) by digest, without rebuilding a node.
void liveOf(const DagState& s,const std::unordered_set<std::string>& candidates,std::unordered_set<std::string>* live) {
    std::unordered_map<Digest,std::string,DigestHash> raw; std::unordered_set<std::string> nodes;
    for(const auto& cid:candidates) {
        if(cid.size()==36 && uint8_t(cid[1])==IpldRaw) { Digest d; std::memcpy(d.data(),cid.data()+4,32); raw.emplace(d,cid); }
        else nodes.insert(cid);
    }
    auto probe=[&](const Digest& d) { if(raw.empty()) return; auto it=raw.find(d); if(it!=raw.end()) live->insert(it->second); };
    if(s.hasCheckpoint) probe(s.checkpoint.digest);
    for(const auto& r:s.segments) probe(r.digest);
    for(const auto& u:s.units) {
        if(u.node.cid.empty()) continue;
        probe(u.metadata.digest);
        for(uint32_t i=u.firstFrame;i<u.firstFrame+u.frameCount;i++) {
            const Frame& fr=s.frames[i]; if(!fr.refs) continue;
            if(!fr.large) { probe(fr.digest); continue; }
            const Large& big=s.large.at(fr.at); if(nodes.count(big.cid)) live->insert(big.cid);
            for(const auto& leaf:big.leaves) probe(leaf);
        }
    }
}
}
int32_t feedDagDrops(Feed* f,uint64_t from,std::vector<DagDrop>* out,std::string* err) {
    // The committed point without its closure: a drop is listed unless the
    // closure still holds its CID, which the cache answers per candidate.
    DagSnapshot snapshot; int32_t rc=feedDagSnapshot(f,&snapshot,err,0,false); if(rc!=P4_OK) return rc;
    std::vector<uint32_t> retired;
    { std::lock_guard<std::mutex> lock(f->smu); if(f->retired) retired.push_back(f->retired->gen); for(const auto& r:f->olderRetired) retired.push_back(r.stream->gen); }
    for(uint32_t gen:retired) {
        auto stream=streamAt(f,gen,&rc); if(!stream) { if(rc!=P4_OK) return rc; continue; }
        const int64_t length=streamSize(stream.get()); if(length<0) return P4_E_IO;
        { std::lock_guard<std::mutex> lock(f->dagMu); const auto cached=f->dagHistory->generations.find(gen); if(cached!=f->dagHistory->generations.end() && cached->second && cached->second->end==uint64_t(length)) continue; }
        uint64_t previous,end; rc=retiredBounds(stream.get(),&previous,&end); if(rc!=P4_OK) return rc;
        std::lock_guard<std::mutex> lock(f->dagMu); DagSnapshot ignored;
        rc=build(f,stream.get(),previous,end,&ignored,err,false); if(rc!=P4_OK) return rc;
    }
    std::lock_guard<std::mutex> lock(f->dagMu); std::set<std::tuple<std::string,uint64_t,uint32_t>> seen;
    const auto current=f->dagHistory->generations.find(snapshot.generation);
    if(current==f->dagHistory->generations.end() || !current->second) return P4_E_INTERNAL;
    // The candidates first, in their order; then one pass over the cache
    // for the ones the closure holds.
    std::vector<DagDrop> candidates;
    for(const auto& generation:f->dagHistory->generations) if(generation.first<=snapshot.generation)
        for(const auto& d:generation.second->drops) if(d.unit>=from &&
            (d.generation<snapshot.generation || d.unit<=snapshot.through) && seen.emplace(d.cid,d.unit,d.generation).second) candidates.push_back(d);
    // A compaction replaces the old generation's still-live metadata too.
    // Those leaves are not removals within the old stream, so retire them at
    // the new generation's first (compaction-completion) checkpoint.
    const uint64_t replacedAt=current->second->firstCheckpoint!=UINT64_MAX?current->second->firstCheckpoint:snapshot.through;
    if(replacedAt>=from) for(const auto& generation:f->dagHistory->generations) if(generation.first<snapshot.generation) {
        const auto append=[&](const Raw& block) {
            std::string cid=rawCid(block.digest);
            if(seen.emplace(cid,replacedAt,snapshot.generation).second) candidates.push_back({std::move(cid),replacedAt,snapshot.generation});
        };
        if(generation.second->hasCheckpoint) append(generation.second->checkpoint);
        for(const auto& block:generation.second->segments) append(block);
        for(const auto& unit:generation.second->units) if(unit.linked) append(unit.metadata);
    }
    if(candidates.empty()) return P4_OK;
    std::unordered_set<std::string> cids,live; for(const auto& d:candidates) cids.insert(d.cid);
    liveOf(*current->second,cids,&live);
    for(auto& d:candidates) if(!live.count(d.cid)) out->push_back(std::move(d));
    return P4_OK;
}
void feedDagReset(Feed* f) { std::lock_guard<std::mutex> lock(f->dagMu); f->dagHistory.reset(); }
int32_t feedExposure(Feed* f,bool on) { f->ipfsExposed.store(on); f->type->e->kickMaintenance(); return P4_OK; }
int32_t feedRelease(Feed* f,uint32_t generation,uint64_t through) {
    uint32_t current;
    { std::lock_guard<std::mutex> lock(f->smu); current=f->gen; }
    if(generation>current) return P4_E_ARG;
    std::vector<uint32_t> retire;
    if(generation==current) {
        // A current-generation acknowledgement covers all earlier ranges
        // only after that generation's compaction checkpoint was published.
        // Offset comparisons never cross generation boundaries.
        uint64_t previous=0,end=0; std::shared_ptr<Stream> stream;
        int32_t rc=committedStream(f,&stream,&previous,&end,nullptr); if(rc!=P4_OK) return rc;
        if(stream->gen!=generation || through>previous) return P4_E_ARG;
        if(generation) {
            uint64_t last,boundary,firstCheckpoint;
            rc=retiredBounds(stream.get(),&last,&boundary,&firstCheckpoint,end); if(rc!=P4_OK) return rc;
            if(through<firstCheckpoint) return P4_OK;
        }
        std::lock_guard<std::mutex> lock(f->smu);
        if(f->retired && f->retired->gen<generation) retire.push_back(f->retired->gen);
        for(const auto& old:f->olderRetired) if(old.stream->gen<generation) retire.push_back(old.stream->gen);
    } else retire.push_back(generation);
    // A retired generation is immutable: naming it releases all of it. Its
    // last commit can be a frame the node never saw (appended after its last
    // FEED_ROOT and before the swap), so `through` never applies to it.
    for(uint32_t old:retire) {
        uint64_t need=UINT64_MAX;
        { std::lock_guard<std::mutex> lock(f->smu); const auto it=f->releaseRequired.find(old); if(it!=f->releaseRequired.end()) need=it->second; }
        if(need==UINT64_MAX) {
            int32_t rc=P4_OK; auto stream=streamAt(f,old,&rc); if(!stream) { if(rc!=P4_OK) return rc; continue; }
            uint64_t end; rc=retiredBounds(stream.get(),&need,&end); if(rc!=P4_OK) return rc;
        }
        std::lock_guard<std::mutex> lock(f->smu);
        f->releaseRequired[old]=need; f->releasedThrough[old]=need;
    }
    f->type->e->kickMaintenance(); return P4_OK;
}
uint64_t feedBlockedGenerations(Feed* f) {
    std::lock_guard<std::mutex> lock(f->smu); uint64_t n=0;
    if(f->retired && !streamGenerationReleased(f,f->retired->gen)) ++n;
    for(const auto& r:f->olderRetired) if(!streamGenerationReleased(f,r.stream->gen)) ++n;
    return n;
}
} }
