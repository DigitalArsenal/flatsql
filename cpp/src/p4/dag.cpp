#include "dag.h"
#include "ipld.h"
#include "sds_frames.h"
#include "checkpoint.h"
#include <algorithm>
#include <functional>

namespace flatsql { namespace p4 {
namespace {
struct LiveRow { size_t unit=0; uint64_t frame=0; };
struct RecordBlock { DagBlock block; std::vector<DagBlock> leaves; size_t owner=0; uint64_t refs=0; };
struct Unit { DagBlock metadata,node; std::vector<uint64_t> frames; std::set<size_t> targets; uint64_t live=0,removedAt=0,droppedAt=0,changedAt=0; bool linked=false; };
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
DagBlock linkBlock(const std::vector<DagBlock>& children,uint64_t unit,const std::string* data=nullptr,const std::vector<std::string>* names=nullptr) {
    DagBlock b; b.kind=P4_RANGE_LINK; b.unit=unit;
    std::vector<IpldLink> links;
    for(size_t i=0;i<children.size();i++) { links.push_back({children[i].cid,names?(*names)[i]:"",names?0:children[i].length}); b.links.push_back(children[i].cid); }
    b.bytes=ipldDagPb(links,data); b.length=b.bytes.size(); b.cid=ipldCid(0x70,b.bytes.data(),b.bytes.size()); return b;
}
}
struct DagState {
    uint32_t gen=0; uint64_t end=0,last=UINT64_MAX,firstCheckpoint=UINT64_MAX;
    std::string path;
    std::vector<Unit> units;
    std::map<uint64_t,RecordBlock> records;
    std::map<uint64_t,LiveRow> live;
    std::vector<DagDrop> drops;
    DagBlock checkpoint;
    std::vector<DagBlock> segments;
    // Fixed 1,024-wide positions let an append or removal replace only its
    // unit and the nodes on its right/affected spine. Empty positions vanish.
    std::vector<std::vector<DagBlock>> tree;
    std::set<size_t> dirty;
};
struct DagHistory {
    // Caches remain generation-scoped. A compaction must not erase drops
    // that the node has not yet read; after restart retained streams rebuild
    // these same entries without an additional authoritative file.
    std::map<uint32_t,std::shared_ptr<DagState>> generations;
};
namespace {
DagBlock raw(DagState& s,uint64_t at,uint64_t n,uint64_t unit,uint32_t kind,const uint8_t* digest) {
    DagBlock b; b.cid=ipldCidDigest(0x55,digest); b.path=s.path; b.offset=at+4; b.length=n; b.unit=unit; b.kind=kind; return b;
}
void eraseRow(DagState& s,std::map<uint64_t,LiveRow>::iterator row,uint64_t removal,Unit* remover) {
    const LiveRow old=row->second; if(remover) remover->targets.insert(old.unit);
    Unit& owner=s.units[old.unit]; if(owner.live) --owner.live; owner.removedAt=std::max(owner.removedAt,removal); owner.changedAt=std::max(owner.changedAt,removal); s.dirty.insert(old.unit);
    auto frame=s.records.find(old.frame);
    if(frame!=s.records.end() && frame->second.refs && !--frame->second.refs) {
        s.dirty.insert(frame->second.owner);
        s.units[frame->second.owner].removedAt=std::max(s.units[frame->second.owner].removedAt,removal);
        s.units[frame->second.owner].changedAt=std::max(s.units[frame->second.owner].changedAt,removal);
        s.drops.push_back({frame->second.block.cid,removal,s.gen});
        for(const auto& leaf:frame->second.leaves) s.drops.push_back({leaf.cid,removal,s.gen});
    }
    s.live.erase(row);
}
int32_t apply(DagState& s,const Meta& m) {
    std::vector<uint64_t> aligned((m.bytes.size()+7)/8); std::memcpy(aligned.data(),m.bytes.data(),m.bytes.size());
    const auto* p=reinterpret_cast<const uint8_t*>(aligned.data()); uint8_t digest[32]; ps::sha256(p,m.bytes.size(),digest);
    DagBlock block=raw(s,m.offset,m.bytes.size(),m.offset,m.kind,digest);
    if(m.kind==P4_RANGE_FCK) {
        if(m.offset && s.firstCheckpoint==UINT64_MAX) s.firstCheckpoint=m.offset;
        if(!s.checkpoint.cid.empty() && s.checkpoint.cid!=block.cid) s.drops.push_back({s.checkpoint.cid,m.offset,s.gen});
        const auto oldSegments=std::move(s.segments);
        s.checkpoint=block; s.segments.clear(); const auto* ck=GetFCK(p);
        if(ck->SEGMENTS()) {
            if(!ck->SEGMENT_DIGESTS() || ck->SEGMENT_DIGESTS()->size()!=ck->SEGMENTS()->size()*32) return P4_E_CORRUPT;
            for(size_t i=0;i<ck->SEGMENTS()->size();i++) { const auto* x=ck->SEGMENTS()->Get(flatbuffers::uoffset_t(i)); s.segments.push_back(raw(s,x->OFFSET(),x->LENGTH(),x->OFFSET(),P4_RANGE_FSG,ck->SEGMENT_DIGESTS()->data()+i*32)); }
        }
        std::unordered_set<std::string> liveSegments; for(const auto& b:s.segments) liveSegments.insert(b.cid);
        for(const auto& b:oldSegments) if(!liveSegments.count(b.cid)) s.drops.push_back({b.cid,m.offset,s.gen});
    } else if(m.kind==P4_RANGE_FMB) {
        const auto* mb=GetFMB(p); const size_t index=s.units.size(); s.units.emplace_back(); Unit& u=s.units.back(); u.metadata=block; u.changedAt=m.offset;
        size_t dig=0;
        if(mb->ROWS()) for(size_t i=0;i<mb->ROWS()->size();i++) {
            const auto* r=mb->ROWS()->Get(flatbuffers::uoffset_t(i)); const uint64_t rid=(r->SEQ()<<16)|r->ROW_J();
            if(r->FLAGS()&FmbNewFrame) {
                if(!mb->DIGESTS() || dig+32>mb->DIGESTS()->size()) return P4_E_CORRUPT;
                const uint8_t* rd=mb->DIGESTS()->data()+dig; dig+=32;
                if(r->FLAGS()&FmbSealed) { bool found=false; if(mb->SEALED()) for(const auto* sealed:*mb->SEALED()) if(sealed->ROW()==i && sealed->LEAF_DIGEST() && sealed->LEAF_DIGEST()->size()==32) { rd=sealed->LEAF_DIGEST()->data(); found=true; break; } if(!found) return P4_E_CORRUPT; }
                RecordBlock rec; rec.owner=index; rec.block=raw(s,r->OFFSET(),r->LENGTH(),m.offset,P4_RANGE_RECORD,rd);
                if(r->FLAGS()&FmbLarge) {
                    const ::FMBLarge* large=nullptr; if(mb->LARGE()) for(const auto* x:*mb->LARGE()) if(x->ROW()==i) { large=x; break; }
                    const size_t leaves=(uint64_t(r->LENGTH())+1048575)/1048576;
                    if(!large || !large->ROOT_CID() || large->ROOT_CID()->size()!=36 || !large->ROOT_NODE() || !large->LEAF_DIGESTS() || large->LEAF_DIGESTS()->size()!=leaves*32) return P4_E_CORRUPT;
                    rec.block.cid.assign(reinterpret_cast<const char*>(large->ROOT_CID()->data()),36);
                    rec.block.bytes.assign(reinterpret_cast<const char*>(large->ROOT_NODE()->data()),large->ROOT_NODE()->size()); rec.block.path.clear(); rec.block.offset=0; rec.block.length=rec.block.bytes.size(); rec.block.kind=P4_RANGE_LARGE_ROOT;
                    for(size_t j=0;j<leaves;j++) { auto leaf=raw(s,r->OFFSET()+j*1048576,std::min<uint64_t>(1048576,r->LENGTH()-j*1048576),m.offset,P4_RANGE_LARGE_LEAF,large->LEAF_DIGESTS()->data()+j*32); rec.block.links.push_back(leaf.cid); rec.leaves.push_back(std::move(leaf)); }
                }
                s.records[r->OFFSET()]=std::move(rec); u.frames.push_back(r->OFFSET());
            }
            auto old=s.live.find(rid); if(old!=s.live.end()) eraseRow(s,old,m.offset,nullptr);
            auto frame=s.records.find(r->OFFSET()); if(frame==s.records.end()) return P4_E_CORRUPT;
            if(!frame->second.refs) s.units[frame->second.owner].changedAt=m.offset;
            ++frame->second.refs; ++u.live; s.live[rid]={index,r->OFFSET()}; s.dirty.insert(frame->second.owner);
        }
        if(mb->URLS()) for(const auto* url:*mb->URLS()) { auto row=s.live.find((url->SEQ()<<16)|url->ROW_J()); if(row!=s.live.end()) u.targets.insert(row->second.unit); }
        // Match the recovery writer's authoritative apply order.
        if(mb->REMOVALS()) for(const auto* r:*mb->REMOVALS()) {
            auto it=s.live.lower_bound(r->SEQ()<<16);
            if(r->ROW_J()!=UINT32_MAX) { it=s.live.find((r->SEQ()<<16)|r->ROW_J()); if(it!=s.live.end()) eraseRow(s,it,m.offset,&u); }
            else while(it!=s.live.end() && (it->first>>16)==r->SEQ()) { auto old=it++; eraseRow(s,old,m.offset,&u); }
        }
        std::sort(u.frames.begin(),u.frames.end());
        s.dirty.insert(index);
    }
    s.end=m.offset+4+m.bytes.size(); s.last=m.offset; return P4_OK;
}
void updateTree(DagState& s,uint64_t through) {
    // Removal units and URL updates are live while a unit they affect is live.
    std::vector<bool> linked(s.units.size(),false);
    for(size_t i=0;i<s.units.size();i++) { linked[i]=s.units[i].live>0; for(uint64_t frame:s.units[i].frames) linked[i]=linked[i] || s.records.at(frame).refs>0; }
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
        if(at>u.metadata.unit && u.droppedAt!=at) { s.drops.push_back({u.metadata.cid,at,s.gen}); u.droppedAt=at; }
    }
    for(size_t i=0;i<s.units.size();i++) if(s.units[i].linked!=linked[i]) { s.units[i].linked=linked[i]; s.dirty.insert(i); }
    for(size_t i:s.dirty) {
        auto& u=s.units[i]; if(!u.linked) { u.node={}; continue; }
        std::vector<DagBlock> children; for(uint64_t frame:u.frames) if(s.records.at(frame).refs) children.push_back(s.records.at(frame).block);
        // A rewritten unit node is a new block published by the change's
        // commit. Keep the FMB leaf's original unit, but let incremental range
        // pulls discover this new link block after a removal in a later unit.
        children.push_back(u.metadata); u.node=linkBlock(children,u.changedAt);
    }
    if(s.tree.empty()) s.tree.emplace_back();
    std::set<size_t> dirty=s.dirty; s.dirty.clear(); size_t count=s.units.size(),level=0;
    do {
        const size_t groups=std::max<size_t>(1,(count+1023)/1024); if(s.tree.size()<=level) s.tree.emplace_back();
        auto& nodes=s.tree[level]; const size_t oldSize=nodes.size(); nodes.resize(groups);
        std::set<size_t> parents; for(size_t i:dirty) parents.insert(i/1024); for(size_t i=oldSize;i<groups;i++) parents.insert(i);
        for(size_t parent:parents) { std::vector<DagBlock> children;
            for(size_t j=parent*1024;j<std::min(count,(parent+1)*1024);j++) { const auto& child=level?s.tree[level-1][j]:s.units[j].node; if(!child.cid.empty()) children.push_back(child); }
            nodes[parent]=linkBlock(children,through);
        }
        dirty=std::move(parents); count=groups; ++level;
    } while(count>1);
    s.tree.resize(level);
}
int32_t build(Feed* feed,Stream* stream,uint64_t previous,uint64_t end,DagSnapshot* out,std::string* err,bool closure=true) {
    if(!feed->dagHistory) feed->dagHistory=std::make_shared<DagHistory>();
    auto& ptr=feed->dagHistory->generations[stream->gen];
    if(!ptr || ptr->end>end) {
        ptr=std::make_shared<DagState>(); ptr->gen=stream->gen;
        const auto& root=feed->type->e->cfg.root; ptr->path=stream->path;
        if(ptr->path.compare(0,root.size(),root)==0 && ptr->path.size()>root.size() && ptr->path[root.size()]=='/') ptr->path.erase(0,root.size()+1);
    }
    feed->dag=ptr;
    DagState& s=*ptr; std::vector<Meta> tail; uint64_t at=previous;
    while(at!=UINT64_MAX && (s.last==UINT64_MAX || at>s.last)) {
        Meta m; const int32_t rc=readMeta(stream,at,end,&m); if(rc!=P4_OK) { if(err) *err="invalid acknowledged DAG chain: "+stream->path; return rc; }
        at=m.previous; tail.push_back(std::move(m));
    }
    if(s.last!=UINT64_MAX && at!=s.last) return P4_E_CORRUPT;
    for(auto it=tail.rbegin();it!=tail.rend();++it) { const int32_t rc=apply(s,*it); if(rc!=P4_OK) return rc; }
    updateTree(s,previous);
    DagSnapshot result; result.through=previous; result.generation=stream->gen;
    const DagBlock commits=s.tree.back().front(),segments=linkBlock(s.segments,previous);
    const std::string dir=ipldUnixfsDirectory(); const std::vector<std::string> names={"checkpoint","commits","segments"};
    const DagBlock root=linkBlock({s.checkpoint,commits,segments},previous,&dir,&names); result.root=root.cid; result.commitsRoot=commits.cid;
    if(!closure) { *out=std::move(result); return P4_OK; }
    std::unordered_map<std::string,const DagBlock*> blocks;
    blocks.emplace(root.cid,&root); blocks.emplace(commits.cid,&commits); blocks.emplace(segments.cid,&segments); blocks.emplace(s.checkpoint.cid,&s.checkpoint);
    for(const auto& b:s.segments) blocks.emplace(b.cid,&b);
    for(const auto& level:s.tree) for(const auto& b:level) if(!b.cid.empty()) blocks.emplace(b.cid,&b);
    for(const auto& u:s.units) if(u.linked) { blocks.emplace(u.node.cid,&u.node); blocks.emplace(u.metadata.cid,&u.metadata); }
    for(const auto& item:s.records) if(item.second.refs) { const auto& r=item.second; blocks.emplace(r.block.cid,&r.block); for(const auto& b:r.leaves) blocks.emplace(b.cid,&b); }
    std::unordered_set<std::string> seen;
    std::function<bool(const std::string&)> visit=[&](const std::string& cid) { if(!seen.insert(cid).second) return true; auto it=blocks.find(cid); if(it==blocks.end()) return false; result.blocks.push_back(*it->second); for(const auto& child:it->second->links) if(!visit(child)) return false; return true; };
    if(!visit(root.cid)) return P4_E_CORRUPT;
    // The read ABI must never register an uncommitted byte range, even if
    // a cached metadata leaf was damaged after the feed was opened.
    for(const auto& block:result.blocks) if(!block.path.empty() &&
        (block.offset>end || block.length>end-block.offset)) return P4_E_CORRUPT;
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
int32_t feedDagSnapshot(Feed* f,DagSnapshot* out,std::string* err) {
    uint64_t previous=0,end=0; std::shared_ptr<Stream> stream;
    int32_t rc;
    {
        // The snapshot's committed point is read under dagMu, as every
        // builder's is: the cache only moves forward, never resets for a
        // point another builder already passed (smu holders never take dagMu).
        std::lock_guard<std::mutex> lock(f->dagMu);
        rc=committedStream(f,&stream,&previous,&end,err); if(rc!=P4_OK) return rc;
        rc=build(f,stream.get(),previous,end,out,err); if(rc!=P4_OK) return rc;
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
int32_t feedDagDrops(Feed* f,uint64_t from,std::vector<DagDrop>* out,std::string* err) {
    DagSnapshot snapshot; int32_t rc=feedDagSnapshot(f,&snapshot,err); if(rc!=P4_OK) return rc;
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
    std::unordered_set<std::string> live; for(const auto& b:snapshot.blocks) live.insert(b.cid);
    for(const auto& generation:f->dagHistory->generations) if(generation.first<=snapshot.generation)
        for(const auto& d:generation.second->drops) if(d.unit>=from &&
            (d.generation<snapshot.generation || d.unit<=snapshot.through) &&
            !live.count(d.cid) && seen.emplace(d.cid,d.unit,d.generation).second) out->push_back(d);
    // A compaction replaces the old generation's still-live metadata too.
    // Those leaves are not removals within the old stream, so retire them at
    // the new generation's first (compaction-completion) checkpoint.
    const auto current=f->dagHistory->generations.find(snapshot.generation);
    const uint64_t replacedAt=current!=f->dagHistory->generations.end() && current->second->firstCheckpoint!=UINT64_MAX?
        current->second->firstCheckpoint:snapshot.through;
    if(replacedAt>=from) for(const auto& generation:f->dagHistory->generations) if(generation.first<snapshot.generation) {
        const auto append=[&](const DagBlock& block) {
            if(!block.cid.empty() && !live.count(block.cid) && seen.emplace(block.cid,replacedAt,snapshot.generation).second)
                out->push_back({block.cid,replacedAt,snapshot.generation});
        };
        append(generation.second->checkpoint);
        for(const auto& block:generation.second->segments) append(block);
        for(const auto& unit:generation.second->units) if(unit.linked) append(unit.metadata);
    }
    return P4_OK;
}
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
