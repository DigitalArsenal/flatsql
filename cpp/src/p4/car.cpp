// Standard CARv2 framing is separate from feed persistence. A restore produces
// an ordinary stream, then uses exactly the recovery/rebuild apply path.
#include "car.h"
#include "dag.h"
#include "ipld.h"
#include "checkpoint.h"
#include "sds_frames.h"
#include "flatsql/flatsql_io.h"
#include <algorithm>
#include <unordered_map>
#include <set>
#include <functional>

namespace flatsql { namespace p4 {
namespace {
const uint8_t pragmaBytes[]={0x0a,0xa1,0x67,0x76,0x65,0x72,0x73,0x69,0x6f,0x6e,0x02};
void varint(std::string& out,uint64_t n) { do { uint8_t b=uint8_t(n&127); n>>=7; out.push_back(char(b|(n?128:0))); } while(n); }
void le32(std::string& out,uint32_t n) { for(unsigned i=0;i<4;i++) out.push_back(char(n>>(8*i))); }
void le64(std::string& out,uint64_t n) { for(unsigned i=0;i<8;i++) out.push_back(char(n>>(8*i))); }
uint64_t get64(const char* p) { uint64_t n=0; for(unsigned i=0;i<8;i++) n|=uint64_t(uint8_t(p[i]))<<(8*i); return n; }
std::string cborHeader(const std::string& root) {
    std::string s("\xa2\x65roots\x81\xd8\x2a\x58\x25\0",13);
    s+=root; s.append("\x67version\x01",9); return s;
}
struct File {
    int32_t h=-1; uint64_t size=0; bool owned=true;
    mutable uint64_t readStart=0;
    mutable std::string readCache;
    uint64_t writeStart=0;
    std::string writeCache;
    static constexpr size_t bufferSize=1u<<20;
    ~File() { if(owned && h>=0) flatsql_io_close(h); }
    bool readDirect(uint64_t at,size_t n,std::string* out) const {
        out->resize(n); size_t done=0;
        while(done<n) { int32_t got=flatsql_io_read(h,&(*out)[done],int32_t(std::min<size_t>(n-done,1u<<26)),double(at+done)); if(got<=0) return false; done+=size_t(got); }
        return true;
    }
    bool read(uint64_t at,size_t n,std::string* out) const {
        if(at>size || n>size-at) return false;
        if(n>bufferSize) return readDirect(at,n,out);
        if(at<readStart || at-readStart>readCache.size() || n>readCache.size()-size_t(at-readStart)) {
            readStart=at;
            if(!readDirect(at,size_t(std::min<uint64_t>(bufferSize,size-at)),&readCache)) return false;
        }
        out->assign(readCache.data()+size_t(at-readStart),n); return true;
    }
    bool writeDirect(uint64_t at,const std::string& data) {
        size_t done=0; while(done<data.size()) { int32_t n=flatsql_io_write(h,data.data()+done,int32_t(std::min<size_t>(data.size()-done,1u<<26)),double(at+done)); if(n<=0) return false; done+=size_t(n); } return true;
    }
    bool flush() {
        if(writeCache.empty()) return true;
        if(!writeDirect(writeStart,writeCache)) return false;
        writeCache.clear(); return true;
    }
    bool write(uint64_t at,const std::string& data) {
        if(!writeCache.empty() && (at!=writeStart+writeCache.size() || writeCache.size()+data.size()>bufferSize)) if(!flush()) return false;
        if(data.size()>bufferSize) { if(!writeDirect(at,data)) return false; }
        else { if(writeCache.empty()) writeStart=at; writeCache+=data; }
        size=std::max(size,at+data.size()); return true;
    }
    bool number(uint64_t* at,uint64_t limit,uint64_t* value) const {
        *value=0; for(unsigned i=0;i<10;i++) { std::string b; if(*at>=limit || !read((*at)++,1,&b)) return false; uint8_t v=uint8_t(b[0]); if(i==9 && v>1) return false; *value|=uint64_t(v&127)<<(i*7); if(!(v&128)) return !(i && !v); } return false;
    }
};

struct Location { uint64_t section=0,offset=0,length=0; };
struct Archive {
    File file; std::string root; std::vector<std::string> order;
    std::unordered_map<std::string,Location> blocks;
    bool block(const std::string& cid,std::string* data) const { auto i=blocks.find(cid); return i!=blocks.end() && file.read(i->second.offset,size_t(i->second.length),data); }
};
std::string indexBytes(const std::vector<std::pair<std::string,uint64_t>>& source) {
    auto entries=source;
    std::sort(entries.begin(),entries.end(),[](const auto& a,const auto& b) { int c=std::memcmp(a.first.data()+4,b.first.data()+4,32); return c<0 || (!c && a.second<b.second); });
    std::string out; varint(out,0x0401); le32(out,1); le64(out,0x12); le32(out,1); le32(out,40); le64(out,entries.size()*40);
    for(const auto& entry:entries) { out.append(entry.first.data()+4,32); le64(out,entry.second); }
    return out;
}
bool parse(Archive* ar,const std::string& path,std::string* err) {
    auto fail=[&](const char* message) { if(err) *err=std::string("CARv2: ")+message; return false; };
    ar->file.h=flatsql_io_open(path.data(),int32_t(path.size()),FLATSQL_IO_READ); if(ar->file.h<0) return fail("open failed");
    double sz=flatsql_io_size(ar->file.h); if(sz<51) return fail("short file"); ar->file.size=uint64_t(sz);
    std::string header; if(!ar->file.read(0,51,&header) || std::memcmp(header.data(),pragmaBytes,11)) return fail("invalid pragma");
    if(header.substr(11,16)!=std::string(16,0) || get64(header.data()+27)!=51) return fail("unsupported header");
    uint64_t dataSize=get64(header.data()+35),indexAt=get64(header.data()+43);
    if(dataSize>ar->file.size-51 || indexAt!=51+dataSize || indexAt>=ar->file.size) return fail("invalid offsets");
    uint64_t at=51,n=0; if(!ar->file.number(&at,indexAt,&n) || n!=58 || !ar->file.read(at,size_t(n),&header)) return fail("invalid CARv1 header");
    ar->root=header.substr(13,36); if(header!=cborHeader(ar->root)) return fail("invalid dag-cbor header"); at+=n;
    std::vector<std::pair<std::string,uint64_t>> indexed;
    while(at<indexAt) {
        uint64_t section=at;
        if(!ar->file.number(&at,indexAt,&n) || n<36 || n>indexAt-at) return fail("invalid block section");
        std::string cid,data; if(!ar->file.read(at,36,&cid) || uint8_t(cid[0])!=1 || (uint8_t(cid[1])!=0x55 && uint8_t(cid[1])!=0x70) || uint8_t(cid[2])!=0x12 || uint8_t(cid[3])!=32) return fail("unsupported CID");
        if(!ar->file.read(at+36,size_t(n-36),&data) || ipldCid(uint8_t(cid[1]),data.data(),data.size())!=cid) return fail("block hash mismatch");
        if(!ar->blocks.emplace(cid,Location{section-51,at+36,n-36}).second) return fail("duplicate block");
        ar->order.push_back(cid); indexed.emplace_back(cid,section-51); at+=n;
    }
    std::string index; if(!ar->file.read(indexAt,size_t(ar->file.size-indexAt),&index) || index!=indexBytes(indexed)) return fail("invalid MultihashIndexSorted");
    std::set<std::string> seen; std::vector<std::string> dfs;
    std::function<bool(const std::string&)> visit=[&](const std::string& cid) {
        if(!seen.insert(cid).second) return true; if(!ar->blocks.count(cid)) return false; dfs.push_back(cid);
        if(uint8_t(cid[1])==0x70) { std::string bytes; std::vector<IpldLink> links; if(!ar->block(cid,&bytes) || !ipldLinks(bytes,&links)) return false; for(const auto& link:links) if(!visit(link.cid)) return false; } return true;
    };
    if(!visit(ar->root) || dfs!=ar->order) return fail("blocks are not the root's DFS closure"); return true;
}
struct Aligned {
    std::vector<uint64_t> words; size_t size=0;
    explicit Aligned(const std::string& bytes):words((bytes.size()+7)/8),size(bytes.size()) { std::memcpy(words.data(),bytes.data(),bytes.size()); }
    const uint8_t* data() const { return reinterpret_cast<const uint8_t*>(words.data()); }
};
FmbEntry decodeEntry(const ::FMBEntry* d) { FmbEntry e; e.kind=uint8_t(d->KIND()); e.id=d->ID(); if(d->TEXT()) for(const auto* s:*d->TEXT()) e.text.push_back(s->str()); return e; }
struct Batch { uint64_t oldOffset=0; FmbCommit commit; };
struct Record { std::array<uint8_t,32> digest{},bytesDigest{}; std::string cid; uint32_t length=0; };
bool decodeBatch(const std::string& bytes,Batch* batch,std::map<uint64_t,Record>* records) {
    Aligned a(bytes); flatbuffers::Verifier v(a.data(),a.size); if(!VerifyFMBBuffer(v)) return false; const auto* m=GetFMB(a.data());
    batch->oldOffset=m->SELF_OFFSET(); auto& out=batch->commit; out.committedAt=m->COMMITTED_AT(); out.typeSeqHigh=m->TYPE_SEQ_HIGH();
    if(m->GROUPS()) for(const auto* g:*m->GROUPS()) out.groups.push_back({g->DELIVERED_AT(),g->NODE_ID(),g->BATCH_ID(),g->CONTENT_KEY_ID(),g->URL_ID()});
    if(m->DICTIONARY()) for(const auto* d:*m->DICTIONARY()) out.dictionary.push_back(decodeEntry(d));
    if(m->REMOVALS()) for(const auto* r:*m->REMOVALS()) out.removals.push_back({r->SEQ(),r->ROW_J(),uint8_t(r->REASON())});
    if(m->URLS()) for(const auto* u:*m->URLS()) out.urls.push_back({u->SEQ(),u->ROW_J(),u->URL_ID()});
    size_t di=0; const size_t rows=m->ROWS()?m->ROWS()->size():0;
    std::vector<const ::FMBSignature*> signatures; std::vector<const ::FMBIdentity*> identities; std::vector<const ::FMBSealed*> sealedRows; std::vector<const ::FMBLarge*> largeRows;
    if(!fmbByRow(m->SIGNATURES(),rows,&signatures) || !fmbByRow(m->IDENTITIES(),rows,&identities) || !fmbByRow(m->SEALED(),rows,&sealedRows) || !fmbByRow(m->LARGE(),rows,&largeRows)) return false;
    for(size_t j=0;j<rows;j++) {
        const auto* r=m->ROWS()->Get(flatbuffers::uoffset_t(j)); FmbRow row;
        row.offset=r->OFFSET(); row.seq=r->SEQ(); row.sourceTs=r->SOURCE_TS(); row.length=r->LENGTH(); row.flags=r->FLAGS(); row.group=r->GROUP(); row.rowJ=r->ROW_J();
        if(row.group>=out.groups.size()) return false;
        if(row.flags&FmbNewFrame) { if(!m->DIGESTS() || di+32>m->DIGESTS()->size()) return false; std::memcpy(row.digest.data(),m->DIGESTS()->data()+di,32); di+=32; }
        if(const auto* x=fmbRow(signatures,j)) if(x->BYTES()) row.signature.assign(reinterpret_cast<const char*>(x->BYTES()->data()),x->BYTES()->size());
        if(const auto* x=fmbRow(identities,j)) if(x->HASH()) row.identity.assign(reinterpret_cast<const char*>(x->HASH()->data()),x->HASH()->size());
        bool sealed=false;
        if(const auto* x=fmbRow(sealedRows,j)) { if(!x->LEAF_DIGEST() || x->LEAF_DIGEST()->size()!=32) return false; sealed=true; row.epoch=x->EPOCH(); row.objectKey=x->OBJECT_KEY_ID(); std::memcpy(row.leafDigest.data(),x->LEAF_DIGEST()->data(),32); if(x->COLUMNS()) row.columns.assign(reinterpret_cast<const char*>(x->COLUMNS()->data()),x->COLUMNS()->size()); }
        if(bool(row.flags&FmbSealed)!=sealed) return false;
        if(row.flags&FmbNewFrame) {
            Record record; record.digest=row.digest; record.bytesDigest=(row.flags&FmbSealed)?row.leafDigest:row.digest; record.length=row.length; record.cid=ipldCidDigest(0x55,(row.flags&FmbSealed)?row.leafDigest.data():row.digest.data());
            if(row.flags&FmbLarge) { const auto* x=fmbRow(largeRows,j); if(!x || !x->ROOT_CID()) return false; record.cid.assign(reinterpret_cast<const char*>(x->ROOT_CID()->data()),x->ROOT_CID()->size()); }
            if(!records->emplace(row.offset,std::move(record)).second) return false;
        }
        out.rows.push_back(std::move(row));
    }
    return di==(m->DIGESTS()?m->DIGESTS()->size():0);
}
bool recordBytes(const Archive& archive,const Record& record,std::string* out) {
    if(record.cid.size()!=36) return false;
    if(uint8_t(record.cid[1])==0x55) return archive.block(record.cid,out) && out->size()==record.length;
    std::string node; std::vector<IpldLink> links;
    if(!archive.block(record.cid,&node) || !ipldLinks(node,&links)) return false;
    out->clear(); for(const auto& link:links) { std::string leaf; if(link.cid.size()!=36 || uint8_t(link.cid[1])!=0x55 || !archive.block(link.cid,&leaf)) return false; out->append(leaf); }
    if(out->size()!=record.length) return false;
    uint8_t digest[32]; ps::sha256(out->data(),out->size(),digest);
    return std::memcmp(digest,record.bytesDigest.data(),32)==0;
}
int32_t appendFrame(Stream* stream,const std::string& bytes,int64_t* end) {
    if(bytes.size()>UINT32_MAX) return FLATSQL_E_CORRUPT;
    std::string data; le32(data,uint32_t(bytes.size())); data+=bytes;
    int32_t rc=streamWrite(stream,*end,reinterpret_cast<const uint8_t*>(data.data()),data.size()); if(rc==FLATSQL_OK) *end+=int64_t(data.size()); return rc;
}
flatbuffers::Offset<::FMBEntry> encodeEntry(flatbuffers::FlatBufferBuilder& b,const ::FMBEntry* e) {
    std::vector<flatbuffers::Offset<flatbuffers::String>> text; if(e->TEXT()) for(const auto* s:*e->TEXT()) text.push_back(b.CreateString(s->str()));
    return CreateFMBEntry(b,e->KIND(),e->ID(),b.CreateVector(text));
}
int32_t appendSegments(Feed* f,Stream* stream,const std::vector<std::string>& segmentBytes,const FCK* ck,
                       const std::map<uint64_t,uint64_t>& refs,const std::string& previousRoot,uint64_t applied,int64_t* end,uint64_t* previous) {
    std::map<uint64_t,std::string> source;
    for(const auto& bytes:segmentBytes) { Aligned a(bytes); flatbuffers::Verifier v(a.data(),a.size); if(!VerifyFSGBuffer(v)) return FLATSQL_E_CORRUPT; source.emplace(GetFSG(a.data())->SELF_OFFSET(),bytes); }
    std::vector<::FCKSegment> desc; std::vector<uint8_t> digests;
    if(ck->SEGMENTS()) for(const auto* old:*ck->SEGMENTS()) {
        auto i=source.find(old->OFFSET()); if(i==source.end()) return FLATSQL_E_CORRUPT;
        Aligned a(i->second); const auto* s=GetFSG(a.data()); flatbuffers::FlatBufferBuilder b;
        std::vector<flatbuffers::Offset<::FMBEntry>> entries,tokens;
        if(s->ENTRIES()) for(const auto* d:*s->ENTRIES()) entries.push_back(encodeEntry(b,d));
        if(s->TOKENS()) for(const auto* d:*s->TOKENS()) tokens.push_back(encodeEntry(b,d));
        auto remap=[&](uint64_t ref) { auto found=refs.find(ref); return found==refs.end()?uint64_t(0):found->second; };
        std::vector<::FSGLatest> latest; if(s->LATEST()) for(const auto* x:*s->LATEST()) latest.emplace_back(x->OBJECT_KEY_ID(),0,x->EPOCH(),x->RID(),remap(x->ROW_REF()));
        std::vector<::FSGInstance> inst; if(s->INSTANCES()) for(const auto* x:*s->INSTANCES()) inst.push_back(*x);
        std::vector<::FSGPair> pairs; if(s->PAIRS()) for(const auto* x:*s->PAIRS()) pairs.emplace_back(x->KEY_HI(),x->KEY_LO(),x->RID(),remap(x->ROW_REF()));
        auto ev=b.CreateVector(entries),tv=b.CreateVector(tokens); auto lv=b.CreateVectorOfStructs(latest); auto iv=b.CreateVectorOfStructs(inst); auto pv=b.CreateVectorOfStructs(pairs);
        flatbuffers::Offset<flatbuffers::Vector<uint32_t>> urls; if(s->INSTANCE_URL_IDS()) urls=b.CreateVector(s->INSTANCE_URL_IDS()->data(),s->INSTANCE_URL_IDS()->size());
        flatbuffers::Offset<flatbuffers::Vector<uint8_t>> column; if(s->COLUMN()) column=b.CreateVector(s->COLUMN()->data(),s->COLUMN()->size());
        uint64_t at=uint64_t(*end); FSGBuilder builder(b); builder.add_SELF_OFFSET(at); builder.add_PREVIOUS(*previous); builder.add_FEED_ID(f->fid); builder.add_KIND(s->KIND()); builder.add_LEVEL(s->LEVEL()); builder.add_MIN_SEQ(s->MIN_SEQ()); builder.add_MAX_SEQ(s->MAX_SEQ()); builder.add_COVERS_THROUGH(applied); builder.add_ENTRIES(ev); builder.add_TOKENS(tv); builder.add_LATEST(lv); builder.add_INSTANCES(iv); builder.add_PAIRS(pv); if(urls.o) builder.add_INSTANCE_URL_IDS(urls); if(column.o) builder.add_COLUMN(column); auto root=builder.Finish(); FinishFSGBuffer(b,root);
        uint8_t digest[32]; ps::sha256(b.GetBufferPointer(),b.GetSize(),digest); digests.insert(digests.end(),digest,digest+32);
        desc.emplace_back(at,uint32_t(b.GetSize()),old->KIND(),old->LEVEL(),0,old->MIN_SEQ(),old->MAX_SEQ(),applied,old->ENTRIES());
        int32_t rc=appendFrame(stream,std::string(reinterpret_cast<const char*>(b.GetBufferPointer()),b.GetSize()),end); if(rc!=FLATSQL_OK) return rc; *previous=at;
    }
    std::string commitsRoot; int32_t dagRc=feedDagCommitRoot(f,stream,*previous,uint64_t(*end),&commitsRoot,nullptr); if(dagRc!=FLATSQL_OK) return dagRc;
    flatbuffers::FlatBufferBuilder b; auto type=b.CreateString(f->type->name),provider=b.CreateString(f->provider),sourceName=b.CreateString(f->source);
    auto dv=b.CreateVectorOfStructs(desc); auto hv=b.CreateVector(digests); auto cr=b.CreateVector(reinterpret_cast<const uint8_t*>(commitsRoot.data()),commitsRoot.size()); auto pr=b.CreateVector(reinterpret_cast<const uint8_t*>(previousRoot.data()),previousRoot.size()); uint64_t at=uint64_t(*end);
    FCKBuilder builder(b); builder.add_SELF_OFFSET(at); builder.add_PREVIOUS(*previous); builder.add_FEED_ID(f->fid); builder.add_TYPE(type); builder.add_PROVIDER(provider); builder.add_SOURCE(sourceName); builder.add_GENERATION(f->gen); builder.add_COMMITTED_AT(ck->COMMITTED_AT()); builder.add_APPLIED_THROUGH(applied); builder.add_TYPE_SEQ_HIGH(ck->TYPE_SEQ_HIGH()); builder.add_COUNTERS(ck->COUNTERS()); builder.add_SEGMENTS(dv); builder.add_SEGMENT_DIGESTS(hv); builder.add_COMMITS_ROOT_CID(cr); builder.add_PREVIOUS_FEED_ROOT_CID(pr); builder.add_RECORD_BYTES(ck->RECORD_BYTES()); builder.add_MIN_W(ck->MIN_W()); builder.add_MAX_W(ck->MAX_W()); auto root=builder.Finish(); FinishFCKBuffer(b,root);
    int32_t rc=appendFrame(stream,std::string(reinterpret_cast<const char*>(b.GetBufferPointer()),b.GetSize()),end); if(rc==FLATSQL_OK) *previous=at; return rc;
}
} // namespace

static int32_t carExportOnWriter(Feed* feed,const std::string& path,CarStats* stats,std::string* err) {
    const uint64_t start=monoNs(); if(stats) *stats=CarStats{};
    int32_t rc=checkpointFeed(feed,true,err); if(rc!=FLATSQL_OK) return rc;
    DagSnapshot dag; rc=feedDagSnapshot(feed,&dag,err); if(rc!=FLATSQL_OK) return rc;
    if(dag.root.size()!=36 || dag.blocks.empty()) return FLATSQL_E_CORRUPT;
    File out; out.h=flatsql_io_open(path.data(),int32_t(path.size()),FLATSQL_IO_WRITE|FLATSQL_IO_CREATE|FLATSQL_IO_TRUNC|FLATSQL_IO_CREATE_PARENTS); if(out.h<0) return FLATSQL_E_IO;
    std::string prefix(reinterpret_cast<const char*>(pragmaBytes),11); prefix.append(40,0); if(!out.write(0,prefix)) return FLATSQL_E_IO;
    std::string header=cborHeader(dag.root),framed; varint(framed,header.size()); framed+=header; uint64_t at=51; if(!out.write(at,framed)) return FLATSQL_E_IO; at+=framed.size();
    File source; source.h=dag.stream->h; source.owned=false; source.size=uint64_t(streamSize(dag.stream.get()));
    std::vector<std::pair<std::string,uint64_t>> index; uint64_t live=uint64_t(feed->k.fbytes);
    for(const auto& block:dag.blocks) {
        if(block.cid.size()!=36) return FLATSQL_E_CORRUPT; std::string bytes=block.bytes;
        if(bytes.empty() && block.length) { if(!source.read(block.offset,size_t(block.length),&bytes)) return FLATSQL_E_IO; }
        if(block.kind==FLATSQL_RANGE_FMB || block.kind==FLATSQL_RANGE_FCK || block.kind==FLATSQL_RANGE_FSG) live+=block.length+4;
        std::string section; varint(section,block.cid.size()+bytes.size()); section+=block.cid;
        index.emplace_back(block.cid,at-51); if(!out.write(at,section)) return FLATSQL_E_IO; at+=section.size(); if(!out.write(at,bytes)) return FLATSQL_E_IO; at+=bytes.size();
    }
    uint64_t indexAt=at; std::string idx=indexBytes(index); if(!out.write(at,idx)) return FLATSQL_E_IO; at+=idx.size();
    std::string header2(16,0); le64(header2,51); le64(header2,indexAt-51); le64(header2,indexAt);
    if(!out.write(11,header2) || !out.flush() || flatsql_io_sync(out.h)<0) return FLATSQL_E_IO;
    if(stats) { stats->bytes=at; stats->blocks=dag.blocks.size(); stats->liveStreamBytes=live; stats->wallNs=monoNs()-start; }
    return FLATSQL_OK;
}

static int32_t carRestoreOnWriter(Type* type,const std::string& path,Feed** restored,CarStats* stats,std::string* err) {
    const uint64_t start=monoNs(); if(restored) *restored=nullptr; if(stats) *stats=CarStats{};
    Archive archive; if(!parse(&archive,path,err)) return FLATSQL_E_CORRUPT;
    std::string root; std::vector<IpldLink> links;
    if(!archive.block(archive.root,&root) || !ipldLinks(root,&links)) return FLATSQL_E_CORRUPT;
    std::string checkpoint; for(const auto& link:links) if(link.name=="checkpoint" && !archive.block(link.cid,&checkpoint)) return FLATSQL_E_CORRUPT;
    if(checkpoint.empty()) return FLATSQL_E_CORRUPT;
    Aligned ckBytes(checkpoint); flatbuffers::Verifier verifier(ckBytes.data(),ckBytes.size); if(!VerifyFCKBuffer(verifier)) return FLATSQL_E_CORRUPT; const auto* ck=GetFCK(ckBytes.data());
    if(!ck->FEED_ID() || ck->FEED_ID()>1000000 || !ck->TYPE() || ck->TYPE()->str()!=type->name || !ck->PROVIDER() || !ck->SOURCE() || !ck->COUNTERS()) return FLATSQL_E_CORRUPT;
    std::vector<Batch> batches; std::vector<std::string> segments; std::map<uint64_t,Record> records;
    for(const auto& cid:archive.order) {
        const auto& location=archive.blocks.at(cid); if(uint8_t(cid[1])!=0x55 || location.length<8) continue;
        std::string identifier; if(!archive.file.read(location.offset+4,4,&identifier)) return FLATSQL_E_IO;
        if(identifier!="$FMB" && identifier!="$FSG") continue;
        std::string bytes; if(!archive.block(cid,&bytes)) return FLATSQL_E_IO;
        if(identifier=="$FMB") { Batch batch; if(!decodeBatch(bytes,&batch,&records)) return FLATSQL_E_CORRUPT; batches.push_back(std::move(batch)); } else segments.push_back(std::move(bytes));
    }
    // Determine the surviving addition for each RID before replay. A unit's
    // FMB can describe dead rows whose blocks intentionally aren't in the CAR.
    std::map<uint64_t,std::pair<size_t,size_t>> live;
    std::map<uint64_t,size_t> seqRows;
    std::map<std::string,uint64_t> identities;
    std::map<uint64_t,std::set<std::string>> seqIdentities;
    for(size_t i=0;i<batches.size();i++) {
        const auto& m=batches[i].commit;
        for(size_t j=0;j<m.rows.size();j++) {
            const auto& row=m.rows[j]; const uint64_t rid=(row.seq<<16)|row.rowJ;
            if(!live.count(rid)) ++seqRows[row.seq]; live[rid]={i,j};
            if(!row.identity.empty()) {
                auto old=identities.find(row.identity);
                if(old!=identities.end()) seqIdentities[old->second].erase(row.identity);
                identities[row.identity]=row.seq; seqIdentities[row.seq].insert(row.identity);
            }
        }
        for(const auto& r:m.removals) {
            uint64_t lo=(r.seq<<16)|(r.rowJ==UINT32_MAX?0:r.rowJ),hi=r.rowJ==UINT32_MAX?(lo|65535):lo;
            for(auto at=live.lower_bound(lo);at!=live.end() && at->first<=hi;) { --seqRows[at->first>>16]; at=live.erase(at); }
            // Match recovery: identities belong to a sequence until its last
            // row disappears, even if their original FMB row disappeared first.
            if(!seqRows[r.seq]) { for(const auto& hash:seqIdentities[r.seq]) identities.erase(hash); seqIdentities.erase(r.seq); }
        }
    }
    for(const auto& entry:seqIdentities) if(entry.second.size()>seqRows[entry.first]) {
        if(err) *err="CAR identity count exceeds surviving row capacity";
        return FLATSQL_E_UNSUPPORTED;
    }
    int32_t rc=typeFilesEnsure(type,err); if(rc!=FLATSQL_OK) return rc; Feed* feed;
    { std::lock_guard<std::mutex> lock(type->mu); feed=feedRestoreFresh(type,ck->FEED_ID(),ck->PROVIDER()->str(),ck->SOURCE()->str(),err); if(!feed) return FLATSQL_E_ARG; }
    Conn* conn=writerPin(type->e,feed,&rc,err); if(!conn) return rc;
    auto fail=[&](int32_t status) { writerUnpin(type->e,feed); if(err && err->empty()) *err="CAR restore replay failed"; return status; };
    auto stream=streamCur(feed,&rc); if(!stream) return fail(rc); int64_t end=feed->end; uint64_t previous=0;
    DagSnapshot genesis; rc=feedDagAt(feed,stream.get(),0,uint64_t(end),&genesis,err); if(rc!=FLATSQL_OK) return fail(rc);
    File recordOutput; recordOutput.h=stream->h; recordOutput.owned=false; recordOutput.size=uint64_t(end);
    std::map<uint64_t,uint64_t> offsets,refs; uint64_t liveBytes=0;
    for(size_t i=0;i<batches.size();i++) {
        auto& source=batches[i]; FmbCommit m=source.commit; m.rows.clear();
        std::set<uint64_t> fresh,emitted;
        for(size_t j=0;j<source.commit.rows.size();j++) {
            const auto& row=source.commit.rows[j]; auto alive=live.find((row.seq<<16)|row.rowJ);
            if(alive!=live.end() && alive->second==std::make_pair(i,j) && !offsets.count(row.offset)) fresh.insert(row.offset);
        }
        // Frame order is the unit's physical stream order. FMB rows retain
        // their plan order and may point backward to any shared frame.
        for(uint64_t oldOffset:fresh) {
            auto record=records.find(oldOffset); if(record==records.end()) return fail(FLATSQL_E_CORRUPT);
            std::string bytes; if(!recordBytes(archive,record->second,&bytes)) return fail(FLATSQL_E_CORRUPT);
            offsets.emplace(oldOffset,uint64_t(end)); std::string framed; le32(framed,uint32_t(bytes.size())); framed+=bytes;
            if(!recordOutput.write(uint64_t(end),framed)) return fail(FLATSQL_E_IO); end+=int64_t(framed.size()); liveBytes+=framed.size();
        }
        for(size_t j=0;j<source.commit.rows.size();j++) {
            FmbRow row=source.commit.rows[j]; uint64_t rid=(row.seq<<16)|row.rowJ; auto alive=live.find(rid);
            if(alive==live.end() || alive->second!=std::make_pair(i,j)) continue;
            auto record=records.find(row.offset); if(record==records.end()) return fail(FLATSQL_E_CORRUPT);
            uint64_t oldOffset=row.offset; row.offset=offsets.at(oldOffset);
            if(fresh.count(oldOffset) && emitted.insert(oldOffset).second) { row.flags|=FmbNewFrame; row.digest=record->second.digest; }
            else row.flags&=~uint32_t(FmbNewFrame);
            row.identity.clear(); auto identity=seqIdentities.find(row.seq);
            if(identity!=seqIdentities.end() && !identity->second.empty()) { row.identity=*identity->second.begin(); identity->second.erase(identity->second.begin()); }
            m.rows.push_back(std::move(row));
        }
        uint64_t commitAt=uint64_t(end); size_t newRow=0;
        for(size_t j=0;j<source.commit.rows.size();j++) { const auto& row=source.commit.rows[j]; auto alive=live.find((row.seq<<16)|row.rowJ); if(alive!=live.end() && alive->second==std::make_pair(i,j)) refs[(source.oldOffset<<16)|j]=(commitAt<<16)|newRow++; }
        if(!recordOutput.flush()) return fail(FLATSQL_E_IO);
        rc=streamAppendCommit(feed,stream.get(),m,&end,&previous); if(rc!=FLATSQL_OK) return fail(rc);
    }
    const uint64_t applied=uint64_t(end); rc=appendSegments(feed,stream.get(),segments,ck,refs,genesis.root,applied,&end,&previous); if(rc!=FLATSQL_OK) return fail(rc);
    rc=streamSync(stream.get()); if(rc!=FLATSQL_OK) return fail(rc);
    std::vector<int64_t> moved; rc=recoverFeed(type,feed,conn,&moved,err); if(rc!=FLATSQL_OK) return fail(rc);
    rc=indexPutFeed(type->idx,*feed,feed->gen); if(rc!=SQLITE_OK) return fail(statusOfSqlite(rc)); feed->registered=true;
    writerUnpin(type->e,feed); rc=feedOpen(type,feed,&moved,err); if(rc!=FLATSQL_OK) return rc;
    { std::lock_guard<std::mutex> lock(type->mu); type->vis.store(std::max(type->vis.load(),type->nextSeq-1)); }
    if(restored) *restored=feed;
    if(stats) { stats->bytes=archive.file.size; stats->blocks=archive.blocks.size(); stats->liveStreamBytes=liveBytes; stats->wallNs=monoNs()-start; }
    return FLATSQL_OK;
}
void carWork(Type* type,int op,Internal* in) {
    if(op==kOpCarExport) {
        Feed* feed=nullptr; { std::lock_guard<std::mutex> lock(type->mu); feed=type->feedById(in->what); }
        in->status=feed?carExportOnWriter(feed,in->archivePath,in->archiveStats,&in->err):FLATSQL_E_ARG;
    } else in->status=carRestoreOnWriter(type,in->archivePath,in->restoredFeed,in->archiveStats,&in->err);
}
int32_t carExportFeed(Feed* feed,const std::string& path,CarStats* stats,std::string* err) {
    if(!feed) return FLATSQL_E_ARG;
    if(!feed->type->e->started) return carExportOnWriter(feed,path,stats,err);
    Internal in; in.what=feed->fid; in.archivePath=path; in.archiveStats=stats;
    int32_t rc=runOnWriter(feed->type->e,feed->type,kOpCarExport,&in); if(err) *err=in.err; return rc;
}
int32_t carRestoreFeed(Type* type,const std::string& path,Feed** restored,CarStats* stats,std::string* err) {
    if(!type) return FLATSQL_E_ARG;
    if(!type->e->started) return carRestoreOnWriter(type,path,restored,stats,err);
    Internal in; in.archivePath=path; in.archiveStats=stats; in.restoredFeed=restored;
    int32_t rc=runOnWriter(type->e,type,kOpCarRestore,&in); if(err) *err=in.err; return rc;
}
} }
