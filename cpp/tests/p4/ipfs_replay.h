// Native phase of p4-replay.e2e.mjs. This is deliberately a stock block reader:
// it obtains ranges through the mailbox and parses protobuf/CAR independently.
#ifndef FLATSQL_P4_IPFS_REPLAY_H
#define FLATSQL_P4_IPFS_REPLAY_H
#if defined(__APPLE__)
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif

struct IpfsBlock {
    std::string cid, path, bytes;
    uint64_t offset=0, length=0, unit=0;
    int kind=0;
    std::vector<std::string> links;
};
struct IpfsFeed {
    std::string type, provider, source, root, path;
    uint32_t generation=0;
    uint64_t unit=0, streamBytes=0, linkBytes=0, records=0;
    std::map<std::string,IpfsBlock> blocks;
};
bool ipfsFail(const std::string& why) { std::fprintf(stderr,"IPFS: %s\n",why.c_str()); return false; }
std::string ipfsCid(const void* p,size_t n,uint8_t codec=0x55) {
    std::string cid(36,'\0'); cid[0]=1; cid[1]=char(codec); cid[2]=0x12; cid[3]=0x20;
    flatsql::ps::sha256(p,n,reinterpret_cast<uint8_t*>(&cid[4])); return cid;
}
void ipfsVar(std::string* b,uint64_t n) { do { b->push_back(char((n&127)|(n>=128?128:0))); n>>=7; } while(n); }
void ipfsField(std::string* b,uint8_t tag,const std::string& value) { b->push_back(char(tag));ipfsVar(b,value.size());b->append(value); }
bool ipfsVarRead(const std::string& b,size_t* p,uint64_t* out) {
    *out=0; for(unsigned shift=0;shift<64 && *p<b.size();shift+=7) { uint8_t v=uint8_t(b[(*p)++]); if(shift==63 && v>1) return false; *out|=uint64_t(v&127)<<shift;if(!(v&128))return true;}return false;
}
bool ipfsPb(const std::string& b,std::vector<std::string>* links,std::vector<std::string>* names=nullptr,std::string* data=nullptr) {
    size_t p=0; while(p<b.size()) {
        uint64_t tag=0,n=0;if(!ipfsVarRead(b,&p,&tag)||!ipfsVarRead(b,&p,&n)||n>b.size()-p) return false;
        const std::string field=b.substr(p,size_t(n));p+=size_t(n);
        if(tag==10) { if(data)*data=field;continue; }
        if(tag!=18) return false;
        size_t q=0;std::string cid,name;
        while(q<field.size()) {
            uint64_t t=0,v=0;if(!ipfsVarRead(field,&q,&t)||!ipfsVarRead(field,&q,&v))return false;
            if(t==24)continue;
            if((t!=10&&t!=18)||v>field.size()-q)return false;
            (t==10?cid:name)=field.substr(q,size_t(v));q+=size_t(v);
        }
        if(cid.size()!=36 || uint8_t(cid[0])!=1 || uint8_t(cid[2])!=0x12 || uint8_t(cid[3])!=0x20)return false;
        links->push_back(cid);if(names)names->push_back(name);
    }return true;
}
// Reference UnixFS encoding: links precede Data in canonical dag-pb bytes;
// a raw chunk's own size is its PBLink.Tsize, including the last short chunk.
std::string ipfsLargeReference(const uint8_t* bytes,size_t n) {
    std::string node, data("\x08\x02",2);data.push_back(0x18);ipfsVar(&data,n);
    for(size_t i=0;i<n;i+=1u<<20) {
        const size_t len=std::min<size_t>(1u<<20,n-i);std::string link;
        ipfsField(&link,10,ipfsCid(bytes+i,len));ipfsField(&link,18,"");link.push_back(24);ipfsVar(&link,len);ipfsField(&node,18,link);
        data.push_back(32);ipfsVar(&data,len);
    }ipfsField(&node,10,data);return node;
}
TlvW ipfsRequest(const IpfsFeed& f) { TlvW w;w.text(1,f.type).text(3,f.provider).text(4,f.source);return w; }
Result ipfsCall(uint32_t op,const TlvW& w) { CallOpts o;o.maxResultRows=2000000;o.maxResultBytes=uint64_t(512)<<20;o.timeoutMs=600000;return call(op,w.b,o); }
std::set<std::string> ipfsRecordCids(const IpfsFeed& f) {
    std::set<std::string> out;for(const auto& b:f.blocks)if(b.second.kind<=3)out.insert(b.first);return out;
}
bool ipfsDfs(const IpfsFeed& f,const std::string& cid,std::set<std::string>* seen,std::vector<std::string>* order=nullptr) {
    if(!seen->insert(cid).second)return true;
    const auto b=f.blocks.find(cid);if(b==f.blocks.end())return ipfsFail("root closure missing block "+flatsql::p4::ipldBase32(cid));
    if(order)order->push_back(cid);
    for(const auto& child:b->second.links)if(!ipfsDfs(f,child,seen,order))return false;
    return true;
}
bool ipfsRanges(const std::string& store,IpfsFeed* f,double* rangeUs=nullptr) {
    auto w=ipfsRequest(*f);const auto t=flatsql::ps::monoNs();const Result r=ipfsCall(P4_OPC_RANGES,w);
    if(rangeUs)*rangeUs=double(flatsql::ps::monoNs()-t)/1e3/std::max<size_t>(1,r.rows.size());
    if(r.status!=P4_OK)return ipfsFail("range op: "+r.err);
    f->blocks.clear();f->linkBytes=f->streamBytes=f->records=0;
    std::map<std::string,int> handles;
    bool ok=true;
    for(size_t i=0;i<r.rows.size()&&ok;i++) {
        IpfsBlock b;b.cid=r.s(i,"cid");b.path=r.s(i,"path");b.offset=uint64_t(r.i(i,"offset"));b.length=uint64_t(r.i(i,"length"));
        b.unit=uint64_t(r.i(i,"unit"));b.kind=int(r.i(i,"kind"));b.bytes=r.s(i,"block");
        if(b.cid.size()!=36 || b.kind<1 || b.kind>7 || b.unit>f->unit) {ok=ipfsFail("range schema or past commit point");break;}
        std::string bytes;
        if(b.kind==3 || b.kind==7) { bytes=b.bytes;f->linkBytes+=bytes.size(); }
        else {
            if(b.path.empty()||b.path.front()=='/'||b.path.find("..")!=std::string::npos||b.offset<4) {ok=ipfsFail("range path/offset");break;}
            auto h=handles.find(b.path);if(h==handles.end())h=handles.emplace(b.path,::open((store+"/"+b.path).c_str(),O_RDONLY)).first;
            if(h->second<0) {ok=ipfsFail("range open "+b.path);break;}
            bytes.resize(size_t(b.length));
            if(::pread(h->second,bytes.data(),bytes.size(),off_t(b.offset))!=ssize_t(bytes.size())) {ok=ipfsFail("range read "+b.path);break;}
            f->streamBytes+=b.length+4;f->path=b.path;
        }
        if(ipfsCid(bytes.data(),bytes.size(),uint8_t(b.cid[1]))!=b.cid) {ok=ipfsFail("range CID hash mismatch");break;}
        if(b.kind==3 || b.kind==7) {
            if(!ipfsPb(bytes,&b.links)) {ok=ipfsFail("dag-pb parse");break;}
        } else b.bytes.clear();
        if(b.kind==1||b.kind==3)f->records++;
        const auto prior=f->blocks.find(b.cid);
        if(prior!=f->blocks.end()) {
            if(prior->second.kind!=b.kind && !(prior->second.kind==1&&b.kind==2)) {ok=ipfsFail("inconsistent duplicate block");break;}
        } else f->blocks.emplace(b.cid,std::move(b));
    }
    for(const auto& h:handles)::close(h.second);
    if(!ok)return false;
    std::set<std::string> closure;if(!ipfsDfs(*f,f->root,&closure)||closure.size()!=f->blocks.size())return ipfsFail("unreachable range blocks");
    const auto root=f->blocks.find(f->root);std::vector<std::string> links,names;std::string data;
    if(root==f->blocks.end()||!ipfsPb(root->second.bytes,&links,&names,&data)||names!=std::vector<std::string>{"checkpoint","commits","segments"}||data!=std::string("\x08\x01",2))return ipfsFail("UnixFS Directory root names/data");
    // Independently obtain the live frame set from the row index, then compare
    // the graph's RECORD/LARGE_ROOT nodes against those exact frame bytes.
    auto* e=flatsql::p4::currentEngine();flatsql::p4::Feed* feed=nullptr;
    for(const auto& type:e->types)if(type->name==f->type)for(const auto& candidate:type->feeds)
        if(candidate&&candidate->provider==f->provider&&candidate->source==f->source)feed=candidate.get();
    if(!feed)return ipfsFail("range feed lookup");
    sqlite3* db=nullptr;sqlite3_stmt* q=nullptr;
    if(sqlite3_open_v2(feed->path.c_str(),&db,SQLITE_OPEN_READONLY,"unix")!=SQLITE_OK)return ipfsFail("live index open");
    if(sqlite3_prepare_v2(db,"SELECT DISTINCT off,len FROM r",-1,&q,nullptr)!=SQLITE_OK) {sqlite3_close(db);return ipfsFail("live frame query");}
    std::set<std::pair<uint64_t,uint64_t>> wanted,actual;
    while(sqlite3_step(q)==SQLITE_ROW)wanted.emplace(uint64_t(sqlite3_column_int64(q,0)),uint64_t(sqlite3_column_int64(q,1)));
    for(const auto& item:f->blocks) {
        const auto& b=item.second;
        if(b.kind==1)actual.emplace(b.offset-4,b.length);
        if(b.kind==3) {
            if(b.links.empty()) {ok=false;break;}
            uint64_t length=0;for(const auto& cid:b.links) {const auto leaf=f->blocks.find(cid);if(leaf==f->blocks.end()||leaf->second.kind!=2) {ok=false;break;}length+=leaf->second.length;}
            if(!ok)break;actual.emplace(f->blocks.at(b.links.front()).offset-4,length);
        }
    }
    sqlite3_finalize(q);sqlite3_close(db);
    if(!ok||actual!=wanted)return ipfsFail("feed closure does not equal live record frame offsets/lengths");
    return true;
}
bool ipfsRefreshRoot(IpfsFeed* f) {
    const auto r=ipfsCall(P4_OPC_FEED_ROOT,ipfsRequest(*f));if(r.status!=P4_OK||r.rows.size()!=1)return ipfsFail("feed root op: "+r.err);
    f->root=r.s(0,"cid");f->unit=uint64_t(r.i(0,"unit"));f->generation=uint32_t(r.i(0,"generation"));return true;
}
uint64_t ipfsLE(const uint8_t* p,size_t n) {uint64_t v=0;for(size_t i=0;i<n;i++)v|=uint64_t(p[i])<<(i*8);return v;}
bool ipfsFileEqual(const std::string& a,const std::string& b) {
    if(std::filesystem::file_size(a)!=std::filesystem::file_size(b))return false;
    std::ifstream x(a,std::ios::binary),y(b,std::ios::binary);std::vector<char> xb(1u<<20),yb(xb.size());
    while(x) {x.read(xb.data(),xb.size());y.read(yb.data(),yb.size());if(x.gcount()!=y.gcount()||std::memcmp(xb.data(),yb.data(),size_t(x.gcount())))return false;}return true;
}
// Independent CARv2 parser. The index offsets are relative to the start of
// the embedded CARv1, and point to each section's length varint.
bool ipfsCar(const std::string& path,const IpfsFeed& feed,uint64_t* blocks) {
    std::ifstream in(path,std::ios::binary);const uint64_t total=std::filesystem::file_size(path);
    uint8_t h[51];in.read(reinterpret_cast<char*>(h),51);
    const uint8_t pragma[]={10,161,103,118,101,114,115,105,111,110,2};
    if(!in||std::memcmp(h,pragma,11)||ipfsLE(h+11,8)||ipfsLE(h+19,8)||ipfsLE(h+27,8)!=51)return ipfsFail("CARv2 pragma/header");
    const uint64_t size=ipfsLE(h+35,8),index=ipfsLE(h+43,8);if(index!=51+size||index>=total)return ipfsFail("CARv2 offsets");
    auto var=[&](uint64_t* v) { *v=0;for(unsigned s=0;s<64;s+=7) {int c=in.get();if(c<0||(s==63&&c>1))return false;*v|=uint64_t(c&127)<<s;if(!(c&128))return true;}return false;};
    uint64_t n=0;if(!var(&n)||n>4096)return ipfsFail("CARv1 header length");
    std::string header(size_t(n),'\0');in.read(header.data(),std::streamsize(n));
    std::string expect("\xa2\x65roots\x81\xd8\x2a\x58\x25\0",13);expect+=feed.root;expect+=std::string("\x67version\x01",9);
    if(header!=expect)return ipfsFail("CARv1 dag-cbor roots/version");
    std::set<std::string> visited;std::vector<std::string> order;if(!ipfsDfs(feed,feed.root,&visited,&order))return false;
    std::map<std::string,uint64_t> offsets;size_t count=0;
    while(uint64_t(in.tellg())<index) {
        const uint64_t off=uint64_t(in.tellg())-51;if(!var(&n)||n<36||n>index-uint64_t(in.tellg()))return ipfsFail("CAR section size");
        std::string cid(36,'\0'),bytes(size_t(n-36),'\0');in.read(cid.data(),36);in.read(bytes.data(),std::streamsize(bytes.size()));
        if(!in||count>=order.size()||cid!=order[count++]||ipfsCid(bytes.data(),bytes.size(),uint8_t(cid[1]))!=cid||!offsets.emplace(cid.substr(4),off).second)return ipfsFail("CAR DFS/CID/dedup");
    }
    if(count!=order.size()||!var(&n)||n!=0x401)return ipfsFail("CAR MultihashIndexSorted codec");
    uint8_t ih[28];in.read(reinterpret_cast<char*>(ih),28);
    if(!in||ipfsLE(ih,4)!=1||ipfsLE(ih+4,8)!=0x12||ipfsLE(ih+12,4)!=1||ipfsLE(ih+16,4)!=40||ipfsLE(ih+20,8)!=count*40)return ipfsFail("CAR multihash index buckets");
    std::string previous;for(size_t i=0;i<count;i++) {
        uint8_t entry[40];in.read(reinterpret_cast<char*>(entry),40);const std::string digest(reinterpret_cast<char*>(entry),32);const auto o=offsets.find(digest);
        if(!in||(!previous.empty()&&digest<previous)||o==offsets.end()||o->second!=ipfsLE(entry+32,8))return ipfsFail("CAR sorted index entry/offset");
        previous=digest;offsets.erase(o);
    }
    if(!offsets.empty()||uint64_t(in.tellg())!=total)return ipfsFail("CAR index trailing bytes");*blocks=count;return true;
}
flatsql::p4::Feed* ipfsEngineFeed(const IpfsFeed& f) {
    auto* type=flatsql::p4::currentEngine()->findType(f.type);if(!type)return nullptr;
    for(const auto& candidate:type->feeds)if(candidate&&candidate->provider==f.provider&&candidate->source==f.source)return candidate.get();return nullptr;
}
bool ipfsFeeds(std::vector<IpfsFeed>* feeds) {
    feeds->clear();
    for(const char* type:{"OMM","WXF","IQC"}) {TlvW w;w.text(1,type);const auto r=ipfsCall(P4_OPC_FEED_ROOT,w);
        if(r.status!=P4_OK)return ipfsFail("type roots: "+r.err);
        for(size_t i=0;i<r.rows.size();i++) {IpfsFeed f;f.type=type;f.provider=r.s(i,"provider");f.source=r.s(i,"source");f.root=r.s(i,"cid");f.unit=uint64_t(r.i(i,"unit"));f.generation=uint32_t(r.i(i,"generation"));feeds->push_back(std::move(f));}
    }return !feeds->empty();
}
// One RANGES answer (one snapshot): record CIDs, exposed CIDs, and every
// CID with its unit, and the snapshot's generation.
bool ipfsRangeRecordSet(const IpfsFeed& f,std::set<std::string>* out,std::set<std::string>* exposed=nullptr,std::map<std::string,uint64_t>* all=nullptr,uint32_t* generation=nullptr) {
    const auto r=ipfsCall(P4_OPC_RANGES,ipfsRequest(f));if(r.status!=P4_OK)return ipfsFail("retention ranges: "+r.err);
    if(generation)*generation=r.rows.empty()?UINT32_MAX:uint32_t(r.i(0,"generation"));
    for(size_t i=0;i<r.rows.size();i++) {if(r.i(i,"kind")<=3)out->insert(r.s(i,"cid"));if(exposed&&r.i(i,"kind")!=7)exposed->insert(r.s(i,"cid"));if(all)all->emplace(r.s(i,"cid"),uint64_t(r.i(i,"unit")));
        if(generation&&uint32_t(r.i(i,"generation"))!=*generation)return ipfsFail("one RANGES answer spans generations");}
    return true;
}
bool ipfsIdentityFixture(int64_t* seq) {
    Batch b;b.type="IQC";b.peer="identity-first";b.at=kT0;b.tags.push_back({"ipfs-e2e","identity","https://example.invalid/identity","first","","",""});
    In rec;rec.ts=kT0;rec.frame=iqcFrame("identity-copy","2026-10-18T00:00:00",128,42);rec.sig.assign(64,0x53);rec.hasIdent=true;
    for(size_t i=0;i<32;i++)rec.ident[i]=uint8_t(0xa0+i);b.recs.push_back(rec);
    const auto first=put(b);if(first.status!=P4_OK||first.rows.size()!=1||first.i(0,"reject"))return ipfsFail("identity original PUT");*seq=first.i(0,"seq");
    b.peer="identity-copy";b.tags[0].batch="survivor";b.tags[0].url="https://example.invalid/identity-copy";b.recs[0].hasIdent=false;b.recs[0].sig.assign(64,0x43);b.at++;b.recs[0].ts++;
    const auto copy=put(b);if(copy.status!=P4_OK||copy.rows.size()!=1||copy.i(0,"reject")||copy.i(0,"seq")!=*seq)return ipfsFail("identity shared copy PUT");
    TlvW remove;remove.text(1,"IQC").text(11,"ipfs-e2e").text(12,"identity").text(60,"survivor").u8(61,1);
    if(ipfsCall(P4_OPC_SUPERSEDE,remove).status!=P4_OK)return ipfsFail("identity original-only removal");return true;
}
bool ipfsIdentityHash(int64_t seq,std::string* out) {
    IpfsFeed f;f.type="IQC";f.provider="ipfs-e2e";f.source="identity";auto* feed=ipfsEngineFeed(f);if(!feed)return ipfsFail("identity feed missing");
    sqlite3* db=nullptr;sqlite3_stmt* q=nullptr;if(sqlite3_open_v2(feed->path.c_str(),&db,SQLITE_OPEN_READONLY,"unix")!=SQLITE_OK)return ipfsFail("identity index open");
    bool ok=sqlite3_prepare_v2(db,"SELECT h,seq FROM ident WHERE h NOT IN (SELECT h FROM idst) UNION ALL SELECT h,seq FROM idst WHERE rowid IN (SELECT max(rowid) FROM idst GROUP BY h) ORDER BY h",-1,&q,nullptr)==SQLITE_OK;Hash hash;size_t count=0;
    if(ok)while(sqlite3_step(q)==SQLITE_ROW) {const auto* p=static_cast<const uint8_t*>(sqlite3_column_blob(q,0));if(sqlite3_column_bytes(q,0)!=32||sqlite3_column_int64(q,1)!=seq) {ok=false;break;}for(size_t i=0;i<32;i++)if(p[i]!=uint8_t(0xa0+i))ok=false;hash.bytes(p,32);hash.i64(seq);count++;}
    sqlite3_finalize(q);q=nullptr;if(ok)ok=sqlite3_prepare_v2(db,"SELECT count(*) FROM r",-1,&q,nullptr)==SQLITE_OK&&sqlite3_step(q)==SQLITE_ROW&&sqlite3_column_int64(q,0)==2;
    sqlite3_finalize(q);sqlite3_close(db);if(!ok||count!=1)return ipfsFail("identity did not survive original-row removal / CAR restore");
    TlvW scan;scan.text(1,"IQC").u8(2,1).u64(3,10).u8(5,P4_ORDER_SEQ_ASC);hash.result(ipfsCall(P4_OPC_SCAN,scan));*out=hash.hex();return true;
}
// Bytes the allocator holds for the process (live allocations only).
uint64_t ipfsHeapInUse() {
#if defined(__APPLE__)
    malloc_statistics_t st{}; malloc_zone_statistics(nullptr,&st); return uint64_t(st.size_in_use);
#else
    return uint64_t(mallinfo2().uordblks);
#endif
}
// The resident derived DAG of the gate's largest feed (the WXF sea feed),
// measured as the allocator's growth across one rebuild of its cache.
bool ipfsDagMemory() {
    IpfsFeed sea;sea.type="WXF";sea.provider="noaa-ncep";sea.source="gfs-0p25-sea";
    auto* feed=ipfsEngineFeed(sea);if(!feed)return ipfsFail("sea feed missing");
    flatsql::p4::feedDagReset(feed);
    const uint64_t before=ipfsHeapInUse();
    { flatsql::p4::DagSnapshot snapshot;std::string err;if(flatsql::p4::feedDagSnapshot(feed,&snapshot,&err)!=P4_OK)return ipfsFail("sea DAG: "+err); }
    const uint64_t after=ipfsHeapInUse();int64_t rows=0;{std::lock_guard<std::mutex> g(feed->type->mu);rows=feed->k.rows;}
    std::printf("REPLAY_IPFS_DAG_MEMORY type=WXF source=gfs-0p25-sea rows=%lld resident_bytes=%lld bytes_per_row=%.1f\n",(long long)rows,(long long)(after-before),double(int64_t(after-before))/double(std::max<int64_t>(1,rows)));
    return true;
}
bool ipfsProof(const std::string& store,const Probe& probe) {
    const uint64_t phaseStart=flatsql::ps::monoNs();
    if(openNode(store,1)!=P4_OK||!registerAll()||registerType(iqcType())!=P4_OK)return false;
    if(!ipfsDagMemory())return false;
    int64_t identitySeq=0;if(!ipfsIdentityFixture(&identitySeq))return false;
    std::setvbuf(stdout,nullptr,_IOLBF,0);
    std::string twoLeaf;if(!flatsql::p4::ipldGoldenVectors(&twoLeaf))return ipfsFail("published IPLD golden vectors");
    std::printf("REPLAY_IPFS_GOLDEN empty_UnixFS=bafybeif7ztnhq65lumvvtr4ekcwd2ifwgm3awq4zfr3srh462rwyinlb4y hello_raw=bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4 two_leaf=%s payload=1048576*a+1048576*b cross_check=ipfs_add_required PASS\n",twoLeaf.c_str());
    Batch large;large.type="WXF";large.peer=kPeer;large.at=kT0;large.tags.push_back({"ipfs-e2e","large","https://example.invalid/large","large-vectors","","",""});
    std::vector<std::string> largeRoots;
    for(size_t n:{size_t(3u<<20),size_t(3u<<19)}) {
        In rec;rec.ts=kT0;rec.sig.resize(64);for(size_t i=0;i<rec.sig.size();i++)rec.sig[i]=uint8_t(i+n/(1u<<19));rec.frame=wxfFrame("ipfs-"+std::to_string(n),0,"large",payloadOf(n-256));
        if(rec.frame.size()>n+4)return ipfsFail("large fixture size");
        rec.frame.resize(n+4,0x5a);flatsql::p4::st32(rec.frame.data(),uint32_t(n));
        const auto node=ipfsLargeReference(rec.frame.data()+4,n);largeRoots.push_back(ipfsCid(node.data(),node.size(),0x70));large.recs.push_back(std::move(rec));
    }
    const auto wrote=put(large);if(wrote.status!=P4_OK||wrote.rows.size()!=2)return ipfsFail("large fixture PUT: "+wrote.err);
    for(size_t i=0;i<wrote.rows.size();i++)if(wrote.i(i,"reject"))return ipfsFail("large fixture reject "+std::to_string(wrote.i(i,"reject")));
    IpfsFeed lf;lf.type="WXF";lf.provider="ipfs-e2e";lf.source="large";
    if(!ipfsRefreshRoot(&lf)||!ipfsRanges(store,&lf))return false;
    for(const auto& cid:largeRoots)if(!lf.blocks.count(cid)||lf.blocks.at(cid).kind!=3)return ipfsFail("large record root does not match independent encoding");
    std::printf("REPLAY_IPFS_LARGE bytes=3145728,1572864 root_3m=%s root_1_5m=%s chunks=3,2 roots=matching PASS\n",flatsql::p4::ipldBase32(largeRoots[0]).c_str(),flatsql::p4::ipldBase32(largeRoots[1]).c_str());
    auto expose=ipfsRequest(lf);expose.u8(81,1);if(ipfsCall(P4_OPC_EXPOSURE,expose).status!=P4_OK)return ipfsFail("exposure on");
    // Take the exact block sets before retention. This includes large roots
    // and every chunk, so a whole-range CID accidentally exposed is caught.
    std::vector<IpfsFeed> before;if(!ipfsFeeds(&before))return false;
    std::set<std::string> prior,priorExposed;std::map<std::pair<std::string,std::string>,std::map<std::string,uint64_t>> priorClosure;std::map<std::pair<std::string,std::string>,uint32_t> priorGeneration;
    for(auto& f:before)if(f.type=="WXF"&&!ipfsRangeRecordSet(f,&prior,&priorExposed,&priorClosure[{f.provider,f.source}],&priorGeneration[{f.provider,f.source}]))return false;
    Hash retained;int64_t retentionRows=0,retentionOps=0;wRetention(probe,&retained,&retentionRows,&retentionOps);
    uint8_t largeKey[36];cidOf(large.recs[0].frame,largeKey);TlvW dw;dw.text(1,"WXF");
    const auto list=cidList({std::vector<uint8_t>(largeKey,largeKey+36)},0,1);dw.raw(40,list.data(),list.size());
    if(ipfsCall(P4_OPC_DELETE,dw).status!=P4_OK)return ipfsFail("large retention DELETE");
    std::vector<IpfsFeed> after;if(!ipfsFeeds(&after))return false;
    std::set<std::string> live,liveExposed;
    for(auto& f:after)if(f.type=="WXF"&&!ipfsRangeRecordSet(f,&live,&liveExposed))return false;
    std::set<std::string> removed;std::set_difference(prior.begin(),prior.end(),live.begin(),live.end(),std::inserter(removed,removed.end()));
    TlvW dropsRequest;dropsRequest.text(1,"WXF");const auto drops=ipfsCall(P4_OPC_DROPS,dropsRequest);std::set<std::string> dropped,allDrops;
    if(drops.status!=P4_OK)return ipfsFail("drop list: "+drops.err);
    for(size_t i=0;i<drops.rows.size();i++) {if(drops.i(i,"unit")<=0)return ipfsFail("drop missing commit unit");const auto cid=drops.s(i,"cid");allDrops.insert(cid);if(prior.count(cid))dropped.insert(cid);}
    if(removed.empty()||removed!=dropped)return ipfsFail("drop list differs from removed record blocks: expected "+std::to_string(removed.size())+" got "+std::to_string(dropped.size()));
    // Incremental pulls: within one generation, every block new since the
    // prior answer carries a unit past the prior FEED_ROOT's commit, so a
    // RANGES from that commit + 1 finds it. Each side is one RANGES answer
    // with its own generation (a background compaction can land between calls).
    live.clear();liveExposed.clear();size_t deltaBlocks=0;
    for(auto& f:after)if(f.type=="WXF") {
        std::map<std::string,uint64_t> closure;uint32_t generation=0;if(!ipfsRangeRecordSet(f,&live,&liveExposed,&closure,&generation))return false;
        const IpfsFeed* old=nullptr;for(const auto& candidate:before)if(candidate.type==f.type&&candidate.provider==f.provider&&candidate.source==f.source)old=&candidate;
        if(!old)return ipfsFail("incremental range old feed");
        const auto key=std::make_pair(f.provider,f.source);
        const uint64_t from=generation==priorGeneration[key]&&generation==old->generation?old->unit+1:0;
        for(const auto& block:closure)if(!priorClosure[key].count(block.first)) {
            if(block.second<from)return ipfsFail("incremental ranges omit rewritten DAG unit/link: source="+f.source+" unit="+std::to_string(block.second)+" from="+std::to_string(from)+" generation="+std::to_string(generation));
            deltaBlocks++;
        }
    }
    std::printf("REPLAY_IPFS_INCREMENTAL new_closure_blocks=%zu delta=complete PASS\n",deltaBlocks);
    size_t metadataDrops=0;
    for(const auto& cid:priorExposed)if(!liveExposed.count(cid)) {if(!allDrops.count(cid))return ipfsFail("missing obsolete metadata drop "+flatsql::p4::ipldBase32(cid));if(!prior.count(cid))metadataDrops++;}
    for(const auto& cid:allDrops)if(liveExposed.count(cid))return ipfsFail("drop list names a live block");
    std::printf("REPLAY_IPFS_RETENTION rows=%lld removed_blocks=%zu metadata_drops=%zu drops=exact closure=live_only PASS\n",(long long)retentionRows+1,removed.size(),metadataDrops);
    if(!ipfsRefreshRoot(&lf))return false;auto* oldFeed=ipfsEngineFeed(lf);if(!oldFeed)return false;
    // Retention may already have compacted this small test feed. Acknowledge
    // that publication before the controlled compaction below.
    auto priorRelease=ipfsRequest(lf);priorRelease.u64(80,lf.unit).u32(82,lf.generation);if(ipfsCall(P4_OPC_RELEASE,priorRelease).status!=P4_OK)return ipfsFail("release earlier automatic compaction");
    if(!ipfsRefreshRoot(&lf))return false;
    const std::string oldPath=oldFeed->stream->path;const uint32_t oldGeneration=oldFeed->gen;const uint64_t oldUnit=lf.unit;
    flatsql::p4::Internal compact;compact.what=oldFeed->fid;compact.force=true;
    if(flatsql::p4::runOnWriter(flatsql::p4::currentEngine(),oldFeed->type,flatsql::p4::kOpCompact,&compact)!=P4_OK)return ipfsFail("IPFS compact: "+compact.err);
    const uint64_t compactAt=flatsql::ps::monoNs();
    flatsql::p4::Internal blocked;blocked.what=oldFeed->fid;blocked.force=true;
    if(flatsql::p4::runOnWriter(flatsql::p4::currentEngine(),oldFeed->type,flatsql::p4::kOpCompact,&blocked)!=P4_E_BUSY)return ipfsFail("compaction accepted an unreleased prior generation");
    if(!std::filesystem::exists(oldPath)||oldFeed->gen<=oldGeneration)return ipfsFail("compaction removed unreleased generation");
    std::vector<IpfsFeed> feeds;if(!ipfsFeeds(&feeds))return false;
    const std::string carDir=std::filesystem::path(store).parent_path().string()+"/cars";std::filesystem::create_directories(carDir);
    std::vector<std::pair<std::string,std::string>> cars;std::map<std::string,uint32_t> sourceFeedIds;
    struct Measure {uint64_t records=0,links=0,car=0,stream=0,tuples=0;double rangeUs=0;size_t feeds=0;};std::map<std::string,Measure> metrics;
    uint64_t exportNs=0,carBlocks=0;size_t feedN=0;
    for(auto& f:feeds) {
        auto* feed=ipfsEngineFeed(f);if(!feed)return ipfsFail("export feed lookup");
        const std::string path=carDir+"/"+std::to_string(feedN++)+".car",again=path+".again";
        flatsql::p4::CarStats first,second;std::string err;
        if(flatsql::p4::carExportFeed(feed,path,&first,&err)!=P4_OK||flatsql::p4::carExportFeed(feed,again,&second,&err)!=P4_OK)return ipfsFail("CAR export: "+err);
        exportNs+=first.wallNs;
        if(!ipfsFileEqual(path,again))return ipfsFail("CAR deterministic bytes");std::filesystem::remove(again);
        double rangeUs=0;if(!ipfsRefreshRoot(&f)||!ipfsRanges(store,&f,&rangeUs))return false;
        uint64_t blocks=0;if(!ipfsCar(path,f,&blocks))return false;carBlocks+=blocks;
        auto& m=metrics[f.type];m.records+=f.records;m.links+=f.linkBytes;m.car+=first.bytes;m.stream+=first.liveStreamBytes;m.rangeUs+=rangeUs*f.blocks.size();m.tuples+=f.blocks.size();m.feeds++;
        cars.push_back({f.type,path});sourceFeedIds[path]=feed->fid;
        std::printf("REPLAY_IPFS_RANGE type=%s source=%s tuples=%zu us_per_tuple=%.3f hashes=matching closure=exact PASS\n",f.type.c_str(),f.source.c_str(),f.blocks.size(),rangeUs);
        auto partial=ipfsRequest(f);partial.u64(80,f.unit);const auto tail=ipfsCall(P4_OPC_RANGES,partial);if(tail.status!=P4_OK)return ipfsFail("range from commit");
        for(size_t i=0;i<tail.rows.size();i++)if(uint64_t(tail.i(i,"unit"))<f.unit)return ipfsFail("range inclusive commit bound");
    }
    uint8_t liveLargeKey[36];cidOf(large.recs[1].frame,liveLargeKey);
    const std::vector<std::vector<uint8_t>> largeGetKeys{std::vector<uint8_t>(liveLargeKey,liveLargeKey+36)};
    auto want=allReadHashes(probe);Hash largeAnswer;const auto signedLarge=get("WXF",largeGetKeys);if(signedLarge.status!=P4_OK||signedLarge.rows.size()!=1||signedLarge.s(0,"sig")!=std::string(reinterpret_cast<const char*>(large.recs[1].sig.data()),large.recs[1].sig.size()))return ipfsFail("large record signature GET");largeAnswer.result(signedLarge);want["large_get"]=largeAnswer.hex();if(!ipfsIdentityHash(identitySeq,&want["identity_survivor"]))return false;
    // Leave exposure enabled for the full reader window. A generation must
    // still exist after both time and work have passed, until release arrives.
    const uint64_t deadline=compactAt+uint64_t(30500)*1000000;
    while(flatsql::ps::monoNs()<deadline)std::this_thread::sleep_for(std::chrono::milliseconds(50));
    if(!std::filesystem::exists(oldPath))return ipfsFail("unreleased generation deleted after reader window");
    auto release=ipfsRequest(lf);release.u64(80,oldUnit).u32(82,oldGeneration);
    if(ipfsCall(P4_OPC_RELEASE,release).status!=P4_OK)return ipfsFail("release through commit");
    const uint64_t releasedAt=flatsql::ps::monoNs();while(std::filesystem::exists(oldPath)&&flatsql::ps::monoNs()-releasedAt<uint64_t(5)*1000000000)std::this_thread::sleep_for(std::chrono::milliseconds(20));
    if(std::filesystem::exists(oldPath))return ipfsFail("released generation not unlinked");
    std::printf("REPLAY_IPFS_RELEASE generation=%u held_before_release=1 compaction_while_unreleased=busy window_ms=%.1f unlinked_after_release=1 PASS\n",oldGeneration,double(flatsql::ps::monoNs()-compactAt)/1e6);
    closeEngine(600000);
    const std::string restored=std::filesystem::path(store).parent_path().string()+"/restored/fsql4";
    if(openNode(restored,1)!=P4_OK||!registerAll()||registerType(iqcType())!=P4_OK)return ipfsFail("restore store open");uint64_t restoreNs=0;
    std::reverse(cars.begin(),cars.end());
    for(const auto& car:cars) {flatsql::p4::CarStats stats;std::string err;flatsql::p4::Feed* feed=nullptr;
        if(flatsql::p4::carRestoreFeed(flatsql::p4::currentEngine()->findType(car.first),car.second,&feed,&stats,&err)!=P4_OK)return ipfsFail("CAR restore: "+err);if(!feed||feed->fid!=sourceFeedIds.at(car.second))return ipfsFail("CAR restore changed FEED_ID");restoreNs+=stats.wallNs;
    }
    auto got=allReadHashes(probe);Hash restoredLarge;restoredLarge.result(get("WXF",largeGetKeys));got["large_get"]=restoredLarge.hex();if(!ipfsIdentityHash(identitySeq,&got["identity_survivor"]))return false;if(want!=got) {for(const auto& h:want)if(got.find(h.first)==got.end()||got.at(h.first)!=h.second)std::fprintf(stderr,"IPFS restore hash %s: want %s got %s\n",h.first.c_str(),h.second.c_str(),got.count(h.first)?got.at(h.first).c_str():"missing");return false;}
    closeEngine(600000);
    if(openNode(restored,1)!=P4_OK||!registerAll()||registerType(iqcType())!=P4_OK)return false;auto reopened=allReadHashes(probe);Hash reopenedLarge;reopenedLarge.result(get("WXF",largeGetKeys));reopened["large_get"]=reopenedLarge.hex();if(!ipfsIdentityHash(identitySeq,&reopened["identity_survivor"]))return false;closeEngine(600000);if(reopened!=want)return ipfsFail("restored CAR reopen answers");
    for(const auto& h:got)std::printf("REPLAY_IPFS_RESTORE workload=%s hash=%s source=equal reopened=equal PASS\n",h.first.c_str(),h.second.c_str());
    for(const auto& item:metrics) {const auto& m=item.second;std::printf("REPLAY_IPFS_METRIC type=%s records=%llu dag_link_bytes_per_record=%.3f car_bytes=%llu live_stream_bytes=%llu car_stream_ratio=%.6f range_us_per_tuple=%.3f\n",item.first.c_str(),(unsigned long long)m.records,double(m.links)/std::max<uint64_t>(1,m.records),(unsigned long long)m.car,(unsigned long long)m.stream,double(m.car)/std::max<uint64_t>(1,m.stream),m.rangeUs/std::max<uint64_t>(1,m.tuples));}
    std::printf("REPLAY_IPFS_CAR feeds=%zu blocks=%llu deterministic=1 header=valid sections=valid index=valid feed_ids=preserved restore_order=reversed export_ms=%.3f restore_ms=%.3f PASS\n",cars.size(),(unsigned long long)carBlocks,double(exportNs)/1e6,double(restoreNs)/1e6);
    std::printf("REPLAY_IPFS wall_ms=%.3f PASS\n",double(flatsql::ps::monoNs()-phaseStart)/1e6);return true;
}
#endif
