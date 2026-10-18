#include "ipld.h"
#include "flatsql/ps/platform.h"
#include <algorithm>
#include <cstring>

namespace flatsql { namespace p4 {
namespace {
void varint(std::string& out,uint64_t n) {
    while(n>=128) { out.push_back(char((n&127)|128)); n>>=7; }
    out.push_back(char(n));
}
void number(std::string& out,uint8_t tag,uint64_t n) { out.push_back(char(tag)); varint(out,n); }
void bytes(std::string& out,uint8_t tag,const std::string& s) {
    out.push_back(char(tag)); varint(out,s.size()); out+=s;
}
bool takeVarint(const std::string& in,size_t& at,uint64_t* n) {
    *n=0;
    for(unsigned shift=0;shift<64;shift+=7) {
        if(at==in.size()) return false;
        const uint8_t b=uint8_t(in[at++]);
        if(shift==63 && b>1) return false;
        *n|=uint64_t(b&127)<<shift;
        if(!(b&128)) return shift==0 || b!=0;
    }
    return false;
}
bool takeBytes(const std::string& in,size_t& at,std::string* out) {
    uint64_t n;
    if(!takeVarint(in,at,&n) || n>in.size()-at) return false;
    out->assign(in.data()+at,size_t(n)); at+=size_t(n); return true;
}
bool takeLink(const std::string& in,IpldLink* out) {
    size_t at=0; unsigned seen=0; uint8_t last=0;
    while(at<in.size()) {
        const uint8_t tag=uint8_t(in[at++]);
        if(tag<=last) return false;
        last=tag;
        if(tag==0x0a) { if(!takeBytes(in,at,&out->cid)) return false; seen|=1; }
        else if(tag==0x12) { if(!takeBytes(in,at,&out->name)) return false; seen|=2; }
        else if(tag==0x18) { if(!takeVarint(in,at,&out->size)) return false; seen|=4; }
        else return false;
    }
    return (seen&1) && ipldCidValid(out->cid);
}
}
std::string ipldCidDigest(uint8_t codec,const uint8_t digest[32]) {
    std::string out; out.reserve(36); out.push_back(1); out.push_back(char(codec));
    out.push_back(0x12); out.push_back(32); out.append(reinterpret_cast<const char*>(digest),32); return out;
}
std::string ipldCid(uint8_t codec,const void* data,size_t size) {
    uint8_t digest[32]; ps::sha256(data,size,digest); return ipldCidDigest(codec,digest);
}
bool ipldCidValid(const std::string& cid) {
    return cid.size()==36 && uint8_t(cid[0])==1 &&
        (uint8_t(cid[1])==IpldRaw || uint8_t(cid[1])==IpldDagPb) &&
        uint8_t(cid[2])==0x12 && uint8_t(cid[3])==32;
}
std::string ipldBase32(const std::string& binary) {
    static constexpr char alphabet[]="abcdefghijklmnopqrstuvwxyz234567";
    std::string out="b"; out.reserve(1+(binary.size()*8+4)/5);
    uint32_t bits=0; unsigned count=0;
    for(uint8_t b:binary) {
        bits=(bits<<8)|b; count+=8;
        while(count>=5) { count-=5; out.push_back(alphabet[(bits>>count)&31]); }
    }
    if(count) out.push_back(alphabet[(bits<<(5-count))&31]);
    return out;
}
std::string ipldDagPb(const std::vector<IpldLink>& links,const std::string* data) {
    std::string out;
    // go-merkledag's canonical PBNode wire order is Links, then Data.
    for(const auto& link:links) {
        std::string pb; bytes(pb,0x0a,link.cid); bytes(pb,0x12,link.name); number(pb,0x18,link.size);
        bytes(out,0x12,pb);
    }
    if(data) bytes(out,0x0a,*data);
    return out;
}
bool ipldLinks(const std::string& node,std::vector<IpldLink>* links,std::string* data) {
    links->clear(); if(data) data->clear(); size_t at=0; bool hadData=false;
    while(at<node.size()) {
        const uint8_t tag=uint8_t(node[at++]); std::string field;
        if(!takeBytes(node,at,&field)) return false;
        if(tag==0x12 && !hadData) {
            IpldLink link; if(!takeLink(field,&link)) return false;
            links->push_back(std::move(link));
        } else if(tag==0x0a && !hadData) { hadData=true; if(data) *data=std::move(field); }
        else return false;
    }
    return true;
}
std::string ipldUnixfsFile(uint64_t size,const std::vector<uint64_t>& blocksizes) {
    std::string out; number(out,0x08,2); number(out,0x18,size);
    for(uint64_t n:blocksizes) number(out,0x20,n);
    return out;
}
std::string ipldUnixfsDirectory() { return std::string("\x08\x01",2); }
bool ipldLargeFromDigests(uint64_t size,const std::string& digests,IpldLarge* out) {
    if(!out || size<=IpldChunkSize || size>174*uint64_t(IpldChunkSize)) return false;
    const size_t count=size_t((size+IpldChunkSize-1)/IpldChunkSize);
    if(digests.size()!=count*32) return false;
    std::vector<IpldLink> links; std::vector<uint64_t> sizes; links.reserve(count); sizes.reserve(count);
    for(size_t i=0;i<count;i++) {
        const uint64_t n=std::min<uint64_t>(IpldChunkSize,size-i*IpldChunkSize);
        links.push_back({ipldCidDigest(IpldRaw,reinterpret_cast<const uint8_t*>(digests.data())+32*i),"",n});
        sizes.push_back(n);
    }
    const std::string data=ipldUnixfsFile(size,sizes);
    out->leafDigests=digests; out->rootNode=ipldDagPb(links,&data);
    out->rootCid=ipldCid(IpldDagPb,out->rootNode.data(),out->rootNode.size()); return true;
}
bool ipldLarge(const void* bytes,size_t size,IpldLarge* out) {
    if(size<=IpldChunkSize || size>174*IpldChunkSize || !bytes || !out) return false;
    std::string digests; digests.resize(((size+IpldChunkSize-1)/IpldChunkSize)*32);
    for(size_t at=0,j=0;at<size;at+=IpldChunkSize,j+=32)
        ps::sha256(static_cast<const uint8_t*>(bytes)+at,std::min(IpldChunkSize,size-at),reinterpret_cast<uint8_t*>(&digests[j]));
    return ipldLargeFromDigests(size,digests,out);
}
bool ipldGoldenVectors(std::string* twoLeafRoot) {
    const std::string emptyData=ipldUnixfsFile(0,{}), empty=ipldDagPb({},&emptyData);
    if(ipldBase32(ipldCid(IpldDagPb,empty.data(),empty.size()))!=
       "bafybeif7ztnhq65lumvvtr4ekcwd2ifwgm3awq4zfr3srh462rwyinlb4y") return false;
    static constexpr char hello[]="hello world\n";
    if(ipldBase32(ipldCid(IpldRaw,hello,sizeof(hello)-1))!=
       "bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4") return false;
    std::string payload(IpldChunkSize,'a'); payload.append(IpldChunkSize,'b'); IpldLarge file;
    if(!ipldLarge(payload.data(),payload.size(),&file)) return false;
    std::vector<IpldLink> links; std::string data;
    if(!ipldLinks(file.rootNode,&links,&data) || links.size()!=2 ||
       links[0].size!=IpldChunkSize || links[1].size!=IpldChunkSize ||
       data!=ipldUnixfsFile(2*IpldChunkSize,{IpldChunkSize,IpldChunkSize}) ||
       links[0].cid!=ipldCid(IpldRaw,payload.data(),IpldChunkSize) ||
       links[1].cid!=ipldCid(IpldRaw,payload.data()+IpldChunkSize,IpldChunkSize)) return false;
    if(twoLeafRoot) *twoLeafRoot=ipldBase32(file.rootCid);
    return true;
}
} }
