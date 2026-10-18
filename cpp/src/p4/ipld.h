// Deterministic IPLD wire codec. No persistence or feed policy lives here.
#ifndef FLATSQL_P4_IPLD_H
#define FLATSQL_P4_IPLD_H
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
namespace flatsql { namespace p4 {
constexpr size_t IpldChunkSize=1048576;
constexpr uint8_t IpldRaw=0x55, IpldDagPb=0x70;
struct IpldLink { std::string cid,name; uint64_t size=0; };
struct IpldLarge { std::string rootCid,rootNode,leafDigests; };
std::string ipldCidDigest(uint8_t codec,const uint8_t digest[32]);
std::string ipldCid(uint8_t codec,const void* bytes,size_t size);
std::string ipldBase32(const std::string& binary);
bool ipldCidValid(const std::string& cid);
// Preserve link order. An unnamed link still carries its empty Name field,
// as in Kubo. A null Data omits the field, distinct from an empty Data field.
std::string ipldDagPb(const std::vector<IpldLink>& links,const std::string* data=nullptr);
bool ipldLinks(const std::string& node,std::vector<IpldLink>* links,std::string* data=nullptr);
std::string ipldUnixfsFile(uint64_t size,const std::vector<uint64_t>& blocksizes);
std::string ipldUnixfsDirectory();
// Format 5's frame limit fits a single balanced level (at most 174 leaves).
bool ipldLarge(const void* bytes,size_t size,IpldLarge* out);
bool ipldLargeFromDigests(uint64_t size,const std::string& digests,IpldLarge* out);
// Called by the one replay E2E; includes the two published interoperability
// vectors and a two-leaf file (one MiB 'a', then one MiB 'b') whose root is
// Kubo's (`ipfs add --raw-leaves --chunker=size-1048576 --cid-version=1`).
bool ipldGoldenVectors(std::string* twoLeafRoot=nullptr);
} }
#endif
