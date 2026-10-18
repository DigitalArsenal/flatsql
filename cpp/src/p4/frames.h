// Store format 5 persistence: semantic commit data, independent of generated SDS tables.
#ifndef FLATSQL_P4_FRAMES_H
#define FLATSQL_P4_FRAMES_H
#include <array>
#include <cstdint>
#include <string>
#include <vector>
namespace flatsql { namespace p4 {
struct Feed; struct Stream; struct Conn; struct Type; struct Counters;
enum : uint32_t { FmbNewFrame=1, FmbSealed=2, FmbLarge=4, FmbMovedIn=8, FmbCompacted=16, FmbCopy=32, FmbRetag=64 };
struct FmbRow {
    uint64_t offset=0, seq=0;
    int64_t sourceTs=0;
    uint32_t length=0, flags=0;
    uint32_t group=0;
    uint16_t rowJ=0;
    std::array<uint8_t,32> digest{};
    std::string signature, columns, identity;
    int64_t epoch=INT64_MIN;
    uint32_t objectKey=0;
    std::array<uint8_t,32> leafDigest{};
};
struct FmbGroup { int64_t deliveredAt=0; uint32_t node=0,batch=0,contentKey=0,url=0; };
struct FmbEntry { uint8_t kind=0; uint32_t id=0; std::vector<std::string> text; };
struct FmbRemove { uint64_t seq=0; uint32_t rowJ=UINT32_MAX; uint8_t reason=0; };
struct FmbUrl { uint64_t seq=0; uint32_t rowJ=0,url=0; };
struct FmbFrameProof { uint32_t size=0; std::array<uint8_t,32> digest{}; };
struct FmbCommit {
    int64_t committedAt=0; uint64_t typeSeqHigh=0;
    std::vector<FmbRow> rows;
    std::vector<FmbGroup> groups;
    std::vector<FmbEntry> dictionary;
    std::vector<FmbRemove> removals;
    std::vector<FmbUrl> urls;
};
// Appends metadata, splitting at 65,536 rows. No sync: the round syncs once.
int32_t streamAppendCommit(Feed*, Stream*, const FmbCommit&, int64_t* end, uint64_t* previous);
int32_t streamGenesis(Feed*, Stream*, int64_t* end);
int32_t recoverFeed(Type*,Feed*,Conn*,std::vector<int64_t>* moved,std::string* err);
int32_t replayRebuildFeed(::P4Engine*,Type*,Feed*,int64_t* indexed,std::string* err);
bool p4ListStreams(const std::string&,std::vector<std::string>*);
bool streamGenerationComplete(Feed*,uint32_t);
int32_t streamDiscover(Type*,Conn*,std::string* err);
int32_t appendConsolidated(Feed*,Conn*,Stream*,int64_t* end,uint64_t* previous,std::string* err,const std::vector<int64_t>* rids=nullptr,bool includeDictionary=true);
} }
#endif
