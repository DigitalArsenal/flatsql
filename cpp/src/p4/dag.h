// Derived IPLD view of the authoritative, acknowledged stream.
#ifndef FLATSQL_P4_DAG_H
#define FLATSQL_P4_DAG_H
#include "internal.h"
namespace flatsql { namespace p4 {
struct DagBlock {
    std::string cid,bytes,path;
    uint64_t offset=0,length=0,unit=0;
    uint32_t kind=0;
    std::vector<std::string> links;
};
struct DagSnapshot {
    std::string root,commitsRoot;
    std::shared_ptr<Stream> stream;
    std::vector<DagBlock> blocks; // deterministic DFS pre-order, first occurrence only
    uint64_t through=0;
    uint32_t generation=0;
};
struct DagDrop { std::string cid; uint64_t unit=0; uint32_t generation=0; };
struct DagState;
int32_t feedDagSnapshot(Feed*,DagSnapshot*,std::string* err);
// Writer/checkpoint-only view; never publishes unacknowledged ranges.
int32_t feedDagAt(Feed*,Stream*,uint64_t previous,uint64_t end,DagSnapshot*,std::string* err);
int32_t feedDagCommitRoot(Feed*,Stream*,uint64_t previous,uint64_t end,std::string*,std::string* err);
// The same with Feed::dagMu already held. The cache is incremental: an end
// before its own replays the generation, so callers ask at a mark no cache
// build can have passed (a committed mark read while dagMu is held).
int32_t feedDagCommitRootHeld(Feed*,Stream*,uint64_t previous,uint64_t end,std::string*,std::string* err);
std::string feedDagCheckpointCid(const void* bytes,size_t length);
int32_t feedDagDrops(Feed*,uint64_t from,std::vector<DagDrop>*,std::string* err);
int32_t feedExposure(Feed*,bool);
int32_t feedRelease(Feed*,uint32_t generation,uint64_t through);
// Drops the feed's derived DAG caches; the next use rebuilds them from the stream.
void feedDagReset(Feed*);
uint64_t feedBlockedGenerations(Feed*);
} }
#endif
