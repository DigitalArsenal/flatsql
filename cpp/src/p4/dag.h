// Derived IPLD view of the authoritative, acknowledged stream.
#ifndef FLATSQL_P4_DAG_H
#define FLATSQL_P4_DAG_H
#include "internal.h"
namespace flatsql { namespace p4 {
struct DagBlock {
    std::string cid,bytes,path;
    uint64_t offset=0,length=0,unit=0;
    uint32_t kind=0;
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
// The feed's root at its committed point. With closure, also the root's
// closure; `from` keeps only blocks whose unit is at or after it, walking
// only the nodes rewritten since (cost follows the change, not the feed).
int32_t feedDagSnapshot(Feed*,DagSnapshot*,std::string* err,uint64_t from=0,bool closure=true);
// Writer/checkpoint-only view; never publishes unacknowledged ranges.
int32_t feedDagAt(Feed*,Stream*,uint64_t previous,uint64_t end,DagSnapshot*,std::string* err);
int32_t feedDagCommitRoot(Feed*,Stream*,uint64_t previous,uint64_t end,std::string*,std::string* err);
// The same with Feed::dagMu already held. The cache only moves forward: an
// earlier commit point is answered from the commits roots it recorded at the
// points it was built to, never by replaying the generation; a point it never
// stopped at is P4_E_BUSY (callers ask at a committed mark read under dagMu).
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
