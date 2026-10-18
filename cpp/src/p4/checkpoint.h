// Immutable format-5 state: codec and folding stay outside the writer/recovery policy.
#ifndef FLATSQL_P4_CHECKPOINT_H
#define FLATSQL_P4_CHECKPOINT_H
#include "internal.h"
namespace flatsql { namespace p4 {
struct CheckpointLatest {
    uint32_t objectKey=0;
    int64_t epoch=INT64_MIN;
    uint64_t rid=0,rowRef=0;
    bool operator==(const CheckpointLatest& x) const { return objectKey==x.objectKey && epoch==x.epoch && rid==x.rid && rowRef==x.rowRef; }
};
struct CheckpointSegment {
    uint64_t offset=0,minSeq=0,maxSeq=0,coversThrough=0,entries=0;
    uint32_t length=0;
    uint8_t kind=0,level=0;
    std::array<uint8_t,32> digest{};
};
struct CheckpointState {
    uint64_t offset=0,end=0,appliedThrough=0,typeSeqHigh=0;
    uint64_t segmentBytes=0,checkpointBytes=0;
    std::string feedRoot,commitsRoot; // derived from this checkpoint, not extra authority
    Counters counters;
    std::map<std::pair<uint8_t,uint32_t>,FmbEntry> dictionary;
    std::map<uint32_t,CheckpointLatest> latest;
    std::map<InstId,InstCount> instances;
    std::map<uint32_t,TokCount> tokens;
    std::set<uint64_t> tombstones;
    std::vector<CheckpointSegment> segments;
};
int32_t checkpointFindNewest(Feed*,Stream*,CheckpointState*,bool* found,std::string* err);
// Reads a candidate checkpoint and every listed segment, including its digest.
// Tail-search and commit-chain validation belong to the recovery caller.
int32_t checkpointReadAt(Feed*,Stream*,uint64_t,CheckpointState*,std::string* err);
// Exact row IDs added by metadata after a checkpoint, including moves and
// retags that preserve a sequence below the checkpoint's sequence high.
int32_t checkpointTailRows(Feed*,Stream*,uint64_t checkpointEnd,uint64_t previous,std::vector<uint64_t>* rids,
                          uint64_t* rowCount=nullptr,int64_t* lastCommitAt=nullptr);
// Restore row/idle cadence from already committed tail metadata after open.
int32_t checkpointRestoreCadence(Feed*,Stream*);
// The caller serializes with this feed's writer. This writes no sync and does
// not publish state: retain *state only once the surrounding sync succeeds.
// fresh=true starts a new generation with full level-zero segments.
int32_t checkpointAppend(Feed*,Conn*,Stream*,const Counters&,int64_t* end,uint64_t* previous,
                         CheckpointState* state,bool fresh,std::string* err);
// Standalone close/idle checkpoint; stream and SQLite mark are published together.
int32_t checkpointFeed(Feed*,bool force,std::string* err,bool syncNow=true);
bool checkpointDue(const Feed*,bool idle=false);
} }
#endif
