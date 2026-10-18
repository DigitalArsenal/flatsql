// Immutable format-5 state: codec and folding stay outside the writer/recovery policy.
#ifndef FLATSQL_P4_CHECKPOINT_H
#define FLATSQL_P4_CHECKPOINT_H
#include "internal.h"
#include <functional>
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
    // No $FMB sits between APPLIED_THROUGH and this $FCK (a checkpoint
    // prepared off the writer is installed after later rounds).
    bool current=true;
    // Whether this state is the feed's whole state at the stream offset mark.
    bool holdsStateAt(uint64_t mark) const { return end==mark && current; }
};
// A checkpoint prepared from one snapshot at `at` (that snapshot's mark).
// Its new $FSG frames carry provisional offsets: every offset >= at names one
// of them and moves to the install position; older segments keep theirs.
struct CheckpointPlan {
    std::shared_ptr<Stream> stream;            // the generation it read
    std::shared_ptr<CheckpointState> base;     // the checkpoint it extends
    uint64_t at=0,previous=0;                  // the snapshot's mark and last commit
    CheckpointState state;                     // offsets and digests final at install
    std::vector<std::vector<uint64_t>> frames; // new FSG FlatBuffers (aligned), stream order
    std::vector<uint32_t> lengths;
    std::string previousRoot;                  // PREVIOUS_FEED_ROOT_CID
};
int32_t checkpointFindNewest(Feed*,Stream*,CheckpointState*,bool* found,std::string* err);
// Reads a candidate checkpoint and every listed segment, including its digest.
// Tail-search and commit-chain validation belong to the recovery caller.
int32_t checkpointReadAt(Feed*,Stream*,uint64_t,CheckpointState*,std::string* err);
// Exact row IDs added by metadata after a checkpoint's APPLIED_THROUGH
// (including the $FMBs before the $FCK itself), including moves and retags
// that preserve a sequence below the checkpoint's sequence high.
int32_t checkpointTailRows(Feed*,Stream*,const CheckpointState&,uint64_t previous,std::vector<uint64_t>* rids,
                          uint64_t* rowCount=nullptr,int64_t* lastCommitAt=nullptr);
// Restore row/idle cadence from already committed tail metadata after open.
int32_t checkpointRestoreCadence(Feed*,Stream*);
// Folds the feed as of mark (previous: its last commit) into a plan, reading
// c's one snapshot and the immutable stream prefix only; old is the state
// it extends (empty with fresh=true: a new generation with full level-zero
// segments). commitsRoot is the commits tree as of mark.
int32_t checkpointPrepare(Feed*,Conn* c,Stream*,uint64_t previous,uint64_t mark,const Counters&,const CheckpointState& old,
                          bool fresh,const std::string& commitsRoot,CheckpointPlan*,std::string* err);
// Appends the plan's segments and its $FCK at *end (the caller owns the
// stream's appends). Writes no sync and publishes nothing.
int32_t checkpointInstall(Feed*,Stream*,CheckpointPlan&,int64_t* end,uint64_t* previous,CheckpointState* state);
// Prepare and install at once. The caller serializes with this feed's
// writer. This writes no sync and does not publish state: retain *state only
// once the surrounding sync succeeds.
int32_t checkpointAppend(Feed*,Conn*,Stream*,const Counters&,int64_t* end,uint64_t* previous,
                         CheckpointState* state,bool fresh,std::string* err);
// Standalone checkpoint on the drained writer (close, export, or an idle
// install's sync); stream and SQLite mark are published together.
int32_t checkpointFeed(Feed*,bool force,std::string* err);
// Off-writer preparation (checkpoint_worker.cpp). Schedule queues a due feed
// for the preparation thread; the feed's appender (its indexer at a round
// boundary, or the drained writer when idle) installs the ready plan, whose
// frames are synced with that round (or alone) and published by its commit.
void checkpointSchedule(Feed*);
int32_t checkpointInstallReady(Feed*);
void checkpointLoop(Engine*);
void checkpointWorkerStop(Engine*);
// Test-only: runs on the preparation thread once its snapshot is taken.
extern std::function<void(Feed*)> checkpointPreparedHook;
bool checkpointDue(const Feed*,bool idle=false);
} }
#endif
