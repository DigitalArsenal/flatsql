// FlatSQL partition store: the quota planner and the space emergency (design
// §13 as amended by A13 and owner decision §22.4-3; T3).
//
// Quota is on on-disk bytes. When the store's usage (every partition's
// disk_bytes and every type log's, less files already retired and waiting
// only for readers) exceeds the cap, the planner, engine code on writer 0, evicts whole sealed
// segments in ARRIVAL order (oldest first, across partitions) down to the low
// water mark (0.85 of the cap): a TOMB_RANGE on each segment's owner
// tombstones its live PUTs except supersede-lane heads and control kinds,
// then a compaction removes them and reclamation returns the space. One wave
// at a time: the next pass looks again only once the wave is compacted.
//
// ENOSPC (A13): a commit that fails with NOSPACE puts the engine in a space
// emergency: record entries stop being consumed (producers see zero credits,
// nothing is acked, nothing is lost), the ballast file is released so the
// tombstone, SWAP, RETIRE and UNLINKED commits of an eviction wave have room,
// and the cap becomes 0.85 of the current usage. Once the wave is reclaimed
// the ballast is recreated and ingest resumes, with no operator action.
#ifndef FLATSQL_PS_QUOTA_H
#define FLATSQL_PS_QUOTA_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flatsql/ps/compaction.h"

namespace flatsql {
namespace ps {

class Engine;
struct Partition;
struct SegmentInfo;
struct LockHist;

// A sealed segment as its owner last published it (the planner never reads
// owner-thread state).
struct SegSummary {
    uint32_t pid = 0;
    uint32_t seg = 0;
    uint32_t lastSeg = 0;
    uint32_t cgen = 0;        // a compaction's output (coalesced ones keep the first id)
    int64_t minArrival = INT64_MAX;
    int64_t maxArrival = INT64_MIN;
    uint64_t firstPseq = 0;   // its rows [firstPseq, endPseq): pseqs survive
    uint64_t endPseq = 0;     // compaction and coalescing, segment ids do not
    uint64_t bytes = 0;       // the segment's files on disk
    bool empty = false;       // compacted with nothing left
};

struct QuotaStats {
    uint64_t passes = 0;            // planning rounds that issued an eviction wave
    uint64_t segmentsEvicted = 0;   // TOMB_RANGE commands issued
    uint64_t emergencies = 0;       // ENOSPC episodes
    uint64_t ballastReleases = 0;
    uint64_t ballastRestores = 0;
    bool emergency = false;
    uint64_t usageBytes = 0;        // disk_bytes, retired-and-waiting excluded
    uint64_t capBytes = 0;          // effective cap (0: none)
};

// ---- per-partition bookkeeping (B4: O(1) or O(log S) per commit) ---------------------
//
// A partition's disk ledger (§13) and the state derived from its segments,
// kept by its owner writer. Every cost here is O(1) or O(log S) per commit
// or per maintenance look, where S is the partition's segment count:
//   - the ledger: the stable files a partition names and their sizes, in an
//     open-addressing table keyed by (letter, seg, gen); the byte total is
//     kept on every change (Partition::ledgerBytes);
//   - a change feed: every ledger change of a segment's files, a seal, a
//     merge, a kill count and a SWAP's inputs mark the segments concerned;
//     the quota planner's summary and the compaction candidate index are
//     brought up to date from it, never by walking every segment (open
//     rebuilds them in one O(S) walk);
//   - the compaction candidate index (compaction.cpp's rules, unchanged):
//     sealed segments past the dead ratio by share, starts of adjacent small
//     pairs, and segments not yet surveyed after a restart, each ordered;
//   - the retired set's size as its RETIRE record encodes it, which the
//     reclaimer keeps under what one batch can carry (reclaim.cpp).
class PartitionLedger {
public:
    // The table. Keys follow the retire letters: 'f' by generation, 'd' 'r'
    // 'a' 'm' by segment, the others ('D' 'R' 'A' 'x') by both.
    void clear();
    uint64_t set(const RetireItem& it, bool* existed);           // returns the previous size
    bool drop(char letter, uint32_t seg, uint32_t gen, uint64_t* size);
    uint64_t size(char letter, uint32_t seg, uint32_t gen) const;
    size_t files() const { return n_; }
    uint64_t memoryBytes() const;
    uint64_t bytesWalk() const;                                   // the sizes summed by a walk (checks)

    // The change feed.
    void touch(uint32_t seg);
    void touchAll();

    // Derived state (quota.cpp and reclaim.cpp; owner thread only).
    std::vector<uint32_t> sumDirty, candDirty;   // segments changed since the last refresh
    bool sumAll = true, candAll = true;          // rebuild from the segment list instead
    uint64_t retiredBytes = 0;                   // sizes of the retired set, kept on every change
    struct Cand {
        double share = 0;                        // dead frame bytes / data bytes when `dead`
        bool dead = false, pair = false, survey = false;
    };
    std::unordered_map<uint32_t, Cand> cand;     // by segment id, members of the sets below
    std::set<std::pair<double, uint32_t>> deadQ; // (-share, seg): step 1, largest share first
    std::set<uint32_t> pairStarts;               // step 2: small segment followed by an adjacent small one
    std::set<uint32_t> survey;                   // step 3: compactable, never surveyed since warm
    double ratio = -1;                           // the thresholds the index holds
    uint64_t smallBytes = 0, maxOutBytes = 0;
    uint32_t maxInputs = 0;
    // The RETIRE valve (reclaim.cpp), owner-written, read by tests after stop.
    uint64_t retireForced = 0;       // items unlinked ahead of the reader gate and grace to fit a batch
    uint64_t retireOverflows = 0;    // batches refused because the set did not fit (should stay 0)
    uint64_t retireHolds = 0;        // times merges were held: the set's newest items are pinned by a compaction
    bool retireHold = false;         // merges wait (no new retirements) until the set can shrink again
    size_t retireEncPeak = 0;        // largest encoded set the reclaimer saw
    // gBookCheck: the split-partition run set's walk signature at a manifest
    // generation (stage1.cpp signs the set by the generation alone).
    bool prepSeen = false;
    uint32_t prepGen = 0;
    uint64_t prepWalkSig = 0;

private:
    size_t slotOf(uint8_t letter, uint32_t seg, uint32_t gen) const;
    void grow();
    bool dropTable(char letter, uint32_t seg, uint32_t gen, uint64_t* size);
    std::vector<RetireItem> slots_;              // letter 0: empty (backward-shift deletion)
    size_t n_ = 0;
    // gBookCheck: the ledger as the linear vector it replaced (same matching
    // rules), kept beside the table from the last clear() and compared on
    // every operation.
    bool shadowOn_ = false;
    std::vector<RetireItem> shadow_;
};

// Segment lookup over Partition::segs (ascending segment id): O(log S).
SegmentInfo* segFind(Partition* p, uint32_t seg);
const SegmentInfo* segFind(const Partition* p, uint32_t seg);
SegmentInfo* segCovering(Partition* p, uint32_t seg);  // the entry itself or the coalesced output covering it
const SegmentInfo* segCovering(const Partition* p, uint32_t seg);
SegmentInfo* segForPseq(Partition* p, uint64_t pseq);  // merged rows: firstPseq <= pseq < mergedEnd
// The entry of `seg`, created in order when absent (a created entry is fed
// to the change feed). Invalidates pointers into Partition::segs.
SegmentInfo* segInsert(Partition* p, uint32_t seg);
// A segment's entry changed outside the ledger: its dead counts (a kill or a
// survey), or it was replaced by a SWAP (every input is touched).
void bookTouch(Partition* p, uint32_t seg);
// Compaction candidate (compaction.cpp's selection rules) from the index.
bool bookPickCandidate(const Engine* e, Partition* p, uint32_t* seg, uint32_t* segEnd);
// The RETIRE set (A12): the items its record would carry now (an upper
// bound: consecutive manifest generations collapse), the most the reclaimer
// lets it hold, and the staging hook for a set that did not fit a batch.
size_t retireSetEncodedItems(const Partition* p);
size_t retireSetSoftCap(const Partition* p);
void bookRetireOverflow(Partition* p);
// Tests: nonzero cross-checks every lookup, summary refresh and candidate
// pick against a walk of the whole segment list (and the ledger against the
// linear vector it replaced), and aborts on a mismatch.
extern std::atomic<int> gBookCheck;
// Tests and benchmarks: nonzero times every summary refresh (per commit that
// publishes one) and every candidate look into these histograms.
extern std::atomic<int> gBookTime;
LockHist& bookSummaryHist();
LockHist& bookPickHist();

}  // namespace ps
}  // namespace flatsql

#endif
