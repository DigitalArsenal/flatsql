// FlatSQL partition store: the quota planner and the space emergency (design
// §13 as amended by A13 and owner decision §22.4-3; T3).
//
// Quota is on on-disk bytes. When the store's usage (every partition's
// disk_bytes, less files already retired and waiting only for readers)
// exceeds the cap, the planner, engine code on writer 0, evicts whole sealed
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

#include <cstdint>

namespace flatsql {
namespace ps {

// A sealed segment as its owner last published it (the planner never reads
// owner-thread state).
struct SegSummary {
    uint32_t pid = 0;
    uint32_t seg = 0;
    uint32_t lastSeg = 0;
    uint32_t cgen = 0;        // a compaction's output (coalesced ones keep the first id)
    int64_t minArrival = INT64_MAX;
    int64_t maxArrival = INT64_MIN;
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

}  // namespace ps
}  // namespace flatsql

#endif
