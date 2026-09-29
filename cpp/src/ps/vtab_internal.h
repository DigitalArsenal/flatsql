// FlatSQL partition store: record vtab internals shared by vtab_partition.cpp
// (module, plans, partition-level row sources, columns) and vtab_fanout.cpp
// (the type-level merge, arrivals, the cid catalog, _current).
#ifndef FLATSQL_PS_VTAB_INTERNAL_H
#define FLATSQL_PS_VTAB_INTERNAL_H

#include <sqlite3.h>

#include <memory>
#include <string>
#include <vector>

#include "flatsql/ps/lane.h"
#include "flatsql/ps/snapshot.h"
#include "flatsql/ps/vtab.h"

namespace flatsql {
namespace ps {

struct TagView;

// Hidden columns follow the schema columns, in this order.
enum MetaCol : int {
    kMcPseq = 0,
    kMcCid,
    kMcCidBin,
    kMcEpoch,
    kMcArrival,
    kMcGseq,
    kMcProducer,
    kMcSource,
    kMcSourceName,
    kMcProvider,
    kMcBatch,
    kMcPeerId,
    kMcSignature,
    kMcData,
    kMcOffset,
    kMcLen,
    kMcKind,
    kMcRowid,
    kMcPid,
    kMcCount
};
extern const char* const kMetaColNames[kMcCount];


enum VtabKind : uint8_t { kVkPartition = 1, kVkType = 2, kVkAlias = 3, kVkCurrent = 4 };

enum Access : uint8_t {
    kAccFull = 0,   // partition: pseq order; type: per-partition pseq (unbounded)
    kAccPseq,       // partition: _pseq / _rowid EQ or range
    kAccCid,        // _cid / _cid_bin EQ
    kAccEpoch,      // EPOCH_CID (default order) with an optional _epoch range
    kAccGseq,       // type: arrivals order (_gseq / _rowid)
    kAccCol,        // COL(n) EQ
    kAccSource,     // SOURCE_EPOCH: source EQ, optional _epoch range
    kAccTag,        // TAG_PROVIDER / TAG_BATCH / TAG_PEER EQ
    kAccCurrent,    // <TYPE>_current
};

struct Plan {
    uint8_t access = kAccFull;
    bool desc = false;
    bool bounded = false;
    uint8_t tagKind = 0;     // kAccTag: 0 provider, 1 batch, 2 peer
    uint16_t col = 0;        // kAccCol: COL index
    int8_t aKey = -1;        // argv index of the EQ key
    int8_t aLo = -1, aHi = -1;
    uint8_t loOp = 0, hiOp = 0;   // SQLITE_INDEX_CONSTRAINT_GT/GE/LT/LE
    int8_t aProducer = -1;
    int8_t aSource = -1;     // post-filter source (alias or constraint)
    bool sourceFull = false; // the source argument is '<TYPE>@<name>'
    int8_t aLimit = -1;
    int8_t aOffset = -1;
    bool orderConsumed = false;
    // Tag conditions the vtab evaluates itself (omitted from SQLite's
    // re-check), argv indexes: _provider, _batch, _peer_id, _source_name,
    // _source ('<TYPE>@<name>'). See TagMatch.
    int8_t aTag[5] = {-1, -1, -1, -1, -1};
    std::string encode() const;
    bool decode(const char* s);
};

// A statement's tag conditions, with the legacy tag table's ANY-row
// semantics: a record matches when ONE of its live tag instances (its PUT's
// own tag or a RETAG, A2) satisfies every condition. An untagged record
// matches none (T2 deviation 8). _peer_id matches the tag's producer peer,
// the TAG_PEER postings.
struct TagMatch {
    bool any = false;
    bool hasProvider = false, hasBatch = false, hasPeer = false, hasSource = false;
    std::string provider, batch, peer, source;
    void setProvider(const std::string& v) { any = hasProvider = true; provider = v; }
    void setBatch(const std::string& v) { any = hasBatch = true; batch = v; }
    void setPeer(const std::string& v) { any = hasPeer = true; peer = v; }
    // false: a different source is already required (no row can match).
    bool setSource(const std::string& v) {
        if (hasSource && source != v) return false;
        any = hasSource = true;
        source = v;
        return true;
    }
    int count() const { return int(hasProvider) + int(hasBatch) + int(hasPeer) + int(hasSource); }
    bool matches(const TagView& t) const;
};

struct RecVtab : sqlite3_vtab {
    ReaderLane* lane = nullptr;
    VtabKind kind = kVkType;
    uint32_t pid = 0;                      // kVkPartition
    std::shared_ptr<const TypeInfo> type;  // columns, config
    std::string typeName;
    std::string source;                    // kVkAlias
    std::string tableName;
    int nSchemaCols = 0;
    // Schema column -> COL(n) index usable for EQ (-1 = none); enum columns
    // map the SQL integer to the enum name the index stores.
    std::vector<int> colIndex;
    std::vector<uint8_t> colIsEnum;
    std::vector<uint8_t> colIsU64;
    bool hasSupersede = false;
};

// One output row candidate.
struct CurRow {
    uint32_t pid = 0;
    PartSnap* snap = nullptr;
    RecRow row{};
    uint64_t gseq = 0;          // type level: the cid's gseq (0: unknown)
    std::string key;            // merge key (posting key bytes)
};

// A stream of rows in a defined order (keys comparable across streams of
// the same plan).
class RowSource {
public:
    virtual ~RowSource() = default;
    // 1 = a row in *out, 0 = end, < 0 = ReaderStatus.
    virtual int32_t next(CurRow* out) = 0;
};

// Shared row filters (partition or type level).
struct RowFilter {
    LaneStore* store = nullptr;
    StmtCtx* stmt = nullptr;
    PartSnap* snap = nullptr;
    TypeSnap* type = nullptr;     // type level: labels (FIRST only) and gseq
    uint64_t bound = 0;           // visibility: pseq_hi or V_p
    uint64_t gseqFloor = 0;       // sandbox window (A18): skip gseq < floor
    TagMatch tags;                // post-filter: a live tag instance matching every condition
    bool knownLive = false;       // liveness already established (arrivals joins)
    // Applies visibility, liveness, labels, tags; reads the PUT row.
    // Returns 1 (keep, *row filled), 0 (skip), < 0 error.
    int32_t accept(uint64_t pseq, CurRow* out);
    // Does PUT `put` have a live tag instance matching `m`?
    int32_t hasLiveTag(uint64_t put, const TagMatch& m, bool* yes);
};

// The plan's tag conditions (and an alias table's source) from its
// arguments. false: they contradict each other, no row can match.
bool buildTagMatch(const RecVtab* vt, const Plan& p, sqlite3_value** argv, TagMatch* out);

// Partition-level sources (vtab_partition.cpp).
std::unique_ptr<RowSource> makePostingRows(const RowFilter& f, uint16_t kind, std::string lo, bool hasLo,
                                           std::string hi, bool hasHi, bool desc, bool instances);
std::unique_ptr<RowSource> makePseqRows(const RowFilter& f, uint64_t lo, uint64_t hi, bool desc);
std::unique_ptr<RowSource> makeCidRowsPartition(const RowFilter& f, const uint8_t cid[kCidLen]);

// Type-level sources (vtab_fanout.cpp).
std::unique_ptr<RowSource> makeMerger(std::vector<std::unique_ptr<RowSource>> subs, bool desc, bool dedupeCid,
                                      size_t groupPrefixTrim);
std::unique_ptr<RowSource> makeConcat(std::vector<std::unique_ptr<RowSource>> subs);
std::unique_ptr<RowSource> makeArrivalRows(ReaderLane* lane, StmtCtx* stmt, TypeSnap* type, uint64_t lo, uint64_t hi,
                                           bool desc, const TagMatch& tags, uint32_t onlyPid);
std::unique_ptr<RowSource> makeCidRowsType(ReaderLane* lane, StmtCtx* stmt, TypeSnap* type, const TypeInfo* ti,
                                           const uint8_t cid[kCidLen], const TagMatch& tags, uint64_t gseqFloor);

// Sandbox window floor (A18): the gseq of the arrivals entry N from the tail.
int32_t windowFloor(ReaderLane* lane, StmtCtx* stmt, TypeSnap* type, uint64_t n, uint64_t* floor);

// Cooperative poll every 4 K steps (A21/A28).
inline int32_t pollEvery(LaneStore* st, uint32_t* counter) {
    if ((++*counter & 4095) != 0) return 0;
    return st->poll();
}

}  // namespace ps
}  // namespace flatsql

#endif
