// Test double of the format-4 reader (flatsql/p4/p4_reader.h, CONTRACT.md
// §3.9): an in-memory store with the cursor semantics of §3.5-§3.8 (A18
// bound applied first, seq ranges clamped to visible-through, ANY-row lane
// filters, predicates, the matched tag), the slot's caps and cancel word,
// and p4_emit into a byte buffer. It is what the SQL surface is tested
// against until the engine lands; it never links the engine.
//
// Compiled only into flatsql_p4sql_fake_test (FLATSQL_P4SQL_FAKE): the
// engine's CMake globs this directory into its own test binaries, where the
// real reader defines these symbols.
#ifndef FLATSQL_P4SQL_FAKE_READER_H
#define FLATSQL_P4SQL_FAKE_READER_H

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "flatsql/p4/flatsql_p4.h"
#include "flatsql/p4/p4_reader.h"

namespace p4fake {

struct Tag {
    std::string provider, source, sourceUrl, batch, contentKeyId, producerPeer, producerPubkey;
    int64_t at = 0;
};

struct Rec {
    int64_t seq = 0;
    std::string cid;        // base32 text
    std::string producer, peer;
    int64_t ts = 0;
    bool hasEpoch = false;
    int64_t epoch = 0;
    std::vector<uint8_t> data;   // d, verbatim
    std::vector<uint8_t> sig;
    std::vector<Tag> tags;
    // What the type's col rules would extract (COL0 u64pos, COL1 str).
    bool hasCol0 = false;
    int64_t col0 = 0;
    bool hasCol1 = false;
    std::string col1;
};

struct Type {
    std::string name;
    uint8_t fid[4] = {0, 0, 0, 0};
    std::vector<uint8_t> bfbs;
    uint64_t bound = 10000;
    uint8_t epochProfile = 0;
    std::string rules;            // the spec's tag-4 rules text
    std::vector<Rec> recs;        // ascending seq
    int64_t visibleThrough = 0;   // 0 = every record
};

// What a cursor was opened with (tests check pushdown).
struct OpenedSpec {
    std::string type;
    std::string laneSource;
    int64_t seqAfter = 0, seqThrough = 0;
    uint8_t order = 0, hydrate = 0;
    uint64_t limit = 0, bound = 0;
    struct Pred {
        uint8_t field, op;
        int64_t i;
        std::string s;
    };
    std::vector<Pred> preds;
};

}  // namespace p4fake

struct P4Engine {
    std::map<std::string, p4fake::Type> types;   // by name
    void put(const std::string& type, p4fake::Rec r);   // keeps seq order
};

struct P4Lane {
    P4Engine* engine = nullptr;
    void* sqlState = nullptr;
    // The slot the lane is running.
    std::atomic<uint32_t> cancel{0};
    uint64_t maxRowsExamined = 0, maxBytesRead = 0, maxResultBytes = 0;
    uint64_t rowsExamined = 0, bytesRead = 0;
    std::vector<uint8_t> out;     // p4_emit
    uint64_t emits = 0;
    std::string err;              // p4_lane_set_error
    uint64_t heapCap = 64ull << 20;   // p4_lane_heap_cap (config tag 48)
    int64_t cancelAfterRows = -1; // tests: set the cancel word after this many rows examined
    std::vector<p4fake::OpenedSpec> opened;
    // p4_types / p4_sources storage (valid until the lane's next call).
    std::vector<P4TypeInfo> typeInfos;
    std::vector<std::string> srcStore;
    std::vector<const char*> srcPtrs;
    void resetSlot() {
        cancel = 0;
        maxRowsExamined = maxBytesRead = maxResultBytes = 0;
        rowsExamined = bytesRead = 0;
        out.clear();
        emits = 0;
        err.clear();
        cancelAfterRows = -1;
        opened.clear();
    }
};

#endif
