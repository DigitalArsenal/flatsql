// FlatSQL partition store: stage-1 preparation of a hot partition's ring
// entries by idle writers (design §12, A26; docs/PARTITION-STORE.md §32).
//
// A partition keeps one appender, its owner. When it is split, the owner
// publishes a window of upcoming ring entries (position, length, kind) in
// ordinal order, idle writers claim them one at a time and run the pure,
// per-entry part of staging: BFBS verification, the CID check (sha256), the
// frame CRC, the attribute check and key extraction. The owner takes the
// entries in ring order and does only what needs partition state: dedupe,
// supersede, pseq assignment, rows, postings, the commit.
//
// Each slot's state word is {ordinal << 3 | state}. It moves by CAS:
//   FREE(k)    published by the owner for ordinal k;
//   CLAIMED(k) a helper claimed it (claim counter, then this CAS);
//   WRITING(k) the helper is copying its result into the slot;
//   READY(k)   the result is complete;
//   OWNER(k)   the owner took the entry itself (from FREE or CLAIMED).
// A helper writes nothing but its claimed slot, and only between its
// CLAIMED -> WRITING and WRITING -> READY transitions; ordinals never
// repeat, so a helper that stalled past the owner (which stole the entry,
// committed and recycled the ring pages) fails its CAS and discards what it
// computed from recycled bytes. Helpers never write a ring slab.
#ifndef FLATSQL_PS_STAGE1_H
#define FLATSQL_PS_STAGE1_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "flatsql/ps/extract.h"
#include "flatsql/ps/format.h"
#include "flatsql/ps/index.h"
#include "flatsql/ps/io.h"

namespace flatsql {
namespace ps {

constexpr uint32_t kPrepSlots = 2048;      // window per split partition (power of two)
constexpr uint32_t kPrepKeyBytes = 480;    // extracted strings kept inline
constexpr uint32_t kPrepHintMax = 8;       // committed copies a dedupe hint carries

enum PrepSlotState : uint8_t {
    kPrepFree = 0,
    kPrepClaimed = 1,
    kPrepWriting = 2,
    kPrepReady = 3,
    kPrepOwner = 4,
};
inline uint64_t prepWord(uint64_t ord, uint8_t st) { return (ord << 3) | st; }
inline uint64_t prepOrd(uint64_t w) { return w >> 3; }
inline uint8_t prepSt(uint64_t w) { return uint8_t(w & 7); }

// What stage 1 learned about one record entry. valid = 0: nothing (not a
// record, a malformed entry, or a stale claim); the owner stages it itself.
struct PrepResult {
    uint8_t valid;
    uint8_t exOk;            // the extraction below is complete
    uint8_t rowFlags;        // kRowCidVerified
    uint8_t hasEpoch;
    int32_t reject;          // 0, or the frame / CID / attribute RejectCode
    uint32_t dataCrc;        // CRC32C of the stored frame bytes
    int8_t objectCol;
    uint8_t colPresent;      // bit i: cols[i] present
    uint8_t colU64;          // bit i: cols[i] is a u64
    uint8_t pad0;
    uint16_t idOff, idLen;   // supersede identity in keys
    uint16_t keyUsed;
    uint16_t pad1;
    int64_t epochMs;
    int64_t epochSec;
    uint8_t cid[kCidLen];
    uint8_t cidKey[kCidKeyLen];
    uint8_t hintOk;          // hint below is complete for run set hintVersion
    uint8_t hintN;
    uint8_t pad2;
    uint64_t hintVersion;
    uint64_t hintThrough;    // the hint also covers the L0 blocks through this pseq
    uint64_t hint[kPrepHintMax];  // PUT pseqs carrying this CID (those runs and blocks)
    uint64_t cidHash;        // hash64(cidKey): the writer's per-batch CID table
    uint64_t cidBloom;       // bloomHash(cidKey)
    uint64_t tagHash;        // tagTupleHash of the attribute's tag (0: none)
    uint64_t laneHash;       // laneHash of that tag
    uint64_t colU[kMaxCols];
    uint16_t colOff[kMaxCols];
    uint16_t colLen[kMaxCols];
    uint8_t keys[kPrepKeyBytes];
};
inline size_t prepResultBytes(const PrepResult& r) { return offsetof(PrepResult, keys) + r.keyUsed; }

struct alignas(64) PrepSlot {
    std::atomic<uint64_t> state{0};
    // Published with FREE (relaxed; the state word's release/acquire orders
    // them). Atomic because the owner may take a CLAIMED slot and republish
    // it for ordinal k + kPrepSlots while the helper still reads them; the
    // helper's CLAIMED -> WRITING step then fails and its result is dropped.
    std::atomic<uint64_t> pos{0};   // ring position
    std::atomic<uint32_t> len{0};   // entryLen
    std::atomic<uint16_t> kind{0};
    PrepResult r;
};

// The owner's L1 run set as helpers see it (dedupe hints): changes bump the
// version; runs are immutable files named by (seg, gen).
struct PrepRunDesc {
    uint32_t seg = 0;
    uint32_t gen = 0;
    uint64_t fileLen = 0;
};
// An unmerged L0 block's CID entries (immutable once the block committed).
struct PrepL0Pub {
    uint64_t firstPseq = 0, lastPseq = 0;
    uint32_t n = 0;
    uint8_t vlen = 0;
    uint32_t entriesBytes = 0;
    uint32_t bloomBytes = 0;   // the block's CID bloom follows the entries (0: none)
    std::shared_ptr<const std::vector<uint8_t>> entries;
};
struct PrepRunSet {
    uint64_t version = 0;              // of the L1 run set (bumps on every change)
    // Shared by every set published at the same run version (B4: a new L0
    // block republishes the set without copying the run list).
    std::shared_ptr<const std::vector<PrepRunDesc>> runs;
    std::vector<PrepL0Pub> l0;         // every unmerged L0 block when published
    uint64_t l0Through = 0;            // highest pseq those blocks cover (0: none covered)
};
// Helpers' own handles and accelerators of that run set (shared by helpers,
// never touched by the owner).
struct PrepHelperRun {
    uint32_t seg = 0, gen = 0;
    uint64_t fileLen = 0;
    FileRef file;
    std::shared_ptr<L1Run> run;
};
struct PrepHelperView {
    uint64_t version = 0;
    IoCtx* io = nullptr;
    std::vector<PrepHelperRun> runs;
    ~PrepHelperView();
};

struct PrepWindow {
    PrepSlot slots[kPrepSlots];
    alignas(64) std::atomic<uint64_t> published{0};  // ordinals below have a slot
    alignas(64) std::atomic<uint64_t> claim{0};      // next ordinal a helper claims
    alignas(64) std::atomic<uint32_t> helpers{0};    // helpers inside the claim loop
    // Owner only.
    bool on = false;         // the owner stages through the window
    uint64_t pubPos = 0;     // ring position of ordinal `published`
    uint64_t takeOrd = 0;    // next ordinal the owner takes
    uint64_t takePos = 0;    // its ring position
    uint64_t runSig = 0;     // signature of the published run set
    uint64_t runVersion = 0;
    uint64_t l0Sig = 0;      // signature of the published L0 blocks
    // Dedupe hints: the owner publishes its run set, helpers keep their view.
    std::shared_ptr<const PrepRunSet> runSet;        // std::atomic_load / atomic_store
    std::shared_ptr<const PrepHelperView> helperView;  // std::atomic_load / atomic_store
    std::mutex helperMu;                              // helpers rebuilding the view (try_lock)
    // Counters.
    std::atomic<uint64_t> hinted{0};    // results that carried a dedupe hint the owner used
    std::atomic<uint64_t> prepared{0};  // results a helper completed (READY)
    std::atomic<uint64_t> used{0};      // READY results the owner staged from
    std::atomic<uint64_t> stolen{0};    // entries the owner took before a helper finished
    std::atomic<uint64_t> wasted{0};    // helper results discarded (the owner had taken the entry)
};

}  // namespace ps
}  // namespace flatsql

#endif
