// FlatSQL partition store: slab pool, per-partition rings, credits, acks
// (design §5.2, §6.2, §7, A24, A27).
//
// Memory model. One node buffer pool of 64 KiB slabs. A partition's ring is a
// byte stream over a page table of slabs; pages are mapped on demand by the
// partition's owning writer (never by a producer), so an idle partition holds
// zero slabs and producers never touch the global free list (A24).
//
// Producer protocol (Go router, or ps::Producer natively). Producers are
// serialized per partition (SPSC):
//   1. prodBusy.fetch_add (seq_cst); if reclaim is set, back off.
//   2. used = tail - head(acquire). With used > 0, need used + L <= cap; an
//      empty ring always admits one entry up to maxEntry (A27).
//   3. Every page of [tail, tail + L) must be mapped (tag == page + 1); if
//      not, publish wantPage and ring the doorbell; wait.
//   4. memcpy, tail.store(release), prodBusy.fetch_sub, doorbell.
// Consumer: the owner reads entries up to tail(acquire) but releases ring
// space (head) and acks (ackedRseq) only after the durable commit (§6.4).
#ifndef FLATSQL_PS_RING_H
#define FLATSQL_PS_RING_H

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace flatsql {
namespace ps {

constexpr uint32_t kNoSlab = 0xffffffffu;

class SlabPool {
public:
    SlabPool() = default;
    ~SlabPool();
    SlabPool(const SlabPool&) = delete;
    SlabPool& operator=(const SlabPool&) = delete;

    bool init(uint64_t bytes, uint32_t slabBytes);
    uint32_t alloc();                          // lock-free; kNoSlab when empty
    void free(uint32_t id);
    uint8_t* ptr(uint32_t id) const { return base_ + uint64_t(id) * slabBytes_; }
    uint32_t slabBytes() const { return slabBytes_; }
    uint32_t total() const { return nSlabs_; }
    uint32_t freeCount() const { return free_.load(std::memory_order_relaxed); }
    uint32_t inUse() const { return nSlabs_ - freeCount(); }
    uint32_t peakInUse() const { return peak_.load(std::memory_order_relaxed); }
    // Bytes of pool memory ever touched (committed): high-water slab id.
    uint64_t committedBytes() const {
        return uint64_t(highWater_.load(std::memory_order_relaxed)) * slabBytes_;
    }

private:
    uint8_t* base_ = nullptr;
    uint64_t bytes_ = 0;
    uint32_t slabBytes_ = 0;
    uint32_t nSlabs_ = 0;
    uint32_t nWords_ = 0;
    std::atomic<uint64_t>* bits_ = nullptr;   // 1 = allocated
    std::atomic<uint32_t> free_{0};
    std::atomic<uint32_t> peak_{0};
    std::atomic<uint32_t> highWater_{0};
    bool mmapped_ = false;
};

// ---- ring entries (§6.2) ------------------------------------------------------
enum EntryKind : uint16_t {
    kEntRecord = 1,
    kEntLicence = 2,
    kEntTombCid = 3,     // partition-level kill by cid (type owner fan-out, A14)
    kEntReconcile = 4,   // payload: u16 plen provider | u16 slen source | u16 klen keep
    kEntTxnBegin = 6,
    kEntTxnEnd = 7,
    kEntCtl = 8,
};

enum EntryFlag : uint16_t {
    kEntSealed = 0x0001,        // frame = plaintext frame, then u32 len + sealed bytes
    kEntCidPresent = 0x0002,
    kEntAckWanted = 0x0004,
    kEntCidOverPrefix = 0x0008, // the router hashed the size-prefixed frame
};

#pragma pack(push, 1)
struct EntryHeader {
    uint32_t entryLen;   // padded to 8, header included
    uint16_t kind;
    uint16_t flags;
    uint64_t rseq;
    int64_t arrivalMs;
    uint8_t cid[36];
    uint8_t rsv[4];
    uint32_t attrLen;
    uint32_t frameLen;
};
static_assert(sizeof(EntryHeader) == 72, "EntryHeader is 72 bytes");
#pragma pack(pop)

enum RingState : uint32_t {
    kRingActive = 0,
    kRingDraining = 1,
    kRingQuarantined = 2,
    kRingPaused = 3,     // I/O error backoff (ENOSPC): credits read as zero
};

struct RejectEntry {
    uint64_t rseq;
    int32_t code;
    uint32_t pad;
};

constexpr uint32_t kRejectSlots = 32;

// Owner word: {epoch u32 | writer u8 << 32 | state u8 << 40} (A26).
enum OwnerState : uint8_t { kOwnOwned = 1, kOwnHandoff = 2 };
inline uint64_t ownerWord(uint32_t epoch, uint8_t writer, uint8_t state) {
    return uint64_t(epoch) | (uint64_t(writer) << 32) | (uint64_t(state) << 40);
}
inline uint32_t ownerEpoch(uint64_t w) { return uint32_t(w); }
inline uint8_t ownerWriter(uint64_t w) { return uint8_t(w >> 32); }
inline uint8_t ownerState(uint64_t w) { return uint8_t(w >> 40); }

// Ring descriptor, 64-byte aligned, followed by the page table. Go reads the
// field offsets from flatsql_ps_layout.
struct alignas(64) RingDesc {
    // ---- producer line
    std::atomic<uint64_t> tail{0};
    std::atomic<uint64_t> prodBusy{0};
    std::atomic<uint32_t> wantPage{0};     // highest page index the producer needs + 1
    std::atomic<uint32_t> prodWaiting{0};
    std::atomic<uint64_t> nextRseq{1};     // producer-side sequence source
    std::atomic<uint64_t> rejectTail{0};   // producer consumed rejects up to here
    uint8_t padP[24];
    // ---- consumer line
    std::atomic<uint64_t> head{0};         // released (durable) position
    std::atomic<uint64_t> ackedRseq{0};
    std::atomic<uint32_t> ackGen{0};       // bumped on every ack release
    std::atomic<uint32_t> mapGen{0};       // bumped whenever pages are mapped
    std::atomic<uint64_t> rejectHead{0};
    std::atomic<uint32_t> state{kRingActive};
    std::atomic<uint32_t> reclaim{0};
    std::atomic<uint64_t> ownerWordV{0};
    uint8_t padC[16];
    // ---- configuration (immutable after registration)
    uint64_t cap = 0;
    uint64_t maxEntry = 0;
    uint32_t nSlots = 0;
    uint32_t pid = 0;
    uint32_t slabBytes = 0;
    std::atomic<uint32_t> handoffTo{0xff};
    std::atomic<uint32_t> mappedPages{0};  // slabs currently held
    uint32_t pad2 = 0;
    RejectEntry rejects[kRejectSlots];
    // page table follows: std::atomic<uint64_t> pages[nSlots]
    std::atomic<uint64_t>* pages() { return reinterpret_cast<std::atomic<uint64_t>*>(this + 1); }
    const std::atomic<uint64_t>* pages() const {
        return reinterpret_cast<const std::atomic<uint64_t>*>(this + 1);
    }
    uint64_t used() const {
        return tail.load(std::memory_order_acquire) - head.load(std::memory_order_acquire);
    }
};

size_t ringDescBytes(uint32_t nSlots);
RingDesc* ringCreate(uint32_t pid, uint64_t cap, uint64_t maxEntry, uint32_t slabBytes);
void ringDestroy(RingDesc* r);

inline uint64_t pageTag(uint64_t page, uint32_t slab) { return ((page + 1) << 32) | slab; }

// Copy len bytes of the ring stream starting at pos into dst (straddles pages).
void ringRead(const RingDesc* r, const SlabPool& pool, uint64_t pos, void* dst, size_t len);
void ringWrite(RingDesc* r, const SlabPool& pool, uint64_t pos, const void* src, size_t len);
// Pointer to pos when [pos, pos+len) lies inside one page, else nullptr.
const uint8_t* ringContiguous(const RingDesc* r, const SlabPool& pool, uint64_t pos, size_t len);

// Consumer side (owner writer only).
// Maps pages so that [tail, tail + want) is writable, within cap. Returns
// false when the pool is at its reserve. `reserveSlabs` is left free.
bool ringMapAhead(RingDesc* r, SlabPool& pool, uint32_t reserveSlabs, uint32_t aheadPages);
// Releases ring space up to newHead: frees fully consumed pages.
void ringRelease(RingDesc* r, SlabPool& pool, uint64_t newHead);
// Frees every mapped page beyond the producer when the ring is empty and no
// producer is active (idle partitions hold zero slabs). Returns slabs freed.
uint32_t ringReclaimIdle(RingDesc* r, SlabPool& pool);
bool ringPushReject(RingDesc* r, uint64_t rseq, int32_t code);

}  // namespace ps
}  // namespace flatsql

#endif
