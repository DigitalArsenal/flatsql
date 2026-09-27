// FlatSQL partition store: slab pool and rings (see ps/ring.h).
#include "flatsql/ps/ring.h"

#include <cstdlib>
#include <cstring>
#include <new>

#include "flatsql/ps/platform.h"

#if !defined(__wasm__)
#  include <sys/mman.h>
#endif

namespace flatsql {
namespace ps {

SlabPool::~SlabPool() {
    if (base_) {
#if !defined(__wasm__)
        if (mmapped_) munmap(base_, bytes_);
        else std::free(base_);
#else
        std::free(base_);
#endif
    }
    delete[] bits_;
}

bool SlabPool::init(uint64_t bytes, uint32_t slabBytes) {
    if (slabBytes == 0 || bytes < slabBytes) return false;
    slabBytes_ = slabBytes;
    nSlabs_ = uint32_t(bytes / slabBytes);
    bytes_ = uint64_t(nSlabs_) * slabBytes;
#if !defined(__wasm__)
    // Anonymous mapping: pages are committed by the kernel on first touch, so
    // committedBytes() (the slab high-water) is what the process pays.
    void* p = mmap(nullptr, bytes_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (p == MAP_FAILED) return false;
    base_ = static_cast<uint8_t*>(p);
    mmapped_ = true;
#else
    base_ = static_cast<uint8_t*>(std::aligned_alloc(64, bytes_));
    if (!base_) return false;
#endif
    nWords_ = (nSlabs_ + 63) / 64;
    bits_ = new std::atomic<uint64_t>[nWords_];
    for (uint32_t i = 0; i < nWords_; i++) bits_[i].store(0, std::memory_order_relaxed);
    // Bits past nSlabs are permanently "allocated".
    const uint32_t extra = nWords_ * 64 - nSlabs_;
    if (extra) {
        const uint64_t mask = ~0ull << (64 - extra);
        bits_[nWords_ - 1].store(mask, std::memory_order_relaxed);
    }
    free_.store(nSlabs_, std::memory_order_relaxed);
    return true;
}

uint32_t SlabPool::alloc() {
    // Lowest free slab first: keeps the committed high-water tight.
    for (uint32_t w = 0; w < nWords_; w++) {
        uint64_t v = bits_[w].load(std::memory_order_relaxed);
        while (v != ~0ull) {
            const int bit = __builtin_ctzll(~v);
            if (bits_[w].compare_exchange_weak(v, v | (1ull << bit), std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
                const uint32_t id = w * 64 + uint32_t(bit);
                const uint32_t f = free_.fetch_sub(1, std::memory_order_relaxed) - 1;
                const uint32_t used = nSlabs_ - f;
                uint32_t pk = peak_.load(std::memory_order_relaxed);
                while (used > pk && !peak_.compare_exchange_weak(pk, used)) {}
                uint32_t hw = highWater_.load(std::memory_order_relaxed);
                while (id + 1 > hw && !highWater_.compare_exchange_weak(hw, id + 1)) {}
                return id;
            }
        }
    }
    return kNoSlab;
}

void SlabPool::free(uint32_t id) {
    if (id >= nSlabs_) return;
    bits_[id / 64].fetch_and(~(1ull << (id % 64)), std::memory_order_acq_rel);
    free_.fetch_add(1, std::memory_order_relaxed);
}

size_t ringDescBytes(uint32_t nSlots) {
    return sizeof(RingDesc) + size_t(nSlots) * sizeof(std::atomic<uint64_t>);
}

RingDesc* ringCreate(uint32_t pid, uint64_t cap, uint64_t maxEntry, uint32_t slabBytes) {
    const uint32_t nSlots = uint32_t((cap + maxEntry) / slabBytes + 3);
    void* mem = nullptr;
    if (posix_memalign(&mem, 64, ringDescBytes(nSlots)) != 0) return nullptr;
    RingDesc* r = new (mem) RingDesc();
    r->cap = cap;
    r->maxEntry = maxEntry;
    r->nSlots = nSlots;
    r->pid = pid;
    r->slabBytes = slabBytes;
    std::memset(r->rejects, 0, sizeof(r->rejects));
    for (uint32_t i = 0; i < nSlots; i++) new (&r->pages()[i]) std::atomic<uint64_t>(0);
    return r;
}

void ringDestroy(RingDesc* r) {
    if (!r) return;
    r->~RingDesc();
    std::free(r);
}

namespace {
inline uint8_t* pagePtr(const RingDesc* r, const SlabPool& pool, uint64_t page) {
    const uint64_t v = r->pages()[page % r->nSlots].load(std::memory_order_acquire);
    return pool.ptr(uint32_t(v & 0xffffffffu));
}
}  // namespace

void ringRead(const RingDesc* r, const SlabPool& pool, uint64_t pos, void* dst, size_t len) {
    const uint32_t S = r->slabBytes;
    uint8_t* out = static_cast<uint8_t*>(dst);
    while (len) {
        const uint64_t page = pos / S;
        const uint32_t off = uint32_t(pos % S);
        const size_t n = (S - off) < len ? (S - off) : len;
        std::memcpy(out, pagePtr(r, pool, page) + off, n);
        out += n;
        pos += n;
        len -= n;
    }
}

void ringWrite(RingDesc* r, const SlabPool& pool, uint64_t pos, const void* src, size_t len) {
    const uint32_t S = r->slabBytes;
    const uint8_t* in = static_cast<const uint8_t*>(src);
    while (len) {
        const uint64_t page = pos / S;
        const uint32_t off = uint32_t(pos % S);
        const size_t n = (S - off) < len ? (S - off) : len;
        std::memcpy(pagePtr(r, pool, page) + off, in, n);
        in += n;
        pos += n;
        len -= n;
    }
}

const uint8_t* ringContiguous(const RingDesc* r, const SlabPool& pool, uint64_t pos, size_t len) {
    const uint32_t S = r->slabBytes;
    if ((pos % S) + len > S) return nullptr;
    return pagePtr(r, pool, pos / S) + (pos % S);
}

bool ringMapAhead(RingDesc* r, SlabPool& pool, uint32_t reserveSlabs, uint32_t aheadPages) {
    const uint32_t S = r->slabBytes;
    const uint64_t tail = r->tail.load(std::memory_order_acquire);
    const uint64_t head = r->head.load(std::memory_order_acquire);
    const uint64_t tailPage = tail / S;
    uint64_t last = tailPage + (aheadPages ? aheadPages - 1 : 0);
    const uint32_t want = r->wantPage.load(std::memory_order_acquire);
    if (want && uint64_t(want - 1) > last) last = want - 1;
    const uint64_t limitPage = (head + r->cap + r->maxEntry) / S;
    if (last > limitPage) last = limitPage;
    bool mapped = false;
    bool ok = true;
    for (uint64_t p = tailPage; p <= last; p++) {
        auto& slot = r->pages()[p % r->nSlots];
        const uint64_t v = slot.load(std::memory_order_acquire);
        if ((v >> 32) == p + 1) continue;
        if (v != 0) break;  // still holds an unreleased older page
        if (pool.freeCount() <= reserveSlabs) {
            ok = false;
            break;
        }
        const uint32_t s = pool.alloc();
        if (s == kNoSlab) {
            ok = false;
            break;
        }
        slot.store(pageTag(p, s), std::memory_order_release);
        r->mappedPages.fetch_add(1, std::memory_order_relaxed);
        mapped = true;
    }
    // A satisfied request is withdrawn (a newer one fails the CAS and stays).
    uint32_t w = want;
    if (ok && want && uint64_t(want - 1) <= last) r->wantPage.compare_exchange_strong(w, 0);
    if (mapped) {
        r->mapGen.fetch_add(1, std::memory_order_release);
        if (r->prodWaiting.load(std::memory_order_acquire)) wakeU32(&r->mapGen, -1);
    }
    return ok;
}

void ringRelease(RingDesc* r, SlabPool& pool, uint64_t newHead) {
    const uint32_t S = r->slabBytes;
    const uint64_t old = r->head.load(std::memory_order_relaxed);
    if (newHead <= old) return;
    for (uint64_t p = old / S; p < newHead / S; p++) {
        auto& slot = r->pages()[p % r->nSlots];
        const uint64_t v = slot.load(std::memory_order_acquire);
        if ((v >> 32) == p + 1) {
            pool.free(uint32_t(v & 0xffffffffu));
            slot.store(0, std::memory_order_release);
            r->mappedPages.fetch_sub(1, std::memory_order_relaxed);
        }
    }
    r->head.store(newHead, std::memory_order_release);
}

uint32_t ringReclaimIdle(RingDesc* r, SlabPool& pool) {
    if (r->mappedPages.load(std::memory_order_relaxed) == 0) return 0;
    const uint64_t head = r->head.load(std::memory_order_acquire);
    if (r->tail.load(std::memory_order_acquire) != head) return 0;
    r->reclaim.store(1, std::memory_order_seq_cst);
    if (r->prodBusy.load(std::memory_order_seq_cst) != 0 ||
        r->tail.load(std::memory_order_seq_cst) != head) {
        r->reclaim.store(0, std::memory_order_seq_cst);
        return 0;
    }
    uint32_t freed = 0;
    for (uint32_t i = 0; i < r->nSlots; i++) {
        const uint64_t v = r->pages()[i].load(std::memory_order_acquire);
        if (v) {
            pool.free(uint32_t(v & 0xffffffffu));
            r->pages()[i].store(0, std::memory_order_release);
            freed++;
        }
    }
    r->mappedPages.fetch_sub(freed, std::memory_order_relaxed);
    r->reclaim.store(0, std::memory_order_seq_cst);
    return freed;
}

bool ringPushReject(RingDesc* r, uint64_t rseq, int32_t code) {
    const uint64_t rh = r->rejectHead.load(std::memory_order_relaxed);
    const uint64_t rt = r->rejectTail.load(std::memory_order_acquire);
    if (rh - rt >= kRejectSlots) return false;
    r->rejects[rh % kRejectSlots].rseq = rseq;
    r->rejects[rh % kRejectSlots].code = code;
    r->rejectHead.store(rh + 1, std::memory_order_release);
    return true;
}

}  // namespace ps
}  // namespace flatsql
