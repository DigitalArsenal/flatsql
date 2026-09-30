// FlatSQL partition store: reclamation and disk accounting (design §11
// step 4, §13, A9, A12; T3).
//
// Disk bytes (§13): a partition's disk_bytes is the exact size of the files
// it names: a ledger of the stable ones (sealed segments' d and m, r/a, c-*,
// L1 runs, manifests, and retired files until they are unlinked) plus the
// extents of the files still growing (h, l, the active d and m, zero-fill
// included). Open rebuilds the ledger from the head, the manifest and file
// sizes; the owner maintains it on every append, merge, seal, SWAP and unlink.
//
// Reclamation (A12): a MERGE_DONE or SWAP names the files it replaces in a
// RETIRE set that rides the same batch (the whole outstanding set, so the
// head can point at one record). A retired file is unlinked with
// UNLINK_IF_UNUSED only when the reader gate (the oldest running reader
// statement's start) is past the time its retirement became durable, checked
// twice a grace apart; BUSY means a handle is still open, retry later. The
// next batch carries UNLINKED and the shrunken set; disk_bytes drops then.
// Open unlinks every file of the persisted set (no reader of the previous
// incarnation can be served from the writer's state anyway).
//
// Meta-segment retirement (A9): a sealed m-<seg> whose batches are all merged
// is retired once its lane table and RETIRE set have been re-emitted into
// the active segment (both ride the retiring batch), and unlinked only after
// a DURABLE_CKPT head names a later first_live_m_seg.
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>

#include "internal.h"

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// Ledger (B4): an open-addressing table keyed by (letter, seg, gen), so every
// lookup, change and drop is O(1) whatever the partition's segment count.
// ---------------------------------------------------------------------------
namespace {
constexpr uint64_t kBusyRetryNs = 50000000ull;  // 50 ms

// The key a retire letter is matched by: manifests by generation, the
// segment-named files (d r a m) by segment, the rest by both.
inline void normKey(uint8_t letter, uint32_t* seg, uint32_t* gen) {
    if (letter == 'f') *seg = 0;
    else if (letter == 'd' || letter == 'r' || letter == 'a' || letter == 'm') *gen = 0;
}

inline uint64_t keyHash(uint8_t letter, uint32_t seg, uint32_t gen) {
    uint64_t x = (uint64_t(seg) << 32 | gen) ^ (uint64_t(letter) * 0x9e3779b97f4a7c15ull);
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ull;
    x ^= x >> 33;
    return x;
}

// Files whose size counts toward a segment's summary bytes and candidate state.
inline bool segLetter(uint8_t letter) {
    return letter == 'd' || letter == 'r' || letter == 'a' || letter == 'D' || letter == 'R' || letter == 'A' ||
           letter == 'x';
}

// The matching rule of the linear ledger the table replaced (gBookCheck's
// shadow applies it unchanged).
bool ledgerMatch(const RetireItem& a, char letter, uint32_t seg, uint32_t gen) {
    if (a.letter != uint8_t(letter)) return false;
    if (letter == 'f') return a.gen == gen;
    if (letter == 'd' || letter == 'r' || letter == 'a' || letter == 'm') return a.seg == seg;
    return a.seg == seg && a.gen == gen;
}

[[noreturn]] void ledgerMismatch(const char* what, uint8_t letter, uint32_t seg, uint32_t gen) {
    std::fprintf(stderr, "flatsql ps bookkeeping check failed: ledger %s ('%c', seg %u, gen %u)\n", what,
                 letter ? char(letter) : '?', seg, gen);
    std::abort();
}
}  // namespace

std::atomic<int> gBookCheck{0};

void PartitionLedger::clear() {
    slots_.clear();
    slots_.shrink_to_fit();
    n_ = 0;
    shadow_.clear();
    shadowOn_ = gBookCheck.load(std::memory_order_relaxed) != 0;
    touchAll();
}

size_t PartitionLedger::slotOf(uint8_t letter, uint32_t seg, uint32_t gen) const {
    const size_t mask = slots_.size() - 1;
    size_t i = size_t(keyHash(letter, seg, gen)) & mask;
    while (slots_[i].letter != 0) {
        const RetireItem& e = slots_[i];
        if (e.letter == letter && e.seg == seg && e.gen == gen) return i;
        i = (i + 1) & mask;
    }
    return i;  // the empty slot the key would take
}

void PartitionLedger::grow() {
    std::vector<RetireItem> old;
    old.swap(slots_);
    slots_.assign(old.empty() ? 16 : old.size() * 2, RetireItem{});
    for (const RetireItem& e : old)
        if (e.letter) slots_[slotOf(e.letter, e.seg, e.gen)] = e;
}

uint64_t PartitionLedger::set(const RetireItem& it, bool* existed) {
    // An empty ledger has an empty shadow: checking starts here too.
    if (!shadowOn_ && n_ == 0 && gBookCheck.load(std::memory_order_relaxed)) shadowOn_ = true;
    RetireItem k = it;
    normKey(k.letter, &k.seg, &k.gen);
    if ((n_ + 1) * 10 > slots_.size() * 7) grow();  // load factor 0.7
    const size_t i = slotOf(k.letter, k.seg, k.gen);
    RetireItem& e = slots_[i];
    uint64_t prev = 0;
    bool had = false;
    if (e.letter) {
        prev = e.size;
        had = true;
        e.size = k.size;
    } else {
        e = k;
        n_++;
    }
    if (existed) *existed = had;
    if (shadowOn_) {
        // The linear ledger's ledgerSet, then the answers compared.
        bool found = false;
        uint64_t sprev = 0;
        for (auto& x : shadow_)
            if (ledgerMatch(x, char(it.letter), it.seg, it.gen)) {
                sprev = x.size;
                x.size = it.size;
                found = true;
                break;
            }
        if (!found) shadow_.push_back(it);
        if (found != had || sprev != prev || shadow_.size() != n_) ledgerMismatch("set", it.letter, it.seg, it.gen);
    }
    return prev;
}

bool PartitionLedger::drop(char letter, uint32_t seg, uint32_t gen, uint64_t* size) {
    if (shadowOn_) {
        // The linear ledger's ledgerDrop, compared with the table's answer.
        bool found = false;
        uint64_t ssize = 0;
        for (size_t j = 0; j < shadow_.size(); j++)
            if (ledgerMatch(shadow_[j], letter, seg, gen)) {
                ssize = shadow_[j].size;
                shadow_[j] = shadow_.back();
                shadow_.pop_back();
                found = true;
                break;
            }
        uint64_t tsize = 0;
        const bool had = dropTable(letter, seg, gen, &tsize);
        if (found != had || ssize != tsize || shadow_.size() != n_) ledgerMismatch("drop", uint8_t(letter), seg, gen);
        if (had && size) *size = tsize;
        return had;
    }
    return dropTable(letter, seg, gen, size);
}

bool PartitionLedger::dropTable(char letter, uint32_t seg, uint32_t gen, uint64_t* size) {
    const uint8_t l = uint8_t(letter);
    normKey(l, &seg, &gen);
    if (!n_) return false;
    size_t i = slotOf(l, seg, gen);
    if (!slots_[i].letter) return false;
    if (size) *size = slots_[i].size;
    // Backward-shift deletion: no tombstones, probe chains stay short.
    const size_t mask = slots_.size() - 1;
    for (size_t j = i;;) {
        j = (j + 1) & mask;
        if (!slots_[j].letter) break;
        const size_t home = size_t(keyHash(slots_[j].letter, slots_[j].seg, slots_[j].gen)) & mask;
        const bool stays = i <= j ? (i < home && home <= j) : (i < home || home <= j);
        if (stays) continue;
        slots_[i] = slots_[j];
        i = j;
    }
    slots_[i] = RetireItem{};
    n_--;
    return true;
}

uint64_t PartitionLedger::size(char letter, uint32_t seg, uint32_t gen) const {
    uint64_t v = 0;
    if (n_) {
        uint8_t l = uint8_t(letter);
        uint32_t s2 = seg, g2 = gen;
        normKey(l, &s2, &g2);
        const RetireItem& e = slots_[slotOf(l, s2, g2)];
        v = e.letter ? e.size : 0;
    }
    if (shadowOn_) {
        uint64_t ref = 0;
        for (const auto& x : shadow_)
            if (ledgerMatch(x, letter, seg, gen)) {
                ref = x.size;
                break;
            }
        if (ref != v) ledgerMismatch("size", uint8_t(letter), seg, gen);
    }
    return v;
}

uint64_t PartitionLedger::memoryBytes() const {
    return slots_.capacity() * sizeof(RetireItem) + (sumDirty.capacity() + candDirty.capacity()) * 4 +
           cand.size() * 48 + (deadQ.size() + pairStarts.size() + survey.size()) * 48;
}

void PartitionLedger::touch(uint32_t seg) {
    // Past kFeedCap pending changes (a feed nobody drains, e.g. with
    // automatic compaction off) the consumer rebuilds in one walk instead.
    constexpr size_t kFeedCap = 65536;
    if (!sumAll) {
        if (sumDirty.size() < kFeedCap) sumDirty.push_back(seg);
        else sumAll = true, sumDirty.clear();
    }
    if (!candAll) {
        if (candDirty.size() < kFeedCap) candDirty.push_back(seg);
        else candAll = true, candDirty.clear();
    }
}

uint64_t PartitionLedger::bytesWalk() const {
    uint64_t b = 0;
    for (const RetireItem& e : slots_)
        if (e.letter) b += e.size;
    return b;
}

void PartitionLedger::touchAll() {
    sumAll = candAll = true;
    sumDirty.clear();
    candDirty.clear();
}

void ledgerSet(Partition* p, const RetireItem& it) {
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // a maintenance event (merge, seal, SWAP), never per record
    const uint64_t prev = p->ledger.set(it, nullptr);
    if (segLetter(it.letter)) p->ledger.touch(it.seg);
    tHotPathDepth = saved;
    p->ledgerBytes += uint64_t(it.size) - prev;
}

void ledgerDrop(Partition* p, char letter, uint32_t seg, uint32_t gen) {
    uint64_t size = 0;
    if (!p->ledger.drop(letter, seg, gen, &size)) return;
    p->ledgerBytes -= size;
    if (segLetter(uint8_t(letter))) {
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;
        p->ledger.touch(seg);
        tHotPathDepth = saved;
    }
}

uint64_t ledgerSize(const Partition* p, char letter, uint32_t seg, uint32_t gen) {
    return p->ledger.size(letter, seg, gen);
}

// ---------------------------------------------------------------------------
// Segment lookup (B4): Partition::segs ascends by segment id, so an entry is
// found by binary search. Entries without merged rows yet (firstPseq 0: m
// placeholders, dead-count holders) sit among the others; a pseq search
// steps over them.
// ---------------------------------------------------------------------------
namespace {
template <typename P>
auto segLowerBound(P* p, uint32_t seg) -> decltype(p->segs.begin()) {
    return std::lower_bound(p->segs.begin(), p->segs.end(), seg,
                            [](const SegmentInfo& s, uint32_t v) { return s.seg < v; });
}

template <typename P, typename S>
S* segCoveringT(P* p, uint32_t seg) {
    auto it = std::upper_bound(p->segs.begin(), p->segs.end(), seg,
                               [](uint32_t v, const SegmentInfo& s) { return v < s.seg; });
    if (it == p->segs.begin()) return nullptr;
    --it;
    const uint32_t last = it->lastSeg ? it->lastSeg : it->seg;
    return seg <= last ? &*it : nullptr;
}

[[noreturn]] void bookMismatch(const char* what, uint32_t pid, uint64_t key) {
    std::fprintf(stderr, "flatsql ps bookkeeping check failed: %s (pid %u, key %llu)\n", what, pid,
                 (unsigned long long)key);
    std::abort();
}
}  // namespace

SegmentInfo* segFind(Partition* p, uint32_t seg) {
    auto it = segLowerBound(p, seg);
    SegmentInfo* r = it != p->segs.end() && it->seg == seg ? &*it : nullptr;
    if (gBookCheck.load(std::memory_order_relaxed)) {
        SegmentInfo* ref = nullptr;
        for (auto& s : p->segs)
            if (s.seg == seg) {
                ref = &s;
                break;
            }
        for (size_t i = 1; i < p->segs.size(); i++)
            if (p->segs[i - 1].seg >= p->segs[i].seg) bookMismatch("segs not ascending", p->pid, p->segs[i].seg);
        if (ref != r) bookMismatch("segFind", p->pid, seg);
    }
    return r;
}

const SegmentInfo* segFind(const Partition* p, uint32_t seg) {
    return segFind(const_cast<Partition*>(p), seg);
}

SegmentInfo* segCovering(Partition* p, uint32_t seg) {
    SegmentInfo* r = segCoveringT<Partition, SegmentInfo>(p, seg);
    if (gBookCheck.load(std::memory_order_relaxed)) {
        SegmentInfo* ref = nullptr;
        for (auto& s : p->segs)
            if (s.seg <= seg && seg <= (s.lastSeg ? s.lastSeg : s.seg)) {
                ref = &s;
                break;
            }
        if (ref != r) bookMismatch("segCovering", p->pid, seg);
    }
    return r;
}

const SegmentInfo* segCovering(const Partition* p, uint32_t seg) {
    return segCovering(const_cast<Partition*>(p), seg);
}

SegmentInfo* segForPseq(Partition* p, uint64_t pseq) {
    // The last entry with merged rows starting at or before pseq: firstPseq
    // ascends with the segment id over entries that have one.
    SegmentInfo* r = nullptr;
    size_t lo = 0, hi = p->segs.size();  // candidates in [lo, hi)
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        size_t m = mid;
        while (m > lo && !p->segs[m].firstPseq) m--;  // step over entries without rows
        const SegmentInfo& s = p->segs[m];
        if (!s.firstPseq) {  // nothing with rows in [lo, mid]
            lo = mid + 1;
            continue;
        }
        if (s.firstPseq <= pseq) {
            r = &p->segs[m];
            lo = mid + 1;
        } else {
            hi = m;
        }
    }
    if (r && !(pseq >= r->firstPseq && pseq < r->mergedEnd)) {
        // An entry sharing its first pseq with an earlier one holds no merged
        // rows: the earlier one may.
        SegmentInfo* q = nullptr;
        for (size_t i = size_t(r - p->segs.data()); i-- > 0;) {
            if (!p->segs[i].firstPseq) continue;
            if (p->segs[i].firstPseq == r->firstPseq) q = &p->segs[i];
            break;
        }
        r = q && pseq < q->mergedEnd ? q : nullptr;
    }
    if (gBookCheck.load(std::memory_order_relaxed)) {
        SegmentInfo* ref = nullptr;
        for (auto& s : p->segs)
            if (pseq >= s.firstPseq && pseq < s.mergedEnd) {
                ref = &s;
                break;
            }
        if (ref != r) bookMismatch("segForPseq", p->pid, pseq);
    }
    return r;
}

SegmentInfo* segInsert(Partition* p, uint32_t seg) {
    auto it = segLowerBound(p, seg);
    if (it != p->segs.end() && it->seg == seg) return &*it;
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // a new segment entry: a maintenance event
    it = p->segs.emplace(it);
    it->seg = seg;
    p->ledger.touch(seg);
    tHotPathDepth = saved;
    return &*it;
}

void bookTouch(Partition* p, uint32_t seg) {
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    p->ledger.touch(seg);
    tHotPathDepth = saved;
}

uint64_t partitionDiskBytes(const Partition* p) {
    return p->ledgerBytes + p->hExtent + p->lDisk + p->dExtent + p->mExtent;
}

void partitionPublishDisk(Partition* p) {
    const uint64_t b = partitionDiskBytes(p);
    p->counters.diskBytes = b;
    p->diskBytesPub.store(b, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// The RETIRE set record
// ---------------------------------------------------------------------------
namespace {
// Consecutive manifest generations collapse into one 'F' item {gen = first,
// seg = count}: a busy partition retires one manifest per merge.
void appendItems(std::vector<RetireItem>* out, const RetireItem* it, size_t n) {
    for (size_t i = 0; i < n; i++) out->push_back(it[i]);
}
}  // namespace

size_t retireSetEncode(const Partition* p, const std::vector<const std::vector<RetireItem>*>& extra,
                       const RetireSetHeader& hdr, uint8_t* out, size_t cap) {
    // Gather (no allocation beyond this maintenance-time vector).
    std::vector<RetireItem> all;
    all.reserve(p->retired.size() + 16);
    for (const auto& r : p->retired) all.push_back(r.it);
    for (const auto* v : extra)
        if (v) appendItems(&all, v->data(), v->size());
    std::vector<uint32_t> mfs;
    std::vector<RetireItem> rest;
    for (const auto& it : all) {
        if (it.letter == 'f') mfs.push_back(it.gen);
        else rest.push_back(it);
    }
    std::sort(mfs.begin(), mfs.end());
    mfs.erase(std::unique(mfs.begin(), mfs.end()), mfs.end());
    for (size_t i = 0; i < mfs.size();) {
        size_t j = i + 1;
        while (j < mfs.size() && mfs[j] == mfs[j - 1] + 1) j++;
        RetireItem f{};
        f.letter = 'F';
        f.gen = mfs[i];
        f.seg = uint32_t(j - i);
        rest.push_back(f);
        i = j;
    }
    const size_t bytes = sizeof(RetireSetHeader) + rest.size() * sizeof(RetireItem);
    if (bytes > cap) return 0;
    RetireSetHeader h = hdr;
    h.n = uint32_t(rest.size());
    std::memcpy(out, &h, sizeof(h));
    if (!rest.empty()) std::memcpy(out + sizeof(h), rest.data(), rest.size() * sizeof(RetireItem));
    return bytes;
}

bool retireSetDecode(const uint8_t* body, size_t len, RetireSetHeader* h, std::vector<RetireItem>* items) {
    items->clear();
    if (len < sizeof(RetireSetHeader)) return false;
    std::memcpy(h, body, sizeof(*h));
    if (sizeof(RetireSetHeader) + size_t(h->n) * sizeof(RetireItem) > len) return false;
    for (uint32_t i = 0; i < h->n; i++) {
        RetireItem it;
        std::memcpy(&it, body + sizeof(RetireSetHeader) + size_t(i) * sizeof(it), sizeof(it));
        if (it.letter == 'F') {
            if (it.seg > (1u << 24)) return false;
            for (uint32_t g = 0; g < it.seg; g++) {
                RetireItem f{};
                f.letter = 'f';
                f.gen = it.gen + g;
                items->push_back(f);
            }
        } else {
            items->push_back(it);
        }
    }
    return true;
}

void partitionRetireCommitted(Partition* p, const std::vector<RetireItem>& items, uint64_t nowNs) {
    for (const auto& it : items) {
        RetiredFile r;
        r.it = it;
        r.retireNs = nowNs;
        p->retired.push_back(r);
        p->ledger.retiredBytes += it.size;
    }
}

void partitionUnlinkedCommitted(Partition* p, uint32_t n) {
    n = std::min<uint32_t>(n, uint32_t(p->unlinked.size()));
    for (uint32_t i = 0; i < n; i++) {
        const RetireItem& it = p->unlinked[i];
        ledgerDrop(p, char(it.letter), it.seg, it.gen);
    }
    p->unlinked.erase(p->unlinked.begin(), p->unlinked.begin() + n);
}

// ---------------------------------------------------------------------------
// The RETIRE valve (A12)
//
// The whole outstanding set rides ONE ctl record of the batch that changes it
// (u16 length) in the batch's 64 KiB ctl buffer, beside an UNLINKED record and
// the lane table the batch may re-emit. A set that does not fit fails every
// batch of the partition (partition_log.cpp) until it shrinks, and waiting
// for the grace to shrink it stalled ingest for the whole grace whenever
// retirements outpaced it (a busy partition retires a manifest and folded
// runs per merge, a coalescing SWAP a dozen files per input). So the set is
// held under a soft cap well inside what a batch carries: past it the oldest
// items go without waiting for the reader gate or the grace (a statement
// older than them gets the retryable SNAPSHOT_GONE; an open handle still
// makes the unlink BUSY). Crash safety never needed the grace: the batch that
// retired a file is durable before its retirement counts, and open replays it.
// ---------------------------------------------------------------------------
namespace {
constexpr size_t kCtlBufBytes = 65536;  // StageScratch::ctl
constexpr size_t kUnlinkedMaxBytes = 4 + sizeof(RetireSetHeader) + 1024 * sizeof(RetireItem);
constexpr size_t kOtherCtlBytes = 1024;  // INTENT, MERGE_DONE, SWAP, SEAL records of the same batch
constexpr size_t kBatchAddsItems = 512;  // one SWAP's or MERGE_DONE's additions, with room to spare
constexpr size_t kRetireSoftItems = 2048;

size_t retireSetHardCap(const Partition* p) {
    const size_t laneRoom = 64 + (p->lanes.size() + 1) * sizeof(LaneCounter);  // as staging reserves it
    const size_t fixed = kUnlinkedMaxBytes + kOtherCtlBytes + laneRoom + 4 + sizeof(RetireSetHeader);
    const size_t room = fixed < kCtlBufBytes ? (kCtlBufBytes - fixed) / sizeof(RetireItem) : 0;
    return std::min(room, (size_t(65535) - sizeof(RetireSetHeader)) / sizeof(RetireItem));
}
}  // namespace

size_t retireSetEncodedItems(const Partition* p) {
    // Consecutive manifest generations collapse into one item (retireSetEncode):
    // counted here in retirement order, which sorting can only merge further.
    size_t n = p->retiring.size();
    bool haveF = false;
    uint32_t lastF = 0;
    for (const auto& r : p->retired) {
        if (r.it.letter != 'f') {
            n++;
            continue;
        }
        if (!haveF || r.it.gen != lastF + 1) n++;
        haveF = true;
        lastF = r.it.gen;
    }
    return n;
}

size_t retireSetSoftCap(const Partition* p) {
    const size_t hard = retireSetHardCap(p);
    return hard > kBatchAddsItems ? std::min(kRetireSoftItems, hard - kBatchAddsItems) : 0;
}

void bookRetireOverflow(Partition* p) { p->ledger.retireOverflows++; }

// ---------------------------------------------------------------------------
// Unlinking (maintenance)
// ---------------------------------------------------------------------------
int32_t partitionReclaimStep(Writer* w, Partition* p) {
    if (p->retired.empty() || p->quarantined) {
        p->ledger.retireHold = false;
        return 0;
    }
    Engine* e = w->engine();
    const EngineConfig& cfg = e->config();
    const uint64_t now = monoNs();
    const uint64_t gate = e->readerGateNs();
    const uint64_t jsafe = cfg.commitJournal ? e->journalSafeNs() : UINT64_MAX;
    const uint64_t grace = cfg.reclaimGraceMs * 1000000ull;
    // The valve: the oldest items past the soft cap go now (see above).
    const size_t enc = retireSetEncodedItems(p);
    const size_t soft = retireSetSoftCap(p);
    if (enc > p->ledger.retireEncPeak) p->ledger.retireEncPeak = enc;
    size_t shed = enc > soft ? enc - soft : 0;
    bool pinned = false;
    const uint64_t pin = partitionCompactPinNs(p);
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    uint32_t done = 0;
    size_t i = 0;
    int32_t out = 0;
    const size_t n0 = p->retired.size();
    // Unlinked items leave the set in one compaction pass after the loop
    // (an erase per item moved the whole set each time).
    std::vector<uint8_t> gone;
    while (i < n0 && done < cfg.reclaimBatch) {
        RetiredFile& r = p->retired[i];
        if (r.retireNs == kRetirePendingNs) break;  // no head has stopped naming it yet (FIFO)
        if (now < r.retryNs) {
            i++;
            continue;
        }
        const bool forced = shed > 0;
        if (!forced) {
            if (gate <= r.retireNs) break;  // FIFO: later items retired later
            // The first passing check of every item past the gate is taken
            // now: one pass starts the grace of all of them, not one per pass.
            if (!r.firstOkNs) r.firstOkNs = now;
            if (now - r.firstOkNs < grace) {
                i++;
                continue;
            }
        }
        if (r.it.letter == 'm' && p->lastDurableHeadNs <= r.retireNs) {
            // A9: a meta segment goes only after a DURABLE_CKPT head names a
            // later first_live_m_seg. A quiet partition gets one from the
            // idle checkpoint, and the items behind do not wait for it (they
            // would block the UNLINKED batches that bring such heads).
            if (!p->metaSinceCkpt) p->metaSinceCkpt = 1;
            i++;
            continue;
        }
        if (r.retireNs >= pin) {  // a compaction in flight may read it
            pinned = forced;
            break;
        }
        if (jsafe <= r.retireNs) break;
        // The owner's own read handles go first (UNLINK_IF_UNUSED).
        if (r.it.letter == 'm') {
            if (SegmentInfo* si = segFind(p, r.it.seg)) w->io().close(&si->m);
        }
        PathBuf path;
        retirePath(&path, w->eng_root(), p->pid, r.it);
        const int32_t rc = path.len ? w->io().unlink(path.c_str(), path.len, true) : 0;
        if (rc == FLATSQL_IO_ERR_BUSY) {
            // A handle is open somewhere (an idle reader closes its own within
            // seconds, the type owner its partition meta handles within 100
            // ms): later items go ahead of it, and it waits a little.
            e->cUnlinkBusy.fetch_add(1, std::memory_order_relaxed);
            r.retryNs = now + kBusyRetryNs;
            i++;
            continue;
        }
        if (rc < 0 && rc != FLATSQL_IO_ERR_NOENT) {
            out = rc;
            break;
        }
        p->unlinked.push_back(r.it);
        if (gone.empty()) gone.assign(n0, 0);
        gone[i] = 1;
        p->ledger.retiredBytes -= std::min<uint64_t>(p->ledger.retiredBytes, r.it.size);
        e->cUnlinked.fetch_add(1, std::memory_order_relaxed);
        if (forced) {
            shed--;
            p->ledger.retireForced++;
        }
        done++;
        i++;
    }
    // A compaction in flight pins what was retired after it was planned: if
    // the set still grows toward what a batch carries, merges (the steady
    // source of retirements) wait until the SWAP or abort releases the pin.
    // Records keep flowing into L0 meanwhile, as when a merge is slow.
    const size_t left = enc > done ? enc - done : 0;
    const bool hold = pinned && left > soft + (retireSetHardCap(p) - std::min(soft, retireSetHardCap(p))) / 2;
    if (hold && !p->ledger.retireHold) p->ledger.retireHolds++;
    p->ledger.retireHold = hold;
    if (done) {
        size_t k = 0;
        for (size_t j = 0; j < n0; j++)
            if (!gone[j]) p->retired[k++] = p->retired[j];
        p->retired.resize(k);
    }
    tHotPathDepth = saved;
    if (done) {
        p->retireDirty = true;
        w->ring();
    }
    return out < 0 ? out : int32_t(done);
}

// ---------------------------------------------------------------------------
// A9: retire merged sealed meta segments
// ---------------------------------------------------------------------------
int32_t partitionRetireMetaStep(Writer* w, Partition* p) {
    Engine* e = w->engine();
    if (!e->config().retireMeta || p->quarantined) return 0;
    if (p->firstLiveMSeg >= p->mSeg) return 0;
    if (p->retireDirty || !p->retiring.empty() || p->nPendingCtl) return 0;  // one change per batch
    // firstLiveMSeg moves when the batch carrying the retirement commits
    // (heads written before it must still name the segment).
    const uint32_t s = p->firstLiveMSeg;
    for (uint32_t i = 0; i < p->nL0; i++)
        if (p->l0[i].mSeg <= s) return 0;  // unmerged batches still live in it
    if (p->mergePhase != kMergeIdle && p->mplan.seg <= s) return 0;
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    if (SegmentInfo* si = segFind(p, s)) w->io().close(&si->m);
    const uint64_t size = ledgerSize(p, 'm', s, 0);
    p->retiring.push_back(retireItem('m', s, 0, size));
    p->forceLaneCkpt = true;  // the lane table moves to a live segment with it
    p->retireDirty = true;
    tHotPathDepth = saved;
    e->cMetaRetired.fetch_add(1, std::memory_order_relaxed);
    w->ring();
    return 1;
}

// ---------------------------------------------------------------------------
// Type logs (T3): meta segments (A9), catalog runs and manifests (A12)
// ---------------------------------------------------------------------------
namespace {
constexpr int32_t kTypeOpenRW = FLATSQL_IO_READ | FLATSQL_IO_WRITE;

int64_t fileSize(IoCtx* io, const PathBuf& path) {
    FileRef f;
    if (io->open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::TypeMeta, &f) < 0) return -1;
    const int64_t n = io->size(f);
    io->close(&f);
    return n;
}

// Both head slots are rewritten and synced: no head open could pick names
// what is being retired.
int32_t typeTwoDurableHeads(Writer* w, TypeOwner* t) {
    for (int i = 0; i < 2; i++) {
        int32_t rc = typeWriteHead(w, t, true);
        if (rc >= 0) rc = w->io().sync(t->h);
        if (rc < 0) return rc;
    }
    t->lastCkptNs = monoNs();
    return 0;
}

// The type meta log starts a new segment past typeMetaSegBytes. The sealed
// one is cut at the end of its last batch first: open follows the chain from
// a segment's end into the next one, and a rolled-back batch left past the
// end must never pass for the chain's continuation.
int32_t typeRotateMeta(Writer* w, TypeOwner* t) {
    int32_t rc = typeWarm(w, t);
    if (rc < 0) return rc;
    rc = w->io().truncate(t->m, t->mEnd);
    if (rc >= 0) rc = w->io().sync(t->m);
    if (rc < 0) return rc;
    PathBuf np;
    pathTypeSeg(&np, w->eng_root(), t->fid, 'm', t->mSeg + 1, "fsl");
    FileRef nm;
    rc = w->io().open(np.c_str(), np.len,
                      kTypeOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::TypeMeta, &nm);
    if (rc < 0) return rc;
    w->io().close(&t->mPrev);
    t->mPrev = t->m;  // unmerged L0 blocks are read from it until merged
    t->mPrevSeg = t->mSeg;
    t->m = nm;
    t->mSealedBytes += t->mEnd;  // cut at its last batch
    t->mExtent = 0;
    t->mSeg++;
    t->mEnd = 0;
    if (t->nextSeg <= t->mSeg) t->nextSeg = t->mSeg + 1;
    // A10: the new segment's first batch checkpoints the labels (with more
    // than 128 partitions), so the old one stops being read for them.
    t->forceFullLabels = true;
    // A durable head names the new segment (open finds it by the chain
    // anyway; the head is what lets the old segment go).
    rc = typeWriteHead(w, t, true);
    if (rc >= 0) rc = w->io().sync(t->h);
    if (rc >= 0) t->lastCkptNs = monoNs();
    return rc < 0 ? rc : 1;
}

// A9 for types: the oldest sealed meta segment goes once no unmerged L0
// block and no label checkpoint lives in it.
int32_t typeRetireMeta(Writer* w, TypeOwner* t) {
    Engine* e = w->engine();
    if (t->firstLiveMSeg >= t->mSeg) return 0;
    const uint32_t s = t->firstLiveMSeg;
    for (uint32_t i = 0; i < t->nL0; i++)
        if (t->l0[i].mSeg <= s) return 0;
    if (t->mergePhase != 0)
        for (const auto& de : t->mergeBatches)
            if (de.mSeg <= s) return 0;
    if (t->haveLabelCkpt && t->labelCkptSeg <= s) return 0;
    t->firstLiveMSeg = s + 1;
    const int32_t rc = typeTwoDurableHeads(w, t);
    if (rc < 0) {
        t->firstLiveMSeg = s;
        return rc;
    }
    int64_t size = -1;
    if (t->mPrevSeg == s) {
        size = w->io().size(t->mPrev);
        w->io().close(&t->mPrev);
        t->mPrevSeg = UINT32_MAX;
    } else {
        PathBuf mp;
        pathTypeSeg(&mp, w->eng_root(), t->fid, 'm', s, "fsl");
        size = fileSize(&w->io(), mp);
    }
    RetiredFile r;
    r.it = retireItem('m', s, 0, size > 0 ? uint64_t(size) : 0);
    r.retireNs = monoNs();
    t->retired.push_back(r);
    t->mSealedBytes -= std::min<uint64_t>(t->mSealedBytes, r.it.size);
    e->cRetired.fetch_add(1, std::memory_order_relaxed);
    e->cMetaRetired.fetch_add(1, std::memory_order_relaxed);
    return 1;
}
}  // namespace

uint64_t typeDiskBytesNow(const TypeOwner* t, uint64_t* retiredOut) {
    uint64_t retired = 0;
    for (const auto& r : t->retired) retired += r.it.size;
    uint64_t b = t->hExtent + t->mSealedBytes + std::max(t->mExtent, t->mEnd) + t->gSealedBytes +
                 std::max(t->gExtent, t->gLen) + t->fenceExtent + retired;
    for (const auto& run : t->runs) b += run.fileLen;
    if (t->manifestGenLoaded) b += t->manifestBytes;
    if (t->mergePhase == 1) b += t->mergeRun.fileLen + t->mergeManifestBytes + t->mergeArrLen;  // built, MERGE_DONE pending
    if (retiredOut) *retiredOut = retired;
    return b;
}

void typePublishDisk(TypeOwner* t) {
    uint64_t retired = 0;
    const uint64_t b = typeDiskBytesNow(t, &retired);
    t->diskBytesPub.store(b, std::memory_order_relaxed);
    t->retiredBytesPub.store(retired, std::memory_order_relaxed);
}

int32_t typeReclaimStep(Writer* w, TypeOwner* t) {
    if (t->st) return 0;
    Engine* e = w->engine();
    const EngineConfig& cfg = e->config();
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    int32_t out = 0;
    if (cfg.typeMetaSegBytes && t->mEnd >= cfg.typeMetaSegBytes) out = typeRotateMeta(w, t);
    if (out >= 0 && cfg.retireMeta) out = typeRetireMeta(w, t);
    typePublishDisk(t);
    if (t->retired.empty()) {
        tHotPathDepth = saved;
        return out;
    }
    // Unlinking: as for partitions (partitionReclaimStep).
    const uint64_t now = monoNs();
    const uint64_t gate = e->readerGateNs();
    const uint64_t jsafe = cfg.commitJournal ? e->journalSafeNs() : UINT64_MAX;
    const uint64_t grace = cfg.reclaimGraceMs * 1000000ull;
    const bool valve = t->retired.size() > 2048;
    uint32_t done = 0;
    size_t i = 0;
    while (i < t->retired.size() && done < cfg.reclaimBatch) {
        RetiredFile& r = t->retired[i];
        if (r.retireNs == kRetirePendingNs) break;  // no head has stopped naming it yet (FIFO)
        if (now < r.retryNs) {
            i++;
            continue;
        }
        const bool forced = valve && t->retired.size() - i > 2048 && now - r.retireNs >= grace;
        if (!forced) {
            if (gate <= r.retireNs) break;
            if (!r.firstOkNs) r.firstOkNs = now;
            if (now - r.firstOkNs < grace) {
                i++;
                continue;
            }
        }
        if (jsafe <= r.retireNs) break;
        PathBuf path;
        typeRetirePath(&path, w->eng_root(), t->fid, r.it);
        const int32_t rc = path.len ? w->io().unlink(path.c_str(), path.len, true) : 0;
        if (rc == FLATSQL_IO_ERR_BUSY) {
            e->cUnlinkBusy.fetch_add(1, std::memory_order_relaxed);
            r.retryNs = now + kBusyRetryNs;
            i++;
            continue;
        }
        if (rc < 0 && rc != FLATSQL_IO_ERR_NOENT) {
            out = rc;
            break;
        }
        t->retired.erase(t->retired.begin() + long(i));
        e->cUnlinked.fetch_add(1, std::memory_order_relaxed);
        done++;
    }
    if (done) typePublishDisk(t);
    tHotPathDepth = saved;
    return out < 0 ? out : int32_t(done);
}

int32_t typeOpenReclaim(IoCtx* io, const char* root, TypeOwner* t, const std::vector<RetireItem>& items) {
    t->retired.clear();
    auto drop = [&](const RetireItem& it, bool ifUnused) -> int32_t {
        PathBuf path;
        typeRetirePath(&path, root, t->fid, it);
        if (!path.len) return 0;
        const int32_t rc = io->unlink(path.c_str(), path.len, ifUnused);
        if (rc == 0 || rc == FLATSQL_IO_ERR_NOENT) return 0;
        if (rc != FLATSQL_IO_ERR_BUSY) return rc;
        // A reader that outlived the writer still holds it: it stays
        // retired, behind the reader gate.
        RetiredFile r;
        r.it = it;
        const int64_t n = fileSize(io, path);
        r.it.size = n > 0 ? uint32_t(std::min<int64_t>(n, 0xffffffffll)) : 0;
        r.retireNs = monoNs();
        t->retired.push_back(r);
        return 0;
    };
    auto live = [&](const RetireItem& it) {
        if (it.letter == 'f') return it.gen == t->manifestGenLoaded;
        if (it.letter == 'x')
            for (const auto& r : t->runs)
                if (r.gen == it.gen) return true;
        if (it.letter == 'm') return it.seg >= t->firstLiveMSeg;
        return false;
    };
    int32_t rc = 0;
    // The set the manifest persists.
    for (const auto& it : items)
        if (rc >= 0 && !live(it)) rc = drop(it, true);
    // Meta segments below first_live_m_seg (retired, maybe not unlinked).
    const uint32_t lo = t->firstLiveMSeg > 256 ? t->firstLiveMSeg - 256 : 0;
    for (uint32_t s = lo; rc >= 0 && s < t->firstLiveMSeg; s++) rc = drop(retireItem('m', s, 0, 0), true);
    // A rotation the crash cut short: a next meta segment the chain does not
    // continue into holds nothing (a later rotation would truncate it).
    {
        PathBuf np;
        pathTypeSeg(&np, root, t->fid, 'm', t->mSeg + 1, "fsl");
        const int32_t urc = io->unlink(np.c_str(), np.len, true);
        if (urc < 0 && urc != FLATSQL_IO_ERR_NOENT) rc = urc;
    }
    // A merge in flight at the crash: outputs no MERGE_DONE named, at the
    // generations after the manifest's. The head's next_gen may lag the
    // generation a crash left (heads are not written every commit), so the
    // sweep covers 64 past it and 16 past the last one it found. The unlinks
    // are durable: merges after this open name later generations, and a file
    // that came back would never be looked for again.
    uint32_t hi = t->nextGen + 64;
    for (uint32_t g = t->manifestGenLoaded + 1; rc >= 0 && g <= hi && g - t->manifestGenLoaded <= 4096; g++) {
        for (const char letter : {'x', 'f', 'G'}) {
            PathBuf path;
            typeRetirePath(&path, root, t->fid, retireItem(letter, 0, g, 0));
            if (io->probe(path.c_str(), path.len) != 0) continue;
            hi = std::max(hi, g + 16);
            if (g >= t->nextGen) t->nextGen = g + 1;  // never reused while it may linger
            rc = drop(retireItem(letter, 0, g, 0), true);
            if (rc < 0) break;
        }
    }
    if (rc < 0) return rc;
    // The ledger: sizes of the files the type names (no data byte is read).
    PathBuf path;
    pathType(&path, root, t->fid, "h.fsh");
    int64_t n = fileSize(io, path);
    t->hExtent = n > 0 ? uint64_t(n) : 0;
    t->mSealedBytes = 0;
    for (uint32_t s = t->firstLiveMSeg; s < t->mSeg; s++) {
        pathTypeSeg(&path, root, t->fid, 'm', s, "fsl");
        n = fileSize(io, path);
        if (n > 0) t->mSealedBytes += uint64_t(n);
    }
    pathTypeSeg(&path, root, t->fid, 'm', t->mSeg, "fsl");
    n = fileSize(io, path);
    t->mExtent = n > 0 ? uint64_t(n) : 0;
    t->gSealedBytes = 0;
    size_t ov = 0;
    for (uint32_t s = 0; s < t->gSeg; s++) {
        while (ov < t->arrOverrides.size() && t->arrOverrides[ov].seg < s) ov++;
        if (ov < t->arrOverrides.size() && t->arrOverrides[ov].seg == s) continue;  // T3b: rewritten (ga-*)
        pathTypeSeg(&path, root, t->fid, 'g', s, "fsg");
        n = fileSize(io, path);
        if (n > 0) t->gSealedBytes += uint64_t(n);
    }
    // T3b (A15): the ga-<gen>.fsg files the arrivals table names.
    std::vector<uint32_t> gens;
    for (const ArrOverride& o : t->arrOverrides)
        if (std::find(gens.begin(), gens.end(), o.gen) == gens.end()) gens.push_back(o.gen);
    for (uint32_t g : gens) {
        pathTypeArrivalsCompact(&path, root, t->fid, g);
        n = fileSize(io, path);
        if (n > 0) t->gSealedBytes += uint64_t(n);
    }
    pathTypeSeg(&path, root, t->fid, 'g', t->gSeg, "fsg");
    n = fileSize(io, path);
    t->gExtent = n > 0 ? uint64_t(n) : 0;
    pathType(&path, root, t->fid, kArrivalFenceName);
    n = fileSize(io, path);
    t->fenceExtent = n > 0 ? uint64_t(n) : 0;
    typePublishDisk(t);
    return 0;
}

// ---------------------------------------------------------------------------
// Open: the persisted set is unlinked; the ledger is rebuilt from the head,
// the manifest and file sizes (no data byte is read).
// ---------------------------------------------------------------------------
namespace {
int64_t statFile(IoCtx* io, const PathBuf& path, FileClass cls) {
    FileRef f;
    if (io->open(path.c_str(), path.len, FLATSQL_IO_READ, cls, &f) < 0) return -1;
    const int64_t n = io->size(f);
    io->close(&f);
    return n;
}
}  // namespace

int32_t partitionOpenReclaim(IoCtx* io, const char* root, Partition* p, const std::vector<RetireItem>& items,
                             uint32_t* unlinked) {
    *unlinked = 0;
    p->retired.clear();
    p->ledger.retiredBytes = 0;
    for (const auto& it : items) {
        PathBuf path;
        retirePath(&path, root, p->pid, it);
        if (!path.len) continue;
        const int32_t rc = io->unlink(path.c_str(), path.len, true);
        if (rc == 0) {
            (*unlinked)++;
        } else if (rc == FLATSQL_IO_ERR_BUSY) {
            // A reader instance that outlived the writer still holds it: it
            // stays retired, behind the reader gate like any other.
            RetiredFile r;
            r.it = it;
            FileRef f;
            if (io->open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::Store, &f) == 0) {
                const int64_t n = io->size(f);
                io->close(&f);
                r.it.size = n > 0 ? uint32_t(std::min<int64_t>(n, 0xffffffffll)) : 0;
            }
            r.retireNs = monoNs();
            p->retired.push_back(r);
            p->ledger.retiredBytes += r.it.size;
        } else if (rc != FLATSQL_IO_ERR_NOENT) {
            return rc;
        }
    }
    if (!p->retired.empty()) p->retireDirty = true;
    return 0;
}

int32_t partitionOpenLedger(IoCtx* io, const char* root, Partition* p) {
    // O(S) in segments: every ledger change is O(1) and the unmerged sealed
    // segments are found by one walk alongside the (ascending) entries.
    p->ledger.clear();
    p->ledgerBytes = 0;
    PathBuf path;
    pathPartition(&path, root, p->pid, "h.fsh");
    int64_t n = statFile(io, path, FileClass::Head);
    p->hExtent = n > 0 ? uint64_t(n) : 0;
    pathPartition(&path, root, p->pid, "l.fsl");
    n = statFile(io, path, FileClass::Lanes);
    p->lDisk = n > 0 ? uint64_t(n) : 0;
    pathPartitionSeg(&path, root, p->pid, 'd', p->dSeg, "fsd");
    n = statFile(io, path, FileClass::Data);
    p->dExtent = n > 0 ? uint64_t(n) : 0;
    pathPartitionSeg(&path, root, p->pid, 'm', p->mSeg, "fsl");
    n = statFile(io, path, FileClass::Meta);
    p->mExtent = n > 0 ? uint64_t(n) : 0;
    for (uint32_t s = p->firstLiveMSeg; s < p->mSeg; s++) {
        pathPartitionSeg(&path, root, p->pid, 'm', s, "fsl");
        n = statFile(io, path, FileClass::Meta);
        if (n >= 0) ledgerSet(p, retireItem('m', s, 0, uint64_t(n)));
    }
    // The next segment's pre-created files (named by next_seg).
    pathPartitionSeg(&path, root, p->pid, 'd', p->nextSeg, "fsd");
    n = statFile(io, path, FileClass::Data);
    if (n >= 0) ledgerSet(p, retireItem('d', p->nextSeg, 0, uint64_t(n)));
    pathPartitionSeg(&path, root, p->pid, 'm', p->nextSeg, "fsl");
    n = statFile(io, path, FileClass::Meta);
    if (n >= 0) ledgerSet(p, retireItem('m', p->nextSeg, 0, uint64_t(n)));
    // The manifest and the files it names.
    if (p->manifestGen) {
        pathPartitionManifest(&path, root, p->pid, p->manifestGen);
        FileRef f;
        int32_t rc = io->open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::Manifest, &f);
        if (rc < 0) return rc;
        const int64_t size = io->size(f);
        std::vector<uint8_t> buf(size > 0 ? size_t(size) : 0);
        ManifestDesc md;
        const bool ok = size > 0 && io->read(f, buf.data(), buf.size(), 0) == size &&
                        decodeManifest(buf.data(), buf.size(), &md);
        io->close(&f);
        if (!ok) return FLATSQL_IO_ERR_IO;
        ledgerSet(p, retireItem('f', 0, p->manifestGen, uint64_t(size)));
        p->segs.clear();
        for (const auto& d : md.segs) {
            SegmentInfo si;
            segFromDesc(d, &si);
            if (d.cgen && !d.empty) {
                ledgerSet(p, retireItem('D', d.seg, d.cgen, d.dLen));
                ledgerSet(p, retireItem('R', d.seg, d.cgen, d.rLen));
                ledgerSet(p, retireItem('A', d.seg, d.cgen, d.aLen));
            } else if (!d.cgen) {
                if (d.rLen) ledgerSet(p, retireItem('r', d.seg, 0, d.rLen));
                if (d.aLen) ledgerSet(p, retireItem('a', d.seg, 0, d.aLen));
                if (d.seg != p->dSeg) {  // sealed since, even if the manifest predates the SEAL
                    pathPartitionSeg(&path, root, p->pid, 'd', d.seg, "fsd");
                    n = statFile(io, path, FileClass::Data);
                    if (n >= 0) ledgerSet(p, retireItem('d', d.seg, 0, uint64_t(n)));
                }
            }
            for (const auto& r : d.runs) ledgerSet(p, retireItem('x', d.seg, r.gen, r.fileLen));
            p->segs.push_back(std::move(si));
        }
    }
    // Sealed segments not merged yet (no manifest entry): their d files.
    size_t k = 0;
    for (uint32_t s = p->firstLiveMSeg; s < p->dSeg; s++) {
        while (k < p->segs.size() && (p->segs[k].lastSeg ? p->segs[k].lastSeg : p->segs[k].seg) < s) k++;
        if (k < p->segs.size() && p->segs[k].seg <= s) continue;  // merged (covered by an entry)
        pathPartitionSeg(&path, root, p->pid, 'd', s, "fsd");
        n = statFile(io, path, FileClass::Data);
        if (n >= 0) ledgerSet(p, retireItem('d', s, 0, uint64_t(n)));
    }
    // Retired files a reader still held at open stay counted until unlinked.
    for (const auto& r : p->retired) ledgerSet(p, r.it);
    partitionPublishDisk(p);
    partitionPublishSummary(p);
    return 0;
}

}  // namespace ps
}  // namespace flatsql
