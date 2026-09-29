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

#include "internal.h"

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// Ledger
// ---------------------------------------------------------------------------
namespace {
constexpr uint64_t kBusyRetryNs = 50000000ull;  // 50 ms

bool ledgerMatch(const RetireItem& a, char letter, uint32_t seg, uint32_t gen) {
    if (a.letter != uint8_t(letter)) return false;
    if (letter == 'f') return a.gen == gen;
    if (letter == 'd' || letter == 'r' || letter == 'a' || letter == 'm') return a.seg == seg;
    return a.seg == seg && a.gen == gen;
}
}  // namespace

void ledgerSet(Partition* p, const RetireItem& it) {
    for (auto& e : p->ledger) {
        if (ledgerMatch(e, char(it.letter), it.seg, it.gen)) {
            p->ledgerBytes += uint64_t(it.size) - uint64_t(e.size);
            e.size = it.size;
            return;
        }
    }
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // a maintenance event (merge, seal, SWAP), never per record
    p->ledger.push_back(it);
    tHotPathDepth = saved;
    p->ledgerBytes += it.size;
}

void ledgerDrop(Partition* p, char letter, uint32_t seg, uint32_t gen) {
    for (size_t i = 0; i < p->ledger.size(); i++) {
        if (!ledgerMatch(p->ledger[i], letter, seg, gen)) continue;
        p->ledgerBytes -= p->ledger[i].size;
        p->ledger[i] = p->ledger.back();
        p->ledger.pop_back();
        return;
    }
}

uint64_t ledgerSize(const Partition* p, char letter, uint32_t seg, uint32_t gen) {
    for (const auto& e : p->ledger)
        if (ledgerMatch(e, letter, seg, gen)) return e.size;
    return 0;
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
// Unlinking (maintenance)
// ---------------------------------------------------------------------------
int32_t partitionReclaimStep(Writer* w, Partition* p) {
    if (p->retired.empty() || p->quarantined) return 0;
    Engine* e = w->engine();
    const EngineConfig& cfg = e->config();
    const uint64_t now = monoNs();
    const uint64_t gate = e->readerGateNs();
    const uint64_t jsafe = cfg.commitJournal ? e->journalSafeNs() : UINT64_MAX;
    const uint64_t grace = cfg.reclaimGraceMs * 1000000ull;
    // A set grown past what one batch can carry (a reader statement older
    // than hours of retirements): the oldest go regardless of the gate; such
    // a statement gets the retryable SNAPSHOT_GONE instead of the writer
    // ever waiting on it.
    const bool valve = p->retired.size() > 2048;
    const uint64_t pin = partitionCompactPinNs(p);
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    uint32_t done = 0;
    size_t i = 0;
    int32_t out = 0;
    while (i < p->retired.size() && done < cfg.reclaimBatch) {
        RetiredFile& r = p->retired[i];
        if (r.retireNs == kRetirePendingNs) break;  // no head has stopped naming it yet (FIFO)
        if (now < r.retryNs) {
            i++;
            continue;
        }
        const bool forced = valve && p->retired.size() - i > 2048 && now - r.retireNs >= grace;
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
        if (r.retireNs >= pin) break;  // a compaction in flight may read it
        if (jsafe <= r.retireNs) break;
        // The owner's own read handles go first (UNLINK_IF_UNUSED).
        if (r.it.letter == 'm') {
            for (auto& s : p->segs)
                if (s.seg == r.it.seg) w->io().close(&s.m);
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
        p->retired.erase(p->retired.begin() + long(i));
        e->cUnlinked.fetch_add(1, std::memory_order_relaxed);
        done++;
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
    for (auto& si : p->segs)
        if (si.seg == s) w->io().close(&si.m);
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
        } else if (rc != FLATSQL_IO_ERR_NOENT) {
            return rc;
        }
    }
    if (!p->retired.empty()) p->retireDirty = true;
    return 0;
}

int32_t partitionOpenLedger(IoCtx* io, const char* root, Partition* p) {
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
    for (uint32_t s = p->firstLiveMSeg; s < p->dSeg; s++) {
        bool known = false;
        for (const auto& si : p->segs) {
            const uint32_t last = si.lastSeg ? si.lastSeg : si.seg;
            if (s >= si.seg && s <= last) known = true;
        }
        if (known) continue;
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
