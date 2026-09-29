// FlatSQL partition store: stage-1 preparation of a split partition's ring
// entries (design §12, A26; flatsql/ps/stage1.h has the slot protocol;
// docs/PARTITION-STORE.md §32).
//
// The owner publishes and takes; idle writers claim and prepare. Nothing
// here allocates: the window is allocated at the first split, results are
// computed in the helper writer's staging scratch.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace ps {

namespace {

// Serializes the extraction into the result; false when its strings do not
// fit (the owner then extracts itself).
bool packExtracted(const Extracted& ex, PrepResult* r) {
    r->hasEpoch = ex.hasEpoch ? 1 : 0;
    r->epochMs = ex.epochMs;
    r->epochSec = ex.epochSec;
    r->objectCol = int8_t(ex.objectCol);
    r->colPresent = 0;
    r->colU64 = 0;
    uint32_t used = 0;
    auto put = [&](const uint8_t* s, size_t n, uint16_t* off, uint16_t* len) {
        if (used + n > kPrepKeyBytes) return false;
        if (n) std::memcpy(r->keys + used, s, n);
        *off = uint16_t(used);
        *len = uint16_t(n);
        used += uint32_t(n);
        return true;
    };
    for (uint32_t i = 0; i < kMaxCols; i++) {
        const ColValue& cv = ex.cols[i];
        r->colU[i] = 0;
        r->colOff[i] = 0;
        r->colLen[i] = 0;
        if (!cv.present) continue;
        r->colPresent |= uint8_t(1u << i);
        if (cv.isU64) {
            r->colU64 |= uint8_t(1u << i);
            r->colU[i] = cv.u;
        } else if (!put(cv.s, cv.n, &r->colOff[i], &r->colLen[i])) {
            return false;
        }
    }
    r->idOff = 0;
    r->idLen = 0;
    if (ex.identity && ex.identityLen && !put(ex.identity, ex.identityLen, &r->idOff, &r->idLen)) return false;
    r->keyUsed = uint16_t(used);
    return true;
}

}  // namespace

void prepUnpack(const PrepResult& r, Extracted* ex) {
    *ex = Extracted();
    ex->hasEpoch = r.hasEpoch != 0;
    ex->epochMs = r.epochMs;
    ex->epochSec = r.epochSec;
    ex->objectCol = r.objectCol;
    for (uint32_t i = 0; i < kMaxCols; i++) {
        if (!(r.colPresent & (1u << i))) continue;
        ColValue& cv = ex->cols[i];
        cv.present = true;
        if (r.colU64 & (1u << i)) {
            cv.isU64 = true;
            cv.u = r.colU[i];
        } else {
            cv.s = r.keys + r.colOff[i];
            cv.n = r.colLen[i];
        }
    }
    if (r.idLen) {
        ex->identity = r.keys + r.idOff;
        ex->identityLen = r.idLen;
    } else {
        ex->identity = reinterpret_cast<const uint8_t*>("");
        ex->identityLen = 0;
    }
}

// Stage 1 of one entry: exactly the checks and derivations stageRecord
// makes before it touches partition state. Reads the ring only; a claim
// that went stale reads recycled bytes and its result is discarded by the
// owner's CAS, so nothing here may trust more than the lengths it checks.
PrepHelperView::~PrepHelperView() {
    for (auto& r : runs)
        if (r.file.valid() && io) io->close(&r.file);
}

void prepCompute(Engine* e, Partition* p, StageScratch* sc, uint64_t pos, uint32_t entryLen, PrepResult* out,
                 const PrepHelperView* runs, const PrepRunSet* set, IoCtx* io, uint8_t* lookupScratch) {
    out->valid = 0;
    out->exOk = 0;
    out->reject = 0;
    out->rowFlags = 0;
    out->keyUsed = 0;
    out->hintOk = 0;
    out->hintN = 0;
    const TypeConfig* cfg = p->type ? p->type->cfg.get() : nullptr;
    if (!cfg || entryLen < sizeof(EntryHeader)) return;
    EntryHeader h;
    ringRead(p->ring, e->pool(), pos, &h, sizeof(h));
    if (h.entryLen != entryLen || h.kind != kEntRecord) return;
    const bool sealed = h.flags & kEntSealed;
    const uint64_t attrPos = pos + sizeof(EntryHeader);
    const uint64_t framePos = attrPos + h.attrLen;
    uint32_t sealedLen = 0;
    if (sizeof(EntryHeader) + uint64_t(h.attrLen) + h.frameLen + (sealed ? 4 : 0) > h.entryLen) return;
    if (sealed) {
        uint8_t b[4];
        ringRead(p->ring, e->pool(), framePos + h.frameLen, b, 4);
        sealedLen = getU32(b);
        if (sizeof(EntryHeader) + uint64_t(h.attrLen) + h.frameLen + 4 + sealedLen > h.entryLen) return;
    }
    if (h.frameLen < 4 || h.frameLen > sc->capPlain || h.attrLen > 65536 ||
        pad8(h.frameLen) + h.attrLen + 8 > sc->capPlain)
        return;
    // Plaintext frame, contiguous; the attribute after it in scratch.
    const uint8_t* plain = ringContiguous(p->ring, e->pool(), framePos, h.frameLen);
    if (!plain) {
        ringRead(p->ring, e->pool(), framePos, sc->plain, h.frameLen);
        plain = sc->plain;
    }
    out->valid = 1;
    int32_t rc = cfg->checkFrame(plain, h.frameLen);
    if (rc) {
        out->reject = rc;
        return;
    }
    if (h.flags & kEntCidPresent) {
        std::memcpy(out->cid, h.cid, kCidLen);
        if (cfg->flags() & TypeConfig::kVerifyCid) {
            uint8_t check[kCidLen];
            if (h.flags & kEntCidOverPrefix) computeCid(plain, h.frameLen, check);
            else computeCid(plain + 4, h.frameLen - 4, check);
            if (std::memcmp(check, out->cid, kCidLen) != 0) {
                out->reject = kRejCid;
                return;
            }
            out->rowFlags |= kRowCidVerified;
        }
    } else {
        computeCid(plain + 4, h.frameLen - 4, out->cid);
        out->rowFlags |= kRowCidVerified;
    }
    if (h.attrLen) {
        const uint8_t* attr = ringContiguous(p->ring, e->pool(), attrPos, h.attrLen);
        if (!attr) {
            uint8_t* dst = sc->plain + (plain == sc->plain ? pad8(h.frameLen) : 0);
            ringRead(p->ring, e->pool(), attrPos, dst, h.attrLen);
            attr = dst;
        }
        AttrView av;
        if (!parseAttr(attr, h.attrLen, &av)) {
            out->reject = kRejAttr;
            return;
        }
        out->tagHash = tagTupleHash(av.tag);
        out->laneHash = av.tag.present ? laneHash(av.tag) : 0;
    } else {
        out->tagHash = 0;
        out->laneHash = 0;
    }
    cidSortKey(out->cid, out->cidKey);
    out->cidHash = hash64(out->cidKey, kCidKeyLen);
    out->cidBloom = bloomHash(out->cidKey, kCidKeyLen);
    // Dedupe hint: this CID's postings in the owner's L1 runs of one version
    // (immutable files); the owner checks only its unmerged L0 blocks when
    // the version is still current.
    if (runs && set && runs->version == set->version) {
        const uint64_t hh = out->cidBloom;
        bool ok = true;
        uint32_t n = 0;
        for (const PrepHelperRun& hr : runs->runs) {
            if (!hr.run->mayContainHash(kIxCid, hh)) continue;
            const int64_t rc = hr.run->lookup(io, hr.file, kIxCid, out->cidKey, kCidKeyLen, lookupScratch,
                                              [&](const uint8_t*, uint16_t, const uint8_t* ev) {
                                                  if (n < kPrepHintMax) out->hint[n] = getBE64(ev);
                                                  n++;
                                              });
            if (rc < 0 || n > kPrepHintMax) {
                ok = false;
                break;
            }
        }
        // The unmerged L0 blocks of the same publication (fixed-size CID
        // entries: binary search).
        const size_t es = 2 + kCidKeyLen;
        for (size_t bi = 0; ok && bi < set->l0.size(); bi++) {
            const PrepL0Pub& b = set->l0[bi];
            const size_t e2 = es + b.vlen;
            const uint8_t* base = b.entries->data();
            if (uint64_t(b.n) * e2 != b.entriesBytes) {
                ok = false;
                break;
            }
            if (b.bloomBytes && !bloomTestHash(base + b.entriesBytes, b.bloomBytes, hh)) continue;
            size_t lo = 0, hi = b.n;
            while (lo < hi) {
                const size_t mid = (lo + hi) / 2;
                const uint8_t* e = base + mid * e2;
                if (keyCmp(e + 2, getU16(e), out->cidKey, kCidKeyLen) < 0) lo = mid + 1;
                else hi = mid;
            }
            for (size_t j = lo; j < b.n; j++) {
                const uint8_t* e = base + j * e2;
                if (keyCmp(e + 2, getU16(e), out->cidKey, kCidKeyLen) != 0) break;
                if (n < kPrepHintMax) out->hint[n] = getBE64(e + es);
                n++;
            }
            if (n > kPrepHintMax) ok = false;
        }
        if (ok) {
            out->hintOk = 1;
            out->hintN = uint8_t(n);
            out->hintVersion = runs->version;
            out->hintThrough = set->l0Through;
        }
    }
    // CRC of the bytes the row will store: the plaintext frame, or the sealed
    // envelope with its size prefix.
    if (!sealed) {
        out->dataCrc = crc32c(plain, h.frameLen);
    } else {
        uint32_t crc = 0;
        uint64_t at = framePos + h.frameLen;
        uint64_t left = 4 + uint64_t(sealedLen);
        uint8_t buf[4096];
        while (left) {
            const size_t n = size_t(std::min<uint64_t>(left, sizeof(buf)));
            ringRead(p->ring, e->pool(), at, buf, n);
            crc = crc32c(crc, buf, n);
            at += n;
            left -= n;
        }
        out->dataCrc = crc;
    }
    Extracted ex;
    cfg->extract(plain, h.frameLen, &ex, sc->extract, sc->capExtract);
    out->exOk = packExtracted(ex, out) ? 1 : 0;
    if (!out->exOk) out->keyUsed = 0;
}

void prepPublishRuns(Writer* w, Partition* p) {
    PrepWindow& pw = *p->prep;
    uint64_t sig = 0x72756e73ull;
    uint32_t n = 0;
    for (const auto& si : p->segs)
        for (const auto& run : si.runs) {
            uint64_t v[3] = {si.seg, run.gen, run.fileLen};
            sig = hash64(v, sizeof(v), sig);
            n++;
        }
    sig = hash64(&n, sizeof(n), sig);
    uint64_t l0v[3] = {p->nL0, p->nL0 ? p->l0[0].firstPseq : 0,
                       p->nL0 ? p->l0[p->nL0 - 1].firstPseq + p->l0[p->nL0 - 1].nRows : 0};
    const uint64_t l0Sig = hash64(l0v, sizeof(l0v), 0x6c30);
    if (sig == pw.runSig && l0Sig == pw.l0Sig && pw.runSet) return;
    // Once per commit of a split partition: not a record path.
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    std::shared_ptr<PrepRunSet> rs(new PrepRunSet());
    if (sig != pw.runSig || !pw.runSet) ++pw.runVersion;
    rs->version = pw.runVersion;
    // L0 blocks whose CID entries are in memory, oldest first; the hint
    // covers them only when every block through the newest is.
    bool all = true;
    for (uint32_t i = 0; i < p->nL0 && p->acc; i++) {
        const L0Accel& a = p->acc[i];
        const L0Accel::Kind* k = a.find(kIxCid);
        if (!k || k->n == 0) continue;
        if (!k->entries || !a.cidEntries) {
            all = false;
            break;
        }
        PrepL0Pub b;
        b.firstPseq = a.firstPseq;
        b.lastPseq = a.firstPseq + a.nRows - 1;
        b.n = k->n;
        b.vlen = k->vlen;
        b.entriesBytes = k->entriesBytes;
        b.bloomBytes = a.cidEntries->size() > k->entriesBytes ? uint32_t(a.cidEntries->size() - k->entriesBytes) : 0;
        b.entries = a.cidEntries;
        rs->l0.push_back(std::move(b));
    }
    if (all && p->nL0) rs->l0Through = p->l0[p->nL0 - 1].firstPseq + p->l0[p->nL0 - 1].nRows - 1;
    else rs->l0.clear();
    rs->runs.reserve(n);
    for (const auto& si : p->segs)
        for (const auto& run : si.runs) {
            PrepRunDesc d;
            d.seg = si.seg;
            d.gen = run.gen;
            d.fileLen = run.fileLen;
            rs->runs.push_back(d);
        }
    std::atomic_store(&pw.runSet, std::shared_ptr<const PrepRunSet>(std::move(rs)));
    tHotPathDepth = saved;
    pw.runSig = sig;
    pw.l0Sig = l0Sig;
}

void prepPublish(Writer* w, Partition* p) {
    PrepWindow& pw = *p->prep;
    Engine* e = w->engine();
    uint64_t ord = pw.published.load(std::memory_order_relaxed);
    uint64_t pos = pw.pubPos;
    const uint64_t tail = p->ring->tail.load(std::memory_order_acquire);
    const uint64_t before = ord;
    while (ord < pw.takeOrd + kPrepSlots && pos + sizeof(EntryHeader) <= tail) {
        EntryHeader h;
        ringRead(p->ring, e->pool(), pos, &h, sizeof(h));
        // A malformed entry stops publishing; the staging loop quarantines.
        if (h.entryLen < sizeof(EntryHeader) || (h.entryLen & 7) || pos + h.entryLen > tail) break;
        // The slot's previous ordinal (ord - kPrepSlots) is taken, so no
        // helper can be writing it (the owner never takes a WRITING entry).
        PrepSlot& s = pw.slots[ord % kPrepSlots];
        s.pos = pos;
        s.len = h.entryLen;
        s.kind = h.kind;
        s.state.store(prepWord(ord, kPrepFree), std::memory_order_release);
        pos += h.entryLen;
        ord++;
    }
    if (ord == before) return;
    pw.pubPos = pos;
    pw.published.store(ord, std::memory_order_release);
    if (p->hot.load(std::memory_order_relaxed)) e->wakePrepHelpers(w->id());
}

namespace {

// Takes ordinal pw.takeOrd for the owner. The window is topped up as the
// owner advances, so helpers stay ahead of it. Returns true when a READY result
// is in the slot (the caller uses it before the next publish).
bool takeOne(Writer* w, Partition* p, bool wantResult) {
    PrepWindow& pw = *p->prep;
    Engine* e = w->engine();
    if (pw.published.load(std::memory_order_relaxed) - pw.takeOrd < kPrepSlots / 2) prepPublish(w, p);
    const uint64_t k = pw.takeOrd;
    PrepSlot& s = pw.slots[k % kPrepSlots];
    bool ready = false;
    if (k < pw.published.load(std::memory_order_relaxed)) {
        uint64_t waitStart = 0;
        uint32_t spins = 0;
        for (;;) {
            uint64_t cur = s.state.load(std::memory_order_acquire);
            if (prepOrd(cur) != k) break;  // cannot happen: published slots carry their ordinal
            const uint8_t st = prepSt(cur);
            if (st == kPrepReady) {
                ready = true;
                break;
            }
            if (st == kPrepOwner) break;
            if (st == kPrepWriting) {
                cpuRelax();  // a copy of one result: bounded (a stopped helper holds it)
                continue;
            }
            if (st == kPrepClaimed && wantResult) {
                // A helper is on it: wait a little rather than redo it.
                if ((spins++ & 63) != 0) {
                    cpuRelax();
                    continue;
                }
                const uint64_t now = monoNs();
                if (!waitStart) waitStart = now;
                if (now - waitStart < uint64_t(e->config().prepClaimWaitUs) * 1000) {
                    cpuRelax();
                    continue;
                }
            }
            if (s.state.compare_exchange_weak(cur, prepWord(k, kPrepOwner), std::memory_order_acq_rel)) {
                if (st == kPrepClaimed) pw.stolen.fetch_add(1, std::memory_order_relaxed);
                break;
            }
        }
        pw.takeOrd = k + 1;
        pw.takePos = s.pos + s.len;
    }
    // Helpers skip what the owner already passed.
    uint64_t c = pw.claim.load(std::memory_order_relaxed);
    while (c < pw.takeOrd && !pw.claim.compare_exchange_weak(c, pw.takeOrd, std::memory_order_relaxed)) {}
    return ready;
}

}  // namespace

const PrepResult* prepTake(Writer* w, Partition* p, uint64_t pos) {
    PrepWindow& pw = *p->prep;
    if (pos != pw.takePos) return nullptr;
    const uint64_t k = pw.takeOrd;
    const bool ready = takeOne(w, p, true);
    if (pw.takeOrd != k + 1) return nullptr;  // not published (cannot happen for a staged entry)
    PrepSlot& s = pw.slots[k % kPrepSlots];
    if (!ready || s.pos != pos || !s.r.valid) return nullptr;
    pw.used.fetch_add(1, std::memory_order_relaxed);
    return &s.r;
}

void prepTakeThrough(Writer* w, Partition* p, uint64_t newPos) {
    PrepWindow& pw = *p->prep;
    while (pw.takePos < newPos) {
        const uint64_t k = pw.takeOrd;
        takeOne(w, p, false);
        if (pw.takeOrd == k) break;  // nothing published there (cannot happen for consumed entries)
    }
}

// ---------------------------------------------------------------------------
// Helpers (idle writers)
// ---------------------------------------------------------------------------
namespace {

// The helpers' handles and accelerators for the owner's current run set.
// Rebuilt by one helper at a time (try_lock: the others go on without hints
// meanwhile); runs of the previous view are reused.
std::shared_ptr<const PrepHelperView> helperRuns(Engine* e, Partition* p) {
    PrepWindow& pw = *p->prep;
    std::shared_ptr<const PrepRunSet> rs = std::atomic_load(&pw.runSet);
    if (!rs) return nullptr;
    std::shared_ptr<const PrepHelperView> hv = std::atomic_load(&pw.helperView);
    if (hv && hv->version == rs->version) return hv;
    std::unique_lock<std::mutex> lk(pw.helperMu, std::try_to_lock);
    if (!lk.owns_lock()) return nullptr;
    hv = std::atomic_load(&pw.helperView);
    if (hv && hv->version == rs->version) return hv;
    std::shared_ptr<PrepHelperView> nv(new PrepHelperView());
    nv->version = rs->version;
    nv->io = e->helperIo();
    IoCtx* io = nv->io;
    for (const PrepRunDesc& d : rs->runs) {
        PrepHelperRun hr;
        hr.seg = d.seg;
        hr.gen = d.gen;
        hr.fileLen = d.fileLen;
        bool reused = false;
        if (hv) {
            for (const PrepHelperRun& o : hv->runs) {
                if (o.seg == d.seg && o.gen == d.gen && o.fileLen == d.fileLen) {
                    // Share the accelerator, own a handle (the old view closes its own).
                    hr.run = o.run;
                    PathBuf rp;
                    pathPartitionRun(&rp, e->root().c_str(), p->pid, d.seg, d.gen);
                    if (io->open(rp.c_str(), rp.len, FLATSQL_IO_READ, FileClass::Index, &hr.file) < 0) return nullptr;
                    reused = true;
                    break;
                }
            }
        }
        if (!reused) {
            PathBuf rp;
            pathPartitionRun(&rp, e->root().c_str(), p->pid, d.seg, d.gen);
            if (io->open(rp.c_str(), rp.len, FLATSQL_IO_READ, FileClass::Index, &hr.file) < 0) {
                nv->runs.push_back(std::move(hr));
                return nullptr;  // retired meanwhile: a newer run set follows
            }
            hr.run.reset(new L1Run());
            if (hr.run->load(io, hr.file, d.fileLen) < 0) {
                nv->runs.push_back(std::move(hr));
                return nullptr;
            }
        }
        nv->runs.push_back(std::move(hr));
    }
    std::shared_ptr<const PrepHelperView> out(std::move(nv));
    std::atomic_store(&pw.helperView, out);
    return out;
}

}  // namespace

uint32_t Engine::prepHelp(Writer* w, uint32_t budget) {
    uint32_t done = 0;
    if (!nHot_.load(std::memory_order_acquire)) return 0;
    for (uint32_t i = 0; i < kMaxHot && done < budget; i++) {
        Partition* p = hot_[i].load(std::memory_order_acquire);
        if (!p || !p->prep || p->ownerWriter.load(std::memory_order_acquire) == w->id()) continue;
        PrepWindow& pw = *p->prep;
        pw.helpers.fetch_add(1, std::memory_order_seq_cst);
        if (!p->hot.load(std::memory_order_seq_cst)) {
            pw.helpers.fetch_sub(1, std::memory_order_release);
            continue;
        }
        PrepResult* r = w->prepScratch_.get();
        const std::shared_ptr<const PrepRunSet> set = std::atomic_load(&pw.runSet);
        const std::shared_ptr<const PrepHelperView> runs = helperRuns(this, p);
        while (done < budget && !stopping()) {
            uint64_t k = pw.claim.load(std::memory_order_acquire);
            if (k >= pw.published.load(std::memory_order_acquire)) break;
            if (!pw.claim.compare_exchange_weak(k, k + 1, std::memory_order_acq_rel)) continue;
            PrepSlot& s = pw.slots[k % kPrepSlots];
            uint64_t expect = prepWord(k, kPrepFree);
            if (!s.state.compare_exchange_strong(expect, prepWord(k, kPrepClaimed), std::memory_order_acq_rel))
                continue;  // the owner took it
            const uint64_t pos = s.pos;
            const uint32_t len = s.len;
            const uint16_t kind = s.kind;
            if (cfg_.testPrepStallNs && cfg_.testPrepStallEvery &&
                prepClaims_.fetch_add(1, std::memory_order_relaxed) % cfg_.testPrepStallEvery == 0) {
                cPrepStalls.fetch_add(1, std::memory_order_relaxed);
                sleepNs(cfg_.testPrepStallNs);
            }
            if (kind == kEntRecord) {
                prepCompute(this, p, w->sc_, pos, len, r, runs.get(), set.get(), &w->io_, w->lookupScratch());
            } else {
                r->valid = 0;
                r->keyUsed = 0;
            }
            expect = prepWord(k, kPrepClaimed);
            if (!s.state.compare_exchange_strong(expect, prepWord(k, kPrepWriting), std::memory_order_acq_rel)) {
                pw.wasted.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            std::memcpy(&s.r, r, r->valid ? prepResultBytes(*r) : offsetof(PrepResult, keys));
            s.state.store(prepWord(k, kPrepReady), std::memory_order_release);
            pw.prepared.fetch_add(1, std::memory_order_relaxed);
            done++;
        }
        pw.helpers.fetch_sub(1, std::memory_order_release);
    }
    return done;
}

void Engine::wakePrepHelpers(uint8_t owner) {
    uint32_t woken = 0;
    for (size_t i = 0; i < writers_.size() && woken < cfg_.hotHelpers; i++) {
        if (writers_[i]->id() == owner) continue;
        writers_[i]->ring();
        woken++;
    }
}

}  // namespace ps
}  // namespace flatsql
