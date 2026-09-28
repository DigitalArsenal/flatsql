// FlatSQL partition store: type owners (design §4.8 amended by A6, A10, A14,
// A15, A16, A25).
//
// A type owner labels every PUT of its partitions FIRST (first copy of a cid
// in the type: gets a gseq and an arrivals entry) or REPEAT, keeps the cid
// catalog (cid -> copies), the LABEL run ((pid, pseq) -> (gseq, label)), the
// REPEAT run and the REHOME run (A14: a promoted REPEAT keeps the gseq). It
// reads partition rows only from durable batches (durablePseqHi) and
// publishes nothing before its own batch is durable.
#include <algorithm>
#include <unordered_map>

#include "internal.h"

namespace flatsql {
namespace ps {

namespace {

enum Label : uint8_t { kLabelFirst = 1, kLabelRepeat = 2, kLabelDead = 3, kLabelPromoted = 4 };
constexpr int32_t kOpenRW = FLATSQL_IO_READ | FLATSQL_IO_WRITE;
constexpr uint16_t kTypeFlagFullLabels = 1;
constexpr uint16_t kTypeFlagMergeDone = 4;

bool isLive(uint8_t label) { return label == kLabelFirst || label == kLabelRepeat || label == kLabelPromoted; }
bool isFirst(uint8_t label) { return label == kLabelFirst || label == kLabelPromoted; }

void encCatVal(uint8_t* v, uint32_t pid, uint64_t pseq, uint64_t tcs, uint8_t label, uint64_t gseq,
               uint32_t len) {
    putBE32(v, pid);
    putBE64(v + 4, pseq);
    putBE64(v + 12, tcs);
    v[20] = label;
    putBE64(v + 21, gseq);
    putBE32(v + 29, len);
}

struct TCtx {
    Writer* w;
    Engine* e;
    TypeOwner* t;
    StageScratch* sc;
    StagedType* st;
    IoCtx* io;
    int32_t err = 0;
};

bool addPosting(TCtx& c, uint16_t kind, const uint8_t* key, size_t klen, const uint8_t* val,
                uint8_t vlen) {
    StageScratch& sc = *c.sc;
    if (sc.nEntries >= sc.capEntries || sc.keyBytes + klen + vlen > sc.capKeys) return false;
    uint8_t* k = sc.keys + sc.keyBytes;
    std::memcpy(k, key, klen);
    std::memcpy(k + klen, val, vlen);
    sc.keyBytes += uint32_t(klen + vlen);
    StagedEntry& e = sc.entries[sc.nEntries++];
    e.kind = kind;
    e.klen = uint16_t(klen);
    e.vlen = vlen;
    e.key = k;
    e.val = k + klen;
    return true;
}

FileRef* typeMHandle(TCtx& c, uint32_t mSeg, FileRef* tmp) {
    if (mSeg == c.t->mSeg && c.t->m.valid()) return &c.t->m;
    PathBuf path;
    pathTypeSeg(&path, c.w->eng_root(), c.t->fid, 'm', mSeg, "fsl");
    if (c.io->open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::TypeMeta, tmp) < 0) return nullptr;
    return tmp;
}

// Committed catalog postings for a cid: unmerged type L0 blocks, then runs.
template <typename F>
int32_t committedCatalog(TCtx& c, const uint8_t* key, F&& visit) {
    TypeOwner* t = c.t;
    const uint64_t h = bloomHash(key, kCidKeyLen);
    for (uint32_t i = 0; i < t->nL0; i++) {
        const L0Accel& a = t->acc[i];
        const L0Accel::Kind* k = a.find(kIxTypeCid);
        if (!k || k->n == 0) continue;
        if (k->bloom && !bloomTestHash(k->bloom, k->bloomBytes, h)) continue;
        FileRef tmp;
        FileRef* f = typeMHandle(c, a.mSeg, &tmp);
        if (!f) return FLATSQL_IO_ERR_IO;
        uint64_t off = 0;
        int32_t rc = 0;
        bool past = false;
        while (off < k->entriesBytes && !past) {
            const uint64_t want = std::min<uint64_t>(c.sc->capSection, k->entriesBytes - off);
            const int64_t n = c.io->read(*f, c.sc->section, size_t(want), k->entriesOff + off);
            if (n != int64_t(want)) {
                rc = n < 0 ? int32_t(n) : FLATSQL_IO_ERR_IO;
                break;
            }
            EntryIter it;
            it.p = c.sc->section;
            it.end = c.sc->section + want;
            it.vlen = k->vlen;
            const uint8_t *ek, *ev;
            uint16_t el;
            const uint8_t* last = it.p;
            while (it.next(&ek, &el, &ev)) {
                last = it.p;
                const int cmp = keyCmp(ek, el, key, kCidKeyLen);
                if (cmp < 0) continue;
                if (cmp > 0) {
                    past = true;
                    break;
                }
                visit(ev);
            }
            if (last == c.sc->section) {
                rc = FLATSQL_IO_ERR_IO;
                break;
            }
            off += uint64_t(last - c.sc->section);
        }
        if (tmp.valid()) c.io->close(&tmp);
        if (rc < 0) return rc;
    }
    for (auto& run : t->runs) {
        if (!run.run || !run.run->mayContainHash(kIxTypeCid, h)) continue;
        const int64_t rc = run.run->lookup(c.io, run.file, kIxTypeCid, key, kCidKeyLen,
                                           c.w->lookupScratch(),
                                           [&](const uint8_t*, uint16_t, const uint8_t* ev) { visit(ev); });
        if (rc < 0) return int32_t(rc);
    }
    return 0;
}

// One type batch may post several catalog entries for one copy (labeled
// REPEAT, promoted, then killed by a later row of the same batch). They share
// the batch's tcs, and postings sort by value, so the in-batch order is lost:
// at equal tcs the label that can only come later wins (a copy never leaves
// DEAD, and a REPEAT only ever becomes PROMOTED).
int labelRank(uint8_t label) { return label == kLabelDead ? 9 : label; }
bool supersedesCopy(uint64_t tcs, uint8_t label, uint64_t curTcs, uint8_t curLabel) {
    return tcs > curTcs || (tcs == curTcs && labelRank(label) >= labelRank(curLabel));
}

// Upserts a copy in the per-batch cid state (latest tcs wins per (pid, pseq)).
void upsertCopy(StageScratch& sc, StageScratch::TCid* tc, uint32_t pid, uint64_t pseq, uint64_t tcs,
                uint8_t label, uint64_t gseq, uint32_t len) {
    for (int32_t i = tc->copies; i >= 0; i = sc.tcopies[i].next) {
        StageScratch::TCopy& cp = sc.tcopies[i];
        if (cp.pid == pid && cp.pseq == pseq) {
            if (supersedesCopy(tcs, label, cp.tcs, cp.label)) {
                cp.tcs = tcs;
                cp.label = label;
                cp.gseq = gseq;
                cp.len = len;
            }
            return;
        }
    }
    if (sc.nTCopies >= StageScratch::kTCopyCap) return;
    StageScratch::TCopy& cp = sc.tcopies[sc.nTCopies];
    cp.pid = pid;
    cp.pseq = pseq;
    cp.tcs = tcs;
    cp.label = label;
    cp.gseq = gseq;
    cp.len = len;
    cp.next = tc->copies;
    tc->copies = int32_t(sc.nTCopies);
    sc.nTCopies++;
}

StageScratch::TCid* catalogState(TCtx& c, const uint8_t key[kCidKeyLen]) {
    StageScratch& sc = *c.sc;
    const uint32_t b = uint32_t(hash64(key, kCidKeyLen) % StageScratch::kCidBuckets);
    for (int32_t i = sc.tcidBuckets[b]; i >= 0; i = sc.tcids[i].next)
        if (std::memcmp(sc.tcids[i].key, key, kCidKeyLen) == 0) return &sc.tcids[i];
    if (sc.nTCids >= StageScratch::kTCidCap || sc.nTCopies + 64 > StageScratch::kTCopyCap) return nullptr;
    StageScratch::TCid& tc = sc.tcids[sc.nTCids];
    std::memcpy(tc.key, key, kCidKeyLen);
    tc.copies = -1;
    const int32_t rc = committedCatalog(c, key, [&](const uint8_t* v) {
        upsertCopy(sc, &tc, getBE32(v), getBE64(v + 4), getBE64(v + 12), v[20], getBE64(v + 21),
                   getBE32(v + 29));
    });
    if (rc < 0) {
        c.err = rc;
        return nullptr;
    }
    tc.next = sc.tcidBuckets[b];
    sc.tcidBuckets[b] = int32_t(sc.nTCids);
    sc.nTCids++;
    return &tc;
}

bool postCatalog(TCtx& c, const uint8_t* key, uint32_t pid, uint64_t pseq, uint64_t tcs, uint8_t label,
                 uint64_t gseq, uint32_t len) {
    uint8_t v[33];
    encCatVal(v, pid, pseq, tcs, label, gseq, len);
    return addPosting(c, kIxTypeCid, key, kCidKeyLen, v, 33);
}

bool postLabel(TCtx& c, uint32_t pid, uint64_t pseq, uint64_t tcs, uint64_t gseq, uint8_t label) {
    uint8_t k[12], v[17];
    putBE32(k, pid);
    putBE64(k + 4, pseq);
    putBE64(v, tcs);
    putBE64(v + 8, gseq);
    v[16] = label;
    return addPosting(c, kIxTypeLabel, k, 12, v, 17);
}

void resetTypeScratch(StageScratch& sc) {
    for (uint32_t i = 0; i < sc.nTCids; i++) {
        const uint32_t b = uint32_t(hash64(sc.tcids[i].key, kCidKeyLen) % StageScratch::kCidBuckets);
        sc.tcidBuckets[b] = -1;
    }
    sc.nTCids = 0;
    sc.nTCopies = 0;
    sc.nArrivals = 0;
    sc.nEntries = 0;
    sc.keyBytes = 0;
}

// One partition row seen by the type owner.
bool labelRow(TCtx& c, Partition* p, const RecRow& r) {
    StageScratch& sc = *c.sc;
    const uint64_t tcs = c.st->commitSeq;
    // Capacity first: a row is labeled completely or not at all.
    if (sc.nEntries + 8 > sc.capEntries || sc.keyBytes + 8 * 64 > sc.capKeys ||
        sc.nArrivals + 1 > StageScratch::kArrivalCap || sc.nTCopies + 64 > StageScratch::kTCopyCap ||
        sc.nTCids + 1 > StageScratch::kTCidCap)
        return false;
    if (r.kind == kRowPut) {
        uint8_t key[kCidKeyLen];
        cidSortKey(r.cid, key);
        StageScratch::TCid* tc = catalogState(c, key);
        if (!tc) return false;
        // Idempotent catch-up: a copy already labeled keeps its label.
        for (int32_t i = tc->copies; i >= 0; i = sc.tcopies[i].next)
            if (sc.tcopies[i].pid == p->pid && sc.tcopies[i].pseq == r.pseq) return true;
        uint64_t firstGseq = 0;
        for (int32_t i = tc->copies; i >= 0; i = sc.tcopies[i].next)
            if (isFirst(sc.tcopies[i].label)) firstGseq = sc.tcopies[i].gseq;
        if (firstGseq) {
            upsertCopy(sc, tc, p->pid, r.pseq, tcs, kLabelRepeat, firstGseq, r.len);
            uint8_t k[12], v[8];
            putBE32(k, p->pid);
            putBE64(k + 4, r.pseq);
            putBE64(v, tcs);
            if (!postCatalog(c, key, p->pid, r.pseq, tcs, kLabelRepeat, firstGseq, r.len) ||
                !postLabel(c, p->pid, r.pseq, tcs, firstGseq, kLabelRepeat) ||
                !addPosting(c, kIxTypeRepeat, k, 12, v, 8))
                return false;
            c.e->cRepeat.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        if (sc.nArrivals >= StageScratch::kArrivalCap) return false;
        const uint64_t gseq = c.e->allocGseq(1);
        ArrivalEntry& a = sc.arrivals[sc.nArrivals++];
        a.gseq = gseq;
        a.pid = p->pid;
        a.flags = kArrivalFirst;
        a.rsv = 0;
        a.pseq = r.pseq;
        if (!c.st->firstGseq) c.st->firstGseq = gseq;
        if (gseq > c.st->gseqHi) c.st->gseqHi = gseq;
        upsertCopy(sc, tc, p->pid, r.pseq, tcs, kLabelFirst, gseq, r.len);
        if (!postCatalog(c, key, p->pid, r.pseq, tcs, kLabelFirst, gseq, r.len) ||
            !postLabel(c, p->pid, r.pseq, tcs, gseq, kLabelFirst))
            return false;
        c.st->firstLiveCount++;
        c.st->firstLiveBytes += r.len - 4;
        c.st->arrivalsCount++;
        c.e->cFirst.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    if (r.kind == kRowTomb) {
        uint8_t key[kCidKeyLen];
        cidSortKey(r.cid, key);
        StageScratch::TCid* tc = catalogState(c, key);
        if (!tc) return false;
        StageScratch::TCopy* dying = nullptr;
        for (int32_t i = tc->copies; i >= 0; i = sc.tcopies[i].next)
            if (sc.tcopies[i].pid == p->pid && sc.tcopies[i].pseq == r.targetPseq) dying = &sc.tcopies[i];
        if (!dying || !isLive(dying->label)) return true;  // not a labeled PUT copy
        const bool wasFirst = isFirst(dying->label);
        const uint64_t gseq = dying->gseq;
        const uint32_t len = dying->len;
        dying->label = kLabelDead;
        dying->tcs = tcs;
        if (!postCatalog(c, key, p->pid, r.targetPseq, tcs, kLabelDead, gseq, len)) return false;
        if (!wasFirst) return true;
        // A14 promotion: the earliest-labeled live REPEAT keeps the gseq.
        StageScratch::TCopy* heir = nullptr;
        for (int32_t i = tc->copies; i >= 0; i = sc.tcopies[i].next) {
            StageScratch::TCopy& cp = sc.tcopies[i];
            if (cp.label != kLabelRepeat) continue;
            if (!heir || cp.tcs < heir->tcs || (cp.tcs == heir->tcs && (cp.pid < heir->pid ||
                                                (cp.pid == heir->pid && cp.pseq < heir->pseq))))
                heir = &cp;
        }
        if (heir) {
            heir->label = kLabelPromoted;
            heir->tcs = tcs;
            uint8_t rk[8], rv[20];
            putBE64(rk, gseq);
            putBE64(rv, tcs);
            putBE32(rv + 8, heir->pid);
            putBE64(rv + 12, heir->pseq);
            if (!addPosting(c, kIxTypeRehome, rk, 8, rv, 20) ||
                !postCatalog(c, key, heir->pid, heir->pseq, tcs, kLabelPromoted, gseq, heir->len) ||
                !postLabel(c, heir->pid, heir->pseq, tcs, gseq, kLabelFirst))
                return false;
            c.st->firstLiveBytes += int64_t(heir->len) - int64_t(len);
            c.e->cPromotions.fetch_add(1, std::memory_order_relaxed);
        } else {
            c.st->firstLiveCount--;
            c.st->firstLiveBytes -= len - 4;
            // The gseq is gone for good (a returning copy gets a new one).
            uint8_t gk[8], gv[8];
            putBE64(gk, gseq);
            putBE64(gv, tcs);
            if (!addPosting(c, kIxTypeGone, gk, 8, gv, 8)) return false;
        }
        return true;
    }
    return true;  // RETAG, TAG_TOMB, LICENCE, CTL: no type-level effect
}

}  // namespace

void typePostNotice(TypeOwner* t, uint32_t pid) {
    // A25: never blocks; a full queue drops the notice (catch-up compares
    // labeled_through with each partition's durable pseq_hi anyway).
    const uint64_t tail = t->noticeTail.load(std::memory_order_relaxed);
    const uint64_t head = t->noticeHead.load(std::memory_order_acquire);
    if (t->noticeCap && tail - head < t->noticeCap) {
        uint64_t expect = tail;
        if (t->noticeTail.compare_exchange_strong(expect, tail + 1, std::memory_order_acq_rel))
            t->notices[tail % t->noticeCap].store(pid, std::memory_order_release);
        else
            t->noticesDropped.fetch_add(1, std::memory_order_relaxed);
    } else {
        t->noticesDropped.fetch_add(1, std::memory_order_relaxed);
    }
    t->dirty.store(1, std::memory_order_release);
}

void encodeTypeHead(const TypeOwner* t, uint8_t* slot, uint32_t* used, bool durable) {
    TypeHeadFixed h{};
    h.p.magic = kMagicHead;
    h.p.format = kFormat;
    h.p.kind = kHeadType;
    h.p.gen = t->headGen;
    h.p.id = fidU32(t->fid);
    h.p.flags = durable ? uint32_t(kHeadDurableCkpt) : 0u;
    h.commitSeq = t->commitSeq;
    h.gseqHi = t->gseqHi;
    h.gSeg = t->gSeg;
    h.incarnation = t->incarnation;
    h.gLen = t->gLen;
    h.mSeg = t->mSeg;
    h.nextSeg = t->nextSeg;
    h.mEnd = t->mEnd;
    h.arrivalsCount = t->arrivalsCount;
    h.manifestGen = t->manifestGenLoaded;
    h.nL0 = uint16_t(t->nL0);
    h.firstLiveCount = t->firstLiveCount;
    h.firstLiveBytes = t->firstLiveBytes;
    h.nextGen = t->nextGen;
    h.gSegFirstGseq = t->gSegFirstGseq;
    h.tcsHi = t->commitSeq;
    size_t off = sizeof(h);
    std::memcpy(slot + off, t->l0, sizeof(TypeL0DirEntry) * t->nL0);
    off += sizeof(TypeL0DirEntry) * t->nL0;
    // labeled_through lives in each partition (written only by this owner).
    const uint32_t nParts = t->nParts.load(std::memory_order_acquire);
    if (nParts <= kMaxInlineLabels) {
        h.nLabels = uint16_t(nParts);
        for (uint32_t i = 0; i < nParts; i++) {
            const Partition* p = t->partAt(i);
            LabelEntry le{};
            le.pid = p->pid;
            le.labeledThrough = p->labeledThrough.load(std::memory_order_relaxed);
            std::memcpy(slot + off, &le, sizeof(le));
            off += sizeof(le);
        }
    } else {
        h.nLabels = 0xffff;  // labels live in the type log (full checkpoint + deltas)
        h.labelCkptOff = t->labelCkptOff;
        h.labelCkptSeg = t->labelCkptSeg;
    }
    std::memcpy(slot, &h, sizeof(h));
    *used = uint32_t(off + 4);
    sealHeadSlot(slot, *used);
}

int32_t typeWriteHead(Writer* w, TypeOwner* t, bool durable) {
    uint8_t slot[kHeadSlotBytes];
    t->headGen++;
    uint32_t used;
    encodeTypeHead(t, slot, &used, durable);
    return w->io().write(t->h, slot, used, (t->headGen % 2) * kHeadSlotBytes);
}

int32_t typeEnsureFiles(IoCtx* io, Engine* e, TypeOwner* t) {
    int32_t rc;
    if (!t->h.valid()) {
        PathBuf path;
        pathType(&path, e->root().c_str(), t->fid, "h.fsh");
        rc = io->open(path.c_str(), path.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::TypeHead, &t->h);
        if (rc < 0) return rc;
    }
    if (!t->m.valid()) {
        PathBuf path;
        pathTypeSeg(&path, e->root().c_str(), t->fid, 'm', t->mSeg, "fsl");
        rc = io->open(path.c_str(), path.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::TypeMeta, &t->m);
        if (rc < 0) return rc;
        const int64_t sz = io->size(t->m);
        t->mExtent = sz > 0 ? uint64_t(sz) : 0;
        if (t->mExtent < t->mEnd) t->mExtent = t->mEnd;
    }
    if (!t->g.valid()) {
        PathBuf path;
        pathTypeSeg(&path, e->root().c_str(), t->fid, 'g', t->gSeg, "fsg");
        rc = io->open(path.c_str(), path.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::Arrivals, &t->g);
        if (rc < 0) return rc;
        const int64_t sz = io->size(t->g);
        t->gExtent = sz > 0 ? uint64_t(sz) : 0;
        if (t->gExtent < t->gLen) t->gExtent = t->gLen;
    }
    return 0;
}

int32_t typeWarm(Writer* w, TypeOwner* t) {
    if (t->warm) return 0;
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    int32_t rc = typeEnsureFiles(&w->io(), w->engine(), t);
    for (uint32_t i = 0; rc >= 0 && i < t->nL0; i++) {
        const TypeL0DirEntry& de = t->l0[i];
        L0Accel& a = t->acc[i];
        a.mSeg = de.mSeg;
        a.mOff = de.mOff;
        a.l0Off = de.mOff + de.l0Off;
        a.l0Len = de.l0Len;
        a.chainPos = 0;
        a.nKinds = 0;
        FileRef tmp;
        FileRef* f = &t->m;
        if (de.mSeg != t->mSeg) {
            PathBuf path;
            pathTypeSeg(&path, w->eng_root(), t->fid, 'm', de.mSeg, "fsl");
            rc = w->io().open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::TypeMeta, &tmp);
            if (rc < 0) break;
            f = &tmp;
        }
        std::vector<uint8_t> block(de.l0Len);
        if (w->io().read(*f, block.data(), block.size(), a.l0Off) != int64_t(block.size())) {
            rc = FLATSQL_IO_ERR_IO;
        } else {
            L0KindInfo kinds[L0Accel::kMaxKinds];
            size_t nk = 0;
            if (!parseL0Block(block.data(), block.size(), kinds, L0Accel::kMaxKinds, &nk)) {
                rc = FLATSQL_IO_ERR_IO;
            } else {
                for (size_t k = 0; k < nk; k++) {
                    L0Accel::Kind& kk = a.kinds[a.nKinds++];
                    kk.kind = kinds[k].kind;
                    kk.vlen = kinds[k].vlen;
                    kk.n = kinds[k].n;
                    kk.entriesOff = a.l0Off + kinds[k].entriesOff;
                    kk.entriesBytes = kinds[k].entriesBytes;
                    kk.bloom = nullptr;
                    kk.bloomBytes = 0;
                    if (kinds[k].bloomBytes) {
                        uint64_t pos;
                        void* mem = t->chain.alloc(w->engine()->pool(), kinds[k].bloomBytes, &pos);
                        if (mem) {
                            if (!a.chainPos) a.chainPos = pos;
                            std::memcpy(mem, block.data() + kinds[k].bloomOff, kinds[k].bloomBytes);
                            kk.bloom = static_cast<const uint8_t*>(mem);
                            kk.bloomBytes = kinds[k].bloomBytes;
                        }
                    }
                }
            }
        }
        if (tmp.valid()) w->io().close(&tmp);
    }
    for (auto& run : t->runs) {
        if (rc < 0 || run.run) continue;
        PathBuf rp;
        pathTypeRun(&rp, w->eng_root(), t->fid, run.gen);
        if (!run.file.valid()) rc = w->io().open(rp.c_str(), rp.len, FLATSQL_IO_READ, FileClass::Index, &run.file);
        if (rc < 0) break;
        run.run.reset(new L1Run());
        rc = run.run->load(&w->io(), run.file, run.fileLen);
    }
    tHotPathDepth = saved;
    if (rc < 0) return rc;
    t->warm = true;
    return 0;
}

// A15: opens the next arrivals segment (fresh: a seal that never committed may
// have left one behind) and the fence file, and builds the sealed segment's
// fence entry. A failure only postpones the seal.
bool typeStageSeal(Writer* w, TypeOwner* t, StagedType* st) {
    Engine* e = w->engine();
    IoCtx* io = &w->io();
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // host opens (once per seal)
    bool ok = true;
    if (!t->gFence.valid()) {
        PathBuf fp;
        pathType(&fp, e->root().c_str(), t->fid, kArrivalFenceName);
        ok = io->open(fp.c_str(), fp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::Arrivals, &t->gFence) == 0;
    }
    if (ok) {
        io->close(&t->gNext);
        PathBuf gp;
        pathTypeSeg(&gp, e->root().c_str(), t->fid, 'g', t->gSeg + 1, "fsg");
        ok = io->open(gp.c_str(), gp.len,
                      kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::Arrivals, &t->gNext) == 0;
    }
    tHotPathDepth = saved;
    if (!ok) return false;
    ArrivalFence& f = st->fence;
    f = ArrivalFence{};
    f.seg = t->gSeg;
    f.firstGseq = t->gSegFirstGseq;
    f.lastGseq = t->gSegLastGseq;
    f.count = t->gLen / kArrivalBytes;
    f.crc = crc32c(&f, offsetof(ArrivalFence, crc));
    st->fenceOff = t->fenceLen;
    return true;
}

bool typeStage(Writer* w, TypeOwner* t, StageScratch* sc, Arena* frames, Arena* batches) {
    Engine* e = w->engine();
    // Anything to label? (A25: compare with each partition's durable HWM.)
    const uint32_t nParts = t->nParts.load(std::memory_order_acquire);
    bool work = !t->deletes.empty() || t->mergePhase == 1;
    for (uint32_t i = 0; i < nParts && !work; i++) {
        Partition* p = t->partAt(i);
        const uint64_t lt = p->labeledThrough.load(std::memory_order_relaxed);
        if (p->durablePseqHi.load(std::memory_order_acquire) > lt) work = true;
    }
    t->dirty.store(0, std::memory_order_relaxed);
    t->noticeHead.store(t->noticeTail.load(std::memory_order_acquire), std::memory_order_release);
    if (!work) return false;
    // Every type commit adds an L0 block to the directory the head lists; a
    // full directory waits for the merge (its MERGE_DONE batch may pass).
    if (t->nL0 >= kMaxTypeL0Dir - 1 && t->mergePhase != 1) return false;
    if (typeWarm(w, t) < 0) return false;
    StagedType* st = batches->make<StagedType>();
    if (!st) return false;
    resetTypeScratch(*sc);
    st->t = t;
    st->commitSeq = t->commitSeq + 1;
    st->firstLiveCount = t->firstLiveCount;
    st->firstLiveBytes = t->firstLiveBytes;
    st->arrivalsCount = t->arrivalsCount;
    st->gseqHi = t->gseqHi;
    TCtx c{w, e, t, sc, st, &w->io()};
    HotPathScope hot;
    const uint32_t budget = e->config().typeCommitRows;
    uint32_t done = 0;
    st->labels = static_cast<StagedType::Label*>(batches->alloc(sizeof(StagedType::Label) * (nParts + 1), 8));
    if (!st->labels) return false;
    for (uint32_t i = 0; i < nParts && done < budget && !c.err; i++) {
        Partition* p = t->partAt(i);
        const uint64_t lt = p->labeledThrough.load(std::memory_order_relaxed);
        const uint64_t hi = p->durablePseqHi.load(std::memory_order_acquire);
        if (hi <= lt) continue;
        // Published batch directory of the partition (seqlock).
        PublishedPart pub;
        uint32_t s;
        do {
            s = p->pubLock.readBegin();
            pub.commitSeq = p->pub.commitSeq;
            pub.pseqHi = p->pub.pseqHi;
            pub.nL0 = p->pub.nL0;
            std::memcpy(pub.l0, p->pub.l0, sizeof(L0DirEntry) * (pub.nL0 <= kMaxL0Dir ? pub.nL0 : 0));
        } while (p->pubLock.readRetry(s));
        const uint64_t upTo = std::min(hi, pub.pseqHi);
        uint64_t through = lt;
        for (uint32_t b = 0; b < pub.nL0 && done < budget && !c.err; b++) {
            const L0DirEntry& de = pub.l0[b];
            const uint64_t bFirst = de.firstPseq;
            const uint64_t bLast = de.firstPseq + de.nRows - 1;
            if (bLast <= through) continue;
            if (bFirst > through + 1) {
                // Rows before this batch are not in the directory (merged
                // before labeling): cannot happen, merges wait for labels.
                c.err = FLATSQL_IO_ERR_IO;
                break;
            }
            if (bFirst > upTo) break;
            const uint64_t from = through + 1;
            uint64_t to = std::min(bLast, upTo);
            if (to - from + 1 > StageScratch::kTRowCap) to = from + StageScratch::kTRowCap - 1;
            if (done + (to - from + 1) > budget && done > 0) break;
            // Read the rows (type owner's own read handle on m-<seg>).
            const uint64_t hkey = (uint64_t(p->pid) << 32) | de.mSeg;
            FileRef* f = nullptr;
            auto fit = t->partM.find(hkey);
            if (fit != t->partM.end()) {
                f = &fit->second;
            } else {
                const int saved = tHotPathDepth;
                tHotPathDepth = 0;
                PathBuf path;
                pathPartitionSeg(&path, w->eng_root(), p->pid, 'm', de.mSeg, "fsl");
                FileRef handle;
                const int32_t orc = w->io().open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::Meta, &handle);
                if (orc >= 0) f = &t->partM.emplace(hkey, handle).first->second;
                tHotPathDepth = saved;
                if (orc < 0) {
                    c.err = orc;
                    break;
                }
            }
            const size_t nRows = size_t(to - from + 1);
            const uint64_t off = de.mOff + sizeof(BatchHeader) + (from - bFirst) * sizeof(RecRow);
            const int64_t n = w->io().read(*f, sc->trows, nRows * sizeof(RecRow), off);
            if (n != int64_t(nRows * sizeof(RecRow))) {
                c.err = n < 0 ? int32_t(n) : FLATSQL_IO_ERR_IO;
                break;
            }
            size_t r = 0;
            for (; r < nRows; r++) {
                if (sc->trows[r].pseq != from + r) {
                    c.err = FLATSQL_IO_ERR_IO;
                    break;
                }
                if (!labelRow(c, p, sc->trows[r])) break;  // scratch full or I/O error
            }
            if (r > 0) through = from + r - 1;
            done += uint32_t(r);
            if (r < nRows) break;
        }
        if (c.err) break;
        if (through > lt) st->labels[st->nLabels++] = {p, through};
    }
    // Type-level deletes (A14): every live copy after labeling.
    if (!c.err) {
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;
        std::vector<TypeOwner::Delete> keep;
        for (auto& d : t->deletes) {
            uint8_t key[kCidKeyLen];
            cidSortKey(d.cid, key);
            StageScratch::TCid* tc = catalogState(c, key);
            if (!tc) {
                keep.push_back(d);
                continue;
            }
            int32_t copies = 0;
            for (int32_t i = tc->copies; i >= 0; i = sc->tcopies[i].next)
                if (isLive(sc->tcopies[i].label)) copies++;
            if (d.remaining) d.remaining->fetch_add(copies, std::memory_order_acq_rel);
            for (int32_t i = tc->copies; i >= 0; i = sc->tcopies[i].next) {
                if (!isLive(sc->tcopies[i].label)) continue;
                Partition* p = e->partition(sc->tcopies[i].pid);
                Cmd cmd;
                cmd.kind = kCmdKillCid;
                cmd.a = sc->tcopies[i].pid;
                cmd.ticket = d.remaining;
                std::memcpy(cmd.data, d.cid, kCidLen);
                const uint32_t owner = p ? p->ownerWriter.load(std::memory_order_acquire) : 0;
                while (!e->writer(owner)->mailbox().push(cmd)) cpuRelax();
                e->writer(owner)->ring();
            }
            if (d.remaining && d.remaining->fetch_sub(1, std::memory_order_acq_rel) == 1)
                wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(d.remaining), -1);
        }
        t->deletes.swap(keep);
        tHotPathDepth = saved;
    }
    if (!c.err && t->mergePhase == 1) st->mergeDone = true;
    if (c.err || (st->nLabels == 0 && sc->nArrivals == 0 && sc->nEntries == 0 && !st->mergeDone)) return false;
    // Arrivals bytes (frames arena).
    const uint32_t gBytes = sc->nArrivals * kArrivalBytes;
    if (gBytes) {
        st->arrivals = static_cast<uint8_t*>(frames->alloc(gBytes, 8));
        if (!st->arrivals) return false;
        std::memcpy(st->arrivals, sc->arrivals, gBytes);
    }
    st->nArrivals = sc->nArrivals;
    st->gOff = t->gLen;
    st->gSeg = t->gSeg;
    st->gSeal = false;
    st->lastGseq = 0;
    if (gBytes) {
        ArrivalEntry lastEntry;
        std::memcpy(&lastEntry, st->arrivals + gBytes - kArrivalBytes, sizeof(lastEntry));
        st->lastGseq = lastEntry.gseq;
    }
    if (gBytes && t->gLen > 0 && t->gLen + gBytes > e->config().arrivalsSegBytes &&
        typeStageSeal(w, t, st)) {
        // A15: this batch's arrivals open segment gSeg + 1; the fence entry of
        // the sealed segment is synced with them in round 1.
        st->gSeal = true;
        st->gSeg = t->gSeg + 1;
        st->gOff = 0;
    }
    // Type batch (A10).
    for (uint32_t i = 0; i < sc->nEntries; i++) sc->order[i] = &sc->entries[i];
    sortStaged(sc->order, sc->nEntries);
    const size_t l0Len = sc->nEntries ? l0BlockSize(sc->order, sc->nEntries) : 0;
    const bool full = nParts > kMaxInlineLabels &&
                      (t->labelCkptOff == 0 || t->metaSinceCkpt >= e->config().ckptMetaBytes);
    const uint32_t nLabelEntries = full ? nParts : st->nLabels;
    const size_t labelsLen = size_t(nLabelEntries) * sizeof(LabelEntry);
    const size_t batchLen = sizeof(TypeBatchHeader) + labelsLen + l0Len + 8;
    uint8_t* b = static_cast<uint8_t*>(batches->alloc(batchLen, 8));
    if (!b) return false;
    TypeBatchHeader h{};
    h.magic = kMagicTypeBatch;
    h.ver = 1;
    h.flags = uint16_t((full ? kTypeFlagFullLabels : 0) | (st->mergeDone ? kTypeFlagMergeDone : 0) |
                       (e->config().commitJournal ? kTypeBatchJournaled : 0));
    if (st->mergeDone) {
        h.mergeGen = t->mergeGen;
        h.mergedThroughCommit = t->mergeThroughCommit;
    }
    h.commitSeq = st->commitSeq;
    h.firstGseq = st->firstGseq;
    h.nArrivals = st->nArrivals;
    h.gSeg = st->gSeg;
    h.gOff = st->gOff;
    h.gCrc = gBytes ? crc32c(st->arrivals, gBytes) : 0;
    h.nLabel = nLabelEntries;
    h.gseqHi = st->gseqHi;
    h.batchLen = uint32_t(batchLen);
    h.l0Off = uint32_t(sizeof(TypeBatchHeader) + labelsLen);
    h.incarnation = e->incarnation();
    h.firstLiveCount = st->firstLiveCount;
    h.firstLiveBytes = st->firstLiveBytes;
    h.arrivalsCount = st->arrivalsCount;
    std::memcpy(b, &h, sizeof(h));
    size_t off = sizeof(h);
    auto putLabel = [&](uint32_t pid, uint64_t through) {
        LabelEntry le{};
        le.pid = pid;
        le.labeledThrough = through;
        std::memcpy(b + off, &le, sizeof(le));
        off += sizeof(le);
    };
    if (full) {
        // Every partition of the type, with this batch's labels applied.
        for (uint32_t i = 0; i < nParts; i++) {
            const Partition* p = t->partAt(i);
            uint64_t through = p->labeledThrough.load(std::memory_order_relaxed);
            for (uint32_t k = 0; k < st->nLabels; k++)
                if (st->labels[k].p == p) through = st->labels[k].through;
            putLabel(p->pid, through);
        }
    } else {
        for (uint32_t i = 0; i < st->nLabels; i++) putLabel(st->labels[i].p->pid, st->labels[i].through);
    }
    st->l0Off = uint32_t(off);
    st->l0Len = uint32_t(l0Len);
    if (l0Len) off += writeL0Block(b + off, sc->order, sc->nEntries, st->commitSeq, st->commitSeq);
    putU32(b + off, crc32c(b, off));
    putU32(b + off + 4, 0);
    st->batch = b;
    st->batchLen = uint32_t(batchLen);
    st->mOff = t->mEnd;
    st->mSeg = t->mSeg;
    t->st = st;
    return true;
}

void typePublish(Writer* w, TypeOwner* t, StagedType* st) {
    Engine* e = w->engine();
    t->commitSeq = st->commitSeq;
    t->gseqHi = st->gseqHi;
    if (st->gSeal) {
        w->io().close(&t->g);
        t->g = t->gNext;
        t->gNext = FileRef();
        t->gExtent = 0;
        t->gSeg = st->gSeg;
        t->fenceLen = st->fenceOff + sizeof(ArrivalFence);
    }
    if (st->nArrivals) {
        if (st->gOff == 0) t->gSegFirstGseq = st->firstGseq;
        t->gSegLastGseq = st->lastGseq;
    }
    t->gLen = st->gOff + uint64_t(st->nArrivals) * kArrivalBytes;
    t->mEnd = st->mOff + st->batchLen;
    t->arrivalsCount = st->arrivalsCount;
    t->firstLiveCount = st->firstLiveCount;
    t->firstLiveBytes = st->firstLiveBytes;
    t->incarnation = e->incarnation();
    t->metaSinceCkpt += st->batchLen;
    TypeBatchHeader h;
    std::memcpy(&h, st->batch, sizeof(h));
    if (h.flags & kTypeFlagFullLabels) {
        t->labelCkptOff = st->mOff;
        t->labelCkptSeg = st->mSeg;
        t->metaSinceCkpt = 0;
    }
    for (uint32_t i = 0; i < st->nLabels; i++)
        st->labels[i].p->labeledThrough.store(st->labels[i].through, std::memory_order_release);
    if (st->mergeDone) {
        // MERGE_DONE durable: the run replaces the merged L0 blocks and the
        // folded runs.
        for (uint32_t i = 0; i < t->mergeFold && !t->runs.empty(); i++) {
            w->io().close(&t->runs.back().file);
            t->runs.pop_back();
        }
        t->runs.push_back(std::move(t->mergeRun));
        t->mergeRun = SegRun();
        w->io().close(&t->mergeMf);
        t->manifestGenLoaded = t->mergeGen;
        const uint32_t k = t->mergeK;
        const uint32_t remaining = t->nL0 - k;
        for (uint32_t i = 0; i < remaining; i++) {
            t->l0[i] = t->l0[i + k];
            t->acc[i] = t->acc[i + k];
        }
        t->nL0 = remaining;
        if (remaining) {
            uint64_t minPos = UINT64_MAX;
            for (uint32_t i = 0; i < remaining; i++)
                if (t->acc[i].chainPos && t->acc[i].chainPos < minPos) minPos = t->acc[i].chainPos;
            if (minPos != UINT64_MAX) t->chain.freeBefore(e->pool(), minPos);
        } else {
            t->chain.freeAll(e->pool());
        }
        t->mergePhase = 0;
    }
    if (st->l0Len) {
        const uint32_t idx = t->nL0;
        if (idx < kMaxTypeL0Dir) {
            TypeL0DirEntry& de = t->l0[idx];
            de.mSeg = st->mSeg;
            de.l0Len = st->l0Len;
            de.mOff = st->mOff;
            de.commitSeq = st->commitSeq;
            de.batchLen = st->batchLen;
            de.l0Off = st->l0Off;
            L0Accel& a = t->acc[idx];
            a.mSeg = st->mSeg;
            a.mOff = st->mOff;
            a.l0Off = st->mOff + st->l0Off;
            a.l0Len = st->l0Len;
            a.chainPos = 0;
            a.nKinds = 0;
            L0KindInfo kinds[L0Accel::kMaxKinds];
            size_t nk = 0;
            if (parseL0Block(st->batch + st->l0Off, st->l0Len, kinds, L0Accel::kMaxKinds, &nk)) {
                for (size_t k = 0; k < nk; k++) {
                    L0Accel::Kind& kk = a.kinds[a.nKinds++];
                    kk.kind = kinds[k].kind;
                    kk.vlen = kinds[k].vlen;
                    kk.n = kinds[k].n;
                    kk.entriesOff = a.l0Off + kinds[k].entriesOff;
                    kk.entriesBytes = kinds[k].entriesBytes;
                    kk.bloom = nullptr;
                    kk.bloomBytes = 0;
                    if (kinds[k].bloomBytes) {
                        uint64_t pos;
                        void* mem = t->chain.alloc(e->pool(), kinds[k].bloomBytes, &pos);
                        if (mem) {
                            if (!a.chainPos) a.chainPos = pos;
                            std::memcpy(mem, st->batch + st->l0Off + kinds[k].bloomOff, kinds[k].bloomBytes);
                            kk.bloom = static_cast<const uint8_t*>(mem);
                            kk.bloomBytes = kinds[k].bloomBytes;
                        }
                    }
                }
            }
            t->nL0++;
        }
    }
    t->publishedGseqHi.store(t->gseqHi, std::memory_order_release);
    t->publishedArrivals.store(t->arrivalsCount, std::memory_order_release);
    for (uint32_t i = 0; i < st->nTickets; i++)
        if (st->tickets[i]->fetch_sub(1, std::memory_order_acq_rel) == 1)
            wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(st->tickets[i]), -1);
    e->cTypeCommits.fetch_add(1, std::memory_order_relaxed);
    t->st = nullptr;
}

void typeRollback(Writer* w, TypeOwner* t, StagedType* st) {
    // A staged seal is retried by a later batch (the next segment is reopened
    // fresh, the fence entry rewritten at the same offset).
    if (st->gSeal) w->io().close(&t->gNext);
    // Allocated gseqs were never published; they are simply not used (A6).
    t->st = nullptr;
}

// Type merge pipeline: builds one catalog run over the unmerged type L0
// blocks between rounds; the run and manifest are synced in the next round's
// first phase and MERGE_DONE rides that round's type batch. Outputs are named
// by the generation counter (never reused once a head records it).
namespace {
// Builds a planned catalog merge's run and manifest (no sync). Reads only the
// plan and immutable files: may run on a helper thread.
int32_t writeTypeMergeOutputs(IoCtx* io, const char* root, TypeOwner* t) {
    const uint32_t k = t->mergeK;
    const uint32_t gen = t->mergeGen;
    std::vector<std::vector<uint8_t>> blocks(k);
    int32_t rc = 0;
    std::unordered_map<uint32_t, FileRef> mfiles;
    for (uint32_t i = 0; i < k && rc >= 0; i++) {
        const TypeL0DirEntry& de = t->mergeBatches[i];
        auto it = mfiles.find(de.mSeg);
        if (it == mfiles.end()) {
            PathBuf path;
            pathTypeSeg(&path, root, t->fid, 'm', de.mSeg, "fsl");
            FileRef f;
            rc = io->open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::TypeMeta, &f);
            if (rc < 0) break;
            it = mfiles.emplace(de.mSeg, f).first;
        }
        blocks[i].resize(de.l0Len);
        if (io->read(it->second, blocks[i].data(), de.l0Len, de.mOff + de.l0Off) != int64_t(de.l0Len))
            rc = FLATSQL_IO_ERR_IO;
    }
    for (auto& kv : mfiles) io->close(&kv.second);
    if (rc < 0) return rc;
    PathBuf xp;
    pathTypeRun(&xp, root, t->fid, gen);
    SegRun& run = t->mergeRun;
    run.gen = gen;
    rc = io->open(xp.c_str(), xp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                  FileClass::Index, &run.file);
    if (rc < 0) return rc;
    std::vector<MergeL0Input> l0s;
    for (uint32_t i = 0; i < k; i++) l0s.push_back({blocks[i].data(), blocks[i].size()});
    uint64_t nEntries = 0;
    const int64_t xLen = mergeToL1(io, run.file, 0, gen, uint16_t(t->mergeFold ? 1 : 0), t->mergeBatches[0].commitSeq,
                                   t->mergeBatches[k - 1].commitSeq, l0s, t->mergeFoldRuns, &nEntries);
    if (xLen < 0) return int32_t(xLen);
    run.fileLen = uint64_t(xLen);
    std::vector<uint8_t> man(16);
    putU32(man.data(), kMagicManifest);
    putU32(man.data() + 4, uint32_t(t->mergeKeepRuns.size() + 1));
    putU32(man.data() + 8, gen);
    putU32(man.data() + 12, 0);
    auto addRun = [&](uint32_t g, uint64_t len) {
        const size_t at = man.size();
        man.resize(at + 16);
        putU32(man.data() + at, g);
        putU32(man.data() + at + 4, 0);
        putU64(man.data() + at + 8, len);
    };
    for (const auto& r : t->mergeKeepRuns) addRun(r.first, r.second);
    addRun(gen, run.fileLen);
    const size_t at = man.size();
    man.resize(at + 8, 0);
    putU32(man.data() + at, crc32c(man.data(), at));
    PathBuf mp;
    char name[32];
    snprintf(name, sizeof(name), "mf-%06x.fsm", gen);
    pathType(&mp, root, t->fid, name);
    rc = io->open(mp.c_str(), mp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                  FileClass::Manifest, &t->mergeMf);
    if (rc >= 0) rc = io->write(t->mergeMf, man.data(), man.size(), 0);
    // Outputs are durable before MERGE_DONE is staged (off the commit path).
    if (rc >= 0) rc = io->sync(run.file);
    if (rc >= 0) rc = io->sync(t->mergeMf);
    if (rc >= 0) {
        run.run.reset(new L1Run());
        rc = run.run->load(io, run.file, run.fileLen);
    }
    return rc;
}
}  // namespace

// Type merge pipeline: planned on the owner, built on a helper (or inline),
// synced in the next round's first phase; MERGE_DONE rides that round's type
// batch. Outputs are named by the generation counter.
int32_t typeMergeStep(Writer* w, TypeOwner* t) {
    Engine* e = w->engine();
    if (t->mergePhase == 2) {
        const int32_t res = t->mergeResult.load(std::memory_order_acquire);
        if (res == 0) return 0;
        if (res < 0) {
            w->io().close(&t->mergeRun.file);
            w->io().close(&t->mergeMf);
            t->mergeRun = SegRun();
            t->mergePhase = 0;
            return res;
        }
        t->mergePhase = 1;
        t->dirty.store(1, std::memory_order_release);
        return 1;
    }
    if (t->mergePhase != 0) return 0;
    uint64_t bytes = 0;
    for (uint32_t i = 0; i < t->nL0; i++) bytes += t->l0[i].batchLen;
    // Type commits are frequent and small (a few labels each): the block
    // count alone triggers a merge, well before the directory limit makes
    // labeling wait (typeStage).
    const bool want = t->nL0 >= e->config().mergeL0Blocks || bytes >= e->config().mergeL0Bytes;
    if (!want) return 0;
    int32_t rc = typeWarm(w, t);
    if (rc < 0) return rc;
    const uint32_t k = t->nL0;
    t->mergeK = k;
    t->mergeGen = t->nextGen++;
    t->mergeThroughCommit = t->l0[k - 1].commitSeq;
    t->mergeBatches.assign(t->l0, t->l0 + k);
    uint64_t newEntries = 1;
    for (uint32_t i = 0; i < k; i++)
        for (int j = 0; j < t->acc[i].nKinds; j++) newEntries += t->acc[i].kinds[j].n;
    uint32_t cand = 0;
    uint64_t acc = newEntries;
    // Folds are capped as for partitions: labeling waits on this merge.
    const uint64_t foldCap = e->config().mergeFoldMaxEntries;
    for (size_t i = t->runs.size(); i-- > 0;) {
        if (!t->runs[i].run || t->runs[i].run->entries() > 2 * acc || acc + t->runs[i].run->entries() > foldCap)
            break;
        acc += t->runs[i].run->entries();
        cand++;
    }
    t->mergeFold = cand >= 3 ? cand : 0;  // tiered, fanout 4
    t->mergeFoldRuns.clear();
    t->mergeKeepRuns.clear();
    for (size_t i = 0; i < t->runs.size(); i++) {
        if (i + t->mergeFold >= t->runs.size()) t->mergeFoldRuns.push_back({t->runs[i].run.get(), t->runs[i].file});
        else t->mergeKeepRuns.push_back({t->runs[i].gen, t->runs[i].fileLen});
    }
    t->runs.reserve(t->runs.size() + 1);
    t->mergeRun = SegRun();
    t->mergeResult.store(0, std::memory_order_release);
    t->mergePhase = 2;
    if (e->config().mergeHelpers && !e->config().cooperative) {
        Writer* owner = w;
        e->submitMaintenance(true, [t, owner](IoCtx* io) {
            const int32_t r = writeTypeMergeOutputs(io, owner->eng_root(), t);
            t->mergeResult.store(r < 0 ? r : 1, std::memory_order_release);
            owner->ring();
        });
        return 1;
    }
    rc = writeTypeMergeOutputs(&w->io(), w->eng_root(), t);
    t->mergeResult.store(rc < 0 ? rc : 1, std::memory_order_release);
    return typeMergeStep(w, t);
}

}  // namespace ps
}  // namespace flatsql
