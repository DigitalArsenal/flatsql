// FlatSQL partition store: the partition log (design §4.2-§4.6, §6, A2, A4,
// A19). Staging turns ring entries into rows, attributes and postings in the
// writer's scratch; finalize lays out one meta batch; publish applies a
// durable commit to the partition's in-memory state and releases acks.
//
// Everything in this file that runs per ring entry is allocation-free: rows,
// postings and keys live in StageScratch, frames and batches in the writer's
// arenas, bloom accelerators in the partition's SlabChain.
#include <algorithm>

#include "flatsql/ps/flatsql_attr_generated.h"
#include "internal.h"

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// RecordAttr
// ---------------------------------------------------------------------------
namespace {
void viewStr(const flatbuffers::String* s, const uint8_t** p, size_t* n) {
    if (s) {
        *p = reinterpret_cast<const uint8_t*>(s->data());
        *n = s->size();
    } else {
        *p = reinterpret_cast<const uint8_t*>("");
        *n = 0;
    }
}
uint64_t mixField(uint64_t h, const uint8_t* p, size_t n) {
    uint8_t len[4];
    putU32(len, uint32_t(n));
    h = hash64(len, 4, h);
    return hash64(p, n, h);
}
bool eqField(const uint8_t* a, size_t an, const uint8_t* b, size_t bn) {
    return an == bn && (an == 0 || std::memcmp(a, b, an) == 0);
}
}  // namespace

bool parseAttr(const uint8_t* attr, size_t len, AttrView* out) {
    *out = AttrView();
    if (len == 0) {
        out->valid = true;
        return true;
    }
    flatbuffers::Verifier v(attr, len, 16, 1024);
    if (!fb::VerifyRecordAttrBuffer(v)) return false;
    const fb::RecordAttr* ra = fb::GetRecordAttr(attr);
    const auto* tags = ra->tags();
    out->nTags = tags ? int(tags->size()) : 0;
    if (out->nTags > 1) return false;
    if (out->nTags == 1) {
        const fb::SourceTag* t = tags->Get(0);
        TagView& tv = out->tag;
        tv.present = true;
        viewStr(t->provider_id(), &tv.provider, &tv.providerLen);
        viewStr(t->source_name(), &tv.source, &tv.sourceLen);
        viewStr(t->source_url(), &tv.url, &tv.urlLen);
        viewStr(t->batch_id(), &tv.batch, &tv.batchLen);
        viewStr(t->content_key_id(), &tv.contentKey, &tv.contentKeyLen);
        viewStr(t->producer_peer_id(), &tv.peer, &tv.peerLen);
        viewStr(t->producer_public_key(), &tv.pubkey, &tv.pubkeyLen);
    }
    if (ra->supersede_key()) {
        out->hasSupersedeKey = true;
        viewStr(ra->supersede_key(), &out->supersedeKey, &out->supersedeKeyLen);
    }
    if (ra->licence_key()) viewStr(ra->licence_key(), &out->licenceKey, &out->licenceKeyLen);
    out->valid = true;
    return true;
}

uint64_t tagTupleHash(const TagView& t) {
    if (!t.present) return 0;
    uint64_t h = 0x7461677475706c65ull;
    h = mixField(h, t.provider, t.providerLen);
    h = mixField(h, t.source, t.sourceLen);
    h = mixField(h, t.batch, t.batchLen);
    h = mixField(h, t.contentKey, t.contentKeyLen);
    h = mixField(h, t.peer, t.peerLen);
    h = mixField(h, t.pubkey, t.pubkeyLen);
    return h ? h : 1;
}

bool tagTupleEqual(const TagView& a, const TagView& b) {
    return a.present == b.present && eqField(a.provider, a.providerLen, b.provider, b.providerLen) &&
           eqField(a.source, a.sourceLen, b.source, b.sourceLen) &&
           eqField(a.batch, a.batchLen, b.batch, b.batchLen) &&
           eqField(a.contentKey, a.contentKeyLen, b.contentKey, b.contentKeyLen) &&
           eqField(a.peer, a.peerLen, b.peer, b.peerLen) &&
           eqField(a.pubkey, a.pubkeyLen, b.pubkey, b.pubkeyLen);
}

uint64_t laneHash(const TagView& t) {
    uint64_t h = 0x6c616e65ull;
    h = mixField(h, t.provider, t.providerLen);
    h = mixField(h, t.source, t.sourceLen);
    h = mixField(h, t.batch, t.batchLen);
    h = mixField(h, t.peer, t.peerLen);
    h = mixField(h, t.pubkey, t.pubkeyLen);
    return h;
}

// ---------------------------------------------------------------------------
// StageScratch
// ---------------------------------------------------------------------------
namespace {
template <typename T>
T* callocArray(size_t n) {
    void* p = std::calloc(n, sizeof(T));
    return static_cast<T*>(p);
}
}  // namespace

bool StageScratch::init(const EngineConfig& cfg) {
    capRows = cfg.commitFrames * 3 + cfg.reconcileStep * 2 + 512;
    capAttrs = uint32_t(std::max<uint64_t>(cfg.commitBytes / 2, 1u << 20));
    capEntries = capRows * 16;
    capKeys = capEntries * 40;
    capPlain = uint32_t(cfg.maxEntryBytes + 4096);
    capExtract = 64 * 1024;
    capSection = 1u << 20;
    rows = callocArray<RecRow>(capRows);
    attrs = callocArray<uint8_t>(capAttrs);
    entries = callocArray<StagedEntry>(capEntries);
    order = callocArray<StagedEntry*>(capEntries);
    keys = callocArray<uint8_t>(capKeys);
    plain = callocArray<uint8_t>(capPlain);
    extract = callocArray<uint8_t>(capExtract);
    section = callocArray<uint8_t>(capSection);
    cids = callocArray<Cid>(kCidCap);
    cidBuckets = callocArray<int32_t>(kCidBuckets);
    deadKeys = callocArray<uint64_t>(kDeadCap * 2);
    deadVals = callocArray<uint64_t>(kDeadCap * 2);
    instPut = callocArray<uint64_t>(kInstCap);
    instHash = callocArray<uint64_t>(kInstCap);
    instPseq = callocArray<uint64_t>(kInstCap);
    tcids = callocArray<TCid>(kTCidCap);
    tcidBuckets = callocArray<int32_t>(kCidBuckets);
    tcopies = callocArray<TCopy>(kTCopyCap);
    arrivals = callocArray<ArrivalEntry>(kArrivalCap);
    trows = callocArray<RecRow>(kTRowCap);
    if (!tcids || !tcidBuckets || !tcopies || !arrivals || !trows) return false;
    for (uint32_t i = 0; i < kCidBuckets; i++) tcidBuckets[i] = -1;
    if (!rows || !attrs || !entries || !order || !keys || !plain || !extract || !section ||
        !cids || !cidBuckets || !deadKeys || !deadVals || !instPut || !instHash || !instPseq)
        return false;
    for (uint32_t i = 0; i < kCidBuckets; i++) cidBuckets[i] = -1;
    return true;
}

void StageScratch::freeAll() {
    std::free(rows);
    std::free(attrs);
    std::free(entries);
    std::free(order);
    std::free(keys);
    std::free(plain);
    std::free(extract);
    std::free(section);
    std::free(cids);
    std::free(cidBuckets);
    std::free(deadKeys);
    std::free(deadVals);
    std::free(instPut);
    std::free(instHash);
    std::free(instPseq);
    std::free(tcids);
    std::free(tcidBuckets);
    std::free(tcopies);
    std::free(arrivals);
    std::free(trows);
    rows = nullptr;
}

void StageScratch::resetPartition() {
    nRows = 0;
    attrBytes = 0;
    nAttr = 0;
    nEntries = 0;
    keyBytes = 0;
    for (uint32_t i = 0; i < nCids; i++) {
        const uint32_t b = uint32_t(hash64(cids[i].key, kCidKeyLen) % kCidBuckets);
        cidBuckets[b] = -1;
    }
    nCids = 0;
    if (nDead) {
        std::memset(deadKeys, 0, sizeof(uint64_t) * kDeadCap * 2);
        nDead = 0;
    }
    nInst = 0;
    nDeltas = 0;
    ctlBytes = 0;
    nCtl = 0;
    laneFrameBytes = 0;
    firstNewLaneIndex = 0;
}

// ---------------------------------------------------------------------------
// Staging context and lookups
// ---------------------------------------------------------------------------
namespace {

struct Ctx {
    Writer* w;
    Engine* e;
    Partition* p;
    StageScratch* sc;
    Staged* st;
    IoCtx* io;
    Arena* frames;
    int32_t err = 0;
};

inline void be8(uint8_t* out, uint64_t v) { putBE64(out, v); }

bool addPosting(Ctx& c, uint16_t kind, const uint8_t* key, size_t klen, const uint8_t* val,
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

bool addPseqPosting(Ctx& c, uint16_t kind, const uint8_t* key, size_t klen, uint64_t pseq) {
    uint8_t v[8];
    be8(v, pseq);
    return addPosting(c, kind, key, klen, v, 8);
}

// (a, b) composite of two strings, order-preserving.
size_t encStr2(uint8_t* out, const uint8_t* a, size_t an, const uint8_t* b, size_t bn) {
    size_t n = 0;
    for (size_t i = 0; i < an; i++) {
        out[n++] = a[i];
        if (a[i] == 0) out[n++] = 0xff;
    }
    out[n++] = 0;
    out[n++] = 1;
    for (size_t i = 0; i < bn; i++) {
        out[n++] = b[i];
        if (b[i] == 0) out[n++] = 0xff;
    }
    return n;
}

// Staged kills: open addressing over target pseq.
bool stagedDeadGet(const StageScratch& sc, uint64_t target, uint64_t* killer) {
    if (sc.nDead == 0) return false;
    const uint32_t cap = StageScratch::kDeadCap * 2;
    uint32_t i = uint32_t(hash64(&target, 8) % cap);
    while (sc.deadKeys[i]) {
        if (sc.deadKeys[i] == target) {
            if (killer) *killer = sc.deadVals[i];
            return true;
        }
        i = (i + 1) % cap;
    }
    return false;
}

bool stagedDeadPut(StageScratch& sc, uint64_t target, uint64_t killer) {
    if (sc.nDead >= StageScratch::kDeadCap) return false;
    const uint32_t cap = StageScratch::kDeadCap * 2;
    uint32_t i = uint32_t(hash64(&target, 8) % cap);
    while (sc.deadKeys[i] && sc.deadKeys[i] != target) i = (i + 1) % cap;
    if (!sc.deadKeys[i]) sc.nDead++;
    sc.deadKeys[i] = target;
    sc.deadVals[i] = killer;
    return true;
}

FileRef* mHandleFor(Ctx& c, uint32_t mSeg);
FileRef* segHandle(Ctx& c, uint32_t seg, char letter);

// Reads a kind section of an unmerged L0 block and visits entries
// (key, klen, val). Chunked, so any section size works.
template <typename F>
int32_t scanL0Section(Ctx& c, const L0Accel& a, const L0Accel::Kind& k, F&& visit) {
    FileRef* f = mHandleFor(c, a.mSeg);
    if (!f) return FLATSQL_IO_ERR_IO;
    uint64_t off = 0;
    const uint64_t total = k.entriesBytes;
    uint8_t* buf = c.sc->section;
    const uint64_t cap = c.sc->capSection;
    while (off < total) {
        const uint64_t want = std::min<uint64_t>(cap, total - off);
        const int64_t n = c.io->read(*f, buf, size_t(want), k.entriesOff + off);
        if (n != int64_t(want)) return n < 0 ? int32_t(n) : FLATSQL_IO_ERR_IO;
        EntryIter it;
        it.p = buf;
        it.end = buf + want;
        it.vlen = k.vlen;
        const uint8_t *ek, *ev;
        uint16_t el;
        const uint8_t* last = buf;
        while (it.next(&ek, &el, &ev)) {
            if (!visit(ek, el, ev)) return 0;
            last = it.p;
        }
        const uint64_t consumed = uint64_t(last - buf);
        if (consumed == 0) return FLATSQL_IO_ERR_IO;  // one entry larger than the buffer
        off += consumed;
    }
    return 0;
}

// Visits every committed posting value for (kind, key): unmerged L0 blocks
// (bloom-gated) and L1 runs (bloom-gated). visit(val) -> continue?
template <typename F>
int32_t committedPostings(Ctx& c, uint16_t kind, const uint8_t* key, size_t klen, F&& visit) {
    Partition* p = c.p;
    bool stop = false;
    const uint64_t h = bloomHash(key, klen);
    for (uint32_t i = 0; i < p->nL0 && !stop; i++) {
        const L0Accel& a = p->acc[i];
        const L0Accel::Kind* k = a.find(kind);
        if (!k || k->n == 0) continue;
        if (k->bloom && !bloomTestHash(k->bloom, k->bloomBytes, h)) continue;
        const int32_t rc = scanL0Section(c, a, *k, [&](const uint8_t* ek, uint16_t el, const uint8_t* ev) {
            const int cmp = keyCmp(ek, el, key, klen);
            if (cmp < 0) return true;
            if (cmp > 0) return false;
            if (!visit(ev)) {
                stop = true;
                return false;
            }
            return true;
        });
        if (rc < 0) return rc;
    }
    for (auto& si : p->segs) {
        if (stop) break;
        for (auto& run : si.runs) {
            if (stop) break;
            if (!run.run || !run.run->mayContainHash(kind, h)) continue;
            const int64_t rc = run.run->lookup(c.io, run.file, kind, key, klen, c.w->lookupScratch(),
                                               [&](const uint8_t*, uint16_t, const uint8_t* ev) {
                                                   if (!stop && !visit(ev)) stop = true;
                                               });
            if (rc < 0) return int32_t(rc);
        }
    }
    return 0;
}

// Visits committed postings whose key starts with prefix: visit(key, klen, val).
template <typename F>
int32_t committedPrefix(Ctx& c, uint16_t kind, const uint8_t* prefix, size_t plen, F&& visit) {
    Partition* p = c.p;
    for (uint32_t i = 0; i < p->nL0; i++) {
        const L0Accel& a = p->acc[i];
        const L0Accel::Kind* k = a.find(kind);
        if (!k || k->n == 0) continue;
        const int32_t rc = scanL0Section(c, a, *k, [&](const uint8_t* ek, uint16_t el, const uint8_t* ev) {
            if (el < plen) return keyCmp(ek, el, prefix, plen) < 0;
            const int cmp = std::memcmp(ek, prefix, plen);
            if (cmp < 0) return true;
            if (cmp > 0) return false;
            visit(ek, el, ev);
            return true;
        });
        if (rc < 0) return rc;
    }
    uint8_t hi[64];
    if (plen > sizeof(hi)) return FLATSQL_IO_ERR_GENERIC;
    // Upper bound: prefix with its last non-0xff byte incremented.
    std::memcpy(hi, prefix, plen);
    size_t hil = plen;
    while (hil && hi[hil - 1] == 0xff) hil--;
    const bool bounded = hil > 0;
    if (bounded) hi[hil - 1]++;
    for (auto& si : p->segs) {
        for (auto& run : si.runs) {
            if (!run.run || !run.run->hasKind(kind)) continue;
            const int64_t rc = run.run->scanRange(
                c.io, run.file, kind, prefix, plen, bounded ? hi : nullptr, hil,
                c.w->lookupScratch(), [&](const uint8_t* ek, uint16_t el, const uint8_t* ev) {
                    if (el >= plen && std::memcmp(ek, prefix, plen) == 0) visit(ek, el, ev);
                    return true;
                });
            if (rc < 0) return int32_t(rc);
        }
    }
    return 0;
}

bool isDead(Ctx& c, uint64_t pseq) {
    if (stagedDeadGet(*c.sc, pseq, nullptr)) return true;
    uint8_t k[8];
    be8(k, pseq);
    bool dead = false;
    const int32_t rc = committedPostings(c, kIxDead, k, 8, [&](const uint8_t*) {
        dead = true;
        return false;
    });
    if (rc < 0) c.err = rc;
    return dead;
}

// A tag instance retired by TAG_TOMB (A2). Staged entries use the high bit.
constexpr uint64_t kTagDeadBit = 1ull << 63;
bool isTagDead(Ctx& c, uint64_t inst) {
    if (stagedDeadGet(*c.sc, inst | kTagDeadBit, nullptr)) return true;
    uint8_t k[8];
    be8(k, inst);
    bool dead = false;
    const int32_t rc = committedPostings(c, kIxTagDead, k, 8, [&](const uint8_t*) {
        dead = true;
        return false;
    });
    if (rc < 0) c.err = rc;
    return dead;
}

int32_t readRow(Ctx& c, uint64_t pseq, RecRow* out) {
    if (c.st && pseq >= c.st->firstPseq && pseq < c.st->nextPseq) {
        *out = c.sc->rows[pseq - c.st->firstPseq];
        return 0;
    }
    return partitionReadRow(c.w, c.p, pseq, out);
}

int32_t readAttrOf(Ctx& c, const RecRow& r, uint8_t* buf, size_t cap, uint32_t* len) {
    if (!(r.flags & kRowHasAttr) || r.attrLen == 0) {
        *len = 0;
        return 0;
    }
    if (r.attrLen > cap) return FLATSQL_IO_ERR_GENERIC;
    if (c.st && r.pseq >= c.st->firstPseq && r.pseq < c.st->nextPseq) {
        // staged: attrOff is relative to the scratch attr block
        std::memcpy(buf, c.sc->attrs + r.attrOff, r.attrLen);
        *len = r.attrLen;
        return 0;
    }
    FileRef* f = (r.flags & kRowAttrInM) ? mHandleFor(c, r.seg) : segHandle(c, r.seg, 'a');
    if (!f) return FLATSQL_IO_ERR_IO;
    const int64_t n = c.io->read(*f, buf, r.attrLen, r.attrOff);
    if (n != int64_t(r.attrLen)) return n < 0 ? int32_t(n) : FLATSQL_IO_ERR_IO;
    *len = r.attrLen;
    return 0;
}

// cid state (current live PUT) with the per-batch cache.
StageScratch::Cid* cidState(Ctx& c, const uint8_t key[kCidKeyLen]) {
    StageScratch& sc = *c.sc;
    const uint32_t b = uint32_t(hash64(key, kCidKeyLen) % StageScratch::kCidBuckets);
    for (int32_t i = sc.cidBuckets[b]; i >= 0; i = sc.cids[i].next)
        if (std::memcmp(sc.cids[i].key, key, kCidKeyLen) == 0) return &sc.cids[i];
    if (sc.nCids >= StageScratch::kCidCap) return nullptr;
    // Committed: the highest live PUT pseq carrying this cid.
    uint64_t best = 0;
    uint64_t cands[64];
    uint32_t nc = 0;
    const int32_t rc = committedPostings(c, kIxCid, key, kCidKeyLen, [&](const uint8_t* v) {
        if (nc < 64) cands[nc++] = getBE64(v);
        return true;
    });
    if (rc < 0) {
        c.err = rc;
        return nullptr;
    }
    std::sort(cands, cands + nc);
    uint32_t putLen = 0;
    for (int i = int(nc) - 1; i >= 0; i--) {
        if (isDead(c, cands[i])) continue;
        if (c.err) return nullptr;
        RecRow r;
        const int32_t rr = readRow(c, cands[i], &r);
        if (rr < 0) {
            c.err = rr;
            return nullptr;
        }
        best = cands[i];
        putLen = r.len;
        break;
    }
    StageScratch::Cid& e = sc.cids[sc.nCids];
    std::memcpy(e.key, key, kCidKeyLen);
    e.putPseq = best;
    e.putLen = putLen;
    e.next = sc.cidBuckets[b];
    sc.cidBuckets[b] = int32_t(sc.nCids);
    sc.nCids++;
    return &e;
}

void stagedInstPut(StageScratch& sc, uint64_t put, uint64_t h, uint64_t pseq) {
    if (sc.nInst >= StageScratch::kInstCap) return;
    sc.instPut[sc.nInst] = put;
    sc.instHash[sc.nInst] = h;
    sc.instPseq[sc.nInst] = pseq;
    sc.nInst++;
}

// A2: does PUT `put` carry a live instance with exactly this tuple?
bool instanceLive(Ctx& c, uint64_t put, uint64_t h, const TagView& tag) {
    StageScratch& sc = *c.sc;
    uint8_t buf[4096];
    uint32_t alen;
    for (uint32_t i = 0; i < sc.nInst; i++) {
        if (sc.instPut[i] != put || sc.instHash[i] != h) continue;
        if (stagedDeadGet(sc, sc.instPseq[i] | kTagDeadBit, nullptr)) continue;
        RecRow r;
        if (readRow(c, sc.instPseq[i], &r) < 0) continue;
        if (readAttrOf(c, r, buf, sizeof(buf), &alen) < 0) continue;
        AttrView av;
        if (parseAttr(buf, alen, &av) && tagTupleEqual(av.tag, tag)) return true;
    }
    uint8_t key[16];
    be8(key, put);
    be8(key + 8, h);
    uint64_t cands[64];
    uint32_t nc = 0;
    const int32_t rc = committedPostings(c, kIxTagOf, key, 16, [&](const uint8_t* v) {
        if (nc < 64) cands[nc++] = getBE64(v);
        return true;
    });
    if (rc < 0) {
        c.err = rc;
        return false;
    }
    for (uint32_t i = 0; i < nc; i++) {
        if (isTagDead(c, cands[i])) continue;
        RecRow r;
        if (readRow(c, cands[i], &r) < 0) continue;
        if (readAttrOf(c, r, buf, sizeof(buf), &alen) < 0) continue;
        AttrView av;
        if (parseAttr(buf, alen, &av) && tagTupleEqual(av.tag, tag)) return true;
    }
    return false;
}

// Collects the live tag instances of PUT `put` (committed + staged).
template <typename F>
int32_t forEachLiveInstance(Ctx& c, uint64_t put, F&& visit) {
    StageScratch& sc = *c.sc;
    uint8_t prefix[8];
    be8(prefix, put);
    uint64_t found[256];
    uint32_t nf = 0;
    for (uint32_t i = 0; i < sc.nInst; i++)
        if (sc.instPut[i] == put && nf < 256) found[nf++] = sc.instPseq[i];
    const int32_t rc = committedPrefix(c, kIxTagOf, prefix, 8,
                                       [&](const uint8_t*, uint16_t, const uint8_t* v) {
                                           if (nf < 256) found[nf++] = getBE64(v);
                                       });
    if (rc < 0) return rc;
    std::sort(found, found + nf);
    nf = uint32_t(std::unique(found, found + nf) - found);
    for (uint32_t i = 0; i < nf; i++) {
        if (isTagDead(c, found[i])) continue;
        if (c.err) return c.err;
        if (!visit(found[i])) break;
    }
    return 0;
}

// Lane index for a tag (interning a new tuple: the only allocation a new
// tuple costs, once per tuple).
int32_t laneFor(Ctx& c, const TagView& t, uint32_t* laneIndex) {
    Partition* p = c.p;
    const uint64_t h = laneHash(t);
    auto range = p->laneByHash.equal_range(h);
    for (auto it = range.first; it != range.second; ++it) {
        const Lane& l = p->lanes[it->second];
        if (eqField(reinterpret_cast<const uint8_t*>(l.provider.data()), l.provider.size(), t.provider, t.providerLen) &&
            eqField(reinterpret_cast<const uint8_t*>(l.source.data()), l.source.size(), t.source, t.sourceLen) &&
            eqField(reinterpret_cast<const uint8_t*>(l.batch.data()), l.batch.size(), t.batch, t.batchLen) &&
            eqField(reinterpret_cast<const uint8_t*>(l.peer.data()), l.peer.size(), t.peer, t.peerLen) &&
            eqField(reinterpret_cast<const uint8_t*>(l.pubkey.data()), l.pubkey.size(), t.pubkey, t.pubkeyLen)) {
            *laneIndex = it->second;
            return 0;
        }
    }
    // New lane: frame for l.fsl [u32 len][u32 crc][u32 laneId][5 x (u16 len, bytes)]
    StageScratch& sc = *c.sc;
    const size_t body = 4 + 10 + t.providerLen + t.sourceLen + t.batchLen + t.peerLen + t.pubkeyLen;
    if (body > 65535 || sc.laneFrameBytes + 8 + body > sizeof(sc.laneFrames)) return 1;  // stop
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // interning a tuple is registration, not the record path
    Lane l;
    l.id = p->nextLaneId++;
    l.provider.assign(reinterpret_cast<const char*>(t.provider), t.providerLen);
    l.source.assign(reinterpret_cast<const char*>(t.source), t.sourceLen);
    l.batch.assign(reinterpret_cast<const char*>(t.batch), t.batchLen);
    l.peer.assign(reinterpret_cast<const char*>(t.peer), t.peerLen);
    l.pubkey.assign(reinterpret_cast<const char*>(t.pubkey), t.pubkeyLen);
    l.c.laneId = l.id;
    if (sc.laneFrameBytes == 0 && sc.firstNewLaneIndex == 0) sc.firstNewLaneIndex = uint32_t(p->lanes.size());
    p->lanes.push_back(std::move(l));
    const uint32_t idx = uint32_t(p->lanes.size() - 1);
    p->laneByHash.emplace(h, idx);
    tHotPathDepth = saved;
    uint8_t* f = sc.laneFrames + sc.laneFrameBytes;
    putU32(f, uint32_t(body));
    uint8_t* q = f + 8;
    putU32(q, p->lanes[idx].id);
    q += 4;
    auto put = [&](const uint8_t* s, size_t n) {
        putU16(q, uint16_t(n));
        if (n) std::memcpy(q + 2, s, n);
        q += 2 + n;
    };
    put(t.provider, t.providerLen);
    put(t.source, t.sourceLen);
    put(t.batch, t.batchLen);
    put(t.peer, t.peerLen);
    put(t.pubkey, t.pubkeyLen);
    putU32(f + 4, crc32c(f + 8, body));
    sc.laneFrameBytes += uint32_t(8 + body);
    *laneIndex = idx;
    return 0;
}

LaneDelta* laneDelta(Ctx& c, uint32_t laneId) {
    StageScratch& sc = *c.sc;
    for (uint32_t i = 0; i < sc.nDeltas; i++)
        if (sc.deltas[i].laneId == laneId) return &sc.deltas[i];
    if (sc.nDeltas >= 256) return nullptr;
    LaneDelta& d = sc.deltas[sc.nDeltas++];
    std::memset(&d, 0, sizeof(d));
    d.laneId = laneId;
    d.firstSeen = INT64_MAX;
    d.updated = INT64_MIN;
    return &d;
}

void applyLaneDelta(Ctx& c, uint32_t laneId, int64_t dCount, int64_t dBytes, uint64_t pseq,
                    int64_t whenMs) {
    if (laneId == 0) return;
    LaneDelta* d = laneDelta(c, laneId);
    if (!d) {
        c.err = FLATSQL_IO_ERR_GENERIC;
        return;
    }
    d->dCount += dCount;
    d->dBytes += dBytes;
    if (dCount > 0 && pseq > d->maxPseq) d->maxPseq = pseq;
    if (dCount > 0 && whenMs < d->firstSeen) d->firstSeen = whenMs;
    if (whenMs > d->updated) d->updated = whenMs;
}

RecRow* newRow(Ctx& c, uint8_t kind) {
    StageScratch& sc = *c.sc;
    if (sc.nRows >= sc.capRows) return nullptr;
    RecRow* r = &sc.rows[sc.nRows++];
    std::memset(r, 0, sizeof(*r));
    r->pseq = c.st->nextPseq++;
    r->kind = kind;
    r->seg = c.p->dSeg;
    std::memcpy(r->fid, c.p->fid, 4);
    return r;
}

void addCtl(Ctx& c, uint16_t kind, const void* body, uint16_t len) {
    StageScratch& sc = *c.sc;
    if (sc.ctlBytes + 4 + len > sizeof(sc.ctl)) {
        c.err = FLATSQL_IO_ERR_GENERIC;
        return;
    }
    putU16(sc.ctl + sc.ctlBytes, kind);
    putU16(sc.ctl + sc.ctlBytes + 2, len);
    if (len) std::memcpy(sc.ctl + sc.ctlBytes + 4, body, len);
    sc.ctlBytes += 4 + len;
    sc.nCtl++;
}

// Stages a TOMB of `target` (any row kind). Updates counters and lanes for PUTs.
bool stageKill(Ctx& c, uint64_t target, uint8_t tombKind = kRowTomb) {
    if (isDead(c, target)) return true;
    if (c.err) return false;
    RecRow tr;
    const int32_t rr = readRow(c, target, &tr);
    if (rr < 0) {
        c.err = rr;
        return false;
    }
    const int64_t now = c.e->nowMs();
    if (tr.kind == kRowPut) {
        // Every live tag instance dies with the record (lane counters).
        const int32_t rc = forEachLiveInstance(c, target, [&](uint64_t inst) {
            RecRow ir;
            if (inst == target) ir = tr;
            else if (readRow(c, inst, &ir) < 0) return true;
            applyLaneDelta(c, ir.laneId, -1, -int64_t(tr.len - 4), 0, now);
            return true;
        });
        if (rc < 0) {
            c.err = rc;
            return false;
        }
    }
    RecRow* r = newRow(c, tombKind);
    if (!r) return false;
    r->targetPseq = target;
    r->len = tr.len;
    r->cidLen = kCidLen;
    std::memcpy(r->cid, tr.cid, kCidLen);
    r->epochMs = now;
    r->arrivalMs = now;
    uint8_t k[8];
    be8(k, target);
    if (!addPseqPosting(c, kIxDead, k, 8, r->pseq)) {
        c.sc->nRows--;
        c.st->nextPseq--;
        return false;
    }
    if (!stagedDeadPut(*c.sc, target, r->pseq)) {
        c.err = FLATSQL_IO_ERR_GENERIC;
        return false;
    }
    Counters& ct = c.st->counters;
    ct.tombCount++;
    if (tr.kind == kRowPut) {
        ct.liveCount--;
        ct.liveBytes -= (tr.len - 4);
        uint8_t ck[kCidKeyLen];
        cidSortKey(tr.cid, ck);
        StageScratch::Cid* cs = cidState(c, ck);
        if (cs && cs->putPseq == target) cs->putPseq = 0;
    }
    c.e->cTombs.fetch_add(1, std::memory_order_relaxed);
    return true;
}

// A2: retire one tag instance.
bool stageTagTomb(Ctx& c, uint64_t inst, const RecRow& ir, uint32_t putLen) {
    RecRow* r = newRow(c, kRowTagTomb);
    if (!r) return false;
    r->targetPseq = inst;
    r->len = putLen;
    r->cidLen = kCidLen;
    std::memcpy(r->cid, ir.cid, kCidLen);
    r->laneId = ir.laneId;
    const int64_t now = c.e->nowMs();
    r->epochMs = now;
    r->arrivalMs = now;
    uint8_t k[8];
    be8(k, inst);
    if (!addPseqPosting(c, kIxTagDead, k, 8, r->pseq)) return false;
    if (!stagedDeadPut(*c.sc, inst | kTagDeadBit, r->pseq)) {
        c.err = FLATSQL_IO_ERR_GENERIC;
        return false;
    }
    applyLaneDelta(c, ir.laneId, -1, -int64_t(putLen - 4), 0, now);
    return true;
}

bool postTagPostings(Ctx& c, const TagView& t, uint64_t pseq, int64_t epochMs) {
    uint8_t key[2 * kMaxKeyLen + 32];
    size_t n;
    n = capKey(key, t.provider, t.providerLen);
    if (!addPseqPosting(c, kIxTagProvider, key, n, pseq)) return false;
    n = capKey(key, t.source, t.sourceLen);
    if (!addPseqPosting(c, kIxTagSource, key, n, pseq)) return false;
    n = capKey(key, t.batch, t.batchLen);
    if (!addPseqPosting(c, kIxTagBatch, key, n, pseq)) return false;
    n = capKey(key, t.peer, t.peerLen);
    if (!addPseqPosting(c, kIxTagPeer, key, n, pseq)) return false;
    n = capKey(key, t.pubkey, t.pubkeyLen);
    if (!addPseqPosting(c, kIxTagPubkey, key, n, pseq)) return false;
    uint8_t cap[kMaxKeyLen];
    const size_t sl = capKey(cap, t.source, t.sourceLen);
    n = encStrI64(key, cap, sl, epochMs);
    if (!addPseqPosting(c, kIxSourceEpoch, key, n, pseq)) return false;
    const size_t pl = capKey(cap, t.provider, t.providerLen);
    n = encStrI64(key, cap, pl, epochMs);
    if (!addPseqPosting(c, kIxProviderEpoch, key, n, pseq)) return false;
    uint8_t cp[kMaxKeyLen], cs[kMaxKeyLen];
    const size_t cpl = capKey(cp, t.provider, t.providerLen);
    const size_t csl = capKey(cs, t.source, t.sourceLen);
    n = encStr2(key, cp, cpl, cs, csl);
    if (!addPseqPosting(c, kIxTagPS, key, n, pseq)) return false;
    return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// Row and handle access (also used by merge and the type owner)
// ---------------------------------------------------------------------------
namespace {
SegmentInfo* findSeg(Partition* p, uint32_t seg) {
    for (auto& s : p->segs)
        if (s.seg == seg) return &s;
    return nullptr;
}

FileRef* openSegFile(Writer* w, Partition* p, SegmentInfo* si, char letter) {
    FileRef* f = letter == 'r' ? &si->r : letter == 'a' ? &si->a : letter == 'd' ? &si->d : &si->m;
    if (f->valid()) return f;
    PathBuf path;
    const char* ext = letter == 'r' ? "fsr" : letter == 'a' ? "fsa" : letter == 'd' ? "fsd" : "fsl";
    pathPartitionSeg(&path, w->eng_root(), p->pid, letter, si->seg, ext);
    const FileClass cls = letter == 'r' ? FileClass::Rows : letter == 'a' ? FileClass::Attrs
                          : letter == 'd' ? FileClass::Data : FileClass::Meta;
    if (w->io().open(path.c_str(), path.len, FLATSQL_IO_READ, cls, f) < 0) return nullptr;
    return f;
}

FileRef* mHandleFor(Ctx& c, uint32_t mSeg) {
    if (mSeg == c.p->mSeg && c.p->m.valid()) return &c.p->m;
    SegmentInfo* si = findSeg(c.p, mSeg);
    if (!si) {
        c.p->segs.emplace_back();
        si = &c.p->segs.back();
        si->seg = mSeg;
    }
    return openSegFile(c.w, c.p, si, 'm');
}

FileRef* segHandle(Ctx& c, uint32_t seg, char letter) {
    SegmentInfo* si = findSeg(c.p, seg);
    if (!si) return nullptr;
    return openSegFile(c.w, c.p, si, letter);
}
}  // namespace

int32_t partitionReadRow(Writer* w, Partition* p, uint64_t pseq, RecRow* out) {
    if (pseq == 0 || pseq > p->pseqHi) return FLATSQL_IO_ERR_NOENT;
    Ctx c{w, w->engine(), p, nullptr, nullptr, &w->io(), nullptr};
    if (pseq > p->mergedThrough) {
        for (uint32_t i = 0; i < p->nL0; i++) {
            const L0DirEntry& e = p->l0[i];
            if (pseq >= e.firstPseq && pseq < e.firstPseq + e.nRows) {
                FileRef* f = mHandleFor(c, e.mSeg);
                if (!f) return FLATSQL_IO_ERR_IO;
                const uint64_t off = e.mOff + sizeof(BatchHeader) + (pseq - e.firstPseq) * sizeof(RecRow);
                const int64_t n = w->io().read(*f, out, sizeof(RecRow), off);
                return n == int64_t(sizeof(RecRow)) ? 0 : (n < 0 ? int32_t(n) : FLATSQL_IO_ERR_IO);
            }
        }
        return FLATSQL_IO_ERR_NOENT;
    }
    for (auto& si : p->segs) {
        if (pseq >= si.firstPseq && pseq < si.mergedEnd) {
            FileRef* f = openSegFile(w, p, &si, 'r');
            if (!f) return FLATSQL_IO_ERR_IO;
            const int64_t n = w->io().read(*f, out, sizeof(RecRow), (pseq - si.firstPseq) * sizeof(RecRow));
            return n == int64_t(sizeof(RecRow)) ? 0 : (n < 0 ? int32_t(n) : FLATSQL_IO_ERR_IO);
        }
    }
    return FLATSQL_IO_ERR_NOENT;
}

int32_t ensureExtent(IoCtx* io, const FileRef& f, uint64_t* extent, uint64_t end, uint64_t step) {
    if (step == 0 || end <= *extent) return 0;
    const uint64_t target = ((end + step - 1) / step) * step;
    const int32_t rc = io->writeZeros(f, *extent, target - *extent);
    if (rc < 0) return rc;
    *extent = target;
    return 0;
}

// ---------------------------------------------------------------------------
// Staging
// ---------------------------------------------------------------------------
namespace {

enum StepResult { kConsumed, kStop, kRejected };

StepResult stageRecord(Ctx& c, const EntryHeader& h, uint64_t pos, int32_t* rejectCode) {
    Partition* p = c.p;
    StageScratch& sc = *c.sc;
    Staged& st = *c.st;
    const TypeConfig* cfg = p->type ? p->type->cfg.get() : nullptr;
    if (!cfg) {
        *rejectCode = kRejNoType;
        return kRejected;
    }
    const bool sealed = h.flags & kEntSealed;
    const uint64_t attrPos = pos + sizeof(EntryHeader);
    const uint64_t framePos = attrPos + h.attrLen;
    uint32_t sealedLen = 0;
    if (sealed) {
        if (sizeof(EntryHeader) + uint64_t(h.attrLen) + h.frameLen + 4 > h.entryLen) {
            *rejectCode = kRejBadEntry;
            return kRejected;
        }
        uint8_t b[4];
        ringRead(p->ring, c.e->pool(), framePos + h.frameLen, b, 4);
        sealedLen = getU32(b);
        if (sizeof(EntryHeader) + uint64_t(h.attrLen) + h.frameLen + 4 + sealedLen > h.entryLen) {
            *rejectCode = kRejBadEntry;
            return kRejected;
        }
    }
    const uint32_t storedLen = sealed ? sealedLen + 4 : h.frameLen;
    // Budgets: stop (commit what is staged) rather than overflow.
    if (sc.nRows + 8 > sc.capRows || sc.nEntries + 64 > sc.capEntries ||
        sc.keyBytes + 16384 > sc.capKeys || sc.attrBytes + h.attrLen + 16 > sc.capAttrs ||
        c.frames->remaining() < storedLen + 16 || sc.nCids + 4 > StageScratch::kCidCap ||
        sc.nDead + 8 > StageScratch::kDeadCap || sc.nInst + 4 > StageScratch::kInstCap) {
        if (st.nextPseq > st.firstPseq || st.consumed) return kStop;
        *rejectCode = kRejFrameSize;
        return kRejected;
    }
    if (h.frameLen > sc.capPlain || h.attrLen > 65536) {
        *rejectCode = kRejFrameSize;
        return kRejected;
    }
    // Seal boundary (minor 3): a frame never straddles a segment.
    const uint64_t segBytes = p->dLen + st.dBytes;
    if (segBytes > 0 && segBytes + storedLen > c.e->config().sealBytes) {
        st.sealAfter = true;
        return kStop;
    }
    if (p->segRecords + (st.nextPseq - st.firstPseq) >= c.e->config().sealRecords) {
        st.sealAfter = true;
        return kStop;
    }
    // Plaintext frame (contiguous for verification).
    const uint8_t* plain = ringContiguous(p->ring, c.e->pool(), framePos, h.frameLen);
    if (!plain) {
        ringRead(p->ring, c.e->pool(), framePos, sc.plain, h.frameLen);
        plain = sc.plain;
    }
    int32_t rc = cfg->checkFrame(plain, h.frameLen);
    if (rc) {
        *rejectCode = rc;
        return kRejected;
    }
    // CID over the plaintext (router computed it before sealing, §6.1).
    uint8_t cid[kCidLen];
    uint8_t flags = 0;
    if (h.flags & kEntCidPresent) {
        std::memcpy(cid, h.cid, kCidLen);
        if (cfg->flags() & TypeConfig::kVerifyCid) {
            uint8_t check[kCidLen];
            if (h.flags & kEntCidOverPrefix) computeCid(plain, h.frameLen, check);
            else computeCid(plain + 4, h.frameLen - 4, check);
            if (std::memcmp(check, cid, kCidLen) != 0) {
                *rejectCode = kRejCid;
                return kRejected;
            }
            flags |= kRowCidVerified;
        }
    } else {
        computeCid(plain + 4, h.frameLen - 4, cid);
        flags |= kRowCidVerified;
    }
    // Attribute.
    const uint8_t* attr = nullptr;
    if (h.attrLen) {
        attr = ringContiguous(p->ring, c.e->pool(), attrPos, h.attrLen);
        if (!attr) {
            // sc.plain may hold the frame; attr goes after it.
            if (h.frameLen + h.attrLen + 8 > sc.capPlain) {
                *rejectCode = kRejAttr;
                return kRejected;
            }
            uint8_t* dst = sc.plain + pad8(h.frameLen) + 0;
            ringRead(p->ring, c.e->pool(), attrPos, dst, h.attrLen);
            attr = dst;
        }
    }
    AttrView av;
    if (!parseAttr(attr, h.attrLen, &av)) {
        *rejectCode = kRejAttr;
        return kRejected;
    }
    const TagView& tag = av.tag;
    const uint64_t tagHash = tagTupleHash(tag);
    uint8_t cidKey[kCidKeyLen];
    cidSortKey(cid, cidKey);
    StageScratch::Cid* cs = cidState(c, cidKey);
    if (!cs) return c.err ? kStop : kStop;
    const int64_t now = h.arrivalMs;

    if (cs->putPseq) {
        // A live copy exists: dedupe (A2).
        if (!tag.present || instanceLive(c, cs->putPseq, tagHash, tag)) {
            if (c.err) return kStop;
            c.e->cDedupe.fetch_add(1, std::memory_order_relaxed);
            return kConsumed;
        }
        if (c.err) return kStop;
        RecRow put;
        if (readRow(c, cs->putPseq, &put) < 0) return kStop;
        uint32_t laneIdx;
        const int32_t lr = laneFor(c, tag, &laneIdx);
        if (lr) return kStop;
        // Attr copy for the RETAG row.
        const uint32_t attrRel = sc.attrBytes + 4;
        putU32(sc.attrs + sc.attrBytes, h.attrLen);
        std::memcpy(sc.attrs + attrRel, attr, h.attrLen);
        const uint32_t attrPad = uint32_t(pad8(4 + h.attrLen));
        std::memset(sc.attrs + attrRel + h.attrLen, 0, attrPad - 4 - h.attrLen);
        sc.attrBytes += attrPad;
        sc.nAttr++;
        RecRow* r = newRow(c, kRowRetag);
        if (!r) return kStop;
        r->off = 0;
        r->len = put.len;
        r->flags = kRowHasAttr | kRowAttrInM;
        r->cidLen = kCidLen;
        std::memcpy(r->cid, cid, kCidLen);
        r->epochMs = put.epochMs;
        r->arrivalMs = now;
        r->targetPseq = cs->putPseq;
        r->attrOff = attrRel;
        r->attrLen = h.attrLen;
        r->laneId = p->lanes[laneIdx].id;
        r->tagHash = tagHash;
        uint8_t k[16];
        be8(k, cs->putPseq);
        be8(k + 8, tagHash);
        if (!addPseqPosting(c, kIxTagOf, k, 16, r->pseq) ||
            !postTagPostings(c, tag, r->pseq, put.epochMs)) {
            c.err = FLATSQL_IO_ERR_GENERIC;
            return kStop;
        }
        stagedInstPut(sc, cs->putPseq, tagHash, r->pseq);
        applyLaneDelta(c, r->laneId, 1, int64_t(put.len) - 4, r->pseq, now);
        c.e->cRetags.fetch_add(1, std::memory_order_relaxed);
        return kConsumed;
    }

    // New PUT. Extract keys from the plaintext.
    Extracted ex;
    cfg->extract(plain, h.frameLen, &ex, sc.extract, sc.capExtract);
    // Supersede (§4.6, record_supersede.go).
    uint8_t stored[kMaxKeyLen + 64];
    size_t storedLen2 = 0;
    const uint8_t* identity = ex.identity;
    size_t identityLen = ex.identityLen;
    if (av.hasSupersedeKey && av.supersedeKeyLen) {
        // Migration supplies the stored key verbatim (minor 9).
        storedLen2 = capKey(stored, av.supersedeKey, av.supersedeKeyLen);
        identity = av.supersedeKey;
        identityLen = av.supersedeKeyLen;
        if (identityLen > 4 && std::memcmp(identity, "src:", 4) == 0) {
            const void* nul = std::memchr(identity + 4, 0, identityLen - 4);
            if (nul) {
                const size_t skip = size_t(static_cast<const uint8_t*>(nul) - identity) + 1;
                identity += skip;
                identityLen -= skip;
            }
        }
    } else if (identityLen) {
        const uint8_t* src = tag.source;
        size_t srcLen = tag.present ? tag.sourceLen : 0;
        if (srcLen) trimSpace(&src, &srcLen);
        // NUL is the scope separator: strip it from the source name.
        uint8_t srcClean[256];
        size_t cl = 0;
        for (size_t i = 0; i < srcLen && cl < sizeof(srcClean); i++)
            if (src[i]) srcClean[cl++] = src[i];
        if (cl) {
            uint8_t tmp[kMaxKeyLen + 300];
            size_t n = 0;
            std::memcpy(tmp, "src:", 4);
            n = 4;
            std::memcpy(tmp + n, srcClean, cl);
            n += cl;
            tmp[n++] = 0;
            const size_t idl = std::min(identityLen, sizeof(tmp) - n);
            std::memcpy(tmp + n, identity, idl);
            n += idl;
            storedLen2 = capKey(stored, tmp, n);
        } else {
            storedLen2 = capKey(stored, identity, identityLen);
        }
    }
    uint64_t killed[8];
    uint32_t nKilled = 0;
    if (storedLen2) {
        uint8_t idKey[kMaxKeyLen];
        const size_t idKeyLen = capKey(idKey, identity, identityLen);
        const uint8_t* keys[2] = {stored, idKey};
        const size_t lens[2] = {storedLen2, idKeyLen};
        const int nk = keyCmp(stored, storedLen2, idKey, idKeyLen) == 0 ? 1 : 2;
        for (int ki = 0; ki < nk; ki++) {
            // staged supersede postings (this batch)
            for (uint32_t i = 0; i < sc.nEntries; i++) {
                const StagedEntry& e = sc.entries[i];
                if (e.kind != kIxSupersede || keyCmp(e.key, e.klen, keys[ki], lens[ki]) != 0) continue;
                const uint64_t ps = getBE64(e.val);
                if (!stagedDeadGet(sc, ps, nullptr) && nKilled < 8) killed[nKilled++] = ps;
            }
            const int32_t prc = committedPostings(c, kIxSupersede, keys[ki], lens[ki], [&](const uint8_t* v) {
                const uint64_t ps = getBE64(v);
                if (nKilled < 8) killed[nKilled++] = ps;
                return true;
            });
            if (prc < 0) {
                c.err = prc;
                return kStop;
            }
        }
        // Keep live copies whose cid differs.
        uint32_t w2 = 0;
        std::sort(killed, killed + nKilled);
        nKilled = uint32_t(std::unique(killed, killed + nKilled) - killed);
        for (uint32_t i = 0; i < nKilled; i++) {
            if (isDead(c, killed[i])) continue;
            RecRow kr;
            if (readRow(c, killed[i], &kr) < 0) continue;
            if (kr.kind != kRowPut || std::memcmp(kr.cid, cid, kCidLen) == 0) continue;
            killed[w2++] = killed[i];
        }
        nKilled = w2;
        if (c.err) return kStop;
    }
    uint32_t laneIdx = 0;
    uint32_t laneId = 0;
    if (tag.present) {
        if (laneFor(c, tag, &laneIdx)) return kStop;
        laneId = p->lanes[laneIdx].id;
    }
    // Frame into the frames arena (the stored bytes: plaintext or sealed).
    uint8_t* fdst = static_cast<uint8_t*>(c.frames->alloc(storedLen, 1));
    if (!fdst) return kStop;
    if (sealed) {
        putU32(fdst, sealedLen);
        ringRead(p->ring, c.e->pool(), framePos + h.frameLen + 4, fdst + 4, sealedLen);
        if (!sealedEnvelopeValid(fdst + 4, sealedLen)) {
            c.frames->truncate(size_t(fdst - c.frames->at(0)));
            *rejectCode = kRejSealed;
            return kRejected;
        }
    } else {
        std::memcpy(fdst, plain, h.frameLen);
    }
    if (!st.frames) st.frames = fdst;
    const uint32_t frameOff = uint32_t(st.dOff + st.dBytes);
    st.dBytes += storedLen;
    // Attribute copy.
    uint32_t attrRel = 0;
    if (h.attrLen) {
        attrRel = sc.attrBytes + 4;
        putU32(sc.attrs + sc.attrBytes, h.attrLen);
        std::memcpy(sc.attrs + attrRel, attr, h.attrLen);
        const uint32_t attrPad = uint32_t(pad8(4 + h.attrLen));
        std::memset(sc.attrs + attrRel + h.attrLen, 0, attrPad - 4 - h.attrLen);
        sc.attrBytes += attrPad;
        sc.nAttr++;
    }
    RecRow* r = newRow(c, kRowPut);
    if (!r) return kStop;
    r->off = frameOff;
    r->len = storedLen;
    r->flags = uint8_t(flags | (sealed ? kRowSealed : 0) |
                       (h.attrLen ? (kRowHasAttr | kRowAttrInM) : 0) |
                       (nKilled ? kRowSupersedes : 0));
    r->cidLen = kCidLen;
    r->dataCrc = crc32c(fdst, storedLen);
    r->epochMs = ex.hasEpoch ? ex.epochMs : h.arrivalMs;
    r->arrivalMs = h.arrivalMs;
    r->targetPseq = nKilled ? killed[0] : 0;
    r->attrOff = attrRel;
    r->attrLen = h.attrLen;
    r->laneId = laneId;
    std::memcpy(r->cid, cid, kCidLen);
    r->supersedeHash = storedLen2 ? hash64(stored, storedLen2) : 0;
    r->tagHash = tagHash;
    const uint64_t pseq = r->pseq;
    const int64_t epochMs = r->epochMs;
    // Postings.
    uint8_t key[2 * kMaxKeyLen + 32];
    bool ok = addPseqPosting(c, kIxCid, cidKey, kCidKeyLen, pseq);
    encI64(key, epochMs);
    ok = ok && addPseqPosting(c, kIxEpoch, key, 8, pseq);
    for (uint32_t ci = 0; ok && ci < cfg->nCols(); ci++) {
        const ColValue& cv = ex.cols[ci];
        if (!cv.present) continue;
        size_t n;
        if (cv.isU64) {
            be8(key, cv.u);
            n = 8;
        } else {
            n = capKey(key, cv.s, cv.n);
        }
        ok = addPseqPosting(c, uint16_t(kIxColBase + ci), key, n, pseq);
    }
    if (ok && ex.objectCol >= 0) {
        const ColValue& cv = ex.cols[ex.objectCol];
        uint8_t ob[kMaxKeyLen];
        size_t on;
        if (cv.isU64) {
            be8(ob, cv.u);
            on = 8;
        } else {
            on = capKey(ob, cv.s, cv.n);
        }
        const size_t n = encStrI64(key, ob, on, epochMs);
        ok = addPseqPosting(c, kIxObjectEpoch, key, n, pseq);
    }
    if (ok && storedLen2) ok = addPseqPosting(c, kIxSupersede, stored, storedLen2, pseq);
    if (ok && tag.present) {
        uint8_t k[16];
        be8(k, pseq);
        be8(k + 8, tagHash);
        ok = addPseqPosting(c, kIxTagOf, k, 16, pseq) && postTagPostings(c, tag, pseq, epochMs);
    }
    if (!ok) {
        c.err = FLATSQL_IO_ERR_GENERIC;
        return kStop;
    }
    Counters& ct = st.counters;
    ct.totalCount++;
    ct.totalBytes += storedLen - 4;
    ct.liveCount++;
    ct.liveBytes += storedLen - 4;
    if (epochMs < ct.minEpoch) ct.minEpoch = epochMs;
    if (epochMs > ct.maxEpoch) ct.maxEpoch = epochMs;
    if (h.arrivalMs > ct.latestArrival) ct.latestArrival = h.arrivalMs;
    cs->putPseq = pseq;
    cs->putLen = storedLen;
    if (tag.present) {
        stagedInstPut(sc, pseq, tagHash, pseq);
        applyLaneDelta(c, laneId, 1, int64_t(storedLen) - 4, pseq, h.arrivalMs);
    }
    for (uint32_t i = 0; i < nKilled; i++)
        if (!stageKill(c, killed[i])) return kStop;
    c.e->cRows.fetch_add(1, std::memory_order_relaxed);
    return kConsumed;
}

StepResult stageLicence(Ctx& c, const EntryHeader& h, uint64_t pos, int32_t* rejectCode) {
    Partition* p = c.p;
    StageScratch& sc = *c.sc;
    Staged& st = *c.st;
    if (sc.nRows + 4 > sc.capRows || c.frames->remaining() < h.frameLen + 16 ||
        sc.attrBytes + h.attrLen + 16 > sc.capAttrs)
        return kStop;
    if (h.frameLen < 4 || h.frameLen > sc.capPlain) {
        *rejectCode = kRejFrameSize;
        return kRejected;
    }
    const uint64_t attrPos = pos + sizeof(EntryHeader);
    uint8_t* adst = sc.plain;
    if (h.attrLen) ringRead(p->ring, c.e->pool(), attrPos, adst, h.attrLen);
    AttrView av;
    if (!parseAttr(adst, h.attrLen, &av) || !av.licenceKey || av.licenceKeyLen == 0) {
        *rejectCode = kRejAttr;
        return kRejected;
    }
    uint8_t lk[kMaxKeyLen];
    const size_t lkl = capKey(lk, av.licenceKey, av.licenceKeyLen);
    // A newer licence for the same (provider, source, batch) supersedes.
    uint64_t prev[8];
    uint32_t np = 0;
    for (uint32_t i = 0; i < sc.nEntries; i++) {
        const StagedEntry& e = sc.entries[i];
        if (e.kind == kIxLicence && keyCmp(e.key, e.klen, lk, lkl) == 0 && np < 8)
            prev[np++] = getBE64(e.val);
    }
    const int32_t prc = committedPostings(c, kIxLicence, lk, lkl, [&](const uint8_t* v) {
        if (np < 8) prev[np++] = getBE64(v);
        return true;
    });
    if (prc < 0) {
        c.err = prc;
        return kStop;
    }
    uint8_t* fdst = static_cast<uint8_t*>(c.frames->alloc(h.frameLen, 1));
    if (!fdst) return kStop;
    ringRead(p->ring, c.e->pool(), attrPos + h.attrLen, fdst, h.frameLen);
    if (!st.frames) st.frames = fdst;
    const uint32_t frameOff = uint32_t(st.dOff + st.dBytes);
    st.dBytes += h.frameLen;
    const uint32_t attrRel = sc.attrBytes + 4;
    putU32(sc.attrs + sc.attrBytes, h.attrLen);
    std::memcpy(sc.attrs + attrRel, adst, h.attrLen);
    const uint32_t attrPad = uint32_t(pad8(4 + h.attrLen));
    std::memset(sc.attrs + attrRel + h.attrLen, 0, attrPad - 4 - h.attrLen);
    sc.attrBytes += attrPad;
    sc.nAttr++;
    RecRow* r = newRow(c, kRowLicence);
    if (!r) return kStop;
    r->off = frameOff;
    r->len = h.frameLen;
    r->flags = kRowHasAttr | kRowAttrInM;
    r->dataCrc = crc32c(fdst, h.frameLen);
    r->epochMs = h.arrivalMs;
    r->arrivalMs = h.arrivalMs;
    r->attrOff = attrRel;
    r->attrLen = h.attrLen;
    r->cidLen = kCidLen;
    computeCid(fdst, h.frameLen, r->cid);
    r->supersedeHash = hash64(lk, lkl);
    const uint64_t pseq = r->pseq;
    if (!addPseqPosting(c, kIxLicence, lk, lkl, pseq)) return kStop;
    for (uint32_t i = 0; i < np; i++)
        if (prev[i] != pseq && !stageKill(c, prev[i])) return kStop;
    return kConsumed;
}

StepResult stageCtl(Ctx& c, const EntryHeader& h, uint64_t pos, uint64_t txnId, int32_t* rejectCode) {
    Partition* p = c.p;
    StageScratch& sc = *c.sc;
    Staged& st = *c.st;
    if (h.frameLen < 4 || h.frameLen > sc.capPlain) {
        *rejectCode = kRejFrameSize;
        return kRejected;
    }
    const uint64_t attrPos = pos + sizeof(EntryHeader);
    uint8_t* adst = sc.plain;
    if (h.attrLen) ringRead(p->ring, c.e->pool(), attrPos, adst, h.attrLen);
    AttrView av;
    if (!parseAttr(adst, h.attrLen, &av) || !av.hasSupersedeKey) {
        *rejectCode = kRejAttr;
        return kRejected;
    }
    uint8_t key[kMaxKeyLen];
    const size_t kl = capKey(key, av.supersedeKey, av.supersedeKeyLen);
    uint64_t prev[8];
    uint32_t np = 0;
    for (uint32_t i = 0; i < sc.nEntries; i++) {
        const StagedEntry& e = sc.entries[i];
        if (e.kind == kIxSupersede && keyCmp(e.key, e.klen, key, kl) == 0 && np < 8)
            prev[np++] = getBE64(e.val);
    }
    const int32_t prc = committedPostings(c, kIxSupersede, key, kl, [&](const uint8_t* v) {
        if (np < 8) prev[np++] = getBE64(v);
        return true;
    });
    if (prc < 0) {
        c.err = prc;
        return kStop;
    }
    const bool del = (h.flags & 0x0010) != 0;
    for (uint32_t i = 0; i < np; i++)
        if (!stageKill(c, prev[i], kRowCtlTomb)) return kStop;
    if (del) return kConsumed;
    uint8_t* fdst = static_cast<uint8_t*>(c.frames->alloc(h.frameLen, 1));
    if (!fdst) return kStop;
    ringRead(p->ring, c.e->pool(), attrPos + h.attrLen, fdst, h.frameLen);
    if (!st.frames) st.frames = fdst;
    const uint32_t frameOff = uint32_t(st.dOff + st.dBytes);
    st.dBytes += h.frameLen;
    const uint32_t attrRel = sc.attrBytes + 4;
    putU32(sc.attrs + sc.attrBytes, h.attrLen);
    std::memcpy(sc.attrs + attrRel, adst, h.attrLen);
    const uint32_t attrPad = uint32_t(pad8(4 + h.attrLen));
    std::memset(sc.attrs + attrRel + h.attrLen, 0, attrPad - 4 - h.attrLen);
    sc.attrBytes += attrPad;
    sc.nAttr++;
    RecRow* r = newRow(c, kRowCtl);
    if (!r) return kStop;
    r->off = frameOff;
    r->len = h.frameLen;
    r->flags = kRowHasAttr | kRowAttrInM | kRowTxn;
    r->dataCrc = crc32c(fdst, h.frameLen);
    r->epochMs = h.arrivalMs;
    r->arrivalMs = h.arrivalMs;
    r->attrOff = attrRel;
    r->attrLen = h.attrLen;
    r->aux = uint32_t(txnId);
    r->supersedeHash = hash64(key, kl);
    if (!addPseqPosting(c, kIxSupersede, key, kl, r->pseq)) return kStop;
    return kConsumed;
}

// RECONCILE (A2): bounded steps across commits. Returns true when complete.
// Idempotent: a restart (after a failed commit) re-collects instances; every
// instance of (provider, source) outside `keep` puts its PUT on the affected
// list whether or not an earlier step already retired it, so phase 2 always
// sees every PUT that may have lost its last live instance.
bool reconcileStep(Ctx& c) {
    Partition* p = c.p;
    ReconcileState& rs = p->rec;
    const uint32_t step = c.e->config().reconcileStep;
    uint32_t done = 0;
    while (!rs.phase2 && rs.next < rs.instances.size() && done < step) {
        if (c.sc->nRows + 4 > c.sc->capRows || c.sc->nEntries + 8 > c.sc->capEntries) return false;
        const uint64_t inst = rs.instances[rs.next];
        RecRow ir;
        if (readRow(c, inst, &ir) < 0) {
            rs.next++;
            continue;
        }
        const uint64_t put = ir.kind == kRowPut ? inst : ir.targetPseq;
        const Lane* lane = nullptr;
        for (const auto& l : p->lanes)
            if (l.id == ir.laneId) lane = &l;
        const bool keep = lane && lane->batch.size() == rs.keep.size() &&
                          std::memcmp(lane->batch.data(), rs.keep.data(), rs.keep.size()) == 0;
        if (!keep && lane) {
            const int saved = tHotPathDepth;
            tHotPathDepth = 0;
            rs.affected.push_back(put);
            tHotPathDepth = saved;
            if (!isTagDead(c, inst) && !isDead(c, put)) {
                RecRow pr;
                if (readRow(c, put, &pr) < 0) return false;
                if (!stageTagTomb(c, inst, ir, pr.len)) return false;
                done++;
            }
        }
        rs.next++;
        if (c.err) return false;
    }
    if (!rs.phase2) {
        if (rs.next < rs.instances.size()) return false;
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;
        std::sort(rs.affected.begin(), rs.affected.end());
        rs.affected.erase(std::unique(rs.affected.begin(), rs.affected.end()), rs.affected.end());
        tHotPathDepth = saved;
        rs.phase2 = true;
        rs.nextAffected = 0;
    }
    while (rs.nextAffected < rs.affected.size() && done < step) {
        if (c.sc->nRows + 4 > c.sc->capRows || c.sc->nEntries + 8 > c.sc->capEntries) return false;
        const uint64_t put = rs.affected[rs.nextAffected];
        if (!isDead(c, put)) {
            bool anyLive = false;
            const int32_t rc = forEachLiveInstance(c, put, [&](uint64_t) {
                anyLive = true;
                return false;
            });
            if (rc < 0) {
                c.err = rc;
                return false;
            }
            if (!anyLive) {
                if (!stageKill(c, put)) return false;
                done++;
            }
        }
        rs.nextAffected++;
        if (c.err) return false;
    }
    return rs.nextAffected >= rs.affected.size();
}

bool reconcileStart(Ctx& c, const EntryHeader& h, uint64_t pos) {
    Partition* p = c.p;
    ReconcileState& rs = p->rec;
    // Control operation: allocations here are not on the per-record path.
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    std::vector<uint8_t> payload(h.frameLen);
    if (h.frameLen)
        ringRead(p->ring, c.e->pool(), pos + sizeof(EntryHeader) + h.attrLen, payload.data(), h.frameLen);
    const uint8_t* q = payload.data();
    const uint8_t* end = q + payload.size();
    std::string parts[3];
    for (int i = 0; i < 3; i++) {
        if (q + 2 > end) {
            tHotPathDepth = saved;
            return false;
        }
        const uint16_t n = getU16(q);
        if (q + 2 + n > end) {
            tHotPathDepth = saved;
            return false;
        }
        parts[i].assign(reinterpret_cast<const char*>(q + 2), n);
        q += 2 + n;
    }
    rs = ReconcileState();
    rs.active = true;
    rs.rseq = h.rseq;
    rs.entryPos = pos;
    rs.entryLen = h.entryLen;
    rs.provider = parts[0];
    rs.source = parts[1];
    rs.keep = parts[2];
    uint8_t cp[kMaxKeyLen], cs[kMaxKeyLen], key[2 * kMaxKeyLen + 8];
    const size_t cpl = capKey(cp, reinterpret_cast<const uint8_t*>(rs.provider.data()), rs.provider.size());
    const size_t csl = capKey(cs, reinterpret_cast<const uint8_t*>(rs.source.data()), rs.source.size());
    const size_t kl = encStr2(key, cp, cpl, cs, csl);
    const int32_t rc = committedPostings(c, kIxTagPS, key, kl, [&](const uint8_t* v) {
        rs.instances.push_back(getBE64(v));
        return true;
    });
    std::sort(rs.instances.begin(), rs.instances.end());
    rs.instances.erase(std::unique(rs.instances.begin(), rs.instances.end()), rs.instances.end());
    tHotPathDepth = saved;
    if (rc < 0) {
        c.err = rc;
        return false;
    }
    return true;
}

// Scans ahead for the TXN_END matching a TXN_BEGIN at pos. Returns the end
// position (after TXN_END) or 0 when it is not in the ring yet.
uint64_t txnEnd(Ctx& c, uint64_t pos, uint64_t tail, uint64_t* bytes) {
    uint64_t q = pos;
    while (q + sizeof(EntryHeader) <= tail) {
        EntryHeader h;
        ringRead(c.p->ring, c.e->pool(), q, &h, sizeof(h));
        if (h.entryLen < sizeof(EntryHeader) || q + h.entryLen > tail) return 0;
        q += h.entryLen;
        if (h.kind == kEntTxnEnd) {
            *bytes = q - pos;
            return q;
        }
    }
    return 0;
}

}  // namespace

// Finalizes the staged batch: meta batch bytes in the batch arena.
static bool finalizeBatch(Ctx& c, Arena* batches) {
    Partition* p = c.p;
    StageScratch& sc = *c.sc;
    Staged& st = *c.st;
    st.nRows = sc.nRows;
    if (st.sealAfter) {
        uint8_t body[20];
        putU32(body, p->dSeg);
        putU64(body + 4, p->dLen + st.dBytes);
        putU64(body + 12, st.nextPseq);
        addCtl(c, kCtlSeal, body, sizeof(body));
    }
    if (sc.nRows == 0 && sc.nCtl == 0 && sc.laneFrameBytes == 0) return true;  // ack-only
    // More than 32 live lanes: the batch carries the full lane table so a
    // head can point at it (the head holds 32 inline).
    uint32_t laneCkptAt = UINT32_MAX;
    if (sc.nDeltas) {
        uint32_t live = 0;
        for (const auto& l : p->lanes) {
            int64_t cnt = l.c.count;
            for (uint32_t i = 0; i < sc.nDeltas; i++)
                if (sc.deltas[i].laneId == l.id) cnt += sc.deltas[i].dCount;
            if (cnt != 0) live++;
        }
        if (live > kMaxInlineLanes) {
            const size_t need = 4 + size_t(live) * sizeof(LaneCounter);
            if (sc.ctlBytes + 4 + need <= sizeof(sc.ctl) && need <= 65535) {
                laneCkptAt = sc.ctlBytes + 4;
                putU16(sc.ctl + sc.ctlBytes, kCtlLaneCkpt);
                putU16(sc.ctl + sc.ctlBytes + 2, uint16_t(need));
                uint8_t* q = sc.ctl + sc.ctlBytes + 4;
                putU32(q, live);
                q += 4;
                const int64_t now = c.e->nowMs();
                for (const auto& l : p->lanes) {
                    LaneCounter lc = l.c;
                    for (uint32_t i = 0; i < sc.nDeltas; i++) {
                        const LaneDelta& d = sc.deltas[i];
                        if (d.laneId != l.id) continue;
                        lc.count += d.dCount;
                        lc.bytes += d.dBytes;
                        if (d.maxPseq > lc.maxPseq) lc.maxPseq = d.maxPseq;
                        if (d.dCount > 0 && (lc.firstSeen == 0 || d.firstSeen < lc.firstSeen)) lc.firstSeen = d.firstSeen;
                        lc.updated = d.updated > lc.updated ? d.updated : now;
                    }
                    if (lc.count == 0) continue;
                    lc.laneId = l.id;
                    std::memcpy(q, &lc, sizeof(lc));
                    q += sizeof(lc);
                }
                sc.ctlBytes += uint32_t(4 + need);
                sc.nCtl++;
            } else {
                c.err = FLATSQL_IO_ERR_GENERIC;  // lane table beyond one batch: refuse
                return false;
            }
        }
    }
    for (uint32_t i = 0; i < sc.nEntries; i++) sc.order[i] = &sc.entries[i];
    sortStaged(sc.order, sc.nEntries);
    const size_t l0Len = sc.nRows ? l0BlockSize(sc.order, sc.nEntries) : 0;
    const size_t rowsLen = size_t(sc.nRows) * sizeof(RecRow);
    const size_t attrLen = pad8(sc.attrBytes);
    const size_t deltasLen = pad8(4 + size_t(sc.nDeltas) * sizeof(LaneDelta));
    const size_t ctlLen = pad8(4 + sc.ctlBytes);
    const size_t batchLen = sizeof(BatchHeader) + rowsLen + attrLen + l0Len + deltasLen + ctlLen +
                            sizeof(BatchTrailer);
    uint8_t* b = static_cast<uint8_t*>(batches->alloc(batchLen, 8));
    if (!b) return false;
    const uint64_t mOff = p->mEnd;
    BatchHeader hdr{};
    hdr.magic = kMagicBatch;
    hdr.ver = 1;
    hdr.flags = uint16_t((st.dBytes ? kBatchHasData : 0) | (c.e->config().commitJournal ? kBatchJournaled : 0));
    hdr.commitSeq = p->commitSeq + 1;
    hdr.firstPseq = st.firstPseq;
    hdr.nRows = sc.nRows;
    hdr.nAttr = sc.nAttr;
    hdr.dSeg = p->dSeg;
    hdr.batchLen = uint32_t(batchLen);
    hdr.dOff = st.dOff;
    hdr.dLen = st.dBytes;
    hdr.attrBytes = uint32_t(attrLen);
    hdr.l0Bytes = uint32_t(l0Len);
    std::memcpy(b, &hdr, sizeof(hdr));
    size_t off = sizeof(BatchHeader);
    const uint64_t attrBase = mOff + sizeof(BatchHeader) + rowsLen;
    for (uint32_t i = 0; i < sc.nRows; i++) {
        RecRow& r = sc.rows[i];
        if (r.flags & kRowAttrInM) r.attrOff = attrBase + r.attrOff;
        std::memcpy(b + off, &r, sizeof(RecRow));
        off += sizeof(RecRow);
    }
    std::memcpy(b + off, sc.attrs, sc.attrBytes);
    std::memset(b + off + sc.attrBytes, 0, attrLen - sc.attrBytes);
    off += attrLen;
    st.l0Off = uint32_t(off);
    st.l0Len = uint32_t(l0Len);
    if (l0Len) off += writeL0Block(b + off, sc.order, sc.nEntries, st.firstPseq, st.nextPseq - 1);
    putU32(b + off, sc.nDeltas);
    std::memcpy(b + off + 4, sc.deltas, size_t(sc.nDeltas) * sizeof(LaneDelta));
    std::memset(b + off + 4 + sc.nDeltas * sizeof(LaneDelta), 0,
                deltasLen - 4 - sc.nDeltas * sizeof(LaneDelta));
    off += deltasLen;
    putU32(b + off, sc.nCtl);
    if (laneCkptAt != UINT32_MAX) st.laneCkptOff = mOff + off + 4 + laneCkptAt;
    std::memcpy(b + off + 4, sc.ctl, sc.ctlBytes);
    std::memset(b + off + 4 + sc.ctlBytes, 0, ctlLen - 4 - sc.ctlBytes);
    off += ctlLen;
    BatchTrailer tr{};
    tr.magic = kMagicTrailer;
    tr.batchLen = uint32_t(batchLen);
    tr.commitSeq = hdr.commitSeq;
    tr.pseqHi = st.nextPseq - 1;
    tr.dCommitted = st.dOff + st.dBytes;
    tr.activeSeg = p->dSeg;
    tr.ownerEpoch = p->ownerEpoch;
    tr.counters = st.counters;
    tr.incarnation = c.e->incarnation();
    tr.writerId = c.w->id();
    std::memcpy(b + off, &tr, sizeof(tr));
    const uint32_t crc = crc32c(b, off + offsetof(BatchTrailer, crc));
    putU32(b + off + offsetof(BatchTrailer, crc), crc);
    st.batch = b;
    st.batchLen = uint32_t(batchLen);
    st.mOff = mOff;
    st.mSeg = p->mSeg;
    if (sc.laneFrameBytes) {
        uint8_t* lf = static_cast<uint8_t*>(batches->alloc(sc.laneFrameBytes, 8));
        if (!lf) return false;
        std::memcpy(lf, sc.laneFrames, sc.laneFrameBytes);
        st.laneFrames = lf;
        st.laneFrameBytes = sc.laneFrameBytes;
        st.lOff = p->lExtent;
    }
    if (sc.nDeltas) {
        LaneDelta* d = static_cast<LaneDelta*>(batches->alloc(sizeof(LaneDelta) * sc.nDeltas, 8));
        if (!d) return false;
        std::memcpy(d, sc.deltas, sizeof(LaneDelta) * sc.nDeltas);
        st.deltas = d;
        st.nDeltas = sc.nDeltas;
    }
    st.firstNewLaneIndex = sc.firstNewLaneIndex;
    return true;
}

// TOMB_RANGE step (§13): examines up to reconcileStep rows of the segment.
void tombRangeStep(Ctx& c) {
    Partition* p = c.p;
    Staged* st = c.st;
    TombRange& tr = p->ranges.front();
    if (!tr.started) {
        tr.started = true;
        tr.next = tr.end = 0;
        if (tr.seg == p->dSeg) {
            tr.next = p->segFirstPseq;  // the active segment, as of now
            tr.end = p->pseqHi + 1;
        } else {
            for (const auto& si : p->segs) {
                if (si.seg != tr.seg || !si.sealed) continue;
                tr.next = si.firstPseq;
                tr.end = si.endPseq;
            }
        }
    }
    const uint32_t step = c.e->config().reconcileStep;
    const uint32_t rows0 = c.sc->nRows;
    uint64_t cur = tr.next;
    for (uint32_t examined = 0; cur < tr.end && examined < step; examined++) {
        if (c.sc->nRows + 4 > c.sc->capRows || c.sc->nEntries + 8 > c.sc->capEntries) break;
        RecRow row;
        const int32_t rc = readRow(c, cur, &row);
        if (rc < 0) {
            c.err = rc;
            return;
        }
        if (row.kind == kRowPut && row.epochMs < tr.beforeMs && row.supersedeHash == 0 && !isDead(c, cur) &&
            !stageKill(c, cur))
            break;
        if (c.err) return;
        cur++;
    }
    const bool done = cur >= tr.end;
    if (c.sc->nRows == rows0) {
        // Nothing staged: the cursor moves now (TOMBs of earlier steps are
        // already durable).
        tr.next = cur;
        if (done) {
            std::atomic<int32_t>* rem = tr.remaining;
            p->ranges.erase(p->ranges.begin());
            if (rem && rem->fetch_sub(1, std::memory_order_acq_rel) == 1)
                wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(rem), -1);
        }
        return;
    }
    st->rangeStep = true;
    st->rangeNext = cur;
    st->rangeDone = done;
    if (done && tr.remaining) st->tickets[st->nTickets++] = tr.remaining;
}

bool partitionStage(Writer* w, Partition* p, StageScratch* sc, Arena* frames, Arena* batches) {
    Engine* e = w->engine();
    Staged* st = batches->make<Staged>();
    if (!st) return false;
    sc->resetPartition();
    st->p = p;
    st->counters = p->counters;
    st->firstPseq = p->pseqHi + 1;
    st->nextPseq = st->firstPseq;
    st->dOff = p->dLen;
    st->dSeg = p->dSeg;
    st->endPos = p->ring->head.load(std::memory_order_acquire);
    const size_t framesMark = frames->used();
    Ctx c{w, e, p, sc, st, &w->io(), frames};
    HotPathScope hot;
    // Merge pipeline ctl records (INTENT_MERGE queued by maintenance, and
    // MERGE_DONE once the outputs are written; their syncs share this round).
    if (p->nPendingCtl && sc->ctlBytes + p->pendingCtlBytes <= sizeof(sc->ctl)) {
        std::memcpy(sc->ctl + sc->ctlBytes, p->pendingCtl, p->pendingCtlBytes);
        sc->ctlBytes += p->pendingCtlBytes;
        sc->nCtl += p->nPendingCtl;
        st->consumedPendingCtl = true;
    }
    if (p->mergePhase == kMergeOutputsWritten) {
        uint8_t body[40];
        mergeDoneBody(p, body);
        addCtl(c, kCtlMergeDone, body, sizeof(body));
        st->mergeDone = true;
    }
    // Type-level kills from the mailbox (A14). They leave the queue when the
    // batch publishes; a discarded batch retries them.
    while (st->killsTaken < p->kills.size() && st->nTickets < 63) {
        PendingKill& k = p->kills[p->kills.size() - 1 - st->killsTaken];
        uint8_t key[kCidKeyLen];
        cidSortKey(k.cid, key);
        StageScratch::Cid* cs = cidState(c, key);
        if (!cs) break;
        if (cs->putPseq && !stageKill(c, cs->putPseq)) break;
        if (k.remaining) st->tickets[st->nTickets++] = k.remaining;
        st->killsTaken++;
    }
    if (!p->ranges.empty() && st->nTickets < 64 && !c.err) tombRangeStep(c);
    const EngineConfig& cfg = e->config();
    uint64_t pos = st->endPos;
    const uint64_t tail = p->ring->tail.load(std::memory_order_acquire);
    uint32_t frames0 = 0;
    struct Rej {
        uint64_t rseq;
        int32_t code;
    };
    Rej rejects[kRejectSlots];
    uint32_t nRej = 0;
    const uint64_t rejHead = p->ring->rejectHead.load(std::memory_order_relaxed);
    const uint64_t rejTail = p->ring->rejectTail.load(std::memory_order_acquire);
    const uint32_t rejRoom = uint32_t(kRejectSlots - (rejHead - rejTail));
    if (p->sealPending && p->dLen > 0) st->sealAfter = true;
    while (!st->sealAfter && !c.err && pos + sizeof(EntryHeader) <= tail) {
        if (st->dBytes >= cfg.commitBytes || frames0 >= cfg.commitFrames) break;
        if (p->nL0 >= kMaxL0Dir - 1) break;  // wait for a merge (type labeling lags)
        if (nRej >= rejRoom) break;
        EntryHeader h;
        ringRead(p->ring, e->pool(), pos, &h, sizeof(h));
        if (h.entryLen < sizeof(EntryHeader) || (h.entryLen & 7) || pos + h.entryLen > tail) {
            // Corrupt producer: quarantine the ring rather than guess.
            p->ring->state.store(kRingQuarantined, std::memory_order_release);
            p->quarantined = true;
            break;
        }
        int32_t rejectCode = 0;
        StepResult r = kConsumed;
        switch (h.kind) {
            case kEntRecord:
                r = stageRecord(c, h, pos, &rejectCode);
                if (r == kConsumed) frames0++;
                break;
            case kEntLicence:
                r = stageLicence(c, h, pos, &rejectCode);
                break;
            case kEntTombCid: {
                uint8_t key[kCidKeyLen];
                cidSortKey(h.cid, key);
                StageScratch::Cid* cs = cidState(c, key);
                if (!cs) r = kStop;
                else if (cs->putPseq && !stageKill(c, cs->putPseq)) r = kStop;
                break;
            }
            case kEntReconcile: {
                if (!p->rec.active) {
                    if (sc->nRows || st->consumed) {
                        r = kStop;  // start RECONCILE on a clean batch
                        break;
                    }
                    if (!reconcileStart(c, h, pos)) {
                        rejectCode = kRejBadEntry;
                        r = kRejected;
                        break;
                    }
                }
                if (reconcileStep(c)) {
                    uint8_t body[8];
                    putU64(body, h.rseq);
                    addCtl(c, kCtlReconcileDone, body, 8);
                    const int saved = tHotPathDepth;
                    tHotPathDepth = 0;
                    p->rec = ReconcileState();
                    tHotPathDepth = saved;
                    st->reconcileDone = true;
                    r = kConsumed;
                } else {
                    r = kStop;
                }
                break;
            }
            case kEntTxnBegin: {
                uint64_t bytes = 0;
                const uint64_t endPos = txnEnd(c, pos, tail, &bytes);
                if (!endPos) {
                    r = kStop;
                    break;
                }
                // Resource bound for the whole transaction: it lands in one
                // batch (never split, §14) or is rejected whole.
                uint32_t nCtl = 0;
                for (uint64_t q = pos + h.entryLen; q < endPos;) {
                    EntryHeader ch;
                    ringRead(p->ring, e->pool(), q, &ch, sizeof(ch));
                    if (ch.kind == kEntCtl) nCtl++;
                    q += ch.entryLen;
                }
                auto fits = [&](uint32_t rows0, uint32_t ents0, uint32_t keys0, uint32_t attrs0,
                                size_t framesRoom) {
                    return rows0 + 2 * nCtl + 8 <= sc->capRows &&
                           ents0 + 4 * nCtl + 16 <= sc->capEntries &&
                           keys0 + uint64_t(nCtl) * (2 * kMaxKeyLen + 32) <= sc->capKeys &&
                           attrs0 + bytes + 8 * nCtl <= sc->capAttrs && framesRoom >= bytes &&
                           sc->nDead + nCtl + 8 <= StageScratch::kDeadCap;
                };
                const bool fitsEmpty = fits(0, 0, 0, 0, frames->remaining() + (frames->used() - framesMark)) &&
                                       bytes <= cfg.commitBytes;
                if (!fitsEmpty) {
                    rejectCode = kRejTxnTooLarge;
                    r = kRejected;
                    h.entryLen = uint32_t(endPos - pos);
                    uint64_t q = pos;
                    while (q < endPos) {
                        EntryHeader ch;
                        ringRead(p->ring, e->pool(), q, &ch, sizeof(ch));
                        h.rseq = ch.rseq;
                        q += ch.entryLen;
                    }
                    break;
                }
                if (!fits(sc->nRows, sc->nEntries, sc->keyBytes, sc->attrBytes, frames->remaining()) ||
                    ((sc->nRows || st->consumed) && st->dBytes + bytes > cfg.commitBytes)) {
                    r = kStop;
                    break;
                }
                // Transaction id: the first pseq it writes (unique forever,
                // unlike ring sequence numbers, which restart with the ring).
                const uint64_t txnId = st->nextPseq;
                uint64_t q = pos + h.entryLen;
                while (q < endPos && !c.err) {
                    EntryHeader ch;
                    ringRead(p->ring, e->pool(), q, &ch, sizeof(ch));
                    if (ch.kind == kEntCtl) {
                        int32_t rc2 = 0;
                        if (stageCtl(c, ch, q, txnId, &rc2) != kConsumed && !c.err)
                            c.err = FLATSQL_IO_ERR_GENERIC;  // cannot happen after the bound
                    }
                    h.rseq = ch.rseq;
                    q += ch.entryLen;
                }
                uint8_t body[8];
                putU64(body, txnId);
                addCtl(c, kCtlTxnEnd, body, 8);
                h.entryLen = uint32_t(endPos - pos);
                r = kConsumed;
                break;
            }
            case kEntCtl: {
                const uint64_t txnId = st->nextPseq;
                r = stageCtl(c, h, pos, txnId, &rejectCode);
                if (r == kConsumed) {
                    uint8_t body[8];
                    putU64(body, txnId);
                    addCtl(c, kCtlTxnEnd, body, 8);
                }
                break;
            }
            case kEntTxnEnd:
                r = kConsumed;  // stray end
                break;
            default:
                rejectCode = kRejBadEntry;
                r = kRejected;
                break;
        }
        if (r == kStop) break;
        if (r == kRejected) {
            rejects[nRej++] = {h.rseq, rejectCode};
            e->cRejects.fetch_add(1, std::memory_order_relaxed);
        }
        pos += h.entryLen;
        st->endPos = pos;
        st->lastRseq = h.rseq;
        st->consumed = true;
    }
    if (c.err) {
        // Nothing of this batch is committed; entries are re-read next time.
        frames->truncate(framesMark);
        p->st = nullptr;
        return false;
    }
    // Rejects become visible with the acks of this commit.
    for (uint32_t i = 0; i < nRej; i++) ringPushReject(p->ring, rejects[i].rseq, rejects[i].code);
    if (!finalizeBatch(c, batches)) {
        frames->truncate(framesMark);
        p->st = nullptr;
        return false;
    }
    if (!st->consumed && st->nextPseq == st->firstPseq && !st->batch && st->nTickets == 0) {
        // Kills of cids this partition does not hold, without tickets: done.
        if (st->killsTaken) p->kills.resize(p->kills.size() - st->killsTaken);
        p->st = nullptr;
        return false;
    }
    p->st = st;
    return true;
}

// ---------------------------------------------------------------------------
// Heads
// ---------------------------------------------------------------------------
void encodePartitionHead(const Partition* p, uint8_t* slot, uint32_t* used, bool durable) {
    PartitionHeadFixed h{};
    h.p.magic = kMagicHead;
    h.p.format = kFormat;
    h.p.kind = kHeadPartition;
    h.p.gen = p->headGen;
    h.p.ownerEpoch = p->ownerEpoch;
    h.p.id = p->pid;
    h.p.flags = (durable ? uint32_t(kHeadDurableCkpt) : 0u) | (p->quarantined ? uint32_t(kHeadQuarantined) : 0u);
    h.commitSeq = p->commitSeq;
    h.pseqHi = p->pseqHi;
    h.mSeg = p->mSeg;
    h.incarnation = p->incarnation;
    h.mEnd = p->mEnd;
    h.dSeg = p->dSeg;
    h.nextSeg = p->nextSeg;
    h.dLen = p->dLen;
    h.mergedThrough = p->mergedThrough;
    h.manifestGen = p->manifestGen;
    h.nL0 = uint16_t(p->nL0);
    h.firstLiveMSeg = 0;
    h.nextGen = p->nextGen;
    h.schemaFp = p->type && p->type->cfg ? p->type->cfg->fingerprint() : 0;
    h.producerHash = hash64(p->token.data(), p->token.size());
    h.counters = p->counters;
    h.nextLaneId = p->nextLaneId;
    h.segFirstPseq = p->segFirstPseq;
    h.intentSeg = p->intentSeg;
    h.intentGen = p->intentGen;
    h.intentROff = p->intentROff;
    h.intentAOff = p->intentAOff;
    h.intentThrough = p->intentThrough;
    // Lanes with a non-zero count go inline; past 32, the head points at the
    // last LANE_CKPT ctl record (written by the commit that overflowed).
    uint32_t nLanes = 0;
    for (const auto& l : p->lanes)
        if (l.c.count != 0) nLanes++;
    size_t off = sizeof(h);
    std::memcpy(slot + off, p->l0, sizeof(L0DirEntry) * p->nL0);
    off += sizeof(L0DirEntry) * p->nL0;
    if (nLanes <= kMaxInlineLanes) {
        h.nLanes = uint16_t(nLanes);
        for (const auto& l : p->lanes) {
            if (l.c.count == 0) continue;
            std::memcpy(slot + off, &l.c, sizeof(LaneCounter));
            off += sizeof(LaneCounter);
        }
    } else {
        h.nLanes = 0xffff;
        h.lanesOverflowSeg = p->lanesOverflowSeg;
        h.lanesOverflowOff = p->lanesOverflowOff;
    }
    std::memcpy(slot, &h, sizeof(h));
    *used = uint32_t(off + 4);
    sealHeadSlot(slot, *used);
}

int32_t partitionWriteHead(Writer* w, Partition* p, bool durable) {
    uint8_t slot[kHeadSlotBytes];
    p->headGen++;
    uint32_t used;
    encodePartitionHead(p, slot, &used, durable);
    int32_t rc = w->io().write(p->h, slot, used, (p->headGen % 2) * kHeadSlotBytes);
    if (rc < 0) return rc;
    return 0;
}

// ---------------------------------------------------------------------------
// Publish / rollback
// ---------------------------------------------------------------------------
int32_t loadAccelFromBlock(Partition* p, SlabPool& pool, uint32_t idx, const uint8_t* block,
                           uint32_t len) {
    L0KindInfo kinds[L0Accel::kMaxKinds];
    size_t nk = 0;
    L0Accel& a = p->acc[idx];
    if (!parseL0Block(block, len, kinds, L0Accel::kMaxKinds, &nk)) return FLATSQL_IO_ERR_IO;
    a.nKinds = 0;
    for (size_t i = 0; i < nk; i++) {
        L0Accel::Kind& k = a.kinds[a.nKinds++];
        k.kind = kinds[i].kind;
        k.vlen = kinds[i].vlen;
        k.n = kinds[i].n;
        k.entriesOff = a.l0Off + kinds[i].entriesOff;
        k.entriesBytes = kinds[i].entriesBytes;
        k.bloom = nullptr;
        k.bloomBytes = 0;
        if (kinds[i].bloomBytes) {
            uint64_t pos;
            void* mem = p->chain.alloc(pool, kinds[i].bloomBytes, &pos);
            if (!mem) return FLATSQL_IO_ERR_NOSPACE;
            if (i == 0 || a.chainPos == 0) a.chainPos = pos;
            std::memcpy(mem, block + kinds[i].bloomOff, kinds[i].bloomBytes);
            k.bloom = static_cast<const uint8_t*>(mem);
            k.bloomBytes = kinds[i].bloomBytes;
        }
    }
    return 0;
}

void partitionPublish(Writer* w, Partition* p, Staged* st) {
    Engine* e = w->engine();
    const uint64_t firstPseq = st->firstPseq;
    const uint32_t nRows = uint32_t(st->nextPseq - st->firstPseq);
    if (st->batch) {
        if (st->consumedPendingCtl) {
            p->nPendingCtl = 0;
            p->pendingCtlBytes = 0;
            if (p->mergePhase == kMergeIntentQueued) {
                p->intentSeg = p->mplan.seg;
                p->intentGen = p->mplan.gen;
                p->intentROff = p->mplan.rOff;
                p->intentAOff = p->mplan.aOff;
                p->intentThrough = p->mplan.through;
                p->mergePhase = kMergeIntentDurable;
            }
        }
        if (st->mergeDone) partitionMergeApply(w, p);
        p->commitSeq++;
        p->pseqHi = st->nextPseq - 1;
        p->mEnd = st->mOff + st->batchLen;
        p->dLen = st->dOff + st->dBytes;
        p->counters = st->counters;
        p->incarnation = e->incarnation();
        p->metaSinceCkpt += st->batchLen;
        if (st->laneFrameBytes) p->lExtent = st->lOff + st->laneFrameBytes;
        if (st->laneCkptOff) {
            p->lanesOverflowSeg = st->mSeg;
            p->lanesOverflowOff = st->laneCkptOff;
        }
        const int64_t now = e->nowMs();
        for (uint32_t i = 0; i < st->nDeltas; i++) {
            const LaneDelta& d = st->deltas[i];
            for (auto& l : p->lanes) {
                if (l.id != d.laneId) continue;
                l.c.count += d.dCount;
                l.c.bytes += d.dBytes;
                if (d.maxPseq > l.c.maxPseq) l.c.maxPseq = d.maxPseq;
                if (d.dCount > 0 && (l.c.firstSeen == 0 || d.firstSeen < l.c.firstSeen))
                    l.c.firstSeen = d.firstSeen;
                l.c.updated = d.updated > l.c.updated ? d.updated : now;
                break;
            }
        }
        if (nRows) {
            const uint32_t idx = p->nL0;
            L0DirEntry& de = p->l0[idx];
            de.mSeg = st->mSeg;
            de.nRows = nRows;
            de.mOff = st->mOff;
            de.firstPseq = firstPseq;
            de.batchLen = st->batchLen;
            de.l0Off = st->l0Off;
            L0Accel& a = p->acc[idx];
            a.mSeg = st->mSeg;
            a.mOff = st->mOff;
            a.firstPseq = firstPseq;
            a.nRows = nRows;
            a.batchLen = st->batchLen;
            a.l0Off = st->mOff + st->l0Off;
            a.l0Len = st->l0Len;
            a.chainPos = 0;
            p->nL0++;
            if (st->l0Len && loadAccelFromBlock(p, e->pool(), idx, st->batch + st->l0Off, st->l0Len) < 0) {
                // Accelerator memory exhausted: lookups fall back to reading
                // the section (no bloom) -- correctness is unaffected.
                for (int k = 0; k < a.nKinds; k++) a.kinds[k].bloom = nullptr;
            }
            p->segRecords += nRows;
        }
        if (st->sealAfter) {
            // Switch the active segment (minor 3: segment numbers come from
            // the durable batch).
            SegmentInfo* si = nullptr;
            for (auto& s : p->segs)
                if (s.seg == p->dSeg) si = &s;
            if (!si) {
                p->segs.emplace_back();
                si = &p->segs.back();
                si->seg = p->dSeg;
                si->firstPseq = p->segFirstPseq;
                si->mergedEnd = p->segFirstPseq;
            }
            si->sealed = true;
            si->endPseq = p->pseqHi + 1;
            si->dLen = p->dLen;
            // The old active handles become the segment's read handles.
            if (!si->m.valid()) si->m = p->m;
            else w->io().close(&p->m);
            if (!si->d.valid()) si->d = p->d;
            else w->io().close(&p->d);
            p->m = FileRef();
            p->d = FileRef();
            p->dSeg = p->nextSeg;
            p->mSeg = p->nextSeg;
            p->nextSeg++;
            p->dLen = 0;
            p->mEnd = 0;
            p->mExtent = 0;
            p->dExtent = 0;
            p->segFirstPseq = p->pseqHi + 1;
            p->segRecords = 0;
            p->segOpenedMs = e->nowMs();
            p->sealPending = false;
            e->cSeals.fetch_add(1, std::memory_order_relaxed);
        }
        const bool lockStats = e->config().lockStats;
        const uint64_t l0 = lockStats ? monoNs() : 0;
        p->pubLock.writeBegin();
        p->pub.commitSeq = p->commitSeq;
        p->pub.pseqHi = p->pseqHi;
        p->pub.nL0 = p->nL0;
        std::memcpy(p->pub.l0, p->l0, sizeof(L0DirEntry) * p->nL0);
        p->pubLock.writeEnd();
        if (lockStats) e->seqlockHist().record(monoNs() - l0);
        p->durablePseqHi.store(p->pseqHi, std::memory_order_release);
        p->durableCommitSeq.store(p->commitSeq, std::memory_order_release);
        p->durableMEnd.store(p->mEnd, std::memory_order_release);
    }
    // Acks (§7): every entry consumed by this commit.
    if (st->consumed) {
        RingDesc* r = p->ring;
        // Adaptive page supply: twice what this commit consumed, so a busy
        // producer never waits for the writer's next iteration.
        const uint64_t consumed = st->endPos - r->head.load(std::memory_order_relaxed);
        const uint32_t pages = uint32_t(consumed / r->slabBytes) + 1;
        const uint32_t capPages = uint32_t((r->cap + r->maxEntry) / r->slabBytes);
        p->mapAheadPages = std::max<uint32_t>(2, std::min<uint32_t>(2 * pages + 1, capPages));
        ringRelease(r, e->pool(), st->endPos);
        r->ackedRseq.store(st->lastRseq, std::memory_order_release);
        r->ackGen.fetch_add(1, std::memory_order_release);
        if (r->prodWaiting.load(std::memory_order_acquire)) wakeU32(&r->ackGen, -1);
    }
    if (st->killsTaken) p->kills.resize(p->kills.size() - st->killsTaken);  // shrink: no allocation
    if (st->rangeStep && !p->ranges.empty()) {
        p->ranges.front().next = st->rangeNext;
        if (st->rangeDone) p->ranges.erase(p->ranges.begin());
    }
    for (uint32_t i = 0; i < st->nTickets; i++) {
        if (st->tickets[i]->fetch_sub(1, std::memory_order_acq_rel) == 1)
            wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(st->tickets[i]), -1);
    }
    if (nRows && p->type) {
        typePostNotice(p->type, p->pid);
        const uint32_t tw = p->type->ownerWriter.load(std::memory_order_relaxed);
        if (tw != w->id() && tw < w->engine()->writerCount()) w->engine()->writer(tw)->ring();
    }
    p->laneCount.store(uint32_t(p->lanes.size()), std::memory_order_relaxed);
    p->lastActivityNs = monoNs();
    p->st = nullptr;
}

void partitionRollback(Writer* w, Partition* p, Staged* st) {
    (void)w;
    // Lanes interned by the failed batch are forgotten (their l.fsl frames
    // were not durable).
    if (st->laneFrameBytes && st->firstNewLaneIndex < p->lanes.size()) {
        // Rebuild the hash index without the dropped lanes.
        p->lanes.resize(st->firstNewLaneIndex);
        p->laneByHash.clear();
        for (uint32_t i = 0; i < p->lanes.size(); i++) {
            TagView t;
            t.present = true;
            const Lane& l = p->lanes[i];
            t.provider = reinterpret_cast<const uint8_t*>(l.provider.data());
            t.providerLen = l.provider.size();
            t.source = reinterpret_cast<const uint8_t*>(l.source.data());
            t.sourceLen = l.source.size();
            t.batch = reinterpret_cast<const uint8_t*>(l.batch.data());
            t.batchLen = l.batch.size();
            t.peer = reinterpret_cast<const uint8_t*>(l.peer.data());
            t.peerLen = l.peer.size();
            t.pubkey = reinterpret_cast<const uint8_t*>(l.pubkey.data());
            t.pubkeyLen = l.pubkey.size();
            p->laneByHash.emplace(laneHash(t), i);
        }
        p->nextLaneId = p->lanes.empty() ? 1 : p->lanes.back().id + 1;
    }
    // A RECONCILE step that did not commit restarts from its entry (idempotent).
    if (p->rec.active || st->reconcileDone) p->rec = ReconcileState();
    p->st = nullptr;
}

}  // namespace ps
}  // namespace flatsql
