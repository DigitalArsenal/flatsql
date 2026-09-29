// FlatSQL partition store: the record virtual tables (design §9, A17, A18,
// A19): the module, plans (xBestIndex), partition-level row sources, and
// column projection. The type-level merge lives in vtab_fanout.cpp.
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/reflection.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <unordered_set>

#include "flatsql/ps/flatsql_attr_generated.h"
#include "flatsql/ps/platform.h"
#include "internal.h"
#include "vtab_internal.h"

namespace flatsql {
namespace ps {

const char* const kMetaColNames[kMcCount] = {
    "_pseq",     "_cid",       "_cid_bin", "_epoch",  "_arrival", "_gseq",   "_producer",
    "_source",   "_source_name", "_provider", "_batch", "_peer_id", "_signature", "_data",
    "_offset",   "_len",       "_kind",    "_rowid",  "_pid",     "_object", "_asof",
    "_forward",  "_nearest"};

static const char* kMetaColTypes[kMcCount] = {"INTEGER", "TEXT",    "BLOB",    "INTEGER", "INTEGER",
                                               "INTEGER", "TEXT",    "TEXT",    "TEXT",    "TEXT",
                                               "TEXT",    "BLOB",    "BLOB",    "BLOB",    "INTEGER",
                                               "INTEGER", "INTEGER", "INTEGER", "INTEGER", "TEXT",
                                               "INTEGER", "INTEGER", "INTEGER"};

// ---------------------------------------------------------------------------
// Plans
// ---------------------------------------------------------------------------
std::string Plan::encode() const {
    char buf[240];
    snprintf(buf, sizeof(buf), "%c:%u:%u:%u:%u:%d:%d:%d:%u:%u:%d:%d:%u:%d:%d:%u:%d:%d:%d:%d:%d:%u:%d:%d:%u:%u",
             bounded ? 'B' : 'U', unsigned(access), unsigned(desc), unsigned(tagKind), unsigned(col), int(aKey),
             int(aLo), int(aHi), unsigned(loOp), unsigned(hiOp), int(aProducer), int(aSource), unsigned(sourceFull),
             int(aLimit), int(aOffset), unsigned(orderConsumed), int(aTag[0]), int(aTag[1]), int(aTag[2]),
             int(aTag[3]), int(aTag[4]), unsigned(pointKind), int(aPoint), int(aOffsetSeen), unsigned(gseqTags),
             unsigned(sandboxWindow));
    return buf;
}

bool Plan::decode(const char* s) {
    if (!s || (s[0] != 'B' && s[0] != 'U')) return false;
    unsigned acc, d, tk, c, lop, hop, sf, oc, pk, gt, sw;
    int k, lo, hi, pr, so, li, of, t0, t1, t2, t3, t4, ap, aos;
    if (sscanf(s + 1, ":%u:%u:%u:%u:%d:%d:%d:%u:%u:%d:%d:%u:%d:%d:%u:%d:%d:%d:%d:%d:%u:%d:%d:%u:%u", &acc, &d, &tk,
               &c, &k, &lo, &hi, &lop, &hop, &pr, &so, &sf, &li, &of, &oc, &t0, &t1, &t2, &t3, &t4, &pk, &ap, &aos,
               &gt, &sw) != 25)
        return false;
    pointKind = uint8_t(pk);
    aPoint = int8_t(ap);
    aOffsetSeen = int8_t(aos);
    gseqTags = gt != 0;
    sandboxWindow = sw != 0;
    aTag[0] = int8_t(t0);
    aTag[1] = int8_t(t1);
    aTag[2] = int8_t(t2);
    aTag[3] = int8_t(t3);
    aTag[4] = int8_t(t4);
    bounded = s[0] == 'B';
    access = uint8_t(acc);
    desc = d != 0;
    tagKind = uint8_t(tk);
    col = uint16_t(c);
    aKey = int8_t(k);
    aLo = int8_t(lo);
    aHi = int8_t(hi);
    loOp = uint8_t(lop);
    hiOp = uint8_t(hop);
    aProducer = int8_t(pr);
    aSource = int8_t(so);
    sourceFull = sf != 0;
    aLimit = int8_t(li);
    aOffset = int8_t(of);
    orderConsumed = oc != 0;
    return true;
}

// ---------------------------------------------------------------------------
// Row filter
// ---------------------------------------------------------------------------
namespace {
bool eqBytes(const std::string& want, const uint8_t* p, size_t n) {
    return want.size() == n && (n == 0 || std::memcmp(want.data(), p, n) == 0);
}
}  // namespace

bool TagMatch::matches(const TagView& t) const {
    if (!t.present) return false;
    return (!hasProvider || eqBytes(provider, t.provider, t.providerLen)) &&
           (!hasBatch || eqBytes(batch, t.batch, t.batchLen)) && (!hasPeer || eqBytes(peer, t.peer, t.peerLen)) &&
           (!hasSource || eqBytes(source, t.source, t.sourceLen));
}

// ---- §37: pruned snapshots, REPEAT state, tags on every copy -------------------
bool laneMatches(const TagMatch& m, const LaneStore::LaneTuple& t);
PartSnap* StmtShared::prunedFor(PartSnap* s, uint64_t pseq) {
    if (!s || !s->manifest || s->manifest->segs.size() < 4) return s;
    const auto& segs = s->manifest->segs;
    // Segments are in pseq order: the first one that can hold a posting
    // keyed by pseq (open, never used, or ending after it).
    size_t a = 0, b = segs.size();
    while (a < b) {
        const size_t mid = (a + b) / 2;
        const ManifestSegRef& m = segs[mid];
        const bool keep = !m.sealed || m.firstPseq == 0 || m.endPseq > pseq;
        if (keep) b = mid;
        else a = mid + 1;
    }
    const size_t step = std::max<size_t>(1, segs.size() / 16);
    const size_t first = a / step * step;
    if (first == 0) return s;
    for (auto& p : pruned)
        if (p.base == s && p.first == first) return p.snap.get();
    std::unique_ptr<PartSnap> c(new PartSnap(*s));
    auto m = std::make_shared<Manifest>(*s->manifest);
    m->segs.erase(m->segs.begin(), m->segs.begin() + std::ptrdiff_t(first));
    c->manifest = m;
    Pruned pr;
    pr.base = s;
    pr.first = first;
    pr.snap = std::move(c);
    pruned.push_back(std::move(pr));
    return pruned.back().snap.get();
}

int32_t StmtShared::readRow(LaneStore* st, const PartSnap& s, uint64_t pseq, RecRow* out) {
    RowAhead* ra = nullptr;
    for (RowAhead& r : ahead_)
        if (r.snap == &s) ra = &r;
    if (!ra) {
        if (ahead_.size() >= 64) ahead_.erase(ahead_.begin());
        ahead_.push_back(RowAhead());
        ra = &ahead_.back();
        ra->snap = &s;
    }
    if (pseq >= ra->first && pseq < ra->first + ra->rows.size()) {
        *out = ra->rows[size_t(pseq - ra->first)];
        ra->next = pseq + 1;
        return 0;
    }
    // Ascending just past what was read: read ahead; otherwise one row (a
    // random access reads no more than it uses).
    const bool ascending = ra->next && pseq >= ra->next && pseq < ra->next + 32;
    uint64_t n = 1;
    if (ascending && s.pseqHi() >= pseq) n = std::min<uint64_t>(32, s.pseqHi() - pseq + 1);
    ra->rows.resize(size_t(n));
    const int32_t rc = st->readRows(s, pseq, uint32_t(n), ra->rows.data());
    if (rc < 0) {
        ra->rows.clear();
        return rc;
    }
    ra->first = pseq;
    ra->next = pseq + 1;
    *out = ra->rows[0];
    return 0;
}

int32_t StmtShared::readFrame(LaneStore* st, const PartSnap& s, const RecRow& r, uint8_t* dst) {
    const bool hit = span_.snap == &s && span_.seg == r.seg && r.off >= span_.off &&
                     uint64_t(r.off) + r.len <= span_.off + span_.data.size();
    if (!hit) {
        uint64_t end = uint64_t(r.off) + r.len;
        for (const RowAhead& ra : ahead_) {
            if (ra.snap != &s) continue;
            for (const RecRow& x : ra.rows) {
                if (x.pseq <= r.pseq || x.seg != r.seg || x.kind != kRowPut || !x.len) continue;
                const uint64_t xe = uint64_t(x.off) + x.len;
                if (x.off < r.off || xe - r.off > (256u << 10)) break;
                if (xe > end) end = xe;
            }
        }
        span_.data.resize(size_t(end - r.off));
        const int32_t rc = st->readFramePart(s, r, 0, uint32_t(span_.data.size()), span_.data.data());
        if (rc < 0) {
            span_.snap = nullptr;
            return rc;
        }
        span_.snap = &s;
        span_.seg = r.seg;
        span_.off = r.off;
    }
    std::memcpy(dst, span_.data.data() + (r.off - span_.off), r.len);
    if (st->config().verifyFrameCrc && r.dataCrc && crc32c(dst, r.len) != r.dataCrc) return kRsCorrupt;
    return 0;
}

bool StmtShared::mayHave(LaneStore* st, const PartSnap& s, uint16_t kind) {
    for (const KindHas& k : kinds_)
        if (k.snap == &s && k.kind == kind) return k.has;
    bool has = false;
    int32_t rc = 0;
    if (!s.empty) {
        const SectionList* list = st->sections(s, kind, &rc);
        if (!list) has = true;  // unknown: look
        else
            for (const auto& sec : *list)
                if (sec && !sec->offs.empty()) has = true;
        if (!has && s.manifest) {
            for (const ManifestSegRef& m : s.manifest->segs) {
                for (const SegRunRef& rr : m.runs) {
                    FileKey fk;
                    auto run = st->run(s.pid, m.seg, rr.gen, rr.fileLen, &fk, &rc);
                    if (!run || run->kindEntries(kind) > 0) {
                        has = true;
                        break;
                    }
                }
                if (has) break;
            }
        }
    }
    if (kinds_.size() >= 256) kinds_.erase(kinds_.begin());
    kinds_.push_back({&s, kind, has});
    return has;
}

uint64_t StmtShared::laneMaxPseq(LaneStore* st, const PartSnap& s, const TagMatch& m) {
    for (const LaneMax& l : laneMax_)
        if (l.snap == &s && l.m == &m) return l.max;
    uint64_t mx = 0;
    std::vector<LaneCounter> lc;
    if (s.empty) {
        mx = 0;
    } else if (st->laneCounters(s, &lc) < 0) {
        mx = UINT64_MAX;
    } else {
        for (const LaneCounter& c : lc) {
            const LaneStore::LaneTuple* t = laneOf(st, s, c.laneId);
            if (!t) {
                mx = UINT64_MAX;  // a lane without its tuple: look at rows
                break;
            }
            if (laneMatches(m, *t) && c.maxPseq > mx) mx = c.maxPseq;
        }
    }
    if (laneMax_.size() >= 64) laneMax_.erase(laneMax_.begin());
    laneMax_.push_back({&s, &m, mx});
    return mx;
}

int32_t StmtShared::isDead(LaneStore* st, const PartSnap& s, uint64_t pseq, uint64_t bound, bool* dead) {
    *dead = false;
    if (!mayHave(st, s, kIxDead)) return 0;
    return st->isDead(s, pseq, bound, dead);
}

int32_t StmtShared::isTagDead(LaneStore* st, const PartSnap& s, uint64_t inst, uint64_t bound, bool* dead) {
    *dead = false;
    if (!mayHave(st, s, kIxTagDead)) return 0;
    return st->isTagDead(s, inst, bound, dead);
}

const LaneStore::LaneTuple* StmtShared::laneOf(LaneStore* st, const PartSnap& s, uint32_t laneId) {
    if (!laneId) return nullptr;
    Lanes* l = nullptr;
    for (Lanes& x : lanes_)
        if (x.snap == &s) l = &x;
    if (!l) {
        lanes_.push_back(Lanes());
        l = &lanes_.back();
        l->snap = &s;
        l->ok = st->laneTuples(s, &l->tuples) >= 0;
        std::sort(l->tuples.begin(), l->tuples.end(),
                  [](const LaneStore::LaneTuple& a, const LaneStore::LaneTuple& b) { return a.id < b.id; });
    }
    if (!l->ok) return nullptr;
    auto it = std::lower_bound(l->tuples.begin(), l->tuples.end(), laneId,
                               [](const LaneStore::LaneTuple& a, uint32_t id) { return a.id < id; });
    return it != l->tuples.end() && it->id == laneId ? &*it : nullptr;
}

namespace {
// The 8-byte head of a posting key (big-endian), 0-padded when shorter.
uint64_t keyHead(const uint8_t* k, uint16_t kl) {
    uint8_t b[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    std::memcpy(b, k, kl < 8 ? kl : 8);
    return getBE64(b);
}
}  // namespace

int32_t StmtShared::tagInstances(LaneStore* st, const PartSnap& s, uint64_t put, std::vector<uint64_t>* out) {
    out->clear();
    if (s.empty) return 0;
    uint8_t key[8];
    putBE64(key, put);
    int32_t rc = 0;
    const SectionList* list = st->sections(s, kIxTagOf, &rc);
    if (!list) return rc;
    for (const auto& sec : *list) {
        if (!sec) continue;
        for (size_t i = sec->lowerBound(key, 8); i < sec->offs.size(); i++) {
            const uint8_t* p = sec->entry(i);
            const uint16_t kl = getU16(p);
            if (kl < 8 || std::memcmp(p + 2, key, 8) != 0) break;
            out->push_back(getBE64(p + 2 + kl));
        }
    }
    if (!s.manifest) return 0;
    // The entries of `put` in a loaded block: binary search on the heads.
    auto collect = [&](const CachedBlock& cb, uint8_t vlen) {
        size_t a = 0, b = cb.offs.size();
        while (a < b) {
            const size_t mid = (a + b) / 2;
            const uint8_t* e = cb.data.data() + cb.offs[mid];
            if (keyHead(e + 2, getU16(e)) < put) a = mid + 1;
            else b = mid;
        }
        for (size_t i = a; i < cb.offs.size(); i++) {
            const uint8_t* e = cb.data.data() + cb.offs[i];
            const uint16_t kl = getU16(e);
            if (keyHead(e + 2, kl) != put) break;
            out->push_back(getBE64(e + 2 + kl));
            (void)vlen;
        }
    };
    for (const ManifestSegRef& m : s.manifest->segs) {
        // A TAG_OF posting keyed by `put` is written by its instance, never
        // before its target.
        if (m.sealed && m.firstPseq && m.endPseq <= put) continue;
        for (const SegRunRef& rr : m.runs) {
            FileKey fk;
            fk.space = FileKey::kPart;
            fk.letter = 'x';
            fk.id = s.pid;
            fk.seg = m.seg;
            fk.gen = rr.gen;
            CachedBlock* cb = nullptr;
            for (CachedBlock& c : blocks_)
                if (c.key == fk) cb = &c;
            // Rows come in pseq order: the block read for the previous
            // lookup usually holds this one's entries whole.
            if (cb && cb->block >= 0 && cb->firstHead < put && put < cb->lastHead) {
                collect(*cb, 0);
                continue;
            }
            auto run = st->run(s.pid, m.seg, rr.gen, rr.fileLen, &fk, &rc);
            if (!run) return rc;
            FenceView f;
            if (!run->fences(st, kIxTagOf, &f) || f.empty()) continue;
            // The block that can hold the first (put, *) key: the last one
            // whose first-key prefix is below the key's (fence prefixes are
            // 23 bytes, so equal 8-byte heads compare as below).
            uint32_t a = 0, b = f.size();
            while (a < b) {
                const uint32_t mid = (a + b) / 2;
                const L1Fence* fe = f.at(mid, &rc);
                if (!fe) return rc;
                if (prefixCmp(key, 8, *fe) > 0) a = mid + 1;
                else b = mid;
            }
            const uint32_t first = a == 0 ? 0 : a - 1;
            const uint8_t vlen = run->kindVlen(kIxTagOf);
            for (uint32_t blk = first; blk < f.size(); blk++) {
                const L1Fence* fe = f.at(blk, &rc);
                if (!fe) return rc;
                // Past the key: a later block whose first key's head is above it.
                if (blk > first && std::memcmp(fe->prefix, key, std::min<size_t>(8, fe->prefixLen)) > 0) break;
                if (!cb) {
                    if (blocks_.size() >= 256) blocks_.erase(blocks_.begin());
                    blocks_.push_back(CachedBlock());
                    cb = &blocks_.back();
                    cb->key = fk;
                }
                if (cb->block != int64_t(blk)) {
                    cb->data.resize(kL1BlockBytes);
                    cb->offs.clear();
                    rc = st->readBlock(fk, fe->blockOff, cb->data.data());
                    if (rc < 0) {
                        cb->block = -1;
                        return rc;
                    }
                    cb->block = int64_t(blk);
                    EntryIter it = l1BlockIter(cb->data.data(), vlen);
                    const uint8_t *ek, *ev;
                    uint16_t el;
                    const uint8_t* at = it.p;
                    while (it.next(&ek, &el, &ev)) {
                        cb->offs.push_back(uint16_t(at - cb->data.data()));
                        at = it.p;
                    }
                    if (!cb->offs.empty()) {
                        const uint8_t* e0 = cb->data.data() + cb->offs.front();
                        const uint8_t* e1 = cb->data.data() + cb->offs.back();
                        cb->firstHead = keyHead(e0 + 2, getU16(e0));
                        cb->lastHead = keyHead(e1 + 2, getU16(e1));
                    } else {
                        cb->firstHead = cb->lastHead = 0;
                    }
                }
                const size_t before = out->size();
                collect(*cb, vlen);
                // The run's entries of `put` end in this block unless its
                // last key is `put` itself.
                if (cb->offs.empty() || cb->lastHead > put) break;
                (void)before;
            }
        }
    }
    std::sort(out->begin(), out->end());
    out->erase(std::unique(out->begin(), out->end()), out->end());
    return 0;
}

int32_t typeHasRepeats(LaneStore* st, const TypeSnap& t, bool* yes) {
    *yes = false;
    if (t.empty) return 0;
    int32_t rc = 0;
    const SectionList* list = st->sections(t, kIxTypeRepeat, &rc);
    if (!list) return rc;
    for (const auto& sec : *list)
        if (sec && !sec->offs.empty()) {
            *yes = true;
            return 0;
        }
    for (const SegRunRef& rr : t.runs) {
        FileKey fk;
        auto run = st->typeRun(t.fid, rr.gen, rr.fileLen, &fk, &rc);
        if (!run) return rc;
        if (run->kindEntries(kIxTypeRepeat) > 0) {
            *yes = true;
            return 0;
        }
    }
    return 0;
}

// Does a tag instance's lane tuple meet the conditions? (The lane of a row
// is the tuple of its own tag: provider, source, batch, producer peer.)
bool laneMatches(const TagMatch& m, const LaneStore::LaneTuple& t) {
    return (!m.hasProvider || t.provider == m.provider) && (!m.hasSource || t.source == m.source) &&
           (!m.hasBatch || t.batch == m.batch) && (!m.hasPeer || t.peer == m.peer);
}

int32_t ownTagMatches(LaneStore* st, StmtShared* sh, const PartSnap& s, const PartSnap& deadSnap, const RecRow& r,
                      const TagMatch& m, uint64_t bound, bool* yes, std::vector<uint8_t>* scratch) {
    *yes = false;
    if (!(r.flags & kRowHasAttr) || r.attrLen == 0) return 0;
    int32_t rc;
    const LaneStore::LaneTuple* lt = sh ? sh->laneOf(st, s, r.laneId) : nullptr;
    if (lt) {
        // The lane table answers without reading the attribute.
        if (!laneMatches(m, *lt)) return 0;
    } else {
        rc = st->readAttr(s, r, scratch);
        if (rc < 0) return rc;
        AttrView av;
        if (!parseAttr(scratch->data(), scratch->size(), &av) || !m.matches(av.tag)) return 0;
    }
    bool dead = false;
    rc = sh ? sh->isTagDead(st, deadSnap, r.pseq, bound, &dead) : st->isTagDead(deadSnap, r.pseq, bound, &dead);
    if (rc < 0) return rc;
    *yes = !dead;
    return 0;
}

namespace {
// A live tag instance of PUT `put` in snapshot `s` (its own tag or a RETAG)
// matching `m` at `bound`. TAG_OF and TAG_DEAD postings keyed by `put` are
// read from the segments that can hold them only.
int32_t liveTagIn(LaneStore* st, StmtShared* sh, PartSnap* s, uint64_t put, uint64_t bound, const TagMatch& m,
                  bool* yes, std::vector<uint8_t>* attr, bool skipPut = false) {
    *yes = false;
    PartSnap* ps = sh ? sh->prunedFor(s, put) : s;
    std::vector<uint64_t> insts;
    if (sh) {
        const int32_t rc = sh->tagInstances(st, *s, put, &insts);
        if (rc < 0) return rc;
    } else {
        uint8_t lo[8], hi[8];
        putBE64(lo, put);
        putBE64(hi, put + 1);
        PostingScan sc = st->scan(*ps, kIxTagOf, lo, 8, hi, 8, false);
        if (sc.err()) return sc.err();
        for (; sc.valid(); sc.next()) insts.push_back(getBE64(sc.val()));
        if (sc.err()) return sc.err();
    }
    for (uint64_t inst : insts) {
        if (inst > bound || (skipPut && inst == put)) continue;
        bool dead = false;
        int32_t rc = sh ? sh->isTagDead(st, *ps, inst, bound, &dead) : st->isTagDead(*ps, inst, bound, &dead);
        if (rc < 0) return rc;
        if (dead) continue;
        RecRow ir;
        rc = sh ? sh->readRow(st, *s, inst, &ir) : st->readRow(*s, inst, &ir);
        if (rc < 0) return rc;
        const LaneStore::LaneTuple* lt = sh ? sh->laneOf(st, *s, ir.laneId) : nullptr;
        if (lt) {
            if (laneMatches(m, *lt)) {
                *yes = true;
                return 0;
            }
            continue;
        }
        rc = st->readAttr(*s, ir, attr);
        if (rc < 0) return rc;
        AttrView av;
        if (!parseAttr(attr->data(), attr->size(), &av)) continue;
        if (m.matches(av.tag)) {
            *yes = true;
            return 0;
        }
    }
    return 0;
}

struct CopyRef {
    PartSnap* snap = nullptr;
    uint64_t pseq = 0;
    uint64_t bound = 0;
    RecRow row{};
};

// The live copies of a record other than (pid, pseq), with their snapshots
// (type-level visibility), in (pid, pseq) order.
int32_t otherCopies(LaneStore* st, StmtShared* sh, StmtCtx* stmt, TypeSnap* ts, const uint8_t cid[kCidLen], uint32_t pid,
                    uint64_t pseq, std::vector<CopyRef>* out) {
    out->clear();
    std::vector<CatalogCopy> copies;
    int32_t rc = st->catalog(*ts, cid, &copies);
    if (rc < 0) return rc;
    std::sort(copies.begin(), copies.end(), [](const CatalogCopy& a, const CatalogCopy& b) {
        return a.pid != b.pid ? a.pid < b.pid : a.pseq < b.pseq;
    });
    for (const CatalogCopy& c : copies) {
        if (c.label == kLblDead) continue;
        if (c.pid == pid && c.pseq == pseq) continue;
        PartSnap* q = nullptr;
        rc = sh->lane->partForType(stmt, c.pid, ts->fid, &q);
        if (rc < 0) return rc;
        CopyRef r;
        r.snap = q;
        r.pseq = c.pseq;
        r.bound = std::min(q->pseqHi(), ts->labeledThrough(c.pid));
        if (c.pseq == 0 || c.pseq > r.bound) continue;
        rc = st->readRow(*q, c.pseq, &r.row);
        if (rc < 0) return rc;
        if (r.row.kind != kRowPut) continue;
        out->push_back(r);
    }
    return 0;
}
}  // namespace

int32_t RowFilter::hasLiveTag(uint64_t put, const TagMatch& m, bool* yes) {
    *yes = false;
    if (shared && put > shared->laneMaxPseq(store, *snap, m)) return 0;
    std::vector<uint8_t> attr;
    return liveTagIn(store, shared.get(), snap, put, bound, m, yes, &attr);
}

int32_t RowFilter::anyCopyTag(const RecRow& row, const TagMatch& m, bool* yes) {
    *yes = false;
    std::vector<uint8_t> attr;
    PartSnap* ds = shared ? shared->prunedFor(snap, row.pseq) : snap;
    // A copy after the last instance of every matching lane cannot match.
    const bool thisCan = !shared || row.pseq <= shared->laneMaxPseq(store, *snap, m);
    // The copy's own tag first (the common case: one read of its attribute).
    int32_t rc = thisCan ? ownTagMatches(store, shared.get(), *snap, *ds, row, m, bound, yes, &attr) : 0;
    if (rc < 0 || *yes) return rc;
    std::vector<CopyRef> others;
    TypeSnap* ts = type ? type : copies;
    if (ts && shared && shared->repeats && shared->lane) {
        rc = otherCopies(store, shared.get(), stmt, ts, row.cid, snap->pid, row.pseq, &others);
        if (rc < 0) return rc;
        for (CopyRef& c : others) {
            if (c.pseq > shared->laneMaxPseq(store, *c.snap, m)) continue;
            PartSnap* cds = shared->prunedFor(c.snap, c.pseq);
            rc = ownTagMatches(store, shared.get(), *c.snap, *cds, c.row, m, c.bound, yes, &attr);
            if (rc < 0 || *yes) return rc;
        }
    }
    // RETAG instances: this copy's, then every other copy's.
    if (thisCan) {
        rc = liveTagIn(store, shared.get(), snap, row.pseq, bound, m, yes, &attr, true);
        if (rc < 0 || *yes) return rc;
    }
    for (CopyRef& c : others) {
        if (c.pseq > shared->laneMaxPseq(store, *c.snap, m)) continue;
        rc = liveTagIn(store, shared.get(), c.snap, c.pseq, c.bound, m, yes, &attr, true);
        if (rc < 0 || *yes) return rc;
    }
    return 0;
}

int32_t RowFilter::accept(uint64_t pseq, CurRow* out) {
    if (pseq == 0 || pseq > bound) return 0;
    out->gseq = 0;
    out->hasObject = false;
    out->objectNull = false;
    if (type) {
        // Every labeled PUT is FIRST or REPEAT, and a REPEAT label always
        // leaves a REPEAT posting (bloom-tested): without one the row is
        // FIRST, and its gseq is looked up only if a column or the sandbox
        // window needs it. With one, the latest LABEL says whether it was
        // promoted since (A14).
        bool repeat = false;
        // A type without REPEAT postings has no REPEAT copy to look up.
        int32_t rc = shared && shared->repeatsKnown && !shared->repeats ? 0 : store->everRepeat(*type, snap->pid, pseq, &repeat);
        if (rc < 0) return rc;
        if (repeat || gseqFloor || wantGseq) {
            uint8_t label = 0;
            uint64_t gseq = 0;
            bool found = false;
            rc = store->labelOf(*type, snap->pid, pseq, &label, &gseq, &found);
            if (rc < 0) return rc;
            if (!found || label != kLblFirst) return 0;
            if (gseq < gseqFloor) return 0;
            out->gseq = gseq;
        }
    }
    int32_t rc;
    if (!knownLive) {
        bool dead = false;
        PartSnap* ds = shared ? shared->prunedFor(snap, pseq) : snap;
        rc = shared ? shared->isDead(store, *ds, pseq, bound, &dead) : store->isDead(*ds, pseq, bound, &dead);
        if (rc < 0) return rc;
        if (dead) return 0;
    }
    rc = shared && readAhead ? shared->readRow(store, *snap, pseq, &out->row) : store->readRow(*snap, pseq, &out->row);
    if (rc < 0) return rc;
    if (out->row.kind != kRowPut) return 0;
    if (tags.any) {
        bool yes = false;
        // Type level (§37): any live copy of the record; partition level:
        // this partition's instances.
        rc = (type || copies) ? anyCopyTag(out->row, tags, &yes) : hasLiveTag(pseq, tags, &yes);
        if (rc < 0) return rc;
        if (!yes) return 0;
    }
    out->pid = snap->pid;
    out->snap = snap;
    return 1;
}

int32_t RowFilter::acceptInstance(uint64_t put, const TagMatch& m, CurRow* out) {
    const int32_t rc0 = accept(put, out);
    if (rc0 != 0 || !type || !shared || !shared->repeats || !shared->lane) return rc0;
    // Rejected at type level. A live REPEAT copy whose instance matched
    // stands for its record: the record's FIRST row is emitted by the first
    // matching copy in (FIRST, then REPEATs by (pid, pseq)) order, among the
    // partitions the plan reads, so each record comes once.
    if (put == 0 || put > bound) return 0;
    uint8_t label = 0;
    uint64_t gseq = 0;
    bool found = false;
    int32_t rc = store->labelOf(*type, snap->pid, put, &label, &gseq, &found);
    if (rc < 0) return rc;
    if (!found || label != kLblRepeat) return 0;
    bool dead = false;
    rc = shared->isDead(store, *shared->prunedFor(snap, put), put, bound, &dead);
    if (rc < 0) return rc;
    if (dead) return 0;
    RecRow r;
    rc = store->readRow(*snap, put, &r);
    if (rc < 0) return rc;
    if (r.kind != kRowPut) return 0;
    std::vector<CatalogCopy> copies;
    rc = store->catalog(*type, r.cid, &copies);
    if (rc < 0) return rc;
    const CatalogCopy* first = nullptr;
    for (const CatalogCopy& c : copies)
        if (c.label == kLblFirst || c.label == kLblPromoted) first = &c;
    if (!first || !shared->allowed(first->pid)) return 0;
    std::vector<uint8_t> attr;
    PartSnap* fs = nullptr;
    rc = shared->lane->partForType(stmt, first->pid, type->fid, &fs);
    if (rc < 0) return rc;
    const uint64_t fb = std::min(fs->pseqHi(), type->labeledThrough(first->pid));
    if (first->pseq == 0 || first->pseq > fb) return 0;
    // The FIRST copy matches on its own: its partition's scan emits it.
    RecRow fr;
    rc = store->readRow(*fs, first->pseq, &fr);
    if (rc < 0) return rc;
    bool yes = false;
    rc = ownTagMatches(store, shared.get(), *fs, *shared->prunedFor(fs, first->pseq), fr, m, fb, &yes, &attr);
    if (rc < 0) return rc;
    if (!yes) {
        rc = liveTagIn(store, shared.get(), fs, first->pseq, fb, m, &yes, &attr, true);
        if (rc < 0) return rc;
    }
    if (yes) return 0;
    // An earlier REPEAT copy that matches emits it.
    std::vector<CatalogCopy> reps;
    for (const CatalogCopy& c : copies)
        if (c.label == kLblRepeat && shared->allowed(c.pid) &&
            (c.pid < snap->pid || (c.pid == snap->pid && c.pseq < put)))
            reps.push_back(c);
    std::sort(reps.begin(), reps.end(), [](const CatalogCopy& a, const CatalogCopy& b) {
        return a.pid != b.pid ? a.pid < b.pid : a.pseq < b.pseq;
    });
    for (const CatalogCopy& c : reps) {
        PartSnap* q = nullptr;
        rc = shared->lane->partForType(stmt, c.pid, type->fid, &q);
        if (rc < 0) return rc;
        const uint64_t qb = std::min(q->pseqHi(), type->labeledThrough(c.pid));
        if (c.pseq == 0 || c.pseq > qb) continue;
        RecRow qr;
        rc = store->readRow(*q, c.pseq, &qr);
        if (rc < 0) return rc;
        if (qr.kind != kRowPut) continue;
        bool qd = false;
        rc = shared->isDead(store, *shared->prunedFor(q, c.pseq), c.pseq, qb, &qd);
        if (rc < 0) return rc;
        if (qd) continue;
        rc = ownTagMatches(store, shared.get(), *q, *shared->prunedFor(q, c.pseq), qr, m, qb, &yes, &attr);
        if (rc < 0) return rc;
        if (!yes) {
            rc = liveTagIn(store, shared.get(), q, c.pseq, qb, m, &yes, &attr, true);
            if (rc < 0) return rc;
        }
        if (yes) return 0;
    }
    // This copy is the record's first match: emit the FIRST row.
    RowFilter f1 = *this;
    f1.snap = fs;
    f1.bound = fb;
    f1.tags = TagMatch();
    f1.knownLive = false;
    return f1.accept(first->pseq, out);
}

// ---------------------------------------------------------------------------
// Partition-level row sources
// ---------------------------------------------------------------------------
namespace {

class PostingRows : public RowSource {
public:
    PostingRows(const RowFilter& f, uint16_t kind, std::string lo, bool hasLo, std::string hi, bool hasHi, bool desc,
                bool instances)
        : f_(f), kind_(kind), lo_(std::move(lo)), hi_(std::move(hi)), hasLo_(hasLo), hasHi_(hasHi), desc_(desc),
          instances_(instances) {
        // Epoch and tag postings meet a partition's rows roughly in pseq
        // order (ascending scans): read rows ahead. CID postings do not.
        f_.readAhead = !desc_ && kind_ != kIxCid;
        if (instances_ && f_.tags.any) {
            // Tag conditions hold on the instance the posting names (ANY-row
            // semantics): checked here, not again on the record's PUT. The
            // scanned key alone needs no attribute read when it is exact
            // (not capped to prefix + hash).
            inst_ = f_.tags;
            f_.tags = TagMatch();
            const TagMatch& m = inst_;
            const bool scannedOnly =
                m.count() == 1 && ((kind_ == kIxTagProvider && m.hasProvider && m.provider.size() <= kMaxKeyLen) ||
                                   (kind_ == kIxTagBatch && m.hasBatch && m.batch.size() <= kMaxKeyLen) ||
                                   (kind_ == kIxTagPeer && m.hasPeer && m.peer.size() <= kMaxKeyLen) ||
                                   (kind_ == kIxSourceEpoch && m.hasSource && m.source.size() <= kMaxKeyLen));
            instCheck_ = !scannedOnly;
        }
    }

    int32_t next(CurRow* out) override {
        if (!started_) {
            started_ = true;
            scan_ = f_.store->scan(*f_.snap, kind_, hasLo_ ? reinterpret_cast<const uint8_t*>(lo_.data()) : nullptr,
                                   lo_.size(), hasHi_ ? reinterpret_cast<const uint8_t*>(hi_.data()) : nullptr,
                                   hi_.size(), desc_);
            if (scan_.err()) return scan_.err();
        } else if (scan_.valid()) {
            scan_.next();
        }
        for (; scan_.valid(); scan_.next()) {
            const int32_t prc = pollEvery(f_.store, &poll_);
            if (prc < 0) return prc;
            uint64_t pseq = getBE64(scan_.val());
            // Duplicate instances of one PUT under one key (TAG kinds) are
            // emitted once; no per-entry allocation.
            if (keyCmp(scan_.key(), scan_.klen(), reinterpret_cast<const uint8_t*>(lastKey_.data()), lastKey_.size()) != 0) {
                lastKey_.assign(reinterpret_cast<const char*>(scan_.key()), scan_.klen());
                emitted_.clear();
                emittedSet_.clear();
            }
            if (instances_) {
                // The value is a tag instance (the PUT itself or a RETAG).
                if (pseq == 0 || pseq > f_.bound) continue;
                bool dead = false;
                PartSnap* ds = f_.shared ? f_.shared->prunedFor(f_.snap, pseq) : f_.snap;
                int32_t rc = f_.shared ? f_.shared->isTagDead(f_.store, *ds, pseq, f_.bound, &dead)
                                       : f_.store->isTagDead(*ds, pseq, f_.bound, &dead);
                if (rc < 0) return rc;
                if (dead) continue;
                RecRow ir;
                rc = f_.shared && f_.readAhead ? f_.shared->readRow(f_.store, *f_.snap, pseq, &ir)
                                               : f_.store->readRow(*f_.snap, pseq, &ir);
                if (rc < 0) return rc;
                if (ir.kind != kRowRetag && ir.kind != kRowPut) continue;
                if (instCheck_) {
                    const LaneStore::LaneTuple* lt = f_.shared ? f_.shared->laneOf(f_.store, *f_.snap, ir.laneId) : nullptr;
                    if (lt) {
                        if (!laneMatches(inst_, *lt)) continue;
                    } else {
                        rc = f_.store->readAttr(*f_.snap, ir, &attr_);
                        if (rc < 0) return rc;
                        AttrView av;
                        if (!parseAttr(attr_.data(), attr_.size(), &av) || !inst_.matches(av.tag)) continue;
                    }
                }
                if (ir.kind == kRowRetag) pseq = ir.targetPseq;
            }
            // Many entries can share one key (a source's records at one epoch
            // second): past a few, a hash set keeps the check O(1).
            if (emittedSet_.empty() ? std::find(emitted_.begin(), emitted_.end(), pseq) != emitted_.end()
                                    : emittedSet_.count(pseq) != 0)
                continue;
            // Type level (§37): a REPEAT copy's instance stands for its record.
            const int32_t rc = instances_ && inst_.any ? f_.acceptInstance(pseq, inst_, out) : f_.accept(pseq, out);
            if (rc < 0) return rc;
            if (rc == 0) continue;
            if (!emittedSet_.empty()) {
                emittedSet_.insert(pseq);
            } else {
                emitted_.push_back(pseq);
                if (emitted_.size() > 32) {
                    emittedSet_.insert(emitted_.begin(), emitted_.end());
                    emitted_.clear();
                }
            }
            out->key.assign(reinterpret_cast<const char*>(scan_.key()), scan_.klen());
            return 1;
        }
        return scan_.err() ? scan_.err() : 0;
    }

private:
    RowFilter f_;
    uint16_t kind_;
    std::string lo_, hi_;
    bool hasLo_, hasHi_, desc_, instances_;
    TagMatch inst_;           // conditions on the instance (instances_)
    bool instCheck_ = false;  // read each instance's attribute to check them
    std::vector<uint8_t> attr_;
    bool started_ = false;
    PostingScan scan_;
    std::string lastKey_;
    std::vector<uint64_t> emitted_;
    std::unordered_set<uint64_t> emittedSet_;
    uint32_t poll_ = 0;
};

class PseqRows : public RowSource {
public:
    PseqRows(const RowFilter& f, uint64_t lo, uint64_t hi, bool desc) : f_(f), desc_(desc) {
        lo_ = lo ? lo : 1;
        hi_ = std::min(hi, f.bound);
        cur_ = desc ? hi_ : lo_;
    }
    int32_t next(CurRow* out) override {
        for (;;) {
            if (lo_ > hi_ || (desc_ ? cur_ < lo_ : cur_ > hi_) || cur_ == 0) return 0;
            const int32_t prc = pollEvery(f_.store, &poll_);
            if (prc < 0) return prc;
            // Cheap pre-filter from a chunk of rows: only PUTs are candidates.
            if (bufAt_ >= buf_.size()) {
                const uint64_t n = std::min<uint64_t>(64, desc_ ? cur_ - lo_ + 1 : hi_ - cur_ + 1);
                const uint64_t first = desc_ ? cur_ - n + 1 : cur_;
                buf_.resize(size_t(n));
                const int32_t rc = f_.store->readRows(*f_.snap, first, uint32_t(n), buf_.data());
                if (rc < 0) return rc;
                if (desc_) std::reverse(buf_.begin(), buf_.end());
                bufAt_ = 0;
            }
            const RecRow& r = buf_[bufAt_++];
            const uint64_t pseq = r.pseq;
            cur_ = desc_ ? pseq - 1 : pseq + 1;
            if (r.kind != kRowPut) continue;
            const int32_t rc = f_.accept(pseq, out);
            if (rc < 0) return rc;
            if (rc == 0) continue;
            out->key.clear();
            return 1;
        }
    }

private:
    RowFilter f_;
    bool desc_;
    uint64_t lo_, hi_, cur_;
    std::vector<RecRow> buf_;
    size_t bufAt_ = 0;
    uint32_t poll_ = 0;
};

class CidRowsPartition : public RowSource {
public:
    CidRowsPartition(const RowFilter& f, const uint8_t cid[kCidLen]) : f_(f) { std::memcpy(cid_, cid, kCidLen); }
    int32_t next(CurRow* out) override {
        if (!started_) {
            started_ = true;
            uint8_t key[kCidKeyLen];
            cidSortKey(cid_, key);
            const int32_t rc = f_.store->lookup(*f_.snap, kIxCid, key, kCidKeyLen, [&](const uint8_t* v) {
                cands_.push_back(getBE64(v));
                return true;
            });
            if (rc < 0) return rc;
            std::sort(cands_.begin(), cands_.end());
        }
        while (at_ < cands_.size()) {
            const uint64_t pseq = cands_[at_++];
            const int32_t rc = f_.accept(pseq, out);
            if (rc < 0) return rc;
            if (rc == 0) continue;
            out->key.clear();
            return 1;
        }
        return 0;
    }

private:
    RowFilter f_;
    uint8_t cid_[kCidLen];
    bool started_ = false;
    std::vector<uint64_t> cands_;
    size_t at_ = 0;
};

}  // namespace

std::unique_ptr<RowSource> makePostingRows(const RowFilter& f, uint16_t kind, std::string lo, bool hasLo,
                                           std::string hi, bool hasHi, bool desc, bool instances) {
    return std::unique_ptr<RowSource>(
        new PostingRows(f, kind, std::move(lo), hasLo, std::move(hi), hasHi, desc, instances));
}

std::unique_ptr<RowSource> makePseqRows(const RowFilter& f, uint64_t lo, uint64_t hi, bool desc) {
    return std::unique_ptr<RowSource>(new PseqRows(f, lo, hi, desc));
}

std::unique_ptr<RowSource> makeCidRowsPartition(const RowFilter& f, const uint8_t cid[kCidLen]) {
    return std::unique_ptr<RowSource>(new CidRowsPartition(f, cid));
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------
namespace {

std::string quoteIdent(const std::string& s) {
    std::string o = "\"";
    for (char c : s) {
        if (c == '"') o += "\"\"";
        else o += c;
    }
    return o + "\"";
}

std::string unquote(const char* a) {
    std::string s = a ? a : "";
    if (s.size() >= 2 && (s[0] == '\'' || s[0] == '"') && s.back() == s[0]) s = s.substr(1, s.size() - 2);
    return s;
}

bool parseFidHex(const std::string& hex, uint8_t fid[4]) {
    if (hex.size() != 8) return false;
    for (int i = 0; i < 4; i++) {
        unsigned v;
        if (sscanf(hex.c_str() + 2 * i, "%2x", &v) != 1) return false;
        fid[i] = uint8_t(v);
    }
    return true;
}

std::string fidHex(const uint8_t fid[4]) {
    char b[9];
    snprintf(b, sizeof(b), "%02x%02x%02x%02x", fid[0], fid[1], fid[2], fid[3]);
    return b;
}

// COL(n) mapping from the type's rule text: one alternative on a top-level
// field ("col <n> kind:FIELD") makes that column's EQ an index lookup.
void mapColumns(RecVtab* vt) {
    const TypeInfo& t = *vt->type;
    vt->colIndex.assign(t.cols.size(), -1);
    vt->colIsEnum.assign(t.cols.size(), 0);
    vt->colIsU64.assign(t.cols.size(), 0);
    if (!t.cfg) return;
    vt->hasSupersede = t.cfg->hasSupersede();
    const std::string& rules = t.cfg->rules();
    std::vector<int> objectCols;
    std::vector<int> u64Cols;
    size_t at = 0;
    while (at < rules.size()) {
        size_t nl = rules.find('\n', at);
        if (nl == std::string::npos) nl = rules.size();
        std::string line = rules.substr(at, nl - at);
        at = nl + 1;
        const size_t hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
        {
            char ow[16], list[200];
            if (sscanf(line.c_str(), "%15s %199s", ow, list) == 2 && std::strcmp(ow, "object") == 0) {
                vt->hasObjectRule = true;
                const char* p = list;
                while (*p) {
                    char* end = nullptr;
                    const long n = std::strtol(p, &end, 10);
                    if (end == p) break;
                    objectCols.push_back(int(n));
                    p = *end == ',' ? end + 1 : end;
                }
                continue;
            }
            unsigned cn;
            char kindAlts[400];
            if (sscanf(line.c_str(), "%15s %u %399s", ow, &cn, kindAlts) == 3 && std::strcmp(ow, "col") == 0) {
                // Any alternative a u64 key (the first present alternative
                // decides the bytes; a u64 one gives 8 big-endian bytes).
                if (std::strstr(kindAlts, "u64pos:")) u64Cols.push_back(int(cn));
            }
        }
        char word[16], alts[400];
        unsigned n;
        if (sscanf(line.c_str(), "%15s %u %399s", word, &n, alts) != 3 || std::strcmp(word, "col") != 0) continue;
        const std::string a = alts;
        if (a.find('|') != std::string::npos) continue;
        const size_t colon = a.find(':');
        if (colon == std::string::npos) continue;
        const std::string kind = a.substr(0, colon);
        const std::string path = a.substr(colon + 1);
        if (path.find('.') != std::string::npos || path.find('[') != std::string::npos) continue;
        for (size_t i = 0; i < t.cols.size(); i++) {
            if (t.cols[i].name != path) continue;
            vt->colIndex[i] = int(n);
            vt->colIsEnum[i] = kind == "enum";
            vt->colIsU64[i] = kind == "u64pos";
        }
    }
    for (int oc : objectCols)
        if (std::find(u64Cols.begin(), u64Cols.end(), oc) != u64Cols.end()) vt->objectU64 = true;
}

int recConnect(sqlite3* db, void* aux, int argc, const char* const* argv, sqlite3_vtab** out, char** err) {
    ReaderLane* lane = static_cast<ReaderLane*>(aux);
    if (argc < 4) {
        *err = sqlite3_mprintf("flatsql_ps: missing argument");
        return SQLITE_ERROR;
    }
    const std::string arg = unquote(argv[3]);
    auto reg = lane->store().registry();
    if (!reg) {
        *err = sqlite3_mprintf("flatsql_ps: no registry");
        return SQLITE_ERROR;
    }
    std::unique_ptr<RecVtab> vt(new RecVtab());
    vt->lane = lane;
    vt->tableName = argv[2] ? argv[2] : "";
    uint8_t fid[4];
    if (arg.size() > 2 && arg[0] == 'p' && arg[1] == ':') {
        vt->kind = kVkPartition;
        vt->pid = uint32_t(std::strtoul(arg.c_str() + 2, nullptr, 10));
        const PartInfo* pi = reg->part(vt->pid);
        if (!pi) {
            *err = sqlite3_mprintf("flatsql_ps: unknown partition");
            return SQLITE_ERROR;
        }
        std::memcpy(fid, pi->fid, 4);
    } else if (arg.size() >= 10 && (arg[0] == 't' || arg[0] == 'a' || arg[0] == 'c') && arg[1] == ':' &&
               parseFidHex(arg.substr(2, 8), fid)) {
        vt->kind = arg[0] == 't' ? kVkType : arg[0] == 'a' ? kVkAlias : kVkCurrent;
        if (vt->kind == kVkAlias) vt->source = arg.size() > 11 ? arg.substr(11) : std::string();
    } else {
        *err = sqlite3_mprintf("flatsql_ps: bad argument %s", arg.c_str());
        return SQLITE_ERROR;
    }
    const TypeInfo* ti = reg->typeByFid(fid);
    if (!ti) {
        *err = sqlite3_mprintf("flatsql_ps: unknown type");
        return SQLITE_ERROR;
    }
    for (const auto& t : reg->types)
        if (t.get() == ti) vt->type = t;
    vt->typeName = ti->typeName;
    vt->nSchemaCols = int(ti->cols.size());
    mapColumns(vt.get());
    std::string ddl = "CREATE TABLE x(";
    bool first = true;
    for (const ColumnDef& c : ti->cols) {
        if (!first) ddl += ", ";
        first = false;
        ddl += quoteIdent(c.name);
        const char* t = c.sqlType();
        if (*t) {
            ddl += " ";
            ddl += t;
        }
    }
    for (int i = 0; i < kMcCount; i++) {
        if (!first) ddl += ", ";
        first = false;
        ddl += quoteIdent(kMetaColNames[i]);
        ddl += " ";
        ddl += kMetaColTypes[i];
        ddl += " HIDDEN";
    }
    ddl += ")";
    const int rc = sqlite3_declare_vtab(db, ddl.c_str());
    if (rc != SQLITE_OK) {
        *err = sqlite3_mprintf("flatsql_ps: declare failed: %s", sqlite3_errmsg(db));
        return rc;
    }
    *out = vt.release();
    return SQLITE_OK;
}

int recDisconnect(sqlite3_vtab* v) {
    delete static_cast<RecVtab*>(v);
    return SQLITE_OK;
}

bool isRangeOp(int op) {
    return op == SQLITE_INDEX_CONSTRAINT_GT || op == SQLITE_INDEX_CONSTRAINT_GE ||
           op == SQLITE_INDEX_CONSTRAINT_LT || op == SQLITE_INDEX_CONSTRAINT_LE;
}
bool isLower(int op) { return op == SQLITE_INDEX_CONSTRAINT_GT || op == SQLITE_INDEX_CONSTRAINT_GE; }

int recBestIndex(sqlite3_vtab* v, sqlite3_index_info* info) {
    RecVtab* vt = static_cast<RecVtab*>(v);
    const bool typeLevel = vt->kind != kVkPartition;
    const int ns = vt->nSchemaCols;
    auto meta = [&](int col) { return col >= ns ? col - ns : -1; };
    int cidEq = -1, pseqEq = -1, pseqLo = -1, pseqHi = -1, gseqEq = -1, gseqLo = -1, gseqHi = -1;
    int epochLo = -1, epochHi = -1, epochEq = -1, producerEq = -1, sourceEq = -1, sourceNameEq = -1;
    int tagEq[3] = {-1, -1, -1};
    int colEq = -1, colIdx = -1;
    int limitC = -1, offsetC = -1;
    // §37: the per-object point inputs, and what the point plan can evaluate
    // before it chooses (epoch range, tags, producer); anything else would be
    // applied by SQLite after the choice, which is not the same query.
    int pointEq[3] = {-1, -1, -1};
    int nPoint = 0, epochLoN = 0, epochHiN = 0, nonGseq = 0;
    bool pointBlocked = false;
    for (int i = 0; i < info->nConstraint; i++) {
        const auto& c = info->aConstraint[i];
        if (!c.usable) continue;
        const int op = c.op;
        if (op == SQLITE_INDEX_CONSTRAINT_LIMIT) {
            limitC = i;
            continue;
        }
        if (op == SQLITE_INDEX_CONSTRAINT_OFFSET) {
            offsetC = i;
            continue;
        }
        const int m = c.iColumn < 0 ? kMcRowid : meta(c.iColumn);
        const bool eq = op == SQLITE_INDEX_CONSTRAINT_EQ;
        // Constraints other than a type-level gseq range: an OFFSET the vtab
        // owns must skip exactly the rows SQLite would have kept.
        if (!(typeLevel && (m == kMcGseq || m == kMcRowid) && (eq || isRangeOp(op)))) nonGseq++;
        if (m < 0) {
            pointBlocked = true;
            if (eq && c.iColumn >= 0 && c.iColumn < int(vt->colIndex.size()) && vt->colIndex[c.iColumn] >= 0 &&
                !vt->colIsEnum[c.iColumn] && colEq < 0) {
                colEq = i;
                colIdx = vt->colIndex[c.iColumn];
            }
            continue;
        }
        if (m == kMcAsof || m == kMcForward || m == kMcNearest) {
            if (eq) {
                pointEq[m - kMcAsof] = i;
                nPoint++;
            } else {
                pointBlocked = true;
            }
            continue;
        }
        if (!(m == kMcEpoch && isRangeOp(op)) && m != kMcProducer && m != kMcSource && m != kMcSourceName &&
            m != kMcProvider && m != kMcBatch && m != kMcPeerId)
            pointBlocked = true;
        if (m == kMcEpoch && isRangeOp(op)) (isLower(op) ? epochLoN : epochHiN)++;
        if ((m == kMcProducer || m == kMcSource || m == kMcSourceName || m == kMcProvider || m == kMcBatch ||
             m == kMcPeerId) &&
            !eq)
            pointBlocked = true;
        const bool rowidIsPseq = !typeLevel;
        if (m == kMcPseq || (m == kMcRowid && rowidIsPseq)) {
            if (typeLevel) continue;
            if (eq) pseqEq = i;
            else if (isRangeOp(op)) (isLower(op) ? pseqLo : pseqHi) = i;
        } else if (m == kMcGseq || (m == kMcRowid && !rowidIsPseq)) {
            if (!typeLevel) continue;
            if (eq) gseqEq = i;
            else if (isRangeOp(op)) (isLower(op) ? gseqLo : gseqHi) = i;
        } else if (m == kMcCid || m == kMcCidBin) {
            if (eq) cidEq = i;
        } else if (m == kMcEpoch) {
            if (eq) epochEq = i;
            else if (isRangeOp(op)) (isLower(op) ? epochLo : epochHi) = i;
        } else if (m == kMcProducer) {
            if (eq) producerEq = i;
        } else if (m == kMcSource) {
            if (eq) sourceEq = i;
        } else if (m == kMcSourceName) {
            if (eq) sourceNameEq = i;
        } else if (m == kMcProvider) {
            if (eq) tagEq[0] = i;
        } else if (m == kMcBatch) {
            if (eq) tagEq[1] = i;
        } else if (m == kMcPeerId) {
            if (eq) tagEq[2] = i;
        }
    }
    // ORDER BY shape.
    enum {
        kOrdNone,
        kOrdEpochDesc,
        kOrdEpochAsc,
        kOrdGseqAsc,
        kOrdGseqDesc,
        kOrdPseqAsc,
        kOrdPseqDesc,
        kOrdCidAsc,
        kOrdCidDesc,
        kOrdOther
    } ord = kOrdNone;
    if (info->nOrderBy >= 1) {
        const auto& o = info->aOrderBy[0];
        const int m = o.iColumn < 0 ? kMcRowid : meta(o.iColumn);
        const bool second = info->nOrderBy == 2;
        const int m2 = second ? (info->aOrderBy[1].iColumn < 0 ? kMcRowid : meta(info->aOrderBy[1].iColumn)) : -1;
        if (m == kMcEpoch && (!second || (o.desc && m2 == kMcCid && !info->aOrderBy[1].desc)) && info->nOrderBy <= 2)
            ord = o.desc ? kOrdEpochDesc : kOrdEpochAsc;
        else if (info->nOrderBy == 1 && (m == kMcGseq || (m == kMcRowid && typeLevel)))
            ord = o.desc ? kOrdGseqDesc : kOrdGseqAsc;
        else if (info->nOrderBy == 1 && (m == kMcPseq || (m == kMcRowid && !typeLevel)))
            ord = o.desc ? kOrdPseqDesc : kOrdPseqAsc;
        else if (info->nOrderBy == 1 && m == kMcCid)
            ord = o.desc ? kOrdCidDesc : kOrdCidAsc;
        else
            ord = kOrdOther;
        if (ord == kOrdEpochAsc && second) ord = kOrdOther;  // (epoch ASC, cid ...) is not an index order
    }
    Plan p;
    int argc = 0;
    auto use = [&](int ci, int8_t* slot) {
        if (ci < 0) return;
        info->aConstraintUsage[ci].argvIndex = ++argc;
        info->aConstraintUsage[ci].omit = 0;
        *slot = int8_t(argc);
    };
    auto isIn = [&](int ci) { return ci >= 0 && sqlite3_vtab_in(info, ci, -1); };
    const bool alias = vt->kind == kVkAlias;
    const bool current = vt->kind == kVkCurrent;
    const bool anyTag = tagEq[0] >= 0 || tagEq[1] >= 0 || tagEq[2] >= 0 || sourceEq >= 0 || sourceNameEq >= 0 || alias;
    // A18: untrusted SQL reads <TYPE> as its newest N arrivals. Every
    // type-level plan but a point lookup reads that window from the arrivals
    // tail (at most N entries), whatever index the statement names.
    StmtCtx* sc = vt->lane ? vt->lane->current() : nullptr;
    const bool sandbox = sc && sc->sandbox() && typeLevel && !current;
    const bool point = nPoint > 0 && !current && !sandbox;
    double cost = 1e12;
    double rows = 1e9;
    bool anyIn = false;
    if (point) {
        if (nPoint > 1 || pointBlocked || epochLoN > 1 || epochHiN > 1 || epochEq >= 0) return SQLITE_CONSTRAINT;
        p.access = kAccObjPoint;
        const int k = pointEq[0] >= 0 ? 0 : pointEq[1] >= 0 ? 1 : 2;
        p.pointKind = uint8_t(k == 0 ? kPointAsof : k == 1 ? kPointForward : kPointNearest);
        use(pointEq[k], &p.aPoint);
        info->aConstraintUsage[pointEq[k]].omit = 1;
        if (isIn(pointEq[k])) return SQLITE_CONSTRAINT;
        // The epoch range is applied before the per-object choice; SQLite's
        // re-check of it is then a no-op.
        if (epochLo >= 0) {
            use(epochLo, &p.aLo);
            p.loOp = uint8_t(info->aConstraint[epochLo].op);
        }
        if (epochHi >= 0) {
            use(epochHi, &p.aHi);
            p.hiOp = uint8_t(info->aConstraint[epochHi].op);
        }
        p.orderConsumed = ord == kOrdNone;
        p.bounded = false;
        cost = 1e6;
        rows = 1e5;
    } else if (current) {
        p.access = kAccCurrent;
        p.bounded = false;
    } else if (cidEq >= 0) {
        p.access = kAccCid;
        use(cidEq, &p.aKey);
        anyIn = isIn(cidEq);
        p.bounded = true;
        cost = 10;
        rows = 1;
        p.orderConsumed = !anyIn;
    } else if (pseqEq >= 0) {
        p.access = kAccPseq;
        use(pseqEq, &p.aLo);
        p.loOp = SQLITE_INDEX_CONSTRAINT_GE;
        p.aHi = p.aLo;
        p.hiOp = SQLITE_INDEX_CONSTRAINT_LE;
        anyIn = isIn(pseqEq);
        p.bounded = true;
        cost = 5;
        rows = 1;
        p.orderConsumed = !anyIn;
    } else if (gseqEq >= 0) {
        p.access = kAccGseq;
        use(gseqEq, &p.aLo);
        p.loOp = SQLITE_INDEX_CONSTRAINT_GE;
        p.aHi = p.aLo;
        p.hiOp = SQLITE_INDEX_CONSTRAINT_LE;
        anyIn = isIn(gseqEq);
        p.bounded = true;
        cost = 20;
        rows = 1;
        p.orderConsumed = !anyIn;
    } else if (colEq >= 0 && !sandbox) {
        p.access = kAccCol;
        p.col = uint16_t(colIdx);
        use(colEq, &p.aKey);
        anyIn = isIn(colEq);
        p.bounded = true;
        cost = 50;
        rows = 10;
    } else if (sandbox || (typeLevel && anyTag && (ord == kOrdGseqAsc || ord == kOrdGseqDesc) &&
                           (limitC >= 0 || (gseqLo >= 0 && gseqHi >= 0)))) {
        // Arrivals (gseq) order. §37 gap 5: a gseq-ordered page with tag
        // conditions reads arrivals and checks each row's tags (or, when the
        // lane counters say the tags are rare, collects and sorts their
        // postings) instead of sorting every match of the tag index.
        p.access = kAccGseq;
        p.sandboxWindow = sandbox;
        p.gseqTags = !sandbox && anyTag;
        if (gseqLo >= 0) {
            use(gseqLo, &p.aLo);
            p.loOp = uint8_t(info->aConstraint[gseqLo].op);
        }
        if (gseqHi >= 0) {
            use(gseqHi, &p.aHi);
            p.hiOp = uint8_t(info->aConstraint[gseqHi].op);
        }
        p.desc = ord == kOrdGseqDesc;
        p.orderConsumed = ord == kOrdGseqAsc || ord == kOrdGseqDesc || ord == kOrdNone;
        p.bounded = (gseqLo >= 0 && gseqHi >= 0) || (limitC >= 0 && p.orderConsumed);
        cost = p.bounded ? 300 : 1e7;
        rows = p.bounded ? 1000 : 1e7;
    } else if (!alias && (tagEq[0] >= 0 || tagEq[1] >= 0 || tagEq[2] >= 0) && sourceEq < 0 && sourceNameEq < 0) {
        p.access = kAccTag;
        const int k = tagEq[1] >= 0 ? 1 : tagEq[0] >= 0 ? 0 : 2;
        p.tagKind = uint8_t(k);
        use(tagEq[k], &p.aKey);
        p.aTag[k] = p.aKey;
        anyIn = isIn(tagEq[k]);
        p.bounded = true;
        cost = 1000;
        rows = 1000;
    } else if (alias || sourceEq >= 0 || sourceNameEq >= 0) {
        p.access = kAccSource;
        if (!alias) {
            const int s = sourceNameEq >= 0 ? sourceNameEq : sourceEq;
            p.sourceFull = sourceNameEq < 0;
            use(s, &p.aKey);
            p.aTag[sourceNameEq >= 0 ? 3 : 4] = p.aKey;
            anyIn = isIn(s);
        }
        if (epochEq >= 0) {
            use(epochEq, &p.aLo);
            p.loOp = SQLITE_INDEX_CONSTRAINT_GE;
            p.aHi = p.aLo;
            p.hiOp = SQLITE_INDEX_CONSTRAINT_LE;
        } else {
            if (epochLo >= 0) {
                use(epochLo, &p.aLo);
                p.loOp = uint8_t(info->aConstraint[epochLo].op);
            }
            if (epochHi >= 0) {
                use(epochHi, &p.aHi);
                p.hiOp = uint8_t(info->aConstraint[epochHi].op);
            }
        }
        p.desc = ord != kOrdEpochAsc;
        p.orderConsumed = !anyIn && (ord == kOrdEpochDesc || ord == kOrdEpochAsc || ord == kOrdNone);
        if (ord == kOrdEpochDesc && info->nOrderBy == 2) p.orderConsumed = false;  // ties are not in cid order
        const bool closed = (p.aLo >= 0 && p.aHi >= 0);
        p.bounded = closed || limitC >= 0 || !alias;
        cost = closed ? 200 : 2000;
        rows = 1000;
    } else if ((ord == kOrdCidAsc || ord == kOrdCidDesc) && gseqLo < 0 && gseqHi < 0 && pseqLo < 0 && pseqHi < 0 &&
               epochLo < 0 && epochHi < 0 && epochEq < 0) {
        // A17: text CID order from the index whose keys sort as the text
        // does: the type's cid catalog (one pass over its entries, no row
        // read for rows skipped by OFFSET), a partition's CID postings.
        p.access = kAccCidOrder;
        p.desc = ord == kOrdCidDesc;
        p.orderConsumed = true;
        p.bounded = limitC >= 0;
        cost = p.bounded ? 500 : 1e8;
        rows = p.bounded ? 1000 : 1e8;
    } else if (typeLevel && (gseqLo >= 0 || gseqHi >= 0 || ord == kOrdGseqAsc || ord == kOrdGseqDesc)) {
        p.access = kAccGseq;
        if (gseqLo >= 0) {
            use(gseqLo, &p.aLo);
            p.loOp = uint8_t(info->aConstraint[gseqLo].op);
        }
        if (gseqHi >= 0) {
            use(gseqHi, &p.aHi);
            p.hiOp = uint8_t(info->aConstraint[gseqHi].op);
        }
        p.desc = ord == kOrdGseqDesc;
        p.orderConsumed = ord == kOrdGseqAsc || ord == kOrdGseqDesc || ord == kOrdNone;
        p.bounded = (gseqLo >= 0 && gseqHi >= 0) || limitC >= 0;
        cost = p.bounded ? 300 : 1e7;
        rows = p.bounded ? 1000 : 1e7;
    } else if (!typeLevel && (pseqLo >= 0 || pseqHi >= 0 || ord == kOrdPseqAsc || ord == kOrdPseqDesc)) {
        p.access = kAccPseq;
        if (pseqLo >= 0) {
            use(pseqLo, &p.aLo);
            p.loOp = uint8_t(info->aConstraint[pseqLo].op);
        }
        if (pseqHi >= 0) {
            use(pseqHi, &p.aHi);
            p.hiOp = uint8_t(info->aConstraint[pseqHi].op);
        }
        p.desc = ord == kOrdPseqDesc;
        p.orderConsumed = ord == kOrdPseqAsc || ord == kOrdPseqDesc || ord == kOrdNone;
        p.bounded = (pseqLo >= 0 && pseqHi >= 0) || limitC >= 0;
        cost = p.bounded ? 300 : 1e7;
        rows = p.bounded ? 1000 : 1e7;
    } else if (epochLo >= 0 || epochHi >= 0 || epochEq >= 0 || ord == kOrdEpochDesc || ord == kOrdEpochAsc ||
               (typeLevel && ord == kOrdNone)) {
        p.access = kAccEpoch;
        if (epochEq >= 0) {
            use(epochEq, &p.aLo);
            p.loOp = SQLITE_INDEX_CONSTRAINT_GE;
            p.aHi = p.aLo;
            p.hiOp = SQLITE_INDEX_CONSTRAINT_LE;
        } else {
            if (epochLo >= 0) {
                use(epochLo, &p.aLo);
                p.loOp = uint8_t(info->aConstraint[epochLo].op);
            }
            if (epochHi >= 0) {
                use(epochHi, &p.aHi);
                p.hiOp = uint8_t(info->aConstraint[epochHi].op);
            }
        }
        if (ord == kOrdEpochDesc || ord == kOrdEpochAsc) {
            p.tagKind = 1;  // EPOCH (ms) index: exact _epoch order
            p.desc = ord == kOrdEpochDesc;
        } else {
            p.tagKind = 0;  // EPOCH_CID: the default order (A19), ascending scan
            p.desc = false;
        }
        p.orderConsumed = ord == kOrdEpochDesc || ord == kOrdEpochAsc || ord == kOrdNone;
        const bool closed = p.aLo >= 0 && p.aHi >= 0;
        p.bounded = closed || limitC >= 0;
        cost = p.bounded ? 400 : 1e8;
        rows = p.bounded ? 1000 : 1e8;
    } else {
        p.access = kAccFull;
        p.orderConsumed = ord == kOrdNone || (!typeLevel && ord == kOrdPseqAsc);
        p.bounded = limitC >= 0 && ord == kOrdNone;
        cost = 1e9;
        rows = 1e9;
    }
    // Tag conditions (A2, ANY-row semantics): the vtab evaluates every one
    // itself, on the tag instance a posting names or, for other plans, on
    // any live instance of the record (RowFilter::hasLiveTag; at type level
    // on every live copy, §37). SQLite must not re-check them against the
    // projected columns, which show only the PUT's own tag: that dropped
    // every record matched through a RETAG.
    // <TYPE>_current groups first and filters after, as SQL says: its tag
    // conditions stay SQLite's.
    bool tagIn = false;
    if (!current) {
        const int tagC[5] = {tagEq[0], tagEq[1], tagEq[2], sourceNameEq, sourceEq};
        for (int k = 0; k < 5; k++) {
            if (tagC[k] < 0) continue;
            if (p.aTag[k] < 0) {
                use(tagC[k], &p.aTag[k]);
                if (isIn(tagC[k])) tagIn = true;
            }
            info->aConstraintUsage[tagC[k]].omit = 1;
        }
    }
    if (tagIn) {
        p.orderConsumed = false;  // one xFilter per IN value
        if (point) return SQLITE_CONSTRAINT;
    }
    if (alias && p.access != kAccSource) p.aSource = 0;  // the alias source post-filters (see xFilter)
    if (producerEq >= 0 && typeLevel) use(producerEq, &p.aProducer);
    if (limitC >= 0 && p.orderConsumed) {
        use(limitC, &p.aLimit);
        sqlite3_value* lv = nullptr;
        if (sqlite3_vtab_rhs_value(info, limitC, &lv) == SQLITE_OK && lv) {
            const double lim = double(sqlite3_value_int64(lv));
            if (lim > 0 && lim < rows) rows = lim;
            if (p.bounded && lim > 0) cost = std::min(cost, lim * 4);
        }
    } else if (limitC >= 0 && !p.orderConsumed) {
        // The LIMIT applies after SQLite's sort: the scan itself is not bounded.
        if (p.access == kAccFull || p.access == kAccEpoch || p.access == kAccGseq || p.access == kAccPseq ||
            p.access == kAccCidOrder) {
            const bool closed = p.aLo >= 0 && p.aHi >= 0;
            p.bounded = closed;
        }
    }
    // OFFSET pushdown (T2 #7): arrivals order skips live entries by fence
    // counts, CID order skips catalog entries without reading their rows.
    // Only where every row the scan finds is emitted (no post filters), so
    // the vtab can own the OFFSET (SQLite then skips none).
    if (offsetC >= 0 && typeLevel && !alias && !current && p.orderConsumed && nonGseq == 0 &&
        (p.access == kAccGseq || p.access == kAccFull || p.access == kAccCidOrder)) {
        use(offsetC, &p.aOffset);
        info->aConstraintUsage[offsetC].omit = 1;
    } else if (offsetC >= 0 && p.gseqTags && p.orderConsumed) {
        // Seen, not owned: a sorted collection keeps LIMIT + OFFSET rows.
        use(offsetC, &p.aOffsetSeen);
    }
    info->orderByConsumed = p.orderConsumed && info->nOrderBy > 0 ? 1 : 0;
    info->estimatedCost = cost;
    info->estimatedRows = sqlite3_int64(rows);
    if (p.access == kAccCid || p.access == kAccPseq) info->idxFlags = (rows <= 1 && !anyIn) ? SQLITE_INDEX_SCAN_UNIQUE : 0;
    info->idxNum = p.access;
    const std::string enc = p.encode();
    info->idxStr = sqlite3_mprintf("%s", enc.c_str());
    info->needToFreeIdxStr = 1;
    return SQLITE_OK;
}

// ---- cursor ---------------------------------------------------------------
struct RecCursor : sqlite3_vtab_cursor {
    RecVtab* vt = nullptr;
    StmtCtx* stmt = nullptr;
    Plan plan;
    std::unique_ptr<RowSource> src;
    CurRow cur;
    bool eof = true;
    bool frameOk = false;
    int32_t frameRc = 0;
    std::vector<uint8_t> frame;
    bool attrOk = false;
    std::vector<uint8_t> attr;
    bool gseqDone = false;
    int64_t gseqVal = -1;
    std::string aliasSource;
    int64_t pointArg = 0;        // kAccObjPoint: the target (epoch seconds)
    std::shared_ptr<StmtShared> shared;  // the plan's per-statement state (frames read ahead)
};

// _object from the stored frame (plans that do not read OBJECT_EPOCH): the
// type's extraction, the first present object column, a u64 key in decimal.
// false: no object key (or a sealed frame, whose plaintext is not stored).
bool objectFromFrame(const TypeInfo& t, const RecRow& r, const std::vector<uint8_t>& frame, std::string* out) {
    if (!t.cfg || (r.flags & kRowSealed) || frame.size() < 8) return false;
    Extracted ex;
    uint8_t scratch[2048];
    t.cfg->extract(frame.data(), frame.size(), &ex, scratch, sizeof(scratch));
    if (ex.objectCol < 0) return false;
    const ColValue& cv = ex.cols[ex.objectCol];
    if (!cv.present) return false;
    if (cv.isU64) {
        *out = std::to_string(cv.u);
    } else {
        uint8_t capped[kMaxKeyLen];
        const size_t n = capKey(capped, cv.s, cv.n);
        out->assign(reinterpret_cast<const char*>(capped), n);
    }
    return true;
}

int recOpen(sqlite3_vtab* v, sqlite3_vtab_cursor** out) {
    RecCursor* c = new RecCursor();
    c->vt = static_cast<RecVtab*>(v);
    *out = c;
    return SQLITE_OK;
}

int recClose(sqlite3_vtab_cursor* c) {
    delete static_cast<RecCursor*>(c);
    return SQLITE_OK;
}

int fail(RecCursor* c, int32_t rc, const char* what) {
    StmtCtx* s = c->stmt;
    if (s && !s->vtabStatus) {
        s->vtabStatus = rc;
        char buf[160];
        snprintf(buf, sizeof(buf), "%s: status %d", what, int(rc));
        s->vtabMessage = buf;
    }
    sqlite3_free(c->vt->zErrMsg);
    c->vt->zErrMsg = sqlite3_mprintf("flatsql_ps: %s (status %d)", what, int(rc));
    if (rc == kRsNoMem) return SQLITE_NOMEM;
    if (rc == kRsCancelled || rc == kRsStopped || rc == kRsTimeout) return SQLITE_INTERRUPT;
    return SQLITE_ERROR;
}

int advance(RecCursor* c) {
    c->frameOk = false;
    c->frameRc = 0;
    c->attrOk = false;
    c->gseqDone = false;
    const int32_t rc = c->src ? c->src->next(&c->cur) : 0;
    if (rc < 0) {
        c->eof = true;
        return fail(c, rc, "scan failed");
    }
    c->eof = rc == 0;
    if (!c->eof && c->stmt) c->stmt->rowEmitted = true;
    return SQLITE_OK;
}

// Range bound from an argv value and operator, in the index's key units.
bool argI64(sqlite3_value* v, int64_t* out) {
    const int t = sqlite3_value_numeric_type(v);
    if (t == SQLITE_INTEGER) {
        *out = sqlite3_value_int64(v);
        return true;
    }
    if (t == SQLITE_FLOAT) {
        const double d = sqlite3_value_double(v);
        if (d != d) return false;
        *out = d >= 9.2e18 ? INT64_MAX : d <= -9.2e18 ? INT64_MIN : int64_t(d);
        return true;
    }
    return false;
}

int32_t buildSources(RecCursor* c, sqlite3_value** argv, int argc);

int recFilter(sqlite3_vtab_cursor* cur, int idxNum, const char* idxStr, int argc, sqlite3_value** argv) {
    RecCursor* c = static_cast<RecCursor*>(cur);
    (void)idxNum;
    c->stmt = c->vt->lane->current();
    c->src.reset();
    c->eof = true;
    if (!c->stmt) return fail(c, kRsSqlError, "no statement context");
    if (!c->plan.decode(idxStr)) return fail(c, kRsSqlError, "bad plan");
    if (c->plan.access == kAccObjPoint && c->plan.aPoint > 0 && c->plan.aPoint <= argc)
        c->pointArg = sqlite3_value_int64(argv[c->plan.aPoint - 1]);
    int32_t rc = buildSources(c, argv, argc);
    if (rc == kRsSnapshotGone && !c->stmt->rowEmitted) {
        // §8.8: a file named by the snapshot vanished before any row was
        // emitted: re-snapshot once.
        c->vt->lane->dropSnapshots(c->stmt);
        c->src.reset();
        rc = buildSources(c, argv, argc);
    }
    if (rc < 0) return fail(c, rc, "filter failed");
    return advance(c);
}

int recNext(sqlite3_vtab_cursor* cur) { return advance(static_cast<RecCursor*>(cur)); }
int recEof(sqlite3_vtab_cursor* cur) { return static_cast<RecCursor*>(cur)->eof ? 1 : 0; }

int loadFrame(RecCursor* c) {
    if (c->frameOk) return c->frameRc;
    c->frameOk = true;
    c->frame.resize(c->cur.row.len);
    LaneStore& st = c->vt->lane->store();
    c->frameRc = c->shared ? c->shared->readFrame(&st, *c->cur.snap, c->cur.row, c->frame.data())
                           : st.readFrame(*c->cur.snap, c->cur.row, c->frame.data());
    return c->frameRc;
}

int loadAttr(RecCursor* c) {
    if (c->attrOk) return 0;
    c->attrOk = true;
    return c->vt->lane->store().readAttr(*c->cur.snap, c->cur.row, &c->attr);
}

void resultString(sqlite3_context* ctx, const flatbuffers::String* s) {
    if (!s) sqlite3_result_null(ctx);
    else sqlite3_result_text(ctx, s->c_str(), int(s->size()), SQLITE_TRANSIENT);
}

int schemaColumn(RecCursor* c, sqlite3_context* ctx, int i) {
    if (c->cur.row.flags & kRowSealed) {
        sqlite3_result_null(ctx);  // sealed: the plaintext is never stored (A19)
        return SQLITE_OK;
    }
    const int32_t rc = loadFrame(c);
    if (rc < 0) return fail(c, rc, "frame read failed");
    if (c->frame.size() < 8) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    const uint8_t* buf = c->frame.data() + frameRootOffset(c->frame.data(), c->frame.size(), c->vt->type->fid);
    const flatbuffers::Table* t = flatbuffers::GetRoot<flatbuffers::Table>(buf);
    const ColumnDef& col = c->vt->type->cols[size_t(i)];
    const uint16_t vo = col.voffset;
    switch (col.baseType) {
        case reflection::Bool:
        case reflection::UByte: sqlite3_result_int64(ctx, t->GetField<uint8_t>(vo, uint8_t(col.defInt))); break;
        case reflection::Byte: sqlite3_result_int64(ctx, t->GetField<int8_t>(vo, int8_t(col.defInt))); break;
        case reflection::Short: sqlite3_result_int64(ctx, t->GetField<int16_t>(vo, int16_t(col.defInt))); break;
        case reflection::UShort: sqlite3_result_int64(ctx, t->GetField<uint16_t>(vo, uint16_t(col.defInt))); break;
        case reflection::Int: sqlite3_result_int64(ctx, t->GetField<int32_t>(vo, int32_t(col.defInt))); break;
        case reflection::UInt: sqlite3_result_int64(ctx, t->GetField<uint32_t>(vo, uint32_t(col.defInt))); break;
        case reflection::Long: sqlite3_result_int64(ctx, t->GetField<int64_t>(vo, col.defInt)); break;
        case reflection::ULong:
            sqlite3_result_int64(ctx, sqlite3_int64(t->GetField<uint64_t>(vo, uint64_t(col.defInt))));
            break;
        case reflection::Float: sqlite3_result_double(ctx, double(t->GetField<float>(vo, float(col.defReal)))); break;
        case reflection::Double: sqlite3_result_double(ctx, t->GetField<double>(vo, col.defReal)); break;
        case reflection::String: resultString(ctx, t->GetPointer<const flatbuffers::String*>(vo)); break;
        case reflection::Vector:
            if (col.element == reflection::UByte || col.element == reflection::Byte) {
                const auto* vec = t->GetPointer<const flatbuffers::Vector<uint8_t>*>(vo);
                if (!vec) sqlite3_result_null(ctx);
                else sqlite3_result_blob(ctx, vec->data(), int(vec->size()), SQLITE_TRANSIENT);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        default: sqlite3_result_null(ctx); break;
    }
    return SQLITE_OK;
}

int recColumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int i) {
    RecCursor* c = static_cast<RecCursor*>(cur);
    RecVtab* vt = c->vt;
    if (c->eof) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    if (i < vt->nSchemaCols) return schemaColumn(c, ctx, i);
    const RecRow& r = c->cur.row;
    const bool typeLevel = vt->kind != kVkPartition;
    switch (i - vt->nSchemaCols) {
        case kMcPseq: sqlite3_result_int64(ctx, sqlite3_int64(r.pseq)); break;
        case kMcCid: {
            const std::string t = cidToText(r.cid);
            sqlite3_result_text(ctx, t.data(), int(t.size()), SQLITE_TRANSIENT);
            break;
        }
        case kMcCidBin: sqlite3_result_blob(ctx, r.cid, kCidLen, SQLITE_TRANSIENT); break;
        case kMcEpoch: sqlite3_result_int64(ctx, r.epochMs); break;
        case kMcArrival: sqlite3_result_int64(ctx, r.arrivalMs); break;
        case kMcGseq:
        case kMcRowid: {
            if (i - vt->nSchemaCols == kMcRowid && !typeLevel) {
                sqlite3_result_int64(ctx, sqlite3_int64(r.pseq));
                break;
            }
            if (c->cur.gseq) {
                sqlite3_result_int64(ctx, sqlite3_int64(c->cur.gseq));
                break;
            }
            if (!c->gseqDone) {
                // Partition level: the label's gseq (REPEAT copies carry the
                // FIRST's), when the row is labeled in a type snapshot.
                c->gseqDone = true;
                c->gseqVal = -1;
                TypeSnap* ts = nullptr;
                if (vt->lane->type(c->stmt, vt->type->fid, &ts) >= 0 && ts &&
                    r.pseq <= ts->labeledThrough(c->cur.pid)) {
                    uint8_t label;
                    uint64_t g;
                    bool found = false;
                    if (vt->lane->store().labelOf(*ts, c->cur.pid, r.pseq, &label, &g, &found) >= 0 && found)
                        c->gseqVal = int64_t(g);
                }
            }
            if (c->gseqVal < 0) sqlite3_result_null(ctx);
            else sqlite3_result_int64(ctx, c->gseqVal);
            break;
        }
        case kMcProducer: {
            const PartInfo* pi = c->stmt && c->stmt->reg ? c->stmt->reg->part(c->cur.pid) : nullptr;
            if (!pi) sqlite3_result_null(ctx);
            else sqlite3_result_text(ctx, pi->token.data(), int(pi->token.size()), SQLITE_TRANSIENT);
            break;
        }
        case kMcSource:
        case kMcSourceName:
        case kMcProvider:
        case kMcBatch: {
            const int m = i - vt->nSchemaCols;
            if (m == kMcSource && vt->kind == kVkAlias) {
                const std::string s = vt->typeName + "@" + vt->source;
                sqlite3_result_text(ctx, s.data(), int(s.size()), SQLITE_TRANSIENT);
                break;
            }
            const int32_t rc = loadAttr(c);
            if (rc < 0) return fail(c, rc, "attr read failed");
            AttrView av;
            const bool ok = parseAttr(c->attr.data(), c->attr.size(), &av);
            const TagView* tg = ok && av.tag.present ? &av.tag : nullptr;
            if (m == kMcSource) {
                std::string s = vt->typeName + "@";
                if (tg) s.append(reinterpret_cast<const char*>(tg->source), tg->sourceLen);
                else if (const PartInfo* pi = c->stmt->reg->part(c->cur.pid)) s += pi->token;
                sqlite3_result_text(ctx, s.data(), int(s.size()), SQLITE_TRANSIENT);
            } else if (!tg) {
                sqlite3_result_null(ctx);
            } else {
                const uint8_t* p = m == kMcSourceName ? tg->source : m == kMcProvider ? tg->provider : tg->batch;
                const size_t n = m == kMcSourceName ? tg->sourceLen : m == kMcProvider ? tg->providerLen : tg->batchLen;
                sqlite3_result_text(ctx, reinterpret_cast<const char*>(p ? p : reinterpret_cast<const uint8_t*>("")),
                                    int(n), SQLITE_TRANSIENT);
            }
            break;
        }
        case kMcPeerId:
        case kMcSignature: {
            const int32_t rc = loadAttr(c);
            if (rc < 0) return fail(c, rc, "attr read failed");
            if (c->attr.empty()) {
                sqlite3_result_null(ctx);
                break;
            }
            flatbuffers::Verifier ver(c->attr.data(), c->attr.size(), 16, 1024);
            if (!fb::VerifyRecordAttrBuffer(ver)) {
                sqlite3_result_null(ctx);
                break;
            }
            const fb::RecordAttr* ra = fb::GetRecordAttr(c->attr.data());
            const auto* vec = i - vt->nSchemaCols == kMcPeerId ? ra->peer_id() : ra->signature();
            if (!vec) sqlite3_result_null(ctx);
            else sqlite3_result_blob(ctx, vec->data(), int(vec->size()), SQLITE_TRANSIENT);
            break;
        }
        case kMcData: {
            const int32_t rc = loadFrame(c);
            if (rc < 0) return fail(c, rc, "frame read failed");
            if (c->frame.size() < 4) sqlite3_result_zeroblob(ctx, 0);
            else sqlite3_result_blob(ctx, c->frame.data() + 4, int(c->frame.size() - 4), SQLITE_TRANSIENT);
            break;
        }
        case kMcObject: {
            if (c->cur.hasObject) {
                if (c->cur.objectNull) sqlite3_result_null(ctx);
                else sqlite3_result_text(ctx, c->cur.object.data(), int(c->cur.object.size()), SQLITE_TRANSIENT);
                break;
            }
            const int32_t rc = loadFrame(c);
            if (rc < 0) return fail(c, rc, "frame read failed");
            std::string o;
            if (objectFromFrame(*vt->type, r, c->frame, &o)) sqlite3_result_text(ctx, o.data(), int(o.size()), SQLITE_TRANSIENT);
            else sqlite3_result_null(ctx);
            break;
        }
        case kMcAsof:
        case kMcForward:
        case kMcNearest: {
            // Inputs: the plan's target on the column the statement named.
            const int m = i - vt->nSchemaCols;
            const uint8_t want = m == kMcAsof ? kPointAsof : m == kMcForward ? kPointForward : kPointNearest;
            if (c->plan.access == kAccObjPoint && c->plan.pointKind == want) sqlite3_result_int64(ctx, c->pointArg);
            else sqlite3_result_null(ctx);
            break;
        }
        case kMcOffset: sqlite3_result_int64(ctx, r.off); break;
        case kMcLen: sqlite3_result_int64(ctx, r.len); break;
        case kMcKind: sqlite3_result_int64(ctx, r.kind); break;
        case kMcPid: sqlite3_result_int64(ctx, c->cur.pid); break;
        default: sqlite3_result_null(ctx); break;
    }
    return SQLITE_OK;
}

int recRowid(sqlite3_vtab_cursor* cur, sqlite3_int64* out) {
    RecCursor* c = static_cast<RecCursor*>(cur);
    if (c->vt->kind == kVkPartition) *out = sqlite3_int64(c->cur.row.pseq);
    else *out = sqlite3_int64(c->cur.gseq ? c->cur.gseq : c->cur.row.pseq);
    return SQLITE_OK;
}

int recCreate(sqlite3* db, void* aux, int argc, const char* const* argv, sqlite3_vtab** out, char** err) {
    return recConnect(db, aux, argc, argv, out, err);
}

sqlite3_module gRecModule = {
    3,              // iVersion
    recCreate,      recConnect, recBestIndex, recDisconnect, recDisconnect, recOpen, recClose, recFilter,
    recNext,        recEof,     recColumn,    recRowid,
    nullptr,  // xUpdate
    nullptr, nullptr, nullptr, nullptr,  // xBegin, xSync, xCommit, xRollback
    nullptr,  // xFindFunction
    nullptr,  // xRename
    nullptr, nullptr, nullptr,  // xSavepoint, xRelease, xRollbackTo
    nullptr,  // xShadowName
    nullptr,  // xIntegrity
};

// ---- building the row sources -----------------------------------------------------
std::string strArg(sqlite3_value* v) {
    const unsigned char* t = sqlite3_value_text(v);
    return t ? std::string(reinterpret_cast<const char*>(t), size_t(sqlite3_value_bytes(v))) : std::string();
}

bool cidArg(sqlite3_value* v, uint8_t cid[kCidLen]) {
    if (sqlite3_value_type(v) == SQLITE_BLOB) {
        if (sqlite3_value_bytes(v) != int(kCidLen)) return false;
        std::memcpy(cid, sqlite3_value_blob(v), kCidLen);
        return true;
    }
    const std::string s = strArg(v);
    return cidFromText(s.data(), s.size(), cid);
}

}  // namespace

// Implemented in vtab_fanout.cpp for type-level plans.
int32_t buildTypeSources(ReaderLane* lane, StmtCtx* stmt, RecVtab* vt, const Plan& p, sqlite3_value** argv,
                         std::unique_ptr<RowSource>* out, std::shared_ptr<StmtShared>* shOut);

namespace {

int32_t buildSources(RecCursor* c, sqlite3_value** argv, int argc) {
    RecVtab* vt = c->vt;
    ReaderLane* lane = vt->lane;
    const Plan& p = c->plan;
    (void)argc;
    c->shared.reset();
    if (vt->kind != kVkPartition) return buildTypeSources(lane, c->stmt, vt, p, argv, &c->src, &c->shared);
    PartSnap* snap = nullptr;
    int32_t rc = lane->part(c->stmt, vt->pid, &snap);
    if (rc < 0) return rc;
    RowFilter f;
    f.store = &lane->store();
    f.stmt = c->stmt;
    f.snap = snap;
    f.bound = snap->pseqHi();
    if (!buildTagMatch(vt, p, argv, &f.tags)) return 0;  // contradictory tag conditions: empty
    auto arg = [&](int8_t a) { return argv[a - 1]; };
    switch (p.access) {
        case kAccCid: {
            uint8_t cid[kCidLen];
            if (!cidArg(arg(p.aKey), cid)) return 0;  // no such CID: empty
            c->src = makeCidRowsPartition(f, cid);
            return 0;
        }
        case kAccPseq:
        case kAccFull: {
            uint64_t lo = 1, hi = f.bound;
            int64_t v;
            if (p.aLo >= 0 && argI64(arg(p.aLo), &v)) {
                if (p.loOp == SQLITE_INDEX_CONSTRAINT_GT) v = v == INT64_MAX ? v : v + 1;
                lo = v < 1 ? 1 : uint64_t(v);
            } else if (p.aLo >= 0) {
                return 0;
            }
            if (p.aHi >= 0 && argI64(arg(p.aHi), &v)) {
                if (p.hiOp == SQLITE_INDEX_CONSTRAINT_LT) v = v == INT64_MIN ? v : v - 1;
                if (v < 1) return 0;
                hi = std::min<uint64_t>(hi, uint64_t(v));
            } else if (p.aHi >= 0) {
                return 0;
            }
            // Pseq order: rows and frames read ahead (§37).
            f.shared = std::make_shared<StmtShared>();
            f.shared->lane = lane;
            f.readAhead = !p.desc;
            c->shared = f.shared;
            c->src = makePseqRows(f, lo, hi, p.desc);
            return 0;
        }
        default: break;
    }
    // Index-driven partition plans share the type-level builder's key logic:
    // a partition vtab is the fan-out restricted to one partition, without
    // labels (partition-level visibility).
    return buildTypeSources(lane, c->stmt, vt, p, argv, &c->src, &c->shared);
}

}  // namespace

// ---------------------------------------------------------------------------
// Registration and lazy creation
// ---------------------------------------------------------------------------
int registerMetaModule(sqlite3* db, ReaderLane* lane);  // vtab_meta.cpp
int metaEnsure(ReaderLane* lane, const std::string& name, std::string* err);

int vtabRegister(sqlite3* db, ReaderLane* lane) {
    int rc = sqlite3_create_module_v2(db, "flatsql_ps", &gRecModule, lane, nullptr);
    if (rc != SQLITE_OK) return rc;
    return registerMetaModule(db, lane);
}

namespace {
bool istarts(const std::string& s, const char* p) {
    const size_t n = std::strlen(p);
    if (s.size() < n) return false;
    for (size_t i = 0; i < n; i++)
        if (std::tolower(uint8_t(s[i])) != std::tolower(uint8_t(p[i]))) return false;
    return true;
}
bool iends(const std::string& s, const char* p) {
    const size_t n = std::strlen(p);
    if (s.size() < n) return false;
    for (size_t i = 0; i < n; i++)
        if (std::tolower(uint8_t(s[s.size() - n + i])) != std::tolower(uint8_t(p[i]))) return false;
    return true;
}
std::string quoteLit(const std::string& s) {
    std::string o = "'";
    for (char c : s) {
        if (c == '\'') o += "''";
        else o += c;
    }
    return o + "'";
}
}  // namespace

bool vtabPublicName(ReaderLane* lane, const std::string& name) {
    if (istarts(name, "flatsql_") || istarts(name, "sqlite_")) return false;
    const auto& allowed = lane->config().sandboxAllowed;
    if (!allowed.empty()) {
        for (const auto& a : allowed)
            if (a.size() == name.size() && istarts(name, a.c_str())) return true;
        return false;
    }
    auto reg = lane->store().registry();
    if (!reg) return false;
    const size_t at = name.find('@');
    if (at != std::string::npos) return reg->typeByName(name.substr(0, at)) != nullptr;
    if (iends(name, "_current")) return reg->typeByName(name.substr(0, name.size() - 8)) != nullptr;
    return reg->typeByName(name) != nullptr || reg->partBySqlName(name) != nullptr;
}

int vtabEnsure(ReaderLane* lane, const std::string& name, bool sandbox, std::string* err) {
    if (istarts(name, "flatsql_")) {
        if (sandbox) return 0;
        return metaEnsure(lane, name, err);
    }
    auto reg = lane->store().registry();
    if (!reg) return 0;
    if (sandbox && !vtabPublicName(lane, name)) return 0;
    std::string arg;
    if (const PartInfo* pi = reg->partBySqlName(name)) {
        if (pi->dropped) return 0;
        arg = "p:" + std::to_string(pi->pid);
    } else {
        const size_t at = name.find('@');
        if (at != std::string::npos) {
            const TypeInfo* t = reg->typeByName(name.substr(0, at));
            if (!t || at + 1 >= name.size()) return 0;
            arg = "a:" + fidHex(t->fid) + ":" + name.substr(at + 1);
        } else if (iends(name, "_current") && reg->typeByName(name.substr(0, name.size() - 8))) {
            arg = "c:" + fidHex(reg->typeByName(name.substr(0, name.size() - 8))->fid);
        } else if (const TypeInfo* t = reg->typeByName(name)) {
            arg = "t:" + fidHex(t->fid);
        } else {
            return 0;
        }
    }
    const std::string sql = "CREATE VIRTUAL TABLE temp." + quoteIdent(name) + " USING flatsql_ps(" + quoteLit(arg) + ")";
    char* msg = nullptr;
    const int rc = sqlite3_exec(lane->db(), sql.c_str(), nullptr, nullptr, &msg);
    if (rc != SQLITE_OK) {
        if (err) *err = msg ? msg : sqlite3_errstr(rc);
        sqlite3_free(msg);
        return rc == SQLITE_NOMEM ? kRsNoMem : kRsSqlError;
    }
    lane->noteTable(name);
    return 1;
}

}  // namespace ps
}  // namespace flatsql
