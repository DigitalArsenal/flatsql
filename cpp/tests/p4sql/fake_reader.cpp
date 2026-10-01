// Test double of the format-4 reader: see fake_reader.h.
#ifdef FLATSQL_P4SQL_FAKE

#include "fake_reader.h"

#include <algorithm>
#include <cstring>
#include <ctime>
#include <memory>
#include <tuple>

using p4fake::Rec;
using p4fake::Tag;
using p4fake::Type;

void P4Engine::put(const std::string& type, Rec r) {
    auto& v = types[type].recs;
    auto it = std::lower_bound(v.begin(), v.end(), r.seq, [](const Rec& a, int64_t s) { return a.seq < s; });
    v.insert(it, std::move(r));
}

namespace {

bool tagLess(const Tag& a, const Tag& b) {
    return std::tie(a.at, a.provider, a.source, a.batch, a.contentKeyId, a.producerPeer, a.producerPubkey) <
           std::tie(b.at, b.provider, b.source, b.batch, b.contentKeyId, b.producerPeer, b.producerPubkey);
}

struct LaneF {
    bool any = false;
    std::string f[6];
    bool has[6] = {false, false, false, false, false, false};
};

bool tagMatches(const Tag& t, const LaneF& lf) {
    const std::string* v[6] = {&t.provider, &t.source, &t.batch, &t.contentKeyId, &t.producerPeer, &t.producerPubkey};
    for (int i = 0; i < 6; i++)
        if (lf.has[i] && *v[i] != lf.f[i]) return false;
    return true;
}

// The matched tag (§3.6): with a lane filter, the earliest satisfying tag;
// without one, the record's earliest tag. nullptr: none.
const Tag* matchedTag(const Rec& r, const LaneF& lf) {
    const Tag* best = nullptr;
    for (const Tag& t : r.tags) {
        if (lf.any && !tagMatches(t, lf)) continue;
        if (!best || tagLess(t, *best)) best = &t;
    }
    return best;
}

std::string epochDay(int64_t e) {
    time_t t = time_t(e);
    struct tm tmv;
    gmtime_r(&t, &tmv);
    char b[16];
    std::snprintf(b, sizeof(b), "%04d-%02d-%02d", tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
    return b;
}

struct PredC {
    uint8_t field, op;
    std::vector<P4Value> vals;
    std::vector<std::string> strs;
};

int cmpInt(int64_t a, int64_t b) { return a < b ? -1 : a > b ? 1 : 0; }

bool evalPred(const Rec& r, const PredC& p) {
    bool isText = false, present = true;
    int64_t iv = 0;
    std::string sv;
    switch (p.field) {
        case P4_F_EPOCH: present = r.hasEpoch; iv = r.epoch; break;
        case P4_F_TS: iv = r.ts; break;
        case P4_F_W: iv = r.hasEpoch ? r.epoch : r.ts; break;
        case P4_F_EPOCH_DAY: present = r.hasEpoch; isText = true; if (present) sv = epochDay(r.epoch); break;
        case P4_F_COL0: present = r.hasCol0; iv = r.col0; break;
        case P4_F_COL1: present = r.hasCol1; isText = true; sv = r.col1; break;
        default: return false;
    }
    if (!present) return false;
    if (p.op == P4_OP_NOTNULL) return true;
    auto cmp = [&](size_t k) -> int {
        if (isText) {
            const int c = sv.compare(p.strs[k]);
            return c < 0 ? -1 : c > 0 ? 1 : 0;
        }
        return cmpInt(iv, p.vals[k].i);
    };
    switch (p.op) {
        case P4_OP_EQ: return cmp(0) == 0;
        case P4_OP_NE: return cmp(0) != 0;
        case P4_OP_LT: return cmp(0) < 0;
        case P4_OP_LE: return cmp(0) <= 0;
        case P4_OP_GT: return cmp(0) > 0;
        case P4_OP_GE: return cmp(0) >= 0;
        case P4_OP_BETWEEN: return cmp(0) >= 0 && cmp(1) <= 0;
        case P4_OP_IN:
            for (size_t k = 0; k < p.vals.size(); k++)
                if (cmp(k) == 0) return true;
            return false;
        default: return false;
    }
}

int64_t visibleOf(const Type& t) {
    if (t.visibleThrough) return t.visibleThrough;
    return t.recs.empty() ? 0 : t.recs.back().seq;
}

}  // namespace

struct P4Cursor {
    P4Lane* lane = nullptr;
    const Type* t = nullptr;
    LaneF lf;
    std::string cid;            // text form of spec.cid, or empty
    bool hasPeer = false, hasProducer = false;
    std::string peer, producer;
    int64_t after = 0, through = 0;
    std::vector<PredC> preds;
    bool desc = false, hydrate = false;
    uint64_t limit = 0, offset = 0;
    std::string boundSource;    // A18 bound of lane.source
    // Iteration over t->recs indexes [lo, hi).
    size_t lo = 0, hi = 0, pos = 0;
    uint64_t skipped = 0, returned = 0;
    // The current row's storage.
    P4Row row{};
    P4Tag tag{};
};

namespace {
bool hasSourceTag(const Rec& r, const std::string& s) {
    for (const Tag& t : r.tags)
        if (t.source == s) return true;
    return false;
}

// 36-byte binary CID -> base32 text (CIDv1: 'b' + base32 lower of the bytes).
std::string cidText(const uint8_t* b) {
    static const char* a = "abcdefghijklmnopqrstuvwxyz234567";
    std::string o = "b";
    uint32_t buf = 0;
    int bits = 0;
    for (int i = 0; i < 36; i++) {
        buf = (buf << 8) | b[i];
        bits += 8;
        while (bits >= 5) {
            o += a[(buf >> (bits - 5)) & 31];
            bits -= 5;
        }
    }
    if (bits) o += a[(buf << (5 - bits)) & 31];
    return o;
}
}  // namespace

extern "C" {

int32_t p4_cursor_open(P4Lane* lane, const P4ScanSpec* spec, P4Cursor** out) {
    *out = nullptr;
    if (!lane || !spec || !spec->type) return P4_E_ARG;
    auto it = lane->engine->types.find(spec->type);
    if (it == lane->engine->types.end()) return P4_E_NOTYPE;
    if (spec->order != P4_ORDER_SEQ_ASC && spec->order != P4_ORDER_SEQ_DESC && spec->order != 0) return P4_E_UNSUPPORTED;
    if (spec->search) return P4_E_UNSUPPORTED;
    std::unique_ptr<P4Cursor> c(new P4Cursor());
    c->lane = lane;
    c->t = &it->second;
    const char* lv[6] = {spec->lane.provider, spec->lane.source, spec->lane.batch, spec->lane.contentKeyId,
                         spec->lane.producerPeer, spec->lane.producerPubkey};
    for (int i = 0; i < 6; i++)
        if (lv[i]) {
            c->lf.any = c->lf.has[i] = true;
            c->lf.f[i] = lv[i];
        }
    if (spec->cid) c->cid = cidText(spec->cid);
    if (spec->peer) c->hasPeer = true, c->peer = spec->peer;
    if (spec->producer) c->hasProducer = true, c->producer = spec->producer;
    const int64_t visible = visibleOf(*c->t);
    c->after = spec->seqAfter;
    c->through = spec->seqThrough ? std::min(spec->seqThrough, visible) : visible;
    for (uint32_t i = 0; i < spec->nPreds; i++) {
        PredC p;
        p.field = spec->preds[i].field;
        p.op = spec->preds[i].op;
        for (uint16_t k = 0; k < spec->preds[i].nvals; k++) {
            const P4Value& v = spec->preds[i].vals[k];
            p.vals.push_back(v);
            p.strs.emplace_back(v.s ? std::string(reinterpret_cast<const char*>(v.s), v.n) : std::string());
        }
        c->preds.push_back(std::move(p));
    }
    c->desc = spec->order == P4_ORDER_SEQ_DESC;
    c->hydrate = spec->hydrate != 0;
    c->limit = spec->limit;
    c->offset = spec->offset;
    // Visible records: [0, nVis).
    const auto& recs = c->t->recs;
    size_t nVis = size_t(std::upper_bound(recs.begin(), recs.end(), visible,
                                          [](int64_t s, const Rec& r) { return s < r.seq; }) -
                         recs.begin());
    c->lo = 0;
    c->hi = nVis;
    if (spec->bound) {
        if (spec->lane.source) {
            // §3.8.8 as the contract states it: the newest N of lane.source.
            c->boundSource = spec->lane.source;
            uint64_t n = 0;
            size_t i = nVis;
            while (i > 0 && n < spec->bound) {
                i--;
                if (hasSourceTag(recs[i], c->boundSource)) n++;
            }
            c->lo = i;
        } else if (nVis > spec->bound) {
            c->lo = nVis - size_t(spec->bound);
        }
    }
    // A seq range is a seek, not a scan (the bound is applied first).
    const size_t afterIdx = size_t(std::upper_bound(recs.begin(), recs.end(), c->after,
                                                    [](int64_t s, const Rec& r) { return s < r.seq; }) -
                                   recs.begin());
    const size_t throughIdx = size_t(std::upper_bound(recs.begin(), recs.end(), c->through,
                                                      [](int64_t s, const Rec& r) { return s < r.seq; }) -
                                     recs.begin());
    c->lo = std::max(c->lo, afterIdx);
    c->hi = std::min(c->hi, throughIdx);
    if (c->hi < c->lo) c->hi = c->lo;
    c->pos = c->desc ? c->hi : c->lo;
    p4fake::OpenedSpec os;
    os.type = spec->type;
    os.laneSource = spec->lane.source ? spec->lane.source : "";
    os.seqAfter = spec->seqAfter;
    os.seqThrough = spec->seqThrough;
    os.order = spec->order;
    os.hydrate = spec->hydrate;
    os.limit = spec->limit;
    os.bound = spec->bound;
    for (const PredC& p : c->preds) os.preds.push_back({p.field, p.op, p.vals.empty() ? 0 : p.vals[0].i, p.strs.empty() ? "" : p.strs[0]});
    lane->opened.push_back(os);
    *out = c.release();
    return P4_OK;
}

int32_t p4_cursor_next(P4Cursor* c, P4Row* row) {
    P4Lane* lane = c->lane;
    const auto& recs = c->t->recs;
    for (;;) {
        if (c->limit && c->returned >= c->limit) return 0;
        if (c->desc ? c->pos <= c->lo : c->pos >= c->hi) return 0;
        const Rec& r = c->desc ? recs[--c->pos] : recs[c->pos++];
        lane->rowsExamined++;
        if (lane->cancelAfterRows >= 0 && int64_t(lane->rowsExamined) >= lane->cancelAfterRows) lane->cancel = 1;
        if (lane->cancel.load()) return P4_E_CANCELLED;
        if (lane->maxRowsExamined && lane->rowsExamined > lane->maxRowsExamined) return P4_E_BUDGET;
        if (!c->boundSource.empty() && !hasSourceTag(r, c->boundSource)) continue;
        if (r.seq <= c->after || r.seq > c->through) continue;
        if (!c->cid.empty() && r.cid != c->cid) continue;
        if (c->hasPeer && r.peer != c->peer) continue;
        if (c->hasProducer && r.producer != c->producer) continue;
        const Tag* mt = matchedTag(r, c->lf);
        if (c->lf.any && !mt) continue;
        bool ok = true;
        for (const PredC& p : c->preds)
            if (!evalPred(r, p)) {
                ok = false;
                break;
            }
        if (!ok) continue;
        if (c->skipped < c->offset) {
            c->skipped++;
            continue;
        }
        c->returned++;
        std::memset(&c->row, 0, sizeof(c->row));
        c->row.seq = r.seq;
        c->row.ts = r.ts;
        c->row.epoch = r.epoch;
        c->row.hasEpoch = r.hasEpoch;
        c->row.cid = r.cid.c_str();
        c->row.producer = r.producer.c_str();
        c->row.peer = r.peer.c_str();
        c->row.sig = r.sig.empty() ? nullptr : r.sig.data();
        c->row.sigLen = uint32_t(r.sig.size());
        c->row.len = int64_t(r.data.size());
        if (c->hydrate) {
            c->row.data = r.data.data();
            c->row.dataLen = uint32_t(r.data.size());
            lane->bytesRead += r.data.size();
            if (lane->maxBytesRead && lane->bytesRead > lane->maxBytesRead) return P4_E_BUDGET;
        }
        if (mt) {
            c->tag.provider = mt->provider.c_str();
            c->tag.source = mt->source.c_str();
            c->tag.sourceUrl = mt->sourceUrl.c_str();
            c->tag.batch = mt->batch.c_str();
            c->tag.contentKeyId = mt->contentKeyId.c_str();
            c->tag.producerPeer = mt->producerPeer.c_str();
            c->tag.producerPubkey = mt->producerPubkey.c_str();
            c->tag.at = mt->at;
            c->row.tag = &c->tag;
        }
        *row = c->row;
        return 1;
    }
}

void p4_cursor_close(P4Cursor* c) { delete c; }

int32_t p4_lane_check(P4Lane* lane) {
    if (lane->cancel.load()) return P4_E_CANCELLED;
    if (lane->maxRowsExamined && lane->rowsExamined > lane->maxRowsExamined) return P4_E_BUDGET;
    return P4_OK;
}

int32_t p4_emit(P4Lane* lane, const uint8_t* bytes, uint32_t n) {
    if (lane->cancel.load()) return P4_E_CANCELLED;
    if (lane->maxResultBytes && lane->out.size() + n > lane->maxResultBytes) return P4_E_BUDGET;
    lane->out.insert(lane->out.end(), bytes, bytes + n);
    lane->emits++;
    return P4_OK;
}

P4Engine* p4_lane_engine(P4Lane* lane) { return lane->engine; }
void* p4_lane_sql_state(P4Lane* lane) { return lane->sqlState; }
void p4_lane_set_sql_state(P4Lane* lane, void* state) { lane->sqlState = state; }

int32_t p4_types(P4Lane* lane, const P4TypeInfo** out, uint32_t* n) {
    lane->typeInfos.clear();
    for (const auto& kv : lane->engine->types) {
        const Type& t = kv.second;
        P4TypeInfo ti;
        std::memset(&ti, 0, sizeof(ti));
        ti.name = t.name.c_str();
        ti.bfbs = t.bfbs.data();
        ti.bfbsLen = uint32_t(t.bfbs.size());
        std::memcpy(ti.fid, t.fid, 4);
        ti.a18Bound = t.bound;
        ti.epochProfile = t.epochProfile;
        lane->typeInfos.push_back(ti);
    }
    *out = lane->typeInfos.data();
    *n = uint32_t(lane->typeInfos.size());
    return P4_OK;
}

int32_t p4_sources(P4Lane* lane, const char* type, const char* const** out, uint32_t* n) {
    lane->srcStore.clear();
    lane->srcPtrs.clear();
    auto it = lane->engine->types.find(type ? type : "");
    if (it == lane->engine->types.end()) return P4_E_NOTYPE;
    const int64_t visible = visibleOf(it->second);
    std::vector<std::string> s;
    for (const Rec& r : it->second.recs)
        if (r.seq <= visible)
            for (const Tag& t : r.tags) s.push_back(t.source);
    std::sort(s.begin(), s.end());
    s.erase(std::unique(s.begin(), s.end()), s.end());
    lane->srcStore = s;
    for (const std::string& x : lane->srcStore) lane->srcPtrs.push_back(x.c_str());
    *out = lane->srcPtrs.data();
    *n = uint32_t(lane->srcPtrs.size());
    return P4_OK;
}

int64_t p4_visible_through(P4Engine* e, const char* type) {
    auto it = e->types.find(type ? type : "");
    return it == e->types.end() ? 0 : visibleOf(it->second);
}

}  // extern "C"

#endif  // FLATSQL_P4SQL_FAKE
