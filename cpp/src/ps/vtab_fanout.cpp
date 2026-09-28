// FlatSQL partition store: the type-level fan-out (design §9, A14, A15,
// A17, A18, A19): partition pruning, per-partition index-ordered
// sub-cursors, the k-way merge on (order key, CID) that drops duplicate CIDs,
// arrivals (gseq) order with REHOME, the cid catalog, and <TYPE>_current.
//
// Type-level rows are FIRST copies only: a row is visible when its pseq is
// at most V_p = min(pseq_hi, labeled_through[p]) and its latest LABEL is
// FIRST (a promoted REPEAT carries a FIRST label, A14), and it is live when
// no DEAD posting with a killer at most V_p names it.
#include <strings.h>

#include <algorithm>
#include <cstring>
#include <unordered_set>

#include "internal.h"
#include "vtab_internal.h"

namespace flatsql {
namespace ps {

namespace {

// ---- k-way merge ------------------------------------------------------------
class Merger : public RowSource {
public:
    Merger(std::vector<std::unique_ptr<RowSource>> subs, bool desc, bool dedupeCid, bool group, size_t groupTrim)
        : subs_(std::move(subs)), desc_(desc), dedupe_(dedupeCid), group_(group), groupTrim_(groupTrim) {}

    int32_t next(CurRow* out) override {
        if (!started_) {
            started_ = true;
            heads_.resize(subs_.size());
            live_.assign(subs_.size(), false);
            for (size_t i = 0; i < subs_.size(); i++) {
                const int32_t rc = subs_[i]->next(&heads_[i]);
                if (rc < 0) return rc;
                live_[i] = rc == 1;
            }
        }
        if (group_) return nextGroupBest(out);
        for (;;) {
            int best = pick();
            if (best < 0) return 0;
            CurRow r = std::move(heads_[size_t(best)]);
            const int32_t rc = subs_[size_t(best)]->next(&heads_[size_t(best)]);
            if (rc < 0) return rc;
            live_[size_t(best)] = rc == 1;
            if (dedupe_ && haveLast_ && std::memcmp(r.row.cid, lastCid_, kCidLen) == 0 && r.key == lastKey_) continue;
            haveLast_ = true;
            std::memcpy(lastCid_, r.row.cid, kCidLen);
            lastKey_ = r.key;
            *out = std::move(r);
            return 1;
        }
    }

private:
    int pick() const {
        int best = -1;
        for (size_t i = 0; i < subs_.size(); i++) {
            if (!live_[i]) continue;
            if (best < 0) {
                best = int(i);
                continue;
            }
            const CurRow& a = heads_[i];
            const CurRow& b = heads_[size_t(best)];
            int c = a.key.compare(b.key);
            if (c == 0) c = std::memcmp(a.row.cid, b.row.cid, kCidLen);
            if (c == 0) c = a.pid < b.pid ? -1 : a.pid > b.pid ? 1 : 0;
            if (c == 0) c = a.row.pseq < b.row.pseq ? -1 : a.row.pseq > b.row.pseq ? 1 : 0;
            if (desc_ ? c > 0 : c < 0) best = int(i);
        }
        return best;
    }
    std::string groupOf(const CurRow& r) const {
        return r.key.size() > groupTrim_ ? r.key.substr(0, r.key.size() - groupTrim_) : r.key;
    }
    static bool better(const CurRow& a, const CurRow& b) {
        if (a.row.epochMs != b.row.epochMs) return a.row.epochMs > b.row.epochMs;
        return a.gseq > b.gseq;
    }
    // <TYPE>_current: one row per group (the key minus its trailing bytes),
    // the one with the latest epoch.
    int32_t nextGroupBest(CurRow* out) {
        int best = pick();
        if (best < 0) return 0;
        CurRow win = std::move(heads_[size_t(best)]);
        const std::string g = groupOf(win);
        for (;;) {
            const int32_t rc = subs_[size_t(best)]->next(&heads_[size_t(best)]);
            if (rc < 0) return rc;
            live_[size_t(best)] = rc == 1;
            best = pick();
            if (best < 0 || groupOf(heads_[size_t(best)]) != g) break;
            if (better(heads_[size_t(best)], win)) win = heads_[size_t(best)];
        }
        *out = std::move(win);
        return 1;
    }

    std::vector<std::unique_ptr<RowSource>> subs_;
    std::vector<CurRow> heads_;
    std::vector<bool> live_;
    bool desc_, dedupe_, group_;
    size_t groupTrim_;
    bool started_ = false;
    bool haveLast_ = false;
    uint8_t lastCid_[kCidLen];
    std::string lastKey_;
};

class Concat : public RowSource {
public:
    explicit Concat(std::vector<std::unique_ptr<RowSource>> subs) : subs_(std::move(subs)) {}
    int32_t next(CurRow* out) override {
        while (at_ < subs_.size()) {
            const int32_t rc = subs_[at_]->next(out);
            if (rc != 0) return rc;
            subs_[at_].reset();  // release the finished partition's scan
            at_++;
        }
        return 0;
    }

private:
    std::vector<std::unique_ptr<RowSource>> subs_;
    size_t at_ = 0;
};

// ---- arrivals (gseq order) ----------------------------------------------------
class ArrivalRows : public RowSource {
public:
    ArrivalRows(ReaderLane* lane, StmtCtx* stmt, TypeSnap* ts, uint64_t lo, uint64_t hi, bool desc, const std::string& src,
                bool hasSrc, std::vector<uint32_t> allowed)
        : lane_(lane), stmt_(stmt), ts_(ts), lo_(lo), hi_(hi), desc_(desc), src_(src), hasSrc_(hasSrc),
          allowed_(std::move(allowed)) {}

    int32_t next(CurRow* out) override {
        LaneStore& st = lane_->store();
        if (!started_) {
            started_ = true;
            if (lo_ > hi_ || ts_->empty) return 0;
            uint64_t a, b;
            int32_t rc = st.arrivalsUpperBound(*ts_, lo_ ? lo_ - 1 : 0, &a);
            if (rc < 0) return rc;
            rc = st.arrivalsUpperBound(*ts_, hi_, &b);
            if (rc < 0) return rc;
            begin_ = a;
            end_ = b;
            pos_ = desc_ ? end_ : begin_;
        }
        for (;;) {
            if (bufAt_ >= buf_.size()) {
                // Refill: 256 entries per pread.
                buf_.clear();
                bufAt_ = 0;
                if (desc_) {
                    if (pos_ <= begin_) return 0;
                    const uint64_t n = std::min<uint64_t>(256, pos_ - begin_);
                    buf_.resize(size_t(n));
                    const int32_t rc = st.arrivalsRead(*ts_, pos_ - n, uint32_t(n), buf_.data());
                    if (rc < 0) return rc;
                    std::reverse(buf_.begin(), buf_.end());
                    pos_ -= n;
                } else {
                    if (pos_ >= end_) return 0;
                    const uint64_t n = std::min<uint64_t>(256, end_ - pos_);
                    buf_.resize(size_t(n));
                    const int32_t rc = st.arrivalsRead(*ts_, pos_, uint32_t(n), buf_.data());
                    if (rc < 0) return rc;
                    pos_ += n;
                }
                if (st.stats()) st.stats()->indexEntries += buf_.size();
            }
            const ArrivalEntry e = buf_[bufAt_++];
            const int32_t prc = pollEvery(&st, &poll_);
            if (prc < 0) return prc;
            if (e.gseq < lo_ || e.gseq > hi_) continue;
            const int32_t rc = resolve(e, out);
            if (rc < 0) return rc;
            if (rc == 1) return 1;
        }
    }

    // Arrival entry -> its live FIRST copy (REHOME after promotion, A14).
    int32_t resolve(const ArrivalEntry& e, CurRow* out) {
        LaneStore& st = lane_->store();
        uint32_t pid = e.pid;
        uint64_t pseq = e.pseq;
        uint32_t rpid;
        uint64_t rpseq;
        bool found = false;
        int32_t rc = st.rehomeOf(*ts_, e.gseq, &rpid, &rpseq, &found);
        if (rc < 0) return rc;
        if (found) {
            pid = rpid;
            pseq = rpseq;
        }
        if (!allowed_.empty() && !std::binary_search(allowed_.begin(), allowed_.end(), pid)) return 0;
        PartSnap* snap = nullptr;
        rc = lane_->partForType(stmt_, pid, ts_->fid, &snap);
        if (rc < 0) return rc;
        RowFilter f;
        f.store = &st;
        f.stmt = stmt_;
        f.snap = snap;
        f.bound = std::min(snap->pseqHi(), ts_->labeledThrough(pid));
        f.source = src_;
        f.hasSource = hasSrc_;
        rc = f.accept(pseq, out);
        if (rc != 1) return rc;
        out->gseq = e.gseq;
        out->key.clear();
        return 1;
    }

private:
    ReaderLane* lane_;
    StmtCtx* stmt_;
    TypeSnap* ts_;
    uint64_t lo_, hi_;
    bool desc_;
    std::string src_;
    bool hasSrc_;
    std::vector<uint32_t> allowed_;  // sorted; empty = every partition
    bool started_ = false;
    uint64_t begin_ = 0, end_ = 0, pos_ = 0;
    std::vector<ArrivalEntry> buf_;
    size_t bufAt_ = 0;
    uint32_t poll_ = 0;
};

// ---- a CID at type level: the catalog (the only cross-partition lookup) -----
class CidRowsType : public RowSource {
public:
    CidRowsType(ReaderLane* lane, StmtCtx* stmt, TypeSnap* ts, const uint8_t cid[kCidLen], const std::string& src,
                bool hasSrc, uint64_t floor, std::vector<uint32_t> allowed)
        : lane_(lane), stmt_(stmt), ts_(ts), src_(src), hasSrc_(hasSrc), floor_(floor), allowed_(std::move(allowed)) {
        std::memcpy(cid_, cid, kCidLen);
    }
    int32_t next(CurRow* out) override {
        if (done_) return 0;
        done_ = true;
        LaneStore& st = lane_->store();
        std::vector<CatalogCopy> copies;
        int32_t rc = st.catalog(*ts_, cid_, &copies);
        if (rc < 0) return rc;
        std::sort(copies.begin(), copies.end(), [](const CatalogCopy& a, const CatalogCopy& b) {
            return a.tcs != b.tcs ? a.tcs < b.tcs : (a.pid != b.pid ? a.pid < b.pid : a.pseq < b.pseq);
        });
        for (const CatalogCopy& c : copies) {
            if (c.label != kLblFirst && c.label != kLblPromoted) continue;
            if (c.gseq < floor_) continue;
            if (!allowed_.empty() && !std::binary_search(allowed_.begin(), allowed_.end(), c.pid)) continue;
            PartSnap* snap = nullptr;
            rc = lane_->partForType(stmt_, c.pid, ts_->fid, &snap);
            if (rc < 0) return rc;
            RowFilter f;
            f.store = &st;
            f.stmt = stmt_;
            f.snap = snap;
            f.bound = std::min(snap->pseqHi(), ts_->labeledThrough(c.pid));
            f.source = src_;
            f.hasSource = hasSrc_;
            rc = f.accept(c.pseq, out);
            if (rc < 0) return rc;
            if (rc == 0) continue;
            out->gseq = c.gseq;
            out->key.clear();
            return 1;
        }
        return 0;
    }

private:
    ReaderLane* lane_;
    StmtCtx* stmt_;
    TypeSnap* ts_;
    uint8_t cid_[kCidLen];
    std::string src_;
    bool hasSrc_;
    uint64_t floor_;
    std::vector<uint32_t> allowed_;
    bool done_ = false;
};

// A source filter on arrivals-ordered or catalog rows reuses RowFilter.
// ---------------------------------------------------------------------------
bool argInt(sqlite3_value* v, int64_t* out) {
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

std::string argText(sqlite3_value* v) {
    const unsigned char* t = sqlite3_value_text(v);
    return t ? std::string(reinterpret_cast<const char*>(t), size_t(sqlite3_value_bytes(v))) : std::string();
}

bool argCid(sqlite3_value* v, uint8_t cid[kCidLen]) {
    if (sqlite3_value_type(v) == SQLITE_BLOB) {
        if (sqlite3_value_bytes(v) != int(kCidLen)) return false;
        std::memcpy(cid, sqlite3_value_blob(v), kCidLen);
        return true;
    }
    const std::string s = argText(v);
    return cidFromText(s.data(), s.size(), cid);
}

// Inclusive [lo, hi] from a plan's range arguments (integers).
bool rangeOf(const Plan& p, sqlite3_value** argv, int64_t* lo, int64_t* hi) {
    *lo = INT64_MIN;
    *hi = INT64_MAX;
    int64_t v;
    if (p.aLo >= 0) {
        if (!argInt(argv[p.aLo - 1], &v)) return false;
        if (p.loOp == SQLITE_INDEX_CONSTRAINT_GT) {
            if (v == INT64_MAX) return false;
            v++;
        }
        *lo = v;
    }
    if (p.aHi >= 0) {
        if (!argInt(argv[p.aHi - 1], &v)) return false;
        if (p.hiOp == SQLITE_INDEX_CONSTRAINT_LT) {
            if (v == INT64_MIN) return false;
            v--;
        }
        *hi = v;
    }
    return *lo <= *hi;
}

std::string encI64s(int64_t v) {
    uint8_t b[8];
    encI64(b, v);
    return std::string(reinterpret_cast<const char*>(b), 8);
}

std::string capKeyStr(const std::string& s) {
    uint8_t buf[kMaxKeyLen];
    const size_t n = capKey(buf, reinterpret_cast<const uint8_t*>(s.data()), s.size());
    return std::string(reinterpret_cast<const char*>(buf), n);
}

// escaped(s) || 00 01 || encI64(v)
std::string strI64Key(const std::string& capped, int64_t v) {
    std::vector<uint8_t> buf(2 * capped.size() + 10);
    const size_t n = encStrI64(buf.data(), reinterpret_cast<const uint8_t*>(capped.data()), capped.size(), v);
    return std::string(reinterpret_cast<const char*>(buf.data()), n);
}

// Exclusive upper bound of every key with prefix escaped(s) || 00 01.
std::string strPrefixEnd(const std::string& capped) {
    std::string k = strI64Key(capped, 0);
    k.resize(k.size() - 8);
    k.back() = 0x02;
    return k;
}

}  // namespace

std::unique_ptr<RowSource> makeMerger(std::vector<std::unique_ptr<RowSource>> subs, bool desc, bool dedupeCid,
                                      size_t groupPrefixTrim) {
    return std::unique_ptr<RowSource>(
        new Merger(std::move(subs), desc, dedupeCid, groupPrefixTrim != 0, groupPrefixTrim));
}

std::unique_ptr<RowSource> makeConcat(std::vector<std::unique_ptr<RowSource>> subs) {
    return std::unique_ptr<RowSource>(new Concat(std::move(subs)));
}

std::unique_ptr<RowSource> makeArrivalRows(ReaderLane* lane, StmtCtx* stmt, TypeSnap* type, uint64_t lo, uint64_t hi,
                                           bool desc, const std::string& source, bool hasSource, uint32_t onlyPid) {
    std::vector<uint32_t> allowed;
    if (onlyPid) allowed.push_back(onlyPid);
    return std::unique_ptr<RowSource>(new ArrivalRows(lane, stmt, type, lo, hi, desc, source, hasSource, allowed));
}

std::unique_ptr<RowSource> makeCidRowsType(ReaderLane* lane, StmtCtx* stmt, TypeSnap* type, const TypeInfo*,
                                           const uint8_t cid[kCidLen], const std::string& source, bool hasSource,
                                           uint64_t gseqFloor) {
    return std::unique_ptr<RowSource>(new CidRowsType(lane, stmt, type, cid, source, hasSource, gseqFloor, {}));
}

int32_t windowFloor(ReaderLane* lane, StmtCtx*, TypeSnap* type, uint64_t n, uint64_t* floor) {
    *floor = 0;
    const uint64_t total = type->arrivalsTotal();
    if (n == 0 || total <= n) return 0;
    ArrivalEntry e;
    const int32_t rc = lane->store().arrivalAt(*type, total - n, &e);
    if (rc < 0) return rc;
    *floor = e.gseq;
    return 0;
}

// Builds the row source of a record vtab plan (type level, and the
// index-driven partition-level plans).
int32_t buildTypeSources(ReaderLane* lane, StmtCtx* stmt, RecVtab* vt, const Plan& p, sqlite3_value** argv,
                         std::unique_ptr<RowSource>* out) {
    LaneStore& st = lane->store();
    const bool partLevel = vt->kind == kVkPartition;
    const uint8_t* fid = vt->type->fid;
    auto arg = [&](int8_t a) { return argv[a - 1]; };
    TypeSnap* ts = nullptr;
    int32_t rc;
    if (!partLevel) {
        rc = lane->type(stmt, fid, &ts);
        if (rc < 0) return rc;
    }
    // Source restriction (alias, or a source constraint).
    std::string source;
    bool hasSource = false;
    if (vt->kind == kVkAlias) {
        source = vt->source;
        hasSource = true;
    } else if (p.access == kAccSource) {
        source = argText(arg(p.aKey));
        hasSource = true;
        if (p.sourceFull) {
            const std::string prefix = vt->typeName + "@";
            if (source.size() < prefix.size() || strncasecmp(source.c_str(), prefix.c_str(), prefix.size()) != 0) {
                out->reset(new Concat({}));
                return 0;
            }
            source = source.substr(prefix.size());
        }
    }
    // Partition set (pruned by _producer).
    std::vector<uint32_t> pids;
    if (partLevel) {
        pids.push_back(vt->pid);
    } else {
        const TypeInfo* ti = stmt->reg ? stmt->reg->typeByFid(fid) : nullptr;
        std::string producer;
        const bool byProducer = p.aProducer >= 0;
        if (byProducer) producer = argText(arg(p.aProducer));
        if (ti)
            for (uint32_t pid : ti->pids) {
                const PartInfo* pi = stmt->reg->part(pid);
                if (!pi || pi->dropped) continue;
                if (byProducer && pi->token != producer) continue;
                pids.push_back(pid);
            }
        std::sort(pids.begin(), pids.end());
    }
    // Sandbox window (A18): the newest N FIRST records by gseq.
    uint64_t floor = 0;
    if (!partLevel && stmt->sandbox()) {
        rc = windowFloor(lane, stmt, ts, lane->hotWindow(vt->typeName), &floor);
        if (rc < 0) return rc;
    }
    // Per-partition filter.
    auto filterFor = [&](uint32_t pid, RowFilter* f) -> int32_t {
        PartSnap* snap = nullptr;
        const int32_t r2 = partLevel ? lane->part(stmt, pid, &snap) : lane->partForType(stmt, pid, fid, &snap);
        if (r2 < 0) return r2;
        f->store = &st;
        f->stmt = stmt;
        f->snap = snap;
        f->type = ts;
        f->bound = partLevel ? snap->pseqHi() : std::min(snap->pseqHi(), ts->labeledThrough(pid));
        f->gseqFloor = floor;
        // The alias restricts by a post-filter unless the scan is by source.
        if (hasSource && p.access != kAccSource) {
            f->source = source;
            f->hasSource = true;
        }
        return 0;
    };
    std::vector<std::unique_ptr<RowSource>> subs;
    switch (p.access) {
        case kAccCid: {
            uint8_t cid[kCidLen];
            if (!argCid(arg(p.aKey), cid)) {
                out->reset(new Concat({}));
                return 0;
            }
            if (partLevel) {
                RowFilter f;
                rc = filterFor(vt->pid, &f);
                if (rc < 0) return rc;
                *out = makeCidRowsPartition(f, cid);
                return 0;
            }
            out->reset(new CidRowsType(lane, stmt, ts, cid, source, hasSource, floor,
                                       p.aProducer >= 0 ? pids : std::vector<uint32_t>()));
            return 0;
        }
        case kAccGseq:
        case kAccFull: {
            if (partLevel) return kRsSqlError;  // partition plans never choose these here
            int64_t lo, hi;
            if (!rangeOf(p, argv, &lo, &hi)) {
                out->reset(new Concat({}));
                return 0;
            }
            const uint64_t ulo = lo < 1 ? 1 : uint64_t(lo);
            uint64_t uhi = hi < 0 ? 0 : uint64_t(hi);
            if (uhi > ts->gseqHi()) uhi = ts->gseqHi();
            const uint64_t eff = std::max<uint64_t>(ulo, floor);
            std::vector<uint32_t> allowed = p.aProducer >= 0 ? pids : std::vector<uint32_t>();
            if (p.aProducer >= 0 && allowed.empty()) {
                out->reset(new Concat({}));
                return 0;
            }
            out->reset(new ArrivalRows(lane, stmt, ts, eff, uhi, p.desc, source, hasSource, allowed));
            return 0;
        }
        case kAccEpoch: {
            int64_t lo, hi;
            if (!rangeOf(p, argv, &lo, &hi)) {
                out->reset(new Concat({}));
                return 0;
            }
            std::string klo, khi;
            bool hasLo = false, hasHi = false;
            uint16_t kind;
            if (p.tagKind == 1) {
                kind = kIxEpoch;
                if (lo != INT64_MIN) {
                    klo = encI64s(lo);
                    hasLo = true;
                }
                if (hi != INT64_MAX) {
                    khi = encI64s(hi + 1);
                    hasHi = true;
                }
            } else {
                kind = kIxEpochCid;
                // Seconds descending: [hi second, lo second] in key order.
                uint8_t b[8];
                if (hi != INT64_MAX) {
                    encEpochSecDesc(b, epochSecFloor(hi));
                    klo.assign(reinterpret_cast<const char*>(b), 8);
                    hasLo = true;
                }
                if (lo != INT64_MIN) {
                    const int64_t s = epochSecFloor(lo);
                    if (s > INT64_MIN) {
                        encEpochSecDesc(b, s - 1);
                        khi.assign(reinterpret_cast<const char*>(b), 8);
                        hasHi = true;
                    }
                }
            }
            for (uint32_t pid : pids) {
                RowFilter f;
                rc = filterFor(pid, &f);
                if (rc < 0) return rc;
                if (f.snap->empty) continue;
                // Zone map pruning (manifest min/max epoch) is implicit: the
                // range scan of an out-of-range partition reads one block.
                // EPOCH_CID keys are second-granular: the exact millisecond
                // bounds are applied by SQLite (the constraint is not omitted).
                subs.push_back(makePostingRows(f, kind, klo, hasLo, khi, hasHi, p.desc, false));
            }
            *out = makeMerger(std::move(subs), p.desc, !partLevel, 0);
            return 0;
        }
        case kAccSource: {
            int64_t lo, hi;
            if (!rangeOf(p, argv, &lo, &hi)) {
                out->reset(new Concat({}));
                return 0;
            }
            const std::string cap = capKeyStr(source);
            const std::string klo = strI64Key(cap, lo);
            const std::string khi = hi == INT64_MAX ? strPrefixEnd(cap) : strI64Key(cap, hi + 1);
            for (uint32_t pid : pids) {
                RowFilter f;
                rc = filterFor(pid, &f);
                if (rc < 0) return rc;
                if (f.snap->empty) continue;
                subs.push_back(makePostingRows(f, kIxSourceEpoch, klo, true, khi, true, p.desc, true));
            }
            *out = makeMerger(std::move(subs), p.desc, !partLevel, 0);
            return 0;
        }
        case kAccCol:
        case kAccTag: {
            std::string key;
            uint16_t kind;
            if (p.access == kAccCol) {
                kind = uint16_t(kIxColBase + p.col);
                // u64pos columns index an 8-byte big-endian value; others the
                // (capped) string bytes.
                bool u64 = false;
                for (size_t i = 0; i < vt->colIndex.size(); i++)
                    if (vt->colIndex[i] == int(p.col) && vt->colIsU64[i]) u64 = true;
                if (u64) {
                    int64_t v;
                    if (!argInt(arg(p.aKey), &v) || v <= 0) {
                        out->reset(new Concat({}));
                        return 0;
                    }
                    uint8_t b[8];
                    putBE64(b, uint64_t(v));
                    key.assign(reinterpret_cast<const char*>(b), 8);
                } else {
                    key = capKeyStr(argText(arg(p.aKey)));
                }
            } else {
                kind = p.tagKind == 0 ? kIxTagProvider : p.tagKind == 1 ? kIxTagBatch : kIxTagPeer;
                key = capKeyStr(argText(arg(p.aKey)));
            }
            std::string hi = key;
            hi.push_back('\0');  // exclusive upper bound: the next key after `key`
            for (uint32_t pid : pids) {
                RowFilter f;
                rc = filterFor(pid, &f);
                if (rc < 0) return rc;
                if (f.snap->empty) continue;
                subs.push_back(makePostingRows(f, kind, key, true, hi, true, false, p.access == kAccTag));
            }
            *out = makeConcat(std::move(subs));
            return 0;
        }
        case kAccCurrent: {
            if (partLevel) return kRsSqlError;
            // Supersede types: one group per stored supersede key (a key has
            // one live version per lane; the latest across partitions wins).
            // Other types: one group per object key (OBJECT_EPOCH minus its
            // epoch), the latest epoch wins.
            const bool sup = vt->hasSupersede;
            const uint16_t kind = sup ? kIxSupersede : kIxObjectEpoch;
            for (uint32_t pid : pids) {
                RowFilter f;
                rc = filterFor(pid, &f);
                if (rc < 0) return rc;
                if (f.snap->empty) continue;
                subs.push_back(makePostingRows(f, kind, std::string(), false, std::string(), false, false, false));
            }
            out->reset(new Merger(std::move(subs), false, false, true, sup ? 0 : 8));
            return 0;
        }
        default: break;
    }
    return kRsSqlError;
}

}  // namespace ps
}  // namespace flatsql
