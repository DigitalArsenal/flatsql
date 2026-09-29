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
        if (!heapBuilt_) {
            heapBuilt_ = true;
            for (size_t i = 0; i < subs_.size(); i++)
                if (live_[i]) heap_.push_back(int(i));
            for (size_t i = heap_.size() / 2; i-- > 0;) siftDown(i);
        }
        for (;;) {
            if (heap_.empty()) return 0;
            const int best = heap_[0];
            // Swap, not move: the head keeps the caller's buffers (no
            // allocation per row).
            std::swap(*out, heads_[size_t(best)]);
            CurRow& r = *out;
            const int32_t rc = subs_[size_t(best)]->next(&heads_[size_t(best)]);
            if (rc < 0) return rc;
            live_[size_t(best)] = rc == 1;
            if (rc != 1) {
                heap_[0] = heap_.back();
                heap_.pop_back();
            }
            if (!heap_.empty()) siftDown(0);
            if (dedupe_ && haveLast_ && std::memcmp(r.row.cid, lastCid_, kCidLen) == 0 && r.key == lastKey_) continue;
            haveLast_ = true;
            std::memcpy(lastCid_, r.row.cid, kCidLen);
            if (dedupe_) lastKey_.assign(r.key);
            return 1;
        }
    }

private:
    bool before(int x, int y) const {
        const CurRow& a = heads_[size_t(x)];
        const CurRow& b = heads_[size_t(y)];
        int c = a.key.compare(b.key);
        if (c == 0) c = std::memcmp(a.row.cid, b.row.cid, kCidLen);
        if (c == 0) c = a.pid < b.pid ? -1 : a.pid > b.pid ? 1 : 0;
        if (c == 0) c = a.row.pseq < b.row.pseq ? -1 : a.row.pseq > b.row.pseq ? 1 : 0;
        if (c == 0) c = x < y ? -1 : 1;
        return desc_ ? c > 0 : c < 0;
    }
    void siftDown(size_t i) {
        const size_t n = heap_.size();
        for (;;) {
            size_t m = i;
            const size_t l = 2 * i + 1, r = 2 * i + 2;
            if (l < n && before(heap_[l], heap_[m])) m = l;
            if (r < n && before(heap_[r], heap_[m])) m = r;
            if (m == i) return;
            std::swap(heap_[i], heap_[m]);
            i = m;
        }
    }
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
    bool heapBuilt_ = false;
    std::vector<int> heap_;
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
    ArrivalRows(ReaderLane* lane, StmtCtx* stmt, TypeSnap* ts, uint64_t lo, uint64_t hi, bool desc, const TagMatch& tags,
                std::vector<uint32_t> allowed, uint64_t offset = 0)
        : lane_(lane), stmt_(stmt), ts_(ts), lo_(lo), hi_(hi), desc_(desc), tags_(tags), allowed_(std::move(allowed)),
          offset_(offset) {}

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
            // Offset paging: skip `offset` live entries by fence counts, not
            // by reading them (T2 #7).
            uint64_t start = desc_ ? end_ : begin_;
            if (offset_ && begin_ < end_) {
                rc = st.seekLive(*ts_, begin_, end_, offset_, desc_, &start);
                if (rc < 0) return rc;
            }
            pos_ = start;
        }
        for (;;) {
            if (bufAt_ >= buf_.size()) {
                // Refill: up to 256 entries per pread.
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
                    bufPos_ = pos_ + n - 1;  // position of buf_[0]
                } else {
                    if (pos_ >= end_) return 0;
                    const uint64_t n = std::min<uint64_t>(256, end_ - pos_);
                    buf_.resize(size_t(n));
                    const int32_t rc = st.arrivalsRead(*ts_, pos_, uint32_t(n), buf_.data());
                    if (rc < 0) return rc;
                    bufPos_ = pos_;
                    pos_ += n;
                }
            }
            const size_t i = bufAt_++;
            const ArrivalEntry e = buf_[i];
            if (st.stats()) st.stats()->indexEntries++;
            const int32_t prc = pollEvery(&st, &poll_);
            if (prc < 0) return prc;
            if (e.gseq < lo_ || e.gseq > hi_) continue;
            // GONE and REHOME are merge-joined with the arrivals by gseq (no
            // per-entry lookups, §8 "merge-joined").
            bool gone = false;
            int32_t jrc = joinGone(e.gseq, &gone);
            if (jrc < 0) return jrc;
            // Dead runs (A15/T2 #8): a GONE entry starts a gallop over fence
            // counts (O(log run) probes, no entry read) to the next position
            // that is not GONE. Short runs back off to per-entry checks.
            if (gone && !desc_ && skipAfter_ <= bufPos_ + i) {
                int32_t rc = 0;
                {
                    const uint64_t here = bufPos_ + i;
                    uint64_t run = 1;  // [here, here + run) all GONE
                    auto allGone = [&](uint64_t k, bool* yes) -> int32_t {
                        uint64_t live = 1;
                        const int32_t r2 = st.liveBetween(*ts_, here, here + k, &live);
                        *yes = live == 0;
                        return r2;
                    };
                    uint64_t hiK = 0;  // smallest known not-all-GONE length (0: none)
                    for (uint64_t k = 2; here + k <= end_; k *= 2) {
                        bool yes = false;
                        rc = allGone(k, &yes);
                        if (rc < 0) return rc;
                        if (!yes) {
                            hiK = k;
                            break;
                        }
                        run = k;
                    }
                    if (!hiK) hiK = end_ - here + 1;
                    while (hiK - run > 1) {
                        const uint64_t mid = run + (hiK - run) / 2;
                        bool yes = false;
                        if (here + mid > end_) {
                            hiK = mid;
                            continue;
                        }
                        rc = allGone(mid, &yes);
                        if (rc < 0) return rc;
                        if (yes) run = mid;
                        else hiK = mid;
                    }
                    if (run >= 4) {
                        skipped_ += run - 1;
                        buf_.clear();
                        bufAt_ = 0;
                        pos_ = here + run;
                        reseek_ = true;  // the joins restart at the new position
                    } else {
                        skipAfter_ = here + 32;  // short runs: check entries one by one for a while
                    }
                    continue;
                }
            }
            if (gone) continue;
            const int32_t rc = resolve(e, out);
            if (rc < 0) return rc;
            if (rc == 1) return 1;
        }
    }
    uint64_t skipped() const { return skipped_; }

    // Arrival entry -> its live FIRST copy (REHOME after promotion, A14).
    // Advances a gseq-ordered posting join to `g` (asc or desc) and reports
    // whether it holds g. Rebuilt after a skip.
    int32_t seekJoin(PostingScan* sc, bool* have, uint16_t kind, uint64_t g) {
        LaneStore& st = lane_->store();
        if (!*have || reseekPending(kind)) {
            uint8_t b[8];
            if (!desc_) {
                putBE64(b, g);
                *sc = st.scanType(*ts_, kind, b, 8, nullptr, 0, false);
            } else {
                putBE64(b, g == UINT64_MAX ? g : g + 1);
                *sc = st.scanType(*ts_, kind, nullptr, 0, b, 8, true);
            }
            if (sc->err()) return sc->err();
            *have = true;
        }
        while (sc->valid()) {
            const uint64_t k = getBE64(sc->key());
            if (desc_ ? k <= g : k >= g) break;
            sc->next();
        }
        return sc->err();
    }
    bool reseekPending(uint16_t kind) {
        if (!reseek_) return false;
        if (kind == kIxTypeGone) reseekGone_ = true;
        else reseekRehome_ = true;
        if (reseekGone_ && reseekRehome_) reseek_ = reseekGone_ = reseekRehome_ = false;
        return true;
    }
    int32_t joinGone(uint64_t g, bool* gone) {
        *gone = false;
        const int32_t rc = seekJoin(&goneScan_, &haveGone_, kIxTypeGone, g);
        if (rc < 0) return rc;
        *gone = goneScan_.valid() && getBE64(goneScan_.key()) == g;
        return 0;
    }
    int32_t joinRehome(uint64_t g) {
        cands_.clear();
        const int32_t rc = seekJoin(&rehomeScan_, &haveRehome_, kIxTypeRehome, g);
        if (rc < 0) return rc;
        // Every REHOME of g at its latest tcs (the scan is in (gseq, tcs)
        // value order within the key).
        uint64_t best = 0;
        PostingScan& sc = rehomeScan_;
        while (sc.valid() && getBE64(sc.key()) == g) {
            const uint8_t* v = sc.val();
            const uint64_t tcs = getBE64(v);
            if (cands_.empty() || tcs > best) {
                best = tcs;
                cands_.clear();
            }
            if (tcs == best) cands_.push_back({getBE32(v + 8), getBE64(v + 12)});
            sc.next();
        }
        return sc.err();
    }

    int32_t resolve(const ArrivalEntry& e, CurRow* out) {
        LaneStore& st = lane_->store();
        int32_t rc = joinRehome(e.gseq);
        if (rc < 0) return rc;
        // Not GONE: a live copy carries this gseq. Without a REHOME it is the
        // arrivals copy; one REHOME at the latest tcs is the promoted copy;
        // several (promote, kill, promote in one type commit) are told apart
        // by liveness.
        const bool knownLive = cands_.size() <= 1;
        if (cands_.empty()) cands_.push_back({e.pid, e.pseq});
        for (const auto& c : cands_) {
            if (!allowed_.empty() && !std::binary_search(allowed_.begin(), allowed_.end(), c.first)) continue;
            PartSnap* snap = nullptr;
            rc = lane_->partForType(stmt_, c.first, ts_->fid, &snap);
            if (rc < 0) return rc;
            RowFilter f;
            f.store = &st;
            f.stmt = stmt_;
            f.snap = snap;
            f.bound = std::min(snap->pseqHi(), ts_->labeledThrough(c.first));
            f.tags = tags_;
            f.knownLive = knownLive;
            rc = f.accept(c.second, out);
            if (rc < 0) return rc;
            if (rc == 0) continue;
            out->gseq = e.gseq;
            out->key.clear();
            return 1;
        }
        return 0;
    }

private:
    PostingScan goneScan_, rehomeScan_;
    bool haveGone_ = false, haveRehome_ = false;
    bool reseek_ = false, reseekGone_ = false, reseekRehome_ = false;
    ReaderLane* lane_;
    StmtCtx* stmt_;
    TypeSnap* ts_;
    uint64_t lo_, hi_;
    bool desc_;
    TagMatch tags_;
    std::vector<uint32_t> allowed_;  // sorted; empty = every partition
    uint64_t offset_ = 0;
    std::vector<std::pair<uint32_t, uint64_t>> cands_;
    bool started_ = false;
    uint64_t begin_ = 0, end_ = 0, pos_ = 0, bufPos_ = 0;
    uint64_t skipAfter_ = 0, skipped_ = 0;
    std::vector<ArrivalEntry> buf_;
    size_t bufAt_ = 0;
    uint32_t poll_ = 0;
};

// ---- a CID at type level: the catalog (the only cross-partition lookup) -----
class CidRowsType : public RowSource {
public:
    CidRowsType(ReaderLane* lane, StmtCtx* stmt, TypeSnap* ts, const uint8_t cid[kCidLen], const TagMatch& tags,
                uint64_t floor, std::vector<uint32_t> allowed)
        : lane_(lane), stmt_(stmt), ts_(ts), tags_(tags), floor_(floor), allowed_(std::move(allowed)) {
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
            f.tags = tags_;
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
    TagMatch tags_;
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
                                           bool desc, const TagMatch& tags, uint32_t onlyPid) {
    std::vector<uint32_t> allowed;
    if (onlyPid) allowed.push_back(onlyPid);
    return std::unique_ptr<RowSource>(new ArrivalRows(lane, stmt, type, lo, hi, desc, tags, allowed));
}

std::unique_ptr<RowSource> makeCidRowsType(ReaderLane* lane, StmtCtx* stmt, TypeSnap* type, const TypeInfo*,
                                           const uint8_t cid[kCidLen], const TagMatch& tags, uint64_t gseqFloor) {
    return std::unique_ptr<RowSource>(new CidRowsType(lane, stmt, type, cid, tags, gseqFloor, {}));
}

bool buildTagMatch(const RecVtab* vt, const Plan& p, sqlite3_value** argv, TagMatch* out) {
    *out = TagMatch();
    auto arg = [&](int8_t a) { return argv[a - 1]; };
    bool ok = true;
    if (vt->kind == kVkAlias) ok = out->setSource(vt->source) && ok;
    if (p.aTag[0] >= 0) out->setProvider(argText(arg(p.aTag[0])));
    if (p.aTag[1] >= 0) out->setBatch(argText(arg(p.aTag[1])));
    if (p.aTag[2] >= 0) {
        sqlite3_value* v = arg(p.aTag[2]);
        if (sqlite3_value_type(v) == SQLITE_BLOB) {
            const void* b = sqlite3_value_blob(v);
            out->setPeer(b ? std::string(static_cast<const char*>(b), size_t(sqlite3_value_bytes(v))) : std::string());
        } else {
            out->setPeer(argText(v));
        }
    }
    if (p.aTag[3] >= 0) ok = out->setSource(argText(arg(p.aTag[3]))) && ok;
    if (p.aTag[4] >= 0) {
        // '<TYPE>@<source name>', compared as the column projects it.
        const std::string full = argText(arg(p.aTag[4]));
        const std::string prefix = vt->typeName + "@";
        if (full.size() < prefix.size() || full.compare(0, prefix.size(), prefix) != 0) ok = false;
        else ok = out->setSource(full.substr(prefix.size())) && ok;
    }
    return ok;
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
    // Tag conditions (the alias source among them), ANY-row semantics.
    TagMatch tags;
    if (!buildTagMatch(vt, p, argv, &tags)) {
        out->reset(new Concat({}));
        return 0;
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
        // Instance scans (source, tag) check the conditions on each posting's
        // instance; other plans on any live instance of the record.
        f->tags = tags;
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
            out->reset(new CidRowsType(lane, stmt, ts, cid, tags, floor,
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
            uint64_t offset = 0;
            if (p.aOffset >= 0) {
                int64_t v;
                if (argInt(arg(p.aOffset), &v) && v > 0) offset = uint64_t(v);
            }
            out->reset(new ArrivalRows(lane, stmt, ts, eff, uhi, p.desc, tags, allowed, offset));
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
            const std::string cap = capKeyStr(tags.source);
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
                key = capKeyStr(p.tagKind == 0 ? tags.provider : p.tagKind == 1 ? tags.batch : tags.peer);
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
