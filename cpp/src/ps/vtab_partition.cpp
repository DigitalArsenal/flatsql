// FlatSQL partition store: the record virtual tables (design §9, A17, A18,
// A19): the module, plans (xBestIndex), partition-level row sources, and
// column projection. The type-level merge lives in vtab_fanout.cpp.
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/reflection.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>

#include "flatsql/ps/flatsql_attr_generated.h"
#include "flatsql/ps/platform.h"
#include "internal.h"
#include "vtab_internal.h"

namespace flatsql {
namespace ps {

const char* const kMetaColNames[kMcCount] = {
    "_pseq",     "_cid",       "_cid_bin", "_epoch",  "_arrival", "_gseq",   "_producer",
    "_source",   "_source_name", "_provider", "_batch", "_peer_id", "_signature", "_data",
    "_offset",   "_len",       "_kind",    "_rowid",  "_pid"};

static const char* kMetaColTypes[kMcCount] = {"INTEGER", "TEXT",    "BLOB",    "INTEGER", "INTEGER",
                                               "INTEGER", "TEXT",    "TEXT",    "TEXT",    "TEXT",
                                               "TEXT",    "BLOB",    "BLOB",    "BLOB",    "INTEGER",
                                               "INTEGER", "INTEGER", "INTEGER", "INTEGER"};

// ---------------------------------------------------------------------------
// Plans
// ---------------------------------------------------------------------------
std::string Plan::encode() const {
    char buf[160];
    snprintf(buf, sizeof(buf), "%c:%u:%u:%u:%u:%d:%d:%d:%u:%u:%d:%d:%u:%d:%d:%u", bounded ? 'B' : 'U',
             unsigned(access), unsigned(desc), unsigned(tagKind), unsigned(col), int(aKey), int(aLo), int(aHi),
             unsigned(loOp), unsigned(hiOp), int(aProducer), int(aSource), unsigned(sourceFull), int(aLimit),
             int(aOffset), unsigned(orderConsumed));
    return buf;
}

bool Plan::decode(const char* s) {
    if (!s || (s[0] != 'B' && s[0] != 'U')) return false;
    unsigned acc, d, tk, c, lop, hop, sf, oc;
    int k, lo, hi, pr, so, li, of;
    if (sscanf(s + 1, ":%u:%u:%u:%u:%d:%d:%d:%u:%u:%d:%d:%u:%d:%d:%u", &acc, &d, &tk, &c, &k, &lo, &hi, &lop, &hop,
               &pr, &so, &sf, &li, &of, &oc) != 15)
        return false;
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
int32_t RowFilter::hasLiveSource(uint64_t put, const std::string& src, bool* yes) {
    *yes = false;
    uint8_t lo[8], hi[8];
    putBE64(lo, put);
    putBE64(hi, put + 1);
    std::vector<uint64_t> insts;
    PostingScan ps = store->scan(*snap, kIxTagOf, lo, 8, hi, 8, false);
    if (ps.err()) return ps.err();
    for (; ps.valid(); ps.next()) insts.push_back(getBE64(ps.val()));
    if (ps.err()) return ps.err();
    std::vector<uint8_t> attr;
    for (uint64_t inst : insts) {
        if (inst > bound) continue;
        bool dead = false;
        int32_t rc = store->isTagDead(*snap, inst, bound, &dead);
        if (rc < 0) return rc;
        if (dead) continue;
        RecRow ir;
        rc = store->readRow(*snap, inst, &ir);
        if (rc < 0) return rc;
        rc = store->readAttr(*snap, ir, &attr);
        if (rc < 0) return rc;
        AttrView av;
        if (!parseAttr(attr.data(), attr.size(), &av) || !av.tag.present) continue;
        if (av.tag.sourceLen == src.size() && std::memcmp(av.tag.source, src.data(), src.size()) == 0) {
            *yes = true;
            return 0;
        }
    }
    return 0;
}

int32_t RowFilter::accept(uint64_t pseq, CurRow* out) {
    if (pseq == 0 || pseq > bound) return 0;
    out->gseq = 0;
    if (type) {
        // Every labeled PUT is FIRST or REPEAT, and a REPEAT label always
        // leaves a REPEAT posting (bloom-tested): without one the row is
        // FIRST, and its gseq is looked up only if a column or the sandbox
        // window needs it. With one, the latest LABEL says whether it was
        // promoted since (A14).
        bool repeat = false;
        int32_t rc = store->everRepeat(*type, snap->pid, pseq, &repeat);
        if (rc < 0) return rc;
        if (repeat || gseqFloor) {
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
        rc = store->isDead(*snap, pseq, bound, &dead);
        if (rc < 0) return rc;
        if (dead) return 0;
    }
    rc = store->readRow(*snap, pseq, &out->row);
    if (rc < 0) return rc;
    if (out->row.kind != kRowPut) return 0;
    if (hasSource) {
        bool yes = false;
        rc = hasLiveSource(pseq, source, &yes);
        if (rc < 0) return rc;
        if (!yes) return 0;
    }
    out->pid = snap->pid;
    out->snap = snap;
    return 1;
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
          instances_(instances) {}

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
            }
            if (instances_) {
                // The value is a tag instance (the PUT itself or a RETAG).
                if (pseq == 0 || pseq > f_.bound) continue;
                bool dead = false;
                int32_t rc = f_.store->isTagDead(*f_.snap, pseq, f_.bound, &dead);
                if (rc < 0) return rc;
                if (dead) continue;
                RecRow ir;
                rc = f_.store->readRow(*f_.snap, pseq, &ir);
                if (rc < 0) return rc;
                if (ir.kind == kRowRetag) pseq = ir.targetPseq;
                else if (ir.kind != kRowPut) continue;
            }
            if (std::find(emitted_.begin(), emitted_.end(), pseq) != emitted_.end()) continue;
            const int32_t rc = f_.accept(pseq, out);
            if (rc < 0) return rc;
            if (rc == 0) continue;
            emitted_.push_back(pseq);
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
    bool started_ = false;
    PostingScan scan_;
    std::string lastKey_;
    std::vector<uint64_t> emitted_;
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
    size_t at = 0;
    while (at < rules.size()) {
        size_t nl = rules.find('\n', at);
        if (nl == std::string::npos) nl = rules.size();
        std::string line = rules.substr(at, nl - at);
        at = nl + 1;
        const size_t hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
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
        if (m < 0) {
            if (eq && c.iColumn >= 0 && c.iColumn < int(vt->colIndex.size()) && vt->colIndex[c.iColumn] >= 0 &&
                !vt->colIsEnum[c.iColumn] && colEq < 0) {
                colEq = i;
                colIdx = vt->colIndex[c.iColumn];
            }
            continue;
        }
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
    enum { kOrdNone, kOrdEpochDesc, kOrdEpochAsc, kOrdGseqAsc, kOrdGseqDesc, kOrdPseqAsc, kOrdPseqDesc, kOrdOther } ord =
        kOrdNone;
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
    double cost = 1e12;
    double rows = 1e9;
    bool anyIn = false;
    if (current) {
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
    } else if (colEq >= 0) {
        p.access = kAccCol;
        p.col = uint16_t(colIdx);
        use(colEq, &p.aKey);
        anyIn = isIn(colEq);
        p.bounded = true;
        cost = 50;
        rows = 10;
    } else if (!alias && (tagEq[0] >= 0 || tagEq[1] >= 0 || tagEq[2] >= 0) && sourceEq < 0 && sourceNameEq < 0) {
        p.access = kAccTag;
        const int k = tagEq[1] >= 0 ? 1 : tagEq[0] >= 0 ? 0 : 2;
        p.tagKind = uint8_t(k);
        use(tagEq[k], &p.aKey);
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
    if (p.access != kAccSource && p.access != kAccCid && !alias && (sourceEq >= 0 || sourceNameEq >= 0)) {
        // post-filter handled by SQLite (not claimed)
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
        if (p.access == kAccFull || p.access == kAccEpoch || p.access == kAccGseq || p.access == kAccPseq) {
            const bool closed = p.aLo >= 0 && p.aHi >= 0;
            p.bounded = closed;
        }
    }
    // OFFSET pushdown (T2 #7): arrivals order skips live entries by fence
    // counts. Only where every row the scan finds is emitted (no post
    // filters), so the vtab can own the OFFSET (SQLite then skips none).
    if (offsetC >= 0 && typeLevel && !alias && !current && p.orderConsumed &&
        (p.access == kAccGseq || p.access == kAccFull) && producerEq < 0 && sourceEq < 0 && sourceNameEq < 0 &&
        cidEq < 0 && colEq < 0) {
        use(offsetC, &p.aOffset);
        info->aConstraintUsage[offsetC].omit = 1;
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
};

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
    c->frameRc = c->vt->lane->store().readFrame(*c->cur.snap, c->cur.row, c->frame.data());
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
    const uint8_t* buf = c->frame.data() + 4;
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
                         std::unique_ptr<RowSource>* out);

namespace {

int32_t buildSources(RecCursor* c, sqlite3_value** argv, int argc) {
    RecVtab* vt = c->vt;
    ReaderLane* lane = vt->lane;
    const Plan& p = c->plan;
    (void)argc;
    if (vt->kind != kVkPartition) return buildTypeSources(lane, c->stmt, vt, p, argv, &c->src);
    PartSnap* snap = nullptr;
    int32_t rc = lane->part(c->stmt, vt->pid, &snap);
    if (rc < 0) return rc;
    RowFilter f;
    f.store = &lane->store();
    f.stmt = c->stmt;
    f.snap = snap;
    f.bound = snap->pseqHi();
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
            c->src = makePseqRows(f, lo, hi, p.desc);
            return 0;
        }
        default: break;
    }
    // Index-driven partition plans share the type-level builder's key logic:
    // a partition vtab is the fan-out restricted to one partition, without
    // labels (partition-level visibility).
    return buildTypeSources(lane, c->stmt, vt, p, argv, &c->src);
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
