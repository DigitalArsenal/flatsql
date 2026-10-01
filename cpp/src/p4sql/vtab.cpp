// FlatSQL store format 4, SQL surface: the record relations (CONTRACT.md
// §3.9 "Relations"; design §6). Copy-adapted from format 2's
// ps/vtab_partition.cpp (module, plans, columns) and ps/vtab_fanout.cpp
// (merging sources) onto the p4 reader (flatsql/p4/p4_reader.h).
//
// The A18 window is format 1's: the type's newest N seqs (N = the type's
// bound), applied by the reader before every other filter, the lane filter
// included (C-17).
//   "<TYPE>@<source>"  one cursor: the window narrowed to records with a live
//                      tag of the source;
//   <TYPE>             one row per (record, live source) of the window: a
//                      cursor per source, the sources one after another
//                      (format 1's UNION ALL view over its per-source tables),
//                      or merged by seq when the statement orders by it.
//
// Pushed down (always re-checked by SQLite, so a pushdown only has to be a
// superset of the rows SQLite keeps): _seq/_rowid ranges, _epoch and _ts
// ranges, COL0/COL1 (the columns the type's `col 0`/`col 1` rules extract),
// and _source equality.
#include <algorithm>
#include <cmath>
#include <cstring>
#include <deque>
#include <memory>

#include "internal.h"

namespace flatsql {
namespace p4sql {

std::string lower(const std::string& s) {
    std::string o = s;
    for (char& c : o)
        if (c >= 'A' && c <= 'Z') c = char(c - 'A' + 'a');
    return o;
}

namespace {

std::string rulesOf(const P4TypeInfo& t) { return t.rules ? std::string(t.rules, t.rulesLen) : std::string(); }

// COL0/COL1 from the type's rules: one alternative on a top-level field
// ("col 0 u64pos:NORAD_CAT_ID", "col 1 str:OBJECT_ID") makes that column's
// constraints a reader predicate (format 2's mapColumns).
void mapCols(TypeEntry* t, const std::string& rules) {
    size_t at = 0;
    while (at < rules.size()) {
        size_t nl = rules.find('\n', at);
        if (nl == std::string::npos) nl = rules.size();
        std::string line = rules.substr(at, nl - at);
        at = nl + 1;
        const size_t hash = line.find('#');
        if (hash != std::string::npos) line.resize(hash);
        char word[16], alts[400];
        unsigned n;
        if (std::sscanf(line.c_str(), "%15s %u %399s", word, &n, alts) != 3 || std::strcmp(word, "col") != 0) continue;
        const std::string a = alts;
        if (a.find('|') != std::string::npos) continue;
        const size_t colon = a.find(':');
        if (colon == std::string::npos) continue;
        const std::string kind = a.substr(0, colon);
        const std::string path = a.substr(colon + 1);
        if (path.find('.') != std::string::npos || path.find('[') != std::string::npos) continue;
        for (size_t i = 0; i < t->cols.size(); i++) {
            const Column& c = t->cols[i];
            if (c.name != path || c.placeholder) continue;
            const bool isInt = c.kind >= kColI8 && c.kind <= kColU64;
            if (n == 0 && kind == "u64pos" && isInt) t->col0 = int(i);
            if (n == 1 && kind == "str" && c.kind == kColText) t->col1 = int(i);
        }
    }
}

bool allZero(const uint8_t fid[4]) { return !fid[0] && !fid[1] && !fid[2] && !fid[3]; }

}  // namespace

int32_t loadTypes(LaneState* ls, std::string* err) {
    const P4TypeInfo* ts = nullptr;
    uint32_t n = 0;
    int32_t rc;
    {
        EngineCall ec;
        rc = p4_types(ls->lane, &ts, &n);
    }
    if (rc < 0) {
        if (err) *err = "registered types unavailable";
        return rc;
    }
    for (uint32_t i = 0; i < n; i++) {
        const P4TypeInfo& ti = ts[i];
        if (!ti.name || allZero(ti.fid)) continue;   // format 1 routes only types with a file identifier
        const std::string key = lower(ti.name);
        if (ls->types.count(key)) continue;
        TypeEntry t;
        t.name = ti.name;
        std::memcpy(t.fid, ti.fid, 4);
        t.bound = ti.a18Bound;
        std::string e;
        if (!format1Columns(t.name, ti.bfbs, ti.bfbsLen, &t.cols, &e)) continue;   // not queryable
        mapCols(&t, rulesOf(ti));
        ls->types.emplace(key, std::move(t));
    }
    return P4_OK;
}

const TypeEntry* typeByName(LaneState* ls, const std::string& name) {
    auto it = ls->types.find(lower(name));
    return it == ls->types.end() ? nullptr : &it->second;
}

int32_t sourcesOf(LaneState* ls, const std::string& type, std::vector<std::string>* out) {
    out->clear();
    const char* const* srcs = nullptr;
    uint32_t n = 0;
    EngineCall ec;
    const int32_t rc = p4_sources(ls->lane, type.c_str(), &srcs, &n);
    if (rc < 0) return rc;
    for (uint32_t i = 0; i < n; i++)
        if (srcs[i]) out->emplace_back(srcs[i]);
    std::sort(out->begin(), out->end());
    out->erase(std::unique(out->begin(), out->end()), out->end());
    return P4_OK;
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------
namespace {

// Hidden columns after the schema columns and format 1's four meta columns.
enum Meta : int { kSource = 0, kRowid, kOffset, kData, kSeq, kCid, kTs, kEpoch, kProducer, kPeer, kMetaCount };
const char* const kMetaDecl[kMetaCount] = {
    "\"_source\" TEXT",          "\"_rowid\" INTEGER",       "\"_offset\" INTEGER",    "\"_data\" BLOB",
    "\"_seq\" INTEGER HIDDEN",   "\"_cid\" TEXT HIDDEN",     "\"_ts\" INTEGER HIDDEN", "\"_epoch\" INTEGER HIDDEN",
    "\"_producer\" TEXT HIDDEN", "\"_peer\" TEXT HIDDEN",
};

struct RelVtab : sqlite3_vtab {
    LaneState* ls = nullptr;
    RelKind kind = kRelType;
    const TypeEntry* t = nullptr;
    std::string source;   // alias
    int ns = 0;           // schema columns
    int meta(Meta m) const { return ns + int(m); }
};

std::string quoteIdent(const std::string& s) {
    std::string o = "\"";
    for (char c : s) {
        if (c == '"') o += "\"\"";
        else o += c;
    }
    return o + "\"";
}

int relConnect(sqlite3* db, void* aux, int argc, const char* const* argv, sqlite3_vtab** out, char** err) {
    LaneState* ls = static_cast<LaneState*>(aux);
    const std::string name = argc > 2 && argv[2] ? argv[2] : "";
    auto it = ls->relations.find(lower(name));
    const TypeEntry* t = it == ls->relations.end() ? nullptr : typeByName(ls, it->second.type);
    if (!t) {
        *err = sqlite3_mprintf("flatsql_p4: %s is not a record relation", name.c_str());
        return SQLITE_ERROR;
    }
    std::unique_ptr<RelVtab> vt(new RelVtab());
    vt->ls = ls;
    vt->kind = it->second.kind;
    vt->t = t;
    vt->source = it->second.source;
    vt->ns = int(t->cols.size());
    std::string ddl = "CREATE TABLE x(";
    for (const Column& c : t->cols) {
        ddl += quoteIdent(c.name);
        ddl += " ";
        ddl += c.sqlType();
        ddl += ", ";
    }
    for (int i = 0; i < kMetaCount; i++) {
        if (i) ddl += ", ";
        ddl += kMetaDecl[i];
    }
    ddl += ")";
    const int rc = sqlite3_declare_vtab(db, ddl.c_str());
    if (rc != SQLITE_OK) {
        *err = sqlite3_mprintf("flatsql_p4: declare failed: %s", sqlite3_errmsg(db));
        return rc;
    }
    *out = vt.release();
    return SQLITE_OK;
}

int relDisconnect(sqlite3_vtab* v) {
    delete static_cast<RelVtab*>(v);
    return SQLITE_OK;
}

// ---- plans ----------------------------------------------------------------
// idxStr: two characters per argv entry, in argv order: the target
// (S seq, E _epoch, T _ts, A COL0, B COL1, R _source) and the operator
// (= EQ, > GT, ) GE, < LT, ( LE). idxNum: kPlan* flags.
enum : int { kPlanOrdered = 1, kPlanDesc = 2, kPlanHydrate = 4 };

char opChar(int op) {
    switch (op) {
        case SQLITE_INDEX_CONSTRAINT_EQ: return '=';
        case SQLITE_INDEX_CONSTRAINT_GT: return '>';
        case SQLITE_INDEX_CONSTRAINT_GE: return ')';
        case SQLITE_INDEX_CONSTRAINT_LT: return '<';
        case SQLITE_INDEX_CONSTRAINT_LE: return '(';
        default: return 0;
    }
}

bool colUsed(sqlite3_uint64 used, int i) { return i >= 63 ? (used >> 63) & 1 : (used >> i) & 1; }

int relBestIndex(sqlite3_vtab* v, sqlite3_index_info* info) {
    RelVtab* vt = static_cast<RelVtab*>(v);
    std::string plan;
    int argc = 0;
    double rows = vt->t->bound ? double(vt->t->bound) : 1e6;
    for (int i = 0; i < info->nConstraint; i++) {
        const auto& c = info->aConstraint[i];
        if (!c.usable) continue;
        const char op = opChar(c.op);
        if (!op) continue;
        char target = 0;
        const int col = c.iColumn;
        if (col == -1 || col == vt->meta(kRowid) || col == vt->meta(kSeq)) target = 'S';
        else if (col == vt->meta(kEpoch)) target = 'E';
        else if (col == vt->meta(kTs)) target = 'T';
        else if (col >= 0 && col == vt->t->col0) target = 'A';
        else if (col >= 0 && col == vt->t->col1 && op == '=') target = 'B';
        else if (col == vt->meta(kSource) && op == '=') target = 'R';
        if (!target) continue;
        info->aConstraintUsage[i].argvIndex = ++argc;
        info->aConstraintUsage[i].omit = 0;
        plan += target;
        plan += op;
        if (op == '=') rows = target == 'S' ? 1 : target == 'R' ? rows : rows / 100;
        else rows /= 4;
    }
    int flags = 0;
    if (info->nOrderBy == 1) {
        const int col = info->aOrderBy[0].iColumn;
        if (col == -1 || col == vt->meta(kRowid) || col == vt->meta(kSeq)) {
            info->orderByConsumed = 1;
            flags |= kPlanOrdered;
            if (info->aOrderBy[0].desc) flags |= kPlanDesc;
        }
    }
    bool hydrate = colUsed(info->colUsed, vt->meta(kData));
    for (int i = 0; i < vt->ns && !hydrate; i++) hydrate = colUsed(info->colUsed, i);
    if (hydrate) flags |= kPlanHydrate;
    if (rows < 1) rows = 1;
    info->idxNum = flags;
    info->idxStr = sqlite3_mprintf("%s", plan.c_str());
    info->needToFreeIdxStr = 1;
    info->estimatedRows = sqlite3_int64(rows);
    info->estimatedCost = rows * (hydrate ? 4.0 : 1.0);
    return SQLITE_OK;
}

// ---- cursors ----------------------------------------------------------------
// One source's cursor.
struct Sub {
    P4Cursor* c = nullptr;
    P4Row row{};
    bool has = false;
    bool done = false;
    std::string source;
};

struct RelCursor : sqlite3_vtab_cursor {
    RelVtab* vt = nullptr;
    // The specs' storage: deques keep every element in place while the
    // specs point into them, for the cursors' lifetime.
    std::deque<P4Value> vals;
    std::vector<P4Pred> preds;
    std::deque<std::string> strs;
    std::vector<P4ScanSpec> specs;   // one per sub
    std::vector<Sub> subs;
    bool merge = false;              // merge by seq (ordered plans), else one after another
    bool desc = false;
    size_t at = 0;                   // concatenation: the current sub; merge: the sub holding the row
    bool eof = true;
    std::string sourceText;          // "<TYPE>@<source>" of the current row
    ~RelCursor() { closeAll(); }
    void closeAll() {
        EngineCall ec;
        for (Sub& s : subs)
            if (s.c) p4_cursor_close(s.c);
        subs.clear();
    }
};

Stmt* stmtOf(RelVtab* vt) { return vt->ls->cur; }

int fail(RelCursor* c, int32_t status, const std::string& what) {
    if (Stmt* s = stmtOf(c->vt)) s->raise(status, what);
    sqlite3_free(c->vt->zErrMsg);
    c->vt->zErrMsg = sqlite3_mprintf("%s", what.c_str());
    return SQLITE_ERROR;
}

// Advances one sub to its next row. 0 or a negative status.
int32_t step(Sub& s) {
    s.has = false;
    if (s.done) return 0;
    int32_t rc;
    {
        EngineCall ec;
        rc = p4_cursor_next(s.c, &s.row);
        if (rc == 0) {
            p4_cursor_close(s.c);
            s.c = nullptr;
            s.done = true;
        }
    }
    if (rc < 0) return rc;
    s.has = rc > 0;
    return 0;
}

int32_t open(RelCursor* c, size_t i) {
    Sub& s = c->subs[i];
    int32_t rc;
    {
        EngineCall ec;
        rc = p4_cursor_open(c->vt->ls->lane, &c->specs[i], &s.c);
    }
    if (rc < 0) {
        s.c = nullptr;
        s.done = true;
        return rc;
    }
    return step(s);
}

void setSource(RelCursor* c, const Sub& s) { c->sourceText = c->vt->t->name + "@" + s.source; }

// Positions on the next output row (after `at` was consumed).
int32_t settle(RelCursor* c) {
    if (c->merge) {
        size_t best = SIZE_MAX;
        for (size_t i = 0; i < c->subs.size(); i++) {
            const Sub& s = c->subs[i];
            if (!s.has) continue;
            if (best == SIZE_MAX) {
                best = i;
                continue;
            }
            const int64_t a = s.row.seq, b = c->subs[best].row.seq;
            if (c->desc ? a > b : a < b) best = i;   // ties: source order
        }
        c->eof = best == SIZE_MAX;
        c->at = best;
    } else {
        while (c->at < c->subs.size() && !c->subs[c->at].has) {
            if (++c->at >= c->subs.size()) break;
            const int32_t rc = open(c, c->at);
            if (rc < 0) return rc;
        }
        c->eof = c->at >= c->subs.size();
    }
    if (!c->eof) setSource(c, c->subs[c->at]);
    return 0;
}

// ---- xFilter ------------------------------------------------------------------
struct Bounds {
    int64_t after = 0;               // exclusive
    int64_t through = INT64_MAX;     // inclusive
    bool empty = false;
};

// An integer constraint value; reals are rounded toward the side that keeps
// every integer SQLite would keep. false: no integer bound (SQLite filters).
bool intBound(sqlite3_value* v, char op, int64_t* out, bool* empty) {
    const int t = sqlite3_value_numeric_type(v);
    if (t == SQLITE_INTEGER) {
        *out = sqlite3_value_int64(v);
        return true;
    }
    if (t != SQLITE_FLOAT) return false;
    const double d = sqlite3_value_double(v);
    if (std::isnan(d) || d > 9.2e18 || d < -9.2e18) return false;
    const double f = std::floor(d), ce = std::ceil(d);
    switch (op) {
        case '=':
            if (f != d) *empty = true;
            *out = int64_t(f);
            return true;
        case '>':
        case '(': *out = int64_t(f); return true;   // x > 5.5 == x > 5;  x <= 5.5 == x <= 5
        case ')':
        case '<': *out = int64_t(ce); return true;  // x >= 5.5 == x >= 6; x < 5.5 == x < 6
        default: return false;
    }
}

void applySeq(Bounds* b, char op, int64_t v) {
    switch (op) {
        case '=':
            b->after = std::max(b->after, v - 1);
            b->through = std::min(b->through, v);
            break;
        case '>': b->after = std::max(b->after, v); break;
        case ')': b->after = std::max(b->after, v - 1); break;
        case '<': b->through = std::min(b->through, v - 1); break;
        case '(': b->through = std::min(b->through, v); break;
    }
}

uint8_t predOp(char op) {
    switch (op) {
        case '=': return P4_OP_EQ;
        case '>': return P4_OP_GT;
        case ')': return P4_OP_GE;
        case '<': return P4_OP_LT;
        case '(': return P4_OP_LE;
        default: return 0;
    }
}

bool spaceAt(const unsigned char* s, int n) {
    auto sp = [](unsigned char ch) { return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v'; };
    return n > 0 && (sp(s[0]) || sp(s[n - 1]));
}

int relFilter(sqlite3_vtab_cursor* cur, int idxNum, const char* idxStr, int argc, sqlite3_value** argv) {
    RelCursor* c = static_cast<RelCursor*>(cur);
    RelVtab* vt = c->vt;
    const TypeEntry* t = vt->t;
    c->closeAll();
    c->vals.clear();
    c->preds.clear();
    c->strs.clear();
    c->specs.clear();
    c->eof = true;
    c->at = 0;
    c->merge = (idxNum & kPlanOrdered) != 0;
    c->desc = (idxNum & kPlanDesc) != 0;
    const bool hydrate = (idxNum & kPlanHydrate) != 0;

    // Constraints.
    Bounds seq;
    std::string sourceEq;
    bool hasSourceEq = false;
    struct PredIn {
        uint8_t field, op;
        P4Value v;
        std::string text;
    };
    std::vector<PredIn> pin;
    int64_t col0Lo = INT64_MIN, col0Hi = INT64_MAX, col0Eq = 0;
    bool col0HasEq = false;
    const size_t plen = idxStr ? std::strlen(idxStr) : 0;
    for (int i = 0; i < argc && size_t(2 * i + 1) < plen; i++) {
        const char target = idxStr[2 * i], op = idxStr[2 * i + 1];
        sqlite3_value* v = argv[i];
        if (sqlite3_value_type(v) == SQLITE_NULL) {   // any comparison with NULL keeps no row
            seq.empty = true;
            continue;
        }
        int64_t iv = 0;
        switch (target) {
            case 'S':
                if (intBound(v, op, &iv, &seq.empty)) applySeq(&seq, op, iv);
                break;
            case 'E':
            case 'T':
                if (intBound(v, op, &iv, &seq.empty)) {
                    PredIn p;
                    p.field = target == 'E' ? P4_F_EPOCH : P4_F_TS;
                    p.op = predOp(op);
                    p.v = P4Value{1, iv, 0, nullptr, 0};
                    pin.push_back(p);
                }
                break;
            case 'A':   // COL0 is `u64pos`: zero and negatives are absent there
                if (intBound(v, op, &iv, &seq.empty)) {
                    if (op == '=') {
                        col0HasEq = true;
                        col0Eq = iv;
                    } else if (op == '>') col0Lo = std::max(col0Lo, iv == INT64_MAX ? iv : iv + 1);
                    else if (op == ')') col0Lo = std::max(col0Lo, iv);
                    else if (op == '<') col0Hi = std::min(col0Hi, iv == INT64_MIN ? iv : iv - 1);
                    else if (op == '(') col0Hi = std::min(col0Hi, iv);
                }
                break;
            case 'B':   // COL1 is `str`, trimmed, empty = absent: push untrimmed non-empty text only
                if (sqlite3_value_type(v) == SQLITE_TEXT) {
                    const unsigned char* s = sqlite3_value_text(v);
                    const int n = sqlite3_value_bytes(v);
                    if (n > 0 && !spaceAt(s, n)) {
                        PredIn p;
                        p.field = P4_F_COL1;
                        p.op = P4_OP_EQ;
                        p.text.assign(reinterpret_cast<const char*>(s), size_t(n));
                        p.v = P4Value{3, 0, 0, nullptr, 0};
                        pin.push_back(p);
                    }
                }
                break;
            case 'R':
                if (sqlite3_value_type(v) == SQLITE_TEXT) {
                    const std::string s(reinterpret_cast<const char*>(sqlite3_value_text(v)), size_t(sqlite3_value_bytes(v)));
                    if (hasSourceEq && s != sourceEq) seq.empty = true;
                    hasSourceEq = true;
                    sourceEq = s;
                }
                break;
        }
    }
    if (col0HasEq) {
        if (col0Eq >= 1) pin.push_back({P4_F_COL0, P4_OP_EQ, P4Value{1, col0Eq, 0, nullptr, 0}, std::string()});
    } else if (col0Lo >= 1) {
        pin.push_back({P4_F_COL0, P4_OP_GE, P4Value{1, col0Lo, 0, nullptr, 0}, std::string()});
        if (col0Hi != INT64_MAX) pin.push_back({P4_F_COL0, P4_OP_LE, P4Value{1, col0Hi, 0, nullptr, 0}, std::string()});
    }
    if (seq.after < 0) seq.after = 0;
    if (seq.through < 1 || seq.after >= seq.through) seq.empty = true;
    if (seq.empty) return SQLITE_OK;

    // Sources: one cursor each.
    std::vector<std::string> srcs;
    int32_t rc = sourcesOf(vt->ls, t->name, &srcs);
    if (rc < 0) return fail(c, rc, "sources unavailable");
    const std::string prefix = t->name + "@";
    std::vector<std::string> want;   // the sources this scan returns
    if (vt->kind == kRelAlias) {
        if (std::binary_search(srcs.begin(), srcs.end(), vt->source)) want.push_back(vt->source);
    } else {
        want = srcs;
    }
    if (hasSourceEq) {
        if (sourceEq.compare(0, prefix.size(), prefix) != 0) return SQLITE_OK;
        const std::string s = sourceEq.substr(prefix.size());
        want.erase(std::remove_if(want.begin(), want.end(), [&](const std::string& x) { return x != s; }), want.end());
    }
    if (want.empty()) return SQLITE_OK;

    // Spec storage: predicates point into vals, text into strs.
    for (PredIn& p : pin) {
        P4Value v = p.v;
        if (p.v.type == 3) {
            c->strs.push_back(p.text);
            v.s = reinterpret_cast<const uint8_t*>(c->strs.back().data());
            v.n = uint32_t(c->strs.back().size());
        }
        c->vals.push_back(v);
        c->preds.push_back(P4Pred{p.field, p.op, 1, &c->vals.back()});
    }
    P4ScanSpec base;
    std::memset(&base, 0, sizeof(base));
    base.type = t->name.c_str();
    base.seqAfter = seq.after;
    base.seqThrough = seq.through == INT64_MAX ? 0 : seq.through;
    base.preds = c->preds.empty() ? nullptr : c->preds.data();
    base.nPreds = uint32_t(c->preds.size());
    base.order = c->desc ? P4_ORDER_SEQ_DESC : P4_ORDER_SEQ_ASC;
    base.hydrate = hydrate ? 1 : 0;
    base.bound = t->bound;
    for (const std::string& src : want) {
        c->strs.push_back(src);
        P4ScanSpec sp = base;
        sp.lane.source = c->strs.back().c_str();
        c->specs.push_back(sp);
        c->subs.emplace_back();
        c->subs.back().source = src;
    }
    // Concatenation opens one cursor at a time; a merge needs every head.
    if (c->merge) {
        for (size_t i = 0; i < c->subs.size(); i++) {
            rc = open(c, i);
            if (rc < 0) return fail(c, rc, "reader cursor failed");
        }
    } else {
        rc = open(c, 0);
        if (rc < 0) return fail(c, rc, "reader cursor failed");
    }
    rc = settle(c);
    if (rc < 0) return fail(c, rc, "reader cursor failed");
    return SQLITE_OK;
}

int relNext(sqlite3_vtab_cursor* cur) {
    RelCursor* c = static_cast<RelCursor*>(cur);
    if (c->eof) return SQLITE_OK;
    int32_t rc = step(c->subs[c->at]);
    if (rc >= 0) rc = settle(c);
    if (rc < 0) return fail(c, rc, "reader cursor failed");
    return SQLITE_OK;
}

int relEof(sqlite3_vtab_cursor* cur) { return static_cast<RelCursor*>(cur)->eof ? 1 : 0; }

int relColumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int i) {
    RelCursor* c = static_cast<RelCursor*>(cur);
    RelVtab* vt = c->vt;
    if (c->eof) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    const P4Row& r = c->subs[c->at].row;
    const uint8_t* p = nullptr;
    size_t n = 0;
    if (i < vt->ns || i == vt->meta(kData)) {
        if (!r.data && r.len > 0) {
            sqlite3_result_error(ctx, "flatsql_p4: record bytes not read (plan without hydration)", -1);
            return SQLITE_ERROR;
        }
        payloadOf(vt->t->fid, r.data, r.dataLen, &p, &n);
    }
    if (i < vt->ns) {
        resultColumn(ctx, vt->t->cols[size_t(i)], p, n);
        return SQLITE_OK;
    }
    switch (i - vt->ns) {
        case kSource: sqlite3_result_text(ctx, c->sourceText.data(), int(c->sourceText.size()), SQLITE_TRANSIENT); break;
        case kRowid:
        case kSeq: sqlite3_result_int64(ctx, r.seq); break;
        case kOffset: sqlite3_result_int64(ctx, 0); break;
        case kData:
            if (n > 0) sqlite3_result_blob(ctx, p, int(n), SQLITE_TRANSIENT);
            else sqlite3_result_null(ctx);
            break;
        case kCid:
            if (r.cid) sqlite3_result_text(ctx, r.cid, -1, SQLITE_TRANSIENT);
            else sqlite3_result_null(ctx);
            break;
        case kTs: sqlite3_result_int64(ctx, r.ts); break;
        case kEpoch:
            if (r.hasEpoch) sqlite3_result_int64(ctx, r.epoch);
            else sqlite3_result_null(ctx);
            break;
        case kProducer:
            if (r.producer) sqlite3_result_text(ctx, r.producer, -1, SQLITE_TRANSIENT);
            else sqlite3_result_null(ctx);
            break;
        case kPeer:
            if (r.peer) sqlite3_result_text(ctx, r.peer, -1, SQLITE_TRANSIENT);
            else sqlite3_result_null(ctx);
            break;
        default: sqlite3_result_null(ctx); break;
    }
    return SQLITE_OK;
}

int relRowid(sqlite3_vtab_cursor* cur, sqlite3_int64* out) {
    RelCursor* c = static_cast<RelCursor*>(cur);
    *out = c->eof ? 0 : c->subs[c->at].row.seq;
    return SQLITE_OK;
}

int relOpen(sqlite3_vtab* v, sqlite3_vtab_cursor** out) {
    RelCursor* c = new RelCursor();
    c->vt = static_cast<RelVtab*>(v);
    *out = c;
    return SQLITE_OK;
}

int relClose(sqlite3_vtab_cursor* cur) {
    delete static_cast<RelCursor*>(cur);
    return SQLITE_OK;
}

sqlite3_module gRelModule = {
    3,          relConnect, relConnect, relBestIndex, relDisconnect, relDisconnect, relOpen, relClose,
    relFilter,  relNext,    relEof,     relColumn,    relRowid,
    nullptr,  // xUpdate
    nullptr, nullptr, nullptr, nullptr,  // xBegin, xSync, xCommit, xRollback
    nullptr,  // xFindFunction
    nullptr,  // xRename
    nullptr, nullptr, nullptr,  // xSavepoint, xRelease, xRollbackTo
    nullptr,  // xShadowName
    nullptr,  // xIntegrity
};

}  // namespace

int registerModule(LaneState* ls) { return sqlite3_create_module_v2(ls->db, "flatsql_p4", &gRelModule, ls, nullptr); }

bool isRelation(const LaneState* ls, const char* name) { return name && ls->relations.count(lower(name)) != 0; }

int32_t ensureRelation(LaneState* ls, const std::string& name, std::string* err) {
    const std::string key = lower(name);
    if (ls->relations.count(key)) return 0;
    const size_t at = name.find('@');
    const std::string typePart = at == std::string::npos ? name : name.substr(0, at);
    const TypeEntry* t = typeByName(ls, typePart);
    if (!t) {
        const int32_t rc = loadTypes(ls, err);
        if (rc < 0) return rc;
        t = typeByName(ls, typePart);
        if (!t) return 0;
    }
    RelSpec spec;
    spec.type = t->name;
    if (at != std::string::npos) {
        std::vector<std::string> srcs;
        const int32_t rc = sourcesOf(ls, t->name, &srcs);
        if (rc < 0) {
            if (err) *err = "sources unavailable";
            return rc;
        }
        const std::string want = lower(name.substr(at + 1));
        for (const std::string& s : srcs)
            if (lower(s) == want) {
                spec.kind = kRelAlias;
                spec.source = s;
            }
        if (spec.kind != kRelAlias) return 0;   // format 1: no per-source table for a source it never saw
    }
    ls->relations.emplace(key, spec);
    const std::string ddl = "CREATE VIRTUAL TABLE " + quoteIdent(name) + " USING flatsql_p4";
    char* msg = nullptr;
    const int rc = sqlite3_exec(ls->db, ddl.c_str(), nullptr, nullptr, &msg);
    if (rc != SQLITE_OK) {
        ls->relations.erase(key);
        if (err) *err = msg ? msg : sqlite3_errstr(rc);
        sqlite3_free(msg);
        return rc == SQLITE_NOMEM ? P4_E_NOMEM : P4_E_SQL;
    }
    return 1;
}

}  // namespace p4sql
}  // namespace flatsql
