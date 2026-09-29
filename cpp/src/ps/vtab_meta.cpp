// FlatSQL partition store: meta virtual tables (design §9): counters from
// heads, lane counters, licences and a type's arrivals. None is public
// (A28): sandboxed statements cannot create them.
//
//   flatsql_partitions  pid, producer, sql_name, type, commit_seq, pseq_hi,
//                       total_count, total_bytes, live_count, live_bytes,
//                       tomb_count, disk_bytes, min_epoch, max_epoch,
//                       latest_arrival, quarantined, manifest_gen,
//                       merged_through
//   flatsql_lanes       pid, producer, type, lane_id, provider, source, batch,
//                       peer, pubkey, count, bytes, max_pseq, first_seen,
//                       updated
//   flatsql_licences    pid, producer, type, licence_key, pseq, arrival, data
//   flatsql_arrivals    gseq, pid, pseq, live, cid, type (hidden, required)
// Counters the writer does not maintain are NULL, never zero (min/max epoch
// of an empty partition, disk bytes before T3's accounting).
#include <sqlite3.h>

#include <algorithm>
#include <cstring>

#include "internal.h"
#include "vtab_internal.h"

namespace flatsql {
namespace ps {

namespace {

enum MetaKind : int { kMkPartitions = 1, kMkLanes = 2, kMkLicences = 3, kMkArrivals = 4, kMkTypes = 5 };

struct MetaVtab : sqlite3_vtab {
    ReaderLane* lane = nullptr;
    int kind = 0;
};

struct Cell {
    int type = SQLITE_NULL;
    int64_t i = 0;
    std::string s;
};

struct MetaCursor : sqlite3_vtab_cursor {
    MetaVtab* vt = nullptr;
    std::vector<std::vector<Cell>> rows;
    size_t at = 0;
    // arrivals streaming
    StmtCtx* stmt = nullptr;
    TypeSnap* ts = nullptr;
    std::unique_ptr<RowSource> arr;
    CurRow cur;
    bool arrEof = true;
    uint64_t lo = 0, hi = UINT64_MAX;
    bool desc = false;
};

Cell ci(int64_t v) {
    Cell c;
    c.type = SQLITE_INTEGER;
    c.i = v;
    return c;
}
Cell ct(const std::string& s) {
    Cell c;
    c.type = SQLITE_TEXT;
    c.s = s;
    return c;
}
Cell cb(const void* p, size_t n) {
    Cell c;
    c.type = SQLITE_BLOB;
    c.s.assign(static_cast<const char*>(p), n);
    return c;
}
Cell cnull() { return Cell(); }

const char* ddlFor(int kind) {
    switch (kind) {
        case kMkPartitions:
            return "CREATE TABLE x(pid INTEGER, producer TEXT, sql_name TEXT, type TEXT, commit_seq INTEGER, "
                   "pseq_hi INTEGER, total_count INTEGER, total_bytes INTEGER, live_count INTEGER, live_bytes INTEGER, "
                   "tomb_count INTEGER, disk_bytes INTEGER, min_epoch INTEGER, max_epoch INTEGER, "
                   "latest_arrival INTEGER, quarantined INTEGER, manifest_gen INTEGER, merged_through INTEGER)";
        case kMkLanes:
            return "CREATE TABLE x(pid INTEGER, producer TEXT, type TEXT, lane_id INTEGER, provider TEXT, source TEXT, "
                   "batch TEXT, peer TEXT, pubkey TEXT, count INTEGER, bytes INTEGER, max_pseq INTEGER, "
                   "first_seen INTEGER, updated INTEGER)";
        case kMkLicences:
            return "CREATE TABLE x(pid INTEGER, producer TEXT, type TEXT, licence_key TEXT, pseq INTEGER, "
                   "arrival INTEGER, data BLOB)";
        case kMkArrivals:
            return "CREATE TABLE x(gseq INTEGER, pid INTEGER, pseq INTEGER, live INTEGER, cid TEXT, "
                   "type TEXT HIDDEN)";
        case kMkTypes:
            // Datasync v1 (A15, A16): MaxRowID = gseq_hi; TotalCount =
            // first_live_count; SnapshotID inputs.
            return "CREATE TABLE x(type TEXT, schema TEXT, commit_seq INTEGER, gseq_hi INTEGER, arrivals INTEGER, "
                   "first_live_count INTEGER, first_live_bytes INTEGER, partitions INTEGER)";
    }
    return nullptr;
}

int metaConnect(sqlite3* db, void* aux, int argc, const char* const* argv, sqlite3_vtab** out, char** err) {
    if (argc < 4) {
        *err = sqlite3_mprintf("flatsql_ps_meta: missing argument");
        return SQLITE_ERROR;
    }
    std::string a = argv[3];
    if (a.size() >= 2 && a[0] == '\'') a = a.substr(1, a.size() - 2);
    int kind = a == "partitions" ? kMkPartitions : a == "lanes" ? kMkLanes : a == "licences" ? kMkLicences
             : a == "arrivals"   ? kMkArrivals
             : a == "types"      ? kMkTypes
                                 : 0;
    if (!kind) {
        *err = sqlite3_mprintf("flatsql_ps_meta: unknown table %s", a.c_str());
        return SQLITE_ERROR;
    }
    const int rc = sqlite3_declare_vtab(db, ddlFor(kind));
    if (rc != SQLITE_OK) return rc;
    MetaVtab* vt = new MetaVtab();
    vt->lane = static_cast<ReaderLane*>(aux);
    vt->kind = kind;
    *out = vt;
    return SQLITE_OK;
}

int metaDisconnect(sqlite3_vtab* v) {
    delete static_cast<MetaVtab*>(v);
    return SQLITE_OK;
}

// idxStr: 'B' (bounded) or 'U', then for arrivals the argv layout.
int metaBestIndex(sqlite3_vtab* v, sqlite3_index_info* info) {
    MetaVtab* vt = static_cast<MetaVtab*>(v);
    if (vt->kind != kMkArrivals) {
        info->estimatedCost = 100;
        info->estimatedRows = 1000;
        info->idxStr = sqlite3_mprintf("B");
        info->needToFreeIdxStr = 1;
        return SQLITE_OK;
    }
    int typeEq = -1, lo = -1, hi = -1, eq = -1, limit = -1;
    for (int i = 0; i < info->nConstraint; i++) {
        const auto& c = info->aConstraint[i];
        if (!c.usable) continue;
        if (c.op == SQLITE_INDEX_CONSTRAINT_LIMIT) limit = i;
        if (c.iColumn == 5 && c.op == SQLITE_INDEX_CONSTRAINT_EQ) typeEq = i;
        if (c.iColumn == 0) {
            if (c.op == SQLITE_INDEX_CONSTRAINT_EQ) eq = i;
            else if (c.op == SQLITE_INDEX_CONSTRAINT_GT || c.op == SQLITE_INDEX_CONSTRAINT_GE) lo = i;
            else if (c.op == SQLITE_INDEX_CONSTRAINT_LT || c.op == SQLITE_INDEX_CONSTRAINT_LE) hi = i;
        }
    }
    if (typeEq < 0) {
        info->estimatedCost = 1e15;  // a type is required
        info->idxStr = sqlite3_mprintf("U:-1:-1:-1:0:0:0");
        info->needToFreeIdxStr = 1;
        return SQLITE_OK;
    }
    int n = 0;
    info->aConstraintUsage[typeEq].argvIndex = ++n;
    info->aConstraintUsage[typeEq].omit = 1;
    int aLo = -1, aHi = -1, loOp = 0, hiOp = 0;
    if (eq >= 0) {
        info->aConstraintUsage[eq].argvIndex = ++n;
        aLo = aHi = n;
        loOp = SQLITE_INDEX_CONSTRAINT_GE;
        hiOp = SQLITE_INDEX_CONSTRAINT_LE;
    } else {
        if (lo >= 0) {
            info->aConstraintUsage[lo].argvIndex = ++n;
            aLo = n;
            loOp = info->aConstraint[lo].op;
        }
        if (hi >= 0) {
            info->aConstraintUsage[hi].argvIndex = ++n;
            aHi = n;
            hiOp = info->aConstraint[hi].op;
        }
    }
    bool desc = false, consumed = info->nOrderBy == 0;
    if (info->nOrderBy == 1 && info->aOrderBy[0].iColumn == 0) {
        desc = info->aOrderBy[0].desc;
        consumed = true;
    }
    info->orderByConsumed = consumed && info->nOrderBy ? 1 : 0;
    const bool bounded = (aLo >= 0 && aHi >= 0) || (limit >= 0 && consumed);
    info->estimatedCost = bounded ? 100 : 1e6;
    info->estimatedRows = bounded ? 100 : 1000000;
    info->idxStr = sqlite3_mprintf("%c:%d:%d:%d:%d:%d:%d", bounded ? 'B' : 'U', 1, aLo, aHi, loOp, hiOp, desc ? 1 : 0);
    info->needToFreeIdxStr = 1;
    return SQLITE_OK;
}

int metaOpen(sqlite3_vtab* v, sqlite3_vtab_cursor** out) {
    MetaCursor* c = new MetaCursor();
    c->vt = static_cast<MetaVtab*>(v);
    *out = c;
    return SQLITE_OK;
}

int metaClose(sqlite3_vtab_cursor* c) {
    delete static_cast<MetaCursor*>(c);
    return SQLITE_OK;
}

int metaFail(MetaCursor* c, int32_t rc, const char* what) {
    if (c->stmt && !c->stmt->vtabStatus) {
        c->stmt->vtabStatus = rc;
        c->stmt->vtabMessage = what;
    }
    sqlite3_free(c->vt->zErrMsg);
    c->vt->zErrMsg = sqlite3_mprintf("flatsql_ps: %s (status %d)", what, int(rc));
    if (rc == kRsCancelled || rc == kRsStopped || rc == kRsTimeout) return SQLITE_INTERRUPT;
    return rc == kRsNoMem ? SQLITE_NOMEM : SQLITE_ERROR;
}

std::string typeNameOf(const RegistryView& reg, const uint8_t fid[4]) {
    const TypeInfo* t = reg.typeByFid(fid);
    return t ? t->typeName : std::string();
}

int32_t fillPartitions(MetaCursor* c) {
    ReaderLane* lane = c->vt->lane;
    const RegistryView& reg = *c->stmt->reg;
    for (const PartInfo& pi : reg.parts) {
        if (!pi.pid || pi.dropped) continue;
        PartSnap* s = nullptr;
        const int32_t rc = lane->partCounters(c->stmt, pi.pid, &s);
        if (rc < 0) return rc;
        std::vector<Cell> r;
        r.push_back(ci(pi.pid));
        r.push_back(ct(pi.token));
        r.push_back(ct(pi.sqlName));
        r.push_back(ct(typeNameOf(reg, pi.fid)));
        const Counters& k = s->head.counters;
        const bool e = s->empty;
        r.push_back(ci(e ? 0 : int64_t(s->head.commitSeq)));
        r.push_back(ci(e ? 0 : int64_t(s->head.pseqHi)));
        r.push_back(ci(e ? 0 : int64_t(k.totalCount)));
        r.push_back(ci(e ? 0 : int64_t(k.totalBytes)));
        r.push_back(ci(e ? 0 : int64_t(k.liveCount)));
        r.push_back(ci(e ? 0 : int64_t(k.liveBytes)));
        r.push_back(ci(e ? 0 : int64_t(k.tombCount)));
        r.push_back(e || !k.diskBytes ? cnull() : ci(int64_t(k.diskBytes)));
        r.push_back(e || k.minEpoch == INT64_MAX ? cnull() : ci(k.minEpoch));
        r.push_back(e || k.maxEpoch == INT64_MIN ? cnull() : ci(k.maxEpoch));
        r.push_back(e || k.latestArrival == INT64_MIN ? cnull() : ci(k.latestArrival));
        r.push_back(ci((pi.quarantined || (!e && (s->head.p.flags & kHeadQuarantined))) ? 1 : 0));
        r.push_back(ci(e ? 0 : int64_t(s->head.manifestGen)));
        r.push_back(ci(e ? 0 : int64_t(s->head.mergedThrough)));
        c->rows.push_back(std::move(r));
    }
    return 0;
}

int32_t fillLanes(MetaCursor* c) {
    ReaderLane* lane = c->vt->lane;
    const RegistryView& reg = *c->stmt->reg;
    for (const PartInfo& pi : reg.parts) {
        if (!pi.pid || pi.dropped) continue;
        PartSnap* s = nullptr;
        int32_t rc = lane->partCounters(c->stmt, pi.pid, &s);
        if (rc < 0) return rc;
        if (s->empty) continue;
        std::vector<LaneCounter> counters;
        rc = lane->store().laneCounters(*s, &counters);
        if (rc < 0) return rc;
        if (counters.empty()) continue;
        std::vector<LaneStore::LaneTuple> tuples;
        rc = lane->store().laneTuples(*s, &tuples);
        if (rc < 0) return rc;
        for (const LaneCounter& lc : counters) {
            const LaneStore::LaneTuple* t = nullptr;
            for (const auto& x : tuples)
                if (x.id == lc.laneId) t = &x;
            std::vector<Cell> r;
            r.push_back(ci(pi.pid));
            r.push_back(ct(pi.token));
            r.push_back(ct(typeNameOf(reg, pi.fid)));
            r.push_back(ci(lc.laneId));
            r.push_back(t ? ct(t->provider) : cnull());
            r.push_back(t ? ct(t->source) : cnull());
            r.push_back(t ? ct(t->batch) : cnull());
            r.push_back(t ? ct(t->peer) : cnull());
            r.push_back(t ? ct(t->pubkey) : cnull());
            r.push_back(ci(lc.count));
            r.push_back(ci(lc.bytes));
            r.push_back(ci(int64_t(lc.maxPseq)));
            r.push_back(lc.firstSeen == 0 || lc.firstSeen == INT64_MAX ? cnull() : ci(lc.firstSeen));
            r.push_back(lc.updated == 0 || lc.updated == INT64_MIN ? cnull() : ci(lc.updated));
            c->rows.push_back(std::move(r));
        }
    }
    return 0;
}

int32_t fillLicences(MetaCursor* c) {
    ReaderLane* lane = c->vt->lane;
    LaneStore& st = lane->store();
    const RegistryView& reg = *c->stmt->reg;
    for (const PartInfo& pi : reg.parts) {
        if (!pi.pid || pi.dropped) continue;
        PartSnap* s = nullptr;
        int32_t rc = lane->part(c->stmt, pi.pid, &s);
        if (rc < 0) return rc;
        if (s->empty) continue;
        PostingScan ps = st.scan(*s, kIxLicence, nullptr, 0, nullptr, 0, false);
        if (ps.err()) return ps.err();
        for (; ps.valid(); ps.next()) {
            const uint64_t pseq = getBE64(ps.val());
            bool dead = false;
            rc = st.isDead(*s, pseq, s->pseqHi(), &dead);
            if (rc < 0) return rc;
            if (dead) continue;
            RecRow row;
            rc = st.readRow(*s, pseq, &row);
            if (rc < 0) return rc;
            if (row.kind != kRowLicence) continue;
            std::vector<uint8_t> frame(row.len);
            rc = st.readFrame(*s, row, frame.data());
            if (rc < 0) return rc;
            std::vector<Cell> r;
            r.push_back(ci(pi.pid));
            r.push_back(ct(pi.token));
            r.push_back(ct(typeNameOf(reg, pi.fid)));
            r.push_back(ct(std::string(reinterpret_cast<const char*>(ps.key()), ps.klen())));
            r.push_back(ci(int64_t(pseq)));
            r.push_back(ci(row.arrivalMs));
            r.push_back(cb(frame.data(), frame.size()));
            c->rows.push_back(std::move(r));
        }
        if (ps.err()) return ps.err();
    }
    return 0;
}

int32_t fillTypes(MetaCursor* c) {
    const RegistryView& reg = *c->stmt->reg;
    for (const auto& t : reg.types) {
        TypeSnap* ts = nullptr;
        const int32_t rc = c->vt->lane->type(c->stmt, t->fid, &ts);
        if (rc < 0) return rc;
        std::vector<Cell> r;
        r.push_back(ct(t->typeName));
        r.push_back(ct(t->schemaName));
        const bool e = ts->empty;
        r.push_back(ci(e ? 0 : int64_t(ts->head.commitSeq)));
        r.push_back(ci(e ? 0 : int64_t(ts->head.gseqHi)));
        r.push_back(ci(e ? 0 : int64_t(ts->head.arrivalsCount)));
        r.push_back(ci(e ? 0 : int64_t(ts->head.firstLiveCount)));
        r.push_back(ci(e ? 0 : int64_t(ts->head.firstLiveBytes)));
        r.push_back(ci(int64_t(t->pids.size())));
        c->rows.push_back(std::move(r));
    }
    return 0;
}

int metaFilter(sqlite3_vtab_cursor* cur, int, const char* idxStr, int argc, sqlite3_value** argv) {
    MetaCursor* c = static_cast<MetaCursor*>(cur);
    c->rows.clear();
    c->at = 0;
    c->stmt = c->vt->lane->current();
    if (!c->stmt || !c->stmt->reg) return metaFail(c, kRsSqlError, "no statement context");
    int32_t rc = 0;
    switch (c->vt->kind) {
        case kMkPartitions: rc = fillPartitions(c); break;
        case kMkLanes: rc = fillLanes(c); break;
        case kMkLicences: rc = fillLicences(c); break;
        case kMkTypes: rc = fillTypes(c); break;
        case kMkArrivals: {
            int one, aLo, aHi, loOp, hiOp, d;
            if (!idxStr || sscanf(idxStr + 1, ":%d:%d:%d:%d:%d:%d", &one, &aLo, &aHi, &loOp, &hiOp, &d) != 6 ||
                one != 1 || argc < 1) {
                c->arrEof = true;
                return SQLITE_OK;  // no type: empty
            }
            const unsigned char* tn = sqlite3_value_text(argv[0]);
            const TypeInfo* ti = tn ? c->stmt->reg->typeByName(reinterpret_cast<const char*>(tn)) : nullptr;
            if (!ti) {
                c->arrEof = true;
                return SQLITE_OK;
            }
            rc = c->vt->lane->type(c->stmt, ti->fid, &c->ts);
            if (rc < 0) break;
            c->lo = 1;
            c->hi = c->ts->gseqHi();
            if (aLo > 0) {
                const int64_t v = sqlite3_value_int64(argv[aLo - 1]);
                const int64_t lv = loOp == SQLITE_INDEX_CONSTRAINT_GT ? v + 1 : v;
                c->lo = lv < 1 ? 1 : uint64_t(lv);
            }
            if (aHi > 0) {
                const int64_t v = sqlite3_value_int64(argv[aHi - 1]);
                const int64_t hv = hiOp == SQLITE_INDEX_CONSTRAINT_LT ? v - 1 : v;
                if (hv < 1) {
                    c->arrEof = true;
                    return SQLITE_OK;
                }
                c->hi = std::min<uint64_t>(c->hi, uint64_t(hv));
            }
            c->desc = d != 0;
            c->arr = makeArrivalRows(c->vt->lane, c->stmt, c->ts, c->lo, c->hi, c->desc, TagMatch(), 0);
            rc = c->arr->next(&c->cur);
            if (rc < 0) break;
            c->arrEof = rc == 0;
            return SQLITE_OK;
        }
    }
    if (rc < 0) return metaFail(c, rc, "meta read failed");
    return SQLITE_OK;
}

int metaNext(sqlite3_vtab_cursor* cur) {
    MetaCursor* c = static_cast<MetaCursor*>(cur);
    if (c->vt->kind == kMkArrivals) {
        const int32_t rc = c->arr ? c->arr->next(&c->cur) : 0;
        if (rc < 0) return metaFail(c, rc, "arrivals read failed");
        c->arrEof = rc == 0;
        return SQLITE_OK;
    }
    c->at++;
    return SQLITE_OK;
}

int metaEof(sqlite3_vtab_cursor* cur) {
    MetaCursor* c = static_cast<MetaCursor*>(cur);
    if (c->vt->kind == kMkArrivals) return c->arrEof ? 1 : 0;
    return c->at >= c->rows.size() ? 1 : 0;
}

int metaColumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int i) {
    MetaCursor* c = static_cast<MetaCursor*>(cur);
    if (c->vt->kind == kMkArrivals) {
        switch (i) {
            case 0: sqlite3_result_int64(ctx, sqlite3_int64(c->cur.gseq)); break;
            case 1: sqlite3_result_int64(ctx, c->cur.pid); break;
            case 2: sqlite3_result_int64(ctx, sqlite3_int64(c->cur.row.pseq)); break;
            case 3: sqlite3_result_int64(ctx, 1); break;
            case 4: {
                const std::string t = cidToText(c->cur.row.cid);
                sqlite3_result_text(ctx, t.data(), int(t.size()), SQLITE_TRANSIENT);
                break;
            }
            default: sqlite3_result_null(ctx); break;
        }
        return SQLITE_OK;
    }
    const Cell& cell = c->rows[c->at][size_t(i)];
    switch (cell.type) {
        case SQLITE_INTEGER: sqlite3_result_int64(ctx, cell.i); break;
        case SQLITE_TEXT: sqlite3_result_text(ctx, cell.s.data(), int(cell.s.size()), SQLITE_TRANSIENT); break;
        case SQLITE_BLOB: sqlite3_result_blob(ctx, cell.s.data(), int(cell.s.size()), SQLITE_TRANSIENT); break;
        default: sqlite3_result_null(ctx); break;
    }
    return SQLITE_OK;
}

int metaRowid(sqlite3_vtab_cursor* cur, sqlite3_int64* out) {
    MetaCursor* c = static_cast<MetaCursor*>(cur);
    *out = c->vt->kind == kMkArrivals ? sqlite3_int64(c->cur.gseq) : sqlite3_int64(c->at);
    return SQLITE_OK;
}

sqlite3_module gMetaModule = {
    0, metaConnect, metaConnect, metaBestIndex, metaDisconnect, metaDisconnect, metaOpen, metaClose, metaFilter,
    metaNext, metaEof, metaColumn, metaRowid, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr};

}  // namespace

int registerMetaModule(sqlite3* db, ReaderLane* lane) {
    return sqlite3_create_module_v2(db, "flatsql_ps_meta", &gMetaModule, lane, nullptr);
}

int metaEnsure(ReaderLane* lane, const std::string& name, std::string* err) {
    std::string lower;
    for (char ch : name) lower.push_back(char(std::tolower(uint8_t(ch))));
    const char* arg = lower == "flatsql_partitions" ? "partitions" : lower == "flatsql_lanes" ? "lanes"
                    : lower == "flatsql_licences"   ? "licences"
                    : lower == "flatsql_arrivals"   ? "arrivals"
                    : lower == "flatsql_types"      ? "types"
                                                    : nullptr;
    if (!arg) return 0;
    const std::string sql = "CREATE VIRTUAL TABLE temp." + lower + " USING flatsql_ps_meta('" + arg + "')";
    char* msg = nullptr;
    const int rc = sqlite3_exec(lane->db(), sql.c_str(), nullptr, nullptr, &msg);
    if (rc != SQLITE_OK) {
        if (err) *err = msg ? msg : sqlite3_errstr(rc);
        sqlite3_free(msg);
        return rc == SQLITE_NOMEM ? kRsNoMem : kRsSqlError;
    }
    lane->noteTable(lower);
    return 1;
}

}  // namespace ps
}  // namespace flatsql
