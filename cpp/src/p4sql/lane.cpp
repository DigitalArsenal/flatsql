// FlatSQL store format 4, SQL surface: the lane hooks (CONTRACT.md §3.9).
//
// Copy-adapted from format 2's ps/lane.cpp (prepare, bind, step, the RB1 and
// raw-stream output, the result caps, the progress handler) and
// ps/admission.cpp (the sandbox authorizer). The engine owns the lane
// thread, the slot and its ring: a statement's output goes out through
// p4_emit, which waits for ring space itself, so nothing here parks.
//
// Every plan is bounded by construction (each relation reads at most its A18
// window), so format 2's interactive admission (index-bounded plans only) has
// nothing left to refuse.
//
// Status mapping (all returned, never trapped):
//   P4_E_ARG        malformed parameters
//   P4_E_SQL        SQL errors, sandbox refusals, a raw stream with a non-BLOB cell
//   P4_E_BUDGET     result rows or bytes, VM steps, the sandbox heap cap, or a
//                   reader cap (rows examined, bytes read)
//   P4_E_CANCELLED  the slot's cancel word
//   P4_E_NOMEM      SQLite out of memory outside the sandbox cap
#include <algorithm>
#include <cctype>
#include <cstring>
#include <memory>

#include "flatsql/ps/result_block.h"
#include "internal.h"

namespace rb1 = flatsql::ps::rb1;

namespace flatsql {
namespace p4sql {

LaneState* stateOf(P4Lane* lane) {
    EngineCall ec;
    return static_cast<LaneState*>(p4_lane_sql_state(lane));
}

namespace {

// Binds the lane's arena (made at the first sandboxed statement) for one
// sandboxed statement.
class SandboxScope {
public:
    SandboxScope(LaneState* ls, Stmt* st) : saved_(arenaBound()) {
        if (!st->sandbox) return;
        if (!ls->arena) {
            std::unique_ptr<LaneArena> a(new LaneArena());
            if (a->init(size_t(kSandboxHeapCap))) {
                arenaRegister(a.get());
                ls->arena = a.release();
            }
        }
        // Without a region the statement runs on the system allocator: the
        // engine's hard heap limit still bounds it.
        if (!ls->arena) return;
        st->arena = ls->arena;
        st->arenaFailures = ls->arena->failures();
        arenaBind(ls->arena);
    }
    ~SandboxScope() { arenaBind(saved_); }
    SandboxScope(const SandboxScope&) = delete;
    SandboxScope& operator=(const SandboxScope&) = delete;

private:
    LaneArena* saved_;
};

int progress(void* p) {
    LaneState* ls = static_cast<LaneState*>(p);
    Stmt* s = ls->cur;
    if (!s) return 0;
    int32_t rc;
    {
        EngineCall ec;
        rc = p4_lane_check(ls->lane);
    }
    if (rc < 0) {
        s->raise(rc, rc == P4_E_CANCELLED ? "cancelled" : "work budget exhausted");
        return 1;
    }
    if (s->sandbox) {
        s->vmSteps += 4096;
        if (s->vmSteps > kSandboxMaxVmSteps) {
            s->raise(P4_E_BUDGET, "work budget exhausted (VM steps)");
            return 1;
        }
    }
    return 0;
}

// ---- the sandbox authorizer (ps/admission.cpp) ---------------------------------
struct SandboxAuth {
    LaneState* ls = nullptr;
    std::string violation;
};

const char* actionName(int action) {
    switch (action) {
        case SQLITE_PRAGMA: return "PRAGMA";
        case SQLITE_ATTACH: return "ATTACH";
        case SQLITE_DETACH: return "DETACH";
        case SQLITE_INSERT: return "INSERT";
        case SQLITE_UPDATE: return "UPDATE";
        case SQLITE_DELETE: return "DELETE";
        case SQLITE_TRANSACTION: return "TRANSACTION";
        case SQLITE_SAVEPOINT: return "SAVEPOINT";
        case SQLITE_ALTER_TABLE: return "ALTER TABLE";
        case SQLITE_REINDEX: return "REINDEX";
        case SQLITE_ANALYZE: return "ANALYZE";
        default: return "this operation";
    }
}

int sandboxAuthorizer(void* user, int action, const char* a1, const char*, const char*, const char*) {
    SandboxAuth* ctx = static_cast<SandboxAuth*>(user);
    switch (action) {
        case SQLITE_SELECT:
        case SQLITE_FUNCTION:
        case SQLITE_RECURSIVE: return SQLITE_OK;
        case SQLITE_READ: {
            // The lane's connection holds no table but the record relations:
            // any other name read is a CTE or subquery (allowed), SQLite's own
            // schema, a pragma function or a flatsql_* name (never public).
            const char* t = a1 ? a1 : "";
            if (isRelation(ctx->ls, t)) return SQLITE_OK;
            if (std::strncmp(t, "sqlite_", 7) != 0 && std::strncmp(t, "pragma_", 7) != 0 &&
                std::strncmp(t, "flatsql_", 8) != 0)
                return SQLITE_OK;
            if (ctx->violation.empty())
                ctx->violation = std::string("table \"") + t + "\" is outside the public query surface";
            return SQLITE_DENY;
        }
        default: break;
    }
    if (ctx->violation.empty())
        ctx->violation = std::string(actionName(action)) + " is not permitted (read-only SELECT sandbox)";
    return SQLITE_DENY;
}

// Name after "no such table: " (schema prefix dropped).
bool missingTable(const char* msg, std::string* name) {
    static const char* kPrefix = "no such table: ";
    const char* at = msg ? std::strstr(msg, kPrefix) : nullptr;
    if (!at) return false;
    std::string n = at + std::strlen(kPrefix);
    const size_t dot = n.find('.');
    if (dot != std::string::npos && (n.compare(0, dot, "main") == 0 || n.compare(0, dot, "temp") == 0))
        n = n.substr(dot + 1);
    *name = n;
    return !n.empty();
}

// Out of memory: the sandbox heap cap, or the process.
int32_t nomemStatus(const Stmt& st) { return st.capTripped() ? P4_E_BUDGET : P4_E_NOMEM; }

const char* kHeapCapMessage = "heap cap: the statement exceeds the sandbox lane heap";

int32_t prepare(LaneState* ls, Stmt& st, const char* sql, size_t n, sqlite3_stmt** out, std::string* msg) {
    for (int attempt = 0; attempt < 64; attempt++) {
        SandboxAuth auth;
        auth.ls = ls;
        if (st.sandbox) sqlite3_set_authorizer(ls->db, sandboxAuthorizer, &auth);
        const char* tail = nullptr;
        sqlite3_stmt* s = nullptr;
        const int rc = sqlite3_prepare_v3(ls->db, sql, int(n), 0, &s, &tail);
        if (st.sandbox) sqlite3_set_authorizer(ls->db, nullptr, nullptr);
        if (rc == SQLITE_OK) {
            if (!s) {
                *msg = "empty statement";
                return P4_E_SQL;
            }
            if (st.sandbox) {
                const char* why = nullptr;
                for (const char* p = tail; p && p < sql + n && *p; p++)
                    if (!std::isspace(uint8_t(*p))) {
                        why = "sandbox: multi-statement: exactly one SELECT statement is allowed";
                        break;
                    }
                if (!why && !sqlite3_stmt_readonly(s)) why = "sandbox: read-only: statement could modify the database";
                if (!why && sqlite3_column_count(s) <= 0) why = "sandbox: not-select: statement returns no result columns";
                if (why) {
                    sqlite3_finalize(s);
                    *msg = why;
                    return P4_E_SQL;
                }
            }
            *out = s;
            return P4_OK;
        }
        const std::string err = sqlite3_errmsg(ls->db);
        if (s) sqlite3_finalize(s);
        if (rc == SQLITE_NOMEM) {
            const int32_t status = nomemStatus(st);
            *msg = status == P4_E_BUDGET ? kHeapCapMessage : "out of memory";
            return status;
        }
        if (st.sandbox && !auth.violation.empty()) {
            *msg = "sandbox: not-authorized: " + auth.violation;
            return P4_E_SQL;
        }
        std::string table;
        if (missingTable(err.c_str(), &table)) {
            std::string e2;
            const int32_t made = ensureRelation(ls, table, &e2);
            if (made < 0) {
                *msg = e2;
                return made;
            }
            if (made == 1) continue;   // retry with the relation in place
        }
        *msg = err;
        return P4_E_SQL;
    }
    *msg = "too many relations";
    return P4_E_SQL;
}

// ---- output -----------------------------------------------------------------------
// One output cell (pointers valid until the row is written).
struct Cell {
    int type = SQLITE_NULL;
    int64_t i = 0;
    double d = 0;
    const void* p = nullptr;
    size_t n = 0;
};

void cellsOf(sqlite3_stmt* s, int ncols, std::vector<Cell>* out) {
    out->resize(size_t(ncols));
    for (int i = 0; i < ncols; i++) {
        Cell& c = (*out)[size_t(i)];
        c.type = sqlite3_column_type(s, i);
        switch (c.type) {
            case SQLITE_INTEGER: c.i = sqlite3_column_int64(s, i); break;
            case SQLITE_FLOAT: c.d = sqlite3_column_double(s, i); break;
            case SQLITE_TEXT:
                c.p = sqlite3_column_text(s, i);
                c.n = size_t(sqlite3_column_bytes(s, i));
                break;
            case SQLITE_BLOB:
                c.p = sqlite3_column_blob(s, i);
                c.n = size_t(sqlite3_column_bytes(s, i));
                break;
            default: break;
        }
    }
}

Cell textCell(const std::string& s) {
    Cell c;
    c.type = SQLITE_TEXT;
    c.p = s.data();
    c.n = s.size();
    return c;
}

Cell intCell(int64_t v) {
    Cell c;
    c.type = SQLITE_INTEGER;
    c.i = v;
    return c;
}

// RB1 (or raw frames) assembled into closed blocks and emitted through the
// slot's ring. The caps are checked before a row is encoded, so a refused row
// is never sent.
class Out {
public:
    Out(LaneState* ls, bool raw, uint64_t maxRows, uint64_t maxBytes)
        : ls_(ls), raw_(raw), maxRows_(maxRows), maxBytes_(maxBytes), enc_(&buf_) {}

    void header(const std::vector<std::string>& names) {
        if (!raw_) enc_.header(names);
    }

    // P4_OK, or a status with *msg set.
    int32_t row(const std::vector<Cell>& cells, std::string* msg) {
        if (maxRows_ && rows_ + 1 > maxRows_) {
            *msg = "row-cap: result exceeds the row limit";
            return P4_E_BUDGET;
        }
        size_t need = raw_ ? 0 : (enc_.blockBytes() == 0 ? 16 : 0);
        for (const Cell& c : cells) {
            if (raw_) {
                if (c.type != SQLITE_BLOB) {
                    *msg = "not-a-record-stream: raw stream queries must return only BLOB cells";
                    return P4_E_SQL;
                }
                need += 4 + c.n;
            } else if (c.type == SQLITE_INTEGER || c.type == SQLITE_FLOAT) {
                need += 9;
            } else if (c.type == SQLITE_TEXT || c.type == SQLITE_BLOB) {
                need += 5 + c.n;
            } else {
                need += 1;
            }
        }
        if (maxBytes_ && emitted_ + buf_.size() + need > maxBytes_) {
            *msg = "byte-cap: result exceeds the byte limit";
            return P4_E_BUDGET;
        }
        if (raw_) {
            for (const Cell& c : cells) rb1::rawFrame(c.p, c.n, &buf_);
        } else {
            enc_.beginRow();
            for (const Cell& c : cells) {
                switch (c.type) {
                    case SQLITE_INTEGER: enc_.i64(c.i); break;
                    case SQLITE_FLOAT: enc_.real(c.d); break;
                    case SQLITE_TEXT: enc_.text(c.p, c.n); break;
                    case SQLITE_BLOB: enc_.blob(c.p, c.n); break;
                    default: enc_.null(); break;
                }
            }
            enc_.endRow();
        }
        rows_++;
        // Emit whole blocks only: an open block's header is patched when it closes.
        if (buf_.size() >= rb1::kBlockTarget && enc_.blockBytes() == 0) return flush(msg);
        return P4_OK;
    }

    // Closes the output: the RB1 end marker (status included) and the rest.
    int32_t finish(int32_t status, const Stmt& st, std::string* msg) {
        if (!raw_) enc_.end(status, rows_, st.rowsExamined, st.bytesRead);
        return flush(msg);
    }

private:
    int32_t flush(std::string* msg) {
        if (buf_.empty()) return P4_OK;
        int32_t rc;
        {
            EngineCall ec;
            rc = p4_emit(ls_->lane, buf_.data(), uint32_t(buf_.size()));
        }
        emitted_ += buf_.size();
        buf_.clear();
        if (rc < 0 && msg->empty())
            *msg = rc == P4_E_CANCELLED ? "cancelled" : rc == P4_E_BUDGET ? "byte-cap: result exceeds the byte limit" : "emit failed";
        return rc;
    }

    LaneState* ls_;
    bool raw_;
    uint64_t maxRows_, maxBytes_;
    std::vector<uint8_t> buf_;
    rb1::Encoder enc_;
    uint64_t rows_ = 0;
    uint64_t emitted_ = 0;
};

int32_t bindParams(sqlite3* db, sqlite3_stmt* s, const std::vector<rb1::Cell>& params, std::string* msg) {
    const int np = sqlite3_bind_parameter_count(s);
    if (np != int(params.size())) {
        *msg = "parameter count mismatch: statement expects " + std::to_string(np);
        return P4_E_SQL;
    }
    for (size_t i = 0; i < params.size(); i++) {
        const rb1::Cell& c = params[i];
        const int idx = int(i + 1);
        int rc;
        switch (c.type) {
            case rb1::kInt: rc = sqlite3_bind_int64(s, idx, c.i); break;
            case rb1::kReal: rc = sqlite3_bind_double(s, idx, c.d); break;
            case rb1::kText: rc = sqlite3_bind_text(s, idx, c.s.data(), int(c.s.size()), SQLITE_TRANSIENT); break;
            case rb1::kBlob: rc = sqlite3_bind_blob(s, idx, c.s.data(), int(c.s.size()), SQLITE_TRANSIENT); break;
            default: rc = sqlite3_bind_null(s, idx); break;
        }
        if (rc != SQLITE_OK) {
            *msg = sqlite3_errmsg(db);
            return rc == SQLITE_NOMEM ? P4_E_NOMEM : P4_E_SQL;
        }
    }
    return P4_OK;
}

// Runs a prepared statement into `out`. Returns the status.
int32_t run(LaneState* ls, Stmt& st, sqlite3_stmt* s, Out& out, std::string* msg) {
    const int ncols = sqlite3_column_count(s);
    std::vector<Cell> cells;
    for (;;) {
        const int rc = sqlite3_step(s);
        if (rc == SQLITE_DONE) {
            if (st.status) *msg = st.message;
            return st.status;
        }
        if (rc != SQLITE_ROW) {
            if (st.status) {
                *msg = st.message;
                return st.status;
            }
            if (rc == SQLITE_NOMEM) {
                const int32_t status = nomemStatus(st);
                *msg = status == P4_E_BUDGET ? kHeapCapMessage : "out of memory";
                return status;
            }
            *msg = sqlite3_errmsg(ls->db);
            return rc == SQLITE_INTERRUPT ? P4_E_CANCELLED : P4_E_SQL;
        }
        if (st.status) {   // raised inside a vtab without failing the step
            *msg = st.message;
            return st.status;
        }
        int32_t check;
        {
            EngineCall ec;
            check = p4_lane_check(ls->lane);   // cancel is honoured between rows
        }
        if (check < 0) {
            *msg = check == P4_E_CANCELLED ? "cancelled" : "work budget exhausted";
            return check;
        }
        cellsOf(s, ncols, &cells);
        const int32_t w = out.row(cells, msg);
        if (w < 0) return w;
    }
}

// SURFACE (op 31): one row per (relation, column), relations in type name
// order, each type's <TYPE> then its "<TYPE>@<source>" relations by source.
int32_t surfaceRows(LaneState* ls, Out& out, std::string* msg) {
    int32_t rc = loadTypes(ls, msg);
    if (rc < 0) return rc;
    std::vector<const TypeEntry*> types;
    for (const auto& kv : ls->types) types.push_back(&kv.second);
    std::sort(types.begin(), types.end(), [](const TypeEntry* a, const TypeEntry* b) { return a->name < b->name; });
    static const char* const kMetaNames[] = {"_source", "_rowid", "_offset", "_data"};
    const std::string table = "table", view = "view", none;
    std::vector<Cell> cells(6);
    for (const TypeEntry* t : types) {
        std::vector<std::string> srcs;
        rc = sourcesOf(ls, t->name, &srcs);
        if (rc < 0) {
            *msg = "sources unavailable";
            return rc;
        }
        auto relation = [&](const std::string& name, const std::string& kind, const std::string& source) -> int32_t {
            const size_t n = t->cols.size() + 4;
            for (size_t i = 0; i < n; i++) {
                const std::string col = i < t->cols.size() ? t->cols[i].name : kMetaNames[i - t->cols.size()];
                cells[0] = textCell(name);
                cells[1] = textCell(kind);
                cells[2] = textCell(source);
                cells[3] = textCell(col);
                cells[4] = intCell(i < t->cols.size() && t->cols[i].placeholder ? 1 : 0);
                cells[5] = intCell(int64_t(t->bound));
                const int32_t w = out.row(cells, msg);
                if (w < 0) return w;
            }
            return P4_OK;
        };
        rc = relation(t->name, srcs.empty() ? table : view, none);
        for (size_t i = 0; rc == P4_OK && i < srcs.size(); i++) rc = relation(t->name + "@" + srcs[i], table, srcs[i]);
        if (rc < 0) return rc;
    }
    return P4_OK;
}

}  // namespace

}  // namespace p4sql
}  // namespace flatsql

using namespace flatsql::p4sql;

extern "C" int32_t p4sql_global_init(void) { return heapInstall(); }

extern "C" int32_t p4sql_lane_init(P4Lane* lane) {
    std::unique_ptr<LaneState> ls(new LaneState());
    ls->lane = lane;
    const int rc = sqlite3_open_v2(":memory:", &ls->db,
                                   SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY | SQLITE_OPEN_NOMUTEX,
                                   nullptr);
    if (rc != SQLITE_OK) {
        if (ls->db) sqlite3_close_v2(ls->db);
        return rc == SQLITE_NOMEM ? P4_E_NOMEM : P4_E_SQL;
    }
    sqlite3_exec(ls->db, "PRAGMA temp_store=MEMORY", nullptr, nullptr, nullptr);
    sqlite3_limit(ls->db, SQLITE_LIMIT_ATTACHED, 0);
    if (registerModule(ls.get()) != SQLITE_OK) {
        sqlite3_close_v2(ls->db);
        return P4_E_SQL;
    }
    sqlite3_progress_handler(ls->db, 4096, progress, ls.get());
    {
        EngineCall ec;
        p4_lane_set_sql_state(lane, ls.get());
    }
    ls.release();
    return P4_OK;
}

extern "C" int32_t p4sql_exec(P4Lane* lane, const P4SqlRequest* req) {
    LaneState* ls = stateOf(lane);
    if (!ls || !req) return P4_E_INTERNAL;
    Stmt st;
    st.sandbox = (req->flags & P4_SLOT_SANDBOX) != 0;
    Out out(ls, (req->flags & P4_SLOT_RAW) != 0, req->maxResultRows, req->maxResultBytes);
    std::string msg;
    int32_t status = P4_OK;
    {
        SandboxScope scope(ls, &st);
        ls->cur = &st;
        sqlite3_stmt* s = nullptr;
        std::vector<rb1::Cell> params;
        if (!rb1::decodeParams(req->params, req->paramsLen, &params)) {
            status = P4_E_ARG;
            msg = "malformed parameters";
        }
        if (status == P4_OK) status = prepare(ls, st, req->sql ? req->sql : "", req->sql ? req->sqlLen : 0, &s, &msg);
        if (status == P4_OK) status = bindParams(ls->db, s, params, &msg);
        std::vector<std::string> names;
        if (s) {
            const int ncols = sqlite3_column_count(s);
            for (int i = 0; i < ncols; i++) {
                const char* n = sqlite3_column_name(s, i);
                names.emplace_back(n ? n : "");
            }
        }
        out.header(names);   // an op that fails before its first row still writes its header
        if (status == P4_OK) status = run(ls, st, s, out, &msg);
        if (s) sqlite3_finalize(s);   // releases the reader cursors and the statement's arena memory now
        ls->cur = nullptr;
    }
    const int32_t emitted = out.finish(status, st, &msg);
    ls->lastError = msg;
    return status != P4_OK ? status : emitted;
}

extern "C" int32_t p4sql_surface(P4Lane* lane) {
    LaneState* ls = stateOf(lane);
    if (!ls) return P4_E_INTERNAL;
    Stmt st;
    Out out(ls, false, 0, 0);
    out.header({"name", "kind", "source", "column", "placeholder", "bound"});
    std::string msg;
    const int32_t status = surfaceRows(ls, out, &msg);
    const int32_t emitted = out.finish(status, st, &msg);
    ls->lastError = msg;
    return status != P4_OK ? status : emitted;
}

extern "C" void p4sql_lane_free(P4Lane* lane) {
    LaneState* ls = stateOf(lane);
    if (!ls) return;
    sqlite3_close_v2(ls->db);   // frees the connection's arena blocks too (found by address)
    ls->db = nullptr;
    {
        EngineCall ec;
        p4_lane_set_sql_state(lane, nullptr);
    }
    if (ls->arena) {
        // Every block is freed with the connection. Should one still be
        // live, the region stays mapped (and findable) rather than dangle.
        if (ls->arena->used() == 0) {
            arenaUnregister(ls->arena);
            delete ls->arena;
        }
    }
    delete ls;
}
