// FlatSQL partition store: admission (design §9 "Admission", A28, §22.4
// ruling 7).
//
// Interactive lanes accept only plans in which every virtual-table cursor is
// index-bounded: xBestIndex records the bit as the first character of the
// plan's idxStr, and admission reads it from the prepared statement's own
// program (sqlite3_stmt_explain), before any row is examined. An unbounded
// plan returns FLATSQL_NEEDS_BULK and the router resubmits it to a bulk lane.
//
// Untrusted SQL (the public sandbox) keeps the SandboxCaps contract: one
// read-only SELECT over the public record tables (the authorizer allow-list
// maps onto the vtab names; flatsql_* tables are never public), and a work
// budget (rows examined, bytes read, VM steps) that is an ADMISSION limit
// returning the existing `timeout` code — it never waits on the data layer.
#include <sqlite3.h>

#include <cctype>
#include <cstring>
#include <string>

#include "admission.h"
#include "flatsql/ps/lane.h"
#include "flatsql/ps/vtab.h"

namespace flatsql {
namespace ps {

int32_t planBounded(sqlite3_stmt* stmt, bool* bounded, std::string* unboundedTable) {
    *bounded = true;
    if (sqlite3_stmt_explain(stmt, 1) != SQLITE_OK) return kRsSqlError;
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char* op = sqlite3_column_text(stmt, 1);
        if (!op || std::strcmp(reinterpret_cast<const char*>(op), "VFilter") != 0) continue;
        const unsigned char* p4 = sqlite3_column_text(stmt, 5);
        if (!p4 || p4[0] != 'B') {
            *bounded = false;
            if (unboundedTable) {
                const unsigned char* cmt = sqlite3_column_text(stmt, 7);
                *unboundedTable = cmt ? reinterpret_cast<const char*>(cmt) : "";
            }
        }
    }
    sqlite3_reset(stmt);
    const int back = sqlite3_stmt_explain(stmt, 0);
    if (rc != SQLITE_DONE || back != SQLITE_OK) return kRsSqlError;
    return 0;
}

// ---- sandbox authorizer ----------------------------------------------------------
namespace {
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

}  // namespace

int sandboxAuthorizer(void* user, int action, const char* a1, const char*, const char*, const char*) {
    SandboxAuth* ctx = static_cast<SandboxAuth*>(user);
    switch (action) {
        case SQLITE_SELECT:
        case SQLITE_FUNCTION:
        case SQLITE_RECURSIVE: return SQLITE_OK;
        case SQLITE_READ: {
            const char* table = a1 ? a1 : "";
            if (std::strncmp(table, "sqlite_", 7) == 0) break;
            if (vtabPublicName(ctx->lane, table)) return SQLITE_OK;
            if (!ctx->lane->hasTable(table) && std::strncmp(table, "flatsql_", 8) != 0) return SQLITE_OK;
            if (ctx->violation.empty())
                ctx->violation = std::string("table \"") + table + "\" is outside the public query surface";
            return SQLITE_DENY;
        }
        default: break;
    }
    if (ctx->violation.empty()) {
        if (action == SQLITE_READ)
            ctx->violation = std::string("table \"") + (a1 ? a1 : "?") + "\" is outside the public query surface";
        else
            ctx->violation = std::string(actionName(action)) + " is not permitted (read-only SELECT sandbox)";
    }
    return SQLITE_DENY;
}

}  // namespace ps
}  // namespace flatsql
