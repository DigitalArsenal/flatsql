// The engine's view of the SQL surface hooks (CONTRACT.md §3.9).
// flatsql/p4sql/p4sql.h is owned by the SQL surface package. Until it is in
// the tree (the engine lands first), the contract's exact declarations are
// used; once it lands, this header includes it and the fallback goes.
#ifndef FLATSQL_P4_SQL_BRIDGE_H
#define FLATSQL_P4_SQL_BRIDGE_H

#include "flatsql/p4/flatsql_p4.h"
#include "flatsql/p4/p4_reader.h"

#if __has_include("flatsql/p4sql/p4sql.h")
#include "flatsql/p4sql/p4sql.h"
#else
#ifdef __cplusplus
extern "C" {
#endif
typedef struct P4SqlRequest {
    const char* sql; uint32_t sqlLen;
    const uint8_t* params; uint32_t paramsLen;   /* RB1 params: u32 n, then n cells */
    uint32_t flags;                              /* P4_SLOT_RAW | P4_SLOT_SANDBOX */
    uint64_t maxResultRows, maxResultBytes;      /* 0 = none */
} P4SqlRequest;
int32_t p4sql_global_init(void);
int32_t p4sql_lane_init(P4Lane* lane);
int32_t p4sql_exec(P4Lane* lane, const P4SqlRequest* req);
int32_t p4sql_surface(P4Lane* lane);
void    p4sql_lane_free(P4Lane* lane);
#ifdef __cplusplus
}
#endif
#endif

#endif
