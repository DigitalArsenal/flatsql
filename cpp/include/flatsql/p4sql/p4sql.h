// FlatSQL store format 4: the SQL surface (CONTRACT.md §3.9; design §6).
//
// Owned by the SQL surface package (cpp/src/p4sql); called by the engine.
// The relations are virtual tables over the format-4 reader
// (flatsql/p4/p4_reader.h), each read through one reader cursor with the
// type's A18 bound N (C-31):
//   <TYPE>             kind 'view':  the type's newest N records, one row each;
//                                    _source is "<TYPE>@" + the source of the
//                                    record's earliest live tag, or
//                                    "<TYPE>@local" when it has none (format 1
//                                    keeps a record in its first source's
//                                    partition, untagged ones in "local")
//   "<TYPE>@<source>"  kind 'table': the newest N records of the type from
//                                    that source
// Columns are format 1's: the record fields its engine declares for the
// type, then _source, _rowid, _offset, _data (C-9), plus the hidden
// _seq, _cid, _ts, _epoch, _producer, _peer.
#ifndef FLATSQL_P4SQL_H
#define FLATSQL_P4SQL_H

#include "flatsql/p4/p4_reader.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct P4SqlRequest {
    const char* sql; uint32_t sqlLen;
    const uint8_t* params; uint32_t paramsLen;   /* RB1 params: u32 n, then n cells */
    uint32_t flags;                              /* P4_SLOT_RAW | P4_SLOT_SANDBOX */
    uint64_t maxResultRows, maxResultBytes;      /* 0 = none */
} P4SqlRequest;
int32_t p4sql_global_init(void);                      /* in flatsql_p4_init, before sqlite3_initialize: installs the per-lane allocators */
int32_t p4sql_lane_init(P4Lane* lane);                /* on the lane thread at lane start: SQL connection + vtab modules */
int32_t p4sql_exec(P4Lane* lane, const P4SqlRequest* req);   /* op 30: emits the RB1 header and whole blocks (the ENGINE appends RB1E, C-28) or raw frames; returns the status */
int32_t p4sql_surface(P4Lane* lane);                  /* op 31: emits the SURFACE rows */
void    p4sql_lane_free(P4Lane* lane);
uint64_t p4sql_heap_used(void);                      /* v9, C-30: bytes live in the SQLITE_CONFIG_MALLOC allocator (stat 29) */
uint64_t p4sql_heap_peak(void);                      /* v9, C-30: high-water mark (stat 30) */

#ifdef __cplusplus
}
#endif

#endif
