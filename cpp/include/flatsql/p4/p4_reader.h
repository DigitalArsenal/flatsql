#ifndef FLATSQL_P4_READER_H
#define FLATSQL_P4_READER_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
typedef struct P4Engine P4Engine;
typedef struct P4Lane P4Lane;
typedef struct P4Cursor P4Cursor;

enum { P4_ORDER_SEQ_ASC = 1, P4_ORDER_SEQ_DESC = 2, P4_ORDER_W_DESC = 3, P4_ORDER_CID = 4 };
enum { P4_F_EPOCH = 1, P4_F_TS = 2, P4_F_W = 3, P4_F_EPOCH_DAY = 4,
       P4_F_COL0 = 10, P4_F_COL1 = 11, P4_F_COL2 = 12, P4_F_COL3 = 13 };
enum { P4_OP_EQ = 1, P4_OP_NE, P4_OP_LT, P4_OP_LE, P4_OP_GT, P4_OP_GE, P4_OP_BETWEEN, P4_OP_LIKE,
       P4_OP_IN, P4_OP_NOTNULL };
#define P4_SLOT_RAW 1u
#define P4_SLOT_SANDBOX 2u

typedef struct P4Value { uint8_t type; int64_t i; double d; const uint8_t* s; uint32_t n; } P4Value; /* RB1 cell type 0..4 */
typedef struct P4Pred { uint8_t field, op; uint16_t nvals; const P4Value* vals; } P4Pred;
typedef struct P4LaneFilter { const char *provider, *source, *batch, *contentKeyId, *producerPeer, *producerPubkey; } P4LaneFilter; /* NULL = any */
typedef struct P4ScanSpec {
    const char* type;            /* required */
    P4LaneFilter lane;
    const uint8_t* cid;          /* 36-byte binary CID, or NULL */
    const char* peer;            /* NULL = any */
    const char* producer;        /* NULL = any */
    int64_t seqAfter;            /* exclusive; 0 = none */
    int64_t seqThrough;          /* inclusive; 0 = none; always clamped to visible-through */
    const P4Pred* preds; uint32_t nPreds;
    const char* search;          /* FTS5 match; NULL = none */
    uint8_t order;               /* P4_ORDER_* */
    uint8_t hydrate;
    uint8_t rsv[2];
    uint64_t limit, offset;      /* 0 = none */
    uint64_t bound;              /* A18 newest-N seqs OF THE TYPE, applied before every other filter incl. lane (v2, C-17); 0 = none */
} P4ScanSpec;
typedef struct P4Tag { const char *provider, *source, *sourceUrl, *batch, *contentKeyId, *producerPeer, *producerPubkey; int64_t at; } P4Tag;
typedef struct P4Row {
    int64_t seq, ts, epoch;
    uint8_t hasEpoch;
    uint8_t keyType;             /* 0 none, 1 int, 3 text */
    uint8_t rsv[6];
    int64_t keyInt; const uint8_t* keyText; uint32_t keyTextLen;
    const char* cid;             /* base32 text, NUL-terminated */
    const char* producer; const char* peer;
    const uint8_t* sig; uint32_t sigLen;
    const uint8_t* data; uint32_t dataLen;   /* NULL unless hydrate */
    int64_t len;
    const P4Tag* tag;            /* matched tag (§3.6), or NULL */
} P4Row;                         /* every pointer valid until the next p4_cursor_next or p4_cursor_close */
typedef struct P4TypeInfo { const char* name; const uint8_t* bfbs; uint32_t bfbsLen; uint8_t fid[4];
                            uint64_t a18Bound; uint8_t epochProfile; uint8_t fullText;
                            const char* rules; uint32_t rulesLen; } P4TypeInfo;   /* rules: tag-4 text, verbatim (v2, C-16) */

int32_t   p4_cursor_open(P4Lane* lane, const P4ScanSpec* spec, P4Cursor** out);  /* P4_OK or status */
int32_t   p4_cursor_next(P4Cursor* c, P4Row* row);   /* 1 row, 0 end, < 0 status (P4_E_BUDGET, P4_E_CANCELLED, ...) */
void      p4_cursor_close(P4Cursor* c);
int32_t   p4_lane_check(P4Lane* lane);               /* P4_OK, P4_E_CANCELLED or P4_E_BUDGET (progress handler) */
int32_t   p4_emit(P4Lane* lane, const uint8_t* bytes, uint32_t n);   /* append to the slot's ring; may wait for space;
                                                                       P4_OK, P4_E_CANCELLED, P4_E_BUDGET (result bytes) */
P4Engine* p4_lane_engine(P4Lane* lane);
void*     p4_lane_sql_state(P4Lane* lane);
void      p4_lane_set_sql_state(P4Lane* lane, void* state);
int32_t   p4_types(P4Lane* lane, const P4TypeInfo** out, uint32_t* n);   /* registered types; valid until the lane's next call */
int32_t   p4_sources(P4Lane* lane, const char* type, const char* const** out, uint32_t* n); /* sources with >= 1 live tag */
int64_t   p4_visible_through(P4Engine* e, const char* type);
uint64_t  p4_lane_heap_cap(P4Lane* lane);            /* v2, C-18: slot sandbox heap cap (tag 48, else default) */
void      p4_lane_counters(P4Lane* lane, uint64_t* rowsExamined, uint64_t* bytesRead); /* v2, C-18: this op's cursor totals */
void      p4_lane_set_error(P4Lane* lane, const char* msg, uint32_t n);  /* v3, C-19: the running slot's err (§3.4), truncated to 255 bytes */
void      p4_lane_set_rows(P4Lane* lane, uint64_t rows);                 /* v8, C-29: the op's result rows (slot rowsOut, RB1E.rows) */
#ifdef __cplusplus
}
#endif
#endif
