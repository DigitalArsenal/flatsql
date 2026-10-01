/* FlatSQL store format 4 ("p4"): one SQLite file per partition x UTC content
 * month. The instance C ABI (CONTRACT.md §3.2-§3.4, contract version 1).
 *
 * i32 and f64 only on the export boundary; everything else is bytes in shared
 * memory. Requests are TLV ([u16 tag][u32 len][value], little-endian,
 * byte-packed), responses are RB1 (flatsql/ps/result_block.h). Go never calls
 * a guest export on the hot path: it claims a mailbox slot, writes the request,
 * enqueues the slot index, rings a doorbell and reads the slot's ring.
 * Errors are return values, never traps; a miss and an error are distinct.
 */
#ifndef FLATSQL_P4_H
#define FLATSQL_P4_H

#include <stdint.h>

#if defined(__wasm__)
#  define FLATSQL_P4_EXPORT(name) __attribute__((export_name(name)))
#else
#  define FLATSQL_P4_EXPORT(name)
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* ---- §3.2 Status and reject codes ------------------------------------- */

#define P4_OK               0
#define P4_E_ARG           -1   /* malformed request or config */
#define P4_E_NOMEM         -2
#define P4_E_IO            -3
#define P4_E_CORRUPT       -4   /* the file is quarantined, never rewritten; reads elsewhere continue */
#define P4_E_FORMAT        -5   /* store/file format or layout above this engine; marker/state mismatch; C-5 rule change */
#define P4_E_BUSY          -6   /* a queue or partition credit is full: nothing was done, retry; reader paths never wait */
#define P4_E_CANCELLED     -7
#define P4_E_BUDGET        -8   /* rows-examined, bytes-read, result rows/bytes, work or heap cap */
#define P4_E_UNSUPPORTED   -9
#define P4_E_NOTYPE       -10   /* type not registered */
#define P4_E_STOPPED      -11   /* stop word set, or the instance is stopping */
#define P4_E_NOSPACE      -12   /* quota or device full: writes refuse, reads continue */
#define P4_E_SQL          -13   /* SQL error; the text is in the slot's err */
#define P4_E_INTERNAL     -14

/* Per-record rejects (PUT rows, `reject` column). -100..-109 are
 * ps::RejectCode from flatsql/typecfg/extract.h, unchanged. */
#define P4_REJ_BAD_ENTRY   -100
#define P4_REJ_FRAME_SIZE  -101
#define P4_REJ_FID         -102
#define P4_REJ_VERIFY      -103
#define P4_REJ_CID         -104
#define P4_REJ_SEALED      -105
#define P4_REJ_ATTR        -106
#define P4_REJ_QUARANTINED -107
#define P4_REJ_NO_TYPE     -108
#define P4_REJ_TXN_TOO_LARGE -109
#define P4_REJ_CID_FORM    -110 /* the CID is not a CIDv1 raw sha2-256 (bafkrei...) */
#define P4_REJ_SEQ         -111 /* a migrate-mode seq is missing, <= 0, or held by a different CID */
#define P4_REJ_TOO_LARGE   -112 /* above the type's max frame, or above the write pool's request area (C-6) */
#define P4_REJ_TAG         -113 /* a tag without provider or source, a tag index out of range, or a record
                                   PEER whose token differs from the call peer's */

/* ---- §3.5 Ops, classes, slot states -------------------------------------- */

enum {
    P4_OPC_PUT = 1, P4_OPC_SUPERSEDE = 2, P4_OPC_DELETE = 3, P4_OPC_QUOTA_GC = 4, P4_OPC_REBUILD = 5,
    P4_OPC_GET = 10, P4_OPC_TAGS = 11, P4_OPC_SCAN = 12, P4_OPC_HEAD = 13, P4_OPC_WINDOW = 14,
    P4_OPC_INDEX_PAGE = 15, P4_OPC_EPOCH = 16, P4_OPC_SUMMARY = 17,
    P4_OPC_SQL = 30, P4_OPC_SURFACE = 31
};
enum { P4_CLASS_WRITE = 1, P4_CLASS_INTERACTIVE = 2, P4_CLASS_BULK = 3, P4_CLASS_SANDBOX = 4,
       P4_CLASS_MAINTENANCE = 5 };
enum { P4_SLOT_FREE = 0, P4_SLOT_CLAIMED = 1, P4_SLOT_QUEUED = 2, P4_SLOT_RUNNING = 3, P4_SLOT_DONE = 5 };
enum { P4_ACT_REJECTED = 0, P4_ACT_NEW = 1, P4_ACT_COPY = 2, P4_ACT_RETAG = 3, P4_ACT_DUP = 4,
       P4_ACT_IDENT_DUP = 5, P4_ACT_MIGRATED = 6 };

/* ---- §3.4 The mailbox ------------------------------------------------------ */

#define FLATSQL_P4_LAYOUT_VERSION 1
#define FLATSQL_P4_SLOT_HEADER 448
typedef struct FlatsqlP4Layout {
    uint32_t version;          /*   0: 1 */
    uint32_t size;             /*   4: 640 */
    uint32_t headerSize;       /*   8: 448 */
    uint32_t stopWord;         /*  12: address of the u32 stop word */
    uint32_t nSlots[2];        /*  16: [0] write pool, [1] read pool */
    uint32_t slotBase[2];      /*  24 */
    uint32_t slotStride[2];    /*  32: headerSize + reqBytes + ringBytes, 64-aligned */
    uint32_t reqBytes[2];      /*  40 */
    uint32_t ringBytes[2];     /*  48: powers of two */
    uint32_t queueCells[4];    /*  56: per class, index = class-1 */
    uint32_t queueMask[4];     /*  72 */
    uint32_t queueEnq[4];      /*  88: address of the u64 enqueue counter */
    uint32_t queueDeq[4];      /* 104: address of the u64 dequeue counter */
    uint32_t nThreads;         /* 120: service threads (writers, lanes, maintenance), <= 64 */
    uint32_t rsv;              /* 124: 0 */
    uint32_t threadClass[64];  /* 128: 1 write, 2 interactive, 3 bulk, 4 sandbox, 5 maintenance */
    uint32_t doorbell[64];     /* 384: address of the thread's doorbell u32; its state u32 is at doorbell+4
                                        (0 idle, 1 busy, 2 stopped) */
} FlatsqlP4Layout;

/* Slot header byte offsets (version 1). */
enum {
    P4_SLOT_OFF_STATE = 0, P4_SLOT_OFF_CANCEL = 4, P4_SLOT_OFF_OUTSEQ = 8, P4_SLOT_OFF_SPACESEQ = 12,
    P4_SLOT_OFF_OP = 16, P4_SLOT_OFF_CLASS = 20, P4_SLOT_OFF_FLAGS = 24, P4_SLOT_OFF_REQLEN = 28,
    P4_SLOT_OFF_REQID = 32, P4_SLOT_OFF_MAX_ROWS_EXAMINED = 40, P4_SLOT_OFF_MAX_BYTES_READ = 48,
    P4_SLOT_OFF_MAX_RESULT_ROWS = 56, P4_SLOT_OFF_MAX_RESULT_BYTES = 64, P4_SLOT_OFF_RING_HEAD = 72,
    P4_SLOT_OFF_RING_TAIL = 80, P4_SLOT_OFF_STATUS = 88, P4_SLOT_OFF_ERRLEN = 92, P4_SLOT_OFF_ROWS_OUT = 96,
    P4_SLOT_OFF_ROWS_EXAMINED = 104, P4_SLOT_OFF_BYTES_READ = 112, P4_SLOT_OFF_SUBMIT_NS = 120,
    P4_SLOT_OFF_START_NS = 128, P4_SLOT_OFF_END_NS = 136, P4_SLOT_OFF_ERR = 144, P4_SLOT_ERR_CAP = 256,
    P4_SLOT_OFF_THREAD = 400
};

/* ---- §3.3 Exports (exact) ------------------------------------------------ */

FLATSQL_P4_EXPORT("flatsql_p4_init")          int32_t flatsql_p4_init(const uint8_t* cfg, int32_t cfgLen);
FLATSQL_P4_EXPORT("flatsql_p4_start")         int32_t flatsql_p4_start(void);      /* >= 0: threads started */
FLATSQL_P4_EXPORT("flatsql_p4_layout")        int32_t flatsql_p4_layout(FlatsqlP4Layout* out); /* bytes written = 640 */
FLATSQL_P4_EXPORT("flatsql_p4_wake")          int32_t flatsql_p4_wake(uint32_t* addr, int32_t n); /* memory.atomic.notify; woken count */
FLATSQL_P4_EXPORT("flatsql_p4_register_type") int32_t flatsql_p4_register_type(const uint8_t* spec, int32_t specLen);
FLATSQL_P4_EXPORT("flatsql_p4_set_quota")     int32_t flatsql_p4_set_quota(double bytes);   /* 0 = none */
FLATSQL_P4_EXPORT("flatsql_p4_activate")      int32_t flatsql_p4_activate(void);            /* create mode 2 only */
FLATSQL_P4_EXPORT("flatsql_p4_stats")         int32_t flatsql_p4_stats(uint8_t* out, int32_t cap); /* bytes written; u64 LE array §3.10 */
FLATSQL_P4_EXPORT("flatsql_p4_stop")          int32_t flatsql_p4_stop(double deadlineMs);   /* P4_OK, or P4_E_BUSY at the deadline */
FLATSQL_P4_EXPORT("flatsql_p4_alloc")         void*   flatsql_p4_alloc(int32_t n);          /* wasm only */
FLATSQL_P4_EXPORT("flatsql_p4_free")          void    flatsql_p4_free(void* p);             /* wasm only */

#ifdef __cplusplus
}
#endif

#endif
