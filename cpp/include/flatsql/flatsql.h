/* FlatSQL: one engine and one ABI for binary records and SQL.
 * SQL input is SDS QRP; all projected results are size-prefixed SDS QRP.
 * Record streams contain unchanged size-prefixed FlatBuffers. Configuration,
 * registration and storage metadata use the documented packed control layout;
 * they never parse or render record text. Mailboxes keep queries off the host
 * executor thread and retain independent cancellation and resource budgets.
 */
#ifndef FLATSQL_H
#define FLATSQL_H

#include <stdint.h>

#if defined(__wasm__)
#  define FLATSQL_EXPORT(name) __attribute__((export_name(name)))
#else
#  define FLATSQL_EXPORT(name)
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* ---- §3.2 Status and reject codes ------------------------------------- */

#define FLATSQL_OK               0
#define FLATSQL_E_ARG           -1   /* malformed request or config */
#define FLATSQL_E_NOMEM         -2
#define FLATSQL_E_IO            -3
#define FLATSQL_E_CORRUPT       -4   /* the file is quarantined, never rewritten; reads elsewhere continue */
#define FLATSQL_E_FORMAT        -5   /* unsupported store/file format or layout; marker/state mismatch; C-5 rule change */
#define FLATSQL_E_BUSY          -6   /* a queue or partition credit is full: nothing was done, retry; reader paths never wait */
#define FLATSQL_E_CANCELLED     -7
#define FLATSQL_E_BUDGET        -8   /* rows-examined, bytes-read, result rows/bytes, work or heap cap */
#define FLATSQL_E_UNSUPPORTED   -9
#define FLATSQL_E_NOTYPE       -10   /* type not registered */
#define FLATSQL_E_STOPPED      -11   /* stop word set, or the instance is stopping */
#define FLATSQL_E_NOSPACE      -12   /* quota or device full: writes refuse, reads continue */
#define FLATSQL_E_SQL          -13   /* SQL error; the text is in the slot's err */
#define FLATSQL_E_INTERNAL     -14

/* Per-record rejects (PUT rows, `reject` column). -100..-109 are
 * ps::RejectCode from flatsql/typecfg/extract.h, unchanged. */
#define FLATSQL_REJ_BAD_ENTRY   -100
#define FLATSQL_REJ_FRAME_SIZE  -101
#define FLATSQL_REJ_FID         -102
#define FLATSQL_REJ_VERIFY      -103
#define FLATSQL_REJ_CID         -104
#define FLATSQL_REJ_SEALED      -105
#define FLATSQL_REJ_ATTR        -106
#define FLATSQL_REJ_QUARANTINED -107
#define FLATSQL_REJ_NO_TYPE     -108
#define FLATSQL_REJ_TXN_TOO_LARGE -109
#define FLATSQL_REJ_CID_FORM    -110 /* the CID is not a CIDv1 raw sha2-256 (bafkrei...) */
#define FLATSQL_REJ_SEQ         -111 /* a migrate-mode seq is missing, <= 0, or held by a different CID */
#define FLATSQL_REJ_TOO_LARGE   -112 /* above the type's max frame, or above the write pool's request area (C-6) */
#define FLATSQL_REJ_TAG         -113 /* a tag without provider or source, a tag index out of range, or a record
                                   PEER whose token differs from the call peer's */

/* ---- §3.5 Ops, classes, slot states -------------------------------------- */

enum {
    FLATSQL_OPC_PUT = 1, FLATSQL_OPC_SUPERSEDE = 2, FLATSQL_OPC_DELETE = 3, FLATSQL_OPC_QUOTA_GC = 4, FLATSQL_OPC_REBUILD = 5,
    FLATSQL_OPC_GET = 10, FLATSQL_OPC_TAGS = 11, FLATSQL_OPC_SCAN = 12, FLATSQL_OPC_HEAD = 13, FLATSQL_OPC_WINDOW = 14,
    FLATSQL_OPC_INDEX_PAGE = 15, FLATSQL_OPC_EPOCH = 16, FLATSQL_OPC_SUMMARY = 17,
    FLATSQL_OPC_RANGES = 18, FLATSQL_OPC_FEED_ROOT = 19, FLATSQL_OPC_DROPS = 20,
    FLATSQL_OPC_EXPOSURE = 21, FLATSQL_OPC_RELEASE = 22,
    FLATSQL_OPC_SQL = 30, FLATSQL_OPC_SURFACE = 31
};
/* IPFS ops: tag 1 type; tags 3/4 provider/source (both omitted = every feed,
 * both empty = local). Tag 80 u64: inclusive from-commit for RANGES/DROPS,
 * through-commit for RELEASE. Tag 81 u8: EXPOSURE (0 default, 1 on).
 * Tag 82 u32: RELEASE generation (default current); offsets are generation-local.
 * RELEASE of a generation older than the current one releases that whole
 * (immutable) generation; tag 80 is ignored. RELEASE of the current generation
 * releases every older one once tag 80 reaches its compaction checkpoint (the
 * first FCK after genesis), and is a no-op before that.
 * RANGES: path,offset,length,cid,kind,unit,block,provider,source,generation.
 * Paths are relative to config root; inline block bytes only for dag-pb nodes.
 * LINK unit is its publication/change commit, including rewrites of old units.
 * FEED_ROOT: provider,source,cid,unit,generation,blocked_generations.
 * DROPS: cid,unit,provider,source,generation. Includes removed records, large
 * roots/leaves and obsolete raw metadata. Derived LINK blocks are reclaimed
 * by pin-update/GC. Unit names the removal or replacement checkpoint.
 * RANGES is the live closure in the current generation, filtered by unit.
 * Reset the cursor when FEED_ROOT reports a changed generation.
 * Exposure is runtime policy: the node must reapply it after opening a store
 * before the 30-second retired-reader window expires.
 * EXPOSURE/RELEASE return no rows. These run on a read lane; exposure/release
 * change only the node's generation-release bookkeeping, never stream bytes.
 */
enum { FLATSQL_RANGE_RECORD=1, FLATSQL_RANGE_LARGE_LEAF=2, FLATSQL_RANGE_LARGE_ROOT=3,
       FLATSQL_RANGE_FMB=4, FLATSQL_RANGE_FCK=5, FLATSQL_RANGE_FSG=6, FLATSQL_RANGE_LINK=7 };
enum { FLATSQL_CLASS_WRITE = 1, FLATSQL_CLASS_INTERACTIVE = 2, FLATSQL_CLASS_BULK = 3, FLATSQL_CLASS_SANDBOX = 4,
       FLATSQL_CLASS_MAINTENANCE = 5 };
enum { FLATSQL_SLOT_FREE = 0, FLATSQL_SLOT_CLAIMED = 1, FLATSQL_SLOT_QUEUED = 2, FLATSQL_SLOT_RUNNING = 3, FLATSQL_SLOT_DONE = 5 };
enum { FLATSQL_ACT_REJECTED = 0, FLATSQL_ACT_NEW = 1, FLATSQL_ACT_COPY = 2, FLATSQL_ACT_RETAG = 3, FLATSQL_ACT_DUP = 4,
       FLATSQL_ACT_IDENT_DUP = 5, FLATSQL_ACT_MIGRATED = 6 };

/* ---- §3.4 The mailbox ------------------------------------------------------ */

#define FLATSQL_LAYOUT_VERSION 1
#define FLATSQL_SLOT_HEADER 448
typedef struct FlatSQLLayout {
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
} FlatSQLLayout;

/* Slot header byte offsets (version 1). */
enum {
    FLATSQL_SLOT_OFF_STATE = 0, FLATSQL_SLOT_OFF_CANCEL = 4, FLATSQL_SLOT_OFF_OUTSEQ = 8, FLATSQL_SLOT_OFF_SPACESEQ = 12,
    FLATSQL_SLOT_OFF_OP = 16, FLATSQL_SLOT_OFF_CLASS = 20, FLATSQL_SLOT_OFF_FLAGS = 24, FLATSQL_SLOT_OFF_REQLEN = 28,
    FLATSQL_SLOT_OFF_REQID = 32, FLATSQL_SLOT_OFF_MAX_ROWS_EXAMINED = 40, FLATSQL_SLOT_OFF_MAX_BYTES_READ = 48,
    FLATSQL_SLOT_OFF_MAX_RESULT_ROWS = 56, FLATSQL_SLOT_OFF_MAX_RESULT_BYTES = 64, FLATSQL_SLOT_OFF_RING_HEAD = 72,
    FLATSQL_SLOT_OFF_RING_TAIL = 80, FLATSQL_SLOT_OFF_STATUS = 88, FLATSQL_SLOT_OFF_ERRLEN = 92, FLATSQL_SLOT_OFF_ROWS_OUT = 96,
    FLATSQL_SLOT_OFF_ROWS_EXAMINED = 104, FLATSQL_SLOT_OFF_BYTES_READ = 112, FLATSQL_SLOT_OFF_SUBMIT_NS = 120,
    FLATSQL_SLOT_OFF_START_NS = 128, FLATSQL_SLOT_OFF_END_NS = 136, FLATSQL_SLOT_OFF_ERR = 144, FLATSQL_SLOT_ERR_CAP = 256,
    FLATSQL_SLOT_OFF_THREAD = 400
};

/* ---- §3.3 Exports (exact) ------------------------------------------------ */

FLATSQL_EXPORT("flatsql_init")          int32_t flatsql_init(const uint8_t* cfg, int32_t cfgLen);
FLATSQL_EXPORT("flatsql_start")         int32_t flatsql_start(void);      /* >= 0: threads started */
FLATSQL_EXPORT("flatsql_layout")        int32_t flatsql_layout(FlatSQLLayout* out); /* bytes written = 640 */
FLATSQL_EXPORT("flatsql_wake")          int32_t flatsql_wake(uint32_t* addr, int32_t n); /* memory.atomic.notify; woken count */
FLATSQL_EXPORT("flatsql_register_type") int32_t flatsql_register_type(const uint8_t* spec, int32_t specLen);
FLATSQL_EXPORT("flatsql_set_quota")     int32_t flatsql_set_quota(double bytes);   /* 0 = none */
FLATSQL_EXPORT("flatsql_activate")      int32_t flatsql_activate(void);            /* create mode 2 only */
FLATSQL_EXPORT("flatsql_stats")         int32_t flatsql_stats(uint8_t* out, int32_t cap); /* bytes written; u64 LE array §3.10 */
FLATSQL_EXPORT("flatsql_stop")          int32_t flatsql_stop(double deadlineMs);   /* FLATSQL_OK, or FLATSQL_E_BUSY at the deadline */
FLATSQL_EXPORT("flatsql_alloc")         void*   flatsql_alloc(int32_t n);          /* wasm only */
FLATSQL_EXPORT("flatsql_free")          void    flatsql_free(void* p);             /* wasm only */

/* Prepare an ingest mailbox request from an unchanged stream of size-prefixed
 * records. Metadata is the control TLV (type, producer, source tags, timestamp).
 * CID computation stays in the guest. Returns status and an allocated buffer.
 * Caller frees *output with flatsql_free. */
FLATSQL_EXPORT("flatsql_ingest_request") int32_t flatsql_ingest_request(
    const uint8_t* frames, int32_t size, const uint8_t* metadata, int32_t metadataSize, uint8_t** output, uint32_t* outLen);

#ifdef __cplusplus
}
#endif

#endif
