/* FlatSQL partition store: the instance C ABI (design §6.3).
 *
 * i32 and f64 only; everything else lives in shared memory. One instance per
 * wasm module: the writer instance, or a reader instance (T2: interactive,
 * bulk or sandbox lanes).
 * Go never calls a guest export on a hot path: producers write rings in
 * shared memory, ring doorbells with flatsql_ps_wake, and poll ack words.
 * Registration and ring lookup are control calls on the instance's exec
 * thread.
 *
 * Config (flatsql_ps_init): TLV [u16 tag][u32 len][bytes]:
 *   1 root (utf-8)          2 writers u32        3 sync threads u32
 *   4 pool bytes u64        5 slab bytes u32     6 default ring cap u64
 *   7 cooperative u8        8 create u8          9 require MIGRATED u8
 *  10 zero-fill step u64   11 arena bytes u64   12 seal bytes u64
 */
#ifndef FLATSQL_PS_H
#define FLATSQL_PS_H

#include <stdint.h>

#if defined(__wasm__)
#  define FLATSQL_PS_EXPORT(name) __attribute__((export_name(name)))
#else
#  define FLATSQL_PS_EXPORT(name)
#endif

#ifdef __cplusplus
extern "C" {
#endif

enum {
    FLATSQL_PS_ROLE_WRITER = 1,
    FLATSQL_PS_ROLE_READER_INTERACTIVE = 2,
    FLATSQL_PS_ROLE_READER_BULK = 3,
    FLATSQL_PS_ROLE_READER_SANDBOX = 4,
};

/* Reader config TLV tags (flatsql_ps_init with a reader role):
 *   1 root (utf-8)      20 lanes u32        21 arena bytes u64 (per lane)
 *  22 shared cache u64  23 max parked u32   24 slots u32
 *  25 request bytes u32 26 ring bytes u32   27 lane cache bytes u64
 *  28 stack bytes u64   29 sandbox rows u64 30 sandbox bytes u64
 *
 * The mailbox (ps/lane.h) lives in the instance's memory; the router
 * programs against flatsql_ps_reader_layout:
 *   submit: claim a slot (CAS state FREE->CLAIMED), write SQL then RB1
 *   parameters at slotBase + i*slotStride + headerSize, set the header
 *   fields, store state QUEUED, push the slot index on the MPMC queue
 *   (Vyukov: cell i at queueCells + (pos & queueMask) * 16 holds
 *   {seq u64, value u32}; enqueue CASes the u64 at queueEnq), then bump an
 *   idle lane's doorbell (laneState 0 or 3) and flatsql_ps_wake it;
 *   read: the SPSC ring after the request area (ringHead/ringTail u64);
 *   done: state DONE, then status and counters; free: store state FREE. */
typedef struct FlatsqlPsReaderLayout {
    uint32_t version;
    uint32_t nLanes;
    uint32_t nSlots;
    uint32_t slotBase;
    uint32_t slotStride;
    uint32_t headerSize;
    uint32_t reqBytes;
    uint32_t ringBytes;
    uint32_t offState, offCancel, offOutSeq, offSpaceSeq, offFlags, offLane, offReqId;
    uint32_t offSqlLen, offParamsLen, offReqCap, offRingCap;
    uint32_t offMaxRowsExamined, offMaxBytesRead, offMaxResultRows, offMaxResultBytes;
    uint32_t offRingHead, offRingTail, offStatus, offErrLen, offRowsOut, offRowsExamined;
    uint32_t offBytesRead, offIndexEntries, offFenceReads, offSubmitNs, offStartNs, offEndNs, offErr;
    uint32_t queueCells;
    uint32_t queueMask;
    uint32_t queueEnq;
    uint32_t queueDeq;
    uint32_t stopWord;
    uint32_t laneDoorbell[64];
    uint32_t laneState[64];
    uint32_t laneAnnounce[64];
} FlatsqlPsReaderLayout;

/* Layout block written by flatsql_ps_layout (all u32: addresses/offsets). */
typedef struct FlatsqlPsLayout {
    uint32_t version;
    uint32_t nWriters;
    uint32_t ringDescSize;
    uint32_t offTail, offHead, offAckedRseq, offAckGen, offMapGen, offWantPage;
    uint32_t offProdBusy, offReclaim, offProdWaiting, offState, offOwnerWord, offHandoffTo;
    uint32_t offRejectHead, offRejectTail, offRejects, offPages, offCap, offMaxEntry;
    uint32_t offNSlots, offSlabBytes, offNextRseq, offMappedPages;
    uint32_t poolBase;
    uint32_t slabBytes;
    uint32_t entryHeaderSize;
    uint32_t writerSeq[64];
    uint32_t writerSleeping[64];
} FlatsqlPsLayout;

FLATSQL_PS_EXPORT("flatsql_ps_init")
int32_t flatsql_ps_init(int32_t role, const uint8_t* cfg, int32_t cfgLen);
FLATSQL_PS_EXPORT("flatsql_ps_start")
int32_t flatsql_ps_start(void);
FLATSQL_PS_EXPORT("flatsql_ps_layout")
int32_t flatsql_ps_layout(FlatsqlPsLayout* out);
FLATSQL_PS_EXPORT("flatsql_ps_wake")
int32_t flatsql_ps_wake(uint32_t* addr, int32_t n);
FLATSQL_PS_EXPORT("flatsql_ps_pump")
int32_t flatsql_ps_pump(double budgetUs);
FLATSQL_PS_EXPORT("flatsql_ps_stop")
int32_t flatsql_ps_stop(double deadlineMs);
/* Stats: little-endian u64 array (see capi_ps.cpp for the order). Returns
 * bytes written. */
FLATSQL_PS_EXPORT("flatsql_ps_stats")
int32_t flatsql_ps_stats(uint8_t* out, int32_t len);
/* Control calls. */
FLATSQL_PS_EXPORT("flatsql_ps_reader_layout")
int32_t flatsql_ps_reader_layout(FlatsqlPsReaderLayout* out);
FLATSQL_PS_EXPORT("flatsql_ps_register_type")
int32_t flatsql_ps_register_type(const uint8_t* cfg, int32_t cfgLen);
/* Returns the pid (> 0) or a negative status. */
FLATSQL_PS_EXPORT("flatsql_ps_register_partition")
int32_t flatsql_ps_register_partition(const uint8_t* peer, int32_t peerLen, const uint8_t* fid);
/* Address of a partition's ring descriptor (0 when unknown). */
FLATSQL_PS_EXPORT("flatsql_ps_ring")
double flatsql_ps_ring(int32_t pid);

#ifdef __cplusplus
}
#endif

#endif
