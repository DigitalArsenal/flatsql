// FlatSQL partition store: format levels (terabyte design §3, A-TB2).
//
// fsql2/STORE's `format` field is the store's level. Every task that adds an
// on-disk structure an older engine could misread raises kFormatMax by one and
// writes that structure only at that level, because older engines skip
// unknown ctl kinds at replay, never check L1Header.ver, and never read
// STORE.flags. Each level task claims this header, so levels land in order.
//
// An engine opens a store whose level is in [kFormatMin, kFormatMax] and
// refuses a higher one without creating or writing any file. At open it
// ratchets STORE.format up to its writeFormat (default kFormatMax), but only
// once registry.fsl is non-empty: an empty store may still be recreated by an
// older engine (open.cpp). A fresh store is created at writeFormat. Nothing of
// a level is written before the ratchet to it is durable. The ratchet has no
// downgrade: rollback after it restores a snapshot or re-syncs (the ratchet
// is durable before the open reads a partition, so a first open that then
// fails for any other reason also leaves the store at the new level).
//
// HeadPrefix.format stays kFormat (2) for as long as SDN parses heads in Go.
//
// Levels:
//   2  the as-built format 2 (T1-T3b, §37-§40).
//   3  TB03 (PARTITION-STORE.md §41): batches carry only the lanes they changed;
//      the lane table lives in a paged checkpoint p/<pid>/lk-<gen>.fsl that
//      the head names with a replay offset (LaneRef), the RETIRE letter 'L',
//      the LANE_REF ctl kind; manifest version 3 is the written version.
#ifndef FLATSQL_PS_FORMAT_LEVEL_H
#define FLATSQL_PS_FORMAT_LEVEL_H

#include <cstdint>

namespace flatsql {
namespace ps {

constexpr uint16_t kFormatMin = 2;       // the oldest STORE.format this engine opens
constexpr uint16_t kFormatMax = 3;       // the newest (this engine's level)
constexpr uint16_t kLevelLaneCkpt = 3;   // TB03: paged lane checkpoints, manifest v3 written
// Writer stats entry 38 (the level an open raised STORE from) when the open
// finished a ratchet whose STORE a crash tore: the old level is unknown.
constexpr uint16_t kRatchetFromTorn = 1;

inline bool formatAccepted(uint32_t format, uint32_t formatMax = kFormatMax) {
    return format >= kFormatMin && format <= formatMax;
}

// The level a store is written at: EngineConfig::writeFormat (writer TLV 31),
// 0 meaning this engine's kFormatMax; clamped to what the engine can write.
inline uint16_t effectiveWriteFormat(uint32_t configured, uint32_t formatMax = kFormatMax) {
    if (configured == 0 || configured > formatMax) return uint16_t(formatMax);
    if (configured < kFormatMin) return kFormatMin;
    return uint16_t(configured);
}

}  // namespace ps
}  // namespace flatsql

#endif
