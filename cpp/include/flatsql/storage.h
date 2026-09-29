#ifndef FLATSQL_STORAGE_H
#define FLATSQL_STORAGE_H

#include "flatsql/types.h"
#include <functional>
#include <unordered_map>
#include <optional>
#include <string_view>
#include <vector>

namespace flatsql {

struct IngestProfile {
    uint64_t recordCount = 0;
    uint64_t byteCount = 0;
    uint64_t decodeNanos = 0;
    uint64_t appendNanos = 0;
    uint64_t indexNanos = 0;

    void reset() {
        recordCount = 0;
        byteCount = 0;
        decodeNanos = 0;
        appendNanos = 0;
        indexNanos = 0;
    }
};

/**
 * Streaming FlatBuffer storage.
 *
 * Storage format (raw FlatBuffer stream):
 *   [4-byte size][FlatBuffer][4-byte size][FlatBuffer]...
 *
 * Each FlatBuffer must contain a file_identifier at bytes 4-7.
 * The library reads:
 *   1. Size prefix (4 bytes, little-endian) → how many bytes to read
 *   2. FlatBuffer data (size bytes)
 *   3. File identifier (bytes 4-7 of FlatBuffer) → routes to table
 *
 * This is a pure streaming format - no custom headers, no conversion.
 * Indexes are built during streaming ingest.
 */
class StreamingFlatBufferStore {
public:
    // Callback invoked for each FlatBuffer during streaming ingest
    // Parameters: file_id (4 bytes), data pointer, data length, assigned sequence, offset
    using IngestCallback = std::function<void(
        std::string_view fileId,
        const uint8_t* data,
        size_t length,
        uint64_t sequence,
        uint64_t offset
    )>;

    explicit StreamingFlatBufferStore(size_t initialCapacity = 1024 * 1024);

    // Preallocate storage for known large ingest targets. writeOffset_ remains unchanged.
    void reserveCapacity(size_t capacity);

    // The arena's hard cap in bytes. Growth never allocates past it, so an
    // append that would need to returns false from canAppend() instead of
    // trapping inside std::vector::resize (the no-EH build turns a failed
    // allocation into `unreachable`, which poisons the host's instance).
    static constexpr size_t kDefaultArenaLimit = size_t(1) << 30;  // 1 GiB
    void setArenaLimit(size_t bytes) { arenaLimit_ = bytes; }
    size_t arenaLimit() const { return arenaLimit_; }
    // True when `needed` more bytes fit under the cap (growing if required).
    bool canAppend(size_t needed) const noexcept {
        return static_cast<uint64_t>(writeOffset_) + needed <= arenaLimit_;
    }

    // Stream raw size-prefixed FlatBuffers
    // Calls callback for each complete FlatBuffer ingested
    // Returns number of bytes consumed (for buffer management)
    // Sets recordsProcessed to number of complete FlatBuffers processed
    size_t ingest(const uint8_t* data, size_t length, IngestCallback callback,
                  size_t* recordsProcessed = nullptr, IngestProfile* profile = nullptr);

    // Ingest a single size-prefixed FlatBuffer, returns sequence
    uint64_t ingestOne(const uint8_t* sizePrefixedData, size_t length,
                       IngestCallback callback, IngestProfile* profile = nullptr);

    // Ingest a single FlatBuffer (without size prefix), returns sequence
    uint64_t ingestFlatBuffer(const uint8_t* data, size_t length,
                              IngestCallback callback, IngestProfile* profile = nullptr);

    // Load existing stream data and rebuild via callback
    void loadAndRebuild(const uint8_t* data, size_t length,
                        IngestCallback callback, IngestProfile* profile = nullptr);

    // Read raw FlatBuffer at offset (returns pointer into storage, no copy)
    const uint8_t* getDataAtOffset(uint64_t offset, uint32_t* outLength) const;

    // Read a record by offset (copies data)
    StoredRecord readRecordAtOffset(uint64_t offset) const;

    // Get sequence number for offset (O(1) lookup)
    uint64_t getSequenceForOffset(uint64_t offset) const;

    // Read a record by sequence
    StoredRecord readRecord(uint64_t sequence) const;

    // Check if sequence exists
    bool hasRecord(uint64_t sequence) const;

    // Get offset for sequence
    std::optional<uint64_t> getOffsetForSequence(uint64_t sequence) const;

    // Iterate all records
    void iterateRecords(std::function<bool(const StoredRecord&)> callback) const;

    // Iterate records with specific file identifier
    void iterateByFileId(std::string_view fileId,
                         std::function<bool(const StoredRecord&)> callback) const;

    // Lightweight iteration - no data copy, just offset/sequence/pointer
    struct RecordRef {
        uint64_t offset;
        uint64_t sequence;
        const uint8_t* data;
        uint32_t length;
    };
    void iterateRefsByFileId(std::string_view fileId,
                             std::function<bool(const RecordRef&)> callback) const;

    // Record info for indexed access
    struct FileRecordInfo {
        uint64_t offset;
        uint64_t sequence;
    };

    // Get next record after the given offset, returns false if no more records
    // For lazy iteration without building a vector of all records
    bool getNextRecord(uint64_t afterOffset, std::string_view fileId,
                       uint64_t* outOffset, uint64_t* outSequence,
                       const uint8_t** outData, uint32_t* outLength) const;

    // Get first record with file ID, returns false if none
    bool getFirstRecord(std::string_view fileId,
                        uint64_t* outOffset, uint64_t* outSequence,
                        const uint8_t** outData, uint32_t* outLength) const;

    // Export raw stream data
    const std::vector<uint8_t>& getData() const { return data_; }
    std::vector<uint8_t> exportData() const {
        return std::vector<uint8_t>(data_.begin(), data_.begin() + writeOffset_);
    }

    // Statistics
    uint64_t getRecordCount() const { return recordCount_; }

    // The sequence the next appended record gets. Records are numbered 1, 2,
    // 3, ... in stream order, so a replay of the same stream into an empty
    // store gives every record the sequence it had.
    uint64_t nextSequence() const { return nextSequence_; }
    uint64_t getDataSize() const { return writeOffset_; }

    // Drop every in-memory record. The stream on disk is NOT touched — this
    // only empties the arena so a full re-derivation can replay it from byte 0.
    void reset() {
        // Deliberately does NOT free data_: ensureCapacity() grows by doubling,
        // so a zero-length buffer would spin forever. Bytes past writeOffset_
        // are never read.
        writeOffset_ = 0;
        recordCount_ = 0;
        nextSequence_ = 1;
        sequenceToOffset_.clear();
        offsetToSequence_.clear();
        fileIdToRecords_.clear();
        sequenceRuns_.clear();
        runCursor_ = 0;
        sequenceRunsConsistent_ = true;
        generation_++;
    }

    // ---- Sequence runs (arena compaction) ---------------------------------
    //
    // A record's sequence is its rowid, its record-encryption index and the
    // key hosts track it by, so it must survive a compaction and every replay
    // after one. The stream carries SDS wire bytes and nothing else, so the
    // numbering lives beside it: a run says "the frame at `offset` has
    // sequence `firstSeq`", and the frames after it count up by one until the
    // next run. A stream that was never compacted has no runs and numbers its
    // frames 1, 2, 3, ... exactly as before.
    struct SequenceRun {
        uint64_t offset = 0;
        uint64_t firstSeq = 0;
    };
    // Replace the runs (sorted by offset). Runs at or past writeOffset_ apply
    // to frames appended from here on; runs below it are history.
    void setSequenceRuns(std::vector<SequenceRun> runs);
    const std::vector<SequenceRun>& sequenceRuns() const { return sequenceRuns_; }
    // Add a run at the current end so the next frame gets `firstSeq`.
    void startSequenceRunAtEnd(uint64_t firstSeq);
    // False once a run asked for a sequence at or below one already given out.
    bool sequenceRunsConsistent() const { return sequenceRunsConsistent_; }
    // Bumped by reset() and by a compaction: a plan made against an older
    // generation no longer describes the arena.
    uint64_t generation() const { return generation_; }

    // ---- Compaction ---------------------------------------------------------
    struct KeptFrame {
        uint64_t oldOffset;
        uint64_t newOffset;
        uint64_t sequence;
    };
    // Size of the frame at `offset` including its 4-byte prefix, or 0 when
    // `offset` is not a whole frame inside the arena.
    uint64_t frameBytesAt(uint64_t offset) const noexcept;
    // Where each frame at `keep` (ascending offsets, each a frame start)
    // lands when the arena is packed from offset 0 in that order, and the
    // packed size. False when an offset is not a whole frame.
    bool planCompacted(const std::vector<uint64_t>& keep, std::vector<KeptFrame>& frames,
                       uint64_t* packedSize) const;
    // Pack the arena IN PLACE to that plan: every kept frame moves down to its
    // planned offset, in order (a frame only ever moves down, so none is
    // overwritten before it moves), and keeps its sequence; the maps are
    // rebuilt, the old ones released. Nothing is allocated for the bytes, so
    // the compaction's peak memory is the arena it already had. The capacity
    // is kept: wasm linear memory never shrinks, and an arena shrunk to its
    // live bytes regrows by doubling into a fragmented heap (measured at
    // host-02 shape: 1.4 GiB of linear memory against 1.0 GiB in place).
    // setArenaLimit bounds it. nextSequence() is unchanged.
    void compactInPlace(const std::vector<KeptFrame>& frames, uint64_t packedSize,
                        std::vector<SequenceRun> runs);
    // Bytes the arena has allocated (its capacity), as opposed to its size.
    uint64_t getCapacity() const { return data_.size(); }

    // Extract file identifier from a FlatBuffer (bytes 4-7)
    static std::string extractFileId(const uint8_t* flatbuffer, size_t length);

    // Get record by index within file ID (O(1) random access)
    // Returns false if index out of bounds
    bool getRecordByFileIndex(std::string_view fileId, size_t index,
                              uint64_t* outOffset, uint64_t* outSequence,
                              const uint8_t** outData, uint32_t* outLength) const;

    // Get count of records for a file ID
    size_t getRecordCountByFileId(std::string_view fileId) const;

    // Get direct pointer to record info vector (avoids map lookup per iteration)
    const std::vector<FileRecordInfo>* getRecordInfoVector(std::string_view fileId) const;

    // Get direct access to underlying storage buffer (for inline iteration)
    const uint8_t* getDataBuffer() const { return data_.data(); }
    uint64_t getWriteOffset() const { return writeOffset_; }

private:
    void ensureCapacity(size_t needed);
    void indexRecord(const std::string& fileId, uint64_t offset);
    // The sequence of a frame appended at `offset`: the next one, unless a
    // run starts there.
    uint64_t takeSequence(uint64_t offset);

    std::vector<uint8_t> data_;
    size_t arenaLimit_ = kDefaultArenaLimit;
    uint64_t writeOffset_ = 0;
    uint64_t recordCount_ = 0;
    uint64_t nextSequence_ = 1;

    // sequence → offset for O(1) lookups
    std::unordered_map<uint64_t, uint64_t> sequenceToOffset_;

    // offset → sequence for reverse lookups (O(1) instead of O(n))
    std::unordered_map<uint64_t, uint64_t> offsetToSequence_;

    // fileId → list of record info for O(1) iteration by file type
    std::unordered_map<std::string, std::vector<FileRecordInfo>> fileIdToRecords_;

    std::vector<SequenceRun> sequenceRuns_;  // sorted by offset
    size_t runCursor_ = 0;                   // first run not yet reached
    bool sequenceRunsConsistent_ = true;
    uint64_t generation_ = 0;
};

// Backwards compatibility alias
using StackedFlatBufferStore = StreamingFlatBufferStore;

}  // namespace flatsql

#endif  // FLATSQL_STORAGE_H
