#ifndef FLATSQL_STORAGE_H
#define FLATSQL_STORAGE_H

#include "flatsql/types.h"
#include <functional>
#include <unordered_map>
#include <optional>
#include <string_view>

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
    uint64_t getDataSize() const { return writeOffset_; }

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

    std::vector<uint8_t> data_;
    uint64_t writeOffset_ = 0;
    uint64_t recordCount_ = 0;
    uint64_t nextSequence_ = 1;

    // sequence → offset for O(1) lookups
    std::unordered_map<uint64_t, uint64_t> sequenceToOffset_;

    // offset → sequence for reverse lookups (O(1) instead of O(n))
    std::unordered_map<uint64_t, uint64_t> offsetToSequence_;

    // fileId → list of record info for O(1) iteration by file type
    std::unordered_map<std::string, std::vector<FileRecordInfo>> fileIdToRecords_;
};

// Backwards compatibility alias
using StackedFlatBufferStore = StreamingFlatBufferStore;

}  // namespace flatsql

#endif  // FLATSQL_STORAGE_H
