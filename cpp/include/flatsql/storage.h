#ifndef FLATSQL_STORAGE_H
#define FLATSQL_STORAGE_H

#include "flatsql/types.h"
#include <functional>
#include <unordered_map>
#include <optional>
#include <string_view>

namespace flatsql {

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

    // Stream raw size-prefixed FlatBuffers
    // Calls callback for each complete FlatBuffer ingested
    // Returns number of bytes consumed (for buffer management)
    // Sets recordsProcessed to number of complete FlatBuffers processed
    size_t ingest(const uint8_t* data, size_t length, IngestCallback callback, size_t* recordsProcessed = nullptr);

    // Ingest a single size-prefixed FlatBuffer, returns sequence
    uint64_t ingestOne(const uint8_t* sizePrefixedData, size_t length, IngestCallback callback);

    // Ingest a single FlatBuffer (without size prefix), returns sequence
    uint64_t ingestFlatBuffer(const uint8_t* data, size_t length, IngestCallback callback);

    // Load existing stream data and rebuild via callback
    void loadAndRebuild(const uint8_t* data, size_t length, IngestCallback callback);

    // Read raw FlatBuffer at offset (returns pointer into storage, no copy)
    const uint8_t* getDataAtOffset(uint64_t offset, uint32_t* outLength) const;

    // Read a record by offset
    StoredRecord readRecordAtOffset(uint64_t offset) const;

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

private:
    void ensureCapacity(size_t needed);

    std::vector<uint8_t> data_;
    uint64_t writeOffset_ = 0;
    uint64_t recordCount_ = 0;
    uint64_t nextSequence_ = 1;

    // sequence → offset for O(1) lookups
    std::unordered_map<uint64_t, uint64_t> sequenceToOffset_;

    // offset → sequence for reverse lookups (O(1) instead of O(n))
    std::unordered_map<uint64_t, uint64_t> offsetToSequence_;
};

// Backwards compatibility alias
using StackedFlatBufferStore = StreamingFlatBufferStore;

}  // namespace flatsql

#endif  // FLATSQL_STORAGE_H
