#include "flatsql/storage.h"
#include <chrono>
#include <cstring>
#include <stdexcept>

namespace flatsql {

using ProfileClock = std::chrono::steady_clock;

// CRC32 implementation (IEEE polynomial) - kept for potential future use
static uint32_t computeCRC32(const uint8_t* data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }
    return ~crc;
}

uint32_t crc32(const uint8_t* data, size_t length) {
    return computeCRC32(data, length);
}

uint32_t crc32(const std::vector<uint8_t>& data) {
    return computeCRC32(data.data(), data.size());
}

// Little-endian helpers
static inline void writeLE32(uint8_t* dest, uint32_t value) {
    dest[0] = static_cast<uint8_t>(value);
    dest[1] = static_cast<uint8_t>(value >> 8);
    dest[2] = static_cast<uint8_t>(value >> 16);
    dest[3] = static_cast<uint8_t>(value >> 24);
}

static inline uint32_t readLE32(const uint8_t* src) {
    return static_cast<uint32_t>(src[0]) |
           (static_cast<uint32_t>(src[1]) << 8) |
           (static_cast<uint32_t>(src[2]) << 16) |
           (static_cast<uint32_t>(src[3]) << 24);
}

// ==================== StreamingFlatBufferStore ====================

StreamingFlatBufferStore::StreamingFlatBufferStore(size_t initialCapacity)
    : data_(initialCapacity) {
}

void StreamingFlatBufferStore::ensureCapacity(size_t needed) {
    size_t totalNeeded = static_cast<size_t>(writeOffset_) + needed;
    if (totalNeeded <= data_.size()) return;

    size_t newSize = data_.size() * 2;
    while (newSize < totalNeeded) {
        newSize *= 2;
    }
    data_.resize(newSize);
}

std::string StreamingFlatBufferStore::extractFileId(const uint8_t* flatbuffer, size_t length) {
    // File identifier is at bytes 4-7 of a FlatBuffer (after the root offset)
    if (length < 8) {
        return "";  // Too small to have file identifier
    }
    return std::string(reinterpret_cast<const char*>(flatbuffer + FILE_IDENTIFIER_OFFSET),
                       FILE_IDENTIFIER_LENGTH);
}

size_t StreamingFlatBufferStore::ingest(const uint8_t* data, size_t length, IngestCallback callback,
                                        size_t* recordsProcessed, IngestProfile* profile) {
    size_t records = 0;
    size_t offset = 0;

    while (offset + SIZE_PREFIX_LENGTH <= length) {
        const auto decodeStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
        // Read size prefix
        uint32_t fbSize = readLE32(data + offset);

        // Check if we have the complete FlatBuffer
        if (offset + SIZE_PREFIX_LENGTH + fbSize > length) {
            break;  // Incomplete, wait for more data
        }

        const uint8_t* fbData = data + offset + SIZE_PREFIX_LENGTH;
        if (profile) {
            profile->decodeNanos += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - decodeStart).count()
            );
        }

        // Store with size prefix
        const auto appendStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
        uint64_t storeOffset = writeOffset_;
        ensureCapacity(SIZE_PREFIX_LENGTH + fbSize);
        std::memcpy(&data_[writeOffset_], data + offset, SIZE_PREFIX_LENGTH + fbSize);
        writeOffset_ += SIZE_PREFIX_LENGTH + fbSize;

        // Assign sequence and index
        uint64_t seq = nextSequence_++;
        sequenceToOffset_[seq] = storeOffset;
        offsetToSequence_[storeOffset] = seq;
        recordCount_++;

        // Extract file identifier and build file ID index
        std::string fileId = extractFileId(fbData, fbSize);
        indexRecord(fileId, storeOffset);
        if (profile) {
            profile->appendNanos += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - appendStart).count()
            );
        }

        if (callback) {
            const auto indexStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
            callback(fileId, fbData, fbSize, seq, storeOffset);
            if (profile) {
                profile->indexNanos += static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - indexStart).count()
                );
            }
        }

        if (profile) {
            profile->recordCount++;
            profile->byteCount += fbSize;
        }

        offset += SIZE_PREFIX_LENGTH + fbSize;
        records++;
    }

    if (recordsProcessed) {
        *recordsProcessed = records;
    }
    return offset;  // Return bytes consumed
}

uint64_t StreamingFlatBufferStore::ingestOne(const uint8_t* sizePrefixedData, size_t length,
                                             IngestCallback callback, IngestProfile* profile) {
    if (length < SIZE_PREFIX_LENGTH) {
        throw std::runtime_error("Data too small for size prefix");
    }

    const auto decodeStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
    uint32_t fbSize = readLE32(sizePrefixedData);
    if (length < SIZE_PREFIX_LENGTH + fbSize) {
        throw std::runtime_error("Incomplete FlatBuffer data");
    }

    const uint8_t* fbData = sizePrefixedData + SIZE_PREFIX_LENGTH;
    if (profile) {
        profile->decodeNanos += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - decodeStart).count()
        );
    }

    // Store
    const auto appendStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
    uint64_t storeOffset = writeOffset_;
    ensureCapacity(SIZE_PREFIX_LENGTH + fbSize);
    std::memcpy(&data_[writeOffset_], sizePrefixedData, SIZE_PREFIX_LENGTH + fbSize);
    writeOffset_ += SIZE_PREFIX_LENGTH + fbSize;

    // Assign sequence
    uint64_t seq = nextSequence_++;
    sequenceToOffset_[seq] = storeOffset;
    offsetToSequence_[storeOffset] = seq;
    recordCount_++;

    // Build file ID index
    std::string fileId = extractFileId(fbData, fbSize);
    indexRecord(fileId, storeOffset);
    if (profile) {
        profile->appendNanos += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - appendStart).count()
        );
    }

    if (callback) {
        const auto indexStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
        callback(fileId, fbData, fbSize, seq, storeOffset);
        if (profile) {
            profile->indexNanos += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - indexStart).count()
            );
        }
    }

    if (profile) {
        profile->recordCount++;
        profile->byteCount += fbSize;
    }

    return seq;
}

uint64_t StreamingFlatBufferStore::ingestFlatBuffer(const uint8_t* data, size_t length,
                                                    IngestCallback callback, IngestProfile* profile) {
    // Store with size prefix
    const auto decodeStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
    if (profile) {
        profile->decodeNanos += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - decodeStart).count()
        );
    }
    const auto appendStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
    uint64_t storeOffset = writeOffset_;
    ensureCapacity(SIZE_PREFIX_LENGTH + length);

    writeLE32(&data_[writeOffset_], static_cast<uint32_t>(length));
    writeOffset_ += SIZE_PREFIX_LENGTH;

    std::memcpy(&data_[writeOffset_], data, length);
    writeOffset_ += length;

    // Assign sequence
    uint64_t seq = nextSequence_++;
    sequenceToOffset_[seq] = storeOffset;
    offsetToSequence_[storeOffset] = seq;
    recordCount_++;

    // Build file ID index
    std::string fileId = extractFileId(data, length);
    indexRecord(fileId, storeOffset);
    if (profile) {
        profile->appendNanos += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - appendStart).count()
        );
    }

    if (callback) {
        const auto indexStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
        callback(fileId, data, length, seq, storeOffset);
        if (profile) {
            profile->indexNanos += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - indexStart).count()
            );
        }
    }

    if (profile) {
        profile->recordCount++;
        profile->byteCount += length;
    }

    return seq;
}

void StreamingFlatBufferStore::loadAndRebuild(const uint8_t* data, size_t length,
                                              IngestCallback callback, IngestProfile* profile) {
    // Copy all data
    ensureCapacity(length);
    std::memcpy(data_.data(), data, length);

    // Scan through and rebuild indexes
    size_t offset = 0;
    while (offset + SIZE_PREFIX_LENGTH <= length) {
        const auto decodeStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
        uint32_t fbSize = readLE32(data + offset);

        if (offset + SIZE_PREFIX_LENGTH + fbSize > length) {
            break;  // Truncated
        }

        const uint8_t* fbData = data + offset + SIZE_PREFIX_LENGTH;
        if (profile) {
            profile->decodeNanos += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - decodeStart).count()
            );
        }

        uint64_t seq = nextSequence_++;
        sequenceToOffset_[seq] = offset;
        offsetToSequence_[offset] = seq;
        recordCount_++;

        std::string fileId = extractFileId(fbData, fbSize);
        indexRecord(fileId, offset);

        if (callback) {
            const auto indexStart = profile ? ProfileClock::now() : ProfileClock::time_point{};
            callback(fileId, fbData, fbSize, seq, offset);
            if (profile) {
                profile->indexNanos += static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - indexStart).count()
                );
            }
        }

        if (profile) {
            profile->recordCount++;
            profile->byteCount += fbSize;
        }

        offset += SIZE_PREFIX_LENGTH + fbSize;
    }

    writeOffset_ = offset;
}

const uint8_t* StreamingFlatBufferStore::getDataAtOffset(uint64_t offset, uint32_t* outLength) const {
    size_t off = static_cast<size_t>(offset);

    if (off + SIZE_PREFIX_LENGTH > writeOffset_) {
        throw std::runtime_error("Invalid offset: beyond data bounds");
    }

    uint32_t fbSize = readLE32(&data_[off]);
    if (off + SIZE_PREFIX_LENGTH + fbSize > writeOffset_) {
        throw std::runtime_error("Invalid record: data extends beyond bounds");
    }

    if (outLength) {
        *outLength = fbSize;
    }
    return &data_[off + SIZE_PREFIX_LENGTH];
}

StoredRecord StreamingFlatBufferStore::readRecordAtOffset(uint64_t offset) const {
    uint32_t fbSize;
    const uint8_t* fbData = getDataAtOffset(offset, &fbSize);

    StoredRecord record;
    record.offset = offset;
    record.header.dataLength = fbSize;
    record.header.fileId = extractFileId(fbData, fbSize);

    // Look up sequence in reverse map (O(1) instead of O(n))
    auto it = offsetToSequence_.find(offset);
    if (it != offsetToSequence_.end()) {
        record.header.sequence = it->second;
    }

    record.data.resize(fbSize);
    std::memcpy(record.data.data(), fbData, fbSize);

    return record;
}

StoredRecord StreamingFlatBufferStore::readRecord(uint64_t sequence) const {
    auto it = sequenceToOffset_.find(sequence);
    if (it == sequenceToOffset_.end()) {
        throw std::runtime_error("Record not found for sequence: " + std::to_string(sequence));
    }
    return readRecordAtOffset(it->second);
}

uint64_t StreamingFlatBufferStore::getSequenceForOffset(uint64_t offset) const {
    auto it = offsetToSequence_.find(offset);
    if (it != offsetToSequence_.end()) {
        return it->second;
    }
    return 0;  // Invalid sequence
}

bool StreamingFlatBufferStore::hasRecord(uint64_t sequence) const {
    return sequenceToOffset_.find(sequence) != sequenceToOffset_.end();
}

std::optional<uint64_t> StreamingFlatBufferStore::getOffsetForSequence(uint64_t sequence) const {
    auto it = sequenceToOffset_.find(sequence);
    if (it == sequenceToOffset_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void StreamingFlatBufferStore::iterateRecords(std::function<bool(const StoredRecord&)> callback) const {
    size_t offset = 0;
    while (offset + SIZE_PREFIX_LENGTH <= writeOffset_) {
        uint32_t fbSize = readLE32(&data_[offset]);
        if (offset + SIZE_PREFIX_LENGTH + fbSize > writeOffset_) {
            break;
        }

        StoredRecord record = readRecordAtOffset(offset);
        if (!callback(record)) {
            break;
        }

        offset += SIZE_PREFIX_LENGTH + fbSize;
    }
}

void StreamingFlatBufferStore::iterateByFileId(std::string_view fileId,
                                                std::function<bool(const StoredRecord&)> callback) const {
    iterateRecords([&](const StoredRecord& record) {
        if (record.header.fileId == fileId) {
            return callback(record);
        }
        return true;  // continue
    });
}

void StreamingFlatBufferStore::iterateRefsByFileId(std::string_view fileId,
                                                    std::function<bool(const RecordRef&)> callback) const {
    size_t offset = 0;
    while (offset + SIZE_PREFIX_LENGTH <= writeOffset_) {
        uint32_t fbSize = readLE32(&data_[offset]);
        if (offset + SIZE_PREFIX_LENGTH + fbSize > writeOffset_) {
            break;
        }

        const uint8_t* fbData = &data_[offset + SIZE_PREFIX_LENGTH];

        // Compare file ID without allocation (inline comparison)
        bool matches = false;
        if (fbSize >= 8) {
            std::string_view recordFileId(reinterpret_cast<const char*>(fbData + FILE_IDENTIFIER_OFFSET),
                                          FILE_IDENTIFIER_LENGTH);
            matches = (recordFileId == fileId);
        }

        if (matches) {
            RecordRef ref;
            ref.offset = offset;
            auto it = offsetToSequence_.find(offset);
            ref.sequence = (it != offsetToSequence_.end()) ? it->second : 0;
            ref.data = fbData;
            ref.length = fbSize;

            if (!callback(ref)) {
                break;
            }
        }

        offset += SIZE_PREFIX_LENGTH + fbSize;
    }
}

bool StreamingFlatBufferStore::getFirstRecord(std::string_view fileId,
                                               uint64_t* outOffset, uint64_t* outSequence,
                                               const uint8_t** outData, uint32_t* outLength) const {
    size_t offset = 0;
    while (offset + SIZE_PREFIX_LENGTH <= writeOffset_) {
        uint32_t fbSize = readLE32(&data_[offset]);
        if (offset + SIZE_PREFIX_LENGTH + fbSize > writeOffset_) {
            break;
        }

        const uint8_t* fbData = &data_[offset + SIZE_PREFIX_LENGTH];

        // Compare file ID inline without allocation
        if (fbSize >= 8) {
            std::string_view recordFileId(reinterpret_cast<const char*>(fbData + FILE_IDENTIFIER_OFFSET),
                                          FILE_IDENTIFIER_LENGTH);
            if (recordFileId == fileId) {
                *outOffset = offset;
                auto it = offsetToSequence_.find(offset);
                *outSequence = (it != offsetToSequence_.end()) ? it->second : 0;
                *outData = fbData;
                *outLength = fbSize;
                return true;
            }
        }

        offset += SIZE_PREFIX_LENGTH + fbSize;
    }
    return false;
}

bool StreamingFlatBufferStore::getNextRecord(uint64_t afterOffset, std::string_view fileId,
                                              uint64_t* outOffset, uint64_t* outSequence,
                                              const uint8_t** outData, uint32_t* outLength) const {
    // Start after the given offset
    size_t offset = static_cast<size_t>(afterOffset);

    // Skip current record
    if (offset + SIZE_PREFIX_LENGTH <= writeOffset_) {
        uint32_t currentSize = readLE32(&data_[offset]);
        offset += SIZE_PREFIX_LENGTH + currentSize;
    }

    // Find next record with matching file ID
    while (offset + SIZE_PREFIX_LENGTH <= writeOffset_) {
        uint32_t fbSize = readLE32(&data_[offset]);
        if (offset + SIZE_PREFIX_LENGTH + fbSize > writeOffset_) {
            break;
        }

        const uint8_t* fbData = &data_[offset + SIZE_PREFIX_LENGTH];

        // Compare file ID inline without allocation
        if (fbSize >= 8) {
            std::string_view recordFileId(reinterpret_cast<const char*>(fbData + FILE_IDENTIFIER_OFFSET),
                                          FILE_IDENTIFIER_LENGTH);
            if (recordFileId == fileId) {
                *outOffset = offset;
                auto it = offsetToSequence_.find(offset);
                *outSequence = (it != offsetToSequence_.end()) ? it->second : 0;
                *outData = fbData;
                *outLength = fbSize;
                return true;
            }
        }

        offset += SIZE_PREFIX_LENGTH + fbSize;
    }
    return false;
}

void StreamingFlatBufferStore::indexRecord(const std::string& fileId, uint64_t offset) {
    auto it = offsetToSequence_.find(offset);
    uint64_t seq = (it != offsetToSequence_.end()) ? it->second : 0;
    fileIdToRecords_[fileId].push_back({offset, seq});
}

bool StreamingFlatBufferStore::getRecordByFileIndex(std::string_view fileId, size_t index,
                                                     uint64_t* outOffset, uint64_t* outSequence,
                                                     const uint8_t** outData, uint32_t* outLength) const {
    // Look up file ID index
    auto it = fileIdToRecords_.find(std::string(fileId));
    if (it == fileIdToRecords_.end() || index >= it->second.size()) {
        return false;
    }

    const FileRecordInfo& info = it->second[index];

    // Inline data access to avoid function call overhead
    size_t off = static_cast<size_t>(info.offset);
    if (off + SIZE_PREFIX_LENGTH > writeOffset_) {
        return false;
    }
    uint32_t fbSize = static_cast<uint32_t>(data_[off]) |
                      (static_cast<uint32_t>(data_[off + 1]) << 8) |
                      (static_cast<uint32_t>(data_[off + 2]) << 16) |
                      (static_cast<uint32_t>(data_[off + 3]) << 24);

    *outOffset = info.offset;
    *outSequence = info.sequence;
    *outData = &data_[off + SIZE_PREFIX_LENGTH];
    *outLength = fbSize;
    return true;
}

size_t StreamingFlatBufferStore::getRecordCountByFileId(std::string_view fileId) const {
    auto it = fileIdToRecords_.find(std::string(fileId));
    if (it == fileIdToRecords_.end()) {
        return 0;
    }
    return it->second.size();
}

const std::vector<StreamingFlatBufferStore::FileRecordInfo>*
StreamingFlatBufferStore::getRecordInfoVector(std::string_view fileId) const {
    auto it = fileIdToRecords_.find(std::string(fileId));
    if (it == fileIdToRecords_.end()) {
        return nullptr;
    }
    return &it->second;
}

}  // namespace flatsql
