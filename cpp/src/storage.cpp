#include "flatsql/storage.h"
#include <cstring>
#include <stdexcept>

namespace flatsql {

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

size_t StreamingFlatBufferStore::ingest(const uint8_t* data, size_t length, IngestCallback callback, size_t* recordsProcessed) {
    size_t records = 0;
    size_t offset = 0;

    while (offset + SIZE_PREFIX_LENGTH <= length) {
        // Read size prefix
        uint32_t fbSize = readLE32(data + offset);

        // Check if we have the complete FlatBuffer
        if (offset + SIZE_PREFIX_LENGTH + fbSize > length) {
            break;  // Incomplete, wait for more data
        }

        const uint8_t* fbData = data + offset + SIZE_PREFIX_LENGTH;

        // Store with size prefix
        uint64_t storeOffset = writeOffset_;
        ensureCapacity(SIZE_PREFIX_LENGTH + fbSize);
        std::memcpy(&data_[writeOffset_], data + offset, SIZE_PREFIX_LENGTH + fbSize);
        writeOffset_ += SIZE_PREFIX_LENGTH + fbSize;

        // Assign sequence and index
        uint64_t seq = nextSequence_++;
        sequenceToOffset_[seq] = storeOffset;
        offsetToSequence_[storeOffset] = seq;
        recordCount_++;

        // Extract file identifier and invoke callback
        std::string fileId = extractFileId(fbData, fbSize);
        if (callback) {
            callback(fileId, fbData, fbSize, seq, storeOffset);
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
                                              IngestCallback callback) {
    if (length < SIZE_PREFIX_LENGTH) {
        throw std::runtime_error("Data too small for size prefix");
    }

    uint32_t fbSize = readLE32(sizePrefixedData);
    if (length < SIZE_PREFIX_LENGTH + fbSize) {
        throw std::runtime_error("Incomplete FlatBuffer data");
    }

    const uint8_t* fbData = sizePrefixedData + SIZE_PREFIX_LENGTH;

    // Store
    uint64_t storeOffset = writeOffset_;
    ensureCapacity(SIZE_PREFIX_LENGTH + fbSize);
    std::memcpy(&data_[writeOffset_], sizePrefixedData, SIZE_PREFIX_LENGTH + fbSize);
    writeOffset_ += SIZE_PREFIX_LENGTH + fbSize;

    // Assign sequence
    uint64_t seq = nextSequence_++;
    sequenceToOffset_[seq] = storeOffset;
    offsetToSequence_[storeOffset] = seq;
    recordCount_++;

    // Callback
    std::string fileId = extractFileId(fbData, fbSize);
    if (callback) {
        callback(fileId, fbData, fbSize, seq, storeOffset);
    }

    return seq;
}

uint64_t StreamingFlatBufferStore::ingestFlatBuffer(const uint8_t* data, size_t length,
                                                     IngestCallback callback) {
    // Store with size prefix
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

    // Callback
    std::string fileId = extractFileId(data, length);
    if (callback) {
        callback(fileId, data, length, seq, storeOffset);
    }

    return seq;
}

void StreamingFlatBufferStore::loadAndRebuild(const uint8_t* data, size_t length,
                                               IngestCallback callback) {
    // Copy all data
    ensureCapacity(length);
    std::memcpy(data_.data(), data, length);

    // Scan through and rebuild indexes
    size_t offset = 0;
    while (offset + SIZE_PREFIX_LENGTH <= length) {
        uint32_t fbSize = readLE32(data + offset);

        if (offset + SIZE_PREFIX_LENGTH + fbSize > length) {
            break;  // Truncated
        }

        const uint8_t* fbData = data + offset + SIZE_PREFIX_LENGTH;

        uint64_t seq = nextSequence_++;
        sequenceToOffset_[seq] = offset;
        offsetToSequence_[offset] = seq;
        recordCount_++;

        std::string fileId = extractFileId(fbData, fbSize);
        if (callback) {
            callback(fileId, fbData, fbSize, seq, offset);
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

}  // namespace flatsql
