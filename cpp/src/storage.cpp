#include "flatsql/storage.h"
#include <cstring>
#include <stdexcept>
#include <chrono>

namespace flatsql {

// CRC32 implementation (IEEE polynomial)
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

// Helper functions for reading/writing little-endian values
static inline void writeLE32(uint8_t* dest, uint32_t value) {
    dest[0] = static_cast<uint8_t>(value);
    dest[1] = static_cast<uint8_t>(value >> 8);
    dest[2] = static_cast<uint8_t>(value >> 16);
    dest[3] = static_cast<uint8_t>(value >> 24);
}

static inline void writeLE64(uint8_t* dest, uint64_t value) {
    dest[0] = static_cast<uint8_t>(value);
    dest[1] = static_cast<uint8_t>(value >> 8);
    dest[2] = static_cast<uint8_t>(value >> 16);
    dest[3] = static_cast<uint8_t>(value >> 24);
    dest[4] = static_cast<uint8_t>(value >> 32);
    dest[5] = static_cast<uint8_t>(value >> 40);
    dest[6] = static_cast<uint8_t>(value >> 48);
    dest[7] = static_cast<uint8_t>(value >> 56);
}

static inline uint32_t readLE32(const uint8_t* src) {
    return static_cast<uint32_t>(src[0]) |
           (static_cast<uint32_t>(src[1]) << 8) |
           (static_cast<uint32_t>(src[2]) << 16) |
           (static_cast<uint32_t>(src[3]) << 24);
}

static inline uint64_t readLE64(const uint8_t* src) {
    return static_cast<uint64_t>(src[0]) |
           (static_cast<uint64_t>(src[1]) << 8) |
           (static_cast<uint64_t>(src[2]) << 16) |
           (static_cast<uint64_t>(src[3]) << 24) |
           (static_cast<uint64_t>(src[4]) << 32) |
           (static_cast<uint64_t>(src[5]) << 40) |
           (static_cast<uint64_t>(src[6]) << 48) |
           (static_cast<uint64_t>(src[7]) << 56);
}

StackedFlatBufferStore::StackedFlatBufferStore(const std::string& schemaName, size_t initialCapacity)
    : data_(initialCapacity), writeOffset_(FILE_HEADER_SIZE), schemaName_(schemaName) {
    writeFileHeader();
}

void StackedFlatBufferStore::writeFileHeader() {
    // Magic number (4 bytes)
    writeLE32(&data_[0], MAGIC);
    // Version (4 bytes)
    writeLE32(&data_[4], VERSION);
    // Data start offset (8 bytes)
    writeLE64(&data_[8], FILE_HEADER_SIZE);
    // Record count (8 bytes) - updated on each append
    writeLE64(&data_[16], 0);
    // Schema name (up to 40 bytes, null-terminated)
    size_t nameLen = std::min(schemaName_.length(), size_t(39));
    std::memcpy(&data_[24], schemaName_.c_str(), nameLen);
    data_[24 + nameLen] = 0;
}

void StackedFlatBufferStore::updateFileHeader() {
    writeLE64(&data_[16], recordCount_);
}

void StackedFlatBufferStore::ensureCapacity(size_t needed) {
    size_t totalNeeded = static_cast<size_t>(writeOffset_) + needed;
    if (totalNeeded <= data_.size()) return;

    size_t newSize = data_.size() * 2;
    while (newSize < totalNeeded) {
        newSize *= 2;
    }

    data_.resize(newSize);
}

uint64_t StackedFlatBufferStore::append(const std::string& tableName, const std::vector<uint8_t>& flatbufferData) {
    return append(tableName, flatbufferData.data(), flatbufferData.size());
}

uint64_t StackedFlatBufferStore::append(const std::string& tableName, const uint8_t* fbData, size_t length) {
    uint32_t checksum = crc32(fbData, length);
    auto now = std::chrono::system_clock::now();
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    uint64_t seq = sequence_++;

    size_t totalSize = RECORD_HEADER_SIZE + length;
    ensureCapacity(totalSize);

    size_t offset = static_cast<size_t>(writeOffset_);

    // Write record header (48 bytes)
    // Sequence (8 bytes)
    writeLE64(&data_[offset], seq);

    // Table name (16 bytes, null-padded)
    std::memset(&data_[offset + 8], 0, 16);
    size_t tableLen = std::min(tableName.length(), size_t(15));
    std::memcpy(&data_[offset + 8], tableName.c_str(), tableLen);

    // Timestamp (8 bytes)
    writeLE64(&data_[offset + 24], timestamp);

    // Data length (4 bytes)
    writeLE32(&data_[offset + 32], static_cast<uint32_t>(length));

    // Checksum (4 bytes)
    writeLE32(&data_[offset + 36], checksum);

    // Reserved (8 bytes) - zeroed
    std::memset(&data_[offset + 40], 0, 8);

    // Write FlatBuffer data
    std::memcpy(&data_[offset + RECORD_HEADER_SIZE], fbData, length);

    uint64_t recordOffset = writeOffset_;
    writeOffset_ += totalSize;
    recordCount_++;
    updateFileHeader();

    return recordOffset;
}

StoredRecord StackedFlatBufferStore::readRecord(uint64_t offset) const {
    size_t off = static_cast<size_t>(offset);

    if (off + RECORD_HEADER_SIZE > data_.size()) {
        throw std::runtime_error("Invalid offset: beyond data bounds");
    }

    StoredRecord record;
    record.offset = offset;

    // Read header
    record.header.sequence = readLE64(&data_[off]);

    // Table name
    const char* tableNamePtr = reinterpret_cast<const char*>(&data_[off + 8]);
    size_t tableNameLen = strnlen(tableNamePtr, 15);
    record.header.tableName = std::string(tableNamePtr, tableNameLen);

    record.header.timestamp = readLE64(&data_[off + 24]);
    record.header.dataLength = readLE32(&data_[off + 32]);
    record.header.checksum = readLE32(&data_[off + 36]);

    // Read data
    size_t dataStart = off + RECORD_HEADER_SIZE;
    if (dataStart + record.header.dataLength > data_.size()) {
        throw std::runtime_error("Invalid record: data extends beyond bounds");
    }

    record.data.resize(record.header.dataLength);
    std::memcpy(record.data.data(), &data_[dataStart], record.header.dataLength);

    // Verify checksum
    uint32_t computedChecksum = crc32(record.data);
    if (computedChecksum != record.header.checksum) {
        throw std::runtime_error("Checksum mismatch at offset " + std::to_string(offset));
    }

    return record;
}

void StackedFlatBufferStore::iterateRecords(std::function<bool(const StoredRecord&)> callback) const {
    uint64_t offset = FILE_HEADER_SIZE;
    while (offset < writeOffset_) {
        StoredRecord record = readRecord(offset);
        if (!callback(record)) {
            break;
        }
        offset += RECORD_HEADER_SIZE + record.header.dataLength;
    }
}

void StackedFlatBufferStore::iterateTableRecords(const std::string& tableName,
                                                  std::function<bool(const StoredRecord&)> callback) const {
    iterateRecords([&](const StoredRecord& record) {
        if (record.header.tableName == tableName) {
            return callback(record);
        }
        return true;  // continue
    });
}

StackedFlatBufferStore StackedFlatBufferStore::fromData(const std::vector<uint8_t>& data) {
    return fromData(data.data(), data.size());
}

StackedFlatBufferStore StackedFlatBufferStore::fromData(const uint8_t* data, size_t length) {
    if (length < FILE_HEADER_SIZE) {
        throw std::runtime_error("Data too small for valid file");
    }

    // Verify magic
    uint32_t magic = readLE32(&data[0]);
    if (magic != MAGIC) {
        throw std::runtime_error("Invalid file magic");
    }

    uint32_t version = readLE32(&data[4]);
    if (version != VERSION) {
        throw std::runtime_error("Unsupported file version: " + std::to_string(version));
    }

    // Read schema name
    const char* schemaNamePtr = reinterpret_cast<const char*>(&data[24]);
    size_t schemaNameLen = strnlen(schemaNamePtr, 39);
    std::string schemaName(schemaNamePtr, schemaNameLen);

    StackedFlatBufferStore store(schemaName, length);
    store.data_.resize(length);
    std::memcpy(store.data_.data(), data, length);

    // Rebuild state by scanning records
    store.writeOffset_ = FILE_HEADER_SIZE;
    store.recordCount_ = 0;
    store.sequence_ = 0;

    uint64_t offset = FILE_HEADER_SIZE;
    while (offset < length) {
        try {
            StoredRecord record = store.readRecord(offset);
            store.recordCount_++;
            if (record.header.sequence >= store.sequence_) {
                store.sequence_ = record.header.sequence + 1;
            }
            offset += RECORD_HEADER_SIZE + record.header.dataLength;
            store.writeOffset_ = offset;
        } catch (...) {
            // End of valid data
            break;
        }
    }

    return store;
}

}  // namespace flatsql
