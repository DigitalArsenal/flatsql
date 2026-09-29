#include "flatsql/storage.h"
#include <chrono>
#include <cstdlib>
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

void StreamingFlatBufferStore::reserveCapacity(size_t capacity) {
    if (capacity > data_.size()) {
        data_.resize(capacity);
    }
}

void StreamingFlatBufferStore::ensureCapacity(size_t needed) {
    size_t totalNeeded = static_cast<size_t>(writeOffset_) + needed;
    if (totalNeeded <= data_.size()) return;

    // Never double from zero — that spins forever. An empty arena starts at a
    // page rather than at nothing.
    size_t newSize = data_.empty() ? 4096 : data_.size() * 2;
    while (newSize < totalNeeded) {
        newSize *= 2;
    }
    // Doubling past the cap is what trapped at ~1 GiB inside the 4 GiB wasm32
    // memory: the resize needs the old and the new vector at once. Growth
    // stops AT the cap; callers check canAppend() before they append, so a
    // request past it never reaches the resize.
    if (newSize > arenaLimit_) {
        newSize = totalNeeded > arenaLimit_ ? totalNeeded : arenaLimit_;
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
        // A record past the arena cap is not consumed (as if incomplete): the
        // caller sees fewer bytes taken instead of a trap in the resize.
        if (!canAppend(SIZE_PREFIX_LENGTH + fbSize)) {
            break;
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
        uint64_t seq = takeSequence(storeOffset);
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
    uint64_t seq = takeSequence(storeOffset);
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
    uint64_t seq = takeSequence(storeOffset);
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

        uint64_t seq = takeSequence(offset);
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

// ==================== Sequence runs ====================

uint64_t StreamingFlatBufferStore::takeSequence(uint64_t offset) {
    while (runCursor_ < sequenceRuns_.size() && sequenceRuns_[runCursor_].offset < offset) {
        runCursor_++;  // a run that does not start on a frame is never reached
    }
    if (runCursor_ < sequenceRuns_.size() && sequenceRuns_[runCursor_].offset == offset) {
        const uint64_t first = sequenceRuns_[runCursor_].firstSeq;
        if (first < nextSequence_) {
            // A run may only skip forward. One that goes back would hand out
            // a sequence a record already has.
            sequenceRunsConsistent_ = false;
        } else {
            nextSequence_ = first;
        }
        runCursor_++;
    }
    return nextSequence_++;
}

void StreamingFlatBufferStore::setSequenceRuns(std::vector<SequenceRun> runs) {
    sequenceRuns_ = std::move(runs);
    runCursor_ = 0;
    while (runCursor_ < sequenceRuns_.size() && sequenceRuns_[runCursor_].offset < writeOffset_) {
        runCursor_++;
    }
}

void StreamingFlatBufferStore::startSequenceRunAtEnd(uint64_t firstSeq) {
    if (firstSeq <= nextSequence_) return;  // numbering already reaches it
    // A run at the end replaces one already waiting there.
    if (!sequenceRuns_.empty() && sequenceRuns_.back().offset == writeOffset_) {
        sequenceRuns_.back().firstSeq = firstSeq;
    } else {
        sequenceRuns_.push_back(SequenceRun{writeOffset_, firstSeq});
    }
    runCursor_ = 0;
    while (runCursor_ < sequenceRuns_.size() && sequenceRuns_[runCursor_].offset < writeOffset_) {
        runCursor_++;
    }
    nextSequence_ = firstSeq;
    // The run is consumed by the next append at writeOffset_: takeSequence
    // sees first == nextSequence_ and keeps it.
}

// ==================== Compaction ====================

uint64_t StreamingFlatBufferStore::frameBytesAt(uint64_t offset) const noexcept {
    if (offset > writeOffset_ || writeOffset_ - offset < SIZE_PREFIX_LENGTH) return 0;
    const uint64_t size = readLE32(&data_[static_cast<size_t>(offset)]);
    if (size > writeOffset_ - offset - SIZE_PREFIX_LENGTH) return 0;
    return SIZE_PREFIX_LENGTH + size;
}

bool StreamingFlatBufferStore::planCompacted(const std::vector<uint64_t>& keep,
                                             std::vector<KeptFrame>& frames,
                                             uint64_t* packedSize) const {
    std::vector<KeptFrame> placed;
    placed.reserve(keep.size());
    uint64_t at = 0;
    for (const uint64_t offset : keep) {
        const uint64_t bytes = frameBytesAt(offset);
        if (bytes == 0 || offset < at) return false;
        auto seqIt = offsetToSequence_.find(offset);
        placed.push_back(KeptFrame{offset, at, seqIt == offsetToSequence_.end() ? 0 : seqIt->second});
        at += bytes;
    }
    frames.swap(placed);
    if (packedSize) *packedSize = at;
    return true;
}

void StreamingFlatBufferStore::compactInPlace(const std::vector<KeptFrame>& frames, uint64_t packedSize,
                                              std::vector<SequenceRun> runs) {
    // Ascending, and newOffset <= oldOffset for every frame: moving them in
    // order never overwrites a frame that has not moved yet.
    for (const auto& frame : frames) {
        const uint64_t bytes = SIZE_PREFIX_LENGTH + readLE32(&data_[static_cast<size_t>(frame.oldOffset)]);
        if (frame.newOffset != frame.oldOffset) {
            std::memmove(&data_[static_cast<size_t>(frame.newOffset)],
                         &data_[static_cast<size_t>(frame.oldOffset)], static_cast<size_t>(bytes));
        }
    }
    writeOffset_ = packedSize;

    // Fresh maps rather than clear(): clear() keeps every bucket array.
    std::unordered_map<uint64_t, uint64_t>().swap(sequenceToOffset_);
    std::unordered_map<uint64_t, uint64_t>().swap(offsetToSequence_);
    std::unordered_map<std::string, std::vector<FileRecordInfo>>().swap(fileIdToRecords_);
    sequenceToOffset_.reserve(frames.size());
    offsetToSequence_.reserve(frames.size());
    for (const auto& frame : frames) {
        sequenceToOffset_[frame.sequence] = frame.newOffset;
        offsetToSequence_[frame.newOffset] = frame.sequence;
        const uint32_t size = readLE32(&data_[static_cast<size_t>(frame.newOffset)]);
        const std::string fileId = extractFileId(&data_[static_cast<size_t>(frame.newOffset) + SIZE_PREFIX_LENGTH], size);
        fileIdToRecords_[fileId].push_back({frame.newOffset, frame.sequence});
    }
    recordCount_ = frames.size();
    setSequenceRuns(std::move(runs));
    generation_++;
}

}  // namespace flatsql
