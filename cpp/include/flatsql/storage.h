#ifndef FLATSQL_STORAGE_H
#define FLATSQL_STORAGE_H

#include "flatsql/types.h"
#include <functional>

namespace flatsql {

/**
 * Stacked FlatBuffer storage.
 *
 * File format:
 *   [FileHeader: 64 bytes]
 *   [RecordHeader: 48 bytes][FlatBuffer data: variable]
 *   [RecordHeader: 48 bytes][FlatBuffer data: variable]
 *   ...
 *
 * Each FlatBuffer is a complete, valid FlatBuffer that can be read
 * independently by standard FlatBuffer tools.
 */
class StackedFlatBufferStore {
public:
    explicit StackedFlatBufferStore(const std::string& schemaName,
                                     size_t initialCapacity = 1024 * 1024);

    // Load from existing data
    static StackedFlatBufferStore fromData(const std::vector<uint8_t>& data);
    static StackedFlatBufferStore fromData(const uint8_t* data, size_t length);

    // Append a FlatBuffer record, returns offset where stored
    uint64_t append(const std::string& tableName, const std::vector<uint8_t>& flatbufferData);
    uint64_t append(const std::string& tableName, const uint8_t* data, size_t length);

    // Read a record at offset
    StoredRecord readRecord(uint64_t offset) const;

    // Iterate all records
    void iterateRecords(std::function<bool(const StoredRecord&)> callback) const;

    // Iterate records for a specific table
    void iterateTableRecords(const std::string& tableName,
                             std::function<bool(const StoredRecord&)> callback) const;

    // Get raw data for export
    const std::vector<uint8_t>& getData() const { return data_; }
    std::vector<uint8_t> getDataCopy() const { return std::vector<uint8_t>(data_.begin(), data_.begin() + writeOffset_); }

    // Statistics
    uint64_t getRecordCount() const { return recordCount_; }
    const std::string& getSchemaName() const { return schemaName_; }
    uint64_t getDataSize() const { return writeOffset_; }

private:
    void writeFileHeader();
    void updateFileHeader();
    void ensureCapacity(size_t needed);

    std::vector<uint8_t> data_;
    uint64_t writeOffset_;
    uint64_t recordCount_ = 0;
    uint64_t sequence_ = 0;
    std::string schemaName_;
};

}  // namespace flatsql

#endif  // FLATSQL_STORAGE_H
