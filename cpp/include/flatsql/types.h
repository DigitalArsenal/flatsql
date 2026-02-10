#ifndef FLATSQL_TYPES_H
#define FLATSQL_TYPES_H

#include <cstdint>
#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <memory>
#include <map>

namespace flatsql {

// Streaming format constants
// FlatBuffers are stored as: [4-byte size prefix][FlatBuffer data]
// The file identifier is read from bytes 4-7 of the FlatBuffer itself
constexpr size_t SIZE_PREFIX_LENGTH = 4;
constexpr size_t FILE_IDENTIFIER_OFFSET = 4;  // Offset within FlatBuffer
constexpr size_t FILE_IDENTIFIER_LENGTH = 4;

// Value types supported in FlatSQL
enum class ValueType {
    Null,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Bytes
};

// A value that can be stored/queried
using Value = std::variant<
    std::monostate,     // null
    bool,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    float,
    double,
    std::string,
    std::vector<uint8_t>
>;

// Get the ValueType for a Value
inline ValueType getValueType(const Value& v) {
    return static_cast<ValueType>(v.index());
}

// Compare two values (returns -1, 0, 1)
int compareValues(const Value& a, const Value& b);

// Column definition
struct ColumnDef {
    std::string name;
    ValueType type;
    bool nullable = true;
    bool indexed = false;
    bool primaryKey = false;
    bool encrypted = false;         // Field uses FlatBuffer field-level encryption
    uint16_t fieldId = 0;           // FlatBuffer field ID (for encryption key derivation)
    std::optional<Value> defaultValue;
};

// Table definition
struct TableDef {
    std::string name;
    std::vector<ColumnDef> columns;
    std::vector<std::string> primaryKeyColumns;

    int getColumnIndex(const std::string& name) const {
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i].name == name) return static_cast<int>(i);
        }
        return -1;
    }
};

// Database schema
struct DatabaseSchema {
    std::string name;
    std::vector<TableDef> tables;

    const TableDef* getTable(const std::string& name) const {
        for (const auto& t : tables) {
            if (t.name == name) return &t;
        }
        return nullptr;
    }
};

// Record metadata (derived from raw FlatBuffer stream, not stored separately)
struct RecordHeader {
    uint64_t sequence;      // Assigned during ingest
    std::string fileId;     // 4-byte file identifier from FlatBuffer (bytes 4-7)
    uint32_t dataLength;    // Size of FlatBuffer (from size prefix)
};

// A stored record
struct StoredRecord {
    RecordHeader header;
    uint64_t offset;
    std::vector<uint8_t> data;
};

// Query result
struct QueryResult {
    std::vector<std::string> columns;
    std::vector<std::vector<Value>> rows;

    size_t rowCount() const { return rows.size(); }
    size_t columnCount() const { return columns.size(); }
};

// Index entry - points to a record in storage
struct IndexEntry {
    Value key;
    uint64_t dataOffset;
    uint32_t dataLength;
    uint64_t sequence;
};

// CRC32 checksum
uint32_t crc32(const uint8_t* data, size_t length);
uint32_t crc32(const std::vector<uint8_t>& data);

}  // namespace flatsql

#endif  // FLATSQL_TYPES_H
