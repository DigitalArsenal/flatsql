#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace flatsql {
// Offset of the root-offset word in a bare or size-prefixed record.
// This selects a view; the stored record and its content address are unchanged.
inline size_t recordRootOffset(const uint8_t* record, size_t length, const uint8_t fid[4]) {
    if (record && length >= 12 &&
        (uint32_t(record[0]) | uint32_t(record[1]) << 8 | uint32_t(record[2]) << 16 | uint32_t(record[3]) << 24) == length - 4 &&
        std::memcmp(record + 8, fid, 4) == 0)
        return 4;
    return 0;
}
}  // namespace flatsql
