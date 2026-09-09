#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

namespace flatsql {
// Canonical BFBS describes the complete record independently of SQL columns.
// Both inputs are untrusted. Failure never returns partially extracted text.
bool reflectedRecordSearchText(const uint8_t* schema, size_t schemaLength,
                               const std::string& fileId,
                               const uint8_t* record, size_t recordLength,
                               std::string& output, std::string* error);
}
