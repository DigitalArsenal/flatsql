#ifndef FLATSQL_SCHEMA_PARSER_H
#define FLATSQL_SCHEMA_PARSER_H

#include "flatsql/types.h"

namespace flatsql {

/**
 * Parse schema definitions from various formats.
 * Supports:
 * - FlatBuffers IDL (.fbs)
 * - JSON Schema
 */
class SchemaParser {
public:
    // Parse FlatBuffers IDL schema
    static DatabaseSchema parseIDL(const std::string& idl, const std::string& dbName = "default");

    // Parse JSON schema
    static DatabaseSchema parseJSONSchema(const std::string& json, const std::string& dbName = "default");

    // Auto-detect format and parse
    static DatabaseSchema parse(const std::string& source, const std::string& dbName = "default");

private:
    static ValueType idlTypeToValueType(const std::string& idlType);
    static ValueType jsonTypeToValueType(const std::string& jsonType, const std::string& format = "");
};

}  // namespace flatsql

#endif  // FLATSQL_SCHEMA_PARSER_H
