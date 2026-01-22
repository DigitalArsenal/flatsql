#include "flatsql/schema_parser.h"
#include <regex>
#include <sstream>
#include <algorithm>
#include <stdexcept>

namespace flatsql {

// Trim whitespace
static std::string trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = str.find_last_not_of(" \t\r\n");
    return str.substr(start, end - start + 1);
}

// Convert to lowercase
static std::string toLower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

ValueType SchemaParser::idlTypeToValueType(const std::string& idlType) {
    std::string type = toLower(trim(idlType));

    if (type == "bool") return ValueType::Bool;
    if (type == "byte" || type == "int8") return ValueType::Int8;
    if (type == "ubyte" || type == "uint8") return ValueType::UInt8;
    if (type == "short" || type == "int16") return ValueType::Int16;
    if (type == "ushort" || type == "uint16") return ValueType::UInt16;
    if (type == "int" || type == "int32") return ValueType::Int32;
    if (type == "uint" || type == "uint32") return ValueType::UInt32;
    if (type == "long" || type == "int64") return ValueType::Int64;
    if (type == "ulong" || type == "uint64") return ValueType::UInt64;
    if (type == "float" || type == "float32") return ValueType::Float32;
    if (type == "double" || type == "float64") return ValueType::Float64;
    if (type == "string") return ValueType::String;
    if (type.find("[ubyte]") != std::string::npos ||
        type.find("[uint8]") != std::string::npos ||
        type.find("[byte]") != std::string::npos) {
        return ValueType::Bytes;
    }

    // Default to string for unknown types
    return ValueType::String;
}

ValueType SchemaParser::jsonTypeToValueType(const std::string& jsonType, const std::string& format) {
    std::string type = toLower(trim(jsonType));

    if (type == "boolean") return ValueType::Bool;
    if (type == "integer") {
        if (format == "int8") return ValueType::Int8;
        if (format == "int16") return ValueType::Int16;
        if (format == "int64") return ValueType::Int64;
        return ValueType::Int32;
    }
    if (type == "number") {
        if (format == "float") return ValueType::Float32;
        return ValueType::Float64;
    }
    if (type == "string") return ValueType::String;
    if (type == "array") return ValueType::Bytes;  // Assume byte array

    return ValueType::String;
}

DatabaseSchema SchemaParser::parseIDL(const std::string& idl, const std::string& dbName) {
    DatabaseSchema schema;
    schema.name = dbName;

    // Match table definitions: table TableName { ... }
    std::regex tableRegex(R"delim(table\s+(\w+)\s*\{([^}]*)\})delim", std::regex::icase);
    std::smatch tableMatch;

    std::string remaining = idl;
    while (std::regex_search(remaining, tableMatch, tableRegex)) {
        TableDef tableDef;
        tableDef.name = tableMatch[1].str();

        std::string fieldsStr = tableMatch[2].str();

        // Parse fields: fieldName:type;
        std::regex fieldRegex(R"delim((\w+)\s*:\s*([^;]+);)delim");
        std::smatch fieldMatch;

        std::string fieldsRemaining = fieldsStr;
        while (std::regex_search(fieldsRemaining, fieldMatch, fieldRegex)) {
            ColumnDef col;
            col.name = trim(fieldMatch[1].str());

            std::string typeStr = trim(fieldMatch[2].str());

            // Check for attributes (id, required, etc.)
            std::regex attrRegex(R"delim(\(([^)]+)\))delim");
            std::smatch attrMatch;
            if (std::regex_search(typeStr, attrMatch, attrRegex)) {
                std::string attrs = toLower(attrMatch[1].str());
                if (attrs.find("id") != std::string::npos) {
                    col.primaryKey = true;
                    col.indexed = true;
                }
                if (attrs.find("required") != std::string::npos) {
                    col.nullable = false;
                }
                if (attrs.find("key") != std::string::npos ||
                    attrs.find("index") != std::string::npos) {
                    col.indexed = true;
                }
                // Remove attributes from type
                typeStr = std::regex_replace(typeStr, attrRegex, "");
                typeStr = trim(typeStr);
            }

            col.type = idlTypeToValueType(typeStr);
            tableDef.columns.push_back(col);

            if (col.primaryKey) {
                tableDef.primaryKeyColumns.push_back(col.name);
            }

            fieldsRemaining = fieldMatch.suffix().str();
        }

        schema.tables.push_back(tableDef);
        remaining = tableMatch.suffix().str();
    }

    return schema;
}

DatabaseSchema SchemaParser::parseJSONSchema(const std::string& json, const std::string& dbName) {
    DatabaseSchema schema;
    schema.name = dbName;

    // Simple JSON parsing for schema
    // This is a simplified parser - for production use a proper JSON library

    // Try to find table name
    std::regex tableNameRegex("\"name\"\\s*:\\s*\"([^\"]+)\"");
    std::smatch match;

    TableDef tableDef;
    tableDef.name = "default";

    if (std::regex_search(json, match, tableNameRegex)) {
        tableDef.name = match[1].str();
    }

    // Parse properties for columns
    std::regex propsRegex("\"properties\"\\s*:\\s*\\{");
    if (std::regex_search(json, match, propsRegex)) {
        // Find the properties object - simple extraction
        size_t propsStart = match.position() + match.length();
        int braceCount = 1;
        size_t propsEnd = propsStart;

        for (size_t i = propsStart; i < json.length() && braceCount > 0; i++) {
            if (json[i] == '{') braceCount++;
            if (json[i] == '}') braceCount--;
            propsEnd = i;
        }

        std::string propsStr = json.substr(propsStart, propsEnd - propsStart);

        // Extract property definitions
        std::regex propRegex("\"(\\w+)\"\\s*:\\s*\\{[^}]*\"type\"\\s*:\\s*\"(\\w+)\"");
        std::smatch propMatch;
        std::string propsRemaining = propsStr;

        while (std::regex_search(propsRemaining, propMatch, propRegex)) {
            ColumnDef col;
            col.name = propMatch[1].str();
            col.type = jsonTypeToValueType(propMatch[2].str());
            tableDef.columns.push_back(col);
            propsRemaining = propMatch.suffix().str();
        }
    }

    if (!tableDef.columns.empty()) {
        schema.tables.push_back(tableDef);
    }

    return schema;
}

DatabaseSchema SchemaParser::parse(const std::string& source, const std::string& dbName) {
    std::string trimmed = trim(source);

    // Detect format
    if (trimmed.empty()) {
        throw std::runtime_error("Empty schema source");
    }

    // JSON starts with {
    if (trimmed[0] == '{') {
        return parseJSONSchema(source, dbName);
    }

    // Otherwise assume IDL
    return parseIDL(source, dbName);
}

}  // namespace flatsql
