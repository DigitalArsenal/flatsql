#include "flatsql/query_cache.h"
#include <cctype>
#include <iomanip>
#include <sstream>
#include <type_traits>

namespace flatsql {

std::string bytesToHex(const uint8_t* data, size_t length) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string out;
    out.reserve(length * 2);
    for (size_t index = 0; index < length; index++) {
        const uint8_t byte = data[index];
        out.push_back(digits[byte >> 4]);
        out.push_back(digits[byte & 0x0F]);
    }
    return out;
}

std::string stringToHex(const std::string& value) {
    return bytesToHex(reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

std::string encodeValueForCacheKey(const Value& value) {
    return std::visit([](const auto& v) -> std::string {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return "n";
        } else if constexpr (std::is_same_v<T, bool>) {
            return v ? "b=1" : "b=0";
        } else if constexpr (
            std::is_same_v<T, int8_t> ||
            std::is_same_v<T, int16_t> ||
            std::is_same_v<T, int32_t> ||
            std::is_same_v<T, int64_t>
        ) {
            return "i=" + std::to_string(static_cast<int64_t>(v));
        } else if constexpr (
            std::is_same_v<T, uint8_t> ||
            std::is_same_v<T, uint16_t> ||
            std::is_same_v<T, uint32_t> ||
            std::is_same_v<T, uint64_t>
        ) {
            return "u=" + std::to_string(static_cast<uint64_t>(v));
        } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
            std::ostringstream stream;
            stream << std::setprecision(17) << static_cast<double>(v);
            return "f=" + stream.str();
        } else if constexpr (std::is_same_v<T, std::string>) {
            return "s=" + stringToHex(v);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return "x=" + bytesToHex(v.data(), v.size());
        }
        return "n";
    }, value);
}

std::string buildQueryCacheKey(const std::string& dataset,
                               const std::string& artifactVersion,
                               const std::string& queryId,
                               const std::vector<Value>& params) {
    std::string key = "flatsql:v1|d=" + stringToHex(dataset) +
                      "|a=" + stringToHex(artifactVersion) +
                      "|q=" + stringToHex(queryId) +
                      "|p=" + std::to_string(params.size());
    for (const auto& param : params) {
        key.push_back(':');
        key += encodeValueForCacheKey(param);
    }
    return key;
}

namespace {

std::string normalizeSqlForCacheKey(const std::string& sql) {
    std::string normalized;
    normalized.reserve(sql.size());
    bool pendingSpace = false;

    for (unsigned char ch : sql) {
        if (std::isspace(ch)) {
            if (!normalized.empty()) {
                pendingSpace = true;
            }
            continue;
        }

        if (pendingSpace) {
            normalized.push_back(' ');
            pendingSpace = false;
        }
        normalized.push_back(static_cast<char>(ch));
    }

    return normalized;
}

}  // namespace

std::string buildResponseArtifactCacheKey(const std::string& schemaName,
                                          const std::string& schemaVersion,
                                          const std::string& sql,
                                          const std::string& format,
                                          const std::string& publishEventKey,
                                          const std::vector<std::string>& projection,
                                          const std::vector<Value>& params) {
    std::string key = "flatsql:response:v1|s=" + stringToHex(schemaName) +
                      "|v=" + stringToHex(schemaVersion) +
                      "|f=" + stringToHex(format) +
                      "|e=" + stringToHex(publishEventKey) +
                      "|q=" + stringToHex(normalizeSqlForCacheKey(sql)) +
                      "|c=" + std::to_string(projection.size());

    for (const auto& column : projection) {
        key.push_back(':');
        key += stringToHex(column);
    }

    key += "|p=" + std::to_string(params.size());
    for (const auto& param : params) {
        key.push_back(':');
        key += encodeValueForCacheKey(param);
    }

    return key;
}

}  // namespace flatsql
