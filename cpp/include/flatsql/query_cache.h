#ifndef FLATSQL_QUERY_CACHE_H
#define FLATSQL_QUERY_CACHE_H

#include "flatsql/types.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace flatsql {

std::string bytesToHex(const uint8_t* data, size_t length);
std::string stringToHex(const std::string& value);
std::string encodeValueForCacheKey(const Value& value);
std::string buildQueryCacheKey(const std::string& dataset,
                               const std::string& artifactVersion,
                               const std::string& queryId,
                               const std::vector<Value>& params);
std::string buildResponseArtifactCacheKey(const std::string& schemaName,
                                          const std::string& schemaVersion,
                                          const std::string& sql,
                                          const std::string& format,
                                          const std::string& publishEventKey,
                                          const std::vector<std::string>& projection,
                                          const std::vector<Value>& params);

}  // namespace flatsql

#endif  // FLATSQL_QUERY_CACHE_H
