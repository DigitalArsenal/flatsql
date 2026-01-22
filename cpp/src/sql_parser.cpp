#include "flatsql/sql_parser.h"
#include <regex>
#include <sstream>
#include <algorithm>
#include <stdexcept>
#include <cctype>

namespace flatsql {

std::string SQLParser::trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = str.find_last_not_of(" \t\r\n");
    return str.substr(start, end - start + 1);
}

std::string SQLParser::toUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

std::vector<std::string> SQLParser::split(const std::string& str, char delim) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delim)) {
        std::string trimmed = trim(token);
        if (!trimmed.empty()) {
            tokens.push_back(trimmed);
        }
    }
    return tokens;
}

Value SQLParser::parseValue(const std::string& str) {
    std::string s = trim(str);

    if (s.empty()) {
        return std::monostate{};  // null
    }

    // String literal (single or double quotes)
    if ((s.front() == '\'' && s.back() == '\'') ||
        (s.front() == '"' && s.back() == '"')) {
        return s.substr(1, s.length() - 2);
    }

    // Boolean
    std::string upper = toUpper(s);
    if (upper == "TRUE") return true;
    if (upper == "FALSE") return false;
    if (upper == "NULL") return std::monostate{};

    // Number
    bool hasDecimal = (s.find('.') != std::string::npos);
    bool isNegative = !s.empty() && s[0] == '-';

    try {
        if (hasDecimal) {
            return std::stod(s);
        } else if (isNegative) {
            int64_t val = std::stoll(s);
            if (val >= INT32_MIN && val <= INT32_MAX) {
                return static_cast<int32_t>(val);
            }
            return val;
        } else {
            uint64_t val = std::stoull(s);
            if (val <= INT32_MAX) {
                return static_cast<int32_t>(val);
            } else if (val <= INT64_MAX) {
                return static_cast<int64_t>(val);
            }
            return val;
        }
    } catch (...) {
        // Not a number, treat as string
        return s;
    }
}

ParsedSQL SQLParser::parse(const std::string& sql) {
    std::string normalized = trim(sql);

    // Remove trailing semicolon
    if (!normalized.empty() && normalized.back() == ';') {
        normalized.pop_back();
        normalized = trim(normalized);
    }

    // Normalize whitespace
    std::regex multiSpace(R"(\s+)");
    normalized = std::regex_replace(normalized, multiSpace, " ");

    std::string upper = toUpper(normalized);

    if (upper.substr(0, 6) == "SELECT") {
        return parseSelect(normalized);
    } else if (upper.substr(0, 6) == "INSERT") {
        return parseInsert(normalized);
    }

    ParsedSQL result;
    result.type = SQLStatementType::Unknown;
    return result;
}

ParsedSQL SQLParser::parseSelect(const std::string& sql) {
    ParsedSQL result;
    result.type = SQLStatementType::Select;

    std::string upper = toUpper(sql);

    // Extract columns
    size_t selectPos = upper.find("SELECT");
    size_t fromPos = upper.find("FROM");
    if (selectPos == std::string::npos || fromPos == std::string::npos) {
        throw std::runtime_error("Invalid SELECT syntax");
    }

    std::string columnsStr = trim(sql.substr(selectPos + 6, fromPos - selectPos - 6));
    result.columns = split(columnsStr, ',');

    // Extract table name
    size_t tableStart = fromPos + 4;
    size_t tableEnd = sql.length();

    // Find end of table name (WHERE, ORDER, LIMIT, or end)
    size_t wherePos = upper.find("WHERE");
    size_t orderPos = upper.find("ORDER");
    size_t limitPos = upper.find("LIMIT");

    if (wherePos != std::string::npos) tableEnd = std::min(tableEnd, wherePos);
    if (orderPos != std::string::npos) tableEnd = std::min(tableEnd, orderPos);
    if (limitPos != std::string::npos) tableEnd = std::min(tableEnd, limitPos);

    result.tableName = trim(sql.substr(tableStart, tableEnd - tableStart));

    // Parse WHERE clause
    if (wherePos != std::string::npos) {
        size_t whereEnd = sql.length();
        if (orderPos != std::string::npos) whereEnd = std::min(whereEnd, orderPos);
        if (limitPos != std::string::npos) whereEnd = std::min(whereEnd, limitPos);

        std::string whereStr = trim(sql.substr(wherePos + 5, whereEnd - wherePos - 5));
        std::string whereUpper = toUpper(whereStr);

        WhereCondition cond;

        // Check for BETWEEN
        size_t betweenPos = whereUpper.find("BETWEEN");
        if (betweenPos != std::string::npos) {
            cond.column = trim(whereStr.substr(0, betweenPos));
            cond.op = "BETWEEN";
            cond.hasBetween = true;

            size_t andPos = whereUpper.find("AND", betweenPos);
            if (andPos != std::string::npos) {
                std::string val1Str = trim(whereStr.substr(betweenPos + 7, andPos - betweenPos - 7));
                std::string val2Str = trim(whereStr.substr(andPos + 3));
                cond.value = parseValue(val1Str);
                cond.value2 = parseValue(val2Str);
            }
        } else {
            // Parse simple condition: column op value
            std::regex condRegex(R"((\w+)\s*(>=|<=|!=|<>|=|<|>)\s*(.+))");
            std::smatch match;
            if (std::regex_match(whereStr, match, condRegex)) {
                cond.column = match[1].str();
                cond.op = match[2].str();
                if (cond.op == "<>") cond.op = "!=";
                cond.value = parseValue(match[3].str());
            }
        }

        result.where = cond;
    }

    // Parse ORDER BY
    if (orderPos != std::string::npos) {
        size_t orderEnd = sql.length();
        if (limitPos != std::string::npos) orderEnd = limitPos;

        std::string orderStr = trim(sql.substr(orderPos + 8, orderEnd - orderPos - 8));
        std::string orderUpper = toUpper(orderStr);

        size_t descPos = orderUpper.find("DESC");
        size_t ascPos = orderUpper.find("ASC");

        if (descPos != std::string::npos) {
            result.orderBy = trim(orderStr.substr(0, descPos));
            result.orderDesc = true;
        } else if (ascPos != std::string::npos) {
            result.orderBy = trim(orderStr.substr(0, ascPos));
            result.orderDesc = false;
        } else {
            result.orderBy = orderStr;
            result.orderDesc = false;
        }
    }

    // Parse LIMIT
    if (limitPos != std::string::npos) {
        std::string limitStr = trim(sql.substr(limitPos + 5));
        try {
            result.limit = std::stoi(limitStr);
        } catch (...) {
            // Invalid limit
        }
    }

    return result;
}

ParsedSQL SQLParser::parseInsert(const std::string& sql) {
    ParsedSQL result;
    result.type = SQLStatementType::Insert;

    std::string upper = toUpper(sql);

    // INSERT INTO table (columns) VALUES (values)
    std::regex insertRegex(R"(INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\))",
                           std::regex::icase);
    std::smatch match;

    if (std::regex_match(sql, match, insertRegex)) {
        result.tableName = match[1].str();
        result.columns = split(match[2].str(), ',');

        std::vector<std::string> valueStrs = split(match[3].str(), ',');
        for (const auto& vs : valueStrs) {
            result.insertValues.push_back(parseValue(vs));
        }
    } else {
        throw std::runtime_error("Invalid INSERT syntax");
    }

    return result;
}

}  // namespace flatsql
