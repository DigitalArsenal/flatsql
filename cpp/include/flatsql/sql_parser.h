#ifndef FLATSQL_SQL_PARSER_H
#define FLATSQL_SQL_PARSER_H

#include "flatsql/types.h"

namespace flatsql {

// SQL statement types
enum class SQLStatementType {
    Select,
    Insert,
    CreateTable,
    Unknown
};

// WHERE clause condition
struct WhereCondition {
    std::string column;
    std::string op;  // =, !=, <, >, <=, >=, BETWEEN, LIKE
    Value value;
    Value value2;    // for BETWEEN
    bool hasBetween = false;
};

// Parsed SQL query
struct ParsedSQL {
    SQLStatementType type = SQLStatementType::Unknown;
    std::string tableName;
    std::vector<std::string> columns;  // SELECT columns or INSERT columns
    std::optional<WhereCondition> where;
    std::vector<Value> insertValues;   // for INSERT
    std::optional<std::string> orderBy;
    bool orderDesc = false;
    std::optional<int> limit;
};

/**
 * Simple SQL parser for basic queries.
 * Supports:
 * - SELECT columns FROM table [WHERE condition] [ORDER BY col [DESC]] [LIMIT n]
 * - INSERT INTO table (columns) VALUES (values)
 */
class SQLParser {
public:
    static ParsedSQL parse(const std::string& sql);

private:
    static ParsedSQL parseSelect(const std::string& sql);
    static ParsedSQL parseInsert(const std::string& sql);
    static Value parseValue(const std::string& str);
    static std::string trim(const std::string& str);
    static std::vector<std::string> split(const std::string& str, char delim);
    static std::string toUpper(const std::string& str);
};

}  // namespace flatsql

#endif  // FLATSQL_SQL_PARSER_H
