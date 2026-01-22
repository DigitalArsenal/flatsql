#include "flatsql/database.h"

#ifdef __EMSCRIPTEN__
#include <emscripten/bind.h>
#include <emscripten/val.h>

using namespace emscripten;

namespace flatsql {

// JavaScript-friendly wrapper for Value
val valueToJS(const Value& v) {
    return std::visit([](const auto& value) -> val {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return val::null();
        } else if constexpr (std::is_same_v<T, bool>) {
            return val(value);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return val(value);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            // Return as Uint8Array
            return val(typed_memory_view(value.size(), value.data()));
        } else {
            // Numeric types
            return val(static_cast<double>(value));
        }
    }, v);
}

// JavaScript-friendly wrapper for QueryResult
struct JSQueryResult {
    std::vector<std::string> columns;
    std::vector<std::vector<val>> rows;
    size_t rowCount;

    JSQueryResult(const QueryResult& result) : columns(result.columns), rowCount(result.rowCount()) {
        for (const auto& row : result.rows) {
            std::vector<val> jsRow;
            for (const auto& cell : row) {
                jsRow.push_back(valueToJS(cell));
            }
            rows.push_back(std::move(jsRow));
        }
    }

    val getColumns() const {
        val arr = val::array();
        for (const auto& col : columns) {
            arr.call<void>("push", val(col));
        }
        return arr;
    }

    val getRows() const {
        val arr = val::array();
        for (const auto& row : rows) {
            val jsRow = val::array();
            for (const auto& cell : row) {
                jsRow.call<void>("push", cell);
            }
            arr.call<void>("push", jsRow);
        }
        return arr;
    }

    size_t getRowCount() const { return rowCount; }
};

// JavaScript-friendly wrapper for FlatSQLDatabase
class JSFlatSQLDatabase {
public:
    JSFlatSQLDatabase(const std::string& schemaSource, const std::string& dbName = "default")
        : db_(FlatSQLDatabase::fromSchema(schemaSource, dbName)) {}

    JSQueryResult query(const std::string& sql) {
        return JSQueryResult(db_.query(sql));
    }

    double insertRawVec(const std::string& tableName, const std::vector<uint8_t>& data) {
        return static_cast<double>(db_.insertRaw(tableName, data));
    }

    val streamVec(const std::string& tableName, const std::vector<std::vector<uint8_t>>& flatbuffers) {
        std::vector<uint64_t> rowids = db_.stream(tableName, flatbuffers);
        val result = val::array();
        for (uint64_t rowid : rowids) {
            result.call<void>("push", val(static_cast<double>(rowid)));
        }
        return result;
    }

    val exportData() {
        std::vector<uint8_t> data = db_.exportData();
        // Copy to a new array that persists
        val result = val::global("Uint8Array").new_(data.size());
        val memView = val(typed_memory_view(data.size(), data.data()));
        result.call<void>("set", memView);
        return result;
    }

    val listTables() {
        std::vector<std::string> tables = db_.listTables();
        val result = val::array();
        for (const auto& t : tables) {
            result.call<void>("push", val(t));
        }
        return result;
    }

    val getStats() {
        auto stats = db_.getStats();
        val result = val::array();
        for (const auto& s : stats) {
            val stat = val::object();
            stat.set("tableName", val(s.tableName));
            stat.set("recordCount", val(static_cast<double>(s.recordCount)));
            val indexes = val::array();
            for (const auto& idx : s.indexes) {
                indexes.call<void>("push", val(idx));
            }
            stat.set("indexes", indexes);
            result.call<void>("push", stat);
        }
        return result;
    }

private:
    FlatSQLDatabase db_;
};

EMSCRIPTEN_BINDINGS(flatsql) {
    register_vector<uint8_t>("VectorUint8");
    register_vector<std::vector<uint8_t>>("VectorVectorUint8");

    class_<JSQueryResult>("QueryResult")
        .function("getColumns", &JSQueryResult::getColumns)
        .function("getRows", &JSQueryResult::getRows)
        .function("getRowCount", &JSQueryResult::getRowCount)
        ;

    class_<JSFlatSQLDatabase>("FlatSQLDatabase")
        .constructor<const std::string&>()
        .constructor<const std::string&, const std::string&>()
        .function("query", &JSFlatSQLDatabase::query)
        .function("insertRaw", &JSFlatSQLDatabase::insertRawVec)
        .function("stream", &JSFlatSQLDatabase::streamVec)
        .function("exportData", &JSFlatSQLDatabase::exportData)
        .function("listTables", &JSFlatSQLDatabase::listTables)
        .function("getStats", &JSFlatSQLDatabase::getStats)
        ;
}

}  // namespace flatsql

#else
// Native build - no WASM bindings
namespace flatsql {
// Can add native CLI or testing code here
}
#endif
