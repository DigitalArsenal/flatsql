// Arena capacity cap (StreamingFlatBufferStore): an ingest past the cap is
// refused, never a trap in the arena's resize (the no-EH wasm build turns a
// failed allocation into `unreachable`, poisoning the instance).
//
// Computable outcomes only: which ingests are accepted or refused, and the
// row count afterwards. The C API's -1 / "arena capacity exhausted" answer is
// checked against the built artifact by wasm/test-arena-limit.mjs.

#include "flatsql/database.h"
#include <flatbuffers/flatbuffers.h>
#include <iostream>
#include <string>
#include <variant>
#include <vector>

using namespace flatsql;

namespace {

int g_failures = 0;

#define CHECK(cond)                                                                        \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            g_failures++;                                                                  \
            std::cerr << "  FAIL " << __FILE__ << ":" << __LINE__ << ": " #cond << std::endl; \
        }                                                                                  \
    } while (0)

const char* kSchema = R"(
    table User {
        id: int (id);
        name: string;
        email: string (key);
        age: int;
    }
    root_type User;
)";

std::vector<uint8_t> user(int32_t id) {
    flatbuffers::FlatBufferBuilder b(128);
    const auto name = b.CreateString("user-" + std::to_string(id));
    const auto email = b.CreateString("u" + std::to_string(id) + "@example.invalid");
    const auto start = b.StartTable();
    b.AddElement<int32_t>(4, id, 0);
    b.AddOffset(6, name);
    b.AddOffset(8, email);
    b.AddElement<int32_t>(10, 30, 0);
    b.Finish(flatbuffers::Offset<flatbuffers::Table>(b.EndTable(start)), "USER");
    return std::vector<uint8_t>(b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize());
}

int64_t countUsers(FlatSQLDatabase& db) {
    QueryResult result;
    std::string error;
    if (!db.queryNoThrow("SELECT COUNT(*) FROM User", {}, result, &error) || result.rows.empty()) return -1;
    const Value& v = result.rows[0][0];
    if (std::holds_alternative<int64_t>(v)) return std::get<int64_t>(v);
    if (std::holds_alternative<int32_t>(v)) return std::get<int32_t>(v);
    if (std::holds_alternative<double>(v)) return static_cast<int64_t>(std::get<double>(v));
    return -1;
}

}  // namespace

int main() {
    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(kSchema, "arena-limit");
    db.registerFileId("USER", "User");
    db.setArenaLimit(8192);

    // ingestOne: accepted until the next record would pass the cap.
    int accepted = 0;
    int id = 1;
    for (; id <= 1000; id++) {
        const auto rec = user(id);
        if (db.ingestOne(rec.data(), rec.size()) == FlatSQLDatabase::kIngestRefused) break;
        accepted++;
    }
    CHECK(accepted > 0);
    CHECK(id <= 1000);  // refused before the end
    {
        const auto rec = user(5000);
        CHECK(db.ingestOne(rec.data(), rec.size()) == FlatSQLDatabase::kIngestRefused);
        CHECK(db.ingestOneWithSource(rec.data(), rec.size(), "src") == FlatSQLDatabase::kIngestRefused);
    }
    CHECK(countUsers(db) == accepted);

    // Stream ingest: a record past the cap is not consumed.
    std::vector<uint8_t> stream;
    for (int i = 0; i < 50; i++) {
        const auto rec = user(10000 + i);
        const uint32_t n = static_cast<uint32_t>(rec.size());
        const uint8_t prefix[4] = {uint8_t(n), uint8_t(n >> 8), uint8_t(n >> 16), uint8_t(n >> 24)};
        stream.insert(stream.end(), prefix, prefix + 4);
        stream.insert(stream.end(), rec.begin(), rec.end());
    }
    size_t records = 0;
    const size_t consumed = db.ingest(stream.data(), stream.size(), &records);
    CHECK(consumed < stream.size());
    CHECK(records == 0);
    CHECK(countUsers(db) == accepted);

    // A higher cap: ingest resumes.
    db.setArenaLimit(size_t(16) << 20);
    const auto rec = user(20000);
    CHECK(db.ingestOne(rec.data(), rec.size()) != FlatSQLDatabase::kIngestRefused);
    CHECK(db.ingest(stream.data(), stream.size(), &records) == stream.size());
    CHECK(countUsers(db) == accepted + 1 + 50);

    if (g_failures) {
        std::cerr << g_failures << " failures" << std::endl;
        return 1;
    }
    std::cout << "arena limit: " << accepted << " records under an 8 KiB cap, refusals clean" << std::endl;
    return 0;
}
