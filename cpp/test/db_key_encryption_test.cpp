// Database-key record encryption: FlatBuffers field-encryption format 3 with
// the record's sequence as its record index (database.h, sqlite_vtab.cpp).
//
// Computable outcomes only: the stored bytes against EncryptBuffer's format-3
// output for the record's sequence, pairwise-distinct ciphertexts across many
// records of one layout, the values SQL reads back, and the refusals.

#include "flatsql/database.h"
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/idl.h>
#include <cstring>
#include <iostream>
#include <memory>
#include <set>
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

// The same text is the database schema and the FlatBuffers schema.
const char* kSchema = R"(
    table Secret {
        serial: int (key);
        name: string;
        pin: long (encrypted);
        code: string (encrypted);
        score: double (encrypted);
        blob: [ubyte] (encrypted);
        note: string;
        flag: bool (encrypted);
        alias: string (encrypted);
    }
    root_type Secret;
    file_identifier "SECR";
)";

// `note` marked (encrypted) too: a binary schema that disagrees.
const char* kOtherSchema = R"(
    table Secret {
        serial: int (key);
        name: string;
        pin: long (encrypted);
        code: string (encrypted);
        score: double (encrypted);
        blob: [ubyte] (encrypted);
        note: string (encrypted);
        flag: bool (encrypted);
        alias: string (encrypted);
    }
    root_type Secret;
    file_identifier "SECR";
)";

// Vtable slots (4 + 2 * field id)
constexpr flatbuffers::voffset_t kSerial = 4, kName = 6, kPin = 8, kCode = 10, kScore = 12,
                                 kBlob = 14, kNote = 16, kFlag = 18, kAlias = 20;

struct Plain {
    int32_t serial = 1;
    std::string name = "public name";
    int64_t pin = 1234567890123LL;
    std::string code = "identical secret";
    double score = 3.25;
    std::vector<uint8_t> blob = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::string note = "public note";
    bool flag = true;
    std::string alias = "another secret";
    bool sharedAlias = false;  // alias refers to code's string
};

std::vector<uint8_t> build(const Plain& p) {
    flatbuffers::FlatBufferBuilder b(256);
    b.ForceDefaults(true);
    const auto name = b.CreateString(p.name);
    const auto code = b.CreateString(p.code);
    const auto alias = p.sharedAlias ? code : b.CreateString(p.alias);
    const auto blob = b.CreateVector(p.blob);
    const auto note = b.CreateString(p.note);
    const auto start = b.StartTable();
    b.AddElement<int32_t>(kSerial, p.serial, 0);
    b.AddOffset(kName, name);
    b.AddElement<int64_t>(kPin, p.pin, 0);
    b.AddOffset(kCode, code);
    b.AddElement<double>(kScore, p.score, 0.0);
    b.AddOffset(kBlob, blob);
    b.AddOffset(kNote, note);
    b.AddElement<uint8_t>(kFlag, p.flag ? 1 : 0, 0);
    b.AddOffset(kAlias, alias);
    b.Finish(flatbuffers::Offset<flatbuffers::Table>(b.EndTable(start)), "SECR");
    return std::vector<uint8_t>(b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize());
}

std::vector<uint8_t> binarySchema(const char* text, bool builtins) {
    flatbuffers::IDLOptions opts;
    opts.binary_schema_builtins = builtins;
    flatbuffers::Parser parser(opts);
    if (!parser.Parse(text)) {
        std::cerr << "schema parse failed: " << parser.error_ << std::endl;
        std::abort();
    }
    parser.Serialize();
    return std::vector<uint8_t>(parser.builder_.GetBufferPointer(),
                                parser.builder_.GetBufferPointer() + parser.builder_.GetSize());
}

const flatbuffers::Table* root(const std::vector<uint8_t>& record) {
    return flatbuffers::GetRoot<flatbuffers::Table>(record.data());
}

std::string stringAt(const std::vector<uint8_t>& record, flatbuffers::voffset_t slot) {
    const auto* s = root(record)->GetPointer<const flatbuffers::String*>(slot);
    return s ? s->str() : std::string();
}

int64_t asInt(const Value& v) {
    if (auto* i = std::get_if<int64_t>(&v)) return *i;
    if (auto* i = std::get_if<int32_t>(&v)) return *i;
    return -1;
}

std::vector<uint8_t> asBytes(const Value& v) {
    if (auto* b = std::get_if<std::vector<uint8_t>>(&v)) return *b;
    return {};
}

std::string asText(const Value& v) {
    if (auto* s = std::get_if<std::string>(&v)) return *s;
    return "<not text>";
}

double asReal(const Value& v) {
    if (auto* d = std::get_if<double>(&v)) return *d;
    return -1;
}

QueryResult query(FlatSQLDatabase& db, const std::string& sql) {
    QueryResult result;
    std::string error;
    if (!db.queryNoThrow(sql, {}, result, &error)) {
        std::cerr << "  query failed: " << sql << ": " << error << std::endl;
        g_failures++;
    }
    return result;
}

// Every row of `sql` (serial, name, pin, code, score, blob, note, flag, alias
// in that order) reads back the plaintext of `plain`.
void checkRows(FlatSQLDatabase& db, const std::string& sql, const std::vector<Plain>& plain) {
    const QueryResult result = query(db, sql);
    CHECK(result.rows.size() == plain.size());
    for (size_t i = 0; i < result.rows.size() && i < plain.size(); i++) {
        const auto& row = result.rows[i];
        const Plain& p = plain[i];
        CHECK(asInt(row[0]) == p.serial);
        CHECK(asText(row[1]) == p.name);
        CHECK(asInt(row[2]) == p.pin);
        CHECK(asText(row[3]) == p.code);
        CHECK(asReal(row[4]) == p.score);
        CHECK(asBytes(row[5]) == p.blob);
        CHECK(asText(row[6]) == p.note);
        CHECK(asInt(row[7]) == (p.flag ? 1 : 0));
        CHECK(asText(row[8]) == (p.sharedAlias ? p.code : p.alias));
    }
}

const char* kColumns = "serial, name, pin, code, score, blob, note, flag, alias";

std::vector<std::pair<uint64_t, std::vector<uint8_t>>> storedRecords(FlatSQLDatabase& db,
                                                                    const std::string& table) {
    std::vector<std::pair<uint64_t, std::vector<uint8_t>>> out;
    db.iterateAll(table, [&](const uint8_t* data, uint32_t length, uint64_t sequence) {
        out.emplace_back(sequence, std::vector<uint8_t>(data, data + length));
    });
    return out;
}

std::unique_ptr<FlatSQLDatabase> openDatabase() {
    auto db = std::make_unique<FlatSQLDatabase>(SchemaParser::parse(kSchema, "db-key"));
    db->registerFileId("SECR", "Secret");
    return db;
}

void keyBytes(uint8_t* key) {
    for (int i = 0; i < 32; i++) key[i] = static_cast<uint8_t>(0xA0 + i);
}

bool contains(const std::string& text, const char* part) {
    return text.find(part) != std::string::npos;
}

void unavailable() {
    // The fallback backend: a database with (encrypted) columns refuses a key.
    uint8_t key[32];
    keyBytes(key);
    auto owned = openDatabase();
    FlatSQLDatabase& db = *owned;
    std::string error;
    CHECK(!db.setEncryptionKey(key, 32, FlatSQLDatabase::kRecordFormatUndeclared, &error));
    CHECK(contains(error, "HKDF-SHA256"));
    CHECK(!db.isEncrypted());
    // A schema without (encrypted) columns still takes a key (HMAC).
    FlatSQLDatabase plain = FlatSQLDatabase::fromSchema("table P { a: int; } root_type P;", "p");
    CHECK(plain.setEncryptionKey(key, 32, FlatSQLDatabase::kRecordFormatUndeclared, &error));
}

void available() {
    uint8_t key[32];
    keyBytes(key);
    const std::vector<uint8_t> bfbs = binarySchema(kSchema, true);
    std::string error;

    auto owned = openDatabase();
    FlatSQLDatabase& db = *owned;
    CHECK(db.setEncryptionKey(key, 32, FlatSQLDatabase::kRecordFormatUndeclared, &error));

    // 300 records of one layout and one plaintext: sequences 1..300 (records
    // 2 and 256 shared a key under the fallback derivation).
    const Plain same;
    const std::vector<uint8_t> plaintext = build(same);
    std::vector<Plain> expected;
    for (uint64_t i = 1; i <= 300; i++) {
        const uint64_t seq = db.ingestOneEncrypted(plaintext.data(), plaintext.size(), bfbs.data(),
                                                   bfbs.size(), "", &error);
        CHECK(seq == i);
        expected.push_back(same);
    }

    flatbuffers::EncryptionContext ctx(key, 32);
    const auto stored = storedRecords(db, "Secret");
    CHECK(stored.size() == 300);
    std::set<std::string> codes, pins, blobs;
    for (const auto& [seq, bytes] : stored) {
        // Stored bytes are EncryptBuffer's format-3 output for the sequence.
        std::vector<uint8_t> want = plaintext;
        CHECK(flatbuffers::EncryptBuffer(want.data(), want.size(), bfbs.data(), bfbs.size(), ctx,
                                         static_cast<uint32_t>(seq),
                                         flatbuffers::kFieldEncryptionV3).ok());
        CHECK(bytes == want);
        // Public fields are stored as written, secrets are not.
        CHECK(stringAt(bytes, kName) == same.name);
        CHECK(stringAt(bytes, kNote) == same.note);
        CHECK(root(bytes)->GetField<int32_t>(kSerial, 0) == same.serial);
        CHECK(stringAt(bytes, kCode) != same.code);
        CHECK(root(bytes)->GetField<int64_t>(kPin, 0) != same.pin);
        codes.insert(stringAt(bytes, kCode));
        pins.insert(std::to_string(root(bytes)->GetField<int64_t>(kPin, 0)));
        const auto* blob = root(bytes)->GetPointer<const flatbuffers::Vector<uint8_t>*>(kBlob);
        blobs.insert(std::string(blob->begin(), blob->end()));
        // decryptRecord with the sequence restores the plaintext record.
        std::vector<uint8_t> copy = bytes;
        CHECK(db.decryptRecord(copy.data(), copy.size(), bfbs.data(), bfbs.size(), seq, &error));
        CHECK(copy == plaintext);
    }
    // Same layout, same plaintext, one key: 300 distinct ciphertexts each.
    CHECK(codes.size() == 300);
    CHECK(pins.size() == 300);
    CHECK(blobs.size() == 300);

    // Known plaintext: the XOR of two records' ciphertexts is not the XOR of
    // their plaintexts (a shared key stream would make it so).
    {
        Plain a, b;
        a.code = "attack at dawn!!";
        b.code = "retreat at nine!";
        const auto ra = build(a), rb = build(b);
        const uint64_t sa = db.ingestOneEncrypted(ra.data(), ra.size(), bfbs.data(), bfbs.size(), "", &error);
        const uint64_t sb = db.ingestOneEncrypted(rb.data(), rb.size(), bfbs.data(), bfbs.size(), "", &error);
        CHECK(sa == 301 && sb == 302);
        expected.push_back(a);
        expected.push_back(b);
        const auto all = storedRecords(db, "Secret");
        const std::string ca = stringAt(all[300].second, kCode), cb = stringAt(all[301].second, kCode);
        CHECK(ca.size() == a.code.size() && cb.size() == b.code.size());
        std::string recovered(a.code.size(), '\0');
        for (size_t i = 0; i < recovered.size() && i < ca.size() && i < cb.size(); i++) {
            recovered[i] = static_cast<char>(ca[i] ^ cb[i] ^ a.code[i]);
        }
        CHECK(recovered != b.code);
    }

    // A string two (encrypted) columns share is encrypted once and read by both.
    {
        Plain shared;
        shared.code = "shared secret";
        shared.sharedAlias = true;
        const auto r = build(shared);
        CHECK(db.ingestOneEncrypted(r.data(), r.size(), bfbs.data(), bfbs.size(), "", &error) == 303);
        expected.push_back(shared);
    }

    // SQL reads plaintext: the vtab decrypts with the record's sequence and
    // each instance's position.
    checkRows(db, std::string("SELECT ") + kColumns + " FROM Secret ORDER BY _rowid", expected);
    // SELECT * takes the vtab too (the engine's fast paths extract stored bytes).
    {
        const QueryResult all = query(db, "SELECT * FROM Secret");
        CHECK(all.rows.size() == expected.size());
        if (!all.rows.empty()) CHECK(asText(all.rows[0][3]) == same.code);
    }
    // A predicate on an (encrypted) column filters plaintext: never an index
    // of ciphertext.
    {
        const QueryResult r = query(db, "SELECT COUNT(*) FROM Secret WHERE code = 'identical secret'");
        CHECK(!r.rows.empty() && asInt(r.rows[0][0]) == 300);
        const QueryResult p = query(db, "SELECT serial FROM Secret WHERE pin = 1234567890123");
        CHECK(p.rows.size() == 303);
    }

    // The stream stores the record index: replayed into an empty database,
    // every record gets its sequence back. A key set after the tables were
    // registered (the first query registers them) reaches them.
    {
        const std::vector<uint8_t> stream = db.exportData();
        auto replayOwned = openDatabase();
        FlatSQLDatabase& replay = *replayOwned;
        replay.loadAndRebuild(stream.data(), stream.size());
        CHECK(query(replay, "SELECT COUNT(*) FROM Secret").rows.size() == 1);
        // Records exist: their format must be declared. Format 2 is refused.
        CHECK(!replay.setEncryptionKey(key, 32, FlatSQLDatabase::kRecordFormatUndeclared, &error));
        CHECK(contains(error, "declare"));
        CHECK(contains(error, "format 2"));
        CHECK(!replay.setEncryptionKey(key, 32, flatbuffers::kFieldEncryptionV2, &error));
        CHECK(contains(error, "format 2"));
        CHECK(!replay.isEncrypted());
        CHECK(replay.setEncryptionKey(key, 32, flatbuffers::kFieldEncryptionV3, &error));
        checkRows(replay, std::string("SELECT ") + kColumns + " FROM Secret ORDER BY _rowid", expected);
    }

    // Source partitions: the record index is the sequence in the one stream.
    {
        auto sourcedOwned = openDatabase();
        FlatSQLDatabase& sourced = *sourcedOwned;
        sourced.registerSource("siteA");
        CHECK(sourced.setEncryptionKey(key, 32, FlatSQLDatabase::kRecordFormatUndeclared, &error));
        Plain p;
        p.code = "partition secret";
        const auto r = build(p);
        CHECK(sourced.ingestOneEncrypted(r.data(), r.size(), bfbs.data(), bfbs.size(), "", &error) == 1);
        CHECK(sourced.ingestOneEncrypted(r.data(), r.size(), bfbs.data(), bfbs.size(), "siteA", &error) == 2);
        checkRows(sourced, std::string("SELECT ") + kColumns + " FROM \"Secret@siteA\"", {p});
        // The base table reads every record with its file identifier.
        checkRows(sourced, std::string("SELECT ") + kColumns + " FROM Secret ORDER BY _rowid", {p, p});
        CHECK(sourced.ingestOneEncrypted(r.data(), r.size(), bfbs.data(), bfbs.size(), "siteB",
                                         &error) == FlatSQLDatabase::kIngestRefused);
        CHECK(contains(error, "siteB"));
    }

    // Refusals leave the record and the database unchanged.
    {
        const uint64_t before = db.getStorage().getRecordCount();
        std::vector<uint8_t> copy = plaintext;
        CHECK(!db.encryptRecord(copy.data(), copy.size(), bfbs.data(), bfbs.size(), 0, &error));
        CHECK(contains(error, "record index 0"));
        CHECK(!db.encryptRecord(copy.data(), copy.size(), bfbs.data(), bfbs.size(), 1ull << 32, &error));
        CHECK(contains(error, "4294967295"));
        const std::vector<uint8_t> bare = binarySchema(kSchema, false);
        CHECK(!db.encryptRecord(copy.data(), copy.size(), bare.data(), bare.size(), 1, &error));
        CHECK(contains(error, "--bfbs-builtins"));
        const std::vector<uint8_t> other = binarySchema(kOtherSchema, true);
        CHECK(!db.encryptRecord(copy.data(), copy.size(), other.data(), other.size(), 1, &error));
        CHECK(contains(error, "note"));
        CHECK(copy == plaintext);
        CHECK(db.ingestOneEncrypted(plaintext.data(), plaintext.size(), bare.data(), bare.size(), "",
                                    &error) == FlatSQLDatabase::kIngestRefused);
        std::vector<uint8_t> unrouted = plaintext;
        std::memcpy(unrouted.data() + 4, "NOPE", 4);
        CHECK(db.ingestOneEncrypted(unrouted.data(), unrouted.size(), bfbs.data(), bfbs.size(), "",
                                    &error) == FlatSQLDatabase::kIngestRefused);
        CHECK(contains(error, "not registered"));
        CHECK(db.getStorage().getRecordCount() == before);
        CHECK(!db.setEncryptionKey(key, 16, flatbuffers::kFieldEncryptionV3, &error));
        auto keylessOwned = openDatabase();
        FlatSQLDatabase& keyless = *keylessOwned;
        CHECK(keyless.ingestOneEncrypted(plaintext.data(), plaintext.size(), bfbs.data(), bfbs.size(),
                                         "", &error) == FlatSQLDatabase::kIngestRefused);
        CHECK(contains(error, "No encryption key"));
    }

    // encryptRecord gives the bytes ingestOneEncrypted stores for that sequence.
    {
        std::vector<uint8_t> copy = plaintext;
        CHECK(db.encryptRecord(copy.data(), copy.size(), bfbs.data(), bfbs.size(), 1, &error));
        CHECK(copy == storedRecords(db, "Secret")[0].second);
    }
}

}  // namespace

int main() {
    const bool available_ = FlatSQLDatabase::recordEncryptionAvailable();
    if (!available_) {
        unavailable();
        if (FLATSQL_EXPECT_RECORD_ENCRYPTION) {
            std::cerr << "  FAIL: this build has OpenSSL but no HKDF-SHA256 record encryption" << std::endl;
            g_failures++;
        }
    } else {
        available();
    }
    if (g_failures) {
        std::cerr << g_failures << " failures" << std::endl;
        return 1;
    }
    std::cout << (available_ ? "db-key encryption: format 3, record index = sequence, 303 records"
                             : "db-key encryption: unavailable in this build, refusals checked")
              << std::endl;
    return 0;
}
