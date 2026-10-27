#include "QRP/main_generated.h"
// One real-engine flow: canonical SDS BFBS registration, binary ingest,
// spatial SQL, exact record recovery after reopen, and rejected text inputs.
#include "p4/p4_test.h"
#include "GJN/main_generated.h"
#include "flatbuffers/idl.h"
#include <cmath>
#include <fstream>
#include <iterator>

using namespace p4t;
namespace {
std::vector<uint8_t> fixture() {
    flatbuffers::FlatBufferBuilder b;
    auto point = CreateGJNGeometry(b, GJNGeometryType_POINT, CreateGJNPosition(b, 20, 20, 420, true));
    std::vector<flatbuffers::Offset<GJNPosition>> line{
        CreateGJNPosition(b, 0, 0), CreateGJNPosition(b, 1, 0)};
    auto ps = b.CreateVector(line);
    auto multiPoint = CreateGJNGeometry(b, GJNGeometryType_MULTI_POINT, 0, ps);
    auto lineString = CreateGJNGeometry(b, GJNGeometryType_LINE_STRING, 0, ps);
    auto multiLine = CreateGJNGeometry(b, GJNGeometryType_MULTI_LINE_STRING, 0, 0,
        b.CreateVector(std::vector<flatbuffers::Offset<GJNLinearRing>>{CreateGJNLinearRing(b, ps)}));
    auto ring = [&](double a, double z) {
        std::vector<flatbuffers::Offset<GJNPosition>> v;
        for (auto xy : std::vector<std::pair<double, double>>{{a,a},{z,a},{z,z},{a,z},{a,a}})
            v.push_back(CreateGJNPosition(b, xy.first, xy.second));
        return CreateGJNLinearRing(b, b.CreateVector(v));
    };
    auto outer = ring(0, 10), hole = ring(4, 6);
    auto rings = b.CreateVector(std::vector<flatbuffers::Offset<GJNLinearRing>>{outer, hole});
    auto polygon = CreateGJNGeometry(b, GJNGeometryType_POLYGON, 0, 0, rings);
    auto multiPolygon = CreateGJNGeometry(b, GJNGeometryType_MULTI_POLYGON, 0, 0, 0,
        b.CreateVector(std::vector<flatbuffers::Offset<GJNPolygonRings>>{CreateGJNPolygonRings(b, rings)}));
    auto collection = CreateGJNGeometry(b, GJNGeometryType_GEOMETRY_COLLECTION, 0, 0, 0, 0,
        b.CreateVector(std::vector<flatbuffers::Offset<GJNGeometry>>{point,multiPoint,lineString,multiLine,polygon,multiPolygon}));
    auto prop = CreateGJNProperty(b, b.CreateString("opaque"), 0, 0, false, false, false, false,
                                 b.CreateString("{\"unparsed\":[1,true,null]}"));
    auto f = CreateGJNFeature(b, b.CreateString("all-seven-kinds"), collection,
        b.CreateVector(&prop, 1), 7, true, true);
    auto nullFeature = CreateGJNFeature(b, b.CreateString("null-geometry"), 0, 0, 0, false, false, true);
    auto root = CreateGJN(b, b.CreateVector(std::vector<flatbuffers::Offset<GJNFeature>>{f, nullFeature}));
    FinishSizePrefixedGJNBuffer(b, root);
    return {b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize()};
}
Result query(const std::string& sql, const std::vector<Cell>& params = {}) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<QRPValue>> values;
    for (const auto& c : params) {
        flatbuffers::Offset<QRPValue> v;
        switch (c.type) {
            case flatsql::ps::rb1::kInt: v = CreateQRPValue(builder, qrpValueKind_Int, false, c.i); break;
            case flatsql::ps::rb1::kReal: v = CreateQRPValue(builder, qrpValueKind_Float, false, 0, c.d); break;
            case flatsql::ps::rb1::kText: v = CreateQRPValue(builder, qrpValueKind_Text, false, 0, 0, builder.CreateString(c.s)); break;
            case flatsql::ps::rb1::kBlob: v = CreateQRPValue(builder, qrpValueKind_Bytes, false, 0, 0, 0,
                builder.CreateVector(reinterpret_cast<const uint8_t*>(c.s.data()), c.s.size())); break;
            default: v = CreateQRPValue(builder); break;
        }
        values.push_back(v);
    }
    auto text = builder.CreateString(sql);
    auto bindings = builder.CreateVector(values);
    QRPBuilder q(builder);
    q.add_SQL(text);
    q.add_PARAMETERS(bindings);
    q.add_REPRESENTATION(qrpRepresentation_FlatBuffer);
    FinishSizePrefixedQRPBuffer(builder, q.Finish());
    std::vector<uint8_t> bytes(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    return call(FLATSQL_OPC_SQL, bytes);
}
}

FLATSQL_TEST(t_gjn_binary_query_reopen) {
    const auto sds = argStr("sds", "node_modules/spacedatastandards.org/schema");
    std::ifstream file(sds + "/GJN/main.fbs");
    std::string source((std::istreambuf_iterator<char>(file)), {});
    REQUIRE(!source.empty(), "released canonical SDS GJN schema is required");
    flatbuffers::Parser parser;
    REQUIRE(parser.Parse(source.c_str()), parser.error_);
    parser.Serialize();
    TestType t;
    t.name = "GJN";
    std::memcpy(t.fid, "$GJN", 4);
    t.bfbs.assign(parser.builder_.GetBufferPointer(), parser.builder_.GetBufferPointer() + parser.builder_.GetSize());
    const auto frame = fixture();
    const auto root = scratchDir("gjn_binary_query_reopen");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    REQUIRE(registerType(t) == FLATSQL_OK, "register canonical binary schema");
    Batch batch;
    batch.type = "GJN";
    batch.peer = "binary-producer";
    batch.at = 1790000000;
    batch.tags.push_back(Tag{"provider", "binary-source", "", "batch", "", "", ""});
    In record;
    record.frame = frame;
    record.ts = batch.at;
    batch.recs.push_back(record);
    REQUIRE(put(batch).status == FLATSQL_OK, "binary ingest");
    auto r = query("SELECT geo_contains(_data,2,2),geo_contains(_data,5,5),geo_area_geom(_data),"
                   "geo_length_geom(_data),geo_envelope(_data),_data FROM GJN");
    REQUIRE(r.status == FLATSQL_OK && r.rows.size() == 1, r.err);
    CHECK_EQ(r.rows[0][0].i, 1, "outer polygon");
    CHECK_EQ(r.rows[0][1].i, 0, "hole excluded");
    CHECK(std::fabs(r.rows[0][2].d - 192) < 1e-9, "two polygon members, each area 100 minus 4");
    CHECK(std::fabs(r.rows[0][3].d - 222.389853289) < 1e-6, "two one-degree equatorial line members, km");
    const auto& derived = r.rows[0][4].s;
    flatbuffers::Verifier verifier(reinterpret_cast<const uint8_t*>(derived.data()), derived.size());
    CHECK(VerifyGJNBuffer(verifier), "geometry result is canonical GJN");
    const std::string payload(reinterpret_cast<const char*>(frame.data() + 4), frame.size() - 4);
    CHECK(r.rows[0][5].s == payload, "all fields including altitude, properties and null geometry are unchanged");
    Cell malformed;
    malformed.type = flatsql::ps::rb1::kBlob;
    malformed.s = payload.substr(0, 17);
    CHECK(query("SELECT geo_contains(?1,2,2)", {malformed}).status == FLATSQL_E_SQL, "malformed binary is a SQL error");
    CHECK(query("SELECT geo_contains('POINT(2 2)',2,2)").status == FLATSQL_E_SQL, "text geometry rejected");
    CHECK(query("SELECT geo_from_geojson('{}')").status == FLATSQL_E_SQL, "GeoJSON conversion removed");
    CHECK(query("SELECT json('{}')").status == FLATSQL_E_SQL, "SQLite JSON extension excluded");
    closeEngine();
    REQUIRE(openEngine(root) == FLATSQL_OK, "reopen");
    r = query("SELECT _data,geo_distance(0,0,0,1) FROM GJN");
    REQUIRE(r.status == FLATSQL_OK && r.rows.size() == 1, r.err);
    CHECK(r.rows[0][0].s == payload, "exact persisted binary recovered after reopen");
    CHECK(std::fabs(r.rows[0][1].d - 111.194926645) < 1e-6, "scalar geo retained");
    closeEngine();
    removeTree(root);
}
