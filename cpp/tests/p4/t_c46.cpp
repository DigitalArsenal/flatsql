// CONTRACT C-46 (BRIEF5): the follow-ups after 3.7.0, each an answer format 1
// gives (or a refusal), driven through the mailbox as a host drives it.
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>

#include "p4/p4_test.h"

namespace flatsql {
namespace p4 {
const std::string& lastError();
}  // namespace p4
}  // namespace flatsql

using namespace p4t;

namespace {

namespace fs = std::filesystem;

// cpp/tests/p4 (P4_SCHEMA_DIR names cpp/tests/p4/schemas): the checkout is
// visible at its own path natively and under the wasm host.
std::string testsDir() {
    const std::string d = P4_SCHEMA_DIR;
    return d.substr(0, d.rfind('/'));
}

std::map<std::string, std::string> filesUnder(const std::string& root) {
    std::map<std::string, std::string> out;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(root, ec); !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        if (!it->is_regular_file()) continue;
        std::ifstream in(it->path(), std::ios::binary);
        out[fs::relative(it->path(), root).string()] = std::string(std::istreambuf_iterator<char>(in), {});
    }
    return out;
}

}  // namespace

// C-46 (6): a store an earlier format-4 build wrote is refused with a format
// error and never crashes. The fixture is a store 74b2f0d wrote (feed index
// r_c on the CID, r_ke on (k, e), no staged rows): one feed and local, OMM.
P4_TEST(t_earlier_layout_refused) {
    const std::string dir = scratchDir("t_earlier_layout");
    std::error_code ec;
    fs::copy(testsDir() + "/fixtures/store-74b2f0d", dir, fs::copy_options::recursive, ec);
    REQUIRE(!ec, "copy the fixture: " + ec.message());
    const std::map<std::string, std::string> before = filesUnder(dir + "/fsql4");
    REQUIRE(before.size() == 9, "the fixture's 9 files");
    for (uint8_t mode : {uint8_t(0), uint8_t(1)}) {
        EngineOpts o;
        o.createMode = mode;
        CHECK_EQ(openEngine(dir + "/fsql4", o), P4_E_FORMAT, "create mode " + std::to_string(mode));
        CHECK(flatsql::p4::lastError().find("older engine") != std::string::npos, flatsql::p4::lastError());
    }
    // Refused as it was: every file of the store has its bytes.
    const std::map<std::string, std::string> after = filesUnder(dir + "/fsql4");
    for (const auto& f : before) {
        auto it = after.find(f.first);
        CHECK(it != after.end() && it->second == f.second, f.first + " unchanged");
    }
    // The engine starts again in this process: a fresh store opens and serves.
    const std::string fresh = scratchDir("t_earlier_layout_fresh");
    REQUIRE(openEngine(fresh) == P4_OK, "a fresh store opens");
    REQUIRE(registerType(ommType()) == P4_OK, "register");
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWExample";
    In r;
    r.frame = ommFrame(25544, "1998-067A", isoTime(1789000000));
    r.ts = 1790000000;
    b.recs.push_back(r);
    CHECK_EQ(put(b).status, P4_OK, "put");
    uint8_t cid[36];
    cidOf(r.frame, cid);
    const Result g = get("OMM", {std::vector<uint8_t>(cid, cid + 36)});
    CHECK(g.status == P4_OK && g.rows.size() == 1, g.err);
    closeEngine();
    removeTree(dir);
    removeTree(fresh);
}
