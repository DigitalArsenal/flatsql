// Format 2 unit tests: checksums, CIDs and the A17 sort key, key encodings,
// heads, L0 blocks, L1 runs, extraction (A19 Go parity), producer tokens (A3).
#include <algorithm>
#include <random>

#include "flatbuffers/reflection.h"
#include "flatsql/ps/extract.h"
#include "flatsql/ps/index.h"
#include "flatsql/ps/platform.h"
#include "ps/ps_test.h"

using namespace pst;

PS_TEST(format_crc32c_sha256_vectors) {
    const char* s = "123456789";
    CHECK_EQ(crc32c(s, 9), 0xE3069283u);
    uint8_t d[32];
    sha256("abc", 3, d);
    const uint8_t abc[32] = {0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40,
                             0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17,
                             0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad};
    CHECK(std::memcmp(d, abc, 32) == 0);
    sha256("", 0, d);
    CHECK_EQ(d[0], 0xe3);
    CHECK_EQ(d[31], 0x55);
    // 56-byte message crosses the padding boundary.
    const char* m56 = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    sha256(m56, 56, d);
    CHECK_EQ(d[0], 0x24);
    CHECK_EQ(d[1], 0x8d);
    CHECK_EQ(d[31], 0xc1);
}

PS_TEST(format_cid_text_and_sort_key_order_A17) {
    // CIDv1 raw sha2-256 text starts "bafkrei" for every record.
    uint8_t cid[kCidLen];
    computeCid("hello", 5, cid);
    char text[128];
    cidText(cid, kCidLen, text);
    CHECK(std::strncmp(text, "bafkrei", 7) == 0);
    CHECK_EQ(std::strlen(text), size_t(59));
    // 2,000 random CIDs: byte order of the sort key equals text order.
    std::mt19937_64 rng(7);
    std::vector<std::pair<std::string, std::vector<uint8_t>>> v;
    for (int i = 0; i < 2000; i++) {
        uint64_t x = rng();
        computeCid(&x, 8, cid);
        cidText(cid, kCidLen, text);
        uint8_t key[kCidKeyLen];
        cidSortKey(cid, key);
        v.push_back({text, std::vector<uint8_t>(key, key + kCidKeyLen)});
        uint8_t back[kCidLen];
        cidFromSortKey(key, back);
        CHECK(std::memcmp(back, cid, kCidLen) == 0);
    }
    auto byText = v, byKey = v;
    std::sort(byText.begin(), byText.end(), [](auto& a, auto& b) { return a.first < b.first; });
    std::sort(byKey.begin(), byKey.end(), [](auto& a, auto& b) { return a.second < b.second; });
    int differ = 0;
    for (size_t i = 0; i < v.size(); i++) differ += byText[i].first != byKey[i].first;
    CHECK_EQ(differ, 0);
    // Binary order differs from text order (why A17 exists).
    auto byBin = v;
    std::sort(byBin.begin(), byBin.end(), [](auto& a, auto& b) {
        uint8_t ca[kCidLen], cb[kCidLen];
        cidFromSortKey(a.second.data(), ca);
        cidFromSortKey(b.second.data(), cb);
        return std::memcmp(ca, cb, kCidLen) < 0;
    });
    int binDiffer = 0;
    for (size_t i = 0; i < v.size(); i++) binDiffer += byText[i].first != byBin[i].first;
    CHECK(binDiffer > 0);
}

PS_TEST(format_key_encodings_preserve_order) {
    std::mt19937_64 rng(3);
    std::vector<int64_t> ints = {INT64_MIN, -1, 0, 1, INT64_MAX, -(int64_t(1) << 53), int64_t(1) << 53};
    for (int i = 0; i < 1000; i++) ints.push_back(int64_t(rng()));
    for (size_t i = 0; i < ints.size(); i++)
        for (size_t j = 0; j < 20 && j < ints.size(); j++) {
            uint8_t a[8], b[8];
            encI64(a, ints[i]);
            encI64(b, ints[j]);
            const int c = std::memcmp(a, b, 8);
            CHECK((c < 0) == (ints[i] < ints[j]));
            CHECK_EQ(decI64(a), ints[i]);
        }
    std::vector<double> ds = {-1e300, -1.5, -0.0, 0.0, 1e-300, 2.5, 1e300};
    for (size_t i = 0; i + 1 < ds.size(); i++) {
        uint8_t a[8], b[8];
        encF64(a, ds[i]);
        encF64(b, ds[i + 1]);
        CHECK(std::memcmp(a, b, 8) <= 0);
    }
    // (string, i64) composites sort by string, then number; embedded NULs too.
    const std::string s1("a\0b", 3), s2("a", 1), s3("ab", 2);
    uint8_t k1[32], k2[32], k3[32], k4[32];
    const size_t n1 = encStrI64(k1, reinterpret_cast<const uint8_t*>(s1.data()), s1.size(), 5);
    const size_t n2 = encStrI64(k2, reinterpret_cast<const uint8_t*>(s2.data()), s2.size(), 9);
    const size_t n3 = encStrI64(k3, reinterpret_cast<const uint8_t*>(s3.data()), s3.size(), -9);
    const size_t n4 = encStrI64(k4, reinterpret_cast<const uint8_t*>(s2.data()), s2.size(), 10);
    CHECK(keyCmp(k2, n2, k1, n1) < 0);   // "a" < "a\0b"
    CHECK(keyCmp(k1, n1, k3, n3) < 0);   // "a\0b" < "ab"
    CHECK(keyCmp(k2, n2, k4, n4) < 0);   // same string: 9 < 10
}

PS_TEST(format_head_slot_torn_detection) {
    uint8_t slot[kHeadSlotBytes];
    std::memset(slot, 0, sizeof(slot));
    PartitionHeadFixed h{};
    h.p.magic = kMagicHead;
    h.p.format = kFormat;
    h.p.kind = kHeadPartition;
    h.p.gen = 7;
    h.pseqHi = 42;
    std::memcpy(slot, &h, sizeof(h));
    sealHeadSlot(slot, sizeof(h) + 4);
    CHECK_EQ(validHeadSlot(slot, sizeof(slot), kHeadPartition), uint32_t(sizeof(h) + 4));
    CHECK_EQ(validHeadSlot(slot, sizeof(slot), kHeadType), 0u);
    slot[100] ^= 1;
    CHECK_EQ(validHeadSlot(slot, sizeof(slot), kHeadPartition), 0u);
}

PS_TEST(format_l0_block_roundtrip_and_bloom) {
    std::vector<std::vector<uint8_t>> keys;
    std::vector<StagedEntry> es;
    std::vector<uint8_t> vals(8 * 3000);
    std::mt19937_64 rng(11);
    for (int i = 0; i < 3000; i++) {
        uint8_t cid[kCidLen];
        uint64_t x = rng();
        computeCid(&x, 8, cid);
        std::vector<uint8_t> k(kCidKeyLen);
        cidSortKey(cid, k.data());
        keys.push_back(k);
    }
    for (int i = 0; i < 3000; i++) {
        putBE64(vals.data() + 8 * i, uint64_t(i + 1));
        StagedEntry e{};
        e.kind = (i % 3 == 0) ? kIxEpoch : kIxCid;
        e.klen = uint16_t(e.kind == kIxEpoch ? 8 : kCidKeyLen);
        e.vlen = 8;
        e.key = keys[i].data();
        e.val = vals.data() + 8 * i;
        es.push_back(e);
    }
    std::vector<StagedEntry*> order;
    for (auto& e : es) order.push_back(&e);
    sortStaged(order.data(), order.size());
    const size_t len = l0BlockSize(order.data(), order.size());
    std::vector<uint8_t> block(len);
    CHECK_EQ(writeL0Block(block.data(), order.data(), order.size(), 1, 3000), len);
    L0KindInfo kinds[8];
    size_t nk = 0;
    REQUIRE(parseL0Block(block.data(), block.size(), kinds, 8, &nk));
    CHECK_EQ(nk, size_t(2));
    size_t totalEntries = 0;
    for (size_t k = 0; k < nk; k++) {
        EntryIter it;
        it.p = block.data() + kinds[k].entriesOff;
        it.end = it.p + kinds[k].entriesBytes;
        it.vlen = kinds[k].vlen;
        const uint8_t *ek, *ev, *prev = nullptr;
        uint16_t el, pl = 0;
        while (it.next(&ek, &el, &ev)) {
            if (prev) CHECK(keyCmp(prev, pl, ek, el) <= 0);
            prev = ek;
            pl = el;
            totalEntries++;
            if (kinds[k].bloomBytes) CHECK(bloomTest(block.data() + kinds[k].bloomOff, kinds[k].bloomBytes, ek, el));
        }
    }
    CHECK_EQ(totalEntries, size_t(3000));
    // Bloom false positives stay near 10 bits/key (~1%).
    const L0KindInfo* cidKind = kinds[0].kind == kIxCid ? &kinds[0] : &kinds[1];
    int fp = 0;
    for (int i = 0; i < 10000; i++) {
        uint8_t cid[kCidLen], key[kCidKeyLen];
        uint64_t x = rng() ^ 0x5555;
        computeCid(&x, 8, cid);
        cidSortKey(cid, key);
        fp += bloomTest(block.data() + cidKind->bloomOff, cidKind->bloomBytes, key, kCidKeyLen);
    }
    report("l0_bloom_false_positive_rate", fp / 10000.0, "fraction");
    CHECK(fp < 300);
    // Corruption is detected.
    block[len / 2] ^= 0x40;
    CHECK(!parseL0Block(block.data(), block.size(), kinds, 8, &nk));
}

PS_TEST(format_l1_run_lookup_scan_range) {
    FaultFs fs(false);
    IoStats st;
    IoCtx io(&fs, &st);
    FileRef f;
    REQUIRE(io.open("/mem/x.fsx", 10, FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE, FileClass::Index, &f) == 0);
    L1Writer w(&io, f, 3, 1, 0, 1, 50000);
    // Kind DEAD: 50,000 u64 keys with a duplicate-key run.
    REQUIRE(w.beginKind(kIxDead, 50001) == 0);
    for (uint64_t i = 1; i <= 50000; i++) {
        uint8_t k[8], v[8];
        putBE64(k, i * 2);
        putBE64(v, i);
        REQUIRE(w.add(k, 8, v) == 0);
        if (i == 777) {
            putBE64(v, i + 1000000);
            REQUIRE(w.add(k, 8, v) == 0);
        }
    }
    REQUIRE(w.endKind() == 0);
    REQUIRE(w.beginKind(kIxTagSource, 3) == 0);
    for (const char* s : {"celestrak", "spacetrack", "spacetrack"}) {
        uint8_t v[8];
        putBE64(v, uint64_t(std::strlen(s)));
        REQUIRE(w.add(reinterpret_cast<const uint8_t*>(s), uint16_t(std::strlen(s)), v) == 0);
    }
    REQUIRE(w.endKind() == 0);
    const int64_t flen = w.finish();
    REQUIRE(flen > 0);
    L1Run run;
    REQUIRE(run.load(&io, f, uint64_t(flen)) == 0);
    CHECK_EQ(run.entries(), uint64_t(50004));
    std::vector<uint8_t> scratch(kL1BlockBytes);
    int misses = 0;
    for (uint64_t i = 1; i <= 50000; i += 97) {
        uint8_t k[8];
        putBE64(k, i * 2);
        uint64_t got = 0;
        int n = 0;
        run.lookup(&io, f, kIxDead, k, 8, scratch.data(), [&](const uint8_t*, uint16_t, const uint8_t* v) {
            got = getBE64(v);
            n++;
        });
        if (n != (i == 777 ? 2 : 1) || (i != 777 && got != i)) misses++;
    }
    CHECK_EQ(misses, 0);
    uint8_t k777[8];
    putBE64(k777, 1554);
    int dup = 0;
    run.lookup(&io, f, kIxDead, k777, 8, scratch.data(), [&](const uint8_t*, uint16_t, const uint8_t*) { dup++; });
    CHECK_EQ(dup, 2);
    // Absent odd keys: no hits (blooms mostly skip the read).
    int ghost = 0;
    for (uint64_t i = 1; i < 20000; i += 2) {
        uint8_t k[8];
        putBE64(k, i);
        run.lookup(&io, f, kIxDead, k, 8, scratch.data(), [&](const uint8_t*, uint16_t, const uint8_t*) { ghost++; });
    }
    CHECK_EQ(ghost, 0);
    int st2 = 0;
    run.lookup(&io, f, kIxTagSource, reinterpret_cast<const uint8_t*>("spacetrack"), 10, scratch.data(),
               [&](const uint8_t*, uint16_t, const uint8_t*) { st2++; });
    CHECK_EQ(st2, 2);
    // Range [1000, 2000): keys 1000..1998 even = 500 keys, 501 entries (1554 twice).
    uint8_t lo[8], hi[8];
    putBE64(lo, 1000);
    putBE64(hi, 2000);
    int inRange = 0;
    run.scanRange(&io, f, kIxDead, lo, 8, hi, 8, scratch.data(), [&](const uint8_t*, uint16_t, const uint8_t*) {
        inRange++;
        return true;
    });
    CHECK_EQ(inRange, 501);
}

PS_TEST(extract_parse_epoch_go_parity) {
    struct Case {
        const char* in;
        bool ok;
        int64_t sec;
    } cases[] = {
        {"2024-03-01T12:34:56Z", true, 1709296496},
        {"2024-03-01T12:34:56.123456Z", true, 1709296496},
        {"2024-03-01T12:34:56.123456", true, 1709296496},
        {"2024-03-01T12:34:56.123", true, 1709296496},
        {"2024-03-01T12:34:56", true, 1709296496},
        {"2024-03-01 12:34:56", true, 1709296496},
        {"2024-03-01", true, 1709251200},
        {"  2024-03-01T12:34:56Z  ", true, 1709296496},
        {"2024-03-01T12:34:56+02:00", true, 1709289296},
        {"2024-03-01T12:34:56.9999999999", true, 1709296496},
        {"1969-12-31T23:59:59", true, -1},
        {"2024-02-30", false, 0},
        {"2023-02-29", false, 0},
        {"2024-02-29", true, 1709164800},
        {"2024-03-01T24:00:00", false, 0},
        {"2024-03-01 12:34:56Z", false, 0},
        {"1709296496.75", true, 1709296496},
        {"-5", false, 0},
        {"0", false, 0},
        {"garbage", false, 0},
        {"", false, 0},
    };
    for (const auto& c : cases) {
        int64_t sec = 0, ms = 0;
        const bool ok = parseEpochString(reinterpret_cast<const uint8_t*>(c.in), std::strlen(c.in), &sec, &ms);
        if (ok != c.ok || (ok && sec != c.sec)) {
            std::fprintf(stderr, "  epoch case '%s': ok=%d sec=%lld\n", c.in, ok, (long long)sec);
            gFailures++;
        }
    }
    int64_t sec, ms;
    parseEpochString(reinterpret_cast<const uint8_t*>("2024-03-01T12:34:56.789Z"), 24, &sec, &ms);
    CHECK_EQ(ms, int64_t(1709296496789));
    char day[10];
    formatEpochDay(1709296496, day);
    CHECK(std::memcmp(day, "2024-03-01", 10) == 0);
    formatEpochDay(-1, day);
    CHECK(std::memcmp(day, "1969-12-31", 10) == 0);
}

PS_TEST(extract_producer_token_A3) {
    auto tok = [](const std::string& s) {
        return producerToken(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    };
    CHECK(tok("") == "unattributed");
    CHECK(tok("   \t") == "unattributed");
    CHECK(tok("12D3KooWabc") == "12D3KooWabc");
    CHECK(tok(" module:celestrak ") == "module_celestrak");
    CHECK(tok("source:space-track.org") == "source_space_track_org");
    // One '_' per rune; an invalid UTF-8 byte is one rune (Go semantics).
    CHECK(tok("a\xc3\xa9z") == "a_z");
    CHECK(tok("a\xff\xfez") == "a__z");
    CHECK(tok("\xe2\x80\x83x\xe2\x80\x83") == "x");  // U+2003 EM SPACE trimmed
}

PS_TEST(extract_type_rules_omm_mpe_cat) {
    TypeConfig omm, mpe, cat;
    CHECK(omm.parse(ommType().config.data(), ommType().config.size()).empty());
    CHECK(mpe.parse(mpeType().config.data(), mpeType().config.size()).empty());
    CHECK(cat.parse(catType().config.data(), catType().config.size()).empty());
    uint8_t scratch[4096];
    // OMM: EPOCH, else CREATION_DATE; NORAD > 0; trimmed OBJECT_ID; epoch day.
    auto f = ommRecord(25544, " 1998-067A ", "2024-03-01T12:34:56.5Z", 15.5);
    CHECK_EQ(omm.checkFrame(f.data(), f.size()), 0);
    Extracted ex;
    omm.extract(f.data(), f.size(), &ex, scratch, sizeof(scratch));
    CHECK(ex.hasEpoch);
    CHECK_EQ(ex.epochSec, int64_t(1709296496));
    CHECK_EQ(ex.epochMs, int64_t(1709296496500));
    CHECK(ex.cols[0].present && ex.cols[0].isU64 && ex.cols[0].u == 25544);
    CHECK(ex.cols[1].present && std::string(reinterpret_cast<const char*>(ex.cols[1].s), ex.cols[1].n) == "1998-067A");
    CHECK(ex.cols[4].present && std::string(reinterpret_cast<const char*>(ex.cols[4].s), ex.cols[4].n) == "2024-03-01");
    CHECK_EQ(ex.objectCol, 0);
    auto f2 = buildRecord(ommType(), {Field::str("OBJECT_ID", "X"), Field::str("EPOCH", "  "),
                                      Field::str("CREATION_DATE", "2024-01-02")});
    omm.extract(f2.data(), f2.size(), &ex, scratch, sizeof(scratch));
    CHECK(ex.hasEpoch && ex.epochSec == 1704153600);
    CHECK(!ex.cols[0].present);  // NORAD absent (0)
    auto f3 = buildRecord(ommType(), {Field::str("EPOCH", "not-a-date"), Field::str("CREATION_DATE", "2024-01-02")});
    omm.extract(f3.data(), f3.size(), &ex, scratch, sizeof(scratch));
    CHECK(!ex.hasEpoch);  // Go does not fall back when EPOCH is present but unparsable
    // MPE: floor(double) epoch, 0 = absent.
    auto m = mpeRecord("SAT-1", -1.5, 1.0);
    mpe.extract(m.data(), m.size(), &ex, scratch, sizeof(scratch));
    CHECK(ex.hasEpoch && ex.epochSec == -2);
    auto m0 = mpeRecord("SAT-1", 0.0, 1.0);
    mpe.extract(m0.data(), m0.size(), &ex, scratch, sizeof(scratch));
    CHECK(!ex.hasEpoch);
    // CAT: supersede identity precedence and enum names ("UNKNOWN" absent).
    auto c1 = catRecord(25544, "1998-067A", "urn:cat", "ISS", "ISS", 0);
    cat.extract(c1.data(), c1.size(), &ex, scratch, sizeof(scratch));
    CHECK(std::string(reinterpret_cast<const char*>(ex.identity), ex.identityLen) == std::string("uri:urn:cat\0ISS", 15));
    CHECK(ex.cols[2].present && std::string(reinterpret_cast<const char*>(ex.cols[2].s), ex.cols[2].n) == "PAYLOAD");
    auto c2 = catRecord(25544, "1998-067A", "", "", "ISS", 3);
    cat.extract(c2.data(), c2.size(), &ex, scratch, sizeof(scratch));
    CHECK(std::string(reinterpret_cast<const char*>(ex.identity), ex.identityLen) == "norad:25544");
    CHECK(!ex.cols[2].present);  // UNKNOWN
    auto c3 = catRecord(0, "2026-001A", "", "", "OBJ", 2);
    cat.extract(c3.data(), c3.size(), &ex, scratch, sizeof(scratch));
    CHECK(std::string(reinterpret_cast<const char*>(ex.identity), ex.identityLen) == "object:2026-001A");
    // Frame checks: fid, size, BFBS verification.
    auto bad = f;
    bad[8] = 'X';
    CHECK_EQ(omm.checkFrame(bad.data(), bad.size()), int32_t(kRejFid));
    auto trunc = f;
    trunc.resize(trunc.size() - 8);
    putU32(trunc.data(), uint32_t(trunc.size() - 4));
    CHECK(omm.checkFrame(trunc.data(), trunc.size()) != 0);
    CHECK(sealedEnvelopeValid(reinterpret_cast<const uint8_t*>("SDF1\x01rest"), 9));
    CHECK(!sealedEnvelopeValid(reinterpret_cast<const uint8_t*>("SDF1\x02rest"), 9));
}

PS_TEST(ring_producer_consumer_straddles_pages) {
    SlabPool pool;
    REQUIRE(pool.init(8u << 20, 64u << 10));
    RingDesc* r = ringCreate(1, 1u << 20, 256u << 10, 64u << 10);
    REQUIRE(r);
    CHECK_EQ(r->mappedPages.load(), 0u);  // idle: zero slabs
    std::vector<uint8_t> big(100000);
    for (size_t i = 0; i < big.size(); i++) big[i] = uint8_t(i * 7);
    r->wantPage.store(uint32_t((big.size() - 1) / (64u << 10) + 1));
    REQUIRE(ringMapAhead(r, pool, 0, 1));
    ringWrite(r, pool, 0, big.data(), big.size());
    r->tail.store(big.size());
    std::vector<uint8_t> back(big.size());
    ringRead(r, pool, 0, back.data(), back.size());
    CHECK(back == big);
    ringRelease(r, pool, big.size());
    CHECK_EQ(r->mappedPages.load(), 1u);  // the partially used page stays
    CHECK_EQ(ringReclaimIdle(r, pool), 1u);
    CHECK_EQ(r->mappedPages.load(), 0u);
    CHECK_EQ(pool.inUse(), 0u);
    ringDestroy(r);
}

// ---------------------------------------------------------------------------
// A19 golden vectors: the per-type rule texts in test/ps/vectors/<T>.rules
// reproduce sdn-server extractIndexedFields + recordSupersedeKey over frames
// built from the published SDS 1.226.0 schemas (vectors/gen).
// ---------------------------------------------------------------------------
namespace {

std::vector<uint8_t> readVectorFile(const std::string& name) {
    const std::string path = std::string(PS_VECTOR_DIR) + "/" + name;
    std::vector<uint8_t> out;
    FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return out;
    uint8_t buf[65536];
    size_t n;
    while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0) out.insert(out.end(), buf, buf + n);
    std::fclose(f);
    return out;
}

// One expected cell: null, an integer, or a string.
struct Cell {
    bool null = true;
    bool isInt = false;
    int64_t i = 0;
    std::string s;
};

bool parseJsonRow(const std::string& line, std::vector<Cell>* row) {
    row->clear();
    size_t p = 0;
    auto ws = [&] { while (p < line.size() && line[p] == ' ') p++; };
    ws();
    if (p >= line.size() || line[p++] != '[') return false;
    for (;;) {
        ws();
        Cell c;
        if (line.compare(p, 4, "null") == 0) {
            p += 4;
        } else if (line[p] == '"') {
            p++;
            c.null = false;
            while (p < line.size() && line[p] != '"') {
                if (line[p] == '\\') {
                    p++;
                    const char e = line[p++];
                    if (e == 'u') {
                        const unsigned v = unsigned(std::stoul(line.substr(p, 4), nullptr, 16));
                        p += 4;
                        if (v >= 0x80) return false;  // vectors carry ASCII escapes only
                        c.s.push_back(char(v));
                    } else if (e == 'n') {
                        c.s.push_back('\n');
                    } else if (e == 't') {
                        c.s.push_back('\t');
                    } else {
                        c.s.push_back(e);
                    }
                } else {
                    c.s.push_back(line[p++]);
                }
            }
            p++;
        } else {
            size_t used = 0;
            c.i = std::stoll(line.substr(p), &used);
            p += used;
            c.null = false;
            c.isInt = true;
        }
        row->push_back(c);
        ws();
        if (p < line.size() && line[p] == ',') {
            p++;
            continue;
        }
        return p < line.size() && line[p] == ']';
    }
}

std::string colText(const ColValue& cv) {
    if (!cv.present) return "<absent>";
    if (cv.isU64) return "u" + std::to_string(cv.u);
    return "s" + std::string(reinterpret_cast<const char*>(cv.s), cv.n);
}

std::string cellText(const Cell& c, bool asU64) {
    if (c.null) return "<absent>";
    if (c.isInt) return (asU64 ? "u" : "i") + std::to_string(c.i);
    return "s" + c.s;
}

}  // namespace

PS_TEST(format_extraction_golden_vectors_A19) {
    const char* types[] = {"OMM", "MPE", "OEM", "CAT", "PNM", "RFB"};
    size_t total = 0;
    for (const char* t : types) {
        const std::vector<uint8_t> bfbs = readVectorFile(std::string(t) + ".bfbs");
        const std::vector<uint8_t> rulesBytes = readVectorFile(std::string(t) + ".rules");
        const std::vector<uint8_t> frames = readVectorFile(std::string(t) + ".frames");
        const std::vector<uint8_t> expectedBytes =
            readVectorFile(std::string(t) + ".expected.jsonl");
        CHECK(!bfbs.empty() && !rulesBytes.empty() && !frames.empty() && !expectedBytes.empty());
        if (bfbs.empty() || frames.size() < 12) continue;
        const reflection::Schema* schema = reflection::GetSchema(bfbs.data());
        CHECK(schema->file_ident() && schema->file_ident()->size() == 4);
        const std::string rules(rulesBytes.begin(), rulesBytes.end());
        const std::vector<uint8_t> blob = TypeConfig::build(
            std::string(t) + ".fbs", reinterpret_cast<const uint8_t*>(schema->file_ident()->c_str()),
            bfbs, rules, 1u << 20, 4u << 20, TypeConfig::kVerifyBfbs | TypeConfig::kVerifyCid);
        TypeConfig cfg;
        const std::string err = cfg.parse(blob.data(), blob.size());
        if (!err.empty()) std::fprintf(stderr, "  %s rules: %s\n", t, err.c_str());
        CHECK(err.empty());
        if (!err.empty()) continue;
        std::vector<std::string> lines;
        {
            std::string cur;
            for (uint8_t b : expectedBytes) {
                if (b == '\n') {
                    lines.push_back(cur);
                    cur.clear();
                } else {
                    cur.push_back(char(b));
                }
            }
            if (!cur.empty()) lines.push_back(cur);
        }
        size_t off = 0, idx = 0, mismatches = 0;
        uint8_t scratch[2048];
        std::vector<Cell> row;
        while (off + 4 <= frames.size()) {
            const size_t len = 4 + (uint32_t(frames[off]) | uint32_t(frames[off + 1]) << 8 |
                                    uint32_t(frames[off + 2]) << 16 |
                                    uint32_t(frames[off + 3]) << 24);
            CHECK(off + len <= frames.size());
            if (off + len > frames.size()) break;
            const uint8_t* frame = frames.data() + off;
            off += len;
            CHECK(idx < lines.size());
            if (idx >= lines.size()) break;
            CHECK(parseJsonRow(lines[idx], &row) && row.size() == 7);
            CHECK_EQ(cfg.checkFrame(frame, len), 0);
            Extracted ex;
            cfg.extract(frame, len, &ex, scratch, sizeof(scratch));
            // [norad, entity, objectType, opsStatus, epoch_unix, epoch_day, supersede]
            std::string got[7], want[7];
            for (int c = 0; c < 4; c++) {
                got[c] = colText(ex.cols[c]);
                want[c] = cellText(row[size_t(c)], c == 0);
            }
            got[4] = ex.hasEpoch ? "i" + std::to_string(ex.epochSec) : "<absent>";
            want[4] = cellText(row[4], false);
            got[5] = colText(ex.cols[4]);
            want[5] = cellText(row[5], false);
            got[6] = "s" + std::string(reinterpret_cast<const char*>(ex.identity), ex.identityLen);
            want[6] = cellText(row[6], false);
            for (int c = 0; c < 7; c++) {
                if (got[c] != want[c]) {
                    if (mismatches++ < 10)
                        std::fprintf(stderr, "  %s #%zu column %d: engine %s, Go %s\n", t, idx, c,
                                     got[c].c_str(), want[c].c_str());
                }
            }
            idx++;
        }
        CHECK_EQ(mismatches, size_t(0));
        CHECK_EQ(idx, lines.size());
        total += idx;
    }
    report("golden_records", double(total), "records");
}
