// Store format 4: byte codecs (TLV, CIDs, markers, config), host-file helpers
// and the SQLite connection wrapper.
#include "internal.h"

#include <ctime>

#include "flatsql/flatsql_io.h"

namespace flatsql {
namespace p4 {

// ---- TLV ------------------------------------------------------------------------
bool tlvParse(const uint8_t* p, size_t n, std::vector<Tlv>* out) {
    out->clear();
    size_t off = 0;
    while (off < n) {
        if (n - off < 6) return false;
        const uint16_t tag = ld16(p + off);
        const uint32_t len = ld32(p + off + 2);
        if (n - off - 6 < len) return false;
        out->push_back(Tlv{tag, len, p + off + 6});
        off += 6 + size_t(len);
    }
    return true;
}

const Tlv* tlvFind(const std::vector<Tlv>& v, uint16_t tag) {
    for (const Tlv& t : v)
        if (t.tag == tag) return &t;
    return nullptr;
}

bool tlvU8(const std::vector<Tlv>& v, uint16_t tag, uint8_t* out, bool* bad) {
    const Tlv* t = tlvFind(v, tag);
    if (!t) return false;
    if (t->n != 1) { *bad = true; return false; }
    *out = t->v[0];
    return true;
}
bool tlvU32(const std::vector<Tlv>& v, uint16_t tag, uint32_t* out, bool* bad) {
    const Tlv* t = tlvFind(v, tag);
    if (!t) return false;
    if (t->n != 4) { *bad = true; return false; }
    *out = ld32(t->v);
    return true;
}
bool tlvU64(const std::vector<Tlv>& v, uint16_t tag, uint64_t* out, bool* bad) {
    const Tlv* t = tlvFind(v, tag);
    if (!t) return false;
    if (t->n != 8) { *bad = true; return false; }
    *out = ld64(t->v);
    return true;
}
bool tlvI64(const std::vector<Tlv>& v, uint16_t tag, int64_t* out, bool* bad) {
    uint64_t u;
    if (!tlvU64(v, tag, &u, bad)) return false;
    *out = int64_t(u);
    return true;
}
bool tlvText(const std::vector<Tlv>& v, uint16_t tag, std::string* out) {
    const Tlv* t = tlvFind(v, tag);
    if (!t) return false;
    out->assign(reinterpret_cast<const char*>(t->v), t->n);
    return true;
}
void tlvPut(std::vector<uint8_t>& out, uint16_t tag, const void* p, size_t n) {
    const size_t at = out.size();
    out.resize(at + 6 + n);
    st16(out.data() + at, tag);
    st32(out.data() + at + 2, uint32_t(n));
    if (n) std::memcpy(out.data() + at + 6, p, n);
}

// ---- CIDs -------------------------------------------------------------------------
namespace {
const char kB32[] = "abcdefghijklmnopqrstuvwxyz234567";
const uint8_t kCidPrefix[4] = {0x01, 0x55, 0x12, 0x20};

int b32v(char c) {
    if (c >= 'a' && c <= 'z') return c - 'a';
    if (c >= '2' && c <= '7') return 26 + c - '2';
    return -1;
}
// The 58 five-bit groups of the 36-byte CID (the last padded with two zero bits).
// MSB-first bit streams through a 64-bit accumulator (the CID text is 58
// five-bit groups of the 36 binary bytes; 290 bits, the last 2 zero).
void groupsOf(const uint8_t d[32], uint8_t g[58]) {
    uint8_t b[37];
    std::memcpy(b, kCidPrefix, 4);
    std::memcpy(b + 4, d, 32);
    b[36] = 0;
    uint64_t acc = 0;
    int nb = 0;
    size_t at = 0;
    for (int i = 0; i < 58; i++) {
        while (nb < 5) {
            acc = acc << 8 | b[at++];
            nb += 8;
        }
        nb -= 5;
        g[i] = uint8_t((acc >> nb) & 31);
    }
}
void digestOfGroups(const uint8_t g[58], uint8_t d[32]) {
    uint8_t b[37];
    uint64_t acc = 0;
    int nb = 0;
    size_t at = 0;
    for (int i = 0; i < 58; i++) {
        acc = acc << 5 | g[i];
        nb += 5;
        while (nb >= 8 && at < sizeof b) {
            nb -= 8;
            b[at++] = uint8_t(acc >> nb);
        }
    }
    std::memcpy(d, b + 4, 32);
}
// The groups of the CID prefix that a key does not carry (g0..g5, g6's high bits).
const uint8_t* prefixGroups() {
    static const struct P {
        uint8_t g[58];
        P() {
            const uint8_t zero[32] = {0};
            groupsOf(zero, g);
        }
    } p;
    return p.g;
}
// A key's 58 groups, read straight from its bits (no digest in between).
void groupsFromKey(const uint8_t k[32], uint8_t g[58]) {
    const uint8_t* pg = prefixGroups();
    uint64_t acc = 0;
    int nb = 0;
    size_t at = 0;
    auto get = [&](int w) {
        while (nb < w) {
            acc = acc << 8 | k[at++];
            nb += 8;
        }
        nb -= w;
        return int((acc >> nb) & ((1u << w) - 1));
    };
    for (int i = 0; i < 6; i++) g[i] = pg[i];
    g[6] = uint8_t((pg[6] & ~7) | get(3));
    for (int i = 7; i <= 56; i++) g[i] = uint8_t((get(5) - 6) & 31);
    g[57] = uint8_t(((get(3) - 1) & 7) << 2);
}
}  // namespace

bool cidBinValid(const uint8_t* cid36) { return std::memcmp(cid36, kCidPrefix, 4) == 0; }

// key = [g6: 3 bits][rank(g7..g56): 5 bits each][rank3(g57): 3 bits]; rank(v)
// = (v + 6) & 31 puts '2'..'7' (26..31) before 'a'..'z' (0..25), which is
// ASCII order. g6 holds 3 digest bits (a..h, already ordered); g57 holds 3
// digest bits << 2, whose letters sort with rank3 = ((g57 >> 2) + 1) & 7.
void cidKeyFromDigest(const uint8_t d[32], uint8_t k[32]) {
    uint8_t g[58];
    groupsOf(d, g);
    std::memset(k, 0, 32);
    int bit = 0;
    auto put = [&](int v, int w) {
        for (int j = w - 1; j >= 0; j--, bit++)
            if ((v >> j) & 1) k[bit >> 3] |= uint8_t(1 << (7 - (bit & 7)));
    };
    put(g[6] & 7, 3);
    for (int i = 7; i <= 56; i++) put((g[i] + 6) & 31, 5);
    put(((g[57] >> 2) + 1) & 7, 3);
}

void cidDigestFromKey(const uint8_t k[32], uint8_t d[32]) {
    uint8_t g[58];
    groupsFromKey(k, g);
    digestOfGroups(g, d);
}

void cidTextFromDigest(const uint8_t d[32], char out[60]) {
    uint8_t g[58];
    groupsOf(d, g);
    out[0] = 'b';
    for (int i = 0; i < 58; i++) out[1 + i] = kB32[g[i]];
    out[59] = 0;
}

void cidTextFromKey(const uint8_t k[32], char out[60]) {
    uint8_t g[58];
    groupsFromKey(k, g);
    out[0] = 'b';
    for (int i = 0; i < 58; i++) out[1 + i] = kB32[g[i]];
    out[59] = 0;
}

bool cidDigestFromText(const char* s, size_t n, uint8_t d[32]) {
    if (n != kCidText || s[0] != 'b') return false;
    uint8_t g[58];
    for (int i = 0; i < 58; i++) {
        const int v = b32v(s[1 + i]);
        if (v < 0) return false;
        g[i] = uint8_t(v);
    }
    uint8_t b[37] = {0};
    for (int i = 0; i < 58; i++)
        for (int j = 0; j < 5; j++) {
            const int bit = 5 * i + j;
            if (bit < 288 && ((g[i] >> (4 - j)) & 1)) b[bit >> 3] |= uint8_t(1 << (7 - (bit & 7)));
        }
    if (std::memcmp(b, kCidPrefix, 4) != 0) return false;
    std::memcpy(d, b + 4, 32);
    return true;
}

// ---- time -----------------------------------------------------------------------------
int64_t nowSec() { return floorDiv(wallMs(), 1000); }

void dayText(int64_t sec, char out[11]) {
    ps::formatEpochDay(sec, out);
    out[10] = 0;
}

// ---- config -------------------------------------------------------------------------
int32_t parseConfig(const uint8_t* p, size_t n, Config* c, std::string* err) {
    std::vector<Tlv> v;
    if (!tlvParse(p, n, &v)) { *err = "config: malformed TLV"; return P4_E_ARG; }
    bool bad = false;
    if (!tlvText(v, 1, &c->root) || c->root.empty() || c->root[0] != '/') {
        *err = "config: tag 1 (root) must be an absolute path";
        return P4_E_ARG;
    }
    while (c->root.size() > 1 && c->root.back() == '/') c->root.pop_back();
    uint32_t u32;
    uint64_t u64;
    uint8_t u8;
    if (tlvU8(v, 2, &u8, &bad)) c->createMode = u8;
    auto setU32 = [&](uint16_t tag, uint32_t* dst) { if (tlvU32(v, tag, &u32, &bad) && u32) *dst = u32; };
    auto setU64 = [&](uint16_t tag, uint64_t* dst) { if (tlvU64(v, tag, &u64, &bad) && u64) *dst = u64; };
    setU32(13, &c->cores);
    setU32(3, &c->writers);
    setU32(4, &c->interactive);
    setU32(5, &c->bulk);
    setU32(6, &c->sandbox);
    setU32(7, &c->writeSlots);
    setU32(8, &c->readSlots);
    setU32(9, &c->writeReqBytes);
    setU32(10, &c->readReqBytes);
    setU32(11, &c->ringBytes);
    setU64(12, &c->gseqFloor);
    setU64(20, &c->engineBytes);
    setU32(21, &c->writerConns);
    setU32(22, &c->writerCacheKiB);
    setU32(23, &c->readerConns);
    setU32(24, &c->readerCacheKiB);
    setU64(25, &c->pendingBytes);
    setU64(26, &c->softHeap);
    setU64(27, &c->hardHeap);
    setU32(28, &c->raStreams);
    setU32(29, &c->raBytes);
    setU32(30, &c->passivePages);
    setU64(31, &c->restartBytes);
    setU64(32, &c->walTotal);
    setU64(33, &c->journalSizeLimit);
    setU32(40, &c->groupRecords);
    setU32(41, &c->groupMs);
    setU32(42, &c->flushEntries);
    setU32(43, &c->seqBlock);
    setU32(44, &c->backlogCredit);
    // Tags 45-47 (file rebuild, quota mode) are ignored (v11: no file
    // rebuilds; quota deletes the oldest records by arrival).
    setU64(48, &c->sandboxHeap);
    setU64(49, &c->sandboxRows);
    setU64(50, &c->sandboxBytes);
    if (bad) { *err = "config: a typed tag has the wrong length"; return P4_E_ARG; }
    if (c->createMode > 2) { *err = "config: create mode must be 0, 1 or 2"; return P4_E_ARG; }
    if (c->writers == 0) {
        const uint32_t w = c->cores > 1 ? c->cores - 1 : 1;
        c->writers = w > 8 ? 8 : w;
    }
    auto pow2 = [](uint32_t x) { return x && !(x & (x - 1)); };
    if (!pow2(c->ringBytes) || c->ringBytes < 4096) { *err = "config: ring bytes must be a power of two >= 4096"; return P4_E_ARG; }
    if (c->writeReqBytes < (128u << 10)) { *err = "config: write request bytes below 128 KiB"; return P4_E_ARG; }
    const uint32_t nThreads = c->writers + c->interactive + c->bulk + c->sandbox + 1;
    if (nThreads > 64 || c->interactive == 0 || c->bulk == 0 || c->sandbox == 0) {
        *err = "config: at most 64 service threads, and at least one lane per read class";
        return P4_E_ARG;
    }
    if (c->writeSlots == 0 || c->readSlots == 0) { *err = "config: no slots"; return P4_E_ARG; }
    if (c->seqBlock < 1024) c->seqBlock = 1024;
    if (c->raStreams > 8) c->raStreams = 8;
    if (c->hardHeap < c->softHeap) c->hardHeap = c->softHeap;
    return P4_OK;
}

// ---- markers ----------------------------------------------------------------------------
void encodeStore(uint8_t out[64], const uint8_t uuid[16], int64_t createdMs, uint64_t floor, uint32_t from) {
    std::memset(out, 0, 64);
    st32(out, 0x34515346u);  // "FSQ4"
    st16(out + 4, 4);
    st16(out + 6, 1);
    std::memcpy(out + 8, uuid, 16);
    st64(out + 24, uint64_t(createdMs));
    st64(out + 32, floor);
    st32(out + 40, from);
    st32(out + 56, ps::crc32c(out, 56));
}

void encodeMigrated(uint8_t out[40], const uint8_t uuid[16], int64_t writtenMs) {
    std::memset(out, 0, 40);
    st32(out, 0x4D515346u);  // "FSQM"
    st16(out + 4, 4);
    std::memcpy(out + 8, uuid, 16);
    st64(out + 24, uint64_t(writtenMs));
    st32(out + 32, ps::crc32c(out, 32));
}

void decodeMarkers(const uint8_t* s, size_t sn, const uint8_t* m, size_t mn, Markers* k) {
    if (s) {
        k->storePresent = true;
        if (sn == 64 && ld32(s) == 0x34515346u && ld32(s + 56) == ps::crc32c(s, 56)) {
            k->storeValid = true;
            k->format = ld16(s + 4);
            k->layout = ld16(s + 6);
            std::memcpy(k->uuid, s + 8, 16);
            k->createdMs = int64_t(ld64(s + 24));
            k->gseqFloor = ld64(s + 32);
            k->migratedFrom = ld32(s + 40);
        }
    }
    if (m) {
        k->migratedPresent = true;
        if (mn == 40 && ld32(m) == 0x4D515346u && ld32(m + 32) == ps::crc32c(m, 32)) {
            k->migratedValid = true;
            k->migratedFormat = ld16(m + 4);
            std::memcpy(k->migratedUuid, m + 8, 16);
            k->writtenMs = int64_t(ld64(m + 24));
        }
    }
}

// ---- host file helpers --------------------------------------------------------------------
bool ioExists(const std::string& path) {
    return flatsql_io_open(path.data(), int32_t(path.size()), FLATSQL_IO_PROBE) >= 0;
}

int32_t ioReadAll(const std::string& path, std::vector<uint8_t>* out) {
    out->clear();
    const int32_t h = flatsql_io_open(path.data(), int32_t(path.size()), FLATSQL_IO_READ);
    if (h < 0) return P4_E_IO;
    const double sz = flatsql_io_size(h);
    int32_t rc = P4_OK;
    if (sz < 0 || sz > double(1u << 30)) {
        rc = P4_E_IO;
    } else {
        out->resize(size_t(sz));
        size_t got = 0;
        while (got < out->size()) {
            const int32_t n = flatsql_io_read(h, out->data() + got, int32_t(out->size() - got), double(got));
            if (n <= 0) { rc = P4_E_IO; break; }
            got += size_t(n);
        }
    }
    flatsql_io_close(h);
    return rc;
}

namespace {
int32_t writeAt(int32_t h, const uint8_t* p, size_t n, double off) {
    size_t done = 0;
    while (done < n) {
        const int32_t w = flatsql_io_write(h, p + done, int32_t(n - done), off + double(done));
        if (w <= 0) return w == FLATSQL_IO_ERR_NOSPACE ? P4_E_NOSPACE : P4_E_IO;
        done += size_t(w);
    }
    return P4_OK;
}
}  // namespace

int32_t ioWriteNew(const std::string& path, const uint8_t* p, size_t n) {
    const int32_t h = flatsql_io_open(path.data(), int32_t(path.size()),
                                      FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC |
                                          FLATSQL_IO_CREATE_PARENTS);
    if (h < 0) return h == FLATSQL_IO_ERR_NOSPACE ? P4_E_NOSPACE : P4_E_IO;
    int32_t rc = writeAt(h, p, n, 0);
    if (rc == P4_OK && flatsql_io_sync(h) < 0) rc = P4_E_IO;
    flatsql_io_close(h);
    return rc;
}

int32_t ioAppend(const std::string& path, const uint8_t* p, size_t n) {
    const int32_t h = flatsql_io_open(path.data(), int32_t(path.size()),
                                      FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS);
    if (h < 0) return h == FLATSQL_IO_ERR_NOSPACE ? P4_E_NOSPACE : P4_E_IO;
    const double sz = flatsql_io_size(h);
    int32_t rc = sz < 0 ? P4_E_IO : writeAt(h, p, n, sz);
    if (rc == P4_OK && flatsql_io_sync(h) < 0) rc = P4_E_IO;
    flatsql_io_close(h);
    return rc;
}

int32_t ioTouch(const std::string& path) {
    const int32_t h = flatsql_io_open(path.data(), int32_t(path.size()),
                                      FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS);
    if (h < 0) return h == FLATSQL_IO_ERR_NOSPACE ? P4_E_NOSPACE : P4_E_IO;
    flatsql_io_close(h);
    return P4_OK;
}

int32_t ioUnlink(const std::string& path) {
    const int32_t rc = flatsql_io_open(path.data(), int32_t(path.size()), FLATSQL_IO_UNLINK);
    return rc >= 0 || rc == FLATSQL_IO_ERR_NOENT ? P4_OK : P4_E_IO;
}

int64_t ioSize(const std::string& path) {
    const int32_t h = flatsql_io_open(path.data(), int32_t(path.size()), FLATSQL_IO_READ);
    if (h < 0) return -1;
    const double sz = flatsql_io_size(h);
    flatsql_io_close(h);
    return sz < 0 ? -1 : int64_t(sz);
}

// ---- SQLite connections ------------------------------------------------------------------
const char* stmtSql(StmtId id) {
    switch (id) {
        // feed file: a record's rows are the rid range [seq << 16, seq << 16 | 0xffff]
        case S_R_SEQ: return "SELECT rid, n, b, c, u, at, cid, e, k, ts, f, x, length(d) FROM r WHERE rid>=?1 AND rid<=?2";
        case S_R_SEQD: return "SELECT rid, n, b, c, u, at, cid, e, k, ts, f, x, length(d), d FROM r WHERE rid>=?1 AND rid<=?2";
        case S_R_D: return "SELECT d FROM r WHERE rid=?1";
        case S_R_INS:
            return "INSERT INTO r(rid,seq,n,b,c,u,at,cid,e,k,ts,f,x,d) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)";
        case S_R_DEL: return "DELETE FROM r WHERE rid=?1";
        case S_R_URL: return "UPDATE r SET u=?2 WHERE rid=?1";
        case S_R_MAXRID: return "SELECT max(rid) FROM r WHERE rid>=?1 AND rid<=?2";
        case S_META_SET: return "INSERT OR REPLACE INTO meta(k,v) VALUES(?1,?2)";
        case S_NODE_INS: return "INSERT INTO node(id,producer,peer) VALUES(?1,?2,?3)";
        case S_BATCH_INS: return "INSERT INTO batch(id,batch,ppeer,pkey) VALUES(?1,?2,?3,?4)";
        case S_CKEY_GET: return "SELECT id FROM ckey WHERE ckey=?1";
        case S_CKEY_INS: return "INSERT INTO ckey(ckey) VALUES(?1)";
        case S_CKEY_TEXT: return "SELECT ckey FROM ckey WHERE id=?1";
        case S_URL_GET: return "SELECT id FROM url WHERE url=?1";
        case S_URL_INS: return "INSERT INTO url(url) VALUES(?1)";
        case S_URL_TEXT: return "SELECT url FROM url WHERE id=?1";
        case S_INST_PUT:
            return "INSERT OR REPLACE INTO inst(b,c,n,bytes,minw,maxw,minseq,maxseq,first,updated,maxat,maxts,url)"
                   " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)";
        case S_INST_DEL: return "DELETE FROM inst WHERE b=?1 AND c=?2";
        case S_R_CID: return "SELECT seq FROM r INDEXED BY r_c WHERE cid=?1 LIMIT 1";
        case S_R_CIDSCAN: return "SELECT seq FROM r WHERE cid=?1 LIMIT 1";
        case S_IDENT_GET: return "SELECT seq FROM ident WHERE h=?1";
        case S_IDENT_INS: return "INSERT OR REPLACE INTO ident(h,seq) VALUES(?1,?2)";
        case S_TOKC_PUT: return "INSERT OR REPLACE INTO tokc(producer,n,bytes,mints,maxts,maxseq) VALUES(?1,?2,?3,?4,?5,?6)";
        case S_TOKC_DEL: return "DELETE FROM tokc WHERE producer=?1";
        // journal
        case S_J_INS: return "INSERT INTO j(op,fid,seq,k,s,v) VALUES(?1,?2,?3,?4,?5,?6)";
        case S_J_DEL: return "DELETE FROM j WHERE id<=?1";
        case S_JM_SET: return "INSERT OR REPLACE INTO jm(k,v) VALUES(?1,?2)";
        default: return nullptr;
    }
}

// ---- small encodings ---------------------------------------------------------------------
void KVal::bind(sqlite3_stmt* q, int at) const {
    if (type == 1) sqlite3_bind_int64(q, at, i);
    else if (type == 3) sqlite3_bind_text(q, at, s.data(), int(s.size()), SQLITE_TRANSIENT);
    else sqlite3_bind_null(q, at);
}

void KVal::from(sqlite3_stmt* q, int col) {
    const int ct = sqlite3_column_type(q, col);
    type = ct == SQLITE_INTEGER ? 1 : ct == SQLITE_TEXT ? 3 : 0;
    i = type == 1 ? sqlite3_column_int64(q, col) : 0;
    if (type == 3) s.assign(reinterpret_cast<const char*>(sqlite3_column_text(q, col)), size_t(sqlite3_column_bytes(q, col)));
    else s.clear();
}

std::string nodeKey(const std::string& producer, const std::string& peer) { return producer + '\x1f' + peer; }
std::string batchKey(const std::string& batch, const std::string& ppeer, const std::string& pkey) {
    return batch + '\x1f' + ppeer + '\x1f' + pkey;
}

Conn::~Conn() {
    for (auto*& s : st)
        if (s) sqlite3_finalize(s);
    for (auto& kv : dyn) sqlite3_finalize(kv.second);
    if (db) sqlite3_close_v2(db);
}

sqlite3_stmt* Conn::get(StmtId id) {
    sqlite3_stmt* s = st[id];
    if (!s) {
        if (sqlite3_prepare_v3(db, stmtSql(id), -1, SQLITE_PREPARE_PERSISTENT, &s, nullptr) != SQLITE_OK) return nullptr;
        st[id] = s;
    } else {
        sqlite3_reset(s);
        sqlite3_clear_bindings(s);
    }
    return s;
}

sqlite3_stmt* Conn::sql(const std::string& text) {
    auto it = dyn.find(text);
    if (it != dyn.end()) {
        sqlite3_reset(it->second);
        sqlite3_clear_bindings(it->second);
        return it->second;
    }
    sqlite3_stmt* s = nullptr;
    if (sqlite3_prepare_v3(db, text.c_str(), int(text.size()), SQLITE_PREPARE_PERSISTENT, &s, nullptr) != SQLITE_OK)
        return nullptr;
    if (dyn.size() >= 64) {
        sqlite3_finalize(dyn.begin()->second);
        dyn.erase(dyn.begin());
    }
    dyn.emplace(text, s);
    return s;
}

int Conn::exec(const char* s) { return sqlite3_exec(db, s, nullptr, nullptr, nullptr); }

int64_t dbBytesOf(Conn* c, int64_t* freeBytes) {
    sqlite3_stmt* q = c->sql("SELECT page_count * page_size, freelist_count * page_size FROM pragma_page_count,"
                             " pragma_freelist_count, pragma_page_size");
    int64_t db = -1;
    if (q && sqlite3_step(q) == SQLITE_ROW) {
        db = sqlite3_column_int64(q, 0);
        if (freeBytes) *freeBytes = sqlite3_column_int64(q, 1);
    }
    if (q) sqlite3_reset(q);
    return db;
}

thread_local char tSqlLog[200] = {0};

int32_t statusOfSqlite(int rc) {
    switch (rc & 0xff) {
        case SQLITE_OK:
        case SQLITE_ROW:
        case SQLITE_DONE: return P4_OK;
        case SQLITE_NOMEM: return P4_E_NOMEM;
        case SQLITE_FULL: return P4_E_NOSPACE;
        case SQLITE_CORRUPT:
        case SQLITE_NOTADB: return P4_E_CORRUPT;
        case SQLITE_BUSY:
        case SQLITE_LOCKED: return P4_E_IO;  // never a miss (M9)
        case SQLITE_INTERRUPT: return P4_E_CANCELLED;
        case SQLITE_CANTOPEN:
        case SQLITE_IOERR:
        case SQLITE_PERM: return P4_E_IO;
        default: return P4_E_INTERNAL;
    }
}

namespace {
// `path` as a file: URI that names exactly `path`. SQLite decodes %HH in a
// URI's path and ends the path at '?' or '#'; a feed file's name carries %HH
// escapes (feedFileName) and the root may hold any of the three, so they are
// escaped here. Without this SQLite opened the decoded name ("a b.db", or a
// directory for "%2F") while the engine's own I/O created and measured the
// escaped one. An absolute path gets an empty authority ("file://" + path),
// so a path that starts with "//" is not read as an authority.
std::string fileUri(const std::string& path) {
    std::string u = path.empty() || path[0] != '/' ? "file:" : "file://";
    for (char c : path) {
        if (c == '%') u += "%25";
        else if (c == '?') u += "%3F";
        else if (c == '#') u += "%23";
        else u.push_back(c);
    }
    return u;
}
}  // namespace

int openConn(const std::string& path, OpenKind kind, uint32_t cacheKiB, uint32_t pageSize, Conn** out,
             std::string* err) {
    *out = nullptr;
    const bool reader = kind == OpenKind::Reader;
    std::string uri = fileUri(path) + "?share=1";
    if (reader) uri += "&ra=1";
    if (kind == OpenKind::Writer || kind == OpenKind::Journal || kind == OpenKind::Index) uri += "&dsync=1";
    sqlite3* db = nullptr;
    const int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX |
                      (kind == OpenKind::Writer || kind == OpenKind::Index || kind == OpenKind::Journal
                           ? SQLITE_OPEN_CREATE
                           : 0);
    int rc = sqlite3_open_v2(uri.c_str(), &db, flags, "flatsql_io");
    if (rc != SQLITE_OK) {
        if (err) *err = db ? sqlite3_errmsg(db) : "open failed";
        sqlite3_close_v2(db);
        return rc;
    }
    sqlite3_extended_result_codes(db, 1);
    sqlite3_busy_timeout(db, kind == OpenKind::Reader ? 2000 : 30000);
    char sql[512];
    if (kind == OpenKind::Writer || kind == OpenKind::Index || kind == OpenKind::Journal) {
        std::snprintf(sql, sizeof sql, "PRAGMA page_size=%u", pageSize ? pageSize : 4096);
        sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
        // A new file's switch to WAL commits through a rollback journal;
        // EXTRA makes SQLite delete that journal durably (with dsync=1 the VFS
        // fsyncs the directory), so a power loss cannot bring it back as a
        // hot journal over the WAL. The pragma below sets the steady level.
        sqlite3_exec(db, "PRAGMA synchronous=EXTRA", nullptr, nullptr, nullptr);
        rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            if (err) *err = sqlite3_errmsg(db);
            sqlite3_close_v2(db);
            return rc;
        }
    }
    // FULL everywhere: a flush trims the journal right after its type-index
    // commit, so that commit must be durable first (NORMAL lost acknowledged
    // records to a power loss between the two: t_power_loss).
    const char* sync = "FULL";
    std::snprintf(sql, sizeof sql,
                  "PRAGMA synchronous=%s; PRAGMA cache_size=-%u; PRAGMA mmap_size=0; PRAGMA temp_store=MEMORY;"
                  " PRAGMA wal_autocheckpoint=0; PRAGMA journal_size_limit=%lld",
                  sync, cacheKiB ? cacheKiB : 512, (long long)(64ll << 20));
    rc = sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
    if (rc == SQLITE_OK && reader) rc = sqlite3_exec(db, "PRAGMA query_only=1", nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
        if (err) *err = sqlite3_errmsg(db);
        sqlite3_close_v2(db);
        return rc;
    }
    Conn* c = new (std::nothrow) Conn();
    if (!c) {
        sqlite3_close_v2(db);
        return SQLITE_NOMEM;
    }
    c->db = db;
    c->path = path;
    *out = c;
    return SQLITE_OK;
}

}  // namespace p4
}  // namespace flatsql
