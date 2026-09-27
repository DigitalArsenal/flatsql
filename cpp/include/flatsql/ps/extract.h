// FlatSQL partition store: per-type configuration and record extraction
// (design §5.2 step 3, §4.6 supersede, A3 producer token, A19 extraction).
//
// A type's behaviour is engine DATA, not code: the router registers a type
// with its binary schema (BFBS) and a small rule text, persisted under
// t/<fid>/s-<fp>.fsc. Rule text, one directive per line ('#' comments):
//
//   epoch   <alt>|<alt>...     first alternative that yields a value wins
//           alt: str:<path>      parseEpochString (Go storage parity)
//                f64floor:<path> floor(double seconds), 0 = absent
//                i64s:<path> | i64ms:<path>
//   col <n> <alt>|<alt>...     COL(n) posting
//           alt: u64pos:<path>  unsigned > 0 | str:<path> trimmed, non-empty
//                enum:<path>    enum name, trimmed; "" and "UNKNOWN" absent
//   epoch_day <n>              COL(n) = UTC "YYYY-MM-DD" of the epoch seconds
//   object <n>[,<n>...]        OBJECT_EPOCH key: first present COL of these
//   supersede <alt>|<alt>...   object identity (record_supersede.go parity)
//           alt: pair:<prefix>:<pathA>,<pathB> | u64:<prefix>:<path>
//                | str:<prefix>:<path>
//   <path> := NAME ( '.' NAME | '[0]' )*   ([0] = first element of a
//             vector of tables)
//
// Nothing here allocates on the record path: extracted strings point into the
// frame, the BFBS, or a caller-supplied scratch buffer.
#ifndef FLATSQL_PS_EXTRACT_H
#define FLATSQL_PS_EXTRACT_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace reflection {
struct Schema;
struct Object;
struct Field;
}  // namespace reflection

namespace flatsql {
namespace ps {

constexpr uint32_t kMaxCols = 8;

// Reject codes (ring reject entries, never traps).
enum RejectCode : int32_t {
    kRejBadEntry = -100,     // malformed ring entry
    kRejFrameSize = -101,    // size prefix / bounds / max frame
    kRejFid = -102,          // file identifier differs from the partition type
    kRejVerify = -103,       // BFBS verification failed
    kRejCid = -104,          // CID does not match the frame
    kRejSealed = -105,       // sealed envelope magic or size invalid
    kRejAttr = -106,         // RecordAttr invalid or carries > 1 tag
    kRejQuarantined = -107,
    kRejNoType = -108,
    kRejTxnTooLarge = -109,
};

struct ColValue {
    bool present = false;
    bool isU64 = false;
    uint64_t u = 0;
    const uint8_t* s = nullptr;
    size_t n = 0;
};

struct Extracted {
    bool hasEpoch = false;
    int64_t epochMs = 0;     // payload epoch (ms)
    int64_t epochSec = 0;    // Go epoch_unix parity (floor seconds)
    ColValue cols[kMaxCols];
    const uint8_t* identity = nullptr;  // supersede identity ("" = none)
    size_t identityLen = 0;
    int objectCol = -1;      // COL index used as the OBJECT_EPOCH key
};

// Go strings.TrimSpace over UTF-8 bytes (unicode.IsSpace set).
void trimSpace(const uint8_t** s, size_t* n);
// A3: sanitizeProducerID(routedProducerID(peer)): trim; empty ->
// "unattributed"; every rune outside [A-Za-z0-9] -> '_' (Go rune semantics:
// an invalid UTF-8 byte is one rune). Returns the token length.
std::string producerToken(const uint8_t* peer, size_t len);
// Go parseEpochString parity. Returns false when no layout matches.
bool parseEpochString(const uint8_t* s, size_t n, int64_t* sec, int64_t* ms);
// time.Unix(sec).UTC().Format("2006-01-02") into out[10].
void formatEpochDay(int64_t sec, char out[10]);

class TypeConfig {
public:
    // Parses a serialized config (see serialize()). Returns an error string
    // (empty on success).
    std::string parse(const uint8_t* data, size_t len);
    std::vector<uint8_t> serialize() const;
    static std::vector<uint8_t> build(const std::string& schemaName, const uint8_t fid[4],
                                      const std::vector<uint8_t>& bfbs, const std::string& rules,
                                      uint64_t maxFrame, uint64_t ringCap, uint32_t flags);

    // Record checks. frame = [u32 size][FlatBuffer]. Returns 0 or a RejectCode.
    int32_t checkFrame(const uint8_t* frame, size_t len) const;
    // Extracts keys from a verified plaintext frame. scratch (>= 1 KiB) holds
    // composed strings (identity, enum fallbacks, epoch day).
    void extract(const uint8_t* frame, size_t len, Extracted* out, uint8_t* scratch,
                 size_t scratchLen) const;

    const std::string& schemaName() const { return schemaName_; }
    const uint8_t* fid() const { return fid_; }
    uint64_t fingerprint() const { return fp_; }
    uint64_t maxFrame() const { return maxFrame_; }
    uint64_t ringCap() const { return ringCap_; }
    uint32_t flags() const { return flags_; }
    bool hasSupersede() const { return !supersede_.empty(); }
    bool verifies() const { return (flags_ & kVerifyBfbs) && schema_ != nullptr; }
    uint32_t nCols() const { return nCols_; }

    enum Flag : uint32_t {
        kVerifyBfbs = 1,
        kVerifyCid = 2,
        kControl = 4,   // control partition type (CTL rows)
    };

private:
    struct Step {
        uint16_t voffset = 0;
        uint8_t baseType = 0;   // reflection::BaseType
        uint8_t element = 0;
        bool first = false;     // [0] on a vector of tables
        int64_t defInt = 0;
        double defReal = 0;
        int32_t enumIndex = -1;
    };
    struct Path {
        std::vector<Step> steps;
    };
    enum AltKind : uint8_t {
        kAltEpochStr, kAltEpochF64Floor, kAltEpochI64s, kAltEpochI64ms,
        kAltColU64Pos, kAltColStr, kAltColEnum,
        kAltSupPair, kAltSupU64, kAltSupStr,
    };
    struct Alt {
        AltKind kind;
        Path a;
        Path b;
        std::string prefix;
    };
    std::string compile(const std::string& rules);
    std::string resolve(const std::string& text, Path* out) const;
    // Walks a path to its leaf table and field address. Returns false when absent.
    bool leaf(const uint8_t* root, const Path& p, const uint8_t** table, const Step** last) const;
    bool readString(const uint8_t* root, const Path& p, const uint8_t** s, size_t* n) const;
    bool readU64(const uint8_t* root, const Path& p, uint64_t* v) const;
    bool readI64(const uint8_t* root, const Path& p, int64_t* v) const;
    bool readF64(const uint8_t* root, const Path& p, double* v) const;
    bool readEnumName(const uint8_t* root, const Path& p, const uint8_t** s, size_t* n,
                      uint8_t* scratch, size_t scratchLen) const;

    std::string schemaName_;
    uint8_t fid_[4] = {0, 0, 0, 0};
    std::vector<uint8_t> bfbs_;
    std::string rules_;
    uint64_t maxFrame_ = 16u << 20;
    uint64_t ringCap_ = 4u << 20;
    uint32_t flags_ = kVerifyBfbs | kVerifyCid;
    uint64_t fp_ = 0;
    const reflection::Schema* schema_ = nullptr;
    std::vector<Alt> epoch_;
    std::vector<Alt> cols_[kMaxCols];
    uint32_t nCols_ = 0;
    int epochDayCol_ = -1;
    std::vector<int> objectCols_;
    std::vector<Alt> supersede_;
};

// Sealed envelope check (A19): encfield "SDF1"/"SDFN" + version 1.
bool sealedEnvelopeValid(const uint8_t* bytes, size_t len);

}  // namespace ps
}  // namespace flatsql

#endif
