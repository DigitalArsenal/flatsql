// Independent format-5 stock-reader check shared by the replay and crash E2Es.
// Reads size prefixes and released SDS tables, never engine recovery code.
#ifndef FLATSQL_P4_TEST_FRAME_WALK_H
#define FLATSQL_P4_TEST_FRAME_WALK_H
#include <array>
#include <cstring>
#include <functional>
#include <map>
#include <string>
#include <vector>
#include <flatbuffers/flatbuffers.h>
#include "sds_frames.h"
#include "flatsql/ps/platform.h"

namespace p4t {
struct FrameWalk {
    uint64_t records = 0, commits = 0, rows = 0, metadataBytes = 0;
    std::map<int64_t, int64_t> frames;
};
inline uint32_t walkU32(const uint8_t* p) {
    return uint32_t(p[0]) | uint32_t(p[1]) << 8 | uint32_t(p[2]) << 16 | uint32_t(p[3]) << 24;
}
inline std::string walkFrames(uint64_t size, const std::function<bool(uint64_t, void*, size_t)>& read,
                              const std::string& fid, FrameWalk* out) {
    struct Record { uint32_t len; std::array<uint8_t, 32> digest; };
    std::map<uint64_t, Record> records;
    uint64_t at = 0, previous = UINT64_MAX, commitEnd = 0;
    uint32_t feed = 0;
    while (at < size) {
        uint8_t prefix[4];
        if (size - at < 4 || !read(at, prefix, 4)) return "torn size prefix at " + std::to_string(at);
        const uint32_t n = walkU32(prefix);
        if (n < 8 || n > size - at - 4) return "incomplete frame at " + std::to_string(at);
        std::vector<uint64_t> aligned((size_t(n) + 7) / 8);
        auto* p = reinterpret_cast<uint8_t*>(aligned.data());
        if (!read(at + 4, p, n)) return "unreadable frame at " + std::to_string(at);
        const bool sealed = !std::memcmp(p, "SDF1", 4) || !std::memcmp(p, "SDFN", 4);
        if (!sealed && walkU32(p) > n - 4) return "invalid root at " + std::to_string(at);
        if (std::memcmp(p + 4, "$FCK", 4) == 0) {
            flatbuffers::Verifier v(p, n);
            if (!VerifyFCKBuffer(v)) return "FCK verifier at " + std::to_string(at);
            const auto* ck = GetFCK(p);
            if (ck->SELF_OFFSET() != at || ck->PREVIOUS() != (at == 0 ? UINT64_MAX : previous) || !ck->TYPE() ||
                "$" + ck->TYPE()->str() != fid || (ck->SEGMENTS() && ck->SEGMENTS()->size()) ||
                (at != 0 && ck->FEED_ID() != feed)) return "invalid checkpoint";
            feed = ck->FEED_ID();
            previous = at;
            commitEnd = at + 4 + n;
            out->commits++;
        } else if (std::memcmp(p + 4, "$FMB", 4) == 0) {
            flatbuffers::Verifier v(p, n);
            if (!VerifyFMBBuffer(v)) return "FMB verifier at " + std::to_string(at);
            const auto* mb = GetFMB(p);
            if (!commitEnd || mb->SELF_OFFSET() != at || mb->PREVIOUS() != previous || mb->FEED_ID() != feed)
                return "broken commit chain at " + std::to_string(at);
            size_t digest = 0;
            if (mb->ROWS()) for (uint32_t i = 0; i < mb->ROWS()->size(); i++) {
                const auto* row = mb->ROWS()->Get(i);
                const auto it = records.find(row->OFFSET());
                if (it == records.end() || it->second.len != row->LENGTH()) return "row names incomplete record";
                if (row->FLAGS() & 1u) {
                    if (row->OFFSET() < commitEnd || row->OFFSET() + 4 + row->LENGTH() > at || !mb->DIGESTS() ||
                        digest + 32 > mb->DIGESTS()->size()) return "NEW_FRAME outside round";
                    const uint8_t* want = mb->DIGESTS()->data() + digest;
                    if (row->FLAGS() & 2u) {
                        want = nullptr;
                        if (mb->SEALED()) for (const auto* sealed : *mb->SEALED())
                            if (sealed->ROW() == i && sealed->LEAF_DIGEST() && sealed->LEAF_DIGEST()->size() == 32)
                                want = sealed->LEAF_DIGEST()->data();
                    }
                    if (!want || std::memcmp(want, it->second.digest.data(), 32)) return "record digest mismatch";
                    digest += 32;
                }
                out->rows++;
            }
            if (mb->DIGESTS() && digest != mb->DIGESTS()->size()) return "unexpected digest bytes";
            previous = at;
            commitEnd = at + 4 + n;
            out->commits++;
            out->metadataBytes += 4 + n;
        } else {
            if (!commitEnd || (std::memcmp(p + 4, fid.data(), 4) && std::memcmp(p, "SDF1", 4) && std::memcmp(p, "SDFN", 4)))
                return "unexpected record identifier at " + std::to_string(at);
            Record r{n, {}};
            flatsql::ps::sha256(p, n, r.digest.data());
            records.emplace(at, r);
            out->frames[int64_t(at)] = n;
            out->records++;
        }
        at += 4 + n;
    }
    return commitEnd != size ? "uncommitted records at EOF" : "";
}
}  // namespace p4t
#endif
