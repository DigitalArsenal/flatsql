// Independent format-5 stock-reader check shared by the replay and crash E2Es.
// Reads size prefixes and released SDS tables, never engine recovery code.
#ifndef FLATSQL_P4_TEST_FRAME_WALK_H
#define FLATSQL_P4_TEST_FRAME_WALK_H
#include <tuple>
#include <array>
#include <cstring>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <flatbuffers/flatbuffers.h>
#include "sds_frames.h"
#include "flatsql/ps/platform.h"

namespace p4t {
struct FrameWalk {
    uint64_t records = 0, commits = 0, rows = 0, metadataBytes = 0;
    uint64_t checkpoints = 0, checkpointBytes = 0, segments = 0, segmentBytes = 0, mergedSegments = 0;
    uint32_t generation = 0;
    bool finalCheckpoint = false;
    std::map<int64_t, int64_t> frames;
};
inline uint32_t walkU32(const uint8_t* p) {
    return uint32_t(p[0]) | uint32_t(p[1]) << 8 | uint32_t(p[2]) << 16 | uint32_t(p[3]) << 24;
}
inline std::string walkFrames(uint64_t size, const std::function<bool(uint64_t, void*, size_t)>& read,
                              const std::string& fid, FrameWalk* out) {
    struct Record { uint32_t len; std::array<uint8_t, 32> digest; };
    struct Segment { Record frame; uint8_t kind, level; uint64_t through, entries; };
    std::map<uint64_t, Record> records;
    std::map<uint64_t, Segment> segments;
    std::set<uint64_t> metadataEnds;
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
        const bool checkpoint = std::memcmp(p + 4, "$FCK", 4) == 0;
        out->finalCheckpoint = checkpoint;
        if (at == 0 && !checkpoint) return "genesis is not FCK";
        if (checkpoint) {
            flatbuffers::Verifier v(p, n);
            if (!VerifyFCKBuffer(v)) return "FCK verifier at " + std::to_string(at);
            const auto* ck = GetFCK(p);
            const auto* live = ck->SEGMENTS();
            const size_t count = live ? live->size() : 0;
            if (ck->SELF_OFFSET() != at || ck->PREVIOUS() != previous || (at && commitEnd != at) || !ck->TYPE() ||
                "$" + ck->TYPE()->str() != fid || (at == 0 && count) || (at != 0 && ck->FEED_ID() != feed))
                return "invalid checkpoint at " + std::to_string(at);
            if ((count && !ck->SEGMENT_DIGESTS()) || (ck->SEGMENT_DIGESTS() && ck->SEGMENT_DIGESTS()->size() != count * 32))
                return "checkpoint digest count";
            if (ck->APPLIED_THROUGH() && !metadataEnds.count(ck->APPLIED_THROUGH())) return "checkpoint applied through is not FMB end";
            for (size_t i = 0; i < count; i++) {
                const auto* ref = live->Get(uint32_t(i));
                const auto it = segments.find(ref->OFFSET());
                if (it == segments.end() || it->second.frame.len != ref->LENGTH() || it->second.kind != uint8_t(ref->KIND()) ||
                    it->second.level != ref->LEVEL() || it->second.through != ref->COVERS_THROUGH() || it->second.entries != ref->ENTRIES() ||
                    std::memcmp(it->second.frame.digest.data(), ck->SEGMENT_DIGESTS()->data() + 32 * i, 32))
                    return "checkpoint segment or digest mismatch";
            }
            feed = ck->FEED_ID();
            out->generation = ck->GENERATION();
            previous = at;
            commitEnd = at + 4 + n;
            out->commits++; out->checkpoints++; out->checkpointBytes += 4 + n;
        } else if (std::memcmp(p + 4, "$FSG", 4) == 0) {
            flatbuffers::Verifier v(p, n);
            if (!VerifyFSGBuffer(v)) return "FSG verifier at " + std::to_string(at);
            const auto* sg = GetFSG(p);
            if (commitEnd != at || sg->SELF_OFFSET() != at || sg->PREVIOUS() != previous || sg->FEED_ID() != feed || uint8_t(sg->KIND()) > 6)
                return "invalid segment or row-order kind";
            if (sg->COVERS_THROUGH() && !metadataEnds.count(sg->COVERS_THROUGH())) return "segment coverage is not FMB end";
            if (sg->REPLACES() && sg->REPLACES()->size()) {
                // A four-way merge one level up, or (row orders) a retention
                // rewrite of one segment at its own level.
                const bool rewrite = uint8_t(sg->KIND()) >= 5 && sg->REPLACES()->size() == 1;
                if (!rewrite && (sg->REPLACES()->size() != 4 || !sg->LEVEL())) return "invalid four-way LSM merge";
                for (auto input : *sg->REPLACES()) {
                    const auto it = segments.find(input);
                    if (it == segments.end() || it->second.kind != uint8_t(sg->KIND()) || it->second.level + (rewrite ? 0 : 1) != sg->LEVEL())
                        return "invalid LSM input";
                }
                out->mergedSegments++;
            }
            uint64_t entries = 0;
            switch (uint8_t(sg->KIND())) {
                case 0: entries = sg->ENTRIES() ? sg->ENTRIES()->size() : 0; break;
                case 1: entries = sg->LATEST() ? sg->LATEST()->size() : 0; break;
                case 2: entries = sg->INSTANCES() ? sg->INSTANCES()->size() : 0; break;
                case 3: entries = sg->TOKENS() ? sg->TOKENS()->size() : 0; break;
                case 4: entries = sg->PAIRS() ? sg->PAIRS()->size() : 0; break;
                case 5: case 6: {
                    // ROWS_BY_SEQ / ROWS_BY_OBJECT_EPOCH: sorted by (KEY_HI, KEY_LO, RID).
                    entries = sg->PAIRS() ? sg->PAIRS()->size() : 0;
                    for (uint32_t i = 1; i < entries; i++) {
                        const auto* a = sg->PAIRS()->Get(i - 1); const auto* b = sg->PAIRS()->Get(i);
                        if (std::make_tuple(a->KEY_HI(), a->KEY_LO(), a->RID()) >= std::make_tuple(b->KEY_HI(), b->KEY_LO(), b->RID()))
                            return "row-order pairs out of order";
                    }
                    break;
                }
            }
            Segment s{{n, {}}, uint8_t(sg->KIND()), sg->LEVEL(), sg->COVERS_THROUGH(), entries};
            flatsql::ps::sha256(p, n, s.frame.digest.data());
            segments.emplace(at, s);
            previous = at; commitEnd = at + 4 + n;
            out->commits++; out->segments++; out->segmentBytes += 4 + n;
        } else if (std::memcmp(p + 4, "$FMB", 4) == 0) {
            flatbuffers::Verifier v(p, n);
            if (!VerifyFMBBuffer(v)) return "FMB verifier at " + std::to_string(at);
            const auto* mb = GetFMB(p);
            if (!commitEnd || mb->SELF_OFFSET() != at || mb->PREVIOUS() != previous || mb->FEED_ID() != feed)
                return "broken commit chain at " + std::to_string(at);
            if (mb->ROWS() && mb->ROWS()->size() > 65536) return "oversized FMB";
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
            metadataEnds.insert(commitEnd);
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
