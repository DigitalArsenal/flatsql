// FlatSQL partition store: registry (design §4.7, A3, A10).
//
// registry.fsl is a log of CRC-framed [u32 len][u32 crc][u16 kind][payload];
// registry.fsh is an A/B head holding the durable frame count, the log end,
// the largest pid and the store incarnation. PARTITION_ADD is appended and
// fsynced before p/<pid>/ exists (intent rule), so a registered pid with no
// valid head is an empty partition and a pid is never reused (A10).
#ifndef FLATSQL_PS_REGISTRY_H
#define FLATSQL_PS_REGISTRY_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "flatsql/ps/format.h"
#include "flatsql/ps/io.h"

namespace flatsql {
namespace ps {

struct PartitionEntry {
    uint32_t pid = 0;
    uint8_t fid[4] = {0, 0, 0, 0};
    std::string token;    // A3 producer token
    std::string sqlName;  // sds_p_<token>__<TYPE>
    uint64_t schemaFp = 0;
    uint32_t ordinal = 0;
    int64_t ctimeMs = 0;
    bool quarantined = false;
    bool dropped = false;
};

struct TypeEntry {
    uint8_t fid[4] = {0, 0, 0, 0};
    std::string schemaName;
    uint64_t configFp = 0;
    uint64_t indexSpecHash = 0;
    uint64_t supersedeRuleHash = 0;
};

class Registry {
public:
    // Opens or creates the registry, scanning frames past the head to the
    // first bad CRC and truncating there (A10). Returns 0 or a negative status.
    int32_t open(IoCtx* io, const std::string& root, bool create, std::string* err);
    // Records and fsyncs the store incarnation chosen by open (strictly above
    // every incarnation found in any head) before any batch is written.
    int32_t beginIncarnation(uint32_t inc);
    void close();

    int32_t appendPartition(const PartitionEntry& e);  // durable on return
    int32_t appendType(const TypeEntry& e);             // durable on return
    int32_t appendQuarantine(uint32_t pid, bool on, const std::string& reason);

    const std::vector<PartitionEntry>& partitions() const { return parts_; }
    const std::vector<TypeEntry>& types() const { return types_; }
    uint32_t maxPid() const { return maxPid_; }
    uint32_t incarnation() const { return incarnation_; }
    uint64_t frameCount() const { return frames_; }
    // Opening I/O stats.
    uint64_t framesTruncated() const { return truncated_; }

private:
    int32_t appendFrame(uint16_t kind, const std::vector<uint8_t>& payload);
    int32_t writeHead(bool sync);
    void apply(uint16_t kind, const uint8_t* p, size_t n);

    IoCtx* io_ = nullptr;
    std::string root_;
    FileRef log_;
    FileRef head_;
    uint64_t fslEnd_ = 0;
    uint64_t frames_ = 0;
    uint32_t maxPid_ = 0;
    uint32_t incarnation_ = 0;
    uint64_t headGen_ = 0;
    uint64_t truncated_ = 0;
    std::vector<PartitionEntry> parts_;
    std::vector<TypeEntry> types_;
};

}  // namespace ps
}  // namespace flatsql

#endif
