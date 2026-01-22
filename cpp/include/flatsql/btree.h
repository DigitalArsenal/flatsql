#ifndef FLATSQL_BTREE_H
#define FLATSQL_BTREE_H

#include "flatsql/types.h"
#include <functional>

namespace flatsql {

/**
 * In-memory B-tree for indexing FlatBuffer records.
 * Keys point to offsets in the stacked FlatBuffer storage.
 */
class BTree {
public:
    explicit BTree(ValueType keyType, int order = 128);

    // Insert an entry
    void insert(const Value& key, uint64_t dataOffset, uint32_t dataLength, uint64_t sequence);

    // Search for entries with exact key match
    std::vector<IndexEntry> search(const Value& key) const;

    // Range query: minKey <= key <= maxKey
    std::vector<IndexEntry> range(const Value& minKey, const Value& maxKey) const;

    // Get all entries (full scan)
    std::vector<IndexEntry> all() const;

    // Statistics
    uint64_t getEntryCount() const { return entryCount_; }
    int getHeight() const;
    size_t getNodeCount() const { return nodes_.size(); }

    // Clear all entries
    void clear();

private:
    struct BTreeNode {
        uint64_t nodeId;
        bool isLeaf;
        std::vector<IndexEntry> entries;
        std::vector<uint64_t> children;  // child node IDs
        uint64_t parentId;
    };

    uint64_t createNode(bool isLeaf, uint64_t parentId);
    BTreeNode& getNode(uint64_t nodeId);
    const BTreeNode& getNode(uint64_t nodeId) const;

    void insertNonFull(uint64_t nodeId, const IndexEntry& entry);
    void splitChild(uint64_t parentId, int childIndex);

    void searchNode(uint64_t nodeId, const Value& key, std::vector<IndexEntry>& results) const;
    void rangeSearch(uint64_t nodeId, const Value& minKey, const Value& maxKey,
                     std::vector<IndexEntry>& results) const;
    void collectAll(uint64_t nodeId, std::vector<IndexEntry>& results) const;
    int getHeightFrom(uint64_t nodeId) const;

    ValueType keyType_;
    int order_;
    std::map<uint64_t, BTreeNode> nodes_;
    uint64_t rootId_ = 0;
    uint64_t nextNodeId_ = 1;
    uint64_t entryCount_ = 0;
};

}  // namespace flatsql

#endif  // FLATSQL_BTREE_H
