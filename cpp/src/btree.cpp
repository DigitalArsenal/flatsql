#include "flatsql/btree.h"
#include <stdexcept>
#include <algorithm>

namespace flatsql {

// Compare two Values
int compareValues(const Value& a, const Value& b) {
    // Handle null comparisons
    if (std::holds_alternative<std::monostate>(a)) {
        return std::holds_alternative<std::monostate>(b) ? 0 : -1;
    }
    if (std::holds_alternative<std::monostate>(b)) {
        return 1;
    }

    // Both non-null - compare by type
    if (a.index() != b.index()) {
        // Different types - compare by type index
        return a.index() < b.index() ? -1 : 1;
    }

    // Same type - compare values
    return std::visit([&b](const auto& aVal) -> int {
        using T = std::decay_t<decltype(aVal)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return 0;
        } else if constexpr (std::is_same_v<T, bool>) {
            bool bVal = std::get<bool>(b);
            return aVal == bVal ? 0 : (aVal < bVal ? -1 : 1);
        } else if constexpr (std::is_same_v<T, std::string>) {
            const auto& bVal = std::get<std::string>(b);
            return aVal.compare(bVal);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            const auto& bVal = std::get<std::vector<uint8_t>>(b);
            size_t minLen = std::min(aVal.size(), bVal.size());
            for (size_t i = 0; i < minLen; i++) {
                if (aVal[i] < bVal[i]) return -1;
                if (aVal[i] > bVal[i]) return 1;
            }
            if (aVal.size() < bVal.size()) return -1;
            if (aVal.size() > bVal.size()) return 1;
            return 0;
        } else {
            // Numeric types
            T bVal = std::get<T>(b);
            if (aVal < bVal) return -1;
            if (aVal > bVal) return 1;
            return 0;
        }
    }, a);
}

BTree::BTree(ValueType keyType, int order)
    : keyType_(keyType), order_(order) {
    // Create initial empty root (leaf)
    rootId_ = createNode(true, 0);
}

uint64_t BTree::createNode(bool isLeaf, uint64_t parentId) {
    uint64_t nodeId = nextNodeId_++;
    BTreeNode node;
    node.nodeId = nodeId;
    node.isLeaf = isLeaf;
    node.parentId = parentId;
    nodes_[nodeId] = std::move(node);
    return nodeId;
}

BTree::BTreeNode& BTree::getNode(uint64_t nodeId) {
    auto it = nodes_.find(nodeId);
    if (it == nodes_.end()) {
        throw std::runtime_error("Node not found: " + std::to_string(nodeId));
    }
    return it->second;
}

const BTree::BTreeNode& BTree::getNode(uint64_t nodeId) const {
    auto it = nodes_.find(nodeId);
    if (it == nodes_.end()) {
        throw std::runtime_error("Node not found: " + std::to_string(nodeId));
    }
    return it->second;
}

void BTree::insert(const Value& key, uint64_t dataOffset, uint32_t dataLength, uint64_t sequence) {
    IndexEntry entry;
    entry.key = key;
    entry.dataOffset = dataOffset;
    entry.dataLength = dataLength;
    entry.sequence = sequence;

    BTreeNode& root = getNode(rootId_);

    // If root is full, split it first
    if (static_cast<int>(root.entries.size()) >= order_ - 1) {
        // Create new root
        uint64_t newRootId = createNode(false, 0);
        BTreeNode& newRoot = getNode(newRootId);
        newRoot.children.push_back(rootId_);

        // Update old root's parent
        getNode(rootId_).parentId = newRootId;

        // Split the old root
        splitChild(newRootId, 0);

        rootId_ = newRootId;
    }

    insertNonFull(rootId_, entry);
    entryCount_++;
}

void BTree::insertNonFull(uint64_t nodeId, const IndexEntry& entry) {
    BTreeNode& node = getNode(nodeId);

    if (node.isLeaf) {
        // Find position to insert
        int i = static_cast<int>(node.entries.size()) - 1;
        while (i >= 0 && compareValues(entry.key, node.entries[i].key) < 0) {
            i--;
        }
        // Insert at position i + 1
        node.entries.insert(node.entries.begin() + i + 1, entry);
    } else {
        // Find child to descend into
        int i = static_cast<int>(node.entries.size()) - 1;
        while (i >= 0 && compareValues(entry.key, node.entries[i].key) < 0) {
            i--;
        }
        i++;

        uint64_t childId = node.children[i];
        BTreeNode& child = getNode(childId);

        // If child is full, split it
        if (static_cast<int>(child.entries.size()) >= order_ - 1) {
            splitChild(nodeId, i);
            // After split, determine which child to use
            // Need to re-fetch node as it may have been reallocated
            BTreeNode& nodeAfterSplit = getNode(nodeId);
            if (compareValues(entry.key, nodeAfterSplit.entries[i].key) > 0) {
                i++;
            }
            insertNonFull(nodeAfterSplit.children[i], entry);
        } else {
            insertNonFull(childId, entry);
        }
    }
}

void BTree::splitChild(uint64_t parentId, int childIndex) {
    BTreeNode& parent = getNode(parentId);
    uint64_t childId = parent.children[childIndex];
    BTreeNode& child = getNode(childId);

    int mid = (order_ - 1) / 2;

    // Create new sibling node
    uint64_t siblingId = createNode(child.isLeaf, parentId);
    BTreeNode& sibling = getNode(siblingId);

    // Move second half of entries to sibling
    for (size_t i = mid + 1; i < child.entries.size(); i++) {
        sibling.entries.push_back(std::move(child.entries[i]));
    }

    IndexEntry midEntry = std::move(child.entries[mid]);
    child.entries.resize(mid);

    // If not leaf, move children too
    if (!child.isLeaf) {
        for (size_t i = mid + 1; i < child.children.size(); i++) {
            sibling.children.push_back(child.children[i]);
            // Update parent references for moved children
            getNode(child.children[i]).parentId = siblingId;
        }
        child.children.resize(mid + 1);
    }

    // Insert mid entry and sibling into parent
    // Need to re-fetch parent as map may have reallocated
    BTreeNode& parentNode = getNode(parentId);
    parentNode.entries.insert(parentNode.entries.begin() + childIndex, std::move(midEntry));
    parentNode.children.insert(parentNode.children.begin() + childIndex + 1, siblingId);
}

std::vector<IndexEntry> BTree::search(const Value& key) const {
    std::vector<IndexEntry> results;
    searchNode(rootId_, key, results);
    return results;
}

void BTree::searchNode(uint64_t nodeId, const Value& key, std::vector<IndexEntry>& results) const {
    const BTreeNode& node = getNode(nodeId);

    size_t i = 0;
    while (i < node.entries.size() && compareValues(key, node.entries[i].key) > 0) {
        i++;
    }

    // Collect all matching entries (handles duplicates)
    while (i < node.entries.size() && compareValues(key, node.entries[i].key) == 0) {
        results.push_back(node.entries[i]);
        i++;
    }

    if (!node.isLeaf) {
        // Search appropriate child
        size_t childIdx = 0;
        while (childIdx < node.entries.size() && compareValues(key, node.entries[childIdx].key) > 0) {
            childIdx++;
        }
        searchNode(node.children[childIdx], key, results);
    }
}

std::vector<IndexEntry> BTree::range(const Value& minKey, const Value& maxKey) const {
    std::vector<IndexEntry> results;
    rangeSearch(rootId_, minKey, maxKey, results);
    return results;
}

void BTree::rangeSearch(uint64_t nodeId, const Value& minKey, const Value& maxKey,
                        std::vector<IndexEntry>& results) const {
    const BTreeNode& node = getNode(nodeId);

    size_t i = 0;
    while (i < node.entries.size()) {
        const IndexEntry& entry = node.entries[i];
        int cmpMin = compareValues(entry.key, minKey);
        int cmpMax = compareValues(entry.key, maxKey);

        // Visit left child if it might contain entries in range
        if (!node.isLeaf && cmpMin >= 0 && i < node.children.size()) {
            rangeSearch(node.children[i], minKey, maxKey, results);
        }

        // Add entry if in range
        if (cmpMin >= 0 && cmpMax <= 0) {
            results.push_back(entry);
        }

        // Stop if we're past maxKey
        if (cmpMax > 0) {
            break;
        }

        i++;
    }

    // Visit the rightmost child if we haven't stopped early
    if (!node.isLeaf && i == node.entries.size() && node.children.size() > i) {
        rangeSearch(node.children[i], minKey, maxKey, results);
    }
}

std::vector<IndexEntry> BTree::all() const {
    std::vector<IndexEntry> results;
    collectAll(rootId_, results);
    return results;
}

void BTree::collectAll(uint64_t nodeId, std::vector<IndexEntry>& results) const {
    const BTreeNode& node = getNode(nodeId);

    for (size_t i = 0; i < node.entries.size(); i++) {
        if (!node.isLeaf) {
            collectAll(node.children[i], results);
        }
        results.push_back(node.entries[i]);
    }

    if (!node.isLeaf && node.children.size() > node.entries.size()) {
        collectAll(node.children[node.children.size() - 1], results);
    }
}

int BTree::getHeight() const {
    return getHeightFrom(rootId_);
}

int BTree::getHeightFrom(uint64_t nodeId) const {
    const BTreeNode& node = getNode(nodeId);
    if (node.isLeaf) {
        return 1;
    }
    return 1 + getHeightFrom(node.children[0]);
}

void BTree::clear() {
    nodes_.clear();
    entryCount_ = 0;
    nextNodeId_ = 1;
    rootId_ = createNode(true, 0);
}

}  // namespace flatsql
