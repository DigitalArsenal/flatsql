// In-memory B-Tree implementation for FlatBuffers-SQLite
// This indexes into offsets in the stacked FlatBuffer data file

import {
  BTreeConfig,
  BTreeNode,
  BTreeStats,
  IndexEntry,
  KeyType,
  compareKeys,
  CompareResult,
} from './types.js';

export class BTree {
  private config: BTreeConfig;
  private nodes: Map<bigint, BTreeNode> = new Map();
  private rootId: bigint = 0n;
  private nextNodeId: bigint = 1n;
  private entryCount: bigint = 0n;

  constructor(config: BTreeConfig) {
    this.config = config;
    // Create initial empty root (leaf)
    this.rootId = this.createNode(true, 0n);
  }

  private createNode(isLeaf: boolean, parentId: bigint): bigint {
    const nodeId = this.nextNodeId++;
    const node: BTreeNode = {
      nodeId,
      isLeaf,
      entries: [],
      children: [],
      parentId,
    };
    this.nodes.set(nodeId, node);
    return nodeId;
  }

  private getNode(nodeId: bigint): BTreeNode {
    const node = this.nodes.get(nodeId);
    if (!node) {
      throw new Error(`Node ${nodeId} not found`);
    }
    return node;
  }

  // Insert a new entry into the B-tree
  insert(key: IndexEntry['key'], dataOffset: bigint, dataLength: number, sequence: bigint): void {
    const entry: IndexEntry = {
      keyType: this.config.keyType,
      key,
      dataOffset,
      dataLength,
      sequence,
    };

    const root = this.getNode(this.rootId);

    // If root is full, split it first
    if (root.entries.length >= this.config.order - 1) {
      // Create new root
      const newRootId = this.createNode(false, 0n);
      const newRoot = this.getNode(newRootId);
      newRoot.children.push(this.rootId);

      // Update old root's parent
      root.parentId = newRootId;

      // Split the old root
      this.splitChild(newRootId, 0);

      this.rootId = newRootId;
    }

    this.insertNonFull(this.rootId, entry);
    this.entryCount++;
  }

  private insertNonFull(nodeId: bigint, entry: IndexEntry): void {
    const node = this.getNode(nodeId);

    if (node.isLeaf) {
      // Find position to insert
      let i = node.entries.length - 1;
      while (i >= 0 && compareKeys(entry.key, node.entries[i].key, this.config.keyType) < 0) {
        i--;
      }
      // Insert at position i + 1
      node.entries.splice(i + 1, 0, entry);
    } else {
      // Find child to descend into
      let i = node.entries.length - 1;
      while (i >= 0 && compareKeys(entry.key, node.entries[i].key, this.config.keyType) < 0) {
        i--;
      }
      i++;

      const childId = node.children[i];
      const child = this.getNode(childId);

      // If child is full, split it
      if (child.entries.length >= this.config.order - 1) {
        this.splitChild(nodeId, i);
        // After split, determine which child to use
        if (compareKeys(entry.key, node.entries[i].key, this.config.keyType) > 0) {
          i++;
        }
      }

      this.insertNonFull(node.children[i], entry);
    }
  }

  private splitChild(parentId: bigint, childIndex: number): void {
    const parent = this.getNode(parentId);
    const child = this.getNode(parent.children[childIndex]);
    const mid = Math.floor((this.config.order - 1) / 2);

    // Create new sibling node
    const siblingId = this.createNode(child.isLeaf, parentId);
    const sibling = this.getNode(siblingId);

    // Move second half of entries to sibling
    sibling.entries = child.entries.splice(mid + 1);
    const midEntry = child.entries.pop()!;

    // If not leaf, move children too
    if (!child.isLeaf) {
      sibling.children = child.children.splice(mid + 1);
      // Update parent references for moved children
      for (const sibChildId of sibling.children) {
        const sibChild = this.getNode(sibChildId);
        sibChild.parentId = siblingId;
      }
    }

    // Insert mid entry and sibling into parent
    parent.entries.splice(childIndex, 0, midEntry);
    parent.children.splice(childIndex + 1, 0, siblingId);
  }

  // Search for entries matching a key
  search(key: IndexEntry['key']): IndexEntry[] {
    return this.searchNode(this.rootId, key);
  }

  private searchNode(nodeId: bigint, key: IndexEntry['key']): IndexEntry[] {
    const node = this.getNode(nodeId);
    const results: IndexEntry[] = [];

    let i = 0;
    while (i < node.entries.length && compareKeys(key, node.entries[i].key, this.config.keyType) > 0) {
      i++;
    }

    // Collect all matching entries (handles duplicates)
    while (i < node.entries.length && compareKeys(key, node.entries[i].key, this.config.keyType) === 0) {
      results.push(node.entries[i]);
      i++;
    }

    if (!node.isLeaf) {
      // Search appropriate child
      let childIdx = 0;
      while (childIdx < node.entries.length && compareKeys(key, node.entries[childIdx].key, this.config.keyType) > 0) {
        childIdx++;
      }
      results.push(...this.searchNode(node.children[childIdx], key));
    }

    return results;
  }

  // Range query: find all entries where minKey <= key <= maxKey
  range(minKey: IndexEntry['key'], maxKey: IndexEntry['key']): IndexEntry[] {
    const results: IndexEntry[] = [];
    this.rangeSearch(this.rootId, minKey, maxKey, results);
    return results;
  }

  private rangeSearch(
    nodeId: bigint,
    minKey: IndexEntry['key'],
    maxKey: IndexEntry['key'],
    results: IndexEntry[]
  ): void {
    const node = this.getNode(nodeId);

    // Find first entry that might be in range
    let i = 0;
    while (i < node.entries.length) {
      const entry = node.entries[i];
      const cmpMin = compareKeys(entry.key, minKey, this.config.keyType);
      const cmpMax = compareKeys(entry.key, maxKey, this.config.keyType);

      // Visit left child if it might contain entries in range
      if (!node.isLeaf && cmpMin >= 0) {
        this.rangeSearch(node.children[i], minKey, maxKey, results);
      }

      // Add entry if in range
      if (cmpMin >= 0 && cmpMax <= 0) {
        results.push(entry);
      }

      // Stop if we're past maxKey
      if (cmpMax > 0) {
        break;
      }

      i++;
    }

    // Visit the rightmost child if we haven't stopped early
    if (!node.isLeaf && i === node.entries.length && node.children.length > i) {
      this.rangeSearch(node.children[i], minKey, maxKey, results);
    }
  }

  // Get all entries (full scan)
  all(): IndexEntry[] {
    const results: IndexEntry[] = [];
    this.collectAll(this.rootId, results);
    return results;
  }

  private collectAll(nodeId: bigint, results: IndexEntry[]): void {
    const node = this.getNode(nodeId);

    for (let i = 0; i < node.entries.length; i++) {
      if (!node.isLeaf) {
        this.collectAll(node.children[i], results);
      }
      results.push(node.entries[i]);
    }

    if (!node.isLeaf && node.children.length > node.entries.length) {
      this.collectAll(node.children[node.children.length - 1], results);
    }
  }

  // Get tree statistics
  getStats(): BTreeStats {
    return {
      entryCount: this.entryCount,
      height: this.getHeight(this.rootId),
      nodeCount: this.nodes.size,
    };
  }

  private getHeight(nodeId: bigint): number {
    const node = this.getNode(nodeId);
    if (node.isLeaf) {
      return 1;
    }
    return 1 + this.getHeight(node.children[0]);
  }

  // Serialize the B-tree to a format suitable for FlatBuffer storage
  serialize(): {
    config: BTreeConfig;
    rootId: bigint;
    nodes: BTreeNode[];
    stats: BTreeStats;
  } {
    return {
      config: this.config,
      rootId: this.rootId,
      nodes: Array.from(this.nodes.values()),
      stats: this.getStats(),
    };
  }

  // Restore a B-tree from serialized data
  static deserialize(data: {
    config: BTreeConfig;
    rootId: bigint;
    nodes: BTreeNode[];
  }): BTree {
    const tree = new BTree(data.config);
    tree.nodes.clear();
    tree.rootId = data.rootId;

    let maxId = 0n;
    let entryCount = 0n;

    for (const node of data.nodes) {
      tree.nodes.set(node.nodeId, node);
      if (node.nodeId > maxId) {
        maxId = node.nodeId;
      }
      if (node.isLeaf) {
        entryCount += BigInt(node.entries.length);
      }
    }

    tree.nextNodeId = maxId + 1n;
    tree.entryCount = entryCount;

    return tree;
  }
}
