import { BTreeConfig, BTreeNode, BTreeStats, IndexEntry } from './types.js';
export declare class BTree {
    private config;
    private nodes;
    private rootId;
    private nextNodeId;
    private entryCount;
    constructor(config: BTreeConfig);
    private createNode;
    private getNode;
    insert(key: IndexEntry['key'], dataOffset: bigint, dataLength: number, sequence: bigint): void;
    private insertNonFull;
    private splitChild;
    search(key: IndexEntry['key']): IndexEntry[];
    private searchNode;
    range(minKey: IndexEntry['key'], maxKey: IndexEntry['key']): IndexEntry[];
    private rangeSearch;
    all(): IndexEntry[];
    private collectAll;
    getStats(): BTreeStats;
    private getHeight;
    serialize(): {
        config: BTreeConfig;
        rootId: bigint;
        nodes: BTreeNode[];
        stats: BTreeStats;
    };
    static deserialize(data: {
        config: BTreeConfig;
        rootId: bigint;
        nodes: BTreeNode[];
    }): BTree;
}
//# sourceMappingURL=btree.d.ts.map