export declare enum KeyType {
    Null = 0,
    Int = 1,
    Float = 2,
    String = 3,
    Bytes = 4
}
export interface IndexEntry {
    keyType: KeyType;
    key: number | string | Uint8Array | null;
    dataOffset: bigint;
    dataLength: number;
    sequence: bigint;
}
export interface BTreeNode {
    nodeId: bigint;
    isLeaf: boolean;
    entries: IndexEntry[];
    children: bigint[];
    parentId: bigint;
}
export interface BTreeConfig {
    name: string;
    tableName: string;
    columnName: string;
    keyType: KeyType;
    order: number;
}
export interface BTreeStats {
    entryCount: bigint;
    height: number;
    nodeCount: number;
}
export type CompareResult = -1 | 0 | 1;
export declare function compareKeys(a: IndexEntry['key'], b: IndexEntry['key'], keyType: KeyType): CompareResult;
//# sourceMappingURL=types.d.ts.map