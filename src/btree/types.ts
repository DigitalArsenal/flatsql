// B-Tree types for FlatBuffers-SQLite

export enum KeyType {
  Null = 0,
  Int = 1,
  Float = 2,
  String = 3,
  Bytes = 4,
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
  order: number; // Maximum children per node (minimum = order/2)
}

export interface BTreeStats {
  entryCount: bigint;
  height: number;
  nodeCount: number;
}

// Comparison result type
export type CompareResult = -1 | 0 | 1;

// Compare two keys of the same type
export function compareKeys(a: IndexEntry['key'], b: IndexEntry['key'], keyType: KeyType): CompareResult {
  if (a === null && b === null) return 0;
  if (a === null) return -1;
  if (b === null) return 1;

  switch (keyType) {
    case KeyType.Int:
    case KeyType.Float:
      const numA = a as number;
      const numB = b as number;
      if (numA < numB) return -1;
      if (numA > numB) return 1;
      return 0;

    case KeyType.String:
      const strA = a as string;
      const strB = b as string;
      if (strA < strB) return -1;
      if (strA > strB) return 1;
      return 0;

    case KeyType.Bytes:
      const bytesA = a as Uint8Array;
      const bytesB = b as Uint8Array;
      const minLen = Math.min(bytesA.length, bytesB.length);
      for (let i = 0; i < minLen; i++) {
        if (bytesA[i] < bytesB[i]) return -1;
        if (bytesA[i] > bytesB[i]) return 1;
      }
      if (bytesA.length < bytesB.length) return -1;
      if (bytesA.length > bytesB.length) return 1;
      return 0;

    default:
      return 0;
  }
}
