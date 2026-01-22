export interface RecordHeader {
    sequence: bigint;
    tableName: string;
    timestamp: bigint;
    dataLength: number;
    checksum: number;
}
export interface DataFileHeader {
    magic: number;
    version: number;
    dataStartOffset: bigint;
    recordCount: bigint;
    schemaName: string;
}
export interface StoredRecord {
    header: RecordHeader;
    offset: bigint;
    data: Uint8Array;
}
export declare const MAGIC = 1179407441;
export declare const VERSION = 1;
export declare function crc32(data: Uint8Array): number;
//# sourceMappingURL=types.d.ts.map