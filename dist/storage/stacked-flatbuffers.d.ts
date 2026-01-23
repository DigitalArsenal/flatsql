import { StoredRecord } from './types.js';
export interface StorageOptions {
    /** Initial storage capacity in bytes (default: 1MB) */
    initialCapacity?: number;
    /** Maximum storage size in bytes (default: 1GB, 0 = unlimited) */
    maxSize?: number;
    /** Callback when storage reaches warning threshold (80% of max) */
    onStorageWarning?: (currentSize: number, maxSize: number) => void;
}
export declare class StackedFlatBufferStore {
    private data;
    private dataView;
    private writeOffset;
    private recordCount;
    private sequence;
    private schemaName;
    private recordIndex;
    private maxSize;
    private warningEmitted;
    private onStorageWarning?;
    constructor(schemaName: string, options?: StorageOptions | number);
    private writeFileHeader;
    private updateFileHeader;
    private ensureCapacity;
    append(tableName: string, flatbufferData: Uint8Array): bigint;
    readRecord(offset: bigint): StoredRecord;
    iterateRecords(): Generator<StoredRecord>;
    iterateTableRecords(tableName: string): Generator<StoredRecord>;
    getData(): Uint8Array;
    static fromData(data: Uint8Array, options?: StorageOptions): StackedFlatBufferStore;
    getRecordCount(): bigint;
    getSchemaName(): string;
    getCurrentSize(): number;
    getMaxSize(): number;
    getUsagePercent(): number;
    isNearCapacity(): boolean;
}
//# sourceMappingURL=stacked-flatbuffers.d.ts.map