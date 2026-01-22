import { StoredRecord } from './types.js';
export declare class StackedFlatBufferStore {
    private data;
    private dataView;
    private writeOffset;
    private recordCount;
    private sequence;
    private schemaName;
    private recordIndex;
    constructor(schemaName: string, initialCapacity?: number);
    private writeFileHeader;
    private updateFileHeader;
    private ensureCapacity;
    append(tableName: string, flatbufferData: Uint8Array): bigint;
    readRecord(offset: bigint): StoredRecord;
    iterateRecords(): Generator<StoredRecord>;
    iterateTableRecords(tableName: string): Generator<StoredRecord>;
    getData(): Uint8Array;
    static fromData(data: Uint8Array): StackedFlatBufferStore;
    getRecordCount(): bigint;
    getSchemaName(): string;
}
//# sourceMappingURL=stacked-flatbuffers.d.ts.map