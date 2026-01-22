import { StackedFlatBufferStore } from '../storage/index.js';
import { TableDef } from '../schema/index.js';
export interface TableRecord {
    rowid: bigint;
    offset: bigint;
    data: Uint8Array;
    fields: Map<string, any>;
}
export declare class TableStore {
    private tableDef;
    private storage;
    private indexes;
    private rowIdCounter;
    private fieldAccessor;
    constructor(tableDef: TableDef, storage: StackedFlatBufferStore, fieldAccessor: (data: Uint8Array, fieldPath: string[]) => any);
    private columnTypeToKeyType;
    insert(flatbufferData: Uint8Array): bigint;
    findByIndex(columnName: string, key: any): TableRecord[];
    findByRange(columnName: string, minKey: any, maxKey: any): TableRecord[];
    scanAll(): TableRecord[];
    getByRowId(rowid: bigint): TableRecord | null;
    private entriesToRecords;
    private extractFields;
    getTableDef(): TableDef;
    getIndexNames(): string[];
    getRecordCount(): bigint;
}
//# sourceMappingURL=table-store.d.ts.map