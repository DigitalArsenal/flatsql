import { DatabaseSchema, TableDef } from '../schema/index.js';
export interface QueryResult {
    columns: string[];
    rows: any[][];
    rowCount: number;
}
export interface FlatBufferAccessor {
    getField(data: Uint8Array, path: string[]): any;
    buildBuffer(tableName: string, fields: Record<string, any>): Uint8Array;
}
export declare class FlatSQLDatabase {
    private schema;
    private storage;
    private tables;
    private accessor;
    constructor(schema: DatabaseSchema, accessor: FlatBufferAccessor);
    static fromSchema(source: string, accessor: FlatBufferAccessor, name?: string): FlatSQLDatabase;
    insert(tableName: string, data: Record<string, any>): bigint;
    insertRaw(tableName: string, flatbufferData: Uint8Array): bigint;
    stream(tableName: string, flatbuffers: Iterable<Uint8Array>): bigint[];
    query(sql: string): QueryResult;
    private parseSQL;
    private parseValue;
    private evaluateCondition;
    getTableDef(tableName: string): TableDef | undefined;
    listTables(): string[];
    exportData(): Uint8Array;
    getSchema(): DatabaseSchema;
    getStats(): {
        tableName: string;
        recordCount: bigint;
        indexes: string[];
    }[];
}
//# sourceMappingURL=database.d.ts.map