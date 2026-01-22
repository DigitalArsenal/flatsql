import { FlatBufferAccessor } from './core/database.js';
interface FlatcRunner {
    generateBinary(schema: SchemaInput, json: string): Uint8Array;
    generateJSON(schema: SchemaInput, binary: {
        path: string;
        data: Uint8Array;
    }): string;
    generateCode(schema: SchemaInput, language: string, options?: any): Record<string, string>;
}
interface SchemaInput {
    entry: string;
    files: Record<string, string>;
}
export declare class FlatcAccessor implements FlatBufferAccessor {
    private flatc;
    private schemaInput;
    private tableSchemas;
    constructor(flatc: FlatcRunner, schemaSource: string, schemaName?: string);
    registerTableSchema(tableName: string, schemaSource: string): void;
    private getSchema;
    getField(data: Uint8Array, path: string[]): any;
    buildBuffer(tableName: string, fields: Record<string, any>): Uint8Array;
    toJSON(data: Uint8Array, tableName?: string): Record<string, any>;
    fromJSON(obj: Record<string, any>, tableName?: string): Uint8Array;
}
export declare class DirectAccessor implements FlatBufferAccessor {
    private accessors;
    private builders;
    registerAccessor(tableName: string, accessor: (data: Uint8Array, path: string[]) => any): void;
    registerBuilder(tableName: string, builder: (fields: Record<string, any>) => Uint8Array): void;
    getField(data: Uint8Array, path: string[]): any;
    buildBuffer(tableName: string, fields: Record<string, any>): Uint8Array;
}
export {};
//# sourceMappingURL=flatbuffer-accessor.d.ts.map