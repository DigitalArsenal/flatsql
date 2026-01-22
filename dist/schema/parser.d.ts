import { DatabaseSchema } from './types.js';
interface FBField {
    name: string;
    type: string;
    isVector: boolean;
    defaultValue?: any;
    attributes?: string[];
}
interface FBTable {
    name: string;
    namespace: string;
    fields: FBField[];
    isStruct: boolean;
}
interface ParsedSchema {
    tables: FBTable[];
    enums: Map<string, string[]>;
    rootType?: string;
}
export declare function parseFlatBufferIDL(source: string): ParsedSchema;
export declare function parseJSONSchema(source: string): ParsedSchema;
export declare function toDBSchema(parsed: ParsedSchema, schemaName: string, source: string): DatabaseSchema;
export declare function parseSchema(source: string, name?: string): DatabaseSchema;
export {};
//# sourceMappingURL=parser.d.ts.map