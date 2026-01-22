export declare enum SQLColumnType {
    NULL = "NULL",
    INTEGER = "INTEGER",
    REAL = "REAL",
    TEXT = "TEXT",
    BLOB = "BLOB"
}
export interface ColumnDef {
    name: string;
    sqlType: SQLColumnType;
    flatbufferPath: string[];
    fbTypeName: string;
    nullable: boolean;
    defaultValue?: string | number | boolean | null;
    isPrimaryKey: boolean;
    isIndexed: boolean;
}
export interface TableDef {
    name: string;
    fbTableName: string;
    fbNamespace: string;
    columns: ColumnDef[];
    primaryKey: string[];
    indexes: string[];
}
export interface DatabaseSchema {
    name: string;
    tables: TableDef[];
    fbSchemaSource: string;
    version: number;
}
export declare function fbTypeToSQL(fbType: string): SQLColumnType;
//# sourceMappingURL=types.d.ts.map