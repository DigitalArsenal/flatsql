// Schema types for FlatBuffers-SQLite

export enum SQLColumnType {
  NULL = 'NULL',
  INTEGER = 'INTEGER',
  REAL = 'REAL',
  TEXT = 'TEXT',
  BLOB = 'BLOB',
}

export interface ColumnDef {
  name: string;
  sqlType: SQLColumnType;
  flatbufferPath: string[]; // Path to field in FlatBuffer (e.g., ["address", "city"])
  fbTypeName: string; // Original FlatBuffer type
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

// FlatBuffer type to SQL type mapping
export function fbTypeToSQL(fbType: string): SQLColumnType {
  const type = fbType.toLowerCase();

  // Integer types
  if (['bool', 'byte', 'ubyte', 'int8', 'uint8', 'short', 'ushort', 'int16', 'uint16',
       'int', 'uint', 'int32', 'uint32', 'long', 'ulong', 'int64', 'uint64'].includes(type)) {
    return SQLColumnType.INTEGER;
  }

  // Float types
  if (['float', 'float32', 'double', 'float64'].includes(type)) {
    return SQLColumnType.REAL;
  }

  // String type
  if (type === 'string') {
    return SQLColumnType.TEXT;
  }

  // Default to BLOB for complex types
  return SQLColumnType.BLOB;
}
