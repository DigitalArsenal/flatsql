// FlatBuffers-SQLite: SQL query interface over FlatBuffer storage
// Files are stacked FlatBuffers readable by standard FlatBuffer tools

export * from './btree/index.js';
export * from './storage/index.js';
export * from './schema/index.js';
export * from './core/index.js';
export * from './flatbuffer-accessor.js';

// Re-export main types for convenience
export { FlatSQLDatabase, QueryResult, FlatBufferAccessor } from './core/database.js';
export { TableStore, TableRecord } from './core/table-store.js';
export { BTree } from './btree/btree.js';
export { StackedFlatBufferStore } from './storage/stacked-flatbuffers.js';
export { parseSchema, DatabaseSchema, TableDef, ColumnDef } from './schema/index.js';
export { FlatcAccessor, DirectAccessor } from './flatbuffer-accessor.js';
