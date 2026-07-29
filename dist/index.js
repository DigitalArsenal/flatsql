// FlatBuffers-SQLite: SQL query interface over FlatBuffer storage
// Files are stacked FlatBuffers readable by standard FlatBuffer tools
export * from './btree/index.js';
export * from './storage/index.js';
export * from './schema/index.js';
export * from './core/index.js';
export * from './cluster/index.js';
export * from './flatbuffer-accessor.js';
// Node-only surfaces (artifacts builder: node:sqlite/node:os; standalone
// runners: node:child_process/node:fs) live behind the 'flatsql/artifacts'
// and 'flatsql/standalone/wasmedge' subpath exports so the root entry stays
// loadable in a browser bundle. Only their type declarations are safe here.
export * from './artifacts/types.js';
export * from './response/index.js';
// Re-export main types for convenience
export { FlatSQLDatabase } from './core/database.js';
export { TableStore } from './core/table-store.js';
export { BTree } from './btree/btree.js';
export { StackedFlatBufferStore } from './storage/stacked-flatbuffers.js';
export { parseSchema } from './schema/index.js';
export { FlatcAccessor, DirectAccessor } from './flatbuffer-accessor.js';
//# sourceMappingURL=index.js.map