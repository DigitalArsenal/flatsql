// Table store: manages FlatBuffer data and indexes for a single table

import { BTree, KeyType, IndexEntry } from '../btree/index.js';
import { StackedFlatBufferStore } from '../storage/index.js';
import { TableDef, ColumnDef, SQLColumnType } from '../schema/index.js';

export interface TableRecord {
  rowid: bigint;
  offset: bigint;
  data: Uint8Array;
  fields: Map<string, any>;
}

export class TableStore {
  private tableDef: TableDef;
  private storage: StackedFlatBufferStore;
  private indexes: Map<string, BTree> = new Map();
  private rowIdCounter: bigint = 1n;

  // Field accessor function - extracts field value from FlatBuffer
  private fieldAccessor: (data: Uint8Array, fieldPath: string[]) => any;

  constructor(
    tableDef: TableDef,
    storage: StackedFlatBufferStore,
    fieldAccessor: (data: Uint8Array, fieldPath: string[]) => any
  ) {
    this.tableDef = tableDef;
    this.storage = storage;
    this.fieldAccessor = fieldAccessor;

    // Create indexes for indexed columns
    for (const column of tableDef.columns) {
      if (column.isIndexed) {
        const keyType = this.columnTypeToKeyType(column.sqlType);
        this.indexes.set(
          column.name,
          new BTree({
            name: `${tableDef.name}_${column.name}_idx`,
            tableName: tableDef.name,
            columnName: column.name,
            keyType,
            order: 64, // B-tree order
          })
        );
      }
    }
  }

  private columnTypeToKeyType(sqlType: SQLColumnType): KeyType {
    switch (sqlType) {
      case SQLColumnType.INTEGER:
        return KeyType.Int;
      case SQLColumnType.REAL:
        return KeyType.Float;
      case SQLColumnType.TEXT:
        return KeyType.String;
      case SQLColumnType.BLOB:
        return KeyType.Bytes;
      default:
        return KeyType.Bytes;
    }
  }

  // Insert a new record
  insert(flatbufferData: Uint8Array): bigint {
    const rowid = this.rowIdCounter++;

    // Append to storage
    const offset = this.storage.append(this.tableDef.name, flatbufferData);

    // Update indexes
    for (const column of this.tableDef.columns) {
      const index = this.indexes.get(column.name);
      if (!index) continue;

      let key: any;
      if (column.name === '_rowid') {
        key = Number(rowid);
      } else if (column.name === '_offset') {
        key = Number(offset);
      } else {
        key = this.fieldAccessor(flatbufferData, column.flatbufferPath);
      }

      index.insert(key, offset, flatbufferData.length, rowid);
    }

    return rowid;
  }

  // Find records by indexed column
  findByIndex(columnName: string, key: any): TableRecord[] {
    const index = this.indexes.get(columnName);
    if (!index) {
      throw new Error(`No index on column: ${columnName}`);
    }

    const entries = index.search(key);
    return this.entriesToRecords(entries);
  }

  // Range query on indexed column
  findByRange(columnName: string, minKey: any, maxKey: any): TableRecord[] {
    const index = this.indexes.get(columnName);
    if (!index) {
      throw new Error(`No index on column: ${columnName}`);
    }

    const entries = index.range(minKey, maxKey);
    return this.entriesToRecords(entries);
  }

  // Get all records (full table scan)
  scanAll(): TableRecord[] {
    const index = this.indexes.get('_rowid');
    if (index) {
      const entries = index.all();
      return this.entriesToRecords(entries);
    }

    // Fallback: scan storage directly
    const records: TableRecord[] = [];
    for (const storedRecord of this.storage.iterateTableRecords(this.tableDef.name)) {
      const fields = this.extractFields(storedRecord.data);
      records.push({
        rowid: storedRecord.header.sequence,
        offset: storedRecord.offset,
        data: storedRecord.data,
        fields,
      });
    }
    return records;
  }

  // Get record by rowid
  getByRowId(rowid: bigint): TableRecord | null {
    const records = this.findByIndex('_rowid', Number(rowid));
    return records.length > 0 ? records[0] : null;
  }

  private entriesToRecords(entries: IndexEntry[]): TableRecord[] {
    const records: TableRecord[] = [];

    for (const entry of entries) {
      const storedRecord = this.storage.readRecord(entry.dataOffset);
      const fields = this.extractFields(storedRecord.data);

      records.push({
        rowid: entry.sequence,
        offset: entry.dataOffset,
        data: storedRecord.data,
        fields,
      });
    }

    return records;
  }

  private extractFields(data: Uint8Array): Map<string, any> {
    const fields = new Map<string, any>();

    for (const column of this.tableDef.columns) {
      if (column.name.startsWith('_')) continue;

      try {
        const value = this.fieldAccessor(data, column.flatbufferPath);
        fields.set(column.name, value);
      } catch {
        fields.set(column.name, null);
      }
    }

    return fields;
  }

  getTableDef(): TableDef {
    return this.tableDef;
  }

  getIndexNames(): string[] {
    return Array.from(this.indexes.keys());
  }

  getRecordCount(): bigint {
    const index = this.indexes.get('_rowid');
    if (index) {
      return index.getStats().entryCount;
    }
    return this.storage.getRecordCount();
  }
}
