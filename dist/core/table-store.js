// Table store: manages FlatBuffer data and indexes for a single table
import { BTree, KeyType } from '../btree/index.js';
import { SQLColumnType } from '../schema/index.js';
export class TableStore {
    tableDef;
    storage;
    indexes = new Map();
    rowIdCounter = 1n;
    // Field accessor function - extracts field value from FlatBuffer
    fieldAccessor;
    constructor(tableDef, storage, fieldAccessor) {
        this.tableDef = tableDef;
        this.storage = storage;
        this.fieldAccessor = fieldAccessor;
        // Create indexes for indexed columns
        for (const column of tableDef.columns) {
            if (column.isIndexed) {
                const keyType = this.columnTypeToKeyType(column.sqlType);
                this.indexes.set(column.name, new BTree({
                    name: `${tableDef.name}_${column.name}_idx`,
                    tableName: tableDef.name,
                    columnName: column.name,
                    keyType,
                    order: 64, // B-tree order
                }));
            }
        }
    }
    columnTypeToKeyType(sqlType) {
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
    insert(flatbufferData) {
        const rowid = this.rowIdCounter++;
        // Append to storage
        const offset = this.storage.append(this.tableDef.name, flatbufferData);
        // Update indexes
        for (const column of this.tableDef.columns) {
            const index = this.indexes.get(column.name);
            if (!index)
                continue;
            let key;
            if (column.name === '_rowid') {
                key = Number(rowid);
            }
            else if (column.name === '_offset') {
                key = Number(offset);
            }
            else {
                key = this.fieldAccessor(flatbufferData, column.flatbufferPath);
            }
            index.insert(key, offset, flatbufferData.length, rowid);
        }
        return rowid;
    }
    // Find records by indexed column
    findByIndex(columnName, key) {
        const index = this.indexes.get(columnName);
        if (!index) {
            throw new Error(`No index on column: ${columnName}`);
        }
        const entries = index.search(key);
        return this.entriesToRecords(entries);
    }
    // Range query on indexed column
    findByRange(columnName, minKey, maxKey) {
        const index = this.indexes.get(columnName);
        if (!index) {
            throw new Error(`No index on column: ${columnName}`);
        }
        const entries = index.range(minKey, maxKey);
        return this.entriesToRecords(entries);
    }
    // Get all records (full table scan)
    scanAll() {
        const index = this.indexes.get('_rowid');
        if (index) {
            const entries = index.all();
            return this.entriesToRecords(entries);
        }
        // Fallback: scan storage directly
        const records = [];
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
    getByRowId(rowid) {
        const records = this.findByIndex('_rowid', Number(rowid));
        return records.length > 0 ? records[0] : null;
    }
    entriesToRecords(entries) {
        const records = [];
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
    extractFields(data) {
        const fields = new Map();
        for (const column of this.tableDef.columns) {
            if (column.name.startsWith('_'))
                continue;
            try {
                const value = this.fieldAccessor(data, column.flatbufferPath);
                fields.set(column.name, value);
            }
            catch {
                fields.set(column.name, null);
            }
        }
        return fields;
    }
    getTableDef() {
        return this.tableDef;
    }
    getIndexNames() {
        return Array.from(this.indexes.keys());
    }
    getRecordCount() {
        const index = this.indexes.get('_rowid');
        if (index) {
            return index.getStats().entryCount;
        }
        return this.storage.getRecordCount();
    }
}
//# sourceMappingURL=table-store.js.map