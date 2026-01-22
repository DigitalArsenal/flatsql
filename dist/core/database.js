// Main database class: FlatBuffers-SQLite
// Provides SQL query interface over FlatBuffer storage
import { TableStore } from './table-store.js';
import { StackedFlatBufferStore } from '../storage/index.js';
import { parseSchema } from '../schema/index.js';
export class FlatSQLDatabase {
    schema;
    storage;
    tables = new Map();
    accessor;
    constructor(schema, accessor) {
        this.schema = schema;
        this.storage = new StackedFlatBufferStore(schema.name);
        this.accessor = accessor;
        // Initialize table stores
        for (const tableDef of schema.tables) {
            const tableStore = new TableStore(tableDef, this.storage, (data, path) => this.accessor.getField(data, path));
            this.tables.set(tableDef.name, tableStore);
        }
    }
    // Create database from schema source (IDL or JSON Schema)
    static fromSchema(source, accessor, name = 'default') {
        const schema = parseSchema(source, name);
        return new FlatSQLDatabase(schema, accessor);
    }
    // Insert a record (as JSON that will be converted to FlatBuffer)
    insert(tableName, data) {
        const tableStore = this.tables.get(tableName);
        if (!tableStore) {
            throw new Error(`Table not found: ${tableName}`);
        }
        const buffer = this.accessor.buildBuffer(tableName, data);
        return tableStore.insert(buffer);
    }
    // Insert raw FlatBuffer data
    insertRaw(tableName, flatbufferData) {
        const tableStore = this.tables.get(tableName);
        if (!tableStore) {
            throw new Error(`Table not found: ${tableName}`);
        }
        return tableStore.insert(flatbufferData);
    }
    // Stream in multiple FlatBuffers
    stream(tableName, flatbuffers) {
        const rowids = [];
        for (const fb of flatbuffers) {
            rowids.push(this.insertRaw(tableName, fb));
        }
        return rowids;
    }
    // Execute a simple SQL query
    // Supports: SELECT columns FROM table WHERE column = value
    //           SELECT columns FROM table WHERE column BETWEEN min AND max
    //           SELECT * FROM table
    query(sql) {
        const parsed = this.parseSQL(sql);
        if (parsed.type !== 'SELECT') {
            throw new Error(`Unsupported query type: ${parsed.type}`);
        }
        const tableStore = this.tables.get(parsed.table);
        if (!tableStore) {
            throw new Error(`Table not found: ${parsed.table}`);
        }
        let records;
        if (parsed.where) {
            if (parsed.where.operator === '=') {
                records = tableStore.findByIndex(parsed.where.column, parsed.where.value);
            }
            else if (parsed.where.operator === 'BETWEEN') {
                records = tableStore.findByRange(parsed.where.column, parsed.where.minValue, parsed.where.maxValue);
            }
            else {
                // Full scan with filter
                records = tableStore.scanAll().filter(record => {
                    const value = record.fields.get(parsed.where.column);
                    return this.evaluateCondition(value, parsed.where.operator, parsed.where.value);
                });
            }
        }
        else {
            records = tableStore.scanAll();
        }
        // Determine columns
        const tableDef = tableStore.getTableDef();
        const columns = parsed.columns[0] === '*'
            ? tableDef.columns.map(c => c.name)
            : parsed.columns;
        // Build result rows
        const rows = records.map(record => {
            return columns.map(col => {
                if (col === '_rowid')
                    return record.rowid;
                if (col === '_offset')
                    return record.offset;
                return record.fields.get(col) ?? null;
            });
        });
        return {
            columns,
            rows,
            rowCount: rows.length,
        };
    }
    parseSQL(sql) {
        const normalized = sql.trim().replace(/\s+/g, ' ');
        // SELECT query
        const selectMatch = normalized.match(/^SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?$/i);
        if (selectMatch) {
            const columns = selectMatch[1].split(',').map(c => c.trim());
            const table = selectMatch[2];
            const whereClause = selectMatch[3];
            let where;
            if (whereClause) {
                // Parse BETWEEN
                const betweenMatch = whereClause.match(/^(\w+)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)$/i);
                if (betweenMatch) {
                    where = {
                        column: betweenMatch[1],
                        operator: 'BETWEEN',
                        minValue: this.parseValue(betweenMatch[2]),
                        maxValue: this.parseValue(betweenMatch[3]),
                    };
                }
                else {
                    // Parse simple condition: column op value
                    const condMatch = whereClause.match(/^(\w+)\s*(=|!=|<>|<|>|<=|>=)\s*(.+)$/);
                    if (condMatch) {
                        where = {
                            column: condMatch[1],
                            operator: condMatch[2],
                            value: this.parseValue(condMatch[3]),
                        };
                    }
                }
            }
            return {
                type: 'SELECT',
                table,
                columns,
                where,
            };
        }
        throw new Error(`Cannot parse SQL: ${sql}`);
    }
    parseValue(str) {
        str = str.trim();
        // String literal
        if ((str.startsWith("'") && str.endsWith("'")) || (str.startsWith('"') && str.endsWith('"'))) {
            return str.slice(1, -1);
        }
        // Number
        if (/^-?\d+(\.\d+)?$/.test(str)) {
            return parseFloat(str);
        }
        // Boolean
        if (str.toLowerCase() === 'true')
            return true;
        if (str.toLowerCase() === 'false')
            return false;
        // NULL
        if (str.toLowerCase() === 'null')
            return null;
        return str;
    }
    evaluateCondition(value, operator, target) {
        switch (operator) {
            case '=':
                return value === target;
            case '!=':
            case '<>':
                return value !== target;
            case '<':
                return value < target;
            case '>':
                return value > target;
            case '<=':
                return value <= target;
            case '>=':
                return value >= target;
            default:
                return false;
        }
    }
    // Get table definition
    getTableDef(tableName) {
        return this.tables.get(tableName)?.getTableDef();
    }
    // List all tables
    listTables() {
        return Array.from(this.tables.keys());
    }
    // Get raw storage data (for export)
    exportData() {
        return this.storage.getData();
    }
    // Get schema
    getSchema() {
        return this.schema;
    }
    // Get statistics
    getStats() {
        return Array.from(this.tables.entries()).map(([name, store]) => ({
            tableName: name,
            recordCount: store.getRecordCount(),
            indexes: store.getIndexNames(),
        }));
    }
}
//# sourceMappingURL=database.js.map