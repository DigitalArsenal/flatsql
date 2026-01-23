// Main database class: FlatBuffers-SQLite
// Provides SQL query interface over FlatBuffer storage

import { TableStore, TableRecord } from './table-store.js';
import { StackedFlatBufferStore } from '../storage/index.js';
import { DatabaseSchema, TableDef, parseSchema } from '../schema/index.js';
import { BTree } from '../btree/index.js';

// Security: Maximum SQL query length to prevent ReDoS attacks
const MAX_SQL_LENGTH = 10000;

// Security: Maximum queries per second (0 = unlimited)
const DEFAULT_RATE_LIMIT = 0;

export interface QueryResult {
  columns: string[];
  rows: any[][];
  rowCount: number;
}

export interface FlatBufferAccessor {
  // Extract a field value from FlatBuffer data given a path
  getField(data: Uint8Array, path: string[]): any;
  // Build a FlatBuffer from field values
  buildBuffer(tableName: string, fields: Record<string, any>): Uint8Array;
}

export interface DatabaseOptions {
  /** Maximum SQL query length (default: 10000) */
  maxSqlLength?: number;
  /** Maximum queries per second, 0 for unlimited (default: 0) */
  rateLimit?: number;
}

export class FlatSQLDatabase {
  private schema: DatabaseSchema;
  private storage: StackedFlatBufferStore;
  private tables: Map<string, TableStore> = new Map();
  private accessor: FlatBufferAccessor;

  // Security: Configuration options
  private maxSqlLength: number;
  private rateLimit: number;
  private queryTimestamps: number[] = [];

  constructor(schema: DatabaseSchema, accessor: FlatBufferAccessor, options: DatabaseOptions = {}) {
    this.schema = schema;
    this.storage = new StackedFlatBufferStore(schema.name);
    this.accessor = accessor;
    this.maxSqlLength = options.maxSqlLength ?? MAX_SQL_LENGTH;
    this.rateLimit = options.rateLimit ?? DEFAULT_RATE_LIMIT;

    // Initialize table stores
    for (const tableDef of schema.tables) {
      const tableStore = new TableStore(
        tableDef,
        this.storage,
        (data, path) => this.accessor.getField(data, path)
      );
      this.tables.set(tableDef.name, tableStore);
    }
  }

  // Create database from schema source (IDL or JSON Schema)
  static fromSchema(
    source: string,
    accessor: FlatBufferAccessor,
    name: string = 'default',
    options: DatabaseOptions = {}
  ): FlatSQLDatabase {
    const schema = parseSchema(source, name);
    return new FlatSQLDatabase(schema, accessor, options);
  }

  // Insert a record (as JSON that will be converted to FlatBuffer)
  insert(tableName: string, data: Record<string, any>): bigint {
    const tableStore = this.tables.get(tableName);
    if (!tableStore) {
      throw new Error(`Table not found: ${tableName}`);
    }

    const buffer = this.accessor.buildBuffer(tableName, data);
    return tableStore.insert(buffer);
  }

  // Insert raw FlatBuffer data
  insertRaw(tableName: string, flatbufferData: Uint8Array): bigint {
    const tableStore = this.tables.get(tableName);
    if (!tableStore) {
      throw new Error(`Table not found: ${tableName}`);
    }

    return tableStore.insert(flatbufferData);
  }

  // Stream in multiple FlatBuffers
  stream(tableName: string, flatbuffers: Iterable<Uint8Array>): bigint[] {
    const rowids: bigint[] = [];
    for (const fb of flatbuffers) {
      rowids.push(this.insertRaw(tableName, fb));
    }
    return rowids;
  }

  // Execute a simple SQL query
  // Supports: SELECT columns FROM table WHERE column = value
  //           SELECT columns FROM table WHERE column BETWEEN min AND max
  //           SELECT * FROM table
  query(sql: string): QueryResult {
    // Security: Check SQL length to prevent ReDoS
    if (sql.length > this.maxSqlLength) {
      throw new Error(`SQL query exceeds maximum length of ${this.maxSqlLength} characters`);
    }

    // Security: Rate limiting
    if (this.rateLimit > 0) {
      this.enforceRateLimit();
    }

    const parsed = this.parseSQL(sql);

    if (parsed.type !== 'SELECT') {
      throw new Error(`Unsupported query type: ${parsed.type}`);
    }

    const tableStore = this.tables.get(parsed.table);
    if (!tableStore) {
      throw new Error(`Table not found: ${parsed.table}`);
    }

    let records: TableRecord[];

    if (parsed.where) {
      if (parsed.where.operator === '=') {
        records = tableStore.findByIndex(parsed.where.column, parsed.where.value);
      } else if (parsed.where.operator === 'BETWEEN') {
        records = tableStore.findByRange(
          parsed.where.column,
          parsed.where.minValue,
          parsed.where.maxValue
        );
      } else {
        // Full scan with filter
        records = tableStore.scanAll().filter(record => {
          const value = record.fields.get(parsed.where!.column);
          return this.evaluateCondition(value, parsed.where!.operator, parsed.where!.value);
        });
      }
    } else {
      records = tableStore.scanAll();
    }

    // Determine columns
    const tableDef = tableStore.getTableDef();
    const columns = parsed.columns[0] === '*'
      ? tableDef.columns.map(c => c.name)
      : parsed.columns;

    // Build result rows
    const rows: any[][] = records.map(record => {
      return columns.map(col => {
        if (col === '_rowid') return record.rowid;
        if (col === '_offset') return record.offset;
        return record.fields.get(col) ?? null;
      });
    });

    return {
      columns,
      rows,
      rowCount: rows.length,
    };
  }

  private parseSQL(sql: string): ParsedQuery {
    const normalized = sql.trim().replace(/\s+/g, ' ');

    // SELECT query
    const selectMatch = normalized.match(
      /^SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?$/i
    );

    if (selectMatch) {
      const columns = selectMatch[1].split(',').map(c => c.trim());
      const table = selectMatch[2];
      const whereClause = selectMatch[3];

      let where: ParsedQuery['where'];

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
        } else {
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

    // Security: Don't expose SQL in error message (could contain sensitive data)
    throw new Error('SQL parse error: unsupported query syntax');
  }

  private parseValue(str: string): any {
    str = str.trim();

    // String literal
    if ((str.startsWith("'") && str.endsWith("'")) || (str.startsWith('"') && str.endsWith('"'))) {
      return str.slice(1, -1);
    }

    // Integer (check for BigInt needs)
    if (/^-?\d+$/.test(str)) {
      const num = BigInt(str);
      // Security: Use BigInt for values exceeding safe integer range
      if (num > BigInt(Number.MAX_SAFE_INTEGER) || num < BigInt(Number.MIN_SAFE_INTEGER)) {
        return num;
      }
      return Number(str);
    }

    // Float
    if (/^-?\d+\.\d+$/.test(str)) {
      return parseFloat(str);
    }

    // Boolean
    if (str.toLowerCase() === 'true') return true;
    if (str.toLowerCase() === 'false') return false;

    // NULL
    if (str.toLowerCase() === 'null') return null;

    return str;
  }

  // Security: Rate limiting enforcement
  private enforceRateLimit(): void {
    const now = Date.now();
    const windowMs = 1000; // 1 second window

    // Remove timestamps older than the window
    this.queryTimestamps = this.queryTimestamps.filter(ts => now - ts < windowMs);

    if (this.queryTimestamps.length >= this.rateLimit) {
      throw new Error(`Rate limit exceeded: maximum ${this.rateLimit} queries per second`);
    }

    this.queryTimestamps.push(now);
  }

  private evaluateCondition(value: any, operator: string, target: any): boolean {
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
  getTableDef(tableName: string): TableDef | undefined {
    return this.tables.get(tableName)?.getTableDef();
  }

  // List all tables
  listTables(): string[] {
    return Array.from(this.tables.keys());
  }

  // Get raw storage data (for export)
  exportData(): Uint8Array {
    return this.storage.getData();
  }

  // Get schema
  getSchema(): DatabaseSchema {
    return this.schema;
  }

  // Get statistics
  getStats(): { tableName: string; recordCount: bigint; indexes: string[] }[] {
    return Array.from(this.tables.entries()).map(([name, store]) => ({
      tableName: name,
      recordCount: store.getRecordCount(),
      indexes: store.getIndexNames(),
    }));
  }
}

interface ParsedQuery {
  type: 'SELECT' | 'INSERT';
  table: string;
  columns: string[];
  where?: {
    column: string;
    operator: string;
    value?: any;
    minValue?: any;
    maxValue?: any;
  };
}
