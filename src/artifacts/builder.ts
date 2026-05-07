import { availableParallelism } from 'node:os';
import { DatabaseSync, type SQLiteColumnMetadata } from 'node:sqlite';

import {
  parseSchema,
  SQLColumnType,
  type ColumnDef,
  type DatabaseSchema,
  type TableDef,
} from '../schema/index.js';
import {
  createArtifactFieldAppender,
  createArtifactFieldValueReader,
  demoExtractors,
  type ArtifactFieldExtractor,
} from './demo-extractors.js';
import type {
  ArtifactBuilderOptions,
  ArtifactIngestOptions,
  ArtifactQueryParams,
  ArtifactQuerySpec,
  ArtifactIngestResult,
  ArtifactPerformanceProfile,
  ArtifactQueryResult,
} from './types.js';

function toSqliteType(column: ColumnDef): string {
  switch (column.sqlType) {
    case SQLColumnType.INTEGER:
      return 'INTEGER';
    case SQLColumnType.REAL:
      return 'REAL';
    case SQLColumnType.TEXT:
      return 'TEXT';
    case SQLColumnType.BLOB:
      return 'BLOB';
    default:
      return 'BLOB';
  }
}

const decoder = new TextDecoder();
const INSERT_BATCH_SIZE = 64;
const DEFAULT_PAGE_SIZE = 32768;
const DEFAULT_MMAP_SIZE = 268435456;
const DEFAULT_CACHE_SIZE = -131072;
const DEFAULT_THREAD_COUNT = Math.min(4, Math.max(2, availableParallelism()));
const MAX_QUERY_STATEMENT_CACHE_ENTRIES = 256;
const MAX_QUERY_RESULT_CACHE_ENTRIES = 1024;
const MAX_QUERY_RESULT_CACHE_ROWS = 1000;
const schemaCache = new Map<string, DatabaseSchema>();
const VOLATILE_QUERY_PATTERNS = [
  /\bRANDOM\s*\(/i,
  /\bCHANGES\s*\(/i,
  /\bTOTAL_CHANGES\s*\(/i,
  /\bLAST_INSERT_ROWID\s*\(/i,
  /\bCURRENT_(TIME|DATE|TIMESTAMP)\b/i,
  /\bDATE\s*\(/i,
  /\bTIME\s*\(/i,
  /\bDATETIME\s*\(/i,
  /\bJULIANDAY\s*\(/i,
  /\bUNIXEPOCH\s*\(/i,
  /\bSTRFTIME\s*\(/i,
];

type QueryStatement = ReturnType<DatabaseSync['prepare']> & {
  setReturnArrays?: (enabled: boolean) => unknown;
  all: (...params: unknown[]) => unknown[];
};

function isCacheableQuerySql(sql: string): boolean {
  const trimmed = sql.trimStart();
  if (trimmed.length === 0) {
    return false;
  }

  const upper = trimmed.toUpperCase();
  if (upper.startsWith('SELECT') || upper.startsWith('WITH')) {
    return true;
  }

  return upper.startsWith('PRAGMA') && !trimmed.includes('=');
}

function isResultCacheableQuerySql(sql: string): boolean {
  return isCacheableQuerySql(sql) && !VOLATILE_QUERY_PATTERNS.some((pattern) => pattern.test(sql));
}

function normalizeRow(row: Record<string, unknown>, columns: string[]): any[] {
  return columns.map((column) => row[column]);
}

function cloneQueryResult(result: ArtifactQueryResult): ArtifactQueryResult {
  return {
    columns: [...result.columns],
    rows: result.rows.map((row) => [...row]),
    rowCount: result.rowCount,
  };
}

function encodeCacheValue(value: unknown): Record<string, unknown> | null {
  if (value === null) {
    return { type: 'null' };
  }

  switch (typeof value) {
    case 'string':
      return { type: 'string', value };
    case 'number':
      return { type: 'number', value: Object.is(value, -0) ? '-0' : String(value) };
    case 'bigint':
      return { type: 'bigint', value: value.toString() };
    case 'boolean':
      return { type: 'boolean', value };
    case 'undefined':
      return { type: 'undefined' };
    default:
      return null;
  }
}

function buildResultCacheKey(sql: string, params: ArtifactQueryParams | undefined): string | null {
  if (!isResultCacheableQuerySql(sql)) {
    return null;
  }

  if (params === undefined) {
    return sql;
  }

  if (Array.isArray(params)) {
    const encoded = params.map(encodeCacheValue);
    if (encoded.some((value) => value === null)) {
      return null;
    }
    return JSON.stringify({ sql, params: { type: 'array', values: encoded } });
  }

  const prototype = Object.getPrototypeOf(params);
  if (prototype !== Object.prototype && prototype !== null) {
    return null;
  }

  const namedParams = params as Record<string, unknown>;
  const encodedEntries: Array<{ key: string; value: Record<string, unknown> }> = [];
  const keys = Object.keys(namedParams).sort();
  for (const key of keys) {
    const encodedValue = encodeCacheValue(namedParams[key]);
    if (encodedValue === null) {
      return null;
    }
    encodedEntries.push({ key, value: encodedValue });
  }
  return JSON.stringify({ sql, params: { type: 'object', entries: encodedEntries } });
}

function getCachedResult(cache: Map<string, ArtifactQueryResult>, key: string): ArtifactQueryResult | undefined {
  const cached = cache.get(key);
  if (!cached) {
    return undefined;
  }
  cache.delete(key);
  cache.set(key, cached);
  return cached;
}

function setCachedResult(cache: Map<string, ArtifactQueryResult>, key: string, result: ArtifactQueryResult): void {
  if (result.rowCount > MAX_QUERY_RESULT_CACHE_ROWS) {
    cache.delete(key);
    return;
  }

  cache.set(key, cloneQueryResult(result));
  while (cache.size > MAX_QUERY_RESULT_CACHE_ENTRIES) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) {
      break;
    }
    cache.delete(oldest);
  }
}

function getCachedStatement<T>(cache: Map<string, T>, key: string): T | undefined {
  const cached = cache.get(key);
  if (!cached) {
    return undefined;
  }
  cache.delete(key);
  cache.set(key, cached);
  return cached;
}

function setCachedStatement<T>(cache: Map<string, T>, key: string, value: T): void {
  cache.set(key, value);
  while (cache.size > MAX_QUERY_STATEMENT_CACHE_ENTRIES) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) {
      break;
    }
    cache.delete(oldest);
  }
}

function executeStatementAll(statement: QueryStatement, params: ArtifactQueryParams | undefined): unknown[] {
  if (params === undefined) {
    return statement.all();
  }

  if (Array.isArray(params)) {
    switch (params.length) {
      case 0:
        return statement.all();
      case 1:
        return statement.all(params[0]);
      case 2:
        return statement.all(params[0], params[1]);
      case 3:
        return statement.all(params[0], params[1], params[2]);
      case 4:
        return statement.all(params[0], params[1], params[2], params[3]);
      default:
        return statement.all(...params);
    }
  }

  return statement.all(params);
}

function readFileId(data: Uint8Array): string {
  if (data.length < 8) {
    throw new Error('FlatBuffer payload is too short to contain a file identifier');
  }
  return decoder.decode(data.subarray(4, 8));
}

function readFileIdCode(data: Uint8Array): number {
  if (data.length < 8) {
    throw new Error('FlatBuffer payload is too short to contain a file identifier');
  }
  return (
    data[4] |
    (data[5] << 8) |
    (data[6] << 16) |
    (data[7] << 24)
  ) >>> 0;
}

function encodeFileId(fileId: string): number {
  if (fileId.length !== 4) {
    throw new Error('FlatBuffer file identifiers must be four characters');
  }

  return (
    fileId.charCodeAt(0) |
    (fileId.charCodeAt(1) << 8) |
    (fileId.charCodeAt(2) << 16) |
    (fileId.charCodeAt(3) << 24)
  ) >>> 0;
}

function withTransaction<T>(db: DatabaseSync, beginSql: string, operation: () => T): T {
  let started = false;

  try {
    db.exec(beginSql);
    started = true;
    const result = operation();
    db.exec('COMMIT');
    started = false;
    return result;
  } catch (error) {
    if (started) {
      db.exec('ROLLBACK');
    }
    throw error;
  }
}

function applyPerformanceProfile(db: DatabaseSync, profile: ArtifactPerformanceProfile): string {
  db.exec(`PRAGMA page_size = ${DEFAULT_PAGE_SIZE}`);
  db.exec(`PRAGMA threads = ${DEFAULT_THREAD_COUNT}`);
  db.exec(`PRAGMA mmap_size = ${DEFAULT_MMAP_SIZE}`);

  if (profile === 'safe') {
    db.exec('PRAGMA journal_mode = DELETE');
    db.exec('PRAGMA synchronous = FULL');
    return 'BEGIN IMMEDIATE';
  }

  db.exec('PRAGMA journal_mode = OFF');
  db.exec('PRAGMA synchronous = OFF');
  db.exec('PRAGMA locking_mode = EXCLUSIVE');
  db.exec('PRAGMA temp_store = MEMORY');
  db.exec(`PRAGMA cache_size = ${DEFAULT_CACHE_SIZE}`);
  return 'BEGIN EXCLUSIVE';
}

interface TablePlan {
  readonly table: TableDef;
  readonly writeRecord: (data: Uint8Array, recordOffset: number, recordLength: number, sequence: number) => void;
  readonly flushRows: () => void;
}

function createArgumentAppender(
  fieldCount: number
): (pendingArgs: unknown[], fieldValues: unknown[], recordOffset: number, recordLength: number, sequence: number) => void {
  switch (fieldCount) {
    case 1:
      return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) =>
        pendingArgs.push(fieldValues[0], recordOffset, recordLength, sequence);
    case 2:
      return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) =>
        pendingArgs.push(fieldValues[0], fieldValues[1], recordOffset, recordLength, sequence);
    case 3:
      return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) =>
        pendingArgs.push(fieldValues[0], fieldValues[1], fieldValues[2], recordOffset, recordLength, sequence);
    case 4:
      return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) =>
        pendingArgs.push(fieldValues[0], fieldValues[1], fieldValues[2], fieldValues[3], recordOffset, recordLength, sequence);
    default:
      return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) => {
        for (let index = 0; index < fieldValues.length; index += 1) {
          pendingArgs.push(fieldValues[index]);
        }
        pendingArgs.push(recordOffset, recordLength, sequence);
      };
  }
}

function buildInsertSql(recordTableName: string, fieldNames: string[], rowCount: number): string {
  const columnNames = [...fieldNames.map((fieldName) => `"${fieldName}"`), 'data_offset', 'data_length', 'sequence'].join(', ');
  const valueTuple = `(${Array.from({ length: fieldNames.length + 3 }, () => '?').join(', ')})`;
  return `INSERT INTO "${recordTableName}" (${columnNames}) VALUES ${Array.from({ length: rowCount }, () => valueTuple).join(', ')}`;
}

function createBatchedRowWriter(
  db: DatabaseSync,
  recordTableName: string,
  fieldNames: string[]
): {
  writeRow: (fieldValues: unknown[], recordOffset: number, recordLength: number, sequence: number) => void;
  writeCompiledRow: (
    appendFieldValues: (pendingArgs: unknown[], data: Uint8Array) => void,
    data: Uint8Array,
    recordOffset: number,
    recordLength: number,
    sequence: number
  ) => void;
  flushRows: () => void;
} {
  const fullStatement = db.prepare(buildInsertSql(recordTableName, fieldNames, INSERT_BATCH_SIZE));
  const partialStatements = new Map<number, ReturnType<DatabaseSync['prepare']>>();
  const appendArgs = createArgumentAppender(fieldNames.length);
  let pendingArgs: unknown[] = [];
  let pendingRowCount = 0;

  function statementFor(rowCount: number): ReturnType<DatabaseSync['prepare']> {
    if (rowCount === INSERT_BATCH_SIZE) {
      return fullStatement;
    }

    let statement = partialStatements.get(rowCount);
    if (!statement) {
      statement = db.prepare(buildInsertSql(recordTableName, fieldNames, rowCount));
      partialStatements.set(rowCount, statement);
    }
    return statement;
  }

  function flushRows(): void {
    if (pendingRowCount === 0) {
      return;
    }

    statementFor(pendingRowCount).run(...pendingArgs);
    pendingArgs = [];
    pendingRowCount = 0;
  }

  return {
    writeRow(fieldValues, recordOffset, recordLength, sequence): void {
      appendArgs(pendingArgs, fieldValues, recordOffset, recordLength, sequence);
      pendingRowCount += 1;

      if (pendingRowCount === INSERT_BATCH_SIZE) {
        flushRows();
      }
    },
    writeCompiledRow(appendFieldValues, data, recordOffset, recordLength, sequence): void {
      appendFieldValues(pendingArgs, data);
      pendingArgs.push(recordOffset, recordLength, sequence);
      pendingRowCount += 1;

      if (pendingRowCount === INSERT_BATCH_SIZE) {
        flushRows();
      }
    },
    flushRows,
  };
}

interface IngestPlan {
  readonly tablePlans: Map<string, TablePlan>;
  readonly touchedTables: Set<string>;
}

export class FlatSQLArtifactBuilder {
  private readonly schema: DatabaseSchema;
  private readonly db: DatabaseSync;
  private readonly beginTransactionSql: string;
  private readonly tableByName = new Map<string, TableDef>();
  private readonly fileIdToTable = new Map<number, string>();
  private readonly extractors = new Map<string, ArtifactFieldExtractor>();
  private readonly queryCache = new Map<string, {
    statement: QueryStatement;
    columns: string[];
    arrayMode: boolean;
  }>();
  private readonly queryResultCache = new Map<string, ArtifactQueryResult>();
  private sequence = 1;

  static fromSchema(source: string, options: ArtifactBuilderOptions): FlatSQLArtifactBuilder {
    const schemaName = options.name ?? 'artifact';
    const cacheKey = `${schemaName}\u0000${source}`;
    let schema = schemaCache.get(cacheKey);
    if (!schema) {
      schema = parseSchema(source, schemaName);
      schemaCache.set(cacheKey, schema);
    }
    return new FlatSQLArtifactBuilder(schema, options);
  }

  constructor(schema: DatabaseSchema, options: ArtifactBuilderOptions) {
    if (!options.sqlitePath) {
      throw new Error('Artifact builder requires sqlitePath');
    }

    this.schema = schema;
    this.db = new DatabaseSync(options.sqlitePath);
    this.beginTransactionSql = applyPerformanceProfile(this.db, options.performanceProfile ?? 'fast');
    for (const table of schema.tables) {
      this.tableByName.set(table.name, table);
    }
    this.createIndexTables();
  }

  registerFileId(fileId: string, tableName: string): void {
    this.fileIdToTable.set(encodeFileId(fileId), tableName);
  }

  setFieldExtractor(tableName: string, extractor: ArtifactFieldExtractor): void {
    this.extractors.set(tableName, extractor);
  }

  enableDemoExtractors(): void {
    for (const [tableName, extractor] of Object.entries(demoExtractors)) {
      this.extractors.set(tableName, extractor);
    }
  }

  ingestBuffers(buffers: Uint8Array[], options: ArtifactIngestOptions = {}): ArtifactIngestResult {
    let currentOffset = options.startOffset ?? 0;
    const plan = this.createIngestPlan();
    this.queryCache.clear();
    this.queryResultCache.clear();

    withTransaction(this.db, this.beginTransactionSql, () => {
      for (let index = 0; index < buffers.length; index++) {
        const buffer = buffers[index];
        const fileIdCode = readFileIdCode(buffer);
        const tableName = this.fileIdToTable.get(fileIdCode);
        if (!tableName) {
          const fileId = readFileId(buffer);
          throw new Error(`No table registered for file identifier ${fileId}`);
        }

        const tablePlan = plan.tablePlans.get(tableName);
        if (!tablePlan) {
          throw new Error(`Table ${tableName} is not present in the parsed schema`);
        }

        if (!plan.touchedTables.has(tableName)) {
          this.dropCoveringIndexes(tablePlan.table);
          plan.touchedTables.add(tableName);
        }

        const recordOffset = options.offsets?.[index] ?? currentOffset;
        tablePlan.writeRecord(buffer, recordOffset, buffer.length, this.sequence);

        this.sequence += 1;
        currentOffset = recordOffset + buffer.length;
      }

      for (const tableName of plan.touchedTables) {
        const tablePlan = plan.tablePlans.get(tableName);
        if (tablePlan) {
          tablePlan.flushRows();
          this.createCoveringIndexes(tablePlan.table);
        }
      }
    });

    return { recordCount: buffers.length };
  }

  query(sql: string, params?: ArtifactQueryParams): ArtifactQueryResult {
    const resultCacheKey = buildResultCacheKey(sql, params);
    const cachedResult = resultCacheKey ? getCachedResult(this.queryResultCache, resultCacheKey) : undefined;
    if (cachedResult) {
      return cloneQueryResult(cachedResult);
    }

    const cacheable = isCacheableQuerySql(sql);
    let cached = cacheable ? getCachedStatement(this.queryCache, sql) : undefined;

    if (!cached) {
      const statement = this.db.prepare(sql) as QueryStatement;
      const arrayMode = typeof statement.setReturnArrays === 'function';
      if (arrayMode) {
        statement.setReturnArrays!(true);
      }
      const columns = statement.columns().map((column: SQLiteColumnMetadata) => column.name);
      cached = { statement, columns, arrayMode };
      if (cacheable) {
        setCachedStatement(this.queryCache, sql, cached);
      }
    }

    const rawRows = executeStatementAll(cached.statement, params);
    const rows = cached.arrayMode
      ? (rawRows as unknown as any[][])
      : (rawRows as Record<string, unknown>[]).map((row) => normalizeRow(row, cached!.columns));

    const result = {
      columns: [...cached.columns],
      rows,
      rowCount: rows.length,
    };

    if (resultCacheKey) {
      setCachedResult(this.queryResultCache, resultCacheKey, result);
    }

    return resultCacheKey ? cloneQueryResult(result) : result;
  }

  queryMany(queries: readonly ArtifactQuerySpec[]): ArtifactQueryResult[] {
    return queries.map(({ sql, params }) => this.query(sql, params));
  }

  close(): void {
    this.queryCache.clear();
    this.queryResultCache.clear();
    this.db.close();
  }

  private createIndexTables(): void {
    for (const table of this.schema.tables) {
      const indexedColumns = table.columns.filter((column) => column.isIndexed && !column.name.startsWith('_'));
      if (indexedColumns.length === 0) {
        continue;
      }

      const columnDefinitions = indexedColumns
        .map((column) => `"${column.name}" ${toSqliteType(column)} NOT NULL`)
        .join(',\n            ');
      this.db.exec(
        `CREATE TABLE IF NOT EXISTS "${this.recordTableName(table.name)}" (
            ${columnDefinitions},
            data_offset INTEGER NOT NULL,
            data_length INTEGER NOT NULL,
            sequence INTEGER NOT NULL
          )`
      );

      for (const column of indexedColumns) {
        this.db.exec(
          `CREATE VIEW IF NOT EXISTS "${this.indexTableName(table.name, column.name)}" AS
           SELECT "${column.name}" AS key, data_offset, data_length, sequence
           FROM "${this.recordTableName(table.name)}"`
        );
      }
    }
  }

  private indexTableName(tableName: string, columnName: string): string {
    return `_idx_${tableName}_${columnName}`;
  }

  private recordTableName(tableName: string): string {
    return `_rows_${tableName}`;
  }

  private coveringIndexName(tableName: string, columnName: string): string {
    return `_cov_${tableName}_${columnName}`;
  }

  private createCoveringIndexes(table: TableDef): void {
    for (const column of table.columns) {
      if (!column.isIndexed || column.name.startsWith('_')) {
        continue;
      }

      this.db.exec(
        `CREATE INDEX "${this.coveringIndexName(table.name, column.name)}"
         ON "${this.recordTableName(table.name)}" ("${column.name}", sequence, data_offset, data_length)`
      );
    }
  }

  private dropCoveringIndexes(table: TableDef): void {
    for (const column of table.columns) {
      if (!column.isIndexed || column.name.startsWith('_')) {
        continue;
      }

      this.db.exec(`DROP INDEX IF EXISTS "${this.coveringIndexName(table.name, column.name)}"`);
    }
  }

  private createIngestPlan(): IngestPlan {
    const tablePlans = new Map<string, TablePlan>();

    for (const [tableName, table] of this.tableByName.entries()) {
      const extractor = this.extractors.get(tableName);
      if (!extractor) {
        continue;
      }

      const fieldNames = table.columns
        .filter((column) => column.isIndexed && !column.name.startsWith('_'))
        .map((column) => column.name);
      if (fieldNames.length === 0) {
        continue;
      }

      const recordTableName = this.recordTableName(table.name);
      const rowWriter = createBatchedRowWriter(this.db, recordTableName, fieldNames);
      const compiledAppender = createArtifactFieldAppender(extractor, fieldNames);
      const extractRowValues = compiledAppender ? null : createArtifactFieldValueReader(extractor, fieldNames);
      tablePlans.set(tableName, {
        table,
        writeRecord: compiledAppender
          ? (data, recordOffset, recordLength, sequence) =>
              rowWriter.writeCompiledRow(compiledAppender, data, recordOffset, recordLength, sequence)
          : (data, recordOffset, recordLength, sequence) =>
              rowWriter.writeRow(
                extractRowValues!(data),
                recordOffset,
                recordLength,
                sequence
              ),
        flushRows: rowWriter.flushRows,
      });
    }

    return {
      tablePlans,
      touchedTables: new Set<string>(),
    };
  }
}
