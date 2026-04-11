import { DatabaseSync, type SQLiteColumnMetadata } from 'node:sqlite';

import {
  parseSchema,
  SQLColumnType,
  type ColumnDef,
  type DatabaseSchema,
  type TableDef,
} from '../schema/index.js';
import {
  createArtifactFieldValueReader,
  demoExtractors,
  type ArtifactFieldExtractor,
} from './demo-extractors.js';
import type {
  ArtifactBuilderOptions,
  ArtifactIngestOptions,
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
const DEFAULT_PAGE_SIZE = 16384;
const schemaCache = new Map<string, DatabaseSchema>();

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
  return new DataView(data.buffer, data.byteOffset, data.byteLength).getUint32(4, true);
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

function normalizeRow(row: Record<string, unknown>, columns: string[]): any[] {
  return columns.map((column) => row[column]);
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
  db.exec('PRAGMA threads = 2');

  if (profile === 'safe') {
    db.exec('PRAGMA journal_mode = DELETE');
    db.exec('PRAGMA synchronous = FULL');
    return 'BEGIN IMMEDIATE';
  }

  db.exec('PRAGMA journal_mode = OFF');
  db.exec('PRAGMA synchronous = OFF');
  db.exec('PRAGMA locking_mode = EXCLUSIVE');
  db.exec('PRAGMA temp_store = MEMORY');
  db.exec('PRAGMA cache_size = -65536');
  return 'BEGIN EXCLUSIVE';
}

interface TablePlan {
  readonly table: TableDef;
  readonly extractRowValues: (data: Uint8Array) => unknown[];
  readonly writeRow: (fieldValues: unknown[], recordOffset: number, recordLength: number, sequence: number) => void;
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
): Pick<TablePlan, 'writeRow' | 'flushRows'> {
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
        const rowValues = tablePlan.extractRowValues(buffer);
        tablePlan.writeRow(rowValues, recordOffset, buffer.length, this.sequence);

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

  query(sql: string): ArtifactQueryResult {
    const statement = this.db.prepare(sql);
    const columns = statement.columns().map((column: SQLiteColumnMetadata) => column.name);
    const rows = statement
      .all()
      .map((row: Record<string, unknown>) => normalizeRow(row, columns));

    return {
      columns,
      rows,
      rowCount: rows.length,
    };
  }

  close(): void {
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
      tablePlans.set(tableName, {
        table,
        extractRowValues: createArtifactFieldValueReader(extractor, fieldNames),
        writeRow: rowWriter.writeRow,
        flushRows: rowWriter.flushRows,
      });
    }

    return {
      tablePlans,
      touchedTables: new Set<string>(),
    };
  }
}
