import { DatabaseSync, type SQLiteColumnMetadata } from 'node:sqlite';

import {
  parseSchema,
  SQLColumnType,
  type ColumnDef,
  type DatabaseSchema,
  type TableDef,
} from '../schema/index.js';
import {
  demoExtractors,
  extractArtifactFieldValues,
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

function readFileId(data: Uint8Array): string {
  if (data.length < 8) {
    throw new Error('FlatBuffer payload is too short to contain a file identifier');
  }
  return decoder.decode(data.subarray(4, 8));
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
  readonly extractor: ArtifactFieldExtractor;
  readonly fieldNames: string[];
  readonly emitRow: (fieldValues: unknown[], recordOffset: number, recordLength: number, sequence: number) => void;
}

function createRowEmitter(
  statement: ReturnType<DatabaseSync['prepare']>,
  fieldCount: number
): (fieldValues: unknown[], recordOffset: number, recordLength: number, sequence: number) => void {
  switch (fieldCount) {
    case 1:
      return (fieldValues, recordOffset, recordLength, sequence) =>
        statement.run(fieldValues[0], recordOffset, recordLength, sequence);
    case 2:
      return (fieldValues, recordOffset, recordLength, sequence) =>
        statement.run(fieldValues[0], fieldValues[1], recordOffset, recordLength, sequence);
    case 3:
      return (fieldValues, recordOffset, recordLength, sequence) =>
        statement.run(fieldValues[0], fieldValues[1], fieldValues[2], recordOffset, recordLength, sequence);
    case 4:
      return (fieldValues, recordOffset, recordLength, sequence) =>
        statement.run(fieldValues[0], fieldValues[1], fieldValues[2], fieldValues[3], recordOffset, recordLength, sequence);
    default:
      return (fieldValues, recordOffset, recordLength, sequence) =>
        statement.run(...fieldValues, recordOffset, recordLength, sequence);
  }
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
  private readonly fileIdToTable = new Map<string, string>();
  private readonly extractors = new Map<string, ArtifactFieldExtractor>();
  private sequence = 1;

  static fromSchema(source: string, options: ArtifactBuilderOptions): FlatSQLArtifactBuilder {
    const schema = parseSchema(source, options.name ?? 'artifact');
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
    this.fileIdToTable.set(fileId, tableName);
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
        const fileId = readFileId(buffer);
        const tableName = this.fileIdToTable.get(fileId);
        if (!tableName) {
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
        const rowValues = extractArtifactFieldValues(tablePlan.extractor, buffer, tablePlan.fieldNames);
        tablePlan.emitRow(rowValues, recordOffset, buffer.length, this.sequence);

        this.sequence += 1;
        currentOffset = recordOffset + buffer.length;
      }

      for (const tableName of plan.touchedTables) {
        const tablePlan = plan.tablePlans.get(tableName);
        if (tablePlan) {
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

      const placeholders = fieldNames.map(() => '?').join(', ');
      tablePlans.set(tableName, {
        table,
        extractor,
        fieldNames,
        emitRow: createRowEmitter(
          this.db.prepare(
          `INSERT INTO "${this.recordTableName(table.name)}" (${fieldNames.map((fieldName) => `"${fieldName}"`).join(', ')}, data_offset, data_length, sequence)
           VALUES (${placeholders}, ?, ?, ?)`
          ),
          fieldNames.length
        ),
      });
    }

    return {
      tablePlans,
      touchedTables: new Set<string>(),
    };
  }
}
