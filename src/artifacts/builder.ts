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
  extractArtifactFields,
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

interface PreparedInsert {
  readonly columnName: string;
  readonly keyType: SQLColumnType;
  readonly statement: ReturnType<DatabaseSync['prepare']>;
  readonly entries: PendingInsert[];
  ordered: boolean;
  lastKey?: unknown;
}

interface IngestPlan {
  readonly extractor: ArtifactFieldExtractor;
  readonly fieldNames: string[];
  readonly inserts: PreparedInsert[];
}

interface PendingInsert {
  readonly key: unknown;
  readonly recordOffset: number;
  readonly recordLength: number;
  readonly sequence: number;
}

function compareKeys(left: unknown, right: unknown, keyType: SQLColumnType): number {
  if (left === right) {
    return 0;
  }

  if (left == null) {
    return -1;
  }

  if (right == null) {
    return 1;
  }

  switch (keyType) {
    case SQLColumnType.INTEGER:
    case SQLColumnType.REAL:
      return Number(left) - Number(right);
    case SQLColumnType.TEXT:
      return String(left) < String(right) ? -1 : 1;
    case SQLColumnType.BLOB:
      return Buffer.compare(Buffer.from(left as Uint8Array), Buffer.from(right as Uint8Array));
    default:
      return String(left) < String(right) ? -1 : 1;
  }
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
    const plans = new Map<string, IngestPlan>();

    for (let index = 0; index < buffers.length; index++) {
      const buffer = buffers[index];
      const fileId = readFileId(buffer);
      const tableName = this.fileIdToTable.get(fileId);
      if (!tableName) {
        throw new Error(`No table registered for file identifier ${fileId}`);
      }

      let plan = plans.get(tableName);
      if (!plan) {
        plan = this.createIngestPlan(tableName);
        plans.set(tableName, plan);
      }

      const recordOffset = options.offsets?.[index] ?? currentOffset;
      const recordLength = buffer.length;
      const extractedFields = extractArtifactFields(plan.extractor, buffer, plan.fieldNames);

      for (const insert of plan.inserts) {
        const key = extractedFields[insert.columnName];
        if (insert.ordered && insert.lastKey !== undefined && compareKeys(insert.lastKey, key, insert.keyType) > 0) {
          insert.ordered = false;
        }
        insert.lastKey = key;
        insert.entries.push({
          key,
          recordOffset,
          recordLength,
          sequence: this.sequence,
        });
      }

      this.sequence += 1;
      currentOffset = recordOffset + recordLength;
    }

    withTransaction(this.db, this.beginTransactionSql, () => {
      for (const plan of plans.values()) {
        for (const insert of plan.inserts) {
          if (!insert.ordered) {
            insert.entries.sort(
              (left, right) => compareKeys(left.key, right.key, insert.keyType) || left.sequence - right.sequence
            );
          }

          for (const entry of insert.entries) {
            insert.statement.run(entry.key, entry.recordOffset, entry.recordLength, entry.sequence);
          }
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
      for (const column of table.columns) {
        if (!column.isIndexed || column.name.startsWith('_')) {
          continue;
        }

        this.db.exec(
          `CREATE TABLE IF NOT EXISTS "${this.indexTableName(table.name, column.name)}" (
            key ${toSqliteType(column)} NOT NULL,
            data_offset INTEGER NOT NULL,
            data_length INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            PRIMARY KEY (key, sequence)
          ) WITHOUT ROWID`
        );
      }
    }
  }

  private indexTableName(tableName: string, columnName: string): string {
    return `_idx_${tableName}_${columnName}`;
  }

  private createIngestPlan(tableName: string): IngestPlan {
    const extractor = this.extractors.get(tableName);
    if (!extractor) {
      throw new Error(`No field extractor registered for table ${tableName}`);
    }

    const table = this.tableByName.get(tableName);
    if (!table) {
      throw new Error(`Table ${tableName} is not present in the parsed schema`);
    }

    return {
      extractor,
      fieldNames: table.columns
        .filter((column) => column.isIndexed && !column.name.startsWith('_'))
        .map((column) => column.name),
      inserts: table.columns
        .filter((column) => column.isIndexed && !column.name.startsWith('_'))
        .map((column) => ({
          columnName: column.name,
          keyType: column.sqlType,
          entries: [],
          ordered: true,
          statement: this.db.prepare(
            `INSERT INTO "${this.indexTableName(table.name, column.name)}" (key, data_offset, data_length, sequence) VALUES (?, ?, ?, ?)`
          ),
        })),
    };
  }
}
