import { DatabaseSync, type SQLiteColumnMetadata } from 'node:sqlite';

import { parseSchema, SQLColumnType, type ColumnDef, type DatabaseSchema } from '../schema/index.js';
import { demoExtractors, type ArtifactFieldExtractor } from './demo-extractors.js';
import type {
  ArtifactBuilderOptions,
  ArtifactIngestOptions,
  ArtifactIngestResult,
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

function readFileId(data: Uint8Array): string {
  if (data.length < 8) {
    throw new Error('FlatBuffer payload is too short to contain a file identifier');
  }
  return new TextDecoder().decode(data.subarray(4, 8));
}

function normalizeRow(row: Record<string, unknown>, columns: string[]): any[] {
  return columns.map((column) => row[column]);
}

export class FlatSQLArtifactBuilder {
  private readonly schema: DatabaseSchema;
  private readonly db: DatabaseSync;
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

    for (let index = 0; index < buffers.length; index++) {
      const buffer = buffers[index];
      const fileId = readFileId(buffer);
      const tableName = this.fileIdToTable.get(fileId);
      if (!tableName) {
        throw new Error(`No table registered for file identifier ${fileId}`);
      }

      const extractor = this.extractors.get(tableName);
      if (!extractor) {
        throw new Error(`No field extractor registered for table ${tableName}`);
      }

      const table = this.schema.tables.find((candidate) => candidate.name === tableName);
      if (!table) {
        throw new Error(`Table ${tableName} is not present in the parsed schema`);
      }

      const recordOffset = options.offsets?.[index] ?? currentOffset;
      const recordLength = buffer.length;

      for (const column of table.columns) {
        if (!column.isIndexed || column.name.startsWith('_')) {
          continue;
        }

        const statement = this.db.prepare(
          `INSERT INTO "${this.indexTableName(table.name, column.name)}" (key, data_offset, data_length, sequence) VALUES (?, ?, ?, ?)`
        );
        statement.run(extractor(buffer, column.name), recordOffset, recordLength, this.sequence);
      }

      this.sequence += 1;
      currentOffset = recordOffset + recordLength;
    }

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
}
