import { DatabaseSync } from 'node:sqlite';
import { parseSchema, SQLColumnType } from '../schema/index.js';
import { demoExtractors } from './demo-extractors.js';
function toSqliteType(column) {
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
function readFileId(data) {
    if (data.length < 8) {
        throw new Error('FlatBuffer payload is too short to contain a file identifier');
    }
    return new TextDecoder().decode(data.subarray(4, 8));
}
function normalizeRow(row, columns) {
    return columns.map((column) => row[column]);
}
export class FlatSQLArtifactBuilder {
    schema;
    db;
    fileIdToTable = new Map();
    extractors = new Map();
    sequence = 1;
    static fromSchema(source, options) {
        const schema = parseSchema(source, options.name ?? 'artifact');
        return new FlatSQLArtifactBuilder(schema, options);
    }
    constructor(schema, options) {
        if (!options.sqlitePath) {
            throw new Error('Artifact builder requires sqlitePath');
        }
        this.schema = schema;
        this.db = new DatabaseSync(options.sqlitePath);
        this.createIndexTables();
    }
    registerFileId(fileId, tableName) {
        this.fileIdToTable.set(fileId, tableName);
    }
    setFieldExtractor(tableName, extractor) {
        this.extractors.set(tableName, extractor);
    }
    enableDemoExtractors() {
        for (const [tableName, extractor] of Object.entries(demoExtractors)) {
            this.extractors.set(tableName, extractor);
        }
    }
    ingestBuffers(buffers, options = {}) {
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
                const statement = this.db.prepare(`INSERT INTO "${this.indexTableName(table.name, column.name)}" (key, data_offset, data_length, sequence) VALUES (?, ?, ?, ?)`);
                statement.run(extractor(buffer, column.name), recordOffset, recordLength, this.sequence);
            }
            this.sequence += 1;
            currentOffset = recordOffset + recordLength;
        }
        return { recordCount: buffers.length };
    }
    query(sql) {
        const statement = this.db.prepare(sql);
        const columns = statement.columns().map((column) => column.name);
        const rows = statement
            .all()
            .map((row) => normalizeRow(row, columns));
        return {
            columns,
            rows,
            rowCount: rows.length,
        };
    }
    close() {
        this.db.close();
    }
    createIndexTables() {
        for (const table of this.schema.tables) {
            for (const column of table.columns) {
                if (!column.isIndexed || column.name.startsWith('_')) {
                    continue;
                }
                this.db.exec(`CREATE TABLE IF NOT EXISTS "${this.indexTableName(table.name, column.name)}" (
            key ${toSqliteType(column)} NOT NULL,
            data_offset INTEGER NOT NULL,
            data_length INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            PRIMARY KEY (key, sequence)
          ) WITHOUT ROWID`);
            }
        }
    }
    indexTableName(tableName, columnName) {
        return `_idx_${tableName}_${columnName}`;
    }
}
//# sourceMappingURL=builder.js.map