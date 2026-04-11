import { DatabaseSync } from 'node:sqlite';
import { parseSchema, SQLColumnType, } from '../schema/index.js';
import { demoExtractors, extractArtifactFields, } from './demo-extractors.js';
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
const decoder = new TextDecoder();
function readFileId(data) {
    if (data.length < 8) {
        throw new Error('FlatBuffer payload is too short to contain a file identifier');
    }
    return decoder.decode(data.subarray(4, 8));
}
function normalizeRow(row, columns) {
    return columns.map((column) => row[column]);
}
function withTransaction(db, beginSql, operation) {
    let started = false;
    try {
        db.exec(beginSql);
        started = true;
        const result = operation();
        db.exec('COMMIT');
        started = false;
        return result;
    }
    catch (error) {
        if (started) {
            db.exec('ROLLBACK');
        }
        throw error;
    }
}
function applyPerformanceProfile(db, profile) {
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
function compareKeys(left, right, keyType) {
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
            return Buffer.compare(Buffer.from(left), Buffer.from(right));
        default:
            return String(left) < String(right) ? -1 : 1;
    }
}
export class FlatSQLArtifactBuilder {
    schema;
    db;
    beginTransactionSql;
    tableByName = new Map();
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
        this.beginTransactionSql = applyPerformanceProfile(this.db, options.performanceProfile ?? 'fast');
        for (const table of schema.tables) {
            this.tableByName.set(table.name, table);
        }
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
        const plans = new Map();
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
                        insert.entries.sort((left, right) => compareKeys(left.key, right.key, insert.keyType) || left.sequence - right.sequence);
                    }
                    for (const entry of insert.entries) {
                        insert.statement.run(entry.key, entry.recordOffset, entry.recordLength, entry.sequence);
                    }
                }
            }
        });
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
    createIngestPlan(tableName) {
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
                statement: this.db.prepare(`INSERT INTO "${this.indexTableName(table.name, column.name)}" (key, data_offset, data_length, sequence) VALUES (?, ?, ?, ?)`),
            })),
        };
    }
}
//# sourceMappingURL=builder.js.map