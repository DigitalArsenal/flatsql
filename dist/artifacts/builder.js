import { availableParallelism } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { parseSchema, SQLColumnType, } from '../schema/index.js';
import { createArtifactFieldAppender, createArtifactFieldValueReader, demoExtractors, } from './demo-extractors.js';
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
const INSERT_BATCH_SIZE = 64;
const DEFAULT_PAGE_SIZE = 32768;
const DEFAULT_MMAP_SIZE = 268435456;
const DEFAULT_CACHE_SIZE = -131072;
const DEFAULT_THREAD_COUNT = Math.min(4, Math.max(2, availableParallelism()));
const schemaCache = new Map();
function isCacheableQuerySql(sql) {
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
function normalizeRow(row, columns) {
    return columns.map((column) => row[column]);
}
function readFileId(data) {
    if (data.length < 8) {
        throw new Error('FlatBuffer payload is too short to contain a file identifier');
    }
    return decoder.decode(data.subarray(4, 8));
}
function readFileIdCode(data) {
    if (data.length < 8) {
        throw new Error('FlatBuffer payload is too short to contain a file identifier');
    }
    return (data[4] |
        (data[5] << 8) |
        (data[6] << 16) |
        (data[7] << 24)) >>> 0;
}
function encodeFileId(fileId) {
    if (fileId.length !== 4) {
        throw new Error('FlatBuffer file identifiers must be four characters');
    }
    return (fileId.charCodeAt(0) |
        (fileId.charCodeAt(1) << 8) |
        (fileId.charCodeAt(2) << 16) |
        (fileId.charCodeAt(3) << 24)) >>> 0;
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
function createArgumentAppender(fieldCount) {
    switch (fieldCount) {
        case 1:
            return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) => pendingArgs.push(fieldValues[0], recordOffset, recordLength, sequence);
        case 2:
            return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) => pendingArgs.push(fieldValues[0], fieldValues[1], recordOffset, recordLength, sequence);
        case 3:
            return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) => pendingArgs.push(fieldValues[0], fieldValues[1], fieldValues[2], recordOffset, recordLength, sequence);
        case 4:
            return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) => pendingArgs.push(fieldValues[0], fieldValues[1], fieldValues[2], fieldValues[3], recordOffset, recordLength, sequence);
        default:
            return (pendingArgs, fieldValues, recordOffset, recordLength, sequence) => {
                for (let index = 0; index < fieldValues.length; index += 1) {
                    pendingArgs.push(fieldValues[index]);
                }
                pendingArgs.push(recordOffset, recordLength, sequence);
            };
    }
}
function buildInsertSql(recordTableName, fieldNames, rowCount) {
    const columnNames = [...fieldNames.map((fieldName) => `"${fieldName}"`), 'data_offset', 'data_length', 'sequence'].join(', ');
    const valueTuple = `(${Array.from({ length: fieldNames.length + 3 }, () => '?').join(', ')})`;
    return `INSERT INTO "${recordTableName}" (${columnNames}) VALUES ${Array.from({ length: rowCount }, () => valueTuple).join(', ')}`;
}
function createBatchedRowWriter(db, recordTableName, fieldNames) {
    const fullStatement = db.prepare(buildInsertSql(recordTableName, fieldNames, INSERT_BATCH_SIZE));
    const partialStatements = new Map();
    const appendArgs = createArgumentAppender(fieldNames.length);
    let pendingArgs = [];
    let pendingRowCount = 0;
    function statementFor(rowCount) {
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
    function flushRows() {
        if (pendingRowCount === 0) {
            return;
        }
        statementFor(pendingRowCount).run(...pendingArgs);
        pendingArgs = [];
        pendingRowCount = 0;
    }
    return {
        writeRow(fieldValues, recordOffset, recordLength, sequence) {
            appendArgs(pendingArgs, fieldValues, recordOffset, recordLength, sequence);
            pendingRowCount += 1;
            if (pendingRowCount === INSERT_BATCH_SIZE) {
                flushRows();
            }
        },
        writeCompiledRow(appendFieldValues, data, recordOffset, recordLength, sequence) {
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
export class FlatSQLArtifactBuilder {
    schema;
    db;
    beginTransactionSql;
    tableByName = new Map();
    fileIdToTable = new Map();
    extractors = new Map();
    queryCache = new Map();
    sequence = 1;
    static fromSchema(source, options) {
        const schemaName = options.name ?? 'artifact';
        const cacheKey = `${schemaName}\u0000${source}`;
        let schema = schemaCache.get(cacheKey);
        if (!schema) {
            schema = parseSchema(source, schemaName);
            schemaCache.set(cacheKey, schema);
        }
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
        this.fileIdToTable.set(encodeFileId(fileId), tableName);
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
        const plan = this.createIngestPlan();
        this.queryCache.clear();
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
    query(sql) {
        const cacheable = isCacheableQuerySql(sql);
        let cached = cacheable ? this.queryCache.get(sql) : undefined;
        if (!cached) {
            const statement = this.db.prepare(sql);
            const arrayMode = typeof statement.setReturnArrays === 'function';
            if (arrayMode) {
                statement.setReturnArrays(true);
            }
            const columns = statement.columns().map((column) => column.name);
            cached = { statement, columns, arrayMode };
            if (cacheable) {
                this.queryCache.set(sql, cached);
            }
        }
        const rawRows = cached.statement.all();
        const rows = cached.arrayMode
            ? rawRows
            : rawRows.map((row) => normalizeRow(row, cached.columns));
        return {
            columns: [...cached.columns],
            rows,
            rowCount: rows.length,
        };
    }
    close() {
        this.queryCache.clear();
        this.db.close();
    }
    createIndexTables() {
        for (const table of this.schema.tables) {
            const indexedColumns = table.columns.filter((column) => column.isIndexed && !column.name.startsWith('_'));
            if (indexedColumns.length === 0) {
                continue;
            }
            const columnDefinitions = indexedColumns
                .map((column) => `"${column.name}" ${toSqliteType(column)} NOT NULL`)
                .join(',\n            ');
            this.db.exec(`CREATE TABLE IF NOT EXISTS "${this.recordTableName(table.name)}" (
            ${columnDefinitions},
            data_offset INTEGER NOT NULL,
            data_length INTEGER NOT NULL,
            sequence INTEGER NOT NULL
          )`);
            for (const column of indexedColumns) {
                this.db.exec(`CREATE VIEW IF NOT EXISTS "${this.indexTableName(table.name, column.name)}" AS
           SELECT "${column.name}" AS key, data_offset, data_length, sequence
           FROM "${this.recordTableName(table.name)}"`);
            }
        }
    }
    indexTableName(tableName, columnName) {
        return `_idx_${tableName}_${columnName}`;
    }
    recordTableName(tableName) {
        return `_rows_${tableName}`;
    }
    coveringIndexName(tableName, columnName) {
        return `_cov_${tableName}_${columnName}`;
    }
    createCoveringIndexes(table) {
        for (const column of table.columns) {
            if (!column.isIndexed || column.name.startsWith('_')) {
                continue;
            }
            this.db.exec(`CREATE INDEX "${this.coveringIndexName(table.name, column.name)}"
         ON "${this.recordTableName(table.name)}" ("${column.name}", sequence, data_offset, data_length)`);
        }
    }
    dropCoveringIndexes(table) {
        for (const column of table.columns) {
            if (!column.isIndexed || column.name.startsWith('_')) {
                continue;
            }
            this.db.exec(`DROP INDEX IF EXISTS "${this.coveringIndexName(table.name, column.name)}"`);
        }
    }
    createIngestPlan() {
        const tablePlans = new Map();
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
                    ? (data, recordOffset, recordLength, sequence) => rowWriter.writeCompiledRow(compiledAppender, data, recordOffset, recordLength, sequence)
                    : (data, recordOffset, recordLength, sequence) => rowWriter.writeRow(extractRowValues(data), recordOffset, recordLength, sequence),
                flushRows: rowWriter.flushRows,
            });
        }
        return {
            tablePlans,
            touchedTables: new Set(),
        };
    }
}
//# sourceMappingURL=builder.js.map