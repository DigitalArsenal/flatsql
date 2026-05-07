import { availableParallelism } from 'node:os';
import { parentPort } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';

const decoder = new TextDecoder();
const builders = new Map();
const INSERT_BATCH_SIZE = 64;
const DEFAULT_PAGE_SIZE = 32768;
const DEFAULT_MMAP_SIZE = 268435456;
const DEFAULT_CACHE_SIZE = -131072;
const DEFAULT_THREAD_COUNT = Math.min(4, Math.max(2, availableParallelism()));
const MAX_QUERY_STATEMENT_CACHE_ENTRIES = 256;

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

function executeStatementAll(statement, params) {
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

function getCachedStatement(cache, key) {
  const cached = cache.get(key);
  if (!cached) {
    return undefined;
  }
  cache.delete(key);
  cache.set(key, cached);
  return cached;
}

function setCachedStatement(cache, key, value) {
  cache.set(key, value);
  while (cache.size > MAX_QUERY_STATEMENT_CACHE_ENTRIES) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) {
      break;
    }
    cache.delete(oldest);
  }
}

function runQuery(builder, sql, params) {
  const cacheable = isCacheableQuerySql(sql);
  let cached = cacheable ? getCachedStatement(builder.queryCache, sql) : undefined;

  if (!cached) {
    const statement = builder.db.prepare(sql);
    const arrayMode = typeof statement.setReturnArrays === 'function';
    if (arrayMode) {
      statement.setReturnArrays(true);
    }
    const columns = statement.columns().map((column) => column.name);
    cached = { statement, columns, arrayMode };
    if (cacheable) {
      setCachedStatement(builder.queryCache, sql, cached);
    }
  }

  const rawRows = executeStatementAll(cached.statement, params);
  const rows = cached.arrayMode ? rawRows : normalizeRows(rawRows, cached.columns);
  return { columns: [...cached.columns], rows, rowCount: rows.length };
}

function sqliteType(column) {
  return column.sqlType ?? 'BLOB';
}

function createState(data) {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const root = view.getUint32(0, true);
  const vtable = root - view.getInt32(root, true);
  return {
    data,
    view,
    root,
    vtable,
    vtableSize: view.getUint16(vtable, true),
  };
}

function getFieldOffset(view, vtable, vtableSize, fieldIndex) {
  const entryOffset = vtable + 4 + fieldIndex * 2;
  if (entryOffset + 2 > vtable + vtableSize) {
    return 0;
  }
  return view.getUint16(entryOffset, true);
}

function readInt32Field(view, root, vtable, vtableSize, fieldIndex) {
  const fieldOffset = getFieldOffset(view, vtable, vtableSize, fieldIndex);
  return fieldOffset === 0 ? 0 : view.getInt32(root + fieldOffset, true);
}

function readStringField(data, view, root, vtable, vtableSize, fieldIndex) {
  const fieldOffset = getFieldOffset(view, vtable, vtableSize, fieldIndex);
  if (fieldOffset === 0) {
    return '';
  }
  const relative = view.getUint32(root + fieldOffset, true);
  const stringStart = root + fieldOffset + relative;
  const stringLength = view.getUint32(stringStart, true);
  return decoder.decode(data.subarray(stringStart + 4, stringStart + 4 + stringLength));
}

function readDescriptorValue(state, descriptor) {
  if (descriptor.kind === 'int32') {
    return readInt32Field(state.view, state.root, state.vtable, state.vtableSize, descriptor.index);
  }

  return readStringField(state.data, state.view, state.root, state.vtable, state.vtableSize, descriptor.index);
}

function readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor) {
  if (descriptor.kind === 'int32') {
    return readInt32Field(view, root, vtable, vtableSize, descriptor.index);
  }

  return readStringField(data, view, root, vtable, vtableSize, descriptor.index);
}

function createMappedExtractor(fieldDescriptors) {
  return {
    getField(data, fieldName) {
      const descriptor = fieldDescriptors[fieldName];
      if (!descriptor) {
        return null;
      }
      return readDescriptorValue(createState(data), descriptor);
    },
    compileFieldAppender(fieldNames) {
      const descriptors = fieldNames.map((fieldName) => fieldDescriptors[fieldName] ?? null);

      switch (descriptors.length) {
        case 1: {
          const [descriptor0] = descriptors;
          return (pendingArgs, data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            pendingArgs.push(
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null
            );
          };
        }
        case 2: {
          const [descriptor0, descriptor1] = descriptors;
          return (pendingArgs, data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            pendingArgs.push(
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
            );
          };
        }
        case 3: {
          const [descriptor0, descriptor1, descriptor2] = descriptors;
          return (pendingArgs, data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            pendingArgs.push(
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
              descriptor2 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor2) : null,
            );
          };
        }
        case 4: {
          const [descriptor0, descriptor1, descriptor2, descriptor3] = descriptors;
          return (pendingArgs, data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            pendingArgs.push(
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
              descriptor2 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor2) : null,
              descriptor3 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor3) : null,
            );
          };
        }
        default:
          return (pendingArgs, data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            for (const descriptor of descriptors) {
              pendingArgs.push(
                descriptor ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor) : null
              );
            }
          };
      }
    },
    compileFieldValues(fieldNames) {
      const descriptors = fieldNames.map((fieldName) => fieldDescriptors[fieldName] ?? null);

      switch (descriptors.length) {
        case 1: {
          const [descriptor0] = descriptors;
          return (data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return [
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null
            ];
          };
        }
        case 2: {
          const [descriptor0, descriptor1] = descriptors;
          return (data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return [
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
            ];
          };
        }
        case 3: {
          const [descriptor0, descriptor1, descriptor2] = descriptors;
          return (data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return [
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
              descriptor2 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor2) : null,
            ];
          };
        }
        case 4: {
          const [descriptor0, descriptor1, descriptor2, descriptor3] = descriptors;
          return (data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return [
              descriptor0 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor0) : null,
              descriptor1 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor1) : null,
              descriptor2 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor2) : null,
              descriptor3 ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor3) : null,
            ];
          };
        }
        default:
          return (data) => {
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
            const root = view.getUint32(0, true);
            const vtable = root - view.getInt32(root, true);
            const vtableSize = view.getUint16(vtable, true);
            return descriptors.map((descriptor) =>
              descriptor ? readDescriptorValueDirect(data, view, root, vtable, vtableSize, descriptor) : null
            );
          };
      }
    },
    getFields(data, fieldNames) {
      const state = createState(data);
      return Object.fromEntries(
        fieldNames.map((fieldName) => {
          const descriptor = fieldDescriptors[fieldName];
          return [fieldName, descriptor ? readDescriptorValue(state, descriptor) : null];
        })
      );
    },
    getFieldValues(data, fieldNames) {
      const state = createState(data);
      return fieldNames.map((fieldName) => {
        const descriptor = fieldDescriptors[fieldName];
        return descriptor ? readDescriptorValue(state, descriptor) : null;
      });
    },
  };
}

function extractFields(extractor, data, fieldNames) {
  if (extractor.getFields) {
    return extractor.getFields(data, fieldNames);
  }

  return Object.fromEntries(fieldNames.map((fieldName) => [fieldName, extractor.getField(data, fieldName)]));
}

function extractFieldValues(extractor, data, fieldNames) {
  if (extractor.compileFieldValues) {
    return extractor.compileFieldValues(fieldNames)(data);
  }

  if (extractor.getFieldValues) {
    return extractor.getFieldValues(data, fieldNames);
  }

  return fieldNames.map((fieldName) => extractor.getField(data, fieldName));
}

function createFieldValueReader(extractor, fieldNames) {
  if (extractor.compileFieldValues) {
    return extractor.compileFieldValues(fieldNames);
  }

  return (data) => extractFieldValues(extractor, data, fieldNames);
}

function createFieldAppender(extractor, fieldNames) {
  if (extractor.compileFieldAppender) {
    return extractor.compileFieldAppender(fieldNames);
  }

  return null;
}

const demoExtractors = {
  User: createMappedExtractor({
    id: { kind: 'int32', index: 0 },
    name: { kind: 'string', index: 1 },
    email: { kind: 'string', index: 2 },
    age: { kind: 'int32', index: 3 },
  }),
  Post: createMappedExtractor({
    id: { kind: 'int32', index: 0 },
    user_id: { kind: 'int32', index: 1 },
    title: { kind: 'string', index: 2 },
  }),
};

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
  return (
    data[4] |
    (data[5] << 8) |
    (data[6] << 16) |
    (data[7] << 24)
  ) >>> 0;
}

function encodeFileId(fileId) {
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

function indexTableName(tableName, columnName) {
  return `_idx_${tableName}_${columnName}`;
}

function recordTableName(tableName) {
  return `_rows_${tableName}`;
}

function coveringIndexName(tableName, columnName) {
  return `_cov_${tableName}_${columnName}`;
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
  } catch (error) {
    if (started) {
      db.exec('ROLLBACK');
    }
    throw error;
  }
}

function readPragmaText(db, name) {
  const row = db.prepare(`PRAGMA ${name}`).get();
  const value = row ? Object.values(row)[0] : undefined;
  return typeof value === 'string' ? value.toLowerCase() : String(value ?? '').toLowerCase();
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
  if (readPragmaText(db, 'journal_mode') !== 'off') {
    db.exec('PRAGMA journal_mode = MEMORY');
  }
  db.exec('PRAGMA synchronous = OFF');
  db.exec('PRAGMA locking_mode = EXCLUSIVE');
  db.exec('PRAGMA temp_store = MEMORY');
  db.exec(`PRAGMA cache_size = ${DEFAULT_CACHE_SIZE}`);
  return 'BEGIN EXCLUSIVE';
}

function createArgumentAppender(fieldCount) {
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

function buildInsertSql(tableName, fieldNames, rowCount) {
  const columnNames = [...fieldNames.map((fieldName) => `"${fieldName}"`), 'data_offset', 'data_length', 'sequence'].join(', ');
  const valueTuple = `(${Array.from({ length: fieldNames.length + 3 }, () => '?').join(', ')})`;
  return `INSERT INTO "${recordTableName(tableName)}" (${columnNames}) VALUES ${Array.from({ length: rowCount }, () => valueTuple).join(', ')}`;
}

function createBatchedRowWriter(db, tableName, fieldNames) {
  const fullStatement = db.prepare(buildInsertSql(tableName, fieldNames, INSERT_BATCH_SIZE));
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
      statement = db.prepare(buildInsertSql(tableName, fieldNames, rowCount));
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

function createBuilder({ builderId, schema, performanceProfile = 'fast', sqlitePath }) {
  const db = new DatabaseSync(sqlitePath);
  const state = {
    schema,
    db,
    beginTransactionSql: applyPerformanceProfile(db, performanceProfile),
    tableByName: new Map(schema.tables.map((table) => [table.name, table])),
    fileIdToTable: new Map(),
    extractors: new Map(),
    queryCache: new Map(),
    sequence: 1,
  };

  for (const table of schema.tables) {
    const indexedColumns = table.columns.filter((column) => column.isIndexed && !column.name.startsWith('_'));
    if (indexedColumns.length === 0) {
      continue;
    }

    db.exec(
      `CREATE TABLE IF NOT EXISTS "${recordTableName(table.name)}" (
        ${indexedColumns.map((column) => `"${column.name}" ${sqliteType(column)} NOT NULL`).join(',\n        ')},
        data_offset INTEGER NOT NULL,
        data_length INTEGER NOT NULL,
        sequence INTEGER NOT NULL
      )`
    );

    for (const column of indexedColumns) {
      db.exec(
        `CREATE VIEW IF NOT EXISTS "${indexTableName(table.name, column.name)}" AS
         SELECT "${column.name}" AS key, data_offset, data_length, sequence
         FROM "${recordTableName(table.name)}"`
      );
    }
  }

  builders.set(builderId, state);
  return { builderId };
}

function getBuilder(builderId) {
  const builder = builders.get(builderId);
  if (!builder) {
    throw new Error(`Artifact builder not found: ${builderId}`);
  }
  return builder;
}

function ingestRecords(state, buffers, options, transportMode) {
  let currentOffset = options?.startOffset ?? 0;
  const plans = new Map();
  state.queryCache.clear();

  withTransaction(state.db, state.beginTransactionSql, () => {
    for (let index = 0; index < buffers.length; index++) {
      const buffer = buffers[index];
      const fileIdCode = readFileIdCode(buffer);
      const tableName = state.fileIdToTable.get(fileIdCode);
      if (!tableName) {
        const fileId = readFileId(buffer);
        throw new Error(`No table registered for file identifier ${fileId}`);
      }

      let plan = plans.get(tableName);
      if (!plan) {
        const extractor = state.extractors.get(tableName);
        if (!extractor) {
          throw new Error(`No field extractor registered for table ${tableName}`);
        }

        const table = state.tableByName.get(tableName);
        if (!table) {
          throw new Error(`Table ${tableName} is not present in the parsed schema`);
        }

        plan = {
          table,
          fieldNames: table.columns
            .filter((column) => column.isIndexed && !column.name.startsWith('_'))
            .map((column) => column.name),
          writeRecord: null,
          ...createBatchedRowWriter(
            state.db,
            table.name,
            table.columns
              .filter((column) => column.isIndexed && !column.name.startsWith('_'))
              .map((column) => column.name)
          ),
          droppedIndexes: false,
        };
        const compiledAppender = createFieldAppender(extractor, plan.fieldNames);
        const extractRowValues = compiledAppender ? null : createFieldValueReader(extractor, plan.fieldNames);
        plan.writeRecord = compiledAppender
          ? (data, recordOffset, recordLength, sequence) =>
              plan.writeCompiledRow(compiledAppender, data, recordOffset, recordLength, sequence)
          : (data, recordOffset, recordLength, sequence) =>
              plan.writeRow(extractRowValues(data), recordOffset, recordLength, sequence);
        plans.set(tableName, plan);
      }

      if (!plan.droppedIndexes) {
        for (const column of plan.table.columns) {
          if (!column.isIndexed || column.name.startsWith('_')) {
            continue;
          }
          state.db.exec(`DROP INDEX IF EXISTS "${coveringIndexName(plan.table.name, column.name)}"`);
        }
        plan.droppedIndexes = true;
      }

      const recordOffset = options?.offsets?.[index] ?? currentOffset;
      plan.writeRecord(buffer, recordOffset, buffer.length, state.sequence);

      state.sequence += 1;
      currentOffset = recordOffset + buffer.length;
    }

    for (const plan of plans.values()) {
      plan.flushRows();
      for (const column of plan.table.columns) {
        if (!column.isIndexed || column.name.startsWith('_')) {
          continue;
        }

        state.db.exec(
          `CREATE INDEX "${coveringIndexName(plan.table.name, column.name)}"
           ON "${recordTableName(plan.table.name)}" ("${column.name}", sequence, data_offset, data_length)`
        );
      }
    }
  });

  return {
    recordCount: buffers.length,
    transportMode,
  };
}

function forEachSizePrefixedBuffer(stream, visitor) {
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  let offset = 0;
  let index = 0;

  while (offset < stream.byteLength) {
    if (offset + 4 > stream.byteLength) {
      throw new Error(`Invalid size-prefixed stream: truncated frame header at offset ${offset}`);
    }
    const size = view.getUint32(offset, true);
    offset += 4;
    if (offset + size > stream.byteLength) {
      throw new Error(`Invalid size-prefixed stream: truncated frame at index ${index}`);
    }
    visitor(stream.subarray(offset, offset + size), index);
    offset += size;
    index += 1;
  }

  return index;
}

function normalizeRows(rows, columns) {
  return rows.map((row) => columns.map((column) => row[column]));
}

const methods = {
  createBuilder,
  registerFileId({ builderId, fileId, tableName }) {
    getBuilder(builderId).fileIdToTable.set(encodeFileId(fileId), tableName);
    return { ok: true };
  },
  enableDemoExtractors({ builderId }) {
    const builder = getBuilder(builderId);
    for (const [tableName, extractor] of Object.entries(demoExtractors)) {
      builder.extractors.set(tableName, extractor);
    }
    return { ok: true };
  },
  ingestClone({ builderId, buffers, options }) {
    const builder = getBuilder(builderId);
    return ingestRecords(builder, buffers.map((buffer) => Uint8Array.from(buffer)), options, 'clone');
  },
  ingestShared({ builderId, sharedBuffer, byteLength, options }) {
    const builder = getBuilder(builderId);
    let currentOffset = options?.startOffset ?? 0;
    const plans = new Map();
    const stream = new Uint8Array(sharedBuffer, 0, byteLength);
    let recordCount = 0;

    builder.queryCache.clear();

    withTransaction(builder.db, builder.beginTransactionSql, () => {
      forEachSizePrefixedBuffer(stream, (buffer, index) => {
        const fileIdCode = readFileIdCode(buffer);
        const tableName = builder.fileIdToTable.get(fileIdCode);
        if (!tableName) {
          const fileId = readFileId(buffer);
          throw new Error(`No table registered for file identifier ${fileId}`);
        }

        let plan = plans.get(tableName);
        if (!plan) {
          const extractor = builder.extractors.get(tableName);
          if (!extractor) {
            throw new Error(`No field extractor registered for table ${tableName}`);
          }

          const table = builder.tableByName.get(tableName);
          if (!table) {
            throw new Error(`Table ${tableName} is not present in the parsed schema`);
          }

          plan = {
            table,
            fieldNames: table.columns
              .filter((column) => column.isIndexed && !column.name.startsWith('_'))
              .map((column) => column.name),
            writeRecord: null,
            ...createBatchedRowWriter(
              builder.db,
              table.name,
              table.columns
                .filter((column) => column.isIndexed && !column.name.startsWith('_'))
                .map((column) => column.name)
            ),
            droppedIndexes: false,
          };
          const compiledAppender = createFieldAppender(extractor, plan.fieldNames);
          const extractRowValues = compiledAppender ? null : createFieldValueReader(extractor, plan.fieldNames);
          plan.writeRecord = compiledAppender
            ? (data, recordOffset, recordLength, sequence) =>
                plan.writeCompiledRow(compiledAppender, data, recordOffset, recordLength, sequence)
            : (data, recordOffset, recordLength, sequence) =>
                plan.writeRow(extractRowValues(data), recordOffset, recordLength, sequence);
          plans.set(tableName, plan);
        }

        if (!plan.droppedIndexes) {
          for (const column of plan.table.columns) {
            if (!column.isIndexed || column.name.startsWith('_')) {
              continue;
            }
            builder.db.exec(`DROP INDEX IF EXISTS "${coveringIndexName(plan.table.name, column.name)}"`);
          }
          plan.droppedIndexes = true;
        }

        const recordOffset = options?.offsets?.[index] ?? currentOffset;
        plan.writeRecord(buffer, recordOffset, buffer.length, builder.sequence);

        builder.sequence += 1;
        currentOffset = recordOffset + buffer.length;
        recordCount = index + 1;
      });

      for (const plan of plans.values()) {
        plan.flushRows();
        for (const column of plan.table.columns) {
          if (!column.isIndexed || column.name.startsWith('_')) {
            continue;
          }

          builder.db.exec(
            `CREATE INDEX "${coveringIndexName(plan.table.name, column.name)}"
             ON "${recordTableName(plan.table.name)}" ("${column.name}", sequence, data_offset, data_length)`
          );
        }
      }
    });

    return {
      recordCount,
      transportMode: 'shared-array-buffer',
    };
  },
  query({ builderId, sql, params }) {
    const builder = getBuilder(builderId);
    return runQuery(builder, sql, params);
  },
  queryMany({ builderId, queries }) {
    const builder = getBuilder(builderId);
    return queries.map(({ sql, params }) => runQuery(builder, sql, params));
  },
  closeBuilder({ builderId }) {
    const builder = getBuilder(builderId);
    builder.queryCache.clear();
    builder.db.close();
    builders.delete(builderId);
    return { ok: true };
  },
};

parentPort.on('message', async (message) => {
  const { id, method, params } = message;
  try {
    const result = await methods[method](params);
    parentPort.postMessage({ id, success: true, result });
  } catch (error) {
    parentPort.postMessage({ id, success: false, error: error instanceof Error ? error.message : String(error) });
  }
});

parentPort.postMessage({ type: 'ready' });
