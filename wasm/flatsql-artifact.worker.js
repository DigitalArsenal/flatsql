import { parentPort } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';

const decoder = new TextDecoder();
const builders = new Map();

function sqliteType(column) {
  return column.sqlType ?? 'BLOB';
}

function createCursor(data) {
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

function getFieldOffset(cursor, fieldIndex) {
  const entryOffset = cursor.vtable + 4 + fieldIndex * 2;
  if (entryOffset + 2 > cursor.vtable + cursor.vtableSize) {
    return 0;
  }
  return cursor.view.getUint16(entryOffset, true);
}

function readInt32Field(cursor, fieldIndex) {
  const fieldOffset = getFieldOffset(cursor, fieldIndex);
  return fieldOffset === 0 ? 0 : cursor.view.getInt32(cursor.root + fieldOffset, true);
}

function readStringField(cursor, fieldIndex) {
  const fieldOffset = getFieldOffset(cursor, fieldIndex);
  if (fieldOffset === 0) {
    return '';
  }
  const relative = cursor.view.getUint32(cursor.root + fieldOffset, true);
  const stringStart = cursor.root + fieldOffset + relative;
  const stringLength = cursor.view.getUint32(stringStart, true);
  return decoder.decode(cursor.data.subarray(stringStart + 4, stringStart + 4 + stringLength));
}

function createMappedExtractor(fieldReaders) {
  return {
    getField(data, fieldName) {
      const reader = fieldReaders[fieldName];
      if (!reader) {
        return null;
      }
      return reader(createCursor(data));
    },
    getFields(data, fieldNames) {
      const cursor = createCursor(data);
      return Object.fromEntries(
        fieldNames.map((fieldName) => {
          const reader = fieldReaders[fieldName];
          return [fieldName, reader ? reader(cursor) : null];
        })
      );
    },
    getFieldValues(data, fieldNames) {
      const cursor = createCursor(data);
      return fieldNames.map((fieldName) => {
        const reader = fieldReaders[fieldName];
        return reader ? reader(cursor) : null;
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
  if (extractor.getFieldValues) {
    return extractor.getFieldValues(data, fieldNames);
  }

  return fieldNames.map((fieldName) => extractor.getField(data, fieldName));
}

const demoExtractors = {
  User: createMappedExtractor({
    id: (cursor) => readInt32Field(cursor, 0),
    name: (cursor) => readStringField(cursor, 1),
    email: (cursor) => readStringField(cursor, 2),
    age: (cursor) => readInt32Field(cursor, 3),
  }),
  Post: createMappedExtractor({
    id: (cursor) => readInt32Field(cursor, 0),
    user_id: (cursor) => readInt32Field(cursor, 1),
    title: (cursor) => readStringField(cursor, 2),
  }),
};

function readFileId(data) {
  if (data.length < 8) {
    throw new Error('FlatBuffer payload is too short to contain a file identifier');
  }
  return decoder.decode(data.subarray(4, 8));
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

function createRowEmitter(statement, fieldCount) {
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

function createBuilder({ builderId, schema, performanceProfile = 'fast', sqlitePath }) {
  const db = new DatabaseSync(sqlitePath);
  const state = {
    schema,
    db,
    beginTransactionSql: applyPerformanceProfile(db, performanceProfile),
    tableByName: new Map(schema.tables.map((table) => [table.name, table])),
    fileIdToTable: new Map(),
    extractors: new Map(),
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

  withTransaction(state.db, state.beginTransactionSql, () => {
    for (let index = 0; index < buffers.length; index++) {
      const buffer = buffers[index];
      const fileId = readFileId(buffer);
      const tableName = state.fileIdToTable.get(fileId);
      if (!tableName) {
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
          extractor,
          fieldNames: table.columns
            .filter((column) => column.isIndexed && !column.name.startsWith('_'))
            .map((column) => column.name),
          emitRow: createRowEmitter(
            state.db.prepare(
            `INSERT INTO "${recordTableName(table.name)}" (${table.columns
              .filter((column) => column.isIndexed && !column.name.startsWith('_'))
              .map((column) => `"${column.name}"`)
              .join(', ')}, data_offset, data_length, sequence)
             VALUES (${table.columns
               .filter((column) => column.isIndexed && !column.name.startsWith('_'))
               .map(() => '?')
               .join(', ')}, ?, ?, ?)`
            ),
            table.columns.filter((column) => column.isIndexed && !column.name.startsWith('_')).length
          ),
          droppedIndexes: false,
        };
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
      const rowValues = extractFieldValues(plan.extractor, buffer, plan.fieldNames);
      plan.emitRow(rowValues, recordOffset, buffer.length, state.sequence);

      state.sequence += 1;
      currentOffset = recordOffset + buffer.length;
    }

    for (const plan of plans.values()) {
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

function decodeSizePrefixedStream(sharedBuffer, byteLength) {
  const stream = new Uint8Array(sharedBuffer, 0, byteLength);
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  const buffers = [];
  let offset = 0;

  while (offset < stream.byteLength) {
    const size = view.getUint32(offset, true);
    offset += 4;
    buffers.push(stream.slice(offset, offset + size));
    offset += size;
  }

  return buffers;
}

function normalizeRows(rows, columns) {
  return rows.map((row) => columns.map((column) => row[column]));
}

const methods = {
  createBuilder,
  registerFileId({ builderId, fileId, tableName }) {
    getBuilder(builderId).fileIdToTable.set(fileId, tableName);
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
    return ingestRecords(builder, decodeSizePrefixedStream(sharedBuffer, byteLength), options, 'shared-array-buffer');
  },
  query({ builderId, sql }) {
    const builder = getBuilder(builderId);
    const statement = builder.db.prepare(sql);
    const columns = statement.columns().map((column) => column.name);
    const rows = normalizeRows(statement.all(), columns);
    return { columns, rows, rowCount: rows.length };
  },
  closeBuilder({ builderId }) {
    const builder = getBuilder(builderId);
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
