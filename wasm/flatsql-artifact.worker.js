import { parentPort } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';

const decoder = new TextDecoder();
const builders = new Map();

function sqliteType(column) {
  return column.sqlType ?? 'BLOB';
}

function getFieldOffset(data, fieldIndex) {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const root = view.getUint32(0, true);
  const vtableOffset = view.getInt32(root, true);
  const vtable = root - vtableOffset;
  const vtableSize = view.getUint16(vtable, true);
  const entryOffset = vtable + 4 + fieldIndex * 2;
  if (entryOffset + 2 > vtable + vtableSize) {
    return 0;
  }
  return view.getUint16(entryOffset, true);
}

function readStringField(data, fieldIndex) {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const root = view.getUint32(0, true);
  const fieldOffset = getFieldOffset(data, fieldIndex);
  if (fieldOffset === 0) {
    return '';
  }
  const relative = view.getUint32(root + fieldOffset, true);
  const stringStart = root + fieldOffset + relative;
  const stringLength = view.getUint32(stringStart, true);
  return decoder.decode(data.subarray(stringStart + 4, stringStart + 4 + stringLength));
}

const demoExtractors = {
  User(data, fieldName) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const root = view.getUint32(0, true);

    switch (fieldName) {
      case 'id': {
        const fieldOffset = getFieldOffset(data, 0);
        return fieldOffset === 0 ? 0 : view.getInt32(root + fieldOffset, true);
      }
      case 'name':
        return readStringField(data, 1);
      case 'email':
        return readStringField(data, 2);
      case 'age': {
        const fieldOffset = getFieldOffset(data, 3);
        return fieldOffset === 0 ? 0 : view.getInt32(root + fieldOffset, true);
      }
      default:
        return null;
    }
  },
  Post(data, fieldName) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const root = view.getUint32(0, true);

    switch (fieldName) {
      case 'id': {
        const fieldOffset = getFieldOffset(data, 0);
        return fieldOffset === 0 ? 0 : view.getInt32(root + fieldOffset, true);
      }
      case 'user_id': {
        const fieldOffset = getFieldOffset(data, 1);
        return fieldOffset === 0 ? 0 : view.getInt32(root + fieldOffset, true);
      }
      case 'title':
        return readStringField(data, 2);
      default:
        return null;
    }
  },
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

function createBuilder({ builderId, schema, sqlitePath }) {
  const db = new DatabaseSync(sqlitePath);
  const state = {
    schema,
    db,
    fileIdToTable: new Map(),
    extractors: new Map(),
    sequence: 1,
  };

  for (const table of schema.tables) {
    for (const column of table.columns) {
      if (!column.isIndexed || column.name.startsWith('_')) {
        continue;
      }

      db.exec(
        `CREATE TABLE IF NOT EXISTS "${indexTableName(table.name, column.name)}" (
          key ${sqliteType(column)} NOT NULL,
          data_offset INTEGER NOT NULL,
          data_length INTEGER NOT NULL,
          sequence INTEGER NOT NULL,
          PRIMARY KEY (key, sequence)
        ) WITHOUT ROWID`
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

  for (let index = 0; index < buffers.length; index++) {
    const buffer = buffers[index];
    const fileId = readFileId(buffer);
    const tableName = state.fileIdToTable.get(fileId);
    if (!tableName) {
      throw new Error(`No table registered for file identifier ${fileId}`);
    }
    const extractor = state.extractors.get(tableName);
    if (!extractor) {
      throw new Error(`No field extractor registered for table ${tableName}`);
    }
    const table = state.schema.tables.find((candidate) => candidate.name === tableName);
    if (!table) {
      throw new Error(`Table ${tableName} is not present in the parsed schema`);
    }

    const recordOffset = options?.offsets?.[index] ?? currentOffset;
    for (const column of table.columns) {
      if (!column.isIndexed || column.name.startsWith('_')) {
        continue;
      }

      state.db
        .prepare(`INSERT INTO "${indexTableName(table.name, column.name)}" (key, data_offset, data_length, sequence) VALUES (?, ?, ?, ?)`)
        .run(extractor(buffer, column.name), recordOffset, buffer.length, state.sequence);
    }

    state.sequence += 1;
    currentOffset = recordOffset + buffer.length;
  }

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
