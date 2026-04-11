import { mkdtemp, rm } from 'node:fs/promises';
import { availableParallelism, tmpdir } from 'node:os';
import { join } from 'node:path';

import { parseSchema } from '../src/schema/index.js';
import {
  FlatSQLArtifactBuilder,
  FlatSQLArtifactWorkerClient,
} from '../src/artifacts/index.js';
import {
  decodeSizePrefixedStream,
  writeSizePrefixedStream,
} from '../src/artifacts/transport.js';

const schema = `
  table User {
    id: int (indexed);
    name: string;
    email: string (indexed);
    age: int;
  }
`;

const expectedFastThreads = Math.min(4, Math.max(2, availableParallelism()));

function writeU32(bytes: number[], value: number): void {
  bytes.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, (value >>> 24) & 0xff);
}

function writeI32(bytes: number[], value: number): void {
  writeU32(bytes, value >>> 0);
}

function pad4(bytes: number[]): void {
  while (bytes.length % 4 !== 0) {
    bytes.push(0);
  }
}

function createUserFlatBuffer(id: number, name: string, email: string, age: number): Uint8Array {
  const fb: number[] = [0, 0, 0, 0];
  fb.push('U'.charCodeAt(0), 'S'.charCodeAt(0), 'E'.charCodeAt(0), 'R'.charCodeAt(0));
  pad4(fb);

  const vtableStart = fb.length;
  writeU32(fb, 0);
  fb.splice(vtableStart, 4, 12, 0, 20, 0);
  fb.push(4, 0, 8, 0, 12, 0, 16, 0);
  pad4(fb);

  const tableStart = fb.length;
  writeI32(fb, tableStart - vtableStart);
  writeI32(fb, id);
  writeU32(fb, 0);
  writeU32(fb, 0);
  writeI32(fb, age);

  const nameFieldPos = tableStart + 8;
  const emailFieldPos = tableStart + 12;

  const namePos = fb.length;
  writeU32(fb, name.length);
  for (const ch of name) fb.push(ch.charCodeAt(0));
  fb.push(0);
  pad4(fb);

  const emailPos = fb.length;
  writeU32(fb, email.length);
  for (const ch of email) fb.push(ch.charCodeAt(0));
  fb.push(0);

  const nameRelOffset = namePos - nameFieldPos;
  fb.splice(nameFieldPos, 4, nameRelOffset & 0xff, (nameRelOffset >>> 8) & 0xff, (nameRelOffset >>> 16) & 0xff, (nameRelOffset >>> 24) & 0xff);

  const emailRelOffset = emailPos - emailFieldPos;
  fb.splice(emailFieldPos, 4, emailRelOffset & 0xff, (emailRelOffset >>> 8) & 0xff, (emailRelOffset >>> 16) & 0xff, (emailRelOffset >>> 24) & 0xff);

  const rootOffset = tableStart;
  fb.splice(0, 4, rootOffset & 0xff, (rootOffset >>> 8) & 0xff, (rootOffset >>> 16) & 0xff, (rootOffset >>> 24) & 0xff);

  return Uint8Array.from(fb);
}

function withFileId(buffer: Uint8Array, fileId: string): Uint8Array {
  if (fileId.length !== 4) {
    throw new Error('FlatBuffer file identifiers must be four characters');
  }

  const copy = Uint8Array.from(buffer);
  for (let index = 0; index < 4; index++) {
    copy[4 + index] = fileId.charCodeAt(index);
  }
  return copy;
}

describe('remote artifact builder', () => {
  let tempDir: string;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'flatsql-artifact-'));
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  test('createArtifactBuilder persists index tables to sqlitePath', async () => {
    const sqlitePath = join(tempDir, 'users.sqlite');
    const parsedSchema = parseSchema(schema, 'artifact');
    expect(parsedSchema.tables[0].indexes).toEqual(['id', 'email']);

    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });
    const initialMaster = builder.query(
      "SELECT type, name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY type, name"
    );

    builder.registerFileId('USER', 'User');
    builder.enableDemoExtractors();
    builder.ingestBuffers(
      [
        createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
        createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25),
      ],
      { sourceName: 'users.bin', startOffset: 4096 }
    );
    expect(initialMaster.rows).toContainEqual(['view', '_idx_User_email']);
    builder.close();

    const reopened = FlatSQLArtifactBuilder.fromSchema(schema, { sqlitePath });
    const master = reopened.query(
      "SELECT type, name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY type, name"
    );
    expect(master.rows).toContainEqual(['view', '_idx_User_email']);
    const result = reopened.query('SELECT key, data_offset, data_length, sequence FROM "_idx_User_email" ORDER BY key');

    expect(result.rows).toEqual([
      ['alice@example.com', 4096, expect.any(Number), 1],
      ['bob@example.com', expect.any(Number), expect.any(Number), 2],
    ]);

    reopened.close();
  });

  test('artifact builder defaults to fast sqlite pragmas', async () => {
    const sqlitePath = join(tempDir, 'users-fast-pragmas.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });

    expect(builder.query('PRAGMA journal_mode').rows).toEqual([['off']]);
    expect(builder.query('PRAGMA synchronous').rows).toEqual([[0]]);
    expect(builder.query('PRAGMA page_size').rows).toEqual([[32768]]);
    expect(builder.query('PRAGMA threads').rows).toEqual([[expectedFastThreads]]);
    expect(builder.query('PRAGMA cache_size').rows).toEqual([[-131072]]);
    expect(builder.query('PRAGMA mmap_size').rows).toEqual([[268435456]]);

    builder.close();
  });

  test('artifact builder can opt into safe sqlite pragmas', async () => {
    const sqlitePath = join(tempDir, 'users-safe-pragmas.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
      performanceProfile: 'safe',
    });

    expect(builder.query('PRAGMA journal_mode').rows).toEqual([['delete']]);
    expect(builder.query('PRAGMA synchronous').rows).toEqual([[2]]);
    expect(builder.query('PRAGMA page_size').rows).toEqual([[32768]]);

    builder.close();
  });

  test('artifact builder uses batch extractor once per record when available', async () => {
    const sqlitePath = join(tempDir, 'users-batch-extractor.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });
    const calls = {
      field: 0,
      fields: 0,
    };

    builder.registerFileId('USER', 'User');
    builder.setFieldExtractor('User', {
      getField(_data: Uint8Array, fieldName: string) {
        calls.field += 1;
        return fieldName === 'id' ? 1 : 'shared@example.com';
      },
      getFields(_data: Uint8Array, fieldNames: string[]) {
        calls.fields += 1;
        return Object.fromEntries(
          fieldNames.map((fieldName) => [fieldName, fieldName === 'id' ? 1 : 'shared@example.com'])
        );
      },
    });

    builder.ingestBuffers(
      [
        createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
        createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25),
      ],
      { sourceName: 'users.bin', startOffset: 0 }
    );

    expect(calls.fields).toBe(2);
    expect(calls.field).toBe(0);

    builder.close();
  });

  test('artifact builder uses ordered field values when extractor provides them', async () => {
    const sqlitePath = join(tempDir, 'users-ordered-values.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });
    const calls = {
      values: 0,
      fields: 0,
    };

    builder.registerFileId('USER', 'User');
    builder.setFieldExtractor('User', {
      getField(_data: Uint8Array, fieldName: string) {
        return fieldName === 'id' ? 1 : 'shared@example.com';
      },
      getFieldValues(_data: Uint8Array, fieldNames: string[]) {
        calls.values += 1;
        return fieldNames.map((fieldName) => (fieldName === 'id' ? 1 : 'shared@example.com'));
      },
      getFields(_data: Uint8Array, fieldNames: string[]) {
        calls.fields += 1;
        return Object.fromEntries(
          fieldNames.map((fieldName) => [fieldName, fieldName === 'id' ? 1 : 'shared@example.com'])
        );
      },
    });

    builder.ingestBuffers(
      [createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30)],
      { sourceName: 'users.bin', startOffset: 0 }
    );

    expect(calls.values).toBe(1);
    expect(calls.fields).toBe(0);

    builder.close();
  });

  test('artifact builder compiles field value extraction once per table when available', async () => {
    const sqlitePath = join(tempDir, 'users-compiled-values.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });
    const calls = {
      compile: 0,
      values: 0,
      fields: 0,
    };

    builder.registerFileId('USER', 'User');
    builder.setFieldExtractor('User', {
      getField(_data: Uint8Array, fieldName: string) {
        return fieldName === 'id' ? 1 : 'shared@example.com';
      },
      compileFieldValues(fieldNames: string[]) {
        calls.compile += 1;
        return (_data: Uint8Array) =>
          fieldNames.map((fieldName) => (fieldName === 'id' ? 1 : 'shared@example.com'));
      },
      getFieldValues(_data: Uint8Array, fieldNames: string[]) {
        calls.values += 1;
        return fieldNames.map((fieldName) => (fieldName === 'id' ? 1 : 'shared@example.com'));
      },
      getFields(_data: Uint8Array, fieldNames: string[]) {
        calls.fields += 1;
        return Object.fromEntries(
          fieldNames.map((fieldName) => [fieldName, fieldName === 'id' ? 1 : 'shared@example.com'])
        );
      },
    });

    builder.ingestBuffers(
      [
        createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
        createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25),
      ],
      { sourceName: 'users.bin', startOffset: 0 }
    );

    expect(calls.compile).toBe(1);
    expect(calls.values).toBe(0);
    expect(calls.fields).toBe(0);

    builder.close();
  });

  test('artifact builder prefers compiled field appenders when available', async () => {
    const sqlitePath = join(tempDir, 'users-compiled-appender.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });
    const calls = {
      compileAppender: 0,
      compileValues: 0,
      values: 0,
    };

    builder.registerFileId('USER', 'User');
    builder.setFieldExtractor('User', {
      getField(_data: Uint8Array, fieldName: string) {
        return fieldName === 'id' ? 1 : 'shared@example.com';
      },
      compileFieldAppender(fieldNames: string[]) {
        calls.compileAppender += 1;
        return (pendingArgs: unknown[]) => {
          for (const fieldName of fieldNames) {
            pendingArgs.push(fieldName === 'id' ? 1 : 'shared@example.com');
          }
        };
      },
      compileFieldValues(fieldNames: string[]) {
        calls.compileValues += 1;
        return (_data: Uint8Array) =>
          fieldNames.map((fieldName) => (fieldName === 'id' ? 1 : 'shared@example.com'));
      },
      getFieldValues(_data: Uint8Array, fieldNames: string[]) {
        calls.values += 1;
        return fieldNames.map((fieldName) => (fieldName === 'id' ? 1 : 'shared@example.com'));
      },
    });

    builder.ingestBuffers(
      [
        createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
        createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25),
      ],
      { sourceName: 'users.bin', startOffset: 0 }
    );

    expect(calls.compileAppender).toBe(1);
    expect(calls.compileValues).toBe(0);
    expect(calls.values).toBe(0);

    builder.close();
  });

  test('artifact builder preserves all rows across internal write batches', async () => {
    const sqlitePath = join(tempDir, 'users-batched-ingest.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });

    builder.registerFileId('USER', 'User');
    builder.enableDemoExtractors();

    const buffers = Array.from({ length: 70 }, (_, index) =>
      createUserFlatBuffer(index + 1, `User ${index + 1}`, `user${index + 1}@example.com`, 20 + (index % 10))
    );

    builder.ingestBuffers(buffers, { sourceName: 'users.bin', startOffset: 512 });

    const result = builder.query(
      'SELECT COUNT(*), MIN(sequence), MAX(sequence) FROM "_idx_User_email"'
    );
    expect(result.rows).toEqual([[70, 1, 70]]);

    builder.close();
  });

  test('artifact builder reuses prepared select statements for repeated identical queries', async () => {
    const sqlitePath = join(tempDir, 'users-query-cache.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });

    builder.registerFileId('USER', 'User');
    builder.enableDemoExtractors();
    builder.ingestBuffers(
      [
        createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
        createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25),
      ],
      { sourceName: 'users.bin', startOffset: 0 }
    );

    const db = (builder as any).db;
    const originalPrepare = db.prepare.bind(db);
    let prepareCalls = 0;
    db.prepare = ((sql: string) => {
      prepareCalls += 1;
      return originalPrepare(sql);
    }) as typeof db.prepare;

    const sql = 'SELECT key FROM "_idx_User_email" WHERE key = \'alice@example.com\'';
    expect(builder.query(sql).rows).toEqual([['alice@example.com']]);
    expect(builder.query(sql).rows).toEqual([['alice@example.com']]);
    expect(prepareCalls).toBe(1);

    builder.close();
  });

  test('artifact worker builds sqlite artifact via worker thread', async () => {
    const sqlitePath = join(tempDir, 'users-worker.sqlite');
    const client = new FlatSQLArtifactWorkerClient();
    await client.init();

    const builder = await client.createBuilder(schema, {
      sqlitePath,
      preferSharedArrayBuffer: false,
    });

    await builder.registerFileId('USER', 'User');
    await builder.enableDemoExtractors();
    const result = await builder.ingestBuffers(
      [
        createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
        createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25),
      ],
      { sourceName: 'users.bin', startOffset: 2048 }
    );

    expect(result.transportMode).toBe('clone');
    const workerQuery = await builder.query(
      "SELECT type, name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY type, name"
    );
    expect(workerQuery.rows).toContainEqual(['view', '_idx_User_email']);

    await builder.close();
    await client.close();

    const reopened = FlatSQLArtifactBuilder.fromSchema(schema, { sqlitePath });
    const master = reopened.query(
      "SELECT type, name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY type, name"
    );
    expect(master.rows).toContainEqual(['view', '_idx_User_email']);
    const query = reopened.query('SELECT key FROM "_idx_User_email" ORDER BY key');
    expect(query.rows).toEqual([['alice@example.com'], ['bob@example.com']]);
    reopened.close();
  });

  test('artifact worker defaults to fast sqlite pragmas', async () => {
    const sqlitePath = join(tempDir, 'users-worker-fast-pragmas.sqlite');
    const client = new FlatSQLArtifactWorkerClient();
    await client.init();

    const builder = await client.createBuilder(schema, {
      sqlitePath,
      preferSharedArrayBuffer: false,
    });

    await expect(builder.query('PRAGMA journal_mode')).resolves.toMatchObject({
      rows: [['off']],
    });
    await expect(builder.query('PRAGMA synchronous')).resolves.toMatchObject({
      rows: [[0]],
    });
    await expect(builder.query('PRAGMA page_size')).resolves.toMatchObject({
      rows: [[32768]],
    });
    await expect(builder.query('PRAGMA threads')).resolves.toMatchObject({
      rows: [[expectedFastThreads]],
    });
    await expect(builder.query('PRAGMA cache_size')).resolves.toMatchObject({
      rows: [[-131072]],
    });
    await expect(builder.query('PRAGMA mmap_size')).resolves.toMatchObject({
      rows: [[268435456]],
    });

    await builder.close();
    await client.close();
  });

  test('artifact worker uses shared-array-buffer transport when supported', async () => {
    const sqlitePath = join(tempDir, 'users-worker-sab.sqlite');
    const client = new FlatSQLArtifactWorkerClient();
    await client.init();

    const builder = await client.createBuilder(schema, {
      sqlitePath,
      preferSharedArrayBuffer: true,
    });

    await builder.registerFileId('USER', 'User');
    await builder.enableDemoExtractors();
    const result = await builder.ingestBuffers(
      [createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30)],
      { sourceName: 'users.bin', startOffset: 1024 }
    );

    expect(result.transportMode).toBe('shared-array-buffer');

    await builder.close();
    await client.close();
  });

  test('size-prefixed decoding returns shared views instead of copied buffers', async () => {
    const stream = new Uint8Array(new SharedArrayBuffer(15));
    writeSizePrefixedStream(stream, [
      Uint8Array.from([1, 2, 3]),
      Uint8Array.from([4, 5, 6, 7]),
    ]);

    const decoded = decodeSizePrefixedStream(stream);
    expect(decoded).toHaveLength(2);
    expect(Array.from(decoded[0])).toEqual([1, 2, 3]);
    expect(Array.from(decoded[1])).toEqual([4, 5, 6, 7]);

    stream[4] = 99;
    expect(decoded[0][0]).toBe(99);
  });

  test('artifact builder rolls back the whole ingest batch on error', async () => {
    const sqlitePath = join(tempDir, 'users-rollback.sqlite');
    const builder = FlatSQLArtifactBuilder.fromSchema(schema, {
      sqlitePath,
    });

    builder.registerFileId('USER', 'User');
    builder.enableDemoExtractors();

    expect(() =>
      builder.ingestBuffers(
        [
          createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
          withFileId(createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25), 'FAIL'),
        ],
        { sourceName: 'users.bin', startOffset: 0 }
      )
    ).toThrow('No table registered for file identifier FAIL');

    const result = builder.query('SELECT key FROM "_idx_User_email" ORDER BY key');
    expect(result.rows).toEqual([]);

    builder.close();
  });

  test('artifact worker rolls back the whole ingest batch on error', async () => {
    const sqlitePath = join(tempDir, 'users-worker-rollback.sqlite');
    const client = new FlatSQLArtifactWorkerClient();
    await client.init();

    const builder = await client.createBuilder(schema, {
      sqlitePath,
      preferSharedArrayBuffer: false,
    });

    await builder.registerFileId('USER', 'User');
    await builder.enableDemoExtractors();

    await expect(
      builder.ingestBuffers(
        [
          createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
          withFileId(createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25), 'FAIL'),
        ],
        { sourceName: 'users.bin', startOffset: 0 }
      )
    ).rejects.toThrow('No table registered for file identifier FAIL');

    const result = await builder.query('SELECT key FROM "_idx_User_email" ORDER BY key');
    expect(result.rows).toEqual([]);

    await builder.close();
    await client.close();
  });
});
