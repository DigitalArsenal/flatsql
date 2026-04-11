import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { parseSchema } from '../src/schema/index.js';
import {
  FlatSQLArtifactBuilder,
  FlatSQLArtifactWorkerClient,
} from '../src/artifacts/index.js';

const schema = `
  table User {
    id: int (indexed);
    name: string;
    email: string (indexed);
    age: int;
  }
`;

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
    const initialMaster = builder.query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name");

    builder.registerFileId('USER', 'User');
    builder.enableDemoExtractors();
    builder.ingestBuffers(
      [
        createUserFlatBuffer(1, 'Alice', 'alice@example.com', 30),
        createUserFlatBuffer(2, 'Bob', 'bob@example.com', 25),
      ],
      { sourceName: 'users.bin', startOffset: 4096 }
    );
    expect(initialMaster.rows).toContainEqual(['_idx_User_email']);
    builder.close();

    const reopened = FlatSQLArtifactBuilder.fromSchema(schema, { sqlitePath });
    const master = reopened.query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name");
    expect(master.rows).toContainEqual(['_idx_User_email']);
    const result = reopened.query('SELECT key, data_offset, data_length, sequence FROM "_idx_User_email" ORDER BY key');

    expect(result.rows).toEqual([
      ['alice@example.com', 4096, expect.any(Number), 1],
      ['bob@example.com', expect.any(Number), expect.any(Number), 2],
    ]);

    reopened.close();
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
    const workerQuery = await builder.query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name");
    expect(workerQuery.rows).toContainEqual(['_idx_User_email']);

    await builder.close();
    await client.close();

    const reopened = FlatSQLArtifactBuilder.fromSchema(schema, { sqlitePath });
    const master = reopened.query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name");
    expect(master.rows).toContainEqual(['_idx_User_email']);
    const query = reopened.query('SELECT key FROM "_idx_User_email" ORDER BY key');
    expect(query.rows).toEqual([['alice@example.com'], ['bob@example.com']]);
    reopened.close();
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
