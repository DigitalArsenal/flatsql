import { existsSync } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createStandaloneArtifactBuilder } from '../src/artifacts/index.js';
import { decodeSizePrefixedStream } from '../src/artifacts/transport.js';
import {
  buildFlatSQLWasmEdgeRunner,
  hasWasmEdgeBuildInputs,
} from '../src/standalone/index.js';
import { loadFlatSQLStandalone } from '../wasm/standalone.js';

const USER_SCHEMA = `
table User {
  id: int (id);
  name: string;
  email: string (key);
  age: int;
}
`;

describe('WasmEdge process runner', () => {
  const maybeTest = hasWasmEdgeBuildInputs() ? test : test.skip;

  maybeTest('builds a persistent runner and uses the C++ query cache through the artifact builder', async () => {
    const buildDir = await mkdtemp(join(tmpdir(), 'flatsql-wasmedge-runner-'));
    const runnerPath = join(buildDir, process.platform === 'win32' ? 'flatsql-wasmedge-runner.exe' : 'flatsql-wasmedge-runner');
    const artifact = await buildFlatSQLWasmEdgeRunner({ outputPath: runnerPath });
    expect(existsSync(artifact.outputPath)).toBe(true);

    const flatsql = await loadFlatSQLStandalone();
    const builder = await createStandaloneArtifactBuilder(USER_SCHEMA, {
      dbName: 'wasmedge-runner-cache-test',
      runtime: 'wasmedge',
      wasmEdgeRunnerBinary: artifact.outputPath,
    });

    try {
      await builder.registerFileId('USER', 'User');
      await builder.enableDemoExtractors();
      await builder.ingestBuffers([
        flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
        flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
      ]);
      await builder.configureQueryCache({ maxEntries: 16, maxRows: 8 });
      await builder.registerQueryTemplate('userByEmail', 'SELECT id FROM User WHERE email = ?', true);

      await expect(builder.queryTemplate('userByEmail', ['alice@example.com'])).resolves.toEqual({
        columns: ['id'],
        rows: [[1]],
        rowCount: 1,
      });
      await expect(builder.getQueryCacheStats()).resolves.toMatchObject({
        hits: 0,
        misses: 1,
        size: 1,
        maxEntries: 16,
        maxRows: 8,
      });

      await expect(builder.queryTemplate('userByEmail', ['alice@example.com'])).resolves.toEqual({
        columns: ['id'],
        rows: [[1]],
        rowCount: 1,
      });
      await expect(builder.getQueryCacheStats()).resolves.toMatchObject({ hits: 1, misses: 1, size: 1 });
      await expect(builder.getFlatBufferByIndex('User', 'email', ['alice@example.com'])).resolves.toBeInstanceOf(Uint8Array);
      const rawStream = await builder.queryRawFlatBufferStream(
        'SELECT _data FROM User WHERE email = ?',
        ['bob@example.com']
      );
      expect(decodeSizePrefixedStream(rawStream)).toEqual([
        flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
      ]);
      await expect(
        builder.buildResponseArtifactCacheKey('PNM', '2', ' SELECT   *   FROM PNM WHERE FILE_ID = ? ', {
          format: 'raw',
          publishEventKey: 'PNM-event-1',
          projection: ['FILE_ID'],
          params: ['PNM|42'],
        })
      ).resolves.toBe(
        'flatsql:response:v1|s=504e4d|v=32|f=726177|e=504e4d2d6576656e742d31|q=53454c454354202a2046524f4d20504e4d2057484552452046494c455f4944203d203f|c=1:46494c455f4944|p=1:s=504e4d7c3432'
      );
    } finally {
      await builder.close();
    }
  }, 30000);
});
