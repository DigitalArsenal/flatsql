import {
  createStandaloneArtifactBuilder,
} from '../src/artifacts/index.js';
import { decodeSizePrefixedStream } from '../src/artifacts/transport.js';
import { loadFlatSQLStandalone } from '../wasm/standalone.js';

const USER_SCHEMA = `
table User {
  id: int (id);
  name: string;
  email: string (key);
  age: int;
}
`;

describe('standalone artifact builder', () => {
  test('uses the standalone C++ runtime cache as the artifact query source of truth', async () => {
    const flatsql = await loadFlatSQLStandalone();
    const builder = await createStandaloneArtifactBuilder(USER_SCHEMA, {
      dbName: 'standalone-artifact-builder-test',
      runtime: 'browser',
    });

    try {
      builder.registerFileId('USER', 'User');
      builder.enableDemoExtractors();
      const recordCount = builder.ingestBuffers([
        flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
        flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
      ]);
      expect(recordCount).toBe(2);
      builder.configureQueryCache({ maxEntries: 16, maxRows: 8 });

      builder.registerQueryTemplate('userByEmail', 'SELECT id FROM User WHERE email = ?', true);
      expect(builder.queryTemplate('userByEmail', ['alice@example.com'])).toEqual({
        columns: ['id'],
        rows: [[1]],
        rowCount: 1,
      });
      expect(builder.getQueryCacheStats()).toMatchObject({
        hits: 0,
        misses: 1,
        size: 1,
        maxEntries: 16,
        maxRows: 8,
      });

      expect(builder.queryTemplate('userByEmail', ['alice@example.com'])).toEqual({
        columns: ['id'],
        rows: [[1]],
        rowCount: 1,
      });
      expect(builder.getQueryCacheStats()).toMatchObject({ hits: 1, misses: 1, size: 1 });
      expect(builder.getFlatBufferByIndex('User', 'email', ['alice@example.com'])).toBeInstanceOf(Uint8Array);
      expect(decodeSizePrefixedStream(await builder.queryRawFlatBufferStream(
        'SELECT _data FROM User WHERE email = ?',
        ['bob@example.com']
      ))).toEqual([
        flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
      ]);
      expect(
        await builder.buildResponseArtifactCacheKey('PNM', '2', ' SELECT   *   FROM PNM WHERE FILE_ID = ? ', {
          format: 'raw',
          publishEventKey: 'PNM-event-1',
          projection: ['FILE_ID'],
          params: ['PNM|42'],
        })
      ).toBe(
        'flatsql:response:v1|s=504e4d|v=32|f=726177|e=504e4d2d6576656e742d31|q=53454c454354202a2046524f4d20504e4d2057484552452046494c455f4944203d203f|c=1:46494c455f4944|p=1:s=504e4d7c3432'
      );
    } finally {
      builder.close();
    }
  });
});
