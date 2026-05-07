import {
  createStandaloneArtifactBuilder,
} from '../src/artifacts/index.js';
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

      builder.registerQueryTemplate('userByEmail', 'SELECT id FROM User WHERE email = ?', true);
      expect(builder.queryTemplate('userByEmail', ['alice@example.com'])).toEqual({
        columns: ['id'],
        rows: [[1]],
        rowCount: 1,
      });
      expect(builder.getQueryCacheStats()).toMatchObject({ hits: 0, misses: 1, size: 1 });

      expect(builder.queryTemplate('userByEmail', ['alice@example.com'])).toEqual({
        columns: ['id'],
        rows: [[1]],
        rowCount: 1,
      });
      expect(builder.getQueryCacheStats()).toMatchObject({ hits: 1, misses: 1, size: 1 });
      expect(builder.getFlatBufferByIndex('User', 'email', ['alice@example.com'])).toBeInstanceOf(Uint8Array);
    } finally {
      builder.close();
    }
  });
});
