import { loadFlatSQLStandalone } from '../wasm/standalone.js';

const USER_SCHEMA = `
table User {
  id: int (id);
  name: string;
  email: string (key);
  age: int;
}
`;

describe('standalone FlatSQL shim', () => {
  test('instantiates the standalone WASI artifact and uses the C++ query cache', async () => {
    const flatsql = await loadFlatSQLStandalone();
    const db = flatsql.createDatabase(USER_SCHEMA, 'standalone-cache-test');

    try {
      db.registerFileId('USER', 'User');
      db.enableDemoExtractors();
      db.ingestBuffers([
        flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
        flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
      ]);
      db.registerQueryTemplate('userById', 'SELECT email FROM User WHERE id = ?', true);

      expect(db.queryTemplate('userById', [2])).toEqual({
        columns: ['email'],
        rows: [['bob@example.com']],
      });
      expect(db.getQueryCacheStats()).toMatchObject({
        hits: 0,
        misses: 1,
        size: 1,
      });

      expect(db.queryTemplate('userById', [2])).toEqual({
        columns: ['email'],
        rows: [['bob@example.com']],
      });
      expect(db.getQueryCacheStats()).toMatchObject({
        hits: 1,
        misses: 1,
        size: 1,
      });
    } finally {
      db.destroy();
    }
  });
});
