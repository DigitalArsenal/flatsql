import { loadFlatSQLStandalone } from '../wasm/standalone.js';

const USER_SCHEMA = `
table User {
  id: int (id);
  name: string;
  email: string (key);
  age: int;
}
root_type User;
`;

// Regression: an unbound `?` (or an explicitly bound NULL) used to reach the
// b-tree index search as std::monostate, which cannot be ordered and looped
// forever — hanging the engine on every build/host. NULL constraints must
// match nothing (SQL semantics) and leave the instance healthy, and the
// no-param query entry point must reject SQL with placeholders outright.
describe('NULL index constraints', () => {
  async function makeDb() {
    const flatsql = await loadFlatSQLStandalone();
    const db = flatsql.createDatabase(USER_SCHEMA, 'null-constraint-test');
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
      flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
    ]);
    return db;
  }

  test('unbound placeholder is rejected by the no-param entry point', async () => {
    const db = await makeDb();
    try {
      expect(() => db.query('SELECT * FROM User WHERE id = ?')).toThrow(
        'SQL statement expects 1 parameters but received 0'
      );
      // Instance stays healthy.
      expect(db.query('SELECT COUNT(*) FROM User').rows[0][0]).toBe(2);
    } finally {
      db.destroy();
    }
  });

  test('NULL bound to indexed and key columns matches nothing and does not hang', async () => {
    const db = await makeDb();
    try {
      expect(db.query('SELECT * FROM User WHERE id = ?', [null]).rows).toEqual([]);
      expect(db.query('SELECT * FROM User WHERE email = ?', [null]).rows).toEqual([]);
      expect(db.query('SELECT COUNT(*) FROM User').rows[0][0]).toBe(2);
    } finally {
      db.destroy();
    }
  });
});
