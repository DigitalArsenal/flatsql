import initFlatSQL from '../wasm/index.js';

describe('WASM parameterized queries', () => {
  test('binds positional parameters through the WASM query API', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(
      `
        table User {
          id: int (id);
          name: string;
          email: string (key);
          age: int;
        }
      `,
      'wasm-query-params'
    );

    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
      flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
    ]);

    expect(db.query('SELECT email FROM User WHERE id = ?', [2])).toEqual({
      columns: ['email'],
      rows: [['bob@example.com']],
    });

    db.destroy();
  });

  test('returns batch query results in request order', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(
      `
        table User {
          id: int (id);
          name: string;
          email: string (key);
          age: int;
        }
      `,
      'wasm-query-many'
    );

    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
      flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
    ]);

    expect(
      db.queryMany([
        { sql: 'SELECT email FROM User WHERE id = ?', params: [1] },
        { sql: 'SELECT email FROM User WHERE id = ?', params: [2] },
      ])
    ).toEqual([
      { columns: ['email'], rows: [['alice@example.com']] },
      { columns: ['email'], rows: [['bob@example.com']] },
    ]);

    db.destroy();
  });

  test('matches literal SQL results for the same lookup', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(
      `
        table User {
          id: int (id);
          name: string;
          email: string (key);
          age: int;
        }
      `,
      'wasm-query-literal-equivalence'
    );

    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
      flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
    ]);

    expect(db.query('SELECT email FROM User WHERE id = ?', [2])).toEqual(
      db.query('SELECT email FROM User WHERE id = 2')
    );

    db.destroy();
  });

  test('preserves SQLite result semantics for blob, null, and boolean parameters', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(
      `
        table User {
          id: int (id);
          name: string;
          email: string (key);
          age: int;
        }
      `,
      'wasm-query-param-types'
    );

    expect(
      db.query('SELECT ? AS payload, ? AS missing, ? AS flag', [
        new Uint8Array([1, 2, 3]),
        null,
        true,
      ])
    ).toEqual({
      columns: ['payload', 'missing', 'flag'],
      rows: [[[1, 2, 3], null, 1]],
    });

    db.destroy();
  });
});
