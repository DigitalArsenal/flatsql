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

  test('rejects parameter count mismatches instead of reusing stale bindings', async () => {
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
      'wasm-query-param-count'
    );

    expect(
      db.query('SELECT ?1 AS first_value, ?2 IS NULL AS second_is_null', [
        9,
        'sticky',
      ])
    ).toEqual({
      columns: ['first_value', 'second_is_null'],
      rows: [[9, 0]],
    });

    expect(() =>
      db.query('SELECT ?1 AS first_value, ?2 IS NULL AS second_is_null', [10])
    ).toThrow(/expects 2 parameters but received 1/);

    db.destroy();
  });

  test('does not fast-path point lookups with trailing predicates', async () => {
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
      'wasm-query-fast-path-trailing-predicate'
    );

    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
    ]);

    expect(db.query('SELECT * FROM User WHERE id = ? AND 0', [1]).rows).toEqual([]);

    db.destroy();
  });

  test('isolates fast-path source caches across database handles', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const schema = `
      table User {
        id: int (id);
        name: string;
        email: string (key);
        age: int;
      }
    `;
    const dbA = flatsql.createDatabase(schema, 'wasm-query-cache-db-a');
    const dbB = flatsql.createDatabase(schema, 'wasm-query-cache-db-b');

    dbA.registerFileId('USER', 'User');
    dbA.enableDemoExtractors();
    dbA.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
    ]);

    dbB.registerFileId('USER', 'User');
    dbB.enableDemoExtractors();
    dbB.ingestBuffers([
      flatsql.createTestUser(1, 'Bob', 'bob@example.com', 25),
    ]);

    expect(dbA.query('SELECT * FROM User WHERE id = ?', [1]).rows[0][2]).toBe('alice@example.com');
    expect(dbB.query('SELECT * FROM User WHERE id = ?', [1]).rows[0][2]).toBe('bob@example.com');

    dbA.destroy();
    dbB.destroy();
  });

  test('exposes direct raw FlatBuffer lookup helpers for indexed keys', async () => {
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
      'wasm-query-raw-flatbuffer-access'
    );

    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    const alice = flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30);
    const bob = flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25);
    db.ingestBuffers([alice, bob]);

    const bobRef = db.getFlatBufferById('User', 2);
    expect(bobRef).not.toBeNull();
    expect(bobRef!.size).toBe(bob.length);
    expect(bobRef!.sequence).toBe(2);
    expect(Array.from(db.getFlatBufferDataById('User', 2)!)).toEqual(Array.from(bob));

    const aliceRef = db.getFlatBufferByEmail('User', 'alice@example.com');
    expect(aliceRef).not.toBeNull();
    expect(aliceRef!.size).toBe(alice.length);
    expect(aliceRef!.sequence).toBe(1);
    expect(db.getFlatBufferById('User', 99)).toBeNull();

    const storage = db.getStorageInfo();
    expect(storage.ptr).toBeGreaterThan(0);
    expect(storage.size).toBeGreaterThanOrEqual(alice.length + bob.length + 8);

    db.destroy();
  });

  test('builds canonical query cache keys through the WASM core', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });

    const key = flatsql.buildQueryCacheKey('PNM', 'sha-123', 'by-file-id', ['PNM|42']);
    expect(key).toBe(
      'flatsql:v1|d=504e4d|a=7368612d313233|q=62792d66696c652d6964|p=1:s=504e4d7c3432'
    );
    expect(flatsql.buildQueryCacheKey('PNM', 'sha-123', 'by-file-id', ['PNM|42'])).toBe(key);
    expect(flatsql.buildQueryCacheKey('PNM', 'sha-456', 'by-file-id', ['PNM|42'])).not.toBe(key);
    expect(flatsql.buildQueryCacheKey('PNM', 'sha-123', 'by-file-id', [42])).not.toBe(key);
  });

  test('exposes generic raw FlatBuffer lookup by indexed column', async () => {
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
      'wasm-query-raw-flatbuffer-by-index'
    );

    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    const alice = flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30);
    const bob = flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25);
    db.ingestBuffers([alice, bob]);

    const bobRef = db.getFlatBufferByIndex('User', 'id', 2);
    expect(bobRef).not.toBeNull();
    expect(bobRef!.size).toBe(bob.length);
    expect(bobRef!.sequence).toBe(2);
    expect(Array.from(db.getFlatBufferDataByIndex('User', 'email', 'alice@example.com')!)).toEqual(
      Array.from(alice)
    );
    expect(db.getFlatBufferByIndex('User', 'email', 'missing@example.com')).toBeNull();

    db.destroy();
  });

  test('executes cached query templates in the WASM core and invalidates them on ingest', async () => {
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
      'wasm-query-template-cache'
    );

    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.registerQueryTemplate('user_count', 'SELECT COUNT(*) FROM User');

    db.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
    ]);

    expect(db.queryTemplate('user_count')).toEqual({
      columns: ['COUNT(*)'],
      rows: [[1]],
    });
    expect(db.getQueryCacheStats()).toMatchObject({ hits: 0, misses: 1, size: 1 });

    expect(db.queryTemplate('user_count')).toEqual({
      columns: ['COUNT(*)'],
      rows: [[1]],
    });
    expect(db.getQueryCacheStats()).toMatchObject({ hits: 1, misses: 1, size: 1 });

    db.ingestBuffers([
      flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
    ]);

    expect(db.queryTemplate('user_count')).toEqual({
      columns: ['COUNT(*)'],
      rows: [[2]],
    });
    expect(db.getQueryCacheStats()).toMatchObject({ hits: 1, misses: 2, size: 1 });

    db.registerQueryTemplate('uncached_count', 'SELECT COUNT(*) FROM User', false);
    db.queryTemplate('uncached_count');
    db.queryTemplate('uncached_count');
    expect(db.getQueryCacheStats()).toMatchObject({ hits: 1, misses: 2 });

    expect(() => db.queryTemplate('missing_template')).toThrow(/Query template not found/);

    db.destroy();
  });
});
