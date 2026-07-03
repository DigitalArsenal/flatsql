/**
 * Raw-stream response artifact cache (loop C.5b).
 *
 * flatsql_query_raw_flatbuffer_stream serves repeated (sql, params) requests
 * from a materialized aligned-stream cache without re-executing SQL. Any
 * ingest, mark-deleted, or DML through plain query() invalidates. Verified
 * on BOTH hosts: the emscripten build (wasm/index.js) and the standalone
 * WASI build (wasm/standalone.js — the artifact the Go server embeds).
 */
import initFlatSQL from '../wasm/index.js';
import { loadFlatSQLStandalone } from '../wasm/standalone.js';
import { decodeSizePrefixedStream } from '../src/artifacts/transport.js';

const USER_SCHEMA = `
  table User {
    id: int (id);
    name: string;
    email: string (key);
    age: int;
  }
`;

describe('raw-stream response artifact cache (emscripten host)', () => {
  test('repeat queries hit the cache and return identical bytes', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(USER_SCHEMA, 'raw-cache-basic');
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    const alice = flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30);
    const bob = flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25);
    db.ingestBuffers([alice, bob]);

    const sql = 'SELECT _data FROM User ORDER BY id';
    const first = db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    const second = db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(true);
    expect(Buffer.from(second)).toEqual(Buffer.from(first));
    expect(decodeSizePrefixedStream(second)).toEqual([alice, bob]);

    const stats = db.getRawStreamCacheStats();
    expect(stats.hits).toBe(1);
    expect(stats.misses).toBe(1);
    expect(stats.entries).toBe(1);
    expect(stats.totalBytes).toBe(first.length);

    db.destroy();
  });

  test('different params are distinct cache entries', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(USER_SCHEMA, 'raw-cache-params');
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    const alice = flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30);
    const bob = flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25);
    db.ingestBuffers([alice, bob]);

    const sql = 'SELECT _data FROM User WHERE id = ?';
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql, [1]))).toEqual([alice]);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql, [2]))).toEqual([bob]);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql, [1]))).toEqual([alice]);
    expect(db.lastRawStreamCacheHit()).toBe(true);
    expect(db.getRawStreamCacheStats().entries).toBe(2);

    db.destroy();
  });

  test('ingest invalidates cached raw streams', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(USER_SCHEMA, 'raw-cache-ingest');
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    const alice = flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30);
    db.ingestBuffers([alice]);

    const sql = 'SELECT _data FROM User ORDER BY id';
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql))).toEqual([alice]);
    db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(true);

    const bob = flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25);
    db.ingestBuffers([bob]);

    const after = db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    expect(decodeSizePrefixedStream(after)).toEqual([alice, bob]);

    db.destroy();
  });

  test('markDeleted invalidates cached raw streams', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(USER_SCHEMA, 'raw-cache-delete');
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    const alice = flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30);
    const bob = flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25);
    db.ingestBuffers([alice, bob]);

    const sql = 'SELECT _data FROM User ORDER BY id';
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql))).toEqual([alice, bob]);

    db.markDeleted('User', 2);

    const after = db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    expect(decodeSizePrefixedStream(after)).toEqual([alice]);

    db.destroy();
  });

  test('DML through plain query() invalidates cached raw streams', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(USER_SCHEMA, 'raw-cache-dml');
    db.query('CREATE TABLE control (k TEXT PRIMARY KEY, v BLOB)');
    db.query("INSERT INTO control (k, v) VALUES ('a', x'01020304')");

    const sql = "SELECT v FROM control WHERE k = 'a'";
    const first = db.queryRawFlatBufferStream(sql);
    expect(decodeSizePrefixedStream(first)).toEqual([new Uint8Array([1, 2, 3, 4])]);
    db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(true);

    db.query("UPDATE control SET v = x'0506' WHERE k = 'a'");

    const after = db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    expect(decodeSizePrefixedStream(after)).toEqual([new Uint8Array([5, 6])]);

    db.destroy();
  });

  test('cache limits evict by LRU and never break results', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(USER_SCHEMA, 'raw-cache-limits');
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    const alice = flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30);
    const bob = flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25);
    db.ingestBuffers([alice, bob]);
    db.configureRawStreamCache(1, 1024 * 1024);

    const sql = 'SELECT _data FROM User WHERE id = ?';
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql, [1]))).toEqual([alice]);
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql, [2]))).toEqual([bob]);
    expect(db.getRawStreamCacheStats().entries).toBe(1);
    // id=1 was evicted (maxEntries=1) — re-running is a miss but correct.
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql, [1]))).toEqual([alice]);
    expect(db.lastRawStreamCacheHit()).toBe(false);

    // Zero-byte budget: everything is served uncached.
    db.configureRawStreamCache(4, 0);
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql, [1]))).toEqual([alice]);
    expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(sql, [1]))).toEqual([alice]);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    expect(db.getRawStreamCacheStats().entries).toBe(0);

    db.destroy();
  });

  test('non-BLOB cells still latch the classic error', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(USER_SCHEMA, 'raw-cache-error');
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.ingestBuffers([flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30)]);

    expect(() => db.queryRawFlatBufferStream('SELECT id FROM User')).toThrow(
      'raw response stream queries must return only BLOB cells'
    );
    // Engine stays usable after the latched error.
    expect(
      decodeSizePrefixedStream(db.queryRawFlatBufferStream('SELECT _data FROM User'))
    ).toHaveLength(1);

    db.destroy();
  });
});

describe('raw-stream response artifact cache (standalone WASI host)', () => {
  test('cache hit round-trip with ingest invalidation on the noeh contract host', async () => {
    const flatsql = await loadFlatSQLStandalone();
    const db = flatsql.createDatabase(USER_SCHEMA, 'raw-cache-standalone');
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    const alice = flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30);
    const bob = flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25);
    db.ingestBuffers([alice]);

    const sql = 'SELECT _data FROM User ORDER BY id';
    const first = db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    const second = db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(true);
    expect(Buffer.from(second)).toEqual(Buffer.from(first));

    db.ingestBuffers([bob]);
    const after = db.queryRawFlatBufferStream(sql);
    expect(db.lastRawStreamCacheHit()).toBe(false);
    expect(decodeSizePrefixedStream(after)).toEqual([alice, bob]);

    const stats = db.getRawStreamCacheStats();
    expect(stats.hits).toBe(1);
    expect(stats.misses).toBe(2);
  });
});
