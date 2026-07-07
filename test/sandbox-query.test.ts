/**
 * Sandboxed public query (gateway loop G.5).
 *
 * flatsql_query_sandboxed executes ONE read-only SELECT for untrusted
 * callers: an authorizer restricts reads to the record tables / source
 * shadow tables / unified views (control tables created through plain SQL
 * DDL are invisible), PRAGMA / ATTACH / every DDL-DML verb / temp writes /
 * transactions are denied, multi-statement input is rejected, a progress-
 * handler deadline bounds runaway statements, and row/byte caps REJECT
 * oversized results (never truncate). Verified on both hosts: the
 * emscripten build (wasm/index.js) and the standalone WASI build
 * (wasm/standalone.js — the artifact the Go server embeds).
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

const PUBLISH_EVENT_SCHEMA = `
table PublishEventRecord {
  FILE_ID: string (key);
  RECORD_ID: string;
  EVENT_INDEX: int;
  PAYLOAD_SIZE: int;
}

root_type PublishEventRecord;
`;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyDb = any;

function expectSandboxError(db: AnyDb, sql: string, needle: string, options = {}) {
  let message = '';
  try {
    db.querySandboxed(sql, [], options);
  } catch (err) {
    message = (err as Error).message;
  }
  expect(message).toContain(needle);
}

async function emscriptenUserDb() {
  const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
  const db = flatsql.createDatabase(USER_SCHEMA, 'sandbox-users');
  db.registerFileId('USER', 'User');
  db.enableDemoExtractors();
  db.ingestBuffers([
    flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
    flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
    flatsql.createTestUser(3, 'Cara', 'cara@example.com', 41),
  ]);
  return { flatsql, db };
}

describe('sandboxed public query (emscripten host)', () => {
  test('plain SELECT works in both output modes', async () => {
    const { flatsql, db } = await emscriptenUserDb();

    // Record-stream mode: all-BLOB projection -> aligned frames, verbatim.
    const stream = db.querySandboxed('SELECT _data FROM User ORDER BY id', [], {
      mode: 'stream',
    });
    expect(stream.rows).toBe(3);
    expect(stream.columns).toBe(1);
    const frames = decodeSizePrefixedStream(stream.payload);
    expect(frames.length).toBe(3);
    // Byte-parity with the unsandboxed raw-stream path.
    const reference = db.queryRawFlatBufferStream('SELECT _data FROM User ORDER BY id');
    expect(Buffer.from(stream.payload)).toEqual(Buffer.from(reference));

    // JSON mode: bare array, column names verbatim.
    const json = db.querySandboxed(
      'SELECT id, name, age FROM User WHERE age > ? ORDER BY id',
      [26],
      { mode: 'json' }
    );
    const rows = JSON.parse(Buffer.from(json.payload).toString('utf8'));
    expect(rows).toEqual([
      { id: 1, name: 'Alice', age: 30 },
      { id: 3, name: 'Cara', age: 41 },
    ]);

    // Column-name capitalization is preserved verbatim (the SDS JSON-key
    // hard rule rides on this).
    const aliased = db.querySandboxed('SELECT id AS NORAD_CAT_ID FROM User WHERE id = 1', [], {
      mode: 'json',
    });
    expect(JSON.parse(Buffer.from(aliased.payload).toString('utf8'))).toEqual([
      { NORAD_CAT_ID: 1 },
    ]);

    db.destroy();
  });

  test('write, DDL, PRAGMA, ATTACH, temp writes and transactions are denied', async () => {
    const { db } = await emscriptenUserDb();

    // Plain (non-vtab) table: the AUTHORIZER denies DML at prepare.
    db.query('CREATE TABLE ctl_plain (k TEXT)');
    expectSandboxError(db, "INSERT INTO ctl_plain VALUES ('x')", 'sandbox: not-authorized');
    expectSandboxError(db, "UPDATE ctl_plain SET k = 'x'", 'sandbox: not-authorized');
    expectSandboxError(db, 'DELETE FROM ctl_plain', 'sandbox: not-authorized');
    // Record vtabs have no xUpdate at all — writes refuse either at the
    // authorizer or at the vtab layer (both structural; which fires first
    // depends on the statement shape).
    for (const sql of [
      "INSERT INTO User (id, name) VALUES (9, 'x')",
      "UPDATE User SET name = 'x'",
      'DELETE FROM User',
    ]) {
      let message = '';
      try {
        db.querySandboxed(sql);
      } catch (err) {
        message = (err as Error).message;
      }
      expect(message).toMatch(/may not be modified|sandbox: not-authorized/);
    }
    expectSandboxError(db, 'DROP TABLE User', 'sandbox: not-authorized');
    expectSandboxError(db, 'CREATE TABLE evil (x)', 'sandbox: not-authorized');
    expectSandboxError(db, 'CREATE TEMP TABLE evil (x)', 'sandbox: not-authorized');
    expectSandboxError(db, 'PRAGMA journal_mode = DELETE', 'sandbox: not-authorized');
    expectSandboxError(db, 'PRAGMA table_info(User)', 'sandbox: not-authorized');
    expectSandboxError(db, "ATTACH DATABASE ':memory:' AS other", 'sandbox: not-authorized');
    expectSandboxError(db, 'BEGIN', 'sandbox: not-authorized');
    expectSandboxError(db, 'VACUUM', 'sandbox:');
    expectSandboxError(db, 'ANALYZE', 'sandbox:');

    // Nothing was modified.
    const check = db.querySandboxed('SELECT count(*) AS n FROM User', [], { mode: 'json' });
    expect(JSON.parse(Buffer.from(check.payload).toString('utf8'))).toEqual([{ n: 3 }]);

    db.destroy();
  });

  test('multi-statement and empty input are rejected', async () => {
    const { db } = await emscriptenUserDb();

    expectSandboxError(
      db,
      'SELECT id FROM User; SELECT name FROM User',
      'sandbox: multi-statement'
    );
    expectSandboxError(db, "SELECT id FROM User; DROP TABLE User", 'sandbox: multi-statement');
    expectSandboxError(db, '   ', 'sandbox: empty-statement');
    expectSandboxError(db, '-- just a comment', 'sandbox: empty-statement');

    db.destroy();
  });

  test('control tables and sqlite_master are outside the public surface', async () => {
    const { db } = await emscriptenUserDb();

    // Control table created through the (trusted) plain query path.
    db.query('CREATE TABLE ctl_secrets (k TEXT, v TEXT)');
    db.query("INSERT INTO ctl_secrets VALUES ('token', 'hunter2')");

    expectSandboxError(db, 'SELECT * FROM ctl_secrets', 'outside the public query surface');
    expectSandboxError(db, 'SELECT * FROM sqlite_master', 'outside the public query surface');
    expectSandboxError(
      db,
      'SELECT id FROM User UNION ALL SELECT 1 FROM ctl_secrets',
      'outside the public query surface'
    );

    db.destroy();
  });

  test('runaway statements hit the deadline in bounded time', async () => {
    const { db } = await emscriptenUserDb();

    const started = Date.now();
    expectSandboxError(
      db,
      'WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c',
      'sandbox: timeout',
      { timeoutMs: 200 }
    );
    const elapsed = Date.now() - started;
    expect(elapsed).toBeLessThan(5000);

    // Cartesian blowup is also bounded (rows never materialize past the cap).
    expectSandboxError(
      db,
      'SELECT a.id FROM User a, User b, User c, User d, User e, User f, User g, User h, User i, User j',
      'sandbox: row-cap',
      { mode: 'json', maxRows: 1000, timeoutMs: 2000 }
    );

    db.destroy();
  });

  test('row and byte caps reject oversized results', async () => {
    const { db } = await emscriptenUserDb();

    expectSandboxError(db, 'SELECT id FROM User', 'sandbox: row-cap', { mode: 'json', maxRows: 2 });
    expectSandboxError(db, 'SELECT _data FROM User', 'sandbox: byte-cap', {
      mode: 'stream',
      maxBytes: 16,
    });
    expectSandboxError(db, 'SELECT name FROM User', 'sandbox: byte-cap', {
      mode: 'json',
      maxBytes: 8,
    });

    // At-cap results still pass.
    const ok = db.querySandboxed('SELECT id FROM User', [], { mode: 'json', maxRows: 3 });
    expect(ok.rows).toBe(3);

    db.destroy();
  });

  test('stream mode requires BLOB-only projections; params are validated', async () => {
    const { db } = await emscriptenUserDb();

    expectSandboxError(db, 'SELECT id, name FROM User', 'sandbox: not-a-record-stream', {
      mode: 'stream',
    });
    expectSandboxError(db, 'SELECT id FROM User WHERE id = ?', 'sandbox: params');

    db.destroy();
  });

  test('sandbox failures latch cleanly and the engine stays usable', async () => {
    const { db } = await emscriptenUserDb();

    expectSandboxError(db, 'DROP TABLE User', 'sandbox: not-authorized');
    expectSandboxError(db, 'SELECT nope FROM User', 'SQL error');

    const after = db.querySandboxed('SELECT count(*) AS n FROM User', [], { mode: 'json' });
    expect(JSON.parse(Buffer.from(after.payload).toString('utf8'))).toEqual([{ n: 3 }]);
    // The unsandboxed path still works too.
    const raw = db.queryRawFlatBufferStream('SELECT _data FROM User ORDER BY id');
    expect(decodeSizePrefixedStream(raw).length).toBe(3);

    db.destroy();
  });
});

describe('sandboxed public query (standalone WASI host — the server artifact)', () => {
  test('SELECT works, injections rejected, caps enforced, capitalization verbatim', async () => {
    const flatsql = await loadFlatSQLStandalone();
    const db = flatsql.createDatabase(PUBLISH_EVENT_SCHEMA, 'sandbox-wasi');
    db.registerFileId('PUBL', 'PublishEventRecord');
    db.enableDemoExtractors();
    db.ingestBuffers([
      flatsql.createTestPublishEvent('PNM|1', 'rec-1', 0, 100),
      flatsql.createTestPublishEvent('PNM|2', 'rec-2', 1, 250),
    ]);

    // Uppercase schema column names survive the JSON path verbatim.
    const json = db.querySandboxed(
      'SELECT FILE_ID, EVENT_INDEX, PAYLOAD_SIZE FROM PublishEventRecord ORDER BY EVENT_INDEX',
      [],
      { mode: 'json' }
    );
    expect(JSON.parse(Buffer.from(json.payload).toString('utf8'))).toEqual([
      { FILE_ID: 'PNM|1', EVENT_INDEX: 0, PAYLOAD_SIZE: 100 },
      { FILE_ID: 'PNM|2', EVENT_INDEX: 1, PAYLOAD_SIZE: 250 },
    ]);

    // Stream mode byte-parity with the raw-stream path.
    const stream = db.querySandboxed(
      'SELECT _data FROM PublishEventRecord ORDER BY EVENT_INDEX',
      [],
      { mode: 'stream' }
    );
    const reference = db.queryRawFlatBufferStream(
      'SELECT _data FROM PublishEventRecord ORDER BY EVENT_INDEX'
    );
    expect(Buffer.from(stream.payload)).toEqual(Buffer.from(reference));

    const cases: Array<[string, string, Record<string, number | string>]> = [
      ["UPDATE PublishEventRecord SET RECORD_ID = 'x'", 'not', {}],  // vtab-layer or authorizer refusal
      ['DROP TABLE PublishEventRecord', 'sandbox: not-authorized', {}],
      ['PRAGMA schema_version', 'sandbox: not-authorized', {}],
      ["ATTACH DATABASE ':memory:' AS other", 'sandbox: not-authorized', {}],
      ['SELECT 1; SELECT 2', 'sandbox: multi-statement', {}],
      ['SELECT * FROM sqlite_master', 'outside the public query surface', {}],
      [
        'WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c',
        'sandbox: timeout',
        { timeoutMs: 200 },
      ],
      ['SELECT FILE_ID FROM PublishEventRecord', 'sandbox: row-cap', { mode: 'json', maxRows: 1 }],
    ];
    for (const [sql, needle, options] of cases) {
      let message = '';
      try {
        db.querySandboxed(sql, [], options as never);
      } catch (err) {
        message = (err as Error).message;
      }
      expect(`${sql} -> ${message}`).toContain(needle);
    }

    // Engine remains healthy after every rejection.
    const after = db.querySandboxed('SELECT count(*) AS N FROM PublishEventRecord', [], {
      mode: 'json',
    });
    expect(JSON.parse(Buffer.from(after.payload).toString('utf8'))).toEqual([{ N: 2 }]);

    db.destroy();
  });
});
