import assert from 'node:assert/strict';
import initFlatSQL from './index.js';

const schema = `
  table User {
    id: int (id);
    name: string;
    email: string (key);
    age: int;
  }
  root_type User;
`;

async function main() {
  const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
  const db = flatsql.createDatabase(schema, 'bulk-ingest-test');

  try {
    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();

    const buffers = [
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
      flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
      flatsql.createTestUser(3, 'Charlie', 'charlie@example.com', 35),
    ];

    db.resetIngestProfile();
    const bytesConsumed = db.ingestBuffers(buffers);
    assert.ok(bytesConsumed > 0, 'bulk ingest should consume bytes');

    const stats = db.getStats();
    assert.ok(stats.some((stat) => stat.tableName === 'User' && stat.recordCount === 3));

    const profile = db.getIngestProfile();
    assert.equal(profile.recordCount, 3);
    assert.ok(profile.byteCount > 0);
    assert.ok(profile.decodeNanos >= 0);
    assert.ok(profile.appendNanos >= 0);
    assert.ok(profile.indexNanos >= 0);

    const exported = db.exportData();
    assert.ok(exported.length > 0, 'bulk-ingested database should export data');

    const queried = db.query('SELECT id, name, email, age FROM User ORDER BY id');
    assert.deepEqual(queried.columns, ['id', 'name', 'email', 'age']);
    assert.deepEqual(queried.rows, [
      [1, 'Alice', 'alice@example.com', 30],
      [2, 'Bob', 'bob@example.com', 25],
      [3, 'Charlie', 'charlie@example.com', 35],
    ]);

    console.log('WASM bulk ingest test passed');
  } finally {
    db.destroy();
  }

  const multiDb = flatsql.createDatabase(schema, 'bulk-ingest-multi-source-test');

  try {
    multiDb.registerFileId('USER', 'User');
    multiDb.enableDemoExtractors();
    multiDb.registerSource('site-a');
    multiDb.registerSource('site-b');
    multiDb.createUnifiedViews();

    multiDb.ingestOne(flatsql.createTestUser(10, 'Delta', 'delta@example.com', 40), 'site-a');
    multiDb.ingestOne(flatsql.createTestUser(11, 'Echo', 'echo@example.com', 41), 'site-b');

    const sourceRows = multiDb.query('SELECT id, name FROM "User@site-a" ORDER BY id');
    assert.deepEqual(sourceRows.columns, ['id', 'name']);
    assert.deepEqual(sourceRows.rows, [[10, 'Delta']]);

    const unifiedRows = multiDb.query('SELECT _source, id, name FROM User ORDER BY id');
    assert.deepEqual(unifiedRows.columns, ['_source', 'id', 'name']);
    assert.deepEqual(unifiedRows.rows, [
      ['User@site-a', 10, 'Delta'],
      ['User@site-b', 11, 'Echo'],
    ]);

    console.log('WASM multi-source query test passed');
  } finally {
    multiDb.destroy();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
