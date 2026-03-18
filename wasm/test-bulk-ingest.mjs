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

    console.log('WASM bulk ingest test passed');
  } finally {
    db.destroy();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
