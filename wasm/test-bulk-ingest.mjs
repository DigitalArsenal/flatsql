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

const mpeSchema = `
  table MPE {
    ENTITY_ID: string (key);
    EPOCH: double;
    MEAN_MOTION: double;
    ECCENTRICITY: double;
    INCLINATION: double;
    RA_OF_ASC_NODE: double;
    ARG_OF_PERICENTER: double;
    MEAN_ANOMALY: double;
    BSTAR: double;
    MEAN_ELEMENT_THEORY: int;
  }
  root_type MPE;
`;

const telemetrySchema = `
  table Telemetry {
    packet_id: int (id);
    spacecraft: string;
    subsystem: string;
    mode: string;
    temperature_c: int;
    signal_db: int;
    timestamp_s: int;
  }
  root_type Telemetry;
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

  const mpeDb = flatsql.createDatabase(mpeSchema, 'bulk-ingest-mpe-test');

  try {
    mpeDb.registerFileId('$MPE', 'MPE');
    mpeDb.enableDemoExtractors();
    mpeDb.ingestBuffers([
      flatsql.createTestMPE('60001', 1710000000, 14.9, 0.00011, 97.4, 15, 120, 45, 0.00008, 0),
      flatsql.createTestMPE('60002', 1710000900, 13.2, 0.00003, 53.1, 33, 210, 80, 0.00002, 1),
    ]);

    const mpeRows = mpeDb.query('SELECT ENTITY_ID, MEAN_ELEMENT_THEORY FROM MPE ORDER BY ENTITY_ID');
    assert.deepEqual(mpeRows.columns, ['ENTITY_ID', 'MEAN_ELEMENT_THEORY']);
    assert.deepEqual(mpeRows.rows, [
      ['60001', 0],
      ['60002', 1],
    ]);

    const mpeCount = mpeDb.query('SELECT COUNT(*) as total FROM MPE WHERE INCLINATION > 90');
    assert.equal(mpeCount.rows[0][0], 1);

    console.log('WASM MPE demo dataset test passed');
  } finally {
    mpeDb.destroy();
  }

  const telemetryDb = flatsql.createDatabase(telemetrySchema, 'bulk-ingest-telemetry-test');

  try {
    telemetryDb.registerFileId('TELE', 'Telemetry');
    telemetryDb.enableDemoExtractors();
    telemetryDb.ingestBuffers([
      flatsql.createTestTelemetry(1, 'SAT-01', 'POWER', 'NOMINAL', 42, 58, 1710000000),
      flatsql.createTestTelemetry(2, 'SAT-02', 'COMMS', 'SAFE', 71, 39, 1710000015),
      flatsql.createTestTelemetry(3, 'SAT-01', 'PAYLOAD', 'SCIENCE', 55, 47, 1710000030),
    ]);

    const telemetryRows = telemetryDb.query(
      'SELECT packet_id, spacecraft, subsystem, mode FROM Telemetry ORDER BY packet_id'
    );
    assert.deepEqual(telemetryRows.columns, ['packet_id', 'spacecraft', 'subsystem', 'mode']);
    assert.deepEqual(telemetryRows.rows, [
      [1, 'SAT-01', 'POWER', 'NOMINAL'],
      [2, 'SAT-02', 'COMMS', 'SAFE'],
      [3, 'SAT-01', 'PAYLOAD', 'SCIENCE'],
    ]);

    const telemetryCount = telemetryDb.query(
      "SELECT COUNT(*) as total FROM Telemetry WHERE signal_db < 45 OR mode = 'SAFE'"
    );
    assert.equal(telemetryCount.rows[0][0], 1);

    console.log('WASM telemetry demo dataset test passed');
  } finally {
    telemetryDb.destroy();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
