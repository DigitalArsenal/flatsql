import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import * as flatbuffers from './lib/flatbuffers.js';
import { DirectAccessor, FlatSQLDatabase } from '../dist/index.js';
import initFlatSQL from '../wasm/index.js';

const { Builder, ByteBuffer } = flatbuffers;
const TABLE_NAME = 'OMMBench';
const FILE_IDENTIFIER = 'OMMB';
const SCHEMA_TEMPLATE = (indexedFields) => `
table ${TABLE_NAME} {
  noradCatId:uint32 (key, indexed);
  objectId:string${indexedFields.includes('objectId') ? ' (indexed)' : ''};
  epoch:long${indexedFields.includes('epoch') ? ' (indexed)' : ''};
  meanMotion:double;
}
root_type ${TABLE_NAME};
`;

const SCENARIOS = [
  {
    id: '10k-ref-1idx',
    label: '10k sorted (1-index)',
    rows: 10000,
    sorted: true,
    indexes: ['noradCatId'],
    gate: 2.0,
  },
  {
    id: '100k-unsorted-3idx',
    label: '100k unsorted (3-index)',
    rows: 100000,
    sorted: false,
    indexes: ['noradCatId', 'objectId', 'epoch'],
    gate: 1.5,
  },
  {
    id: 'sorted-1idx-easy',
    label: 'sorted 1-index stretch',
    rows: 25000,
    sorted: true,
    indexes: ['noradCatId'],
    gate: 3.0, // stretch target
  },
];

class OMMBenchRecord {
  constructor() {
    this.bb = null;
    this.bb_pos = 0;
  }

  __init(position, bb) {
    this.bb_pos = position;
    this.bb = bb;
    return this;
  }

  static getRoot(data) {
    const bb = new ByteBuffer(data);
    return new OMMBenchRecord().__init(bb.readInt32(bb.position()) + bb.position(), bb);
  }

  noradCatId() {
    const offset = this.bb.__offset(this.bb_pos, 4);
    return offset ? this.bb.readUint32(this.bb_pos + offset) : 0;
  }

  objectId() {
    const offset = this.bb.__offset(this.bb_pos, 6);
    return offset ? this.bb.__string(this.bb_pos + offset) : '';
  }

  epoch() {
    const offset = this.bb.__offset(this.bb_pos, 8);
    return offset ? Number(this.bb.readInt64(this.bb_pos + offset)) : 0;
  }

  meanMotion() {
    const offset = this.bb.__offset(this.bb_pos, 10);
    return offset ? this.bb.readFloat64(this.bb_pos + offset) : 0;
  }
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle];
  }
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function buildBenchmarkRecord(index) {
  const baseEpoch = Date.UTC(2026, 2, 16, 12, 0, 0) / 1000;
  const noradCatId = 70000 + index;
  const objectId = `2026-${String((index % 365) + 1).padStart(3, '0')}A`;
  const epoch = baseEpoch + index * 60;
  const meanMotion = 14.5 + (index % 120) / 1000;

  const builder = new Builder(128);
  const objectIdOffset = builder.createString(objectId);

  builder.startObject(4);
  builder.addFieldInt32(0, noradCatId, 0);
  builder.addFieldOffset(1, objectIdOffset, 0);
  builder.addFieldInt64(2, BigInt(Math.floor(epoch)), 0n);
  builder.addFieldFloat64(3, meanMotion, 0);
  const root = builder.endObject();
  builder.finish(root, FILE_IDENTIFIER);

  const flatbuffer = builder.asUint8Array().slice();
  return {
    noradCatId,
    objectId,
    epoch,
    meanMotion,
    flatbuffer,
  };
}

function buildRecords(count, sorted) {
  const records = Array.from({ length: count }, (_, i) => buildBenchmarkRecord(i));
  if (!sorted) {
    for (let i = records.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [records[i], records[j]] = [records[j], records[i]];
    }
  }
  return records;
}

function buildSizePrefixedStream(buffers) {
  let total = 0;
  for (const buffer of buffers) {
    total += 4 + buffer.length;
  }

  const stream = new Uint8Array(total);
  let offset = 0;
  for (const buffer of buffers) {
    new DataView(stream.buffer, offset, 4).setUint32(0, buffer.length, true);
    offset += 4;
    stream.set(buffer, offset);
    offset += buffer.length;
  }
  return stream;
}

function createAccessor() {
  const accessor = new DirectAccessor();
  accessor.registerAccessor(TABLE_NAME, (data, path) => {
    const field = path[path.length - 1];
    const record = OMMBenchRecord.getRoot(data);

    switch (field) {
      case 'noradCatId':
        return record.noradCatId();
      case 'objectId':
        return record.objectId();
      case 'epoch':
        return record.epoch();
      case 'meanMotion':
        return record.meanMotion();
      default:
        return undefined;
    }
  });
  return accessor;
}

function measureFlatSQLJS(records, schema) {
  const db = FlatSQLDatabase.fromSchema(schema, createAccessor(), 'flatjs-bench');
  const midRecord = records[Math.floor(records.length / 2)];
  const started = performance.now();
  db.stream(TABLE_NAME, records.map((record) => record.flatbuffer));
  const elapsed = performance.now() - started;

  const query = db.query(`SELECT objectId FROM ${TABLE_NAME} WHERE noradCatId = ${midRecord.noradCatId}`);
  assert.strictEqual(query.rowCount, 1);
  assert.strictEqual(query.rows[0][0], midRecord.objectId);
  return elapsed;
}

async function measureFlatSQLWASM(records, schema, flatSql, profileMode = false) {
  const db = flatSql.createDatabase(schema, `bench-${Date.now()}`);
  db.registerFileId(FILE_IDENTIFIER, TABLE_NAME);
  const buffers = records.map((record) => record.flatbuffer);

  let packElapsed = 0;
  let ingestElapsed = 0;
  let profile = null;
  if (profileMode) {
    db.resetIngestProfile();
    const packStarted = performance.now();
    const stream = buildSizePrefixedStream(buffers);
    packElapsed = performance.now() - packStarted;
    const ingestStarted = performance.now();
    db.ingest(stream);
    ingestElapsed = performance.now() - ingestStarted;
    profile = db.getIngestProfile();
  } else {
    const started = performance.now();
    db.ingestBuffers(buffers);
    ingestElapsed = performance.now() - started;
  }
  const stats = db.getStats();
  const recordCount = stats.reduce((sum, stat) => sum + stat.recordCount, 0);
  assert.strictEqual(recordCount, records.length, 'WASM path ingested record count mismatch');
  const verifyStarted = performance.now();
  assert.ok(db.exportData().length > 0, 'WASM path export should not be empty after ingest');
  const verifyElapsed = performance.now() - verifyStarted;
  db.destroy();
  return {
    totalElapsed: profileMode ? packElapsed + ingestElapsed : ingestElapsed,
    packElapsed,
    ingestElapsed,
    verifyElapsed,
    profile,
  };
}

async function run() {
  const flatSql = await initFlatSQL({ skipIntegrityCheck: true });
  const caseFilter = process.argv.filter((arg) => arg.startsWith('--case='));
  const selectedCases = caseFilter.length ? caseFilter.map((arg) => arg.split('=')[1]) : SCENARIOS.map((s) => s.id);
  const showProfile = process.argv.includes('--profile');
  let gateFailed = false;
  const summary = [];

  for (const scenario of SCENARIOS) {
    if (!selectedCases.includes(scenario.id)) {
      continue;
    }

    const records = buildRecords(scenario.rows, scenario.sorted);
    const schema = SCHEMA_TEMPLATE(scenario.indexes);
    const jsRuns = [];
    const wasmRuns = [];
    const wasmPackRuns = [];
    const wasmIngestRuns = [];
    const wasmVerifyRuns = [];
    const wasmDecodeRuns = [];
    const wasmAppendRuns = [];
    const wasmIndexRuns = [];

    for (let round = 0; round < 3; round++) {
      jsRuns.push(measureFlatSQLJS(records, schema));
      const wasmRun = await measureFlatSQLWASM(records, schema, flatSql, showProfile);
      wasmRuns.push(wasmRun.totalElapsed);
      if (showProfile && wasmRun.profile) {
        wasmPackRuns.push(wasmRun.packElapsed);
        wasmIngestRuns.push(wasmRun.ingestElapsed);
        wasmVerifyRuns.push(wasmRun.verifyElapsed);
        wasmDecodeRuns.push(wasmRun.profile.decodeNanos / 1e6);
        wasmAppendRuns.push(wasmRun.profile.appendNanos / 1e6);
        wasmIndexRuns.push(wasmRun.profile.indexNanos / 1e6);
      }
    }

    const jsMedian = median(jsRuns);
    const wasmMedian = median(wasmRuns);
    const ratio = jsMedian / wasmMedian;

    if (!showProfile && scenario.gate && ratio < scenario.gate) {
      gateFailed = true;
      console.error(`GATE FAILED: ${scenario.label} ratio ${ratio.toFixed(2)} < ${scenario.gate}`);
    }

    summary.push({
      scenario: scenario.label,
      jsMedian,
      wasmMedian,
      ratio,
      packMedian: showProfile ? median(wasmPackRuns) : 0,
      ingestMedian: showProfile ? median(wasmIngestRuns) : 0,
      verifyMedian: showProfile ? median(wasmVerifyRuns) : 0,
      decodeMedian: showProfile ? median(wasmDecodeRuns) : 0,
      appendMedian: showProfile ? median(wasmAppendRuns) : 0,
      indexMedian: showProfile ? median(wasmIndexRuns) : 0,
    });
  }

  console.table(
    summary.map((row) => ({
      Scenario: row.scenario,
      'FlatSQL JS (ms)': row.jsMedian.toFixed(2),
      'FlatSQL WASM (ms)': row.wasmMedian.toFixed(2),
      'JS / WASM': row.ratio.toFixed(2),
    })),
  );

  if (showProfile) {
    console.log('Profiling mode is diagnostic and includes instrumentation overhead; use `npm run bench:perf` for merge gates.');
    console.table(
      summary.map((row) => ({
        Scenario: row.scenario,
        'Pack (ms)': row.packMedian.toFixed(2),
        'Native Ingest (ms)': row.ingestMedian.toFixed(2),
        'Verify (ms)': row.verifyMedian.toFixed(2),
        'Decode (ms)': row.decodeMedian.toFixed(2),
        'Append (ms)': row.appendMedian.toFixed(2),
        'Index (ms)': row.indexMedian.toFixed(2),
      })),
    );
  }

  if (gateFailed) {
    process.exitCode = 1;
  }
}

await run();
