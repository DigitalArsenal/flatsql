// Query-path regressions on the shipped artifacts (graph task
// flatsql-query-regressions-20260928). Outcomes are computed, never timed:
// rows returned, and what the virtual table did, read from the engine's
// flatsql_scan_stats() counters. cpp/test/query_regression_test.cpp covers the
// same engine natively.
import initFlatSQL from '../wasm/index.js';
import { loadFlatSQLStandalone } from './support/legacy-standalone.js';

const SCHEMA = `
table PublishEventRecord {
  FILE_ID: string (key);
  RECORD_ID: string (index);
  EVENT_INDEX: int (index);
  PAYLOAD_SIZE: int;
}

root_type PublishEventRecord;
`;

const RECORDS = 5000;
const HOT = 4500;

type Stats = {
  fullScans: number;
  rowidLookups: number;
  indexEqualityScans: number;
  indexRangeScans: number;
  indexEntriesRead: number;
  rowsVisited: number;
};

type Db = {
  query(sql: string, params?: unknown[]): { columns: string[]; rows: unknown[][] };
  registerQueryTemplate(id: string, sql: string, cacheable?: boolean): void;
  queryTemplate(id: string, params?: unknown[]): { columns: string[]; rows: unknown[][] };
  getQueryCacheStats(): { hits: number; misses: number; size: number };
  destroy(): void;
};

function stats(db: Db): Stats {
  return JSON.parse(String(db.query('SELECT flatsql_scan_stats()').rows[0][0])) as Stats;
}

function measured(db: Db, sql: string, params: unknown[]) {
  const before = stats(db);
  const result = db.query(sql, params);
  const after = stats(db);
  const work = Object.fromEntries(
    Object.keys(after).map((key) => [key, after[key as keyof Stats] - before[key as keyof Stats]])
  ) as Stats;
  return { rows: result.rows, work };
}

async function standaloneDatabase(): Promise<{ db: Db; bytesOf: (i: number) => Uint8Array }> {
  const flatsql = await loadFlatSQLStandalone();
  const db = flatsql.createDatabase(SCHEMA, 'query-regressions');
  db.registerFileId('PUBL', 'PublishEventRecord');
  db.enableDemoExtractors();
  const buffers: Uint8Array[] = [];
  for (let i = 0; i < RECORDS; i++) {
    buffers.push(
      flatsql.createTestPublishEvent(i < HOT ? 'hot' : `cold-${i}`, `record-${i}`, i, 96 + (i % 7) * 32)
    );
  }
  // A FILE_ID with the same text as an existing RECORD_ID.
  buffers.push(flatsql.createTestPublishEvent('record-5', 'decoy', 100000, 96));
  db.ingestBuffers(buffers);
  return { db: db as unknown as Db, bytesOf: (i) => buffers[i] };
}

describe('query regressions (standalone WASI artifact)', () => {
  test('indexed point lookup visits one record', async () => {
    const { db } = await standaloneDatabase();
    try {
      const { rows, work } = measured(
        db,
        'SELECT FILE_ID, RECORD_ID FROM PublishEventRecord WHERE RECORD_ID = ?',
        ['record-4321']
      );
      expect(rows).toEqual([['hot', 'record-4321']]);
      expect(work).toMatchObject({ fullScans: 0, indexEqualityScans: 1, rowsVisited: 1 });
    } finally {
      db.destroy();
    }
  });

  test('LIMIT 1 over a key with 4500 entries reads one index entry', async () => {
    const { db } = await standaloneDatabase();
    try {
      const { rows, work } = measured(
        db,
        'SELECT RECORD_ID FROM PublishEventRecord WHERE FILE_ID = ? LIMIT 1',
        ['hot']
      );
      expect(rows).toEqual([['record-0']]);
      expect(work).toMatchObject({ fullScans: 0, indexEqualityScans: 1, indexEntriesRead: 1, rowsVisited: 1 });
    } finally {
      db.destroy();
    }
  });

  test('a range reads only its keys', async () => {
    const { db } = await standaloneDatabase();
    try {
      const between = measured(
        db,
        'SELECT RECORD_ID FROM PublishEventRecord WHERE EVENT_INDEX BETWEEN 0 AND ?',
        [3]
      );
      expect(between.rows).toEqual([['record-0'], ['record-1'], ['record-2'], ['record-3']]);
      expect(between.work).toMatchObject({ fullScans: 0, indexRangeScans: 1, indexEntriesRead: 4, rowsVisited: 4 });

      const open = measured(db, 'SELECT EVENT_INDEX FROM PublishEventRecord WHERE EVENT_INDEX > ?', [4998]);
      expect(open.rows).toEqual([[4999], [100000]]);
      expect(open.work).toMatchObject({ fullScans: 0, indexRangeScans: 1, indexEntriesRead: 2 });
    } finally {
      db.destroy();
    }
  });

  test('several indexed terms return the right rows', async () => {
    const { db } = await standaloneDatabase();
    try {
      // The old planner looked "record-5" up in the FILE_ID index, dropped
      // both terms and returned the decoy.
      expect(
        db.query("SELECT RECORD_ID FROM PublishEventRecord WHERE RECORD_ID = 'record-5' AND FILE_ID = 'hot'").rows
      ).toEqual([['record-5']]);
      expect(
        db.query('SELECT RECORD_ID FROM PublishEventRecord WHERE FILE_ID = ? AND RECORD_ID = ?', ['hot', 'record-5'])
          .rows
      ).toEqual([['record-5']]);
      expect(
        db.query("SELECT RECORD_ID FROM PublishEventRecord WHERE RECORD_ID = 'RECORD-77' COLLATE NOCASE").rows
      ).toEqual([['record-77']]);
    } finally {
      db.destroy();
    }
  });

  test('a large hot result stays in the query cache', async () => {
    const { db, bytesOf } = await standaloneDatabase();
    try {
      db.registerQueryTemplate('hot', 'SELECT _data FROM PublishEventRecord WHERE FILE_ID = ?', true);
      const before = db.getQueryCacheStats();
      let result = db.queryTemplate('hot', ['hot']);
      for (let i = 0; i < 3; i++) {
        result = db.queryTemplate('hot', ['hot']);
      }
      const after = db.getQueryCacheStats();
      expect(result.rows).toHaveLength(HOT);
      expect(after.misses - before.misses).toBe(1);
      expect(after.hits - before.hits).toBe(3);
      expect(after.size).toBe(1);

      // BLOB cells stay plain arrays of byte values, byte for byte.
      const first = result.rows[0][0];
      expect(Array.isArray(first)).toBe(true);
      expect(first).toEqual(Array.from(bytesOf(0)));
    } finally {
      db.destroy();
    }
  });
});

describe('query regressions (browser artifact)', () => {
  test('BLOB cells are plain byte arrays and LIMIT stops early', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(SCHEMA, 'query-regressions-browser');
    try {
      db.registerFileId('PUBL', 'PublishEventRecord');
      db.enableDemoExtractors();
      const buffers: Uint8Array[] = [];
      for (let i = 0; i < 200; i++) {
        buffers.push(flatsql.createTestPublishEvent('hot', `record-${i}`, i, 96));
      }
      db.ingestBuffers(buffers);

      const data = db.query('SELECT _data FROM PublishEventRecord WHERE RECORD_ID = ?', ['record-7']);
      expect(Array.isArray(data.rows[0][0])).toBe(true);
      expect(data.rows[0][0]).toEqual(Array.from(buffers[7]));

      const before = stats(db as unknown as Db);
      const limited = db.query('SELECT RECORD_ID FROM PublishEventRecord WHERE FILE_ID = ? LIMIT 1', ['hot']);
      const after = stats(db as unknown as Db);
      expect(limited.rows).toEqual([['record-0']]);
      expect(after.indexEntriesRead - before.indexEntriesRead).toBe(1);
    } finally {
      db.destroy();
    }
  });
});
