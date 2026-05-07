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

describe('standalone FlatSQL shim', () => {
  test('exposes native response artifact cache keys', async () => {
    const flatsql = await loadFlatSQLStandalone();

    const key = flatsql.buildResponseArtifactCacheKey(
      'PNM',
      '2',
      ' SELECT   *   FROM PNM WHERE FILE_ID = ? ',
      {
        format: 'raw',
        publishEventKey: 'PNM-event-1',
        projection: ['FILE_ID'],
        params: ['PNM|42'],
      }
    );

    expect(key).toBe(
      'flatsql:response:v1|s=504e4d|v=32|f=726177|e=504e4d2d6576656e742d31|q=53454c454354202a2046524f4d20504e4d2057484552452046494c455f4944203d203f|c=1:46494c455f4944|p=1:s=504e4d7c3432'
    );
    expect(
      flatsql.buildResponseArtifactCacheKey('PNM', '2', 'SELECT * FROM PNM WHERE FILE_ID = ?', {
        format: 'raw',
        publishEventKey: 'PNM-event-1',
        projection: ['FILE_ID'],
        params: ['PNM|42'],
      })
    ).toBe(key);
  });

  test('instantiates the standalone WASI artifact and uses the C++ query cache', async () => {
    const flatsql = await loadFlatSQLStandalone();
    const db = flatsql.createDatabase(USER_SCHEMA, 'standalone-cache-test');

    try {
      db.registerFileId('USER', 'User');
      db.enableDemoExtractors();
      db.configureQueryCache({ maxEntries: 16, maxRows: 8 });
      db.ingestBuffers([
        flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
        flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
      ]);
      db.registerQueryTemplate('userById', 'SELECT email FROM User WHERE id = ?', true);

      expect(db.queryTemplate('userById', [2])).toEqual({
        columns: ['email'],
        rows: [['bob@example.com']],
      });
      expect(db.getQueryCacheStats()).toMatchObject({
        hits: 0,
        misses: 1,
        size: 1,
        maxEntries: 16,
        maxRows: 8,
      });

      expect(db.queryTemplate('userById', [2])).toEqual({
        columns: ['email'],
        rows: [['bob@example.com']],
      });
      expect(db.getQueryCacheStats()).toMatchObject({
        hits: 1,
        misses: 1,
        size: 1,
      });

      const stream = db.queryRawFlatBufferStream('SELECT _data FROM User WHERE email = ?', ['bob@example.com']);
      expect(decodeSizePrefixedStream(stream)).toEqual([
        flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
      ]);
    } finally {
      db.destroy();
    }
  });

  test('uses native PublishEventRecord extractors for FILE_ID cache and raw stream metrics', async () => {
    const flatsql = await loadFlatSQLStandalone();
    const db = flatsql.createDatabase(PUBLISH_EVENT_SCHEMA, 'standalone-publish-event-test');
    const first = flatsql.createTestPublishEvent('publish-1', 'record-1', 1, 128);
    const second = flatsql.createTestPublishEvent('publish-2', 'record-2', 2, 256);

    try {
      db.registerFileId('PUBL', 'PublishEventRecord');
      db.enableDemoExtractors();
      db.ingestBuffers([first, second]);
      db.registerQueryTemplate(
        'publishByFileId',
        'SELECT RECORD_ID, PAYLOAD_SIZE FROM PublishEventRecord WHERE FILE_ID = ?',
        true
      );

      expect(db.queryTemplate('publishByFileId', ['publish-2'])).toEqual({
        columns: ['RECORD_ID', 'PAYLOAD_SIZE'],
        rows: [['record-2', 256]],
      });
      expect(db.queryTemplate('publishByFileId', ['publish-2'])).toEqual({
        columns: ['RECORD_ID', 'PAYLOAD_SIZE'],
        rows: [['record-2', 256]],
      });
      expect(db.getQueryCacheStats()).toMatchObject({ hits: 1, misses: 1, size: 1 });

      expect(decodeSizePrefixedStream(db.queryRawFlatBufferStream(
        'SELECT _data FROM PublishEventRecord WHERE FILE_ID = ?',
        ['publish-1']
      ))).toEqual([first]);
      expect(db.getStorageInfo().size).toBeGreaterThanOrEqual(first.length + second.length);
    } finally {
      db.destroy();
    }
  });

  test('rebuilds a standalone database directly from another database storage buffer', async () => {
    const flatsql = await loadFlatSQLStandalone();
    const source = flatsql.createDatabase(PUBLISH_EVENT_SCHEMA, 'standalone-source-rebuild-test');
    const target = flatsql.createDatabase(PUBLISH_EVENT_SCHEMA, 'standalone-target-rebuild-test');
    const records = [
      flatsql.createTestPublishEvent('publish-direct-rebuild-1', 'record-1', 1, 128),
      flatsql.createTestPublishEvent('publish-direct-rebuild-2', 'record-2', 2, 256),
    ];

    try {
      source.registerFileId('PUBL', 'PublishEventRecord');
      source.enableDemoExtractors();
      target.registerFileId('PUBL', 'PublishEventRecord');
      target.enableDemoExtractors();
      source.ingestBuffers(records);

      (target as unknown as {
        reserveStorageBytes(bytes: number): void;
        loadAndRebuildFrom(sourceDb: typeof source): void;
      }).reserveStorageBytes(source.getStorageInfo().size);
      (target as unknown as {
        loadAndRebuildFrom(sourceDb: typeof source): void;
      }).loadAndRebuildFrom(source);

      expect(target.query('SELECT RECORD_ID FROM PublishEventRecord WHERE FILE_ID = ?', ['publish-direct-rebuild-2'])).toEqual({
        columns: ['RECORD_ID'],
        rows: [['record-2']],
      });
      expect(target.getStorageInfo().size).toBe(source.getStorageInfo().size);
    } finally {
      source.destroy();
      target.destroy();
    }
  });

  test('exports growable standalone memory for full-node stress storage', async () => {
    const flatsql = await loadFlatSQLStandalone();
    const initialPages = flatsql.memory.grow(1);
    expect(initialPages).toBeGreaterThan(0);

    const db = flatsql.createDatabase(PUBLISH_EVENT_SCHEMA, 'standalone-large-storage-test');
    const records = 380;
    const payloadBytes = 51_283;
    const batchTargetBytes = 1024 * 1024;
    let batchBytes = 0;
    let batch: Uint8Array[] = [];
    let totalBytes = 0;

    try {
      (db as unknown as { reserveStorageBytes(bytes: number): void }).reserveStorageBytes(24 * 1024 * 1024);
      db.registerFileId('PUBL', 'PublishEventRecord');
      db.enableDemoExtractors();

      for (let index = 0; index < records; index++) {
        const record = flatsql.createTestPublishEvent(
          index < 4 ? 'standalone-large-hot' : `standalone-large-cold-${index}`,
          `record-${index}`,
          index,
          payloadBytes
        );
        batch.push(record);
        batchBytes += record.length + 4;
        totalBytes += record.length + 4;

        if (batchBytes >= batchTargetBytes) {
          db.ingestBuffers(batch);
          batch = [];
          batchBytes = 0;
        }
      }
      if (batch.length > 0) {
        db.ingestBuffers(batch);
      }

      expect(totalBytes).toBeGreaterThan(18 * 1024 * 1024);
      expect(db.getStorageInfo().size).toBeGreaterThan(18 * 1024 * 1024);
      expect(flatsql.memory.buffer.byteLength).toBeGreaterThan(64 * 1024 * 1024);
    } finally {
      db.destroy();
    }
  });
});
