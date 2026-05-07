import { decodeSizePrefixedStream } from '../src/artifacts/transport.js';
import {
  createQueryResponseArtifact,
  createResponseCacheKey,
  getResponseArtifactChunk,
  hashString,
  MemoryResponseArtifactCache,
} from '../src/response/index.js';
import type { QueryResult } from '../src/index.js';

describe('response artifacts', () => {
  test('hashes strings with the SHA-256 standard vector', () => {
    expect(hashString('abc')).toBe(
      'sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
    );
  });

  test('creates deterministic cache keys from query shape and cache dimensions', () => {
    const left = createResponseCacheKey({
      schemaName: 'sds',
      schemaVersion: 7,
      sql: ' SELECT _data   FROM PNM WHERE FILE_ID = ? ',
      params: { FILE_ID: 'publish-1', projection: '_data' },
      format: 'raw-flatbuffer-stream',
      publishEventKey: 'publish-1',
      projection: ['_data'],
    });

    const right = createResponseCacheKey({
      schemaVersion: 7,
      schemaName: 'sds',
      sql: 'SELECT _data FROM PNM WHERE FILE_ID = ?',
      params: { projection: '_data', FILE_ID: 'publish-1' },
      projection: ['_data'],
      publishEventKey: 'publish-1',
      format: 'raw-flatbuffer-stream',
    });

    const jsonKey = createResponseCacheKey({
      schemaName: 'sds',
      schemaVersion: 7,
      sql: 'SELECT _data FROM PNM WHERE FILE_ID = ?',
      params: { FILE_ID: 'publish-1', projection: '_data' },
      format: 'json',
      publishEventKey: 'publish-1',
      projection: ['_data'],
    });

    expect(left).toBe(right);
    expect(left).toMatch(/^flatsql:v1:sha256:/);
    expect(jsonKey).not.toBe(left);
  });

  test('creates immutable chunked raw FlatBuffer response artifacts', () => {
    const result: QueryResult = {
      columns: ['_data'],
      rows: [
        [Uint8Array.from([1, 2, 3, 4])],
        [Uint8Array.from([5, 6, 7])],
        [Uint8Array.from([8, 9, 10, 11, 12])],
      ],
      rowCount: 3,
    };

    const artifact = createQueryResponseArtifact(result, {
      schemaName: 'sds',
      schemaVersion: 1,
      sql: "SELECT _data FROM PNM WHERE FILE_ID = 'publish-1'",
      publishEventKey: 'publish-1',
      format: 'raw-flatbuffer-stream',
      chunkBytes: 8,
      createdAt: '2026-05-07T00:00:00.000Z',
    });

    expect(artifact.metadata.format).toBe('raw-flatbuffer-stream');
    expect(artifact.metadata.rowCount).toBe(3);
    expect(artifact.metadata.byteLength).toBe(artifact.bytes.byteLength);
    expect(artifact.metadata.contentHash).toMatch(/^sha256:/);
    expect(artifact.metadata.etag).toBe(`"${artifact.metadata.contentHash}"`);
    expect(artifact.metadata.chunks.length).toBeGreaterThan(1);
    expect(artifact.metadata.chunks[0]).toEqual(expect.objectContaining({
      index: 0,
      offset: 0,
      byteLength: 8,
      contentHash: expect.stringMatching(/^sha256:/),
      etag: expect.stringMatching(/^"sha256:/),
    }));

    const reassembled = new Uint8Array(artifact.bytes.byteLength);
    let offset = 0;
    for (const chunk of artifact.metadata.chunks) {
      const bytes = getResponseArtifactChunk(artifact, chunk.index);
      reassembled.set(bytes, offset);
      offset += bytes.byteLength;
    }

    expect(reassembled).toEqual(artifact.bytes);
    expect(decodeSizePrefixedStream(artifact.bytes)).toEqual([
      Uint8Array.from([1, 2, 3, 4]),
      Uint8Array.from([5, 6, 7]),
      Uint8Array.from([8, 9, 10, 11, 12]),
    ]);
  });

  test('deduplicates repeated artifact creation through memory cache', () => {
    const result: QueryResult = {
      columns: ['id', 'name'],
      rows: [[1, 'alpha']],
      rowCount: 1,
    };
    const cache = new MemoryResponseArtifactCache();
    const keyInput = {
      schemaName: 'catalog',
      schemaVersion: 1,
      sql: 'SELECT id, name FROM Catalog WHERE FILE_ID = ?',
      params: ['publish-1'],
      format: 'json' as const,
      publishEventKey: 'publish-1',
    };
    let factoryCalls = 0;

    const first = cache.getOrCreate(keyInput, () => {
      factoryCalls += 1;
      return createQueryResponseArtifact(result, {
        ...keyInput,
        createdAt: '2026-05-07T00:00:00.000Z',
      });
    });
    const second = cache.getOrCreate(keyInput, () => {
      factoryCalls += 1;
      return createQueryResponseArtifact(result, {
        ...keyInput,
        createdAt: '2026-05-07T00:00:01.000Z',
      });
    });

    expect(factoryCalls).toBe(1);
    expect(second).toBe(first);
    expect(cache.get(first.metadata.cacheKey)).toBe(first);
    expect(first.metadata.byteLength).toBe(first.bytes.byteLength);
  });

  test('uses native response cache keys supplied by the WASM core', () => {
    const result: QueryResult = {
      columns: ['id'],
      rows: [[1]],
      rowCount: 1,
    };
    const nativeCacheKey =
      'flatsql:response:v1|s=504e4d|v=32|f=6a736f6e|e=504e4d2d6576656e742d31|q=53454c4543542069642046524f4d20504e4d2057484552452046494c455f4944203d203f|c=1:6964|p=1:s=504e4d7c3432';
    const cache = new MemoryResponseArtifactCache();
    let factoryCalls = 0;

    const first = cache.getOrCreateByKey(nativeCacheKey, () => {
      factoryCalls += 1;
      return createQueryResponseArtifact(result, {
        schemaName: 'PNM',
        schemaVersion: 2,
        sql: 'SELECT id FROM PNM WHERE FILE_ID = ?',
        params: ['PNM|42'],
        format: 'json',
        publishEventKey: 'PNM-event-1',
        projection: ['id'],
        cacheKey: nativeCacheKey,
      });
    });
    const second = cache.getOrCreateByKey(nativeCacheKey, () => {
      factoryCalls += 1;
      return createQueryResponseArtifact(result, {
        schemaName: 'PNM',
        schemaVersion: 2,
        sql: 'SELECT id FROM PNM WHERE FILE_ID = ?',
        params: ['PNM|42'],
        format: 'json',
        publishEventKey: 'PNM-event-1',
        projection: ['id'],
        cacheKey: nativeCacheKey,
      });
    });

    expect(first.metadata.cacheKey).toBe(nativeCacheKey);
    expect(second).toBe(first);
    expect(factoryCalls).toBe(1);
  });
});
