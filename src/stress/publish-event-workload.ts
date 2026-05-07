import { performance } from 'node:perf_hooks';
import { Builder, ByteBuffer } from 'flatbuffers';
import { DirectAccessor, FlatSQLDatabase, type QueryResult } from '../index.js';
import {
  createQueryResponseArtifact,
  createResponseCacheKey,
  MemoryResponseArtifactCache,
} from '../response/index.js';
import type { MetricEvent, StressUseCase, WorkloadManifest } from './types.js';

const TABLE_NAME = 'PublishEventRecord';
const TEXT_ENCODER = new TextEncoder();
const COLD_FANOUT_QUERIES_PER_CONCURRENCY = 256;

const PUBLISH_EVENT_SCHEMA = `
table PublishEventRecord {
  FILE_ID: string;
  RECORD_ID: string;
  EVENT_INDEX: int;
  PAYLOAD_SIZE: int;
  PAYLOAD: [ubyte];
}

root_type PublishEventRecord;
`;

interface FixtureRecord {
  fileId: string;
  recordId: string;
  eventIndex: number;
  buffer: Uint8Array;
}

interface PublishEventFixture {
  db: FlatSQLDatabase;
  records: FixtureRecord[];
  buffers: Uint8Array[];
  hotFileId: string;
  coldFileIds: string[];
  flatbufferBytes: number;
  ingestDurationMs: number;
}

interface CachedQueryResult {
  result: QueryResult;
  hit: boolean;
  durationMs: number;
}

type QueryCache = Map<string, QueryResult>;

function bytes(value: string): number {
  return TEXT_ENCODER.encode(value).length;
}

function measuredDuration(start: number): number {
  return Number(Math.max(performance.now() - start, 0.001).toFixed(3));
}

function createPayload(nodeId: number, recordIndex: number): Uint8Array {
  const size = 96 + ((nodeId + recordIndex) % 7) * 32;
  const payload = new Uint8Array(size);
  for (let index = 0; index < payload.length; index++) {
    payload[index] = (nodeId * 31 + recordIndex * 17 + index) % 256;
  }
  return payload;
}

function buildPublishEventBuffer(fileId: string, recordId: string, eventIndex: number, payload: Uint8Array): Uint8Array {
  const builder = new Builder(256 + payload.length);
  const fileIdOffset = builder.createString(fileId);
  const recordIdOffset = builder.createString(recordId);
  const payloadOffset = builder.createByteVector(payload);

  builder.startObject(5);
  builder.addFieldOffset(0, fileIdOffset, 0);
  builder.addFieldOffset(1, recordIdOffset, 0);
  builder.addFieldInt32(2, eventIndex, 0);
  builder.addFieldInt32(3, payload.length, 0);
  builder.addFieldOffset(4, payloadOffset, 0);
  const recordOffset = builder.endObject();
  builder.finish(recordOffset);

  return builder.asUint8Array().slice();
}

function rootPosition(buffer: Uint8Array): { bb: ByteBuffer; root: number } {
  const bb = new ByteBuffer(buffer);
  const root = bb.readInt32(bb.position()) + bb.position();
  return { bb, root };
}

function readStringField(buffer: Uint8Array, vtableOffset: number): string | undefined {
  const { bb, root } = rootPosition(buffer);
  const offset = bb.__offset(root, vtableOffset);
  return offset === 0 ? undefined : bb.__string(root + offset) as string;
}

function readIntField(buffer: Uint8Array, vtableOffset: number): number | undefined {
  const { bb, root } = rootPosition(buffer);
  const offset = bb.__offset(root, vtableOffset);
  return offset === 0 ? undefined : bb.readInt32(root + offset);
}

function createAccessor(): DirectAccessor {
  const accessor = new DirectAccessor();
  accessor.registerAccessor(TABLE_NAME, (data, path) => {
    const field = path[0];
    if (field === 'FILE_ID') return readStringField(data, 4);
    if (field === 'RECORD_ID') return readStringField(data, 6);
    if (field === 'EVENT_INDEX') return readIntField(data, 8);
    if (field === 'PAYLOAD_SIZE') return readIntField(data, 10);
    return undefined;
  });
  accessor.registerBuilder(TABLE_NAME, (fields) => buildPublishEventBuffer(
    String(fields.FILE_ID),
    String(fields.RECORD_ID),
    Number(fields.EVENT_INDEX),
    createPayload(0, Number(fields.EVENT_INDEX))
  ));
  return accessor;
}

function createDatabase(): FlatSQLDatabase {
  const db = FlatSQLDatabase.fromSchema(PUBLISH_EVENT_SCHEMA, createAccessor(), 'sdn-publish-event');
  db.createIndex(TABLE_NAME, 'FILE_ID');
  db.createIndex(TABLE_NAME, 'RECORD_ID');
  db.createIndex(TABLE_NAME, 'EVENT_INDEX');
  return db;
}

function buildFixture(manifest: WorkloadManifest, nodeId: number): PublishEventFixture {
  const db = createDatabase();
  const recordCount = Math.max(1, manifest.recordsPerNode);
  const hotRecordCount = Math.max(1, Math.ceil(recordCount * manifest.hotQueryRatio));
  const hotFileId = `publish-${manifest.runId}-${nodeId}-hot`;
  const records: FixtureRecord[] = [];
  const coldFileIds: string[] = [];

  for (let index = 0; index < recordCount; index++) {
    const isHot = index < hotRecordCount;
    const fileId = isHot ? hotFileId : `publish-${manifest.runId}-${nodeId}-cold-${index}`;
    if (!isHot) {
      coldFileIds.push(fileId);
    }

    const recordId = `record-${nodeId}-${index}`;
    const buffer = buildPublishEventBuffer(fileId, recordId, index, createPayload(nodeId, index));
    records.push({ fileId, recordId, eventIndex: index, buffer });
  }

  const buffers = records.map((record) => record.buffer);
  const start = performance.now();
  db.stream(TABLE_NAME, buffers);

  return {
    db,
    records,
    buffers,
    hotFileId,
    coldFileIds,
    flatbufferBytes: buffers.reduce((total, buffer) => total + buffer.length, 0),
    ingestDurationMs: measuredDuration(start),
  };
}

function queryCache(cache: QueryCache, db: FlatSQLDatabase, sql: string): CachedQueryResult {
  const start = performance.now();
  const cached = cache.get(sql);
  if (cached) {
    return {
      result: cached,
      hit: true,
      durationMs: measuredDuration(start),
    };
  }

  const result = db.query(sql);
  cache.set(sql, result);
  return {
    result,
    hit: false,
    durationMs: measuredDuration(start),
  };
}

function resultCellBytes(cell: unknown): number {
  if (cell instanceof Uint8Array) {
    return cell.length;
  }
  if (typeof cell === 'bigint') {
    return bytes(cell.toString());
  }
  if (cell === null || cell === undefined) {
    return 0;
  }
  return bytes(String(cell));
}

function queryResultBytes(result: QueryResult): number {
  const columnBytes = result.columns.reduce((total, column) => total + bytes(column), 0);
  const rowBytes = result.rows.reduce(
    (rowTotal, row) => rowTotal + row.reduce((cellTotal, cell) => cellTotal + resultCellBytes(cell), 0),
    0
  );
  return columnBytes + rowBytes;
}

function rawFlatbufferBytes(result: QueryResult): number {
  return result.rows.reduce((total, row) => total + row.reduce((cellTotal, cell) => {
    return cellTotal + (cell instanceof Uint8Array ? cell.length : 0);
  }, 0), 0);
}

function requestBytes(sql: string): number {
  return bytes(sql) + 16;
}

function baseMetric(
  manifest: WorkloadManifest,
  useCase: StressUseCase,
  nodeId: number,
  values: Omit<MetricEvent, 'runId' | 'nodeId' | 'schema' | 'useCase' | 'operation' | 'measurement' | 'transport'>,
  schema = TABLE_NAME
): MetricEvent {
  return {
    runId: manifest.runId,
    nodeId,
    schema,
    useCase: useCase.id,
    operation: useCase.operation,
    measurement: 'measured',
    transport: manifest.transport,
    runtime: 'typescript',
    ...values,
  };
}

function metricFromQuery(
  manifest: WorkloadManifest,
  useCase: StressUseCase,
  nodeId: number,
  fixture: PublishEventFixture,
  sql: string,
  result: QueryResult,
  durationMs: number,
  cacheHits = 0,
  cacheMisses = 0,
  cacheSize = 0
): MetricEvent {
  const flatbufferBytes = rawFlatbufferBytes(result);
  const responseBytes = useCase.id === 'raw-flatbuffer-retrieval'
    ? flatbufferBytes
    : queryResultBytes(result);
  return baseMetric(manifest, useCase, nodeId, {
    records: result.rowCount,
    requestBytes: requestBytes(sql),
    responseBytes,
    flatbufferBytes,
    storageBytes: fixture.db.getStorageBytes(),
    durationMs,
    cacheHits,
    cacheMisses,
    cacheSize,
    errors: 0,
  });
}

function findUseCase(manifest: WorkloadManifest, id: string): StressUseCase {
  const useCase = manifest.useCases.find((candidate) => candidate.id === id);
  if (!useCase) {
    throw new Error(`Stress use case not found: ${id}`);
  }
  return useCase;
}

function buildSchemaSweepMetrics(manifest: WorkloadManifest, assignmentSchemas: readonly string[], nodeId: number): MetricEvent[] {
  const useCase = findUseCase(manifest, 'schema-sweep');
  const schemaByName = new Map(manifest.schemas.map((schema) => [schema.name, schema]));

  return assignmentSchemas.map((schemaName) => {
    const schema = schemaByName.get(schemaName);
    const request = schema ? bytes(schema.path) : bytes(schemaName);
    const response = schema ? bytes(schema.name) + bytes(schema.rootType) : bytes(schemaName);

    return baseMetric(manifest, useCase, nodeId, {
      records: 1,
      requestBytes: request,
      responseBytes: response,
      flatbufferBytes: 0,
      storageBytes: 0,
      durationMs: 0.001,
      cacheHits: 0,
      cacheMisses: 0,
      cacheSize: 0,
      errors: 0,
    }, schemaName);
  });
}

function buildHotCacheMetric(manifest: WorkloadManifest, fixture: PublishEventFixture, nodeId: number): MetricEvent {
  const useCase = findUseCase(manifest, 'hot-file-id-cache');
  const sql = `SELECT _data FROM ${TABLE_NAME} WHERE FILE_ID = '${fixture.hotFileId}'`;
  const artifactCache = new MemoryResponseArtifactCache();
  const cacheKeyInput = {
    schemaName: TABLE_NAME,
    schemaVersion: 1,
    sql,
    format: 'raw-flatbuffer-stream' as const,
    publishEventKey: fixture.hotFileId,
    projection: ['_data'],
  };
  const cacheKey = createResponseCacheKey(cacheKeyInput);
  let result: QueryResult | undefined;
  let responseBytes = 0;
  let flatbufferBytes = 0;
  let cacheHits = 0;
  let cacheMisses = 0;
  let durationMs = 0;
  const repetitions = Math.max(4, Math.min(32, manifest.queryConcurrency));

  for (let index = 0; index < repetitions; index++) {
    const start = performance.now();
    const cached = artifactCache.get(cacheKey);
    if (cached) {
      responseBytes += cached.metadata.byteLength;
      durationMs += measuredDuration(start);
      cacheHits += 1;
      continue;
    }

    result = fixture.db.query(sql);
    const artifact = createQueryResponseArtifact(result, cacheKeyInput);
    artifactCache.set(artifact);
    responseBytes += artifact.metadata.byteLength;
    flatbufferBytes = rawFlatbufferBytes(result);
    durationMs += measuredDuration(start);
    cacheMisses += 1;
  }

  return baseMetric(manifest, useCase, nodeId, {
    records: result!.rowCount * repetitions,
    requestBytes: requestBytes(sql) * repetitions,
    responseBytes,
    flatbufferBytes: flatbufferBytes * repetitions,
    storageBytes: fixture.db.getStorageBytes(),
    durationMs: Number(durationMs.toFixed(3)),
    cacheHits,
    cacheMisses,
    cacheSize: artifactCache.size,
    errors: 0,
  });
}

function buildColdFanoutMetric(manifest: WorkloadManifest, fixture: PublishEventFixture, nodeId: number): MetricEvent {
  const useCase = findUseCase(manifest, 'cold-file-id-fanout');
  const coldFileIds = fixture.coldFileIds.length > 0
    ? fixture.coldFileIds
    : fixture.records.slice(0, 1).map((record) => record.fileId);
  const queriedFileIds = coldFileIds.slice(
    0,
    Math.max(1, manifest.queryConcurrency * COLD_FANOUT_QUERIES_PER_CONCURRENCY)
  );
  const cache: QueryCache = new Map();
  let records = 0;
  let requestTotal = 0;
  let responseTotal = 0;
  let flatbufferTotal = 0;
  let durationMs = 0;

  for (const fileId of queriedFileIds) {
    const sql = `SELECT RECORD_ID, EVENT_INDEX FROM ${TABLE_NAME} WHERE FILE_ID = '${fileId}'`;
    const query = queryCache(cache, fixture.db, sql);
    records += query.result.rowCount;
    requestTotal += requestBytes(sql);
    responseTotal += queryResultBytes(query.result);
    flatbufferTotal += rawFlatbufferBytes(query.result);
    durationMs += query.durationMs;
  }

  return baseMetric(manifest, useCase, nodeId, {
    records,
    requestBytes: requestTotal,
    responseBytes: responseTotal,
    flatbufferBytes: flatbufferTotal,
    storageBytes: fixture.db.getStorageBytes(),
    durationMs: Number(durationMs.toFixed(3)),
    cacheHits: 0,
    cacheMisses: queriedFileIds.length,
    cacheSize: cache.size,
    errors: 0,
  });
}

function buildMixedCacheMetric(manifest: WorkloadManifest, fixture: PublishEventFixture, nodeId: number): MetricEvent {
  const useCase = findUseCase(manifest, 'mixed-hot-cold-query');
  const cache: QueryCache = new Map();
  const cold = fixture.coldFileIds[0] ?? fixture.hotFileId;
  const queries = [
    fixture.hotFileId,
    fixture.hotFileId,
    cold,
    fixture.hotFileId,
    cold,
    fixture.hotFileId,
  ];
  let records = 0;
  let requestTotal = 0;
  let responseTotal = 0;
  let flatbufferTotal = 0;
  let durationMs = 0;
  let cacheHits = 0;
  let cacheMisses = 0;

  for (const fileId of queries) {
    const sql = `SELECT RECORD_ID, PAYLOAD_SIZE FROM ${TABLE_NAME} WHERE FILE_ID = '${fileId}'`;
    const query = queryCache(cache, fixture.db, sql);
    records += query.result.rowCount;
    requestTotal += requestBytes(sql);
    responseTotal += queryResultBytes(query.result);
    flatbufferTotal += rawFlatbufferBytes(query.result);
    durationMs += query.durationMs;
    cacheHits += query.hit ? 1 : 0;
    cacheMisses += query.hit ? 0 : 1;
  }

  return baseMetric(manifest, useCase, nodeId, {
    records,
    requestBytes: requestTotal,
    responseBytes: responseTotal,
    flatbufferBytes: flatbufferTotal,
    storageBytes: fixture.db.getStorageBytes(),
    durationMs: Number(durationMs.toFixed(3)),
    cacheHits,
    cacheMisses,
    cacheSize: cache.size,
    errors: 0,
  });
}

function buildBackfillLikeMetric(
  manifest: WorkloadManifest,
  fixture: PublishEventFixture,
  nodeId: number,
  useCaseId: 'backfill-sync' | 'restart-rebuild'
): MetricEvent {
  const useCase = findUseCase(manifest, useCaseId);
  const db = createDatabase();
  const start = performance.now();
  db.stream(TABLE_NAME, fixture.buffers);
  const durationMs = measuredDuration(start);
  return baseMetric(manifest, useCase, nodeId, {
    records: fixture.buffers.length,
    requestBytes: fixture.flatbufferBytes,
    responseBytes: 64,
    flatbufferBytes: fixture.flatbufferBytes,
    storageBytes: db.getStorageBytes(),
    durationMs,
    cacheHits: 0,
    cacheMisses: 0,
    cacheSize: 0,
    errors: 0,
  });
}

function buildCacheInvalidationMetric(manifest: WorkloadManifest, fixture: PublishEventFixture, nodeId: number): MetricEvent {
  const useCase = findUseCase(manifest, 'cache-invalidation-churn');
  const sql = `SELECT RECORD_ID FROM ${TABLE_NAME} WHERE FILE_ID = '${fixture.hotFileId}'`;
  const cache: QueryCache = new Map();
  const first = queryCache(cache, fixture.db, sql);
  const second = queryCache(cache, fixture.db, sql);
  const churnBuffer = buildPublishEventBuffer(
    fixture.hotFileId,
    `record-${nodeId}-churn`,
    fixture.records.length + 1,
    createPayload(nodeId, fixture.records.length + 1)
  );
  const ingestStart = performance.now();
  fixture.db.insertRaw(TABLE_NAME, churnBuffer);
  const ingestDurationMs = measuredDuration(ingestStart);
  cache.clear();
  const third = queryCache(cache, fixture.db, sql);

  const resultBytes = queryResultBytes(first.result) + queryResultBytes(second.result) + queryResultBytes(third.result);
  return baseMetric(manifest, useCase, nodeId, {
    records: first.result.rowCount + second.result.rowCount + third.result.rowCount + 1,
    requestBytes: requestBytes(sql) * 3 + churnBuffer.length,
    responseBytes: resultBytes,
    flatbufferBytes: churnBuffer.length,
    storageBytes: fixture.db.getStorageBytes(),
    durationMs: Number((first.durationMs + second.durationMs + ingestDurationMs + third.durationMs).toFixed(3)),
    cacheHits: 1,
    cacheMisses: 2,
    cacheSize: cache.size,
    errors: 0,
  });
}

function buildBandwidthStreamingMetric(manifest: WorkloadManifest, fixture: PublishEventFixture, nodeId: number): MetricEvent {
  const useCase = findUseCase(manifest, 'bandwidth-constrained-streaming');
  const db = createDatabase();
  const start = performance.now();
  for (const buffer of fixture.buffers) {
    db.stream(TABLE_NAME, [buffer]);
  }
  const durationMs = measuredDuration(start);
  return baseMetric(manifest, useCase, nodeId, {
    records: fixture.buffers.length,
    requestBytes: fixture.flatbufferBytes + fixture.buffers.length * 4,
    responseBytes: fixture.buffers.length * 8,
    flatbufferBytes: fixture.flatbufferBytes,
    storageBytes: db.getStorageBytes(),
    durationMs,
    cacheHits: 0,
    cacheMisses: 0,
    cacheSize: 0,
    errors: 0,
  });
}

export function buildMeasuredPublishEventMetrics(manifest: WorkloadManifest): MetricEvent[] {
  const metrics: MetricEvent[] = [];

  for (const assignment of manifest.assignments) {
    const fixture = buildFixture(manifest, assignment.nodeId);

    metrics.push(...buildSchemaSweepMetrics(manifest, assignment.schemas, assignment.nodeId));

    metrics.push(baseMetric(manifest, findUseCase(manifest, 'bulk-streaming-ingest'), assignment.nodeId, {
      records: fixture.buffers.length,
      requestBytes: fixture.flatbufferBytes + fixture.buffers.length * 4,
      responseBytes: fixture.buffers.length * 8,
      flatbufferBytes: fixture.flatbufferBytes,
      storageBytes: fixture.db.getStorageBytes(),
      durationMs: fixture.ingestDurationMs,
      cacheHits: 0,
      cacheMisses: 0,
      cacheSize: 0,
      errors: 0,
    }));

    metrics.push(buildHotCacheMetric(manifest, fixture, assignment.nodeId));
    metrics.push(buildColdFanoutMetric(manifest, fixture, assignment.nodeId));
    metrics.push(buildMixedCacheMetric(manifest, fixture, assignment.nodeId));

    const rawSql = `SELECT _data FROM ${TABLE_NAME} WHERE FILE_ID = '${fixture.hotFileId}'`;
    const rawStart = performance.now();
    const rawResult = fixture.db.query(rawSql);
    metrics.push(metricFromQuery(
      manifest,
      findUseCase(manifest, 'raw-flatbuffer-retrieval'),
      assignment.nodeId,
      fixture,
      rawSql,
      rawResult,
      measuredDuration(rawStart)
    ));

    const projectionSql = `SELECT RECORD_ID, EVENT_INDEX, PAYLOAD_SIZE FROM ${TABLE_NAME} WHERE FILE_ID = '${fixture.hotFileId}'`;
    const projectionStart = performance.now();
    const projectionResult = fixture.db.query(projectionSql);
    metrics.push(metricFromQuery(
      manifest,
      findUseCase(manifest, 'sql-projection-query'),
      assignment.nodeId,
      fixture,
      projectionSql,
      projectionResult,
      measuredDuration(projectionStart)
    ));

    const rawQuerySql = `SELECT RECORD_ID FROM ${TABLE_NAME} WHERE EVENT_INDEX BETWEEN 0 AND ${Math.min(3, fixture.records.length - 1)}`;
    const rawQueryStart = performance.now();
    const rawQueryResult = fixture.db.query(rawQuerySql);
    metrics.push(metricFromQuery(
      manifest,
      findUseCase(manifest, 'raw-sql-query'),
      assignment.nodeId,
      fixture,
      rawQuerySql,
      rawQueryResult,
      measuredDuration(rawQueryStart)
    ));

    const largeResultSql = `SELECT RECORD_ID, FILE_ID FROM ${TABLE_NAME} LIMIT 5000`;
    const largeResultStart = performance.now();
    const largeResult = fixture.db.query(largeResultSql);
    metrics.push(metricFromQuery(
      manifest,
      findUseCase(manifest, 'large-result-query'),
      assignment.nodeId,
      fixture,
      largeResultSql,
      largeResult,
      measuredDuration(largeResultStart)
    ));

    const fanoutSql = `SELECT RECORD_ID FROM ${TABLE_NAME} WHERE FILE_ID = '${fixture.hotFileId}'`;
    const fanoutStart = performance.now();
    const fanoutResult = fixture.db.query(fanoutSql);
    metrics.push(metricFromQuery(
      manifest,
      findUseCase(manifest, 'node-fanout-query'),
      assignment.nodeId,
      fixture,
      fanoutSql,
      fanoutResult,
      measuredDuration(fanoutStart)
    ));

    metrics.push(buildBackfillLikeMetric(manifest, fixture, assignment.nodeId, 'backfill-sync'));
    metrics.push(buildBackfillLikeMetric(manifest, fixture, assignment.nodeId, 'restart-rebuild'));
    metrics.push(buildCacheInvalidationMetric(manifest, fixture, assignment.nodeId));
    metrics.push(buildBandwidthStreamingMetric(manifest, fixture, assignment.nodeId));

    const domainSql = `SELECT FILE_ID, RECORD_ID, PAYLOAD_SIZE FROM ${TABLE_NAME} WHERE RECORD_ID = '${fixture.records[0].recordId}'`;
    const domainStart = performance.now();
    const domainResult = fixture.db.query(domainSql);
    metrics.push(metricFromQuery(
      manifest,
      findUseCase(manifest, 'sds-domain-query-pack'),
      assignment.nodeId,
      fixture,
      domainSql,
      domainResult,
      measuredDuration(domainStart)
    ));

    const apiShapeSql = `SELECT RECORD_ID, EVENT_INDEX FROM ${TABLE_NAME} WHERE RECORD_ID = '${fixture.records[fixture.records.length - 1].recordId}'`;
    const apiShapeStart = performance.now();
    const apiShapeResult = fixture.db.query(apiShapeSql);
    metrics.push(metricFromQuery(
      manifest,
      findUseCase(manifest, 'udl-style-api-shape'),
      assignment.nodeId,
      fixture,
      apiShapeSql,
      apiShapeResult,
      measuredDuration(apiShapeStart)
    ));
  }

  return metrics;
}
