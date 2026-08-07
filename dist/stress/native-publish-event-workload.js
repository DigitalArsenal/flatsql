import { performance } from 'node:perf_hooks';
const TABLE_NAME = 'PublishEventRecord';
const FILE_IDENTIFIER = 'PUBL';
const TEXT_ENCODER = new TextEncoder();
const COLD_FANOUT_QUERIES_PER_CONCURRENCY = 256;
const BYTES_PER_GIB = 1024 * 1024 * 1024;
const FULL_MODE_STORAGE_TARGET_PCT = 0.96;
const FULL_MODE_RECORD_OVERHEAD_BYTES = 256;
const MAX_DERIVED_PAYLOAD_BYTES = 1024 * 1024;
const PUBLISH_EVENT_SCHEMA = `
table PublishEventRecord {
  FILE_ID: string (key);
  RECORD_ID: string;
  EVENT_INDEX: int;
  PAYLOAD_SIZE: int;
}

root_type PublishEventRecord;
`;
function bytes(value) {
    return TEXT_ENCODER.encode(value).length;
}
function measuredDuration(start) {
    return Number(Math.max(performance.now() - start, 0.001).toFixed(3));
}
function basePayloadSize(nodeId, recordIndex) {
    return 96 + ((nodeId + recordIndex) % 7) * 32;
}
function payloadSize(manifest, nodeId, recordIndex) {
    const baseSize = basePayloadSize(nodeId, recordIndex);
    if (manifest.mode !== 'full') {
        return baseSize;
    }
    const targetBytesPerRecord = Math.floor((manifest.nodeStorageGb * BYTES_PER_GIB * FULL_MODE_STORAGE_TARGET_PCT) / Math.max(1, manifest.recordsPerNode));
    const derivedSize = Math.max(baseSize, targetBytesPerRecord - FULL_MODE_RECORD_OVERHEAD_BYTES);
    return Math.min(derivedSize, MAX_DERIVED_PAYLOAD_BYTES);
}
function resultCellBytes(cell) {
    if (cell instanceof Uint8Array)
        return cell.length;
    if (typeof cell === 'bigint')
        return bytes(cell.toString());
    if (cell === null || cell === undefined)
        return 0;
    return bytes(String(cell));
}
function queryResultBytes(result) {
    const columnBytes = result.columns.reduce((total, column) => total + bytes(column), 0);
    return result.rows.reduce((rowTotal, row) => rowTotal + row.reduce((cellTotal, cell) => cellTotal + resultCellBytes(cell), 0), columnBytes);
}
function rawFlatbufferBytes(result) {
    return result.rows.reduce((total, row) => total + row.reduce((cellTotal, cell) => {
        return cellTotal + (cell instanceof Uint8Array ? cell.length : 0);
    }, 0), 0);
}
function requestBytes(sql, params = []) {
    return bytes(sql) + params.reduce((total, param) => total + resultCellBytes(param), 0) + 16;
}
function findUseCase(manifest, id) {
    const useCase = manifest.useCases.find((candidate) => candidate.id === id);
    if (!useCase) {
        throw new Error(`Stress use case not found: ${id}`);
    }
    return useCase;
}
function baseMetric(manifest, useCase, nodeId, values, schema = TABLE_NAME) {
    return {
        runId: manifest.runId,
        nodeId,
        schema,
        useCase: useCase.id,
        operation: useCase.operation,
        measurement: 'measured',
        transport: manifest.transport,
        runtime: 'standalone',
        ...values,
    };
}
function buildSchemaSweepMetrics(manifest, assignmentSchemas, nodeId) {
    const useCase = findUseCase(manifest, 'schema-sweep');
    const schemaByName = new Map(manifest.schemas.map((schema) => [schema.name, schema]));
    return assignmentSchemas.map((schemaName) => {
        const schema = schemaByName.get(schemaName);
        return baseMetric(manifest, useCase, nodeId, {
            records: 1,
            requestBytes: schema ? bytes(schema.path) : bytes(schemaName),
            responseBytes: schema ? bytes(schema.name) + bytes(schema.rootType) : bytes(schemaName),
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
function createDatabase(flatsql, dbName) {
    const db = flatsql.createDatabase(PUBLISH_EVENT_SCHEMA, dbName);
    db.registerFileId(FILE_IDENTIFIER, TABLE_NAME);
    db.enableDemoExtractors();
    return db;
}
function reserveFullModeStorage(db, manifest) {
    if (manifest.mode !== 'full') {
        return;
    }
    const targetBytes = Math.ceil(manifest.nodeStorageGb * BYTES_PER_GIB * FULL_MODE_STORAGE_TARGET_PCT);
    db.reserveStorageBytes(targetBytes);
}
function createRecord(flatsql, manifest, nodeId, index, hotRecordCount) {
    const fileId = index < hotRecordCount
        ? `publish-${manifest.runId}-${nodeId}-hot`
        : `publish-${manifest.runId}-${nodeId}-cold-${index}`;
    const recordId = `record-${nodeId}-${index}`;
    return {
        fileId,
        recordId,
        buffer: flatsql.createTestPublishEvent(fileId, recordId, index, payloadSize(manifest, nodeId, index)),
    };
}
function buildFixture(flatsql, manifest, nodeId) {
    const db = createDatabase(flatsql, `sds-stress-${manifest.runId}-${nodeId}`);
    reserveFullModeStorage(db, manifest);
    const recordCount = Math.max(1, manifest.recordsPerNode);
    const hotRecordCount = Math.max(1, Math.ceil(recordCount * manifest.hotQueryRatio));
    const coldLimit = Math.max(1, manifest.queryConcurrency * COLD_FANOUT_QUERIES_PER_CONCURRENCY);
    const coldFileIds = [];
    let firstRecordId = '';
    let firstRecordBytes = 0;
    let flatbufferBytes = 0;
    let batchBytes = 0;
    let batch = [];
    const start = performance.now();
    let index = 0;
    try {
        for (; index < recordCount; index++) {
            const record = createRecord(flatsql, manifest, nodeId, index, hotRecordCount);
            if (index === 0) {
                firstRecordId = record.recordId;
                firstRecordBytes = record.buffer.length;
            }
            if (index >= hotRecordCount && coldFileIds.length < coldLimit) {
                coldFileIds.push(record.fileId);
            }
            flatbufferBytes += record.buffer.length;
            batch.push(record.buffer);
            batchBytes += record.buffer.length + 4;
            if (batchBytes >= manifest.batchBytes) {
                db.ingestBuffers(batch);
                batch = [];
                batchBytes = 0;
            }
        }
        if (batch.length > 0) {
            db.ingestBuffers(batch);
        }
    }
    catch (error) {
        throw new Error(`fixture ingest failed at record ${index} of ${recordCount}, ` +
            `flatbufferBytes=${flatbufferBytes}, pendingBatchBytes=${batchBytes}: ${describeError(error)}`);
    }
    return {
        db,
        hotFileId: `publish-${manifest.runId}-${nodeId}-hot`,
        coldFileIds,
        firstRecordId,
        firstRecordBytes,
        flatbufferBytes,
        ingestDurationMs: measuredDuration(start),
        recordCount,
    };
}
function queryMetric(manifest, useCaseId, nodeId, fixture, sql, params, result, durationMs, cacheHits = 0, cacheMisses = 0, cacheSize = 0) {
    const useCase = findUseCase(manifest, useCaseId);
    const flatBytes = rawFlatbufferBytes(result);
    const responseBytes = useCaseId === 'raw-flatbuffer-retrieval' ? flatBytes : queryResultBytes(result);
    return baseMetric(manifest, useCase, nodeId, {
        records: result.rows.length,
        requestBytes: requestBytes(sql, params),
        responseBytes,
        flatbufferBytes: flatBytes,
        storageBytes: fixture.db.getStorageInfo().size,
        durationMs,
        cacheHits,
        cacheMisses,
        cacheSize,
        errors: 0,
    });
}
function runQuery(db, sql, params = []) {
    const start = performance.now();
    const result = db.query(sql, [...params]);
    return { result, durationMs: measuredDuration(start) };
}
function describeError(error) {
    if (error instanceof Error) {
        return error.stack ?? error.message;
    }
    return Object.prototype.toString.call(error);
}
function buildHotCacheMetric(manifest, fixture, nodeId) {
    const sql = `SELECT _data FROM ${TABLE_NAME} WHERE FILE_ID = ?`;
    fixture.db.registerQueryTemplate('hotFileIdRaw', sql, true);
    const before = fixture.db.getQueryCacheStats();
    let result;
    let durationMs = 0;
    const repetitions = Math.max(4, Math.min(32, manifest.queryConcurrency));
    for (let index = 0; index < repetitions; index++) {
        const start = performance.now();
        result = fixture.db.queryTemplate('hotFileIdRaw', [fixture.hotFileId]);
        durationMs += measuredDuration(start);
    }
    const after = fixture.db.getQueryCacheStats();
    const flatBytes = result ? rawFlatbufferBytes(result) : 0;
    return baseMetric(manifest, findUseCase(manifest, 'hot-file-id-cache'), nodeId, {
        records: (result?.rows.length ?? 0) * repetitions,
        requestBytes: requestBytes(sql, [fixture.hotFileId]) * repetitions,
        responseBytes: result ? queryResultBytes(result) * repetitions : 0,
        flatbufferBytes: flatBytes * repetitions,
        storageBytes: fixture.db.getStorageInfo().size,
        durationMs: Number(durationMs.toFixed(3)),
        cacheHits: after.hits - before.hits,
        cacheMisses: after.misses - before.misses,
        cacheSize: after.size,
        errors: 0,
    });
}
function buildColdFanoutMetric(manifest, fixture, nodeId) {
    const useCase = findUseCase(manifest, 'cold-file-id-fanout');
    const fileIds = fixture.coldFileIds.length > 0 ? fixture.coldFileIds : [fixture.hotFileId];
    const sql = `SELECT RECORD_ID, EVENT_INDEX FROM ${TABLE_NAME} WHERE FILE_ID = ?`;
    let records = 0;
    let requestTotal = 0;
    let responseTotal = 0;
    let durationMs = 0;
    for (const fileId of fileIds) {
        const query = runQuery(fixture.db, sql, [fileId]);
        records += query.result.rows.length;
        requestTotal += requestBytes(sql, [fileId]);
        responseTotal += queryResultBytes(query.result);
        durationMs += query.durationMs;
    }
    return baseMetric(manifest, useCase, nodeId, {
        records,
        requestBytes: requestTotal,
        responseBytes: responseTotal,
        flatbufferBytes: 0,
        storageBytes: fixture.db.getStorageInfo().size,
        durationMs: Number(durationMs.toFixed(3)),
        cacheHits: 0,
        cacheMisses: fileIds.length,
        cacheSize: fixture.db.getQueryCacheStats().size,
        errors: 0,
    });
}
function buildMixedCacheMetric(manifest, fixture, nodeId) {
    const sql = `SELECT RECORD_ID, PAYLOAD_SIZE FROM ${TABLE_NAME} WHERE FILE_ID = ?`;
    fixture.db.registerQueryTemplate('mixedFileId', sql, true);
    const before = fixture.db.getQueryCacheStats();
    const cold = fixture.coldFileIds[0] ?? fixture.hotFileId;
    const queries = [fixture.hotFileId, fixture.hotFileId, cold, fixture.hotFileId, cold, fixture.hotFileId];
    let records = 0;
    let requestTotal = 0;
    let responseTotal = 0;
    let durationMs = 0;
    for (const fileId of queries) {
        const start = performance.now();
        const result = fixture.db.queryTemplate('mixedFileId', [fileId]);
        durationMs += measuredDuration(start);
        records += result.rows.length;
        requestTotal += requestBytes(sql, [fileId]);
        responseTotal += queryResultBytes(result);
    }
    const after = fixture.db.getQueryCacheStats();
    return baseMetric(manifest, findUseCase(manifest, 'mixed-hot-cold-query'), nodeId, {
        records,
        requestBytes: requestTotal,
        responseBytes: responseTotal,
        flatbufferBytes: 0,
        storageBytes: fixture.db.getStorageInfo().size,
        durationMs: Number(durationMs.toFixed(3)),
        cacheHits: after.hits - before.hits,
        cacheMisses: after.misses - before.misses,
        cacheSize: after.size,
        errors: 0,
    });
}
function buildLoadAndRebuildMetric(flatsql, manifest, fixture, nodeId, useCaseId) {
    const useCase = findUseCase(manifest, useCaseId);
    const db = createDatabase(flatsql, `${manifest.runId}-${nodeId}-${useCaseId}`);
    const sourceStorageBytes = fixture.db.getStorageInfo().size;
    db.reserveStorageBytes(sourceStorageBytes);
    const start = performance.now();
    db.loadAndRebuildFrom(fixture.db);
    const durationMs = measuredDuration(start);
    const storageBytes = db.getStorageInfo().size;
    db.destroy();
    return baseMetric(manifest, useCase, nodeId, {
        records: fixture.recordCount,
        requestBytes: sourceStorageBytes,
        responseBytes: 64,
        flatbufferBytes: fixture.flatbufferBytes,
        storageBytes,
        durationMs,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
    });
}
function buildCacheInvalidationMetric(flatsql, manifest, fixture, nodeId) {
    const sql = `SELECT RECORD_ID FROM ${TABLE_NAME} WHERE FILE_ID = ?`;
    fixture.db.registerQueryTemplate('cacheInvalidationFileId', sql, true);
    const before = fixture.db.getQueryCacheStats();
    const firstStart = performance.now();
    const first = fixture.db.queryTemplate('cacheInvalidationFileId', [fixture.hotFileId]);
    const firstDuration = measuredDuration(firstStart);
    fixture.db.queryTemplate('cacheInvalidationFileId', [fixture.hotFileId]);
    const churn = flatsql.createTestPublishEvent(fixture.hotFileId, `record-${nodeId}-churn`, fixture.recordCount + 1, payloadSize(manifest, nodeId, fixture.recordCount + 1));
    const ingestStart = performance.now();
    fixture.db.ingestOne(churn);
    const ingestDuration = measuredDuration(ingestStart);
    const thirdStart = performance.now();
    const third = fixture.db.queryTemplate('cacheInvalidationFileId', [fixture.hotFileId]);
    const thirdDuration = measuredDuration(thirdStart);
    const after = fixture.db.getQueryCacheStats();
    return baseMetric(manifest, findUseCase(manifest, 'cache-invalidation-churn'), nodeId, {
        records: first.rows.length + third.rows.length + 1,
        requestBytes: requestBytes(sql, [fixture.hotFileId]) * 3 + churn.length,
        responseBytes: queryResultBytes(first) + queryResultBytes(third),
        flatbufferBytes: churn.length,
        storageBytes: fixture.db.getStorageInfo().size,
        durationMs: Number((firstDuration + ingestDuration + thirdDuration).toFixed(3)),
        cacheHits: after.hits - before.hits,
        cacheMisses: after.misses - before.misses,
        cacheSize: after.size,
        errors: 0,
    });
}
function buildBandwidthStreamingMetric(flatsql, manifest, fixture, nodeId) {
    const useCase = findUseCase(manifest, 'bandwidth-constrained-streaming');
    const db = createDatabase(flatsql, `${manifest.runId}-${nodeId}-bandwidth`);
    const sourceStorageBytes = fixture.db.getStorageInfo().size;
    db.reserveStorageBytes(sourceStorageBytes);
    const start = performance.now();
    db.loadAndRebuildFrom(fixture.db);
    const durationMs = measuredDuration(start);
    const storageBytes = db.getStorageInfo().size;
    db.destroy();
    return baseMetric(manifest, useCase, nodeId, {
        records: fixture.recordCount,
        requestBytes: sourceStorageBytes,
        responseBytes: fixture.recordCount * 8,
        flatbufferBytes: fixture.flatbufferBytes,
        storageBytes,
        durationMs,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
    });
}
export async function buildMeasuredStandalonePublishEventMetrics(manifest) {
    const { loadFlatSQLStandalone } = await import(new URL('../../wasm/standalone.js', import.meta.url).href);
    const flatsql = await loadFlatSQLStandalone();
    const metrics = [];
    for (const assignment of manifest.assignments) {
        let stage = 'build-fixture';
        let fixture;
        try {
            fixture = buildFixture(flatsql, manifest, assignment.nodeId);
            stage = 'schema-sweep';
            metrics.push(...buildSchemaSweepMetrics(manifest, assignment.schemas, assignment.nodeId));
            stage = 'bulk-streaming-ingest';
            metrics.push(baseMetric(manifest, findUseCase(manifest, 'bulk-streaming-ingest'), assignment.nodeId, {
                records: fixture.recordCount,
                requestBytes: fixture.flatbufferBytes + fixture.recordCount * 4,
                responseBytes: fixture.recordCount * 8,
                flatbufferBytes: fixture.flatbufferBytes,
                storageBytes: fixture.db.getStorageInfo().size,
                durationMs: fixture.ingestDurationMs,
                cacheHits: 0,
                cacheMisses: 0,
                cacheSize: 0,
                errors: 0,
            }));
            stage = 'hot-file-id-cache';
            metrics.push(buildHotCacheMetric(manifest, fixture, assignment.nodeId));
            stage = 'cold-file-id-fanout';
            metrics.push(buildColdFanoutMetric(manifest, fixture, assignment.nodeId));
            stage = 'mixed-hot-cold-query';
            metrics.push(buildMixedCacheMetric(manifest, fixture, assignment.nodeId));
            stage = 'raw-flatbuffer-retrieval';
            const rawSql = `SELECT _data FROM ${TABLE_NAME} WHERE FILE_ID = ?`;
            const rawStart = performance.now();
            const rawStream = fixture.db.queryRawFlatBufferStream(rawSql, [fixture.hotFileId]);
            metrics.push(baseMetric(manifest, findUseCase(manifest, 'raw-flatbuffer-retrieval'), assignment.nodeId, {
                records: 1,
                requestBytes: requestBytes(rawSql, [fixture.hotFileId]),
                responseBytes: rawStream.length,
                flatbufferBytes: rawStream.length,
                storageBytes: fixture.db.getStorageInfo().size,
                durationMs: measuredDuration(rawStart),
                cacheHits: 0,
                cacheMisses: 0,
                cacheSize: fixture.db.getQueryCacheStats().size,
                errors: 0,
            }));
            stage = 'sql-projection-query';
            const projectionSql = `SELECT RECORD_ID, EVENT_INDEX, PAYLOAD_SIZE FROM ${TABLE_NAME} WHERE FILE_ID = ?`;
            const projection = runQuery(fixture.db, projectionSql, [fixture.hotFileId]);
            metrics.push(queryMetric(manifest, 'sql-projection-query', assignment.nodeId, fixture, projectionSql, [fixture.hotFileId], projection.result, projection.durationMs));
            stage = 'raw-sql-query';
            const rawQuerySql = `SELECT RECORD_ID FROM ${TABLE_NAME} WHERE EVENT_INDEX BETWEEN 0 AND ?`;
            const rawQuery = runQuery(fixture.db, rawQuerySql, [Math.min(3, fixture.recordCount - 1)]);
            metrics.push(queryMetric(manifest, 'raw-sql-query', assignment.nodeId, fixture, rawQuerySql, [Math.min(3, fixture.recordCount - 1)], rawQuery.result, rawQuery.durationMs));
            stage = 'large-result-query';
            const largeResultSql = `SELECT RECORD_ID, FILE_ID FROM ${TABLE_NAME} LIMIT 5000`;
            const largeResult = runQuery(fixture.db, largeResultSql);
            metrics.push(queryMetric(manifest, 'large-result-query', assignment.nodeId, fixture, largeResultSql, [], largeResult.result, largeResult.durationMs));
            stage = 'node-fanout-query';
            const fanoutSql = `SELECT RECORD_ID FROM ${TABLE_NAME} WHERE FILE_ID = ?`;
            const fanout = runQuery(fixture.db, fanoutSql, [fixture.hotFileId]);
            metrics.push(queryMetric(manifest, 'node-fanout-query', assignment.nodeId, fixture, fanoutSql, [fixture.hotFileId], fanout.result, fanout.durationMs));
            stage = 'backfill-sync';
            metrics.push(buildLoadAndRebuildMetric(flatsql, manifest, fixture, assignment.nodeId, 'backfill-sync'));
            stage = 'restart-rebuild';
            metrics.push(buildLoadAndRebuildMetric(flatsql, manifest, fixture, assignment.nodeId, 'restart-rebuild'));
            stage = 'cache-invalidation-churn';
            metrics.push(buildCacheInvalidationMetric(flatsql, manifest, fixture, assignment.nodeId));
            stage = 'bandwidth-constrained-streaming';
            metrics.push(buildBandwidthStreamingMetric(flatsql, manifest, fixture, assignment.nodeId));
            stage = 'sds-domain-query-pack';
            const domainSql = `SELECT FILE_ID, RECORD_ID, PAYLOAD_SIZE FROM ${TABLE_NAME} WHERE RECORD_ID = ?`;
            const domain = runQuery(fixture.db, domainSql, [fixture.firstRecordId]);
            metrics.push(queryMetric(manifest, 'sds-domain-query-pack', assignment.nodeId, fixture, domainSql, [fixture.firstRecordId], domain.result, domain.durationMs));
            stage = 'udl-style-api-shape';
            const udlSql = `SELECT RECORD_ID, EVENT_INDEX FROM ${TABLE_NAME} WHERE FILE_ID = ? LIMIT 1`;
            const udl = runQuery(fixture.db, udlSql, [fixture.hotFileId]);
            metrics.push(queryMetric(manifest, 'udl-style-api-shape', assignment.nodeId, fixture, udlSql, [fixture.hotFileId], udl.result, udl.durationMs));
            if (fixture.firstRecordBytes === 0) {
                throw new Error('Native publish-event fixture did not create any records.');
            }
        }
        catch (error) {
            throw new Error(`Standalone PublishEventRecord workload failed at node ${assignment.nodeId} stage ${stage}: ${describeError(error)}`);
        }
        finally {
            fixture?.db.destroy();
        }
    }
    return metrics;
}
//# sourceMappingURL=native-publish-event-workload.js.map