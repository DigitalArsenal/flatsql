function normalizeQueryParams(params) {
    if (params === undefined) {
        return undefined;
    }
    if (!Array.isArray(params)) {
        throw new TypeError('Standalone artifact queries currently require positional parameter arrays.');
    }
    return params.map((value) => {
        if (value === null ||
            typeof value === 'boolean' ||
            typeof value === 'number' ||
            typeof value === 'string' ||
            value instanceof Uint8Array) {
            return value;
        }
        throw new TypeError(`Unsupported standalone artifact query parameter type: ${typeof value}`);
    });
}
function normalizeResult(result) {
    return {
        columns: result.columns,
        rows: result.rows,
        rowCount: result.rows.length,
    };
}
async function loadStandaloneRuntime(options) {
    if (options.runtime === 'wasmedge') {
        const { createFlatSQLWasmEdgeProcessRuntime } = await import('../standalone/process-client.js');
        return createFlatSQLWasmEdgeProcessRuntime({
            runnerPath: options.wasmEdgeRunnerBinary ?? 'flatsql-wasmedge-runner',
            wasmPath: options.wasmPath,
        });
    }
    const standaloneModule = await import(new URL('../../wasm/standalone.js', import.meta.url).href);
    return standaloneModule.loadFlatSQLStandalone({
        path: options.wasmPath,
        url: options.wasmUrl,
        bytes: options.wasmBytes,
    });
}
export class FlatSQLStandaloneArtifactBuilder {
    runtime;
    standaloneRuntime;
    db;
    closed = false;
    constructor(db, runtime, runtimeKind) {
        this.db = db;
        this.standaloneRuntime = runtime;
        this.runtime = { kind: runtimeKind };
    }
    static async fromSchema(schema, options = {}) {
        const runtimeKind = options.runtime ?? 'standalone';
        const runtime = await loadStandaloneRuntime({ ...options, runtime: runtimeKind });
        const db = await runtime.createDatabase(schema, options.dbName ?? 'standalone-artifact');
        return new FlatSQLStandaloneArtifactBuilder(db, runtime, runtimeKind);
    }
    close() {
        if (!this.closed) {
            this.closed = true;
            const destroyed = this.db.destroy();
            if (destroyed instanceof Promise) {
                return destroyed.then(() => this.standaloneRuntime.close?.());
            }
            return this.standaloneRuntime.close?.();
        }
        return undefined;
    }
    destroy() {
        return this.close();
    }
    registerFileId(fileId, tableName) {
        return this.db.registerFileId(fileId, tableName);
    }
    enableDemoExtractors() {
        return this.db.enableDemoExtractors();
    }
    ingest(data, options = {}) {
        return this.db.ingest(data, options.sourceName ?? null);
    }
    ingestBuffers(buffers, options = {}) {
        const result = this.db.ingestBuffers(buffers, options.sourceName ?? null);
        if (result instanceof Promise) {
            return result.then(() => buffers.length);
        }
        return buffers.length;
    }
    query(sql, params) {
        const result = this.db.query(sql, normalizeQueryParams(params));
        return result instanceof Promise ? result.then(normalizeResult) : normalizeResult(result);
    }
    queryMany(queries) {
        const result = this.db.queryMany(queries.map((query) => ({
            sql: query.sql,
            params: normalizeQueryParams(query.params),
        })));
        return result instanceof Promise ? result.then((items) => items.map(normalizeResult)) : result.map(normalizeResult);
    }
    queryRawFlatBufferStream(sql, params) {
        return this.db.queryRawFlatBufferStream(sql, normalizeQueryParams(params) ?? []);
    }
    registerQueryTemplate(queryId, sql, cacheable = true) {
        return this.db.registerQueryTemplate(queryId, sql, cacheable);
    }
    queryTemplate(queryId, params) {
        const result = this.db.queryTemplate(queryId, normalizeQueryParams(params) ?? []);
        return result instanceof Promise ? result.then(normalizeResult) : normalizeResult(result);
    }
    clearQueryCache() {
        return this.db.clearQueryCache();
    }
    configureQueryCache(config) {
        return this.db.configureQueryCache(config);
    }
    getQueryCacheStats() {
        return this.db.getQueryCacheStats();
    }
    buildResponseArtifactCacheKey(schemaName, schemaVersion, sql, options = {}) {
        return this.standaloneRuntime.buildResponseArtifactCacheKey(schemaName, String(schemaVersion), sql, {
            format: options.format,
            publishEventKey: options.publishEventKey,
            projection: options.projection,
            params: normalizeQueryParams(options.params) ?? [],
        });
    }
    getFlatBufferByIndex(tableName, indexName, keyParams) {
        return this.db.getFlatBufferByIndex(tableName, indexName, normalizeQueryParams(keyParams) ?? []);
    }
    exportData() {
        return this.db.exportData();
    }
    loadAndRebuild(data) {
        return this.db.loadAndRebuild(data);
    }
    reserveStorageBytes(bytes) {
        return this.db.reserveStorageBytes(bytes);
    }
    loadAndRebuildFrom(source) {
        return this.db.loadAndRebuildFrom(source.db);
    }
}
export async function createStandaloneArtifactBuilder(schema, options = {}) {
    return FlatSQLStandaloneArtifactBuilder.fromSchema(schema, options);
}
//# sourceMappingURL=standalone-builder.js.map