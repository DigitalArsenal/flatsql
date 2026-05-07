import type {
  ArtifactIngestOptions,
  ArtifactQueryParams,
  ArtifactQueryResult,
  ArtifactQuerySpec,
} from './types.js';

export type StandaloneArtifactRuntime = 'browser' | 'standalone' | 'wasmedge';

export interface StandaloneArtifactBuilderOptions {
  runtime?: StandaloneArtifactRuntime;
  dbName?: string;
  wasmPath?: string;
  wasmUrl?: string | URL;
  wasmBytes?: Uint8Array | ArrayBuffer | ArrayBufferView;
  wasmEdgeRunnerBinary?: string;
}

type MaybePromise<T> = T | Promise<T>;
type StandaloneQueryParam = null | boolean | number | string | Uint8Array;

export interface StandaloneResponseArtifactCacheKeyOptions {
  format?: string;
  publishEventKey?: string | null;
  projection?: readonly string[];
  params?: ArtifactQueryParams;
}

export interface StandaloneQueryCacheConfig {
  maxEntries: number;
  maxRows: number;
}

export interface StandaloneQueryCacheStats {
  hits: number;
  misses: number;
  size: number;
  generation: number;
  maxEntries: number;
  maxRows: number;
}

interface StandaloneDatabase {
  destroy(): MaybePromise<void>;
  registerFileId(fileId: string, tableName: string): MaybePromise<void>;
  enableDemoExtractors(): MaybePromise<void>;
  ingest(data: Uint8Array, source?: string | null): MaybePromise<number>;
  ingestBuffers(buffers: Uint8Array[], source?: string | null): MaybePromise<number>;
  query(sql: string, params?: StandaloneQueryParam[]): MaybePromise<{ columns: string[]; rows: unknown[][] }>;
  queryMany(queries: Array<{ sql: string; params?: StandaloneQueryParam[] }>): MaybePromise<Array<{ columns: string[]; rows: unknown[][] }>>;
  queryRawFlatBufferStream(sql: string, params?: StandaloneQueryParam[]): MaybePromise<Uint8Array>;
  registerQueryTemplate(queryId: string, sql: string, cacheable?: boolean): MaybePromise<void>;
  queryTemplate(queryId: string, params?: StandaloneQueryParam[]): MaybePromise<{ columns: string[]; rows: unknown[][] }>;
  clearQueryCache(): MaybePromise<void>;
  configureQueryCache(config: StandaloneQueryCacheConfig): MaybePromise<void>;
  getQueryCacheStats(): MaybePromise<StandaloneQueryCacheStats>;
  getFlatBufferByIndex(tableName: string, indexName: string, keyParams?: StandaloneQueryParam[]): MaybePromise<Uint8Array | null>;
  exportData(): MaybePromise<Uint8Array>;
  loadAndRebuild(data: Uint8Array): MaybePromise<void>;
  reserveStorageBytes(bytes: number): MaybePromise<void>;
  loadAndRebuildFrom(sourceDb: StandaloneDatabase): MaybePromise<void>;
}

interface StandaloneRuntime {
  buildResponseArtifactCacheKey(
    schemaName: string,
    schemaVersion: string,
    sql: string,
    options?: {
      format?: string;
      publishEventKey?: string | null;
      projection?: readonly string[];
      params?: StandaloneQueryParam[];
    }
  ): MaybePromise<string>;
  createDatabase(schema: string, dbName?: string): MaybePromise<StandaloneDatabase>;
  close?(): MaybePromise<void>;
}

function normalizeQueryParams(params: ArtifactQueryParams | undefined): StandaloneQueryParam[] | undefined {
  if (params === undefined) {
    return undefined;
  }
  if (!Array.isArray(params)) {
    throw new TypeError('Standalone artifact queries currently require positional parameter arrays.');
  }
  return params.map((value) => {
    if (
      value === null ||
      typeof value === 'boolean' ||
      typeof value === 'number' ||
      typeof value === 'string' ||
      value instanceof Uint8Array
    ) {
      return value;
    }
    throw new TypeError(`Unsupported standalone artifact query parameter type: ${typeof value}`);
  });
}

function normalizeResult(result: { columns: string[]; rows: unknown[][] }): ArtifactQueryResult {
  return {
    columns: result.columns,
    rows: result.rows as any[][],
    rowCount: result.rows.length,
  };
}

async function loadStandaloneRuntime(options: StandaloneArtifactBuilderOptions): Promise<StandaloneRuntime> {
  if (options.runtime === 'wasmedge') {
    const { createFlatSQLWasmEdgeProcessRuntime } = await import('../standalone/process-client.js');
    return createFlatSQLWasmEdgeProcessRuntime({
      runnerPath: options.wasmEdgeRunnerBinary ?? 'flatsql-wasmedge-runner',
      wasmPath: options.wasmPath,
    }) as StandaloneRuntime;
  }

  const standaloneModule = await import(new URL('../../wasm/standalone.js', import.meta.url).href);
  return standaloneModule.loadFlatSQLStandalone({
    path: options.wasmPath,
    url: options.wasmUrl,
    bytes: options.wasmBytes,
  }) as Promise<StandaloneRuntime>;
}

export class FlatSQLStandaloneArtifactBuilder {
  readonly runtime: { kind: StandaloneArtifactRuntime };
  private readonly standaloneRuntime: StandaloneRuntime;
  private readonly db: StandaloneDatabase;
  private closed = false;

  private constructor(db: StandaloneDatabase, runtime: StandaloneRuntime, runtimeKind: StandaloneArtifactRuntime) {
    this.db = db;
    this.standaloneRuntime = runtime;
    this.runtime = { kind: runtimeKind };
  }

  static async fromSchema(
    schema: string,
    options: StandaloneArtifactBuilderOptions = {}
  ): Promise<FlatSQLStandaloneArtifactBuilder> {
    const runtimeKind = options.runtime ?? 'standalone';
    const runtime = await loadStandaloneRuntime({ ...options, runtime: runtimeKind });
    const db = await runtime.createDatabase(schema, options.dbName ?? 'standalone-artifact');
    return new FlatSQLStandaloneArtifactBuilder(db, runtime, runtimeKind);
  }

  close(): MaybePromise<void> {
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

  destroy(): MaybePromise<void> {
    return this.close();
  }

  registerFileId(fileId: string, tableName: string): MaybePromise<void> {
    return this.db.registerFileId(fileId, tableName);
  }

  enableDemoExtractors(): MaybePromise<void> {
    return this.db.enableDemoExtractors();
  }

  ingest(data: Uint8Array, options: ArtifactIngestOptions = {}): MaybePromise<number> {
    return this.db.ingest(data, options.sourceName ?? null);
  }

  ingestBuffers(buffers: Uint8Array[], options: ArtifactIngestOptions = {}): MaybePromise<number> {
    const result = this.db.ingestBuffers(buffers, options.sourceName ?? null);
    if (result instanceof Promise) {
      return result.then(() => buffers.length);
    }
    return buffers.length;
  }

  query(sql: string, params?: ArtifactQueryParams): MaybePromise<ArtifactQueryResult> {
    const result = this.db.query(sql, normalizeQueryParams(params));
    return result instanceof Promise ? result.then(normalizeResult) : normalizeResult(result);
  }

  queryMany(queries: ArtifactQuerySpec[]): MaybePromise<ArtifactQueryResult[]> {
    const result = this.db.queryMany(
      queries.map((query) => ({
        sql: query.sql,
        params: normalizeQueryParams(query.params),
      }))
    );
    return result instanceof Promise ? result.then((items) => items.map(normalizeResult)) : result.map(normalizeResult);
  }

  queryRawFlatBufferStream(sql: string, params?: ArtifactQueryParams): MaybePromise<Uint8Array> {
    return this.db.queryRawFlatBufferStream(sql, normalizeQueryParams(params) ?? []);
  }

  registerQueryTemplate(queryId: string, sql: string, cacheable = true): MaybePromise<void> {
    return this.db.registerQueryTemplate(queryId, sql, cacheable);
  }

  queryTemplate(queryId: string, params?: ArtifactQueryParams): MaybePromise<ArtifactQueryResult> {
    const result = this.db.queryTemplate(queryId, normalizeQueryParams(params) ?? []);
    return result instanceof Promise ? result.then(normalizeResult) : normalizeResult(result);
  }

  clearQueryCache(): MaybePromise<void> {
    return this.db.clearQueryCache();
  }

  configureQueryCache(config: StandaloneQueryCacheConfig): MaybePromise<void> {
    return this.db.configureQueryCache(config);
  }

  getQueryCacheStats(): MaybePromise<StandaloneQueryCacheStats> {
    return this.db.getQueryCacheStats();
  }

  buildResponseArtifactCacheKey(
    schemaName: string,
    schemaVersion: string | number,
    sql: string,
    options: StandaloneResponseArtifactCacheKeyOptions = {}
  ): MaybePromise<string> {
    return this.standaloneRuntime.buildResponseArtifactCacheKey(schemaName, String(schemaVersion), sql, {
      format: options.format,
      publishEventKey: options.publishEventKey,
      projection: options.projection,
      params: normalizeQueryParams(options.params) ?? [],
    });
  }

  getFlatBufferByIndex(tableName: string, indexName: string, keyParams?: ArtifactQueryParams): MaybePromise<Uint8Array | null> {
    return this.db.getFlatBufferByIndex(tableName, indexName, normalizeQueryParams(keyParams) ?? []);
  }

  exportData(): MaybePromise<Uint8Array> {
    return this.db.exportData();
  }

  loadAndRebuild(data: Uint8Array): MaybePromise<void> {
    return this.db.loadAndRebuild(data);
  }

  reserveStorageBytes(bytes: number): MaybePromise<void> {
    return this.db.reserveStorageBytes(bytes);
  }

  loadAndRebuildFrom(source: FlatSQLStandaloneArtifactBuilder): MaybePromise<void> {
    return this.db.loadAndRebuildFrom(source.db);
  }
}

export async function createStandaloneArtifactBuilder(
  schema: string,
  options: StandaloneArtifactBuilderOptions = {}
): Promise<FlatSQLStandaloneArtifactBuilder> {
  return FlatSQLStandaloneArtifactBuilder.fromSchema(schema, options);
}
