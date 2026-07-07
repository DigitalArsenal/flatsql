export type StandaloneQueryParam = null | boolean | number | string | Uint8Array;

export interface StandaloneQueryResult {
  columns: string[];
  rows: unknown[][];
}

export interface StandaloneQueryCacheStats {
  hits: number;
  misses: number;
  size: number;
  generation: number;
  maxEntries: number;
  maxRows: number;
}

export interface StandaloneQueryCacheConfig {
  maxEntries: number;
  maxRows: number;
}

export interface StandaloneResponseArtifactCacheKeyOptions {
  format?: string;
  publishEventKey?: string | null;
  projection?: string[];
  params?: StandaloneQueryParam[];
}

export interface LoadFlatSQLStandaloneOptions {
  url?: string | URL;
  path?: string;
  bytes?: Uint8Array | ArrayBuffer | ArrayBufferView;
  imports?: WebAssembly.Imports;
  stdout?: (text: string) => void;
  stderr?: (text: string) => void;
  stdin?: Uint8Array;
  wasi?: {
    stdout?: (text: string) => void;
    stderr?: (text: string) => void;
    stdin?: Uint8Array;
  };
}

export function getFlatSQLStandaloneWasmURL(): URL;
export function loadFlatSQLStandalone(options?: LoadFlatSQLStandaloneOptions): Promise<FlatSQLStandalone>;
export const createStandaloneFlatSQL: typeof loadFlatSQLStandalone;

export class FlatSQLStandalone {
  readonly module: WebAssembly.Module;
  readonly memory: WebAssembly.Memory;

  createDatabase(schema: string, dbName?: string): FlatSQLStandaloneDatabase;
  createTestUser(id: number, name: string, email: string, age: number): Uint8Array;
  createTestPost(id: number, userId: number, title: string): Uint8Array;
  createTestPublishEvent(fileId: string, recordId: string, eventIndex: number, payloadSize: number): Uint8Array;
  buildQueryCacheKey(
    dataset: string,
    artifactVersion: string,
    queryId: string,
    params?: StandaloneQueryParam[]
  ): string;
  buildResponseArtifactCacheKey(
    schemaName: string,
    schemaVersion: string,
    sql: string,
    options?: StandaloneResponseArtifactCacheKeyOptions
  ): string;
}

export class FlatSQLStandaloneDatabase {
  destroy(): void;
  registerFileId(fileId: string, tableName: string): void;
  enableDemoExtractors(): void;
  ingest(data: Uint8Array, source?: string | null): number;
  ingestBuffers(buffers: Uint8Array[], source?: string | null): number;
  ingestOne(data: Uint8Array, source?: string | null): number;
  query(sql: string, params?: StandaloneQueryParam[]): StandaloneQueryResult;
  queryMany(queries: Array<{ sql: string; params?: StandaloneQueryParam[] }>): StandaloneQueryResult[];
  queryRawFlatBufferStream(sql: string, params?: StandaloneQueryParam[]): Uint8Array;

  /**
   * Sandboxed public query (gateway loop G.5): one read-only SELECT under
   * the engine authorizer (record tables / shadow tables / unified views
   * only), single statement, statement timeout, row/byte caps (reject, not
   * truncate). mode "stream" returns aligned size-prefixed frames (all
   * cells must be BLOB); mode "json" returns UTF-8 bare-array JSON bytes
   * with column names verbatim. Rejections throw "sandbox: <code>: ...".
   */
  querySandboxed(
    sql: string,
    params?: StandaloneQueryParam[],
    options?: { mode?: 'stream' | 'json'; maxRows?: number; maxBytes?: number; timeoutMs?: number }
  ): { payload: Uint8Array; rows: number; columns: number };
  lastRawStreamCacheHit(): boolean;
  configureRawStreamCache(maxEntries: number, maxTotalBytes: number): void;
  getRawStreamCacheStats(): {
    hits: number;
    misses: number;
    entries: number;
    totalBytes: number;
  };
  registerQueryTemplate(queryId: string, sql: string, cacheable?: boolean): void;
  queryTemplate(queryId: string, params?: StandaloneQueryParam[]): StandaloneQueryResult;
  clearQueryCache(): void;
  configureQueryCache(config: StandaloneQueryCacheConfig): void;
  getQueryCacheStats(): StandaloneQueryCacheStats;
  exportData(): Uint8Array;
  loadAndRebuild(data: Uint8Array): void;
  reserveStorageBytes(bytes: number): void;
  loadAndRebuildFrom(sourceDb: FlatSQLStandaloneDatabase): void;
  getFlatBufferByIndex(tableName: string, indexName: string, keyParams?: StandaloneQueryParam[]): Uint8Array | null;
  getStorageInfo(): { ptr: number; size: number };
}
