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
  buildQueryCacheKey(
    dataset: string,
    artifactVersion: string,
    queryId: string,
    params?: StandaloneQueryParam[]
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
  registerQueryTemplate(queryId: string, sql: string, cacheable?: boolean): void;
  queryTemplate(queryId: string, params?: StandaloneQueryParam[]): StandaloneQueryResult;
  clearQueryCache(): void;
  getQueryCacheStats(): StandaloneQueryCacheStats;
  exportData(): Uint8Array;
  loadAndRebuild(data: Uint8Array): void;
  getFlatBufferByIndex(tableName: string, indexName: string, keyParams?: StandaloneQueryParam[]): Uint8Array | null;
}
