import type { ArtifactIngestOptions, ArtifactQueryParams, ArtifactQueryResult, ArtifactQuerySpec } from './types.js';
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
export declare class FlatSQLStandaloneArtifactBuilder {
    readonly runtime: {
        kind: StandaloneArtifactRuntime;
    };
    private readonly standaloneRuntime;
    private readonly db;
    private closed;
    private constructor();
    static fromSchema(schema: string, options?: StandaloneArtifactBuilderOptions): Promise<FlatSQLStandaloneArtifactBuilder>;
    close(): MaybePromise<void>;
    destroy(): MaybePromise<void>;
    registerFileId(fileId: string, tableName: string): MaybePromise<void>;
    enableDemoExtractors(): MaybePromise<void>;
    ingest(data: Uint8Array, options?: ArtifactIngestOptions): MaybePromise<number>;
    ingestBuffers(buffers: Uint8Array[], options?: ArtifactIngestOptions): MaybePromise<number>;
    query(sql: string, params?: ArtifactQueryParams): MaybePromise<ArtifactQueryResult>;
    queryMany(queries: ArtifactQuerySpec[]): MaybePromise<ArtifactQueryResult[]>;
    queryRawFlatBufferStream(sql: string, params?: ArtifactQueryParams): MaybePromise<Uint8Array>;
    registerQueryTemplate(queryId: string, sql: string, cacheable?: boolean): MaybePromise<void>;
    queryTemplate(queryId: string, params?: ArtifactQueryParams): MaybePromise<ArtifactQueryResult>;
    clearQueryCache(): MaybePromise<void>;
    configureQueryCache(config: StandaloneQueryCacheConfig): MaybePromise<void>;
    getQueryCacheStats(): MaybePromise<StandaloneQueryCacheStats>;
    buildResponseArtifactCacheKey(schemaName: string, schemaVersion: string | number, sql: string, options?: StandaloneResponseArtifactCacheKeyOptions): MaybePromise<string>;
    getFlatBufferByIndex(tableName: string, indexName: string, keyParams?: ArtifactQueryParams): MaybePromise<Uint8Array | null>;
    exportData(): MaybePromise<Uint8Array>;
    loadAndRebuild(data: Uint8Array): MaybePromise<void>;
    reserveStorageBytes(bytes: number): MaybePromise<void>;
    loadAndRebuildFrom(source: FlatSQLStandaloneArtifactBuilder): MaybePromise<void>;
}
export declare function createStandaloneArtifactBuilder(schema: string, options?: StandaloneArtifactBuilderOptions): Promise<FlatSQLStandaloneArtifactBuilder>;
export {};
//# sourceMappingURL=standalone-builder.d.ts.map