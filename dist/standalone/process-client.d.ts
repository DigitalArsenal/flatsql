type StandaloneQueryParam = null | boolean | number | string | Uint8Array;
interface QueryResult {
    columns: string[];
    rows: unknown[][];
}
export interface FlatSQLWasmEdgeProcessRuntimeOptions {
    runnerPath: string;
    wasmPath?: string;
    env?: NodeJS.ProcessEnv;
}
declare class BinaryReader {
    private offset;
    private readonly data;
    constructor(data: Uint8Array);
    u8(): number;
    u32(): number;
    f64(): number;
    string(): string;
    bytes(): Uint8Array;
    private require;
}
declare class FlatSQLWasmEdgeProcess {
    private readonly child;
    private readonly pending;
    private stdout;
    private stderr;
    private closed;
    constructor(options: FlatSQLWasmEdgeProcessRuntimeOptions);
    request(payload: Uint8Array): Promise<BinaryReader>;
    close(): Promise<void>;
    private drainStdout;
    private rejectAll;
}
export declare class FlatSQLWasmEdgeProcessRuntime {
    private readonly process;
    constructor(options: FlatSQLWasmEdgeProcessRuntimeOptions);
    createDatabase(schema: string, dbName?: string): Promise<FlatSQLWasmEdgeProcessDatabase>;
    close(): Promise<void>;
    buildResponseArtifactCacheKey(schemaName: string, schemaVersion: string, sql: string, options?: {
        format?: string;
        publishEventKey?: string | null;
        projection?: readonly string[];
        params?: StandaloneQueryParam[];
    }): Promise<string>;
}
export declare class FlatSQLWasmEdgeProcessDatabase {
    private readonly process;
    private handle;
    constructor(process: FlatSQLWasmEdgeProcess, handle: number);
    destroy(): Promise<void>;
    registerFileId(fileId: string, tableName: string): Promise<void>;
    enableDemoExtractors(): Promise<void>;
    ingest(data: Uint8Array, source?: string | null): Promise<number>;
    ingestBuffers(buffers: Uint8Array[], source?: string | null): Promise<number>;
    ingestOne(data: Uint8Array, source?: string | null): Promise<number>;
    query(sql: string, params?: StandaloneQueryParam[]): Promise<QueryResult>;
    queryMany(queries: Array<{
        sql: string;
        params?: StandaloneQueryParam[];
    }>): Promise<QueryResult[]>;
    queryRawFlatBufferStream(sql: string, params?: StandaloneQueryParam[]): Promise<Uint8Array>;
    registerQueryTemplate(queryId: string, sql: string, cacheable?: boolean): Promise<void>;
    queryTemplate(queryId: string, params?: StandaloneQueryParam[]): Promise<QueryResult>;
    clearQueryCache(): Promise<void>;
    configureQueryCache({ maxEntries, maxRows }: {
        maxEntries: number;
        maxRows: number;
    }): Promise<void>;
    getQueryCacheStats(): Promise<{
        hits: number;
        misses: number;
        size: number;
        generation: number;
        maxEntries: number;
        maxRows: number;
    }>;
    getFlatBufferByIndex(tableName: string, indexName: string, keyParams?: StandaloneQueryParam[]): Promise<Uint8Array | null>;
    exportData(): Promise<Uint8Array>;
    loadAndRebuild(data: Uint8Array): Promise<void>;
    reserveStorageBytes(bytes: number): Promise<void>;
    loadAndRebuildFrom(sourceDb: FlatSQLWasmEdgeProcessDatabase): Promise<void>;
}
export declare function createFlatSQLWasmEdgeProcessRuntime(options: FlatSQLWasmEdgeProcessRuntimeOptions): FlatSQLWasmEdgeProcessRuntime;
export {};
//# sourceMappingURL=process-client.d.ts.map