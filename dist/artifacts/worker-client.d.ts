import { type DatabaseSchema } from '../schema/index.js';
import type { ArtifactIngestOptions, ArtifactIngestResult, ArtifactQueryParams, ArtifactQuerySpec, ArtifactQueryResult, ArtifactWorkerBuilderOptions } from './types.js';
export declare class FlatSQLArtifactWorkerClient {
    private readonly workerPath;
    private worker;
    private nextId;
    private readonly pending;
    private closed;
    constructor(workerPath?: URL);
    init(): Promise<void>;
    createBuilder(schemaSource: string, options: ArtifactWorkerBuilderOptions): Promise<FlatSQLArtifactWorkerBuilder>;
    close(): Promise<void>;
    private rejectAllPending;
    call(method: string, params: Record<string, unknown>): Promise<any>;
}
export declare class FlatSQLArtifactWorkerBuilder {
    private readonly client;
    private readonly builderId;
    private readonly schema;
    private readonly preferSharedArrayBuffer;
    private readonly queryResultCache;
    private readonly inFlightQueries;
    private queryGeneration;
    constructor(client: FlatSQLArtifactWorkerClient, builderId: string, schema: DatabaseSchema, options: ArtifactWorkerBuilderOptions);
    registerFileId(fileId: string, tableName: string): Promise<void>;
    enableDemoExtractors(): Promise<void>;
    ingestBuffers(buffers: Uint8Array[], options?: ArtifactIngestOptions): Promise<ArtifactIngestResult>;
    query(sql: string, params?: ArtifactQueryParams): Promise<ArtifactQueryResult>;
    queryMany(queries: readonly ArtifactQuerySpec[]): Promise<ArtifactQueryResult[]>;
    close(): Promise<void>;
}
//# sourceMappingURL=worker-client.d.ts.map