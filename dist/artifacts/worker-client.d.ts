import { type DatabaseSchema } from '../schema/index.js';
import type { ArtifactIngestOptions, ArtifactIngestResult, ArtifactWorkerBuilderOptions } from './types.js';
export declare class FlatSQLArtifactWorkerClient {
    private readonly workerPath;
    private worker;
    private nextId;
    private readonly pending;
    constructor(workerPath?: URL);
    init(): Promise<void>;
    createBuilder(schemaSource: string, options: ArtifactWorkerBuilderOptions): Promise<FlatSQLArtifactWorkerBuilder>;
    close(): Promise<void>;
    call(method: string, params: Record<string, unknown>): Promise<any>;
}
export declare class FlatSQLArtifactWorkerBuilder {
    private readonly client;
    private readonly builderId;
    private readonly schema;
    private readonly preferSharedArrayBuffer;
    constructor(client: FlatSQLArtifactWorkerClient, builderId: string, schema: DatabaseSchema, options: ArtifactWorkerBuilderOptions);
    registerFileId(fileId: string, tableName: string): Promise<void>;
    enableDemoExtractors(): Promise<void>;
    ingestBuffers(buffers: Uint8Array[], options?: ArtifactIngestOptions): Promise<ArtifactIngestResult>;
    query(sql: string): Promise<{
        columns: string[];
        rows: any[][];
        rowCount: number;
    }>;
    close(): Promise<void>;
}
//# sourceMappingURL=worker-client.d.ts.map