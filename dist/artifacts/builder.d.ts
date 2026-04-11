import { type DatabaseSchema } from '../schema/index.js';
import { type ArtifactFieldExtractor } from './demo-extractors.js';
import type { ArtifactBuilderOptions, ArtifactIngestOptions, ArtifactIngestResult, ArtifactQueryResult } from './types.js';
export declare class FlatSQLArtifactBuilder {
    private readonly schema;
    private readonly db;
    private readonly fileIdToTable;
    private readonly extractors;
    private sequence;
    static fromSchema(source: string, options: ArtifactBuilderOptions): FlatSQLArtifactBuilder;
    constructor(schema: DatabaseSchema, options: ArtifactBuilderOptions);
    registerFileId(fileId: string, tableName: string): void;
    setFieldExtractor(tableName: string, extractor: ArtifactFieldExtractor): void;
    enableDemoExtractors(): void;
    ingestBuffers(buffers: Uint8Array[], options?: ArtifactIngestOptions): ArtifactIngestResult;
    query(sql: string): ArtifactQueryResult;
    close(): void;
    private createIndexTables;
    private indexTableName;
}
//# sourceMappingURL=builder.d.ts.map