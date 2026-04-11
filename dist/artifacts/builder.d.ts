import { type DatabaseSchema } from '../schema/index.js';
import { type ArtifactFieldExtractor } from './demo-extractors.js';
import type { ArtifactBuilderOptions, ArtifactIngestOptions, ArtifactQueryParams, ArtifactQuerySpec, ArtifactIngestResult, ArtifactQueryResult } from './types.js';
export declare class FlatSQLArtifactBuilder {
    private readonly schema;
    private readonly db;
    private readonly beginTransactionSql;
    private readonly tableByName;
    private readonly fileIdToTable;
    private readonly extractors;
    private readonly queryCache;
    private readonly queryResultCache;
    private sequence;
    static fromSchema(source: string, options: ArtifactBuilderOptions): FlatSQLArtifactBuilder;
    constructor(schema: DatabaseSchema, options: ArtifactBuilderOptions);
    registerFileId(fileId: string, tableName: string): void;
    setFieldExtractor(tableName: string, extractor: ArtifactFieldExtractor): void;
    enableDemoExtractors(): void;
    ingestBuffers(buffers: Uint8Array[], options?: ArtifactIngestOptions): ArtifactIngestResult;
    query(sql: string, params?: ArtifactQueryParams): ArtifactQueryResult;
    queryMany(queries: readonly ArtifactQuerySpec[]): ArtifactQueryResult[];
    close(): void;
    private createIndexTables;
    private indexTableName;
    private recordTableName;
    private coveringIndexName;
    private createCoveringIndexes;
    private dropCoveringIndexes;
    private createIngestPlan;
}
//# sourceMappingURL=builder.d.ts.map