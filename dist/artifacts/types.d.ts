import type { DatabaseSchema } from '../schema/index.js';
export type ArtifactPerformanceProfile = 'fast' | 'safe';
export interface ArtifactBuilderOptions {
    sqlitePath: string;
    name?: string;
    performanceProfile?: ArtifactPerformanceProfile;
}
export interface ArtifactIngestOptions {
    sourceName?: string;
    startOffset?: number;
    offsets?: number[];
}
export interface ArtifactQueryResult {
    columns: string[];
    rows: any[][];
    rowCount: number;
}
export type ArtifactQueryParams = readonly unknown[] | Record<string, unknown>;
export interface ArtifactQuerySpec {
    sql: string;
    params?: ArtifactQueryParams;
}
export type ArtifactTransportMode = 'clone' | 'shared-array-buffer';
export interface ArtifactIngestResult {
    recordCount: number;
    transportMode?: ArtifactTransportMode;
}
export interface ArtifactWorkerBuilderOptions extends ArtifactBuilderOptions {
    preferSharedArrayBuffer?: boolean;
}
export interface ParsedArtifactSchema {
    schema: DatabaseSchema;
}
//# sourceMappingURL=types.d.ts.map