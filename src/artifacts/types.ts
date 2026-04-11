import type { DatabaseSchema } from '../schema/index.js';

export interface ArtifactBuilderOptions {
  sqlitePath: string;
  name?: string;
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
