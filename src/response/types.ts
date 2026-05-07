export type QueryResponseFormat = 'json' | 'raw-flatbuffer-stream';
export type QueryResponseEncoding = 'identity';

export interface ResponseCacheKeyInput {
  schemaName?: string;
  schemaVersion?: string | number;
  sql: string;
  params?: readonly unknown[] | Record<string, unknown>;
  format?: QueryResponseFormat;
  publishEventKey?: string;
  projection?: readonly string[];
  filters?: Record<string, unknown>;
  encoding?: QueryResponseEncoding;
}

export interface QueryResponseArtifactOptions extends ResponseCacheKeyInput {
  cacheKey?: string;
  chunkBytes?: number;
  createdAt?: string;
}

export interface QueryResponseArtifactChunk {
  index: number;
  offset: number;
  byteLength: number;
  contentHash: string;
  etag: string;
}

export interface QueryResponseArtifactMetadata {
  cacheKey: string;
  contentHash: string;
  etag: string;
  byteLength: number;
  rowCount: number;
  columnCount: number;
  format: QueryResponseFormat;
  encoding: QueryResponseEncoding;
  createdAt: string;
  chunkBytes: number;
  chunks: QueryResponseArtifactChunk[];
  schemaName?: string;
  schemaVersion?: string | number;
  publishEventKey?: string;
  projection?: readonly string[];
}

export interface QueryResponseArtifact {
  metadata: QueryResponseArtifactMetadata;
  bytes: Uint8Array;
}

export interface MemoryResponseArtifactCacheOptions {
  maxEntries?: number;
  maxBytes?: number;
}
