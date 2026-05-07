import { createResponseCacheKey } from './cache-key.js';
import type {
  MemoryResponseArtifactCacheOptions,
  QueryResponseArtifact,
  ResponseCacheKeyInput,
} from './types.js';

const DEFAULT_MAX_ENTRIES = 1024;
const DEFAULT_MAX_BYTES = 256 * 1024 * 1024;

export class MemoryResponseArtifactCache {
  private artifacts = new Map<string, QueryResponseArtifact>();
  private totalBytes = 0;
  private maxEntries: number;
  private maxBytes: number;

  constructor(options: MemoryResponseArtifactCacheOptions = {}) {
    this.maxEntries = options.maxEntries ?? DEFAULT_MAX_ENTRIES;
    this.maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
    if (!Number.isSafeInteger(this.maxEntries) || this.maxEntries <= 0) {
      throw new Error(`maxEntries must be a positive safe integer, received: ${this.maxEntries}`);
    }
    if (!Number.isSafeInteger(this.maxBytes) || this.maxBytes <= 0) {
      throw new Error(`maxBytes must be a positive safe integer, received: ${this.maxBytes}`);
    }
  }

  get size(): number {
    return this.artifacts.size;
  }

  get byteLength(): number {
    return this.totalBytes;
  }

  get(cacheKey: string): QueryResponseArtifact | undefined {
    const artifact = this.artifacts.get(cacheKey);
    if (!artifact) {
      return undefined;
    }

    this.artifacts.delete(cacheKey);
    this.artifacts.set(cacheKey, artifact);
    return artifact;
  }

  set(artifact: QueryResponseArtifact): void {
    const existing = this.artifacts.get(artifact.metadata.cacheKey);
    if (existing) {
      this.totalBytes -= existing.metadata.byteLength;
      this.artifacts.delete(artifact.metadata.cacheKey);
    }

    this.artifacts.set(artifact.metadata.cacheKey, artifact);
    this.totalBytes += artifact.metadata.byteLength;
    this.evict();
  }

  getOrCreate(
    input: ResponseCacheKeyInput,
    factory: () => QueryResponseArtifact
  ): QueryResponseArtifact {
    const cacheKey = createResponseCacheKey(input);
    return this.getOrCreateByKey(cacheKey, factory);
  }

  getOrCreateByKey(
    cacheKey: string,
    factory: () => QueryResponseArtifact
  ): QueryResponseArtifact {
    const existing = this.get(cacheKey);
    if (existing) {
      return existing;
    }

    const artifact = factory();
    if (artifact.metadata.cacheKey !== cacheKey) {
      throw new Error('Response artifact factory returned an artifact with a different cache key');
    }
    this.set(artifact);
    return artifact;
  }

  clear(): void {
    this.artifacts.clear();
    this.totalBytes = 0;
  }

  private evict(): void {
    while (this.artifacts.size > this.maxEntries || this.totalBytes > this.maxBytes) {
      const oldest = this.artifacts.entries().next();
      if (oldest.done) {
        return;
      }
      const [cacheKey, artifact] = oldest.value;
      this.artifacts.delete(cacheKey);
      this.totalBytes -= artifact.metadata.byteLength;
    }
  }
}
