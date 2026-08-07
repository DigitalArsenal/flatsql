import { createResponseCacheKey } from './cache-key.js';
const DEFAULT_MAX_ENTRIES = 1024;
const DEFAULT_MAX_BYTES = 256 * 1024 * 1024;
export class MemoryResponseArtifactCache {
    artifacts = new Map();
    totalBytes = 0;
    maxEntries;
    maxBytes;
    constructor(options = {}) {
        this.maxEntries = options.maxEntries ?? DEFAULT_MAX_ENTRIES;
        this.maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
        if (!Number.isSafeInteger(this.maxEntries) || this.maxEntries <= 0) {
            throw new Error(`maxEntries must be a positive safe integer, received: ${this.maxEntries}`);
        }
        if (!Number.isSafeInteger(this.maxBytes) || this.maxBytes <= 0) {
            throw new Error(`maxBytes must be a positive safe integer, received: ${this.maxBytes}`);
        }
    }
    get size() {
        return this.artifacts.size;
    }
    get byteLength() {
        return this.totalBytes;
    }
    get(cacheKey) {
        const artifact = this.artifacts.get(cacheKey);
        if (!artifact) {
            return undefined;
        }
        this.artifacts.delete(cacheKey);
        this.artifacts.set(cacheKey, artifact);
        return artifact;
    }
    set(artifact) {
        const existing = this.artifacts.get(artifact.metadata.cacheKey);
        if (existing) {
            this.totalBytes -= existing.metadata.byteLength;
            this.artifacts.delete(artifact.metadata.cacheKey);
        }
        this.artifacts.set(artifact.metadata.cacheKey, artifact);
        this.totalBytes += artifact.metadata.byteLength;
        this.evict();
    }
    getOrCreate(input, factory) {
        const cacheKey = createResponseCacheKey(input);
        return this.getOrCreateByKey(cacheKey, factory);
    }
    getOrCreateByKey(cacheKey, factory) {
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
    clear() {
        this.artifacts.clear();
        this.totalBytes = 0;
    }
    evict() {
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
//# sourceMappingURL=memory-cache.js.map