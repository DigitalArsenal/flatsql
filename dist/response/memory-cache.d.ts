import type { MemoryResponseArtifactCacheOptions, QueryResponseArtifact, ResponseCacheKeyInput } from './types.js';
export declare class MemoryResponseArtifactCache {
    private artifacts;
    private totalBytes;
    private maxEntries;
    private maxBytes;
    constructor(options?: MemoryResponseArtifactCacheOptions);
    get size(): number;
    get byteLength(): number;
    get(cacheKey: string): QueryResponseArtifact | undefined;
    set(artifact: QueryResponseArtifact): void;
    getOrCreate(input: ResponseCacheKeyInput, factory: () => QueryResponseArtifact): QueryResponseArtifact;
    getOrCreateByKey(cacheKey: string, factory: () => QueryResponseArtifact): QueryResponseArtifact;
    clear(): void;
    private evict;
}
//# sourceMappingURL=memory-cache.d.ts.map