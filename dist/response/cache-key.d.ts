import type { ResponseCacheKeyInput } from './types.js';
export declare function stableStringify(value: unknown): string;
export declare function hashBytes(bytes: Uint8Array): string;
export declare function hashString(value: string): string;
export declare function createResponseCacheKey(input: ResponseCacheKeyInput): string;
//# sourceMappingURL=cache-key.d.ts.map