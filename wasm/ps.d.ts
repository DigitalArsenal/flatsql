// Types for wasm/ps.js (the partition store artifact, flatsql-ps-threads.wasm).

export const FLATSQL_PS_THREADS_FILE: 'flatsql-ps-threads.wasm';

/** URL of the packaged flatsql-ps-threads.wasm. */
export function getFlatSQLPsThreadsURL(): URL;

export interface LoadFlatSQLPsThreadsOptions {
  url?: string | URL;
  path?: string;
  /** false skips the integrity.json check. */
  verify?: boolean;
  /** SHA-256 (hex) provider outside Node; back it with WASM or native crypto. Browser WebCrypto is intentionally not used. */
  computeSHA256?: (bytes: Uint8Array) => string | Promise<string>;
}

/** The artifact's bytes, checked against integrity.json (Node, or with computeSHA256). */
export function loadFlatSQLPsThreads(options?: LoadFlatSQLPsThreadsOptions): Promise<Uint8Array>;

/** The artifact's sha256 (hex) from the packaged integrity.json, or null. */
export function flatsqlPsThreadsSha256(): Promise<string | null>;
