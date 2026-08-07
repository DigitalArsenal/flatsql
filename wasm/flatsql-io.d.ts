/**
 * Types for the FlatSQL host I/O contract (cpp/include/flatsql/flatsql_io.h).
 *
 * These live in the flatsql package on purpose: the seven-call shape is an ABI,
 * and an ABI that each consumer re-declares locally is an ABI that drifts.
 */

export declare const FLATSQL_IO_READ: number;
export declare const FLATSQL_IO_WRITE: number;
export declare const FLATSQL_IO_CREATE: number;
export declare const FLATSQL_IO_EXCL: number;
export declare const FLATSQL_IO_TRUNC: number;
export declare const FLATSQL_IO_DELETE_ON_CLOSE: number;
/** Namespace op: does the path exist? Returns a status, allocates no handle. */
export declare const FLATSQL_IO_PROBE: number;
/** Namespace op: remove the path. Returns a status, allocates no handle. */
export declare const FLATSQL_IO_UNLINK: number;

export declare const FLATSQL_IO_ERR_GENERIC: number;
export declare const FLATSQL_IO_ERR_NOENT: number;
export declare const FLATSQL_IO_ERR_ACCESS: number;
export declare const FLATSQL_IO_ERR_IO: number;
export declare const FLATSQL_IO_ERR_NOSPACE: number;
export declare const FLATSQL_IO_ERR_BADHANDLE: number;

export declare const DEFAULT_CHUNK_BYTES: number;

/**
 * A backend answers the seven imports SYNCHRONOUSLY, because a wasm import is
 * synchronous. Offsets and sizes are plain numbers (f64 on the wire — never
 * i64, which emscripten legalizes differently in the two lanes).
 */
export interface FlatSqlIoBackend {
  /** Returns a handle, or a negative status. PROBE/UNLINK return a status. */
  open(path: string, flags: number): number;
  /** Bytes read; may be short at EOF. Never moves an implicit cursor. */
  read(handle: number, dst: Uint8Array, offset: number): number;
  /** Bytes written. Writing past EOF extends; the gap reads as zeroes. */
  write(handle: number, src: Uint8Array, offset: number): number;
  truncate(handle: number, size: number): number;
  /**
   * Durability barrier. On a synchronous backend bytes are durable at return;
   * on an async store they become durable when the caller awaits flush().
   */
  sync(handle: number): number;
  size(handle: number): number;
  close(handle: number): number;

  /** Pull a file into the synchronous cache BEFORE the engine opens it. */
  hydrate(path: string): Promise<unknown>;
  /** Persist dirty state. No-op on synchronous backends. */
  flush(): Promise<void>;
  /** Remove a file and every key backing it. */
  drop(path: string): Promise<void>;

  readonly kind?: string;
}

export interface FlatSqlIoChunkOptions {
  /** Page-group size in bytes. */
  chunkBytes?: number;
  /** Key namespace inside the store. */
  prefix?: string;
  /** Paths already known to exist. */
  knownPaths?: string[];
}

/** Flat key -> bytes store: the shape every browser persistence store has. */
export interface FlatSqlKeyValueStore {
  readBytes(key: string): Promise<Uint8Array | null>;
  writeBytes(key: string, bytes: Uint8Array): Promise<void>;
  deleteKey(key: string): Promise<void>;
}

export declare function createMemoryBackend(
  options?: { chunkBytes?: number },
): FlatSqlIoBackend;

export declare function createNodeFsBackend(
  fs: unknown,
  options?: { root?: string },
): FlatSqlIoBackend;

/**
 * Give a flat key -> bytes store real pread/pwrite semantics by addressing
 * fixed-size page groups. A flush costs O(dirty chunks), not O(file).
 */
export declare function createChunkedStoreBackend(
  store: FlatSqlKeyValueStore,
  options?: FlatSqlIoChunkOptions,
): FlatSqlIoBackend & { knownPaths(): string[]; chunkBytes: number };

/** Build the `env` import object for a directly-instantiated (WASI) module. */
export declare function createFlatSqlIoImports(
  backend: FlatSqlIoBackend,
  memoryRef: () => WebAssembly.Memory,
): Record<string, (...args: number[]) => number>;
