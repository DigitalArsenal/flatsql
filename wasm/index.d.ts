export class FlatSQLError extends Error { readonly status: number; }
export interface FlatSQLOptions {
  /** Host filesystem directory in Node; OPFS directory in browsers. */
  root?: string;
  /** Absolute path inside the host's filesystem. */
  path?: string;
  wasmUrl?: string | URL;
  wasmBytes?: Uint8Array;
  verify?: (bytes: Uint8Array) => void | Promise<void>;
  maximumPages?: number;
  requestBytes?: number;
  writers?: number;
  readers?: number;
  /** Shared row-order and verified-metadata cache budget; default 32 MiB. */
  rowCacheBytes?: number;
  backend?: 'opfs' | 'memory';
  workerUrl?: string | URL;
  threadWorkerUrl?: string | URL;
  ioWorkerUrl?: string | URL;
  /** Browser: how long one attempt waits for the engine's thread workers to be ready (100 to 60000 ms). */
  threadArmingMs?: number;
  /** Browser: attempts with fresh workers when some were not ready in time (1 to 5). */
  threadAttempts?: number;
}
export interface QueryOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
  maxRows?: number;
  maxRowsExamined?: number;
  maxBytesRead?: number;
  maxResultBytes?: number;
  sandbox?: boolean;
  bulk?: boolean;
  /** SELECT _data returns unchanged size-prefixed records. */
  records?: boolean;
}
export interface BinarySchema {
  name: string;
  fileIdentifier: string;
  bfbs: Uint8Array;
  indexRules?: string;
  maxRecordBytes?: number;
  /** Zero requests capability-checked unlimited visibility. */
  maxRows?: number;
  fullText?: boolean;
}
export interface IngestOptions extends QueryOptions {
  /** One opaque signature per input record, in frame order. */
  signatures?: Uint8Array[];
  provider?: string;
  source?: string;
  producer?: string;
  batch?: string;
  timestamp?: number;
}
export class FlatSQL {
  static open(options?: FlatSQLOptions): Promise<FlatSQL>;
  capabilities(): Promise<{unlimitedRelations: boolean}>;
  registerSchema(schema: BinarySchema): Promise<void>;
  /** Frames are [u32 little-endian length][FlatBuffer payload], concatenated. */
  ingest(type: string, frames: Uint8Array, options?: IngestOptions): Promise<Uint8Array>;
  /** Remove records by their original binary payloads; returns an SDS QRP receipt. */
  remove(type: string, frames: Uint8Array, options?: QueryOptions): Promise<Uint8Array>;
  /** QRP request in; chunks of a size-prefixed QRP result stream out. */
  query(request: Uint8Array, options?: QueryOptions): AsyncGenerator<Uint8Array>;
  execute(request: Uint8Array, options?: QueryOptions): Promise<Uint8Array>;
  surface(options?: QueryOptions): Promise<Uint8Array>;
  setQuota(bytes: number): Promise<void>;
  close(): Promise<void>;
}
