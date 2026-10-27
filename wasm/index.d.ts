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
  backend?: 'opfs' | 'memory';
  workerUrl?: string | URL;
  threadWorkerUrl?: string | URL;
  ioWorkerUrl?: string | URL;
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
  maxRows?: number;
  fullText?: boolean;
}
export interface IngestOptions extends QueryOptions {
  provider?: string;
  source?: string;
  producer?: string;
  batch?: string;
  timestamp?: number;
}
export class FlatSQL {
  static open(options?: FlatSQLOptions): Promise<FlatSQL>;
  registerSchema(schema: BinarySchema): Promise<void>;
  /** Frames are [u32 little-endian length][FlatBuffer payload], concatenated. */
  ingest(type: string, frames: Uint8Array, options?: IngestOptions): Promise<Uint8Array>;
  /** QRP request in; chunks of a size-prefixed QRP result stream out. */
  query(request: Uint8Array, options?: QueryOptions): AsyncGenerator<Uint8Array>;
  execute(request: Uint8Array, options?: QueryOptions): Promise<Uint8Array>;
  surface(options?: QueryOptions): Promise<Uint8Array>;
  setQuota(bytes: number): Promise<void>;
  close(): Promise<void>;
}
