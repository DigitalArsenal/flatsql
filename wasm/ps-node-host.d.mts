// Types for wasm/ps-node-host.mjs (the partition store's Node wasi-threads host).

export interface PsThreadReport {
  /** wasi.thread-spawn calls that started a guest thread. */
  guestThreadsSpawned: number;
  /** Most guest threads running at once, the guest's own thread included. */
  maxConcurrentGuestThreads: number;
  runningGuestThreads: number;
  spawnsDeclined: number;
  /** Node worker threads (OS threads) that ran guest code. */
  workerThreads: number;
  distinctWorkerThreadIds: number;
  memoryBytes: number;
}

export interface PsMount {
  guest: string;
  host: string;
}

export interface PsHostOptions {
  /** Host directory the guest sees as "/" (WASI and flatsql_io). Default: a private temporary directory, removed afterwards. */
  root?: string;
  keepRoot?: boolean;
  /** Further preopened directories. */
  mounts?: PsMount[];
  env?: Record<string, string>;
  /** Pool cap on guest threads (default 512). */
  maxThreads?: number;
  /** Pool workers started up front (default 16). */
  warmThreads?: number;
  /** How long a spawn waits for a free worker before failing with EAGAIN (default 60000). */
  spawnWaitMs?: number;
  /** Fault overlay (ps-node-fault.mjs createFaultOverlay). */
  fault?: { buffer: SharedArrayBuffer };
  stdoutFd?: number;
  stderrFd?: number;
  /** Seeded random_get. */
  deterministic?: { randomSeed?: bigint };
  initialPages?: number;
  maximumPages?: number;
  maxHandles?: number;
  instanceId?: number;
}

export interface PsCommandOptions extends PsHostOptions {
  args?: string[];
  timeoutMs?: number;
  /** Kill the guest after this long: a crash (result.killed), not a failure. */
  killAfterMs?: number;
  beforeKill?: () => void;
}

export interface PsCommandResult {
  exitCode: number;
  killed: boolean;
  timedOut: boolean;
  fault: boolean;
  error: string | null;
  elapsedMs: number;
  threads: PsThreadReport;
  root: string;
  poolErrors: string[];
}

export function runPsCommand(wasm: string | Uint8Array, options?: PsCommandOptions): Promise<PsCommandResult>;

export interface PsNodeInstance {
  memory: WebAssembly.Memory;
  root: string;
  /** Call an export of the instance on its exec thread. */
  call(fn: string, ...args: number[]): Promise<number>;
  /** Copy bytes into instance memory (flatsql_ps_alloc); returns the pointer. */
  write(bytes: Uint8Array | ArrayBuffer): Promise<number>;
  read(ptr: number, len: number): Promise<Uint8Array>;
  threads(): PsThreadReport;
  faulted(): boolean;
  close(): Promise<void>;
}

/** flatsql-ps-threads.wasm as one instance (writer or reader) with its own exec thread and thread pool. */
export function createPsNodeInstance(wasm: string | Uint8Array, options?: PsHostOptions): Promise<PsNodeInstance>;

export function importedMemoryLimits(
  bytes: Uint8Array | ArrayBuffer,
): { module: string; name: string; initial: number; maximum?: number; shared: boolean } | null;
