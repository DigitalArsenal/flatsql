import { Worker } from 'node:worker_threads';

import { parseSchema, type DatabaseSchema } from '../schema/index.js';
import {
  sizePrefixedByteLength,
  writeSizePrefixedStream,
} from './transport.js';
import type {
  ArtifactBuilderOptions,
  ArtifactIngestOptions,
  ArtifactIngestResult,
  ArtifactQueryParams,
  ArtifactQuerySpec,
  ArtifactQueryResult,
  ArtifactTransportMode,
  ArtifactWorkerBuilderOptions,
} from './types.js';

const schemaCache = new Map<string, DatabaseSchema>();
const MAX_PENDING_CALLS = 1024;
const MAX_QUERY_RESULT_CACHE_ENTRIES = 1024;
const MAX_QUERY_RESULT_CACHE_ROWS = 1000;
const VOLATILE_QUERY_PATTERNS = [
  /\bRANDOM\s*\(/i,
  /\bCHANGES\s*\(/i,
  /\bTOTAL_CHANGES\s*\(/i,
  /\bLAST_INSERT_ROWID\s*\(/i,
  /\bCURRENT_(TIME|DATE|TIMESTAMP)\b/i,
  /\bDATE\s*\(/i,
  /\bTIME\s*\(/i,
  /\bDATETIME\s*\(/i,
  /\bJULIANDAY\s*\(/i,
  /\bUNIXEPOCH\s*\(/i,
  /\bSTRFTIME\s*\(/i,
];

interface PendingCall {
  resolve: (value: any) => void;
  reject: (error: Error) => void;
}

interface WorkerMessage {
  id?: number;
  type?: string;
  success?: boolean;
  result?: any;
  error?: string;
}

interface Deferred<T> {
  resolve: (value: T) => void;
  reject: (error: Error) => void;
  promise: Promise<T>;
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { resolve, reject, promise };
}

function supportsSharedArrayBuffer(): boolean {
  return typeof SharedArrayBuffer !== 'undefined' && typeof Atomics !== 'undefined';
}

function isCacheableQuerySql(sql: string): boolean {
  const trimmed = sql.trimStart();
  if (trimmed.length === 0) {
    return false;
  }

  const upper = trimmed.toUpperCase();
  if (upper.startsWith('SELECT') || upper.startsWith('WITH')) {
    return true;
  }

  return upper.startsWith('PRAGMA') && !trimmed.includes('=');
}

function isResultCacheableQuerySql(sql: string): boolean {
  return isCacheableQuerySql(sql) && !VOLATILE_QUERY_PATTERNS.some((pattern) => pattern.test(sql));
}

function cloneQueryResult(result: ArtifactQueryResult): ArtifactQueryResult {
  return {
    columns: [...result.columns],
    rows: result.rows.map((row) => [...row]),
    rowCount: result.rowCount,
  };
}

function encodeCacheValue(value: unknown): Record<string, unknown> | null {
  if (value === null) {
    return { type: 'null' };
  }

  switch (typeof value) {
    case 'string':
      return { type: 'string', value };
    case 'number':
      return { type: 'number', value: Object.is(value, -0) ? '-0' : String(value) };
    case 'bigint':
      return { type: 'bigint', value: value.toString() };
    case 'boolean':
      return { type: 'boolean', value };
    case 'undefined':
      return { type: 'undefined' };
    default:
      return null;
  }
}

function buildResultCacheKey(sql: string, params: ArtifactQueryParams | undefined): string | null {
  if (!isResultCacheableQuerySql(sql)) {
    return null;
  }

  if (params === undefined) {
    return sql;
  }

  if (Array.isArray(params)) {
    const encoded = params.map(encodeCacheValue);
    if (encoded.some((value) => value === null)) {
      return null;
    }
    return JSON.stringify({ sql, params: { type: 'array', values: encoded } });
  }

  const prototype = Object.getPrototypeOf(params);
  if (prototype !== Object.prototype && prototype !== null) {
    return null;
  }

  const namedParams = params as Record<string, unknown>;
  const encodedEntries: Array<{ key: string; value: Record<string, unknown> }> = [];
  const keys = Object.keys(namedParams).sort();
  for (const key of keys) {
    const encodedValue = encodeCacheValue(namedParams[key]);
    if (encodedValue === null) {
      return null;
    }
    encodedEntries.push({ key, value: encodedValue });
  }
  return JSON.stringify({ sql, params: { type: 'object', entries: encodedEntries } });
}

function getCachedResult(cache: Map<string, ArtifactQueryResult>, key: string): ArtifactQueryResult | undefined {
  const cached = cache.get(key);
  if (!cached) {
    return undefined;
  }
  cache.delete(key);
  cache.set(key, cached);
  return cached;
}

function setCachedResult(cache: Map<string, ArtifactQueryResult>, key: string, result: ArtifactQueryResult): void {
  if (result.rowCount > MAX_QUERY_RESULT_CACHE_ROWS) {
    cache.delete(key);
    return;
  }

  cache.set(key, cloneQueryResult(result));
  while (cache.size > MAX_QUERY_RESULT_CACHE_ENTRIES) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) {
      break;
    }
    cache.delete(oldest);
  }
}

export class FlatSQLArtifactWorkerClient {
  private readonly workerPath: URL;
  private worker: Worker | null = null;
  private nextId = 1;
  private readonly pending = new Map<number, PendingCall>();
  private closed = false;

  constructor(workerPath: URL = new URL('../../wasm/flatsql-artifact.worker.js', import.meta.url)) {
    this.workerPath = workerPath;
  }

  async init(): Promise<void> {
    if (this.worker) {
      return;
    }

    this.closed = false;
    const worker = new Worker(this.workerPath);
    this.worker = worker;

    await new Promise<void>((resolve, reject) => {
      let ready = false;
      let settled = false;
      const rejectInit = (error: Error) => {
        if (!settled) {
          settled = true;
          reject(error);
        }
      };
      const handleMessage = (message: WorkerMessage) => {
        if (message.type === 'ready') {
          ready = true;
          settled = true;
          resolve();
          return;
        }

        if (typeof message.id !== 'number') {
          return;
        }

        const pending = this.pending.get(message.id);
        if (!pending) {
          return;
        }

        this.pending.delete(message.id);
        if (message.success) {
          pending.resolve(message.result);
        } else {
          pending.reject(new Error(message.error ?? 'Artifact worker failed'));
        }
      };

      const handleFailure = (error: Error) => {
        if (this.worker === worker) {
          this.worker = null;
          this.closed = true;
        }
        this.rejectAllPending(error);
        if (!ready) {
          rejectInit(error);
        }
      };

      worker.on('message', handleMessage);
      worker.on('error', handleFailure);
      worker.on('exit', (code) => {
        if (this.worker !== worker) {
          return;
        }
        this.worker = null;
        this.closed = true;
        if (code !== 0) {
          handleFailure(new Error(`Artifact worker exited with code ${code}`));
        } else {
          this.rejectAllPending(new Error('Artifact worker exited'));
        }
      });
    });
  }

  async createBuilder(schemaSource: string, options: ArtifactWorkerBuilderOptions): Promise<FlatSQLArtifactWorkerBuilder> {
    const schemaName = options.name ?? 'artifact';
    const cacheKey = `${schemaName}\u0000${schemaSource}`;
    let schema = schemaCache.get(cacheKey);
    if (!schema) {
      schema = parseSchema(schemaSource, schemaName);
      schemaCache.set(cacheKey, schema);
    }
    const builderId = `artifact_${this.nextId++}`;
    await this.call('createBuilder', {
      builderId,
      schema,
      performanceProfile: options.performanceProfile ?? 'fast',
      sqlitePath: options.sqlitePath,
    });
    return new FlatSQLArtifactWorkerBuilder(this, builderId, schema, options);
  }

  async close(): Promise<void> {
    if (!this.worker) {
      return;
    }

    const worker = this.worker;
    this.worker = null;
    this.closed = true;
    this.rejectAllPending(new Error('Artifact worker client closed'));
    await worker.terminate();
  }

  private rejectAllPending(error: Error): void {
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
  }

  async call(method: string, params: Record<string, unknown>): Promise<any> {
    if (!this.worker || this.closed) {
      throw new Error('Artifact worker client is not initialized');
    }
    if (this.pending.size >= MAX_PENDING_CALLS) {
      throw new Error(`Artifact worker has too many pending calls (${MAX_PENDING_CALLS})`);
    }

    const id = this.nextId++;
    return await new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      try {
        this.worker!.postMessage({ id, method, params });
      } catch (error) {
        this.pending.delete(id);
        reject(error);
      }
    });
  }
}

export class FlatSQLArtifactWorkerBuilder {
  private readonly client: FlatSQLArtifactWorkerClient;
  private readonly builderId: string;
  private readonly schema: DatabaseSchema;
  private readonly preferSharedArrayBuffer: boolean;
  private readonly queryResultCache = new Map<string, ArtifactQueryResult>();
  private readonly inFlightQueries = new Map<string, Promise<ArtifactQueryResult>>();
  private queryGeneration = 0;

  constructor(
    client: FlatSQLArtifactWorkerClient,
    builderId: string,
    schema: DatabaseSchema,
    options: ArtifactWorkerBuilderOptions
  ) {
    this.client = client;
    this.builderId = builderId;
    this.schema = schema;
    this.preferSharedArrayBuffer = options.preferSharedArrayBuffer ?? true;
  }

  async registerFileId(fileId: string, tableName: string): Promise<void> {
    await this.client.call('registerFileId', { builderId: this.builderId, fileId, tableName });
  }

  async enableDemoExtractors(): Promise<void> {
    await this.client.call('enableDemoExtractors', { builderId: this.builderId });
  }

  async ingestBuffers(buffers: Uint8Array[], options: ArtifactIngestOptions = {}): Promise<ArtifactIngestResult> {
    this.queryGeneration += 1;
    this.queryResultCache.clear();
    this.inFlightQueries.clear();
    const canUseShared = this.preferSharedArrayBuffer && supportsSharedArrayBuffer();
    if (canUseShared) {
      const byteLength = sizePrefixedByteLength(buffers);
      const sharedBuffer = new SharedArrayBuffer(byteLength);
      writeSizePrefixedStream(new Uint8Array(sharedBuffer), buffers);
      return await this.client.call('ingestShared', {
        builderId: this.builderId,
        sharedBuffer,
        byteLength,
        options,
      });
    }

    return await this.client.call('ingestClone', {
      builderId: this.builderId,
      buffers,
      options,
    });
  }

  async query(sql: string, params?: ArtifactQueryParams): Promise<ArtifactQueryResult> {
    const resultCacheKey = buildResultCacheKey(sql, params);
    const cached = resultCacheKey ? getCachedResult(this.queryResultCache, resultCacheKey) : undefined;
    if (cached) {
      return cloneQueryResult(cached);
    }

    if (!resultCacheKey) {
      return await this.client.call('query', { builderId: this.builderId, sql, params }) as ArtifactQueryResult;
    }

    const existing = this.inFlightQueries.get(resultCacheKey);
    if (existing) {
      return cloneQueryResult(await existing);
    }

    const generation = this.queryGeneration;
    const promise = (async () => {
      const result = await this.client.call('query', { builderId: this.builderId, sql, params }) as ArtifactQueryResult;
      if (generation === this.queryGeneration) {
        setCachedResult(this.queryResultCache, resultCacheKey, result);
      }
      return result;
    })();
    this.inFlightQueries.set(resultCacheKey, promise);

    try {
      return cloneQueryResult(await promise);
    } finally {
      if (this.inFlightQueries.get(resultCacheKey) === promise) {
        this.inFlightQueries.delete(resultCacheKey);
      }
    }
  }

  async queryMany(queries: readonly ArtifactQuerySpec[]): Promise<ArtifactQueryResult[]> {
    if (queries.length === 0) {
      return [];
    }

    const results: ArtifactQueryResult[] = new Array(queries.length);
    const uncachedEntries: Array<{
      query: ArtifactQuerySpec;
      indices: number[];
      cacheKey: string | null;
      deferred?: Deferred<ArtifactQueryResult>;
    }> = [];
    const uncachedByKey = new Map<string, typeof uncachedEntries[number]>();
    const pendingExisting: Array<{ index: number; promise: Promise<ArtifactQueryResult> }> = [];

    for (let index = 0; index < queries.length; index += 1) {
      const query = queries[index];
      const resultCacheKey = buildResultCacheKey(query.sql, query.params);
      const cached = resultCacheKey ? getCachedResult(this.queryResultCache, resultCacheKey) : undefined;
      if (cached) {
        results[index] = cloneQueryResult(cached);
        continue;
      }

      if (resultCacheKey) {
        const existing = this.inFlightQueries.get(resultCacheKey);
        if (existing) {
          pendingExisting.push({ index, promise: existing });
          continue;
        }

        const duplicate = uncachedByKey.get(resultCacheKey);
        if (duplicate) {
          duplicate.indices.push(index);
          continue;
        }
      }

      const entry = { query, indices: [index], cacheKey: resultCacheKey };
      uncachedEntries.push(entry);
      if (resultCacheKey) {
        uncachedByKey.set(resultCacheKey, entry);
      }
    }

    if (uncachedEntries.length === 0 && pendingExisting.length === 0) {
      return results;
    }

    const generation = this.queryGeneration;
    for (const entry of uncachedEntries) {
      if (!entry.cacheKey) {
        continue;
      }
      const deferred = createDeferred<ArtifactQueryResult>();
      entry.deferred = deferred;
      this.inFlightQueries.set(entry.cacheKey, deferred.promise);
    }

    try {
      if (uncachedEntries.length > 0) {
        const freshResults = await this.client.call('queryMany', {
          builderId: this.builderId,
          queries: uncachedEntries.map((entry) => entry.query),
        }) as ArtifactQueryResult[];

        for (let index = 0; index < freshResults.length; index += 1) {
          const result = freshResults[index];
          const entry = uncachedEntries[index];
          if (entry.cacheKey && generation === this.queryGeneration) {
            setCachedResult(this.queryResultCache, entry.cacheKey, result);
          }
          entry.deferred?.resolve(result);
          for (const resultIndex of entry.indices) {
            results[resultIndex] = entry.cacheKey ? cloneQueryResult(result) : result;
          }
        }
      }

      await Promise.all(
        pendingExisting.map(async ({ index, promise }) => {
          results[index] = cloneQueryResult(await promise);
        })
      );
    } catch (error) {
      for (const entry of uncachedEntries) {
        entry.deferred?.reject(error instanceof Error ? error : new Error(String(error)));
      }
      throw error;
    } finally {
      for (const entry of uncachedEntries) {
        if (entry.cacheKey && this.inFlightQueries.get(entry.cacheKey) === entry.deferred?.promise) {
          this.inFlightQueries.delete(entry.cacheKey);
        }
      }
    }

    return results;
  }

  async close(): Promise<void> {
    this.queryResultCache.clear();
    this.inFlightQueries.clear();
    await this.client.call('closeBuilder', { builderId: this.builderId });
  }
}
