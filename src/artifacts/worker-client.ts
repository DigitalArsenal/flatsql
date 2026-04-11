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

function encodeCacheValue(value: unknown): string | null {
  if (value === null) {
    return 'l';
  }

  switch (typeof value) {
    case 'string':
      return `s:${value.length}:${value}`;
    case 'number':
      return `n:${Object.is(value, -0) ? '-0' : String(value)}`;
    case 'bigint':
      return `i:${value.toString()}`;
    case 'boolean':
      return value ? 'b:1' : 'b:0';
    case 'undefined':
      return 'u';
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
    return `${sql}\u0000a:${encoded.join('\u0001')}`;
  }

  const prototype = Object.getPrototypeOf(params);
  if (prototype !== Object.prototype && prototype !== null) {
    return null;
  }

  const namedParams = params as Record<string, unknown>;
  const encodedEntries: string[] = [];
  const keys = Object.keys(namedParams).sort();
  for (const key of keys) {
    const encodedValue = encodeCacheValue(namedParams[key]);
    if (encodedValue === null) {
      return null;
    }
    encodedEntries.push(`${key}\u0002${encodedValue}`);
  }
  return `${sql}\u0000o:${encodedEntries.join('\u0001')}`;
}

export class FlatSQLArtifactWorkerClient {
  private readonly workerPath: URL;
  private worker: Worker | null = null;
  private nextId = 1;
  private readonly pending = new Map<number, PendingCall>();

  constructor(workerPath: URL = new URL('../../wasm/flatsql-artifact.worker.js', import.meta.url)) {
    this.workerPath = workerPath;
  }

  async init(): Promise<void> {
    if (this.worker) {
      return;
    }

    this.worker = new Worker(this.workerPath);

    await new Promise<void>((resolve, reject) => {
      const handleMessage = (message: WorkerMessage) => {
        if (message.type === 'ready') {
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

      this.worker!.on('message', handleMessage);
      this.worker!.on('error', reject);
      this.worker!.on('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`Artifact worker exited with code ${code}`));
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

    await this.worker.terminate();
    this.worker = null;
  }

  async call(method: string, params: Record<string, unknown>): Promise<any> {
    if (!this.worker) {
      throw new Error('Artifact worker client is not initialized');
    }

    const id = this.nextId++;
    return await new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker!.postMessage({ id, method, params });
    });
  }
}

export class FlatSQLArtifactWorkerBuilder {
  private readonly client: FlatSQLArtifactWorkerClient;
  private readonly builderId: string;
  private readonly schema: DatabaseSchema;
  private readonly preferSharedArrayBuffer: boolean;
  private readonly queryResultCache = new Map<string, ArtifactQueryResult>();

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
    this.queryResultCache.clear();
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
      buffers: buffers.map((buffer) => Array.from(buffer)),
      options,
    });
  }

  async query(sql: string, params?: ArtifactQueryParams): Promise<ArtifactQueryResult> {
    const resultCacheKey = buildResultCacheKey(sql, params);
    const cached = resultCacheKey ? this.queryResultCache.get(resultCacheKey) : undefined;
    if (cached) {
      return cloneQueryResult(cached);
    }

    const result = await this.client.call('query', { builderId: this.builderId, sql, params }) as ArtifactQueryResult;
    if (resultCacheKey) {
      this.queryResultCache.set(resultCacheKey, result);
      return cloneQueryResult(result);
    }
    return result;
  }

  async queryMany(queries: readonly ArtifactQuerySpec[]): Promise<ArtifactQueryResult[]> {
    if (queries.length === 0) {
      return [];
    }

    const results: ArtifactQueryResult[] = new Array(queries.length);
    const uncachedQueries: ArtifactQuerySpec[] = [];
    const uncachedIndices: number[] = [];
    const uncachedCacheKeys: Array<string | null> = [];

    for (let index = 0; index < queries.length; index += 1) {
      const query = queries[index];
      const resultCacheKey = buildResultCacheKey(query.sql, query.params);
      const cached = resultCacheKey ? this.queryResultCache.get(resultCacheKey) : undefined;
      if (cached) {
        results[index] = cloneQueryResult(cached);
        continue;
      }

      uncachedQueries.push(query);
      uncachedIndices.push(index);
      uncachedCacheKeys.push(resultCacheKey);
    }

    if (uncachedQueries.length === 0) {
      return results;
    }

    const freshResults = await this.client.call('queryMany', {
      builderId: this.builderId,
      queries: uncachedQueries,
    }) as ArtifactQueryResult[];

    for (let index = 0; index < freshResults.length; index += 1) {
      const result = freshResults[index];
      const resultIndex = uncachedIndices[index];
      const resultCacheKey = uncachedCacheKeys[index];
      if (resultCacheKey) {
        this.queryResultCache.set(resultCacheKey, result);
        results[resultIndex] = cloneQueryResult(result);
      } else {
        results[resultIndex] = result;
      }
    }

    return results;
  }

  async close(): Promise<void> {
    this.queryResultCache.clear();
    await this.client.call('closeBuilder', { builderId: this.builderId });
  }
}
