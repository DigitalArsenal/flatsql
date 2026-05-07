import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const OP_CREATE_DB = 1;
const OP_DESTROY_DB = 2;
const OP_REGISTER_FILE_ID = 3;
const OP_ENABLE_DEMO_EXTRACTORS = 4;
const OP_INGEST = 5;
const OP_QUERY = 6;
const OP_QUERY_MANY = 7;
const OP_REGISTER_QUERY_TEMPLATE = 8;
const OP_QUERY_TEMPLATE = 9;
const OP_CLEAR_QUERY_CACHE = 10;
const OP_QUERY_CACHE_STATS = 11;
const OP_GET_FLATBUFFER_BY_INDEX = 12;
const OP_EXPORT_DATA = 13;
const OP_LOAD_AND_REBUILD = 14;
const OP_BUILD_RESPONSE_ARTIFACT_CACHE_KEY = 15;
const OP_QUERY_RAW_FLATBUFFER_STREAM = 16;
const OP_RESERVE_STORAGE = 17;
const OP_LOAD_FROM_DB = 18;
const OP_CONFIGURE_QUERY_CACHE = 19;

const PARAM_NULL = 0;
const PARAM_BOOL = 1;
const PARAM_INT64 = 2;
const PARAM_FLOAT64 = 3;
const PARAM_STRING = 4;
const PARAM_BYTES = 5;

type StandaloneQueryParam = null | boolean | number | string | Uint8Array;

interface QueryResult {
  columns: string[];
  rows: unknown[][];
}

export interface FlatSQLWasmEdgeProcessRuntimeOptions {
  runnerPath: string;
  wasmPath?: string;
  env?: NodeJS.ProcessEnv;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

class BinaryWriter {
  private readonly chunks: Uint8Array[] = [];
  private size = 0;

  u8(value: number): void {
    this.push(Uint8Array.of(value & 0xff));
  }

  u32(value: number): void {
    const bytes = new Uint8Array(4);
    new DataView(bytes.buffer).setUint32(0, value >>> 0, true);
    this.push(bytes);
  }

  f64(value: number): void {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setFloat64(0, value, true);
    this.push(bytes);
  }

  string(value: string): void {
    this.bytes(textEncoder.encode(value));
  }

  bytes(value: Uint8Array): void {
    this.u32(value.length);
    this.push(value);
  }

  finish(): Uint8Array {
    const out = new Uint8Array(this.size);
    let offset = 0;
    for (const chunk of this.chunks) {
      out.set(chunk, offset);
      offset += chunk.length;
    }
    return out;
  }

  private push(value: Uint8Array): void {
    this.chunks.push(value);
    this.size += value.length;
  }
}

class BinaryReader {
  private offset = 0;
  private readonly data: Uint8Array;

  constructor(data: Uint8Array) {
    this.data = new Uint8Array(data);
  }

  u8(): number {
    this.require(1);
    return this.data[this.offset++];
  }

  u32(): number {
    this.require(4);
    const value = new DataView(this.data.buffer, this.data.byteOffset + this.offset, 4).getUint32(0, true);
    this.offset += 4;
    return value;
  }

  f64(): number {
    this.require(8);
    const value = new DataView(this.data.buffer, this.data.byteOffset + this.offset, 8).getFloat64(0, true);
    this.offset += 8;
    return value;
  }

  string(): string {
    return textDecoder.decode(this.bytes());
  }

  bytes(): Uint8Array {
    const length = this.u32();
    this.require(length);
    const value = this.data.slice(this.offset, this.offset + length);
    this.offset += length;
    return value;
  }

  private require(length: number): void {
    if (this.offset > this.data.length || length > this.data.length - this.offset) {
      throw new Error('Malformed FlatSQL WasmEdge runner response.');
    }
  }
}

function encodeQueryParams(params: readonly StandaloneQueryParam[] = []): Uint8Array {
  const parts: Uint8Array[] = [];
  let total = 0;

  for (const value of params) {
    let tag: number;
    let payload: Uint8Array;

    if (value === null) {
      tag = PARAM_NULL;
      payload = new Uint8Array();
    } else if (typeof value === 'boolean') {
      tag = PARAM_BOOL;
      payload = Uint8Array.of(value ? 1 : 0);
    } else if (typeof value === 'number' && Number.isInteger(value)) {
      if (!Number.isSafeInteger(value)) {
        throw new TypeError('Integer query parameters must be safe integers');
      }
      tag = PARAM_INT64;
      payload = new Uint8Array(8);
      new DataView(payload.buffer).setBigInt64(0, BigInt(value), true);
    } else if (typeof value === 'number') {
      tag = PARAM_FLOAT64;
      payload = new Uint8Array(8);
      new DataView(payload.buffer).setFloat64(0, value, true);
    } else if (typeof value === 'string') {
      tag = PARAM_STRING;
      payload = textEncoder.encode(value);
    } else if (value instanceof Uint8Array) {
      tag = PARAM_BYTES;
      payload = value;
    } else {
      throw new TypeError(`Unsupported query parameter type: ${typeof value}`);
    }

    const header = new Uint8Array(5);
    const view = new DataView(header.buffer);
    view.setUint8(0, tag);
    view.setUint32(1, payload.length, true);
    parts.push(header, payload);
    total += header.length + payload.length;
  }

  const encoded = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    encoded.set(part, offset);
    offset += part.length;
  }
  return encoded;
}

function encodeQueryRequests(queries: Array<{ sql: string; params?: StandaloneQueryParam[] }>): Uint8Array {
  const parts: Uint8Array[] = [];
  let total = 0;

  for (const query of queries) {
    const sqlBytes = textEncoder.encode(query.sql);
    const params = query.params ?? [];
    const paramBytes = encodeQueryParams(params);
    const header = new Uint8Array(12);
    const view = new DataView(header.buffer);
    view.setUint32(0, sqlBytes.length, true);
    view.setUint32(4, params.length, true);
    view.setUint32(8, paramBytes.length, true);
    parts.push(header, sqlBytes, paramBytes);
    total += header.length + sqlBytes.length + paramBytes.length;
  }

  const encoded = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    encoded.set(part, offset);
    offset += part.length;
  }
  return encoded;
}

function buildSizePrefixedStream(buffers: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const buffer of buffers) {
    total += 4 + buffer.length;
  }

  const stream = new Uint8Array(total);
  let offset = 0;
  for (const buffer of buffers) {
    new DataView(stream.buffer, offset, 4).setUint32(0, buffer.length, true);
    offset += 4;
    stream.set(buffer, offset);
    offset += buffer.length;
  }
  return stream;
}

function defaultWasmPath(): string {
  return fileURLToPath(new URL('../../wasm/flatsql-wasi.wasm', import.meta.url));
}

function writeRequest(opcode: number, writeBody: (writer: BinaryWriter) => void): Uint8Array {
  const writer = new BinaryWriter();
  writer.u32(opcode);
  writeBody(writer);
  return writer.finish();
}

function readQueryResult(reader: BinaryReader): QueryResult {
  const columnCount = reader.u32();
  const columns: string[] = [];
  for (let index = 0; index < columnCount; index++) {
    columns.push(reader.string());
  }

  const rowCount = reader.u32();
  const rows: unknown[][] = [];
  for (let row = 0; row < rowCount; row++) {
    const values: unknown[] = [];
    for (let column = 0; column < columnCount; column++) {
      const type = reader.u8();
      switch (type) {
        case 0:
          values.push(null);
          break;
        case 1:
          values.push(reader.u8() !== 0);
          break;
        case 2:
        case 3:
        case 4:
          values.push(reader.f64());
          break;
        case 5:
          values.push(reader.string());
          break;
        case 6:
          values.push(reader.bytes());
          break;
        default:
          throw new Error(`Unsupported FlatSQL WasmEdge cell type: ${type}`);
      }
    }
    rows.push(values);
  }
  return { columns, rows };
}

class FlatSQLWasmEdgeProcess {
  private readonly child: ChildProcessWithoutNullStreams;
  private readonly pending: Array<{
    resolve: (reader: BinaryReader) => void;
    reject: (error: Error) => void;
  }> = [];
  private stdout = Buffer.alloc(0);
  private stderr = '';
  private closed = false;

  constructor(options: FlatSQLWasmEdgeProcessRuntimeOptions) {
    this.child = spawn(options.runnerPath, [options.wasmPath ?? defaultWasmPath()], {
      env: {
        ...process.env,
        ...(options.env ?? {}),
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    this.child.stdout.on('data', (chunk: Buffer) => {
      this.stdout = Buffer.concat([this.stdout, chunk]);
      this.drainStdout();
    });

    this.child.stderr.on('data', (chunk: Buffer) => {
      this.stderr += chunk.toString('utf8');
    });

    this.child.on('error', (error) => {
      this.rejectAll(error instanceof Error ? error : new Error(String(error)));
    });

    this.child.on('exit', (code, signal) => {
      this.closed = true;
      if (this.pending.length > 0) {
        this.rejectAll(
          new Error(`FlatSQL WasmEdge runner exited with code ${code ?? 'null'} signal ${signal ?? 'null'}: ${this.stderr}`)
        );
      }
    });
  }

  request(payload: Uint8Array): Promise<BinaryReader> {
    if (this.closed) {
      return Promise.reject(new Error('FlatSQL WasmEdge runner is closed.'));
    }

    return new Promise((resolve, reject) => {
      const pendingRequest = { resolve, reject };
      this.pending.push(pendingRequest);
      const header = Buffer.allocUnsafe(4);
      header.writeUInt32LE(payload.length, 0);
      this.child.stdin.write(Buffer.concat([header, Buffer.from(payload)]), (error) => {
        if (error) {
          const index = this.pending.indexOf(pendingRequest);
          if (index >= 0) {
            this.pending.splice(index, 1);
          }
          reject(error);
        }
      });
    });
  }

  close(): Promise<void> {
    if (this.closed) {
      return Promise.resolve();
    }
    this.closed = true;
    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        this.child.kill('SIGKILL');
        resolve();
      }, 1000);
      this.child.once('exit', () => {
        clearTimeout(timeout);
        resolve();
      });
      this.child.stdin.end();
      this.child.kill();
    });
  }

  private drainStdout(): void {
    while (this.stdout.length >= 4) {
      const length = this.stdout.readUInt32LE(0);
      if (this.stdout.length < 4 + length) {
        return;
      }
      const payload = this.stdout.subarray(4, 4 + length);
      this.stdout = this.stdout.subarray(4 + length);

      const pending = this.pending.shift();
      if (!pending) {
        continue;
      }

      try {
        const reader = new BinaryReader(payload);
        const ok = reader.u8();
        if (ok === 0) {
          pending.reject(new Error(reader.string()));
        } else {
          pending.resolve(reader);
        }
      } catch (error) {
        pending.reject(error instanceof Error ? error : new Error(String(error)));
      }
    }
  }

  private rejectAll(error: Error): void {
    while (this.pending.length > 0) {
      this.pending.shift()?.reject(error);
    }
  }
}

export class FlatSQLWasmEdgeProcessRuntime {
  private readonly process: FlatSQLWasmEdgeProcess;

  constructor(options: FlatSQLWasmEdgeProcessRuntimeOptions) {
    this.process = new FlatSQLWasmEdgeProcess(options);
  }

  async createDatabase(schema: string, dbName = 'default'): Promise<FlatSQLWasmEdgeProcessDatabase> {
    const reader = await this.process.request(
      writeRequest(OP_CREATE_DB, (writer) => {
        writer.string(schema);
        writer.string(dbName);
      })
    );
    return new FlatSQLWasmEdgeProcessDatabase(this.process, reader.u32());
  }

  close(): Promise<void> {
    return this.process.close();
  }

  async buildResponseArtifactCacheKey(
    schemaName: string,
    schemaVersion: string,
    sql: string,
    options: {
      format?: string;
      publishEventKey?: string | null;
      projection?: readonly string[];
      params?: StandaloneQueryParam[];
    } = {}
  ): Promise<string> {
    const params = options.params ?? [];
    const paramBytes = encodeQueryParams(params);
    const reader = await this.process.request(
      writeRequest(OP_BUILD_RESPONSE_ARTIFACT_CACHE_KEY, (writer) => {
        writer.string(schemaName);
        writer.string(schemaVersion);
        writer.string(sql);
        writer.string(options.format ?? 'json');
        writer.string(options.publishEventKey ?? '');
        writer.u32(options.projection?.length ?? 0);
        for (const column of options.projection ?? []) {
          writer.string(column);
        }
        writer.u32(params.length);
        writer.bytes(paramBytes);
      })
    );
    return reader.string();
  }
}

export class FlatSQLWasmEdgeProcessDatabase {
  constructor(
    private readonly process: FlatSQLWasmEdgeProcess,
    private handle: number
  ) {}

  async destroy(): Promise<void> {
    if (this.handle === 0) {
      return;
    }
    const handle = this.handle;
    this.handle = 0;
    await this.process.request(writeRequest(OP_DESTROY_DB, (writer) => writer.u32(handle)));
  }

  async registerFileId(fileId: string, tableName: string): Promise<void> {
    await this.process.request(
      writeRequest(OP_REGISTER_FILE_ID, (writer) => {
        writer.u32(this.handle);
        writer.string(fileId);
        writer.string(tableName);
      })
    );
  }

  async enableDemoExtractors(): Promise<void> {
    await this.process.request(writeRequest(OP_ENABLE_DEMO_EXTRACTORS, (writer) => writer.u32(this.handle)));
  }

  async ingest(data: Uint8Array, source?: string | null): Promise<number> {
    const reader = await this.process.request(
      writeRequest(OP_INGEST, (writer) => {
        writer.u32(this.handle);
        writer.u8(source ? 1 : 0);
        if (source) {
          writer.string(source);
        }
        writer.bytes(data);
      })
    );
    return reader.f64();
  }

  ingestBuffers(buffers: Uint8Array[], source?: string | null): Promise<number> {
    return this.ingest(buildSizePrefixedStream(buffers), source);
  }

  ingestOne(data: Uint8Array, source?: string | null): Promise<number> {
    return this.ingest(data, source);
  }

  async query(sql: string, params: StandaloneQueryParam[] = []): Promise<QueryResult> {
    const paramBytes = encodeQueryParams(params);
    const reader = await this.process.request(
      writeRequest(OP_QUERY, (writer) => {
        writer.u32(this.handle);
        writer.string(sql);
        writer.u32(params.length);
        writer.bytes(paramBytes);
      })
    );
    return readQueryResult(reader);
  }

  async queryMany(queries: Array<{ sql: string; params?: StandaloneQueryParam[] }>): Promise<QueryResult[]> {
    const requestBytes = encodeQueryRequests(queries);
    const reader = await this.process.request(
      writeRequest(OP_QUERY_MANY, (writer) => {
        writer.u32(this.handle);
        writer.u32(queries.length);
        writer.bytes(requestBytes);
      })
    );
    const count = reader.u32();
    const results: QueryResult[] = [];
    for (let index = 0; index < count; index++) {
      results.push(readQueryResult(reader));
    }
    return results;
  }

  async queryRawFlatBufferStream(sql: string, params: StandaloneQueryParam[] = []): Promise<Uint8Array> {
    const paramBytes = encodeQueryParams(params);
    const reader = await this.process.request(
      writeRequest(OP_QUERY_RAW_FLATBUFFER_STREAM, (writer) => {
        writer.u32(this.handle);
        writer.string(sql);
        writer.u32(params.length);
        writer.bytes(paramBytes);
      })
    );
    return reader.bytes();
  }

  async registerQueryTemplate(queryId: string, sql: string, cacheable = true): Promise<void> {
    await this.process.request(
      writeRequest(OP_REGISTER_QUERY_TEMPLATE, (writer) => {
        writer.u32(this.handle);
        writer.string(queryId);
        writer.string(sql);
        writer.u8(cacheable ? 1 : 0);
      })
    );
  }

  async queryTemplate(queryId: string, params: StandaloneQueryParam[] = []): Promise<QueryResult> {
    const paramBytes = encodeQueryParams(params);
    const reader = await this.process.request(
      writeRequest(OP_QUERY_TEMPLATE, (writer) => {
        writer.u32(this.handle);
        writer.string(queryId);
        writer.u32(params.length);
        writer.bytes(paramBytes);
      })
    );
    return readQueryResult(reader);
  }

  async clearQueryCache(): Promise<void> {
    await this.process.request(writeRequest(OP_CLEAR_QUERY_CACHE, (writer) => writer.u32(this.handle)));
  }

  async configureQueryCache({ maxEntries, maxRows }: { maxEntries: number; maxRows: number }): Promise<void> {
    if (!Number.isSafeInteger(maxEntries) || maxEntries <= 0) {
      throw new TypeError(`maxEntries must be a positive safe integer, received: ${maxEntries}`);
    }
    if (!Number.isSafeInteger(maxRows) || maxRows <= 0) {
      throw new TypeError(`maxRows must be a positive safe integer, received: ${maxRows}`);
    }
    await this.process.request(
      writeRequest(OP_CONFIGURE_QUERY_CACHE, (writer) => {
        writer.u32(this.handle);
        writer.u32(maxEntries);
        writer.u32(maxRows);
      })
    );
  }

  async getQueryCacheStats(): Promise<{
    hits: number;
    misses: number;
    size: number;
    generation: number;
    maxEntries: number;
    maxRows: number;
  }> {
    const reader = await this.process.request(writeRequest(OP_QUERY_CACHE_STATS, (writer) => writer.u32(this.handle)));
    return {
      hits: reader.f64(),
      misses: reader.f64(),
      size: reader.f64(),
      generation: reader.f64(),
      maxEntries: reader.f64(),
      maxRows: reader.f64(),
    };
  }

  async getFlatBufferByIndex(
    tableName: string,
    indexName: string,
    keyParams: StandaloneQueryParam[] = []
  ): Promise<Uint8Array | null> {
    const paramBytes = encodeQueryParams(keyParams);
    const reader = await this.process.request(
      writeRequest(OP_GET_FLATBUFFER_BY_INDEX, (writer) => {
        writer.u32(this.handle);
        writer.string(tableName);
        writer.string(indexName);
        writer.u32(keyParams.length);
        writer.bytes(paramBytes);
      })
    );
    return reader.u8() === 0 ? null : reader.bytes();
  }

  async exportData(): Promise<Uint8Array> {
    const reader = await this.process.request(writeRequest(OP_EXPORT_DATA, (writer) => writer.u32(this.handle)));
    return reader.bytes();
  }

  async loadAndRebuild(data: Uint8Array): Promise<void> {
    await this.process.request(
      writeRequest(OP_LOAD_AND_REBUILD, (writer) => {
        writer.u32(this.handle);
        writer.bytes(data);
      })
    );
  }

  async reserveStorageBytes(bytes: number): Promise<void> {
    await this.process.request(
      writeRequest(OP_RESERVE_STORAGE, (writer) => {
        writer.u32(this.handle);
        writer.u32(bytes);
      })
    );
  }

  async loadAndRebuildFrom(sourceDb: FlatSQLWasmEdgeProcessDatabase): Promise<void> {
    await this.process.request(
      writeRequest(OP_LOAD_FROM_DB, (writer) => {
        writer.u32(this.handle);
        writer.u32(sourceDb.handle);
      })
    );
  }
}

export function createFlatSQLWasmEdgeProcessRuntime(
  options: FlatSQLWasmEdgeProcessRuntimeOptions
): FlatSQLWasmEdgeProcessRuntime {
  return new FlatSQLWasmEdgeProcessRuntime(options);
}
