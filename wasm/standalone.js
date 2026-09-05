import { createFlatSqlIoImports, createMemoryBackend } from './flatsql-io.js';

const DEFAULT_STANDALONE_URL = new URL('./flatsql-wasi.wasm', import.meta.url);

const PARAM_NULL = 0;
const PARAM_BOOL = 1;
const PARAM_INT64 = 2;
const PARAM_FLOAT64 = 3;
const PARAM_STRING = 4;
const PARAM_BYTES = 5;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function getFlatSQLStandaloneWasmURL() {
  return DEFAULT_STANDALONE_URL;
}

async function readViaFsPath(path) {
  const fs = await import('node:fs/promises');
  return new Uint8Array(await fs.readFile(path));
}

async function loadWasmBytes(options = {}) {
  if (options.bytes instanceof Uint8Array) {
    return options.bytes;
  }
  if (options.bytes instanceof ArrayBuffer) {
    return new Uint8Array(options.bytes);
  }
  if (ArrayBuffer.isView(options.bytes)) {
    return new Uint8Array(options.bytes.buffer, options.bytes.byteOffset, options.bytes.byteLength);
  }
  if (options.path) {
    return readViaFsPath(options.path);
  }

  const wasmUrl = options.url ? new URL(options.url, import.meta.url) : DEFAULT_STANDALONE_URL;
  if (wasmUrl.protocol === 'file:') {
    const { fileURLToPath } = await import('node:url');
    return readViaFsPath(fileURLToPath(wasmUrl));
  }

  const response = await fetch(wasmUrl);
  if (!response.ok) {
    throw new Error(`Failed to load flatsql-wasi.wasm: ${response.status}`);
  }
  return new Uint8Array(await response.arrayBuffer());
}

function makeWasiImports(state, options = {}) {
  const stdout = options.stdout ?? (() => {});
  const stderr = options.stderr ?? (() => {});
  const stdin = options.stdin instanceof Uint8Array ? options.stdin : new Uint8Array();
  let stdinOffset = 0;

  function view() {
    return new DataView(state.memory.buffer);
  }

  function heap() {
    return new Uint8Array(state.memory.buffer);
  }

  function writeUint64(ptr, value) {
    view().setBigUint64(ptr, BigInt(value), true);
  }

  function readIov(iovPtr, iovIndex) {
    const base = iovPtr + iovIndex * 8;
    return {
      ptr: view().getUint32(base, true),
      length: view().getUint32(base + 4, true),
    };
  }

  function writeFd(fd, iovPtr, iovCount, bytesWrittenPtr) {
    let written = 0;
    const chunks = [];
    const memory = heap();
    for (let index = 0; index < iovCount; index++) {
      const iov = readIov(iovPtr, index);
      chunks.push(memory.slice(iov.ptr, iov.ptr + iov.length));
      written += iov.length;
    }
    view().setUint32(bytesWrittenPtr, written, true);

    if (chunks.length > 0) {
      const total = new Uint8Array(written);
      let offset = 0;
      for (const chunk of chunks) {
        total.set(chunk, offset);
        offset += chunk.length;
      }
      const text = textDecoder.decode(total);
      if (fd === 2) {
        stderr(text);
      } else {
        stdout(text);
      }
    }
    return 0;
  }

  function readFd(fd, iovPtr, iovCount, bytesReadPtr) {
    if (fd !== 0 || stdinOffset >= stdin.length) {
      view().setUint32(bytesReadPtr, 0, true);
      return 0;
    }

    let read = 0;
    const memory = heap();
    for (let index = 0; index < iovCount && stdinOffset < stdin.length; index++) {
      const iov = readIov(iovPtr, index);
      const length = Math.min(iov.length, stdin.length - stdinOffset);
      memory.set(stdin.subarray(stdinOffset, stdinOffset + length), iov.ptr);
      stdinOffset += length;
      read += length;
    }
    view().setUint32(bytesReadPtr, read, true);
    return 0;
  }

  function randomGet(ptr, length) {
    const target = heap().subarray(ptr, ptr + length);
    const cryptoApi = globalThis.crypto;
    if (cryptoApi?.getRandomValues) {
      cryptoApi.getRandomValues(target);
      return 0;
    }
    for (let index = 0; index < target.length; index++) {
      target[index] = Math.floor(Math.random() * 256);
    }
    return 0;
  }

  return {
    wasi_snapshot_preview1: {
      clock_time_get(clockId, _precision, timePtr) {
        const millis = clockId === 0 ? Date.now() : performance.now();
        writeUint64(timePtr, Math.round(millis * 1_000_000));
        return 0;
      },
      fd_write: writeFd,
      fd_read: readFd,
      environ_sizes_get(environCountPtr, environBufferSizePtr) {
        view().setUint32(environCountPtr, 0, true);
        view().setUint32(environBufferSizePtr, 0, true);
        return 0;
      },
      environ_get() {
        return 0;
      },
      random_get: randomGet,
    },
  };
}

function encodeQueryParams(params) {
  const parts = [];
  let total = 0;

  for (const value of params) {
    let tag;
    let payload;

    if (value === null) {
      tag = PARAM_NULL;
      payload = new Uint8Array(0);
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
    const headerView = new DataView(header.buffer);
    headerView.setUint8(0, tag);
    headerView.setUint32(1, payload.length, true);
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

function encodeQueryRequests(queries) {
  const parts = [];
  let total = 0;

  for (const query of queries) {
    const sqlBytes = textEncoder.encode(query.sql);
    const params = query.params ?? [];
    const paramBytes = encodeQueryParams(params);
    const header = new Uint8Array(12);
    const headerView = new DataView(header.buffer);
    headerView.setUint32(0, sqlBytes.length, true);
    headerView.setUint32(4, params.length, true);
    headerView.setUint32(8, paramBytes.length, true);
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

function buildSizePrefixedStream(buffers) {
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

function createRuntime(exports) {
  const memory = exports.memory;
  if (!(memory instanceof WebAssembly.Memory)) {
    throw new Error('Standalone FlatSQL artifact does not export WebAssembly.Memory as memory.');
  }

  function heap() {
    return new Uint8Array(memory.buffer);
  }

  function malloc(size) {
    const ptr = exports.malloc(size);
    if (!ptr) {
      throw new Error(`FlatSQL standalone allocation failed for ${size} bytes`);
    }
    return ptr;
  }

  function free(ptr) {
    if (ptr) {
      exports.free(ptr);
    }
  }

  function readCString(ptr) {
    if (!ptr) {
      return '';
    }
    const memoryBytes = heap();
    if (ptr < 0 || ptr >= memoryBytes.length) {
      throw new Error(`FlatSQL standalone string pointer is outside memory: ${ptr}`);
    }
    let end = ptr;
    while (end < memoryBytes.length && memoryBytes[end] !== 0) {
      end++;
    }
    if (end === memoryBytes.length) {
      throw new Error(`FlatSQL standalone string is not null-terminated at pointer ${ptr}`);
    }
    return textDecoder.decode(memoryBytes.subarray(ptr, end));
  }

  function withCString(value, callback) {
    const bytes = textEncoder.encode(String(value));
    const ptr = malloc(bytes.length + 1);
    try {
      heap().set(bytes, ptr);
      heap()[ptr + bytes.length] = 0;
      return callback(ptr);
    } finally {
      free(ptr);
    }
  }

  function withBytes(bytes, callback) {
    if (bytes.length === 0) {
      return callback(0);
    }
    const ptr = malloc(bytes.length);
    try {
      heap().set(bytes, ptr);
      return callback(ptr);
    } finally {
      free(ptr);
    }
  }

  function check(success) {
    if (!success) {
      throw new Error(readCString(exports.flatsql_get_error()));
    }
  }

  function readCellValue(row, column) {
    const type = exports.flatsql_result_cell_type(row, column);
    switch (type) {
      case 0:
        return null;
      case 1:
        return exports.flatsql_result_cell_number(row, column) !== 0;
      case 2:
      case 3:
      case 4:
        return exports.flatsql_result_cell_number(row, column);
      case 5:
        return readCString(exports.flatsql_result_cell_string(row, column));
      case 6: {
        const ptr = exports.flatsql_result_cell_blob(row, column);
        const size = exports.flatsql_result_cell_blob_size(row, column);
        return ptr && size > 0 ? Array.from(heap().slice(ptr, ptr + size)) : [];
      }
      default:
        return null;
    }
  }

  function readQueryResult() {
    const columnCount = exports.flatsql_result_column_count();
    const rowCount = exports.flatsql_result_row_count();
    const columns = [];
    const rows = [];

    for (let column = 0; column < columnCount; column++) {
      columns.push(readCString(exports.flatsql_result_column_name(column)));
    }
    for (let row = 0; row < rowCount; row++) {
      const values = [];
      for (let column = 0; column < columnCount; column++) {
        values.push(readCellValue(row, column));
      }
      rows.push(values);
    }
    return { columns, rows };
  }

  return {
    exports,
    memory,
    readCString,
    withCString,
    withBytes,
    check,
    readQueryResult,
    readBytes(ptr, size) {
      return heap().slice(ptr, ptr + size);
    },
  };
}

export async function loadFlatSQLStandalone(options = {}) {
  const wasmBytes = await loadWasmBytes(options);
  const state = { memory: null };
  const imports = makeWasiImports(state, options.wasi ?? options);

  // The seven-import host I/O contract (cpp/include/flatsql/flatsql_io.h).
  // The module imports these unconditionally, so they are always supplied: with
  // no backend the default is in-memory, which keeps the historical ephemeral
  // behaviour byte-for-byte and never pretends a path is durable.
  const io = options.io ?? createMemoryBackend();
  imports.env = {
    ...createFlatSqlIoImports(io, () => state.memory),
    ...(imports.env ?? {}),
  };

  const mergedImports = options.imports
    ? {
        ...imports,
        ...options.imports,
        env: {
          ...imports.env,
          ...(options.imports.env ?? {}),
        },
        wasi_snapshot_preview1: {
          ...imports.wasi_snapshot_preview1,
          ...(options.imports.wasi_snapshot_preview1 ?? {}),
        },
      }
    : imports;
  const { instance, module } = await WebAssembly.instantiate(wasmBytes, mergedImports);
  state.memory = instance.exports.memory;

  if (typeof instance.exports._initialize === 'function') {
    instance.exports._initialize();
  }

  return new FlatSQLStandalone(createRuntime(instance.exports), module, io);
}

export const createStandaloneFlatSQL = loadFlatSQLStandalone;

export class FlatSQLStandalone {
  constructor(runtime, module, io) {
    this._runtime = runtime;
    this.module = module;
    this.memory = runtime.memory;
    // The I/O backend this instance was wired to. Callers await io.hydrate()
    // before opening a disk-backed database and io.flush() after
    // flushIndex(); on synchronous backends both are no-ops.
    this.io = io ?? null;
  }

  /**
   * Disk-backed open. `path` is a name in the backend's namespace — a real
   * path under a WASI preopen, a key prefix in a browser store. The module
   * never learns which, and that is the point.
   *
   * journalMode: 0 DELETE, 1 WAL (unavailable on wasm), 2 TRUNCATE, 3 MEMORY.
   * TRUNCATE is the wasm default because WAL needs xShmMap shared memory that
   * neither lane provides (docs/STORAGE-DURABILITY.md §3.5).
   */
  openDatabase(schema, dbName = 'default', path = '', journalMode = 2) {
    const handle = this._runtime.withCString(schema, (schemaPtr) =>
      this._runtime.withCString(dbName, (namePtr) =>
        this._runtime.withCString(path ?? '', (pathPtr) =>
          this._runtime.exports.flatsql_open_db(schemaPtr, namePtr, pathPtr, journalMode)
        )
      )
    );
    if (!handle) {
      const errPtr = this._runtime.exports.flatsql_get_error();
      const message = errPtr ? this._runtime.readCString(errPtr) : 'unknown error';
      throw new Error(`Failed to open FlatSQL database at "${path}": ${message}`);
    }
    return new FlatSQLStandaloneDatabase(this._runtime, handle);
  }

  createDatabase(schema, dbName = 'default') {
    const handle = this._runtime.withCString(schema, (schemaPtr) =>
      this._runtime.withCString(dbName, (namePtr) =>
        this._runtime.exports.flatsql_create_db(schemaPtr, namePtr)
      )
    );
    if (!handle) {
      throw new Error('Failed to create FlatSQL standalone database.');
    }
    return new FlatSQLStandaloneDatabase(this._runtime, handle);
  }

  createTestUser(id, name, email, age) {
    const ptr = this._runtime.withCString(name, (namePtr) =>
      this._runtime.withCString(email, (emailPtr) =>
        this._runtime.exports.flatsql_create_test_user(id, namePtr, emailPtr, age)
      )
    );
    const size = this._runtime.exports.flatsql_test_buffer_size();
    return this._runtime.readBytes(ptr, size);
  }

  createTestPost(id, userId, title) {
    const ptr = this._runtime.withCString(title, (titlePtr) =>
      this._runtime.exports.flatsql_create_test_post(id, userId, titlePtr)
    );
    const size = this._runtime.exports.flatsql_test_buffer_size();
    return this._runtime.readBytes(ptr, size);
  }

  createTestPublishEvent(fileId, recordId, eventIndex, payloadSize) {
    const ptr = this._runtime.withCString(fileId, (fileIdPtr) =>
      this._runtime.withCString(recordId, (recordIdPtr) =>
        this._runtime.exports.flatsql_create_test_publish_event(
          fileIdPtr,
          recordIdPtr,
          eventIndex,
          payloadSize
        )
      )
    );
    const size = this._runtime.exports.flatsql_test_buffer_size();
    return this._runtime.readBytes(ptr, size);
  }

  buildQueryCacheKey(dataset, artifactVersion, queryId, params = []) {
    const paramBytes = encodeQueryParams(params);
    const keyPtr = this._runtime.withCString(dataset, (datasetPtr) =>
      this._runtime.withCString(artifactVersion, (versionPtr) =>
        this._runtime.withCString(queryId, (queryIdPtr) =>
          this._runtime.withBytes(paramBytes, (paramPtr) =>
            this._runtime.exports.flatsql_build_query_cache_key(
              datasetPtr,
              versionPtr,
              queryIdPtr,
              paramPtr,
              paramBytes.length,
              params.length
            )
          )
        )
      )
    );
    const key = this._runtime.readCString(keyPtr);
    if (!key) {
      throw new Error(this._runtime.readCString(this._runtime.exports.flatsql_get_error()));
    }
    return key;
  }

  buildResponseArtifactCacheKey(schemaName, schemaVersion, sql, options = {}) {
    const format = options.format ?? 'json';
    const publishEventKey = options.publishEventKey ?? '';
    const projectionList = (options.projection ?? []).join('\n');
    const params = options.params ?? [];
    const paramBytes = encodeQueryParams(params);
    const keyPtr = this._runtime.withCString(schemaName, (schemaPtr) =>
      this._runtime.withCString(schemaVersion, (versionPtr) =>
        this._runtime.withCString(sql, (sqlPtr) =>
          this._runtime.withCString(format, (formatPtr) =>
            this._runtime.withCString(publishEventKey, (eventPtr) =>
              this._runtime.withCString(projectionList, (projectionPtr) =>
                this._runtime.withBytes(paramBytes, (paramPtr) =>
                  this._runtime.exports.flatsql_build_response_artifact_cache_key(
                    schemaPtr,
                    versionPtr,
                    sqlPtr,
                    formatPtr,
                    eventPtr,
                    projectionPtr,
                    paramPtr,
                    paramBytes.length,
                    params.length
                  )
                )
              )
            )
          )
        )
      )
    );
    const key = this._runtime.readCString(keyPtr);
    if (!key) {
      throw new Error(this._runtime.readCString(this._runtime.exports.flatsql_get_error()));
    }
    return key;
  }
}

export class FlatSQLStandaloneDatabase {
  constructor(runtime, handle) {
    this._runtime = runtime;
    this._handle = handle;
  }

  destroy() {
    if (this._handle) {
      this._runtime.exports.flatsql_destroy_db(this._handle);
      this._handle = 0;
    }
  }

  // ---- Durable state (docs/STORAGE-DURABILITY.md §3.3) --------------------
  // Codes are values, never throws:
  //   >=0 record count / OK   -1 absent   -2 format or schema   -3 corrupt
  //   -4 torn pair            -5 no filesystem
  // EVERY negative is recoverable with reindexAll(); none means data loss.

  isDiskBacked() {
    return this._runtime.exports.flatsql_is_disk_backed(this._handle) === 1;
  }

  openState() {
    return this._runtime.exports.flatsql_open_state(this._handle);
  }

  reindexAll() {
    return this._runtime.exports.flatsql_reindex_all(this._handle);
  }

  reindexStep(maxRecords = 4096) {
    if (!Number.isSafeInteger(maxRecords) || maxRecords <= 0 || maxRecords > 0x7fffffff) {
      throw new RangeError('maxRecords must be a positive 32-bit integer');
    }
    return this._runtime.exports.flatsql_reindex_step(this._handle, maxRecords);
  }

  flushIndex() {
    return this._runtime.exports.flatsql_flush_index(this._handle);
  }

  flushedOffset() {
    return this._runtime.exports.flatsql_flushed_offset(this._handle);
  }

  streamPath() {
    const ptr = this._runtime.exports.flatsql_stream_path(this._handle);
    return ptr ? this._runtime.readCString(ptr) : '';
  }

  registerFileId(fileId, tableName) {
    this._runtime.withCString(fileId, (filePtr) =>
      this._runtime.withCString(tableName, (tablePtr) =>
        this._runtime.exports.flatsql_register_file_id(this._handle, filePtr, tablePtr)
      )
    );
  }

  enableDemoExtractors() {
    this._runtime.exports.flatsql_enable_demo_extractors(this._handle);
  }

  // Source partitions. The artifact has always exported these; the shim did
  // not wrap them, so the wasm lane could ingest WITH a source but never
  // declare one, and could not see what a reopen restored. Same three calls,
  // same names, same order as wasm/index.js — one engine, one surface.
  registerSource(sourceName) {
    this._runtime.withCString(sourceName, (ptr) =>
      this._runtime.exports.flatsql_register_source(this._handle, ptr)
    );
  }

  createUnifiedViews() {
    this._runtime.exports.flatsql_create_unified_views(this._handle);
  }

  listSources() {
    const count = this._runtime.exports.flatsql_get_sources_count(this._handle);
    const sources = [];
    for (let i = 0; i < count; i++) {
      const ptr = this._runtime.exports.flatsql_get_source_name(i);
      sources.push(ptr ? this._runtime.readCString(ptr) : '');
    }
    return sources;
  }

  ingest(data, source = null) {
    const result = this._runtime.withBytes(data, (ptr) => {
      if (source) {
        return this._runtime.withCString(source, (sourcePtr) =>
          this._runtime.exports.flatsql_ingest_with_source(this._handle, ptr, data.length, sourcePtr)
        );
      }
      return this._runtime.exports.flatsql_ingest(this._handle, ptr, data.length);
    });
    if (result < 0) throw new Error(this._runtime.readCString(this._runtime.exports.flatsql_get_error()));
    return result;
  }

  ingestBuffers(buffers, source = null) {
    return this.ingest(buildSizePrefixedStream(buffers), source);
  }

  ingestOne(data, source = null) {
    const result = this._runtime.withBytes(data, (ptr) => {
      if (source) {
        return this._runtime.withCString(source, (sourcePtr) =>
          this._runtime.exports.flatsql_ingest_one_with_source(this._handle, ptr, data.length, sourcePtr)
        );
      }
      return this._runtime.exports.flatsql_ingest_one(this._handle, ptr, data.length);
    });
    if (result < 0) throw new Error(this._runtime.readCString(this._runtime.exports.flatsql_get_error()));
    return result;
  }

  query(sql, params = undefined) {
    const success = this._runtime.withCString(sql, (sqlPtr) => {
      if (params === undefined) {
        return this._runtime.exports.flatsql_query(this._handle, sqlPtr);
      }
      const paramBytes = encodeQueryParams(params);
      return this._runtime.withBytes(paramBytes, (paramPtr) =>
        this._runtime.exports.flatsql_query_params(
          this._handle,
          sqlPtr,
          paramPtr,
          paramBytes.length,
          params.length
        )
      );
    });
    this._runtime.check(success);
    return this._runtime.readQueryResult();
  }

  queryMany(queries) {
    if (queries.length === 0) {
      return [];
    }
    const requestBytes = encodeQueryRequests(queries);
    const success = this._runtime.withBytes(requestBytes, (ptr) =>
      this._runtime.exports.flatsql_query_many(this._handle, ptr, requestBytes.length, queries.length)
    );
    this._runtime.check(success);

    const count = this._runtime.exports.flatsql_batch_result_count();
    const results = [];
    for (let index = 0; index < count; index++) {
      this._runtime.check(this._runtime.exports.flatsql_select_batch_result(index));
      results.push(this._runtime.readQueryResult());
    }
    return results;
  }

  queryRawFlatBufferStream(sql, params = []) {
    const paramBytes = encodeQueryParams(params);
    const success = this._runtime.withCString(sql, (sqlPtr) =>
      this._runtime.withBytes(paramBytes, (paramPtr) =>
        this._runtime.exports.flatsql_query_raw_flatbuffer_stream(
          this._handle,
          sqlPtr,
          paramPtr,
          paramBytes.length,
          params.length
        )
      )
    );
    this._runtime.check(success);

    const ptr = this._runtime.exports.flatsql_response_artifact_data();
    const size = this._runtime.exports.flatsql_response_artifact_size();
    return ptr && size > 0 ? this._runtime.readBytes(ptr, size) : new Uint8Array();
  }

  // Sandboxed public query (gateway loop G.5): one read-only SELECT under
  // the engine authorizer (record tables / shadow tables / unified views
  // only), single-statement, statement timeout, row/byte caps. options:
  // { mode: "stream" | "json", maxRows, maxBytes, timeoutMs }. Returns
  // { payload: Uint8Array, rows, columns } — payload is the aligned frame
  // stream (mode "stream") or UTF-8 bare-array JSON bytes (mode "json").
  // Sandbox rejections throw with the engine's "sandbox: <code>: ..." text.
  querySandboxed(sql, params = [], options = {}) {
    const paramBytes = encodeQueryParams(params);
    const mode = options.mode === "json" ? 1 : 0;
    const success = this._runtime.withCString(sql, (sqlPtr) =>
      this._runtime.withBytes(paramBytes, (paramPtr) =>
        this._runtime.exports.flatsql_query_sandboxed(
          this._handle,
          sqlPtr,
          paramPtr,
          paramBytes.length,
          params.length,
          mode,
          options.maxRows > 0 ? options.maxRows : 0,
          options.maxBytes > 0 ? options.maxBytes : 0,
          options.timeoutMs > 0 ? options.timeoutMs : 0
        )
      )
    );
    this._runtime.check(success);

    const ptr = this._runtime.exports.flatsql_response_artifact_data();
    const size = this._runtime.exports.flatsql_response_artifact_size();
    return {
      payload: ptr && size > 0 ? this._runtime.readBytes(ptr, size) : new Uint8Array(),
      rows: this._runtime.exports.flatsql_response_artifact_row_count(),
      columns: this._runtime.exports.flatsql_response_artifact_column_count(),
    };
  }

  // True when the last queryRawFlatBufferStream call was served from the
  // engine's response-artifact cache (no SQL re-execution).
  lastRawStreamCacheHit() {
    return this._runtime.exports.flatsql_response_artifact_cache_hit() !== 0;
  }

  configureRawStreamCache(maxEntries, maxTotalBytes) {
    const ok = this._runtime.exports.flatsql_configure_raw_stream_cache(
      this._handle,
      maxEntries,
      maxTotalBytes
    );
    this._runtime.check(ok);
  }

  getRawStreamCacheStats() {
    return {
      hits: this._runtime.exports.flatsql_raw_stream_cache_hits(this._handle),
      misses: this._runtime.exports.flatsql_raw_stream_cache_misses(this._handle),
      entries: this._runtime.exports.flatsql_raw_stream_cache_size(this._handle),
      totalBytes: this._runtime.exports.flatsql_raw_stream_cache_total_bytes(this._handle),
    };
  }

  registerQueryTemplate(queryId, sql, cacheable = true) {
    const success = this._runtime.withCString(queryId, (queryIdPtr) =>
      this._runtime.withCString(sql, (sqlPtr) =>
        this._runtime.exports.flatsql_register_query_template(
          this._handle,
          queryIdPtr,
          sqlPtr,
          cacheable ? 1 : 0
        )
      )
    );
    this._runtime.check(success);
  }

  queryTemplate(queryId, params = []) {
    const paramBytes = encodeQueryParams(params);
    const success = this._runtime.withCString(queryId, (queryIdPtr) =>
      this._runtime.withBytes(paramBytes, (paramPtr) =>
        this._runtime.exports.flatsql_query_template(
          this._handle,
          queryIdPtr,
          paramPtr,
          paramBytes.length,
          params.length
        )
      )
    );
    this._runtime.check(success);
    return this._runtime.readQueryResult();
  }

  clearQueryCache() {
    this._runtime.exports.flatsql_clear_query_cache(this._handle);
  }

  configureQueryCache({ maxEntries, maxRows }) {
    if (!Number.isSafeInteger(maxEntries) || maxEntries <= 0) {
      throw new TypeError(`maxEntries must be a positive safe integer, received: ${maxEntries}`);
    }
    if (!Number.isSafeInteger(maxRows) || maxRows <= 0) {
      throw new TypeError(`maxRows must be a positive safe integer, received: ${maxRows}`);
    }
    this._runtime.check(
      this._runtime.exports.flatsql_configure_query_cache(this._handle, maxEntries, maxRows)
    );
  }

  getQueryCacheStats() {
    return {
      hits: this._runtime.exports.flatsql_query_cache_hits(this._handle),
      misses: this._runtime.exports.flatsql_query_cache_misses(this._handle),
      size: this._runtime.exports.flatsql_query_cache_size(this._handle),
      generation: this._runtime.exports.flatsql_query_cache_generation(this._handle),
      maxEntries: this._runtime.exports.flatsql_query_cache_max_entries(this._handle),
      maxRows: this._runtime.exports.flatsql_query_cache_max_rows(this._handle),
    };
  }

  exportData() {
    const ptr = this._runtime.exports.flatsql_export_data(this._handle);
    const error = this._runtime.readCString(this._runtime.exports.flatsql_get_error());
    if (error) throw new Error(error);
    const size = this._runtime.exports.flatsql_export_size();
    return this._runtime.readBytes(ptr, size);
  }

  loadAndRebuild(data) {
    this._runtime.withBytes(data, (ptr) =>
      this._runtime.exports.flatsql_load_and_rebuild(this._handle, ptr, data.length)
    );
  }

  reserveStorageBytes(bytes) {
    this._runtime.exports.flatsql_reserve_storage(this._handle, bytes);
  }

  loadAndRebuildFrom(sourceDb) {
    if (!sourceDb?._handle) {
      throw new TypeError('loadAndRebuildFrom requires a FlatSQL standalone database.');
    }
    this._runtime.exports.flatsql_load_from_db(this._handle, sourceDb._handle);
  }

  getFlatBufferByIndex(tableName, indexName, keyParams = []) {
    const paramBytes = encodeQueryParams(keyParams);
    const ptr = this._runtime.withCString(tableName, (tablePtr) =>
      this._runtime.withCString(indexName, (indexPtr) =>
        this._runtime.withBytes(paramBytes, (paramPtr) =>
          this._runtime.exports.flatsql_get_flatbuffer_by_index(
            this._handle,
            tablePtr,
            indexPtr,
            paramPtr,
            paramBytes.length,
            keyParams.length
          )
        )
      )
    );
    const size = this._runtime.exports.flatsql_get_raw_flatbuffer_size();
    return ptr && size > 0 ? this._runtime.readBytes(ptr, size) : null;
  }

  getStorageInfo() {
    return {
      ptr: this._runtime.exports.flatsql_get_storage_buffer(this._handle),
      size: this._runtime.exports.flatsql_get_storage_size(this._handle),
    };
  }
}
