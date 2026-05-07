// FlatSQL JavaScript API - Uses C exports (worker-compatible)
// This wrapper uses cwrap/ccall instead of embind to avoid worker issues

import FlatSQLModule from './flatsql.js';

let Module = null;
let api = null;
const textEncoder = new TextEncoder();

const PARAM_NULL = 0;
const PARAM_BOOL = 1;
const PARAM_INT64 = 2;
const PARAM_FLOAT64 = 3;
const PARAM_STRING = 4;
const PARAM_BYTES = 5;

// Security: Track if integrity was verified
let integrityVerified = false;

function toBase64(bytes) {
    if (typeof Buffer !== 'undefined') {
        return Buffer.from(bytes).toString('base64');
    }
    let binary = '';
    for (let offset = 0; offset < bytes.length; offset += 0x8000) {
        binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
    }
    return btoa(binary);
}

function normalizeHashResult(value) {
    if (typeof value === 'string') {
        return value;
    }
    if (value instanceof Uint8Array) {
        return toBase64(value);
    }
    if (ArrayBuffer.isView(value)) {
        return toBase64(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    }
    if (value instanceof ArrayBuffer) {
        return toBase64(new Uint8Array(value));
    }
    throw new Error('SHA-384 provider must return a base64 string or binary digest');
}

function hasNodeProcess() {
    return typeof process !== 'undefined' &&
        process.versions !== undefined &&
        typeof process.versions.node === 'string';
}

/**
 * Compute SHA-384 hash of data and return as base64.
 * Browser WebCrypto is intentionally not used; browser callers that require
 * integrity verification must pass computeSHA384 backed by WASM/native crypto.
 * @param {ArrayBuffer} data
 * @param {(data: ArrayBuffer) => Promise<string|Uint8Array|ArrayBuffer>|string|Uint8Array|ArrayBuffer} [provider]
 * @returns {Promise<string>}
 */
async function computeSHA384(data, provider) {
    if (typeof provider === 'function') {
        return normalizeHashResult(await provider(data));
    }
    if (hasNodeProcess()) {
        const cryptoModule = await import('node:crypto');
        const hash = cryptoModule.createHash('sha384');
        hash.update(Buffer.from(data));
        return hash.digest('base64');
    }
    throw new Error(
        'WASM integrity verification requires a computeSHA384 option backed by WASM/native crypto; browser WebCrypto is intentionally not used.'
    );
}

/**
 * Verify WASM binary integrity
 * @param {ArrayBuffer} wasmBinary - The WASM binary data
 * @param {string} expectedHash - Expected SHA-384 hash (base64)
 * @returns {Promise<boolean>}
 */
async function verifyWASMIntegrity(wasmBinary, expectedHash, options = {}) {
    const computedHash = await computeSHA384(wasmBinary, options.computeSHA384);
    return computedHash === expectedHash;
}

/**
 * Load integrity.json if available
 * @param {string} [basePath] - Base path for integrity.json
 * @returns {Promise<{hash: string, sri: string, size: number} | null>}
 */
async function loadIntegrityFile(basePath = '') {
    try {
        if (typeof fetch !== 'undefined') {
            // Browser or Node.js with fetch
            const url = basePath ? `${basePath}/integrity.json` : new URL('./integrity.json', import.meta.url).href;
            const response = await fetch(url);
            if (response.ok) {
                return await response.json();
            }
        } else {
            // Node.js fallback
            const fs = await import('fs');
            const path = await import('path');
            const url = await import('url');
            const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
            const integrityPath = path.join(__dirname, 'integrity.json');
            if (fs.existsSync(integrityPath)) {
                return JSON.parse(fs.readFileSync(integrityPath, 'utf8'));
            }
        }
    } catch (e) {
        // Integrity file not available - this is OK in development
    }
    return null;
}

/**
 * @typedef {Object} InitOptions
 * @property {string} [integrity] - Expected SHA-384 hash for WASM verification (base64)
 * @property {string} [wasmPath] - Custom path to WASM files directory
 * @property {boolean} [skipIntegrityCheck] - Skip integrity verification (not recommended for production)
 * @property {boolean} [requireIntegrity] - Fail if integrity cannot be verified (default: false)
 * @property {function} [moduleFactory] - Custom Emscripten module factory
 */

/**
 * Initialize the FlatSQL WASM module with optional integrity verification
 * @param {function|InitOptions} [moduleFactoryOrOptions] - Module factory or initialization options
 * @returns {Promise<FlatSQL>}
 *
 * @example
 * // Basic usage (auto-loads integrity.json if available)
 * const flatsql = await initFlatSQL();
 *
 * @example
 * // With explicit integrity hash
 * const flatsql = await initFlatSQL({
 *   integrity: 'base64-hash-here',
 *   requireIntegrity: true
 * });
 *
 * @example
 * // Skip integrity check (development only)
 * const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
 */
export async function initFlatSQL(moduleFactoryOrOptions) {
    let moduleFactory = FlatSQLModule;
    let options = {};

    // Parse arguments - support both legacy and new API
    if (moduleFactoryOrOptions) {
        if (typeof moduleFactoryOrOptions === 'function') {
            // Legacy: moduleFactory passed directly
            moduleFactory = moduleFactoryOrOptions;
        } else if (typeof moduleFactoryOrOptions === 'object') {
            // New: options object
            options = moduleFactoryOrOptions;
            if (options.moduleFactory) {
                moduleFactory = options.moduleFactory;
            }
        }
    }

    // Determine expected integrity hash
    let expectedIntegrity = options.integrity || null;

    // Load integrity file if no explicit integrity provided
    if (!expectedIntegrity && !options.skipIntegrityCheck) {
        const integrityData = await loadIntegrityFile(options.wasmPath);
        if (integrityData) {
            expectedIntegrity = integrityData.hash;
        }
    }

    // Check if integrity is required but not available
    if (options.requireIntegrity && !expectedIntegrity) {
        throw new Error(
            'WASM integrity verification required but no integrity hash available. ' +
            'Ensure integrity.json exists or pass integrity option.'
        );
    }

    // Create module configuration
    const moduleConfig = {};

    // If we have an expected hash, use custom WASM instantiation with verification
    if (expectedIntegrity && !options.skipIntegrityCheck) {
        moduleConfig.instantiateWasm = async (imports, successCallback) => {
            try {
                // Determine WASM path
                let wasmUrl;
                if (options.wasmPath) {
                    wasmUrl = `${options.wasmPath}/flatsql.wasm`;
                } else {
                    wasmUrl = new URL('./flatsql.wasm', import.meta.url).href;
                }

                // Fetch WASM binary
                let wasmBinary;
                if (typeof fetch !== 'undefined') {
                    const response = await fetch(wasmUrl);
                    if (!response.ok) {
                        throw new Error(`Failed to fetch WASM: ${response.status}`);
                    }
                    wasmBinary = await response.arrayBuffer();
                } else {
                    // Node.js fallback
                    const fs = await import('fs');
                    const path = await import('path');
                    const url = await import('url');
                    const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
                    const wasmPath = path.join(__dirname, 'flatsql.wasm');
                    wasmBinary = fs.readFileSync(wasmPath).buffer;
                }

                // Verify integrity
                const isValid = await verifyWASMIntegrity(wasmBinary, expectedIntegrity, options);
                if (!isValid) {
                    throw new Error(
                        'WASM integrity check failed: hash mismatch. ' +
                        'The WASM binary may have been tampered with or corrupted.'
                    );
                }

                // Mark as verified
                integrityVerified = true;

                // Instantiate verified WASM
                const result = await WebAssembly.instantiate(wasmBinary, imports);
                successCallback(result.instance);
                return result.instance.exports;
            } catch (error) {
                throw new Error(`WASM integrity verification failed: ${error.message}`);
            }
        };
    }

    // Initialize module
    Module = await moduleFactory(moduleConfig);

    // Wrap C functions using cwrap
    api = {
        // Database lifecycle
        createDb: Module.cwrap('flatsql_create_db', 'number', ['string', 'string']),
        destroyDb: Module.cwrap('flatsql_destroy_db', null, ['number']),
        registerFileId: Module.cwrap('flatsql_register_file_id', null, ['number', 'string', 'string']),
        enableDemoExtractors: Module.cwrap('flatsql_enable_demo_extractors', null, ['number']),

        // Data ingestion
        ingest: Module.cwrap('flatsql_ingest', 'number', ['number', 'number', 'number']),
        ingestOne: Module.cwrap('flatsql_ingest_one', 'number', ['number', 'number', 'number']),

        // Source-aware ingestion
        registerSource: Module.cwrap('flatsql_register_source', null, ['number', 'string']),
        createUnifiedViews: Module.cwrap('flatsql_create_unified_views', null, ['number']),
        ingestWithSource: Module.cwrap('flatsql_ingest_with_source', 'number', ['number', 'number', 'number', 'string']),
        ingestOneWithSource: Module.cwrap('flatsql_ingest_one_with_source', 'number', ['number', 'number', 'number', 'string']),
        getSourcesCount: Module.cwrap('flatsql_get_sources_count', 'number', ['number']),
        getSourceName: Module.cwrap('flatsql_get_source_name', 'string', ['number']),

        // Query execution
        query: Module.cwrap('flatsql_query', 'number', ['number', 'string']),
        queryParams: Module.cwrap('flatsql_query_params', 'number', ['number', 'string', 'number', 'number', 'number']),
        queryMany: Module.cwrap('flatsql_query_many', 'number', ['number', 'number', 'number', 'number']),
        buildQueryCacheKey: Module.cwrap('flatsql_build_query_cache_key', 'string', ['string', 'string', 'string', 'number', 'number', 'number']),
        buildResponseArtifactCacheKey: Module.cwrap('flatsql_build_response_artifact_cache_key', 'string', ['string', 'string', 'string', 'string', 'string', 'string', 'number', 'number', 'number']),
        registerQueryTemplate: Module.cwrap('flatsql_register_query_template', 'number', ['number', 'string', 'string', 'number']),
        queryTemplate: Module.cwrap('flatsql_query_template', 'number', ['number', 'string', 'number', 'number', 'number']),
        clearQueryCache: Module.cwrap('flatsql_clear_query_cache', null, ['number']),
        configureQueryCache: Module.cwrap('flatsql_configure_query_cache', 'number', ['number', 'number', 'number']),
        queryCacheHits: Module.cwrap('flatsql_query_cache_hits', 'number', ['number']),
        queryCacheMisses: Module.cwrap('flatsql_query_cache_misses', 'number', ['number']),
        queryCacheSize: Module.cwrap('flatsql_query_cache_size', 'number', ['number']),
        queryCacheGeneration: Module.cwrap('flatsql_query_cache_generation', 'number', ['number']),
        queryCacheMaxEntries: Module.cwrap('flatsql_query_cache_max_entries', 'number', ['number']),
        queryCacheMaxRows: Module.cwrap('flatsql_query_cache_max_rows', 'number', ['number']),
        batchResultCount: Module.cwrap('flatsql_batch_result_count', 'number', []),
        selectBatchResult: Module.cwrap('flatsql_select_batch_result', 'number', ['number']),
        getError: Module.cwrap('flatsql_get_error', 'string', []),

        // Result access
        resultColumnCount: Module.cwrap('flatsql_result_column_count', 'number', []),
        resultRowCount: Module.cwrap('flatsql_result_row_count', 'number', []),
        resultColumnName: Module.cwrap('flatsql_result_column_name', 'string', ['number']),
        resultCellType: Module.cwrap('flatsql_result_cell_type', 'number', ['number', 'number']),
        resultCellNumber: Module.cwrap('flatsql_result_cell_number', 'number', ['number', 'number']),
        resultCellString: Module.cwrap('flatsql_result_cell_string', 'string', ['number', 'number']),
        resultCellBlob: Module.cwrap('flatsql_result_cell_blob', 'number', ['number', 'number']),
        resultCellBlobSize: Module.cwrap('flatsql_result_cell_blob_size', 'number', ['number', 'number']),
        queryRawFlatBufferStream: Module.cwrap('flatsql_query_raw_flatbuffer_stream', 'number', ['number', 'string', 'number', 'number', 'number']),
        responseArtifactData: Module.cwrap('flatsql_response_artifact_data', 'number', []),
        responseArtifactSize: Module.cwrap('flatsql_response_artifact_size', 'number', []),
        responseArtifactRowCount: Module.cwrap('flatsql_response_artifact_row_count', 'number', []),
        responseArtifactColumnCount: Module.cwrap('flatsql_response_artifact_column_count', 'number', []),

        // Export/Import
        exportData: Module.cwrap('flatsql_export_data', 'number', ['number']),
        exportSize: Module.cwrap('flatsql_export_size', 'number', []),
        loadAndRebuild: Module.cwrap('flatsql_load_and_rebuild', null, ['number', 'number', 'number']),
        reserveStorage: Module.cwrap('flatsql_reserve_storage', null, ['number', 'number']),
        loadFromDb: Module.cwrap('flatsql_load_from_db', null, ['number', 'number']),

        // Test helpers
        createTestUser: Module.cwrap('flatsql_create_test_user', 'number', ['number', 'string', 'string', 'number']),
        createTestPost: Module.cwrap('flatsql_create_test_post', 'number', ['number', 'number', 'string']),
        createTestMPE: Module.cwrap('flatsql_create_test_mpe', 'number', ['string', 'number', 'number', 'number', 'number', 'number', 'number', 'number', 'number', 'number']),
        createTestTelemetry: Module.cwrap('flatsql_create_test_telemetry', 'number', ['number', 'string', 'string', 'string', 'number', 'number', 'number']),
        createTestPublishEvent: Module.cwrap('flatsql_create_test_publish_event', 'number', ['string', 'string', 'number', 'number']),
        testBufferSize: Module.cwrap('flatsql_test_buffer_size', 'number', []),

        // Stats
        getStatsCount: Module.cwrap('flatsql_get_stats_count', 'number', ['number']),
        getStatTableName: Module.cwrap('flatsql_get_stat_table_name', 'string', ['number']),
        getStatFileId: Module.cwrap('flatsql_get_stat_file_id', 'string', ['number']),
        getStatRecordCount: Module.cwrap('flatsql_get_stat_record_count', 'number', ['number']),
        resetIngestProfile: Module.cwrap('flatsql_reset_ingest_profile', null, ['number']),
        getIngestProfileRecordCount: Module.cwrap('flatsql_get_ingest_profile_record_count', 'number', ['number']),
        getIngestProfileByteCount: Module.cwrap('flatsql_get_ingest_profile_byte_count', 'number', ['number']),
        getIngestProfileDecodeNanos: Module.cwrap('flatsql_get_ingest_profile_decode_nanos', 'number', ['number']),
        getIngestProfileAppendNanos: Module.cwrap('flatsql_get_ingest_profile_append_nanos', 'number', ['number']),
        getIngestProfileIndexNanos: Module.cwrap('flatsql_get_ingest_profile_index_nanos', 'number', ['number']),

        // Delete support
        markDeleted: Module.cwrap('flatsql_mark_deleted', null, ['number', 'string', 'number']),
        getDeletedCount: Module.cwrap('flatsql_get_deleted_count', 'number', ['number', 'string']),
        clearTombstones: Module.cwrap('flatsql_clear_tombstones', null, ['number', 'string']),

        // Raw FlatBuffer access
        getFlatBufferById: Module.cwrap('flatsql_get_flatbuffer_by_id', 'number', ['number', 'string', 'number']),
        getFlatBufferByEmail: Module.cwrap('flatsql_get_flatbuffer_by_email', 'number', ['number', 'string', 'string']),
        getFlatBufferByIndex: Module.cwrap('flatsql_get_flatbuffer_by_index', 'number', ['number', 'string', 'string', 'number', 'number', 'number']),
        getRawFlatBufferSize: Module.cwrap('flatsql_get_raw_flatbuffer_size', 'number', []),
        getRawFlatBufferSequence: Module.cwrap('flatsql_get_raw_flatbuffer_sequence', 'number', []),
        getStorageBuffer: Module.cwrap('flatsql_get_storage_buffer', 'number', ['number']),
        getStorageSize: Module.cwrap('flatsql_get_storage_size', 'number', ['number']),

        // Encryption
        setEncryptionKey: Module.cwrap('flatsql_set_encryption_key', 'number', ['number', 'number', 'number']),
        isEncrypted: Module.cwrap('flatsql_is_encrypted', 'number', ['number']),
        encryptBuffer: Module.cwrap('flatsql_encrypt_buffer', 'number', ['number', 'number', 'number', 'number', 'number']),
        decryptBuffer: Module.cwrap('flatsql_decrypt_buffer', 'number', ['number', 'number', 'number', 'number', 'number']),

        // HMAC Authentication
        setHMACVerification: Module.cwrap('flatsql_set_hmac_verification', 'number', ['number', 'number']),
        isHMACEnabled: Module.cwrap('flatsql_is_hmac_enabled', 'number', ['number']),
        computeHMAC: Module.cwrap('flatsql_compute_hmac', 'number', ['number', 'number', 'number', 'number']),
        verifyHMAC: Module.cwrap('flatsql_verify_hmac', 'number', ['number', 'number', 'number', 'number']),
    };

    return new FlatSQL();
}

/**
 * Check if WASM was loaded with integrity verification
 * @returns {boolean}
 */
export function wasIntegrityVerified() {
    return integrityVerified;
}

function withHeapBytes(data, callback) {
    const ptr = Module._malloc(data.length);
    try {
        Module.HEAPU8.set(data, ptr);
        return callback(ptr);
    } finally {
        Module._free(ptr);
    }
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
            new DataView(payload.buffer, payload.byteOffset, payload.byteLength).setBigInt64(0, BigInt(value), true);
        } else if (typeof value === 'number') {
            tag = PARAM_FLOAT64;
            payload = new Uint8Array(8);
            new DataView(payload.buffer, payload.byteOffset, payload.byteLength).setFloat64(0, value, true);
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

function encodeQueryRequests(queries) {
    const parts = [];
    let total = 0;

    for (const query of queries) {
        const sqlBytes = textEncoder.encode(query.sql);
        const paramList = query.params ?? [];
        const paramBytes = encodeQueryParams(paramList);
        const header = new Uint8Array(12);
        const view = new DataView(header.buffer);
        view.setUint32(0, sqlBytes.length, true);
        view.setUint32(4, paramList.length, true);
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

function readCellValue(row, col) {
    const type = api.resultCellType(row, col);
    switch (type) {
        case 0:
            return null;
        case 1:
            return api.resultCellNumber(row, col) !== 0;
        case 2:
        case 3:
        case 4:
            return api.resultCellNumber(row, col);
        case 5:
            return api.resultCellString(row, col);
        case 6: {
            const blobPtr = api.resultCellBlob(row, col);
            const blobSize = api.resultCellBlobSize(row, col);
            return blobPtr && blobSize > 0
                ? Array.from(new Uint8Array(Module.HEAPU8.buffer, blobPtr, blobSize))
                : [];
        }
        default:
            return null;
    }
}

function readQueryResult() {
    const colCount = api.resultColumnCount();
    const rowCount = api.resultRowCount();

    const columns = [];
    for (let i = 0; i < colCount; i++) {
        columns.push(api.resultColumnName(i));
    }

    const rows = [];
    for (let r = 0; r < rowCount; r++) {
        const row = [];
        for (let c = 0; c < colCount; c++) {
            row.push(readCellValue(r, c));
        }
        rows.push(row);
    }

    return { columns, rows };
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

// High-level FlatSQL API class
export class FlatSQL {
    createDatabase(schema, dbName = 'default') {
        const handle = api.createDb(schema, dbName);
        return new FlatSQLDatabase(handle);
    }

    // Create test FlatBuffers
    createTestUser(id, name, email, age) {
        const ptr = api.createTestUser(id, name, email, age);
        const size = api.testBufferSize();
        return new Uint8Array(Module.HEAPU8.buffer, ptr, size).slice();
    }

    createTestPost(id, userId, title) {
        const ptr = api.createTestPost(id, userId, title);
        const size = api.testBufferSize();
        return new Uint8Array(Module.HEAPU8.buffer, ptr, size).slice();
    }

    createTestPublishEvent(fileId, recordId, eventIndex, payloadSize) {
        const ptr = api.createTestPublishEvent(fileId, recordId, eventIndex, payloadSize);
        const size = api.testBufferSize();
        return new Uint8Array(Module.HEAPU8.buffer, ptr, size).slice();
    }

    createTestMPE(entityId, epoch, meanMotion, eccentricity, inclination,
                  raOfAscNode, argOfPericenter, meanAnomaly, bstar, meanElementTheory = 0) {
        const ptr = api.createTestMPE(
            entityId,
            epoch,
            meanMotion,
            eccentricity,
            inclination,
            raOfAscNode,
            argOfPericenter,
            meanAnomaly,
            bstar,
            meanElementTheory
        );
        const size = api.testBufferSize();
        return new Uint8Array(Module.HEAPU8.buffer, ptr, size).slice();
    }

    createTestTelemetry(packetId, spacecraft, subsystem, mode, temperatureC, signalDb, timestampS) {
        const ptr = api.createTestTelemetry(packetId, spacecraft, subsystem, mode, temperatureC, signalDb, timestampS);
        const size = api.testBufferSize();
        return new Uint8Array(Module.HEAPU8.buffer, ptr, size).slice();
    }

    /**
     * Check if WASM was loaded with integrity verification
     * @returns {boolean}
     */
    wasIntegrityVerified() {
        return integrityVerified;
    }

    buildQueryCacheKey(dataset, artifactVersion, queryId, params = []) {
        const encodedParams = encodeQueryParams(params);
        const key = encodedParams.length === 0
            ? api.buildQueryCacheKey(dataset, artifactVersion, queryId, 0, 0, 0)
            : withHeapBytes(
                encodedParams,
                (ptr) => api.buildQueryCacheKey(dataset, artifactVersion, queryId, ptr, encodedParams.length, params.length)
            );
        if (!key) {
            throw new Error(api.getError());
        }
        return key;
    }

    buildResponseArtifactCacheKey(schemaName, schemaVersion, sql, options = {}) {
        const format = options.format ?? 'json';
        const publishEventKey = options.publishEventKey ?? '';
        const projectionList = (options.projection ?? []).join('\n');
        const params = options.params ?? [];
        const encodedParams = encodeQueryParams(params);
        const key = encodedParams.length === 0
            ? api.buildResponseArtifactCacheKey(
                schemaName,
                schemaVersion,
                sql,
                format,
                publishEventKey,
                projectionList,
                0,
                0,
                0
            )
            : withHeapBytes(
                encodedParams,
                (ptr) => api.buildResponseArtifactCacheKey(
                    schemaName,
                    schemaVersion,
                    sql,
                    format,
                    publishEventKey,
                    projectionList,
                    ptr,
                    encodedParams.length,
                    params.length
                )
            );
        if (!key) {
            throw new Error(api.getError());
        }
        return key;
    }
}

// Database wrapper class
export class FlatSQLDatabase {
    constructor(handle) {
        this._handle = handle;
    }

    destroy() {
        if (this._handle) {
            api.destroyDb(this._handle);
            this._handle = null;
        }
    }

    registerFileId(fileId, tableName) {
        api.registerFileId(this._handle, fileId, tableName);
    }

    enableDemoExtractors() {
        api.enableDemoExtractors(this._handle);
    }

    // Ingest data from Uint8Array (routes to base tables or source tables)
    ingest(data, source = null) {
        return withHeapBytes(data, (ptr) => {
            if (source) {
                return api.ingestWithSource(this._handle, ptr, data.length, source);
            }
            return api.ingest(this._handle, ptr, data.length);
        });
    }

    // Ingest many FlatBuffers via the native bulk stream path.
    ingestBuffers(buffers, source = null) {
        return this.ingest(buildSizePrefixedStream(buffers), source);
    }

    ingestOne(data, source = null) {
        return withHeapBytes(data, (ptr) => {
            if (source) {
                return api.ingestOneWithSource(this._handle, ptr, data.length, source);
            }
            return api.ingestOne(this._handle, ptr, data.length);
        });
    }

    // Register a named data source for source-aware ingestion
    // Creates source-specific tables: User@siteA, Post@siteA, etc.
    registerSource(sourceName) {
        api.registerSource(this._handle, sourceName);
    }

    // Create unified views for cross-source queries
    // Must be called after registering all sources and file IDs
    createUnifiedViews() {
        api.createUnifiedViews(this._handle);
    }

    // List registered sources
    listSources() {
        const count = api.getSourcesCount(this._handle);
        const sources = [];
        for (let i = 0; i < count; i++) {
            sources.push(api.getSourceName(i));
        }
        return sources;
    }

    query(sql, params = undefined) {
        const success = params === undefined
            ? api.query(this._handle, sql)
            : (() => {
                const encodedParams = encodeQueryParams(params);
                if (encodedParams.length === 0) {
                    return api.queryParams(this._handle, sql, 0, 0, 0);
                }
                return withHeapBytes(
                    encodedParams,
                    (ptr) => api.queryParams(this._handle, sql, ptr, encodedParams.length, params.length)
                );
            })();
        if (!success) {
            throw new Error(api.getError());
        }
        return readQueryResult();
    }

    registerQueryTemplate(queryId, sql, cacheable = true) {
        const success = api.registerQueryTemplate(this._handle, queryId, sql, cacheable ? 1 : 0);
        if (!success) {
            throw new Error(api.getError());
        }
    }

    queryTemplate(queryId, params = []) {
        const encodedParams = encodeQueryParams(params);
        const success = encodedParams.length === 0
            ? api.queryTemplate(this._handle, queryId, 0, 0, 0)
            : withHeapBytes(
                encodedParams,
                (ptr) => api.queryTemplate(this._handle, queryId, ptr, encodedParams.length, params.length)
            );
        if (!success) {
            throw new Error(api.getError());
        }
        return readQueryResult();
    }

    clearQueryCache() {
        api.clearQueryCache(this._handle);
    }

    configureQueryCache({ maxEntries, maxRows }) {
        if (!Number.isSafeInteger(maxEntries) || maxEntries <= 0) {
            throw new TypeError(`maxEntries must be a positive safe integer, received: ${maxEntries}`);
        }
        if (!Number.isSafeInteger(maxRows) || maxRows <= 0) {
            throw new TypeError(`maxRows must be a positive safe integer, received: ${maxRows}`);
        }
        const success = api.configureQueryCache(this._handle, maxEntries, maxRows);
        if (!success) {
            throw new Error(api.getError());
        }
    }

    getQueryCacheStats() {
        return {
            hits: api.queryCacheHits(this._handle),
            misses: api.queryCacheMisses(this._handle),
            size: api.queryCacheSize(this._handle),
            generation: api.queryCacheGeneration(this._handle),
            maxEntries: api.queryCacheMaxEntries(this._handle),
            maxRows: api.queryCacheMaxRows(this._handle)
        };
    }

    queryMany(queries) {
        if (queries.length === 0) {
            return [];
        }

        const encodedRequests = encodeQueryRequests(queries);
        const success = withHeapBytes(
            encodedRequests,
            (ptr) => api.queryMany(this._handle, ptr, encodedRequests.length, queries.length)
        );
        if (!success) {
            throw new Error(api.getError());
        }

        const resultCount = api.batchResultCount();
        const results = [];
        for (let index = 0; index < resultCount; index++) {
            if (!api.selectBatchResult(index)) {
                throw new Error(`Failed to select batch result ${index}`);
            }
            results.push(readQueryResult());
        }
        return results;
    }

    queryRawFlatBufferStream(sql, params = []) {
        const encodedParams = encodeQueryParams(params);
        const success = encodedParams.length === 0
            ? api.queryRawFlatBufferStream(this._handle, sql, 0, 0, 0)
            : withHeapBytes(
                encodedParams,
                (ptr) => api.queryRawFlatBufferStream(this._handle, sql, ptr, encodedParams.length, params.length)
            );
        if (!success) {
            throw new Error(api.getError());
        }

        const ptr = api.responseArtifactData();
        const size = api.responseArtifactSize();
        return ptr && size > 0 ? new Uint8Array(Module.HEAPU8.buffer, ptr, size).slice() : new Uint8Array();
    }

    exportData() {
        const ptr = api.exportData(this._handle);
        const size = api.exportSize();
        return new Uint8Array(Module.HEAPU8.buffer, ptr, size).slice();
    }

    loadAndRebuild(data) {
        const ptr = Module._malloc(data.length);
        Module.HEAPU8.set(data, ptr);
        api.loadAndRebuild(this._handle, ptr, data.length);
        Module._free(ptr);
    }

    reserveStorageBytes(bytes) {
        api.reserveStorage(this._handle, bytes);
    }

    loadAndRebuildFrom(sourceDb) {
        if (!sourceDb?._handle) {
            throw new TypeError('loadAndRebuildFrom requires a FlatSQL WASM database.');
        }
        api.loadFromDb(this._handle, sourceDb._handle);
    }

    getStats() {
        const count = api.getStatsCount(this._handle);
        const stats = [];
        for (let i = 0; i < count; i++) {
            stats.push({
                tableName: api.getStatTableName(i),
                fileId: api.getStatFileId(i),
                recordCount: api.getStatRecordCount(i)
            });
        }
        return stats;
    }

    resetIngestProfile() {
        api.resetIngestProfile(this._handle);
    }

    getIngestProfile() {
        return {
            recordCount: api.getIngestProfileRecordCount(this._handle),
            byteCount: api.getIngestProfileByteCount(this._handle),
            decodeNanos: api.getIngestProfileDecodeNanos(this._handle),
            appendNanos: api.getIngestProfileAppendNanos(this._handle),
            indexNanos: api.getIngestProfileIndexNanos(this._handle),
        };
    }

    markDeleted(tableName, sequence) {
        api.markDeleted(this._handle, tableName, sequence);
    }

    getDeletedCount(tableName) {
        return api.getDeletedCount(this._handle, tableName);
    }

    clearTombstones(tableName) {
        api.clearTombstones(this._handle, tableName);
    }

    getFlatBufferById(tableName, id) {
        const ptr = api.getFlatBufferById(this._handle, tableName, id);
        if (!ptr) {
            return null;
        }
        return {
            ptr,
            size: api.getRawFlatBufferSize(),
            sequence: api.getRawFlatBufferSequence()
        };
    }

    getFlatBufferByEmail(tableName, email) {
        const ptr = api.getFlatBufferByEmail(this._handle, tableName, email);
        if (!ptr) {
            return null;
        }
        return {
            ptr,
            size: api.getRawFlatBufferSize(),
            sequence: api.getRawFlatBufferSequence()
        };
    }

    getFlatBufferByIndex(tableName, columnName, value) {
        const encodedParams = encodeQueryParams([value]);
        const ptr = withHeapBytes(
            encodedParams,
            (paramPtr) => api.getFlatBufferByIndex(
                this._handle,
                tableName,
                columnName,
                paramPtr,
                encodedParams.length,
                1
            )
        );
        if (!ptr) {
            const error = api.getError();
            if (error) {
                throw new Error(error);
            }
            return null;
        }
        return {
            ptr,
            size: api.getRawFlatBufferSize(),
            sequence: api.getRawFlatBufferSequence()
        };
    }

    getFlatBufferDataById(tableName, id) {
        const record = this.getFlatBufferById(tableName, id);
        if (!record) {
            return null;
        }
        return new Uint8Array(Module.HEAPU8.buffer, record.ptr, record.size).slice();
    }

    getFlatBufferDataByIndex(tableName, columnName, value) {
        const record = this.getFlatBufferByIndex(tableName, columnName, value);
        if (!record) {
            return null;
        }
        return new Uint8Array(Module.HEAPU8.buffer, record.ptr, record.size).slice();
    }

    getStorageInfo() {
        return {
            ptr: api.getStorageBuffer(this._handle),
            size: api.getStorageSize(this._handle)
        };
    }

    // ==================== Encryption API ====================

    /**
     * Set the encryption key for field-level FlatBuffer decryption.
     * @param {Uint8Array} key - 32-byte AES-256 key
     */
    setEncryptionKey(key) {
        const ptr = Module._malloc(key.length);
        Module.HEAPU8.set(key, ptr);
        const result = api.setEncryptionKey(this._handle, ptr, key.length);
        Module._free(ptr);
        if (!result) throw new Error(api.getError());
    }

    /**
     * Check if encryption is enabled.
     * @returns {boolean}
     */
    isEncrypted() {
        return api.isEncrypted(this._handle) !== 0;
    }

    /**
     * Encrypt a FlatBuffer in-place using the database's encryption key.
     * @param {Uint8Array} buffer - FlatBuffer data
     * @param {Uint8Array} schema - Binary schema (.bfbs)
     * @returns {Uint8Array} Encrypted buffer (copy)
     */
    encryptBuffer(buffer, schema) {
        const bufPtr = Module._malloc(buffer.length);
        Module.HEAPU8.set(buffer, bufPtr);
        const schemaPtr = Module._malloc(schema.length);
        Module.HEAPU8.set(schema, schemaPtr);
        const result = api.encryptBuffer(this._handle, bufPtr, buffer.length, schemaPtr, schema.length);
        const encrypted = new Uint8Array(Module.HEAPU8.buffer, bufPtr, buffer.length).slice();
        Module._free(bufPtr);
        Module._free(schemaPtr);
        if (!result) throw new Error(api.getError());
        return encrypted;
    }

    /**
     * Decrypt a FlatBuffer in-place using the database's encryption key.
     * @param {Uint8Array} buffer - Encrypted FlatBuffer data
     * @param {Uint8Array} schema - Binary schema (.bfbs)
     * @returns {Uint8Array} Decrypted buffer (copy)
     */
    decryptBuffer(buffer, schema) {
        const bufPtr = Module._malloc(buffer.length);
        Module.HEAPU8.set(buffer, bufPtr);
        const schemaPtr = Module._malloc(schema.length);
        Module.HEAPU8.set(schema, schemaPtr);
        const result = api.decryptBuffer(this._handle, bufPtr, buffer.length, schemaPtr, schema.length);
        const decrypted = new Uint8Array(Module.HEAPU8.buffer, bufPtr, buffer.length).slice();
        Module._free(bufPtr);
        Module._free(schemaPtr);
        if (!result) throw new Error(api.getError());
        return decrypted;
    }

    // ==================== HMAC Authentication API ====================

    /**
     * Enable or disable HMAC verification on ingest.
     * Requires an encryption key to be set first.
     * @param {boolean} enabled
     */
    setHMACVerification(enabled) {
        const result = api.setHMACVerification(this._handle, enabled ? 1 : 0);
        if (!result) throw new Error(api.getError());
    }

    /**
     * Check if HMAC verification is enabled.
     * @returns {boolean}
     */
    isHMACEnabled() {
        return api.isHMACEnabled(this._handle) !== 0;
    }

    /**
     * Compute HMAC-SHA256 for a FlatBuffer.
     * @param {Uint8Array} buffer - FlatBuffer data
     * @returns {Uint8Array} 32-byte HMAC
     */
    computeHMAC(buffer) {
        const bufPtr = Module._malloc(buffer.length);
        Module.HEAPU8.set(buffer, bufPtr);
        const macPtr = Module._malloc(32);
        const result = api.computeHMAC(this._handle, bufPtr, buffer.length, macPtr);
        const mac = new Uint8Array(Module.HEAPU8.buffer, macPtr, 32).slice();
        Module._free(bufPtr);
        Module._free(macPtr);
        if (!result) throw new Error('HMAC computation failed - is encryption key set?');
        return mac;
    }

    /**
     * Verify HMAC-SHA256 for a FlatBuffer.
     * @param {Uint8Array} buffer - FlatBuffer data
     * @param {Uint8Array} mac - 32-byte HMAC to verify
     * @returns {boolean} true if valid
     */
    verifyHMAC(buffer, mac) {
        const bufPtr = Module._malloc(buffer.length);
        Module.HEAPU8.set(buffer, bufPtr);
        const macPtr = Module._malloc(32);
        Module.HEAPU8.set(mac, macPtr);
        const result = api.verifyHMAC(this._handle, bufPtr, buffer.length, macPtr);
        Module._free(bufPtr);
        Module._free(macPtr);
        return result !== 0;
    }
}

export default initFlatSQL;
