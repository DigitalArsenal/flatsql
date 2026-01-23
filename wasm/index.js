// FlatSQL JavaScript API - Uses C exports (worker-compatible)
// This wrapper uses cwrap/ccall instead of embind to avoid worker issues

import FlatSQLModule from './flatsql.js';

let Module = null;
let api = null;

// Security: Track if integrity was verified
let integrityVerified = false;

/**
 * Compute SHA-384 hash of data and return as base64
 * Works in both Node.js and browser environments
 * @param {ArrayBuffer} data
 * @returns {Promise<string>}
 */
async function computeSHA384(data) {
    if (typeof crypto !== 'undefined' && crypto.subtle) {
        // Browser or Node.js 18+
        const hashBuffer = await crypto.subtle.digest('SHA-384', data);
        const hashArray = new Uint8Array(hashBuffer);
        // Convert to base64
        if (typeof btoa !== 'undefined') {
            return btoa(String.fromCharCode(...hashArray));
        } else {
            return Buffer.from(hashArray).toString('base64');
        }
    } else {
        // Node.js fallback using dynamic import
        const cryptoModule = await import('crypto');
        const hash = cryptoModule.createHash('sha384');
        hash.update(Buffer.from(data));
        return hash.digest('base64');
    }
}

/**
 * Verify WASM binary integrity
 * @param {ArrayBuffer} wasmBinary - The WASM binary data
 * @param {string} expectedHash - Expected SHA-384 hash (base64)
 * @returns {Promise<boolean>}
 */
async function verifyWASMIntegrity(wasmBinary, expectedHash) {
    const computedHash = await computeSHA384(wasmBinary);
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
                const isValid = await verifyWASMIntegrity(wasmBinary, expectedIntegrity);
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

        // Export/Import
        exportData: Module.cwrap('flatsql_export_data', 'number', ['number']),
        exportSize: Module.cwrap('flatsql_export_size', 'number', []),
        loadAndRebuild: Module.cwrap('flatsql_load_and_rebuild', null, ['number', 'number', 'number']),

        // Test helpers
        createTestUser: Module.cwrap('flatsql_create_test_user', 'number', ['number', 'string', 'string', 'number']),
        createTestPost: Module.cwrap('flatsql_create_test_post', 'number', ['number', 'number', 'string']),
        testBufferSize: Module.cwrap('flatsql_test_buffer_size', 'number', []),

        // Stats
        getStatsCount: Module.cwrap('flatsql_get_stats_count', 'number', ['number']),
        getStatTableName: Module.cwrap('flatsql_get_stat_table_name', 'string', ['number']),
        getStatFileId: Module.cwrap('flatsql_get_stat_file_id', 'string', ['number']),
        getStatRecordCount: Module.cwrap('flatsql_get_stat_record_count', 'number', ['number']),

        // Delete support
        markDeleted: Module.cwrap('flatsql_mark_deleted', null, ['number', 'string', 'number']),
        getDeletedCount: Module.cwrap('flatsql_get_deleted_count', 'number', ['number', 'string']),
        clearTombstones: Module.cwrap('flatsql_clear_tombstones', null, ['number', 'string']),
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

    /**
     * Check if WASM was loaded with integrity verification
     * @returns {boolean}
     */
    wasIntegrityVerified() {
        return integrityVerified;
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
        const ptr = Module._malloc(data.length);
        Module.HEAPU8.set(data, ptr);
        let count;
        if (source) {
            count = api.ingestWithSource(this._handle, ptr, data.length, source);
        } else {
            count = api.ingest(this._handle, ptr, data.length);
        }
        Module._free(ptr);
        return count;
    }

    ingestOne(data, source = null) {
        const ptr = Module._malloc(data.length);
        Module.HEAPU8.set(data, ptr);
        let count;
        if (source) {
            count = api.ingestOneWithSource(this._handle, ptr, data.length, source);
        } else {
            count = api.ingestOne(this._handle, ptr, data.length);
        }
        Module._free(ptr);
        return count;
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

    query(sql) {
        const success = api.query(this._handle, sql);
        if (!success) {
            throw new Error(api.getError());
        }

        // Read results
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
                const type = api.resultCellType(r, c);
                switch (type) {
                    case 0: // null
                        row.push(null);
                        break;
                    case 1: // bool
                        row.push(api.resultCellNumber(r, c) !== 0);
                        break;
                    case 2: // int32
                    case 3: // int64
                    case 4: // double
                        row.push(api.resultCellNumber(r, c));
                        break;
                    case 5: // string
                        row.push(api.resultCellString(r, c));
                        break;
                    case 6: // blob
                        const blobPtr = api.resultCellBlob(r, c);
                        const blobSize = api.resultCellBlobSize(r, c);
                        if (blobPtr && blobSize > 0) {
                            row.push(Array.from(new Uint8Array(Module.HEAPU8.buffer, blobPtr, blobSize)));
                        } else {
                            row.push([]);
                        }
                        break;
                    default:
                        row.push(null);
                }
            }
            rows.push(row);
        }

        return { columns, rows };
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

    markDeleted(tableName, sequence) {
        api.markDeleted(this._handle, tableName, sequence);
    }

    getDeletedCount(tableName) {
        return api.getDeletedCount(this._handle, tableName);
    }

    clearTombstones(tableName) {
        api.clearTombstones(this._handle, tableName);
    }
}

export default initFlatSQL;
