// FlatSQL JavaScript API - Uses C exports (worker-compatible)
// This wrapper uses cwrap/ccall instead of embind to avoid worker issues

import FlatSQLModule from './flatsql.js';

let Module = null;
let api = null;

/**
 * Initialize the FlatSQL WASM module
 * @param {any} moduleFactory - Optional module factory (defaults to FlatSQLModule)
 * @returns {Promise<FlatSQL>}
 */
export async function initFlatSQL(moduleFactory) {
    Module = await (moduleFactory || FlatSQLModule)();

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
