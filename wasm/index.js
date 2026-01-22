// FlatSQL WASM Module Wrapper
// Provides a clean JavaScript API for the FlatSQL WASM module

import FlatSQLModule from './flatsql.js';

let modulePromise = null;
let module = null;

/**
 * Initialize the FlatSQL WASM module.
 * Call this once before using other functions.
 * @returns {Promise<void>}
 */
export async function init() {
    if (!modulePromise) {
        modulePromise = FlatSQLModule();
    }
    module = await modulePromise;
    return module;
}

/**
 * Get the initialized module instance.
 * @returns {Promise<Object>}
 */
async function getModule() {
    if (!module) {
        await init();
    }
    return module;
}

/**
 * Create a new FlatSQL database from a schema.
 * @param {string} schema - FlatBuffers IDL or JSON Schema
 * @param {string} [name='default'] - Database name
 * @returns {Promise<FlatSQLDatabase>}
 */
export async function createDatabase(schema, name = 'default') {
    const m = await getModule();
    const db = new m.FlatSQLDatabase(schema, name);
    return new FlatSQLDatabase(db, m);
}

/**
 * FlatSQL Database wrapper class.
 * Provides a clean JavaScript API over the WASM module.
 */
export class FlatSQLDatabase {
    constructor(wasmDb, module) {
        this._db = wasmDb;
        this._module = module;
    }

    /**
     * Convert a JavaScript Uint8Array to a C++ vector.
     * @private
     */
    _toVec(arr) {
        const vec = new this._module.VectorUint8();
        for (const byte of arr) {
            vec.push_back(byte);
        }
        return vec;
    }

    /**
     * Execute a SQL query.
     * @param {string} sql - SQL query string
     * @returns {{columns: string[], rows: any[][], rowCount: number}}
     */
    query(sql) {
        const result = this._db.query(sql);
        const ret = {
            columns: result.getColumns(),
            rows: result.getRows(),
            rowCount: result.getRowCount()
        };
        result.delete();
        return ret;
    }

    /**
     * Insert raw FlatBuffer data.
     * @param {string} tableName - Name of the table
     * @param {Uint8Array} data - FlatBuffer binary data
     * @returns {number} Row ID
     */
    insertRaw(tableName, data) {
        const vec = this._toVec(data);
        const rowid = this._db.insertRaw(tableName, vec);
        vec.delete();
        return rowid;
    }

    /**
     * Stream multiple FlatBuffers into a table.
     * @param {string} tableName - Name of the table
     * @param {Uint8Array[]} flatbuffers - Array of FlatBuffer data
     * @returns {number[]} Array of row IDs
     */
    stream(tableName, flatbuffers) {
        const vecVec = new this._module.VectorVectorUint8();
        for (const fb of flatbuffers) {
            vecVec.push_back(this._toVec(fb));
        }
        const rowids = this._db.stream(tableName, vecVec);
        vecVec.delete();
        return rowids;
    }

    /**
     * Export the database as a single binary blob.
     * This is a stacked FlatBuffer file readable by standard FlatBuffer tools.
     * @returns {Uint8Array}
     */
    exportData() {
        return new Uint8Array(this._db.exportData());
    }

    /**
     * List all tables in the database.
     * @returns {string[]}
     */
    listTables() {
        return this._db.listTables();
    }

    /**
     * Get database statistics.
     * @returns {Array<{tableName: string, recordCount: number, indexes: string[]}>}
     */
    getStats() {
        return this._db.getStats();
    }

    /**
     * Clean up WASM resources.
     */
    delete() {
        if (this._db) {
            this._db.delete();
            this._db = null;
        }
    }
}

export default {
    init,
    createDatabase,
    FlatSQLDatabase
};
