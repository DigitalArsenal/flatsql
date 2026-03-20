// FlatSQL Web Worker - Uses C API (no embind) for worker compatibility
import FlatSQLModule from './flatsql.js';
import { initFlatSQL } from './flatsql-api.js';
import { createDemoRecords, getDemoDataset } from './demo-datasets.js';

let flatsql = null;
let db = null;
let currentDatasetKey = 'user';

function buildStream(buffers) {
    const parts = [];
    for (const buf of buffers) {
        const size = new Uint8Array(4);
        new DataView(size.buffer).setUint32(0, buf.length, true);
        parts.push(size, buf);
    }
    const total = parts.reduce((sum, part) => sum + part.length, 0);
    const result = new Uint8Array(total);
    let offset = 0;
    for (const part of parts) {
        result.set(part, offset);
        offset += part.length;
    }
    return result;
}

function resetDatabase(datasetKey = currentDatasetKey) {
    const dataset = getDemoDataset(datasetKey);
    if (db) {
        db.destroy();
    }
    db = flatsql.createDatabase(dataset.schema, 'demo');
    db.registerFileId(dataset.fileId, dataset.tableName);
    db.enableDemoExtractors();
    currentDatasetKey = dataset.key;
    return { success: true, datasetKey: dataset.key };
}

async function init(datasetKey = 'user') {
    if (!flatsql) {
        flatsql = await initFlatSQL(FlatSQLModule);
    }
    return resetDatabase(datasetKey);
}

function query(sql) {
    const result = db.query(sql);
    return { columns: result.columns, rows: result.rows };
}

function streamRecords(count, startId) {
    const { buffers, samples } = createDemoRecords(flatsql, currentDatasetKey, count, startId);
    const stream = buildStream(buffers);
    db.ingest(stream);

    return {
        count,
        bytes: stream.length,
        samples
    };
}

function clearAll() {
    return resetDatabase(currentDatasetKey);
}

self.onmessage = async function(e) {
    const { id, action, params } = e.data;

    try {
        let result;

        switch (action) {
            case 'init':
                result = await init(params.datasetKey);
                break;
            case 'setDataset':
                result = resetDatabase(params.datasetKey);
                break;
            case 'query':
                result = query(params.sql);
                break;
            case 'streamRecords':
                result = streamRecords(params.count, params.startId);
                break;
            case 'clear':
                result = clearAll();
                break;
            default:
                throw new Error(`Unknown action: ${action}`);
        }

        self.postMessage({ id, success: true, result });
    } catch (error) {
        self.postMessage({ id, success: false, error: error.message });
    }
};
