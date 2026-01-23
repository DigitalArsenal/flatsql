// FlatSQL Web Worker - Uses C API (no embind) for worker compatibility
import FlatSQLModule from './flatsql.js';
import { initFlatSQL } from './flatsql-api.js';

let flatsql = null;
let db = null;

const schema = `
    table User {
        id: int (id);
        name: string;
        email: string (key);
        age: int;
    }
    table Post {
        id: int (id);
        user_id: int (key);
        title: string;
    }
`;

function buildStream(buffers) {
    const parts = [];
    for (const buf of buffers) {
        const size = new Uint8Array(4);
        new DataView(size.buffer).setUint32(0, buf.length, true);
        parts.push(size, buf);
    }
    const total = parts.reduce((s, p) => s + p.length, 0);
    const result = new Uint8Array(total);
    let offset = 0;
    for (const p of parts) {
        result.set(p, offset);
        offset += p.length;
    }
    return result;
}

async function init() {
    flatsql = await initFlatSQL(FlatSQLModule);
    db = flatsql.createDatabase(schema, 'demo');
    db.registerFileId('USER', 'User');
    db.registerFileId('POST', 'Post');
    db.enableDemoExtractors();
    return { success: true };
}

function query(sql) {
    const result = db.query(sql);
    return { columns: result.columns, rows: result.rows };
}

const emailProviders = [
    'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'icloud.com',
    'protonmail.com', 'aol.com', 'zoho.com', 'fastmail.com', 'tutanota.com',
    'yandex.com', 'mail.com', 'gmx.com', 'hey.com', 'pm.me'
];

function randomProvider(id) {
    return emailProviders[id % emailProviders.length];
}

function registerSource(sourceName) {
    db.registerSource(sourceName);
    if (!registeredSources.includes(sourceName)) {
        registeredSources.push(sourceName);
    }
    return { success: true, source: sourceName };
}

function createUnifiedViews() {
    db.createUnifiedViews();
    return { success: true };
}

function listSources() {
    return db.listSources();
}

function streamUsers(count, startId, source = null) {
    const buffers = [];
    const userData = [];

    for (let i = 0; i < count; i++) {
        const id = startId + i + 1;
        const name = `User${id}`;
        const email = `user${id}@${randomProvider(id)}`;
        const age = 20 + (id % 50);

        const fb = flatsql.createTestUser(id, name, email, age);
        buffers.push(fb);
        userData.push({ id, name, email, age, fb: Array.from(fb) });
    }

    const stream = buildStream(buffers);
    db.ingest(stream, source);

    return {
        count: count,
        bytes: stream.length,
        source: source,
        samples: userData.slice(0, 5)
    };
}

// Track registered sources for clear operation
let registeredSources = [];

function clearAll() {
    if (db) db.destroy();
    db = flatsql.createDatabase(schema, 'demo');
    // Re-register all sources
    for (const src of registeredSources) {
        db.registerSource(src);
    }
    db.registerFileId('USER', 'User');
    db.registerFileId('POST', 'Post');
    db.enableDemoExtractors();
    if (registeredSources.length > 0) {
        db.createUnifiedViews();
    }
    return { success: true };
}

// Message handler
self.onmessage = async function(e) {
    const { id, action, params } = e.data;

    try {
        let result;

        switch (action) {
            case 'init':
                result = await init();
                break;
            case 'query':
                result = query(params.sql);
                break;
            case 'streamUsers':
                result = streamUsers(params.count, params.startId, params.source);
                break;
            case 'registerSource':
                result = registerSource(params.sourceName);
                break;
            case 'createUnifiedViews':
                result = createUnifiedViews();
                break;
            case 'listSources':
                result = listSources();
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
