// FlatSQL Web Worker - Handles database operations off the main thread
import FlatSQLModule from './flatsql.js';

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

function toVec(arr) {
    const vec = new flatsql.VectorUint8();
    for (const byte of arr) vec.push_back(byte);
    return vec;
}

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
    flatsql = await FlatSQLModule();
    db = new flatsql.FlatSQLDatabase(schema, 'demo');
    db.registerFileId('USER', 'User');
    db.registerFileId('POST', 'Post');
    db.enableDemoExtractors();
    return { success: true };
}

function query(sql) {
    const result = db.query(sql);
    const cols = result.getColumns();
    const rawRows = result.getRows();
    result.delete();

    // Convert any Uint8Array values to regular arrays for postMessage compatibility
    const rows = rawRows.map(row =>
        row.map(cell => {
            if (cell instanceof Uint8Array) {
                return Array.from(cell);
            }
            return cell;
        })
    );

    return { columns: cols, rows: rows };
}

function streamUsers(count, startId) {
    const buffers = [];
    const userData = [];

    for (let i = 0; i < count; i++) {
        const id = startId + i + 1;
        const name = `User${id}`;
        const email = `user${id}@test.com`;
        const age = 20 + (id % 50);

        const fb = flatsql.createTestUser(id, name, email, age);
        buffers.push(fb);
        userData.push({ id, name, email, age, fb: Array.from(fb) });
    }

    const stream = buildStream(buffers);
    const vec = toVec(stream);
    db.ingest(vec);
    vec.delete();

    return {
        count: count,
        bytes: stream.length,
        samples: userData.slice(0, 5)
    };
}

function streamMixed(startId) {
    const buffers = [];
    const logData = [];

    for (let i = 0; i < 10; i++) {
        const userId = startId + i + 1;
        const userFb = flatsql.createTestUser(userId, `MixedUser${userId}`, `mixed${userId}@test.com`, 25 + i);
        buffers.push(userFb);
        logData.push({ type: 'User', id: userId, fb: Array.from(userFb) });

        for (let j = 0; j < 3; j++) {
            const postId = userId * 100 + j;
            const postFb = flatsql.createTestPost(postId, userId, `Post ${postId} by user ${userId}`);
            buffers.push(postFb);
            logData.push({ type: 'Post', id: postId, userId, fb: Array.from(postFb) });
        }
    }

    const stream = buildStream(buffers);
    const vec = toVec(stream);
    db.ingest(vec);
    vec.delete();

    return {
        count: buffers.length,
        bytes: stream.length,
        samples: logData.slice(0, 8)
    };
}

function clearAll() {
    if (db) db.delete();
    db = new flatsql.FlatSQLDatabase(schema, 'demo');
    db.registerFileId('USER', 'User');
    db.registerFileId('POST', 'Post');
    db.enableDemoExtractors();
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
                result = streamUsers(params.count, params.startId);
                break;
            case 'streamMixed':
                result = streamMixed(params.startId);
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
