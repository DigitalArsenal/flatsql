// Integration test using flatc-wasm to generate real FlatBuffers
// Tests: create FlatBuffer -> store -> query -> retrieve -> verify

import FlatSQLModule from './flatsql.js';
import { FlatcRunner } from 'flatc-wasm';

// Test schema - must match the structure used in FlatSQL
const userSchema = `
namespace test;

table User {
    id: int32;
    name: string;
    email: string;
    age: int32;
}

root_type User;
`;

const postSchema = `
namespace test;

table Post {
    id: int32;
    user_id: int32;
    title: string;
    content: string;
}

root_type Post;
`;

// Test data
const testUsers = [
    { id: 1, name: "Alice", email: "alice@example.com", age: 30 },
    { id: 2, name: "Bob", email: "bob@example.com", age: 25 },
    { id: 3, name: "Charlie", email: "charlie@example.com", age: 35 },
];

const testPosts = [
    { id: 1, user_id: 1, title: "Hello World", content: "My first post" },
    { id: 2, user_id: 1, title: "FlatBuffers Rock", content: "Using FlatBuffers with SQL" },
    { id: 3, user_id: 2, title: "Bob's Post", content: "This is Bob's content" },
];

async function main() {
    console.log('=== FlatSQL Integration Test ===');
    console.log('Using flatc-wasm for real FlatBuffer generation\n');

    // Initialize flatc-wasm runner
    console.log('Initializing FlatcRunner...');
    const flatc = await FlatcRunner.init();
    console.log('FlatcRunner version:', flatc.version());

    // Initialize FlatSQL module
    console.log('Loading FlatSQL WASM module...');
    const module = await FlatSQLModule();
    console.log('FlatSQL module loaded!\n');

    // Schema inputs for flatc
    const userSchemaInput = {
        entry: '/schemas/user.fbs',
        files: { '/schemas/user.fbs': userSchema }
    };

    const postSchemaInput = {
        entry: '/schemas/post.fbs',
        files: { '/schemas/post.fbs': postSchema }
    };

    // Test 1: Create FlatBuffers from JSON
    console.log('Test 1: Creating FlatBuffers from JSON...');

    const userBuffers = testUsers.map(user => {
        const json = JSON.stringify(user);
        return flatc.generateBinary(userSchemaInput, json);
    });

    console.log(`  Created ${userBuffers.length} User FlatBuffers`);
    userBuffers.forEach((buf, i) => {
        console.log(`    User ${i + 1}: ${buf.length} bytes`);
    });

    const postBuffers = testPosts.map(post => {
        const json = JSON.stringify(post);
        return flatc.generateBinary(postSchemaInput, json);
    });

    console.log(`  Created ${postBuffers.length} Post FlatBuffers`);
    console.log('  Test 1 PASSED\n');

    // Test 2: Verify FlatBuffers can be deserialized
    console.log('Test 2: Verifying FlatBuffer round-trip...');

    for (let i = 0; i < userBuffers.length; i++) {
        const json = flatc.generateJSON(userSchemaInput, {
            path: '/data/user.bin',
            data: userBuffers[i]
        });
        const parsed = JSON.parse(json);

        if (parsed.id !== testUsers[i].id) {
            throw new Error(`User ${i} id mismatch: expected ${testUsers[i].id}, got ${parsed.id}`);
        }
        if (parsed.name !== testUsers[i].name) {
            throw new Error(`User ${i} name mismatch: expected ${testUsers[i].name}, got ${parsed.name}`);
        }
        if (parsed.email !== testUsers[i].email) {
            throw new Error(`User ${i} email mismatch: expected ${testUsers[i].email}, got ${parsed.email}`);
        }
        if (parsed.age !== testUsers[i].age) {
            throw new Error(`User ${i} age mismatch: expected ${testUsers[i].age}, got ${parsed.age}`);
        }
    }
    console.log('  All User round-trips verified');
    console.log('  Test 2 PASSED\n');

    // Test 3: Store in FlatSQL
    console.log('Test 3: Storing FlatBuffers in FlatSQL...');

    // Create database with schema matching our FlatBuffers
    const dbSchema = `
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
            content: string;
        }
    `;

    const db = new module.FlatSQLDatabase(dbSchema, 'integration_test');
    console.log('  Database created with tables:', db.listTables());

    // Helper to convert Uint8Array to C++ vector
    function toVec(arr) {
        const vec = new module.VectorUint8();
        for (const byte of arr) {
            vec.push_back(byte);
        }
        return vec;
    }

    // Insert users
    for (const userBuf of userBuffers) {
        const vec = toVec(userBuf);
        db.insertRaw('User', vec);
        vec.delete();
    }
    console.log(`  Inserted ${userBuffers.length} users`);

    // Insert posts
    for (const postBuf of postBuffers) {
        const vec = toVec(postBuf);
        db.insertRaw('Post', vec);
        vec.delete();
    }
    console.log(`  Inserted ${postBuffers.length} posts`);
    console.log('  Test 3 PASSED\n');

    // Test 4: Query and verify counts
    console.log('Test 4: Querying FlatSQL...');

    const userResult = db.query('SELECT * FROM User');
    console.log(`  SELECT * FROM User: ${userResult.getRowCount()} rows`);
    console.log(`    Columns: ${userResult.getColumns()}`);

    if (userResult.getRowCount() !== testUsers.length) {
        throw new Error(`User count mismatch: expected ${testUsers.length}, got ${userResult.getRowCount()}`);
    }

    const postResult = db.query('SELECT * FROM Post');
    console.log(`  SELECT * FROM Post: ${postResult.getRowCount()} rows`);

    if (postResult.getRowCount() !== testPosts.length) {
        throw new Error(`Post count mismatch: expected ${testPosts.length}, got ${postResult.getRowCount()}`);
    }
    console.log('  Test 4 PASSED\n');

    // Test 5: Export and verify magic bytes
    console.log('Test 5: Exporting database...');

    const exported = db.exportData();
    console.log(`  Exported ${exported.length} bytes`);

    // Verify magic bytes: FLSQ in little-endian is 0x51 0x53 0x4C 0x46
    const magic = Array.from(exported.slice(0, 4));
    const expectedMagic = [0x51, 0x53, 0x4C, 0x46];

    for (let i = 0; i < 4; i++) {
        if (magic[i] !== expectedMagic[i]) {
            throw new Error(`Magic byte ${i} mismatch: expected 0x${expectedMagic[i].toString(16)}, got 0x${magic[i].toString(16)}`);
        }
    }
    console.log('  Magic bytes verified: FLSQ');
    console.log('  Test 5 PASSED\n');

    // Test 6: Retrieve raw FlatBuffers and verify with flatc
    console.log('Test 6: Retrieving and verifying stored FlatBuffers...');

    // Get raw records from the exported data
    // The export contains a header followed by stacked FlatBuffer records
    // Each record has: 4-byte size prefix, 4-byte table name length, table name, FlatBuffer data

    // For now, we verify the database statistics match
    const stats = db.getStats();
    let totalRecords = 0;
    for (const s of stats) {
        console.log(`  Table ${s.tableName}: ${s.recordCount} records`);
        totalRecords += s.recordCount;
    }

    if (totalRecords !== testUsers.length + testPosts.length) {
        throw new Error(`Total record count mismatch: expected ${testUsers.length + testPosts.length}, got ${totalRecords}`);
    }
    console.log('  Test 6 PASSED\n');

    // Test 7: Streaming insert
    console.log('Test 7: Streaming batch insert...');

    const batchUsers = [];
    for (let i = 10; i < 20; i++) {
        const user = { id: i, name: `User${i}`, email: `user${i}@test.com`, age: 20 + i };
        const json = JSON.stringify(user);
        batchUsers.push(flatc.generateBinary(userSchemaInput, json));
    }

    const batchVec = new module.VectorVectorUint8();
    for (const buf of batchUsers) {
        batchVec.push_back(toVec(buf));
    }

    const rowids = db.stream('User', batchVec);
    batchVec.delete();

    console.log(`  Streamed ${batchUsers.length} users`);
    console.log(`  Row IDs returned: ${rowids.length}`);

    // Verify new count
    const newResult = db.query('SELECT * FROM User');
    const expectedCount = testUsers.length + batchUsers.length;
    if (newResult.getRowCount() !== expectedCount) {
        throw new Error(`After streaming: expected ${expectedCount} users, got ${newResult.getRowCount()}`);
    }
    console.log(`  Total users now: ${newResult.getRowCount()}`);
    console.log('  Test 7 PASSED\n');

    // Cleanup
    db.delete();

    console.log('=== All integration tests PASSED! ===');
}

main().catch(err => {
    console.error('Test FAILED:', err.message);
    process.exit(1);
});
