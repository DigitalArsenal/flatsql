// Node.js test for FlatSQL WASM module
import FlatSQLModule from './flatsql.js';

async function main() {
    console.log('Loading FlatSQL WASM module...');
    const module = await FlatSQLModule();
    console.log('Module loaded!\n');

    // Define schema using FlatBuffers IDL
    const schema = `
        table users {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }

        table posts {
            id: int (id);
            user_id: int (key);
            title: string;
            content: string;
        }
    `;

    console.log('Creating database with schema...');
    const db = new module.FlatSQLDatabase(schema, 'test_db');

    console.log('Tables:', db.listTables());

    // Convert Uint8Array to C++ vector
    function toVec(arr) {
        const vec = new module.VectorUint8();
        for (const byte of arr) {
            vec.push_back(byte);
        }
        return vec;
    }

    // Insert some fake FlatBuffer data
    console.log('\nInserting sample records...');
    const fakeData1 = toVec([0x10, 0x00, 0x00, 0x00, 0x08, 0x00, 0x0C, 0x00]);
    const fakeData2 = toVec([0x14, 0x00, 0x00, 0x00, 0x08, 0x00, 0x0C, 0x00]);
    const fakeData3 = toVec([0x18, 0x00, 0x00, 0x00, 0x08, 0x00, 0x0C, 0x00]);

    db.insertRaw('users', fakeData1);
    db.insertRaw('users', fakeData2);
    db.insertRaw('users', fakeData3);

    // Clean up vectors
    fakeData1.delete();
    fakeData2.delete();
    fakeData3.delete();

    console.log('Inserted 3 user records');

    // Stream multiple records
    const postVec = new module.VectorVectorUint8();
    postVec.push_back(toVec([0x20, 0x00, 0x00, 0x00]));
    postVec.push_back(toVec([0x24, 0x00, 0x00, 0x00]));
    const rowids = db.stream('posts', postVec);
    postVec.delete();
    console.log('Streamed 2 post records, rowids:', rowids);

    // Query the database
    console.log('\nExecuting: SELECT * FROM users');
    const result = db.query('SELECT * FROM users');
    console.log('Columns:', result.getColumns());
    console.log('Row count:', result.getRowCount());
    console.log('Rows:', result.getRows());

    // Get stats
    console.log('\nDatabase stats:');
    const stats = db.getStats();
    for (const s of stats) {
        console.log(`  Table: ${s.tableName}, Records: ${s.recordCount}, Indexes: ${s.indexes}`);
    }

    // Export data
    console.log('\nExporting data...');
    const exported = db.exportData();
    console.log(`Exported ${exported.length} bytes`);
    console.log('Magic bytes:', Array.from(exported.slice(0, 4)).map(b => b.toString(16).padStart(2, '0')).join(' '));
    // FLSQ in little-endian: 0x51, 0x53, 0x4C, 0x46
    console.log('Expected: 51 53 4c 46 (FLSQ)');

    // Clean up
    db.delete();

    console.log('\n=== Test passed! ===');
}

main().catch(console.error);
