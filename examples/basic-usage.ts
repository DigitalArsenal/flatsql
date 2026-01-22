// Basic usage example for FlatBuffers-SQLite
// Demonstrates creating a database, inserting records, and querying

import { FlatSQLDatabase, FlatcAccessor, parseSchema } from '../src/index.js';

// This example assumes flatc-wasm is available
// import { FlatcRunner } from 'flatc-wasm';

async function main() {
  // Initialize flatc-wasm
  // const flatc = await FlatcRunner.init();

  // Define your FlatBuffer schema
  const schema = `
    namespace App;

    // User table
    table User {
      id: int (key);
      name: string (required);
      email: string;
      age: int;
      created_at: long;
    }

    // Event log table
    table Event {
      id: long (key);
      user_id: int (indexed);
      type: string (indexed);
      payload: string;
      timestamp: long (indexed);
    }

    root_type User;
  `;

  // Create the database
  // const accessor = new FlatcAccessor(flatc, schema);
  // const db = FlatSQLDatabase.fromSchema(schema, accessor, 'myapp');

  // For testing without flatc-wasm, use a mock accessor
  const mockAccessor = {
    getField(data: Uint8Array, path: string[]) {
      const json = new TextDecoder().decode(data);
      let obj = JSON.parse(json);
      for (const key of path) {
        if (obj === null || obj === undefined) return null;
        obj = obj[key];
      }
      return obj;
    },
    buildBuffer(tableName: string, fields: Record<string, any>) {
      return new TextEncoder().encode(JSON.stringify(fields));
    }
  };

  const db = FlatSQLDatabase.fromSchema(schema, mockAccessor, 'myapp');

  console.log('Tables:', db.listTables());
  console.log('User table:', db.getTableDef('User'));

  // Insert some users
  const userId1 = db.insert('User', {
    id: 1,
    name: 'Alice',
    email: 'alice@example.com',
    age: 30,
    created_at: Date.now()
  });

  const userId2 = db.insert('User', {
    id: 2,
    name: 'Bob',
    email: 'bob@example.com',
    age: 25,
    created_at: Date.now()
  });

  console.log('Inserted users:', userId1, userId2);

  // Insert some events
  const events = [
    { id: 1, user_id: 1, type: 'login', payload: '{}', timestamp: Date.now() },
    { id: 2, user_id: 1, type: 'page_view', payload: '{"page":"/home"}', timestamp: Date.now() },
    { id: 3, user_id: 2, type: 'login', payload: '{}', timestamp: Date.now() },
    { id: 4, user_id: 1, type: 'click', payload: '{"button":"submit"}', timestamp: Date.now() },
  ];

  for (const event of events) {
    db.insert('Event', event);
  }

  // Query all users
  console.log('\n--- All Users ---');
  const allUsers = db.query('SELECT * FROM User');
  console.log('Columns:', allUsers.columns);
  console.log('Rows:', allUsers.rows);

  // Query with condition
  console.log('\n--- Users age 25 ---');
  const youngUsers = db.query('SELECT name, email FROM User WHERE age = 25');
  console.log('Rows:', youngUsers.rows);

  // Query events
  console.log('\n--- All Events ---');
  const allEvents = db.query('SELECT * FROM Event');
  console.log('Event count:', allEvents.rowCount);

  // Get statistics
  console.log('\n--- Database Stats ---');
  const stats = db.getStats();
  for (const table of stats) {
    console.log(`${table.tableName}: ${table.recordCount} records, indexes: ${table.indexes.join(', ')}`);
  }

  // Export data (this is standard stacked FlatBuffers format)
  const exportedData = db.exportData();
  console.log('\n--- Exported Data ---');
  console.log(`Total size: ${exportedData.length} bytes`);
  console.log('This data can be read by standard FlatBuffer tools!');
}

main().catch(console.error);
