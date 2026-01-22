// Streaming example for FlatBuffers-SQLite
// Shows how to ingest FlatBuffers from a stream and query them

import { FlatSQLDatabase, FlatcAccessor, parseSchema, StackedFlatBufferStore } from '../src/index.js';

// Simulates streaming FlatBuffer data from an external source
async function* generateEventStream(count: number) {
  const encoder = new TextEncoder();

  for (let i = 0; i < count; i++) {
    // In real usage, this would be actual FlatBuffer binary data
    // Here we use JSON as a mock
    const event = {
      id: i,
      type: ['click', 'view', 'scroll', 'submit'][i % 4],
      user_id: Math.floor(Math.random() * 100),
      timestamp: Date.now() + i * 1000,
      metadata: JSON.stringify({ index: i }),
    };

    // Simulate network latency
    await new Promise(resolve => setTimeout(resolve, 10));

    yield encoder.encode(JSON.stringify(event));
  }
}

async function main() {
  const schema = `
    namespace Analytics;

    table Event {
      id: int (key);
      type: string (indexed);
      user_id: int (indexed);
      timestamp: long (indexed);
      metadata: string;
    }

    root_type Event;
  `;

  // Mock accessor for demonstration
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

  const db = FlatSQLDatabase.fromSchema(schema, mockAccessor, 'analytics');

  console.log('Starting stream ingestion...');
  const startTime = Date.now();
  let count = 0;

  // Stream in events
  for await (const flatbufferData of generateEventStream(1000)) {
    db.insertRaw('Event', flatbufferData);
    count++;

    if (count % 100 === 0) {
      console.log(`Ingested ${count} events...`);
    }
  }

  const elapsed = Date.now() - startTime;
  console.log(`\nIngested ${count} events in ${elapsed}ms (${(count / elapsed * 1000).toFixed(0)} events/sec)`);

  // Query the data
  console.log('\n--- Query Results ---');

  // Count by type
  const allEvents = db.query('SELECT type FROM Event');
  const typeCounts = new Map<string, number>();
  for (const row of allEvents.rows) {
    const type = row[allEvents.columns.indexOf('type')];
    typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
  }
  console.log('Events by type:', Object.fromEntries(typeCounts));

  // Get recent events (using timestamp range would require index lookup)
  console.log('Total events:', allEvents.rowCount);

  // Export for external analysis
  const data = db.exportData();
  console.log(`\nExported data size: ${data.length} bytes`);
  console.log('Data format: Stacked FlatBuffers (readable by standard FB tools)');

  // Show that we can reconstruct from exported data
  const store = StackedFlatBufferStore.fromData(data);
  console.log(`Restored store: ${store.getRecordCount()} records`);

  // Iterate raw records (demonstrates standard FlatBuffer access)
  console.log('\nFirst 3 records from raw data:');
  let i = 0;
  for (const record of store.iterateRecords()) {
    if (i++ >= 3) break;
    const decoded = new TextDecoder().decode(record.data);
    console.log(`  Offset ${record.offset}: ${decoded.substring(0, 60)}...`);
  }
}

main().catch(console.error);
