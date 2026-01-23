# FlatSQL

[![CI](https://github.com/DigitalArsenal/flatsql/actions/workflows/ci.yml/badge.svg)](https://github.com/DigitalArsenal/flatsql/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/flatsql.svg)](https://www.npmjs.com/package/flatsql)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)

**SQL queries over raw FlatBuffer storage** — A streaming query engine that keeps data in native FlatBuffer format while providing SQL access via SQLite virtual tables.

## Live Demo

Try FlatSQL in your browser: **[https://digitalarsenal.github.io/flatsql/](https://digitalarsenal.github.io/flatsql/)**

## Installation

```bash
npm install flatsql
```

## Quick Start

```javascript
import { initFlatSQL } from 'flatsql/wasm';

// Initialize
const flatsql = await initFlatSQL();

// Create database with schema
const db = flatsql.createDatabase(`
  table User {
    id: int (id);
    name: string;
    email: string (key);
    age: int;
  }
`, 'myapp');

// Register file identifier for routing
db.registerFileId('USER', 'User');

// Ingest FlatBuffer data (streaming)
db.ingest(flatbufferStream);

// Query with SQL
const result = db.query('SELECT * FROM User WHERE age > 25');
console.log(result.columns, result.rows);
```

## What is FlatSQL?

FlatSQL bridges two technologies:

- **[FlatBuffers](https://github.com/digitalarsenal/flatbuffers)** — Google's efficient cross-platform serialization library. Data is stored in binary format with zero-copy access (no parsing/unpacking needed).
- **SQLite** — The most widely deployed SQL database engine, used here only for SQL parsing and query execution.

The key insight: instead of converting FlatBuffers to SQLite rows (expensive), FlatSQL uses [SQLite virtual tables](https://sqlite.org/vtab.html) to query FlatBuffer data directly. Your data stays in portable FlatBuffer format, readable by any FlatBuffer tooling, while you get SQL query capabilities.

## Why FlatSQL?

Traditional approach:
```
FlatBuffer → Deserialize → SQLite rows → Query → Serialize → FlatBuffer
```

FlatSQL approach:
```
FlatBuffer → Query (via virtual table) → FlatBuffer
```

**Benefits:**
- **Zero conversion overhead** — Data stays in FlatBuffer format
- **Streaming ingestion** — Indexes built during data arrival, not after
- **Portable output** — Exported data is standard FlatBuffers, readable by any tooling
- **Multi-source federation** — Query across multiple FlatBuffer sources with automatic source tagging

## Source Code

| Repository | Description |
|------------|-------------|
| [digitalarsenal/flatsql](https://github.com/digitalarsenal/flatsql) | This project — FlatSQL query engine |
| [digitalarsenal/flatbuffers](https://github.com/digitalarsenal/flatbuffers) | Fork of Google FlatBuffers with WASM support |
| [flatc-wasm](https://digitalarsenal.github.io/flatbuffers/) | FlatBuffer compiler running in WebAssembly |

## Usage

### WASM (Browser/Node.js)

The C++ engine compiles to WebAssembly for cross-platform deployment:

```javascript
import { initFlatSQL } from 'flatsql/wasm';

const flatsql = await initFlatSQL();

// Create database with schema
const db = flatsql.createDatabase(`
  table User {
    id: int (id);
    name: string;
    email: string (key);
    age: int;
  }
`, 'myapp');

// Register file identifier routing
db.registerFileId('USER', 'User');

// Enable demo field extractors (for testing)
db.enableDemoExtractors();

// Ingest FlatBuffer stream
// Format: [4-byte size LE][FlatBuffer][4-byte size LE][FlatBuffer]...
db.ingest(streamData);

// Query with SQL
const result = db.query('SELECT id, name, email FROM User WHERE age > 25');
console.log(result.columns); // ['id', 'name', 'email']
console.log(result.rows);    // [[1, 'Alice', 'alice@example.com'], ...]

// Export database
const exported = db.exportData();

// Cleanup
db.destroy();
```

### TypeScript (Pure JavaScript)

A TypeScript implementation for environments where WASM isn't available:

```typescript
import { FlatSQLDatabase, FlatcAccessor } from 'flatsql';
import { FlatcRunner } from 'flatc-wasm';

const flatc = await FlatcRunner.init();

const schema = `
  namespace App;

  table User {
    id: int (key);
    name: string (required);
    email: string;
    age: int;
  }
`;

const accessor = new FlatcAccessor(flatc, schema);
const db = FlatSQLDatabase.fromSchema(schema, accessor, 'myapp');

// Insert records
db.insert('User', { id: 1, name: 'Alice', email: 'alice@example.com', age: 30 });
db.insert('User', { id: 2, name: 'Bob', email: 'bob@example.com', age: 25 });

// Query
const result = db.query('SELECT name, email FROM User WHERE age > 20');
console.log(result.rows);

// Export as standard FlatBuffers
const exported = db.exportData();
```

### Native C++ (Embedded)

For performance-critical applications, link the C++ library directly:

```cpp
#include <flatsql/database.h>

auto db = flatsql::FlatSQLDatabase::fromSchema(schema, "mydb");

// Register file ID routing
db.registerFileId("USER", "User");

// Set field extractor
db.setFieldExtractor("User", extractUserField);

// Ingest streaming data
size_t recordsIngested = 0;
db.ingest(data, length, &recordsIngested);

// Query
auto result = db.query("SELECT * FROM User WHERE id = 5");
for (size_t i = 0; i < result.rowCount(); i++) {
    std::cout << result.getString(i, "name") << std::endl;
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     FlatSQLDatabase                          │
├─────────────────────────────────────────────────────────────┤
│   SchemaParser        │      SQLiteEngine                    │
│   (FlatBuffers IDL)   │      (Virtual Tables)                │
├─────────────────────────────────────────────────────────────┤
│                    TableStore (per table)                    │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  B-Tree Indexes            Field Extractors             │ │
│  │  (id, email, timestamp)    (getField callbacks)         │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│              StackedFlatBufferStore (append-only)            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ [Header][FB₁][FB₂][FB₃]...                           │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Stream Format

FlatSQL ingests size-prefixed FlatBuffer streams:

```
[4-byte size LE][FlatBuffer with file_id][4-byte size LE][FlatBuffer]...
```

The 4-byte file identifier in each FlatBuffer determines which table receives the record.

## SQL Support

### Supported

- `SELECT` with column selection
- `WHERE` with `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `LIKE`, `AND`, `OR`
- `ORDER BY` (ASC/DESC)
- `LIMIT` and `OFFSET`
- `COUNT(*)` aggregate
- Index-accelerated queries on `(id)` and `(key)` columns

### Not Supported

- `JOIN` (query one table at a time)
- `GROUP BY`, `HAVING`, most aggregates
- `INSERT`, `UPDATE`, `DELETE` (use API methods instead)
- Subqueries, CTEs, window functions

## Performance

FlatSQL outperforms traditional SQLite on query operations:

| Operation | FlatSQL | SQLite | Speedup |
|-----------|---------|--------|---------|
| Point query (by id) | 3.50 ms | 3.93 ms | 1.1x |
| Point query (by key) | 5.23 ms | 6.94 ms | 1.3x |
| Direct index lookup | 1.56 ms | 3.93 ms | 2.5x |
| Full scan | 0.84 ms | 1.25 ms | 1.5x |
| Direct iteration | 0.05 ms | 1.25 ms | 25x |

*Benchmarks: 10,000 records, 10,000 query iterations, Apple M3 Ultra*

## Building from Source

### Prerequisites

- Node.js 18+
- CMake 3.20+ (for WASM builds)
- Emscripten (for WASM builds)

### TypeScript Build

```bash
npm install
npm run build
npm test
```

### WASM Build

```bash
# Clone DA-FlatBuffers (required dependency)
git clone https://github.com/DigitalArsenal/flatbuffers.git ../flatbuffers

# Build WASM
cd cpp
emcmake cmake -B build-wasm -DCMAKE_BUILD_TYPE=Release
cmake --build build-wasm
```

Output: `wasm/flatsql.js` and `wasm/flatsql.wasm`

### Run Demo Locally

```bash
npm run serve
# Open http://localhost:8081
```

## API Reference

### initFlatSQL()

```typescript
import { initFlatSQL } from 'flatsql/wasm';

const flatsql = await initFlatSQL();
```

### createDatabase(schema, name)

```typescript
const db = flatsql.createDatabase(schemaString, 'dbname');
```

### db.registerFileId(fileId, tableName)

```typescript
db.registerFileId('USER', 'User');  // Route "USER" FlatBuffers to User table
```

### db.ingest(data)

```typescript
const bytesConsumed = db.ingest(uint8ArrayStream);
```

### db.query(sql)

```typescript
const result = db.query('SELECT * FROM User WHERE age > 25');
// result.columns: string[]
// result.rows: any[][]
```

### db.exportData()

```typescript
const data = db.exportData();  // Returns Uint8Array
```

## License

Apache 2.0

## Contributing

Contributions welcome. Please open an issue first to discuss significant changes.

## Contact

For questions, licensing inquiries, or commercial support: [tj@digitalarsenal.io](mailto:tj@digitalarsenal.io)

---

Built on [DA-FlatBuffers](https://digitalarsenal.github.io/flatbuffers/) and [SQLite](https://sqlite.org/).
