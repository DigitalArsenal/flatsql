# FlatSQL - SQL Query Interface over FlatBuffer Storage

A C++/WASM engine that provides SQL query capabilities over data stored as stacked FlatBuffers.

## Key Features

- **FlatBuffer-native storage**: Data is stored as stacked FlatBuffers, readable by standard FlatBuffer tools
- **B-tree indexing**: In-memory B-trees for fast queries on indexed columns
- **SQL queries**: SELECT with WHERE, ORDER BY, LIMIT support
- **Schema parsing**: Supports FlatBuffers IDL and JSON Schema
- **Streaming inserts**: Efficiently stream multiple FlatBuffers into tables
- **WASM build**: Runs in browsers and Node.js

## Building

### Prerequisites

- CMake 3.20+
- C++17 compiler
- FlatBuffers source (expected at `../flatbuffers`)

### Native Build (for testing)

```bash
cd cpp
./scripts/build-native.sh
./build/flatsql_test
```

### WASM Build

```bash
cd cpp
./scripts/setup-emsdk.sh   # First time only - installs Emscripten
./scripts/build-wasm.sh
```

Output files will be in `../wasm/`:
- `flatsql.js` - JavaScript glue code
- `flatsql.wasm` - WebAssembly binary

## Usage (JavaScript)

```javascript
import { createDatabase } from './wasm/index.js';

// Define schema using FlatBuffers IDL
const schema = `
    table users {
        id: int (id);
        name: string;
        email: string (key);
    }
`;

// Create database
const db = await createDatabase(schema, 'mydb');

// Insert FlatBuffer data
db.insertRaw('users', flatbufferBytes);

// Stream multiple records
db.stream('users', [fb1, fb2, fb3]);

// Query
const result = db.query('SELECT * FROM users WHERE id = 1');
console.log(result.columns, result.rows);

// Export data (stacked FlatBuffers)
const exported = db.exportData();

// Clean up
db.delete();
```

## File Format

The storage format is append-only stacked FlatBuffers:

```
[File Header: 64 bytes]
  - Magic: "FLSQ" (4 bytes)
  - Version: 1 (4 bytes)
  - Data offset (8 bytes)
  - Record count (8 bytes)
  - Schema name (40 bytes)

[Record Header: 48 bytes][FlatBuffer data]
[Record Header: 48 bytes][FlatBuffer data]
...
```

Each FlatBuffer record is complete and valid, so you can:
1. Read individual records with standard FlatBuffer code
2. Skip to any record by offset
3. Append new records without rewriting

## Architecture

```
┌─────────────────────────────────────────────┐
│            FlatSQLDatabase                   │
├─────────────────────────────────────────────┤
│  SchemaParser  │  SQLParser                  │
├─────────────────────────────────────────────┤
│           TableStore (per table)             │
│  ┌─────────────────────────────────────┐    │
│  │  B-Tree indexes (per indexed col)    │    │
│  └─────────────────────────────────────┘    │
├─────────────────────────────────────────────┤
│        StackedFlatBufferStore               │
│  [Header][FB1][FB2][FB3]...                 │
└─────────────────────────────────────────────┘
```

## Limitations

- This is NOT SQLite - it's a purpose-built query engine for FlatBuffer data
- Field extraction requires a custom `FieldExtractor` function
- No UPDATE/DELETE operations (append-only)
- B-trees are in-memory (not persisted)
- Limited SQL syntax (no JOINs, subqueries, etc.)
