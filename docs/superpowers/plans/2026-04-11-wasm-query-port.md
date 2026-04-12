# WASM Query Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port parameterized and batched query execution into the portable FlatSQL WASM runtime while preserving native SQLite semantics and verifying statement-cache correctness against upstream SQLite behavior.

**Architecture:** Keep all SQL semantics inside the existing C++ `SQLiteEngine`, expose parameterized and batched query entrypoints through the C/WASM boundary, and extend the JS/TS WASM wrapper to marshal parameters and decode multiple results. Use native tests to harden statement-cache reuse, then use Jest-based WASM tests to verify API behavior and semantic parity with literal SQL.

**Tech Stack:** C++17, vendored SQLite, Emscripten, WASM C API exports, ES module wrapper, TypeScript typings, Jest, Node

---

### Task 1: Harden Native Statement Cache Reuse

**Files:**
- Modify: `cpp/src/sqlite_engine.cpp`
- Modify: `cpp/include/flatsql/sqlite_engine.h`
- Modify: `cpp/test/test_main.cpp`

- [ ] **Step 1: Write the failing native regression test**

Add a targeted regression in `cpp/test/test_main.cpp` that proves cached statements do not retain old bindings:

```cpp
void testSQLiteParameterizedStatementReuse() {
    std::cout << "Testing SQLite parameterized statement reuse..." << std::endl;

    SQLiteEngine engine;
    char* errMsg = nullptr;
    int rc = sqlite3_exec(
        engine.getDb(),
        "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, email TEXT, active INTEGER);"
        "INSERT INTO cache_test VALUES (1, 'alice@example.com', 1);"
        "INSERT INTO cache_test VALUES (2, 'bob@example.com', 0);",
        nullptr,
        nullptr,
        &errMsg
    );
    assert(rc == SQLITE_OK);

    QueryResult alice = engine.execute(
        "SELECT email FROM cache_test WHERE id = ?",
        { int64_t(1) }
    );
    assert(alice.rowCount() == 1);
    assert(std::get<std::string>(alice.rows[0][0]) == "alice@example.com");

    QueryResult bob = engine.execute(
        "SELECT email FROM cache_test WHERE id = ?",
        { int64_t(2) }
    );
    assert(bob.rowCount() == 1);
    assert(std::get<std::string>(bob.rows[0][0]) == "bob@example.com");

    QueryResult staleBinding = engine.execute(
        "SELECT ?1 AS first_value, ?2 IS NULL AS second_is_null",
        { int64_t(9), std::string("sticky") }
    );
    assert(std::get<int64_t>(staleBinding.rows[0][1]) == 0);

    QueryResult clearedBinding = engine.execute(
        "SELECT ?1 AS first_value, ?2 IS NULL AS second_is_null",
        { int64_t(10) }
    );
    assert(std::get<int64_t>(clearedBinding.rows[0][0]) == 10);
    assert(std::get<int64_t>(clearedBinding.rows[0][1]) == 1);

    std::cout << "SQLite parameterized statement reuse tests passed!" << std::endl;
}
```

Call `testSQLiteParameterizedStatementReuse();` from `main()` after `testSQLiteEngine();`.

- [ ] **Step 2: Run the native test target and verify it fails**

Run: `cmake -S cpp -B cpp/build-native -DCMAKE_BUILD_TYPE=Release && cmake --build cpp/build-native --target flatsql_test -j4 && ./cpp/build-native/flatsql_test`
Expected: FAIL in `testSQLiteParameterizedStatementReuse()` because the cached statement retains an old `?2` binding

- [ ] **Step 3: Reset and clear cached bindings on every statement reuse**

Add a helper to `cpp/include/flatsql/sqlite_engine.h` and use it from `cpp/src/sqlite_engine.cpp`:

```cpp
private:
    void resetPreparedStatement(sqlite3_stmt* stmt) const;
```

```cpp
void SQLiteEngine::resetPreparedStatement(sqlite3_stmt* stmt) const {
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
}

sqlite3_stmt* SQLiteEngine::getOrPrepareStmt(const std::string& sql) const {
    auto it = stmtCache_.find(sql);
    if (it != stmtCache_.end()) {
        resetPreparedStatement(it->second);
        return it->second;
    }

    if (stmtCache_.size() >= MAX_STMT_CACHE_SIZE) {
        for (auto& [cachedSql, cachedStmt] : stmtCache_) {
            if (cachedStmt) {
                sqlite3_finalize(cachedStmt);
            }
        }
        stmtCache_.clear();
    }

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        throw std::runtime_error("SQL error: " + std::string(sqlite3_errmsg(db_)));
    }

    stmtCache_[sql] = stmt;
    return stmt;
}
```

At the end of `SQLiteEngine::execute(...)`, reset the statement before returning or throwing:

```cpp
if (rc != SQLITE_DONE) {
    resetPreparedStatement(stmt);
    throw std::runtime_error("SQL execution error: " + std::string(sqlite3_errmsg(db_)));
}

resetPreparedStatement(stmt);
return result;
```

- [ ] **Step 4: Re-run the native test target**

Run: `cmake --build cpp/build-native --target flatsql_test -j4 && ./cpp/build-native/flatsql_test`
Expected: PASS, including `testSQLiteParameterizedStatementReuse()`

- [ ] **Step 5: Commit the native cache safety fix**

```bash
git add cpp/src/sqlite_engine.cpp cpp/include/flatsql/sqlite_engine.h cpp/test/test_main.cpp
git commit -m "fix: clear cached sqlite bindings before reuse"
```

### Task 2: Add End-To-End WASM Parameterized Query Support

**Files:**
- Modify: `cpp/src/flatsql_capi.cpp`
- Modify: `cpp/CMakeLists.txt`
- Modify: `wasm/index.js`
- Modify: `wasm/index.d.ts`
- Create: `test/wasm-query-params.test.ts`

- [ ] **Step 1: Write the failing WASM parameterized-query test**

Create `test/wasm-query-params.test.ts` with a focused end-to-end test:

```ts
import initFlatSQL from '../wasm/index.js';

describe('WASM parameterized queries', () => {
  test('binds positional parameters through the WASM query API', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const db = flatsql.createDatabase(`
      table User {
        id: int (id);
        name: string;
        email: string (key);
        age: int;
      }
    `, 'wasm-query-params');

    db.registerFileId('USER', 'User');
    db.enableDemoExtractors();
    db.ingestBuffers([
      flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
      flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
    ]);

    expect(db.query('SELECT email FROM User WHERE id = ?', [2])).toEqual({
      columns: ['email'],
      rows: [['bob@example.com']],
    });

    db.destroy();
  });
});
```

- [ ] **Step 2: Run the targeted WASM test and verify it fails**

Run: `npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts`
Expected: FAIL because `db.query(sql, params)` is not implemented and the raw `?` placeholder is rejected by the current wrapper

- [ ] **Step 3: Add parameter encoding, C exports, and JS wrapper support**

In `cpp/src/flatsql_capi.cpp`, add tagged parameter decoding and a new export:

```cpp
namespace {

enum ParamTag : uint8_t {
    PARAM_NULL = 0,
    PARAM_BOOL = 1,
    PARAM_INT64 = 2,
    PARAM_FLOAT64 = 3,
    PARAM_STRING = 4,
    PARAM_BYTES = 5,
};

std::vector<Value> decodeParams(const uint8_t* data, size_t length, int paramCount) {
    std::vector<Value> params;
    params.reserve(paramCount);
    size_t offset = 0;

    for (int index = 0; index < paramCount; index++) {
        uint8_t tag = data[offset++];
        uint32_t size = flatbuffers::ReadScalar<uint32_t>(data + offset);
        offset += 4;

        switch (tag) {
            case PARAM_NULL:
                params.emplace_back(std::monostate{});
                break;
            case PARAM_BOOL:
                params.emplace_back(data[offset] != 0);
                break;
            case PARAM_INT64:
                params.emplace_back(flatbuffers::ReadScalar<int64_t>(data + offset));
                break;
            case PARAM_FLOAT64:
                params.emplace_back(flatbuffers::ReadScalar<double>(data + offset));
                break;
            case PARAM_STRING:
                params.emplace_back(std::string(reinterpret_cast<const char*>(data + offset), size));
                break;
            case PARAM_BYTES:
                params.emplace_back(std::vector<uint8_t>(data + offset, data + offset + size));
                break;
            default:
                throw std::runtime_error("Unsupported parameter tag");
        }

        offset += size;
    }

    return params;
}

}  // namespace

EMSCRIPTEN_KEEPALIVE
int flatsql_query_params(void* handle, const char* sql, const uint8_t* paramData, size_t paramLength, int paramCount) {
    try {
        g_lastResult = static_cast<FlatSQLDatabase*>(handle)->query(
            sql,
            decodeParams(paramData, paramLength, paramCount)
        );
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}
```

Export the new symbol from both WASM targets in `cpp/CMakeLists.txt`:

```cmake
"_flatsql_query", "_flatsql_query_params", "_flatsql_get_error",
```

In `wasm/index.js`, add parameter encoding and call the new export when `params` is provided:

```js
const PARAM_NULL = 0;
const PARAM_BOOL = 1;
const PARAM_INT64 = 2;
const PARAM_FLOAT64 = 3;
const PARAM_STRING = 4;
const PARAM_BYTES = 5;

function encodeQueryParams(params) {
    const parts = [];
    let total = 0;

    for (const value of params) {
        let tag;
        let payload;

        if (value === null) {
            tag = PARAM_NULL;
            payload = new Uint8Array(0);
        } else if (typeof value === 'boolean') {
            tag = PARAM_BOOL;
            payload = Uint8Array.of(value ? 1 : 0);
        } else if (typeof value === 'number' && Number.isSafeInteger(value)) {
            tag = PARAM_INT64;
            payload = new Uint8Array(8);
            new DataView(payload.buffer).setBigInt64(0, BigInt(value), true);
        } else if (typeof value === 'number') {
            tag = PARAM_FLOAT64;
            payload = new Uint8Array(8);
            new DataView(payload.buffer).setFloat64(0, value, true);
        } else if (typeof value === 'string') {
            tag = PARAM_STRING;
            payload = new TextEncoder().encode(value);
        } else if (value instanceof Uint8Array) {
            tag = PARAM_BYTES;
            payload = value;
        } else {
            throw new TypeError(`Unsupported query parameter type: ${typeof value}`);
        }

        const header = new Uint8Array(5);
        const view = new DataView(header.buffer);
        view.setUint8(0, tag);
        view.setUint32(1, payload.length, true);
        parts.push(header, payload);
        total += header.length + payload.length;
    }

    const encoded = new Uint8Array(total);
    let offset = 0;
    for (const part of parts) {
        encoded.set(part, offset);
        offset += part.length;
    }
    return encoded;
}

function readCellValue(row, col) {
    const type = api.resultCellType(row, col);
    switch (type) {
        case 0:
            return null;
        case 1:
            return api.resultCellNumber(row, col) !== 0;
        case 2:
        case 3:
        case 4:
            return api.resultCellNumber(row, col);
        case 5:
            return api.resultCellString(row, col);
        case 6: {
            const blobPtr = api.resultCellBlob(row, col);
            const blobSize = api.resultCellBlobSize(row, col);
            return blobPtr && blobSize > 0
                ? Array.from(new Uint8Array(Module.HEAPU8.buffer, blobPtr, blobSize))
                : [];
        }
        default:
            return null;
    }
}

function readQueryResult() {
    const colCount = api.resultColumnCount();
    const rowCount = api.resultRowCount();
    const columns = [];
    for (let i = 0; i < colCount; i++) {
        columns.push(api.resultColumnName(i));
    }

    const rows = [];
    for (let r = 0; r < rowCount; r++) {
        const row = [];
        for (let c = 0; c < colCount; c++) {
            row.push(readCellValue(r, c));
        }
        rows.push(row);
    }

    return { columns, rows };
}

query(sql, params = undefined) {
    const encodedParams = params && params.length > 0 ? encodeQueryParams(params) : null;
    const success = encodedParams
        ? withHeapBytes(encodedParams, (ptr) =>
            api.queryParams(this._handle, sql, ptr, encodedParams.length, params.length))
        : api.query(this._handle, sql);
    if (!success) {
        throw new Error(api.getError());
    }
    return readQueryResult();
}
```

Also add the new native binding in the API initialization block:

```js
queryParams: Module.cwrap('flatsql_query_params', 'number', ['number', 'string', 'number', 'number', 'number']),
```

In `wasm/index.d.ts`, add:

```ts
export type QueryParam = null | boolean | number | string | Uint8Array;
```

and change:

```ts
query(sql: string, params?: QueryParam[]): QueryResult;
```

- [ ] **Step 4: Re-run the targeted WASM test**

Run: `npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts`
Expected: PASS for the positional parameter query test

- [ ] **Step 5: Commit the parameterized WASM query API**

```bash
git add cpp/src/flatsql_capi.cpp cpp/CMakeLists.txt wasm/index.js wasm/index.d.ts test/wasm-query-params.test.ts
git commit -m "feat: add wasm parameterized query api"
```

### Task 3: Add Native And WASM Batch Query Support

**Files:**
- Modify: `cpp/src/flatsql_capi.cpp`
- Modify: `cpp/CMakeLists.txt`
- Modify: `wasm/index.js`
- Modify: `wasm/index.d.ts`
- Modify: `test/wasm-query-params.test.ts`

- [ ] **Step 1: Add the failing batch-query test**

Extend `test/wasm-query-params.test.ts` with:

```ts
test('returns batch query results in request order', async () => {
  const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
  const db = flatsql.createDatabase(`
    table User {
      id: int (id);
      name: string;
      email: string (key);
      age: int;
    }
  `, 'wasm-query-many');

  db.registerFileId('USER', 'User');
  db.enableDemoExtractors();
  db.ingestBuffers([
    flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
    flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
  ]);

  expect(db.queryMany([
    { sql: 'SELECT email FROM User WHERE id = ?', params: [1] },
    { sql: 'SELECT email FROM User WHERE id = ?', params: [2] },
  ])).toEqual([
    { columns: ['email'], rows: [['alice@example.com']] },
    { columns: ['email'], rows: [['bob@example.com']] },
  ]);

  db.destroy();
});
```

- [ ] **Step 2: Run the targeted WASM test and verify it fails**

Run: `npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts`
Expected: FAIL because `db.queryMany(...)` and batch result accessors do not exist

- [ ] **Step 3: Implement native batch result storage and `queryMany(...)`**

In `cpp/src/flatsql_capi.cpp`, add a native batch buffer and current-result selector:

```cpp
namespace {

QueryResult g_lastResult;
std::vector<QueryResult> g_batchResults;
int g_selectedBatchResult = -1;

QueryResult& currentResult() {
    if (g_selectedBatchResult >= 0 && g_selectedBatchResult < static_cast<int>(g_batchResults.size())) {
        return g_batchResults[g_selectedBatchResult];
    }
    return g_lastResult;
}

}  // namespace

EMSCRIPTEN_KEEPALIVE
int flatsql_query_many(void* handle, const uint8_t* requestData, size_t requestLength, int requestCount) {
    try {
        g_batchResults.clear();
        auto requests = decodeQueryRequests(requestData, requestLength, requestCount);
        g_batchResults.reserve(requests.size());
        for (const auto& request : requests) {
            g_batchResults.push_back(
                static_cast<FlatSQLDatabase*>(handle)->query(request.sql, request.params)
            );
        }
        g_selectedBatchResult = g_batchResults.empty() ? -1 : 0;
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_batchResults.clear();
        g_selectedBatchResult = -1;
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_batch_result_count() {
    return static_cast<int>(g_batchResults.size());
}

EMSCRIPTEN_KEEPALIVE
int flatsql_select_batch_result(int index) {
    if (index < 0 || index >= static_cast<int>(g_batchResults.size())) {
        return 0;
    }
    g_selectedBatchResult = index;
    return 1;
}
```

Update all result accessors to use `currentResult()` instead of `g_lastResult`.

Export the new symbols in `cpp/CMakeLists.txt`:

```cmake
"_flatsql_query_many", "_flatsql_batch_result_count", "_flatsql_select_batch_result",
```

In `wasm/index.js`, factor result decoding into one helper and add `queryMany`:

```js
queryMany(queries) {
    const payload = encodeQueryRequests(queries);
    const success = withHeapBytes(payload, (ptr) =>
        api.queryMany(this._handle, ptr, payload.length, queries.length));
    if (!success) {
        throw new Error(api.getError());
    }

    const results = [];
    const count = api.batchResultCount();
    for (let index = 0; index < count; index++) {
        if (!api.selectBatchResult(index)) {
            throw new Error(`Failed to select batch result ${index}`);
        }
        results.push(readQueryResult());
    }
    return results;
}
```

Add the new cwrap bindings in the API initialization block:

```js
queryMany: Module.cwrap('flatsql_query_many', 'number', ['number', 'number', 'number', 'number']),
batchResultCount: Module.cwrap('flatsql_batch_result_count', 'number', []),
selectBatchResult: Module.cwrap('flatsql_select_batch_result', 'number', ['number']),
```

In `wasm/index.d.ts`, add:

```ts
queryMany(queries: readonly { sql: string; params?: QueryParam[] }[]): QueryResult[];
```

- [ ] **Step 4: Re-run the targeted WASM batch test**

Run: `npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts`
Expected: PASS for both single-query and batch-query tests

- [ ] **Step 5: Commit the WASM batch query API**

```bash
git add cpp/src/flatsql_capi.cpp cpp/CMakeLists.txt wasm/index.js wasm/index.d.ts test/wasm-query-params.test.ts
git commit -m "feat: add wasm batch query api"
```

### Task 4: Add SQLite-Compatibility And Type Round-Trip Coverage

**Files:**
- Modify: `cpp/src/flatsql_capi.cpp`
- Modify: `wasm/index.js`
- Modify: `test/wasm-query-params.test.ts`

- [ ] **Step 1: Add failing semantic and type tests**

Extend `test/wasm-query-params.test.ts` with:

```ts
test('matches literal SQL results and round-trips null, boolean, and blob params', async () => {
  const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
  const db = flatsql.createDatabase(`
    table User {
      id: int (id);
      name: string;
      email: string (key);
      age: int;
    }
  `, 'wasm-query-types');

  db.registerFileId('USER', 'User');
  db.enableDemoExtractors();
  db.ingestBuffers([
    flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
    flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
  ]);

  expect(
    db.query('SELECT email FROM User WHERE id = ?', [2])
  ).toEqual(
    db.query('SELECT email FROM User WHERE id = 2')
  );

  expect(
    db.query('SELECT ? AS payload, ? AS missing, ? AS flag', [
      new Uint8Array([1, 2, 3]),
      null,
      true,
    ])
  ).toEqual({
    columns: ['payload', 'missing', 'flag'],
    rows: [[[1, 2, 3], null, true]],
  });

  db.destroy();
});
```

- [ ] **Step 2: Run the targeted WASM test and verify it fails**

Run: `npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts`
Expected: FAIL because one or more parameter tags or result conversions are incomplete

- [ ] **Step 3: Complete the full parameter codec and shared result reader**

In `cpp/src/flatsql_capi.cpp`, ensure `decodeParams(...)` supports all v1 tags:

```cpp
switch (tag) {
    case PARAM_NULL:
        params.emplace_back(std::monostate{});
        break;
    case PARAM_BOOL:
        params.emplace_back(data[offset] != 0);
        break;
    case PARAM_INT64:
        params.emplace_back(flatbuffers::ReadScalar<int64_t>(data + offset));
        break;
    case PARAM_FLOAT64:
        params.emplace_back(flatbuffers::ReadScalar<double>(data + offset));
        break;
    case PARAM_STRING:
        params.emplace_back(std::string(reinterpret_cast<const char*>(data + offset), size));
        break;
    case PARAM_BYTES:
        params.emplace_back(std::vector<uint8_t>(data + offset, data + offset + size));
        break;
    default:
        throw std::runtime_error("Unsupported parameter tag");
}
```

In `wasm/index.js`, keep result decoding centralized so `query()` and `queryMany()` return the same value shapes:

```js
// Keep the Task 2 readCellValue/readQueryResult helpers and only widen
// the parameter encoder/decoder here if one of the new semantic tests fails.
```

- [ ] **Step 4: Re-run the semantic coverage test**

Run: `npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts`
Expected: PASS for literal equivalence plus `null`/`boolean`/`blob` round-trip coverage

- [ ] **Step 5: Commit the compatibility and type coverage work**

```bash
git add cpp/src/flatsql_capi.cpp wasm/index.js test/wasm-query-params.test.ts
git commit -m "test: cover wasm query semantics and param types"
```

### Task 5: Rebuild WASM And Run Full Verification

**Files:**
- Modify: `cpp/CMakeLists.txt`
- Modify: `cpp/src/flatsql_capi.cpp`
- Modify: `cpp/src/sqlite_engine.cpp`
- Modify: `cpp/include/flatsql/sqlite_engine.h`
- Modify: `cpp/test/test_main.cpp`
- Modify: `wasm/index.js`
- Modify: `wasm/index.d.ts`
- Test: `test/wasm-query-params.test.ts`
- Test: `test/wasi.test.ts`

- [ ] **Step 1: Rebuild the WASM outputs with the new exports**

Run: `npm run build:wasm`
Expected: PASS and regenerated `wasm/flatsql.js`, `wasm/flatsql.wasm`, and `wasm/flatsql-wasi.wasm`

- [ ] **Step 2: Run the focused WASM query regression suite**

Run: `npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts test/wasi.test.ts`
Expected: PASS

- [ ] **Step 3: Run unaffected existing repository suites**

Run: `npm test -- --runInBand --runTestsByPath test/basic.test.ts test/schema-joins.test.ts test/cluster-mode.test.ts`
Expected: PASS

- [ ] **Step 4: Re-run the native regression target**

Run: `cmake --build cpp/build-native --target flatsql_test -j4 && ./cpp/build-native/flatsql_test`
Expected: PASS

- [ ] **Step 5: Commit the full subproject A implementation**

```bash
git add cpp/CMakeLists.txt cpp/src/flatsql_capi.cpp cpp/src/sqlite_engine.cpp cpp/include/flatsql/sqlite_engine.h cpp/test/test_main.cpp wasm/index.js wasm/index.d.ts wasm/flatsql.js wasm/flatsql.wasm wasm/flatsql-wasi.wasm test/wasm-query-params.test.ts docs/superpowers/specs/2026-04-11-wasm-query-port-design.md docs/superpowers/plans/2026-04-11-wasm-query-port.md
git commit -m "feat: port query batching into wasm runtime"
```
