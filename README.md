# FlatSQL

[![CI](https://github.com/DigitalArsenal/flatsql/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/DigitalArsenal/flatsql/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/flatsql)](https://www.npmjs.com/package/flatsql)

SQL over persistent FlatBuffer records. FlatBuffers in, FlatBuffers out.

FlatSQL 3.12 provides one API and one `wasm32-wasip1-threads` engine for Node,
browsers, and WASI hosts. The optimized store, SQL lanes, and geospatial
functions share that engine. There are no separate PS/P4 entry points.

```sh
npm install flatsql
```

```js
import { FlatSQL } from 'flatsql';

const db = await FlatSQL.open({ root: './customer-data' });
await db.registerSchema({ name: 'GJN', fileIdentifier: '$GJN', bfbs });
await db.ingest('GJN', sizePrefixedRecords);
for await (const chunk of db.query(qrpRequest)) {
  consumeBinaryResults(chunk);
}
await db.close();
```

`bfbs` is a binary reflection schema. Records are concatenated size-prefixed
FlatBuffers. `qrpRequest` is an SDS QRP FlatBuffer containing SQL and typed
parameters. Results are a stream of size-prefixed SDS QRP pages. Request
`{ records: true }` with `SELECT _data` to return the original record frames.
Stream chunks can split frames; consume the four-byte lengths when framing.

Use the canonical [SDS GJN schema](https://github.com/DigitalArsenal/spacedatastandards.org)
for geometry. For example, a QRP request containing
`SELECT _data FROM GJN WHERE geo_contains(_data, 40.75, -73.98)` filters binary
records. Geometry projections such as `geo_envelope(_data)` return GJN bytes
inside QRP values. Scalar distance and radius queries remain available.

Text conversion belongs in a separate program such as
[flatc-wasm](https://digitalarsenal.github.io/flatc-wasm/). The engine has no
JSON, GeoJSON, WKT, or schema-text conversion and no SQLite JSON extension.
It has no runtime npm dependencies. Generated SDS bindings can construct and
read the binary requests/results in your application.

## Binary CLI

```sh
# Feed the binary output of your converter through stdin.
flatsql ingest --root ./store --schema GJN.bfbs --type GJN --id '$GJN' < records.fb > receipts.fb
flatsql query --root ./store --request query.qrp > results.qrp
flatsql query --root ./store --records < records-query.qrp > records.fb
```

CLI ingest batches are bounded to 4 MiB, including each record's size prefix.
The JavaScript request capacity is configurable. Errors go to stderr and produce
a nonzero exit status; stdout contains binary results only.

## Runtime requirements

Node 24 or later uses the bundled generic thread/filesystem adapter. With an
explicit `root`, data survives `close()` and later opens. Without one, Node uses
a temporary store. Browsers use OPFS and require cross-origin isolation
(`Cross-Origin-Opener-Policy: same-origin` and
`Cross-Origin-Embedder-Policy: require-corp` or `credentialless`). Serve the
package's runtime workers and WASM file as static assets. The
[interactive demo](https://digitalarsenal.github.io/flatsql/) shows this setup.

The same artifact imports WASI preview 1, shared memory, `wasi.thread-spawn`, and
seven filesystem operations. Native builds expose the same `flatsql_*` ABI.
See [the complete API and geometry contract](docs/API-3.12.md),
[the native build](cpp/README.md), and [CI evidence](docs/CI.md).

## Development

```sh
npm ci --ignore-scripts
npm run build
npm run test:e2e # set FLATBUFFERS_DIR to the pinned checkout for native checks
npm run test:browser # installed Chromium headless shell required
```

CI runs short end-to-end flows against the packed npm artifact and native
engine. It does not run unit suites. Release CI reproduces the committed WASM
bytes with wasi-sdk 30 before npm trusted publishing.

Licensed under [PolyForm Noncommercial 1.0.0](LICENSE). Commercial licensing:
[tj@edgesource.com](mailto:tj@edgesource.com).
