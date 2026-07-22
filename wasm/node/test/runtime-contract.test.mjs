import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

import { Builder, ByteBuffer } from "flatbuffers";
import { loadFlatSQLStandalone } from "../../standalone.js";

const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const nodeDirectory = path.resolve(testDirectory, "..");
const repositoriesDirectory = path.resolve(testDirectory, "../../../../..");
const sdkDirectory = process.env.SPACE_DATA_MODULE_SDK_ROOT
  ? path.resolve(process.env.SPACE_DATA_MODULE_SDK_ROOT)
  : path.join(repositoriesDirectory, "ancillary-packages/space-data-module-sdk");
const manifest = JSON.parse(
  await readFile(path.join(nodeDirectory, "plugin-manifest.json"), "utf8"),
);
const publisher = JSON.parse(
  await readFile(path.join(nodeDirectory, "publisher.json"), "utf8"),
);
const artifactBytes = new Uint8Array(
  await readFile(path.join(nodeDirectory, "dist/isomorphic/module.wasm")),
);

const FSO_SIZE = 361648;
const FSB_SIZE = 1048744;

const FSO_OPERATION = Object.freeze({
  APPEND_RECORDS: 1,
  QUERY_RECORDS: 2,
  CONFIGURE_INDEX: 3,
  UPSERT_VIEW: 4,
  COMPACT: 5,
  CONFIGURE_RETENTION: 6,
  SNAPSHOT: 7,
  RELOAD: 8,
});
const FSO_STATUS_COMPLETE = 4;
const FSO_STATUS_INVALID_ARGUMENT = 5;
const FSO_STATUS_INTERNAL_ERROR = 10;
const FSB_KIND = Object.freeze({
  RECORD_STREAM: 1,
  QUERY_RESULT: 2,
  SNAPSHOT: 3,
});

const USER_SCHEMA = `
table User {
  id: int (id);
  name: string;
  email: string (key);
  age: int;
}
`;

const ORBIT_SCHEMA = `
table Orbit {
  id: int (id);
  semi_major_axis: double;
}
`;

function createSizePrefixedOrbitRecord(id, semiMajorAxis) {
  const builder = new Builder(128);
  builder.startObject(2);
  builder.addFieldInt32(0, id, 0);
  builder.addFieldFloat64(1, semiMajorAxis, 0);
  const root = builder.endObject();
  // Build the size prefix as part of the FlatBuffer so the builder can retain
  // the table's 8-byte alignment relative to the true start of the record.
  // Prepending four bytes after finishing a non-prefixed buffer is not a valid
  // size-prefixed FlatBuffer when its schema contains doubles or int64s.
  builder.finishSizePrefixed(root, "ORBT");
  return builder.asUint8Array();
}

function sdkUrl(relativePath) {
  return pathToFileURL(path.join(sdkDirectory, relativePath)).href;
}

async function createHarness(options = {}) {
  const wasmEdgeRunner = process.env.FLATSQL_NODE_WASMEDGE_RUNNER;
  if (wasmEdgeRunner) {
    const wasmPath = process.env.FLATSQL_NODE_WASMEDGE_WASM;
    assert.ok(
      wasmPath,
      "FLATSQL_NODE_WASMEDGE_WASM must name the trailer-stripped bytes from the exact signed artifact",
    );
    const { createModuleHarness } = await import(
      sdkUrl("src/testing/moduleHarness.js")
    );
    return createModuleHarness({
      runtime: {
        kind: "wasmedge",
        wasmPath,
        wasmEdgeRunnerBinary: wasmEdgeRunner,
      },
    });
  }
  const { createBrowserModuleHarness } = await import(
    sdkUrl("src/testing/browserModuleHarness.js")
  );
  const hostcallDispatch =
    options.hostcallDispatch ?? createOpaqueStateAdapter().dispatch;
  return createBrowserModuleHarness({
    wasmSource: artifactBytes,
    manifest,
    surface: "direct",
    hostcallDispatch,
    verifySignature: {
      trustedPublicKeys: [publisher.publicKeyHex],
      requireSignature: true,
    },
  });
}

function createOpaqueStateAdapter(options = {}) {
  const values = new Map();
  const calls = [];
  const scheduledFailures = [];
  const storageKey = (params) => `${params.namespace}\0${params.key}`;
  const visibleKeys = () =>
    [...values.keys()].map((key) => key.split("\0").at(-1)).sort();
  return {
    calls,
    failNext(operation, occurrence = 1) {
      assert.ok(Number.isInteger(occurrence) && occurrence > 0);
      scheduledFailures.push({ operation, remaining: occurrence });
    },
    keys() {
      return visibleKeys();
    },
    mutate(key, mutate) {
      const storage = storageKey({ namespace: "primary", key });
      const current = values.get(storage);
      assert.ok(current, `opaque key ${key} exists`);
      const replacement = current.slice();
      mutate(replacement);
      values.set(storage, replacement);
    },
    copy(sourceKey, targetKey) {
      const source = values.get(storageKey({
        namespace: "primary",
        key: sourceKey,
      }));
      assert.ok(source, `opaque key ${sourceKey} exists`);
      values.set(
        storageKey({ namespace: "primary", key: targetKey }),
        source.slice(),
      );
    },
    dispatch(operation, params) {
      calls.push({ operation, params });
      const failure = scheduledFailures.find(
        (candidate) => candidate.operation === operation,
      );
      if (failure) {
        failure.remaining -= 1;
        if (failure.remaining === 0) {
          scheduledFailures.splice(scheduledFailures.indexOf(failure), 1);
          throw new Error(`injected ${operation} failure`);
        }
      }
      if (operation === "storage.adapter.opaque.read") {
        const value = values.get(storageKey(params));
        return {
          found: value !== undefined,
          bytes_b64: options.base64Reads
            ? Buffer.from(value ?? new Uint8Array()).toString("base64")
            : value?.slice() ?? new Uint8Array(),
        };
      }
      if (operation === "storage.adapter.opaque.replace") {
        assert.ok(params.data instanceof Uint8Array);
        values.set(storageKey(params), params.data.slice());
        return { stored_bytes: params.data.byteLength };
      }
      if (operation === "storage.adapter.opaque.sync") {
        return { synced: true };
      }
      if (operation === "storage.adapter.opaque.list") {
        return { keys: visibleKeys() };
      }
      if (operation === "storage.adapter.opaque.delete") {
        values.delete(storageKey(params));
        return { deleted: true };
      }
      throw new Error(`unexpected opaque-state operation: ${operation}`);
    },
  };
}

function methodType(methodId, direction, portId, wireFormat) {
  const method = manifest.methods.find((candidate) => candidate.methodId === methodId);
  assert.ok(method, `manifest method ${methodId} exists`);
  const ports = direction === "input" ? method.inputPorts : method.outputPorts;
  const port = ports.find((candidate) => candidate.portId === portId);
  assert.ok(port, `${methodId} ${direction} port ${portId} exists`);
  const type = port.acceptedTypeSets
    .flatMap((set) => set.allowedTypes)
    .find((candidate) => candidate.wireFormat === wireFormat);
  assert.ok(type, `${methodId} ${portId} supports ${wireFormat}`);
  return type;
}

function createString(builder, value) {
  return value ? builder.createString(value) : 0;
}

function createBytes(builder, value) {
  const bytes = value instanceof Uint8Array ? value : new TextEncoder().encode(value ?? "");
  return bytes.byteLength > 0 ? builder.createByteVector(bytes) : 0;
}

function createFtb(builder, binding) {
  const fileIdentifier = createString(builder, binding.fileIdentifier);
  const tableName = createString(builder, binding.tableName);
  builder.startObject(2);
  builder.addFieldOffset(0, fileIdentifier, 0);
  builder.addFieldOffset(1, tableName, 0);
  return builder.endObject();
}

function createOffsetVector(builder, offsets) {
  if (offsets.length === 0) return 0;
  builder.startVector(4, offsets.length, 4);
  for (let index = offsets.length - 1; index >= 0; index -= 1) {
    builder.addOffset(offsets[index]);
  }
  return builder.endVector();
}

function buildCanonicalFso(options = {}) {
  const builder = new Builder(1024);
  const databaseName = createString(builder, options.databaseName);
  const schemaIdl = createBytes(builder, options.schemaIdl);
  const tableBindings = createOffsetVector(
    builder,
    (options.tableBindings ?? []).map((binding) => createFtb(builder, binding)),
  );
  const tableName = createString(builder, options.tableName);
  const indexName = createString(builder, options.indexName);
  const indexExpression = createBytes(builder, options.indexExpression);
  const viewName = createString(builder, options.viewName);
  const query = createBytes(builder, options.query);
  const parameters = createBytes(builder, options.parameters);
  const upsertKeyExpression = createBytes(builder, options.upsertKeyExpression);

  builder.startObject(21);
  builder.addFieldInt8(0, options.operation ?? 0, 0);
  builder.addFieldInt64(1, BigInt(options.requestId ?? 0), 0n);
  builder.addFieldOffset(2, databaseName, 0);
  builder.addFieldOffset(3, schemaIdl, 0);
  builder.addFieldOffset(4, tableBindings, 0);
  builder.addFieldOffset(5, tableName, 0);
  builder.addFieldOffset(6, indexName, 0);
  builder.addFieldOffset(7, indexExpression, 0);
  builder.addFieldOffset(8, viewName, 0);
  builder.addFieldOffset(9, query, 0);
  builder.addFieldOffset(10, parameters, 0);
  builder.addFieldInt32(11, options.parameterCount ?? 0, 0);
  builder.addFieldOffset(12, upsertKeyExpression, 0);
  builder.addFieldInt64(13, BigInt(options.retentionMaxRecords ?? 0), 0n);
  builder.addFieldInt64(14, BigInt(options.retentionMaxAgeMillis ?? 0), 0n);
  builder.addFieldInt64(15, BigInt(options.compactionTargetBytes ?? 0), 0n);
  const root = builder.endObject();
  builder.finish(root, "$FSO");
  return builder.asUint8Array();
}

function buildCanonicalFsb(options = {}) {
  const builder = new Builder(1024);
  const schemaName = createString(builder, options.schemaName);
  const fileIdentifier = createString(builder, options.fileIdentifier);
  const data = createBytes(builder, options.data);
  const sha256 = createBytes(builder, options.sha256);
  builder.startObject(11);
  builder.addFieldInt64(0, BigInt(options.requestId ?? 0), 0n);
  builder.addFieldInt8(1, options.kind ?? 0, 0);
  builder.addFieldInt32(2, options.chunkSequence ?? 0, 0);
  builder.addFieldInt8(3, options.final === true ? 1 : 0, 0);
  builder.addFieldInt64(4, BigInt(options.totalBytes ?? options.data?.byteLength ?? 0), 0n);
  builder.addFieldInt64(5, BigInt(options.recordCount ?? 0), 0n);
  builder.addFieldInt32(6, options.columnCount ?? 0, 0);
  builder.addFieldOffset(7, schemaName, 0);
  builder.addFieldOffset(8, fileIdentifier, 0);
  builder.addFieldOffset(9, data, 0);
  builder.addFieldOffset(10, sha256, 0);
  const root = builder.endObject();
  builder.finish(root, "$FSB");
  return builder.asUint8Array();
}

function writeAlignedString(bytes, offset, value) {
  const encoded = new TextEncoder().encode(value ?? "");
  assert.ok(encoded.byteLength <= 255);
  bytes[offset] = encoded.byteLength;
  bytes.set(encoded, offset + 1);
}

function writeAlignedVector(bytes, offset, value) {
  const encoded = value instanceof Uint8Array ? value : new TextEncoder().encode(value ?? "");
  new DataView(bytes.buffer).setUint32(offset, encoded.byteLength, true);
  bytes.set(encoded, offset + 4);
}

function buildAlignedFso(options = {}) {
  const bytes = new Uint8Array(FSO_SIZE);
  const view = new DataView(bytes.buffer);
  bytes[2] = options.operation ?? 0;
  view.setBigUint64(8, BigInt(options.requestId ?? 0), true);
  if (options.databaseName) {
    bytes[0] |= 1;
    writeAlignedString(bytes, 16, options.databaseName);
  }
  if (options.schemaIdl) {
    bytes[0] |= 2;
    writeAlignedVector(bytes, 148, options.schemaIdl);
  }
  if ((options.tableBindings ?? []).length > 0) {
    bytes[0] |= 4;
    view.setUint32(262296, options.tableBindings.length, true);
    for (const [index, binding] of options.tableBindings.entries()) {
      const base = 262300 + index * 135;
      bytes[base] = 3;
      writeAlignedString(bytes, base + 1, binding.fileIdentifier);
      writeAlignedString(bytes, base + 6, binding.tableName);
    }
  }
  if (options.tableName) {
    bytes[0] |= 8;
    writeAlignedString(bytes, 270940, options.tableName);
  }
  if (options.indexName) {
    bytes[0] |= 16;
    writeAlignedString(bytes, 271069, options.indexName);
  }
  if (options.indexExpression) {
    bytes[0] |= 32;
    writeAlignedVector(bytes, 271200, options.indexExpression);
  }
  if (options.viewName) {
    bytes[0] |= 64;
    writeAlignedString(bytes, 273252, options.viewName);
  }
  if (options.query) {
    bytes[0] |= 128;
    writeAlignedVector(bytes, 273384, options.query);
  }
  if (options.parameters) {
    bytes[1] |= 1;
    writeAlignedVector(bytes, 289772, options.parameters);
  }
  view.setUint32(355312, options.parameterCount ?? 0, true);
  if (options.upsertKeyExpression) {
    bytes[1] |= 2;
    writeAlignedVector(bytes, 355316, options.upsertKeyExpression);
  }
  view.setBigUint64(357368, BigInt(options.retentionMaxRecords ?? 0), true);
  view.setBigUint64(357376, BigInt(options.retentionMaxAgeMillis ?? 0), true);
  view.setBigUint64(357384, BigInt(options.compactionTargetBytes ?? 0), true);
  return bytes;
}

function buildAlignedFsb(options = {}) {
  const bytes = new Uint8Array(FSB_SIZE);
  const view = new DataView(bytes.buffer);
  view.setBigUint64(8, BigInt(options.requestId ?? 0), true);
  bytes[16] = options.kind ?? 0;
  view.setUint32(20, options.chunkSequence ?? 0, true);
  bytes[24] = options.final === true ? 1 : 0;
  view.setBigUint64(32, BigInt(options.totalBytes ?? options.data?.byteLength ?? 0), true);
  view.setBigUint64(40, BigInt(options.recordCount ?? 0), true);
  view.setUint32(48, options.columnCount ?? 0, true);
  if (options.schemaName) {
    bytes[0] |= 1;
    writeAlignedString(bytes, 52, options.schemaName);
  }
  if (options.fileIdentifier) {
    bytes[0] |= 2;
    writeAlignedString(bytes, 117, options.fileIdentifier);
  }
  if (options.data) {
    bytes[0] |= 4;
    writeAlignedVector(bytes, 124, options.data);
  }
  if (options.sha256) {
    bytes[0] |= 8;
    writeAlignedVector(bytes, 1048704, options.sha256);
  }
  return bytes;
}

function flatbufferRoot(bytes, identifier) {
  const buffer = new ByteBuffer(bytes);
  assert.equal(buffer.__has_identifier(identifier), true, `expected ${identifier}`);
  const position = buffer.position();
  return { buffer, table: position + buffer.readInt32(position) };
}

function fieldOffset(buffer, table, slot) {
  return buffer.__offset(table, 4 + slot * 2);
}

function readVector(buffer, table, slot) {
  const offset = fieldOffset(buffer, table, slot);
  if (!offset) return new Uint8Array();
  const start = buffer.__vector(table + offset);
  const length = buffer.__vector_len(table + offset);
  return buffer.bytes().subarray(start, start + length).slice();
}

function readString(buffer, table, slot) {
  const offset = fieldOffset(buffer, table, slot);
  return offset ? buffer.__string(table + offset) : null;
}

function decodeCanonicalFso(bytes) {
  const { buffer, table } = flatbufferRoot(bytes, "$FSO");
  const operationOffset = fieldOffset(buffer, table, 0);
  const requestOffset = fieldOffset(buffer, table, 1);
  const statusOffset = fieldOffset(buffer, table, 16);
  const affectedOffset = fieldOffset(buffer, table, 17);
  const resultOffset = fieldOffset(buffer, table, 18);
  const errorOffset = fieldOffset(buffer, table, 19);
  return {
    operation: operationOffset ? buffer.readUint8(table + operationOffset) : 0,
    requestId: requestOffset ? buffer.readUint64(table + requestOffset) : 0n,
    status: statusOffset ? buffer.readUint8(table + statusOffset) : 0,
    affectedRecords: affectedOffset ? buffer.readUint64(table + affectedOffset) : 0n,
    resultBytes: resultOffset ? buffer.readUint64(table + resultOffset) : 0n,
    errorCode: errorOffset ? buffer.__string(table + errorOffset) : null,
    message: new TextDecoder().decode(readVector(buffer, table, 20)),
  };
}

function decodeCanonicalFsb(bytes) {
  const { buffer, table } = flatbufferRoot(bytes, "$FSB");
  const number = (slot, reader, fallback = 0) => {
    const offset = fieldOffset(buffer, table, slot);
    return offset ? buffer[reader](table + offset) : fallback;
  };
  return {
    requestId: number(0, "readUint64", 0n),
    kind: number(1, "readUint8"),
    chunkSequence: number(2, "readUint32"),
    final: number(3, "readUint8") !== 0,
    totalBytes: number(4, "readUint64", 0n),
    recordCount: number(5, "readUint64", 0n),
    columnCount: number(6, "readUint32"),
    schemaName: readString(buffer, table, 7),
    fileIdentifier: readString(buffer, table, 8),
    data: readVector(buffer, table, 9),
    sha256: readVector(buffer, table, 10),
  };
}

function readAlignedVector(bytes, offset) {
  const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    .getUint32(offset, true);
  return bytes.subarray(offset + 4, offset + 4 + length).slice();
}

function readAlignedString(bytes, offset) {
  return new TextDecoder().decode(bytes.subarray(offset + 1, offset + 1 + bytes[offset]));
}

function decodeAlignedFso(bytes) {
  assert.equal(bytes.byteLength, FSO_SIZE);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return {
    operation: bytes[2],
    requestId: view.getBigUint64(8, true),
    status: bytes[357392],
    affectedRecords: view.getBigUint64(357400, true),
    resultBytes: view.getBigUint64(357408, true),
    errorCode: (bytes[1] & 4) !== 0 ? readAlignedString(bytes, 357416) : null,
    message: (bytes[1] & 8) !== 0
      ? new TextDecoder().decode(readAlignedVector(bytes, 357548))
      : "",
  };
}

function decodeAlignedFsb(bytes) {
  assert.equal(bytes.byteLength, FSB_SIZE);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return {
    requestId: view.getBigUint64(8, true),
    kind: bytes[16],
    chunkSequence: view.getUint32(20, true),
    final: bytes[24] !== 0,
    totalBytes: view.getBigUint64(32, true),
    recordCount: view.getBigUint64(40, true),
    columnCount: view.getUint32(48, true),
    schemaName: (bytes[0] & 1) !== 0 ? readAlignedString(bytes, 52) : null,
    fileIdentifier: (bytes[0] & 2) !== 0 ? readAlignedString(bytes, 117) : null,
    data: (bytes[0] & 4) !== 0 ? readAlignedVector(bytes, 124) : new Uint8Array(),
    sha256: (bytes[0] & 8) !== 0
      ? readAlignedVector(bytes, 1048704)
      : new Uint8Array(),
  };
}

function decodeOutputFrame(frame) {
  if (frame.typeRef.wireFormat === "aligned-binary") {
    return frame.portId === "status"
      ? decodeAlignedFso(frame.payload)
      : decodeAlignedFsb(frame.payload);
  }
  return frame.portId === "status"
    ? decodeCanonicalFso(frame.payload)
    : decodeCanonicalFsb(frame.payload);
}

function assertComplete(response, expectedOperation, requestId) {
  assert.equal(response.statusCode, 0, response.errorMessage ?? response.errorCode);
  const statusFrame = response.outputs.find((frame) => frame.portId === "status");
  assert.ok(statusFrame, "operation emits status");
  const status = decodeOutputFrame(statusFrame);
  assert.equal(status.operation, expectedOperation);
  assert.equal(status.requestId, BigInt(requestId));
  assert.equal(status.status, FSO_STATUS_COMPLETE, status.message || status.errorCode);
  return status;
}

function assertRejected(response, expectedOperation, requestId) {
  assert.equal(response.statusCode, 0, response.errorMessage ?? response.errorCode);
  const statusFrame = response.outputs.find((frame) => frame.portId === "status");
  assert.ok(statusFrame, "rejected operation emits fail-closed status");
  const status = decodeOutputFrame(statusFrame);
  assert.equal(status.operation, expectedOperation);
  assert.equal(status.requestId, BigInt(requestId));
  assert.equal(
    status.status,
    FSO_STATUS_INVALID_ARGUMENT,
    `expected INVALID_ARGUMENT, received ${status.message || status.errorCode}`,
  );
  assert.ok(status.errorCode, "rejection includes a stable error code");
  return status;
}

function assertInternalError(response, expectedOperation, requestId, errorCode) {
  assert.equal(response.statusCode, 0, response.errorMessage ?? response.errorCode);
  const statusFrame = response.outputs.find((frame) => frame.portId === "status");
  assert.ok(statusFrame, "failed operation emits status");
  const status = decodeOutputFrame(statusFrame);
  assert.equal(status.operation, expectedOperation);
  assert.equal(status.requestId, BigInt(requestId));
  assert.equal(status.status, FSO_STATUS_INTERNAL_ERROR);
  assert.equal(status.errorCode, errorCode);
  return status;
}

function wrapSizePrefixed(records) {
  const total = records.reduce((sum, record) => sum + 4 + record.byteLength, 0);
  const stream = new Uint8Array(total);
  const view = new DataView(stream.buffer);
  let offset = 0;
  for (const record of records) {
    view.setUint32(offset, record.byteLength, true);
    stream.set(record, offset + 4);
    offset += 4 + record.byteLength;
  }
  return stream;
}

function countSizePrefixedRecords(stream) {
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  let count = 0;
  let offset = 0;
  while (offset < stream.byteLength) {
    assert.ok(stream.byteLength - offset >= 4, "fixture stream has a size prefix");
    const size = view.getUint32(offset, true);
    assert.ok(size <= stream.byteLength - offset - 4, "fixture record is complete");
    offset += 4 + size;
    count += 1;
  }
  return count;
}

function outputStream(response, portId) {
  const chunks = response.outputs
    .filter((frame) => frame.portId === portId)
    .map(decodeOutputFrame)
    .sort((left, right) => left.chunkSequence - right.chunkSequence);
  assert.ok(chunks.length > 0, `${portId} emitted at least one chunk`);
  assert.equal(chunks.at(-1).final, true);
  const length = chunks.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const data = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    data.set(chunk.data, offset);
    offset += chunk.data.byteLength;
  }
  return { chunks, data };
}

async function configure(harness, wireFormat, requestId = 1) {
  const typeRef = methodType("configure_index", "input", "control", wireFormat);
  const payloadOptions = {
    operation: FSO_OPERATION.CONFIGURE_INDEX,
    requestId,
    databaseName: "node-test",
    schemaIdl: USER_SCHEMA,
    tableBindings: [{ fileIdentifier: "USER", tableName: "User" }],
  };
  const payload = wireFormat === "aligned-binary"
    ? buildAlignedFso(payloadOptions)
    : buildCanonicalFso(payloadOptions);
  const response = await harness.invoke({
    methodId: "configure_index",
    inputs: [{ portId: "control", typeRef, payload }],
  });
  assertComplete(response, FSO_OPERATION.CONFIGURE_INDEX, requestId);
}

async function invokeAppend(harness, wireFormat, data, requestId = 2) {
  const typeRef = methodType("append_records", "input", "records", wireFormat);
  const payloadOptions = {
    requestId,
    kind: FSB_KIND.RECORD_STREAM,
    chunkSequence: 0,
    final: true,
    totalBytes: data.byteLength,
    recordCount: countSizePrefixedRecords(data),
    schemaName: "User",
    fileIdentifier: "USER",
    data,
  };
  const payload = wireFormat === "aligned-binary"
    ? buildAlignedFsb(payloadOptions)
    : buildCanonicalFsb(payloadOptions);
  return harness.invoke({
    methodId: "append_records",
    inputs: [{ portId: "records", typeRef, payload }],
  });
}

async function append(harness, wireFormat, data, requestId = 2) {
  const response = await invokeAppend(harness, wireFormat, data, requestId);
  return assertComplete(response, FSO_OPERATION.APPEND_RECORDS, requestId);
}

async function invokeQuery(
  harness,
  wireFormat,
  queryText,
  requestId = 3,
  viewName = "",
) {
  const typeRef = methodType("query_records", "input", "query", wireFormat);
  const payloadOptions = {
    operation: FSO_OPERATION.QUERY_RECORDS,
    requestId,
    query: queryText,
    viewName,
  };
  const payload = wireFormat === "aligned-binary"
    ? buildAlignedFso(payloadOptions)
    : buildCanonicalFso(payloadOptions);
  return harness.invoke({
    methodId: "query_records",
    inputs: [{ portId: "query", typeRef, payload }],
  });
}

async function query(harness, wireFormat, queryText, requestId = 3, viewName = "") {
  const response = await invokeQuery(
    harness,
    wireFormat,
    queryText,
    requestId,
    viewName,
  );
  assertComplete(response, FSO_OPERATION.QUERY_RECORDS, requestId);
  return { response, ...outputStream(response, "records") };
}

test("signed FlatSQL outputs carry their exact declared SDS identity", async () => {
  const harness = await createHarness();
  try {
    const typeRef = methodType("append_records", "input", "records", "flatbuffer");
    const response = await harness.invoke({
      methodId: "append_records",
      inputs: [{
        portId: "records",
        typeRef,
        payload: buildCanonicalFsb({
          requestId: 41,
          kind: FSB_KIND.RECORD_STREAM,
          final: true,
          data: new Uint8Array(),
        }),
      }],
    });
    const output = response.outputs.find((frame) => frame.portId === "status");
    const declared = methodType("append_records", "output", "status", "flatbuffer");
    assert.equal(output.typeRef.schemaVersion, declared.schemaVersion);
    assert.equal(
      Buffer.from(output.typeRef.schemaHash).toString("hex"),
      declared.schemaHash,
    );
  } finally {
    harness.destroy();
  }
});

test("canonical configure, append, and query execute inside the signed node", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const expected = wrapSizePrefixed([
    fixtureRuntime.createTestUser(1, "Alice", "alice@example.com", 30),
  ]);
  const harness = await createHarness();
  try {
    await configure(harness, "flatbuffer");
    const appended = await append(harness, "flatbuffer", expected);
    assert.equal(appended.affectedRecords, 1n);
    const result = await query(
      harness,
      "flatbuffer",
      "SELECT _data FROM User ORDER BY id",
    );
    assert.deepEqual(result.data, expected);
  } finally {
    harness.destroy();
  }
});

test("size-prefixed records with 8-byte fields retain their verifier alignment context", async () => {
  const expected = createSizePrefixedOrbitRecord(1, 6_878_137.25);
  const harness = await createHarness();
  try {
    const configureResponse = await harness.invoke({
      methodId: "configure_index",
      inputs: [{
        portId: "control",
        typeRef: methodType(
          "configure_index",
          "input",
          "control",
          "flatbuffer",
        ),
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.CONFIGURE_INDEX,
          requestId: 118,
          databaseName: "double-alignment-test",
          schemaIdl: ORBIT_SCHEMA,
          tableBindings: [{ fileIdentifier: "ORBT", tableName: "Orbit" }],
        }),
      }],
    });
    assertComplete(configureResponse, FSO_OPERATION.CONFIGURE_INDEX, 118);
    const appendResponse = await harness.invoke({
      methodId: "append_records",
      inputs: [{
        portId: "records",
        typeRef: methodType(
          "append_records",
          "input",
          "records",
          "flatbuffer",
        ),
        payload: buildCanonicalFsb({
          requestId: 119,
          kind: FSB_KIND.RECORD_STREAM,
          chunkSequence: 0,
          final: true,
          totalBytes: expected.byteLength,
          recordCount: 1,
          schemaName: "Orbit",
          fileIdentifier: "ORBT",
          data: expected,
        }),
      }],
    });
    const status = assertComplete(
      appendResponse,
      FSO_OPERATION.APPEND_RECORDS,
      119,
    );
    assert.equal(status.affectedRecords, 1n);
    assert.deepEqual(
      (await query(
        harness,
        "flatbuffer",
        "SELECT _data FROM Orbit ORDER BY id",
        120,
      )).data,
      expected,
    );
  } finally {
    harness.destroy();
  }
});

test("append_records configures a fresh instance and ingests independent streams in one invocation", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const first = fixtureRuntime.createTestUser(
    31,
    "Thirty One",
    "thirty-one@example.com",
    31,
  );
  const second = fixtureRuntime.createTestUser(
    32,
    "Thirty Two",
    "thirty-two@example.com",
    32,
  );
  const harness = await createHarness();
  try {
    const controlType = methodType(
      "append_records",
      "input",
      "control",
      "flatbuffer",
    );
    const recordsType = methodType(
      "append_records",
      "input",
      "records",
      "flatbuffer",
    );
    const response = await harness.invoke({
      methodId: "append_records",
      inputs: [
        {
          portId: "control",
          typeRef: controlType,
          payload: buildCanonicalFso({
            operation: FSO_OPERATION.CONFIGURE_INDEX,
            requestId: 120,
            databaseName: "composite-node-test",
            schemaIdl: USER_SCHEMA,
            tableBindings: [{ fileIdentifier: "USER", tableName: "User" }],
          }),
        },
        ...[
          [120, first],
          [121, second],
        ].map(([requestId, record]) => {
          const data = wrapSizePrefixed([record]);
          return {
            portId: "records",
            typeRef: recordsType,
            payload: buildCanonicalFsb({
              requestId,
              kind: FSB_KIND.RECORD_STREAM,
              chunkSequence: 0,
              final: true,
              totalBytes: data.byteLength,
              recordCount: 1,
              schemaName: "User",
              fileIdentifier: "USER",
              data,
            }),
          };
        }),
      ],
    });
    const status = assertComplete(
      response,
      FSO_OPERATION.APPEND_RECORDS,
      120,
    );
    assert.equal(status.affectedRecords, 2n);
    const expected = wrapSizePrefixed([first, second]);
    assert.deepEqual(
      (await query(
        harness,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        122,
      )).data,
      expected,
    );
  } finally {
    harness.destroy();
  }
});

test("a rejected composite append leaves a fresh instance unconfigured", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const validRecord = fixtureRuntime.createTestUser(
    33,
    "Thirty Three",
    "thirty-three@example.com",
    33,
  );
  const malformedRecord = new Uint8Array(16);
  new DataView(malformedRecord.buffer).setUint32(0, 0xfffffff0, true);
  malformedRecord.set(new TextEncoder().encode("USER"), 4);
  const harness = await createHarness();
  try {
    const controlType = methodType(
      "append_records",
      "input",
      "control",
      "flatbuffer",
    );
    const recordsType = methodType(
      "append_records",
      "input",
      "records",
      "flatbuffer",
    );
    const invokeComposite = async ({ databaseName, requestId, record }) => {
      const data = wrapSizePrefixed([record]);
      return harness.invoke({
        methodId: "append_records",
        inputs: [
          {
            portId: "control",
            typeRef: controlType,
            payload: buildCanonicalFso({
              operation: FSO_OPERATION.CONFIGURE_INDEX,
              requestId,
              databaseName,
              schemaIdl: USER_SCHEMA,
              tableBindings: [{ fileIdentifier: "USER", tableName: "User" }],
            }),
          },
          {
            portId: "records",
            typeRef: recordsType,
            payload: buildCanonicalFsb({
              requestId,
              kind: FSB_KIND.RECORD_STREAM,
              chunkSequence: 0,
              final: true,
              totalBytes: data.byteLength,
              recordCount: 1,
              schemaName: "User",
              fileIdentifier: "USER",
              data,
            }),
          },
        ],
      });
    };

    assertRejected(
      await invokeComposite({
        databaseName: "must-rollback",
        requestId: 123,
        record: malformedRecord,
      }),
      FSO_OPERATION.APPEND_RECORDS,
      123,
    );
    assertComplete(
      await invokeComposite({
        databaseName: "accepted-after-rollback",
        requestId: 124,
        record: validRecord,
      }),
      FSO_OPERATION.APPEND_RECORDS,
      124,
    );
    assert.deepEqual(
      (await query(
        harness,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        125,
      )).data,
      wrapSizePrefixed([validRecord]),
    );
  } finally {
    harness.destroy();
  }
});

test("append_records treats FSB RECORD_COUNT as a stream total across chunks", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const first = wrapSizePrefixed([
    fixtureRuntime.createTestUser(41, "Forty One", "forty-one@example.com", 41),
  ]);
  const second = wrapSizePrefixed([
    fixtureRuntime.createTestUser(42, "Forty Two", "forty-two@example.com", 42),
  ]);
  const expected = new Uint8Array(first.byteLength + second.byteLength);
  expected.set(first, 0);
  expected.set(second, first.byteLength);
  const harness = await createHarness();
  try {
    await configure(harness, "flatbuffer", 130);
    const typeRef = methodType(
      "append_records",
      "input",
      "records",
      "flatbuffer",
    );
    const response = await harness.invoke({
      methodId: "append_records",
      inputs: [first, second].map((data, chunkSequence) => ({
        portId: "records",
        typeRef,
        payload: buildCanonicalFsb({
          requestId: 131,
          kind: FSB_KIND.RECORD_STREAM,
          chunkSequence,
          final: chunkSequence === 1,
          totalBytes: expected.byteLength,
          recordCount: 2,
          schemaName: "User",
          fileIdentifier: "USER",
          data,
        }),
      })),
    });
    const status = assertComplete(
      response,
      FSO_OPERATION.APPEND_RECORDS,
      131,
    );
    assert.equal(status.affectedRecords, 2n);
    assert.deepEqual(
      (await query(
        harness,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        132,
      )).data,
      expected,
    );
  } finally {
    harness.destroy();
  }
});

test("invalid table bindings fail closed instead of trapping the signed node", async () => {
  const harness = await createHarness();
  try {
    const response = await harness.invoke({
      methodId: "configure_index",
      inputs: [{
        portId: "control",
        typeRef: methodType("configure_index", "input", "control", "flatbuffer"),
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.CONFIGURE_INDEX,
          requestId: 101,
          databaseName: "invalid-binding",
          schemaIdl: USER_SCHEMA,
          tableBindings: [{ fileIdentifier: "USER", tableName: "Missing" }],
        }),
      }],
    });
    assertRejected(response, FSO_OPERATION.CONFIGURE_INDEX, 101);
  } finally {
    harness.destroy();
  }
});

test("append rejects wrong FSB identity and malformed enclosed FlatBuffers", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const validRecord = fixtureRuntime.createTestUser(
    10,
    "Ten",
    "ten@example.com",
    50,
  );
  const harness = await createHarness();
  try {
    await configure(harness, "flatbuffer", 102);
    const typeRef = methodType("append_records", "input", "records", "flatbuffer");
    const wrongIdentity = await harness.invoke({
      methodId: "append_records",
      inputs: [{
        portId: "records",
        typeRef,
        payload: buildCanonicalFsb({
          requestId: 103,
          kind: FSB_KIND.RECORD_STREAM,
          final: true,
          schemaName: "WrongTable",
          fileIdentifier: "NOPE",
          recordCount: 1,
          data: wrapSizePrefixed([validRecord]),
        }),
      }],
    });
    assertRejected(wrongIdentity, FSO_OPERATION.APPEND_RECORDS, 103);

    const malformedRecord = new Uint8Array(16);
    new DataView(malformedRecord.buffer).setUint32(0, 0xfffffff0, true);
    malformedRecord.set(new TextEncoder().encode("USER"), 4);
    const malformed = await harness.invoke({
      methodId: "append_records",
      inputs: [{
        portId: "records",
        typeRef,
        payload: buildCanonicalFsb({
          requestId: 104,
          kind: FSB_KIND.RECORD_STREAM,
          final: true,
          schemaName: "User",
          fileIdentifier: "USER",
          recordCount: 1,
          data: wrapSizePrefixed([malformedRecord]),
        }),
      }],
    });
    assertRejected(malformed, FSO_OPERATION.APPEND_RECORDS, 104);
  } finally {
    harness.destroy();
  }
});

test("aligned configure, append, and query are semantically equivalent", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const expected = wrapSizePrefixed([
    fixtureRuntime.createTestUser(2, "Bob", "bob@example.com", 25),
  ]);
  const harness = await createHarness();
  try {
    await configure(harness, "aligned-binary", 11);
    await append(harness, "aligned-binary", expected, 12);
    const result = await query(
      harness,
      "aligned-binary",
      "SELECT _data FROM User ORDER BY id",
      13,
    );
    assert.deepEqual(result.data, expected);
    assert.equal(
      result.response.outputs.find((frame) => frame.portId === "records").typeRef.wireFormat,
      "aligned-binary",
    );
  } finally {
    harness.destroy();
  }
});

test("upserted view and record retention survive compaction", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const first = fixtureRuntime.createTestUser(1, "Alice", "alice@example.com", 30);
  const second = fixtureRuntime.createTestUser(2, "Bob", "bob@example.com", 25);
  const both = wrapSizePrefixed([first, second]);
  const expected = wrapSizePrefixed([second]);
  const harness = await createHarness();
  try {
    await configure(harness, "flatbuffer", 21);
    await append(harness, "flatbuffer", both, 22);

    const upsertType = methodType("upsert_view", "input", "control", "flatbuffer");
    const upsert = await harness.invoke({
      methodId: "upsert_view",
      inputs: [{
        portId: "control",
        typeRef: upsertType,
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.UPSERT_VIEW,
          requestId: 23,
          viewName: "latest-user",
          query: "SELECT _data FROM User ORDER BY id DESC LIMIT 1",
        }),
      }],
    });
    assertComplete(upsert, FSO_OPERATION.UPSERT_VIEW, 23);
    assert.deepEqual((await query(harness, "flatbuffer", "", 24, "latest-user")).data, expected);

    const retentionType = methodType(
      "configure_retention",
      "input",
      "control",
      "flatbuffer",
    );
    const retention = await harness.invoke({
      methodId: "configure_retention",
      inputs: [{
        portId: "control",
        typeRef: retentionType,
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.CONFIGURE_RETENTION,
          requestId: 25,
          retentionMaxRecords: 1,
        }),
      }],
    });
    assertComplete(retention, FSO_OPERATION.CONFIGURE_RETENTION, 25);

    const compactType = methodType("compact", "input", "control", "flatbuffer");
    const compact = await harness.invoke({
      methodId: "compact",
      inputs: [{
        portId: "control",
        typeRef: compactType,
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.COMPACT,
          requestId: 26,
        }),
      }],
    });
    assertComplete(compact, FSO_OPERATION.COMPACT, 26);
    assert.deepEqual(
      (await query(harness, "flatbuffer", "SELECT _data FROM User ORDER BY id", 27)).data,
      expected,
    );
  } finally {
    harness.destroy();
  }
});

test("snapshot reloads into a fresh signed node with byte-identical query output", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const expected = wrapSizePrefixed([
    fixtureRuntime.createTestUser(7, "Seven", "seven@example.com", 47),
  ]);
  const source = await createHarness();
  let snapshotFrames;
  try {
    await configure(source, "flatbuffer", 31);
    await append(source, "flatbuffer", expected, 32);
    const snapshotType = methodType("snapshot", "input", "control", "flatbuffer");
    const snapshotResponse = await source.invoke({
      methodId: "snapshot",
      inputs: [{
        portId: "control",
        typeRef: snapshotType,
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.SNAPSHOT,
          requestId: 33,
        }),
      }],
    });
    assertComplete(snapshotResponse, FSO_OPERATION.SNAPSHOT, 33);
    snapshotFrames = snapshotResponse.outputs.filter((frame) => frame.portId === "snapshot");
    assert.ok(snapshotFrames.length > 0);
    const snapshot = outputStream(snapshotResponse, "snapshot");
    const expectedHash = createHash("sha256").update(snapshot.data).digest("hex");
    for (const chunk of snapshot.chunks) {
      assert.equal(Buffer.from(chunk.sha256).toString("hex"), expectedHash);
    }
  } finally {
    source.destroy();
  }

  const target = await createHarness();
  try {
    const reloadType = methodType("reload", "input", "snapshot", "flatbuffer");
    const reloadResponse = await target.invoke({
      methodId: "reload",
      inputs: snapshotFrames.map((frame) => ({
        portId: "snapshot",
        typeRef: reloadType,
        payload: frame.payload,
      })),
    });
    assertComplete(reloadResponse, FSO_OPERATION.RELOAD, 33);
    assert.deepEqual(
      (await query(target, "flatbuffer", "SELECT _data FROM User ORDER BY id", 34)).data,
      expected,
    );
  } finally {
    target.destroy();
  }
});

test("append persists a node-owned snapshot that a fresh signed instance reloads automatically", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const expected = wrapSizePrefixed([
    fixtureRuntime.createTestUser(77, "Durable", "durable@example.com", 42),
  ]);
  const opaqueState = createOpaqueStateAdapter({ base64Reads: true });
  const source = await createHarness({
    hostcallDispatch: opaqueState.dispatch,
  });
  try {
    await configure(source, "flatbuffer", 201);
    await append(source, "flatbuffer", expected, 202);
  } finally {
    source.destroy();
  }

  assert.deepEqual(
    opaqueState.calls.map(({ operation }) => operation),
    [
      "storage.adapter.opaque.read",
      "storage.adapter.opaque.list",
      "storage.adapter.opaque.replace",
      "storage.adapter.opaque.sync",
      "storage.adapter.opaque.replace",
      "storage.adapter.opaque.sync",
      "storage.adapter.opaque.replace",
      "storage.adapter.opaque.sync",
      "storage.adapter.opaque.replace",
      "storage.adapter.opaque.sync",
      "storage.adapter.opaque.delete",
      "storage.adapter.opaque.sync",
    ],
    "the signed node stages chunks, commits the manifest, then cleans the prior generation",
  );
  for (const { params } of opaqueState.calls) {
    assert.equal(params.namespace, "primary");
  }
  assert.deepEqual(
    opaqueState.calls
      .filter(({ operation }) => operation === "storage.adapter.opaque.replace")
      .map(({ params }) => params.key),
    [
      "snapshot.g1.c0.bin",
      "snapshot.manifest",
      "snapshot.g2.c0.bin",
      "snapshot.manifest",
    ],
  );
  assert.equal(
    opaqueState.calls.find(({ operation }) => operation === "storage.adapter.opaque.delete")
      .params.key,
    "snapshot.g1.c0.bin",
  );

  const targetCallOffset = opaqueState.calls.length;
  const target = await createHarness({
    hostcallDispatch: opaqueState.dispatch,
  });
  try {
    await configure(target, "flatbuffer", 203);
    assert.deepEqual(
      (await query(
        target,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        204,
      )).data,
      expected,
    );
  } finally {
    target.destroy();
  }
  assert.deepEqual(
    opaqueState.calls.slice(targetCallOffset).map(({ operation }) => operation),
    [
      "storage.adapter.opaque.read",
      "storage.adapter.opaque.read",
      "storage.adapter.opaque.list",
    ],
    "a fresh signed instance restores the committed manifest and chunk without explicit reload",
  );
});

test("opaque replace and commit-sync failures roll back live and restart-visible rows", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const baseline = wrapSizePrefixed([
    fixtureRuntime.createTestUser(80, "Baseline", "baseline@example.com", 40),
  ]);
  const candidates = [
    {
      operation: "storage.adapter.opaque.replace",
      occurrence: 1,
      record: fixtureRuntime.createTestUser(81, "Replace", "replace@example.com", 41),
    },
    {
      operation: "storage.adapter.opaque.sync",
      occurrence: 2,
      record: fixtureRuntime.createTestUser(82, "Sync", "sync@example.com", 42),
    },
  ];
  const opaqueState = createOpaqueStateAdapter();
  const source = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    await configure(source, "flatbuffer", 210);
    await append(source, "flatbuffer", baseline, 211);

    for (let index = 0; index < candidates.length; index += 1) {
      const candidate = candidates[index];
      opaqueState.failNext(candidate.operation, candidate.occurrence);
      const requestId = 212 + index * 3;
      assertInternalError(
        await invokeAppend(
          source,
          "flatbuffer",
          wrapSizePrefixed([candidate.record]),
          requestId,
        ),
        FSO_OPERATION.APPEND_RECORDS,
        requestId,
        "durable-state-persist-failed",
      );
      assert.deepEqual(
        (await query(
          source,
          "flatbuffer",
          "SELECT _data FROM User ORDER BY id",
          requestId + 1,
        )).data,
        baseline,
        "failed persistence rolls back the live database",
      );

      const fresh = await createHarness({ hostcallDispatch: opaqueState.dispatch });
      try {
        assert.deepEqual(
          (await query(
            fresh,
            "flatbuffer",
            "SELECT _data FROM User ORDER BY id",
            requestId + 2,
          )).data,
          baseline,
          "failed persistence leaves the prior committed generation restart-visible",
        );
      } finally {
        fresh.destroy();
      }
      assert.deepEqual(
        opaqueState.keys(),
        ["snapshot.g2.c0.bin", "snapshot.manifest"],
        "failed generations are cleaned without deleting the prior commit",
      );
    }
  } finally {
    source.destroy();
  }
});

test("indeterminate manifest rollback poisons normal APIs until explicit repair", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const baseline = wrapSizePrefixed([
    fixtureRuntime.createTestUser(90, "Poison Base", "poison-base@example.com", 50),
  ]);
  const candidate = wrapSizePrefixed([
    fixtureRuntime.createTestUser(91, "Poison Next", "poison-next@example.com", 51),
  ]);
  const opaqueState = createOpaqueStateAdapter();
  const source = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    await configure(source, "flatbuffer", 220);
    await append(source, "flatbuffer", baseline, 221);
    const snapshotType = methodType("snapshot", "input", "control", "flatbuffer");
    const snapshotResponse = await source.invoke({
      methodId: "snapshot",
      inputs: [{
        portId: "control",
        typeRef: snapshotType,
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.SNAPSHOT,
          requestId: 225,
        }),
      }],
    });
    assertComplete(snapshotResponse, FSO_OPERATION.SNAPSHOT, 225);
    const snapshotFrames = snapshotResponse.outputs.filter(
      (frame) => frame.portId === "snapshot",
    );
    opaqueState.failNext("storage.adapter.opaque.sync", 2);
    opaqueState.failNext("storage.adapter.opaque.replace", 3);
    assertInternalError(
      await invokeAppend(source, "flatbuffer", candidate, 222),
      FSO_OPERATION.APPEND_RECORDS,
      222,
      "durable-state-persist-failed",
    );
    for (const requestId of [223, 224]) {
      assertInternalError(
        await invokeQuery(
          source,
          "flatbuffer",
          "SELECT _data FROM User ORDER BY id",
          requestId,
        ),
        FSO_OPERATION.QUERY_RECORDS,
        requestId,
        "durable-state-load-failed",
      );
    }
    const reloadType = methodType("reload", "input", "snapshot", "flatbuffer");
    assertComplete(
      await source.invoke({
        methodId: "reload",
        inputs: snapshotFrames.map((frame) => ({
          portId: "snapshot",
          typeRef: reloadType,
          payload: frame.payload,
        })),
      }),
      FSO_OPERATION.RELOAD,
      225,
    );
    assert.deepEqual(
      (await query(
        source,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        226,
      )).data,
      baseline,
      "explicit reload is the only operation that clears the poison state",
    );
  } finally {
    source.destroy();
  }
});

test("opaque snapshots are digest-checked chunks below the browser hostcall ceiling", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const records = [];
  let recordStreamBytes = 0;
  for (let index = 0; recordStreamBytes < 1_200_000; index += 1) {
    const record = fixtureRuntime.createTestUser(
      20_000 + index,
      `Durable Chunk ${index}`,
      `durable-chunk-${index}@example.com`,
      20 + (index % 70),
    );
    records.push(record);
    recordStreamBytes += 4 + record.byteLength;
  }
  const expected = wrapSizePrefixed(records);
  const opaqueState = createOpaqueStateAdapter();
  const source = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  let snapshotFrames;
  try {
    await configure(source, "flatbuffer", 230);
    await append(source, "flatbuffer", expected, 231);
    const snapshotType = methodType("snapshot", "input", "control", "flatbuffer");
    const snapshotResponse = await source.invoke({
      methodId: "snapshot",
      inputs: [{
        portId: "control",
        typeRef: snapshotType,
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.SNAPSHOT,
          requestId: 234,
        }),
      }],
    });
    assertComplete(snapshotResponse, FSO_OPERATION.SNAPSHOT, 234);
    snapshotFrames = snapshotResponse.outputs.filter(
      (frame) => frame.portId === "snapshot",
    );
  } finally {
    source.destroy();
  }

  const committedChunks = opaqueState.keys().filter((key) =>
    key.startsWith("snapshot.g2.c")
  );
  assert.ok(committedChunks.length >= 2, "the catalog spans multiple opaque chunks");
  const chunkWrites = opaqueState.calls.filter(({ operation, params }) =>
    operation === "storage.adapter.opaque.replace" &&
      params.key.startsWith("snapshot.g2.c")
  );
  assert.equal(chunkWrites.length, committedChunks.length);
  assert.equal(
    Math.max(...chunkWrites.map(({ params }) => params.data.byteLength)),
    1024 * 1024,
    "node-owned chunks stay well below the canonical 16 MiB bridge request limit",
  );

  const target = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    assert.deepEqual(
      (await query(
        target,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        232,
      )).data,
      expected,
    );
  } finally {
    target.destroy();
  }

  opaqueState.mutate(committedChunks[0], (bytes) => {
    bytes[Math.floor(bytes.byteLength / 2)] ^= 0x01;
  });
  const corrupted = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    const queryType = methodType("query_records", "input", "query", "flatbuffer");
    const response = await corrupted.invoke({
      methodId: "query_records",
      inputs: [{
        portId: "query",
        typeRef: queryType,
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.QUERY_RECORDS,
          requestId: 233,
          query: "SELECT _data FROM User ORDER BY id",
        }),
      }],
    });
    assertInternalError(
      response,
      FSO_OPERATION.QUERY_RECORDS,
      233,
      "durable-state-load-failed",
    );
  } finally {
    corrupted.destroy();
  }

  const repair = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    const reloadType = methodType("reload", "input", "snapshot", "flatbuffer");
    const reloadResponse = await repair.invoke({
      methodId: "reload",
      inputs: snapshotFrames.map((frame) => ({
        portId: "snapshot",
        typeRef: reloadType,
        payload: frame.payload,
      })),
    });
    assertComplete(reloadResponse, FSO_OPERATION.RELOAD, 234);
  } finally {
    repair.destroy();
  }
  const recovered = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    assert.deepEqual(
      (await query(
        recovered,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        235,
      )).data,
      expected,
      "an explicit checksum-valid snapshot repairs corrupt durable chunks",
    );
  } finally {
    recovered.destroy();
  }
});

test("fresh durable load sweeps orphan chunk generations", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const expected = wrapSizePrefixed([
    fixtureRuntime.createTestUser(92, "Orphan", "orphan@example.com", 52),
  ]);
  const opaqueState = createOpaqueStateAdapter();
  const source = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    await configure(source, "flatbuffer", 240);
    await append(source, "flatbuffer", expected, 241);
  } finally {
    source.destroy();
  }
  opaqueState.copy("snapshot.g2.c0.bin", "snapshot.g999.c0.bin");

  const target = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    assert.deepEqual(
      (await query(
        target,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        242,
      )).data,
      expected,
    );
  } finally {
    target.destroy();
  }
  assert.deepEqual(
    opaqueState.keys(),
    ["snapshot.g2.c0.bin", "snapshot.manifest"],
    "load deletes only unreferenced FlatSQL generation chunks",
  );
});

test("oversized durable metadata fails before chunk reads or allocation growth", async () => {
  const opaqueState = createOpaqueStateAdapter();
  const source = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    await configure(source, "flatbuffer", 250);
  } finally {
    source.destroy();
  }
  opaqueState.mutate("snapshot.manifest", (bytes) => {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    view.setBigUint64(12, 513n * 1024n * 1024n, true);
    view.setUint32(24, 513, true);
  });
  const callOffset = opaqueState.calls.length;
  const target = await createHarness({ hostcallDispatch: opaqueState.dispatch });
  try {
    assertInternalError(
      await invokeQuery(
        target,
        "flatbuffer",
        "SELECT _data FROM User ORDER BY id",
        251,
      ),
      FSO_OPERATION.QUERY_RECORDS,
      251,
      "durable-state-load-failed",
    );
  } finally {
    target.destroy();
  }
  assert.deepEqual(
    opaqueState.calls.slice(callOffset).map(({ operation }) => operation),
    ["storage.adapter.opaque.read"],
    "oversized metadata is rejected before reserving or fetching any chunk",
  );
});

test("multi-chunk snapshots reload completely and reject middle-chunk corruption without mutation", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const records = [];
  let recordStreamBytes = 0;
  for (let index = 0; recordStreamBytes < 2_200_000; index += 1) {
    const record = fixtureRuntime.createTestUser(
      10_000 + index,
      `Snapshot User ${index}`,
      `snapshot-${index}@example.com`,
      20 + (index % 70),
    );
    records.push(record);
    recordStreamBytes += 4 + record.byteLength;
  }
  const expected = wrapSizePrefixed(records);
  const inputChunks = [];
  let pending = [];
  let pendingBytes = 0;
  for (const record of records) {
    const framedBytes = 4 + record.byteLength;
    if (pendingBytes > 0 && pendingBytes + framedBytes > 900_000) {
      inputChunks.push(wrapSizePrefixed(pending));
      pending = [];
      pendingBytes = 0;
    }
    pending.push(record);
    pendingBytes += framedBytes;
  }
  if (pending.length > 0) inputChunks.push(wrapSizePrefixed(pending));

  const source = await createHarness();
  let snapshotFrames;
  let snapshot;
  try {
    await configure(source, "flatbuffer", 140);
    const recordsType = methodType(
      "append_records",
      "input",
      "records",
      "flatbuffer",
    );
    const appendResponse = await source.invoke({
      methodId: "append_records",
      inputs: inputChunks.map((data, chunkSequence) => ({
        portId: "records",
        typeRef: recordsType,
        payload: buildCanonicalFsb({
          requestId: 141,
          kind: FSB_KIND.RECORD_STREAM,
          chunkSequence,
          final: chunkSequence + 1 === inputChunks.length,
          totalBytes: expected.byteLength,
          recordCount: records.length,
          schemaName: "User",
          fileIdentifier: "USER",
          data,
        }),
      })),
    });
    assert.equal(
      assertComplete(
        appendResponse,
        FSO_OPERATION.APPEND_RECORDS,
        141,
      ).affectedRecords,
      BigInt(records.length),
    );
    const snapshotResponse = await source.invoke({
      methodId: "snapshot",
      inputs: [{
        portId: "control",
        typeRef: methodType("snapshot", "input", "control", "flatbuffer"),
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.SNAPSHOT,
          requestId: 142,
        }),
      }],
    });
    assertComplete(snapshotResponse, FSO_OPERATION.SNAPSHOT, 142);
    snapshotFrames = snapshotResponse.outputs.filter(
      (frame) => frame.portId === "snapshot",
    );
    snapshot = outputStream(snapshotResponse, "snapshot");
    assert.ok(snapshot.chunks.length >= 3, "fixture produces a middle snapshot chunk");
    assert.ok(
      snapshot.chunks.every((chunk) => chunk.data.byteLength <= 1_048_576),
      "every canonical FSB snapshot chunk remains bounded to one MiB",
    );
  } finally {
    source.destroy();
  }

  const retained = wrapSizePrefixed([
    fixtureRuntime.createTestUser(9_999, "Retained", "retained@example.com", 99),
  ]);
  const target = await createHarness();
  try {
    await configure(target, "flatbuffer", 143);
    await append(target, "flatbuffer", retained, 144);
    const middleIndex = Math.floor(snapshot.chunks.length / 2);
    const corruptedFrames = snapshot.chunks.map((chunk, index) => {
      const data = chunk.data.slice();
      if (index === middleIndex) data[Math.floor(data.byteLength / 2)] ^= 0xff;
      return {
        portId: "snapshot",
        typeRef: methodType("reload", "input", "snapshot", "flatbuffer"),
        payload: buildCanonicalFsb({
          ...chunk,
          requestId: 145,
          data,
        }),
      };
    });
    assertRejected(
      await target.invoke({ methodId: "reload", inputs: corruptedFrames }),
      FSO_OPERATION.RELOAD,
      145,
    );
    assert.deepEqual(
      (await query(target, "flatbuffer", "SELECT _data FROM User ORDER BY id", 146)).data,
      retained,
    );
  } finally {
    target.destroy();
  }

  const restored = await createHarness();
  try {
    const reloadType = methodType("reload", "input", "snapshot", "flatbuffer");
    const reloadResponse = await restored.invoke({
      methodId: "reload",
      inputs: snapshotFrames.map((frame) => ({
        portId: "snapshot",
        typeRef: reloadType,
        payload: frame.payload,
      })),
    });
    assertComplete(reloadResponse, FSO_OPERATION.RELOAD, 142);
    const result = await query(
      restored,
      "flatbuffer",
      "SELECT _data FROM User ORDER BY id",
      147,
    );
    assert.deepEqual(result.data, expected);
  } finally {
    restored.destroy();
  }
});

test("reload rejects checksum corruption and truncation without mutating configured state", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const recordStream = wrapSizePrefixed([
    fixtureRuntime.createTestUser(70, "Seventy", "70@example.com", 70),
  ]);
  const source = await createHarness();
  let snapshot;
  try {
    await configure(source, "flatbuffer", 105);
    await append(source, "flatbuffer", recordStream, 106);
    const response = await source.invoke({
      methodId: "snapshot",
      inputs: [{
        portId: "control",
        typeRef: methodType("snapshot", "input", "control", "flatbuffer"),
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.SNAPSHOT,
          requestId: 107,
        }),
      }],
    });
    assertComplete(response, FSO_OPERATION.SNAPSHOT, 107);
    snapshot = outputStream(response, "snapshot");
    assert.equal(snapshot.chunks.length, 1, "fixture snapshot fits one FSB chunk");
  } finally {
    source.destroy();
  }

  const retained = wrapSizePrefixed([
    fixtureRuntime.createTestUser(71, "Seventy One", "71@example.com", 71),
  ]);
  const target = await createHarness();
  try {
    await configure(target, "flatbuffer", 108);
    await append(target, "flatbuffer", retained, 109);
    const invokeReload = async (data, requestId) => {
      const original = snapshot.chunks[0];
      return target.invoke({
        methodId: "reload",
        inputs: [{
          portId: "snapshot",
          typeRef: methodType("reload", "input", "snapshot", "flatbuffer"),
          payload: buildCanonicalFsb({
            requestId,
            kind: FSB_KIND.SNAPSHOT,
            chunkSequence: 0,
            final: true,
            totalBytes: data.byteLength,
            schemaName: original.schemaName,
            fileIdentifier: original.fileIdentifier,
            data,
            sha256: original.sha256,
          }),
        }],
      });
    };

    const corrupted = snapshot.data.slice();
    corrupted[Math.floor(corrupted.byteLength / 2)] ^= 0xff;
    assertRejected(
      await invokeReload(corrupted, 110),
      FSO_OPERATION.RELOAD,
      110,
    );
    assert.deepEqual(
      (await query(target, "flatbuffer", "SELECT _data FROM User ORDER BY id", 111)).data,
      retained,
    );
    assertRejected(
      await invokeReload(
        snapshot.data.subarray(0, snapshot.data.byteLength - 1),
        112,
      ),
      FSO_OPERATION.RELOAD,
      112,
    );
    assert.deepEqual(
      (await query(target, "flatbuffer", "SELECT _data FROM User ORDER BY id", 113)).data,
      retained,
    );
  } finally {
    target.destroy();
  }
});

test("unsupported retention and control fields are rejected instead of silently claimed", async () => {
  const harness = await createHarness();
  try {
    await configure(harness, "flatbuffer", 110);
    const retention = await harness.invoke({
      methodId: "configure_retention",
      inputs: [{
        portId: "control",
        typeRef: methodType(
          "configure_retention",
          "input",
          "control",
          "flatbuffer",
        ),
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.CONFIGURE_RETENTION,
          requestId: 111,
          retentionMaxRecords: 1,
          retentionMaxAgeMillis: 60_000,
          compactionTargetBytes: 4096,
        }),
      }],
    });
    assertRejected(retention, FSO_OPERATION.CONFIGURE_RETENTION, 111);

    const explicitIndex = await harness.invoke({
      methodId: "configure_index",
      inputs: [{
        portId: "control",
        typeRef: methodType("configure_index", "input", "control", "flatbuffer"),
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.CONFIGURE_INDEX,
          requestId: 112,
          databaseName: "explicit-index",
          schemaIdl: USER_SCHEMA,
          tableBindings: [{ fileIdentifier: "USER", tableName: "User" }],
          tableName: "User",
          indexName: "idx_age",
          indexExpression: "age",
        }),
      }],
    });
    assertRejected(explicitIndex, FSO_OPERATION.CONFIGURE_INDEX, 112);

    const upsertKey = await harness.invoke({
      methodId: "upsert_view",
      inputs: [{
        portId: "control",
        typeRef: methodType("upsert_view", "input", "control", "flatbuffer"),
        payload: buildCanonicalFso({
          operation: FSO_OPERATION.UPSERT_VIEW,
          requestId: 113,
          viewName: "unsupported-key",
          query: "SELECT _data FROM User",
          upsertKeyExpression: "id",
        }),
      }],
    });
    assertRejected(upsertKey, FSO_OPERATION.UPSERT_VIEW, 113);
  } finally {
    harness.destroy();
  }
});

test("aligned policy, compaction, snapshot, and reload stay inside the signed node", async () => {
  const fixtureRuntime = await loadFlatSQLStandalone();
  const first = fixtureRuntime.createTestUser(8, "Eight", "eight@example.com", 48);
  const second = fixtureRuntime.createTestUser(9, "Nine", "nine@example.com", 49);
  const expected = wrapSizePrefixed([second]);
  const source = await createHarness();
  let snapshotFrames;
  try {
    await configure(source, "aligned-binary", 51);
    await append(source, "aligned-binary", wrapSizePrefixed([first, second]), 52);

    const upsert = await source.invoke({
      methodId: "upsert_view",
      inputs: [{
        portId: "control",
        typeRef: methodType("upsert_view", "input", "control", "aligned-binary"),
        payload: buildAlignedFso({
          operation: FSO_OPERATION.UPSERT_VIEW,
          requestId: 53,
          viewName: "latest-user",
          query: "SELECT _data FROM User ORDER BY id DESC LIMIT 1",
        }),
      }],
    });
    assertComplete(upsert, FSO_OPERATION.UPSERT_VIEW, 53);

    const retention = await source.invoke({
      methodId: "configure_retention",
      inputs: [{
        portId: "control",
        typeRef: methodType(
          "configure_retention",
          "input",
          "control",
          "aligned-binary",
        ),
        payload: buildAlignedFso({
          operation: FSO_OPERATION.CONFIGURE_RETENTION,
          requestId: 54,
          retentionMaxRecords: 1,
        }),
      }],
    });
    assertComplete(retention, FSO_OPERATION.CONFIGURE_RETENTION, 54);

    const compact = await source.invoke({
      methodId: "compact",
      inputs: [{
        portId: "control",
        typeRef: methodType("compact", "input", "control", "aligned-binary"),
        payload: buildAlignedFso({
          operation: FSO_OPERATION.COMPACT,
          requestId: 55,
        }),
      }],
    });
    assertComplete(compact, FSO_OPERATION.COMPACT, 55);
    assert.deepEqual(
      (await query(source, "aligned-binary", "", 56, "latest-user")).data,
      expected,
    );

    const snapshotResponse = await source.invoke({
      methodId: "snapshot",
      inputs: [{
        portId: "control",
        typeRef: methodType("snapshot", "input", "control", "aligned-binary"),
        payload: buildAlignedFso({
          operation: FSO_OPERATION.SNAPSHOT,
          requestId: 57,
        }),
      }],
    });
    assertComplete(snapshotResponse, FSO_OPERATION.SNAPSHOT, 57);
    snapshotFrames = snapshotResponse.outputs.filter((frame) => frame.portId === "snapshot");
    assert.ok(snapshotFrames.length > 0);
    assert.equal(snapshotFrames[0].typeRef.wireFormat, "aligned-binary");
  } finally {
    source.destroy();
  }

  const target = await createHarness();
  try {
    const reloadResponse = await target.invoke({
      methodId: "reload",
      inputs: snapshotFrames.map((frame) => ({
        portId: "snapshot",
        typeRef: methodType("reload", "input", "snapshot", "aligned-binary"),
        payload: frame.payload,
      })),
    });
    assertComplete(reloadResponse, FSO_OPERATION.RELOAD, 57);
    assert.deepEqual(
      (await query(target, "aligned-binary", "", 58, "latest-user")).data,
      expected,
    );
  } finally {
    target.destroy();
  }
});
