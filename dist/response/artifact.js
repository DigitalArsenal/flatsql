import { sizePrefixedByteLength, writeSizePrefixedStream } from '../artifacts/transport.js';
import { createResponseCacheKey, hashBytes } from './cache-key.js';
const DEFAULT_CHUNK_BYTES = 1024 * 1024;
const TEXT_ENCODER = new TextEncoder();
function jsonSafeValue(value) {
    if (value instanceof Uint8Array) {
        return Array.from(value);
    }
    if (typeof value === 'bigint') {
        return value.toString();
    }
    if (Array.isArray(value)) {
        return value.map((item) => jsonSafeValue(item));
    }
    if (value && typeof value === 'object') {
        return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, jsonSafeValue(item)]));
    }
    return value;
}
function encodeJsonResult(result) {
    return TEXT_ENCODER.encode(JSON.stringify({
        columns: result.columns,
        rows: jsonSafeValue(result.rows),
        rowCount: result.rowCount,
    }));
}
function extractRawFlatBuffers(result) {
    const buffers = [];
    for (const row of result.rows) {
        for (const cell of row) {
            if (!(cell instanceof Uint8Array)) {
                throw new Error('raw-flatbuffer-stream artifacts require Uint8Array result cells');
            }
            buffers.push(cell);
        }
    }
    return buffers;
}
function encodeRawFlatBufferStream(result) {
    const buffers = extractRawFlatBuffers(result);
    const bytes = new Uint8Array(sizePrefixedByteLength(buffers));
    writeSizePrefixedStream(bytes, buffers);
    return bytes;
}
function encodeResult(result, format) {
    if (format === 'raw-flatbuffer-stream') {
        return encodeRawFlatBufferStream(result);
    }
    return encodeJsonResult(result);
}
function createChunks(bytes, chunkBytes) {
    const chunks = [];
    for (let offset = 0; offset < bytes.byteLength; offset += chunkBytes) {
        const chunk = bytes.subarray(offset, Math.min(offset + chunkBytes, bytes.byteLength));
        const contentHash = hashBytes(chunk);
        chunks.push({
            index: chunks.length,
            offset,
            byteLength: chunk.byteLength,
            contentHash,
            etag: `"${contentHash}"`,
        });
    }
    return chunks;
}
export function createQueryResponseArtifact(result, options) {
    const format = options.format ?? 'json';
    const chunkBytes = options.chunkBytes ?? DEFAULT_CHUNK_BYTES;
    if (!Number.isSafeInteger(chunkBytes) || chunkBytes <= 0) {
        throw new Error(`chunkBytes must be a positive safe integer, received: ${chunkBytes}`);
    }
    const bytes = encodeResult(result, format);
    const contentHash = hashBytes(bytes);
    return {
        metadata: {
            cacheKey: options.cacheKey ?? createResponseCacheKey(options),
            contentHash,
            etag: `"${contentHash}"`,
            byteLength: bytes.byteLength,
            rowCount: result.rowCount,
            columnCount: result.columns.length,
            format,
            encoding: options.encoding ?? 'identity',
            createdAt: options.createdAt ?? new Date().toISOString(),
            chunkBytes,
            chunks: createChunks(bytes, chunkBytes),
            schemaName: options.schemaName,
            schemaVersion: options.schemaVersion,
            publishEventKey: options.publishEventKey,
            projection: options.projection,
        },
        bytes,
    };
}
export function getResponseArtifactChunk(artifact, index) {
    const chunk = artifact.metadata.chunks[index];
    if (!chunk) {
        throw new Error(`Response artifact chunk not found: ${index}`);
    }
    return artifact.bytes.subarray(chunk.offset, chunk.offset + chunk.byteLength);
}
//# sourceMappingURL=artifact.js.map