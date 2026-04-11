import { Worker } from 'node:worker_threads';
import { parseSchema } from '../schema/index.js';
const schemaCache = new Map();
function sizePrefixedByteLength(buffers) {
    let totalLength = 0;
    for (const buffer of buffers) {
        totalLength += 4 + buffer.length;
    }
    return totalLength;
}
function writeSizePrefixedStream(target, buffers) {
    let offset = 0;
    const view = new DataView(target.buffer, target.byteOffset, target.byteLength);
    for (const buffer of buffers) {
        view.setUint32(offset, buffer.length, true);
        offset += 4;
        target.set(buffer, offset);
        offset += buffer.length;
    }
    return offset;
}
function supportsSharedArrayBuffer() {
    return typeof SharedArrayBuffer !== 'undefined' && typeof Atomics !== 'undefined';
}
export class FlatSQLArtifactWorkerClient {
    workerPath;
    worker = null;
    nextId = 1;
    pending = new Map();
    constructor(workerPath = new URL('../../wasm/flatsql-artifact.worker.js', import.meta.url)) {
        this.workerPath = workerPath;
    }
    async init() {
        if (this.worker) {
            return;
        }
        this.worker = new Worker(this.workerPath);
        await new Promise((resolve, reject) => {
            const handleMessage = (message) => {
                if (message.type === 'ready') {
                    resolve();
                    return;
                }
                if (typeof message.id !== 'number') {
                    return;
                }
                const pending = this.pending.get(message.id);
                if (!pending) {
                    return;
                }
                this.pending.delete(message.id);
                if (message.success) {
                    pending.resolve(message.result);
                }
                else {
                    pending.reject(new Error(message.error ?? 'Artifact worker failed'));
                }
            };
            this.worker.on('message', handleMessage);
            this.worker.on('error', reject);
            this.worker.on('exit', (code) => {
                if (code !== 0) {
                    reject(new Error(`Artifact worker exited with code ${code}`));
                }
            });
        });
    }
    async createBuilder(schemaSource, options) {
        const schemaName = options.name ?? 'artifact';
        const cacheKey = `${schemaName}\u0000${schemaSource}`;
        let schema = schemaCache.get(cacheKey);
        if (!schema) {
            schema = parseSchema(schemaSource, schemaName);
            schemaCache.set(cacheKey, schema);
        }
        const builderId = `artifact_${this.nextId++}`;
        await this.call('createBuilder', {
            builderId,
            schema,
            performanceProfile: options.performanceProfile ?? 'fast',
            sqlitePath: options.sqlitePath,
        });
        return new FlatSQLArtifactWorkerBuilder(this, builderId, schema, options);
    }
    async close() {
        if (!this.worker) {
            return;
        }
        await this.worker.terminate();
        this.worker = null;
    }
    async call(method, params) {
        if (!this.worker) {
            throw new Error('Artifact worker client is not initialized');
        }
        const id = this.nextId++;
        return await new Promise((resolve, reject) => {
            this.pending.set(id, { resolve, reject });
            this.worker.postMessage({ id, method, params });
        });
    }
}
export class FlatSQLArtifactWorkerBuilder {
    client;
    builderId;
    schema;
    preferSharedArrayBuffer;
    constructor(client, builderId, schema, options) {
        this.client = client;
        this.builderId = builderId;
        this.schema = schema;
        this.preferSharedArrayBuffer = options.preferSharedArrayBuffer ?? true;
    }
    async registerFileId(fileId, tableName) {
        await this.client.call('registerFileId', { builderId: this.builderId, fileId, tableName });
    }
    async enableDemoExtractors() {
        await this.client.call('enableDemoExtractors', { builderId: this.builderId });
    }
    async ingestBuffers(buffers, options = {}) {
        const canUseShared = this.preferSharedArrayBuffer && supportsSharedArrayBuffer();
        if (canUseShared) {
            const byteLength = sizePrefixedByteLength(buffers);
            const sharedBuffer = new SharedArrayBuffer(byteLength);
            writeSizePrefixedStream(new Uint8Array(sharedBuffer), buffers);
            return await this.client.call('ingestShared', {
                builderId: this.builderId,
                sharedBuffer,
                byteLength,
                options,
            });
        }
        return await this.client.call('ingestClone', {
            builderId: this.builderId,
            buffers: buffers.map((buffer) => Array.from(buffer)),
            options,
        });
    }
    async query(sql) {
        return await this.client.call('query', { builderId: this.builderId, sql });
    }
    async close() {
        await this.client.call('closeBuilder', { builderId: this.builderId });
    }
}
//# sourceMappingURL=worker-client.js.map