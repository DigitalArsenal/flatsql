import { Worker } from 'node:worker_threads';

import { parseSchema, type DatabaseSchema } from '../schema/index.js';
import type {
  ArtifactBuilderOptions,
  ArtifactIngestOptions,
  ArtifactIngestResult,
  ArtifactTransportMode,
  ArtifactWorkerBuilderOptions,
} from './types.js';

interface PendingCall {
  resolve: (value: any) => void;
  reject: (error: Error) => void;
}

interface WorkerMessage {
  id?: number;
  type?: string;
  success?: boolean;
  result?: any;
  error?: string;
}

function buildSizePrefixedStream(buffers: Uint8Array[]): Uint8Array {
  let totalLength = 0;
  for (const buffer of buffers) {
    totalLength += 4 + buffer.length;
  }

  const stream = new Uint8Array(totalLength);
  let offset = 0;
  for (const buffer of buffers) {
    new DataView(stream.buffer, offset, 4).setUint32(0, buffer.length, true);
    offset += 4;
    stream.set(buffer, offset);
    offset += buffer.length;
  }
  return stream;
}

function supportsSharedArrayBuffer(): boolean {
  return typeof SharedArrayBuffer !== 'undefined' && typeof Atomics !== 'undefined';
}

export class FlatSQLArtifactWorkerClient {
  private readonly workerPath: URL;
  private worker: Worker | null = null;
  private nextId = 1;
  private readonly pending = new Map<number, PendingCall>();

  constructor(workerPath: URL = new URL('../../wasm/flatsql-artifact.worker.js', import.meta.url)) {
    this.workerPath = workerPath;
  }

  async init(): Promise<void> {
    if (this.worker) {
      return;
    }

    this.worker = new Worker(this.workerPath);

    await new Promise<void>((resolve, reject) => {
      const handleMessage = (message: WorkerMessage) => {
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
        } else {
          pending.reject(new Error(message.error ?? 'Artifact worker failed'));
        }
      };

      this.worker!.on('message', handleMessage);
      this.worker!.on('error', reject);
      this.worker!.on('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`Artifact worker exited with code ${code}`));
        }
      });
    });
  }

  async createBuilder(schemaSource: string, options: ArtifactWorkerBuilderOptions): Promise<FlatSQLArtifactWorkerBuilder> {
    const schema = parseSchema(schemaSource, options.name ?? 'artifact');
    const builderId = `artifact_${this.nextId++}`;
    await this.call('createBuilder', {
      builderId,
      schema,
      sqlitePath: options.sqlitePath,
    });
    return new FlatSQLArtifactWorkerBuilder(this, builderId, schema, options);
  }

  async close(): Promise<void> {
    if (!this.worker) {
      return;
    }

    await this.worker.terminate();
    this.worker = null;
  }

  async call(method: string, params: Record<string, unknown>): Promise<any> {
    if (!this.worker) {
      throw new Error('Artifact worker client is not initialized');
    }

    const id = this.nextId++;
    return await new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker!.postMessage({ id, method, params });
    });
  }
}

export class FlatSQLArtifactWorkerBuilder {
  private readonly client: FlatSQLArtifactWorkerClient;
  private readonly builderId: string;
  private readonly schema: DatabaseSchema;
  private readonly preferSharedArrayBuffer: boolean;

  constructor(
    client: FlatSQLArtifactWorkerClient,
    builderId: string,
    schema: DatabaseSchema,
    options: ArtifactWorkerBuilderOptions
  ) {
    this.client = client;
    this.builderId = builderId;
    this.schema = schema;
    this.preferSharedArrayBuffer = options.preferSharedArrayBuffer ?? true;
  }

  async registerFileId(fileId: string, tableName: string): Promise<void> {
    await this.client.call('registerFileId', { builderId: this.builderId, fileId, tableName });
  }

  async enableDemoExtractors(): Promise<void> {
    await this.client.call('enableDemoExtractors', { builderId: this.builderId });
  }

  async ingestBuffers(buffers: Uint8Array[], options: ArtifactIngestOptions = {}): Promise<ArtifactIngestResult> {
    const canUseShared = this.preferSharedArrayBuffer && supportsSharedArrayBuffer();
    if (canUseShared) {
      const stream = buildSizePrefixedStream(buffers);
      const sharedBuffer = new SharedArrayBuffer(stream.byteLength);
      new Uint8Array(sharedBuffer).set(stream);
      return await this.client.call('ingestShared', {
        builderId: this.builderId,
        sharedBuffer,
        byteLength: stream.byteLength,
        options,
      });
    }

    return await this.client.call('ingestClone', {
      builderId: this.builderId,
      buffers: buffers.map((buffer) => Array.from(buffer)),
      options,
    });
  }

  async query(sql: string): Promise<{ columns: string[]; rows: any[][]; rowCount: number }> {
    return await this.client.call('query', { builderId: this.builderId, sql });
  }

  async close(): Promise<void> {
    await this.client.call('closeBuilder', { builderId: this.builderId });
  }
}
