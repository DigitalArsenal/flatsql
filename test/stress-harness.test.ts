import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { aggregateStressRunArtifacts } from '../src/stress/aggregate.js';
import { buildFullStressDockerCompose } from '../src/stress/docker-compose.js';
import { discoverSdsSchemas, parseSchemaMetadata } from '../src/stress/sds-discovery.js';
import { reduceMetricEvents } from '../src/stress/metrics.js';
import { buildMeasuredStandalonePublishEventMetrics } from '../src/stress/native-publish-event-workload.js';
import { buildMeasuredPublishEventMetrics } from '../src/stress/publish-event-workload.js';
import { assertProductionReadiness } from '../src/stress/production-gate.js';
import { buildDerivedStressReport } from '../src/stress/report.js';
import { runSmokeHarness } from '../src/stress/smoke-runner.js';
import { buildWorkloadManifest, DEFAULT_USE_CASES } from '../src/stress/workload.js';
import type { MetricEvent } from '../src/stress/types.js';

describe('SDS stress harness', () => {
  test('discovers SDS schemas and extracts root metadata', async () => {
    const root = await mkdtemp(join(tmpdir(), 'flatsql-sds-fixture-'));
    await mkdir(join(root, 'PNM'), { recursive: true });
    await writeFile(
      join(root, 'PNM', 'main.fbs'),
      'table PNM { FILE_ID:string; } root_type PNM;'
    );

    const schemas = await discoverSdsSchemas(root);

    expect(schemas).toEqual([
      expect.objectContaining({ name: 'PNM', rootType: 'PNM', tableNames: ['PNM'] }),
    ]);
    expect(schemas[0]).not.toHaveProperty('hasFileIdentifier');
    expect(schemas[0]).not.toHaveProperty('indexedFields');
  });

  test('builds a 100 node manifest with all transport use cases', () => {
    const schemas = [
      parseSchemaMetadata(
        'PNM',
        'table PNM { FILE_ID:string; } root_type PNM;',
        '/schema/PNM/main.fbs'
      ),
      parseSchemaMetadata(
        'DPM',
        'table DPM { FILE_ID:string; } root_type DPM;',
        '/schema/DPM/main.fbs'
      ),
    ];

    const manifest = buildWorkloadManifest({ mode: 'full', nodeCount: 100, nodeStorageGb: 1 }, schemas);

    expect(manifest.nodeCount).toBe(100);
    expect(manifest.nodeStorageGb).toBe(1);
    expect(manifest.recordsPerNode).toBe(20000);
    expect(manifest.nodeCount * manifest.recordsPerNode).toBeGreaterThanOrEqual(1_000_000);
    expect(manifest.useCases.map((useCase) => useCase.id)).toEqual(
      DEFAULT_USE_CASES.map((useCase) => useCase.id)
    );
    expect(manifest.assignments).toHaveLength(100);
    expect(manifest.assignments[0].schemas.length).toBeGreaterThan(0);
  });

  test('supports global node offsets for per-container workers', () => {
    const schemas = [
      parseSchemaMetadata(
        'PNM',
        'table PNM { FILE_ID:string; } root_type PNM;',
        '/schema/PNM/main.fbs'
      ),
    ];

    const manifest = buildWorkloadManifest({
      nodeCount: 1,
      nodeIdOffset: 42,
      nodeIsolation: 'container',
      transport: 'docker-compose',
    }, schemas);

    expect(manifest.nodeIdOffset).toBe(42);
    expect(manifest.nodeIsolation).toBe('container');
    expect(manifest.assignments).toEqual([
      expect.objectContaining({ nodeId: 42 }),
    ]);
  });

  test('reduces metrics with transport byte totals and latency percentiles', () => {
    const summary = reduceMetricEvents([
      {
        runId: 'r',
        nodeId: 0,
        schema: 'PNM',
        useCase: 'hot-file-id-cache',
        operation: 'query-template',
        records: 1,
        requestBytes: 20,
        responseBytes: 100,
        flatbufferBytes: 80,
        storageBytes: 1000,
        durationMs: 10,
        cacheHits: 0,
        cacheMisses: 1,
        cacheSize: 1,
        errors: 0,
        measurement: 'measured',
        transport: 'in-process',
      },
      {
        runId: 'r',
        nodeId: 0,
        schema: 'PNM',
        useCase: 'hot-file-id-cache',
        operation: 'query-template',
        records: 1,
        requestBytes: 20,
        responseBytes: 100,
        flatbufferBytes: 80,
        storageBytes: 1000,
        durationMs: 30,
        cacheHits: 1,
        cacheMisses: 0,
        cacheSize: 1,
        errors: 0,
        measurement: 'measured',
        transport: 'in-process',
      },
    ]);

    expect(summary.totalRequestBytes).toBe(40);
    expect(summary.totalResponseBytes).toBe(200);
    expect(summary.totalCacheHits).toBe(1);
    expect(summary.useCases['hot-file-id-cache'].latencyP95Ms).toBe(30);
  });

  test('flags runs that do not fill the requested per-node storage budget', () => {
    const manifest = buildWorkloadManifest({
      mode: 'full',
      runId: 'underfilled',
      nodeCount: 2,
      nodeStorageGb: 1,
      recordsPerNode: 100,
      transport: 'docker-compose',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);
    const metrics: MetricEvent[] = [
      {
        runId: 'underfilled',
        nodeId: 0,
        schema: 'PNM',
        useCase: 'bulk-streaming-ingest',
        operation: 'stream-ingest',
        records: 100,
        requestBytes: 4096,
        responseBytes: 128,
        flatbufferBytes: 2048,
        storageBytes: 32 * 1024 * 1024,
        durationMs: 10,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured',
        transport: 'docker-compose',
      },
      {
        runId: 'underfilled',
        nodeId: 1,
        schema: 'PNM',
        useCase: 'bulk-streaming-ingest',
        operation: 'stream-ingest',
        records: 100,
        requestBytes: 4096,
        responseBytes: 128,
        flatbufferBytes: 2048,
        storageBytes: 32 * 1024 * 1024,
        durationMs: 10,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured',
        transport: 'docker-compose',
      },
    ];
    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(report.aggregate.maxNodeStorageFillPct).toBeCloseTo(3.125, 3);
    expect(report.aggregate.minNodeStorageFillPct).toBeCloseTo(3.125, 3);
    expect(report.productionReadiness.status).toBe('fail');
    expect(report.productionReadiness.failedRequirements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'storage-fill',
          actual: expect.stringContaining('min 3.1250%'),
          required: '>= 95% on every node',
        }),
      ])
    );
  });

  test('fails production readiness when any node misses the per-node storage-fill target', () => {
    const manifest = buildWorkloadManifest({
      runId: 'single-underfilled-node',
      nodeCount: 100,
      nodeStorageGb: 1,
      recordsPerNode: 100,
      transport: 'docker-compose',
      nodeIsolation: 'container',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);
    const metrics: MetricEvent[] = [
      {
        runId: 'single-underfilled-node',
        nodeId: 0,
        schema: 'PNM',
        useCase: 'schema-sweep',
        operation: 'schema-parse',
        records: 1,
        requestBytes: 1,
        responseBytes: 1,
        flatbufferBytes: 0,
        storageBytes: 0,
        durationMs: 1,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured',
        transport: 'docker-compose',
        runtime: 'standalone',
      },
      ...Array.from({ length: 100 }, (_, nodeId) => ({
        runId: 'single-underfilled-node',
        nodeId,
        schema: 'PublishEventRecord',
        useCase: 'bulk-streaming-ingest',
        operation: 'stream-ingest',
        records: 100,
        requestBytes: 1024,
        responseBytes: 64,
        flatbufferBytes: 1024,
        storageBytes: nodeId === 42 ? 32 * 1024 * 1024 : 1014 * 1024 * 1024,
        durationMs: 10,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured' as const,
        transport: 'docker-compose' as const,
        runtime: 'standalone' as const,
      })),
    ];

    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(report.aggregate.maxNodeStorageFillPct).toBeGreaterThan(95);
    expect(report.aggregate.minNodeStorageFillPct).toBeCloseTo(3.125, 3);
    expect(report.productionReadiness.status).toBe('fail');
    expect(report.productionReadiness.failedRequirements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'storage-fill',
          required: '>= 95% on every node',
          actual: expect.stringContaining('node 42'),
        }),
      ])
    );
  });

  test('does not mark storage-only probes production ready without 100 Docker nodes', () => {
    const manifest = buildWorkloadManifest({
      runId: 'storage-only-probe',
      nodeCount: 1,
      nodeStorageGb: 1,
      recordsPerNode: 2900000,
      transport: 'in-process',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);
    const metrics: MetricEvent[] = [
      {
        runId: 'storage-only-probe',
        nodeId: 0,
        schema: 'PNM',
        useCase: 'bulk-streaming-ingest',
        operation: 'stream-ingest',
        records: 2900000,
        requestBytes: 1024,
        responseBytes: 64,
        flatbufferBytes: 1024,
        storageBytes: 1014 * 1024 * 1024,
        durationMs: 1000,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured',
        transport: 'in-process',
      },
    ];

    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(report.aggregate.maxNodeStorageFillPct).toBeGreaterThan(95);
    expect(report.productionReadiness.status).toBe('fail');
    expect(report.productionReadiness.failedRequirements.map((item) => item.id)).toEqual(
      expect.arrayContaining(['node-count', 'transport'])
    );
  });

  test('does not mark logical nodes as production ready even under docker-compose transport', () => {
    const manifest = buildWorkloadManifest({
      runId: 'logical-docker-nodes',
      nodeCount: 100,
      nodeStorageGb: 1,
      recordsPerNode: 2900000,
      transport: 'docker-compose',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);
    const metrics: MetricEvent[] = Array.from({ length: 100 }, (_, nodeId) => ({
      runId: 'logical-docker-nodes',
      nodeId,
      schema: 'PNM',
      useCase: 'bulk-streaming-ingest',
      operation: 'stream-ingest',
      records: 2900000,
      requestBytes: 1024,
      responseBytes: 64,
      flatbufferBytes: 1024,
      storageBytes: 1014 * 1024 * 1024,
      durationMs: 1000,
      cacheHits: 0,
      cacheMisses: 0,
      cacheSize: 0,
      errors: 0,
      measurement: 'measured',
      transport: 'docker-compose',
    }));

    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(report.productionReadiness.status).toBe('fail');
    expect(report.productionReadiness.failedRequirements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'node-isolation',
          required: 'container',
          actual: 'logical',
        }),
      ])
    );
  });

  test('fails production readiness when schema sweep metrics do not cover all manifest schemas', () => {
    const manifest = buildWorkloadManifest({
      runId: 'missing-schema-sweep-coverage',
      nodeCount: 100,
      nodeStorageGb: 1,
      recordsPerNode: 2900000,
      transport: 'docker-compose',
      nodeIsolation: 'container',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
      parseSchemaMetadata('VCM', 'table VCM { OBJECT_ID:string; } root_type VCM;', '/schema/VCM/main.fbs'),
    ]);
    const metrics: MetricEvent[] = Array.from({ length: 100 }, (_, nodeId) => ({
      runId: 'missing-schema-sweep-coverage',
      nodeId,
      schema: nodeId === 0 ? 'PNM' : 'PublishEventRecord',
      useCase: nodeId === 0 ? 'schema-sweep' : 'bulk-streaming-ingest',
      operation: nodeId === 0 ? 'schema-parse' : 'stream-ingest',
      records: nodeId === 0 ? 1 : 2900000,
      requestBytes: 1024,
      responseBytes: 64,
      flatbufferBytes: 1024,
      storageBytes: 1014 * 1024 * 1024,
      durationMs: 1000,
      cacheHits: 0,
      cacheMisses: 0,
      cacheSize: 0,
      errors: 0,
      measurement: 'measured' as const,
      transport: 'docker-compose' as const,
    }));

    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(report.productionReadiness.status).toBe('fail');
    expect(report.productionReadiness.failedRequirements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'schema-sweep-coverage',
          required: '2 schemas',
          actual: '1 schemas',
        }),
      ])
    );
  });

  test('fails production readiness when measured events come from the TypeScript fallback runtime', () => {
    const manifest = buildWorkloadManifest({
      runId: 'typescript-runtime-is-not-production',
      nodeCount: 100,
      nodeStorageGb: 1,
      recordsPerNode: 2900000,
      transport: 'docker-compose',
      nodeIsolation: 'container',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);
    const metrics: Array<MetricEvent & { runtime: 'typescript' }> = Array.from({ length: 100 }, (_, nodeId) => ({
      runId: 'typescript-runtime-is-not-production',
      nodeId,
      schema: 'PNM',
      useCase: nodeId === 0 ? 'schema-sweep' : 'bulk-streaming-ingest',
      operation: nodeId === 0 ? 'schema-parse' : 'stream-ingest',
      records: nodeId === 0 ? 1 : 2900000,
      requestBytes: 1024,
      responseBytes: 64,
      flatbufferBytes: 1024,
      storageBytes: 1014 * 1024 * 1024,
      durationMs: 1000,
      cacheHits: 0,
      cacheMisses: 0,
      cacheSize: 0,
      errors: 0,
      measurement: 'measured' as const,
      transport: 'docker-compose' as const,
      runtime: 'typescript' as const,
    }));

    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(report.productionReadiness.status).toBe('fail');
    expect(report.productionReadiness.failedRequirements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'runtime',
          required: 'native, standalone, or wasmedge',
          actual: 'typescript',
        }),
      ])
    );
  });

  test('fails production readiness below the 75 fps ingest guardrail', () => {
    const manifest = buildWorkloadManifest({
      runId: 'below-fps-guardrail',
      nodeCount: 100,
      nodeStorageGb: 1,
      recordsPerNode: 100,
      transport: 'docker-compose',
      nodeIsolation: 'container',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);
    const metrics: MetricEvent[] = [
      {
        runId: 'below-fps-guardrail',
        nodeId: 0,
        schema: 'PNM',
        useCase: 'schema-sweep',
        operation: 'schema-parse',
        records: 1,
        requestBytes: 1,
        responseBytes: 1,
        flatbufferBytes: 0,
        storageBytes: 0,
        durationMs: 1,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured',
        transport: 'docker-compose',
        runtime: 'standalone',
      },
      ...Array.from({ length: 100 }, (_, nodeId) => ({
        runId: 'below-fps-guardrail',
        nodeId,
        schema: 'PublishEventRecord',
        useCase: 'bulk-streaming-ingest',
        operation: 'stream-ingest',
        records: 100,
        requestBytes: 1024,
        responseBytes: 64,
        flatbufferBytes: 1024,
        storageBytes: 1014 * 1024 * 1024,
        durationMs: 200000,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured' as const,
        transport: 'docker-compose' as const,
        runtime: 'standalone' as const,
      })),
    ];

    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(report.aggregate.ingestRecordsPerSecond).toBeLessThan(75);
    expect(report.productionReadiness.status).toBe('fail');
    expect(report.productionReadiness.failedRequirements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'fps',
          required: '>= 75 fps on every node',
          actual: expect.stringMatching(/fps$/),
        }),
      ])
    );
  });

  test('fails production readiness when any node drops below the 75 fps ingest guardrail', () => {
    const manifest = buildWorkloadManifest({
      runId: 'single-slow-node-fps-guardrail',
      nodeCount: 100,
      nodeStorageGb: 1,
      recordsPerNode: 100,
      transport: 'docker-compose',
      nodeIsolation: 'container',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);
    const metrics: MetricEvent[] = [
      {
        runId: 'single-slow-node-fps-guardrail',
        nodeId: 0,
        schema: 'PNM',
        useCase: 'schema-sweep',
        operation: 'schema-parse',
        records: 1,
        requestBytes: 1,
        responseBytes: 1,
        flatbufferBytes: 0,
        storageBytes: 0,
        durationMs: 1,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured',
        transport: 'docker-compose',
        runtime: 'standalone',
      },
      ...Array.from({ length: 100 }, (_, nodeId) => ({
        runId: 'single-slow-node-fps-guardrail',
        nodeId,
        schema: 'PublishEventRecord',
        useCase: 'bulk-streaming-ingest',
        operation: 'stream-ingest',
        records: 100,
        requestBytes: 1024,
        responseBytes: 64,
        flatbufferBytes: 1024,
        storageBytes: 1014 * 1024 * 1024,
        durationMs: nodeId === 73 ? 2000 : 1,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured' as const,
        transport: 'docker-compose' as const,
        runtime: 'standalone' as const,
      })),
    ];

    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(report.aggregate.ingestRecordsPerSecond).toBeGreaterThan(75);
    expect(report.productionReadiness.status).toBe('fail');
    expect(report.productionReadiness.failedRequirements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'fps',
          required: '>= 75 fps on every node',
          actual: expect.stringContaining('node 73'),
        }),
      ])
    );
  });

  test('scales full-mode standalone payloads toward the storage-fill target', async () => {
    const manifest = buildWorkloadManifest({
      mode: 'full',
      runId: 'full-payload-storage-fill',
      nodeCount: 1,
      nodeStorageGb: 0.001,
      recordsPerNode: 10,
      batchBytes: 128 * 1024,
      transport: 'docker-compose',
      nodeIsolation: 'container',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);

    const metrics = await buildMeasuredStandalonePublishEventMetrics(manifest);
    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));
    const ingest = metrics.find((metric) => metric.useCase === 'bulk-streaming-ingest');

    expect(ingest?.flatbufferBytes).toBeGreaterThan(900 * 1024);
    expect(report.aggregate.maxNodeStorageFillPct).toBeGreaterThan(90);
  });

  test('imports full-mode bandwidth artifacts near 10 MiB without falling through single-record ingest', async () => {
    const manifest = buildWorkloadManifest({
      mode: 'full',
      runId: 'full-bandwidth-import',
      nodeCount: 1,
      nodeStorageGb: 0.01,
      recordsPerNode: 200,
      batchBytes: 128 * 1024,
      transport: 'docker-compose',
      nodeIsolation: 'container',
      queryConcurrency: 2,
      hotQueryRatio: 0.01,
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);

    const metrics = await buildMeasuredStandalonePublishEventMetrics(manifest);
    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));
    const bandwidth = metrics.find((metric) => metric.useCase === 'bandwidth-constrained-streaming');

    expect(bandwidth).toEqual(expect.objectContaining({
      errors: 0,
      records: 200,
      requestBytes: expect.any(Number),
    }));
    expect(bandwidth!.requestBytes).toBeGreaterThan(9 * 1024 * 1024);
    expect(report.aggregate.maxNodeStorageFillPct).toBeGreaterThan(90);
  });

  test('throws a release-gate error when production readiness fails', () => {
    const manifest = buildWorkloadManifest({
      runId: 'failed-release-gate',
      nodeCount: 1,
      nodeStorageGb: 1,
      recordsPerNode: 1,
      transport: 'in-process',
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);
    const metrics: MetricEvent[] = [
      {
        runId: 'failed-release-gate',
        nodeId: 0,
        schema: 'PNM',
        useCase: 'bulk-streaming-ingest',
        operation: 'stream-ingest',
        records: 1,
        requestBytes: 1,
        responseBytes: 1,
        flatbufferBytes: 1,
        storageBytes: 1,
        durationMs: 1,
        cacheHits: 0,
        cacheMisses: 0,
        cacheSize: 0,
        errors: 0,
        measurement: 'measured',
        transport: 'in-process',
      },
    ];
    const report = buildDerivedStressReport(manifest, metrics, reduceMetricEvents(metrics));

    expect(() => assertProductionReadiness(report)).toThrow(
      'Production readiness gate failed for failed-release-gate'
    );
  });

  test('writes smoke run artifacts into the requested output directory', async () => {
    const schemaRoot = await mkdtemp(join(tmpdir(), 'flatsql-sds-smoke-schema-'));
    const outputDir = await mkdtemp(join(tmpdir(), 'flatsql-sds-smoke-output-'));
    await mkdir(join(schemaRoot, 'PNM'), { recursive: true });
    await writeFile(
      join(schemaRoot, 'PNM', 'main.fbs'),
      'table PNM { FILE_ID:string; } root_type PNM;'
    );

    const result = await runSmokeHarness({
      schemaRoot,
      outputDir,
      nodeCount: 2,
      recordsPerNode: 10,
      runId: 'smoke-test',
    });

    expect(result.summary.totalRecords).toBeGreaterThan(0);
    await expect(readFile(join(outputDir, 'manifest.json'), 'utf8')).resolves.toContain('"nodeCount": 2');
    await expect(readFile(join(outputDir, 'metrics.ndjson'), 'utf8')).resolves.toContain('hot-file-id-cache');
    await expect(readFile(join(outputDir, 'summary.json'), 'utf8')).resolves.toContain('"totalRequestBytes"');
    await expect(readFile(join(outputDir, 'derived-report.json'), 'utf8')).resolves.toContain('"productionReadiness"');
  });

  test('emits schema-sweep metrics for the assigned SDS schema names', async () => {
    const schemaRoot = await mkdtemp(join(tmpdir(), 'flatsql-sds-schema-sweep-'));
    const outputDir = await mkdtemp(join(tmpdir(), 'flatsql-sds-schema-sweep-output-'));
    await mkdir(join(schemaRoot, 'PNM'), { recursive: true });
    await mkdir(join(schemaRoot, 'VCM'), { recursive: true });
    await writeFile(
      join(schemaRoot, 'PNM', 'main.fbs'),
      'table PNM { FILE_ID:string; } root_type PNM;'
    );
    await writeFile(
      join(schemaRoot, 'VCM', 'main.fbs'),
      'table VCM { OBJECT_ID:string; } root_type VCM;'
    );

    const result = await runSmokeHarness({
      schemaRoot,
      outputDir,
      nodeCount: 1,
      recordsPerNode: 2,
      runId: 'schema-sweep-test',
    });
    const schemaSweepSchemas = result.metrics
      .filter((metric) => metric.useCase === 'schema-sweep')
      .map((metric) => metric.schema)
      .sort();

    expect(schemaSweepSchemas).toEqual(['PNM', 'VCM']);
    expect(schemaSweepSchemas).not.toContain('PublishEventRecord');
  });

  test('aggregates per-container worker artifacts into one fleet report', async () => {
    const schemaRoot = await mkdtemp(join(tmpdir(), 'flatsql-sds-aggregate-schema-'));
    const outputRoot = await mkdtemp(join(tmpdir(), 'flatsql-sds-aggregate-output-'));
    const worker0 = join(outputRoot, 'node-0');
    const worker1 = join(outputRoot, 'node-1');
    const aggregateOutput = join(outputRoot, 'aggregate');
    await mkdir(join(schemaRoot, 'PNM'), { recursive: true });
    await writeFile(
      join(schemaRoot, 'PNM', 'main.fbs'),
      'table PNM { FILE_ID:string; } root_type PNM;'
    );

    await runSmokeHarness({
      schemaRoot,
      outputDir: worker0,
      nodeCount: 1,
      nodeIdOffset: 0,
      nodeIsolation: 'container',
      transport: 'docker-compose',
      recordsPerNode: 2,
      runId: 'worker-0',
    });
    await runSmokeHarness({
      schemaRoot,
      outputDir: worker1,
      nodeCount: 1,
      nodeIdOffset: 1,
      nodeIsolation: 'container',
      transport: 'docker-compose',
      recordsPerNode: 2,
      runId: 'worker-1',
    });

    const result = await aggregateStressRunArtifacts({
      inputDirs: [worker0, worker1],
      outputDir: aggregateOutput,
      runId: 'fleet',
      mode: 'full',
      nodeStorageGb: 0.01,
      recordsPerNode: 2,
      nodeIsolation: 'container',
      transport: 'docker-compose',
    });

    expect(result.summary.nodes).toEqual([0, 1]);
    expect(result.manifest.nodeCount).toBe(2);
    expect(result.report.nodeIsolation).toBe('container');
    await expect(readFile(join(aggregateOutput, 'metrics.ndjson'), 'utf8')).resolves.toContain('"nodeId":1');
    await expect(readFile(join(aggregateOutput, 'derived-report.json'), 'utf8')).resolves.toContain('"nodeIsolation": "container"');
  });

  test('generates a full Docker topology with one container-isolated worker per node', () => {
    const compose = buildFullStressDockerCompose({
      projectRoot: '/repo/flatsql',
      schemaRootHost: '/repo/spacedatastandards.org/schema',
      outputDirHost: '/repo/flatsql/stress/results/full',
      runId: 'sds-full',
      nodeCount: 100,
      nodeStorageGb: 1,
      recordsPerNode: 2900000,
      batchBytes: 1048576,
      queryConcurrency: 64,
      hotQueryRatio: 0.01,
      runtime: 'standalone',
    });

    const workerServices = [...compose.matchAll(/^  worker-\d+:/gm)].map((match) => match[0].trim());

    expect(workerServices).toHaveLength(100);
    expect(workerServices[0]).toBe('worker-0:');
    expect(workerServices[99]).toBe('worker-99:');
    expect(compose).toContain('  aggregator:');
    expect(compose).toContain('      worker-99:');
    expect(compose).toContain('        condition: service_completed_successfully');
    expect(compose).toContain('      - stress/sds/aggregate.mjs');
    expect(compose).toContain('      - --node-isolation');
    expect(compose).toContain('      - container');
    expect(compose).toContain('      - --skip-production-gate');
    expect(compose).toContain('      - --node-id-offset');
    expect(compose).toContain('      - "42"');
    expect(compose).toContain('      - --nodes');
    expect(compose).toContain('      - "1"');
    expect(compose).toContain('      source: "/repo/flatsql/stress/results/full/nodes/node-42"');
  });

  test('measures real publish-event FlatSQL ingest, cache, query, and raw retrieval operations', async () => {
    const schemaRoot = await mkdtemp(join(tmpdir(), 'flatsql-sds-measured-schema-'));
    const outputDir = await mkdtemp(join(tmpdir(), 'flatsql-sds-measured-output-'));
    await mkdir(join(schemaRoot, 'PNM'), { recursive: true });
    await writeFile(
      join(schemaRoot, 'PNM', 'main.fbs'),
      'table PNM { FILE_ID:string; } root_type PNM;'
    );

    const result = await runSmokeHarness({
      schemaRoot,
      outputDir,
      nodeCount: 1,
      recordsPerNode: 16,
      runId: 'measured-test',
    });

    expect(new Set(result.metrics.map((metric) => metric.measurement))).toEqual(new Set(['measured']));
    expect(new Set(result.metrics.map((metric) => metric.runtime))).toEqual(new Set(['standalone']));
    expect(result.report.runtime).toEqual(['standalone']);
    expect(result.report.productionReadiness.failedRequirements.map((item) => item.id)).not.toContain('runtime');
    const ingest = result.metrics.find((metric) => metric.useCase === 'bulk-streaming-ingest');
    const hotCache = result.metrics.find((metric) => metric.useCase === 'hot-file-id-cache');
    const raw = result.metrics.find((metric) => metric.useCase === 'raw-flatbuffer-retrieval');
    const projection = result.metrics.find((metric) => metric.useCase === 'sql-projection-query');

    expect(ingest).toEqual(expect.objectContaining({
      operation: 'stream-ingest',
      records: 16,
      flatbufferBytes: expect.any(Number),
    }));
    expect(ingest!.durationMs).toBeGreaterThan(0);
    expect(hotCache!.cacheHits).toBeGreaterThan(0);
    expect(hotCache!.cacheMisses).toBe(1);
    expect(raw!.responseBytes).toBe(raw!.flatbufferBytes);
    expect(projection!.responseBytes).toBeLessThan(raw!.responseBytes);
    await expect(readFile(join(outputDir, 'metrics.ndjson'), 'utf8')).resolves.toContain('"measurement":"measured"');
  });

  test('bounds large result stress queries while staying above the native cache row cap', () => {
    const manifest = buildWorkloadManifest({
      runId: 'bounded-large-result',
      nodeCount: 1,
      recordsPerNode: 6000,
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);

    const metrics = buildMeasuredPublishEventMetrics(manifest);
    const largeResult = metrics.find((metric) => metric.useCase === 'large-result-query');

    expect(largeResult).toEqual(expect.objectContaining({
      records: 5000,
      errors: 0,
    }));
  });

  test('bounds cold FILE_ID fanout independently from storage-fill record count', () => {
    const manifest = buildWorkloadManifest({
      runId: 'bounded-cold-fanout',
      nodeCount: 1,
      recordsPerNode: 20000,
      queryConcurrency: 2,
      hotQueryRatio: 0.5,
    }, [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
    ]);

    const metrics = buildMeasuredPublishEventMetrics(manifest);
    const coldFanout = metrics.find((metric) => metric.useCase === 'cold-file-id-fanout');

    expect(coldFanout).toEqual(expect.objectContaining({
      records: 512,
      cacheMisses: 512,
      errors: 0,
    }));
  });

  test('treats FILE_ID indexing as SDN policy instead of a schema requirement', async () => {
    const schemaRoot = await mkdtemp(join(tmpdir(), 'flatsql-sds-policy-schema-'));
    const outputDir = await mkdtemp(join(tmpdir(), 'flatsql-sds-policy-output-'));
    await mkdir(join(schemaRoot, 'VCM'), { recursive: true });
    await writeFile(
      join(schemaRoot, 'VCM', 'main.fbs'),
      'table VCM { OBJECT_ID:string; } root_type VCM;'
    );

    const result = await runSmokeHarness({
      schemaRoot,
      outputDir,
      nodeCount: 1,
      recordsPerNode: 2,
      runId: 'policy-test',
    });

    expect(result.findings.indexingModel.publishEventKey).toBe('FILE_ID');
    expect(result.findings.indexingModel.perSchemaRulesOwner).toBe('space-data-network');
    expect(result.findings.schemaPolicy.databaseSpecificAnnotationsRequired).toBe(false);
    expect(result.findings.schemaPolicy.canonicalSchemasRemainDatabaseNeutral).toBe(true);
    expect(result.findings).not.toHaveProperty('schemasWithoutFileIdentifier');
    expect(result.findings).not.toHaveProperty('schemasWithoutIndexedFields');
    await expect(readFile(join(outputDir, 'findings.json'), 'utf8')).resolves.toContain('"publishEventKey": "FILE_ID"');
  });
});
