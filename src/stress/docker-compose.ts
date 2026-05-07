import type { StressMode, StressRuntime } from './types.js';

export interface FullStressDockerComposeOptions {
  projectRoot: string;
  schemaRootHost: string;
  outputDirHost: string;
  runId: string;
  mode?: StressMode;
  nodeCount: number;
  nodeStorageGb: number;
  recordsPerNode: number;
  batchBytes: number;
  queryConcurrency: number;
  hotQueryRatio: number;
  runtime: StressRuntime;
}

function quote(value: string | number): string {
  return JSON.stringify(String(value));
}

function commandScalar(value: string | number): string {
  const text = String(value);
  if (/^-?\d+(?:\.\d+)?$/.test(text)) {
    return quote(text);
  }
  if (/^[A-Za-z0-9_.:/-]+$/.test(text)) {
    return text;
  }
  return quote(text);
}

function hostPath(root: string, ...segments: string[]): string {
  return [root.replace(/\/+$/, ''), ...segments].join('/');
}

function commandLines(args: readonly (string | number)[]): string[] {
  return ['    command:', ...args.map((arg) => `      - ${commandScalar(arg)}`)];
}

function stressMode(options: FullStressDockerComposeOptions): StressMode {
  return options.mode ?? 'full';
}

function commonEnvironmentLines(options: FullStressDockerComposeOptions, runId: string): string[] {
  return [
    '    environment:',
    `      STRESS_MODE: ${commandScalar(stressMode(options))}`,
    `      RUN_ID: ${commandScalar(runId)}`,
    `      NODE_STORAGE_GB: ${quote(options.nodeStorageGb)}`,
    `      RECORDS_PER_NODE: ${quote(options.recordsPerNode)}`,
    `      BATCH_BYTES: ${quote(options.batchBytes)}`,
    `      QUERY_CONCURRENCY: ${quote(options.queryConcurrency)}`,
    `      HOT_QUERY_RATIO: ${quote(options.hotQueryRatio)}`,
    '      SDS_SCHEMA_ROOT: /sds/schema',
    `      RUNTIME: ${commandScalar(options.runtime)}`,
    '      NODE_ISOLATION: container',
    '      STRESS_TRANSPORT: docker-compose',
  ];
}

function schemaVolumeLines(schemaRootHost: string): string[] {
  return [
    '      - type: bind',
    `        source: ${quote(schemaRootHost)}`,
    '        target: /sds/schema',
    '        read_only: true',
  ];
}

function outputVolumeLines(outputDirHost: string, target = '/stress-results'): string[] {
  return [
    '      - type: bind',
    `        source: ${quote(outputDirHost)}`,
    `        target: ${target}`,
  ];
}

function workerServiceLines(options: FullStressDockerComposeOptions, nodeId: number): string[] {
  const workerRunId = `${options.runId}-node-${nodeId}`;
  const outputDir = hostPath(options.outputDirHost, 'nodes', `node-${nodeId}`);
  return [
    `  worker-${nodeId}:`,
    '    build: *flatsql-build',
    '    restart: "no"',
    ...commonEnvironmentLines(options, workerRunId),
    '      NODE_COUNT: "1"',
    `      NODE_ID_OFFSET: ${quote(nodeId)}`,
    '      OUTPUT_DIR: /stress-results',
    '    volumes:',
    ...schemaVolumeLines(options.schemaRootHost),
    ...outputVolumeLines(outputDir),
    ...commandLines([
      'node',
      'stress/sds/run.mjs',
      '--mode',
      stressMode(options),
      '--schema-root',
      '/sds/schema',
      '--output-dir',
      '/stress-results',
      '--nodes',
      1,
      '--node-id-offset',
      nodeId,
      '--storage-gb',
      options.nodeStorageGb,
      '--records-per-node',
      options.recordsPerNode,
      '--batch-bytes',
      options.batchBytes,
      '--query-concurrency',
      options.queryConcurrency,
      '--hot-query-ratio',
      options.hotQueryRatio,
      '--run-id',
      workerRunId,
      '--runtime',
      options.runtime,
      '--node-isolation',
      'container',
      '--transport',
      'docker-compose',
      '--skip-production-gate',
    ]),
  ];
}

function aggregatorServiceLines(options: FullStressDockerComposeOptions): string[] {
  return [
    '  aggregator:',
    '    build: *flatsql-build',
    '    restart: "no"',
    '    depends_on:',
    ...Array.from({ length: options.nodeCount }, (_, nodeId) => [
      `      worker-${nodeId}:`,
      '        condition: service_completed_successfully',
    ]).flat(),
    ...commonEnvironmentLines(options, options.runId),
    `      NODE_COUNT: ${quote(options.nodeCount)}`,
    '      OUTPUT_DIR: /stress-results/aggregate',
    '    volumes:',
    ...outputVolumeLines(options.outputDirHost),
    ...commandLines([
      'node',
      'stress/sds/aggregate.mjs',
      '--input-dir',
      '/stress-results/nodes',
      '--output-dir',
      '/stress-results/aggregate',
      '--run-id',
      options.runId,
      '--mode',
      stressMode(options),
      '--node-storage-gb',
      options.nodeStorageGb,
      '--records-per-node',
      options.recordsPerNode,
      '--transport',
      'docker-compose',
      '--node-isolation',
      'container',
    ]),
  ];
}

export function buildFullStressDockerCompose(options: FullStressDockerComposeOptions): string {
  if (!Number.isInteger(options.nodeCount) || options.nodeCount <= 0) {
    throw new Error(`nodeCount must be a positive integer; received ${options.nodeCount}`);
  }

  const lines = [
    'name: flatsql-sds-stress',
    'x-flatsql-build: &flatsql-build',
    `  context: ${quote(options.projectRoot)}`,
    '  dockerfile: stress/docker/Dockerfile',
    'services:',
    ...Array.from({ length: options.nodeCount }, (_, nodeId) => workerServiceLines(options, nodeId)).flat(),
    ...aggregatorServiceLines(options),
    '',
  ];
  return lines.join('\n');
}
