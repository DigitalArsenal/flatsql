#!/usr/bin/env node

import { resolve } from 'node:path';
import { assertProductionReadiness, runSmokeHarness } from '../../dist/stress/index.js';

function readOption(argv, name, defaultValue = undefined) {
  const prefix = `--${name}=`;
  let value = defaultValue;
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === `--${name}`) {
      value = argv[index + 1];
      index++;
    } else if (arg.startsWith(prefix)) {
      value = arg.slice(prefix.length);
    }
  }
  return value;
}

function readNumber(argv, name, defaultValue = undefined) {
  const value = readOption(argv, name, defaultValue === undefined ? undefined : String(defaultValue));
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric value for --${name}: ${value}`);
  }
  return parsed;
}

function printHelp() {
  console.log(`Usage: node stress/sds/run.mjs [options]

Options:
  --mode smoke|full
  --schema-root <path>
  --output-dir <path>
  --nodes <count>
  --node-id-offset <count>
  --storage-gb <gb>
  --records-per-node <count>
  --batch-bytes <bytes>
  --query-concurrency <count>
  --hot-query-ratio <ratio>
  --runtime standalone|wasmedge|native
  --transport in-process|docker-compose
  --node-isolation logical|container
  --skip-production-gate
  --run-id <id>
`);
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes('--help') || argv.includes('-h')) {
    printHelp();
    return;
  }

  const mode = readOption(argv, 'mode', process.env.STRESS_MODE ?? 'smoke');
  const runId = readOption(argv, 'run-id', process.env.RUN_ID ?? `sds-${mode}-${Date.now()}`);
  const schemaRoot = resolve(
    readOption(argv, 'schema-root', process.env.SDS_SCHEMA_ROOT ?? '../spacedatastandards.org/schema')
  );
  const outputDir = resolve(
    readOption(argv, 'output-dir', process.env.OUTPUT_DIR ?? `stress/results/${runId}`)
  );

  const result = await runSmokeHarness({
    mode,
    runId,
    schemaRoot,
    outputDir,
    nodeCount: readNumber(argv, 'nodes', process.env.NODE_COUNT),
    nodeIdOffset: readNumber(argv, 'node-id-offset', process.env.NODE_ID_OFFSET),
    nodeStorageGb: readNumber(argv, 'storage-gb', process.env.NODE_STORAGE_GB),
    recordsPerNode: readNumber(argv, 'records-per-node', process.env.RECORDS_PER_NODE),
    batchBytes: readNumber(argv, 'batch-bytes', process.env.BATCH_BYTES),
    queryConcurrency: readNumber(argv, 'query-concurrency', process.env.QUERY_CONCURRENCY),
    hotQueryRatio: readNumber(argv, 'hot-query-ratio', process.env.HOT_QUERY_RATIO),
    runtime: readOption(argv, 'runtime', process.env.RUNTIME),
    transport: readOption(argv, 'transport', process.env.STRESS_TRANSPORT),
    nodeIsolation: readOption(argv, 'node-isolation', process.env.NODE_ISOLATION),
  });

  console.log(`FlatSQL SDS stress ${result.manifest.mode} run: ${result.manifest.runId}`);
  console.log(`Schemas: ${result.manifest.schemaCount}`);
  console.log(`Nodes: ${result.manifest.nodeCount}`);
  console.log(`Result directory: ${outputDir}`);
  console.log(`Events: ${result.summary.eventCount}`);
  console.log(`Request bytes: ${result.summary.totalRequestBytes}`);
  console.log(`Response bytes: ${result.summary.totalResponseBytes}`);
  console.log(`Cache hits/misses: ${result.summary.totalCacheHits}/${result.summary.totalCacheMisses}`);

  if (result.manifest.mode === 'full' && !argv.includes('--skip-production-gate')) {
    assertProductionReadiness(result.report);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
