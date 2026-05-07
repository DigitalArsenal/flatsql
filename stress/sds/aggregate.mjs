#!/usr/bin/env node

import { readdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { aggregateStressRunArtifacts, assertProductionReadiness } from '../../dist/stress/index.js';

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
  console.log(`Usage: node stress/sds/aggregate.mjs [options]

Options:
  --input-dir <path>         Directory containing node result subdirectories
  --output-dir <path>        Directory for aggregate result artifacts
  --run-id <id>
  --mode smoke|full
  --node-storage-gb <gb>
  --records-per-node <count>
  --transport in-process|docker-compose
  --node-isolation logical|container
`);
}

async function listInputDirs(inputRoot) {
  const entries = await readdir(inputRoot, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => join(inputRoot, entry.name))
    .sort();
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes('--help') || argv.includes('-h')) {
    printHelp();
    return;
  }

  const inputRoot = resolve(readOption(argv, 'input-dir', 'stress/results/nodes'));
  const outputDir = resolve(readOption(argv, 'output-dir', 'stress/results/aggregate'));
  const runId = readOption(argv, 'run-id', process.env.RUN_ID ?? `sds-aggregate-${Date.now()}`);
  const mode = readOption(argv, 'mode', process.env.STRESS_MODE ?? 'smoke');
  const inputDirs = await listInputDirs(inputRoot);

  const result = await aggregateStressRunArtifacts({
    inputDirs,
    outputDir,
    runId,
    mode,
    nodeStorageGb: readNumber(argv, 'node-storage-gb', process.env.NODE_STORAGE_GB) ?? 1,
    recordsPerNode: readNumber(argv, 'records-per-node', process.env.RECORDS_PER_NODE) ?? 0,
    transport: readOption(argv, 'transport', process.env.STRESS_TRANSPORT ?? 'docker-compose'),
    nodeIsolation: readOption(argv, 'node-isolation', process.env.NODE_ISOLATION ?? 'container'),
  });

  console.log(`FlatSQL SDS aggregate ${result.manifest.mode} run: ${result.manifest.runId}`);
  console.log(`Nodes: ${result.summary.nodes.length}`);
  console.log(`Result directory: ${outputDir}`);
  console.log(`Events: ${result.summary.eventCount}`);
  console.log(`Request bytes: ${result.summary.totalRequestBytes}`);
  console.log(`Response bytes: ${result.summary.totalResponseBytes}`);
  console.log(`Cache hits/misses: ${result.summary.totalCacheHits}/${result.summary.totalCacheMisses}`);

  if (result.manifest.mode === 'full') {
    assertProductionReadiness(result.report);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
