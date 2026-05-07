#!/usr/bin/env node

import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { buildFullStressDockerCompose } from '../../dist/stress/index.js';

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
  console.log(`Usage: node stress/sds/write-compose.mjs [options]

Options:
  --output-file <path>
  --project-root <path>
  --schema-root-host <path>
  --output-dir-host <path>
  --run-id <id>
  --mode smoke|full
  --nodes <count>
  --storage-gb <gb>
  --records-per-node <count>
  --batch-bytes <bytes>
  --query-concurrency <count>
  --hot-query-ratio <ratio>
  --runtime standalone|wasmedge|native
`);
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes('--help') || argv.includes('-h')) {
    printHelp();
    return;
  }

  const outputFile = resolve(readOption(argv, 'output-file', 'stress/results/docker-compose.full.yml'));
  const projectRoot = resolve(readOption(argv, 'project-root', process.cwd()));
  const schemaRootHost = resolve(readOption(argv, 'schema-root-host', '../spacedatastandards.org/schema'));
  const outputDirHost = resolve(readOption(argv, 'output-dir-host', 'stress/results/full'));
  const nodeCount = readNumber(argv, 'nodes', process.env.NODE_COUNT) ?? 100;

  const compose = buildFullStressDockerCompose({
    projectRoot,
    schemaRootHost,
    outputDirHost,
    runId: readOption(argv, 'run-id', process.env.RUN_ID ?? `sds-full-${Date.now()}`),
    mode: readOption(argv, 'mode', process.env.STRESS_MODE ?? 'full'),
    nodeCount,
    nodeStorageGb: readNumber(argv, 'storage-gb', process.env.NODE_STORAGE_GB) ?? 1,
    recordsPerNode: readNumber(argv, 'records-per-node', process.env.RECORDS_PER_NODE) ?? 2900000,
    batchBytes: readNumber(argv, 'batch-bytes', process.env.BATCH_BYTES) ?? 1048576,
    queryConcurrency: readNumber(argv, 'query-concurrency', process.env.QUERY_CONCURRENCY) ?? 64,
    hotQueryRatio: readNumber(argv, 'hot-query-ratio', process.env.HOT_QUERY_RATIO) ?? 0.01,
    runtime: readOption(argv, 'runtime', process.env.RUNTIME ?? 'standalone'),
  });

  await mkdir(dirname(outputFile), { recursive: true });
  await mkdir(outputDirHost, { recursive: true });
  await mkdir(`${outputDirHost}/nodes`, { recursive: true });
  await mkdir(`${outputDirHost}/aggregate`, { recursive: true });
  await Promise.all(
    Array.from({ length: nodeCount }, (_, nodeId) => mkdir(`${outputDirHost}/nodes/node-${nodeId}`, { recursive: true }))
  );
  await writeFile(outputFile, compose);
  console.log(outputFile);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
