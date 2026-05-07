# Docker SDS Stress Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Docker-oriented Space Data Standards stress harness with local smoke execution, use-case manifests, transport metrics, and ignored result artifacts.

**Architecture:** Put reusable harness logic in `src/stress/*` so it is build-checked and testable. Keep Docker and shell wrappers thin; they call the compiled CLI path and do not own benchmark semantics. The first implementation slice discovers SDS schemas, builds the 100-node/1GB workload manifest, emits representative smoke metrics for the full use-case matrix, reduces results, and records run artifacts for later real ingest/transport expansion.

**Tech Stack:** TypeScript, Node.js ESM, Jest/ts-jest, Docker Compose, shell script entry point.

---

### Task 1: Stress Harness Tests

**Files:**
- Create: `test/stress-harness.test.ts`

- [x] **Step 1: Write failing tests for schema discovery, manifest generation, metrics reduction, and smoke result writing**

```ts
import { mkdtemp, readFile, writeFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { discoverSdsSchemas, parseSchemaMetadata } from '../src/stress/sds-discovery.js';
import { buildWorkloadManifest, DEFAULT_USE_CASES } from '../src/stress/workload.js';
import { reduceMetricEvents } from '../src/stress/metrics.js';
import { runSmokeHarness } from '../src/stress/smoke-runner.js';

describe('SDS stress harness', () => {
  test('discovers SDS schemas and extracts root metadata', async () => {
    const root = await mkdtemp(join(tmpdir(), 'flatsql-sds-fixture-'));
    await mkdir(join(root, 'PNM'), { recursive: true });
    await writeFile(join(root, 'PNM', 'main.fbs'), 'table PNM { FILE_ID:string; } root_type PNM;');

    const schemas = await discoverSdsSchemas(root);

    expect(schemas).toEqual([
      expect.objectContaining({ name: 'PNM', rootType: 'PNM', tableNames: ['PNM'] }),
    ]);
  });

  test('builds a 100 node manifest with all transport use cases', async () => {
    const schemas = [
      parseSchemaMetadata('PNM', 'table PNM { FILE_ID:string; } root_type PNM;', '/schema/PNM/main.fbs'),
      parseSchemaMetadata('DPM', 'table DPM { FILE_ID:string; } root_type DPM;', '/schema/DPM/main.fbs'),
    ];

    const manifest = buildWorkloadManifest({ mode: 'full', nodeCount: 100, nodeStorageGb: 1 }, schemas);

    expect(manifest.nodeCount).toBe(100);
    expect(manifest.nodeStorageGb).toBe(1);
    expect(manifest.useCases.map((useCase) => useCase.id)).toEqual(DEFAULT_USE_CASES.map((useCase) => useCase.id));
    expect(manifest.assignments).toHaveLength(100);
    expect(manifest.assignments[0].schemas.length).toBeGreaterThan(0);
  });

  test('reduces metrics with transport byte totals and latency percentiles', () => {
    const summary = reduceMetricEvents([
      { runId: 'r', nodeId: 0, schema: 'PNM', useCase: 'hot-file-id-cache', operation: 'query-template', records: 1, requestBytes: 20, responseBytes: 100, flatbufferBytes: 80, storageBytes: 1000, durationMs: 10, cacheHits: 0, cacheMisses: 1, cacheSize: 1, errors: 0 },
      { runId: 'r', nodeId: 0, schema: 'PNM', useCase: 'hot-file-id-cache', operation: 'query-template', records: 1, requestBytes: 20, responseBytes: 100, flatbufferBytes: 80, storageBytes: 1000, durationMs: 30, cacheHits: 1, cacheMisses: 0, cacheSize: 1, errors: 0 },
    ]);

    expect(summary.totalRequestBytes).toBe(40);
    expect(summary.totalResponseBytes).toBe(200);
    expect(summary.totalCacheHits).toBe(1);
    expect(summary.useCases['hot-file-id-cache'].latencyP95Ms).toBe(30);
  });

  test('writes smoke run artifacts into the requested output directory', async () => {
    const schemaRoot = await mkdtemp(join(tmpdir(), 'flatsql-sds-smoke-schema-'));
    const outputDir = await mkdtemp(join(tmpdir(), 'flatsql-sds-smoke-output-'));
    await mkdir(join(schemaRoot, 'PNM'), { recursive: true });
    await writeFile(join(schemaRoot, 'PNM', 'main.fbs'), 'table PNM { FILE_ID:string; } root_type PNM;');

    const result = await runSmokeHarness({ schemaRoot, outputDir, nodeCount: 2, recordsPerNode: 10, runId: 'smoke-test' });

    expect(result.summary.totalRecords).toBeGreaterThan(0);
    await expect(readFile(join(outputDir, 'manifest.json'), 'utf8')).resolves.toContain('"nodeCount": 2');
    await expect(readFile(join(outputDir, 'metrics.ndjson'), 'utf8')).resolves.toContain('hot-file-id-cache');
    await expect(readFile(join(outputDir, 'summary.json'), 'utf8')).resolves.toContain('"totalRequestBytes"');
  });
});
```

- [x] **Step 2: Run tests to verify they fail before implementation**

Run: `npm test -- --runInBand --runTestsByPath test/stress-harness.test.ts`
Expected: FAIL because `src/stress/*` modules do not exist.

### Task 2: Core Harness Modules

**Files:**
- Create: `src/stress/types.ts`
- Create: `src/stress/sds-discovery.ts`
- Create: `src/stress/workload.ts`
- Create: `src/stress/metrics.ts`
- Create: `src/stress/smoke-runner.ts`
- Create: `src/stress/index.ts`

- [x] **Step 1: Implement typed harness contracts**

Define schema metadata, workload manifest, metric events, summary records, and smoke-run options in `src/stress/types.ts`.

- [x] **Step 2: Implement SDS discovery**

Read `*/main.fbs` under a schema root and extract schema name, root type, and table names with deterministic sorting. Do not require or report database-specific annotations in canonical schemas.

- [x] **Step 3: Implement workload manifest generation**

Generate default use cases, node assignments, 100-node full defaults, smoke defaults, and storage-budget settings.

- [x] **Step 4: Implement metrics reduction**

Aggregate request bytes, response bytes, FlatBuffer bytes, storage bytes, cache hits, cache misses, errors, and per-use-case latency percentiles.

- [x] **Step 5: Implement smoke runner**

Discover schemas, build a smoke manifest, emit representative metrics for all use cases, and write `manifest.json`, `metrics.ndjson`, `summary.json`, and `findings.json`.

- [x] **Step 6: Run tests to verify they pass**

Run: `npm test -- --runInBand --runTestsByPath test/stress-harness.test.ts`
Expected: PASS.

### Task 3: CLI, Docker, and Ignored Result Directory

**Files:**
- Create: `stress/sds/run.mjs`
- Create: `stress/docker/Dockerfile`
- Create: `stress/docker/docker-compose.yml`
- Create: `scripts/stress-docker.sh`
- Modify: `.gitignore`
- Modify: `package.json`

- [x] **Step 1: Add CLI wrapper**

Create `stress/sds/run.mjs` that imports `dist/stress/index.js`, accepts `--mode`, `--schema-root`, `--output-dir`, `--nodes`, `--storage-gb`, `--records-per-node`, and `--run-id`, then runs the smoke harness.

- [x] **Step 2: Add Docker assets**

Create a Dockerfile that installs dependencies, builds the package, and runs the CLI. Create a Compose file with a controller service suitable for smoke mode.

- [x] **Step 3: Add shell entry point**

Create `scripts/stress-docker.sh` with `smoke`, `docker-smoke`, and `full` commands. `smoke` runs locally; `docker-smoke` requires Docker; `full` requires explicit `CONFIRM_FULL_STRESS=1`.

- [x] **Step 4: Ignore result artifacts**

Add `stress/results/` to `.gitignore`.

- [x] **Step 5: Add package scripts**

Add `stress:sds:smoke` and `stress:sds:docker-smoke`.

### Task 4: Execute Smoke Run and Store Results

**Files:**
- Output: `stress/results/<run-id>/manifest.json`
- Output: `stress/results/<run-id>/metrics.ndjson`
- Output: `stress/results/<run-id>/summary.json`
- Output: `stress/results/<run-id>/findings.json`

- [x] **Step 1: Build the project**

Run: `npm run build`
Expected: PASS.

- [x] **Step 2: Run local smoke harness**

Run: `npm run stress:sds:smoke -- --schema-root ../spacedatastandards.org/schema --output-dir stress/results/<run-id> --nodes 2 --records-per-node 25 --run-id <run-id>`
Expected: PASS and writes all result files.

- [x] **Step 3: Attempt Docker smoke when Docker daemon is available**

Run: `npm run stress:sds:docker-smoke -- --schema-root ../spacedatastandards.org/schema --output-dir stress/results/<run-id>-docker --nodes 2 --records-per-node 10 --run-id <run-id>-docker`
Expected: PASS when Docker is running; clear failure recorded when Docker daemon is unavailable.

### Task 5: Verification and Reporting

**Files:**
- Modify: `docs/superpowers/plans/2026-05-07-docker-sds-stress-harness.md`

- [x] **Step 1: Run targeted tests**

Run: `npm test -- --runInBand --runTestsByPath test/stress-harness.test.ts`
Expected: PASS.

- [x] **Step 2: Run build**

Run: `npm run build`
Expected: PASS.

- [x] **Step 3: Check whitespace**

Run: `git diff --check`
Expected: PASS.

- [x] **Step 4: Audit objective coverage**

Confirm evidence for implementation plan, execution, ignored result storage, and result report before final response.
