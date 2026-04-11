# Remote Artifact Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a worker-backed artifact builder that persists SQLite index pages to disk while preserving the current default FlatSQL behavior.

**Architecture:** Implement the artifact path on the TypeScript/Node side using `node:sqlite`, add a dedicated artifact-builder wrapper plus worker protocol, and verify artifact persistence by reopening and querying the generated SQLite index tables.

**Tech Stack:** TypeScript, Node `worker_threads`, Node `node:sqlite`, Jest

---

### Task 1: Add Direct Artifact Builder

**Files:**
- Create: `src/artifacts/builder.ts`
- Create: `src/artifacts/demo-extractors.ts`
- Create: `src/artifacts/types.ts`
- Create: `src/artifacts/index.ts`
- Modify: `src/index.ts`
- Test: `test/remote-artifact.test.ts`

- [ ] **Step 1: Write the failing API test**

```ts
test('createArtifactBuilder persists index tables to sqlitePath', async () => {
  expect(true).toBe(false);
});
```

- [ ] **Step 2: Run the targeted test and verify it fails**

Run: `npm test -- --runInBand --runTestsByPath test/remote-artifact.test.ts`
Expected: FAIL because the new artifact API does not exist yet

- [ ] **Step 3: Add a direct artifact builder**

Implement `FlatSQLArtifactBuilder.fromSchema(schema, { sqlitePath })` using `node:sqlite`.

- [ ] **Step 4: Re-run the targeted test**

Run: `npm test -- --runInBand --runTestsByPath test/remote-artifact.test.ts`
Expected: PASS for the direct artifact build case and FAIL later in the flow because the worker path is still missing

### Task 2: Add Worker Artifact Client And Protocol

**Files:**
- Create: `wasm/flatsql-artifact.worker.js`
- Create: `wasm/flatsql-artifact-client.js`
- Modify: `wasm/index.d.ts`
- Test: `test/remote-artifact.test.ts`

- [ ] **Step 1: Add a failing worker test**

```ts
test('artifact worker builds sqlite artifact via worker thread', async () => {
  expect(true).toBe(false);
});
```

- [ ] **Step 2: Run the targeted test and verify it fails**

Run: `npm test -- --runInBand --runTestsByPath test/remote-artifact.test.ts`
Expected: FAIL because no artifact worker client exists

- [ ] **Step 3: Implement the worker and client**

Implement a dedicated worker protocol with methods for:
- `init`
- `createArtifactBuilder`
- `registerFileId`
- `enableDemoExtractors`
- `ingestBuffers`
- `query`
- `destroy`

- [ ] **Step 4: Re-run the targeted test**

Run: `npm test -- --runInBand --runTestsByPath test/remote-artifact.test.ts`
Expected: PASS for worker clone transport

### Task 3: Add SharedArrayBuffer Transport

**Files:**
- Modify: `wasm/flatsql-artifact.worker.js`
- Modify: `wasm/flatsql-artifact-client.js`
- Test: `test/remote-artifact.test.ts`

- [ ] **Step 1: Add a failing transport-mode test**

```ts
test('artifact worker uses shared-array-buffer transport when supported', async () => {
  expect(true).toBe(false);
});
```

- [ ] **Step 2: Run the targeted test and verify it fails**

Run: `npm test -- --runInBand --runTestsByPath test/remote-artifact.test.ts`
Expected: FAIL because transport mode reporting/shared buffer ingest is not implemented

- [ ] **Step 3: Implement batch shared-buffer transport**

Implement:
- worker capability detection
- a shared batch buffer write path on the client
- a worker read path that reconstructs the ingest stream from the shared buffer
- transport mode reporting (`shared-array-buffer` or `clone`)

- [ ] **Step 4: Re-run the targeted test**

Run: `npm test -- --runInBand --runTestsByPath test/remote-artifact.test.ts`
Expected: PASS for SAB transport in Node

### Task 4: Verify Regressions

**Files:**
- Test: `test/remote-artifact.test.ts`

- [ ] **Step 1: Run focused verification for the new feature**

Run: `npm test -- --runInBand --runTestsByPath test/remote-artifact.test.ts`
Expected: PASS

- [ ] **Step 2: Run unaffected existing suites**

Run: `npm test -- --runInBand --runTestsByPath test/basic.test.ts test/schema-joins.test.ts test/cluster-mode.test.ts test/wasi.test.ts`
Expected: PASS

- [ ] **Step 3: Commit the feature branch changes**

```bash
git add src/artifacts src/index.ts wasm/flatsql-artifact.worker.js test/remote-artifact.test.ts docs/superpowers/specs/2026-04-11-remote-artifact-worker-design.md docs/superpowers/plans/2026-04-11-remote-artifact-worker.md
git commit -m "feat: add worker-backed remote artifact builder"
```
