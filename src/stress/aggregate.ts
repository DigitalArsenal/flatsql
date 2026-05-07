import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { reduceMetricEvents } from './metrics.js';
import { buildDerivedStressReport } from './report.js';
import type {
  MetricEvent,
  SmokeHarnessFindings,
  SmokeHarnessResult,
  StressAggregateOptions,
  WorkloadManifest,
} from './types.js';

async function readJson<T>(path: string): Promise<T> {
  return JSON.parse(await readFile(path, 'utf8')) as T;
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

function parseMetrics(text: string): MetricEvent[] {
  return text
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as MetricEvent);
}

export async function aggregateStressRunArtifacts(options: StressAggregateOptions): Promise<SmokeHarnessResult> {
  if (options.inputDirs.length === 0) {
    throw new Error('At least one stress result input directory is required.');
  }

  const manifests = await Promise.all(
    options.inputDirs.map((dir) => readJson<WorkloadManifest>(join(dir, 'manifest.json')))
  );
  const metrics = (
    await Promise.all(
      options.inputDirs.map(async (dir) => parseMetrics(await readFile(join(dir, 'metrics.ndjson'), 'utf8')))
    )
  ).flat();
  const findings = await readJson<SmokeHarnessFindings>(join(options.inputDirs[0], 'findings.json'));
  const firstManifest = manifests[0];
  const nodeIds = [...new Set(metrics.map((metric) => metric.nodeId))].sort((left, right) => left - right);

  const manifest: WorkloadManifest = {
    ...firstManifest,
    mode: options.mode,
    runId: options.runId,
    nodeCount: nodeIds.length,
    nodeIdOffset: nodeIds[0] ?? 0,
    nodeStorageGb: options.nodeStorageGb,
    recordsPerNode: options.recordsPerNode,
    transport: options.transport,
    nodeIsolation: options.nodeIsolation,
    createdAt: new Date().toISOString(),
    assignments: manifests.flatMap((item) => item.assignments),
  };
  const normalizedMetrics = metrics.map((metric) => ({
    ...metric,
    runId: options.runId,
    transport: options.transport,
  }));
  const summary = reduceMetricEvents(normalizedMetrics);
  const report = buildDerivedStressReport(manifest, normalizedMetrics, summary);

  await mkdir(options.outputDir, { recursive: true });
  await writeJson(join(options.outputDir, 'manifest.json'), manifest);
  await writeFile(
    join(options.outputDir, 'metrics.ndjson'),
    normalizedMetrics.map((metric) => JSON.stringify(metric)).join('\n') + '\n'
  );
  await writeJson(join(options.outputDir, 'summary.json'), summary);
  await writeJson(join(options.outputDir, 'derived-report.json'), report);
  await writeJson(join(options.outputDir, 'findings.json'), findings);

  return {
    manifest,
    metrics: normalizedMetrics,
    summary,
    findings,
    report,
  };
}
