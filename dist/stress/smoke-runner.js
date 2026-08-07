import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { discoverSdsSchemas } from './sds-discovery.js';
import { reduceMetricEvents } from './metrics.js';
import { buildMeasuredPublishEventMetrics } from './publish-event-workload.js';
import { buildMeasuredStandalonePublishEventMetrics } from './native-publish-event-workload.js';
import { buildDerivedStressReport } from './report.js';
import { buildWorkloadManifest } from './workload.js';
function detectFindings(manifest) {
    return {
        indexingModel: {
            publishEventKey: 'FILE_ID',
            publishEventScope: 'Space Data Network publish event partition containing all records returned for that event.',
            perSchemaRulesOwner: 'space-data-network',
            perSchemaRulesEnforcedInSchema: false,
        },
        schemaPolicy: {
            canonicalSchemasRemainDatabaseNeutral: true,
            databaseSpecificAnnotationsRequired: false,
        },
        externalIndexProfileCoverage: {
            loadedProfiles: 0,
            schemaCount: manifest.schemaCount,
            status: 'External per-schema SDN index profiles are not loaded by this harness slice.',
        },
        expectedPressurePoints: [
            'Native query result cache defaults to 1024 entries and 1000 rows per result; production FILE_ID profiles should tune maxEntries and maxRows for hot/cold traffic shape.',
            'Large FILE_ID responses still need paging, raw streaming, or response artifacts rather than assuming every SQL result should stay resident in process memory.',
            'Per-schema query speed depends on external SDN index profiles, not database-specific annotations in canonical schemas.',
            'Cache hits reduce native query work but still return response bytes unless callers use narrower projections, raw binary paths, compression, or edge-local aggregation.',
        ],
    };
}
async function writeJson(path, value) {
    await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
export async function runSmokeHarness(options) {
    const schemas = await discoverSdsSchemas(options.schemaRoot);
    if (schemas.length === 0) {
        throw new Error(`No SDS schemas found under ${options.schemaRoot}`);
    }
    const manifest = buildWorkloadManifest({ ...options, mode: options.mode ?? 'smoke' }, schemas);
    const metrics = manifest.runtime === 'standalone'
        ? await buildMeasuredStandalonePublishEventMetrics(manifest)
        : buildMeasuredPublishEventMetrics(manifest);
    const summary = reduceMetricEvents(metrics);
    const report = buildDerivedStressReport(manifest, metrics, summary);
    const findings = detectFindings(manifest);
    await mkdir(options.outputDir, { recursive: true });
    await writeJson(join(options.outputDir, 'manifest.json'), manifest);
    await writeFile(join(options.outputDir, 'metrics.ndjson'), metrics.map((metric) => JSON.stringify(metric)).join('\n') + '\n');
    await writeJson(join(options.outputDir, 'summary.json'), summary);
    await writeJson(join(options.outputDir, 'derived-report.json'), report);
    await writeJson(join(options.outputDir, 'findings.json'), findings);
    return {
        manifest,
        metrics,
        summary,
        findings,
        report,
    };
}
//# sourceMappingURL=smoke-runner.js.map