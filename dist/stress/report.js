const BYTES_PER_MIB = 1024 * 1024;
const BYTES_PER_GIB = 1024 * BYTES_PER_MIB;
const MIN_PRODUCTION_STORAGE_FILL_PCT = 95;
const MIN_PRODUCTION_FPS = 75;
function round(value, decimals = 3) {
    const factor = 10 ** decimals;
    return Math.round(value * factor) / factor;
}
function bytesToMiB(value) {
    return round(value / BYTES_PER_MIB);
}
function rateMiBps(bytes, durationMs) {
    if (durationMs <= 0) {
        return 0;
    }
    return round((bytes / BYTES_PER_MIB) / (durationMs / 1000));
}
function pct(value, total, decimals = 4) {
    if (total <= 0) {
        return 0;
    }
    return round((value / total) * 100, decimals);
}
function requirement(id, passed, required, actual) {
    return {
        id,
        status: passed ? 'pass' : 'fail',
        required,
        actual,
    };
}
export function buildDerivedStressReport(manifest, metrics, summary) {
    const targetNodeStorageBytes = manifest.nodeStorageGb * BYTES_PER_GIB;
    const perNode = summary.nodes.map((nodeId) => {
        const nodeMetrics = metrics.filter((metric) => metric.nodeId === nodeId);
        const ingestMetrics = nodeMetrics.filter((metric) => metric.useCase === 'bulk-streaming-ingest');
        const requestBytes = nodeMetrics.reduce((total, metric) => total + metric.requestBytes, 0);
        const responseBytes = nodeMetrics.reduce((total, metric) => total + metric.responseBytes, 0);
        const flatbufferBytes = nodeMetrics.reduce((total, metric) => total + metric.flatbufferBytes, 0);
        const durationMs = nodeMetrics.reduce((total, metric) => total + metric.durationMs, 0);
        const ingestDurationMs = ingestMetrics.reduce((total, metric) => total + metric.durationMs, 0);
        const ingestRecords = ingestMetrics.reduce((total, metric) => total + metric.records, 0);
        const maxStorageBytes = nodeMetrics.reduce((max, metric) => Math.max(max, metric.storageBytes), 0);
        return {
            nodeId,
            requestMiB: bytesToMiB(requestBytes),
            responseMiB: bytesToMiB(responseBytes),
            flatbufferMiB: bytesToMiB(flatbufferBytes),
            maxStorageMiB: bytesToMiB(maxStorageBytes),
            storageFillPct: pct(maxStorageBytes, targetNodeStorageBytes),
            ingressMiBps: rateMiBps(requestBytes, durationMs),
            egressMiBps: rateMiBps(responseBytes, durationMs),
            storageFillMiBps: rateMiBps(maxStorageBytes, durationMs),
            ingestRecordsPerSecond: ingestDurationMs > 0 ? round(ingestRecords / (ingestDurationMs / 1000), 3) : 0,
            cacheHits: nodeMetrics.reduce((total, metric) => total + metric.cacheHits, 0),
            cacheMisses: nodeMetrics.reduce((total, metric) => total + metric.cacheMisses, 0),
        };
    });
    const maxNodeStorageMiB = perNode.reduce((max, node) => Math.max(max, node.maxStorageMiB), 0);
    const minNodeStorageMiB = perNode.length === 0
        ? 0
        : perNode.reduce((min, node) => Math.min(min, node.maxStorageMiB), perNode[0].maxStorageMiB);
    const avgNodeStorageMiB = perNode.length === 0
        ? 0
        : round(perNode.reduce((total, node) => total + node.maxStorageMiB, 0) / perNode.length);
    const totalNodeStorageMiB = perNode.reduce((total, node) => total + node.maxStorageMiB, 0);
    const maxNodeStorageFillPct = perNode.reduce((max, node) => Math.max(max, node.storageFillPct), 0);
    const minNodeStorageFillPct = perNode.length === 0
        ? 0
        : perNode.reduce((min, node) => Math.min(min, node.storageFillPct), perNode[0].storageFillPct);
    const avgNodeStorageFillPct = perNode.length === 0
        ? 0
        : round(perNode.reduce((total, node) => total + node.storageFillPct, 0) / perNode.length, 4);
    const ingest = summary.useCases['bulk-streaming-ingest'];
    const ingestDurationMs = metrics
        .filter((metric) => metric.useCase === 'bulk-streaming-ingest')
        .reduce((total, metric) => total + metric.durationMs, 0);
    const ingestRecordsPerSecond = ingest && ingestDurationMs > 0
        ? round(ingest.records / (ingestDurationMs / 1000), 3)
        : 0;
    const slowestIngestNode = perNode.reduce((slowest, node) => {
        if (!slowest || node.ingestRecordsPerSecond < slowest.ingestRecordsPerSecond) {
            return node;
        }
        return slowest;
    }, undefined);
    const minNodeIngestRecordsPerSecond = slowestIngestNode?.ingestRecordsPerSecond ?? 0;
    const leastFilledNode = perNode.reduce((leastFilled, node) => {
        if (!leastFilled || node.storageFillPct < leastFilled.storageFillPct) {
            return node;
        }
        return leastFilled;
    }, undefined);
    const cacheTotal = summary.totalCacheHits + summary.totalCacheMisses;
    const topByteUseCases = Object.values(summary.useCases)
        .map((useCase) => ({
        useCase: useCase.useCase,
        requestMiB: bytesToMiB(useCase.requestBytes),
        responseMiB: bytesToMiB(useCase.responseBytes),
        flatbufferMiB: bytesToMiB(useCase.flatbufferBytes),
        durationMs: round(metrics
            .filter((metric) => metric.useCase === useCase.useCase)
            .reduce((total, metric) => total + metric.durationMs, 0)),
        ingressMiBps: rateMiBps(useCase.requestBytes, metrics
            .filter((metric) => metric.useCase === useCase.useCase)
            .reduce((total, metric) => total + metric.durationMs, 0)),
        egressMiBps: rateMiBps(useCase.responseBytes, metrics
            .filter((metric) => metric.useCase === useCase.useCase)
            .reduce((total, metric) => total + metric.durationMs, 0)),
        cacheHits: useCase.cacheHits,
        cacheMisses: useCase.cacheMisses,
    }))
        .sort((left, right) => (right.requestMiB + right.responseMiB + right.flatbufferMiB) -
        (left.requestMiB + left.responseMiB + left.flatbufferMiB))
        .slice(0, 8);
    const transport = [...new Set(metrics.map((metric) => metric.transport))].sort();
    const measurement = [...new Set(metrics.map((metric) => metric.measurement))].sort();
    const runtime = [...new Set(metrics.map((metric) => metric.runtime ?? 'unknown'))].sort();
    const expectedSchemaNames = new Set(manifest.schemas.map((schema) => schema.name));
    const schemaSweepNames = new Set(metrics
        .filter((metric) => metric.useCase === 'schema-sweep' && expectedSchemaNames.has(metric.schema))
        .map((metric) => metric.schema));
    const requirements = [
        requirement('errors', summary.totalErrors === 0, '0 errors', String(summary.totalErrors)),
        requirement('measurement', measurement.length === 1 && measurement[0] === 'measured', 'all events measured', measurement.join(',') || 'none'),
        requirement('transport', transport.includes('docker-compose'), 'docker-compose transport', transport.join(',') || 'none'),
        requirement('runtime', runtime.length > 0 && runtime.every((item) => item === 'native' || item === 'standalone' || item === 'wasmedge'), 'native, standalone, or wasmedge', runtime.join(',') || 'none'),
        requirement('node-isolation', manifest.nodeIsolation === 'container', 'container', manifest.nodeIsolation),
        requirement('node-count', summary.nodes.length >= 100, '>= 100 nodes', String(summary.nodes.length)),
        requirement('schema-sweep-coverage', schemaSweepNames.size === expectedSchemaNames.size, `${expectedSchemaNames.size} schemas`, `${schemaSweepNames.size} schemas`),
        requirement('storage-fill', minNodeStorageFillPct >= MIN_PRODUCTION_STORAGE_FILL_PCT, `>= ${MIN_PRODUCTION_STORAGE_FILL_PCT}% on every node`, leastFilledNode
            ? `min ${minNodeStorageFillPct.toFixed(4)}% on node ${leastFilledNode.nodeId}; max ${maxNodeStorageFillPct.toFixed(4)}%`
            : 'no storage metrics'),
        requirement('fps', minNodeIngestRecordsPerSecond >= MIN_PRODUCTION_FPS, `>= ${MIN_PRODUCTION_FPS} fps on every node`, slowestIngestNode
            ? `min ${minNodeIngestRecordsPerSecond} fps on node ${slowestIngestNode.nodeId}; aggregate ${ingestRecordsPerSecond} fps`
            : 'no ingest metrics'),
    ];
    const failedRequirements = requirements.filter((item) => item.status === 'fail');
    return {
        runId: manifest.runId,
        nodeCount: manifest.nodeCount,
        nodeStorageGb: manifest.nodeStorageGb,
        recordsPerNode: manifest.recordsPerNode,
        uniqueIngestRecords: manifest.nodeCount * manifest.recordsPerNode,
        metricEvents: metrics.length,
        transport,
        runtime: runtime.filter((item) => item === 'native' || item === 'standalone' || item === 'wasmedge' || item === 'typescript'),
        nodeIsolation: manifest.nodeIsolation,
        measurement,
        aggregate: {
            requestMiB: bytesToMiB(summary.totalRequestBytes),
            responseMiB: bytesToMiB(summary.totalResponseBytes),
            flatbufferMiB: bytesToMiB(summary.totalFlatbufferBytes),
            maxNodeStorageMiB,
            minNodeStorageMiB,
            avgNodeStorageMiB,
            maxNodeStorageFillPct,
            minNodeStorageFillPct,
            avgNodeStorageFillPct,
            ingestRecordsPerSecond,
            minNodeIngestRecordsPerSecond,
            ingestStorageFillMiBps: rateMiBps(totalNodeStorageMiB * BYTES_PER_MIB, ingestDurationMs),
            cacheHitRatePct: pct(summary.totalCacheHits, cacheTotal),
        },
        productionReadiness: {
            status: failedRequirements.length === 0 ? 'pass' : 'fail',
            failedRequirements,
            requirements,
        },
        topByteUseCases,
        perNode,
    };
}
//# sourceMappingURL=report.js.map