function percentile(values, percentileValue) {
    if (values.length === 0) {
        return 0;
    }
    const sorted = [...values].sort((left, right) => left - right);
    const index = Math.ceil((sorted.length - 1) * percentileValue);
    return sorted[Math.min(index, sorted.length - 1)];
}
function createUseCaseSummary(useCase) {
    return {
        useCase,
        events: 0,
        records: 0,
        requestBytes: 0,
        responseBytes: 0,
        flatbufferBytes: 0,
        storageBytes: 0,
        cacheHits: 0,
        cacheMisses: 0,
        maxCacheSize: 0,
        errors: 0,
        latencyP50Ms: 0,
        latencyP95Ms: 0,
        latencyP99Ms: 0,
    };
}
export function reduceMetricEvents(events) {
    const durations = events.map((event) => event.durationMs);
    const nodes = [...new Set(events.map((event) => event.nodeId))].sort((left, right) => left - right);
    const runId = events[0]?.runId ?? '';
    const groupedDurations = new Map();
    const useCases = {};
    const summary = {
        runId,
        nodes,
        eventCount: events.length,
        totalRecords: 0,
        totalRequestBytes: 0,
        totalResponseBytes: 0,
        totalFlatbufferBytes: 0,
        totalStorageBytes: 0,
        totalCacheHits: 0,
        totalCacheMisses: 0,
        totalErrors: 0,
        latencyP50Ms: percentile(durations, 0.5),
        latencyP95Ms: percentile(durations, 0.95),
        latencyP99Ms: percentile(durations, 0.99),
        useCases,
    };
    for (const event of events) {
        summary.totalRecords += event.records;
        summary.totalRequestBytes += event.requestBytes;
        summary.totalResponseBytes += event.responseBytes;
        summary.totalFlatbufferBytes += event.flatbufferBytes;
        summary.totalStorageBytes += event.storageBytes;
        summary.totalCacheHits += event.cacheHits;
        summary.totalCacheMisses += event.cacheMisses;
        summary.totalErrors += event.errors;
        const useCaseSummary = useCases[event.useCase] ?? createUseCaseSummary(event.useCase);
        useCases[event.useCase] = useCaseSummary;
        useCaseSummary.events += 1;
        useCaseSummary.records += event.records;
        useCaseSummary.requestBytes += event.requestBytes;
        useCaseSummary.responseBytes += event.responseBytes;
        useCaseSummary.flatbufferBytes += event.flatbufferBytes;
        useCaseSummary.storageBytes += event.storageBytes;
        useCaseSummary.cacheHits += event.cacheHits;
        useCaseSummary.cacheMisses += event.cacheMisses;
        useCaseSummary.maxCacheSize = Math.max(useCaseSummary.maxCacheSize, event.cacheSize);
        useCaseSummary.errors += event.errors;
        const useCaseDurations = groupedDurations.get(event.useCase) ?? [];
        groupedDurations.set(event.useCase, useCaseDurations);
        useCaseDurations.push(event.durationMs);
    }
    for (const [useCase, useCaseDurations] of groupedDurations.entries()) {
        const useCaseSummary = useCases[useCase];
        useCaseSummary.latencyP50Ms = percentile(useCaseDurations, 0.5);
        useCaseSummary.latencyP95Ms = percentile(useCaseDurations, 0.95);
        useCaseSummary.latencyP99Ms = percentile(useCaseDurations, 0.99);
    }
    return summary;
}
//# sourceMappingURL=metrics.js.map