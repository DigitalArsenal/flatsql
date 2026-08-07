export const DEFAULT_USE_CASES = [
    {
        id: 'schema-sweep',
        label: 'SDS schema sweep',
        operation: 'schema-parse',
        description: 'Discover every SDS FlatBuffer schema and record compatibility metadata.',
    },
    {
        id: 'bulk-streaming-ingest',
        label: 'Bulk streaming ingest',
        operation: 'stream-ingest',
        description: 'Stream generated FlatBuffers in size-prefixed batches.',
    },
    {
        id: 'hot-file-id-cache',
        label: 'Hot FILE_ID query cache',
        operation: 'query-template',
        description: 'Repeat the same PNM/DPM-style FILE_ID lookup to measure cache behavior.',
    },
    {
        id: 'cold-file-id-fanout',
        label: 'Cold FILE_ID fanout',
        operation: 'query-template',
        description: 'Fan out mostly unique FILE_ID lookups and measure cache churn.',
    },
    {
        id: 'mixed-hot-cold-query',
        label: 'Mixed hot/cold query distribution',
        operation: 'query-template',
        description: 'Run a skewed query mix with hot and long-tail FILE_ID values.',
    },
    {
        id: 'raw-flatbuffer-retrieval',
        label: 'Raw FlatBuffer retrieval',
        operation: 'raw-flatbuffer',
        description: 'Return original FlatBuffer bytes through indexed lookup paths.',
    },
    {
        id: 'sql-projection-query',
        label: 'SQL projection query',
        operation: 'parameterized-sql',
        description: 'Return a narrow SQL projection to compare against binary payload transfer.',
    },
    {
        id: 'raw-sql-query',
        label: 'Raw SQL query',
        operation: 'raw-sql',
        description: 'Execute untemplated SQL and compare request overhead with query templates.',
    },
    {
        id: 'large-result-query',
        label: 'Large result query',
        operation: 'large-result',
        description: 'Return more rows than the result-cache row cap and measure transport cost.',
    },
    {
        id: 'node-fanout-query',
        label: 'Node fanout query',
        operation: 'fanout-query',
        description: 'Send one logical query across many nodes and measure tail latency.',
    },
    {
        id: 'backfill-sync',
        label: 'Backfill sync',
        operation: 'backfill-stream',
        description: 'Bootstrap a node by streaming records from another source.',
    },
    {
        id: 'restart-rebuild',
        label: 'Restart and rebuild',
        operation: 'restart-rebuild',
        description: 'Restart a node and measure rebuild or reload behavior.',
    },
    {
        id: 'cache-invalidation-churn',
        label: 'Cache invalidation churn',
        operation: 'ingest-query-churn',
        description: 'Alternate ingest and hot queries to expose broad invalidation cost.',
    },
    {
        id: 'bandwidth-constrained-streaming',
        label: 'Bandwidth-constrained streaming',
        operation: 'throttled-stream',
        description: 'Measure chunked streaming behavior under an artificial transport limit.',
    },
    {
        id: 'sds-domain-query-pack',
        label: 'SDS domain query pack',
        operation: 'domain-query',
        description: 'Exercise named space-domain lookups such as catalog, ephemeris, sensor, and weather queries.',
    },
    {
        id: 'udl-style-api-shape',
        label: 'UDL-style API comparison shape',
        operation: 'api-shape',
        description: 'Run centralized API-shaped requests without calling an external UDL service.',
    },
];
export function normalizeStressRunConfig(config = {}) {
    const mode = config.mode ?? 'smoke';
    return {
        mode,
        runId: config.runId ?? `sds-${mode}-${Date.now()}`,
        nodeCount: config.nodeCount ?? (mode === 'full' ? 100 : 2),
        nodeIdOffset: config.nodeIdOffset ?? 0,
        nodeStorageGb: config.nodeStorageGb ?? (mode === 'full' ? 1 : 0.01),
        recordsPerNode: config.recordsPerNode ?? (mode === 'full' ? 20_000 : 100),
        batchBytes: config.batchBytes ?? 1024 * 1024,
        queryConcurrency: config.queryConcurrency ?? (mode === 'full' ? 64 : 4),
        hotQueryRatio: config.hotQueryRatio ?? 0.9,
        runtime: config.runtime ?? 'standalone',
        transport: config.transport ?? 'in-process',
        nodeIsolation: config.nodeIsolation ?? 'logical',
        seed: config.seed ?? 0x5d5,
    };
}
export function buildNodeAssignments(schemas, config) {
    const schemaNames = schemas.map((schema) => schema.name);
    return Array.from({ length: config.nodeCount }, (_, localNodeIndex) => {
        const nodeId = config.nodeIdOffset + localNodeIndex;
        const assigned = schemaNames.filter((_, index) => index % config.nodeCount === localNodeIndex);
        if (assigned.length === 0 && schemaNames.length > 0) {
            assigned.push(schemaNames[nodeId % schemaNames.length]);
        }
        return {
            nodeId,
            storageGb: config.nodeStorageGb,
            schemas: assigned,
        };
    });
}
export function buildWorkloadManifest(config, schemas) {
    const normalized = normalizeStressRunConfig(config);
    const sortedSchemas = [...schemas].sort((left, right) => left.name.localeCompare(right.name));
    return {
        ...normalized,
        createdAt: new Date().toISOString(),
        schemaCount: sortedSchemas.length,
        schemas: sortedSchemas,
        useCases: DEFAULT_USE_CASES,
        assignments: buildNodeAssignments(sortedSchemas, normalized),
    };
}
//# sourceMappingURL=workload.js.map