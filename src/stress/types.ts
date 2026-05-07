export type StressMode = 'smoke' | 'full';
export type StressRuntime = 'standalone' | 'wasmedge' | 'native';
export type MetricRuntime = StressRuntime | 'typescript';
export type MetricMeasurement = 'measured' | 'estimated';
export type MetricTransport = 'in-process' | 'docker-compose';
export type StressNodeIsolation = 'logical' | 'container';

export interface SdsSchemaInfo {
  name: string;
  path: string;
  rootType: string;
  flatbufferIdentifier?: string;
  tableNames: string[];
}

export interface StressUseCase {
  id: string;
  label: string;
  operation: string;
  description: string;
}

export interface StressRunConfig {
  mode?: StressMode;
  runId?: string;
  nodeCount?: number;
  nodeIdOffset?: number;
  nodeStorageGb?: number;
  recordsPerNode?: number;
  batchBytes?: number;
  queryConcurrency?: number;
  hotQueryRatio?: number;
  runtime?: StressRuntime;
  transport?: MetricTransport;
  nodeIsolation?: StressNodeIsolation;
  seed?: number;
}

export interface NormalizedStressRunConfig {
  mode: StressMode;
  runId: string;
  nodeCount: number;
  nodeIdOffset: number;
  nodeStorageGb: number;
  recordsPerNode: number;
  batchBytes: number;
  queryConcurrency: number;
  hotQueryRatio: number;
  runtime: StressRuntime;
  transport: MetricTransport;
  nodeIsolation: StressNodeIsolation;
  seed: number;
}

export interface NodeAssignment {
  nodeId: number;
  storageGb: number;
  schemas: string[];
}

export interface WorkloadManifest extends NormalizedStressRunConfig {
  createdAt: string;
  schemaCount: number;
  schemas: SdsSchemaInfo[];
  useCases: StressUseCase[];
  assignments: NodeAssignment[];
}

export interface MetricEvent {
  runId: string;
  nodeId: number;
  schema: string;
  useCase: string;
  operation: string;
  records: number;
  requestBytes: number;
  responseBytes: number;
  flatbufferBytes: number;
  storageBytes: number;
  durationMs: number;
  cacheHits: number;
  cacheMisses: number;
  cacheSize: number;
  errors: number;
  measurement: MetricMeasurement;
  transport: MetricTransport;
  runtime?: MetricRuntime;
}

export interface UseCaseSummary {
  useCase: string;
  events: number;
  records: number;
  requestBytes: number;
  responseBytes: number;
  flatbufferBytes: number;
  storageBytes: number;
  cacheHits: number;
  cacheMisses: number;
  maxCacheSize: number;
  errors: number;
  latencyP50Ms: number;
  latencyP95Ms: number;
  latencyP99Ms: number;
}

export interface StressSummary {
  runId: string;
  nodes: number[];
  eventCount: number;
  totalRecords: number;
  totalRequestBytes: number;
  totalResponseBytes: number;
  totalFlatbufferBytes: number;
  totalStorageBytes: number;
  totalCacheHits: number;
  totalCacheMisses: number;
  totalErrors: number;
  latencyP50Ms: number;
  latencyP95Ms: number;
  latencyP99Ms: number;
  useCases: Record<string, UseCaseSummary>;
}

export interface SmokeHarnessOptions extends StressRunConfig {
  schemaRoot: string;
  outputDir: string;
}

export interface SmokeHarnessFindings {
  indexingModel: {
    publishEventKey: 'FILE_ID';
    publishEventScope: string;
    perSchemaRulesOwner: 'space-data-network';
    perSchemaRulesEnforcedInSchema: false;
  };
  schemaPolicy: {
    canonicalSchemasRemainDatabaseNeutral: true;
    databaseSpecificAnnotationsRequired: false;
  };
  externalIndexProfileCoverage: {
    loadedProfiles: number;
    schemaCount: number;
    status: string;
  };
  expectedPressurePoints: string[];
}

export interface SmokeHarnessResult {
  manifest: WorkloadManifest;
  metrics: MetricEvent[];
  summary: StressSummary;
  findings: SmokeHarnessFindings;
  report: StressDerivedReport;
}

export interface StressAggregateOptions {
  inputDirs: string[];
  outputDir: string;
  runId: string;
  mode: StressMode;
  nodeStorageGb: number;
  recordsPerNode: number;
  nodeIsolation: StressNodeIsolation;
  transport: MetricTransport;
}

export interface StressNodeReport {
  nodeId: number;
  requestMiB: number;
  responseMiB: number;
  flatbufferMiB: number;
  maxStorageMiB: number;
  storageFillPct: number;
  ingressMiBps: number;
  egressMiBps: number;
  storageFillMiBps: number;
  ingestRecordsPerSecond: number;
  cacheHits: number;
  cacheMisses: number;
}

export interface StressUseCaseByteReport {
  useCase: string;
  requestMiB: number;
  responseMiB: number;
  flatbufferMiB: number;
  durationMs: number;
  ingressMiBps: number;
  egressMiBps: number;
  cacheHits: number;
  cacheMisses: number;
}

export interface StressReadinessRequirement {
  id: string;
  status: 'pass' | 'fail';
  required: string;
  actual: string;
}

export interface StressDerivedReport {
  runId: string;
  nodeCount: number;
  nodeStorageGb: number;
  recordsPerNode: number;
  uniqueIngestRecords: number;
  metricEvents: number;
  transport: MetricTransport[];
  runtime: MetricRuntime[];
  nodeIsolation: StressNodeIsolation;
  measurement: MetricMeasurement[];
  aggregate: {
    requestMiB: number;
    responseMiB: number;
    flatbufferMiB: number;
    maxNodeStorageMiB: number;
    minNodeStorageMiB: number;
    avgNodeStorageMiB: number;
    maxNodeStorageFillPct: number;
    minNodeStorageFillPct: number;
    avgNodeStorageFillPct: number;
    ingestRecordsPerSecond: number;
    minNodeIngestRecordsPerSecond: number;
    ingestStorageFillMiBps: number;
    cacheHitRatePct: number;
  };
  productionReadiness: {
    status: 'pass' | 'fail';
    failedRequirements: StressReadinessRequirement[];
    requirements: StressReadinessRequirement[];
  };
  topByteUseCases: StressUseCaseByteReport[];
  perNode: StressNodeReport[];
}
