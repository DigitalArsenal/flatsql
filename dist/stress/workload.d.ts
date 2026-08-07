import type { NodeAssignment, NormalizedStressRunConfig, SdsSchemaInfo, StressRunConfig, StressUseCase, WorkloadManifest } from './types.js';
export declare const DEFAULT_USE_CASES: StressUseCase[];
export declare function normalizeStressRunConfig(config?: StressRunConfig): NormalizedStressRunConfig;
export declare function buildNodeAssignments(schemas: readonly SdsSchemaInfo[], config: Pick<NormalizedStressRunConfig, 'nodeCount' | 'nodeIdOffset' | 'nodeStorageGb'>): NodeAssignment[];
export declare function buildWorkloadManifest(config: StressRunConfig, schemas: readonly SdsSchemaInfo[]): WorkloadManifest;
//# sourceMappingURL=workload.d.ts.map