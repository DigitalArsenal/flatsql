import type { StressMode, StressRuntime } from './types.js';
export interface FullStressDockerComposeOptions {
    projectRoot: string;
    schemaRootHost: string;
    outputDirHost: string;
    runId: string;
    mode?: StressMode;
    nodeCount: number;
    nodeStorageGb: number;
    recordsPerNode: number;
    batchBytes: number;
    queryConcurrency: number;
    hotQueryRatio: number;
    runtime: StressRuntime;
}
export declare function buildFullStressDockerCompose(options: FullStressDockerComposeOptions): string;
//# sourceMappingURL=docker-compose.d.ts.map