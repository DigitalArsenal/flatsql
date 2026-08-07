export interface FlatSQLWasmEdgeRunnerBuildOptions {
    outputPath: string;
    sourcePath?: string;
    includeDir?: string;
    libDir?: string;
    cxx?: string;
    extraArgs?: string[];
}
export interface FlatSQLWasmEdgeRunnerArtifact {
    outputPath: string;
    sourcePath: string;
    includeDir: string;
    libDir: string;
    command: string;
    args: string[];
}
export declare function hasWasmEdgeBuildInputs(options?: Partial<FlatSQLWasmEdgeRunnerBuildOptions>): boolean;
export declare function buildFlatSQLWasmEdgeRunner(options: FlatSQLWasmEdgeRunnerBuildOptions): Promise<FlatSQLWasmEdgeRunnerArtifact>;
//# sourceMappingURL=wasmedge-runner.d.ts.map