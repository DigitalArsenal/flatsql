export interface ClusterEnvironment {
    hasSharedArrayBuffer: boolean;
    hasAtomics: boolean;
    hasCrossOriginIsolation: boolean;
    hasOpfs: boolean;
}
export declare function detectClusterEnvironment(): ClusterEnvironment;
export declare function isClusterModeSupported(env?: ClusterEnvironment): boolean;
export declare function describeClusterSupport(env?: ClusterEnvironment): string;
export declare function ensureClusterModeSupported(env?: ClusterEnvironment): void;
//# sourceMappingURL=index.d.ts.map