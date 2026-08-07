function detectSharedArrayBuffer() {
    return typeof SharedArrayBuffer !== 'undefined';
}
function detectAtomics() {
    return typeof Atomics !== 'undefined';
}
function detectCrossOriginIsolation() {
    const globalAny = globalThis;
    return globalAny.crossOriginIsolated === true;
}
function detectOpfs() {
    if (typeof navigator !== 'undefined') {
        const storageManager = navigator.storage;
        if (storageManager && typeof storageManager.getDirectory === 'function') {
            return true;
        }
    }
    const globalAny = globalThis;
    return typeof globalAny.originPrivateFileSystem === 'object' && globalAny.originPrivateFileSystem !== null;
}
export function detectClusterEnvironment() {
    return {
        hasSharedArrayBuffer: detectSharedArrayBuffer(),
        hasAtomics: detectAtomics(),
        hasCrossOriginIsolation: detectCrossOriginIsolation(),
        hasOpfs: detectOpfs(),
    };
}
export function isClusterModeSupported(env = detectClusterEnvironment()) {
    return (env.hasSharedArrayBuffer &&
        env.hasAtomics &&
        env.hasCrossOriginIsolation &&
        env.hasOpfs);
}
export function describeClusterSupport(env = detectClusterEnvironment()) {
    const missing = [];
    if (!env.hasSharedArrayBuffer) {
        missing.push('SharedArrayBuffer');
    }
    if (!env.hasAtomics) {
        missing.push('Atomics');
    }
    if (!env.hasCrossOriginIsolation) {
        missing.push('cross-origin isolation');
    }
    if (!env.hasOpfs) {
        missing.push('OPFS');
    }
    if (missing.length === 0) {
        return 'Cluster mode supported on this runtime';
    }
    return `Cluster mode unsupported: missing ${missing.join(', ')}`;
}
export function ensureClusterModeSupported(env = detectClusterEnvironment()) {
    if (!isClusterModeSupported(env)) {
        throw new Error(describeClusterSupport(env));
    }
}
//# sourceMappingURL=index.js.map