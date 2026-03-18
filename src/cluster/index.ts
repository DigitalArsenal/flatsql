type StorageManagerWithDirectory = StorageManager & {
  getDirectory?: () => Promise<FileSystemDirectoryHandle>;
};

export interface ClusterEnvironment {
  hasSharedArrayBuffer: boolean;
  hasAtomics: boolean;
  hasCrossOriginIsolation: boolean;
  hasOpfs: boolean;
}

function detectSharedArrayBuffer(): boolean {
  return typeof SharedArrayBuffer !== 'undefined';
}

function detectAtomics(): boolean {
  return typeof Atomics !== 'undefined';
}

function detectCrossOriginIsolation(): boolean {
  const globalAny = globalThis as typeof globalThis & { crossOriginIsolated?: boolean };
  return globalAny.crossOriginIsolated === true;
}

function detectOpfs(): boolean {
  if (typeof navigator !== 'undefined') {
    const storageManager = navigator.storage as StorageManagerWithDirectory | undefined;
    if (storageManager && typeof storageManager.getDirectory === 'function') {
      return true;
    }
  }

  const globalAny = globalThis as typeof globalThis & { originPrivateFileSystem?: unknown };
  return typeof globalAny.originPrivateFileSystem === 'object' && globalAny.originPrivateFileSystem !== null;
}

export function detectClusterEnvironment(): ClusterEnvironment {
  return {
    hasSharedArrayBuffer: detectSharedArrayBuffer(),
    hasAtomics: detectAtomics(),
    hasCrossOriginIsolation: detectCrossOriginIsolation(),
    hasOpfs: detectOpfs(),
  };
}

export function isClusterModeSupported(env: ClusterEnvironment = detectClusterEnvironment()): boolean {
  return (
    env.hasSharedArrayBuffer &&
    env.hasAtomics &&
    env.hasCrossOriginIsolation &&
    env.hasOpfs
  );
}

export function describeClusterSupport(env: ClusterEnvironment = detectClusterEnvironment()): string {
  const missing: string[] = [];
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

export function ensureClusterModeSupported(env: ClusterEnvironment = detectClusterEnvironment()): void {
  if (!isClusterModeSupported(env)) {
    throw new Error(describeClusterSupport(env));
  }
}
