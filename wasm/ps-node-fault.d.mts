// Types for wasm/ps-node-fault.mjs (the Node host's fault overlay).

export type PsCrashMode = 'dropAll' | 'pageSubset' | 'keepAll';

export interface PsFaultOverlay {
  buffer: SharedArrayBuffer;
  /** Every later mutating call fails with EIO (the process is dying). */
  freeze(): void;
  stats(): { writes: number; syncs: number; pagesUsed: number; overflow: boolean };
  /** Apply a crash once every guest thread has stopped, then reset the overlay. */
  crash(
    mode: PsCrashMode,
    random?: () => number,
  ): { pagesUndone: number; pagesKept: number; filesTruncated: number; entriesDropped: number };
}

export const PAGE: number;
export function createFaultOverlay(options?: { maxFiles?: number; maxPages?: number }): PsFaultOverlay;
export function attachFaultOverlay(buffer: SharedArrayBuffer): PsFaultOverlay;
export function createFaultInterposer(
  buffer: SharedArrayBuffer,
  root?: string,
): (op: string, fn: (...args: unknown[]) => unknown, args: unknown[]) => unknown;
