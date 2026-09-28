// The partition store artifact, flatsql-ps-threads.wasm (docs/PARTITION-STORE-WASM.md).
//
// A wasm32-wasip1-threads reactor: hosts instantiate it once per writer or
// reader instance over a shared env.memory, provide WASI preview1,
// wasi.thread-spawn and the seven env.flatsql_io_* imports, and drive it
// through the flatsql_ps_* C ABI (cpp/include/flatsql/ps/flatsql_ps.h).
// Node: flatsql/ps/node-host. Go/WasmEdge: the SDN host. Browsers: sdn-js.

const PS_THREADS_URL = new URL('./flatsql-ps-threads.wasm', import.meta.url);

/** File name of the artifact next to this module. */
export const FLATSQL_PS_THREADS_FILE = 'flatsql-ps-threads.wasm';

/** URL of the packaged flatsql-ps-threads.wasm. */
export function getFlatSQLPsThreadsURL() {
  return PS_THREADS_URL;
}

/**
 * The artifact's bytes, checked against wasm/integrity.json: in Node with
 * node:crypto, elsewhere with `computeSHA256` (bytes -> hex), which the caller
 * backs with WASM or native crypto (browser WebCrypto is intentionally not
 * used, as in wasm/index.js). A mismatch throws.
 *
 * @param {{ url?: string | URL, path?: string, verify?: boolean,
 *   computeSHA256?: (bytes: Uint8Array) => string | Promise<string> }} [options]
 * @returns {Promise<Uint8Array>}
 */
export async function loadFlatSQLPsThreads(options = {}) {
  let bytes;
  const url = options.url ? new URL(options.url, import.meta.url) : PS_THREADS_URL;
  if (options.path || url.protocol === 'file:') {
    const [{ readFile }, { fileURLToPath }] = await Promise.all([import('node:fs/promises'), import('node:url')]);
    bytes = new Uint8Array(await readFile(options.path ?? fileURLToPath(url)));
  } else {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Failed to load ${FLATSQL_PS_THREADS_FILE}: ${response.status}`);
    bytes = new Uint8Array(await response.arrayBuffer());
  }
  if (options.verify === false || options.url || options.path) return bytes;
  const hash = options.computeSHA256 ?? (await nodeSha256());
  if (!hash) return bytes;
  const expected = await flatsqlPsThreadsSha256();
  if (!expected) throw new Error(`integrity.json has no sha256 for ${FLATSQL_PS_THREADS_FILE}`);
  const actual = await hash(bytes);
  if (actual !== expected) {
    throw new Error(`${FLATSQL_PS_THREADS_FILE}: sha256 ${actual} does not match integrity.json (${expected})`);
  }
  return bytes;
}

/** The artifact's sha256 (hex) from the packaged integrity.json, or null. */
export async function flatsqlPsThreadsSha256() {
  const integrityUrl = new URL('./integrity.json', import.meta.url);
  let text;
  try {
    if (integrityUrl.protocol === 'file:') {
      const [{ readFile }, { fileURLToPath }] = await Promise.all([import('node:fs/promises'), import('node:url')]);
      text = await readFile(fileURLToPath(integrityUrl), 'utf8');
    } else {
      const response = await fetch(integrityUrl);
      if (!response.ok) return null;
      text = await response.text();
    }
  } catch {
    return null;
  }
  return JSON.parse(text)?.files?.[FLATSQL_PS_THREADS_FILE]?.sha256 ?? null;
}

async function nodeSha256() {
  if (!globalThis.process?.versions?.node) return null;
  const { createHash } = await import('node:crypto');
  return (bytes) => createHash('sha256').update(bytes).digest('hex');
}
