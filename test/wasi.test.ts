import { fileURLToPath } from 'node:url';
import { stat } from 'node:fs/promises';

import { getFlatSQLWASIURL, loadFlatSQLWASI } from '../wasm/wasi.js';

function asBytes(data: Uint8Array | ArrayBuffer): Uint8Array {
  return data instanceof Uint8Array ? data : new Uint8Array(data);
}

describe('WASI package loader', () => {
  test('resolves packaged WASI URL', () => {
    const url = getFlatSQLWASIURL();
    expect(url).toBeInstanceOf(URL);
    expect(url.pathname.endsWith('/flatsql-wasi.wasm')).toBe(true);
  });

  test('loads WASI module as Uint8Array', async () => {
    const bytes = asBytes(await loadFlatSQLWASI({ as: 'uint8array' }));
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(bytes.length).toBeGreaterThan(8);

    // WebAssembly binary magic number: 00 61 73 6D
    expect(Array.from(bytes.slice(0, 4))).toEqual([0x00, 0x61, 0x73, 0x6d]);
  });

  test('loads WASI module by explicit filesystem path', async () => {
    const path = fileURLToPath(getFlatSQLWASIURL());
    const meta = await stat(path);

    expect(meta.isFile()).toBe(true);
    expect(meta.size).toBeGreaterThan(0);

    const bytes = asBytes(
      await loadFlatSQLWASI({
        path,
        as: 'uint8array',
      })
    );

    expect(bytes.length).toBe(meta.size);
    expect(Array.from(bytes.slice(0, 4))).toEqual([0x00, 0x61, 0x73, 0x6d]);
  });
});
