import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = dirname(fileURLToPath(new URL('../package.json', import.meta.url)));

function readRepoFile(path: string): string {
  return readFileSync(resolve(repoRoot, path), 'utf8');
}

describe('WASM package crypto boundary', () => {
  test('browser wrapper avoids forbidden browser crypto APIs', () => {
    const source = readRepoFile('wasm/index.js');
    const forbiddenGlobal = 'crypto' + '\\.subtle';
    const forbiddenNodeGlobal = 'webcrypto' + '\\.subtle';
    const forbiddenInterface = 'Subtle' + 'Crypto';
    const forbiddenMethods = 'subtle' + '\\.(digest|deriveBits|deriveKey|encrypt|decrypt)';

    expect(source).not.toMatch(new RegExp(`\\b${forbiddenGlobal}\\b`));
    expect(source).not.toMatch(new RegExp(`\\b${forbiddenNodeGlobal}\\b`));
    expect(source).not.toMatch(new RegExp(`\\b${forbiddenInterface}\\b`));
    expect(source).not.toMatch(new RegExp(`\\b${forbiddenMethods}\\b`));
  });

  test('types expose an explicit WASM/native SHA-384 provider option', () => {
    const types = readRepoFile('wasm/index.d.ts');

    expect(types).toContain('computeSHA384?');
    expect(types).toContain('Browser WebCrypto is intentionally not used');
  });
});
