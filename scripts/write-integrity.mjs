#!/usr/bin/env node
/**
 * wasm/integrity.json: the digest of every shipped wasm artifact.
 *
 *   node scripts/write-integrity.mjs          write it from wasm/*.wasm
 *   node scripts/write-integrity.mjs --check  fail unless it matches the files
 *
 * `files` lists every wasm/*.wasm with its sha256 (what hosts pin, e.g. the SDN
 * node embedding flatsql-ps-threads.wasm), its sha384 SRI and its size. The
 * top-level algorithm/hash/sri/size fields describe flatsql.wasm and keep the
 * shape wasm/index.js has always read. No timestamps: the file is a pure
 * function of the artifacts, so a check can compare it byte for byte.
 */
import { createHash } from 'node:crypto';
import { readFileSync, readdirSync, writeFileSync, existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const WASM = join(ROOT, 'wasm');
const OUT = join(WASM, 'integrity.json');

function digest(bytes, algorithm, encoding) {
  return createHash(algorithm).update(bytes).digest(encoding);
}

const files = {};
for (const name of readdirSync(WASM).filter((f) => f.endsWith('.wasm')).sort()) {
  const bytes = readFileSync(join(WASM, name));
  const sha384 = digest(bytes, 'sha384', 'base64');
  files[name] = {
    sha256: digest(bytes, 'sha256', 'hex'),
    sri: `sha384-${sha384}`,
    size: bytes.length,
  };
}
const main = files['flatsql.wasm'];
if (!main) {
  console.error('wasm/flatsql.wasm is missing');
  process.exit(1);
}
const integrity = {
  algorithm: 'sha384',
  hash: main.sri.slice('sha384-'.length),
  sri: main.sri,
  size: main.size,
  files,
};
const text = `${JSON.stringify(integrity, null, 2)}\n`;

if (process.argv.includes('--check')) {
  const current = existsSync(OUT) ? readFileSync(OUT, 'utf8') : '';
  if (current !== text) {
    console.error('wasm/integrity.json does not match wasm/*.wasm; run node scripts/write-integrity.mjs');
    process.exit(1);
  }
  console.log(`wasm/integrity.json matches ${Object.keys(files).length} artifacts`);
} else {
  writeFileSync(OUT, text);
  console.log(`wrote wasm/integrity.json (${Object.keys(files).length} artifacts)`);
}
