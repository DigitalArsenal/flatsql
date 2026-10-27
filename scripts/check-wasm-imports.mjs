#!/usr/bin/env node
// The unified artifact has one closed import/export contract across hosts.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const path = new URL('../wasm/flatsql.wasm', import.meta.url);
const bytes = new Uint8Array(readFileSync(path));
const module = new WebAssembly.Module(bytes);
const WASI = [
  'clock_time_get',
  'environ_get',
  'environ_sizes_get',
  'fd_close',
  'fd_prestat_dir_name',
  'fd_prestat_get',
  'fd_seek',
  'fd_write',
  'proc_exit',
  'sched_yield',
];
function importedMemory(bytes) {
  let at = 8;
  const leb = () => {
    let result = 0;
    let shift = 0;
    for (;;) {
      const b = bytes[at++];
      result += (b & 0x7f) * 2 ** shift;
      shift += 7;
      if (!(b & 0x80)) return result;
    }
  };
  const name = () => {
    const n = leb();
    const text = Buffer.from(bytes.subarray(at, at + n)).toString('utf8');
    at += n;
    return text;
  };
  while (at < bytes.length) {
    const id = bytes[at++];
    const size = leb();
    const end = at + size;
    if (id === 2) {
      const count = leb();
      for (let i = 0; i < count; i++) {
        const module = name();
        const field = name();
        const kind = bytes[at++];
        if (kind === 0) leb();
        else if (kind === 1) {
          at += 1;
          if (leb() & 1) leb();
          leb();
        } else if (kind === 2) {
          const flags = leb();
          const initial = leb();
          const maximum = flags & 1 ? leb() : null;
          return { module, field, initial, maximum, shared: (flags & 2) !== 0 };
        } else if (kind === 3) at += 2;
        else return null;
      }
      return null;
    }
    at = end;
  }
  return null;
}

const io = ['close','open','read','size','sync','truncate','write'].map(n => 'flatsql_io_' + n);
const imported = WebAssembly.Module.imports(module);
assert.deepEqual(imported.map(i => `${i.module}.${i.name}:${i.kind}`).sort(), [
  ...WASI.map(n => `wasi_snapshot_preview1.${n}:function`), ...io.map(n => `env.${n}:function`),
  'env.memory:memory', 'wasi.thread-spawn:function',
].sort());
assert.deepEqual(WebAssembly.Module.exports(module).map(e => `${e.name}:${e.kind}`).sort(), [
  ...['activate','alloc','free','init','layout','register_type','set_quota','start','stats','stop','wake','ingest_request'].map(n => `flatsql_${n}:function`),
  '_initialize:function', 'wasi_thread_start:function', 'memory:memory',
].sort());
const memory = importedMemory(bytes);
assert.equal(memory?.module, 'env'); assert.equal(memory?.field, 'memory');
assert.equal(memory?.shared, true); assert.equal(memory?.maximum, 32768);
assert.ok(memory.initial >= 512);
for (const removed of ['geo_from_geojson','geo_as_geojson','geo_from_text','geo_as_text','JSONPath'])
  assert.equal(Buffer.from(bytes).includes(Buffer.from(removed)), false, `${removed} remains in the binary`);
console.log(`Unified FlatSQL ABI/imports verified (${bytes.length} bytes)`);
