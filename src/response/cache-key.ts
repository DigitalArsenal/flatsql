import type { ResponseCacheKeyInput } from './types.js';

const SHA256_INITIAL = Uint32Array.of(
  0x6a09e667,
  0xbb67ae85,
  0x3c6ef372,
  0xa54ff53a,
  0x510e527f,
  0x9b05688c,
  0x1f83d9ab,
  0x5be0cd19
);

const SHA256_K = Uint32Array.of(
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
  0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
  0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
  0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
  0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
  0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
);

function rotr(value: number, shift: number): number {
  return (value >>> shift) | (value << (32 - shift));
}

function toHex32(value: number): string {
  return (value >>> 0).toString(16).padStart(8, '0');
}

function normalizeSql(sql: string): string {
  return sql.trim().replace(/\s+/g, ' ');
}

function stableValue(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return {
      type: 'Uint8Array',
      sha256: hashBytes(value),
      byteLength: value.byteLength,
    };
  }

  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (Array.isArray(value)) {
    return value.map((item) => stableValue(item));
  }

  if (value && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return Object.fromEntries(
      Object.keys(object)
        .sort()
        .map((key) => [key, stableValue(object[key])])
    );
  }

  return value;
}

export function stableStringify(value: unknown): string {
  return JSON.stringify(stableValue(value));
}

export function hashBytes(bytes: Uint8Array): string {
  const paddedLength = Math.ceil((bytes.byteLength + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(bytes);
  padded[bytes.byteLength] = 0x80;

  let bitLength = BigInt(bytes.byteLength) * 8n;
  for (let index = 0; index < 8; index++) {
    padded[paddedLength - 1 - index] = Number(bitLength & 0xffn);
    bitLength >>= 8n;
  }

  const hash = new Uint32Array(SHA256_INITIAL);
  const schedule = new Uint32Array(64);

  for (let offset = 0; offset < padded.byteLength; offset += 64) {
    for (let index = 0; index < 16; index++) {
      const base = offset + index * 4;
      schedule[index] = (
        (padded[base] << 24) |
        (padded[base + 1] << 16) |
        (padded[base + 2] << 8) |
        padded[base + 3]
      ) >>> 0;
    }

    for (let index = 16; index < 64; index++) {
      const s0 = (rotr(schedule[index - 15], 7) ^ rotr(schedule[index - 15], 18) ^ (schedule[index - 15] >>> 3)) >>> 0;
      const s1 = (rotr(schedule[index - 2], 17) ^ rotr(schedule[index - 2], 19) ^ (schedule[index - 2] >>> 10)) >>> 0;
      schedule[index] = (schedule[index - 16] + s0 + schedule[index - 7] + s1) >>> 0;
    }

    let a = hash[0];
    let b = hash[1];
    let c = hash[2];
    let d = hash[3];
    let e = hash[4];
    let f = hash[5];
    let g = hash[6];
    let h = hash[7];

    for (let index = 0; index < 64; index++) {
      const s1 = (rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)) >>> 0;
      const ch = ((e & f) ^ (~e & g)) >>> 0;
      const temp1 = (h + s1 + ch + SHA256_K[index] + schedule[index]) >>> 0;
      const s0 = (rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)) >>> 0;
      const maj = ((a & b) ^ (a & c) ^ (b & c)) >>> 0;
      const temp2 = (s0 + maj) >>> 0;

      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }

    hash[0] = (hash[0] + a) >>> 0;
    hash[1] = (hash[1] + b) >>> 0;
    hash[2] = (hash[2] + c) >>> 0;
    hash[3] = (hash[3] + d) >>> 0;
    hash[4] = (hash[4] + e) >>> 0;
    hash[5] = (hash[5] + f) >>> 0;
    hash[6] = (hash[6] + g) >>> 0;
    hash[7] = (hash[7] + h) >>> 0;
  }

  return `sha256:${Array.from(hash, toHex32).join('')}`;
}

export function hashString(value: string): string {
  return hashBytes(new TextEncoder().encode(value));
}

export function createResponseCacheKey(input: ResponseCacheKeyInput): string {
  const canonical = {
    encoding: input.encoding ?? 'identity',
    filters: input.filters ?? {},
    format: input.format ?? 'json',
    params: input.params ?? [],
    projection: input.projection ?? [],
    publishEventKey: input.publishEventKey ?? null,
    schemaName: input.schemaName ?? null,
    schemaVersion: input.schemaVersion ?? null,
    sql: normalizeSql(input.sql),
  };
  return `flatsql:v1:${hashString(stableStringify(canonical))}`;
}
