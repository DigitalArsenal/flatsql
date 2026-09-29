import crypto from 'node:crypto';
import initFlatSQL from '../wasm/index.js';
import { FlatcRunner } from 'flatc-wasm';

// Database-key record encryption through the browser artifact's JS API. The
// wasm builds compile the FlatBuffers fallback crypto backend, which derives
// keys with HKDF-SHA256 from FlatBuffers 8af3053e on. The stored bytes are
// checked against a node:crypto reference of field-encryption format 3:
//   K  = HKDF-SHA256(key, no salt, "flatbuffers-buffer-v3" || BE32(sequence))
//   IV = BE32(position of the value's first byte) || 12 zero bytes
//   ciphertext = AES-256-CTR(K, IV, value bytes)
// cpp/test/db_key_encryption_test.cpp covers the rest natively (vectors and
// unions of tables, shared strings, source partitions, a late key).

const SCHEMA = `
  table Secret {
    serial: int (key);
    name: string;
    code: string (encrypted);
    pin: long (encrypted);
  }
  root_type Secret;
  file_identifier "SECR";
`;

const PLAIN = { serial: 7, name: 'public', code: 'identical secret payload', pin: 1234567890123 };
const RECORDS = 300;

async function fixtures() {
  const flatc = await FlatcRunner.init();
  const schemaInput = { entry: '/secret.fbs', files: { '/secret.fbs': SCHEMA } };
  // generateBinary writes a size-prefixed buffer; a record has no prefix.
  const prefixed = new Uint8Array(flatc.generateBinary(schemaInput, JSON.stringify(PLAIN)));
  expect(new DataView(prefixed.buffer, prefixed.byteOffset).getUint32(0, true)).toBe(prefixed.length - 4);
  const record = prefixed.slice(4);
  expect(new TextDecoder().decode(record.subarray(4, 8))).toBe('SECR');
  flatc.mountFile('/secret.fbs', SCHEMA);
  const run = flatc.runCommand(['-b', '--schema', '--bfbs-builtins', '-o', '/out', '/secret.fbs']);
  expect(run.code).toBe(0);
  const bfbs = flatc.readFile('/out/secret.bfbs', { encoding: 'binary' }) as Uint8Array;
  return { record, bfbs };
}

// Byte ranges of the (encrypted) values in a Secret record: the bytes of
// `code` (field 2) and of `pin` (field 3).
function encryptedRanges(record: Uint8Array): Array<[number, number]> {
  const view = new DataView(record.buffer, record.byteOffset, record.byteLength);
  const table = view.getUint32(0, true);
  const vtable = table - view.getInt32(table, true);
  const field = (id: number) => {
    const offset = view.getUint16(vtable + 4 + 2 * id, true);
    expect(offset).toBeGreaterThan(0);
    return table + offset;
  };
  const codeRef = field(2);
  const code = codeRef + view.getUint32(codeRef, true);
  return [
    [code + 4, view.getUint32(code, true)],
    [field(3), 8],
  ];
}

function referenceRecord(plain: Uint8Array, key: Uint8Array, sequence: number): Uint8Array {
  const info = Buffer.alloc('flatbuffers-buffer-v3'.length + 4);
  info.write('flatbuffers-buffer-v3', 0, 'latin1');
  info.writeUInt32BE(sequence, info.length - 4);
  const bufferKey = Buffer.from(crypto.hkdfSync('sha256', key, Buffer.alloc(0), info, 32));
  const out = new Uint8Array(plain);
  for (const [position, length] of encryptedRanges(plain)) {
    const iv = Buffer.alloc(16);
    iv.writeUInt32BE(position, 0);
    const cipher = crypto.createCipheriv('aes-256-ctr', bufferKey, iv);
    out.set(cipher.update(plain.subarray(position, position + length)), position);
  }
  return out;
}

// The records of an exported stream: u32 little-endian size, then the record.
function streamRecords(stream: Uint8Array): Uint8Array[] {
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  const out: Uint8Array[] = [];
  for (let offset = 0; offset + 4 <= stream.length; ) {
    const size = view.getUint32(offset, true);
    out.push(stream.slice(offset + 4, offset + 4 + size));
    offset += 4 + size;
  }
  return out;
}

const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');

describe('database-key encryption (wasm)', () => {
  test('record indexes are sequences; format 2 is refused', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const { record, bfbs } = await fixtures();
    const key = new Uint8Array(32).map((_, i) => i + 1);

    const db = flatsql.createDatabase(SCHEMA, 'db-key-wasm-refusals');
    db.registerFileId('SECR', 'Secret');

    // A record index is the record's sequence: missing, 0 or past 2^32 - 1
    // is refused before the engine is called.
    expect(() => (db as any).encryptBuffer(record, bfbs)).toThrow(/recordIndex must be the record's sequence/);
    expect(() => db.decryptBuffer(record, bfbs, 0)).toThrow(/recordIndex/);
    expect(() => db.encryptBuffer(record, bfbs, 2 ** 32)).toThrow(/recordIndex/);

    // No key yet.
    expect(() => db.ingestOneEncrypted(record, bfbs)).toThrow(/No encryption key set/);
    expect(() => db.encryptBuffer(record, bfbs, 1)).toThrow(/No encryption key set/);
    expect(() => db.computeHMAC(record)).toThrow(/HMAC computation failed/);

    expect(() => db.setEncryptionKey(key, { format: 2 as any })).toThrow(/format 2/);
    expect(() => db.setEncryptionKey(key.subarray(0, 16))).toThrow(/32 bytes/);
    expect(db.isEncrypted()).toBe(false);
    db.destroy();
  });

  test('format-3 round trip: stored bytes are HKDF-SHA256/AES-256-CTR per record, SQL reads plaintext', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const { record, bfbs } = await fixtures();
    const key = new Uint8Array(32).map((_, i) => 0xa0 + i);

    const db = flatsql.createDatabase(SCHEMA, 'db-key-wasm');
    db.registerFileId('SECR', 'Secret');
    db.setEncryptionKey(key);
    expect(db.isEncrypted()).toBe(true);

    // One layout, one plaintext, one key: sequences 1..RECORDS.
    for (let i = 1; i <= RECORDS; i++) {
      expect(db.ingestOneEncrypted(record, bfbs)).toBe(i);
    }

    const stored = streamRecords(db.exportData());
    expect(stored.length).toBe(RECORDS);
    const ciphertexts = new Set<string>();
    stored.forEach((bytes, i) => {
      const sequence = i + 1;
      // The stored record is the reference encryption for its sequence.
      expect(hex(bytes)).toBe(hex(referenceRecord(record, key, sequence)));
      // Only the (encrypted) values changed.
      const ranges = encryptedRanges(record);
      const inRange = (j: number) => ranges.some(([p, n]) => j >= p && j < p + n);
      let changed = 0;
      for (let j = 0; j < record.length; j++) {
        if (bytes[j] !== record[j]) {
          expect(inRange(j)).toBe(true);
          changed++;
        }
      }
      expect(changed).toBeGreaterThan(0);
      ciphertexts.add(ranges.map(([p, n]) => hex(bytes.subarray(p, p + n))).join('/'));
      // encryptBuffer/decryptBuffer with the sequence agree with the store.
      expect(hex(db.decryptBuffer(bytes, bfbs, sequence))).toBe(hex(record));
      expect(hex(db.encryptBuffer(record, bfbs, sequence))).toBe(hex(bytes));
    });
    // Every record has its own key stream.
    expect(ciphertexts.size).toBe(RECORDS);

    // SQL decrypts with each record's sequence.
    const rows = db.query('SELECT serial, name, code, pin FROM Secret ORDER BY _rowid').rows;
    expect(rows.length).toBe(RECORDS);
    for (const row of rows) {
      expect(row[0]).toBe(PLAIN.serial);
      expect(row[1]).toBe(PLAIN.name);
      expect(row[2]).toBe(PLAIN.code);
      expect(Number(row[3])).toBe(PLAIN.pin);
    }
    const count = db.query('SELECT COUNT(*) FROM Secret WHERE code = ?', [PLAIN.code]).rows;
    expect(Number(count[0][0])).toBe(RECORDS);

    // The stream carries the index: replayed into an empty database with the
    // key and format 3 declared, SQL reads plaintext.
    const replay = flatsql.createDatabase(SCHEMA, 'db-key-wasm-replay');
    replay.registerFileId('SECR', 'Secret');
    replay.loadAndRebuild(db.exportData());
    expect(() => replay.setEncryptionKey(key)).toThrow(/declare their field-encryption format/);
    replay.setEncryptionKey(key, { format: 3 });
    const replayed = replay.query('SELECT code, pin FROM Secret ORDER BY _rowid').rows;
    expect(replayed.length).toBe(RECORDS);
    expect(replayed.every((row) => row[0] === PLAIN.code && Number(row[1]) === PLAIN.pin)).toBe(true);
    replay.destroy();
    db.destroy();
  });

  test('HMAC-SHA256 under the database key', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const key = new Uint8Array(32).map((_, i) => i + 1);
    const plain = flatsql.createDatabase('table P { a: int; } root_type P;', 'db-key-wasm-hmac');
    plain.setEncryptionKey(key);
    expect(plain.isEncrypted()).toBe(true);
    const buffer = new Uint8Array(1000).map((_, i) => (i * 7) & 0xff);
    const mac = plain.computeHMAC(buffer);
    expect(hex(mac)).toBe(crypto.createHmac('sha256', key).update(buffer).digest('hex'));
    expect(plain.verifyHMAC(buffer, mac)).toBe(true);
    const tampered = buffer.slice();
    tampered[500] ^= 1;
    expect(plain.verifyHMAC(tampered, mac)).toBe(false);
    plain.destroy();
  });
});
