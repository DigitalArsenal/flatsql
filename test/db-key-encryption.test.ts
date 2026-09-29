import initFlatSQL from '../wasm/index.js';
import { FlatcRunner } from 'flatc-wasm';

// Database-key record encryption through the browser artifact's JS API.
// The format-3 round trip itself (independent key streams, SQL decryption,
// replay) is cpp/test/db_key_encryption_test.cpp: it needs a FlatBuffers
// crypto backend with HKDF-SHA256, which the wasm builds do not link.

const SCHEMA = `
  table Secret {
    serial: int (key);
    name: string;
    code: string (encrypted);
  }
  root_type Secret;
  file_identifier "SECR";
`;

async function fixtures() {
  const flatc = await FlatcRunner.init();
  const schemaInput = { entry: '/secret.fbs', files: { '/secret.fbs': SCHEMA } };
  // generateBinary writes a size-prefixed buffer; a record has no prefix.
  const prefixed = new Uint8Array(
    flatc.generateBinary(schemaInput, JSON.stringify({ serial: 7, name: 'public', code: 'secret' }))
  );
  expect(new DataView(prefixed.buffer, prefixed.byteOffset).getUint32(0, true)).toBe(prefixed.length - 4);
  const record = prefixed.slice(4);
  expect(new TextDecoder().decode(record.subarray(4, 8))).toBe('SECR');
  flatc.mountFile('/secret.fbs', SCHEMA);
  const run = flatc.runCommand(['-b', '--schema', '--bfbs-builtins', '-o', '/out', '/secret.fbs']);
  expect(run.code).toBe(0);
  const bfbs = flatc.readFile('/out/secret.bfbs', { encoding: 'binary' }) as Uint8Array;
  return { record, bfbs };
}

describe('database-key encryption (wasm)', () => {
  test('record indexes are sequences; format 2 and weak key derivation are refused', async () => {
    const flatsql = await initFlatSQL({ skipIntegrityCheck: true });
    const { record, bfbs } = await fixtures();
    const key = new Uint8Array(32).map((_, i) => i + 1);

    const db = flatsql.createDatabase(SCHEMA, 'db-key-wasm');
    db.registerFileId('SECR', 'Secret');

    // A record index is the record's sequence: missing, 0 or past 2^32 - 1
    // is refused before the engine is called.
    expect(() => (db as any).encryptBuffer(record, bfbs)).toThrow(/recordIndex must be the record's sequence/);
    expect(() => db.decryptBuffer(record, bfbs, 0)).toThrow(/recordIndex/);
    expect(() => db.encryptBuffer(record, bfbs, 2 ** 32)).toThrow(/recordIndex/);

    // No key yet.
    expect(() => db.ingestOneEncrypted(record, bfbs)).toThrow(/No encryption key set/);
    expect(() => db.encryptBuffer(record, bfbs, 1)).toThrow(/No encryption key set/);

    // Format 2 is refused whatever the build.
    expect(() => db.setEncryptionKey(key, { format: 2 as any })).toThrow(/format 2/);
    expect(() => db.setEncryptionKey(key.subarray(0, 16))).toThrow(/32 bytes/);

    let available = true;
    try {
      db.setEncryptionKey(key);
    } catch (error) {
      available = false;
      // The wasm builds compile the FlatBuffers fallback backend, whose key
      // derivation is not HKDF-SHA256: a table with (encrypted) columns
      // refuses a key rather than share key streams across records.
      expect((error as Error).message).toMatch(/HKDF-SHA256/);
    }
    if (available) {
      throw new Error(
        'this wasm build has record encryption: add the format-3 round trip here ' +
          '(cpp/test/db_key_encryption_test.cpp)'
      );
    }
    expect(db.isEncrypted()).toBe(false);
    expect(() => db.ingestOneEncrypted(record, bfbs)).toThrow(/No encryption key set/);
    db.destroy();

    // A schema without (encrypted) columns takes a key (HMAC).
    const plain = flatsql.createDatabase('table P { a: int; } root_type P;', 'db-key-wasm-plain');
    plain.setEncryptionKey(key);
    expect(plain.isEncrypted()).toBe(true);
    plain.destroy();
  });
});
