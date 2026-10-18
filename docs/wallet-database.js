/**
 * An encrypted FlatSQL database keyed to the signed-in wallet.
 *
 * Each database gets a fresh random 32-byte content key. The key is wrapped
 * to the wallet's contact-encryption key ($ENC + $KMF, wallet-field-keys.js),
 * then handed to the engine as its database key: the (encrypted) columns of
 * every record are stored under field-encryption format 3 (one key per
 * record, from the content key and the record's sequence) and SQL reads them
 * decrypted in this session. The page keeps no copy of the content key.
 *
 * The page saves the database in this browser as ciphertext: the exported
 * records and the wrapped key, nothing that opens them. Reopening it in a later
 * session asks the wallet to unwrap the content key (unwrapKey, after the user
 * approves); the engine then reads the records under it again.
 */

import { databaseContext, encodeTable, unwrapContentKey, wrapContentKey } from './wallet-field-keys.js';

export const TABLE = 'Contact';
export const FILE_ID = 'CNTC';
export const SCHEMA = `
table Contact {
  id: int (key);
  name: string;
  email: string (encrypted);
  phone: string (encrypted);
}
root_type Contact;
file_identifier "CNTC";
`;

// SCHEMA as a binary schema with builtin attributes (flatc -b --schema
// --bfbs-builtins): ingestOneEncrypted finds the (encrypted) fields in it.
const SCHEMA_BFBS_BASE64 =
    'GAAAAEJGQlMQABwABAAIAAwAEAAUABgAEAAAADQAAAAsAAAAHAAAABAAAABAAAAABAAAAAAAAAAAAAAAAAAAAAQAAABDTlRDAAAAAAAAAAABAAAAGAAAABQAFAAEAAgAAAAMAAAAAAAAABAAFAAAACwAAAAUAAAAAQAAAAQAAAAAAAAAAAAAAAQAAACUAAAATAEAAAQBAAAQAAAABwAAAENvbnRhY3QApP///wAAAAEDAAoARAAAADQAAAAEAAAAAQAAAAQAAADQ/v//EAAAAAQAAAABAAAAMAAAAAkAAABlbmNyeXB0ZWQAAADQ/v//AAAADQEAAAAFAAAAcGhvbmUAAAAcABgADAAQAAgACgAAAAAAAAAAAAAAFAAAAAcAHAAAAAAAAAECAAgARAAAADQAAAAEAAAAAQAAAAQAAABI////EAAAAAQAAAABAAAAMAAAAAkAAABlbmNyeXB0ZWQAAABI////AAAADQEAAAAFAAAAZW1haWwAAAAcABQADAAQAAgACgAAAAAAAAAAAAAAAAAAAAcAHAAAAAAAAAEBAAYAFAAAAAQAAACQ////AAAADQEAAAAEAAAAbmFtZQAAAAAYABQACAAMAAAABgAAAAAAAAAAAAUAEAAYAAAAAAEEAFQAAABEAAAABAAAAAEAAAAMAAAACAAMAAQACAAIAAAAEAAAAAQAAAABAAAAMAAAAAMAAABrZXkAEAAMAAcAAAAAAAAAAAAIABAAAAAAAAAHAQAAAAIAAABpZAAA';

const SCHEMA_BFBS = Uint8Array.from(atob(SCHEMA_BFBS_BASE64), (c) => c.charCodeAt(0));

const NAMES = ['Ada Okafor', 'Bo Lindqvist', 'Chen Wei', 'Dana Ruiz', 'Eli Haddad', 'Farah Naqvi', 'Gus Moreau', 'Hana Sato'];

function contactRecord(id) {
    const name = NAMES[id % NAMES.length];
    return encodeTable(FILE_ID, 4, [
        { slot: 0, i32: id },
        { slot: 1, string: name },
        { slot: 2, string: `${name.split(' ')[0].toLowerCase()}.${id}@example.org` },
        { slot: 3, string: `+1-555-${String(1000 + id).padStart(4, '0')}` },
    ]);
}

/**
 * Create a database of `count` contacts encrypted under a new content key
 * wrapped to `walletKey` (the signed-in wallet's encryption key). Returns the
 * open database (the caller destroys it) and the key envelope.
 */
export async function createWalletDatabase(flatsql, walletKey, count) {
    const contentKey = globalThis.crypto.getRandomValues(new Uint8Array(32));
    let db = null;
    try {
        const envelope = await wrapContentKey(walletKey.publicKey, contentKey, databaseContext(TABLE));
        db = flatsql.createDatabase(SCHEMA, 'wallet-keyed');
        db.registerFileId(FILE_ID, TABLE);
        db.setEncryptionKey(contentKey);
        for (let id = 1; id <= count; id += 1) db.ingestOneEncrypted(contactRecord(id), SCHEMA_BFBS);
        return { db, envelope };
    } catch (err) {
        db?.destroy();
        throw err;
    } finally {
        contentKey.fill(0);
    }
}

// ---- Saved in this browser: ciphertext only ----

const SAVED = 'flatsql.wallet-database.v1';
const toBase64 = (bytes) => btoa(Array.from(bytes, (b) => String.fromCharCode(b)).join(''));
const fromBase64 = (text) => Uint8Array.from(atob(text), (c) => c.charCodeAt(0));

/** Save the open database's records (encrypted at rest) and its wrapped key. */
export function saveWalletDatabase({ db, envelope }) {
    const saved = { context: envelope.context, enc: toBase64(envelope.enc), kmf: toBase64(envelope.kmf), records: toBase64(db.exportData()) };
    try {
        globalThis.localStorage.setItem(SAVED, JSON.stringify(saved));
        return true;
    } catch {
        return false;
    }
}

/** The saved database, or null. */
export function savedWalletDatabase() {
    try {
        const saved = JSON.parse(globalThis.localStorage.getItem(SAVED) ?? 'null');
        if (!saved) return null;
        return { envelope: { context: saved.context, enc: fromBase64(saved.enc), kmf: fromBase64(saved.kmf) }, records: fromBase64(saved.records) };
    } catch {
        return null;
    }
}

/**
 * Reopen the saved database: the wallet unwraps its content key (asking the
 * user), the engine loads the encrypted records under it.
 */
export async function reopenWalletDatabase(flatsql, saved) {
    const contentKey = await unwrapContentKey(saved.envelope);
    let db = null;
    try {
        db = flatsql.createDatabase(SCHEMA, 'wallet-keyed');
        db.registerFileId(FILE_ID, TABLE);
        db.setEncryptionKey(contentKey);
        db.loadAndRebuild(saved.records);
        return { db, envelope: saved.envelope };
    } catch (err) {
        db?.destroy();
        throw err;
    } finally {
        contentKey.fill(0);
    }
}
