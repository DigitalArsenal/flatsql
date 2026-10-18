/**
 * A database content key wrapped to the signed-in wallet.
 *
 * The recipient is the wallet's own contact-encryption key: X25519 at SLIP-10
 * m/44'/0'/0'/1'/0', read from the public identity the wallet client reports.
 * The page never holds a wallet secret.
 *
 * The wrap is the SDN Unified ECIES envelope (sdn-server internal/ecies Wrap,
 * docs/UNIFIED_ECIES.md): an SDS `$ENC` header and a `$KMF` record whose
 * KEY_BYTES (field 4, record 0) carries the wrapped key:
 *
 *   Z        = X25519(ephemeral, recipient)
 *   K        = HKDF-SHA256(Z, salt = none, info = CONTEXT)
 *   CONTEXT  = "sdn-field-encryption/v1|flatsql|" table "|db|k" version
 *   fieldKey = HKDF-SHA256(K, "flatbuffers-field" || BE16(4) || BE32(0))
 *   fieldIV  = HKDF-SHA256(K, "flatbuffers-iv"    || BE16(4) || BE32(0))
 *   KEY_BYTES = AES-256-CTR(fieldKey, fieldIV) XOR contentKey
 *
 * Only the recipient's wallet can unwrap it again (sdn.wallet.key-unwrap.v1).
 */

const WALLET_RECORD = Symbol.for('sdn.wallet.public-client.v1');
const LABEL = /^[A-Za-z0-9_.-]{1,64}$/u;
const HEX32 = /^[0-9a-f]{64}$/u;
const KMF_KEY_BYTES_FIELD = 4;

export const CONTEXT_PREFIX = 'sdn-field-encryption/v1';

export function databaseContext(table, version = 1) {
    if (typeof table !== 'string' || !LABEL.test(table)) throw new Error('invalid table label');
    if (!Number.isInteger(version) || version < 1) throw new Error('invalid key version');
    return `${CONTEXT_PREFIX}|flatsql|${table}|db|k${version}`;
}

function hexToBytes(hex) {
    const out = new Uint8Array(hex.length / 2);
    for (let i = 0; i < out.length; i += 1) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    return out;
}

export function bytesToHex(bytes) {
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

/** The signed-in wallet's encryption key, or null when signed out. */
export function walletEncryptionKey(snapshot) {
    if (snapshot?.status !== 'connected' || !snapshot.identity) return null;
    const key = snapshot.identity.keys?.find((k) => k.purpose === 'contact-encryption' && k.curve === 'x25519');
    if (!key || !HEX32.test(key.publicKeyHex)) return null;
    return Object.freeze({ publicKey: hexToBytes(key.publicKeyHex), publicKeyHex: key.publicKeyHex, path: key.path });
}

function walletRecord() {
    return globalThis.document?.[WALLET_RECORD] ?? null;
}

/**
 * Call listener(key | null) now and whenever the wallet signs in or out.
 * The SDN stack nav owns the wallet client; it may start after this module.
 */
export function watchWallet(listener) {
    let attached = false;
    const attach = () => {
        const record = walletRecord();
        if (!record || attached) return Boolean(record);
        attached = true;
        record.addPresenter({ render: () => listener(walletEncryptionKey(record.getSnapshot())) });
        listener(walletEncryptionKey(record.getSnapshot()));
        return true;
    };
    listener(null);
    if (!attach()) {
        globalThis.addEventListener?.('DOMContentLoaded', attach, { once: true });
        globalThis.addEventListener?.('load', attach, { once: true });
    }
}

/** Open the wallet's sign-in. Resolves false when the page has no wallet client. */
export async function signIn() {
    const record = walletRecord();
    if (!record) return false;
    await record.client.connect();
    return true;
}

// ---- SDS tables, built by hand (this site carries no FlatBuffers library) ----

/**
 * One finished FlatBuffer: a root table of `slots` vtable entries holding the
 * given fields, each { slot, u8 }, { slot, i32 }, { slot, bytes } or
 * { slot, string }. Vectors and strings follow the table, 4-byte aligned.
 */
export function encodeTable(fileId, slots, fields) {
    const words = fields.filter((f) => !('u8' in f));
    const scalars = fields.filter((f) => 'u8' in f);
    const vtableSize = 4 + 2 * slots;
    const vtable = 8;
    const table = vtable + ((vtableSize + 3) & ~3);
    const tableSize = (4 + 4 * words.length + scalars.length + 3) & ~3;
    let end = table + tableSize;
    const vectors = words.map((f) => {
        if ('i32' in f) return null;
        const data = 'string' in f ? new TextEncoder().encode(f.string) : f.bytes;
        const at = end;
        end = (at + 4 + data.length + ('string' in f ? 1 : 0) + 3) & ~3;
        return { at, data };
    });
    const out = new Uint8Array(end);
    const view = new DataView(out.buffer);
    view.setUint32(0, table, true);
    out.set(new TextEncoder().encode(fileId), 4);
    view.setUint16(vtable, vtableSize, true);
    view.setUint16(vtable + 2, tableSize, true);
    view.setInt32(table, table - vtable, true);
    words.forEach((f, i) => {
        const at = table + 4 + 4 * i;
        view.setUint16(vtable + 4 + 2 * f.slot, at - table, true);
        if ('i32' in f) {
            view.setInt32(at, f.i32, true);
            return;
        }
        view.setUint32(at, vectors[i].at - at, true);
        view.setUint32(vectors[i].at, vectors[i].data.length, true);
        out.set(vectors[i].data, vectors[i].at + 4);
    });
    scalars.forEach((f, i) => {
        const at = table + 4 + 4 * words.length + i;
        view.setUint16(vtable + 4 + 2 * f.slot, at - table, true);
        out[at] = f.u8;
    });
    return out;
}

/** SDS `$ENC`: VERSION 1, X25519 / AES-256-CTR / HKDF-SHA256 (the enum defaults). */
function encodeENC({ ephemeralPublicKey, nonceStart, recipientKeyId, context }) {
    return encodeTable('$ENC', 8, [
        { slot: 0, u8: 1 },
        { slot: 4, bytes: ephemeralPublicKey },
        { slot: 5, bytes: nonceStart },
        { slot: 6, bytes: recipientKeyId },
        { slot: 7, string: context },
    ]);
}

/** SDS `$KMF` holding only KEY_BYTES. */
function encodeKMF(keyBytes) {
    return encodeTable('$KMF', 5, [{ slot: KMF_KEY_BYTES_FIELD, bytes: keyBytes }]);
}

// ---- Unified ECIES wrap (WebCrypto) ----

const subtle = () => globalThis.crypto.subtle;

async function hkdf(ikm, info, length) {
    const key = await subtle().importKey('raw', ikm, 'HKDF', false, ['deriveBits']);
    const bits = await subtle().deriveBits({ name: 'HKDF', hash: 'SHA-256', salt: new Uint8Array(0), info }, key, length * 8);
    return new Uint8Array(bits);
}

function fieldInfo(label, fieldId, recordIndex) {
    const head = new TextEncoder().encode(label);
    const info = new Uint8Array(head.length + 6);
    info.set(head);
    const view = new DataView(info.buffer);
    view.setUint16(head.length, fieldId);
    view.setUint32(head.length + 2, recordIndex);
    return info;
}

/**
 * Wrap a 32-byte content key to a recipient X25519 key. Returns the `$ENC`
 * and `$KMF` bytes; the ephemeral secret never leaves WebCrypto.
 */
export async function wrapContentKey(recipientPublicKey, contentKey, context) {
    if (contentKey.length !== 32) throw new Error('a content key is 32 bytes');
    const ephemeral = await subtle().generateKey({ name: 'X25519' }, false, ['deriveBits']);
    const recipient = await subtle().importKey('raw', recipientPublicKey, { name: 'X25519' }, false, []);
    const shared = new Uint8Array(await subtle().deriveBits({ name: 'X25519', public: recipient }, ephemeral.privateKey, 256));
    const master = await hkdf(shared, new TextEncoder().encode(context), 32);
    shared.fill(0);
    const fieldKey = await hkdf(master, fieldInfo('flatbuffers-field', KMF_KEY_BYTES_FIELD, 0), 32);
    const fieldIV = await hkdf(master, fieldInfo('flatbuffers-iv', KMF_KEY_BYTES_FIELD, 0), 16);
    master.fill(0);
    const aes = await subtle().importKey('raw', fieldKey, 'AES-CTR', false, ['encrypt']);
    fieldKey.fill(0);
    const wrapped = new Uint8Array(await subtle().encrypt({ name: 'AES-CTR', counter: fieldIV, length: 128 }, aes, contentKey));
    const recipientKeyId = new Uint8Array(await subtle().digest('SHA-256', recipientPublicKey)).slice(0, 8);
    const enc = encodeENC({
        ephemeralPublicKey: new Uint8Array(await subtle().exportKey('raw', ephemeral.publicKey)),
        nonceStart: globalThis.crypto.getRandomValues(new Uint8Array(12)),
        recipientKeyId,
        context,
    });
    return { enc, kmf: encodeKMF(wrapped), context };
}
