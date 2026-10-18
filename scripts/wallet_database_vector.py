"""Open a wallet-keyed FlatSQL database outside the browser (Python `cryptography`).

Independent of the page's code: SLIP-10 from the wallet seed to the
contact-encryption X25519 key (m/44'/0'/<account>'/1'/0'), the SDS $ENC and
$KMF records parsed by hand, the content key unwrapped (Unified ECIES: X25519,
HKDF-SHA256, AES-256-CTR on KMF KEY_BYTES, field 4, record 0), then each
exported Contact record's (encrypted) strings decrypted under field-encryption
format 3: K = HKDF-SHA256(contentKey, "flatbuffers-buffer-v3" || BE32(seq)),
IV = BE32(position of the value's first byte) || 12 zero bytes.

stdin: JSON {"seedHex", "account", "encHex", "kmfHex", "exportHex"}.
stdout: JSON {"publicKeyHex", "context", "contentKeyHex", "rows": [{"id", "name", "email",
"phone"}...]} or {"publicKeyHex", "refused": reason}.
"""

import hashlib
import hmac
import json
import struct
import sys

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

HARDENED = 0x80000000


def contact_key(seed: bytes, account: int) -> X25519PrivateKey:
    digest = hmac.new(b"ed25519 seed", seed, hashlib.sha512).digest()
    key, chain = digest[:32], digest[32:]
    for index in (44, 0, account, 1, 0):
        digest = hmac.new(chain, b"\x00" + key + struct.pack(">I", index | HARDENED), hashlib.sha512).digest()
        key, chain = digest[:32], digest[32:]
    return X25519PrivateKey.from_private_bytes(key)


class Table:
    """The root table of a finished FlatBuffer with a file identifier."""

    def __init__(self, buf: bytes, file_id: bytes):
        if buf[4:8] != file_id:
            raise ValueError(f"not a {file_id.decode()} record")
        self.buf = buf
        self.pos = struct.unpack_from("<I", buf, 0)[0]
        self.vtable = self.pos - struct.unpack_from("<i", buf, self.pos)[0]
        self.vt_len = struct.unpack_from("<H", buf, self.vtable)[0]

    def slot(self, i: int) -> int:
        at = self.vtable + 4 + 2 * i
        return struct.unpack_from("<H", self.buf, at)[0] if at < self.vtable + self.vt_len else 0

    def vector(self, i: int):
        """(position of the first byte, length), or None when absent."""
        off = self.slot(i)
        if not off:
            return None
        ref = self.pos + off
        start = ref + struct.unpack_from("<I", self.buf, ref)[0]
        return start + 4, struct.unpack_from("<I", self.buf, start)[0]

    def bytes(self, i: int) -> bytes:
        span = self.vector(i)
        return b"" if span is None else bytes(self.buf[span[0]:span[0] + span[1]])

    def scalar(self, i: int, fmt: str, default):
        off = self.slot(i)
        return struct.unpack_from(fmt, self.buf, self.pos + off)[0] if off else default


def hkdf(ikm: bytes, info: bytes, length: int) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=length, salt=None, info=info).derive(ikm)


def ctr(key: bytes, iv: bytes, data: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(key), modes.CTR(iv)).decryptor()
    return cipher.update(data) + cipher.finalize()


def unwrap(private: X25519PrivateKey, public: bytes, enc_bytes: bytes, kmf_bytes: bytes):
    enc = Table(enc_bytes, b"$ENC")
    if (enc.scalar(1, "<B", 0), enc.scalar(2, "<B", 0), enc.scalar(3, "<B", 0)) != (0, 0, 0):
        raise ValueError("unsupported $ENC suite")
    context = enc.bytes(7).decode("utf-8")
    if not context.startswith("sdn-field-encryption/v1|flatsql|"):
        raise ValueError("not a FlatSQL database-key context")
    if enc.bytes(6) != hashlib.sha256(public).digest()[:8]:
        raise ValueError("wrapped for another wallet")
    shared = private.exchange(X25519PublicKey.from_public_bytes(enc.bytes(4)))
    master = hkdf(shared, context.encode("utf-8"), 32)
    suffix = struct.pack(">HI", 4, 0)
    wrapped = Table(kmf_bytes, b"$KMF").bytes(4)
    if len(wrapped) != 32:
        raise ValueError("KMF KEY_BYTES is not a 32-byte key")
    return ctr(hkdf(master, b"flatbuffers-field" + suffix, 32), hkdf(master, b"flatbuffers-iv" + suffix, 16), wrapped), context


def records(stream: bytes):
    offset = 0
    while offset + 4 <= len(stream):
        size = struct.unpack_from("<I", stream, offset)[0]
        yield stream[offset + 4:offset + 4 + size]
        offset += 4 + size


def open_record(content_key: bytes, sequence: int, record: bytes) -> dict:
    table = Table(record, b"CNTC")
    key = hkdf(content_key, b"flatbuffers-buffer-v3" + struct.pack(">I", sequence), 32)

    def secret(slot: int) -> str:
        position, length = table.vector(slot)
        iv = struct.pack(">I", position) + bytes(12)
        return ctr(key, iv, record[position:position + length]).decode("utf-8")

    return {
        "id": table.scalar(0, "<i", 0),
        "name": table.bytes(1).decode("utf-8"),
        "email": secret(2),
        "phone": secret(3),
    }


def main() -> int:
    request = json.load(sys.stdin)
    private = contact_key(bytes.fromhex(request["seedHex"]), request.get("account", 0))
    public = private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    out = {"publicKeyHex": public.hex()}
    try:
        content_key, out["context"] = unwrap(private, public, bytes.fromhex(request["encHex"]), bytes.fromhex(request["kmfHex"]))
    except ValueError as error:
        out["refused"] = str(error)
        json.dump(out, sys.stdout)
        return 0
    out["contentKeyHex"] = content_key.hex()
    stream = bytes.fromhex(request.get("exportHex", ""))
    out["rows"] = [open_record(content_key, seq, rec) for seq, rec in enumerate(records(stream), start=1)]
    json.dump(out, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
