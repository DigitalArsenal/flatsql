#!/usr/bin/env python3
"""Seed corpus for the terabyte harness, from a legacy (format 1) record store.

    scripts/tb/extract-corpus.py --store <legacy store dir> --out <dir>
        [--types <migrated store>/fsql2/t] [--per-type OMM=400000,CAT=200000,MPE=400000,IQC=100000]

Reads <store>/control.flatsqldb read-only (SQLite URI mode=ro&immutable=1:
the file is never written, not even its WAL; run it on an APFS clone or a
copy anyway). For every routed (producer, standard) table, sds_p_<token>__<STD>,
it samples up to the per-type count of records spread over the table (every
k-th rowid), keeps the plaintext FlatBuffers whose file identifier is the
standard's, drops CIDs seen in an earlier table, and joins each record's first
source tag (sdn_record_source_tags: provider, source, batch).

Writes <out>/corpus.tbc:
    "TBC1" u32 version=1 u64 count
    count x { fid[4] u32 len bytes[len]
              5 x (u16 len, bytes): peer, provider, source, batch, supersede key }
and, with --types, copies each type's production config t/<fid>/s-*.fsc to
<out>/types/ (the harness registers the types with them).
"""
import argparse
import os
import shutil
import sqlite3
import struct
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--store', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--types', default='')
    ap.add_argument('--per-type', default='OMM=400000,CAT=200000,MPE=400000,IQC=100000')
    a = ap.parse_args()
    limits = {}
    for kv in a.per_type.split(','):
        k, v = kv.split('=')
        limits[k.strip()] = int(v)
    os.makedirs(a.out, exist_ok=True)
    db = os.path.join(a.store, 'control.flatsqldb')
    con = sqlite3.connect(f'file:{db}?mode=ro&immutable=1', uri=True)
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'sds\\_p\\_%' ESCAPE '\\' "
        "AND COALESCE(sql, '') NOT LIKE 'CREATE VIRTUAL%' ORDER BY name")]
    have_tags = con.execute(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'sdn_record_source_tags'").fetchone()[0]
    seen = set()
    taken = {}
    out_path = os.path.join(a.out, 'corpus.tbc')
    tmp = out_path + '.tmp'
    n = 0
    total_bytes = 0
    with open(tmp, 'wb') as f:
        f.write(b'TBC1' + struct.pack('<IQ', 1, 0))
        for t in tables:
            body = t[len('sds_p_'):]
            i = body.rfind('__')
            if i <= 0:
                continue
            std = body[i + 2:]
            want = limits.get(std, 0) - taken.get(std, 0)
            if want <= 0:
                continue
            count, lo, hi = con.execute(f'SELECT count(*), min(rowid), max(rowid) FROM "{t}"').fetchone()
            if not count:
                continue
            stride = max(1, count // want)
            fid = ('$' + std).encode()[:4].ljust(4, b'\0')
            rows = con.execute(
                f'SELECT cid, data, peer_id, COALESCE(hex(supersede_key), \'\') FROM "{t}" '
                f'WHERE (rowid - ?) % ? = 0 ORDER BY rowid LIMIT ?', (lo, stride, want))
            got = 0
            for cid, data, peer, skey in rows:
                if cid in seen or data is None or len(data) < 8 or bytes(data[4:8]) != fid:
                    continue  # a copy another table holds, or sealed / not this standard
                seen.add(cid)
                provider = source = batch = ''
                if have_tags:
                    tag = con.execute(
                        'SELECT provider_id, source_name, batch_id FROM sdn_record_source_tags '
                        'WHERE schema_name = ? AND cid = ? LIMIT 1', (std + '.fbs', cid)).fetchone()
                    if tag:
                        provider, source, batch = tag
                sk = bytes.fromhex(skey) if skey else b''
                f.write(fid + struct.pack('<I', len(data)) + bytes(data))
                for s in (peer.encode(), provider.encode(), source.encode(), batch.encode(), sk):
                    s = s[:65535]
                    f.write(struct.pack('<H', len(s)) + s)
                got += 1
                total_bytes += len(data)
            taken[std] = taken.get(std, 0) + got
            n += got
            print(f'{t}: {got} of {count} records (every {stride}th rowid)', file=sys.stderr)
        f.seek(8)
        f.write(struct.pack('<Q', n))
    os.replace(tmp, out_path)
    print(f'corpus {out_path}: {n} records, {total_bytes} FlatBuffer bytes; per type {taken}', file=sys.stderr)
    if a.types:
        tdir = os.path.join(a.out, 'types')
        os.makedirs(tdir, exist_ok=True)
        for fidhex in sorted(os.listdir(a.types)):
            d = os.path.join(a.types, fidhex)
            cfgs = sorted((os.path.getmtime(os.path.join(d, x)), x) for x in os.listdir(d)
                          if x.startswith('s-') and x.endswith('.fsc'))
            if not cfgs:
                continue
            name = cfgs[-1][1]  # the newest config of the type
            shutil.copyfile(os.path.join(d, name), os.path.join(tdir, f'{fidhex}-{name}'))
            print(f'type config {fidhex}/{name}', file=sys.stderr)


if __name__ == '__main__':
    main()
