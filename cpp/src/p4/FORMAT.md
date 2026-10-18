# FlatSQL feed format: linear FlatBuffers, in-situ IPFS (store format 5)

Status: **approved by the owner, 2026-10-18**, with the four decisions in §13.
This file is the frozen spec for store format 5. Format 4 (as built) is
`docs/STORE-FORMAT-4.md`. When format 5 lands, that file is replaced by a
format-5 one that links here for the bytes.

Scope: a feed's bytes on disk, what an acknowledgement promises, how a feed is
rebuilt from its own stream, and how a feed maps to IPLD (per-record CIDs,
the feed root DAG, CARv2 backups). Out of scope: the node's Kubo side (filestore
registration, providing, pin policy). That is listed in §11 as the sdn work it
needs.

## 0. Principles

1. **The stream is the only authority.** Each feed is one linear stream of
   `[u32 LE size][FlatBuffer]` frames, appended in order. Everything needed to
   rebuild the feed is in those frames: the records, their metadata, and the
   checkpointed state. Everything else is a cache derived from the stream:
   the SQLite index, the type index, the views, the IPLD link nodes and CARs.
   Any of it can be deleted and rebuilt.
2. **Records are verbatim.** A record frame holds exactly the bytes that arrived.
   No envelope, header or trailer is ever put around a record. Metadata goes
   in sibling frames.
3. **One record is one contiguous byte range.** The raw-leaf CIDv1 (sha2-256)
   of that range is the record's IPFS block. Ranges are never changed while a
   filestore entry can name them.
4. **Every frame is a standard FlatBuffer.** Each frame is identified by its
   4-byte file identifier. The non-record frames are SDS tables (§3). A stock
   FlatBuffers reader walks the file from byte 0 and dispatches on the
   identifier. It needs no FlatSQL code.
5. **No back-compat.** Format-4 stores are refused with `P4_E_FORMAT`, and nodes
   rebuild by re-sync (stack AGENTS.md).
6. **Each value lives in exactly one place** (owner, 2026-10-18). A value that
   can be re-derived from the record bytes is not stored again: epoch and object
   key are re-extracted on rebuild. `$FMB` holds only what the bytes cannot
   give. Checkpoint segments are derived snapshots that can be discarded; the
   authoritative data is the records plus their `$FMB` frames.

## 1. Files

```
<data>/fsql4/
  STORE                    64 B, magic FSQ4, format 5 (format 4 => P4_E_FORMAT)
  T/<TYPE>.spec            registered spec TLV (unchanged)
  T/<TYPE>.idx             type index: CACHE (feed and token registries, rebuilt from the streams)
  T/<TYPE>.fts             full text: CACHE (unchanged)
  P/<TYPE>/<feed>[.<gen>].fsdata   THE FEED: the linear frame stream (§2)
  P/<TYPE>/<feed>.db       SQLite index: CACHE (optional per workload once FSG segments land, §7)
```

The changes from format 4:
- The stream carries metadata, checkpoint and segment frames beside the records.
- The `.db` and `.idx` stop being needed for durability.
- `inst`, `tokc`, `ident`/`idst`, `moved` and `meta` become derived state.

File naming and generations are unchanged (`docs/STORE-FORMAT-4.md` §1, §5).

## 2. The stream

### 2.1 Frame bytes

```
frame at offset F:
  F+0  u32 LE  N            size of the FlatBuffer; no flag bits, N >= 8
  F+4  N bytes FlatBuffer   [u32 root uoffset][4-byte file identifier][...]
next frame at F + 4 + N; the file ends exactly at a frame end.
```

- There is no file header, magic, padding, CRC or trailer.
- A frame's **offset** is `F`, the offset of its size prefix.
- Its **range** is `[F+4, F+4+N)`, the FlatBuffer bytes only.
- The identifier at `F+8..F+12` (bytes 4..8 of the FlatBuffer) names the kind:

| Kind | Identifier | Written when | Holds |
|---|---|---|---|
| record | the type's (`$OMM`, `$WXF`, ...) or a sealed envelope | a NEW record or new bytes enter the feed | the record bytes exactly as they arrived |
| metadata | `$FMB` | once per feed per commit round, after that round's record frames; also alone for removals | the round's rows, dictionary additions and removals (§3.1) |
| checkpoint | `$FCK` | at stream creation (genesis), then on the cadence in §6 | the feed's identity, counters and live segment list (§3.2) |
| segment | `$FSG` | at checkpoints and in LSM merges | an immutable sorted state segment (§3.3) |

- **Reserved identifiers.** A type cannot be registered with the identifier `FMB`,
  `FCK` or `FSG`. A record of another type is refused if it carries one of them
  (`P4_REJ_FID`, as today), so no record frame can pose as a metadata frame.
- **The commit chain.** Every non-record frame carries `SELF_OFFSET` (its own `F`)
  and `PREVIOUS` (the `F` of the non-record frame before it). Genesis has
  `PREVIOUS = 2^64-1`. Recovery walks this chain (§9). It never classifies a
  frame by its identifier alone.
- **Alignment.** Frames are back to back, so a FlatBuffer may start at 4 mod 8.
  - Record frames are read in place, as today.
  - Metadata, checkpoint and segment frames are read only at open, recovery,
    rebuild and DAG/CAR export. They are copied into an 8-aligned buffer before
    verification. No padding frames are written.

### 2.2 Record frames

- **Bytes.** `N` bytes, verbatim. A sealed record's frame is its sealed envelope
  (`SDF1`/`SDFN`).
- **The record CID.** The record CID is SDN's key and the stored CID. For an
  unsealed record it is `CIDv1(raw 0x55, sha2-256(range))`: text `bafkrei...`,
  binary `01 55 12 20 || digest`.
  - The ingest check `kVerifyCid` (default on) already computes this digest and
    refuses a mismatch (`P4_REJ_CID`). So the stored CID **is** the range's
    raw-leaf CID, with no extra hashing.
  - A type configured without `kVerifyCid` has the digest computed at write. A
    store never holds a record CID that differs from its range, except for
    sealed records.
- **Sealed records.** The record CID is the plaintext's (SDN's key, unchanged).
  The **leaf CID** is the ciphertext range's sha2-256. The metadata frame
  carries both (§3.1), and IPFS sees only the leaf CID. Encrypted feeds are
  addressed by ciphertext.
- **Records over 1 MiB (`N > 1,048,576`)** become a UnixFS file DAG (§4.2). The
  record CID stays the raw sha2-256 of the whole range (SDN's key). IPFS sees the
  DAG root and its 1 MiB raw leaves.
- **Shared frames.** A record's copies and instances share one frame when the
  bytes are equal (as format 4). A row that points at an earlier frame adds no
  record frame.

### 2.3 What a stock reader sees

- A loop from byte 0 reads `N`, slices `[F+4, F+4+N)`, reads the identifier and
  verifies with that root type.
- The SDS readers for `$FMB`/`$FCK`/`$FSG` (§3) decode the metadata.
- To list a feed's live records with no FlatSQL code, follow the newest
  checkpoint's segments, or replay the metadata frames (§9).
- `fsdata-reader-check.py` is extended to all four kinds.

## 3. Metadata, checkpoint and segment tables (proposed SDS addition)

SDS ruling (sds-oracle, 2026-10-18, against SDS HEAD 1.251.0; npm latest is
1.254.0; flatsql pins 1.206.0): **no existing table fits.**
- FSB is opaque by contract ("must not interpret DATA").
- PNM is a single-object announcement.
- DPM is a signed publication manifest with no per-record offset, seq or
  dictionary.
- REC wraps a record, which rule 0.2 forbids.

Proposed: three new standards, **FMB**, **FCK** and **FSG**, as an
additive SDS minor release (1.255.0 or later), followed by a flatsql pin bump.
The format-5 code builds against the released SDS. Neither tree carries an
internal schema.

The segment code is `FSG` (FlatSQL segment), not the `FIX` first proposed:
GPX already has a field named `FIX`, and REC's union scope refuses a table
with that name. FSG joins FSB and FSO in the FlatSQL family.

Conventions:
- Fixed-width per-row data is in structs (zero-copy vectors).
- Digests are `[ubyte]` vectors with a 32-byte stride, parallel to the struct
  vector. Fixed arrays in structs are avoided because some SDS languages lack
  them.
- Ids are feed-scoped, except `TOKEN` ids, which are type-scoped (C-12 ordering).
- `0` means "none" for every `*_ID`. `EPOCH = INT64_MIN` means no epoch.
- Offsets are u64 and local to the stream. They are never trusted across copies
  (§8).

### 3.1 `$FMB`: batch metadata (one per feed per commit round)

```fbs
// FMB: FlatSQL batch metadata. Sibling of the record frames it commits; never holds record bytes.
enum fmbDictKind : ubyte { TOKEN, NODE, BATCH, CONTENT_KEY, URL, OBJECT_KEY }
enum fmbRemoveReason : ubyte { DELETE, SUPERSEDE, CAT_SUPERSEDE, QUOTA, RETENTION, MOVED_OUT }

/// One row = one delivery of one record to this feed (rid = SEQ << 16 | ROW_J). 40 bytes.
/// No epoch or object key: both are re-extracted from the record bytes (principle 6).
struct FMBRow {
  OFFSET:ulong;          // F of the record frame (this round's, or an earlier shared frame)
  SEQ:ulong;             // the record's seq in the type's seq space
  SOURCE_TS:long;        // the copy's source timestamp: a PUT field (tag 53 entry), not in the bytes
  LENGTH:uint;           // N of the record frame
  GROUP:ushort;          // index into GROUPS
  ROW_J:ushort;          // j of the rid
  FLAGS:uint;            // bit0 NEW_FRAME (frame written this round), bit1 SEALED, bit2 LARGE,
                         // bit3 MOVED_IN (local -> feed, seq kept), bit4 COMPACTED (re-emitted by a compaction),
                         // bit5 COPY, bit6 RETAG, others 0
  RESERVED:uint;         // 0
}
/// What a call's rows share. 24 bytes.
struct FMBGroup {
  DELIVERED_AT:long;     // the feed's delivery time (format 4's `at`)
  NODE_ID:uint;          // NODE (producer, peer)
  BATCH_ID:uint;         // BATCH (batch, producer peer, producer key)
  CONTENT_KEY_ID:uint;
  URL_ID:uint;
}
struct FMBRemove { SEQ:ulong; ROW_J:uint; REASON:fmbRemoveReason; }   // ROW_J 0xFFFFFFFF: every row of SEQ
struct FMBUrl    { SEQ:ulong; ROW_J:uint; URL_ID:uint; }               // a DUP moving a row's url (C-3)
table FMBEntry   { KIND:fmbDictKind; ID:uint; TEXT:[string]; }         // TOKEN [token, peer]; NODE [producer, peer];
                                                                       // BATCH [batch, ppeer, pkey]; others [value]
table FMBSignature { ROW:uint; BYTES:[ubyte]; }                        // format 4's `x`
table FMBSealed  { ROW:uint; EPOCH:long; OBJECT_KEY_ID:uint;          // ciphertext cannot be re-extracted, so a sealed
                   LEAF_DIGEST:[ubyte]; COLUMNS:[ubyte]; }             // row's epoch/key live here only; LEAF_DIGEST:
                                                                       // ciphertext sha2-256; COLUMNS: format 4's `f`
table FMBIdentity { ROW:uint; HASH:[ubyte]; }                          // IQC ingest identity (32 B)
table FMBLarge   { ROW:uint; ROOT_CID:[ubyte]; ROOT_NODE:[ubyte]; LEAF_DIGESTS:[ubyte]; }  // §4.2

table FMB {
  SELF_OFFSET:ulong;
  PREVIOUS:ulong;            // F of the previous non-record frame
  FEED_ID:uint;
  COMMITTED_AT:long;         // the round's clock (restamps instances on removals, C-39 E5)
  TYPE_SEQ_HIGH:ulong;       // the type's highest seq issued so far (seqs never go back)
  ROWS:[FMBRow];             // in plan order; at most 65,536 per frame (a round splits beyond)
  DIGESTS:[ubyte];           // 32 B per NEW_FRAME row, in row order: the record CID's digest
                             // (a shared-frame row takes the digest of the frame it names)
  GROUPS:[FMBGroup];
  DICTIONARY:[FMBEntry];     // entries first used by this round (deltas)
  REMOVALS:[FMBRemove];
  URLS:[FMBUrl];
  SIGNATURES:[FMBSignature];
  SEALED:[FMBSealed];
  IDENTITIES:[FMBIdentity];
  LARGE:[FMBLarge];
}
root_type FMB;
file_identifier "$FMB";
```

What the vectors cover:
- `ROWS` and `GROUPS` hold every column of format 4's row `r`.
- `DICTIONARY` covers `node`, `batch`, `ckey`, `url` and the type's `tok`.
  `OBJECT_KEY` entries are written only for sealed rows. Other rows' object
  keys come from their bytes, with ids given in order of first appearance in
  the stream, so a rebuild gives the same ids.
- **Why the digest stays.** It is the commit's integrity check (§9) and the
  record's key. Without it, recovery could not tell a torn record frame from a
  good one, and a rebuild would have to re-hash every byte.
- `IDENTITIES` covers `ident`.
- `MOVED_IN` rows plus a `MOVED_OUT` removal in local cover `moved`.

`inst`, `tokc` and the counters are folds over the rows.

Format 4 changed four things in place. Each is now an appended frame:
- **DELETE, SUPERSEDE, CAT supersede, QUOTA, retention.** An `$FMB` with
  `REMOVALS` and no rows.
- **A DUP's url.** `URLS`.
- **A move.** The feed's `$FMB` with `MOVED_IN` rows first, then local's `$FMB`
  with `MOVED_OUT`. Recovery finishes a move that has only the first half.
- **A copy, retag or new instance.** Rows that point at the existing frame.

### 3.2 `$FCK`: checkpoint (small; the state lives in segments)

```fbs
enum fckSegmentKind : ubyte {          // the FSG kinds, §3.3
  DICTIONARY, LATEST, INSTANCES, TOKENS, TOMBSTONES,
  ROWS_BY_SEQ, ROWS_BY_OBJECT_EPOCH, ROWS_BY_CID, ROWS_BY_W, ROWS_BY_AT,   // item 5
  COLUMN = 64, ZONE_MAP = 65, ROLLUP = 66                                 // reserved: analytics, 64..127
}
struct FCKCounters {                   // format 4's meta counters
  ROWS:ulong; RECORDS:ulong; BYTES:ulong; RECORDS_NO_EPOCH:ulong;
  COPIES:ulong; COPY_BYTES:ulong; LIVE_FRAME_BYTES:ulong;
  MIN_SEQ:ulong; MAX_SEQ:ulong; MIN_EPOCH:long; MAX_EPOCH:long;
  MIN_TS:long; MAX_TS:long; MIN_AT:long; MAX_AT:long;
}
struct FCKSegment {                    // one live FSG frame. 48 bytes
  OFFSET:ulong; LENGTH:uint; KIND:fckSegmentKind; LEVEL:ubyte; RESERVED:ushort;
  MIN_SEQ:ulong; MAX_SEQ:ulong; COVERS_THROUGH:ulong; ENTRIES:ulong;
}
table FCK {
  SELF_OFFSET:ulong;
  PREVIOUS:ulong;
  FEED_ID:uint;
  TYPE:string; PROVIDER:string; SOURCE:string;   // the feed's identity: the type index is rebuilt from these
  GENERATION:uint;
  COMMITTED_AT:long;
  APPLIED_THROUGH:ulong;      // end (F+4+N) of the last FMB folded into the segments
  TYPE_SEQ_HIGH:ulong;
  COUNTERS:FCKCounters;
  SEGMENTS:[FCKSegment];
  SEGMENT_DIGESTS:[ubyte];    // 32 B per segment: its raw-leaf CID digest
  COMMITS_ROOT_CID:[ubyte];   // the commits tree (§4.3) as of APPLIED_THROUGH
  PREVIOUS_FEED_ROOT_CID:[ubyte];
}
root_type FCK;
file_identifier "$FCK";
```

- **Genesis.** A new stream's first frame is an `$FCK` with zero counters and no
  segments. A stream names its own feed, and the type index is a cache.
- **Latest per object.** Kept in the `LATEST` segments, not in the `$FCK` itself
  (run 3's suggestion, made LSM so the checkpoint stays small). Each segment is
  sorted by object id, and the newest entry wins across segments.

### 3.3 `$FSG`: immutable state segment

```fbs
struct FSGPair {             // the one sorted entry shape. 32 bytes
  KEY_HI:ulong; KEY_LO:ulong;
  RID:ulong;                 // SEQ << 16 | ROW_J
  ROW_REF:ulong;             // (F of the FMB holding the row) << 16 | row index in it
}
struct FSGLatest { OBJECT_KEY_ID:uint; RESERVED:uint; EPOCH:long; RID:ulong; ROW_REF:ulong; }  // 32 B
struct FSGInstance {         // format 4's inst row. 88 B
  BATCH_ID:uint; CONTENT_KEY_ID:uint; RECORDS:ulong; BYTES:ulong; MIN_W:long; MAX_W:long;
  MIN_SEQ:ulong; MAX_SEQ:ulong; FIRST:long; UPDATED:long; MAX_AT:long; MAX_TS:long;
}
table FSG {
  SELF_OFFSET:ulong;
  PREVIOUS:ulong;
  FEED_ID:uint;
  KIND:ubyte;                // fckSegmentKind
  LEVEL:ubyte;
  MIN_SEQ:ulong; MAX_SEQ:ulong;
  COVERS_THROUGH:ulong;
  REPLACES:[ulong];          // F of the segments an LSM merge replaced
  ENTRIES:[FMBEntry];        // DICTIONARY, sorted by (KIND, ID)
  LATEST:[FSGLatest];        // LATEST, sorted by OBJECT_KEY_ID
  INSTANCES:[FSGInstance];   // INSTANCES, sorted by (BATCH_ID, CONTENT_KEY_ID)
  TOKENS:[FMBEntry];         // TOKENS: per-producer counts as TEXT [token, n, bytes, mints, maxts, maxseq]
  PAIRS:[FSGPair];           // TOMBSTONES (KEY_HI = rid) and ROWS_BY_*, sorted by (KEY_HI, KEY_LO, RID)
  COLUMN:[ubyte];            // reserved (analytics task)
}
root_type FSG;
file_identifier "$FSG";
```

How the item-5 orders fill `PAIRS`:

| Kind | KEY_HI | KEY_LO |
|---|---|---|
| `ROWS_BY_SEQ` | rid | 0 |
| `ROWS_BY_OBJECT_EPOCH` | object id | epoch XOR 2^63 |
| `ROWS_BY_CID` | the 8-byte key prefix (`cp`) | the next 8 bytes |
| `ROWS_BY_W` | w XOR 2^63 | 0 |
| `ROWS_BY_AT` | at XOR 2^63 | 0 |

- `ROW_REF` reaches every other column of the row in its `$FMB` with one read.
- `FSG` field layout is **frozen when item 5 lands** (§7). The kinds above
  `TOMBSTONES` are provisional until then; everything else in §3 is frozen by
  this review.

## 4. IPFS mapping

### 4.1 Per-record blocks

- For a record of `N <= 1 MiB`, the IPFS block is **its range**, with CID
  `CIDv1(raw, sha2-256)`. This equals the record CID (unsealed) or the leaf
  CID (sealed).
- The node registers `(path of the generation file, F+4, N, CID)` in Kubo's
  filestore (nocopy). The bytes stay where they are.
- FlatSQL exposes these tuples through a new read op. It answers RB1 rows
  `(gen path, offset, length, cid, kind, unit)` for a feed and a commit range,
  and the node pulls them per acknowledged round (§5).
- Non-record frames (`$FMB`, `$FCK`, `$FSG`) are raw leaves the same way. The
  DAG links them in situ too.

### 4.2 Records over 1 MiB

- **Leaves.** Leaf `i` is the range `[F+4 + i*2^20, F+4 + min(N, (i+1)*2^20))`,
  as a raw block.
- **Root.** A dag-pb UnixFS `File` node:
  - links unnamed, `Tsize` = leaf size, in order;
  - `Data` = UnixFS{Type File, filesize N, blocksizes};
  - balanced and one level deep (the write pool caps a frame below 8 MiB, so a
    record has at most 8 leaves; a level is added only past 174 leaves).
- The root CID equals what `ipfs add --cid-version=1 --raw-leaves
  --chunker=size-1048576` gives for the same bytes. That is the interoperability
  check.
- `FMBLarge` keeps the root CID, the root node's bytes (about 410 B for 8
  leaves) and the leaf digests. The DAG and the CAR need no re-hash, and the
  node puts `ROOT_NODE` in its blockstore.
- The whole-range raw CID (SDN's key) is never registered or provided: bitswap
  cannot move a block that large. The `ROW` entry maps the two CIDs to each
  other.

### 4.3 The feed root DAG

All link nodes are dag-pb, CIDv1, sha2-256. FlatSQL computes them as a pure
function of the frames: they are never FlatSQL disk state. The node keeps them
in its blockstore when exposure is on, and the CAR export emits them.

```
feed root                       dag-pb UnixFS Directory, links sorted by name
├── "checkpoint" -> the newest $FCK frame              raw leaf, in situ
├── "commits"    -> commits tree                       dag-pb, no Data; fanout <= 1,024
│     └── ... -> commit unit (one per FMB, stream order)  dag-pb, no Data, links in stream order:
│                  NEW record frames (raw leaf | §4.2 root), then the $FMB frame (raw leaf)
├── "segments"   -> node linking each live $FSG frame (raw leaf, in situ), in FCK.SEGMENTS order
└── "columns"    -> reserved for the analytics column segments (absent until that task lands)
```

- **Liveness.**
  - A commit unit is linked while at least one row it added is live, or while
    it removes a row of a linked unit.
  - Its record links are only the live NEW frames.
  - Recursive pin of the feed root = the feed's live state, and nothing dead.
- **When the root changes.**
  - The root is recomputed at each checkpoint (§6): the new units, the commits
    tree's right spine (O(depth) nodes), the segments node and the root.
  - Retention or a delete rewrites the touched units and their spine.
  - Compaction rewrites the whole tree.
- **What `PREVIOUS_FEED_ROOT_CID` gives.** Inside the `$FCK` it is just bytes:
  a raw leaf has no links. History is reachable on purpose but never pinned
  by the current root.
- **Announcing.** Only feed roots (and, if the node chooses, segment roots) are
  announced. Records are fetched by CID over bitswap from root holders.

## 5. Acknowledgement and durability

**Rule.** For one call, the writer:
1. appends its record frames and then its feed's `$FMB` to each touched stream;
2. syncs each touched stream once (in parallel when several);
3. commits the SQLite index without a device sync (WAL, `synchronous=NORMAL`),
   for read-after-write;
4. publishes and acknowledges.

**What an ack promises.** The record bytes and every column of their rows are
durable in the stream. The index may be behind after a power loss, and
recovery (§9) restores it from the `$FMB` frames.

**What it costs.** Format 4 pays a stream sync plus an index commit sync per
call, so a single-feed call drops from 2 syncs to 1. A call that also creates a
feed pays once for the feed: the new file, its genesis `$FCK` and a directory
sync. A new token has no extra sync, because it lives in the `$FMB`
dictionary.

**Order of persistence.** The index commit is written only after its stream
sync returned. A persisted index commit therefore never names bytes the stream
lost (format 4's invariant). The index's `mark` is the end of the last `$FMB`
it applied.

**Background work** keeps its own syncs off the ack path:
- WAL checkpoints (they sync);
- `$FCK`/`$FSG` frames (synced with the next round, or alone when idle);
- merges of staged rows.

**Registration** follows the ack:
- The node pulls `(range, CID)` tuples only for acknowledged rounds, so a
  filestore entry never names unacknowledged bytes.
- The node fsyncs nothing extra. A crash that loses registrations is repaired
  by re-pulling from the node's last registered commit.

## 6. Checkpoints and segments

**Cadence.** A checkpoint is written when the first of these holds:
- `max(64 MiB, 4 x live segment bytes)` of frames have been appended since the
  last one;
- 65,536 rows have been appended since the last one;
- the feed has had commits but has been idle for 30 s;
- at close, at the end of a compaction, or before an export.

**Writing one.**
1. Fold the rows since `APPLIED_THROUGH` into new level-0 segments, one per
   changed kind: dictionary entries, latest-per-object, instances, tokens,
   tombstones, and the item-5 row orders where enabled.
2. Append the segments, then the `$FCK` that lists every live segment.

**LSM.**
- When a kind has 4 segments at a level, they merge into one at the next level
  (newest wins; tombstones drop the rows they name).
- `REPLACES` names the inputs, which become dead frames.
- Row-order segments merge only with neighbors in seq order, and a merge's
  output stays at most 1/8 of the feed's live rows. Retention by arrival
  therefore drops **whole segments**.

**Open.** Open is the newest `$FCK`, its segments and the tail `$FMB`s after
`APPLIED_THROUGH`. No SQLite file is needed for counters, dictionary,
instances, tokens or latest.

## 7. Index choice (item 5)

- The SQLite index (`<feed>.db`) stays the default. It is a cache rebuilt from
  the `$FMB` chain (§9).
- FSG row-order segments replace it **per workload** only if every replay
  workload on the 15 GB fixture is within about 10% of SQLite (native and AOT).
- Reads go through the same `p4_reader.h` / virtual-table interface over
  "segments + tail": the in-memory view of rows after the newest checkpoint
  plays the part that `Staged` plays today.
- The `FSG` layout freezes when item 5 lands. Until then only kinds below
  `ROWS_BY_SEQ` are written.

## 8. Backups and archives: CARv2

**A backup of a feed is the CARv2 of its feed root.** A store or type backup is
the CAR of a dag-pb UnixFS Directory `<TYPE>/<feed>`, whose links are the feed
roots.

**Bytes.**
- Pragma `0a a1 67 76 65 72 73 69 6f 6e 02` (11 B).
- CARv2 header (40 B): characteristics 0, data offset 51, data size, index
  offset.
- CARv1 payload:
  - the dag-cbor header `{roots: [root], version: 1}`;
  - sections `varint(len(cid)+len(data)) | cid | data` in **deterministic DFS
    pre-order** from the root, each block once at its first occurrence.
  - Order: root, "checkpoint", "commits" (interior node, unit node, its leaves
    in stream order; a large record's root then its leaves), "segments" and its
    leaves.
- `MultihashIndexSorted` index (0x0401), as go-car writes it.

The same state always gives the same CAR bytes. There is no custom container.

**Restore.**
- `ipfs dag import` works as is.
- FlatSQL restore walks the CAR in order. For each unit it appends the NEW
  record leaves as frames, then applies the unit's `$FMB` through the writer in
  replay mode. Seqs, timestamps, delivery times, nodes, batches, urls,
  signatures and identities are kept. Only `OFFSET` is local, so it is remapped
  and written into a fresh `$FMB`.
- This is the same apply code as crash recovery and rebuild (§9): one path.
- Pass condition: every replay answer hashes equal to the source's.

**"Backups are tables themselves."**
- A restored backup is a feed and serves as a table.
- A backup set is a feed-root DAG of the same shape, so it can be pinned,
  announced, backed up and restored like any feed: recursive storage.
- **The backup catalog is a feed of `$DPM` records** (owner, 2026-10-18), one
  record per CAR. The node writes them, because a DPM is signed by its provider.
  The catalog feed is itself backed up the same way.
  - `DATASET_ID` is `<TYPE>/<provider>@<source>`. `UPDATE_ID` is the feed root
    CID's text. `PROVIDER_PEER_ID`, `PUBLISH_TIMESTAMP` and `PROVIDER_SIGNATURE`
    come from the node.
  - Each CAR is one `ASSETS` entry, `TRANSPORT_KIND = CONTENT_ADDRESS`:
    `CID` and `DATA_ROOT` are the feed root, `BYTE_LENGTH` is the CAR's size and
    `FILE_NAME` is the CAR's name.
  - DPM lacks two things, so this is a proposed **DPM amendment** (additive, in
    the same SDS release as FMB/FCK/FSG):
    - `publicationAssetKind.CAR_ARCHIVE`, appended;
    - `DPMAsset.THROUGH_SEQ:ulong`, the highest seq the archive covers (its
      as-of point).

## 9. Crash recovery from the stream tail

**The commit point.** A commit point is a verified non-record frame on the
commit chain. An `$FMB` **verifies** when all of these hold:
- the FlatBuffers Verifier passes on its aligned copy;
- `SELF_OFFSET` = its `F`;
- `PREVIOUS` = the last commit point's `F`;
- every `NEW_FRAME` row names a whole frame between the previous commit point
  and it;
- each such frame's sha2-256 equals the row's digest (or `LEAF_DIGEST`, sealed).

An `$FCK`/`$FSG` verifies on the first three. The digest check makes recovery
safe even when a crash persisted later pages before earlier ones.

**Open, per feed:**
1. **Start.**
   - With an index: start at its `mark`. A stream shorter than its mark lost
     acknowledged frames, and the feed is quarantined (`P4_E_CORRUPT`, named), as
     in format 4.
   - Without one: find the newest `$FCK` by the tail search below and load its
     segments.
2. **Walk** forward from the start, collecting record frames until a candidate
   commit point. If the candidate verifies, apply it with **its** seqs and
   metadata. Rows get no fresh seqs and provenance is never blank (unlike
   format 4's rebuild).
3. **Cut the tail.** At EOF, or at a torn or unverifiable frame, cut the stream
   back to the end of the last commit point. Those bytes were never
   acknowledged and never registered.
4. **Finish moves.** A `MOVED_IN` row in a feed whose `MOVED_OUT` is missing in
   local gets its removal `$FMB` appended to local.
5. **Rebuild caches** as needed:
   - the views from the index's staged rows (as format 4);
   - a missing or damaged `.db` from the `$FMB` chain;
   - a missing `.idx` from the genesis frames.

**Tail search (no index).**
- Read the stream backward in 1 MiB chunks.
- A candidate is a position `p` with `"$FCK"` at `p`, giving `F = p - 8`.
- Accept it only if all of these hold: it verifies, `SELF_OFFSET = F`, its
  `PREVIOUS` names a frame that ends exactly at `F` and verifies as a commit
  point, and the forward walk from it reaches EOF or a torn tail.
- The checkpoint cadence bounds the search to about 64 MiB. On any doubt, the
  search falls back to a forward walk of the size prefixes, which is a
  sequential read of the file.

**Rebuilding the SQLite index without its file.**
- Walk the stream forward in one sequential read. Apply each `$FMB` in one
  bulk transaction, with the secondary indexes built after.
- The epoch and object key of each unsealed row are re-extracted from its
  record frame, which is already in the read buffer (principle 6).
- Nothing is re-hashed except the tail after the newest checkpoint.
- Open without the SQLite index needs none of this: the newest `$FCK` and its
  segments already hold the epoch and object orders.

## 10. Retention and compaction (pins and filestore first)

**Retention, QUOTA_GC, DELETE, SUPERSEDE:**
1. Append the removal `$FMB` and sync. The decision is now durable.
2. Remove the index rows. Reads stop answering them.
3. Publish the drop list (CID, unit) and the next feed root at the next
   checkpoint (or at once when the feed is exposed).
4. The node does `pin update old-root -> new-root`, removes the dropped CIDs'
   filestore entries (reference-counted across feeds, §11), and calls
   `p4 release(feed, through-commit)`.
5. Only released bytes may physically go (step 2 of a compaction). A feed that
   is not exposed is released implicitly.

The stream is append-only, so no byte under a live filestore entry is ever
changed. Retention by arrival drops whole FSG segments (§6).

**Compaction** (a stream more than half dead, at least 1 MiB dead; unchanged
trigger):
1. Write generation `g+1`: genesis `$FCK`, the live record frames in stream
   order, consolidated `$FMB`s (rows `COMPACTED`, new offsets, at most 65,536 per
   frame), fresh segments, and the final `$FCK`. Then sync.
2. Repoint the index (one transaction) and publish the moved ranges and the new
   root.
3. The node re-registers the same CIDs at their new `(path, offset)` and pin-
   updates to the new root. Record CIDs do not change, because records are
   verbatim. Then it releases generation `g`.
4. Unlink `g` after the release and after the 30 s reader window (format 4).

A node that never releases blocks compaction, and the store reports it as a
stat. It never loses data.

## 11. sdn work this needs (child tasks; FlatSQL does none of it)

All of these are on the node:
- **Filestore.** Kubo filestore (nocopy) in the in-process Kubo, with the store
  directory inside the filestore root. Registration goes through
  `filestore.FileManager.Put` with `PosInfo{FullPath, Offset}`. `ipfs add` is not
  used: it re-chunks.
- **Register per batch.** Pull the acknowledged rounds' tuples, put them in the
  filestore in one datastore batch, and keep a per-feed "registered through"
  commit.
- **Reference counts.** Filestore entries are keyed by multihash. A record held
  by two feeds has one entry: count holders, and on a drop re-point the entry
  to a remaining holder, never delete it.
- **Provide roots only.** Announce feed roots (and optionally segment roots).
  Never provide record CIDs.
- **Per-feed policy, off by default.** Restricted feeds (UDL, anything not on
  `ipfs_schemas`) are never registered, provided or served. Encrypted feeds are
  addressed by ciphertext.
- **Unpin on retention and compaction**, per §10, then `release`.
- **E2E.** A second node pins a feed root (or imports the CAR) and restores it
  through FlatSQL. Every replay answer hashes equal to the first node's.
- **Format-5 names.** The store-format names move to 5:
  `store-migrate --to 5`, `format5proof` (was `format4proof`), and the layout
  checks. The lead-per-call task (`sdn-format4-lead-per-call-20261018`) becomes
  format 5's.
- **Backup catalog.** Write one signed `$DPM` per CAR into the catalog feed
  (§8).

## 12. How this meets each owner requirement, and what it costs

M = measured on this machine (Mac Studio, run 2/3 gate fixture, native).
E = estimated; the implementing release measures it.

| Owner requirement | How | Cost |
|---|---|---|
| On disk stays linear `[u32 size][FlatBuffer]`, appended in order; any FlatBuffer reader opens it | §2: one stream per feed generation; four frame kinds told apart by identifier; no header, padding or trailer | metadata frames are copied to an aligned buffer when read (open/rebuild only): negligible |
| Everything to rebuild a table is FlatBuffers: sibling per-record metadata frames, never an envelope, plus periodic checkpoints | §3.1 `$FMB` per feed per round (all of `r`, the dictionaries, removals); §3.2–3.3 `$FCK` + `$FSG`; SQLite, the type index and the views become caches | `$FMB` = 72 B/row (40 B struct + 32 B digest) + 24 B/call + dictionary deltas. Gate fixture (M sizes): OMM 623,462 rows → 44.9 MB = **+22%** on a 204.7 MB stream (328 B records); WXF sea 1,720,179 rows → 123.9 MB = **+0.8%** on 15.0 GB (8.7 KB records), plus about 11 MB (E) for its 191,131 batch strings. Checkpoint segments: ≤ 25% of appended bytes by the cadence rule, typically about 1–2% (E); LSM write amplification about log4(segments) (E) |
| Pin records in situ: each record one contiguous range, raw-leaf CIDv1 (sha256) = record CID, Kubo filestore nocopy | §2.2, §4.1: the stored CID is already the range's sha256 (`kVerifyCid`, default on); the node registers `(path, off, len, CID)` | write path: **0 extra hashing** (the digest exists); 72 B/row in `$FMB` (above). Kubo side (E): about 150–200 B per filestore entry in its datastore, about 50–60% of OMM's stream bytes and about 2% of WXF's: why exposure is opt-in per feed |
| Records over 1 MiB are a DAG of raw-leaf sub-ranges | §4.2: 1 MiB leaves; UnixFS root byte-equal to `ipfs add --raw-leaves --chunker=size-1048576` | about 410 B root node + 32 B per leaf in `FMBLarge`; no fixture record exceeds 1 MiB (M: max 8,720 B) |
| Backups and archives are IPLD CARv2 (record blocks, index segments, checkpoint root DAG); backups are tables themselves | §8: deterministic CARv2 of the feed root; restore = the same apply path as recovery; a restored CAR is a feed | CAR overhead (E): about 40 B per block section + 40 B index entry: OMM about 1.25x its live stream, WXF about 1.01x. dag-pb links about 44 B per record (in the node's blockstore, not FlatSQL's) |
| Announce roots, not records; IPFS per feed, off by default; restricted feeds never registered or served | §4.3, §11: FlatSQL computes roots and range tuples for every feed (cheap, and needed for backups); registration, providing and serving are the node's per-feed policy | DHT load: one provider record per feed root per checkpoint, instead of per record |
| FlatBuffer index segments may replace SQLite per workload within about 10% | §3.3, §6, §7: `FSGPair` sorted vectors, LSM, whole-segment retention, behind the same reader interface | 32 B per row per order (5 orders = 160 B/row) against SQLite's 193 B/row for OMM (M: 120.7 MB / 623,462) and 180 B/row for WXF (M: 309.8 MB / 1,720,179) |
| Room for analytics column segments | `fckSegmentKind` 64–127 and `FSG.COLUMN` reserved; root link `"columns"` reserved; no code (YAGNI) | none |
| Zero back-compat | format 4 refused (`P4_E_FORMAT`); nodes rebuild by re-sync | one re-sync per node |
| Ack and durability | §5: one stream sync per touched stream; index commit unsynced (WAL NORMAL) | **M: 4.79 ms per sync** (F_FULLFSYNC, gate: 16 syncs = 76.66 ms). Gate `wxf_lead` 746 ms with 128 syncs (613 ms) → 64 syncs, about **−307 ms (−41%)**; `gp_ingest` 476 ms with 16 → 8, about **−38 ms (−8%)** (E from M) |
| Crash recovery from the tail | §9: commit chain + digest-checked `$FMB`s; tail search ≤ about 64 MiB | open without the index (E): newest `$FCK` + segments + tail ≤ 64 MiB, **about 1 s**. Rebuilding the 15 GB fixture's SQLite index from the stream (E): one sequential read of 15.2 GB, about 1 µs/row of re-extraction, bulk-insert 2.34M rows, **about 15–30 s**, against format 4's rebuild, which re-hashes 15 GB with FlatSQL's software sha256 and loses seqs and provenance |
| Read through IPFS vs direct | not a format change | (E) direct warm `pread` 5–20 µs/record; through the local filestore +30–100 µs/block (datastore lookup, `pread`, Kubo's re-hash); remote = network RTT + that. The sdn child measures it |

What the releases measure (each its own release, `cmd node
cpp/tests/p4-replay.e2e.mjs`):
- **Item 3:** syncs per call, the ack p50/p99 and index lag. Kill -9 in the
  middle of a lead: every acknowledged record is present once with its seq and
  provenance, and the answers hash equal to 3.7.6's on the same input.
- **Item 3b:** delete every `.db` and `.idx`, reopen, and hash every answer
  equal. Report the rebuild time on the 15 GB fixture.
- **IPFS layout:** the per-record tuple read. Every tuple's range hashes to its
  CID, and the large-record root matches `ipfs add`.
- **Item 5:** every workload against SQLite (the 10% bar).

## 13. Owner decisions (2026-10-18)

1. **SDS codes.** `FMB`, `FCK` and `FSG` are published as ordinary SDS standards
   in every registry, through the SDS publish workflow (1.255.0 or later),
   together with the DPM amendment (§8). flatsql's SDS pin moves up from 1.206.0.
2. **Re-extract, do not store.** `EPOCH` and `OBJECT_KEY_ID` are re-extracted
   from the record bytes on rebuild and are not in `$FMB`.
   - `SOURCE_TS` stays. It is a PUT field (the tag 53 entry), not part of the
     record bytes, so `$FMB` is its only place.
   - Sealed rows keep their epoch and key in `FMBSealed`, because ciphertext
     cannot be re-extracted.
   - Net cost: 72 B per row (+22% for OMM), not the 60 B (about 18%) assumed
     when the question was asked.
3. **The backup catalog is `$DPM`**, amended additively (§8). There is no new
   table.
4. **Store format 5.** The `STORE` header says 5, and format-4 stores are
   refused with `P4_E_FORMAT`. The sdn names follow in the sdn child tasks
   (§11).
