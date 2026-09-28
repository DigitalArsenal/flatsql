# FlatSQL partition store (format 2): native engine

The engine of the partition store program. Part I (§1–§11) is T1: append-only
partition logs, one writer per partition on a pinned pool, durable acks,
per-commit indexes, type owners, and durable-tail open. Part II (§12–§21) is
T2: reader instances and lanes, the snapshot protocol, the SQL surface, the
fan-out merge, admission and results. Part III (§23–§30) is T3: compaction,
reclamation, meta-segment retirement, disk accounting and quota. The design is
the stack's `docs/architecture/flatsql-partition-store.md` (§22 overrides
§0–§21; §22.4 holds the owner rulings). This file records what was built, how
to run it, the measured acceptance, and every place the build departs from the
design.

The wasm artifact is T4; SDN and browser integration are T6/T10.

## 1. Code map

| Path | Contents |
|---|---|
| `cpp/include/flatsql/flatsql_io.h`, `cpp/src/flatsql_io_native.cpp` | Seven-import host contract; flags `CREATE_PARENTS` 0x0100, `UNLINK_IF_UNUSED` 0x0200, `OPEN_DEFERRED` 0x0400, status `BUSY` −7 (the module-sdk 0.8.22 values). Native host: lock-free handle table, `F_FULLFSYNC` on darwin, `fdatasync` on Linux, parent-directory fsyncs. |
| `cpp/include/flatsql/ps/format.h`, `src/ps/format.cpp` | On-disk structs, magics, key encodings, A17 CID sort key, head slots, paths. |
| `ps/index.h`, `src/ps/index_l0.cpp`, `index_l1.cpp` | Per-commit L0 blocks, L1 runs (4 KiB blocks, fences, blooms, TOC), streaming k-way merge. |
| `ps/ring.h`, `src/ps/ring.cpp` | 64 KiB slab pool, SPSC rings, page map-ahead, rejects, owner word. |
| `ps/extract.h`, `src/ps/extract.cpp` | Per-type config (BFBS + rule text), frame checks, key extraction (A19), producer token (A3), sealed envelopes. |
| `ps/registry.h`, `src/ps/registry.cpp` | Registry frames and head (A10). |
| `ps/writer.h`, `src/ps/writer_pool.cpp` | Engine, writers, commit rounds, sync pool, mailbox, maintenance, producers. |
| `src/ps/partition_log.cpp` | Staging, dedupe, RETAG, supersede, TOMB, RECONCILE, TOMB_RANGE, transactions, publish. |
| `src/ps/merge.cpp` | Warm/cool, merge planning and outputs (A11), manifests. |
| `src/ps/type_owner.cpp` | Labels, arrivals (A15), cid catalog, REPEAT, REHOME (A14), type-level deletes, notices (A25). |
| `src/ps/journal.cpp` | Per-writer commit journal (A8 fallback). |
| `src/ps/open.cpp` | Open, journal replay, durable-tail adoption (A4), registration. |
| `ps/flatsql_ps.h`, `src/ps/capi_ps.cpp` | C ABI (i32/f64 only). |
| `schemas/flatsql_attr.fbs` | `RecordAttr` / `SourceTag` (generated with the published flatc-wasm). |
| `cpp/test/ps/`, `cpp/test/io_fault.cpp` | Tests, fault-injecting host, bench driver, A19 vectors. |

## 2. On-disk layout

```
<root>/fsql2/
  STORE  MIGRATED  registry.fsl  registry.fsh
  j/<writer:02x>-{a,b}.fsj           commit journal (only with commitJournal)
  p/<pid:08x>/
    h.fsh                             head, A/B 4 KiB slots
    m-<seg:06x>.fsl                   meta log, one per data segment (A9)
    d-<seg:06x>.fsd                   frames, [u32 size][FlatBuffer]
    r-<seg>.fsr  a-<seg>.fsa          dense rows / attributes (merged ranges)
    x-<seg>-<gen:04x>.fsx             L1 runs
    mf-<gen:06x>.fsm                  segment manifest
    l.fsl                             lane table
  t/<fid hex8>/
    h.fsh  m-<seg>.fsl                type head and log (A10 batches)
    g-<seg:06x>.fsg                   arrivals, 24 B entries (A15 segments)
    g.fsf                             arrivals gseq fence index (A15)
    x-<gen:06x>.fsx  mf-<gen>.fsm     cid catalog runs and manifest
    s-<fp:016x>.fsc                   type config (BFBS + rules)
```

Byte layouts are the structs in `format.h` (all little-endian, CRC32C).

## 3. Write path

1. A producer reserves credits (`min(ring cap − used, pool free − reserve)`),
   copies one entry into its ring and rings the owner's doorbell.
2. The owning writer stages each partition with backlog: verify (size, fid,
   BFBS), CID check, extraction, dedupe (writer cid table, blooms, L0/L1),
   RETAG for a new tag tuple (A2), supersede, lane deltas, the L0 block.
3. A commit round writes every staged batch and makes it durable:
   - **file mode** (default, §6.4 as amended by A8): pwrite `d`/`l`/`g`/fence,
     one concurrent sync round; pwrite `m` batches, a second sync round. Two
     sync rounds per iteration whatever the number of dirty partitions.
   - **journal mode** (`commitJournal`, §22.4 ruling 5): the same pwrites plus
     one journal record holding their bytes; the journal fsync is the round's
     only sync. A checkpoint thread later syncs the touched files and heads
     (paced, see deviation 13) and truncates the retired journal file.
4. Publish: state, seqlock publication, head slot pwrite (checkpoint heads are
   synced one round later in file mode, by the journal checkpoint in journal
   mode), acks, notices to the type owners (A25: a full queue drops the
   notice; the owner compares `labeled_through` with each partition's HWM).
5. Maintenance, bounded per iteration: merge planning (INTENT_MERGE first,
   A11), MERGE_DONE once a helper has written and synced the outputs, seal by
   size/records/age, next-segment pre-creation, idle reclaim, journal
   checkpoints.

Mailbox commands (never ring entries, A24): adopt/handoff, rebalance, type
registration, type-level kills (TOMB_CID fan-out, A14) and `TOMB_RANGE{seg,
epoch < t}` for the quota planner. TOMB_RANGE runs in bounded steps, spares
supersede-lane heads and control kinds, and completes when its TOMBs are
durable.

Ownership (A26): owner word `{epoch u32, writer u8, state}`. A rebalance waits
for any in-flight merge helper, sets HANDOFF, and the target CASes it to
OWNED with epoch + 1. A helper re-validates `{epoch, OWNED}` before every file
mutation.

## 4. Open and recovery

1. `STORE` and `MIGRATED` (refused without the marker).
2. Registry: frames scanned past the head, truncated at the first bad CRC;
   incarnation = max seen + 1, recorded durably before any batch.
3. Journal replay (A8): both files of every writer id from 0 until the first
   id without journals; records in `seq` order; parts pwritten into their
   files; the files fsynced; the journals truncated.
4. Types: head slot, label checkpoint (A10, > 128 pids), tail batches (commit
   chain, incarnation, arrivals CRC, A15 segment switch; a journaled batch only
   if replayed), fence index validation, fsync before adoption.
5. Partitions: head slot, tail batches across SEAL continuations (same rules),
   fsync `d`/`m`, A11 intent cleanup (`UNLINK_IF_UNUSED`), DURABLE_CKPT head.
6. Type heads for adopted type tails, written after the partitions are
   attached (their inline labels come from them).

Open never reads a `d-*` byte and never parses a frame
(`openDataBytes = framesParsedAtOpen = 0` in every crash trial).

## 5. Extraction rules (A19)

A type's extraction is engine data: `TypeConfig` = schema name, fid, BFBS,
rule text, limits, flags. The rule language is documented in `extract.h`
(`epoch`, `col`, `epoch_day`, `object`, `require`, `supersede`).

The production rule texts for OMM, MPE, OEM, CAT, PNM and RFB are
`cpp/test/ps/vectors/<TYPE>.rules`. `format_extraction_golden_vectors_A19`
checks them against rows produced by sdn-server's `extractIndexedFields`,
`recordIndexArgs` and `recordSupersedeKey` (copied verbatim into
`vectors/gen/goref.go`, built against the published SDS Go bindings 1.226.0)
over 420 frames built from the published SDS 1.226.0 schemas with the
published flatc-wasm: 310 OMM from a Space-Track GP archive day plus edge
cases (fallbacks, time zones, invalid dates, pre-1970), 44 MPE, 10 OEM, 48 CAT
(every enum value, out-of-range values, all supersede branches), 4 PNM,
4 RFB. All 420 match.

## 6. C ABI

`flatsql_ps_init(role, cfg, len)`, `flatsql_ps_start()`,
`flatsql_ps_layout(out)`, `flatsql_ps_wake(addr, n)`,
`flatsql_ps_pump(budget_us)`, `flatsql_ps_stop(deadline_ms)`,
`flatsql_ps_stats(out, len)`, `flatsql_ps_register_type(cfg, len)`,
`flatsql_ps_register_partition(peer, len, fid)`, `flatsql_ps_ring(pid)`.
Config TLV tags are listed in `flatsql_ps.h`.

## 7. Configuration (`EngineConfig`)

| Field | Default | Meaning |
|---|---|---|
| `writers` | 1 | writer threads (production: clamp(cores − 2, 1, 16)) |
| `syncThreads` | 0 = clamp(writers, 4, 8) | concurrent sync pool, fair per writer |
| `poolBytes` / `slabBytes` | 192 MiB / 64 KiB | ring slab pool |
| `defaultRingCap` | 4 MiB | per partition (type config may override) |
| `commitBytes` / `commitFrames` | 4 MiB / 1024 | per partition per commit |
| `sealBytes` / `sealRecords` / `sealAgeMs` | 64 MiB / 1 M / 1 h | segment seal |
| `ckptIntervalMs` / `ckptMetaBytes` | 250 / 64 KiB | head checkpoints (file mode) |
| `mergeL0Blocks` / `mergeL0Bytes` / `mergeMinL0Bytes` | 16 / 1 MiB / 64 KiB | merge triggers |
| `mergeHelpers` | 1 | merge builder threads (0: build on the writer) |
| `zeroFillStep` | 1 MiB | A8 zero-fill ahead |
| `arrivalsSegBytes` | 64 MiB | A15 arrivals segment |
| `commitJournal` | false | A8 fallback: per-writer commit journal |
| `journalCkptBytes` / `journalCkptMs` | 8 MiB / 1000 | journal checkpoint triggers |
| `noticeQueue` | 1024 | A25 notice queue per type |
| `typeCommitRows` / `reconcileStep` | 8192 / 4096 | bounded steps |

## 8. Tests and benchmarks

```
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build cpp/build -j 8 --target flatsql_ps_test flatsql_ps_bench
cpp/build/flatsql_ps_test                         # default suite, T1 + T2 (~2 min)
cpp/build/flatsql_ps_test --test=<name>           # one test, slow ones included
cpp/build/flatsql_ps_test --crash-trials=N --crash-seed=S --test=crash_faults_T1_1
cpp/build/flatsql_ps_bench --mode=scaling|host02|dirty|soak [--journal=1] [--io=mem|fs] [--dir=D]
```

Slow tests: `crash_faults_T1_1_full` (10,000 trials, 64 types, 256
partitions), `writer_single_writer_under_rebalance_T1_4_full` (10 min),
`type_notices_never_block_A25_full` (10 min),
`open_clean_1M_records_256_partitions_T1_2_full` and `_real_fs`.

The crash harness (FaultFs) injects: drop all unsynced writes, drop a random
subset, tear the last write at a 512 B boundary, reorder unsynced writes, and
kill -9 keeping the page cache; every other trial adds the A4 sequence
(reopen, resend, publish, second crash dropping everything). Half the stores
run in journal mode; seals, merges (helper and inline), arrivals seals,
RECONCILE, TOMB_CID, TOMB_RANGE, transactions and registrations run under the
crashes. A stalled ack counts as a failure.

## 9. Acceptance (§18 T1 as amended)

Machines: **Mac** = owner Mac Studio (Darwin 25.3.0, arm64, 28 cores,
APFS, `F_FULLFSYNC`), shared with other lanes (load average 13–42 during
these runs). **Docker** = Linux 6.12 linuxkit arm64 in Docker Desktop on that
Mac (ext4 on a virtual disk), `--cpus`/`--memory` as noted. Neither is
host-02 or a Linux-8 box; §11 lists what needs one. All numbers come from
`MEASURED` lines of the tests and `flatsql_ps_bench`, run on task-branch builds from 16a4c63 to 7f941bc.

| # | Acceptance | Result | Where |
|---|---|---|---|
| 1 | 10,000 injected crashes, 256 partitions, 64 types (OMM/MPE/CAT/IQC shapes); acked bytes identical, gap-free pseqs, counters = recount, no gseq reassigned, 0 `d-*` bytes read by open; A4 crash → reopen → resend → second drop-all crash; A10 type/registry crash points; A11 merge crash points | **Pass.** Final build (7f941bc), Mac: 10,000 trials over the five modes (about 2,000 each: drop all unsynced, drop a subset, tear the last write at 512 B, reorder, kill -9) plus an A4 second drop-all crash every other trial; 3,437,331 acked records verified; 526,105 merges, 1,087 seals, 69,126 arrivals seals, 11,107 TOMB_RANGE commands, 983,753 journal records replayed (half the stores journaled); 243 s. Linux (Docker): 2 × 1,000 trials on the final build, 5 × 1,000 on earlier ones. | Mac `crash_faults_T1_1_full`; Docker `--crash-trials=1000` |
| 2 | Open, 1 M records in 256 partitions: clean ≤ 150 ms and ≤ 1 MiB I/O; after drop-unsynced ≤ 64 KiB `m` per active partition; 0 frames parsed | **Pass.** Real fs: Docker ext4 39.6 ms, 476 KB read; Mac APFS 34.1 ms, 574 KB read, 1 fsync (the registry incarnation). In-memory host: 4.5 ms, 577 KB. After drop-unsynced: 24 KB of `m` per partition (64 partitions). `framesParsedAtOpen = 0`, `openDataBytes = 0`. | `open_clean_1M_records_256_partitions_T1_2_real_fs`, `_full`, `open_after_drop_unsynced_reads_bounded_meta` |
| 3 | Resend the last 10 k acked after a crash: 0 new rows, 0 `d-*` bytes read | **Pass.** 0 rows, 0 bytes. | `open_resend_after_crash_dedupes_T1_3` |
| 4 | Rebalance every 50 ms for 10 min: pseq strictly increasing and gap-free, one writer thread per ownership interval, no overlap; A26: rebalances during merges with helpers stalled 100 ms, 0 non-owner writes | **Pass.** 600 s, 8 writers, 64 partitions, producers paced to ~6,400 records/s: 10,623 moves, 761,217 audited commits, 7,624 ownership intervals, 0 violations (gap, order, overlap, second thread in an interval); 33,511 merges, 5,181 injected 100 ms helper stalls, 5,448 handoffs that waited for an in-flight helper, 0 non-owner helper writes; every sent record present after reopen. | `writer_single_writer_under_rebalance_T1_4_full` (Mac) |
| 5 | One partition flooded: ring ≤ cap, others' p99 changes ≤ 20%, pool within budget, credits reach 0 and recover, 0 frames lost | **Pass.** Ring max 4,193,680 of 4,194,304 B; others' ack p99 34.5 → 30.4 ms (ratio 0.88; 28 hardware threads); pool peak 111 slabs (≤ total − reserve); 101,094 zero-credit samples and 14,006 credit waits, credits > 0 after; every sent frame present after reopen. The test enforces the ring, pool, credit and frame checks on every machine. The 20% ack-p99 ratio is a Linux-8 acceptance: it is enforced only on machines with ≥ 8 hardware threads and otherwise printed with the thread count (a 3-vCPU CI runner measured 91.6 → 154.9 ms once, then passed on rerun). | `writer_backpressure_flood_T1_5` (Mac) |
| 6 | Linux-8: N=8/N=1 ≥ 5× in memory, ≥ 3× with real fdatasync; 2 fsyncs per partition commit | **Pass on the proxy.** Docker `--cpus 8`: in memory 154,230 → 829,233 records/s (5.38×); real fs 80,883 → 456,263 records/s (5.64×); `d` 1.000 + `m` 1.000 fsyncs per framed partition commit. Journal mode: 1 journal fsync per committing iteration. | `flatsql_ps_bench --mode=scaling --io=mem|fs --writers=8` |
| 7 | host-02: N=1 ≥ 5,000 OMM records/s durable over 50 partitions; ack p99 ≤ 40 ms at 2,500/s | **Needs host-02.** Sustained rate passes everywhere measured (Docker 4 vCPU 38,912–100,598 records/s; 1 vCPU 35,788–85,149; Mac 19,920–26,936). Ack p99 at 2,500/s follows the disk's fsync tail and the shared box's load, not the engine: the same Docker 4 vCPU profile gave file mode p50 17–20 ms / p99 153–214 ms and journal mode p50 1.1–4.0 ms / p99 120 ms at load 15–20 (that disk's `fio` fdatasync p50 0.66 ms, p99 27 ms, p99.9 142 ms), and p99 0.4–4.4 s in both modes (an A/B run of the build at da0ad51 gave the same), when the box ran at load 12–42 with other lanes' I/O. Mac APFS (`F_FULLFSYNC`, load 14–22): file p50 757 / p99 998 ms, journal p50 343 / p99 567 ms. A8 at 1/10/50 dirty partitions (Docker 4 vCPU, load 15–20): file p99 23 / 95 / 185 ms, journal 17 / 93 / 170 ms. | `--mode=host02`, `--mode=dirty`, `--journal=0|1` |
| 8 | 30-min soak: every shared lock max hold ≤ 50 ms, p99.9 ≤ 1 ms; hot-path `malloc` per record = 0 | **Pass.** Mac, 1,800 s, 3,600,000 records over 50 partitions, 2 writers, real fs: hot-path `malloc` 0 per record; partition seqlock writer sections (144,399) max 0.027 ms, p99.9 ≤ 0.001 ms; registration lock (116 holds) max 0.025 ms, p99.9 bucket ≤ 0.033 ms. The other locks are queue mutexes held for one push or pop. `writer_hot_path_zero_malloc_T1_8`: 0 allocations over 4,000 records in file and in journal mode. (Ack p99 in that run was 6.5 s: the Mac's `F_FULLFSYNC` at 2 per partition commit, see row 7.) | `flatsql_ps_bench --mode=soak` (Mac); `writer_hot_path_zero_malloc_T1_8` |
| 9 | 2,048 partitions, 64 active: writer committed memory ≤ 288 MiB; idle partitions hold 0 slabs | **Pass.** 80.6 MiB committed (pool 25.8 MiB touched of 192 MiB, descriptors 11.9 MiB); 0 idle partitions with slabs. | `writer_memory_2048_partitions_64_active_T1_9` |

§22 amendments naming T1:

| Amendment | Status |
|---|---|
| A2 TAG rows, RETAG, RECONCILE | Built; B1/B2 sequence gives 0 TOMBs and 0 new arrivals, B3 tombstones exactly the removed record (`writer_dedupe_retag_reconcile_A2`); RECONCILE and TAG_TOMB under crashes. |
| A4 durable tails | Built (fsync before adoption, DURABLE_CKPT head, quarantine on fsync EIO, dedupe acks at durable commit); the crash harness's A4 mode. |
| A8 sync rounds, zero-fill, journal fallback | Built: 2 sync rounds per iteration (file mode) or 1 (journal mode), zero-fill ahead, A8 ack p99 reported at 1/10/50 dirty partitions. |
| A9 segmented meta log | Built (`m-<seg>` per data segment). Retirement is T3 (deviation 3). |
| A10 type-log and registry durability | Built (type batch format, > 128 pids label checkpoints, registry truncation, EXCL heads, pid never reused). |
| A11 merge intents | Built (INTENT_MERGE, idempotent `r`/`a`, fresh `x`/`mf`, open unlinks orphans). |
| A14 kills and promotion | Built (TOMB_CID fan-out, REHOME keeps gseq). |
| A15 arrivals | Segments and fence index built; compaction is T3 (deviation 4). MaxRowID = published `gseq_hi`. |
| A19 extraction parity, sealed frames | Built; 420/420 golden rows (§5); sealed envelopes checked by magic and size only. |
| A25 notices never block | Built. 10-minute run (Mac, 2 writers, crossed type ownership, notice queue of 1): 253,777 notices dropped, `labeled_through` lag p99 0.001 ms and max 222 ms, longest producer ack wait 414 ms (0 stalls ≥ 1 s). Under a load that saturates the type owners' labeling, the lag is throughput-bound, not bounded by the commit window. |
| A26 ownership epochs | Built except stage-1 prep states (deviation 2). |
| A34 published deps | `flatsql_attr_generated.h` and the A19 vectors come from the published flatc-wasm, SDS npm 1.226.0 and SDS Go 1.226.0; no sibling checkout is read at build time except the flatbuffers sources CMake already used. |
| A35 host-02 budget | Memory measured against the host-02 profile (row 9); the ops receipt (`nproc`, `free -m`, `df`, `fio`) needs the hosts (§11). |

## 10. Design deviations

Each entry: what the design says, what was built, why, and the evidence.

1. **Merge outputs are built and synced by a maintenance helper thread**
   (design §4.5/§11: the owner merges between commits; helpers only for split
   hot partitions, §12). The owner plans the merge (INTENT_MERGE), a helper
   writes `r`/`a` at plan-derived offsets and fresh `x`/`mf` files, syncs them,
   and the owner commits MERGE_DONE. Why: building a run on the writer stalled
   every partition it owns (maintenance step max at 50 dirty partitions, Linux
   1 vCPU profile: 325.8 ms with merges on the writer, 125.7 ms with the helper
   sharing that one CPU); the owner law puts
   compaction "per partition, off the hot path". The helper writes only new
   files or regions no reader can see before MERGE_DONE, re-validates the owner
   epoch before every mutation (A26), and a HANDOFF waits for it
   (`writer_single_writer_under_rebalance_T1_4`: helpers stalled 100 ms, 0
   non-owner writes). Its syncs no longer ride the commit's first sync round.
2. **No stage-1 helpers; ring entries carry no prep state** (§5.2, A26 "FREE →
   CLAIMED(epoch) → PREPARED"). The owning writer verifies, extracts and
   checks CIDs itself. Scaling came from partitions spread over writers
   (T1 #6 met without them, see §9), so the extra protocol bought
   nothing measurable. A26's other clauses (owner word, epoch re-validation,
   slab grace) are built.
3. **A torn-both-heads rebuild replays the meta log from segment 0** (A9:
   "scans only the active m segment plus the manifest"). Sealed `m` segments
   are retired by T3 (A12 lifecycle), which also records `first_live_m_seg`;
   until then segment 0 onward is the only complete source.
4. **A15 arrivals compaction is not built.** Arrivals are segmented with a
   gseq fence index (`g-<seg>.fsg`, `g.fsf`); dropping dead entries and
   folding DEAD entries out of catalog runs belong with T3's compaction and
   quota planner, which trigger them. MaxRowID is the published `gseq_hi`.
5. **The A8 commit journal is a configuration mode, off by default.** §22.4
   ruling 5 makes it the fallback "if a host misses T1 #7"; host-02 has not
   been measured (no ssh from this lane). Both modes pass the crash harness
   (half its stores run journaled) and are benchmarked in §9.
6. **A19 golden vectors come from a local archive day, not the host-02
   inventory sample** (no host access). The frames are real Space-Track GP
   records (`/opt/data/sdn-archive/spacetrack/gp/2026/2026-09-27.json.gz`)
   plus constructed edge cases; the expected rows come from the Go functions
   themselves.
7. **New rule directive `require <table path>`**: OEM's Go extraction returns
   no keys at all when the first block has no OBJECT; the rule language had no
   way to say that.
8. **New index kind `TAG_DEAD`** (A2 lists only TAG_TOMB rows). A PUT row is
   also its record's first tag instance; posting its TAG_TOMB under `DEAD`
   would have killed the record. `TAG_DEAD` keys a dead tag instance
   separately from a dead record.
9. **A full type L0 directory (48 blocks) makes the type owner wait for its
   merge** instead of committing more label batches; the head lists at most
   48 unmerged blocks.
10. **The registration lock holds no I/O.** A registry frame reserves its
    offset under a short mutex, is pwritten outside every lock, and returns
    once all earlier frames are written (so its fsync covers them); a failed
    pwrite breaks the registry for the run and open truncates at the hole.
    The hint head is encoded under the lock and written after it. A second
    registrant of the same key waits for the first. With the fsyncs inside,
    the 30-minute Mac soak measured a 92 ms hold; with only the pwrite inside,
    1.8 ms (a pwrite queued behind another registrant's `F_FULLFSYNC`).
11. **In journal mode heads carry no DURABLE_CKPT meaning.** Heads are written
    every commit and made durable by the journal checkpoint; open trusts
    `max(head, replayed journal)` and never adopts a journaled batch its
    journal did not replay.
12. **Backpressure acceptance "credits go to 0"** is measured as samples with
    less than one entry of credit (< 1 KiB) and producer credit waits during
    the flood, then credits > 0 after it.
13. **Journal checkpoints run on their own thread and are paced** (a 50% duty
    cycle per file sync). Back-to-back checkpoint fsyncs queued ahead of the
    journal fsyncs acks wait on, and a long checkpoint on the merge helper
    delayed merges until L0 directories filled.

## 11. Needs a Linux host or ops

Commands for the rows this lane could not measure (run from a flatsql
checkout at the landed commit, `cmake --build cpp/build -j 8 --target
flatsql_ps_bench flatsql_ps_test` first):

- **T1 #7 on host-02** (4 vCPU / 7.9 GiB), on the store disk, both modes:
  `cpp/build/flatsql_ps_bench --mode=host02 --dir=<store disk>/ps-bench --journal=0` and
  `--journal=1`; `cpp/build/flatsql_ps_bench --mode=dirty --dir=<store disk>/ps-bench --journal=0|1`.
  If file mode misses p99 ≤ 40 ms and journal mode meets it, §22.4 ruling 5
  selects `commitJournal` for that host.
- **A8/A35 ops receipt** on host-01 and host-02: `nproc; free -m; df -h <store disk>;
  fio --name=fds --rw=write --bs=16k --size=256m --fdatasync=1 --directory=<store disk>`
  (p50/p99 of `sync`).
- **T1 #6 on a real Linux-8 box**: `cpp/build/flatsql_ps_bench --mode=scaling --io=mem --writers=8`
  and `--io=fs --dir=<ext4 dir>`.
- **LazyFS** power-loss runs (the crash harness emulates the loss modes in
  memory; a filesystem-level run needs Linux with FUSE):
  `lazyfs` mounted at `<dir>`, then `flatsql_ps_test --test=open_clean_1M_records_256_partitions_T1_2_real_fs --dir=<dir>`
  with `lazyfs::clear-cache` between ingest and open.

# Part II: readers and SQL (T2)

## 12. Code map

| Path | Contents |
|---|---|
| `ps/lane.h`, `src/ps/lane.cpp` | Reader instances, lanes, the mailbox ABI (request slots, result rings, parking, cancellation), the native client, `CounterReader` (counters without a lane, A28). |
| `ps/lane_arena.h`, `src/ps/lane_arena.cpp` | Per-lane TLSF arenas behind `SQLITE_CONFIG_MALLOC`, instrumented SQLite mutexes, the null VFS. |
| `ps/snapshot.h`, `src/ps/snapshot.cpp` | The reader's view of committed state: registry view, partition and type snapshots, rows, frames, attributes, posting scans and lookups, lazily read L1 runs, the instance's shared index cache, arrivals, GONE counting. |
| `ps/vtab.h`, `src/ps/vtab_partition.cpp` | Record virtual tables: module, plans (`xBestIndex`), columns, partition-level row sources. |
| `src/ps/vtab_fanout.cpp` | Type level: pruning, the k-way merge, arrivals order with REHOME/GONE joins, the cid catalog, `<TYPE>_current`, offset paging. |
| `src/ps/vtab_meta.cpp` | `flatsql_partitions`, `flatsql_lanes`, `flatsql_licences`, `flatsql_arrivals`, `flatsql_types`. |
| `src/ps/admission.cpp` | Plan admission (bounded bit), the sandbox authorizer. |
| `ps/result_block.h`, `src/ps/result_block.cpp` | RB1 encoder/decoder, raw-stream framing. |
| `cpp/test/ps/{lane,fanout_merge,int64_exact,lane_isolation,snapshot,arrivals_paging,reader_concurrency}_test.cpp` | T2 tests; `reader_fixtures.*` helpers; `vectors/rb1_int64.hex` (the RB1 golden vector for T6's Go decoder). |

## 13. Reader instances and lanes

- A `ReaderInstance` runs L lane threads of one class: **interactive**
  (index-bounded plans only), **bulk** (any plan; `setpriority` nice +10 on
  Linux, QoS utility on darwin) or **sandbox** (untrusted SQL under a work
  budget, A28). Instances share nothing with a writer: they read files only.
- Each lane owns a SQLite connection (`:memory:`, `SQLITE_OPEN_NOMUTEX`, the
  `flatsql_ps_null` VFS that refuses every file open, `temp_store=MEMORY`), a
  TLSF arena (8 MiB interactive and sandbox, 128 MiB bulk) installed through
  `SQLITE_CONFIG_MALLOC` (thread-bound dispatch), lookaside carved from that
  arena, its own file handles (LRU of host handles) and caches.
- SQLite is built separately for the readers (`sqlite3_ps`):
  `THREADSAFE=2`, `DEFAULT_MEMSTATUS=0`, `OMIT_WAL`, `TEMP_STORE=3`.
- Lane threads get explicit stacks (2 MiB default, ≥ 1 MiB enforced) with a
  guard page and a canary word checked every loop iteration (A30).
- **Mailbox ABI** (what the Go router programs against): a pool of request
  slots in instance memory. A slot is a `SlotHeader` (state, cancel word,
  doorbells, budgets, status, statistics) followed by a request area (SQL,
  then RB1-encoded parameters) and an SPSC result ring. Clients claim a free
  slot by CAS, write the request and queue its index on an MPMC queue; an
  idle lane is woken through its doorbell word.
- **Parking (A28).** A lane never blocks on output: when a slot's ring is
  full the statement parks (its `sqlite3_stmt` stays resumable) and the lane
  serves other slots, up to 8 parked statements per lane. A client read that
  frees space rings the owning lane.
- **Cancellation** is the slot's cancel word, polled by the progress handler
  every 4,096 VM steps and by every vtab loop every 4,096 entries (A21, A28).
  The instance's stop word is polled the same way. No deadline, no abandon.
- **A12 announcements.** Each lane publishes the start time of its oldest
  running (or parked) statement; `ReaderInstance::oldestActiveStart()` is the
  minimum. The reclaimer (T3) may unlink files a SWAP retired at time t once
  that minimum is later than t, checked twice a grace apart.

## 14. Snapshot protocol (§8)

1. Each statement re-reads the registry head and parses new frames only
   (copy-on-write view; a changed incarnation rebuilds the view).
2. A partition is snapshotted once per statement at first touch: the valid
   head slot with the highest gen (8 KiB, one pread), then its manifest
   (immutable, cached per lane).
3. Type-level statements snapshot the type head first (labels inline, or the
   A10 checkpoint plus deltas for > 128 partitions, cached incrementally),
   then each partition. **V_p = min(pseq_hi, labeled_through[p]).** A
   partition head older than `labeled_through` (the writer pwrites the head
   right after the commit its type owner may already have labeled) is re-read,
   bounded (4 ms), so V_p never hides a promotion.
4. Rows come from `r-<seg>` (merged range, per the manifest) or the meta
   batch the head's L0 directory names; payloads from `d-<seg>` at
   `(off, len)`, CRC-checked against the row's `dataCrc`.
5. Liveness: a DEAD posting with killer ≤ V_p. Type level: a row is FIRST when
   it has no REPEAT posting (REPEAT is a lookup kind with a bloom; a REPEAT
   label always leaves one), otherwise its latest LABEL decides (A14
   promotions). Readers take no lock shared with a writer.
6. A file named by the snapshot that fails with ENOENT before the first row
   re-snapshots once; after a row it returns `FLATSQL_SNAPSHOT_GONE`
   (retryable). The announcement protocol makes that happen only after a
   restart.

## 15. SQL surface (§9, A17, A18)

Virtual tables are created lazily in the lane's temp schema the first time a
statement names them:

| Table | Rows |
|---|---|
| `sds_p_<producer>__<TYPE>` | every live PUT of one partition (partition-level visibility) |
| `<TYPE>` | the fan-out: one row per live CID, FIRST copies only |
| `<TYPE>@<source>` | `<TYPE>` with a live tag instance of that source |
| `<TYPE>_current` | latest live record per stored supersede key, else per object key (OBJECT_EPOCH) |
| `flatsql_partitions`, `flatsql_lanes`, `flatsql_licences`, `flatsql_arrivals`, `flatsql_types` | meta (never public) |

Record vtabs expose the root table's fields (BFBS reflection, vtable-slot
order; sealed rows project NULL) and hidden columns `_pseq _cid _cid_bin
_epoch _arrival _gseq _producer _source _source_name _provider _batch _peer_id
_signature _data _offset _len _kind _rowid _pid`. `_source` is
`'<TYPE>@<source_name>'`; `_rowid` is `_pseq` on a partition and `_gseq` at
type level.

Plans (`xBestIndex`), each recording a bounded bit in its `idxStr`:

| Access | Claims | Order |
|---|---|---|
| CID | `_cid` / `_cid_bin` EQ (type level: the cid catalog) | – |
| pseq | `_pseq` / `_rowid` EQ or range (partition) | pseq ± |
| gseq | `_gseq` / `_rowid` EQ or range (type: arrivals), OFFSET pushdown | gseq ± |
| COL | indexed column EQ (`col <n> u64pos|str:FIELD` rules) | – |
| tag | `_provider` / `_batch` / `_peer_id` EQ | – |
| source | `_source` / `_source_name` EQ (+ `_epoch` range; the alias) | epoch ± |
| epoch | `_epoch` range or `ORDER BY _epoch` (EPOCH index) | epoch ± |
| default | none of the above at type level (EPOCH_CID index) | seconds DESC, CID ASC |
| full | anything else | unbounded |

A plan is bounded when its index range is closed or a LIMIT applies to an
index order the vtab emits. The fan-out prunes by `_producer` EQ, merges
index-ordered sub-cursors on (key, CID, pid, pseq) with a heap, drops
adjacent duplicate CIDs, and stops at LIMIT.

**Admission.** Interactive lanes read the prepared statement's own program
(`sqlite3_stmt_explain`): any `VFilter` whose plan is unbounded returns
`FLATSQL_NEEDS_BULK` before a row is examined. Every statement reports
rows examined, bytes read, index entries and fence reads.

**Untrusted SQL (A28, §22.4 ruling 7).** Sandbox requests keep the SandboxCaps
contract: one read-only SELECT, the authorizer allow-list mapped onto the
record vtab names (meta tables never public), `<TYPE>` bounded to the newest N
arrivals (A18; `OMM` 400,000, other types 10,000), and a work budget — rows
examined, bytes read, VM steps — that is an admission limit returning the
existing `timeout` code. Sandbox SQL never runs on the shared bulk lane: it is
refused `needs-bulk` there, admitted by plan on interactive lanes, and runs
any plan on a dedicated sandbox lane under the budget.

## 16. Results (RB1, raw stream)

`ps/result_block.h` has the byte layout. Integers are exact 8-byte int64
(no float64 rounding above 2^53, §2); REAL, TEXT and BLOB cells are typed per
cell; an end record carries the status and the work counters. Raw-stream mode
emits `[u32le size][bytes]` per BLOB cell, so `SELECT _data …` streams the
stored frames verbatim. `cpp/test/ps/vectors/rb1_int64.hex` is the golden
vector for T6's Go decoder.

## 17. Index access and caches

- **L0 blocks** are read whole once (CRC-checked) and kept as a kind
  directory; each kind's section (entries, offsets, bloom) is cached separately
  and binary-searched. A statement resolves each snapshot's sections once per
  kind.
- **L1 runs** open lazily: footer and TOC, the metadata CRC checked once per
  lane, then a kind's fences on first use and its bloom while it fits.
- **Ascending scans prime runs lazily.** A run joins a posting merge with a
  lower bound of its first entry (the scan's `lo`, or its first block's fence
  prefix) and reads its first 4 KiB block only when it reaches the top of the
  merge. The default order (`EPOCH_CID`, newest first) and gseq order are
  ascending, so a LIMIT window reads blocks only from the runs it draws rows
  from, not one block from every run of every partition. Descending scans
  prime eagerly (a fence gives no upper bound).
- **The instance's shared cache** holds these immutable pieces (L0
  directories and sections, fences, blooms) for every lane: 64 shards, each
  only ever try-locked, so a busy shard is a miss and a lane never waits on
  another lane (writers never touch it). A lane-private front map serves
  repeated hits without any lock.
- Default budgets: 16 MiB shared per instance, 4 MiB private per lane,
  4,096 host handles per lane (the host's handles are virtual; T5 bounds real
  fds with its LRU).

## 18. Arrivals, offset paging, dead history (A14, A15)

- Arrivals rows merge-join two type postings by gseq: **REHOME** (the latest
  promotion keeps the gseq, A14) and **GONE** (a gseq whose last live copy
  died; new, posted by the type owner). No per-entry lookup.
- **Offset paging.** `ORDER BY _gseq|_rowid [DESC] LIMIT n OFFSET k` pushes the
  OFFSET into the vtab. Live entries in positions [a, b) = (b − a) − GONE
  gseqs in [g_a, g_b), counted from L1 fences (whole blocks by their counts,
  the two edge blocks read) and the unmerged L0 sections; a binary search over
  positions finds the k-th live entry in O(log n) probes.
- **Dead runs.** A GONE entry starts a gallop over the same counts to the next
  live position; short runs fall back to per-entry checks.
- `flatsql_types` exposes MaxRowID (`gseq_hi`), TotalCount
  (`first_live_count`) and the SnapshotID inputs (A16).

## 19. Writer-side changes made by T2

T2 owns the whole flatsql component; these changes to T1's writer serve the
readers and are covered by T1's suites as well as T2's.

| Change | Why |
|---|---|
| Index kind `EPOCH_CID` (19): complemented `floor(epoch_ms/1000)` + the A17 CID key → PUT pseq | A19's default order (seconds DESC, text CID ASC) as one ascending index scan, with copies of a CID adjacent for the merge. |
| Type kind `GONE` (0x204): a gseq whose last live copy died | Dead arrivals counted by fences: offset paging (T2 #7), dead-run skipping (T2 #8), merge-joined arrivals. |
| `REPEAT` (0x203) is a lookup kind (bloom) | The type-level FIRST test is a bloom probe for most rows; LABEL is read only for REPEAT copies. |
| SWAP seam: `Engine::swapSegment`, ctl `SWAP{seg, gen, oldCgen}`, manifest segment `cgen`, `c-<seg>-<gen>.{fsd,fsr,fsa}`; the owner reads compacted files, open replays SWAP | The smallest seam T3's compaction plugs into (§20 #5 uses a verbatim copier through it). |
| Catalog precedence at equal tcs: DEAD > PROMOTED > REPEAT > FIRST (writer `upsertCopy`, reader `catalog`/`labelOf`; REHOME candidates of one tcs resolved by liveness) | **T1 defect** found by the fan-out oracle: a batch that promoted a REPEAT and then killed it left two entries with one tcs; value order made the copy read as live, and a returning copy was labeled REPEAT instead of FIRST. |
| A cid's catalog is gathered, resolved per copy and loaded without its dead copies | **T1 defect** found by the A14 reader test: a cid killed and re-added over 1,000 times overflowed the per-batch copy table, dropped its live REPEAT, and died without an heir (63 of 64 promotions at round 1,027). |
| Writer threads tag themselves for the lock report | Lock-set disjointness instrumentation (T2 #1). |
| Test fixture `buildRecord` passes a NUL-terminated file identifier | `FinishSizePrefixed` takes a C string; the fixture passed the type's four `fid` bytes, and Debug builds (CI) asserted when the next byte was not zero (T2's type variants). |

## 20. Acceptance (§18 T2 as amended)

Machines: **Mac** = the owner's Mac Studio (Darwin 25.3.0, arm64, 28 hardware
threads), shared with other lanes; the 1-minute load average is recorded per
run (20–46 during these). **Mac mem** runs use the in-memory test host (no
fsync), **Mac APFS** the native host on APFS (`F_FULLFSYNC`). **Docker** =
Linux 6.12 arm64 in Docker Desktop on that Mac (a 16-vCPU VM shared with the
Mac's load), either `--cpuset-cpus 0-7` (8 pinned vCPUs, "Docker-8") or
`--cpus 8` (a CFS quota over 16 vCPUs, whose throttling periods show up as
100 ms+ tails; used only for correctness and disjointness); ext4 on the VM's
disk with `fdatasync`. None is a quiet Linux-8 box: latency bounds are enforced
by the tests only on one (a release build, 8+ usable hardware threads, load
below a quarter of them) and reported everywhere else. Numbers come from
`MEASURED` lines of `flatsql_ps_test` and `flatsql_ps_bench --mode=test` on
task-branch builds of 2026-09-28; rows 1, 4 and A28 are from the final build
(lazy run priming, §17).

| # | Acceptance | Result | Where |
|---|---|---|---|
| 1 | 8 writers saturate 256 partitions, 16 lanes, 10 min: `flatsql_partitions` p99 ≤ 1 ms; window LIMIT 1000 p99 ≤ 10 ms; reader and writer lock sets disjoint; max lane lock wait ≤ 1 ms | **Disjointness and lock wait pass everywhere; the latency bounds are not met on any box measured (all of them CPU-saturated by the test and by other work: needs a quiet Linux-8 box).** Every SQLite mutex and the shared index cache are acquired by lanes only (writer acquisitions 0 in every run); max lane lock wait 0.000–0.022 ms (the cache is only ever try-locked). The engine's own cost, writers stopped and one client (100 statements, 1–4 L0 blocks per partition after merges, 4.7–18 M records): Mac window p50 3.0–8.1 ms, p99 6.3–48 ms following the Mac's load, `flatsql_partitions` p99 0.95–1.3 ms; Docker-8 in-memory window p50 6.3 / p99 60 ms, ext4 p50 14.6 / p99 154 ms. Under saturation the lanes share the CPUs with 8 producer threads, the writers and their merges: Mac mem 40 s at load 46 (15.4 M records): `flatsql_partitions` p99 6.5 ms, window p50 14.5 / p99 91 ms, source window p99 77 ms, counters without a lane (A28) p50 0.42 / p99 2.0 ms. Docker-8 in-memory 30 s at VM load 12 (8.6 M records): `flatsql_partitions` p99 15.7 ms, window p50 75 / p99 259 ms. Docker-8 ext4 60 s at VM load 19 (8.1 M records): `flatsql_partitions` p99 24 ms, window p50 278 / p99 762 ms (an earlier build's run at VM load 18: p50 3.6 / p99 347 ms; host contention decides). Mac APFS 10 min at load 16 (earlier build, 14.8 M records, fsync-bound): 0 errors, window p50 70 / p99 370 ms. | `readers_under_saturating_writers_T2_1[_full]` (`--dir` for a real file system) |
| 2 | 10,000 randomized workloads (a CID across up to 8 producers, supersede, tombstones, epoch-less types) equal a brute-force reference: one row per CID, FIRST semantics, order; rows examined ≤ LIMIT × (partitions + 1). A14: readers during FIRST deaths never miss a live CID | **Pass.** Mac mem: 10,000 workloads (49,833 promotions, 10,064 type deletes, 22,043 supersedes, 13,119 RECONCILEs), 229 s; every Q1–Q5 check (default order, LIMIT prefix and its examined bound, CID lookups, gseq order, partition counts) against the Inspector's reference. A14: 10 min, 2,979 rounds of FIRST deaths, 190,656 promotions, 5,179 reader statements, 0 missing CIDs, 0 gseq changes. Two T1 defects were found on the way (§19). Docker (`--cpus 8`): 500 workloads pass. | `fanout_randomized_vs_bruteforce_T2_2_full`, `fanout_readers_during_first_deaths_A14_full` |
| 3 | −2^63, 2^53+1, 2^63−1 round-trip exactly through RB1 (C++ encoder; Go decoder vectors for T6) | **Pass.** Encoder/decoder in any byte split, the golden vector `vectors/rb1_int64.hex`, and parameters and literals through a lane. Docker: pass. | `int64_rb1_encoder_decoder_exact_T2_3`, `int64_exact_through_sql_lane_T2_3` |
| 4 | Bulk statement past its 128 MiB arena → `SQLITE_NOMEM`, interactive p99 within ±10%, nothing poisoned; unbounded plan on an interactive lane → `FLATSQL_NEEDS_BULK` in ≤ 1 ms with 0 rows examined | **NOMEM, no poisoning and NEEDS_BULK pass; the ±10% ratio holds on Docker-8 and is noise-bound on the Mac.** Every hog returns `kRsNoMem`; the same lane then answers, its arena passes a full consistency check and holds 160 KiB. NEEDS_BULK: lane time p50 0.012–0.016 / p99 0.051–0.099 / max 0.17 ms, 0 rows examined, over 200 unbounded shapes (Mac and Docker). Ratio (p99 with the hog / without, four alternating rounds): Docker-8 at VM load 2.0 and 3.5: 1.030 and 1.087 (rounds 0.95–1.09 and 1.02–1.43); Docker `--cpus 8`: 0.947; Mac control runs (no hog) 1.02–1.05, with the hog 0.94–2.3 at load 20–47 (one busy core streaming 300 MB beside the lanes; no shared lock, see #1). | `lane_isolation_bulk_nomem_T2_4`, `lane_needs_bulk_under_1ms_zero_rows_T2_4` |
| 5 | 1,000 compaction SWAPs under load: statements started before a SWAP complete with pre-SWAP rows. A12: 10-min bulk export under continuous compaction, 0 SNAPSHOT_GONE | **Pass.** Mac APFS: 1,000 SWAPs while ingest runs, 2,406 partition scans verified row by row against the frames sent, 987 of them spanning a SWAP (up to 28 SWAPs during one statement), 0 errors, every retired file set unlinked once announcements allowed. A12, Mac APFS 10 min: 2,769 type-level exports (84.5 M frames), 8,153 SWAPs, 8,090 retired sets unlinked, 0 SNAPSHOT_GONE. Negative control: unlinking without the announcement check gives SNAPSHOT_GONE. Docker (`--cpus 8`, in-memory): 1,000 SWAPs, 56 partition scans all spanning SWAPs (up to 257 during one), 0 errors; A12 10 min: 18,891 exports (296.5 M frames) across 300 SWAPs (the in-memory cap), 0 SNAPSHOT_GONE. | `snapshot_swaps_under_load_T2_5`, `snapshot_bulk_export_under_compaction_A12_full --dir=…`, `snapshot_unlink_without_announce_is_snapshot_gone` |
| 6 | Arrivals paging while 8 writers produce: gseq strictly increasing within and across pages; no gseq ≤ the snapshot head appears after the snapshot; union of pages = the FIRST-live set at the snapshot. A14: a CID's gseq never changes while a copy lives | **Pass.** Mac mem: 62 syncs (18,716 pages of 500) against 1.44 M records from 8 producers into 32 partitions; every sync equals the oracle (final arrivals ≤ its MaxRowID), 0 violations, 0 gseq changes. Docker: 46 syncs, 9,648 pages, 921,888 records, 0 violations. | `arrivals_paging_while_writers_produce_T2_6` |
| 7 | Page 10,000 of 100 rows on a 3 M-record type examines ≤ 2 × 100 rows plus fence reads | **Pass.** Mac mem, 3,000,000 records, 10% scattered deaths: OFFSET 999,900 in gseq order examined 100 rows and 132 arrival entries with 268 fence reads, 2.5 ms; pages equal a full scan's (60 random offsets, ascending and descending, 30% deaths). Docker: 100 rows, 150 entries, 196 fence reads, 7.3 ms. | `offset_paging_page_10000_T2_7_full`, `offset_paging_matches_reference_with_deaths` |
| 8 | A full sync of a 99%-dead history examines ≤ 2 × (live rows + fence reads) | **Pass.** 100,000-entry history, 1,000 live. Deaths clustered: 1,000 rows and 1,008–1,009 arrival entries examined, 724–1,052 fence reads. Deaths scattered (1 in 100 live): 1,000 rows and 2,001 entries examined, 57,388–67,740 fence reads (deviation 4). Docker: clustered 1,000 rows / 1,008 entries / 707 fence reads; scattered 1,000 / 2,001 / 57,492. | `dead_history_full_sync_T2_8` |
| A30 | A maximum-depth SQL expression on a lane | **Pass.** A depth-1000 expression (`SQLITE_MAX_EXPR_DEPTH`) runs; depth 1001 is refused; the lane serves on, canary intact. | `lane_max_depth_expression_A30` |
| A28 | 8 clients reading at 10 KB/s: counter p99 ≤ 10 ms, window p99 ≤ 50 ms. A sandbox cartesian join returns `timeout` while the bulk lane stays free | **Counter bound met on the Mac and Docker-8 in-memory; window bound met only at Mac load 20; the sandbox contract passes.** 8 slow clients park their lanes while others read. Mac mem 40 s: counter p99 1.1 / 4.1 / 2.4 ms and window p99 31.9 / 58 / 64 ms at load 20 (earlier build) / 31 / 46. Docker-8: counter p99 6.4 (in-memory) and 14.1 ms (ext4), window p99 148 and 224 ms at VM load 12 and 19. The join returns `timeout` from the work budget while a bulk statement completes in 1.5–2.3 ms; meta tables are not public; sandbox SQL is refused on the shared bulk lane. | `readers_under_saturating_writers_T2_1_full`, `lane_sandbox_contract_A28` |

A12's announcement protocol, A14's promotion reads, A15's page fill and
MaxRowID, A17's CID order in the default merge, A18's `_source` and bounded
`<TYPE>`, A21's cancellation polls, A28 and A30 are covered by the rows above
and `lane_test.cpp`.

## 21. Design deviations (T2)

Each entry: what the design says, what was built, why, and the evidence.

1. **Fan-out sub-cursors run inline; no idle-lane borrowing** (§5.3: "borrows
   idle lanes … as sub-cursor producers … when no lane is idle, the
   sub-cursors run inline"). Only the inline form is built. A partition
   sub-cursor costs its sections' binary searches and one block per L1 run;
   the measured window cost (§20 #1) is dominated by per-row label, liveness
   and row reads that a producer lane would not remove. Borrowing is an
   optimization T6 can add behind the same `RowSource` interface.
2. **Type-level FIRST test per row, not a REPEAT merge-join** (§8.6). Index
   order is not pseq order, so a merge-join with the REPEAT run applies only to
   pseq scans. Rows test the REPEAT bloom (REPEAT became a lookup kind); only
   rows with a REPEAT posting read their latest LABEL. Arrivals order does
   merge-join (GONE, REHOME).
3. **Offset paging by GONE counts in arrivals order, not per-block live-FIRST
   counts in L1 fences** (§9, A17). T1's merges write each block's entry count
   as its live count, and liveness changes after a merge; a new type kind,
   GONE, is counted by fences instead (§18). Offset paging is pushed down for
   `_gseq`/`_rowid` order (the data explorer's order); the default and CID
   orders page through SQLite (O(offset)).
4. **A15 #8 without arrivals compaction.** Arrivals compaction (dropping dead
   entries) is T3's. Dead runs are skipped by galloping over GONE fence counts;
   the bound holds for rows and for arrival entries examined, but a history
   whose deaths are scattered one-in-a-hundred costs about 60 fence probes per
   live row (§20 #8) until T3 compacts it.
5. **RB1 cells are typed per cell** (§9: a type vector per block). SQLite
   values are dynamically typed per row.
6. **One result ring per request slot** (256 KiB default), parked per
   statement, rather than one 1 MiB ring per lane (A28 parking is per
   statement either way).
7. **Sandbox work budget adds VM steps** to rows examined and bytes read, so a
   statement that reads nothing (a recursive CTE) is bounded too; the A18
   window counts the newest N arrivals entries (dead included), not N
   FIRST-live records (one entry read instead of a count).
8. **`<TYPE>@<source>` matches live tag instances of the FIRST copy's PUT** in
   its partition. An untagged record never matches a source (the legacy
   `_source` falls back to the producer token for untagged rows); T6's
   equivalence run on a migrated copy decides whether that case exists.
9. **Type-level visibility is labeled rows only**; the A20 read-your-writes
   scan of the unlabeled gap belongs to `GetRecord` (T6), which can use the
   partition vtab (partition-level visibility is the acked HWM).
10. **Interactive lanes at nice −5 on ≤ 2 vCPU** (A28) needs privileges the
    engine does not have natively; bulk lanes do take nice +10 on Linux. The
    host (T5/T6) sets both.
11. **Shared index cache, try-locked.** The design budgets a 16 MiB block cache
    per reader instance; the built cache holds parsed index pieces (L0 sections,
    fences, blooms) for all lanes of an instance and is only ever try-locked, so
    lanes never wait on each other. Per-lane budgets (4 MiB) hold manifests and
    run views; 4,096 host handles per lane (virtual handles; T5 bounds real fds).
12. **SWAP seam built in the writer** (T3's scope). `Engine::swapSegment` copies
    a fully merged sealed segment verbatim to `c-<seg>-<gen>.*`, commits SWAP
    with a new manifest, and reports the retired files; it has no
    INTENT_COMPACT (T3 adds it with crash handling). Rows stay dense by pseq.
13. **Lock disjointness natively covers SQLite's mutexes and the shared
    cache**; the host handle table is measured under WasmEdge (A29, T5/T6).
14. **CounterReader reads heads** (O(partitions) preads), the A28 path without
    a lane; the Go snapshot refreshed on every acked commit is T6's.

## 22. Running the T2 tests; what needs a Linux-8 box

```
cmake --build cpp/build -j 8 --target flatsql_ps_test flatsql_ps_bench
cpp/build/flatsql_ps_test --test=lane_                     # lanes, parking, sandbox, isolation, A30, C ABI
cpp/build/flatsql_ps_test --test=fanout_randomized_vs_bruteforce_T2_2_full   # 10,000 workloads
cpp/build/flatsql_ps_test --test=fanout_readers_during_first_deaths_A14_full # 10 min
cpp/build/flatsql_ps_test --test=snapshot_ [--dir=<real fs dir>]
cpp/build/flatsql_ps_test --test=snapshot_bulk_export_under_compaction_A12_full --dir=<dir>  # 10 min
cpp/build/flatsql_ps_test --test=offset_paging_page_10000_T2_7_full           # 3 M records
cpp/build/flatsql_ps_bench --mode=test --test=readers_under_saturating_writers_T2_1_full [--dir=<dir>]
```

The default suite runs shortened forms of every slow test. Long runs that
write a lot use `--dir` (the in-memory test host keeps every file it held).

**T2 #1 and #4 on a quiet Linux-8 box** (8 cores, ext4, nothing else
running), from a checkout at the landed commit:

```
cpp/build/flatsql_ps_bench --mode=test --test=readers_under_saturating_writers_T2_1_full --dir=<ext4 dir>
cpp/build/flatsql_ps_test --test=lane_isolation_bulk_nomem_T2_4
```

Both enforce their bounds on such a box (a release build, 8+ usable hardware
threads, 1-minute load below a quarter of them; the load is sampled at the end
of the run, so the test's own writers count). The disjointness claim itself is
measured under WasmEdge with the C host module in T5/T6 (A29).

What to expect there: the window's own cost is 3–8 ms at p50 (§20 #1), so the
10 ms p99 bound leaves little room for waiting on a CPU. The test runs 8
producer threads, the writers and their merges and 16 lanes on 8 cores; when
they saturate the CPUs, the window p99 is decided by the scheduler, not by the
engine. If the bound fails there, the next steps are the design's own levers:
fewer writer threads than cores (design §5.1: `clamp(cores−2, 1, 16)`), interactive
lanes at a higher priority than writers (design A28 gives nice −5 only on ≤ 2 vCPU),
and idle-lane sub-cursors (§21 deviation 1).

# Part III: compaction, reclamation and quota (T3)

## 23. Code map

| Path | Contents |
|---|---|
| `ps/compaction.h`, `src/ps/compaction.cpp` | Manifest version 2, the compacted rows format, retire items; the compaction pipeline (plan, build, SWAP, apply, abort). |
| `src/ps/reclaim.cpp` | The per-partition file ledger (`disk_bytes`), the RETIRE and UNLINKED records, reader-gated unlinking, meta-segment retirement (A9), open-time cleanup; the same for the type logs (meta log rotation and retirement, retired catalog runs and manifests, the type ledger). |
| `ps/quota.h`, `src/ps/quota.cpp` | The quota planner, the space emergency, the ballast. |
| `src/ps/sources-t3.cmake` | Registers the T3 sources and tests on the targets `cpp/CMakeLists.txt` defines. |
| `cpp/test/ps/{compaction,orphan,quota}_test.cpp` | T3 tests. `checkPartitionDir` and `checkTypeDir` (fixtures) walk a partition's or a type's directory against the files its head and manifest name. |

## 24. Compaction (§11, A11, minor 1)

A compaction rewrites a run of sealed, fully merged segments `[seg, segEnd]`
into one new file set, `c-<seg>-<gen>.{fsd,fsr,fsa}` plus
`x-<seg>-<gen>.fsx` and `mf-<gen>.fsm`:

1. The owner plans. `INTENT_COMPACT{seg, segEnd, gen}` rides the next batch; no
   output exists before it is durable. The head carries the intent.
2. A compaction thread builds. It reads the DEAD and TAG_DEAD postings over the
   inputs' pseq range from every run and L0 block at or after the inputs, keeps
   a killer only when it is at or below the plan's kill bound (the type owner's
   `labeled_through`), and decides per row:
   - PUT, LICENCE and CTL rows with a killer go;
   - RETAG rows go when their instance is TAG_DEAD, or their PUT is dead or gone;
   - TOMB, CTL_TOMB and TAG_TOMB rows go only when an earlier SWAP removed their
     target (minor 1): outside the inputs, or VOID within them.

   It copies the surviving frames, attributes and rows in 4 MiB slices, filters
   the inputs' L1 runs down to the surviving rows' postings (every partition
   posting's value is the pseq of the row that emitted it), fsyncs everything,
   and re-checks the owner word and an abort flag before every mutation (A26).
3. The owner, once no merge is in flight, writes the manifest (its fsync joins
   the next round's first sync phase) and queues `SWAP{seg, gen, oldCgen,
   segEnd}`; the SWAP, the RETIRE set and the counters' adjustment ride one
   batch. Publish attaches the new files.

Rows keep their pseqs. `c-*.fsr` holds the surviving rows (dense by rank) and a
presence directory (a bit per pseq, a rank per 512); a pseq the directory
lacks reads as a `VOID` row (kind 8, never stored, never live). A segment with
no survivor is `EMPTY` in the manifest and has no files.

Candidates (maintenance, every 50 ms per warm partition, and at once after a
SWAP): the sealed segment with the largest dead share at or over
`compactDeadRatio` (15%; dead frame bytes counted as kills commit), joined by
its adjacent segments past the ratio too (one output per wave, within the
coalescing limits); adjacent sealed segments under 8 MiB, coalesced up to 16
at a time and 64 MiB of output into one segment named by the first (`lastSeg`
in the manifest; rows' `seg` is rewritten); after a restart, when the
partition's dead share is over the ratio and the per-segment counts are
unknown, the oldest
segment not yet surveyed (a build that would save under 10% is abandoned and
its count remembered). `Engine::swapSegment` requests one (targeted, or the
oldest-generation compactable segment).

Kill bound and readers. A type-level reader's visibility is `V_p =
min(pseq_hi, labeled_through[p])`. A compacted segment records `killThrough`
(the largest killer behind a removed row, the inputs' own, and the bound of
the SWAP that removed a dropped tombstone's target) and `prevGen` (the manifest
before its SWAP). A type-level snapshot whose `V_p` is below a segment's
`killThrough` reads that segment through `prevGen`, repeatedly for a segment
compacted more than once; the reader gate keeps those files for any statement
older than the SWAP. Partition-level readers see `pseq_hi`, which is never
below a SWAP's bound.

Counters: `total_count`, `total_bytes` and `tomb_count` count the rows the
files hold (a SWAP subtracts what it removed), so they equal a recount;
`min_epoch`, `max_epoch` and `latest_arrival` stay bounds over everything ever
appended.

## 25. Reclamation (A12) and meta segments (A9)

- **Retire.** A MERGE_DONE retires the folded runs and the manifest it
  replaces; a SWAP retires the inputs' files, their runs and the manifest. The
  whole outstanding set is a `RETIRE` ctl record in the same batch (the head
  points at it: `retireSeg/retireOff/retireN`); consecutive manifests collapse
  into one range item.
- **Unlink.** Maintenance unlinks a retired file with `UNLINK_IF_UNUSED` once
  the reader gate (the start of the oldest running reader statement) is past
  the time its retirement became durable, checked twice a grace apart (60 s by
  default); in journal mode once every journal record older than the
  retirement is checkpointed; a meta segment once a DURABLE_CKPT head names a
  later `first_live_m_seg` (a quiet partition writes one; later items do not
  wait for it, and a partition with retired files never goes cold). Each pass
  takes the first check of every file past the gate, so a backlog goes about
  one grace after its statement ends (`reclaim_backlog_goes_in_one_grace`:
  133 files in 0.31 s at a 0.3 s grace). BUSY (a handle is open) skips to
  later items and retries after 50 ms; idle
  reader lanes close cached handles after 5 s. The next batch carries
  `UNLINKED` and the shrunken set; `disk_bytes` drops then. A set past 2,048
  items unlinks its oldest regardless of the gate (a statement older than
  hours of retirements gets the retryable SNAPSHOT_GONE instead of a writer
  waiting on it).
- **Open** unlinks every file of the persisted set (a file a surviving reader
  instance still holds stays retired), the outputs of an INTENT_COMPACT
  without SWAP, and r/a files a first merge created without MERGE_DONE, then
  writes a durable head.
- **Meta segments (A9).** A sealed `m-<seg>` whose batches are all merged is
  retired by a batch that also re-emits the full lane table (LANE_CKPT) and the
  RETIRE set, whose header carries the manifest generation, `merged_through`,
  `first_live_m_seg` and `next_gen`; `first_live_m_seg` advances when that
  batch commits. A rebuild of a head whose both slots are torn probes for the
  first existing meta segment and replays from its first batch.
- **Type logs.** A type's MERGE_DONE retires the catalog runs it folds and
  the manifest it replaces. The new manifest persists the whole outstanding
  set (these and any still waiting) in an appendix after its CRC trailer,
  `[magic FSTR][n][n x RetireItem][crc][pad]`, which readers that predate it
  never read. Unlinking follows the partition rules (reader gate twice a
  grace apart, journal safe time, BUSY retried after 50 ms). Open unlinks the
  appendix's files, the outputs of a merge a crash cut short (it sweeps the
  generations after the manifest's to `next_gen + 64`, and 16 past the last
  leftover it finds, since a head's `next_gen` can lag), and anything below
  `first_live_m_seg`. A failed merge unlinks its outputs at once (durably;
  one that cannot go yet is retired like the rest) and retries with the same
  generation. At stop a merge in flight closes its handles; an unfinished
  build's outputs go, a finished one's are left to open (a MERGE_DONE already
  in the log may name them).
- **Type meta segments.** The type meta log starts `m-<seg+1>` once it passes
  `typeMetaSegBytes` (64 MiB): the sealed segment is cut at the end of its
  last batch and synced, the new one created, and a durable head names it.
  Open follows the batch chain from a segment's end into the next one, so a
  stale head slot loses nothing. With more than 128 partitions the new
  segment's first batch is a FULL_LABELS checkpoint, and readers read label
  deltas across segments. The oldest sealed segment retires once no unmerged
  L0 block, merge input or label checkpoint lives in it: `first_live_m_seg`
  (a type head field) is written to both head slots and synced, then the file
  waits for readers like any retired file.
- **Dead copies leave the catalog (A15, catalog half).** A type merge drops the
  CID, LABEL and REPEAT entries of every copy killed within its inputs whose
  first posting (FIRST or REPEAT) is also among them: a copy never leaves
  DEAD, and no older run holds any of its postings. A full fold drops every
  dead copy. GONE and REHOME entries stay: they are keyed by gseq for
  arrivals order, and arrivals are not compacted yet (§30).
- **Reader gate in production.** The writer is its own wasm instance: the host
  reads every reader instance's lane announcements
  (`FlatsqlPsReaderLayout.laneAnnounce`) and passes the minimum through
  `flatsql_ps_reader_gate` (T6 wires it). Natively, `Engine::setReaderGate`.

## 26. Disk bytes (§13)

`disk_bytes` is the exact size of the files a partition names: a ledger of the
stable files (sealed segments' d and m, r/a, c-*, runs, manifests, the next
segment's pre-created files, retired files until UNLINKED) plus the extents of
the files still growing (h, l, the active d and m, zero-fill included). Open
rebuilds it from the head, the manifest and file sizes; the owner maintains it
on every append, merge, seal, SWAP and unlink. After every reopen in the crash
tests, a walk of each partition directory finds exactly the named files and a
total equal to `disk_bytes`. Head files are written as whole slots until they
hold both, so a head write never grows the file again.

A type's disk bytes are kept the same way: its head, meta segments from
`first_live_m_seg`, arrivals segments and fence, catalog runs, manifest, a
built merge's outputs until MERGE_DONE, and retired files until they are
unlinked, maintained on every write, seal, rotation, merge and unlink and
rebuilt at open from file sizes. Type configs (`s-*.fsc`) are registration
and not counted. The crash tests check the type directory the same way.

## 27. Quota and the full disk (§13, A13, §22.4-3)

- **Planner** (writer 0's maintenance, every 100 ms): usage = Σ partition
  `disk_bytes` + Σ type log bytes, less retired files waiting only for
  readers. Over the cap
  (`EngineConfig::quotaBytes`, `Engine::setQuota`, `flatsql_ps_set_quota`), it
  evicts whole sealed segments in arrival order across partitions (their
  `minArrival` zone) down to 0.85 of the cap: a `TOMB_RANGE{seg, all}` on the
  owner (bounded 512-row steps; supersede-lane heads and control kinds are
  spared; each step also stops after 4 ms of CPU), then, once the tombstones
  are durable, a compaction of the segment. One wave at a time.
- **ENOSPC.** A commit that fails NOSPACE starts an emergency: record entries
  stop being consumed (producers see zero credits; nothing is acked), the
  ballast file (`fsql2/ballast`, `ballastBytes`) is released, and waves evict one
  segment at a time down to 0.85 of the usage at the failure, and at least the
  ballast's size, but never more than half the store in one episode. Once
  usage is under that cap the ballast is recreated and ingest resumes. NOSPACE
  in a merge, a compaction, a head write or a segment creation is transient;
  a merge that failed retries after 100 ms (a type merge with the same
  generation when it left nothing behind). A partition whose L0 directory is
  full stages no rows at all (entries, mailbox kills, TOMB_RANGE steps) until
  a merge frees a slot.

## 28. Acceptance (§18 T3 as amended)

Machines: **Mac** = the owner's Mac Studio (Darwin 25.3.0, arm64, 28 hardware
threads), shared with other lanes (1-minute load recorded per run). **Mac mem**
runs use the in-memory fault host (no fsync). Numbers come from `MEASURED`
lines of `flatsql_ps_test` (RelWithDebInfo) on the task-branch build landed on
2026-09-28, run one after another at load 28–38.

| # | Acceptance | Result | Where |
|---|---|---|---|
| 1 | A partition with 50% dead bytes compacts to ≤ 55% of its prior disk bytes; owner commit p99 and interactive lane p99 ≤ 1.5× baseline during compaction; 1,000 pre-SWAP statements complete correctly | **Size and statements pass; the latency ratio is not met on the Mac (needs a quiet Linux-8 box).** Mac mem, load 31–35: 4 partitions × 20,000 OMM records, half killed: 106.4 MB → 51.9 MB (0.488×); 1,003 bulk statements that started before a SWAP returned exactly the live rows with their bytes, 0 errors (2,177 SWAPs). Latency over the whole SWAP period (the compaction plus the re-compactions that produced those statements; 16,093 samples): ack p99 0.133 → 18.5 ms, interactive lane p99 0.339 → 3.75 ms, commit round p99 1.05 ms. | `compaction_half_dead_T3_1_full` |
| 2 | CAT supersede, 20% changed per cycle, 100 cycles: disk plateaus ≤ 1.3× live bytes; A9: `m` bytes ≤ one active plus one sealed segment | **Pass with the 15% trigger (deviation 24), "live bytes" read as the live set's compacted footprint.** Mac mem, load 31–33: 20,000 CAT objects, 100 cycles (each updates a random 20%), the store measured at rest after each cycle: disk peaked at 1.120× the footprint of the same live set fully compacted (19.5 MB); 346 compactions, 520 meta segments retired, 5,291 files unlinked; at most 1 meta segment on disk (A9). With §11's 25% trigger it peaks at 1.31–1.38× (six runs). Against the records' own bytes (Σ len − 4 of live PUTs, 4.9 MB) the disk is 4.5×: a fully compacted CAT record costs about 4× its frame in rows, attributes and postings, which no compaction removes | `compaction_cat_supersede_plateau_T3_2_full` |
| 3 | Quota at 80% of current bytes: oldest records evicted first; no supersede-lane head evicted; disk ≤ cap within 3 passes; each eviction step ≤ 10 ms | **Pass (arrival order, §22.4-3).** Mac mem, load 33–35: the store with its type logs 14.1 MB, cap 11.3 MB: under the cap after 1 pass (14 segments), 0 of 256 CAT heads evicted, in each partition the evicted records are exactly its oldest (0 holes); step max 3.4 ms over 15 steps. Across partitions eviction is by whole segment, so a survivor can be older than an evicted record of another partition (288 of 6,367 records here). The 10 ms bound is enforced on a quiet box (the wasm suite at load 41 measured past it). | `quota_arrival_order_heads_spared_T3_3` |
| 4 | 1,000 crash points during compaction: exactly one of the old or new set is live, 0 orphans, 0 acked records lost; A12: crash points inside the grace window, Σ on-disk sizes = Σ `disk_bytes` after each reopen | **Pass.** Mac mem, load 28–38: 3,224 trials, 1,000 with a compaction planned but not applied at the crash, 1,744 with retired files waiting; all five crash modes (664 drop-all, 637 drop-subset, 658 torn, 623 reordered, 642 kill -9); 41,825 compactions, 291,864 files unlinked, 24,719 meta segments retired (partitions and the type's), 723,969 dead catalog entries folded out; after every reopen each partition directory and the type directory held exactly the named files with sizes equal to the engine's disk bytes, every acked live record live with its bytes, every acked kill dead, counters equal to a recount; once labels caught up, `first_live_count` equal to the live records and 16 catalog lookups right. 507 s. The T1 crash harness (merges, A9, reclamation; compaction off: its oracle keeps dead bytes) checks the partition directories after every reopen too; the wasm build runs it ×1000. | `orphan_crash_points_during_compaction_T3_4_full`, `crash_faults_T1_1` |
| 5 | Hot split at 4× single-writer capacity with 4 helpers | **Not built** (§30). | – |
| 6 | Fill the disk to ENOSPC under ingest: ingest resumes with 0 acked losses and no operator action | **Pass.** Mac mem, load 34–35, a 16 MiB device, 1 MiB ballast, two OMM producers for 4 s: 17 episodes, the ballast released 17 times and restored 16, 41,112 records acked after a recovery, 6,318 alive at the end; in each partition the surviving acked records are exactly its newest (0 holes); after a reopen (before the engine starts) the partition and type directories hold exactly the named files at the engine's disk bytes. Before type-log reclamation the type logs filled 16 of the device's 17 MB and the run ended in the emergency with no survivor. | `quota_disk_full_resumes_without_operator_T3_6` |

§22 amendments naming T3:

| Amendment | Status |
|---|---|
| A9 meta-segment retirement | Built (§25). |
| A11 intents | INTENT_COMPACT built; merge and compaction intents coexist in one batch. |
| A12 reclamation lifecycle | Built (§25): RETIRE/UNLINKED, reader gate twice a grace apart, open replays the set. |
| A13 ENOSPC, ballast, arrival order | Built with deviations 12–13. |
| A15 arrivals compaction | **Half built**: dead catalog copies are folded out of the type's runs and the type logs are reclaimed and counted (§25–§27); arrivals entries and GONE postings of dead gseqs stay (§30). |
| A26 stage-1 prep states | Not built (with #5). |

## 29. Design deviations (T3)

1. **Compaction builds on its own threads** (§11: the owner in 4 MiB / 10 ms
   slices; helpers only for split partitions). As T1 deviation 1 did for
   merges: a build on the writer stalls every partition it owns. The builder
   re-checks the owner word and an abort flag before every mutation, and a
   HANDOFF waits for it to stop (one slice).
2. **The owner writes the manifest at SWAP time**, synced in the next round's
   first phase, instead of the builder: merges of the active segment continue
   while a build runs; only the SWAP waits for no merge in flight.
3. **Kill bound and `prevGen` fallback** (not in the design): a row is removed
   only when its killer is labeled, and a type-level snapshot below a
   segment's bound reads the previous file set. Without it, a type-level
   statement whose type snapshot predates a death could miss a live copy
   (A14).
4. **Counters are physical** (§24): recounts equal them after compaction.
5. **Coalescing** adjacent small segments (§11 "adjacent sealed segments under
   8 MiB each") into one output named by the first segment.
6. **Minor 1 extended**: a tombstone whose target is VOID in the same inputs
   (an earlier SWAP of that segment removed it) goes too. Without it the
   supersede tombstones of a partition compacted in one piece never went
   (measured: the CAT plateau grew by 77 KB a cycle).
7. **Tombstone-only segments seal by age** (T1 sealed only segments with
   frames), so a partition that stops ingesting can drop its tombstones.
8. **Reader gate by statement start time**, not by announced manifest
   generations (A12 text): T2 built announcements as start times
   (`oldestActiveStart`); the host passes the minimum
   (`flatsql_ps_reader_gate`).
9. **RETIRE is the whole set** per changing batch, not incremental records, so
   open reads one record (head pointer) plus the tail.
10. **UNLINK_IF_UNUSED BUSY skips** to later items (A12: "retry later");
    reader lanes close idle handles after 5 s.
11. **Disk bytes include zero-fill and pre-created files** (the file sizes);
    open stats the files it names.
12. **ENOSPC evicts with tombstones, one segment per wave** (A13: "first unlink
    whole eligible sealed segments, oldest first ... needs no new space").
    Dropping a segment without per-record tombstones would leave the type
    owner unaware of the deaths (A14: FIRST copies must be labeled DEAD and
    REPEATs promoted), and those catalog updates need space too. The ballast
    must hold one segment's tombstones: about 128 B of row plus postings per
    record, twice until the tombstones' own segment is compacted.
13. **Ballast is off in the engine default**; hosts set it (servers 256 MiB,
    browsers one seal plus one commit), so tests and benchmarks are unaffected.
14. **Quota counts partitions and type logs**, not the registry (a few KB)
    or type configs (registration).
15. **A rebalance of a partition already handed off is ignored** (a T1 race
    the compaction timing exposed: a second release before the target adopted
    could leave two owners).
16. **RecordAttr bytes**: `buildRecordAttr` created its strings inside one
    argument list, whose evaluation order C++ leaves unspecified; GCC on x86_64
    built different bytes from clang. Each string is now created in field order;
    `format_record_attr_golden_bytes` checks `vectors/record_attr.hex`
    (22.3a-6).
17. **T3 #2 "live bytes" is the live set's compacted footprint**: the same
    records fully compacted, rows, attributes and postings included. Every
    record carries about four times its frame in index structure, so a ratio
    to frame bytes alone would measure the index, not garbage; both ratios are
    reported (§28).
18. **Type logs persist their retired set in the manifest** (not in ctl
    records): a type has no ctl-record log, and the manifest is the one file a
    MERGE_DONE names. The appendix sits after the CRC trailer, so readers and
    opens that predate it parse the manifest unchanged.
19. **A type meta segment retires with two synced head writes** instead of a
    LANE_CKPT batch and a later DURABLE_CKPT head (A9): a type head has no
    lane table, and after the second write neither slot names the segment,
    whichever one a crash leaves torn.
20. **A compaction plan pins its files**: anything retired after the plan was
    made stays until the SWAP or the abort (a build reads the runs and meta
    segments current at planning).
21. **A failed build never quarantines**: the inputs are untouched, the
    partition carries on, and requests end with an error (NOSPACE starts the
    emergency). A quarantined partition ends its compaction requests at once.
22. **TOMB_RANGE steps are also bounded in time** (4 ms of CPU, besides 512
    rows): a kill looks up the row's tag instances in every run, so rows cost
    more as a partition grows, and more under wasm (T3 #3: ≤ 10 ms a step).
23. **Dead catalog copies are dropped by merges** (A15 says "type-log
    compaction"): the type merge already reads every entry it could drop, so
    no separate pass rewrites the runs; a copy goes once its first posting and
    its DEAD posting meet in one merge (at the latest, the next full fold).
24. **The dead-share trigger is 15%, not 25%** (§11). At rest a segment can
    hold up to the trigger's share of dead rows, and a dead row costs as much
    as a live one in rows, attributes and postings, so a store at rest sits
    at up to 1/(1 − trigger) of its compacted footprint: 1.33× at 25%, above
    T3 #2's 1.3. Measured on the T3 #2 run (20,000 CAT objects, 20% updated
    per cycle, 100 cycles; Mac mem, load 6–26): 25%: 1.31–1.38 over six
    runs; 20%: 1.35, 1.38; 15%: 1.115, 1.120; 10%: 1.19, with about the
    same number of compactions (334–361). The price is write
    amplification: a compaction at 15% rewrites 5.7 bytes per byte it
    reclaims (3 at 25%). `EngineConfig::compactDeadRatio` sets it.
25. **A compaction wave is one output**: segments next to the chosen one that
    are past the trigger too join it, and the next candidate is looked for
    right after a SWAP (not 50 ms later).

## 30. Not built yet; running the T3 tests

Remaining T3 scope:

- **#5 hot split** with stage-1 helpers and sealed-segment re-homing (§12), and
  A26's ring prep states (FREE → CLAIMED(epoch) → PREPARED). Sealed segments'
  merges and compactions already build off the owner (deviation 1 here, T1
  deviation 1); what is missing is stage-1 (verify, sha256, key extraction
  claimed in ranges by idle writers) and its trigger. Its acceptance (≥ 2.5×
  the unsplit rate with 4 helpers) is a throughput ratio that needs a quiet
  many-core box to mean anything.
- **A15 arrivals compaction**: arrivals entries (24 B) and the GONE posting
  of every dead gseq stay, about 40 B per record ever ingested; the quota
  counts them but nothing shrinks them (the type-log test: 2.4 MB of catalog
  runs, mostly GONE, and 2.5 MB of arrivals after 104,100 records with 835
  alive). The type-log half (catalog runs, manifests, meta segments, dead
  catalog copies) is built (§25). The arrivals half must drop an entry and its
  GONE posting in one commit, or offset paging (live = entries − GONE, T2)
  miscounts: the plan is to rewrite sealed arrivals segments inside the type
  merge that drops their GONE postings, named by the merge's generation in
  its manifest.
- **#1 latency ratio** on a quiet Linux-8 box.

```
cpp/build/flatsql_ps_test --test=compaction_          # basic, T3 #1 and #2 (short forms)
cpp/build/flatsql_ps_test --test=compaction_half_dead_T3_1_full [--dir=<dir>]
cpp/build/flatsql_ps_test --test=compaction_cat_supersede_plateau_T3_2_full
cpp/build/flatsql_ps_test --test=orphan_crash_points_during_compaction_T3_4_full   # 1,000 crash points
cpp/build/flatsql_ps_test --test=quota_                # T3 #3, #6
cpp/build/flatsql_ps_test --test=type_logs_reclaimed_under_readers
```

**T3 #1 on a quiet Linux-8 box** (8 cores, ext4, nothing else running), from a
checkout at the landed commit; the test enforces the 1.5× bounds on such a box:

```
cpp/build/flatsql_ps_test --test=compaction_half_dead_T3_1_full --dir=<ext4 dir>
```
