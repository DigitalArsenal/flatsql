# FlatSQL partition store (format 2): native engine

The T1 engine of the partition store program: append-only partition logs,
one writer per partition on a pinned pool, durable acks, per-commit indexes,
type owners, and durable-tail open. The design is the stack's
`docs/architecture/flatsql-partition-store.md` (§22 overrides §0–§21; §22.4
holds the owner rulings). This file records what was built, how to run it, the
measured acceptance, and every place the build departs from the design.

Readers (lanes, SQL, snapshots) are T2; compaction, splits and quota are T3;
the wasm artifact is T4; SDN and browser integration are T6/T10.

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
cpp/build/flatsql_ps_test                         # default suite (~1 min)
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
