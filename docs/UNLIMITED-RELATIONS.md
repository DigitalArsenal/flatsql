# Unlimited SQL relations

Complete relations expose more work to scans, sorts, and joins. Those queries
can take longer or fail with an explicit execution-budget error. The shared
row-order cache also replaces some retained-memory access with file reads.

`flatsql_capabilities()` bit 0 advertises unlimited relations. Type-spec tag 13
is a uint8: 1 explicitly requests unlimited visibility. Tag 10 is a uint32:
a positive value selects the newest-N policy; zero or an absent tag retains
the historical default. An absent or zero tag 13 preserves bounded behavior.
The JavaScript API accepts `maxRows: 0` only after checking the capability.
COUNT, raw-record output, source aliases, and ordinary SQL use the same policy.
Binary-collated `_cid = ?` predicates use the durable reader's CID index.

Registrations without reflection schemas have no SQL relation, including
through trusted queries. They retain durable record access by CID.
A supplied schema follows the reflected-column path.
Supplied reflection schemas without SQL columns remain ingest-only. The engine
logs the hidden relation and its reason; other relations and statements work.
Execution row, byte, result, deadline, and heap limits remain in force.

The first unlimited registration promotes STORE to layout 2 before writing its
specification. The stream format remains 5. Layout-1-only engines refuse that
store, and a supporting engine rejects a registration that changes an unlimited
relation back to a bounded relation. Activation of a migration target preserves
the layout requirement. Promotion first syncs the complete new marker in
`STORE.unlimited`, then writes and syncs STORE, then removes the intent. Open
automatically completes a valid intent whose UUID matches MIGRATED. A torn
intent is discarded only beside an intact activated marker pair. UUID and
sequence-floor conflicts fail explicitly. No unlimited specification precedes
the durable layout guard.

Type-spec replacement syncs a complete CRC-protected `.spec.intent` before
overwriting `.spec`. Open completes a valid intent and discards an incomplete
intent only beside a valid prior spec. Layout 1 skips a corrupt spec so the
host can restore its bounded registration. Layout 2 refuses a corrupt spec;
it cannot hide an unlimited relation from the bounded-downgrade check. This
guard covers normal registration and interrupted writes, not manual file edits.

Config tag 52 (`rowCacheBytes` in JavaScript) sets the shared row-order page and
verified-metadata budget; its default is 32 MiB. Owning page views retain their
stream generation. The 16 KiB minimum permits SQLite fallback; row-order
verification requires a 129 KiB workspace plus metadata, so it cannot run at
that minimum. Committed tails enter the same cache before row-order use;
tails whose reservation cannot fit use SQLite.
Pins and temporary loads count against admission. Duplicate
misses share a load, idle entries expire after 30 seconds, and the cache evicts
unpinned entries to admit new pages. Exhausted pins produce `FLATSQL_E_CACHE_BUDGET`.
Large verification work that exceeds this budget leaves the SQLite reader
fallback available. Statistics append current bytes, peak reservations, and
configured bytes at zero-based positions 40, 41, and 42.

This budget does not bound the entire engine: staging, checkpoint construction,
SQLite, query results, and allocator retention are separate. Releasing cache
entries does not guarantee that the runtime returns resident pages to the OS.
Idle index merging can raise SQLite's temporary allocation peak while cache
entries expire. Repeated scans can retain that resident high-water mark after
live allocations fall. Sustained ingestion can still exhaust guest memory;
row-cache admission does not bound every allocation path.
Allocation failure inside a cache loader can trap in exception-free WebAssembly;
admission accounting is not allocator-failure recovery.
Legacy arena stores and their host-side migration require separate changes.

Readers use a snapshot for each buffered page, with a sequence ceiling fixed
when the cursor opens. A deleted or superseded row already buffered can still
be returned by that cursor. Later pages observe the changed state, and a new
cursor observes the deletion or supersede. Owning generation views keep old
stream bytes readable across compaction. Checkpoint publication alone does
not replace the stream generation; compaction does.
