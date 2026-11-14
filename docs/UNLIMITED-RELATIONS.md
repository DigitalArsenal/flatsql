# Unlimited SQL relations

Complete relations expose more work to scans, sorts, and joins. Those queries
can take longer or fail with an explicit execution-budget error. The shared
row-order cache also replaces some retained-memory access with file reads.

`flatsql_capabilities()` bit 0 advertises unlimited relations. Type-spec tag 10
is a uint32: zero explicitly requests unlimited visibility, a positive value
retains the newest-N policy, and an absent tag retains the historical default.
The JavaScript API accepts `maxRows: 0` only after checking the capability.
COUNT, raw-record output, source aliases, and ordinary SQL use the same policy.
Binary-collated `_cid = ?` predicates use the durable reader's CID index.

Registrations without reflection schemas expose metadata and stored bytes.
They have no reflected plaintext fields or plaintext extraction indexes.
Malformed supplied reflection schemas fail instead of silently hiding a type.
Execution row, byte, result, deadline, and heap limits remain in force.

The first unlimited registration promotes STORE to layout 2 before writing its
specification. The stream format remains 5. Layout-1-only engines refuse that
store, and a supporting engine rejects a registration that changes an unlimited
relation back to a bounded relation. Activation of a migration target preserves
the layout requirement. An interrupted marker write fails closed and requires
marker recovery with an engine that supports layout 2; automatic recovery of
this transition is not provided.

Config tag 51 (`rowCacheBytes` in JavaScript) sets the shared row-order page and
verified-metadata budget; its default is 32 MiB. Owning page views retain their
stream generation. Pins and temporary loads count against admission. Duplicate
misses share a load, idle entries expire after 30 seconds, and the cache evicts
unpinned entries to admit new pages. Exhausted pins produce `FLATSQL_E_BUDGET`.
Large verification work that exceeds this budget leaves the SQLite reader
fallback available. Statistics append current bytes, peak reservations, and
configured bytes at zero-based positions 40, 41, and 42.

This budget does not bound the entire engine: staging, checkpoint construction,
SQLite, query results, and allocator retention are separate. Releasing cache
entries does not guarantee that the runtime returns resident pages to the OS.
Legacy arena stores and their host-side migration require separate changes.
