#!/usr/bin/env node
// The full p4 replay benchmark, outside the gate: p4-replay.e2e.mjs with the
// "full" profile (the 15 GB sea feed, ten seed days of gp_history, the whole
// ingest day, a 512-tile lead and 20 retention pages; each build's fixture
// needs about 17 GB of free disk). Same environment as the E2E.
process.env.P4_REPLAY_PROFILE = 'full';
await import('./p4-replay.e2e.mjs');
