#!/usr/bin/env node
/**
 * GUARDRAIL: the tracked `dist/` must be the COMPLETE built `dist/`.
 *
 * Why this exists (2026-08-07, flatsql-abi-stream-contract-read):
 * `.gitignore` ignores `dist/`, but consumers pin this repo as a GitHub
 * tarball (`.../archive/refs/tags/vX.Y.Z.tar.gz`), and a git tarball contains
 * ONLY TRACKED FILES. A partially force-added `dist/` therefore publishes a
 * HALF-BUILT package: `dist/index.js` re-exports `./cluster/index.js`,
 * `./response/index.js` and `./standalone/index.js`, and 84 of 164 built files
 * — including all three of those — were never tracked. Result: `import
 * 'flatsql'` threw ERR_MODULE_NOT_FOUND for every tarball consumer, while the
 * npm tarball (built by `prepublishOnly`, packed via `files:["dist"]`) was
 * fine. The two distribution channels silently disagreed.
 *
 * This is the same failure class as a check that measures the wrong copy: the
 * package LOOKED published and was structurally unloadable. Both directions
 * fail here — a missing tracked file AND a tracked file with no build output
 * (stale leftovers ship too).
 *
 *   node scripts/check-dist-tracked.mjs
 */
import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, statSync } from 'node:fs';
import { join, relative, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const DIST = join(ROOT, 'dist');

if (!existsSync(DIST)) {
  console.error('check-dist-tracked: dist/ does not exist — run `npm run build` first.');
  process.exit(1);
}

const walk = (dir) => readdirSync(dir).flatMap((entry) => {
  const full = join(dir, entry);
  return statSync(full).isDirectory() ? walk(full) : [relative(ROOT, full)];
});

const built = new Set(walk(DIST).map((p) => p.split('\\').join('/')));
const tracked = new Set(
  execFileSync('git', ['ls-files', 'dist'], { cwd: ROOT, encoding: 'utf8' })
    .split('\n').filter(Boolean),
);

const untracked = [...built].filter((f) => !tracked.has(f)).sort();
const orphaned = [...tracked].filter((f) => !built.has(f)).sort();

const show = (label, list) => {
  console.error(`\n${label} (${list.length}):`);
  for (const f of list.slice(0, 20)) console.error(`  ${f}`);
  if (list.length > 20) console.error(`  ... and ${list.length - 20} more`);
};

if (untracked.length || orphaned.length) {
  console.error('check-dist-tracked: FAIL — tracked dist/ != built dist/.');
  console.error('A GitHub-tarball consumer would receive this incomplete tree.');
  if (untracked.length) {
    show('BUILT BUT NOT TRACKED (missing from every tarball pin)', untracked);
    console.error('\n  fix: git add -f dist');
  }
  if (orphaned.length) {
    show('TRACKED BUT NOT BUILT (stale artifacts that still ship)', orphaned);
    console.error('\n  fix: git rm --cached <file>');
  }
  process.exit(1);
}

console.log(`check-dist-tracked: OK — ${built.size} built files, all tracked.`);
