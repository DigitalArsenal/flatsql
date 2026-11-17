// End to end: the browser guest starts the engine only on a thread pool that
// holds every thread the engine starts.
//
// In a browser each engine thread is a pooled worker. Workers arm (load their
// script and instantiate the module over the shared memory) before the engine
// starts; a worker that arms late was dropped from the pool, the engine's
// std::thread then failed to start, and the build without exceptions aborted:
// "RuntimeError: unreachable" at flatsql::p4::startThreads <- flatsql_start.
// A worker script that is slow to arrive arms late (on the live site one fetch
// in twenty took 2.3 s); here it is certain: the server answers the first
// worker-script requests at once and treats the rest as the case says.
//   slow:    they come after 2.3 s (past the old 1.5 s arming time): the
//            engine starts with the default options and answers a request.
//   broken:  they fail (404): open fails with a FlatSQLThreadPoolError within
//            a second of the last failure, without waiting out any arming time.
//   failing: they load and then fail while arming (the answer a worker gives
//            when it cannot instantiate the module): FlatSQLThreadPoolError
//            within a second of the last failure, one attempt.
//   stalled: they never come: FlatSQLThreadPoolError once the default budget
//            (2 attempts of 4 s) is spent, not later.
// Every case: no wasm fault on the page.
// Chromium headless shell only.
// Usage: node scripts/ci-browser-threads-e2e.mjs
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const PROMPT = 3;
const cases = { slow: 2300, broken: 0, failing: 0, stalled: 3600000 };
// A worker script that loads and answers the pool's probe as a worker that
// cannot instantiate the module does (space-data-module-sdk wasiThreadBrowserWorker).
const failingWorker = "self.onmessage = e => { if (e.data && e.data.t === 'probe') self.postMessage({ t: 'ready', ok: false, error: 'test: the module cannot be instantiated here' }); };";
const BUDGET_MS = 2 * 4000;
function shell() {
  if (process.env.GAUNTLET_HEADLESS_SHELL) return process.env.GAUNTLET_HEADLESS_SHELL;
  const cache = process.env.PLAYWRIGHT_BROWSERS_PATH ?? path.join(os.homedir(), process.platform === 'darwin' ? 'Library/Caches/ms-playwright' : '.cache/ms-playwright');
  const dirs = fs.readdirSync(cache).filter(d => /^chromium_headless_shell-\d+$/.test(d)).sort((a, b) => Number(b.split('-')[1]) - Number(a.split('-')[1]));
  for (const dir of dirs) for (const sub of fs.readdirSync(path.join(cache, dir))) {
    const bin = path.join(cache, dir, sub, 'chrome-headless-shell'); if (fs.existsSync(bin)) return bin;
  }
  throw new Error('Install chromium-headless-shell, or set GAUNTLET_HEADLESS_SHELL');
}
const page = `<!doctype html><script type="module">
import { FlatSQL } from '/wasm/index.js';
window.runCase = async run => {
  try {
    const db = await FlatSQL.open({ backend: 'memory', root: 'threads-' + run,
      threadWorkerUrl: location.origin + '/wasm/runtime/wasiThreadBrowserWorker.mjs?run=' + run });
    const surface = await db.surface();
    await db.close();
    return { started: true, surfaceBytes: surface.length };
  } catch (error) {
    return { started: false, name: error.name, message: String(error.message), needed: error.needed, armed: error.armed, attempts: error.attempts };
  }
};
</script>`;
const requests = new Map();
const held = new Set();
const timers = new Set();
const lastFailure = new Map();
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  res.setHeader('cross-origin-opener-policy', 'same-origin');
  res.setHeader('cross-origin-embedder-policy', 'require-corp');
  res.setHeader('cross-origin-resource-policy', 'same-origin');
  res.setHeader('cache-control', 'no-store');
  if (url.pathname === '/threads.html') { res.writeHead(200, { 'content-type': 'text/html' }); res.end(page); return; }
  const file = path.resolve(root, '.' + url.pathname);
  if (!file.startsWith(path.join(root, 'wasm') + '/') || !fs.existsSync(file) || !fs.statSync(file).isFile()) { res.writeHead(404); res.end(); return; }
  res.setHeader('content-type', { '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm', '.json': 'application/json' }[path.extname(file)] ?? 'application/octet-stream');
  const respond = () => { held.delete(res); fs.createReadStream(file).pipe(res); };
  const run = url.searchParams.get('run');
  if (url.pathname === '/wasm/runtime/wasiThreadBrowserWorker.mjs' && run) {
    const n = (requests.get(run) ?? 0) + 1; requests.set(run, n);
    // Headers at once, body later: the browser's cache lock on one URL then
    // lets every worker's request through instead of queuing them.
    if (n > PROMPT && run.startsWith('broken')) { lastFailure.set(run, Date.now()); res.writeHead(404); res.end(); return; }
    if (n > PROMPT && run.startsWith('failing')) { lastFailure.set(run, Date.now()); res.writeHead(200); res.end(failingWorker); return; }
    if (n > PROMPT) { held.add(res); res.flushHeaders(); timers.add(setTimeout(respond, cases[run.split('-')[0]])); return; }
  }
  respond();
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'flatsql-threads-'));
const child = spawn(shell(), ['--remote-debugging-port=0', `--user-data-dir=${profile}`, '--no-first-run', '--no-default-browser-check',
  ...(process.platform === 'linux' ? ['--no-sandbox'] : []), 'about:blank'], { stdio: ['ignore', 'ignore', 'pipe'] });
let socket;
let failed = false;
try {
  const port = await new Promise((resolve, reject) => {
    let output = ''; const timer = setTimeout(() => reject(new Error('Browser startup timed out')), 20000);
    child.on('error', reject);
    child.stderr.on('data', b => { output += b; const m = output.match(/DevTools listening on ws:\/\/[^:]+:(\d+)\//); if (m) { clearTimeout(timer); resolve(Number(m[1])); } });
    child.on('exit', code => { clearTimeout(timer); reject(new Error(`Browser exited: ${code}`)); });
  });
  const tab = await (await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, { method: 'PUT' })).json();
  socket = new WebSocket(tab.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject; });
  let serial = 0; const pending = new Map(); const errors = [];
  socket.onmessage = ({ data }) => {
    const m = JSON.parse(data);
    if (m.id) { const p = pending.get(m.id); pending.delete(m.id); if (m.error) p.reject(new Error(m.error.message)); else p.resolve(m.result); }
    else if (m.method === 'Runtime.exceptionThrown') errors.push(m.params.exceptionDetails?.exception?.description ?? m.params.exceptionDetails.text);
    else if (m.method === 'Runtime.consoleAPICalled' && m.params.type === 'error') errors.push(m.params.args.map(a => a.value ?? a.description).join(' '));
  };
  const send = (method, params = {}) => new Promise((resolve, reject) => { const id = ++serial; pending.set(id, { resolve, reject }); socket.send(JSON.stringify({ id, method, params })); });
  const evaluate = async expression => {
    const r = await send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description ?? r.exceptionDetails.text);
    return r.result.value;
  };
  await send('Runtime.enable');
  await send('Page.enable');
  const origin = `http://127.0.0.1:${server.address().port}`;
  for (const name of Object.keys(cases)) {
    errors.length = 0;
    await send('Page.navigate', { url: `${origin}/threads.html` });
    const deadline = Date.now() + 20000;
    while (!(await evaluate('crossOriginIsolated && typeof runCase === "function"').catch(() => false))) {
      if (Date.now() > deadline) throw new Error('Test page did not load cross-origin isolated');
      await new Promise(r => setTimeout(r, 50));
    }
    const run = `${name}-${Date.now()}`;
    const started = Date.now();
    const result = await evaluate(`runCase(${JSON.stringify(run)})`);
    const answered = Date.now();
    const afterLastFailureMs = lastFailure.has(run) ? answered - lastFailure.get(run) : undefined;
    const report = { case: name, seconds: (answered - started) / 1000, afterLastFailureMs, workerScriptRequests: requests.get(run) ?? 0, result, pageErrors: errors.slice() };
    console.log(JSON.stringify(report));
    try {
      if (name === 'slow') {
        assert.equal(result.started, true, `slow pool: the engine did not start: ${result.message}`);
        assert.ok(result.surfaceBytes > 0);
      } else {
        assert.equal(result.started, false);
        assert.equal(result.name, 'FlatSQLThreadPoolError', `${name} pool: expected FlatSQLThreadPoolError, got ${result.name}: ${result.message}`);
        assert.ok(Number.isInteger(result.needed) && result.needed > PROMPT, `needed ${result.needed}`);
        if (name === 'broken' || name === 'failing') {
          assert.equal(result.attempts.length, 1, 'a worker that cannot load is not retried');
          assert.ok(afterLastFailureMs < 1000, `the error came ${afterLastFailureMs} ms after the last worker failed`);
        } else {
          assert.equal(result.armed, PROMPT);
          assert.equal(result.attempts.length, 2);
          assert.ok(answered - started < BUDGET_MS + 2000, `the error came after ${answered - started} ms (budget ${BUDGET_MS} ms)`);
        }
      }
      assert.ok(!errors.some(e => /unreachable|startThreads/.test(e)), `wasm fault on the page: ${errors.join('\n')}`);
      console.log(`PASS ${name}`);
    } catch (error) { failed = true; console.log(`FAIL ${name}: ${error.message}`); }
  }
} finally {
  socket?.close();
  const exited = new Promise(resolve => { if (child.exitCode !== null || child.signalCode) resolve(); else child.once('exit', resolve); });
  child.kill();
  await Promise.race([exited, new Promise(r => setTimeout(r, 5000))]);
  for (const timer of timers) clearTimeout(timer);
  for (const res of held) res.destroy();
  server.closeAllConnections();
  server.close();
  fs.rmSync(profile, { recursive: true, force: true });
}
if (failed) process.exit(1);
