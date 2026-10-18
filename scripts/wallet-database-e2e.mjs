// E2E: the FlatSQL site's encrypted database is keyed to the signed-in
// wallet, and code outside the browser opens it.
//
//   node scripts/wallet-database-e2e.mjs [--root docs]
//
// The site is served under /flatsql/ and loaded in Chromium's headless shell
// with a throwaway test wallet: the public identity of the published
// hd-wallet-wasm vector account alice_01, handed to the page the way the SDN
// stack nav hands it the wallet client. The page gets only public keys. Then:
//   - signed out, the page cannot create an encrypted database;
//   - signed in, it creates one: 100 contacts whose email and phone columns
//     are stored encrypted (no plaintext in the exported stream) and read
//     back decrypted through SQL in the session;
//   - Python `cryptography` (wallet_database_vector.py) derives the wallet key
//     from the seed, unwraps the content key from the page's $ENC/$KMF and
//     decrypts every exported record to the rows SQL returned;
//   - another account of the same wallet is refused;
//   - two databases get different content keys (different stored bytes).
import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';

const here = path.dirname(new URL(import.meta.url).pathname);
const repo = path.resolve(here, '..');
const argRoot = process.argv.indexOf('--root');
const siteRoot = path.resolve(repo, argRoot > 0 ? process.argv[argRoot + 1] : 'docs');

const SEED = 'c63fbe791b92d91eb828eab4b203cffe7f333e9d200ec2355d77c7796c080f79c9ed753af213bf155512e3c667c3be26a22c1c599cbae52cb44add279559fe5b';
const IDENTITY = {
  schemaVersion: 1,
  identityScheme: 'sdn-bip32-slip10-purpose-v1',
  seedProfile: 'password-scrypt-v2',
  accountIndex: 0,
  accountLabel: null,
  accountXpub: 'xpub6D9SXNXfAWtnHw8uWqUwMCBFh4R5bvzzWWemXtzwNhojQnYXyQARwhphkvtN4AJ93QFhzzHQZHj7MYQ7KuQ8vsXiTEwUq6MiF7iaLXTPFRT',
  accountPeerId: '16Uiu2HAkzgWPa6HTtNTU8WQi1kppaRsYUDBxNKygQNYUk7N73CMA',
  accountFingerprint: '9b582711',
  keys: [{
    purpose: 'contact-encryption', identityScheme: 'sdn-bip32-slip10-purpose-v1', seedProfile: 'password-scrypt-v2',
    signatureProfile: null, curve: 'x25519', derivation: 'slip10', path: "m/44'/0'/0'/1'/0'", encoding: 'raw',
    publicKeyHex: '1349c6136a8765e4b2a8795037cc6233e22d31a08c76e328ad247daf836c6c0c', bip32Fingerprint: null,
    keyId: 'sha256:0000000000000000000000000000000000000000000000000000000000000000',
  }],
};

function fail(message) {
  console.error(`field-encryption E2E FAIL: ${message}`);
  process.exit(1);
}

function headlessShell() {
  if (process.env.GAUNTLET_HEADLESS_SHELL) return process.env.GAUNTLET_HEADLESS_SHELL;
  const cache = process.env.PLAYWRIGHT_BROWSERS_PATH || path.join(os.homedir(), 'Library', 'Caches', 'ms-playwright');
  const dirs = fs.readdirSync(cache).filter((d) => /^chromium_headless_shell-\d+$/.test(d))
    .sort((a, b) => Number(b.split('-')[1]) - Number(a.split('-')[1]));
  for (const dir of dirs) {
    for (const sub of fs.readdirSync(path.join(cache, dir))) {
      const bin = path.join(cache, dir, sub, 'chrome-headless-shell');
      if (fs.existsSync(bin)) return bin;
    }
  }
  fail('no chromium headless shell');
}

const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.css': 'text/css', '.wasm': 'application/wasm', '.json': 'application/json' };
function serve() {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://x');
    if (!url.pathname.startsWith('/flatsql/')) { res.writeHead(404).end(); return; }
    let file = path.join(siteRoot, decodeURIComponent(url.pathname.slice('/flatsql/'.length)));
    if (!file.startsWith(siteRoot)) { res.writeHead(403).end(); return; }
    if (fs.existsSync(file) && fs.statSync(file).isDirectory()) file = path.join(file, 'index.html');
    if (!fs.existsSync(file)) { res.writeHead(404).end(); return; }
    res.writeHead(200, { 'content-type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve(server)));
}

// The wallet record the SDN stack nav would publish, holding the test identity.
function walletScript(identity) {
  return `(() => {
    const snapshot = Object.freeze({ status: 'connected', identity: ${JSON.stringify(identity)} });
    const client = Object.freeze({
      connect: async () => snapshot.identity, openAccount: async () => {}, disconnect: async () => {},
      destroy: async () => {}, getSnapshot: () => snapshot, subscribe: () => () => {},
    });
    Object.defineProperty(document, Symbol.for('sdn.wallet.public-client.v1'), {
      configurable: false, enumerable: false, writable: false,
      value: Object.freeze({ identity: 'sdn.wallet.public-client.document.v1', client,
        addPresenter() {}, getSnapshot: () => snapshot }),
    });
  })();`;
}

class Page {
  static async open(port) {
    const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
    const page = new Page(targets.find((t) => t.type === 'page').webSocketDebuggerUrl);
    await new Promise((resolve, reject) => { page.ws.onopen = resolve; page.ws.onerror = reject; });
    return page;
  }
  constructor(url) {
    this.ws = new WebSocket(url);
    this.next = 1;
    this.pending = new Map();
    this.errors = [];
    this.ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.id && this.pending.has(msg.id)) {
        const { resolve, reject } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result);
      } else if (msg.method === 'Runtime.exceptionThrown') {
        this.errors.push(msg.params.exceptionDetails.exception?.description || msg.params.exceptionDetails.text);
      }
    };
  }
  send(method, params = {}) {
    const id = this.next++;
    this.ws.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
  }
  async eval(expression) {
    const r = await this.send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text);
    return r.result.value;
  }
  async waitFor(expression, ms = 60000) {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      if (await this.eval(expression).catch(() => false)) return;
      await new Promise((r) => setTimeout(r, 250));
    }
    fail(`timed out waiting for ${expression}`);
  }
}

async function session(page, origin, signedIn) {
  const script = signedIn ? await page.send('Page.addScriptToEvaluateOnNewDocument', { source: walletScript(IDENTITY) }) : null;
  await page.send('Page.navigate', { url: `${origin}/flatsql/` });
  await page.waitFor(`document.readyState === 'complete' && !!document.getElementById('walletDbCreate')`);
  await new Promise((r) => setTimeout(r, 1000));
  let result = { canCreate: await page.eval(`!document.getElementById('walletDbCreate').disabled`) };
  if (signedIn) {
    await page.waitFor(`!document.getElementById('walletDbCreate').disabled`);
    await page.eval(`document.getElementById('walletDbCreate').click()`);
    await page.waitFor(`!document.getElementById('walletDbEnvelope').hidden`);
    await page.eval(`(() => { const q = document.getElementById('walletDbQuery'); q.value = 'SELECT id, name, email, phone FROM Contact'; document.getElementById('walletDbRun').click(); })()`);
    result = { ...result, ...await page.eval(`(() => {
      const box = document.getElementById('walletDbEnvelope').dataset;
      return { encHex: box.encHex, kmfHex: box.kmfHex, exportHex: box.exportHex,
        status: document.getElementById('walletDbStatus').textContent,
        rows: [...document.querySelectorAll('#walletDbResults tbody tr')].map((tr) => [...tr.cells].map((c) => c.textContent)) };
    })()`) };
  }
  if (script) await page.send('Page.removeScriptToEvaluateOnNewDocument', { identifier: script.identifier });
  return result;
}

function python(account, db) {
  const r = spawnSync('python3', ['-I', path.join(here, 'wallet_database_vector.py')], {
    input: JSON.stringify({ seedHex: SEED, account, encHex: db.encHex, kmfHex: db.kmfHex, exportHex: db.exportHex }), encoding: 'utf8',
  });
  if (r.status !== 0) fail(`python: ${r.stderr}`);
  return JSON.parse(r.stdout);
}

const server = await serve();
const origin = `http://127.0.0.1:${server.address().port}`;
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'wallet-database-e2e-'));
const port = 9300 + (process.pid % 500);
const browser = spawn(headlessShell(), [`--remote-debugging-port=${port}`, `--user-data-dir=${profile}`, '--no-first-run', 'about:blank'], { stdio: 'ignore' });
try {
  let page;
  for (let i = 0; i < 40 && !page; i += 1) {
    page = await Page.open(port).catch(() => null);
    if (!page) await new Promise((r) => setTimeout(r, 250));
  }
  if (!page) fail('headless shell did not start');
  await page.send('Runtime.enable');
  await page.send('Page.enable');

  const signedOut = await session(page, origin, false);
  if (signedOut.canCreate) fail('signed out, the page offers to create an encrypted database');

  const first = await session(page, origin, true);
  const second = await session(page, origin, true);
  if (first.rows.length !== 100) fail(`SQL returned ${first.rows.length} rows: ${first.status}`);
  if (Buffer.from(first.exportHex, 'hex').includes(Buffer.from(first.rows[0][2]))) fail('an email is stored in plaintext');
  if (first.exportHex === second.exportHex || first.kmfHex === second.kmfHex) fail('two databases share a content key');

  const opened = python(0, first);
  if (opened.publicKeyHex !== IDENTITY.keys[0].publicKeyHex) fail('Python derives a different wallet key');
  if (opened.refused) fail(`Python could not unwrap: ${opened.refused}`);
  if (opened.context !== 'sdn-field-encryption/v1|flatsql|Contact|db|k1') fail(`context ${opened.context}`);
  const pythonRows = opened.rows.map((r) => [String(r.id), r.name, r.email, r.phone]);
  if (JSON.stringify(pythonRows) !== JSON.stringify(first.rows)) fail('Python decrypted rows differ from SQL');
  if (python(0, second).rows?.length !== 100) fail('Python did not open the second database');
  if (!python(1, first).refused) fail('another wallet account unwrapped the content key');
  if (page.errors.length) fail(`page errors: ${page.errors.join(' | ')}`);
  console.log(`wallet-database E2E PASS: 100 contacts stored encrypted under a content key wrapped to the wallet; Python unwrapped it from the seed and decrypted every record to the SQL rows; another account refused; signed out cannot create; two databases, two keys`);
} finally {
  browser.kill();
  server.close();
  fs.rmSync(profile, { recursive: true, force: true });
}
