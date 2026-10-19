// E2E: the FlatSQL site's encrypted database is keyed to the signed-in
// wallet, and a later session reopens it only through the wallet.
//
//   node scripts/wallet-database-e2e.mjs [--root docs]
//
// wallet-fixture.mjs serves the site at https://digitalarsenal.github.io/flatsql/
// (the origin hd-wallet-ui 2.2.1 binds this site's client to), the published
// 2.2.1 wallet origin, and an in-memory relay, in Chromium's headless shell.
// The wallet is the throwaway vector account alice_01. The user signs in and
// approves through the wallet popup with real input events.
//
//  1. As published, the page keeps "Reopen" off at the shared github.io
//     origin (signed in, database saved) and says why.
//  2. Served with SITE_ORIGIN set to the registered origin (the one line the
//     site's own domain will make true):
//     - signed in, the page creates a database of 100 contacts whose email
//       and phone are stored encrypted; Python derives the wallet key from
//       the seed, unwraps the content key and decrypts every record to the
//       rows SQL returns; account 1 cannot unwrap it;
//     - a fresh session (new tab, nothing in memory) reopens the saved
//       database: Cancel in the wallet's consent opens nothing; after
//       consent SQL returns the same rows Python decrypted;
//     - the wallet refuses a key wrapped to another account, and the client
//       refuses a key bound to the flatbuffers site;
//     - the relay never holds the content key.
import { spawnSync } from 'node:child_process';
import path from 'node:path';

import { PAGES, SEED, answerWallet, fail, startWalletFixture } from './wallet-fixture.mjs';
import { databaseContext, wrapContentKey } from '../docs/wallet-field-keys.js';

const here = path.dirname(new URL(import.meta.url).pathname);
const repo = path.resolve(here, '..');
const argRoot = process.argv.indexOf('--root');
const siteRoot = path.resolve(repo, argRoot > 0 ? process.argv[argRoot + 1] : 'docs');
const SITE_ORIGIN = 'https://flatsql.spacedatanetwork.org';
const QUERY = 'SELECT id, name, email, phone FROM Contact';
const checks = [];
const step = (message) => { if (process.env.E2E_TRACE) console.error(`  ..  ${message}`); };
const pass = (message) => { checks.push(message); console.log(`  ok  ${message}`); };
const hex = (bytes) => Buffer.from(bytes).toString('hex');

function python(account, db) {
  const r = spawnSync('python3', ['-I', path.join(here, 'wallet_database_vector.py')], {
    input: JSON.stringify({ seedHex: SEED, account, encHex: db.encHex, kmfHex: db.kmfHex, exportHex: db.exportHex ?? '' }), encoding: 'utf8',
  });
  if (r.status !== 0) fail(`python: ${r.stderr}`);
  return JSON.parse(r.stdout);
}

let gateOn = false;
const fixture = await startWalletFixture({
  site: 'flatsql',
  siteRoot,
  rewrite: (file, body) => {
    if (!gateOn || file !== 'wallet-field-keys.js') return body;
    const text = body.toString('utf8');
    if (text.split(SITE_ORIGIN).length !== 2) fail('wallet-field-keys.js does not name the site origin exactly once');
    return Buffer.from(text.replace(SITE_ORIGIN, PAGES));
  },
});

async function load(tab) {
  step('load');
  await tab.goto(`${PAGES}/flatsql/`);
  await tab.waitFor(`!!document.getElementById('walletDbCreate') && !!document[Symbol.for('sdn.wallet.public-client.v1')]`);
  await new Promise((r) => setTimeout(r, 1500));
  const signedIn = `!document.getElementById('walletDbCreate').disabled`;
  if (!(await tab.eval(signedIn))) {
    step('sign in');
    const popup = fixture.nextPopup();
    await tab.click('#walletDbSignIn');
    await answerWallet(await popup);
    await tab.waitFor(signedIn);
  }
}

const readDatabase = (tab) => tab.eval(`(() => {
  const box = document.getElementById('walletDbEnvelope').dataset;
  return { encHex: box.encHex, kmfHex: box.kmfHex, exportHex: box.exportHex, reopened: box.reopened === 'true',
    status: document.getElementById('walletDbStatus').textContent,
    rows: [...document.querySelectorAll('#walletDbResults tbody tr')].map((tr) => [...tr.cells].map((c) => c.textContent)) };
})()`);

async function query(tab) {
  await tab.eval(`document.getElementById('walletDbQuery').value = ${JSON.stringify(QUERY)}`);
  await tab.click('#walletDbRun');
  await tab.waitFor(`document.querySelectorAll('#walletDbResults tbody tr').length === 100`);
}

async function create(tab) {
  step('create');
  await tab.click('#walletDbCreate');
  await tab.waitFor(`!document.getElementById('walletDbEnvelope').hidden && document.getElementById('walletDbStatus').textContent.includes('Saved in this browser')`);
  await query(tab);
  return readDatabase(tab);
}

// The page's own wallet client, asked directly.
const unwrapDirect = (tab, envelope) => tab.eval(`(() => {
  const bytes = (h) => Uint8Array.from(h.match(/../g), (p) => parseInt(p, 16));
  const e = ${JSON.stringify(envelope)};
  return document[Symbol.for('sdn.wallet.public-client.v1')].client
    .unwrapKey({ enc: bytes(e.encHex), kmf: bytes(e.kmfHex), context: e.context })
    .then(() => 'unwrapped', (err) => err?.code ?? String(err));
})()`);

async function wrapped(publicKeyHex, context) {
  const env = await wrapContentKey(Buffer.from(publicKeyHex, 'hex'), crypto.getRandomValues(new Uint8Array(32)), context);
  return { encHex: hex(env.enc), kmfHex: hex(env.kmf), context };
}

try {
  let tab = await fixture.newTab();

  // 1. As published: off at the shared origin.
  await load(tab);
  await create(tab);
  const off = await tab.eval(`({ reopen: document.getElementById('walletDbReopen').disabled, note: document.getElementById('walletDbOpenNote').textContent })`);
  if (!off.reopen) fail('Reopen is on at the shared github.io origin');
  if (!off.note.includes('off at this address') || !off.note.includes(SITE_ORIGIN)) fail(`off-origin note: ${off.note}`);
  pass(`as published, "Reopen" stays off at ${PAGES} (signed in, database saved) and the page names ${SITE_ORIGIN}`);

  // 2. On the registered origin.
  gateOn = true;
  await load(tab);
  const first = await create(tab);
  if (first.rows.length !== 100) fail(`SQL returned ${first.rows.length} rows`);
  if (Buffer.from(first.exportHex, 'hex').includes(Buffer.from(first.rows[0][2]))) fail('an email is stored in plaintext');
  const opened = python(0, first);
  if (opened.refused) fail(`Python could not unwrap: ${opened.refused}`);
  if (opened.context !== databaseContext('Contact')) fail(`context ${opened.context}`);
  const pythonRows = opened.rows.map((r) => [String(r.id), r.name, r.email, r.phone]);
  if (JSON.stringify(pythonRows) !== JSON.stringify(first.rows)) fail('Python decrypted rows differ from SQL');
  if (!python(1, first).refused) fail('another wallet account unwrapped the content key');
  pass('the page created 100 contacts stored encrypted; Python unwraps the content key from the seed and decrypts every record to the SQL rows; account 1 cannot');

  // A fresh session: a new tab, nothing in memory but the saved ciphertext.
  await fixture.closeTab(tab);
  tab = await fixture.newTab();
  await load(tab);
  if (await tab.eval(`document.querySelectorAll('#walletDbResults tbody tr').length`)) fail('the fresh session shows rows before reopening');
  await tab.waitFor(`!document.getElementById('walletDbReopen').disabled`);
  let popup = fixture.nextPopup();
  await tab.click('#walletDbReopen');
  const heading = await answerWallet(await popup, { action: 'cancel' });
  if (heading !== 'Reopen a database for FlatSQL Documentation') fail(`consent heading: ${heading}`);
  await new Promise((r) => setTimeout(r, 2000));
  if ((await readDatabase(tab)).reopened) fail('the database reopened after Cancel');
  if (fixture.relay.results('sdn.wallet.key-unwrap.v1')) fail('the wallet answered after Cancel');
  pass(`a fresh session asks the wallet first ("${heading}"); after Cancel nothing opens`);

  popup = fixture.nextPopup();
  await tab.click('#walletDbReopen');
  await answerWallet(await popup);
  await tab.waitFor(`document.getElementById('walletDbEnvelope').dataset.reopened === 'true'`);
  await query(tab);
  const reopened = await readDatabase(tab);
  if (JSON.stringify(reopened.rows) !== JSON.stringify(pythonRows)) fail('the reopened database reads other rows than Python decrypted');
  if (reopened.exportHex !== first.exportHex) fail('the reopened database stores other bytes');
  pass('after consent the fresh session reopens the database: SQL returns the 100 rows Python decrypted, and the stored bytes are unchanged');

  const toAccount1 = await wrapped(python(1, { encHex: '', kmfHex: '' }).publicKeyHex, databaseContext('Contact'));
  popup = fixture.nextPopup();
  const otherPending = unwrapDirect(tab, toAccount1);
  const otherHeading = await answerWallet(await popup).catch((e) => `(${e.message})`);
  const other = await Promise.race([otherPending, new Promise((r) => setTimeout(() => r('no key returned'), 20_000))]);
  if (other === 'unwrapped') fail('the wallet unwrapped a key wrapped to another account');
  step(`account 1: consent "${otherHeading}", result ${other}`);
  pass(`a key wrapped to another account of the wallet is refused (${other})`);

  const ownKey = opened.publicKeyHex;
  const crossSite = await wrapped(ownKey, 'sdn-field-encryption/v1|flatbuffers|Contact|db|k1');
  const cross = await unwrapDirect(tab, crossSite);
  if (cross !== 'INVALID_REQUEST') fail(`a flatbuffers-bound key on flatsql: ${cross}`);
  pass('a key bound to the flatbuffers site is refused on flatsql (INVALID_REQUEST), though it is wrapped to this account');

  const results = fixture.relay.results('sdn.wallet.key-unwrap.v1');
  const key = Buffer.from(opened.contentKeyHex, 'hex');
  if (results.includes(opened.contentKeyHex) || results.includes(key.toString('base64url')) || results.includes(key.toString('base64'))) fail('the relay holds the content key');
  pass('the relay holds only reply ciphertext, never the content key');

  if (fixture.errors.length) fail(`page errors: ${fixture.errors.join(' | ')}`);
  console.log(`wallet-database E2E PASS (${checks.length} checks)`);
} finally {
  await fixture.close();
}
