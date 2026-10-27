// One engine, one data API: binary schemas and records in, SDS QRP/records out.
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const pause = () => new Promise(resolve => setTimeout(resolve, 1));
const bytes = value => {
  if (!(value instanceof Uint8Array)) throw new TypeError('Expected Uint8Array FlatBuffer bytes');
  return value;
};
function integer(value, name, minimum=0, maximum=Number.MAX_SAFE_INTEGER) {
  if(!Number.isSafeInteger(value) || value<minimum || value>maximum)
    throw new RangeError(`${name} must be an integer from ${minimum} to ${maximum}`);
  return value;
}
function queryOptions(options) {
  for(const key of ['timeoutMs','maxRows','maxRowsExamined','maxBytesRead','maxResultBytes'])
    if(options[key]!==undefined)integer(options[key],key);
  if(options.sandbox && options.bulk)throw new TypeError('Choose either sandbox or bulk');
  return options;
}
function join(parts) {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) { out.set(p, at); at += p.length; }
  return out;
}
// Private engine control encoding; no record fields are decoded by the host.
function control(entries) {
  return join(entries.map(([tag, value, width]) => {
    let data;
    if (typeof value === 'string') data = encoder.encode(value);
    else if (value instanceof Uint8Array) data = value;
    else {
      data = new Uint8Array(width ?? 4);
      const view = new DataView(data.buffer);
      if (data.length === 8) view.setBigUint64(0, BigInt(value), true);
      else if (data.length === 1) view.setUint8(0, value);
      else view.setUint32(0, value, true);
    }
    const h = new Uint8Array(6);
    const v = new DataView(h.buffer);
    v.setUint16(0, tag, true); v.setUint32(2, data.length, true);
    return join([h, data]);
  }));
}
export class FlatSQLError extends Error {
  constructor(status, message) { super(message || `FlatSQL status ${status}`); this.name = 'FlatSQLError'; this.status = status; }
}
function check(status) { if (status < 0) throw new FlatSQLError(status); return status; }
async function load(url, integrity) {
  if (url.protocol === 'file:') {
    const { readFile } = await import('node:fs/promises');
    return new Uint8Array(await readFile(url));
  }
  const response = await fetch(url, integrity ? { integrity } : undefined);
  if (!response.ok) throw new Error(`FlatSQL artifact: HTTP ${response.status}`);
  return new Uint8Array(await response.arrayBuffer());
}
export class FlatSQL {
  #host; #layout; #memory; #u8; #i32; #u64; #view; #closed = false; #active = new Set();
  static async open(options = {}) {
    integer(options.maximumPages??8192,'maximumPages',8192,32768);
    integer(options.requestBytes??8*1024*1024,'requestBytes',1024,16*1024*1024);
    integer(options.writers??1,'writers',1,8);integer(options.readers??1,'readers',1,16);
    if(options.backend && !['opfs','memory'].includes(options.backend))throw new TypeError('Unknown browser storage backend');
    const url = new URL(options.wasmUrl ?? './flatsql.wasm', import.meta.url);
    let artifact;
    if (options.wasmBytes) {
      if (!options.verify) throw new TypeError('Supplied WASM bytes require an artifact verification callback');
      artifact = bytes(options.wasmBytes);
    } else {
      const manifest = JSON.parse(decoder.decode(await load(new URL('./integrity.json', url))));
      const expected = manifest.files?.['flatsql.wasm'];
      if (!expected?.sri || !expected?.sha256) throw new Error('Missing FlatSQL artifact integrity');
      artifact = await load(url, expected.sri);
      if (artifact.length !== expected.size) throw new Error('FlatSQL artifact size mismatch');
      if (globalThis.process?.versions?.node) {
        const { createHash } = await import('node:crypto');
        if (createHash('sha256').update(artifact).digest('hex') !== expected.sha256) throw new Error('FlatSQL artifact digest mismatch');
      }
    }
    if (options.verify) await options.verify(artifact);
    const hostOptions = { ...options, maximumPages: options.maximumPages ?? 8192 };
    const host = globalThis.process?.versions?.node
      ? await (await import('./runtime/node-host.mjs')).createNodeInstance(artifact, hostOptions)
      : await (await import('./runtime/browser-host.mjs')).createBrowserInstance(artifact, hostOptions);
    const db = new FlatSQL();
    db.#host = host;
    db.#memory = host.memory;
    db.#u8 = new Uint8Array(host.memory.buffer);
    db.#i32 = new Int32Array(host.memory.buffer);
    db.#u64 = new BigUint64Array(host.memory.buffer);
    db.#view = new DataView(host.memory.buffer);
    try {
      const requestBytes = options.requestBytes ?? 8 * 1024 * 1024;
      const cfg = control([[1, options.path ?? '/store'], [2, 1, 1], [3, options.writers ?? 1],
        [4, options.readers ?? 1], [5, 1], [6, 1], [7, 2], [8, 4], [9, requestBytes], [10, requestBytes],
        [11, 256 * 1024], [20, 384 * 1024 * 1024, 8], [21, 8], [23, 16], [25, 16 * 1024 * 1024, 8],
        [26, 128 * 1024 * 1024, 8], [27, 192 * 1024 * 1024, 8], [48, 32 * 1024 * 1024, 8]]);
      await db.#withBytes(cfg, async p => check(await host.call('flatsql_init', p, cfg.length)));
      const p = await host.call('flatsql_alloc', 640);
      if (!p) throw new FlatSQLError(-2);
      try {
        if (check(await host.call('flatsql_layout', p)) !== 640) throw new Error('Invalid FlatSQL mailbox layout');
        db.#layout = new DataView((await host.read(p, 640)).buffer);
        if (db.#layout.getUint32(0, true) !== 1 || db.#layout.getUint32(8, true) !== 448) throw new Error('Invalid FlatSQL mailbox layout');
      } finally { await host.call('flatsql_free', p); }
      check(await host.call('flatsql_start'));
      return db;
    } catch (error) { await host.close(); throw error; }
  }
  async #withBytes(data, fn) {
    const p = await this.#host.write(data);
    try { return await fn(p); } finally { await this.#host.call('flatsql_free', p); }
  }
  #assertOpen() { if (this.#closed || this.#host.faulted()) throw new FlatSQLError(-11, 'FlatSQL is closed or its guest failed'); }
  async registerSchema({ name, fileIdentifier, bfbs, indexRules = '', maxRecordBytes = 16 * 1024 * 1024, maxRows = 400000, fullText = false }) {
    this.#assertOpen(); bytes(bfbs);
    integer(maxRecordBytes,'maxRecordBytes',8,128*1024*1024);integer(maxRows,'maxRows',1,0xffffffff);
    if(typeof fileIdentifier!=='string')throw new TypeError('A four-byte file identifier is required');
    if (!/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(name) || encoder.encode(fileIdentifier).length !== 4) throw new TypeError('Schema name and four-byte file identifier are required');
    const spec = control([[1, name], [2, fileIdentifier], [3, bfbs], [4, indexRules], [5, maxRecordBytes, 8],
      [6, 4 * 1024 * 1024, 8], [7, 3], [8, 4096], [10, maxRows], [12, fullText ? 1 : 0, 1]]);
    await this.#withBytes(spec, async p => check(await this.#host.call('flatsql_register_type', p, spec.length)));
  }
  ingest(type, frames, options = {}) { return this.#records(1, type, frames, options); }
  remove(type, frames, options = {}) { return this.#records(3, type, frames, options); }
  async #records(opcode, type, frames, options) {
    this.#assertOpen(); bytes(frames);queryOptions(options);
    if(options.timestamp!==undefined)integer(options.timestamp,'timestamp');
    const tags = options.source ? [[51, control([[1, options.provider ?? 'local'], [2, options.source],
      ...(options.batch ? [[4, options.batch]] : [])])]] : [];
    const signatures = [];
    if (options.signatures !== undefined) {
      if (opcode !== 1 || !Array.isArray(options.signatures)) throw new TypeError('Signatures require an ingest array');
      const header = new Uint8Array(4); new DataView(header.buffer).setUint32(0, options.signatures.length, true);
      const parts = [header];
      for (const signature of options.signatures) {
        bytes(signature); integer(signature.length, 'signature size', 0, 65535);
        const length = new Uint8Array(2); new DataView(length.buffer).setUint16(0, signature.length, true);
        parts.push(length, signature);
      }
      signatures.push([55, join(parts)]);
    }
    const meta = control([[1, type], [50, options.producer ?? 'local'], ...tags, ...signatures, [54, options.timestamp ?? Math.floor(Date.now() / 1000), 8]]);
    const request = await this.#withBytes(frames, f => this.#withBytes(meta, async m => {
      const out = await this.#host.call('flatsql_alloc', 8);
      if (!out) throw new FlatSQLError(-2);
      let prepared = 0;
      try {
        check(await this.#host.call('flatsql_record_request', opcode, f, frames.length, m, meta.length, out, out + 4));
        const view = new DataView((await this.#host.read(out, 8)).buffer);
        prepared = view.getUint32(0, true);
        return await this.#host.read(prepared, view.getUint32(4, true));
      } finally {
        if (prepared) await this.#host.call('flatsql_free', prepared);
        await this.#host.call('flatsql_free', out);
      }
    }));
    return this.#collect(this.#stream(opcode, request, options), options.maxResultBytes);
  }
  query(request, options = {}) { bytes(request); return this.#stream(30, request, options); }
  async execute(request, options = {}) { return this.#collect(this.query(request, options), options.maxResultBytes); }
  async surface(options = {}) { return this.#collect(this.#stream(31, new Uint8Array(), options), options.maxResultBytes); }
  async setQuota(bytes) { integer(bytes,'quota'); this.#assertOpen(); check(await this.#host.call('flatsql_set_quota', bytes)); }
  async #collect(stream, maxBytes = 16 * 1024 * 1024) {
    const parts = []; let size = 0;
    for await (const part of stream) {
      size += part.length;
      if (size > maxBytes + 65536) throw new FlatSQLError(-8, 'Collected result exceeds the byte budget');
      parts.push(part);
    }
    return join(parts);
  }
  #get(offset) { return this.#layout.getUint32(offset, true); }
  #wake(cls) {
    for (let i = 0; i < this.#get(120); ++i) if (this.#get(128 + i * 4) === cls) {
      const bell = this.#get(384 + i * 4) >>> 2;
      Atomics.add(this.#i32, bell, 1); Atomics.notify(this.#i32, bell);
    }
  }
  #enqueue(cls, slot) {
    const i = cls - 1, enq = this.#get(88 + i * 4) >>> 3;
    const mask = BigInt(this.#get(72 + i * 4));
    let pos = Atomics.load(this.#u64, enq);
    for (;;) {
      const cell = this.#get(56 + i * 4) + Number(pos & mask) * 16;
      const seq = Atomics.load(this.#u64, cell >>> 3);
      if (seq < pos) return false;
      if (seq === pos && Atomics.compareExchange(this.#u64, enq, pos, pos + 1n) === pos) {
        this.#view.setUint32(cell + 8, slot, true);
        Atomics.store(this.#u64, cell >>> 3, pos + 1n);
        return true;
      }
      pos = Atomics.load(this.#u64, enq);
    }
  }
  #stream(op, request, options) {
    queryOptions(options);
    const handle = {};
    handle.iterator = this.#run(op, request, options, handle);
    return handle.iterator;
  }
  async *#run(op, request, options, handle) {
    this.#assertOpen();
    const cls = op < 10 ? 1 : options.sandbox ? 4 : options.bulk ? 3 : 2;
    const pool = cls === 1 ? 0 : 1, first = pool ? this.#get(16) : 0;
    const count = this.#get(16 + pool * 4), base = this.#get(24 + pool * 4), stride = this.#get(32 + pool * 4);
    const capacity = this.#get(40 + pool * 4), ringSize = this.#get(48 + pool * 4);
    if (request.length > capacity) throw new FlatSQLError(-8, 'Request exceeds the configured mailbox capacity');
    let address = 0, slot = 0, queued = false, finished = false;
    const started = performance.now(), timeout = options.timeoutMs ?? 30000;
    const expired = () => options.signal?.aborted || (timeout > 0 && performance.now() - started >= timeout);
    for (;;) {
      this.#assertOpen();
      if (expired()) throw new FlatSQLError(-7, 'Request cancelled before submission');
      for (let i = 0; i < count; ++i) {
        const a = base + i * stride;
        if (Atomics.compareExchange(this.#i32, a >>> 2, 0, 1) === 0) { address = a; slot = first + i; break; }
      }
      if (address) break;
      await pause();
    }
    const h = address, ring = h + 448 + capacity, state = h >>> 2;
    let done;
    const completion = new Promise(resolve => { done = resolve; });
    const active = { cancel: () => { Atomics.store(this.#i32, (h + 4) >>> 2, 1); this.#wake(cls); }, completion, finish: () => handle.iterator.return() };
    this.#active.add(active);
    const read = () => {
      const head = Atomics.load(this.#u64, (h + 72) >>> 3), tail = Atomics.load(this.#u64, (h + 80) >>> 3);
      if (head === tail) return null;
      const n = Number(tail - head), at = Number(head & BigInt(ringSize - 1));
      if (n < 0 || n > ringSize) throw new Error('Invalid FlatSQL ring counters');
      const take = Math.min(n, 65536), a = Math.min(take, ringSize - at), out = new Uint8Array(take);
      out.set(this.#u8.subarray(ring + at, ring + at + a));
      if (a < take) out.set(this.#u8.subarray(ring, ring + take - a), a);
      Atomics.store(this.#u64, (h + 72) >>> 3, head + BigInt(take));
      Atomics.add(this.#i32, (h + 12) >>> 2, 1); this.#wake(cls);
      return out;
    };
    try {
      this.#u8.fill(0, h + 4, h + 448);
      this.#u8.set(request, h + 448);
      this.#view.setUint32(h + 16, op, true); this.#view.setUint32(h + 20, cls, true);
      this.#view.setUint32(h + 24, (options.records ? 1 : 0) | (options.sandbox ? 2 : 0), true);
      this.#view.setUint32(h + 28, request.length, true);
      for (const [off, value] of [[40, options.maxRowsExamined ?? 0], [48, options.maxBytesRead ?? 0],
        [56, options.maxRows ?? 0], [64, options.maxResultBytes ?? 16 * 1024 * 1024]])
        this.#view.setBigUint64(h + off, BigInt(value), true);
      Atomics.store(this.#i32, state, 2);
      while (!this.#enqueue(cls, slot)) {
        if (expired()) throw new FlatSQLError(-7, 'Request cancelled before enqueue');
        await pause();
      }
      queued = true; this.#wake(cls);
      for (;;) {
        if (expired()) active.cancel();
        if (this.#host.faulted()) throw new FlatSQLError(-14, 'FlatSQL guest failed');
        const part = read();
        if (part) { yield part; continue; }
        if (Atomics.load(this.#i32, state) === 5) {
          finished = true;
          const status = this.#view.getInt32(h + 88, true);
          if (status < 0) throw new FlatSQLError(status, decoder.decode(this.#u8.subarray(h + 144, h + 144 + Math.min(256, this.#view.getUint32(h + 92, true)))));
          break;
        }
        await pause();
      }
    } finally {
      if (queued && !finished && !this.#host.faulted()) {
        active.cancel();
        // A consumer that breaks early must release ring space until the lane
        // acknowledges cancellation before this slot can be reused.
        while (Atomics.load(this.#i32, state) !== 5 && !this.#host.faulted()) { read(); await pause(); }
      }
      if (!this.#host.faulted()) Atomics.store(this.#i32, state, 0);
      this.#active.delete(active); done();
    }
  }
  async close() {
    if (this.#closed) return;
    this.#closed = true;
    for (const operation of this.#active) operation.cancel();
    await Promise.allSettled([...this.#active].map(operation => operation.finish()));
    await Promise.all([...this.#active].map(operation => operation.completion));
    try { if (!this.#host.faulted()) check(await this.#host.call('flatsql_stop', 5000)); }
    finally { await this.#host.close(); }
  }
}
