const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

function normalizeChange(change) {
  if (typeof change?.key !== 'string' || change.key === '') {
    throw new TypeError('a change needs a key');
  }
  if (change.op === 'remove') {
    return { op: 'remove', key: change.key };
  }
  if (change.op !== 'upsert') {
    throw new TypeError('op must be upsert or remove');
  }
  if (!isPlainObject(change.config)) {
    throw new TypeError('an upsert needs a config object');
  }
  return { op: 'upsert', key: change.key, config: structuredClone(change.config) };
}

export class FlagStore {
  #flags = new Map();
  #version = 0;
  #log = [];
  #capacity;
  #clock;
  #listeners = new Set();

  constructor({ capacity = 1000, clock = Date.now } = {}) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError('capacity must be a positive integer');
    }
    this.#capacity = capacity;
    this.#clock = clock;
  }

  get version() {
    return this.#version;
  }

  upsert(key, config) {
    return this.commit([{ op: 'upsert', key, config }]);
  }

  remove(key) {
    return this.commit([{ op: 'remove', key }]);
  }

  commit(changes) {
    const normalized = changes.map(normalizeChange);
    const applied = [];
    for (const change of normalized) {
      if (this.#alters(change)) {
        this.#apply(change);
        applied.push(change);
      }
    }
    if (applied.length === 0) {
      return null;
    }
    const previousVersion = this.#version;
    this.#version = previousVersion + 1;
    const entry = { v: this.#version, committedAtMs: this.#clock(), changes: applied };
    this.#log.push(entry);
    if (this.#log.length > this.#capacity) {
      this.#log.shift();
    }
    for (const listener of this.#listeners) {
      listener(entry, previousVersion);
    }
    return entry;
  }

  snapshot() {
    return { v: this.#version, flags: Object.fromEntries(structuredClone(this.#flags)) };
  }

  planSync(since) {
    if (!Number.isSafeInteger(since) || since > this.#version || !this.#covers(since)) {
      return { kind: 'snapshot', snapshot: this.snapshot() };
    }
    const missed = this.#log.filter((entry) => entry.v > since);
    return { kind: 'deltas', from: since, to: this.#version, entries: structuredClone(missed) };
  }

  subscribe(listener) {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  #covers(since) {
    if (since === this.#version) {
      return true;
    }
    return this.#log.length > 0 && this.#log[0].v <= since + 1;
  }

  #alters(change) {
    if (change.op === 'remove') {
      return this.#flags.has(change.key);
    }
    const current = this.#flags.get(change.key);
    return current === undefined || JSON.stringify(current) !== JSON.stringify(change.config);
  }

  #apply(change) {
    if (change.op === 'remove') {
      this.#flags.delete(change.key);
    } else {
      this.#flags.set(change.key, change.config);
    }
  }
}
