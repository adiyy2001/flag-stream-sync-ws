import { describe, expect, it } from 'vitest';
import { FlagStore } from '../src/flag-store.js';

function storeWith(count, options) {
  const store = new FlagStore(options);
  for (let index = 1; index <= count; index += 1) {
    store.upsert(`flag${index}`, { rank: index });
  }
  return store;
}

describe('FlagStore commits', () => {
  it('numbers a batch once and stamps the commit time', () => {
    const store = new FlagStore({ clock: () => 1700000000000 });
    const entry = store.commit([
      { op: 'upsert', key: 'alpha', config: { enabled: true } },
      { op: 'upsert', key: 'beta', config: { enabled: false } }
    ]);
    expect(entry.v).toBe(1);
    expect(entry.committedAtMs).toBe(1700000000000);
    expect(entry.changes).toHaveLength(2);
    expect(store.version).toBe(1);
  });

  it('ignores changes that alter nothing', () => {
    const store = storeWith(1);
    expect(store.upsert('flag1', { rank: 1 })).toBeNull();
    expect(store.remove('missing')).toBeNull();
    expect(store.version).toBe(1);
  });

  it('rejects an invalid batch without touching the state', () => {
    const store = new FlagStore();
    const invalid = [
      { op: 'upsert', key: 'ok', config: {} },
      { op: 'rename', key: 'ok' }
    ];
    expect(() => store.commit(invalid)).toThrow(TypeError);
    expect(store.version).toBe(0);
    expect(store.snapshot().flags).toEqual({});
  });

  it('hands out snapshots that cannot change the store', () => {
    const store = storeWith(1);
    store.snapshot().flags.flag1.rank = 99;
    expect(store.snapshot().flags.flag1.rank).toBe(1);
  });

  it('tells subscribers the entry and the previous version until they unsubscribe', () => {
    const store = new FlagStore();
    const seen = [];
    const unsubscribe = store.subscribe((entry, previous) => seen.push([previous, entry.v]));
    store.upsert('alpha', {});
    unsubscribe();
    store.upsert('beta', {});
    expect(seen).toEqual([[0, 1]]);
  });
});

describe('FlagStore planSync', () => {
  it('answers a client without a version with a snapshot', () => {
    const plan = storeWith(2).planSync(undefined);
    expect(plan.kind).toBe('snapshot');
    expect(plan.snapshot.v).toBe(2);
    expect(Object.keys(plan.snapshot.flags)).toEqual(['flag1', 'flag2']);
  });

  it('answers an up to date client with empty deltas', () => {
    const plan = storeWith(2).planSync(2);
    expect(plan).toEqual({ kind: 'deltas', from: 2, to: 2, entries: [] });
  });

  it('sends exactly the missed entries when the window covers them', () => {
    const plan = storeWith(4).planSync(1);
    expect(plan.kind).toBe('deltas');
    expect([plan.from, plan.to]).toEqual([1, 4]);
    expect(plan.entries.map((entry) => entry.v)).toEqual([2, 3, 4]);
  });

  it('falls back to a snapshot when the window no longer covers the client', () => {
    const store = storeWith(6, { capacity: 3 });
    expect(store.planSync(1).kind).toBe('snapshot');
    const covered = store.planSync(3);
    expect(covered.kind).toBe('deltas');
    expect(covered.entries.map((entry) => entry.v)).toEqual([4, 5, 6]);
  });

  it('answers a client that is ahead of the server with a snapshot', () => {
    expect(storeWith(2).planSync(99).kind).toBe('snapshot');
  });

  it('rejects a capacity below one', () => {
    expect(() => new FlagStore({ capacity: 0 })).toThrow(RangeError);
  });
});
