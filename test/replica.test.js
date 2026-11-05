import { describe, expect, it } from 'vitest';
import { Outcome, Replica } from '../src/replica.js';

const snapshot = (v, flags) => ({ t: 'snapshot', v, flags });
const upsert = (key, config) => ({ op: 'upsert', key, config });
const deltas = (from, to, entries) => ({ t: 'deltas', from, to, entries });

function replicaAt(version, flags = {}) {
  const replica = new Replica();
  replica.apply(snapshot(version, flags));
  return replica;
}

describe('Replica', () => {
  it('starts without a version and adopts a snapshot', () => {
    const replica = new Replica();
    expect(replica.version).toBeNull();
    expect(replica.apply(snapshot(3, { alpha: { on: true } }))).toBe(Outcome.APPLIED);
    expect(replica.version).toBe(3);
    expect(replica.get('alpha')).toEqual({ on: true });
  });

  it('replaces everything it held when a new snapshot arrives', () => {
    const replica = replicaAt(3, { alpha: {} });
    replica.apply(snapshot(9, { beta: {} }));
    expect(replica.flags).toEqual({ beta: {} });
  });

  it('applies deltas in order on top of its version', () => {
    const replica = replicaAt(1, { alpha: { on: true } });
    const outcome = replica.apply(
      deltas(1, 3, [
        { v: 2, changes: [upsert('beta', { on: true }), { op: 'remove', key: 'alpha' }] },
        { v: 3, changes: [upsert('beta', { on: false })] }
      ])
    );
    expect(outcome).toBe(Outcome.APPLIED);
    expect(replica.version).toBe(3);
    expect(replica.flags).toEqual({ beta: { on: false } });
  });

  it('reports a gap when deltas do not start at its version', () => {
    const replica = replicaAt(1);
    expect(replica.apply(deltas(5, 6, [{ v: 6, changes: [] }]))).toBe(Outcome.GAP);
    expect(replica.version).toBe(1);
  });

  it('reports a gap when deltas arrive before any snapshot', () => {
    expect(new Replica().apply(deltas(0, 1, [{ v: 1, changes: [] }]))).toBe(Outcome.GAP);
  });

  it('rejects non consecutive entries and keeps its state', () => {
    const replica = replicaAt(1, { alpha: {} });
    const outcome = replica.apply(
      deltas(1, 3, [
        { v: 2, changes: [upsert('beta', {})] },
        { v: 4, changes: [upsert('gamma', {})] }
      ])
    );
    expect(outcome).toBe(Outcome.INVALID);
    expect(replica.flags).toEqual({ alpha: {} });
    expect(replica.version).toBe(1);
  });

  it('treats empty deltas at its own version as current', () => {
    expect(replicaAt(4).apply(deltas(4, 4, []))).toBe(Outcome.CURRENT);
  });

  it('compares heartbeat versions with its own', () => {
    const replica = replicaAt(4);
    expect(replica.apply({ t: 'hb', ts: 1, v: 4 })).toBe(Outcome.CURRENT);
    expect(replica.apply({ t: 'hb', ts: 2, v: 7 })).toBe(Outcome.GAP);
  });

  it('rejects frames it does not know', () => {
    expect(new Replica().apply({ t: 'mystery' })).toBe(Outcome.INVALID);
    expect(new Replica().apply(null)).toBe(Outcome.INVALID);
  });
});
