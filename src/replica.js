import { FrameType, isVersion } from './protocol.js';

export const Outcome = Object.freeze({
  APPLIED: 'applied',
  CURRENT: 'current',
  GAP: 'gap',
  INVALID: 'invalid'
});

const isObject = (value) => value !== null && typeof value === 'object';

function isChange(change) {
  if (!isObject(change) || typeof change.key !== 'string') {
    return false;
  }
  return change.op === 'remove' || (change.op === 'upsert' && isObject(change.config));
}

export class Replica {
  #version = null;
  #flags = new Map();

  get version() {
    return this.#version;
  }

  get flags() {
    return Object.fromEntries(this.#flags);
  }

  get(key) {
    return this.#flags.get(key);
  }

  apply(frame) {
    switch (frame?.t) {
      case FrameType.SNAPSHOT:
        return this.#applySnapshot(frame);
      case FrameType.DELTAS:
        return this.#applyDeltas(frame);
      case FrameType.HEARTBEAT:
        return frame.v === this.#version ? Outcome.CURRENT : Outcome.GAP;
      default:
        return Outcome.INVALID;
    }
  }

  #applySnapshot({ v, flags }) {
    if (!isVersion(v) || !isObject(flags)) {
      return Outcome.INVALID;
    }
    this.#flags = new Map(Object.entries(flags));
    this.#version = v;
    return Outcome.APPLIED;
  }

  #applyDeltas({ from, to, entries }) {
    if (!isVersion(from) || !isVersion(to) || !Array.isArray(entries)) {
      return Outcome.INVALID;
    }
    if (this.#version === null || from !== this.#version) {
      return Outcome.GAP;
    }
    if (entries.length === 0) {
      return to === from ? Outcome.CURRENT : Outcome.INVALID;
    }
    if (!this.#isContiguous(from, to, entries)) {
      return Outcome.INVALID;
    }
    for (const entry of entries) {
      for (const change of entry.changes) {
        this.#applyChange(change);
      }
    }
    this.#version = to;
    return Outcome.APPLIED;
  }

  #isContiguous(from, to, entries) {
    let expected = from;
    for (const entry of entries) {
      expected += 1;
      if (entry?.v !== expected || !Array.isArray(entry.changes)) {
        return false;
      }
      if (!entry.changes.every(isChange)) {
        return false;
      }
    }
    return expected === to;
  }

  #applyChange(change) {
    if (change.op === 'remove') {
      this.#flags.delete(change.key);
    } else {
      this.#flags.set(change.key, change.config);
    }
  }
}
