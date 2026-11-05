import { EventEmitter } from 'node:events';
import WebSocket from 'ws';
import { ackFrame, helloFrame } from './protocol.js';
import { Outcome, Replica } from './replica.js';

export function backoffDelay(attempt, { baseMs, capMs }, random) {
  return Math.floor(random() * Math.min(capMs, baseMs * 2 ** attempt));
}

export class FlagStreamClient extends EventEmitter {
  #url;
  #clientId;
  #staleAfterMs;
  #backoff;
  #random;
  #replica = new Replica();
  #socket = null;
  #status = 'idle';
  #attempt = 0;
  #staleTimer = null;
  #retryTimer = null;
  #stopped = true;

  constructor({
    url,
    clientId = 'client',
    staleAfterMs = 40000,
    backoff = { baseMs: 500, capMs: 30000 },
    random = Math.random
  }) {
    super();
    this.#url = url;
    this.#clientId = clientId;
    this.#staleAfterMs = staleAfterMs;
    this.#backoff = backoff;
    this.#random = random;
  }

  get status() {
    return this.#status;
  }

  get version() {
    return this.#replica.version;
  }

  get flags() {
    return this.#replica.flags;
  }

  flag(key) {
    return this.#replica.get(key);
  }

  start() {
    if (!this.#stopped) {
      return;
    }
    this.#stopped = false;
    this.#setStatus(this.#waitingStatus());
    this.#connect();
  }

  stop() {
    this.#stopped = true;
    clearTimeout(this.#staleTimer);
    clearTimeout(this.#retryTimer);
    const socket = this.#socket;
    this.#socket = null;
    socket?.terminate();
    this.#setStatus('stopped');
  }

  #waitingStatus() {
    return this.#replica.version === null ? 'connecting' : 'reconnecting';
  }

  #setStatus(status) {
    if (status !== this.#status) {
      this.#status = status;
      this.emit('status', status);
    }
  }

  #connect() {
    const socket = new WebSocket(this.#url);
    this.#socket = socket;
    socket.on('open', () => {
      socket.send(
        helloFrame({ clientId: this.#clientId, version: this.#replica.version ?? undefined })
      );
      this.#armStaleTimer();
    });
    socket.on('message', (data) => this.#receive(socket, data.toString()));
    socket.on('error', (error) => this.emit('problem', error));
    socket.on('close', () => this.#lost(socket));
  }

  #receive(socket, text) {
    this.#armStaleTimer();
    let frame = null;
    try {
      frame = JSON.parse(text);
    } catch {
      frame = null;
    }
    const outcome = this.#replica.apply(frame);
    if (outcome === Outcome.GAP || outcome === Outcome.INVALID) {
      socket.terminate();
      return;
    }
    this.#attempt = 0;
    this.#setStatus('live');
    if (outcome === Outcome.APPLIED) {
      socket.send(ackFrame(this.#replica.version));
      this.emit('update', { version: this.#replica.version, via: frame.t });
    }
  }

  #armStaleTimer() {
    clearTimeout(this.#staleTimer);
    this.#staleTimer = setTimeout(() => this.#socket?.terminate(), this.#staleAfterMs);
  }

  #lost(socket) {
    if (socket !== this.#socket) {
      return;
    }
    clearTimeout(this.#staleTimer);
    this.#socket = null;
    this.#setStatus(this.#waitingStatus());
    const delay = backoffDelay(this.#attempt, this.#backoff, this.#random);
    this.#attempt += 1;
    this.#retryTimer = setTimeout(() => this.#connect(), delay);
  }
}
