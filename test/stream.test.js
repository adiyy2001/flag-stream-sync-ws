import { once } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import WebSocket, { WebSocketServer } from 'ws';
import { FlagStreamClient } from '../src/client.js';
import { FlagStore } from '../src/flag-store.js';
import { startFlagServer } from '../src/server.js';

const cleanups = [];

afterEach(async () => {
  while (cleanups.length > 0) {
    await cleanups.pop()();
  }
});

const eventually = (assertion) => vi.waitFor(assertion, { timeout: 3000, interval: 10 });

async function startServer(options = {}) {
  const store = options.store ?? new FlagStore();
  const server = await startFlagServer({ ...options, store });
  cleanups.push(() => server.close());
  return { store, server };
}

function startClient(url, options = {}) {
  const client = new FlagStreamClient({
    url,
    backoff: { baseMs: 5, capMs: 20 },
    random: () => 0,
    ...options
  });
  const updates = [];
  client.on('update', (update) => updates.push(update));
  cleanups.push(() => client.stop());
  client.start();
  return { client, updates };
}

function openRaw(url) {
  const socket = new WebSocket(url);
  const frames = [];
  socket.on('message', (data) => frames.push(JSON.parse(data.toString())));
  cleanups.push(() => socket.terminate());
  const opened = once(socket, 'open');
  return {
    frames,
    closed: once(socket, 'close'),
    async send(payload) {
      await opened;
      socket.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
    }
  };
}

async function startScriptedServer(script) {
  const wss = new WebSocketServer({ port: 0, host: '127.0.0.1' });
  await once(wss, 'listening');
  const hellos = [];
  let connections = 0;
  wss.on('connection', (socket) => {
    const index = connections;
    connections += 1;
    socket.on('error', () => socket.terminate());
    socket.on('message', (data) => {
      const frame = JSON.parse(data.toString());
      if (frame.t === 'hello') {
        hellos.push(frame);
        script(socket, index);
      }
    });
  });
  cleanups.push(async () => {
    for (const client of wss.clients) {
      client.terminate();
    }
    wss.close();
    await once(wss, 'close');
  });
  return { url: `ws://127.0.0.1:${wss.address().port}`, hellos };
}

const sendFrame = (socket, frame) => socket.send(JSON.stringify(frame));

describe('server handshake', () => {
  it('answers a hello without a version with a snapshot', async () => {
    const { store, server } = await startServer();
    store.upsert('checkout', { enabled: true });
    const raw = openRaw(server.url);
    await raw.send({ t: 'hello' });
    await eventually(() => expect(raw.frames).toHaveLength(1));
    expect(raw.frames[0]).toEqual({ t: 'snapshot', v: 1, flags: { checkout: { enabled: true } } });
  });

  it('answers a known version with exactly the missed entries', async () => {
    const { store, server } = await startServer();
    for (const key of ['a', 'b', 'c']) {
      store.upsert(key, {});
    }
    const raw = openRaw(server.url);
    await raw.send({ t: 'hello', version: 1 });
    await eventually(() => expect(raw.frames).toHaveLength(1));
    expect(raw.frames[0]).toMatchObject({ t: 'deltas', from: 1, to: 3 });
    expect(raw.frames[0].entries.map((entry) => entry.v)).toEqual([2, 3]);
  });

  it('answers a version ahead of the server with a snapshot', async () => {
    const { server } = await startServer();
    const raw = openRaw(server.url);
    await raw.send({ t: 'hello', version: 99 });
    await eventually(() => expect(raw.frames[0]?.t).toBe('snapshot'));
  });

  it('pushes each later commit as deltas from the previous version', async () => {
    const { store, server } = await startServer();
    const raw = openRaw(server.url);
    await raw.send({ t: 'hello', version: 0 });
    await eventually(() => expect(raw.frames).toHaveLength(1));
    store.upsert('alpha', { on: true });
    await eventually(() => expect(raw.frames).toHaveLength(2));
    expect(raw.frames[1]).toMatchObject({ t: 'deltas', from: 0, to: 1 });
  });

  it('sends heartbeats with the latest version and reports acknowledgements', async () => {
    const onAck = vi.fn();
    const { store, server } = await startServer({ heartbeatMs: 20, onAck });
    store.upsert('alpha', {});
    const raw = openRaw(server.url);
    await raw.send({ t: 'hello', version: 1, clientId: 'tester' });
    await raw.send({ t: 'ack', v: 1 });
    await eventually(() => expect(raw.frames.some((f) => f.t === 'hb' && f.v === 1)).toBe(true));
    expect(onAck).toHaveBeenCalledWith({ clientId: 'tester', version: 1 });
  });
});

describe('server protocol violations', () => {
  it('closes with 4408 when no hello arrives in time', async () => {
    const { server } = await startServer({ helloTimeoutMs: 30 });
    const raw = openRaw(server.url);
    const [code] = await raw.closed;
    expect(code).toBe(4408);
  });

  it('closes with 4400 on a second hello and explains why', async () => {
    const { server } = await startServer();
    const raw = openRaw(server.url);
    await raw.send({ t: 'hello' });
    await raw.send({ t: 'hello' });
    const [code] = await raw.closed;
    expect(code).toBe(4400);
    expect(raw.frames.at(-1)).toEqual({ t: 'error', code: 4400, message: 'hello was already sent' });
  });

  it('closes with 4400 when ack comes before hello', async () => {
    const { server } = await startServer();
    const raw = openRaw(server.url);
    await raw.send({ t: 'ack', v: 1 });
    const [code] = await raw.closed;
    expect(code).toBe(4400);
  });

  it.each([
    ['text that is not JSON', 'not json'],
    ['an unknown frame type', { t: 'mystery' }],
    ['a fractional version', { t: 'hello', version: 1.5 }],
    ['a frame that is too long', 'x'.repeat(5000)]
  ])('closes with 4400 on %s', async (label, payload) => {
    const { server } = await startServer();
    const raw = openRaw(server.url);
    await raw.send(payload);
    const [code] = await raw.closed;
    expect(code).toBe(4400);
  });
});

describe('client against the real server', () => {
  it('syncs a snapshot and then follows live changes', async () => {
    const { store, server } = await startServer();
    store.upsert('alpha', { on: true });
    const { client, updates } = startClient(server.url);
    await eventually(() => expect(client.flags).toEqual({ alpha: { on: true } }));
    store.upsert('beta', { on: false });
    await eventually(() => expect(client.flag('beta')).toEqual({ on: false }));
    expect(client.status).toBe('live');
    expect(updates.map((update) => update.via)).toEqual(['snapshot', 'deltas']);
  });

  it('acknowledges every applied version', async () => {
    const onAck = vi.fn();
    const { store, server } = await startServer({ onAck });
    startClient(server.url, { clientId: 'reader' });
    await eventually(() => expect(onAck).toHaveBeenCalledWith({ clientId: 'reader', version: 0 }));
    store.upsert('alpha', {});
    await eventually(() => expect(onAck).toHaveBeenCalledWith({ clientId: 'reader', version: 1 }));
  });

  it('catches up with deltas after a drop inside the retained window', async () => {
    const { store, server } = await startServer();
    store.upsert('alpha', {});
    const { client, updates } = startClient(server.url);
    await eventually(() => expect(client.version).toBe(1));
    server.dropConnections();
    store.upsert('beta', {});
    store.remove('alpha');
    await eventually(() => expect(client.version).toBe(3));
    expect(client.flags).toEqual({ beta: {} });
    expect(updates.at(-1)).toEqual({ version: 3, via: 'deltas' });
  });

  it('resyncs with a snapshot after a drop beyond the retained window', async () => {
    const { store, server } = await startServer({ store: new FlagStore({ capacity: 2 }) });
    store.upsert('alpha', {});
    const { client, updates } = startClient(server.url);
    await eventually(() => expect(client.version).toBe(1));
    server.dropConnections();
    for (const key of ['b', 'c', 'd', 'e']) {
      store.upsert(key, {});
    }
    await eventually(() => expect(client.version).toBe(5));
    expect(updates.at(-1)).toEqual({ version: 5, via: 'snapshot' });
    expect(Object.keys(client.flags)).toEqual(['alpha', 'b', 'c', 'd', 'e']);
  });
});

describe('client resync triggers', () => {
  it('reconnects with its version when deltas skip ahead', async () => {
    const { url, hellos } = await startScriptedServer((socket, index) => {
      if (index === 0) {
        sendFrame(socket, { t: 'snapshot', v: 1, flags: { alpha: { on: true } } });
        sendFrame(socket, { t: 'deltas', from: 5, to: 6, entries: [] });
      } else {
        sendFrame(socket, { t: 'snapshot', v: 6, flags: { alpha: { on: false } } });
      }
    });
    const { client } = startClient(url);
    await eventually(() => expect(client.version).toBe(6));
    expect(hellos.map((hello) => hello.version)).toEqual([undefined, 1]);
    expect(client.flag('alpha')).toEqual({ on: false });
  });

  it('reconnects when a heartbeat reports a newer version', async () => {
    const { url, hellos } = await startScriptedServer((socket, index) => {
      if (index === 0) {
        sendFrame(socket, { t: 'snapshot', v: 1, flags: {} });
        sendFrame(socket, { t: 'hb', ts: 1, v: 4 });
      } else {
        sendFrame(socket, { t: 'snapshot', v: 4, flags: {} });
      }
    });
    const { client } = startClient(url);
    await eventually(() => expect(client.version).toBe(4));
    expect(hellos[1].version).toBe(1);
  });

  it('treats a silent connection as dead and reconnects', async () => {
    const { url, hellos } = await startScriptedServer((socket) => {
      sendFrame(socket, { t: 'snapshot', v: 1, flags: {} });
    });
    startClient(url, { staleAfterMs: 60 });
    await eventually(() => expect(hellos.length).toBeGreaterThanOrEqual(2));
    expect(hellos[1].version).toBe(1);
  });

  it('stops reconnecting after stop', async () => {
    const { server } = await startServer();
    const { client } = startClient(server.url);
    await eventually(() => expect(client.status).toBe('live'));
    client.stop();
    expect(client.status).toBe('stopped');
    server.dropConnections();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(client.status).toBe('stopped');
  });
});
