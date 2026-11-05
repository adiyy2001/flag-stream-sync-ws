import { once } from 'node:events';
import WebSocket, { WebSocketServer } from 'ws';
import {
  CloseCode,
  FrameType,
  MAX_FRAME_LENGTH,
  ProtocolError,
  deltasFrame,
  errorFrame,
  heartbeatFrame,
  parseClientFrame,
  snapshotFrame
} from './protocol.js';

function sendIfOpen(socket, text) {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(text);
  }
}

function closeWithError(socket, code, message) {
  sendIfOpen(socket, errorFrame(code, message));
  socket.close(code, message);
}

function openSession(socket, hello, { store, heartbeatMs, clock }) {
  const plan = store.planSync(hello.version);
  sendIfOpen(
    socket,
    plan.kind === 'snapshot'
      ? snapshotFrame(plan.snapshot)
      : deltasFrame(plan.from, plan.to, plan.entries)
  );
  const unsubscribe = store.subscribe((entry, previousVersion) => {
    sendIfOpen(socket, deltasFrame(previousVersion, entry.v, [entry]));
  });
  const heartbeat = setInterval(() => {
    sendIfOpen(socket, heartbeatFrame(clock(), store.version));
  }, heartbeatMs);
  return {
    clientId: hello.clientId,
    stop() {
      unsubscribe();
      clearInterval(heartbeat);
    }
  };
}

function serveSocket(socket, options) {
  let session = null;
  const helloDeadline = setTimeout(() => {
    closeWithError(socket, CloseCode.NO_HELLO_IN_TIME, 'send hello within the deadline');
  }, options.helloTimeoutMs);

  const handle = (frame) => {
    if (frame.t === FrameType.HELLO) {
      if (session) {
        throw new ProtocolError(CloseCode.BAD_FRAME, 'hello was already sent');
      }
      clearTimeout(helloDeadline);
      session = openSession(socket, frame, options);
      return;
    }
    if (!session) {
      throw new ProtocolError(CloseCode.BAD_FRAME, 'send hello before ack');
    }
    options.onAck({ clientId: session.clientId, version: frame.v });
  };

  socket.on('message', (data) => {
    try {
      handle(parseClientFrame(data.toString()));
    } catch (error) {
      if (!(error instanceof ProtocolError)) {
        throw error;
      }
      closeWithError(socket, error.code, error.message);
    }
  });
  socket.on('error', () => socket.terminate());
  socket.on('close', () => {
    clearTimeout(helloDeadline);
    session?.stop();
  });
}

export async function startFlagServer({
  store,
  port = 0,
  host = '127.0.0.1',
  heartbeatMs = 15000,
  helloTimeoutMs = 5000,
  onAck = () => {},
  clock = Date.now
}) {
  const wss = new WebSocketServer({ port, host, maxPayload: MAX_FRAME_LENGTH * 4 });
  await once(wss, 'listening');
  const options = { store, heartbeatMs, helloTimeoutMs, onAck, clock };
  wss.on('connection', (socket) => serveSocket(socket, options));

  const dropConnections = () => {
    for (const client of wss.clients) {
      client.terminate();
    }
  };
  const boundPort = wss.address().port;

  return {
    port: boundPort,
    url: `ws://${host}:${boundPort}`,
    dropConnections,
    async close() {
      dropConnections();
      wss.close();
      await once(wss, 'close');
    }
  };
}
