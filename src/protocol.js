export const MAX_FRAME_LENGTH = 4096;

export const FrameType = Object.freeze({
  HELLO: 'hello',
  ACK: 'ack',
  SNAPSHOT: 'snapshot',
  DELTAS: 'deltas',
  HEARTBEAT: 'hb',
  ERROR: 'error'
});

export const CloseCode = Object.freeze({
  BAD_FRAME: 4400,
  NO_HELLO_IN_TIME: 4408
});

export class ProtocolError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'ProtocolError';
    this.code = code;
  }
}

const badFrame = (message) => new ProtocolError(CloseCode.BAD_FRAME, message);

export const isVersion = (value) => Number.isSafeInteger(value) && value >= 0;

export function parseClientFrame(text) {
  if (text.length > MAX_FRAME_LENGTH) {
    throw badFrame('frame is too long');
  }
  let frame;
  try {
    frame = JSON.parse(text);
  } catch {
    throw badFrame('frame is not valid JSON');
  }
  if (frame === null || typeof frame !== 'object') {
    throw badFrame('frame must be an object');
  }
  if (frame.t === FrameType.HELLO) {
    return parseHello(frame);
  }
  if (frame.t === FrameType.ACK) {
    return parseAck(frame);
  }
  throw badFrame('unknown frame type');
}

function parseHello(frame) {
  if (frame.version !== undefined && !isVersion(frame.version)) {
    throw badFrame('version must be a whole number from zero');
  }
  if (frame.clientId !== undefined && typeof frame.clientId !== 'string') {
    throw badFrame('clientId must be a string');
  }
  return { t: FrameType.HELLO, version: frame.version, clientId: frame.clientId ?? 'anonymous' };
}

function parseAck(frame) {
  if (!isVersion(frame.v)) {
    throw badFrame('ack needs a version');
  }
  return { t: FrameType.ACK, v: frame.v };
}

export const helloFrame = ({ clientId, version }) =>
  JSON.stringify({ t: FrameType.HELLO, clientId, version });

export const ackFrame = (v) => JSON.stringify({ t: FrameType.ACK, v });

export const snapshotFrame = ({ v, flags }) => JSON.stringify({ t: FrameType.SNAPSHOT, v, flags });

export const deltasFrame = (from, to, entries) =>
  JSON.stringify({ t: FrameType.DELTAS, from, to, entries });

export const heartbeatFrame = (ts, v) => JSON.stringify({ t: FrameType.HEARTBEAT, ts, v });

export const errorFrame = (code, message) => JSON.stringify({ t: FrameType.ERROR, code, message });
