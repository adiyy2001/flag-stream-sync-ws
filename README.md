# flag-stream-sync-ws

A Node WebSocket server and client that stream versioned feature flag changes and resync after a dropped connection.

## Requirements

Node 24 or newer.

## Install

```
npm install
```

## Usage

There is no standalone binary. The modules are meant to be imported, and the tests show a complete session.

```js
import { FlagStore } from './src/flag-store.js';
import { startFlagServer } from './src/server.js';
import { FlagStreamClient } from './src/client.js';

const store = new FlagStore({ capacity: 1000 });
const server = await startFlagServer({ store, port: 8080 });

const client = new FlagStreamClient({ url: server.url, clientId: 'reader' });
client.on('update', ({ version, via }) => console.log(version, via));
client.start();

store.upsert('newCheckout', { enabled: true });
```

## How the protocol works

Every message is a JSON text frame with a `t` field.

1. The client sends `hello` with the last version it applied, or no version on the first connection.
2. The server answers with one `snapshot` frame, or with one `deltas` frame holding exactly the changes the client missed. A snapshot is used when the client has no version, is ahead of the server, or is older than the retained change window.
3. Every later commit is pushed as a `deltas` frame, and the client replies with `ack`.
4. The server sends `hb` frames with the latest version. A client that sees a different version than its own, a `deltas` frame that does not start at its version, or no frame at all for too long drops the socket and reconnects with `hello`.
5. Reconnect delays use exponential backoff with full jitter.

The server closes the socket with code 4400 on a bad frame and 4408 when no `hello` arrives in time.

## Project layout

* `src/protocol.js`: frame types, close codes, frame builders and the client frame parser
* `src/flag-store.js`: versioned flag store with a bounded change log and the snapshot or deltas decision
* `src/replica.js`: client side state that applies frames and reports gaps
* `src/server.js`: WebSocket server with hello deadline, live pushes and heartbeats
* `src/client.js`: reconnecting client with stale detection and backoff
* `test/flag-store.test.js`: store and sync planning tests
* `test/replica.test.js`: frame application and gap detection tests
* `test/stream.test.js`: tests against a server on localhost

## Tests

```
npm test
```

The tests start servers on localhost with a random port and need no network access.
