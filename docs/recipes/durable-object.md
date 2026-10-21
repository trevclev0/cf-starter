# Recipe: Durable Objects (real-time state + WebSockets)

A Durable Object (DO) is a single-threaded instance with its own SQLite
storage, addressed by name. Use one when many clients need to share live,
consistent state — a room, a counter, a game lobby. With the WebSocket
Hibernation API, idle connections cost nothing while the DO sleeps.

> Proven in [Click Rampage](https://github.com/trevclev0/click-rampage)
> (`src/worker/durable-objects/Room.ts`), a live multiplayer counter. Each
> "Gotcha" below cost that project a bug fix; its git history has the
> details.

## 1. Write the class

`src/worker/durable-objects/Room.ts`:

```ts
import { DurableObject } from "cloudflare:workers";

/**
 * Per-socket state. Instance fields are lost when the DO hibernates, so
 * anything a socket needs later goes through serializeAttachment.
 */
type Attachment = {
  playerId: string;
  /** When (ms) the client last pinged; see "Presence" below. */
  lastPing: number;
  /** Set once a sweep has handled this socket as gone. */
  swept?: true;
};

export class Room extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Runs before any request is delivered — safe place for schema setup.
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS players (id TEXT PRIMARY KEY, count INTEGER NOT NULL DEFAULT 0)",
      );
    });
  }

  async fetch(request: Request): Promise<Response> {
    // The Worker authenticates; the DO trusts only what the Worker passes.
    const playerId = request.headers.get("x-player-id");
    if (!playerId) return new Response("Missing player", { status: 400 });

    const { 0: client, 1: server } = new WebSocketPair();
    // acceptWebSocket (not server.accept()) opts into hibernation. The tag
    // lets getWebSockets(playerId) find every tab of one player.
    this.ctx.acceptWebSocket(server, [playerId]);
    server.serializeAttachment({
      playerId,
      lastPing: Date.now(),
    } satisfies Attachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, raw: string | ArrayBuffer) {
    // Identity comes only from the socket, never from the message body.
    const attachment = ws.deserializeAttachment() as Attachment;
    if (attachment.swept) return;
    // ...parse `raw` with a shared schema, update state, then broadcast.
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string) {
    this.#handleDisconnect(ws);
    try {
      // Complete the closing handshake; a no-op if the runtime already did.
      ws.close(code, reason);
    } catch {
      // Already closed.
    }
  }

  async webSocketError(ws: WebSocket) {
    this.#handleDisconnect(ws);
  }

  #handleDisconnect(_ws: WebSocket) {
    // See "Presence" below.
  }

  /** Accepted sockets that are still OPEN; see "Presence" below. */
  #openSockets(tag?: string): WebSocket[] {
    return this.ctx
      .getWebSockets(tag)
      .filter((socket) => socket.readyState === WebSocket.OPEN);
  }

  #broadcast(data: string, except?: WebSocket) {
    for (const socket of this.#openSockets()) {
      if (socket !== except) socket.send(data);
    }
  }
}
```

Validate every message against a schema shared with the client
(`src/shared/protocol.ts`, zod `discriminatedUnion`) and reject oversized
frames by length before `JSON.parse`. A socket that is closing can't take a
reply — check `readyState === WebSocket.OPEN` before `send`, or it throws.

## 2. Export it from the Worker entry

`src/worker/index.ts`:

```ts
export { Room } from "@worker/durable-objects/Room";
export default app;
```

## 3. Bind it in `wrangler.jsonc`

`durable_objects` is **not** inherited by named environments; `migrations`
is.

```jsonc
"durable_objects": {
  "bindings": [{ "name": "ROOM", "class_name": "Room" }]
},
"migrations": [{ "tag": "v1", "new_sqlite_classes": ["Room"] }],
"env": {
  "preview": {
    "durable_objects": {
      "bindings": [{ "name": "ROOM", "class_name": "Room" }]
    }
  }
}
```

Migrations are append-only: never edit an applied tag; add `v2`, `v3`, … for
new, renamed, or deleted classes. Each per-PR preview Worker is a fresh script,
so it starts from `v1`.

Then `bun run cf-typegen`.

## 4. Route WebSocket upgrades to it

```ts
export const wsRouter = new Hono<AppEnv>().get(
  "/",
  playerMiddleware, // reads or mints the HttpOnly player cookie
  async (c) => {
    if (c.req.header("upgrade")?.toLowerCase() !== "websocket") {
      return c.text("Expected WebSocket upgrade", 426);
    }
    // Browsers always send Origin on WebSocket upgrades; anything else is
    // cross-site WebSocket hijacking or a non-browser client.
    if (c.req.header("origin") !== new URL(c.req.url).origin) {
      return c.text("Forbidden", 403);
    }

    const headers = new Headers(c.req.raw.headers);
    // Overwritten here, so a client can't spoof it.
    headers.set("x-player-id", c.get("playerId"));
    const upgrade = await c.env.ROOM.getByName("global").fetch(
      new Request(c.req.raw, { headers }),
    );

    // Returning the DO's 101 as-is drops the cookie the middleware just
    // minted, so the next reconnect arrives as a new player. Copy it over.
    const responseHeaders = new Headers(upgrade.headers);
    for (const cookie of c.res.headers.getSetCookie()) {
      responseHeaders.append("set-cookie", cookie);
    }
    return new Response(upgrade.body, {
      status: upgrade.status,
      statusText: upgrade.statusText,
      headers: responseHeaders,
      webSocket: upgrade.webSocket,
    });
  },
);
```

**Gotcha — the lost cookie:** on a player's first visit the cookie is minted
on the same request as the upgrade. Hono's `c.res` holds it, but the DO's
response replaces `c.res`, so it never reaches the browser unless you copy
it as above.

## 5. Presence: who is online

- **Several tabs, one player.** Tag each socket with the player id. A
  player joins when `#openSockets(playerId)` was empty before this socket,
  and leaves when their last open socket goes. Announce joins and leaves
  only on those edges.
- **Use one definition of "open."** A closing socket can stay in
  `getWebSockets()` briefly. Filter on `readyState === WebSocket.OPEN`
  everywhere you decide presence.
- **Keep join order in the attachment.** `getWebSockets()` order is not
  join order (newest-first in practice). Store a Room-wide sequence number
  in each attachment (a second tab reuses its player's), derive the next
  one from the max across sockets, and sort by it. A plain counter field
  would reset on hibernation.

**Gotcha — ghost players.** Some sockets never report a close: half-open
connections, suspended phone tabs, and, under `bun run dev`, an upgrade the
browser abandoned mid-handshake (`@cloudflare/vite-plugin` destroys the TCP
socket without closing the Worker end). The DO keeps them OPEN forever. Fix
it with a heartbeat:

1. The client pings every `PING_INTERVAL_MS` (30 s; share the constant in
   `src/shared/`), and the DO stores `lastPing` in the attachment.
2. On a join, on a close, and on a ping (throttled to once every 10 s, so a
   busy room isn't O(N²)), sweep: close sockets with no ping for
   `STALE_SOCKET_MS`, handle each as a disconnect, and mark it `swept`.
3. Ignore messages from `swept` sockets only. Don't skip every non-OPEN
   socket — a client that closes on its own may still deliver its last
   message.

No alarm is needed while anyone is connected, because every client's pings
drive the sweep. Set `STALE_SOCKET_MS` to 120 s or more: Chrome runs a long
hidden tab's timers only once a minute, and gaps between pings can reach
90 s.

**Gotcha — StrictMode in dev.** React StrictMode mounts effects twice, so
the first socket is closed mid-handshake, which is exactly the abandoned
upgrade above. Click Rampage's `useRoomSocket` parks a socket torn down
while `CONNECTING` for one tick and lets the effect's second run adopt it,
so each page load makes one handshake.

## 6. Storage: stay under the Free-plan write cap

SQLite-backed DOs on Workers Free get **100,000 rows written per day**, and
writes fail until 00:00 UTC after that. One row per event (a click, a
message) exhausts it in about 83 minutes at 20 events a second.

- **Write back, don't write through.** Keep hot rows in an in-memory
  `Map`, broadcast changes at once, and flush dirty rows at most once per
  second with one `UPDATE` per row inside `ctx.storage.transactionSync`.
- **Flush with `setTimeout`, not `setAlarm`.** An alarm bills a row write
  each time it is set. A pending timer also keeps the DO from hibernating,
  which is what keeps unflushed data in memory (at the cost of billed
  duration while anything is dirty).
- **On a player's last disconnect, write only that player.** Flushing
  everyone on each leave brings back per-event writes under reconnect churn.
- **Back off failed flushes** (double per failure, cap around 60 s, reset
  on any success), or a full write cap means a retry and an error line
  every second until midnight UTC.
- **Name the trade-off in the class doc.** An eviction or deploy loses up
  to one flush interval of data. While writes keep failing, it loses
  everything unflushed.

## 7. Rate limiting

Rate-limit per **player**, in an instance `Map` keyed by player id — not in
each socket's attachment. A per-socket window multiplies with every tab or
scripted socket a player opens. Keep a window after its player leaves, so
reconnecting doesn't refill it, and prune expired windows in the presence
sweep. Losing the map on hibernation only resets a window at most a second
old.

## 8. The client

- Reconnect with exponential backoff and jitter (1 s doubling to 30 s,
  times 0.5–1), so a DO restart doesn't stampede.
- Reset the backoff only after a connection stays up for a while (10 s).
  A socket that opens and drops at once should keep backing off.
- Treat a missing pong (10 s) or a slow handshake (10 s) as a drop.
- Keep counts server-authoritative: the client sends intents
  (`{ type: "increment" }`) and renders what the DO broadcasts.

## 9. Test it

In `vitest.config.integration.ts`, bind the class (it is found in `main`).
Use the object form so the test DO is SQLite-backed like production:

```ts
miniflare: {
  // ...
  durableObjects: {
    ROOM: { className: "Room", useSQLite: true },
  },
},
```

`cloudflare:test` (`runInDurableObject`, `evictDurableObject`) needs its
types in scope. Add `src/worker/test-utils/integration-test-types.d.ts`:

```ts
// Types for `cloudflare:test` in *.integration.spec.ts (workerd pool only).
/// <reference path="../../../node_modules/@cloudflare/vitest-pool-workers/types/cloudflare-test.d.ts" />
```

### A WebSocket test helper

`src/worker/test-utils/socket.ts` — opens a socket and queues what the
server sends, so a test can `await next()` in order:

```ts
import { exports } from "cloudflare:workers";
import type { ServerMessage } from "@shared/protocol";

export const ORIGIN = "http://localhost";

/** Through the real Worker entry: middleware, cookie, origin check. */
export async function connect(headers: Record<string, string> = {}) {
  const response = await exports.default.fetch(
    new Request(`${ORIGIN}/api/ws`, {
      headers: { Upgrade: "websocket", Origin: ORIGIN, ...headers },
    }),
  );
  return open(response);
}

/** Straight to one DO as `playerId`, so a test can use its own instance. */
export async function connectToRoom(
  room: { fetch(request: Request): Promise<Response> },
  playerId: string,
) {
  const response = await room.fetch(
    new Request(`${ORIGIN}/api/ws`, {
      headers: { Upgrade: "websocket", "x-player-id": playerId },
    }),
  );
  return open(response);
}

export type TestSocket = ReturnType<typeof open>;

function open(response: Response) {
  const ws = response.webSocket;
  if (!ws) throw new Error(`Upgrade failed with ${response.status}`);
  ws.accept();

  const queue: ServerMessage[] = [];
  const waiters: ((message: ServerMessage) => void)[] = [];
  ws.addEventListener("message", (event) => {
    const message = JSON.parse(String(event.data)) as ServerMessage;
    const waiter = waiters.shift();
    if (waiter) waiter(message);
    else queue.push(message);
  });

  return {
    ws,
    response,
    close: () => ws.close(1000, "done"),
    send: (data: unknown) =>
      ws.send(typeof data === "string" ? data : JSON.stringify(data)),
    next: () =>
      new Promise<ServerMessage>((resolve) => {
        const queued = queue.shift();
        if (queued) resolve(queued);
        else waiters.push(resolve);
      }),
  };
}

/**
 * Reads messages until the pong for a fresh ping. Messages arrive in
 * order, so everything returned came before the pong — this lets a test
 * assert that something was NOT sent.
 */
export async function drain(socket: TestSocket) {
  const t = Math.random();
  socket.send({ type: "ping", t });
  const seen: ServerMessage[] = [];
  for (;;) {
    const message = await socket.next();
    if (message.type === "pong" && message.t === t) return seen;
    seen.push(message);
  }
}
```

`drain` needs the DO to answer `{ type: "ping", t }` with
`{ type: "pong", t }` — have it reply to the heartbeat pings in
"Presence" that way.

### Patterns that held up

- **One instance per test.** `env.ROOM.getByName(crypto.randomUUID())`
  keeps state and timers from leaking between tests. Only the route specs
  need the shared `"global"` instance.
- **Fake time inside the DO.** The DO runs in the test's isolate, so
  `vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] })`
  fakes its timers too. Advance them from inside the instance:
  `runInDurableObject(stub, () => vi.advanceTimersByTime(ms))`. Call
  `vi.useRealTimers()` in `afterEach`.
- **Count rows written.** Spy on `state.storage.sql.exec` (calling through)
  and sum each result's `rowsWritten` to pin the write budget in a test.
- **Simulate hibernation or eviction** with `evictDurableObject(stub)`, then
  check what survived in SQLite and the attachments.
- **Prove a fix with a failing spec.** Run the new spec against the old code
  before writing the fix (`git stash` the source file); a regression spec
  that passes on the old code proves nothing.

## Notes

- SQLite-backed DOs are available on the Workers Free plan; mind the
  100,000 rows written per day (section 6).
- `bun run dev` runs DOs locally through `@cloudflare/vite-plugin`.
- Docs: <https://developers.cloudflare.com/durable-objects/>
