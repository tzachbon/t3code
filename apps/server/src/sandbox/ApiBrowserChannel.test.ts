// @effect-diagnostics nodeBuiltinImport:off -- Tests the loopback transport with a fake Depot API; never starts Chromium.
import { expect } from "vite-plus/test";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";

import * as ApiBrowserChannel from "./ApiBrowserChannel.ts";

function mailbox<A>() {
  const values: A[] = [];
  let wake: (() => void) | undefined;
  let closed = false;
  return {
    push(value: A) { values.push(value); wake?.(); wake = undefined; },
    end() { closed = true; wake?.(); wake = undefined; },
    async *[Symbol.asyncIterator]() {
      for (;;) {
        if (closed && values.length === 0) return;
        if (values.length > 0) yield values.shift()!;
        else await new Promise<void>((resolve) => { wake = resolve; });
      }
    },
  };
}

function vendor(mode: "echo" | "error" | "gap" | "finished" | "evicted" = "echo") {
  const events = mailbox<unknown>();
  const commands: string[] = [];
  let offset = 0n;
  let requests = 0;
  const stdout = (data: Uint8Array) => {
    offset += BigInt(data.byteLength);
    events.push({ event: { case: "stdout", value: { data, byteOffset: offset } } });
  };
  // Only the external vendor is faked; requests and streamed bytes exercise the real channel.
  const client = { rpc: {
    runCommand: (_input: unknown, options: { signal: AbortSignal }) => {
      requests++;
      options.signal.addEventListener("abort", () => events.end(), { once: true });
      events.push({ event: { case: "stderr", value: { data: new TextEncoder().encode("READY\n") } } });
      return events;
    },
    writeFile: async (input: AsyncIterable<{ input: { case: string; value: Uint8Array } }>) => {
      for await (const message of input) {
        if (message.input.case !== "data") continue;
        const line = new TextDecoder().decode(message.input.value);
        if (line === '{"__close":true}\n') { events.end(); continue; }
        commands.push(line);
        if (mode === "error" || mode === "evicted") {
          events.push({ event: { case: mode } });
        } else if (mode === "finished") {
          events.push({ event: { case: "finished", value: { exitCode: 0 } } });
        } else if (mode === "gap") {
          events.push({ event: { case: "stdout", value: { data: new Uint8Array([10]), byteOffset: 9n } } });
        } else {
          const response = new TextEncoder().encode(JSON.stringify({ id: 1, result: "héllo" }) + "\n");
          const split = response.indexOf(0xc3) + 1;
          stdout(response.subarray(0, split));
          stdout(response.subarray(split));
        }
      }
      return { bytesWritten: 0n };
    },
  } } as unknown as Parameters<ApiBrowserChannel.ApiBrowserChannel["Service"]["open"]>[0]["client"];
  return { client, commands, get requests() { return requests; } };
}

const opened = (socket: WebSocket) => new Promise<void>((resolve, reject) => {
  socket.addEventListener("open", () => resolve(), { once: true });
  socket.addEventListener("error", reject, { once: true });
});
const closed = (socket: WebSocket) => new Promise<number>((resolve) => {
  socket.addEventListener("close", (event) => resolve(event.code), { once: true });
});

it.effect("captures commands sent on open and reconstructs fragmented UTF-8 and newline frames", () => {
  const fake = vendor();
  return Effect.scoped(Effect.gen(function* () {
    const service = yield* ApiBrowserChannel.ApiBrowserChannel;
    const channel = yield* service.open({ sandbox: { sandboxId: "owned" }, client: fake.client });
    yield* Effect.tryPromise(async () => {
      const socket = new WebSocket(channel.endpoint);
      const result = new Promise<string>((resolve) => socket.addEventListener("message", e => resolve(String(e.data)), { once: true }));
      socket.addEventListener("open", () => socket.send('{\n  "id": 1,\n  "method": "Browser.getVersion"\n}'), { once: true });
      expect(JSON.parse(await result)).toEqual({ id: 1, result: "héllo" });
      expect(fake.commands).toEqual(['{"id":1,"method":"Browser.getVersion"}\n']);
      const end = closed(socket); socket.close(); await end;
    });
  })).pipe(Effect.provide(ApiBrowserChannel.layer));
});

it.effect.each(["error", "evicted", "gap", "finished"] as const)("closes the channel on vendor %s", (mode) => {
  const fake = vendor(mode);
  return Effect.scoped(Effect.gen(function* () {
    const channel = yield* (yield* ApiBrowserChannel.ApiBrowserChannel).open({ sandbox: { sandboxId: "owned" }, client: fake.client });
    yield* Effect.tryPromise(async () => {
      const socket = new WebSocket(channel.endpoint);
      const end = closed(socket);
      socket.addEventListener("open", () => socket.send('{"id":1}'), { once: true });
      expect(await end).toBe(1011);
    });
  })).pipe(Effect.provide(ApiBrowserChannel.layer));
});

it.effect("rejects a concurrent controller and an oversized message without another vendor session", () => {
  const fake = vendor();
  return Effect.scoped(Effect.gen(function* () {
    const channel = yield* (yield* ApiBrowserChannel.ApiBrowserChannel).open({ sandbox: { sandboxId: "owned" }, client: fake.client });
    yield* Effect.tryPromise(async () => {
      const first = new WebSocket(channel.endpoint); await opened(first);
      const second = new WebSocket(channel.endpoint); const secondEnd = closed(second);
      expect(await secondEnd).toBe(1008);
      expect(fake.requests).toBe(1);
      const end = closed(first); first.send("x".repeat(8 * 1024 * 1024 + 1));
      expect([1009, 1011]).toContain(await end);
    });
  })).pipe(Effect.provide(ApiBrowserChannel.layer));
});

it.effect("scope cleanup closes the endpoint without stopping its workspace", () => {
  const fake = vendor();
  return Effect.gen(function* () {
    const scope = yield* Scope.make();
    const channel = yield* Effect.gen(function* () {
      return yield* (yield* ApiBrowserChannel.ApiBrowserChannel).open({ sandbox: { sandboxId: "owned" }, client: fake.client });
    }).pipe(Effect.provide(ApiBrowserChannel.layer), Scope.provide(scope));
    const socket = new WebSocket(channel.endpoint);
    yield* Effect.tryPromise(() => opened(socket));
    const end = closed(socket);
    yield* Scope.close(scope, Exit.void);
    expect([1000, 1005]).toContain(yield* Effect.tryPromise(() => end));
  });
});
