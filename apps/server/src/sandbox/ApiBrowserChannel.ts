// @effect-diagnostics preferSchemaOverJson:off -- CDP is an opaque JSON transport; no application schema decodes these messages.
// @effect-diagnostics nodeBuiltinImport:off -- Adapts Depot's Promise streams to a scoped loopback CDP socket.
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer";
import type { Sandbox, SandboxClient } from "@depot/sandbox";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Socket from "effect/socket/Socket";
import * as NodeCrypto from "node:crypto";

import { RUNTIME_DIRECTORY } from "./browserRuntime.ts";

const MAX_BYTES = 8 * 1024 * 1024;

export class ApiBrowserChannelError extends Schema.TaggedError<ApiBrowserChannelError>()(
  "ApiBrowserChannelError",
  { stage: Schema.Literals(["listen", "input", "output", "vendor"]), cause: Schema.Defect() },
) {
  override get message() { return "The remote browser connection failed."; }
}

export class ApiBrowserChannel extends Context.Service<
  ApiBrowserChannel,
  {
    /** Runtime must be prepared first. The scope owns only this connection, never Chrome. */
    readonly open: (input: {
      readonly sandbox: Pick<Sandbox, "sandboxId">;
      readonly client: { readonly rpc: Pick<SandboxClient["rpc"], "runCommand" | "writeFile"> };
    }) => Effect.Effect<{ readonly endpoint: string }, ApiBrowserChannelError, Scope.Scope>;
  }
>()("t3/sandbox/ApiBrowserChannel") {}

/** AsyncIterable accepted by the public SDK; closes without replaying pending mutations. */
class InputQueue implements AsyncIterable<Uint8Array> {
  private values: Uint8Array[] = [];
  private bytes = 0;
  private wake: (() => void) | undefined;
  private ended = false;

  offer(message: string) {
    const data = new TextEncoder().encode(`${JSON.stringify(JSON.parse(message))}\n`);
    if (this.ended || this.bytes + data.byteLength > MAX_BYTES || this.values.length >= 256) {
      throw new Error("Remote browser input limit exceeded.");
    }
    this.values.push(data);
    this.bytes += data.byteLength;
    this.wake?.();
    this.wake = undefined;
  }

  close() {
    if (this.ended) return;
    this.ended = true;
    this.values = [new TextEncoder().encode('{"__close":true}\n')];
    this.bytes = this.values[0]!.byteLength;
    this.wake?.();
    this.wake = undefined;
  }

  async *[Symbol.asyncIterator]() {
    for (;;) {
      const value = this.values.shift();
      if (value) {
        this.bytes -= value.byteLength;
        yield value;
      } else if (this.ended) return;
      else await new Promise<void>((resolve) => { this.wake = resolve; });
    }
  }
}

const make = Effect.sync(() => ApiBrowserChannel.of({
  open: Effect.fn("ApiBrowserChannel.open")(function* ({ sandbox, client }) {
    const secret = NodeCrypto.randomBytes(24).toString("base64url");
    const server = yield* NodeSocketServer.makeWebSocket({
      host: "127.0.0.1", port: 0, path: `/${secret}`, maxPayload: MAX_BYTES,
    }).pipe(Effect.mapError((cause) => new ApiBrowserChannelError({ stage: "listen", cause })));
    let connected = false;
    yield* server.run((socket) => Effect.gen(function* () {
      // NodeSocketServer pauses the accepted socket; acquire its reader before any vendor I/O.
      const reader = yield* socket.reader;
      const writer = yield* socket.writer;
      const webSocket = yield* Effect.serviceOption(Socket.WebSocket);
      if (Option.isNone(webSocket)) return yield* new ApiBrowserChannelError({ stage: "listen", cause: new Error("Missing controller socket.") });
      if (connected) {
        yield* writer.write(new Socket.CloseEvent(1008, "This connection already has a controller."));
        return;
      }
      connected = true;
      const input = new InputQueue();
      const abort = new AbortController();
      const ready = yield* Deferred.make<void>();
      const directory = `${RUNTIME_DIRECTORY}/channels/${NodeCrypto.randomUUID()}`;
      const ref = { selector: { case: "id" as const, value: sandbox.sandboxId } };
      let writing: Promise<unknown> | undefined;
      yield* Effect.addFinalizer(() => Effect.gen(function* () {
        input.close();
        if (writing) yield* Effect.tryPromise(() => writing!).pipe(Effect.timeoutOption("1 second"), Effect.ignore);
        abort.abort();
        connected = false;
      }));
      const receiveInput = Effect.forever(reader.pull.pipe(Effect.flatMap((frames) => Effect.try({
        try: () => {
          for (const frame of frames) input.offer(typeof frame === "string" ? frame : new TextDecoder("utf8", { fatal: true }).decode(frame));
        },
        catch: (cause) => new ApiBrowserChannelError({ stage: "input", cause }),
      }))));
      let offset = 0n;
      let pending = "";
      let stderr = "";
      const decoder = new TextDecoder("utf8", { fatal: true });
      const output = client.rpc.runCommand({
        sandbox: ref, cmd: "/usr/bin/timeout",
        args: ["86400", "/usr/local/bin/node", `${RUNTIME_DIRECTORY}/helper.mjs`, directory],
      }, { signal: abort.signal })[Symbol.asyncIterator]();
      // Abort is owned by our finalizer. Awaiting iterator.return before abort can deadlock pending next().
      const receiveOutput = Stream.fromAsyncIterable({
        [Symbol.asyncIterator]: () => ({ next: () => output.next() }),
      }, (cause) => new ApiBrowserChannelError({ stage: "vendor", cause })).pipe(
        Stream.runForEach((message) => Effect.gen(function* () {
          const event = message.event;
          if (event.case === "stderr") {
            stderr += new TextDecoder().decode(event.value.data);
            if (stderr.length > 16_384) return yield* new ApiBrowserChannelError({ stage: "output", cause: new Error("Helper diagnostics limit.") });
            if (stderr.includes("READY\n")) yield* Deferred.succeed(ready, undefined);
            return;
          }
          if (event.case === "error" || event.case === "evicted" || event.case === "finished") {
            return yield* new ApiBrowserChannelError({ stage: "output", cause: new Error("Remote browser output ended or was lost.") });
          }
          if (event.case !== "stdout") return;
          const lines = yield* Effect.try({
            try: () => {
              offset += BigInt(event.value.data.byteLength);
              if (event.value.byteOffset !== offset) throw new Error("Remote browser output gap.");
              pending += decoder.decode(event.value.data, { stream: true });
              if (Buffer.byteLength(pending) > MAX_BYTES) throw new Error("Remote browser output limit.");
              const lines = pending.split("\n");
              pending = lines.pop()!;
              for (const line of lines) JSON.parse(line);
              return lines;
            },
            catch: (cause) => new ApiBrowserChannelError({ stage: "output", cause }),
          });
          for (const line of lines) {
            const buffered = Reflect.get(webSocket.value, "bufferedAmount");
            if (typeof buffered !== "number" || buffered + Buffer.byteLength(line) > MAX_BYTES) {
              return yield* new ApiBrowserChannelError({ stage: "output", cause: new Error("Browser controller output limit.") });
            }
            yield* writer.write(line);
          }
        })),
        Effect.andThen(Effect.fail(new ApiBrowserChannelError({ stage: "output", cause: new Error("Remote browser stream ended.") }))),
      );
      const sendInput = Effect.gen(function* () {
        yield* Deferred.await(ready).pipe(Effect.timeout("15 seconds"));
        async function* messages(): Parameters<typeof client.rpc.writeFile>[0] {
          yield { input: { case: "init", value: { sandbox: ref, path: `${directory}/input`, append: true } } };
          for await (const data of input) yield { input: { case: "data", value: data } };
        }
        yield* Effect.tryPromise({
          try: () => { writing = client.rpc.writeFile(messages(), { signal: abort.signal }); return writing; },
          catch: (cause) => new ApiBrowserChannelError({ stage: "vendor", cause }),
        });
        return yield* new ApiBrowserChannelError({ stage: "vendor", cause: new Error("Remote browser input stream ended.") });
      });
      yield* Effect.raceFirst(receiveInput, Effect.raceFirst(receiveOutput, sendInput)).pipe(
        Effect.catchCause(() => writer.write(new Socket.CloseEvent(1011, "Remote browser connection ended.")).pipe(Effect.ignore)),
      );
    }).pipe(Effect.scoped, Effect.ignore)).pipe(Effect.forkScoped);
    const address = server.address;
    if (address._tag !== "InetAddressV4") return yield* new ApiBrowserChannelError({ stage: "listen", cause: new Error("Unexpected loopback address.") });
    return { endpoint: `ws://127.0.0.1:${address.port}/${secret}` };
  }),
}));

export const layer = Layer.effect(ApiBrowserChannel, make);
