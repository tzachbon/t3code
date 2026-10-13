import * as NodeEvents from "node:events";
import type { Sandbox, SandboxClient } from "@depot/sandbox";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import type { Browser, BrowserContext, Page, CDPSession } from "playwright-core";
import { chromium } from "playwright-core";
import { beforeEach, expect, vi } from "vite-plus/test";

import * as ApiBrowserChannel from "../sandbox/ApiBrowserChannel.ts";
import * as Workspaces from "../sandbox/Workspaces.ts";
import * as RemoteBrowser from "./RemoteBrowser.ts";
import { readRemoteFile } from "./RemoteBrowserFiles.ts";

vi.mock("./RemoteBrowserFiles.ts", () => ({
  readRemoteFile: vi.fn(async () => Buffer.from([0, 255, 10])),
}));
vi.mock("../sandbox/browserRuntime.ts", () => ({
  RUNTIME_DIRECTORY: "/tmp/t3-managed-browser",
  ensure: () => Effect.void,
}));
const connect = vi.spyOn(chromium, "connectOverCDP");
const launch = vi.spyOn(chromium, "launch");
const statuses = new Map<string, Workspaces.WorkspaceStatus>();
const browsers: ReturnType<typeof browser>[] = [];
const starts: string[] = [];
const touches: string[] = [];
const closedChannels: string[] = [];
const unlink = vi.fn(async (_path: string) => {});
function status(threadId: string, generation = 1): Workspaces.WorkspaceStatus {
  return {
    threadId,
    sandboxId: `private-${threadId}`,
    state: "running",
    generation,
    expiresAt: 9_000_000_000_000,
    idleExpiresAt: 9_000_000_000_000,
    absoluteExpiresAt: 9_000_000_000_000,
    accruedMicroUsd: 100,
    reservedMicroUsd: 180_000,
    remainingMicroUsd: 99_819_900,
    reason: null,
    sourceSnapshot: null,
    remoteRoot: "/tmp/t3/workspace",
  };
}
function browser(pages: Page[] = []) {
  const events = new NodeEvents.EventEmitter();
  let connected = true;
  const root = new NodeEvents.EventEmitter();
  const rootSession = {
    on: (event: string, callback: (...args: unknown[]) => void) => root.on(event, callback),
    send: vi.fn(async () => ({})),
  };
  const context = {
    pages: () => pages,
    browser: () => connectedBrowser as unknown as Browser,
    newPage: vi.fn(async () => {
      const page = makePage(`target-${pages.length}`, context);
      pages.push(page);
      return page;
    }),
    newCDPSession: async (page: Page) => ({
      send: async () => ({
        targetInfo: { targetId: page.url() },
        frameTree: { frame: { id: `frame:${page.url()}` } },
      }),
      detach: async () => {},
    }),
    close: vi.fn(async () => {}),
  };
  const connectedBrowser = {
    context,
    root,
    rootSession,
    newBrowserCDPSession: async () => rootSession as unknown as CDPSession,
    contexts: () => [context],
    isConnected: () => connected,
    close: vi.fn(async () => {
      connected = false;
      events.emit("disconnected");
    }),
    on: (event: string, callback: () => void) => events.on(event, callback),
    disconnect: () => {
      connected = false;
      events.emit("disconnected");
    },
    newContext: vi.fn(async () => browser().context as unknown as BrowserContext),
  };
  return connectedBrowser;
}
function makePage(id: string, context: ReturnType<typeof browser>["context"]): Page {
  return {
    once: vi.fn(),
    url: () => id,
    context: () => context as unknown as BrowserContext,
    close: vi.fn(async () => {}),
  } as unknown as Page;
}
const dependencies = Layer.mergeAll(
  Layer.succeed(Workspaces.Workspaces, {
    enabled: true,
    status: (id) => Effect.sync(() => statuses.get(id) ?? null),
    assertRunning: (id, generation) =>
      Effect.suspend(() => {
        const value = statuses.get(id);
        return value?.state === "running" &&
          (generation === undefined || generation === value.generation)
          ? Effect.void
          : Effect.fail(new Workspaces.WorkspaceError({ reason: "not-running" }));
      }),
    start: (id, requestId) =>
      Effect.sync(() => {
        starts.push(requestId);
        const previous = statuses.get(id);
        const next =
          previous?.state === "running" ? previous : status(id, (previous?.generation ?? 0) + 1);
        statuses.set(id, next);
        return next;
      }),
    sandbox: (id) =>
      Effect.succeed({ sandboxId: `private-${id}`, fs: () => ({ unlink }) } as unknown as Sandbox),
    touch: (id) =>
      Effect.sync(() => {
        touches.push(id);
        return statuses.get(id)!;
      }),
    stop: (id) =>
      Effect.sync(() => {
        const previous = statuses.get(id);
        if (!previous) return null;
        const next = { ...previous, state: "stopped" as const };
        statuses.set(id, next);
        return next;
      }),
    registerJob: () => Effect.die("unused"),
    finishJob: () => Effect.die("unused"),
    job: () => Effect.die("unused"),
    setSourceSnapshot: () => Effect.die("unused"),
    reconcile: Effect.void,
  }),
  Layer.succeed(Workspaces.DepotSandbox, {
    enabled: true,
    client: {} as SandboxClient,
    create: () => Effect.die("unused"),
    list: Effect.die("unused"),
    get: () => Effect.die("unused"),
    renew: () => Effect.die("unused"),
    kill: () => Effect.die("unused"),
  }),
  Layer.succeed(ApiBrowserChannel.ApiBrowserChannel, {
    open: ({ sandbox }) =>
      Effect.acquireRelease(Effect.succeed({ endpoint: `ws://private/${sandbox.sandboxId}` }), () =>
        Effect.sync(() => {
          closedChannels.push(sandbox.sandboxId);
        }),
      ),
  }),
);
const layer = RemoteBrowser.layer.pipe(Layer.provide(dependencies));
beforeEach(() => {
  statuses.clear();
  starts.length = 0;
  touches.length = 0;
  closedChannels.length = 0;
  browsers.length = 0;
  connect.mockReset();
  launch.mockClear();
  unlink.mockClear();
  vi.mocked(readRemoteFile).mockClear();
  connect.mockImplementation(async () => {
    const connected = browser();
    browsers.push(connected);
    return connected as unknown as Browser;
  });
});

it.effect(
  "starts lazily per thread, keeps tabs in that thread, and exposes no private connection metadata",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* RemoteBrowser.RemoteBrowser;
        expect(yield* remote.status("a")).toBeUndefined();
        expect(starts).toEqual([]);
        const a = yield* remote.page("a", "one");
        const again = yield* remote.page("a", "one");
        const b = yield* remote.page("b", "one");
        expect(a.page).toBe(again.page);
        expect(a.page).not.toBe(b.page);
        expect(starts).toEqual(["browser:one", "browser:one"]);
        expect(yield* remote.status("a")).toEqual({
          state: "running",
          generation: 1,
          expiresAt: 9_000_000_000_000,
          idleExpiresAt: 9_000_000_000_000,
          absoluteExpiresAt: 9_000_000_000_000,
          accruedMicroUsd: 100,
          reservedMicroUsd: 180_000,
          remainingMicroUsd: 99_819_900,
          reason: null,
        });
        expect(touches).toEqual([]);
        expect(launch).not.toHaveBeenCalled();
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect(
  "reattaches surviving targets and adopted popups after disconnect without closing pages",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* RemoteBrowser.RemoteBrowser;
        const first = yield* remote.page("a", "one");
        const previous = browsers[0]!;
        const popup = makePage("popup-target", previous.context);
        previous.context.pages().push(popup);
        yield* remote.page("a", "popup", popup);
        previous.disconnect();
        const reconnected = browser(previous.context.pages());
        connect.mockResolvedValueOnce(reconnected as unknown as Browser);
        expect((yield* remote.page("a", "one")).page).toBe(first.page);
        expect((yield* remote.page("a", "popup")).page).toBe(popup);
        expect(first.page.close).not.toHaveBeenCalled();
        expect(popup.close).not.toHaveBeenCalled();
        expect(reconnected.context.newPage).not.toHaveBeenCalled();
        expect(starts).toHaveLength(1);
        expect(closedChannels).toEqual(["private-a"]);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect(
  "terminal generations stay stopped until explicit recreation, and repeated stop is safe",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* RemoteBrowser.RemoteBrowser;
        yield* remote.page("a", "one");
        yield* remote.stop("a");
        yield* remote.stop("a");
        expect(browsers[0]!.close).toHaveBeenCalledTimes(1);
        const denied = yield* remote.page("a", "two").pipe(Effect.flip);
        expect(denied.reason).toBe("not-running");
        expect(starts).toHaveLength(1);
        expect((yield* remote.recreate("a", "explicit-id")).generation).toBe(2);
        expect((yield* remote.page("a", "one")).generation).toBe(2);
        expect(starts).toEqual(["browser:one", "explicit-id"]);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("a failed remote connection never launches a local browser", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      connect.mockRejectedValueOnce(new Error("transport failed"));
      expect((yield* remote.page("a", "one").pipe(Effect.flip)).reason).toBe("provider");
      expect(launch).not.toHaveBeenCalled();
      expect(closedChannels).toEqual(["private-a"]);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("rejects adoption from a different conversation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      const foreign = yield* remote.page("b", "one");
      expect((yield* remote.page("a", "popup", foreign.page).pipe(Effect.flip)).reason).toBe(
        "invalid-input",
      );
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("checks stopped leases before every action while coalescing renewal RPCs", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      yield* remote.page("a", "one");
      yield* remote.touch("a");
      yield* remote.touch("a");
      expect(touches).toEqual(["a"]);
      statuses.set("a", { ...statuses.get("a")!, state: "stop-pending" });
      expect((yield* remote.touch("a").pipe(Effect.flip)).reason).toBe("not-running");
      expect(touches).toEqual(["a"]);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("flushes coalesced real activity once and never renews periodically while idle", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      yield* remote.page("a", "one");
      yield* remote.touch("a", 1);
      yield* TestClock.adjust("20 seconds");
      yield* remote.touch("a", 1);
      yield* remote.touch("a", 1);
      yield* TestClock.adjust("9 seconds");
      yield* remote.status("a");
      expect(touches).toEqual(["a"]);
      yield* TestClock.adjust("1 second");
      expect(touches).toEqual(["a", "a"]);
      yield* TestClock.adjust("5 minutes");
      yield* remote.status("a");
      expect(touches).toEqual(["a", "a"]);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("pending activity cannot renew a stopped or replaced generation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      for (const threadId of ["stopped", "replaced"]) {
        yield* remote.page(threadId, "one");
        yield* remote.touch(threadId, 1);
        yield* remote.touch(threadId, 1);
      }
      statuses.set("stopped", { ...statuses.get("stopped")!, state: "stop-pending" });
      statuses.set("replaced", status("replaced", 2));
      yield* TestClock.adjust("30 seconds");
      expect(touches).toEqual(["stopped", "replaced"]);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect(
  "stop and recreation discard pending activity and renew the new generation immediately",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* RemoteBrowser.RemoteBrowser;
        yield* remote.page("a", "one");
        yield* remote.touch("a", 1);
        yield* remote.touch("a", 1);
        yield* remote.stop("a");
        yield* remote.recreate("a", "new-generation");
        yield* remote.page("a", "one");
        yield* remote.touch("a", 2);
        yield* TestClock.adjust("1 minute");
        expect(touches).toEqual(["a", "a"]);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("records in an incognito context on the same remote browser", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      const tab = yield* remote.page("a", "one");
      const scratch = yield* remote.scratchPage("a", "one", 1);
      expect(scratch.context()).not.toBe(tab.page.context());
      expect(browsers[0]!.newContext).toHaveBeenCalledTimes(1);
      expect(launch).not.toHaveBeenCalled();
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("downloads correlate by source frame and GUID and transfer bounded remote bytes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      const tab = yield* remote.page("a", "one");
      const root = browsers[0]!;
      const guid = "12345678-1234-1234-1234-123456789abc";
      root.root.emit("Browser.downloadWillBegin", {
        guid,
        frameId: `frame:${tab.page.url()}`,
        url: "https://example.com/file.bin",
      });
      root.root.emit("Browser.downloadProgress", {
        guid,
        state: "completed",
        receivedBytes: 3,
        totalBytes: 3,
      });
      const bytes = yield* remote.receiveDownload("a", 1, tab.page, "https://example.com/file.bin");
      expect(bytes).toEqual(Buffer.from([0, 255, 10]));
      expect(readRemoteFile).toHaveBeenCalledWith(
        expect.objectContaining({
          root: "/tmp/t3-managed-browser/downloads",
          relative: guid,
          maxBytes: 50 * 1024 * 1024,
        }),
      );
      expect(root.rootSession.send).toHaveBeenCalledWith("Browser.setDownloadBehavior", {
        behavior: "allowAndName",
        downloadPath: "/tmp/t3-managed-browser/downloads",
        eventsEnabled: true,
      });
      expect(unlink).toHaveBeenCalledWith(`/tmp/t3-managed-browser/downloads/${guid}`);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("oversized downloads are canceled before file bytes enter the host", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      const tab = yield* remote.page("a", "one");
      const root = browsers[0]!;
      const guid = "12345678-1234-1234-1234-123456789abc";
      root.root.emit("Browser.downloadWillBegin", {
        guid,
        frameId: `frame:${tab.page.url()}`,
        url: "https://example.com/large.bin",
      });
      root.root.emit("Browser.downloadProgress", {
        guid,
        state: "inProgress",
        receivedBytes: 1,
        totalBytes: 50 * 1024 * 1024 + 1,
      });
      expect(
        (yield* remote
          .receiveDownload("a", 1, tab.page, "https://example.com/large.bin")
          .pipe(Effect.flip)).reason,
      ).toBe("invalid-input");
      expect(readRemoteFile).not.toHaveBeenCalled();
      expect(root.rootSession.send).toHaveBeenCalledWith("Browser.cancelDownload", { guid });
      expect(unlink).toHaveBeenCalledWith(`/tmp/t3-managed-browser/downloads/${guid}`);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("a timed-out transfer cancels Chrome and removes its constrained GUID file", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      const tab = yield* remote.page("a", "one");
      const root = browsers[0]!;
      const guid = "12345678-1234-1234-1234-123456789abc";
      root.root.emit("Browser.downloadWillBegin", {
        guid,
        frameId: `frame:${tab.page.url()}`,
        url: "https://example.com/slow.bin",
      });
      const transfer = yield* remote
        .receiveDownload("a", 1, tab.page, "https://example.com/slow.bin")
        .pipe(Effect.forkChild);
      yield* TestClock.adjust("60 seconds");
      expect((yield* Fiber.join(transfer).pipe(Effect.flip)).reason).toBe("provider");
      expect(root.rootSession.send).toHaveBeenCalledWith("Browser.cancelDownload", { guid });
      expect(unlink).toHaveBeenCalledWith(`/tmp/t3-managed-browser/downloads/${guid}`);
      expect(readRemoteFile).not.toHaveBeenCalled();
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("agent uploads only read the current source snapshot and never host paths", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* RemoteBrowser.RemoteBrowser;
      yield* remote.page("a", "one");
      statuses.set("a", { ...statuses.get("a")!, sourceSnapshot: "a".repeat(64) });
      const files = yield* remote.readFiles("a", 1, ["/tmp/t3/workspace/report.csv"]);
      expect(files).toEqual([
        {
          name: "report.csv",
          mimeType: "application/octet-stream",
          buffer: Buffer.from([0, 255, 10]),
        },
      ]);
      expect(readRemoteFile).toHaveBeenCalledWith(
        expect.objectContaining({
          root: `/tmp/t3/runtime/workspaces/${"a".repeat(64)}`,
          relative: "report.csv",
        }),
      );
      expect((yield* remote.readFiles("a", 1, ["/etc/passwd"]).pipe(Effect.flip)).reason).toBe(
        "invalid-input",
      );
    }),
  ).pipe(Effect.provide(layer)),
);
