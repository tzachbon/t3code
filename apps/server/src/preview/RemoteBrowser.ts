// @effect-diagnostics nodeBuiltinImport:off -- Connects Playwright to the application-owned CDP channel.
import { PROVIDER_SEND_TURN_MAX_FILE_BYTES, type PreviewWorkspaceStatus } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as NodeModule from "node:module";
import type { Browser, Page, CDPSession } from "playwright-core";

import * as ApiBrowserChannel from "../sandbox/ApiBrowserChannel.ts";
import * as BrowserRuntime from "../sandbox/browserRuntime.ts";
import * as Workspaces from "../sandbox/Workspaces.ts";
import { readRemoteFile } from "./RemoteBrowserFiles.ts";

const isWorkspaceError = Schema.is(Workspaces.WorkspaceError);
const requirePlaywright = NodeModule.createRequire(import.meta.url);
const publicStatus = (status: Workspaces.WorkspaceStatus): PreviewWorkspaceStatus => ({
  state: status.state,
  generation: status.generation,
  expiresAt: status.expiresAt,
  idleExpiresAt: status.idleExpiresAt,
  absoluteExpiresAt: status.absoluteExpiresAt,
  accruedMicroUsd: status.accruedMicroUsd,
  reservedMicroUsd: status.reservedMicroUsd,
  remainingMicroUsd: status.remainingMicroUsd,
  reason: status.reason,
});

interface RemoteDownload {
  readonly url: string;
  readonly frameId: string;
  readonly done: Deferred.Deferred<void, Workspaces.WorkspaceError>;
  claimed: boolean;
}
interface Connection {
  readonly generation: number;
  readonly browser: Browser;
  readonly scope: Scope.Closeable;
  readonly downloads: Map<string, RemoteDownload>;
  readonly downloadSession: CDPSession;
  downloadStarted: Deferred.Deferred<void>;
}
const DOWNLOAD_DIRECTORY = `${BrowserRuntime.RUNTIME_DIRECTORY}/downloads`;

export class RemoteBrowser extends Context.Service<
  RemoteBrowser,
  {
    readonly enabled: boolean;
    readonly page: (
      threadId: string,
      tabId: string,
      adopted?: Page,
    ) => Effect.Effect<
      {
        readonly page: Page;
        readonly reused: boolean;
        readonly generation: number;
      },
      Workspaces.WorkspaceError
    >;
    readonly scratchPage: (
      threadId: string,
      tabId: string,
      generation: number,
    ) => Effect.Effect<Page, Workspaces.WorkspaceError>;
    readonly readFiles: (
      threadId: string,
      generation: number,
      paths: readonly string[],
    ) => Effect.Effect<
      readonly { readonly name: string; readonly mimeType: string; readonly buffer: Buffer }[],
      Workspaces.WorkspaceError
    >;
    readonly receiveDownload: (
      threadId: string,
      generation: number,
      page: Page,
      url: string,
    ) => Effect.Effect<Buffer, Workspaces.WorkspaceError>;
    readonly status: (
      threadId: string,
    ) => Effect.Effect<PreviewWorkspaceStatus | undefined, Workspaces.WorkspaceError>;
    readonly assertRunning: (
      threadId: string,
      generation?: number,
    ) => Effect.Effect<void, Workspaces.WorkspaceError>;
    readonly touch: (
      threadId: string,
      generation?: number,
    ) => Effect.Effect<void, Workspaces.WorkspaceError>;
    readonly stop: (
      threadId: string,
    ) => Effect.Effect<PreviewWorkspaceStatus | undefined, Workspaces.WorkspaceError>;
    readonly recreate: (
      threadId: string,
      requestId: string,
    ) => Effect.Effect<PreviewWorkspaceStatus, Workspaces.WorkspaceError>;
  }
>()("t3/preview/RemoteBrowser") {}

const make = Effect.gen(function* () {
  const workspaces = yield* Workspaces.Workspaces;
  const depot = yield* Workspaces.DepotSandbox;
  const channel = yield* ApiBrowserChannel.ApiBrowserChannel;
  const serviceScope = yield* Effect.scope;
  const connections = new Map<string, Connection>();
  const touchedAt = new Map<string, number>();
  const pendingTouches = new Map<string, { readonly generation: number }>();
  const targets = new Map<string, { generation: number; id: string }>();
  const locks = new Map<string, Semaphore.Semaphore>();
  const locked = <A, E, R>(threadId: string, operation: Effect.Effect<A, E, R>) => {
    let lock = locks.get(threadId);
    if (!lock) {
      lock = Semaphore.makeUnsafe(1);
      locks.set(threadId, lock);
    }
    return lock.withPermit(operation);
  };
  const vendor = <A>(run: () => Promise<A>) =>
    Effect.tryPromise({
      try: run,
      catch: (cause) => new Workspaces.WorkspaceError({ reason: "provider", cause }),
    });
  const disconnect = (threadId: string) =>
    Effect.gen(function* () {
      pendingTouches.delete(threadId);
      touchedAt.delete(threadId);
      const connection = connections.get(threadId);
      if (!connection) return;
      connections.delete(threadId);
      yield* Scope.close(connection.scope, Exit.void);
    });
  const status = (threadId: string) =>
    workspaces
      .status(threadId)
      .pipe(Effect.map((value) => (value ? publicStatus(value) : undefined)));
  const assertRunning = (threadId: string, generation?: number) =>
    workspaces.assertRunning(threadId, generation);
  const renew = (threadId: string, generation: number) =>
    Effect.gen(function* () {
      yield* assertRunning(threadId, generation);
      const touched = yield* workspaces.touch(threadId);
      if (touched.state !== "running" || touched.generation !== generation)
        return yield* new Workspaces.WorkspaceError({ reason: "not-running" });
      touchedAt.set(threadId, yield* Clock.currentTimeMillis);
    });
  const page: RemoteBrowser["Service"]["page"] = (threadId, tabId, adopted) =>
    locked(
      threadId,
      Effect.gen(function* () {
        let workspace = yield* workspaces.status(threadId);
        // The first tab starts lazily. Terminal generations need an explicit Recreate action.
        if (!workspace) workspace = yield* workspaces.start(threadId, `browser:${tabId}`);
        yield* assertRunning(threadId, workspace.generation);
        let connection = connections.get(threadId);
        if (
          connection &&
          (connection.generation !== workspace.generation || !connection.browser.isConnected())
        ) {
          yield* disconnect(threadId);
          connection = undefined;
        }
        if (!connection) {
          if (!depot.client) return yield* new Workspaces.WorkspaceError({ reason: "disabled" });
          const sandbox = yield* workspaces.sandbox(threadId);
          yield* BrowserRuntime.ensure(sandbox).pipe(
            Effect.mapError(
              (cause) => new Workspaces.WorkspaceError({ reason: "provider", cause }),
            ),
          );
          yield* assertRunning(threadId, workspace.generation);
          const scope = yield* Scope.make();
          const connected = yield* Effect.gen(function* () {
            const { endpoint } = yield* channel
              .open({ sandbox, client: depot.client! })
              .pipe(Scope.provide(scope));
            const browser = yield* vendor(() =>
              (
                requirePlaywright("playwright-core") as typeof import("playwright-core")
              ).chromium.connectOverCDP(endpoint, { timeout: 15_000 }),
            );
            yield* Effect.addFinalizer(() =>
              vendor(() => browser.close()).pipe(Effect.ignore),
            ).pipe(Scope.provide(scope));
            const downloadSession = yield* vendor(() => browser.newBrowserCDPSession());
            const connected: Connection = {
              generation: workspace.generation,
              browser,
              scope,
              downloadSession,
              downloads: new Map(),
              downloadStarted: Deferred.makeUnsafe<void>(),
            };
            downloadSession.on("Browser.downloadWillBegin", (event) => {
              if (connected.downloads.size >= 100) {
                void downloadSession
                  .send("Browser.cancelDownload", { guid: event.guid })
                  .catch(() => {});
                return;
              }
              connected.downloads.set(event.guid, {
                url: event.url,
                frameId: event.frameId,
                done: Deferred.makeUnsafe<void, Workspaces.WorkspaceError>(),
                claimed: false,
              });
              const notification = connected.downloadStarted;
              connected.downloadStarted = Deferred.makeUnsafe<void>();
              Deferred.doneUnsafe(notification, Effect.void);
            });
            downloadSession.on("Browser.downloadProgress", (event) => {
              const download = connected.downloads.get(event.guid);
              if (!download) return;
              if (
                event.receivedBytes > PROVIDER_SEND_TURN_MAX_FILE_BYTES ||
                event.totalBytes > PROVIDER_SEND_TURN_MAX_FILE_BYTES
              ) {
                void downloadSession
                  .send("Browser.cancelDownload", { guid: event.guid })
                  .catch(() => {});
                Deferred.doneUnsafe(
                  download.done,
                  Effect.fail(new Workspaces.WorkspaceError({ reason: "invalid-input" })),
                );
              } else if (event.state === "completed")
                Deferred.doneUnsafe(download.done, Effect.void);
              else if (event.state === "canceled")
                Deferred.doneUnsafe(
                  download.done,
                  Effect.fail(new Workspaces.WorkspaceError({ reason: "provider" })),
                );
            });
            yield* vendor(() =>
              downloadSession.send("Browser.setDownloadBehavior", {
                behavior: "allowAndName",
                downloadPath: DOWNLOAD_DIRECTORY,
                eventsEnabled: true,
              }),
            );
            return connected;
          }).pipe(
            Effect.onError(() => Scope.close(scope, Exit.void)),
            Effect.mapError(
              (cause) => new Workspaces.WorkspaceError({ reason: "provider", cause }),
            ),
          );
          connections.set(threadId, connected);
          connection = connected;
        }
        yield* assertRunning(threadId, workspace.generation);
        const key = `${threadId}\u0000${tabId}`;
        const target = targets.get(key);
        const pages = connection.browser.contexts().flatMap((context) => context.pages());
        if (adopted && !connection.browser.contexts().includes(adopted.context()))
          return yield* new Workspaces.WorkspaceError({ reason: "invalid-input" });
        let selected = adopted;
        let reused = adopted !== undefined;
        const targetId = (page: Page) =>
          vendor(async () => {
            const session = await page.context().newCDPSession(page);
            try {
              return (await session.send("Target.getTargetInfo")).targetInfo.targetId;
            } finally {
              await session.detach();
            }
          });
        if (!selected && target?.generation === workspace.generation) {
          for (const candidate of pages) {
            if (yield* targetId(candidate).pipe(Effect.map((id) => id === target.id))) {
              selected = candidate;
              reused = true;
              break;
            }
          }
          // Losing an existing target must not silently create a blank replacement.
          if (!selected) return yield* new Workspaces.WorkspaceError({ reason: "not-running" });
        }
        if (!selected) {
          const context = connection.browser.contexts()[0];
          if (!context) return yield* new Workspaces.WorkspaceError({ reason: "provider" });
          selected = yield* vendor(() => context.newPage());
        }
        targets.set(key, { generation: workspace.generation, id: yield* targetId(selected) });
        return { page: selected, reused, generation: workspace.generation };
      }),
    );
  const scratchPage: RemoteBrowser["Service"]["scratchPage"] = (threadId, tabId, generation) =>
    Effect.gen(function* () {
      yield* assertRunning(threadId, generation);
      const tab = yield* page(threadId, tabId);
      if (tab.generation !== generation)
        return yield* new Workspaces.WorkspaceError({ reason: "not-running" });
      return yield* vendor(async () => {
        const browser = tab.page.context().browser();
        if (!browser) throw new Error("Missing remote browser.");
        const context = await browser.newContext();
        try {
          const scratch = await context.newPage();
          scratch.once("close", () => {
            void context.close().catch(() => {});
          });
          return scratch;
        } catch (cause) {
          await context.close().catch(() => {});
          throw cause;
        }
      });
    });
  const readFiles: RemoteBrowser["Service"]["readFiles"] = (threadId, generation, paths) =>
    Effect.gen(function* () {
      yield* assertRunning(threadId, generation);
      if (!depot.client || paths.length > 20)
        return yield* new Workspaces.WorkspaceError({ reason: "invalid-input" });
      if (paths.length === 0) return [];
      const workspace = yield* workspaces.status(threadId);
      if (workspace?.generation !== generation)
        return yield* new Workspaces.WorkspaceError({ reason: "not-running" });
      if (!workspace?.sourceSnapshot || !/^[a-f0-9]{64}$/.test(workspace.sourceSnapshot))
        return yield* new Workspaces.WorkspaceError({ reason: "invalid-input" });
      const sandbox = yield* workspaces.sandbox(threadId);
      let remaining = PROVIDER_SEND_TURN_MAX_FILE_BYTES;
      const files = [];
      for (const path of paths) {
        if (!path.startsWith("/tmp/t3/workspace/"))
          return yield* new Workspaces.WorkspaceError({ reason: "invalid-input" });
        const relative = path.slice("/tmp/t3/workspace/".length);
        const buffer = yield* vendor(() =>
          readRemoteFile({
            sandbox,
            client: depot.client!,
            root: `/tmp/t3/runtime/workspaces/${workspace.sourceSnapshot}`,
            relative,
            maxBytes: remaining,
          }),
        );
        remaining -= buffer.byteLength;
        files.push({
          name: relative.split("/").at(-1)!,
          mimeType: "application/octet-stream",
          buffer,
        });
      }
      return files;
    });
  const receiveDownload: RemoteBrowser["Service"]["receiveDownload"] = (
    threadId,
    generation,
    sourcePage,
    url,
  ) =>
    Effect.gen(function* () {
      yield* assertRunning(threadId, generation);
      const connection = connections.get(threadId);
      if (
        !connection ||
        connection.generation !== generation ||
        !depot.client ||
        !connection.browser.contexts().includes(sourcePage.context())
      )
        return yield* new Workspaces.WorkspaceError({ reason: "not-running" });
      const sandbox = yield* workspaces.sandbox(threadId);
      const frameIds = yield* vendor(async () => {
        const session = await sourcePage.context().newCDPSession(sourcePage);
        try {
          const { frameTree } = await session.send("Page.getFrameTree");
          const pending = [frameTree];
          const ids = new Set<string>();
          while (pending.length) {
            const frame = pending.pop()!;
            ids.add(frame.frame.id);
            pending.push(...(frame.childFrames ?? []));
          }
          return ids;
        } finally {
          await session.detach();
        }
      });
      let selected: [string, RemoteDownload] | undefined;
      while (!selected) {
        selected = [...connection.downloads].find(
          ([, download]) =>
            !download.claimed && frameIds.has(download.frameId) && download.url === url,
        );
        if (!selected) yield* Deferred.await(connection.downloadStarted);
      }
      const [guid, download] = selected;
      download.claimed = true;
      if (!/^[a-f0-9-]{36}$/.test(guid)) {
        connection.downloads.delete(guid);
        return yield* new Workspaces.WorkspaceError({ reason: "invalid-input" });
      }
      return yield* Effect.gen(function* () {
        yield* Deferred.await(download.done);
        yield* assertRunning(threadId, generation);
        return yield* vendor(() =>
          readRemoteFile({
            sandbox,
            client: depot.client!,
            root: DOWNLOAD_DIRECTORY,
            relative: guid,
            maxBytes: PROVIDER_SEND_TURN_MAX_FILE_BYTES,
          }),
        );
      }).pipe(
        Effect.onExit((exit) =>
          vendor(async () => {
            await Promise.allSettled([
              ...(Exit.isFailure(exit)
                ? [connection.downloadSession.send("Browser.cancelDownload", { guid })]
                : []),
              sandbox.fs().unlink(`${DOWNLOAD_DIRECTORY}/${guid}`),
            ]);
          }).pipe(
            Effect.timeout("5 seconds"),
            Effect.ignore,
            Effect.andThen(
              Effect.sync(() => {
                connection.downloads.delete(guid);
              }),
            ),
          ),
        ),
      );
    }).pipe(
      Effect.timeout("60 seconds"),
      Effect.mapError((cause) =>
        isWorkspaceError(cause)
          ? cause
          : new Workspaces.WorkspaceError({ reason: "provider", cause }),
      ),
    );
  yield* Effect.addFinalizer(() =>
    Effect.forEach([...connections.keys()], disconnect, { discard: true }),
  );
  return RemoteBrowser.of({
    enabled: workspaces.enabled,
    page,
    scratchPage,
    readFiles,
    receiveDownload,
    status,
    assertRunning,
    touch: (threadId, generation) =>
      locked(
        threadId,
        Effect.gen(function* () {
          const currentGeneration =
            generation ??
            connections.get(threadId)?.generation ??
            (yield* workspaces.status(threadId))?.generation;
          if (currentGeneration === undefined)
            return yield* new Workspaces.WorkspaceError({ reason: "not-running" });
          yield* assertRunning(threadId, currentGeneration);
          const now = yield* Clock.currentTimeMillis;
          const elapsed = now - (touchedAt.get(threadId) ?? -Infinity);
          // ponytail: Real activity flushes within 30s; pass action timestamps only if tighter idle precision is needed.
          if (elapsed < 30_000) {
            if (!pendingTouches.has(threadId)) {
              const pending = { generation: currentGeneration };
              pendingTouches.set(threadId, pending);
              yield* Effect.sleep(30_000 - elapsed).pipe(
                Effect.andThen(
                  locked(
                    threadId,
                    Effect.gen(function* () {
                      if (pendingTouches.get(threadId) !== pending) return;
                      pendingTouches.delete(threadId);
                      yield* renew(threadId, pending.generation);
                    }),
                  ),
                ),
                Effect.ignore,
                Effect.forkIn(serviceScope),
              );
            }
            return;
          }
          pendingTouches.delete(threadId);
          yield* renew(threadId, currentGeneration);
        }),
      ),
    stop: (threadId) =>
      locked(
        threadId,
        Effect.gen(function* () {
          pendingTouches.delete(threadId);
          const stopped = yield* workspaces.stop(threadId, "user");
          yield* disconnect(threadId);
          return stopped ? publicStatus(stopped) : undefined;
        }),
      ),
    recreate: (threadId, requestId) =>
      locked(
        threadId,
        Effect.gen(function* () {
          if (!requestId.trim())
            return yield* new Workspaces.WorkspaceError({ reason: "invalid-input" });
          yield* disconnect(threadId);
          return publicStatus(yield* workspaces.start(threadId, requestId));
        }),
      ),
  });
});
export const layer = Layer.effect(RemoteBrowser, make);
