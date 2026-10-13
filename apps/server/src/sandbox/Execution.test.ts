import { afterEach, expect, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture exercises the actual native filesystem and Python supervisor, isolated in temporary directories.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture exercises the actual native filesystem and Python supervisor, isolated in temporary directories.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture exercises the actual native filesystem and Python supervisor, isolated in temporary directories.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture exercises the actual native filesystem and Python supervisor, isolated in temporary directories.
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { StatResult, type Sandbox, type SandboxClient } from "@depot/sandbox";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type RuntimeMode,
  type ProviderInteractionMode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as Execution from "./Execution.ts";
import { readRemoteFile } from "../preview/RemoteBrowserFiles.ts";
import * as Workspaces from "./Workspaces.ts";
import * as SourceSnapshot from "./SourceSnapshot.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerConfig from "../config.ts";
import { liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";

const execute = NodeUtil.promisify(NodeChildProcess.execFile);
// @effect-diagnostics-next-line globalDate:off -- The native Python supervisor uses real wall time rather than TestClock.
const now = () => Date.now();
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(
    roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
});
const threadId = ThreadId.make("thread:execution");
const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("environment"),
  requestNamespace: "provider:execution",
  capabilities: new Set(["orchestration", "sandbox"]),
  thread: {
    threadId,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "session",
  },
  client: undefined,
  issuedAt: 0,
};
async function fixture() {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-execution-test-"));
  roots.push(directory);
  const local = NodePath.join(directory, "project");
  const remote = NodePath.join(directory, "remote");
  await NodeFSP.mkdir(local);
  await NodeFSP.mkdir(remote);
  await NodeFSP.mkdir(NodePath.join(remote, "jobs"));
  await NodeFSP.writeFile(NodePath.join(local, "app.txt"), "canonical");
  let runtimeMode: RuntimeMode = "full-access";
  let interactionMode: ProviderInteractionMode = "default";
  let loseLaunch = false;
  let losePublish = false;
  let needsCompletionActivity = false;
  let completionActivity = false;
  let sdkBlocked = false;
  let touches = 0;
  let launches = 0;
  const map = (value: string) =>
    value === "/tmp/t3/workspace"
      ? NodePath.join(directory, "workspace")
      : value.startsWith("/tmp/t3/runtime")
        ? value.replace("/tmp/t3/runtime", remote)
        : value;
  const sdk = {
    fs: () => ({
      mkdir: (p: string, options: object) => {
        // Depot's runner cannot create top-level directories.
        if (!p.startsWith("/tmp/")) throw new Error("Permission denied");
        return NodeFSP.mkdir(map(p), options);
      },
      mkdtemp: (p: string) => NodeFSP.mkdtemp(map(p)),
      writeFile: async (
        p: string,
        data: Uint8Array | string,
        options?: { recursive?: boolean },
      ) => {
        if (options?.recursive) await NodeFSP.mkdir(NodePath.dirname(map(p)), { recursive: true });
        await NodeFSP.writeFile(map(p), data);
      },
      readFile: (p: string) => NodeFSP.readFile(map(p)),
      rm: (p: string, options: object) => NodeFSP.rm(map(p), options),
      unlink: (p: string) => NodeFSP.unlink(map(p)),
      exists: (p: string) =>
        NodeFSP.access(map(p)).then(
          () => true,
          () => false,
        ),
      lstat: async (p: string) => {
        const stat = await NodeFSP.lstat(map(p));
        return new StatResult({
          path: p,
          size: stat.size,
          mode: stat.mode,
          uname: "test",
          gname: "test",
          mtime: stat.mtime,
          type: stat.isFile() ? "file" : stat.isSymbolicLink() ? "symlink" : "directory",
        });
      },
    }),
    runCommand: async ({ cmd, args }: { cmd: string; args: string[] }) => {
      if (sdkBlocked) throw new Error("workspace stopped before logs were read");
      // Depot exec does not resolve executable names through a shell.
      if (!cmd.startsWith("/")) throw new Error("Executable file not found");
      if (args[1]?.includes("launch.lock")) launches++;
      let output = "";
      let exitCode = 0;
      try {
        const localArgs = args.map((arg) =>
          arg.includes("t3-browser-file-")
            ? arg.replaceAll("/tmp/t3/runtime/workspaces", NodePath.join(remote, "workspaces"))
            : map(arg),
        );
        output = (await execute(cmd, localArgs, { maxBuffer: 1024 * 1024 })).stdout.replaceAll(
          remote,
          "/tmp/t3/runtime",
        );
      } catch (error) {
        exitCode = Number((error as { code: number }).code);
      }
      if (loseLaunch && args[1]?.includes("launch.lock")) {
        loseLaunch = false;
        throw new Error("lost launcher response");
      }
      if (losePublish && args[1]?.includes("workspaces=os.path.join")) {
        losePublish = false;
        throw new Error("lost publication response");
      }
      const result = {
        exitCode,
        output: async () => output,
        stdout: async () => output,
        logs: async function* () {
          yield { stream: "stdout", data: output };
        },
        wait: async () => result,
      };
      return result;
    },
  } as unknown as Sandbox;
  const status: Workspaces.WorkspaceStatus = {
    threadId,
    sandboxId: "fake",
    generation: 1,
    state: "running",
    expiresAt: now() + 1800000,
    idleExpiresAt: now() + 1800000,
    absoluteExpiresAt: now() + 86400000,
    accruedMicroUsd: 0,
    reservedMicroUsd: 0,
    remainingMicroUsd: 100000000,
    reason: null,
    sourceSnapshot: null,
    remoteRoot: "/tmp/t3/workspace",
  };
  const jobs = new Map<string, Workspaces.WorkspaceJob>();
  const workspace = Workspaces.Workspaces.of({
    enabled: true,
    start: () => Effect.succeed(status),
    status: () => Effect.succeed(status),
    sandbox: () => Effect.succeed(sdk),
    touch: () =>
      Effect.sync(() => {
        touches++;
        completionActivity = true;
        return status;
      }),
    stop: () => Effect.succeed({ ...status, state: "stopped" }),
    reconcile: Effect.void,
    assertRunning: () => Effect.void,
    setSourceSnapshot: (_thread, snapshot) =>
      Effect.sync(() => {
        status.sourceSnapshot = snapshot;
      }),
    job: (_thread, requestId) => Effect.succeed(jobs.get(requestId) ?? null),
    registerJob: (input) =>
      Effect.sync(() => {
        const job = {
          ...input,
          generation: 1,
          state: "running" as const,
          exitCode: null,
          artifactManifest: null,
        };
        jobs.set(input.requestId, job);
        return job;
      }),
    finishJob: (_thread, requestId, result) =>
      Effect.sync(() => {
        if (needsCompletionActivity && !completionActivity) sdkBlocked = true;
        const job = jobs.get(requestId)!;
        job.state = "finished";
        job.exitCode = result.exitCode ?? null;
        job.artifactManifest = result.artifactManifest ?? null;
        return job;
      }),
  });
  const layer = Execution.layer.pipe(
    Layer.provide(Layer.succeed(Workspaces.Workspaces, workspace)),
    Layer.provide(SourceSnapshot.layer),
    Layer.provide(
      Layer.succeed(ThreadManagement.ThreadManagementService, {
        getThreadShell: () =>
          Effect.succeed(liveThreadShell(threadId, { runtimeMode, interactionMode })),
      } as unknown as ThreadManagement.ThreadManagementService["Service"]),
    ),
    Layer.provide(
      Layer.succeed(ProjectService.ProjectService, {
        getById: () => Effect.succeed(Option.some({ workspaceRoot: local })),
      } as unknown as ProjectService.ProjectService["Service"]),
    ),
    Layer.provide(
      Layer.succeed(ServerConfig.ServerConfig, {
        stateDir: NodePath.join(directory, "state"),
      } as ServerConfig.ServerConfig["Service"]),
    ),
  );
  const invoke = <A, E>(run: (service: Execution.Execution["Service"]) => Effect.Effect<A, E>) =>
    Effect.flatMap(Execution.Execution, run).pipe(Effect.provide(layer));
  const finished = () => {
    let close = () => {};
    const promise = new Promise<void>((resolve) => {
      const watcher = NodeFS.watch(remote, { recursive: true }, (_event, filename) => {
        if (filename?.endsWith("result.json")) {
          watcher.close();
          resolve();
        }
      });
      close = () => watcher.close();
    });
    return { promise, close: () => close() };
  };
  return {
    completionActivity: () => {
      needsCompletionActivity = true;
      completionActivity = false;
    },
    touchCount: () => touches,
    launchCount: () => launches,
    sdk,
    client: {
      rpc: {
        readFile: async function* ({ path }: { path: string }) {
          yield { data: await NodeFSP.readFile(path) };
        },
      },
    } as unknown as SandboxClient,
    invoke,
    local,
    remote,
    directory,
    jobs,
    status,
    finished,
    modes: (runtime: RuntimeMode, interaction: ProviderInteractionMode) => {
      runtimeMode = runtime;
      interactionMode = interaction;
    },
    loseLaunch: () => {
      loseLaunch = true;
    },
    losePublish: () => {
      losePublish = true;
    },
  };
}

it.live("refuses commands in approval and plan modes before allocating or executing", () =>
  Effect.gen(function* () {
    const f = yield* Effect.promise(fixture);
    for (const [runtime, interaction] of [
      ["approval-required", "default"],
      ["full-access", "plan"],
    ] as const) {
      f.modes(runtime, interaction);
      const error = yield* f
        .invoke((service) => service.start(scope, { requestId: "start" }))
        .pipe(Effect.flip);
      expect(error.message).toContain("full-access/default");
      expect(f.jobs.size).toBe(0);
    }
  }),
);

it.live(
  "retries a lost launch response once, reconnects from a fresh service and preserves canonical source",
  () =>
    Effect.gen(function* () {
      const f = yield* Effect.promise(fixture);
      const snapshot = yield* f.invoke((service) =>
        service.sync(scope, { requestId: "sync", selectedPaths: ["app.txt"] }),
      );
      const completed = f.finished();
      const input = {
        requestId: "job",
        snapshotId: snapshot.snapshotId,
        command: "python3",
        args: [
          "-c",
          "import os;os.makedirs('artifacts',exist_ok=True);open('artifacts/count.txt','a').write('once');open('app.txt','w').write('remote')",
        ],
        deadline: now() + 5000,
      };
      f.loseLaunch();
      expect(
        (yield* f.invoke((service) => service.run(scope, input)).pipe(Effect.flip)).message,
      ).toContain("provider");
      yield* f.invoke((service) =>
        service.run({ ...scope, requestNamespace: "later-turn" }, input),
      );
      yield* Effect.promise(() => completed.promise);
      const result = yield* f.invoke((service) =>
        service.job({ ...scope, requestNamespace: "later-turn" }, { requestId: "job" }),
      );
      expect(result.exitCode).toBe(0);
      expect(result.snapshotId).toBe(snapshot.snapshotId);
      expect(
        yield* Effect.promise(() =>
          NodeFSP.readFile(
            NodePath.join(f.remote, "workspaces", snapshot.snapshotId, "artifacts/count.txt"),
            "utf8",
          ),
        ),
      ).toBe("once");
      expect(
        yield* Effect.promise(() => NodeFSP.readFile(NodePath.join(f.local, "app.txt"), "utf8")),
      ).toBe("canonical");
      const evidence = yield* f.invoke((service) =>
        service.collect(scope, { requestId: "proof", paths: ["artifacts/count.txt"] }),
      );
      expect(
        yield* Effect.promise(() =>
          NodeFSP.readFile(NodePath.join(evidence.directory, "artifacts/count.txt"), "utf8"),
        ),
      ).toBe("once");
      expect(
        (yield* f
          .invoke((service) =>
            service.collect(scope, { requestId: "proof", paths: ["artifacts/count.txt"] }),
          )
          .pipe(Effect.flip)).message,
      ).toContain("request-conflict");
      const restored = yield* f.invoke((service) =>
        service.sync(scope, { requestId: "sync-again", selectedPaths: ["app.txt"] }),
      );
      expect(restored.snapshotId).toBe(snapshot.snapshotId);
      const remoteFile = NodePath.join(f.remote, "workspaces", snapshot.snapshotId, "app.txt");
      expect(yield* Effect.promise(() => NodeFSP.readFile(remoteFile, "utf8"))).toBe("canonical");
      const browserFile = yield* Effect.promise(() =>
        readRemoteFile({
          sandbox: f.sdk,
          client: f.client,
          root: `/tmp/t3/runtime/workspaces/${snapshot.snapshotId}`,
          relative: "app.txt",
          maxBytes: 1024,
        }),
      );
      expect(browserFile.toString("utf8")).toBe("canonical");
      yield* Effect.promise(() => NodeFSP.writeFile(remoteFile, "new remote edit"));
      yield* f.invoke((service) =>
        service.sync(scope, { requestId: "sync-again", selectedPaths: ["app.txt"] }),
      );
      expect(yield* Effect.promise(() => NodeFSP.readFile(remoteFile, "utf8"))).toBe(
        "new remote edit",
      );
      yield* Effect.promise(() =>
        NodeFSP.writeFile(NodePath.join(f.local, "app.txt"), "changed canonical source"),
      );
      expect(
        (yield* f
          .invoke((service) =>
            service.sync(scope, { requestId: "sync-again", selectedPaths: ["app.txt"] }),
          )
          .pipe(Effect.flip)).message,
      ).toContain("request-conflict");
      completed.close();
    }),
);

it.live("a lost publication response retry preserves subsequent command artifacts", () =>
  Effect.gen(function* () {
    const f = yield* Effect.promise(fixture);
    const input = { requestId: "initial", selectedPaths: ["app.txt"] };
    const snapshot = yield* f.invoke((service) => service.sync(scope, input));
    const retry = { ...input, requestId: "lost-publication" };
    f.losePublish();
    expect(
      (yield* f.invoke((service) => service.sync(scope, retry)).pipe(Effect.flip)).message,
    ).toContain("provider");
    const completed = f.finished();
    yield* f.invoke((service) =>
      service.run(scope, {
        requestId: "after-publication",
        snapshotId: snapshot.snapshotId,
        command: "python3",
        args: [
          "-c",
          "import os;os.makedirs('artifacts');open('artifacts/proof.txt','w').write('keep')",
        ],
        deadline: now() + 5000,
      }),
    );
    yield* Effect.promise(() => completed.promise);
    yield* f.invoke((service) => service.sync(scope, retry));
    expect(
      yield* Effect.promise(() =>
        NodeFSP.readFile(
          NodePath.join(f.remote, "workspaces", snapshot.snapshotId, "artifacts/proof.txt"),
          "utf8",
        ),
      ),
    ).toBe("keep");
    completed.close();
  }),
);

it.live("retrying the first lost publication response restores its host source pointer", () =>
  Effect.gen(function* () {
    const f = yield* Effect.promise(fixture);
    const input = { requestId: "first", selectedPaths: ["app.txt"] };
    f.losePublish();
    yield* f.invoke((service) => service.sync(scope, input)).pipe(Effect.flip);
    expect(f.status.sourceSnapshot).toBeNull();
    const snapshot = yield* f.invoke((service) => service.sync(scope, input));
    expect(f.status.sourceSnapshot).toBe(snapshot.snapshotId);
    expect(
      yield* Effect.promise(() =>
        NodeFSP.readFile(NodePath.join(f.directory, "workspace", "app.txt"), "utf8"),
      ),
    ).toBe("canonical");
  }),
);

it.live("a stale lost publication retry preserves the newer remote and host source", () =>
  Effect.gen(function* () {
    const f = yield* Effect.promise(fixture);
    const input = { requestId: "lost-a", selectedPaths: ["app.txt"] };
    f.losePublish();
    yield* f.invoke((service) => service.sync(scope, input)).pipe(Effect.flip);
    yield* Effect.promise(() => NodeFSP.writeFile(NodePath.join(f.local, "app.txt"), "source B"));
    const snapshotB = yield* f.invoke((service) =>
      service.sync(scope, { ...input, requestId: "sync-b" }),
    );
    yield* Effect.promise(() => NodeFSP.writeFile(NodePath.join(f.local, "app.txt"), "canonical"));
    expect(
      (yield* f.invoke((service) => service.sync(scope, input)).pipe(Effect.flip)).message,
    ).toContain("request-conflict");
    expect(f.status.sourceSnapshot).toBe(snapshotB.snapshotId);
    expect(
      yield* Effect.promise(() =>
        NodeFSP.readFile(NodePath.join(f.directory, "workspace", "app.txt"), "utf8"),
      ),
    ).toBe("source B");
  }),
);

it.live("an expired incomplete sync receipt cannot report publication success", () =>
  Effect.gen(function* () {
    const f = yield* Effect.promise(fixture);
    yield* f.invoke((service) =>
      service.sync(scope, { requestId: "initial", selectedPaths: ["app.txt"] }),
    );
    f.jobs.set("sync:expired", {
      ...f.jobs.get("sync:initial")!,
      requestId: "sync:expired",
      deadline: now() - 1,
      state: "finished",
      exitCode: 124,
      artifactManifest: null,
    });
    f.status.sourceSnapshot = null;
    expect(
      (yield* f
        .invoke((service) =>
          service.sync(scope, { requestId: "expired", selectedPaths: ["app.txt"] }),
        )
        .pipe(Effect.flip)).message,
    ).toContain("request-conflict");
    expect(f.status.sourceSnapshot).toBeNull();
  }),
);

it.live("caps logs and kills the job process group at its deadline", () =>
  Effect.gen(function* () {
    const f = yield* Effect.promise(fixture);
    const snapshot = yield* f.invoke((service) =>
      service.sync(scope, { requestId: "sync", selectedPaths: ["app.txt"] }),
    );
    const completed = f.finished();
    yield* f.invoke((service) =>
      service.run(scope, {
        requestId: "bounded",
        snapshotId: snapshot.snapshotId,
        command: "python3",
        args: [
          "-c",
          "import sys,time;sys.stdout.write('x'*400000);sys.stdout.flush();time.sleep(60)",
        ],
        deadline: now() + 500,
        service: true,
      }),
    );
    yield* Effect.promise(() => completed.promise);
    const result = yield* f.invoke((service) => service.job(scope, { requestId: "bounded" }));
    expect(result.exitCode).toBe(124);
    expect(result.stdout.length).toBe(262144);
    expect(result.truncated).toBe(true);
    expect(f.jobs.get("bounded")!.deadline).toBeLessThanOrEqual(f.status.idleExpiresAt);
    completed.close();
  }),
);

it.live("refuses source publication while a foreground job holds the source lock", () =>
  Effect.gen(function* () {
    const f = yield* Effect.promise(fixture);
    const snapshot = yield* f.invoke((service) =>
      service.sync(scope, { requestId: "sync", selectedPaths: ["app.txt"] }),
    );
    const completed = f.finished();
    let close = () => {};
    const ready = new Promise<void>((resolve) => {
      const watcher = NodeFS.watch(f.remote, { recursive: true }, (_event, filename) => {
        if (filename?.endsWith("ready.txt")) {
          watcher.close();
          resolve();
        }
      });
      close = () => watcher.close();
    });
    yield* f.invoke((service) =>
      service.run(scope, {
        requestId: "foreground",
        snapshotId: snapshot.snapshotId,
        command: "python3",
        args: [
          "-c",
          "import os,time;os.makedirs('artifacts',exist_ok=True);open('artifacts/ready.txt','w').write('ready');time.sleep(60)",
        ],
        deadline: now() + 1500,
      }),
    );
    yield* Effect.promise(() => ready);
    yield* Effect.promise(() =>
      NodeFSP.writeFile(NodePath.join(f.local, "app.txt"), "new canonical source"),
    );
    expect(
      (yield* f
        .invoke((service) =>
          service.sync(scope, { requestId: "blocked", selectedPaths: ["app.txt"] }),
        )
        .pipe(Effect.flip)).message,
    ).toContain("busy");
    expect(f.status.sourceSnapshot).toBe(snapshot.snapshotId);
    yield* Effect.promise(() => completed.promise);
    completed.close();
    close();
  }),
);

it.live(
  "completed discovery preserves logs and records activity once before ending the foreground lease",
  () =>
    Effect.gen(function* () {
      const f = yield* Effect.promise(fixture);
      const snapshot = yield* f.invoke((service) =>
        service.sync(scope, { requestId: "sync", selectedPaths: ["app.txt"] }),
      );
      const completed = f.finished();
      yield* f.invoke((service) =>
        service.run(scope, {
          requestId: "completion",
          snapshotId: snapshot.snapshotId,
          command: "python3",
          args: ["-c", "print('completed output')"],
          deadline: now() + 5000,
        }),
      );
      yield* Effect.promise(() => completed.promise);
      f.jobs.get("completion")!.state = "running";
      f.completionActivity();
      const before = f.touchCount();
      const result = yield* f.invoke((service) => service.job(scope, { requestId: "completion" }));
      expect(result.stdout).toBe("completed output\n");
      expect(result.exitCode).toBe(0);
      expect(f.touchCount()).toBe(before + 1);
      yield* f.invoke((service) => service.job(scope, { requestId: "completion" }));
      expect(f.touchCount()).toBe(before + 1);
      completed.close();
    }),
);

it.live(
  "refuses old-generation job and sync retries before launching in a recreated workspace",
  () =>
    Effect.gen(function* () {
      const f = yield* Effect.promise(fixture);
      const snapshot = yield* f.invoke((service) =>
        service.sync(scope, { requestId: "generation-sync", selectedPaths: ["app.txt"] }),
      );
      const completed = f.finished();
      const input = {
        requestId: "generation-job",
        snapshotId: snapshot.snapshotId,
        command: "python3",
        args: ["-c", "print('once')"],
        deadline: now() + 5000,
      };
      yield* f.invoke((service) => service.run(scope, input));
      yield* Effect.promise(() => completed.promise);
      f.status.generation++;
      const before = f.launchCount();
      const failure = yield* f.invoke((service) => service.run(scope, input)).pipe(Effect.flip);
      expect(f.launchCount()).toBe(before);
      expect(failure.message).toContain("request-conflict");
      expect(
        (yield* f
          .invoke((service) =>
            service.sync(scope, { requestId: "generation-sync", selectedPaths: ["app.txt"] }),
          )
          .pipe(Effect.flip)).message,
      ).toContain("request-conflict");
      completed.close();
    }),
);
