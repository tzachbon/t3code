// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous SHA-256 identifies immutable manifests and remote receipt paths without runtime I/O.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native descriptors reserve evidence publication without replacing existing files.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native descriptors reserve evidence publication without replacing existing files.
import * as NodePath from "node:path";
import type { Sandbox } from "@depot/sandbox";
import {
  OrchestratorMcpFailure,
  WorkspaceOperationError,
  type WorkspaceStatus,
  WorkspaceJob,
  type WorkspaceSnapshot,
  WorkspaceArtifacts,
  WorkspaceRunInput,
  WorkspaceSyncInput,
  WorkspaceCollectInput,
  WorkspaceRequestInput,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as ServerConfig from "../config.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import {
  readCaller,
  assertFullAccess,
  assertLiveCaller,
  unavailable,
} from "../mcp/threadAccess.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as SourceSnapshot from "./SourceSnapshot.ts";
import * as Workspaces from "./Workspaces.ts";

const REMOTE = "/tmp/t3/runtime";
const LOG_LIMIT = 256 * 1024;
const EVIDENCE_LIMIT = 32 * 1024 * 1024;
const decodeJobMetadata = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      snapshotId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
      deadline: Schema.Number,
      service: Schema.Boolean,
      requestHash: Schema.String,
    }),
  ),
);
const decodeJobResult = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ exitCode: Schema.Int, truncated: Schema.Boolean })),
);
const decodeCollected = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({ remoteDirectory: Schema.String, files: WorkspaceArtifacts.fields.files }),
  ),
);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const hash = (value: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const failed = (
  operation: WorkspaceOperationError["operation"],
  reason: WorkspaceOperationError["reason"],
) => new WorkspaceOperationError({ operation, reason });
const isOperationError = Schema.is(WorkspaceOperationError);
const provider = <A>(operation: WorkspaceOperationError["operation"], run: () => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) =>
      isOperationError(cause)
        ? cause
        : new WorkspaceOperationError({ operation, reason: "provider", cause }),
  }).pipe(
    Effect.timeout("30 seconds"),
    Effect.mapError((cause) =>
      isOperationError(cause)
        ? cause
        : new WorkspaceOperationError({ operation, reason: "provider", cause }),
    ),
  );

// Commands run only remotely. The launcher returns promptly; the supervisor owns the durable receipt,
// process group, bounded output and deadline, so a lost SDK stream cannot rerun the user's command.
const SUPERVISOR = String.raw`
import os,sys,json,time,fcntl,subprocess,selectors,signal
base,job=sys.argv[1:3]
d=os.path.join(base,'jobs',job)
os.makedirs(d,exist_ok=True)
lock=open(os.path.join(d,'execution.lock'),'a')
try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError: sys.exit(0)
if os.path.exists(os.path.join(d,'claimed')): sys.exit(0)
spec=json.load(open(os.path.join(d,'spec.json')))
source_lock=None
if not spec['service']:
 source_lock=open(os.path.join(base,'source.lock'),'a')
 while True:
  try: fcntl.flock(source_lock,fcntl.LOCK_EX|fcntl.LOCK_NB);break
  except BlockingIOError:
   if time.time()*1000>=spec['deadline']: break
   time.sleep(0.05)
# Claim before spawning. A crash in this gap can fail the job, but must never execute it twice.
with open(os.path.join(d,'claimed'),'x') as f:
 f.write('claimed');f.flush();os.fsync(f.fileno())
limit=262144
out=[open(os.path.join(d,n),'wb',buffering=0) for n in ('stdout','stderr')]
sizes=[0,0];truncated=False;code=125;p=None
try:
 if time.time()*1000>=spec['deadline']: code=124
 else:
  cwd=os.path.join(base,'workspaces',spec['snapshotId'])
  p=subprocess.Popen([spec['command']]+spec['args'],cwd=cwd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
  selector=selectors.DefaultSelector()
  for i,stream in enumerate((p.stdout,p.stderr)):
   os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,i)
  timed_out=False
  while selector.get_map() or p.poll() is None:
   remaining=(spec['deadline']-time.time()*1000)/1000
   if remaining<=0:
    timed_out=True
    try: os.killpg(p.pid,signal.SIGKILL)
    except ProcessLookupError: pass
   for key,_ in selector.select(max(0,min(0.1,remaining))):
    data=os.read(key.fileobj.fileno(),65536)
    if not data: selector.unregister(key.fileobj);key.fileobj.close();continue
    i=key.data;allowed=max(0,limit-sizes[i]);out[i].write(data[:allowed]);sizes[i]+=min(allowed,len(data));truncated=truncated or len(data)>allowed
   if timed_out:
    p.wait()
    for key in list(selector.get_map().values()): selector.unregister(key.fileobj);key.fileobj.close()
    break
  code=124 if timed_out else p.wait()
finally:
 if p is not None:
  # A command may leave grandchildren after its main process exits.
  try: os.killpg(p.pid,signal.SIGKILL)
  except ProcessLookupError: pass
 for f in out: f.close()
 result={'exitCode':code,'truncated':truncated}
 temporary=os.path.join(d,'result.tmp')
 with open(temporary,'w') as f: json.dump(result,f);f.flush();os.fsync(f.fileno())
 os.replace(temporary,os.path.join(d,'result.json'))
`;
const LAUNCH = String.raw`
import os,sys,json,fcntl,subprocess
base,job,payload,supervisor=sys.argv[1:5]
d=os.path.join(base,'jobs',job);os.makedirs(d,exist_ok=True)
lock=open(os.path.join(d,'launch.lock'),'a');fcntl.flock(lock,fcntl.LOCK_EX)
spec=json.loads(payload)
p=os.path.join(d,'spec.json')
if os.path.exists(p):
 if json.load(open(p))['requestHash']!=spec['requestHash']: sys.exit(73)
else:
 with open(p,'x') as f: json.dump(spec,f);f.flush();os.fsync(f.fileno())
if not os.path.exists(os.path.join(d,'claimed')):
 subprocess.Popen([sys.executable,'-c',supervisor,base,job],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True,close_fds=True)
`;
const PUBLISH = String.raw`
import os,sys,json,fcntl,hashlib,shutil,tempfile
base,stage,workspace,request=sys.argv[1:5]
os.makedirs(base,exist_ok=True)
lock=open(os.path.join(base,'source.lock'),'a')
try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError: sys.exit(75)
m=json.load(open(os.path.join(stage,'manifest.json')))
publications=os.path.join(base,'publications');os.makedirs(publications,exist_ok=True)
receipt=os.path.join(publications,request+'.json')
if os.path.exists(receipt):
 if json.load(open(receipt))!=m['snapshotId']: sys.exit(73)
 if os.path.realpath(workspace)!=os.path.realpath(os.path.join(base,'workspaces',m['snapshotId'])): sys.exit(73)
 sys.exit(0)
for e in m['files']:
 if e['hash'] is None: continue
 p=os.path.join(stage,'source',e['path'])
 with open(p,'rb') as f: data=f.read(8388609)
 if len(data)!=e['size'] or hashlib.sha256(data).hexdigest()!=e['hash']: sys.exit(74)
 os.chmod(p,0o755 if e['executable'] else 0o644)
snapshots=os.path.join(base,'snapshots');os.makedirs(snapshots,exist_ok=True)
final=os.path.join(snapshots,m['snapshotId'])
if not os.path.exists(final): os.rename(stage,final)
else: shutil.rmtree(stage)
workspaces=os.path.join(base,'workspaces');os.makedirs(workspaces,exist_ok=True)
working=os.path.join(workspaces,m['snapshotId'])
temporary=tempfile.mkdtemp(prefix=m['snapshotId']+'-',dir=workspaces)
shutil.copytree(os.path.join(final,'source'),temporary,dirs_exist_ok=True)
# A new workcopy resets remote edits; existing services keep their old cwd until explicitly restarted.
link=working+'.next'
try: os.unlink(link)
except FileNotFoundError: pass
os.symlink(temporary,link);os.replace(link,working)
link=workspace+'.next'
try: os.unlink(link)
except FileNotFoundError: pass
os.symlink(working,link);os.replace(link,workspace)
with open(receipt+'.tmp','w') as f:
 json.dump(m['snapshotId'],f);f.flush();os.fsync(f.fileno())
os.replace(receipt+'.tmp',receipt)
fd=os.open(publications,os.O_RDONLY);os.fsync(fd);os.close(fd)
`;
const COLLECT = String.raw`
import os,sys,json,hashlib,tempfile
base,job,paths_json,snapshot=sys.argv[1:5]
source=os.path.join(base,'workspaces',snapshot)
paths=json.loads(paths_json);target=tempfile.mkdtemp(prefix='evidence-',dir=base)
entries=[];total=0
for relative in paths:
 p=os.path.join(source,relative);cursor=source
 for part in relative.split('/'):
  cursor=os.path.join(cursor,part)
  if os.path.islink(cursor): sys.exit(76)
 if os.path.commonpath([os.path.realpath(p),os.path.realpath(source)])!=os.path.realpath(source) or not os.path.isfile(p): sys.exit(76)
 with open(p,'rb') as f: data=f.read(8388609)
 total+=len(data)
 if len(data)>8388608 or total>33554432: sys.exit(77)
 dest=os.path.join(target,relative);os.makedirs(os.path.dirname(dest),exist_ok=True)
 with open(dest,'xb') as f: f.write(data)
 entries.append({'path':relative,'size':len(data),'hash':hashlib.sha256(data).hexdigest()})
print(json.dumps({'remoteDirectory':target,'files':entries}))
`;

type Scope = McpInvocationContext.McpInvocationScope;
type ExecutionError = WorkspaceOperationError | Workspaces.WorkspaceError | OrchestratorMcpFailure;
export class Execution extends Context.Service<
  Execution,
  {
    readonly start: (
      scope: Scope,
      input: WorkspaceRequestInput,
    ) => Effect.Effect<WorkspaceStatus, ExecutionError>;
    readonly status: (scope: Scope) => Effect.Effect<WorkspaceStatus | null, ExecutionError>;
    readonly sync: (
      scope: Scope,
      input: WorkspaceSyncInput,
    ) => Effect.Effect<WorkspaceSnapshot, ExecutionError>;
    readonly run: (
      scope: Scope,
      input: WorkspaceRunInput,
    ) => Effect.Effect<WorkspaceJob, ExecutionError>;
    readonly job: (
      scope: Scope,
      input: WorkspaceRequestInput,
    ) => Effect.Effect<WorkspaceJob, ExecutionError>;
    readonly collect: (
      scope: Scope,
      input: WorkspaceCollectInput,
    ) => Effect.Effect<WorkspaceArtifacts, ExecutionError>;
    readonly stop: (scope: Scope) => Effect.Effect<WorkspaceStatus | null, ExecutionError>;
  }
>()("t3/sandbox/Execution") {}

const make = Effect.gen(function* () {
  const workspaces = yield* Workspaces.Workspaces;
  const snapshots = yield* SourceSnapshot.SourceSnapshot;
  const projects = yield* ProjectService.ProjectService;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const config = yield* ServerConfig.ServerConfig;
  const locks = new Map<string, Semaphore.Semaphore>();
  const threadLock = (threadId: string) => {
    const existing = locks.get(threadId);
    if (existing) return existing;
    const created = Semaphore.makeUnsafe(1);
    locks.set(threadId, created);
    return created;
  };
  const authorize = Effect.fnUntraced(function* (scope: Scope, write = false) {
    if (!scope.thread || !scope.capabilities.has("sandbox"))
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "This tool needs this thread's sandbox capability.",
      });
    const caller = yield* readCaller().pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
      Effect.provideService(ThreadManagement.ThreadManagementService, threads),
    );
    if (write) {
      yield* assertLiveCaller(caller);
      yield* assertFullAccess(
        caller,
        "Remote workspace actions require a live full-access/default calling thread.",
      );
    }
    return { threadId: scope.thread.threadId, caller: caller.caller! };
  });
  const decode = <S extends Schema.Top>(
    schema: S,
    input: unknown,
    operation: WorkspaceOperationError["operation"],
  ) =>
    Schema.decodeUnknownEffect(schema)(input).pipe(
      Effect.mapError(
        (cause) => new WorkspaceOperationError({ operation, reason: "invalid-input", cause }),
      ),
    );
  const python = (
    sandbox: Sandbox,
    operation: WorkspaceOperationError["operation"],
    script: string,
    args: string[],
  ) =>
    provider(operation, async () => {
      const result = await (
        await sandbox.runCommand({ cmd: "/usr/bin/python3", args: ["-c", script, ...args] })
      ).wait();
      if (result.exitCode !== 0)
        throw failed(
          operation,
          result.exitCode === 75
            ? "busy"
            : result.exitCode === 73
              ? "request-conflict"
              : result.exitCode === 76
                ? "unsafe-path"
                : result.exitCode === 77
                  ? "size-limit"
                  : "provider",
        );
      return result;
    });
  const bytes = (
    sandbox: Sandbox,
    filename: string,
    maximum: number,
    operation: WorkspaceOperationError["operation"],
  ) =>
    provider(operation, async () => {
      // SDK readFile buffers the entire remote file before returning. Bound the read remotely and
      // consume capped command chunks so a concurrently growing file cannot allocate unbounded host RAM.
      const command = await sandbox.runCommand({
        cmd: "/usr/bin/python3",
        args: [
          "-c",
          String.raw`
import os,sys,stat,base64
p,cap=sys.argv[1],int(sys.argv[2])
fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
with os.fdopen(fd,'rb') as f:
 if not stat.S_ISREG(os.fstat(f.fileno()).st_mode): sys.exit(76)
 data=f.read(cap+1)
 if len(data)>cap: sys.exit(77)
 sys.stdout.write(base64.b64encode(data).decode('ascii'))
`,
          filename,
          String(maximum),
        ],
      });
      let output = "";
      const encodedLimit = 4 * Math.ceil(maximum / 3);
      for await (const chunk of command.logs()) {
        if (chunk.stream !== "stdout" || output.length + chunk.data.length > encodedLimit)
          throw failed(operation, "size-limit");
        output += chunk.data;
      }
      const result = await command.wait();
      if (result.exitCode !== 0)
        throw failed(
          operation,
          result.exitCode === 77
            ? "size-limit"
            : result.exitCode === 76
              ? "unsafe-path"
              : "provider",
        );
      if (!/^[a-zA-Z0-9+/]*={0,2}$/.test(output)) throw failed(operation, "provider");
      const data = Buffer.from(output, "base64");
      if (data.length > maximum) throw failed(operation, "size-limit");
      return data;
    });
  const jobKey = (requestId: string) => hash(requestId);
  const readJob = Effect.fnUntraced(function* (threadId: string, requestId: string) {
    const record = yield* workspaces.job(threadId, requestId);
    if (!record) return yield* failed("job", "not-found");
    const current = yield* workspaces.status(threadId);
    if (!current || current.generation !== record.generation)
      return yield* failed("job", "not-found");
    const sandbox = yield* workspaces.sandbox(threadId);
    const directory = `${REMOTE}/jobs/${jobKey(requestId)}`;
    const metadata = yield* bytes(sandbox, `${directory}/spec.json`, 64 * 1024, "job").pipe(
      Effect.flatMap((data) => decodeJobMetadata(data.toString("utf8"))),
      Effect.mapError((cause) =>
        isOperationError(cause)
          ? cause
          : new WorkspaceOperationError({ operation: "job", reason: "provider", cause }),
      ),
    );
    if (metadata.requestHash !== record.requestHash)
      return yield* failed("job", "request-conflict");
    const finished = yield* provider("job", () => sandbox.fs().exists(`${directory}/result.json`));
    let exitCode: number | null = null;
    let truncated = false;
    if (finished) {
      const result = yield* bytes(sandbox, `${directory}/result.json`, 4096, "job").pipe(
        Effect.flatMap((data) => decodeJobResult(data.toString("utf8"))),
        Effect.mapError((cause) =>
          isOperationError(cause)
            ? cause
            : new WorkspaceOperationError({ operation: "job", reason: "provider", cause }),
        ),
      );
      exitCode = result.exitCode;
      truncated = result.truncated;
    } else if ((yield* Clock.currentTimeMillis) >= metadata.deadline) {
      // The receipt may be unavailable after a supervisor crash. The deadline still prevents a retry.
      exitCode = 124;
    }
    const output = (name: string) =>
      provider("job", () => sandbox.fs().exists(`${directory}/${name}`)).pipe(
        Effect.flatMap((exists) =>
          exists
            ? bytes(sandbox, `${directory}/${name}`, LOG_LIMIT, "job").pipe(
                Effect.map((data) => data.toString("utf8")),
              )
            : Effect.succeed(""),
        ),
      );
    const stdout = yield* output("stdout");
    const stderr = yield* output("stderr");
    if (exitCode !== null && record.state === "running") {
      // Discovering completion is real activity once. Passive running/finished polling never renews.
      yield* workspaces.touch(threadId);
      yield* workspaces.finishJob(threadId, requestId, { exitCode });
    }
    return {
      requestId,
      snapshotId: metadata.snapshotId,
      deadline: metadata.deadline,
      service: metadata.service,
      state: exitCode === null ? ("running" as const) : ("finished" as const),
      exitCode,
      stdout,
      stderr,
      truncated,
    };
  });
  const start = Effect.fn("Execution.start")(function* (
    scope: Scope,
    input: WorkspaceRequestInput,
  ) {
    const { threadId } = yield* authorize(scope, true);
    const checked = yield* decode(WorkspaceRequestInput, input, "run");
    return { ...(yield* workspaces.start(threadId, checked.requestId)), threadId };
  });
  const status = Effect.fn("Execution.status")(function* (scope: Scope) {
    const { threadId } = yield* authorize(scope);
    const result = yield* workspaces.status(threadId);
    return result ? { ...result, threadId } : null;
  });
  const sync = Effect.fn("Execution.sync")(function* (scope: Scope, input: WorkspaceSyncInput) {
    const checked = yield* decode(WorkspaceSyncInput, input, "sync");
    const owner = yield* authorize(scope, true);
    return yield* threadLock(owner.threadId).withPermits(1)(
      Effect.gen(function* () {
        const { threadId, caller } = yield* authorize(scope, true);
        const project = yield* projects
          .getById(caller.projectId)
          .pipe(Effect.mapError(unavailable));
        if (Option.isNone(project)) return yield* failed("sync", "not-found");
        const root = caller.worktreePath ?? project.value.workspaceRoot;
        const captured = yield* snapshots.capture(root, { selectedPaths: checked.selectedPaths });
        // Snapshot capture can take time; modes are re-read before transferring any source.
        yield* authorize(scope, true);
        // ':' cannot occur in caller request ids, so internal sync receipts cannot collide with jobs.
        const requestId = `sync:${checked.requestId}`;
        const previous = yield* workspaces.job(threadId, requestId);
        const previousWorkspace = yield* workspaces.status(threadId);
        if (previous && previous.generation !== previousWorkspace?.generation)
          return yield* failed("sync", "request-conflict");
        if (previous && previous.requestHash !== captured.manifest.snapshotId)
          return yield* failed("sync", "request-conflict");
        if (previous?.state === "finished") {
          if (previous.exitCode !== 0 || previous.artifactManifest !== captured.manifest.snapshotId)
            return yield* failed("sync", "request-conflict");
          return captured.manifest;
        }
        const current = yield* workspaces.touch(threadId);
        yield* workspaces.registerJob({
          threadId,
          requestId,
          requestHash: captured.manifest.snapshotId,
          deadline: Math.min(
            (yield* Clock.currentTimeMillis) + 5 * 60_000,
            current.absoluteExpiresAt,
          ),
          purpose: "sync",
        });
        const sandbox = yield* workspaces.sandbox(threadId);
        const stage = yield* provider("sync", async () => {
          await sandbox.fs().mkdir(REMOTE, { recursive: true });
          return sandbox.fs().mkdtemp(`${REMOTE}/stage-`);
        });
        yield* Effect.gen(function* () {
          yield* provider("sync", async () => {
            const remote = sandbox.fs();
            await remote.mkdir(`${stage}/source`, { recursive: true });
            for (const [relative, content] of captured.files)
              await remote.writeFile(`${stage}/source/${relative}`, content, { recursive: true });
            await remote.writeFile(`${stage}/manifest.json`, encodeJson(captured.manifest));
          });
          yield* workspaces.assertRunning(threadId, current.generation);
          yield* python(sandbox, "sync", PUBLISH, [REMOTE, stage, "/tmp/t3/workspace", jobKey(requestId)]);
        }).pipe(
          Effect.ensuring(
            provider("sync", () => sandbox.fs().rm(stage, { recursive: true, force: true })).pipe(
              Effect.ignore,
            ),
          ),
        );
        yield* workspaces.assertRunning(threadId, current.generation);
        yield* workspaces.setSourceSnapshot(
          threadId,
          captured.manifest.snapshotId,
          current.generation,
        );
        const finished = yield* workspaces.finishJob(threadId, requestId, {
          exitCode: 0,
          artifactManifest: captured.manifest.snapshotId,
        });
        if (finished.exitCode !== 0 || finished.artifactManifest !== captured.manifest.snapshotId)
          return yield* failed("sync", "request-conflict");
        return captured.manifest;
      }),
    );
  });
  const run = Effect.fn("Execution.run")(function* (scope: Scope, input: WorkspaceRunInput) {
    const checked = yield* decode(WorkspaceRunInput, input, "run");
    const owner = yield* authorize(scope, true);
    return yield* threadLock(owner.threadId).withPermits(1)(
      Effect.gen(function* () {
        const { threadId } = yield* authorize(scope, true);
        const spec = {
          snapshotId: checked.snapshotId,
          command: checked.command,
          args: checked.args ?? [],
          deadline: checked.deadline,
          service: checked.service ?? false,
        };
        if (spec.command.includes("\0") || spec.args.some((arg) => arg.includes("\0")))
          return yield* failed("run", "invalid-input");
        const encoded = encodeJson(spec);
        if (Buffer.byteLength(encoded, "utf8") > 48 * 1024)
          return yield* failed("run", "size-limit");
        const requestHash = hash(encoded);
        const previous = yield* workspaces.job(threadId, checked.requestId);
        if (previous && previous.requestHash !== requestHash)
          return yield* failed("run", "request-conflict");
        if (previous) {
          const current = yield* workspaces.status(threadId);
          if (previous.generation !== current?.generation)
            return yield* failed("run", "request-conflict");
          const sandbox = yield* workspaces.sandbox(threadId);
          yield* workspaces.assertRunning(threadId, previous.generation);
          yield* python(sandbox, "run", LAUNCH, [
            REMOTE,
            jobKey(checked.requestId),
            encodeJson({ ...spec, requestHash }),
            SUPERVISOR,
          ]);
          return yield* readJob(threadId, checked.requestId);
        }
        const current = yield* workspaces.status(threadId);
        const now = yield* Clock.currentTimeMillis;
        if (
          !current ||
          current.state !== "running" ||
          current.sourceSnapshot !== checked.snapshotId
        )
          return yield* failed("run", "source-changed");
        if (
          checked.deadline <= now ||
          checked.deadline > Math.min(now + 24 * 60 * 60_000, current.absoluteExpiresAt)
        )
          return yield* failed("run", "invalid-input");
        const touched = yield* workspaces.touch(threadId);
        const sandbox = yield* workspaces.sandbox(threadId);
        yield* workspaces.assertRunning(threadId, current.generation);
        // Services have a process deadline, but do not extend the workspace's idle lease to it.
        yield* workspaces.registerJob({
          threadId,
          requestId: checked.requestId,
          requestHash,
          deadline: spec.service
            ? Math.min(checked.deadline, touched.idleExpiresAt)
            : checked.deadline,
          purpose: spec.service ? "service" : "foreground",
        });
        yield* python(sandbox, "run", LAUNCH, [
          REMOTE,
          jobKey(checked.requestId),
          encodeJson({ ...spec, requestHash }),
          SUPERVISOR,
        ]);
        return yield* readJob(threadId, checked.requestId);
      }),
    );
  });
  const job = Effect.fn("Execution.job")(function* (scope: Scope, input: WorkspaceRequestInput) {
    const { threadId } = yield* authorize(scope);
    const checked = yield* decode(WorkspaceRequestInput, input, "job");
    return yield* readJob(threadId, checked.requestId);
  });
  const collect = Effect.fn("Execution.collect")(function* (
    scope: Scope,
    input: WorkspaceCollectInput,
  ) {
    const { threadId } = yield* authorize(scope, true);
    const checked = yield* decode(WorkspaceCollectInput, input, "collect");
    if (checked.paths.length === 0 || new Set(checked.paths).size !== checked.paths.length)
      return yield* failed("collect", "invalid-input");
    for (const relative of checked.paths) {
      try {
        SourceSnapshot.validateRelativePath(relative);
      } catch (cause) {
        return yield* new WorkspaceOperationError({
          operation: "collect",
          reason: "unsafe-path",
          cause,
        });
      }
      if (
        SourceSnapshot.sensitivePath(relative) ||
        !/^(?:artifacts|test-results|playwright-report|screenshots|evidence)\//.test(relative) ||
        !/\.(?:png|jpe?g|webp|webm|mp4|txt|log|json|zip|html|pdf)$/i.test(relative)
      )
        return yield* failed("collect", "unsafe-path");
    }
    const current = yield* workspaces.status(threadId);
    if (!current?.sourceSnapshot) return yield* failed("collect", "not-found");
    const snapshotId = current.sourceSnapshot;
    const sandbox = yield* workspaces.sandbox(threadId);
    const result = yield* python(sandbox, "collect", COLLECT, [
      REMOTE,
      jobKey(checked.requestId),
      encodeJson(checked.paths),
      snapshotId,
    ]);
    const collected = yield* provider("collect", () => result.output()).pipe(
      Effect.flatMap((text) => decodeCollected(text)),
      Effect.mapError((cause) =>
        isOperationError(cause)
          ? cause
          : new WorkspaceOperationError({ operation: "collect", reason: "provider", cause }),
      ),
    );
    if (
      !collected.remoteDirectory.startsWith(`${REMOTE}/evidence-`) ||
      collected.files.length !== checked.paths.length ||
      collected.files.some((file, index) => file.path !== checked.paths[index])
    )
      return yield* failed("collect", "unsafe-path");
    const local = NodePath.join(
      config.stateDir,
      "depot-evidence",
      hash(threadId),
      jobKey(checked.requestId),
    );
    let total = 0;
    return yield* Effect.gen(function* () {
      const contents: { path: string; data: Buffer }[] = [];
      for (const file of collected.files) {
        total += file.size;
        if (file.size < 0 || file.size > 8 * 1024 * 1024 || total > EVIDENCE_LIMIT)
          return yield* failed("collect", "size-limit");
        const data = yield* bytes(
          sandbox,
          `${collected.remoteDirectory}/${file.path}`,
          file.size,
          "collect",
        );
        if (hash(data) !== file.hash) return yield* failed("collect", "source-changed");
        contents.push({ path: file.path, data });
      }
      yield* Effect.tryPromise({
        try: async () => {
          await NodeFSP.mkdir(NodePath.dirname(local), { recursive: true, mode: 0o700 });
          const stage = await NodeFSP.mkdtemp(`${local}.stage-`);
          try {
            for (const file of contents) {
              const target = NodePath.join(stage, file.path);
              await NodeFSP.mkdir(NodePath.dirname(target), { recursive: true, mode: 0o700 });
              await NodeFSP.writeFile(target, file.data, { flag: "wx", mode: 0o600 });
            }
            await NodeFSP.writeFile(
              NodePath.join(stage, "manifest.json"),
              encodeJson({ requestId: checked.requestId, snapshotId, files: collected.files }),
              { flag: "wx", mode: 0o600 },
            );
            // The reservation prevents concurrent publishers without replacing an existing receipt.
            const reservation = await NodeFSP.open(`${local}.lock`, "wx", 0o600);
            try {
              if (
                await NodeFSP.lstat(local).then(
                  () => true,
                  (error: NodeJS.ErrnoException) => {
                    if (error.code === "ENOENT") return false;
                    throw error;
                  },
                )
              )
                throw failed("collect", "request-conflict");
              await NodeFSP.rename(stage, local);
            } finally {
              await reservation.close();
              await NodeFSP.unlink(`${local}.lock`);
            }
          } catch (cause) {
            await NodeFSP.rm(stage, { recursive: true, force: true });
            throw cause;
          }
        },
        catch: (cause) =>
          isOperationError(cause)
            ? cause
            : new WorkspaceOperationError({ operation: "collect", reason: "filesystem", cause }),
      });
      return { requestId: checked.requestId, snapshotId, directory: local, files: collected.files };
    }).pipe(
      Effect.ensuring(
        provider("collect", () =>
          sandbox.fs().rm(collected.remoteDirectory, { recursive: true, force: true }),
        ).pipe(Effect.ignore),
      ),
    );
  });
  const stop = Effect.fn("Execution.stop")(function* (scope: Scope) {
    const { threadId } = yield* authorize(scope, true);
    const result = yield* workspaces.stop(threadId, "user");
    return result ? { ...result, threadId } : null;
  });
  return Execution.of({ start, status, sync, run, job, collect, stop });
});
export const layer = Layer.effect(Execution, make);
