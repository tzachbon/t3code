import { createClient, Sandbox, type SandboxClient } from "@depot/sandbox";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";

const IDLE_MS = 30 * 60_000;
const ABSOLUTE_MS = 24 * 60 * 60_000;
// Native expiry is asynchronous; measured termination was 51 seconds late.
const STOP_LAG_MS = 120_000;
const MONTHLY_MICRO_USD = 100_000_000;

// $0.00005/vCPU-second, rounded up once for each accounted interval.
export const computeMicroUsd = (vcpus: number, milliseconds: number) =>
  Number((BigInt(vcpus) * BigInt(Math.max(0, Math.ceil(milliseconds))) + 19n) / 20n);

export const monthWindow = (instant: number): readonly [number, number] => {
  const from = DateTime.startOf(DateTime.makeUnsafe(instant), "month");
  return [DateTime.toEpochMillis(from), DateTime.toEpochMillis(DateTime.add(from, { months: 1 }))];
};

export class WorkspaceError extends Schema.TaggedError<WorkspaceError>()("WorkspaceError", {
  reason: Schema.Literals([
    "disabled",
    "budget",
    "not-running",
    "invalid-input",
    "request-conflict",
    "provider",
    "persistence",
  ]),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Remote workspace operation failed (${this.reason}).`;
  }
}
const isWorkspaceError = Schema.is(WorkspaceError);

export interface DepotResource {
  id: string;
  name: string;
  vcpus: number;
  createdAt: number;
  expiresAt: number;
  stoppedAt: number | null;
  state: "live" | "terminal";
  sandbox: Sandbox;
}

/** The single vendor seam; credentials and SDK clients never cross client contracts. */
export class DepotSandbox extends Context.Service<
  DepotSandbox,
  {
    readonly enabled: boolean;
    readonly client: SandboxClient | null;
    readonly create: (input: {
      name: string;
      timeoutMinutes: number;
      vcpus: number;
    }) => Effect.Effect<DepotResource, WorkspaceError>;
    readonly list: Effect.Effect<readonly DepotResource[], WorkspaceError>;
    readonly get: (id: string) => Effect.Effect<DepotResource, WorkspaceError>;
    readonly renew: (
      id: string,
      timeoutMinutes: number,
    ) => Effect.Effect<DepotResource, WorkspaceError>;
    readonly kill: (id: string) => Effect.Effect<void, WorkspaceError>;
  }
>()("t3/sandbox/Workspaces/DepotSandbox") {}

function resource(sandbox: Sandbox): DepotResource {
  const vcpus = sandbox.resources?.vcpus;
  const createdAt = sandbox.createdAt?.getTime();
  const expiresAt = sandbox.expiresAt?.getTime();
  if (
    !vcpus ||
    !Number.isSafeInteger(vcpus) ||
    createdAt === undefined ||
    expiresAt === undefined
  ) {
    throw new Error("Incomplete Depot allocation metadata");
  }
  return {
    id: sandbox.sandboxId,
    name: sandbox.name ?? "",
    vcpus,
    createdAt,
    expiresAt,
    stoppedAt: sandbox.stoppedAt?.getTime() ?? null,
    state: ["finished", "cancelled", "failed"].includes(sandbox.status ?? "") ? "terminal" : "live",
    sandbox,
  };
}

export const layerDepot = Layer.effect(
  DepotSandbox,
  Effect.gen(function* () {
    const environment = yield* HostProcess.Environment;
    const token = environment.DEPOT_API_TOKEN ?? environment.DEPOT_TOKEN;
    const orgID = environment.DEPOT_ORG_ID;
    const client = token && orgID ? createClient({ token, orgID }) : null;
    const vendor = <A>(run: (client: SandboxClient) => Promise<A>) =>
      client
        ? Effect.tryPromise({
            try: () => run(client),
            catch: (cause) => new WorkspaceError({ reason: "provider", cause }),
          }).pipe(
            Effect.timeout("20 seconds"),
            Effect.mapError((cause) =>
              isWorkspaceError(cause) ? cause : new WorkspaceError({ reason: "provider", cause }),
            ),
          )
        : Effect.fail(new WorkspaceError({ reason: "disabled" }));
    return DepotSandbox.of({
      enabled: client !== null && environment.DEPOT_WORKSPACES_DISABLED !== "1",
      client,
      create: ({ name, timeoutMinutes, vcpus }) =>
        vendor(async (client) =>
          resource(
            await Sandbox.create(client, {
              name,
              timeoutMinutes,
              resources: { vcpus, memoryMb: 8192, diskGb: 30 },
              disableTailnet: true,
              runtime: {
                imageRef:
                  environment.DEPOT_WORKSPACE_IMAGE ??
                  "library.registry.depot.dev/base:ubuntu-24.04-amd64-20261006141617",
              },
            }),
          ),
        ),
      list: vendor(async (client) => {
        const result: DepotResource[] = [];
        for await (const sandbox of Sandbox.listAll(client)) result.push(resource(sandbox));
        return result;
      }),
      get: (id) => vendor(async (client) => resource(await Sandbox.get(client, id))),
      renew: (id, timeoutMinutes) =>
        vendor(async (client) => {
          const sandbox = await Sandbox.get(client, id);
          await sandbox.setTimeout({ timeoutMinutes });
          return resource(await Sandbox.get(client, id));
        }),
      kill: (id) =>
        vendor(async (client) => {
          await (await Sandbox.get(client, id)).kill();
        }),
    });
  }),
);

type State = "creating" | "running" | "stop-pending" | "stopped" | "expired";
interface Row {
  thread_id: string;
  generation: number;
  name: string;
  request_id: string;
  sandbox_id: string | null;
  state: State;
  created_at: number;
  idle_expires_at: number;
  expires_at: number;
  reserved_expires_at: number;
  absolute_expires_at: number;
  stopped_at: number | null;
  vcpus: number;
  reason: string | null;
  source_snapshot: string | null;
}
export interface WorkspaceStatus {
  threadId: string;
  sandboxId: string | null;
  generation: number;
  state: State;
  expiresAt: number;
  idleExpiresAt: number;
  absoluteExpiresAt: number;
  accruedMicroUsd: number;
  reservedMicroUsd: number;
  remainingMicroUsd: number;
  reason: string | null;
  sourceSnapshot: string | null;
  remoteRoot: "/tmp/t3/workspace";
}
export interface WorkspaceJob {
  threadId: string;
  generation: number;
  requestId: string;
  requestHash: string;
  deadline: number;
  purpose: string;
  state: "running" | "finished";
  exitCode: number | null;
  artifactManifest: string | null;
}

export class Workspaces extends Context.Service<
  Workspaces,
  {
    readonly enabled: boolean;
    readonly start: (
      threadId: string,
      requestId: string,
    ) => Effect.Effect<WorkspaceStatus, WorkspaceError>;
    readonly status: (threadId: string) => Effect.Effect<WorkspaceStatus | null, WorkspaceError>;
    readonly assertRunning: (
      threadId: string,
      generation?: number,
    ) => Effect.Effect<void, WorkspaceError>;
    readonly sandbox: (threadId: string) => Effect.Effect<Sandbox, WorkspaceError>;
    readonly touch: (threadId: string) => Effect.Effect<WorkspaceStatus, WorkspaceError>;
    readonly stop: (
      threadId: string,
      reason: string,
    ) => Effect.Effect<WorkspaceStatus | null, WorkspaceError>;
    readonly registerJob: (input: {
      threadId: string;
      requestId: string;
      requestHash: string;
      deadline: number;
      purpose: string;
    }) => Effect.Effect<WorkspaceJob, WorkspaceError>;
    readonly finishJob: (
      threadId: string,
      requestId: string,
      result: { exitCode?: number; artifactManifest?: string },
    ) => Effect.Effect<WorkspaceJob, WorkspaceError>;
    readonly job: (
      threadId: string,
      requestId: string,
    ) => Effect.Effect<WorkspaceJob | null, WorkspaceError>;
    readonly setSourceSnapshot: (
      threadId: string,
      sourceSnapshot: string,
      generation?: number,
    ) => Effect.Effect<void, WorkspaceError>;
    readonly reconcile: Effect.Effect<void, WorkspaceError>;
  }
>()("t3/sandbox/Workspaces") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const depot = yield* DepotSandbox;
  const environment = yield* HostProcess.Environment;
  const explicitBudget = environment.DEPOT_WORKSPACES_MONTHLY_BUDGET_MICRO_USD;
  const budgetValue = explicitBudget === undefined ? MONTHLY_MICRO_USD : Number(explicitBudget);
  const validBudget =
    Number.isSafeInteger(budgetValue) && budgetValue > 0 && budgetValue <= MONTHLY_MICRO_USD;
  const budget = validBudget ? budgetValue : 0;
  const enabled = () =>
    depot.enabled && validBudget && environment.DEPOT_WORKSPACES_DISABLED !== "1";
  const crypto = yield* Crypto.Crypto;
  const lock = yield* Semaphore.make(1);
  const db = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isWorkspaceError(cause) ? cause : new WorkspaceError({ reason: "persistence", cause }),
      ),
    );
  yield* db(
    sql`INSERT OR IGNORE INTO depot_workspace_manager(singleton,owner) VALUES(1,${yield* crypto.randomUUIDv4.pipe(Effect.orDie)})`,
  );
  const [manager] = yield* db(
    sql<{ owner: string }>`SELECT owner FROM depot_workspace_manager WHERE singleton=1`,
  );
  const ownerPrefix = `t3-${manager!.owner}-`;
  let recoveryReady = false;
  const rows = db(sql<Row>`SELECT * FROM depot_workspaces`);
  const latest = (threadId: string) =>
    db(sql<Row>`SELECT * FROM depot_workspaces
    WHERE thread_id=${threadId} ORDER BY generation DESC LIMIT 1`).pipe(
      Effect.map((rows) => rows[0] ?? null),
    );
  const save = (row: Row) =>
    db(sql`INSERT INTO depot_workspaces ${sql.insert({ ...row })}
    ON CONFLICT(thread_id,generation) DO UPDATE SET
      sandbox_id=excluded.sandbox_id,state=excluded.state,created_at=excluded.created_at,
      idle_expires_at=excluded.idle_expires_at,expires_at=excluded.expires_at,reserved_expires_at=excluded.reserved_expires_at,
      absolute_expires_at=excluded.absolute_expires_at,stopped_at=excluded.stopped_at,
      vcpus=excluded.vcpus,reason=excluded.reason,source_snapshot=excluded.source_snapshot`);
  // Every full-row update reads under this short lock. Vendor calls never hold it.
  const update = Effect.fnUntraced(function* (
    name: string,
    change: (row: Row) => Effect.Effect<void, WorkspaceError>,
  ) {
    return yield* lock.withPermits(1)(
      sql
        .withTransaction(
          Effect.gen(function* () {
            const [row] = yield* db(sql<Row>`SELECT * FROM depot_workspaces WHERE name=${name}`);
            if (!row) return yield* new WorkspaceError({ reason: "not-running" });
            yield* change(row);
            yield* save(row);
            return row;
          }),
        )
        .pipe(db),
    );
  });
  const live = (row: Row) => row.stopped_at === null;
  const spent = (row: Row, from: number, to: number) =>
    computeMicroUsd(row.vcpus, Math.min(to, row.stopped_at ?? to) - Math.max(from, row.created_at));
  const exposure = (row: Row, from: number, to: number, now: number) =>
    spent(
      row,
      from,
      Math.min(to, row.stopped_at ?? Math.max(now, row.reserved_expires_at + STOP_LAG_MS)),
    );
  const ledger = Effect.fnUntraced(function* (now: number) {
    const all = yield* rows;
    const [from, to] = monthWindow(now);
    const accrued = all.reduce((sum, row) => sum + spent(row, from, Math.min(now, to)), 0);
    const total = all.reduce((sum, row) => sum + exposure(row, from, to, now), 0);
    return {
      all,
      accrued,
      reserved: Math.max(0, total - accrued),
      remaining: Math.max(0, budget - total),
    };
  });
  const statusOf = Effect.fnUntraced(function* (row: Row) {
    const now = yield* Clock.currentTimeMillis;
    const costs = yield* ledger(now);
    const [from, to] = monthWindow(now);
    const accruedMicroUsd = spent(row, from, Math.min(now, to));
    return {
      threadId: row.thread_id,
      sandboxId: row.sandbox_id,
      generation: row.generation,
      state: row.state,
      expiresAt: row.expires_at,
      idleExpiresAt: row.idle_expires_at,
      absoluteExpiresAt: row.absolute_expires_at,
      accruedMicroUsd,
      reservedMicroUsd: Math.max(0, exposure(row, from, to, now) - accruedMicroUsd),
      remainingMicroUsd: costs.remaining,
      reason: row.reason,
      sourceSnapshot: row.source_snapshot,
      remoteRoot: "/tmp/t3/workspace" as const,
    };
  });
  const status = Effect.fn("Workspaces.status")(function* (threadId: string) {
    const row = yield* latest(threadId);
    return row ? yield* statusOf(row) : null;
  });
  const admit = Effect.fnUntraced(function* (candidate: Row, now: number, requireRecovery = true) {
    const [settings] = yield* db(
      sql<{ frozen_until: number }>`SELECT frozen_until FROM depot_workspace_manager`,
    );
    if (!validBudget) return yield* new WorkspaceError({ reason: "invalid-input" });
    if (!enabled()) return yield* new WorkspaceError({ reason: "disabled" });
    if (requireRecovery && !recoveryReady) return yield* new WorkspaceError({ reason: "provider" });
    if ((settings?.frozen_until ?? Infinity) > now)
      return yield* new WorkspaceError({ reason: "budget" });
    const all = (yield* rows).filter((row) => row.name !== candidate.name).concat(candidate);
    for (let instant = now; instant <= candidate.reserved_expires_at + STOP_LAG_MS;) {
      const [from, to] = monthWindow(instant);
      if (all.reduce((sum, row) => sum + exposure(row, from, to, now), 0) >= budget) {
        return yield* new WorkspaceError({ reason: "budget" });
      }
      instant = to;
    }
  });
  const requireRunning = Effect.fnUntraced(function* (threadId: string, generation?: number) {
    const row = yield* latest(threadId);
    const now = yield* Clock.currentTimeMillis;
    if (!enabled()) return yield* new WorkspaceError({ reason: "disabled" });
    const [settings] = yield* db(
      sql<{ frozen_until: number }>`SELECT frozen_until FROM depot_workspace_manager`,
    );
    if ((settings?.frozen_until ?? Infinity) > now)
      return yield* new WorkspaceError({ reason: "budget" });
    if (
      !row ||
      row.state !== "running" ||
      !row.sandbox_id ||
      row.expires_at <= now ||
      row.absolute_expires_at <= now ||
      (generation !== undefined && generation !== row.generation)
    ) {
      return yield* new WorkspaceError({ reason: "not-running" });
    }
    const [jobs] = yield* db(sql<{ deadline: number | null }>`SELECT MAX(deadline) AS deadline
      FROM depot_workspace_jobs WHERE thread_id=${threadId} AND generation=${row.generation}
      AND state='running' AND deadline>${now}`);
    if (Math.max(row.idle_expires_at, jobs?.deadline ?? 0) <= now) {
      return yield* new WorkspaceError({ reason: "not-running" });
    }
    return row;
  });
  const stopRow = Effect.fnUntraced(function* (row: Row, reason: string) {
    const pending = yield* update(row.name, (current) =>
      Effect.gen(function* () {
        if (!live(current)) return;
        if (reason === "idle" && current.state !== "stop-pending") {
          const now = yield* Clock.currentTimeMillis;
          const [jobs] = yield* db(sql<{ deadline: number | null }>`SELECT MAX(deadline) AS deadline
          FROM depot_workspace_jobs WHERE thread_id=${current.thread_id} AND generation=${current.generation}
          AND state='running' AND deadline>${now}`);
          if (
            now <
            Math.min(
              current.absolute_expires_at,
              Math.max(current.idle_expires_at, jobs?.deadline ?? 0),
            )
          )
            return;
        }
        current.state = "stop-pending";
        current.reason = current.reason === "budget_exhausted" ? current.reason : reason;
      }),
    );
    if (!live(pending) || pending.state !== "stop-pending") return;
    if (pending.sandbox_id) {
      yield* depot.kill(pending.sandbox_id).pipe(
        Effect.timeout("10 seconds"),
        Effect.catch(() => Effect.void),
      );
      const confirmed = yield* depot
        .get(pending.sandbox_id)
        .pipe(
          Effect.timeout("10 seconds"),
          Effect.match({ onSuccess: (value) => value, onFailure: () => null }),
        );
      if (
        confirmed?.state === "terminal" &&
        confirmed.name === pending.name &&
        confirmed.id === pending.sandbox_id
      ) {
        const stoppedAt = confirmed.stoppedAt ?? (yield* Clock.currentTimeMillis);
        yield* update(pending.name, (current) =>
          Effect.sync(() => {
            current.state = reason === "idle" || reason === "native-expiry" ? "expired" : "stopped";
            current.stopped_at = stoppedAt;
            current.vcpus = Math.max(current.vcpus, confirmed.vcpus);
          }),
        );
      }
    }
    // A successful kill request is not proof that billing has stopped.
  });
  const stop = Effect.fn("Workspaces.stop")(function* (threadId: string, reason: string) {
    const row = yield* latest(threadId);
    if (!row) return null;
    yield* stopRow(row, reason.slice(0, 100));
    return yield* status(threadId);
  });
  const shutdownAll = Effect.fnUntraced(function* (reason: string, freeze: boolean) {
    if (freeze) {
      const now = yield* Clock.currentTimeMillis;
      yield* db(
        sql`UPDATE depot_workspace_manager SET frozen_until=${monthWindow(now)[1]} WHERE singleton=1`,
      );
    }
    yield* Effect.forEach((yield* rows).filter(live), (row) => stopRow(row, reason), {
      concurrency: "unbounded",
      discard: true,
    });
  });
  const start = Effect.fn("Workspaces.start")(function* (threadId: string, requestId: string) {
    if (!threadId || threadId.length > 200 || !requestId || requestId.length > 200) {
      return yield* new WorkspaceError({ reason: "invalid-input" });
    }
    const reserved = yield* lock.withPermits(1)(
      db(
        sql.withTransaction(
          Effect.gen(function* () {
            const [retried] = yield* db(sql<Row>`SELECT * FROM depot_workspaces
        WHERE thread_id=${threadId} AND request_id=${requestId}`);
            if (retried) return { row: retried, create: false };
            const previous = yield* latest(threadId);
            if (previous && live(previous)) return { row: previous, create: false };
            const now = yield* Clock.currentTimeMillis;
            const generation = (previous?.generation ?? 0) + 1;
            const row: Row = {
              thread_id: threadId,
              generation,
              request_id: requestId,
              name: `${ownerPrefix}${yield* crypto.randomUUIDv4.pipe(Effect.orDie)}`,
              sandbox_id: null,
              state: "creating",
              created_at: now,
              idle_expires_at: now + IDLE_MS,
              expires_at: now + IDLE_MS,
              reserved_expires_at: now + IDLE_MS,
              absolute_expires_at: now + ABSOLUTE_MS,
              stopped_at: null,
              vcpus: 4,
              reason: null,
              source_snapshot: null,
            };
            yield* admit(row, now);
            yield* save(row);
            return { row, create: true };
          }),
        ),
      ),
    );
    if (!reserved.create) return yield* statusOf(reserved.row);
    const allocation = yield* depot
      .create({ name: reserved.row.name, timeoutMinutes: 30, vcpus: 4 })
      .pipe(Effect.match({ onSuccess: (value) => value, onFailure: () => null }));
    if (allocation) {
      let accepted = false;
      const row = yield* update(reserved.row.name, (current) =>
        Effect.gen(function* () {
          current.sandbox_id = allocation.id;
          current.vcpus = allocation.vcpus;
          current.created_at = Math.min(current.created_at, allocation.createdAt);
          current.expires_at = allocation.expiresAt;
          current.reserved_expires_at = Math.max(current.reserved_expires_at, allocation.expiresAt);
          accepted = yield* admit(current, yield* Clock.currentTimeMillis, false).pipe(
            Effect.match({ onSuccess: () => true, onFailure: () => false }),
          );
          if (current.state === "creating") {
            current.state = accepted ? "running" : "stop-pending";
            if (!accepted) current.reason = enabled() ? "budget_exhausted" : "disabled";
          }
        }),
      );
      if (!accepted) yield* shutdownAll(enabled() ? "budget_exhausted" : "disabled", enabled());
      else if (row.state === "stop-pending") yield* stopRow(row, row.reason ?? "user");
    } else {
      recoveryReady = false;
      yield* db(sql`UPDATE depot_workspaces SET reason='creation-unconfirmed'
        WHERE name=${reserved.row.name} AND state='creating'`);
    }
    return (yield* status(threadId))!;
  });
  interface JobRow {
    thread_id: string;
    generation: number;
    request_id: string;
    request_hash: string;
    deadline: number;
    purpose: string;
    state: "running" | "finished";
    exit_code: number | null;
    artifact_manifest: string | null;
  }
  const job = Effect.fn("Workspaces.job")(function* (threadId: string, requestId: string) {
    const [row] = yield* db(sql<JobRow>`SELECT * FROM depot_workspace_jobs
      WHERE thread_id=${threadId} AND request_id=${requestId}`);
    return row
      ? {
          threadId: row.thread_id,
          generation: row.generation,
          requestId: row.request_id,
          requestHash: row.request_hash,
          deadline: row.deadline,
          purpose: row.purpose,
          state: row.state,
          exitCode: row.exit_code,
          artifactManifest: row.artifact_manifest,
        }
      : null;
  });
  const extendNative = Effect.fnUntraced(function* (threadId: string, activity: boolean) {
    const pending = yield* lock.withPermits(1)(
      sql
        .withTransaction(
          Effect.gen(function* () {
            const row = yield* requireRunning(threadId);
            const now = yield* Clock.currentTimeMillis;
            if (activity) row.idle_expires_at = Math.min(row.absolute_expires_at, now + IDLE_MS);
            const [jobs] = yield* db(sql<{
              deadline: number | null;
            }>`SELECT MAX(deadline) AS deadline
        FROM depot_workspace_jobs WHERE thread_id=${threadId} AND generation=${row.generation}
        AND state='running' AND deadline>${now}`);
            const target = Math.min(
              row.absolute_expires_at,
              Math.max(row.idle_expires_at, jobs?.deadline ?? 0),
            );
            if (target <= now) return { row, requestedExpiry: now, timeoutMinutes: 0 };
            // Reserve before issuing the RPC, including an uncertain/lost renewal response.
            const timeoutMinutes = Math.min(
              Math.ceil((target - now) / 60_000),
              Math.floor((row.absolute_expires_at - now) / 60_000),
            );
            row.reserved_expires_at = Math.max(
              row.reserved_expires_at,
              now + timeoutMinutes * 60_000,
            );
            yield* admit(row, now);
            yield* save(row);
            return { row, requestedExpiry: now + timeoutMinutes * 60_000, timeoutMinutes };
          }),
        )
        .pipe(db),
    );
    const { row, requestedExpiry, timeoutMinutes } = pending;
    const now = yield* Clock.currentTimeMillis;
    if (row.idle_expires_at <= now) {
      const [jobs] = yield* db(sql<{
        count: number;
      }>`SELECT COUNT(*) AS count FROM depot_workspace_jobs
        WHERE thread_id=${threadId} AND generation=${row.generation} AND state='running' AND deadline>${now}`);
      if (!jobs?.count) {
        yield* stopRow(row, "idle");
        return yield* statusOf(row);
      }
    }
    if (requestedExpiry <= now) {
      yield* stopRow(row, "native-expiry");
      return yield* statusOf(row);
    }
    const renewed = yield* depot
      .renew(row.sandbox_id!, timeoutMinutes)
      .pipe(Effect.match({ onSuccess: (value) => value, onFailure: () => null }));
    if (!renewed) yield* stopRow(row, "renewal-unconfirmed");
    else {
      const current = yield* update(row.name, (current) =>
        Effect.gen(function* () {
          if (!live(current)) return;
          current.expires_at = renewed.expiresAt;
          const [pendingJobs] = yield* db(sql<{
            deadline: number | null;
          }>`SELECT MAX(deadline) AS deadline
          FROM depot_workspace_jobs WHERE thread_id=${threadId} AND generation=${current.generation} AND state='running' AND deadline>${now}`);
          current.reserved_expires_at = Math.max(renewed.expiresAt, pendingJobs?.deadline ?? 0);
          current.vcpus = Math.max(current.vcpus, renewed.vcpus);
        }),
      );
      const accepted = yield* admit(current, yield* Clock.currentTimeMillis, false).pipe(
        Effect.match({ onSuccess: () => true, onFailure: () => false }),
      );
      if (!accepted) yield* shutdownAll(enabled() ? "budget_exhausted" : "disabled", enabled());
      else if (current.state !== "running") yield* stopRow(current, current.reason ?? "user");
    }
    return (yield* status(threadId))!;
  });
  const renewalLocks = new Map<string, Semaphore.Semaphore>();
  const extend = Effect.fnUntraced(function* (threadId: string, activity: boolean) {
    const lane = renewalLocks.get(threadId) ?? Semaphore.makeUnsafe(1);
    renewalLocks.set(threadId, lane);
    return yield* lane.withPermits(1)(extendNative(threadId, activity));
  });
  const registerJob: Workspaces["Service"]["registerJob"] = Effect.fn("Workspaces.registerJob")(
    function* (input) {
      const now = yield* Clock.currentTimeMillis;
      if (
        !/^[a-f0-9]{64}$/.test(input.requestHash) ||
        !/^[a-z][a-z0-9-]{0,63}$/.test(input.purpose) ||
        !input.requestId ||
        input.requestId.length > 200 ||
        !Number.isSafeInteger(input.deadline) ||
        input.deadline <= now
      ) {
        return yield* new WorkspaceError({ reason: "invalid-input" });
      }
      const existing = yield* lock.withPermits(1)(
        sql
          .withTransaction(
            Effect.gen(function* () {
              const existing = yield* job(input.threadId, input.requestId);
              if (existing) {
                if (existing.requestHash !== input.requestHash)
                  return yield* new WorkspaceError({ reason: "request-conflict" });
                return existing;
              }
              const row = yield* requireRunning(input.threadId);
              if (input.deadline > row.absolute_expires_at || input.deadline > now + ABSOLUTE_MS) {
                return yield* new WorkspaceError({ reason: "invalid-input" });
              }
              const requestedExpiry = Math.min(
                row.absolute_expires_at,
                now +
                  Math.ceil((Math.max(row.idle_expires_at, input.deadline) - now) / 60_000) *
                    60_000,
              );
              row.reserved_expires_at = Math.max(row.reserved_expires_at, requestedExpiry);
              yield* admit(row, now);
              yield* save(row);
              yield* db(
                sql`INSERT INTO depot_workspace_jobs ${sql.insert({
                  thread_id: input.threadId,
                  generation: row.generation,
                  request_id: input.requestId,
                  request_hash: input.requestHash,
                  deadline: input.deadline,
                  purpose: input.purpose,
                  state: "running",
                  exit_code: null,
                  artifact_manifest: null,
                })}`,
              );
              return null;
            }),
          )
          .pipe(db),
      );
      if (existing) return existing;
      yield* extend(input.threadId, false);
      return (yield* job(input.threadId, input.requestId))!;
    },
  );
  const finishJob: Workspaces["Service"]["finishJob"] = Effect.fn("Workspaces.finishJob")(
    function* (threadId, requestId, result) {
      if (
        (result.exitCode !== undefined && !Number.isSafeInteger(result.exitCode)) ||
        (result.artifactManifest !== undefined && result.artifactManifest.length > 1024)
      ) {
        return yield* new WorkspaceError({ reason: "invalid-input" });
      }
      const existing = yield* job(threadId, requestId);
      if (!existing) return yield* new WorkspaceError({ reason: "invalid-input" });
      if (existing.state === "finished") return existing;
      yield* db(sql`UPDATE depot_workspace_jobs SET state='finished', exit_code=${result.exitCode ?? null},
      artifact_manifest=${result.artifactManifest ?? null} WHERE thread_id=${threadId} AND request_id=${requestId} AND state='running'`);
      const row = yield* latest(threadId);
      if (row?.state === "running" && row.generation === existing.generation)
        yield* extend(threadId, false);
      return (yield* job(threadId, requestId))!;
    },
  );
  const recoverOwned = Effect.fnUntraced(function* (
    listed: readonly DepotResource[],
    startup: boolean,
  ) {
    const all = yield* rows;
    for (const item of listed) {
      if (
        !item.name.startsWith(ownerPrefix) ||
        !/^[a-f0-9-]{36}$/.test(item.name.slice(ownerPrefix.length))
      )
        continue;
      let row = all.find((candidate) => candidate.name === item.name);
      if (!row) {
        row = {
          thread_id: `recovered:${item.id}`,
          generation: 1,
          request_id: "recovered",
          name: item.name,
          sandbox_id: item.id,
          state: item.state === "terminal" ? "stopped" : "stop-pending",
          created_at: item.createdAt,
          idle_expires_at: item.createdAt,
          expires_at: item.expiresAt,
          reserved_expires_at: item.expiresAt,
          absolute_expires_at: item.createdAt + ABSOLUTE_MS,
          stopped_at:
            item.state === "terminal" ? (item.stoppedAt ?? (yield* Clock.currentTimeMillis)) : null,
          vcpus: item.vcpus,
          reason: "controller-restart",
          source_snapshot: null,
        };
        yield* lock.withPermits(1)(
          db(sql`INSERT OR IGNORE INTO depot_workspaces ${sql.insert({ ...row })}`),
        );
        if (item.state !== "terminal") yield* stopRow(row, "controller-restart");
      } else if (startup && live(row)) {
        row = yield* update(row.name, (current) =>
          Effect.sync(() => {
            current.sandbox_id = item.id;
            current.vcpus = Math.max(current.vcpus, item.vcpus);
            current.expires_at = item.expiresAt;
            current.reserved_expires_at = Math.max(current.reserved_expires_at, item.expiresAt);
          }),
        );
        yield* stopRow(row, enabled() ? "controller-restart" : "disabled");
      }
    }
    recoveryReady = true;
  });
  const reconcile = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const costs = yield* ledger(now);
    if (costs.accrued + costs.reserved >= budget || !enabled()) {
      yield* shutdownAll(
        enabled() ? "budget_exhausted" : "disabled",
        costs.accrued + costs.reserved >= budget,
      );
    }
    const listed = yield* depot.list.pipe(
      Effect.timeout("10 seconds"),
      Effect.mapError((cause) => new WorkspaceError({ reason: "provider", cause })),
    );
    yield* recoverOwned(listed, false);
    for (let row of yield* rows) {
      if (!live(row)) continue;
      const listedResource = listed.find(
        (item) => item.name === row.name && (!row.sandbox_id || item.id === row.sandbox_id),
      );
      const fetched =
        !listedResource && row.sandbox_id
          ? yield* depot
              .get(row.sandbox_id)
              .pipe(
                Effect.timeout("10 seconds"),
                Effect.match({ onSuccess: (value) => value, onFailure: () => null }),
              )
          : null;
      const resource =
        listedResource ??
        (fetched?.name === row.name && fetched.id === row.sandbox_id ? fetched : null);
      if (!resource) {
        recoveryReady = false;
        if (now >= row.expires_at) {
          yield* update(row.name, (current) =>
            Effect.sync(() => {
              if (!live(current)) return;
              current.state = "stop-pending";
              current.reason = "native-expiry-unconfirmed";
            }),
          );
        }
        continue;
      }
      row = yield* update(row.name, (current) =>
        Effect.sync(() => {
          if (!live(current)) return;
          current.sandbox_id = resource.id;
          current.vcpus = Math.max(current.vcpus, resource.vcpus);
          current.expires_at = Math.max(current.expires_at, resource.expiresAt);
          current.reserved_expires_at = Math.max(current.reserved_expires_at, resource.expiresAt);
          if (resource.state === "terminal") {
            current.state =
              current.reason === "idle" || current.reason === "native-expiry"
                ? "expired"
                : "stopped";
            current.stopped_at = resource.stoppedAt ?? now;
          } else if (current.state === "creating") current.state = "running";
        }),
      );
      if (!live(row)) continue;
      if (row.state === "stop-pending") {
        yield* stopRow(row, row.reason ?? "stop");
        continue;
      }
      const [jobs] = yield* db(sql<{ deadline: number | null }>`SELECT MAX(deadline) AS deadline
        FROM depot_workspace_jobs WHERE thread_id=${row.thread_id} AND generation=${row.generation} AND state='running' AND deadline>${now}`);
      if (
        now >= Math.min(row.absolute_expires_at, Math.max(row.idle_expires_at, jobs?.deadline ?? 0))
      ) {
        yield* stopRow(row, "idle");
      }
    }
    yield* db(
      sql`UPDATE depot_workspace_jobs SET state='finished' WHERE state='running' AND deadline<=${now}`,
    );
    const updated = yield* ledger(yield* Clock.currentTimeMillis);
    if (updated.accrued + updated.reserved >= budget) yield* shutdownAll("budget_exhausted", true);
  });
  // A controller restart ends prior leases; confirmed names also find a create whose response was lost.
  yield* Effect.forEach(
    (yield* rows).filter(live),
    (row) => stopRow(row, depot.enabled ? "controller-restart" : "disabled"),
    { concurrency: "unbounded", discard: true },
  );
  yield* depot.list.pipe(
    Effect.timeout("10 seconds"),
    Effect.flatMap((listed) => recoverOwned(listed, true)),
    Effect.catch(() => Effect.void),
  );
  yield* Effect.gen(function* () {
    while (true) {
      yield* Effect.sleep("30 seconds");
      yield* reconcile.pipe(Effect.catch(() => Effect.void));
    }
  }).pipe(Effect.forkScoped);
  return Workspaces.of({
    get enabled() {
      return enabled();
    },
    start,
    status,
    stop,
    assertRunning: (threadId, generation) =>
      requireRunning(threadId, generation).pipe(Effect.asVoid),
    sandbox: Effect.fn("Workspaces.sandbox")(function* (threadId) {
      const row = yield* requireRunning(threadId);
      const resource = yield* depot.get(row.sandbox_id!);
      if (
        resource.name !== row.name ||
        resource.id !== row.sandbox_id ||
        resource.state !== "live"
      ) {
        return yield* new WorkspaceError({ reason: "not-running" });
      }
      return resource.sandbox;
    }),
    touch: (threadId) => extend(threadId, true),
    registerJob,
    finishJob,
    job,
    setSourceSnapshot: Effect.fn("Workspaces.setSourceSnapshot")(
      function* (threadId, sourceSnapshot, generation) {
        if (!/^[a-zA-Z0-9._-]{1,200}$/.test(sourceSnapshot))
          return yield* new WorkspaceError({ reason: "invalid-input" });
        const row = yield* requireRunning(threadId, generation);
        yield* update(row.name, (current) =>
          Effect.gen(function* () {
            if (current.state !== "running")
              return yield* new WorkspaceError({ reason: "not-running" });
            current.source_snapshot = sourceSnapshot;
          }),
        );
      },
    ),
    reconcile,
  });
});

export const layer = Layer.effect(Workspaces, make);
