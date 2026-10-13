import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as Layer from "effect/Layer";
import type { Sandbox } from "@depot/sandbox";

import migration from "../persistence/Migrations/062_DepotWorkspaces.ts";
import * as Workspaces from "./Workspaces.ts";

const resources = new Map<string, Workspaces.DepotResource>();
let creates = 0;
let loseCreateResponse = false;
let failKill = false;
let failRenew = false;
let allocatedVcpus = 4;
let omitListed = false;
const killed: string[] = [];
const sdk = Workspaces.DepotSandbox.of({
  enabled: true,
  client: null,
  create: ({ name, timeoutMinutes, vcpus }) =>
    Effect.gen(function* () {
      creates++;
      const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
      const resource: Workspaces.DepotResource = {
        id: `sandbox-${creates}`,
        name,
        vcpus: allocatedVcpus || vcpus,
        createdAt: now,
        expiresAt: now + timeoutMinutes * 60_000,
        stoppedAt: null,
        state: "live",
        sandbox: {} as Sandbox,
      };
      resources.set(resource.id, resource);
      if (loseCreateResponse)
        return yield* new Workspaces.WorkspaceError({
          reason: "provider",
          cause: new Error("secret-value"),
        });
      return resource;
    }),
  list: Effect.sync(() => (omitListed ? [] : [...resources.values()])),
  get: (id) => Effect.sync(() => resources.get(id)!),
  renew: (id, timeoutMinutes) =>
    Effect.gen(function* () {
      if (failRenew) return yield* new Workspaces.WorkspaceError({ reason: "provider" });
      const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
      const resource = resources.get(id)!;
      resource.expiresAt = now + timeoutMinutes * 60_000;
      return resource;
    }),
  kill: (id) =>
    Effect.gen(function* () {
      killed.push(id);
      if (failKill) return yield* new Workspaces.WorkspaceError({ reason: "provider" });
      const resource = resources.get(id)!;
      resource.state = "terminal";
      resource.stoppedAt = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    }),
});
const depot = Layer.succeed(Workspaces.DepotSandbox, sdk);
const database = NodeSqliteClient.layer({ filename: ":memory:" });
const testLayer = Workspaces.layer.pipe(
  Layer.provide(NodeCrypto.layer),
  Layer.provide(depot),
  Layer.provide(Layer.effectDiscard(migration).pipe(Layer.provideMerge(database))),
);

it.effect("a late renewal from a stopped generation cannot modify its replacement", () =>
  Effect.gen(function* () {
    const renewing = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const boundary = Layer.succeed(Workspaces.DepotSandbox, {
      ...sdk,
      renew: (id: string, minutes: number) =>
        Effect.gen(function* () {
          const result = { ...(yield* sdk.renew(id, minutes)) };
          yield* Deferred.succeed(renewing, undefined);
          yield* Deferred.await(release);
          return result;
        }),
    });
    yield* Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      const old = yield* service.start("race-thread", "old-start");
      const pending = yield* service.touch("race-thread").pipe(Effect.forkChild);
      yield* Deferred.await(renewing);
      yield* service.stop("race-thread", "user");
      const replacement = yield* service.start("race-thread", "new-start");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(pending);
      const current = yield* service.status("race-thread");
      assert.equal(current?.generation, old.generation + 1);
      assert.equal(current?.sandboxId, replacement.sandboxId);
      assert.equal(current?.expiresAt, replacement.expiresAt);
      assert.equal(current?.state, "running");
    }).pipe(
      Effect.provide(
        Workspaces.layer.pipe(
          Layer.provide(boundary),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(Layer.effectDiscard(migration).pipe(Layer.provideMerge(database))),
        ),
      ),
    );
  }),
);

it.effect("startup resolves a lost-create name to its exact ID before issuing kill", () => {
  const persistent = Layer.effectDiscard(migration).pipe(Layer.provideMerge(database));
  const serviceLayer = Workspaces.layer.pipe(Layer.provide(depot), Layer.provide(NodeCrypto.layer));
  return Effect.gen(function* () {
    loseCreateResponse = true;
    yield* Effect.gen(function* () {
      yield* (yield* Workspaces.Workspaces).start("lost-restart", "lost-request");
    }).pipe(Effect.provide(serviceLayer));
    loseCreateResponse = false;
    const actualId = `sandbox-${creates}`;
    yield* Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      assert.include(killed, actualId);
      assert.equal((yield* service.status("lost-restart"))?.state, "stopped");
    }).pipe(Effect.provide(serviceLayer));
  }).pipe(
    Effect.provide(persistent),
    Effect.ensuring(
      Effect.sync(() => {
        loseCreateResponse = false;
      }),
    ),
  );
});

it.effect("UTC month rollover splits accrued and reserved compute at midnight", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse("2026-10-31T23:50:00Z"));
    yield* Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      const initial = yield* service.start("month-thread", "month-start");
      assert.equal(initial.reservedMicroUsd, 120_000);
      yield* TestClock.adjust("10 minutes");
      const next = yield* service.status("month-thread");
      assert.equal(next?.accruedMicroUsd, 0);
      assert.equal(next?.reservedMicroUsd, 264_000);
      assert.equal(next?.remainingMicroUsd, 99_736_000);
    }).pipe(Effect.provide(testLayer));
  }),
);

it.effect("the scoped ticker kills all owned workspaces after observed allocation debt", () =>
  Effect.gen(function* () {
    const service = yield* Workspaces.Workspaces;
    const first = yield* service.start("ticker-a", "ticker-start-a");
    const second = yield* service.start("ticker-b", "ticker-start-b");
    resources.get(first.sandboxId!)!.vcpus = 16;
    yield* TestClock.adjust("30 seconds");
    assert.include(killed, first.sandboxId);
    assert.include(killed, second.sandboxId);
    assert.equal((yield* service.status("ticker-a"))?.reason, "budget_exhausted");
  }).pipe(
    Effect.provide(testLayer),
    Effect.provideService(HostProcess.Environment, {
      DEPOT_WORKSPACES_MONTHLY_BUDGET_MICRO_USD: "1000000",
    }),
  ),
);

it.effect("an explicit invalid or above-cap budget disables allocation", () =>
  Effect.forEach(["", "0", "Infinity", "100000001", "1.5", "invalid"], (budget) =>
    Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      assert.isFalse(service.enabled);
      assert.equal(
        (yield* service.start("invalid-budget", "start").pipe(Effect.result))._tag,
        "Failure",
      );
    }).pipe(
      Effect.provide(testLayer),
      Effect.provideService(HostProcess.Environment, {
        DEPOT_WORKSPACES_MONTHLY_BUDGET_MICRO_USD: budget,
      }),
    ),
  ),
);

it.effect("startup stops exact manager-owned resources and leaves foreign resources alone", () => {
  const persistent = Layer.effectDiscard(migration).pipe(Layer.provideMerge(database));
  const serviceLayer = Workspaces.layer.pipe(Layer.provide(depot), Layer.provide(NodeCrypto.layer));
  return Effect.gen(function* () {
    const first = yield* Effect.gen(function* () {
      return yield* (yield* Workspaces.Workspaces).start("restart-thread", "restart-request");
    }).pipe(Effect.provide(serviceLayer));
    resources.set("foreign", {
      ...resources.get(first.sandboxId!)!,
      id: "foreign",
      name: "another-owner",
      state: "live",
      stoppedAt: null,
    });
    yield* Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      const restored = yield* service.status("restart-thread");
      assert.notEqual(restored?.state, "running");
      assert.include(killed, first.sandboxId);
      assert.notInclude(killed, "foreign");
    }).pipe(Effect.provide(serviceLayer));
  }).pipe(Effect.provide(persistent));
});

it.effect(
  "repeated concurrent starts allocate one owned generation and reserve its full expiry",
  () =>
    Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      const before = creates;
      const results = yield* Effect.all(
        [service.start("thread-a", "request-a"), service.start("thread-a", "request-a")],
        { concurrency: 2 },
      );
      const status = yield* service.status("thread-a");
      assert.equal(creates - before, 1);
      assert.equal(results[0].generation, results[1].generation);
      assert.equal(status?.sandboxId, `sandbox-${creates}`);
      assert.equal(status?.reservedMicroUsd, 384_000);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("passive status reads do not renew idle expiry, while a bounded job does", () =>
  Effect.gen(function* () {
    const service = yield* Workspaces.Workspaces;
    const initial = yield* service.start("thread-job", "start-job");
    yield* TestClock.adjust("20 minutes");
    assert.equal((yield* service.status("thread-job"))?.expiresAt, initial.expiresAt);
    const deadline = initial.absoluteExpiresAt;
    const registered = yield* service.registerJob({
      threadId: "thread-job",
      requestId: "job-1",
      requestHash: "a".repeat(64),
      deadline,
      purpose: "test",
    });
    assert.equal(registered.deadline, deadline);
    assert.equal((yield* service.status("thread-job"))?.expiresAt, deadline);
    yield* service.finishJob("thread-job", "job-1", { exitCode: 0 });
    assert.equal((yield* service.job("thread-job", "job-1"))?.state, "finished");
    yield* TestClock.adjust("11 minutes");
    yield* service.reconcile;
    assert.notEqual((yield* service.status("thread-job"))?.state, "running");
  }).pipe(Effect.provide(testLayer)),
);

it.effect("a rejected job extension does not leave a running job behind", () =>
  Effect.gen(function* () {
    const service = yield* Workspaces.Workspaces;
    const started = yield* service.start("job-budget", "start-budget");
    const result = yield* service
      .registerJob({
        threadId: "job-budget",
        requestId: "too-large",
        requestHash: "b".repeat(64),
        deadline: started.absoluteExpiresAt,
        purpose: "test",
      })
      .pipe(Effect.result);
    assert.equal(result._tag, "Failure");
    assert.isNull(yield* service.job("job-budget", "too-large"));
    assert.equal((yield* service.status("job-budget"))?.state, "running");
  }).pipe(
    Effect.provide(testLayer),
    Effect.provideService(HostProcess.Environment, {
      DEPOT_WORKSPACES_MONTHLY_BUDGET_MICRO_USD: "500000",
    }),
  ),
);

it.effect(
  "an unconfirmed shorter renewal and failed kill retain the prior full native exposure",
  () =>
    Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      const started = yield* service.start("shorten-thread", "start-shorten");
      yield* service.registerJob({
        threadId: "shorten-thread",
        requestId: "long-job",
        requestHash: "c".repeat(64),
        deadline: started.absoluteExpiresAt,
        purpose: "test",
      });
      failRenew = true;
      failKill = true;
      yield* service.finishJob("shorten-thread", "long-job", { exitCode: 0 });
      const status = yield* service.status("shorten-thread");
      assert.equal(status?.state, "stop-pending");
      assert.equal(status?.reservedMicroUsd, 17_304_000);
    }).pipe(
      Effect.provide(testLayer),
      Effect.ensuring(
        Effect.sync(() => {
          failRenew = false;
          failKill = false;
        }),
      ),
    ),
);

it.effect(
  "allocation mismatch freezes admissions and kills every owned workspace, retaining failed-stop exposure",
  () =>
    Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      const first = yield* service.start("cutoff-a", "start-a");
      allocatedVcpus = 16;
      failKill = true;
      const second = yield* service.start("cutoff-b", "start-b");
      assert.equal((yield* service.status("cutoff-a"))?.state, "stop-pending");
      assert.equal(second.state, "stop-pending");
      assert.equal(second.reason, "budget_exhausted");
      assert.equal(second.remainingMicroUsd, 0);
      assert.include(killed, first.sandboxId);
      assert.include(killed, second.sandboxId);
      assert.equal(
        (yield* service.start("cutoff-c", "start-c").pipe(Effect.result))._tag,
        "Failure",
      );
    }).pipe(
      Effect.provide(testLayer),
      Effect.provideService(HostProcess.Environment, {
        DEPOT_WORKSPACES_MONTHLY_BUDGET_MICRO_USD: "1000000",
      }),
      Effect.ensuring(
        Effect.sync(() => {
          allocatedVcpus = 4;
          failKill = false;
        }),
      ),
    ),
);

it.effect(
  "a lost create response and absent listing retain uncertain exposure beyond native expiry",
  () =>
    Effect.gen(function* () {
      const service = yield* Workspaces.Workspaces;
      const before = creates;
      loseCreateResponse = true;
      const pending = yield* service.start("unknown-thread", "unknown-start");
      assert.equal(pending.state, "creating");
      assert.equal(pending.reservedMicroUsd, 384_000);
      loseCreateResponse = false;
      omitListed = true;
      yield* TestClock.adjust("33 minutes");
      const uncertain = yield* service.status("unknown-thread");
      assert.isAbove(uncertain!.accruedMicroUsd + uncertain!.reservedMicroUsd, 384_000);
      yield* service.start("unknown-thread", "different-request");
      assert.equal(creates - before, 1);
      omitListed = false;
      yield* service.reconcile;
      assert.isNotNull((yield* service.status("unknown-thread"))?.sandboxId);
    }).pipe(
      Effect.provide(testLayer),
      Effect.ensuring(
        Effect.sync(() => {
          loseCreateResponse = false;
          omitListed = false;
        }),
      ),
    ),
);
