import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/ai";

import * as Execution from "../../../sandbox/Execution.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpHttpServer from "../../McpHttpServer.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";
import { SandboxToolkit } from "./tools.ts";
import * as Handlers from "./handlers.ts";

const threadId = ThreadId.make("thread:sandbox-gate");
const scope: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment"),
  requestNamespace: "provider:sandbox",
  thread: {
    threadId,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "session",
  },
  client: undefined,
  capabilities: new Set(["orchestration", "sandbox"]),
  issuedAt: 0,
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
it.effect(
  "sandbox gates refuse other capabilities and ended writes while preserving ended-thread status reads",
  () =>
    Effect.gen(function* () {
      let calls = 0;
      const execution = {
        status: () =>
          Effect.sync(() => {
            calls++;
            return null;
          }),
        start: () => Effect.die("denied write reached execution"),
      } as unknown as Execution.Execution["Service"];
      const layer = McpHttpServer.toolkitRegistration(SandboxToolkit, Handlers.layer).pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provideMerge(Layer.succeed(Execution.Execution, execution)),
        Layer.provideMerge(
          Layer.succeed(ThreadManagement.ThreadManagementService, {
            getThreadShell: () => Effect.succeed(liveThreadShell(threadId, { activeRunId: null })),
          } as unknown as ThreadManagement.ThreadManagementService["Service"]),
        ),
      );
      const call = (
        name: string,
        invocation: McpInvocationContext.McpInvocationScope,
        args: object = {},
      ) =>
        McpServer.McpServer.pipe(
          Effect.flatMap((server) => server.callTool({ name, arguments: args })),
          Effect.map((result) =>
            result.content.map((part) => (part.type === "text" ? part.text : "")).join(""),
          ),
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
      yield* Effect.gen(function* () {
        expect(
          yield* call("workspace_status", { ...scope, capabilities: new Set(["preview"]) }),
        ).toContain("sandbox");
        expect(calls).toBe(0);
        expect(yield* call("workspace_start", scope, { requestId: "start" })).toContain(
          "no longer owns an active thread run",
        );
        expect(calls).toBe(0);
        yield* call("workspace_status", scope);
        expect(calls).toBe(1);
      }).pipe(Effect.provide(layer));
    }),
);
