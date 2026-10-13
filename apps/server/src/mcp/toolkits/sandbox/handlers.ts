import * as Effect from "effect/Effect";

import * as Execution from "../../../sandbox/Execution.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { SandboxToolkit } from "./tools.ts";

const handlers = {
  workspace_start: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.requireMcpCapability("sandbox");
      return yield* (yield* Execution.Execution).start(scope, input);
    }),
  ),
  workspace_status: McpToolAccess.readsAsCaller(() =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.requireMcpCapability("sandbox");
      return yield* (yield* Execution.Execution).status(scope);
    }),
  ),
  workspace_sync: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.requireMcpCapability("sandbox");
      return yield* (yield* Execution.Execution).sync(scope, input);
    }),
  ),
  workspace_run: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.requireMcpCapability("sandbox");
      return yield* (yield* Execution.Execution).run(scope, input);
    }),
  ),
  workspace_job: McpToolAccess.readsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.requireMcpCapability("sandbox");
      return yield* (yield* Execution.Execution).job(scope, input);
    }),
  ),
  workspace_collect: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.requireMcpCapability("sandbox");
      return yield* (yield* Execution.Execution).collect(scope, input);
    }),
  ),
  workspace_stop: McpToolAccess.actsAsCaller(() =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.requireMcpCapability("sandbox");
      return yield* (yield* Execution.Execution).stop(scope);
    }),
  ),
} satisfies McpToolAccess.Handlers<typeof SandboxToolkit.tools>;
export const layer = McpToolAccess.toLayer(SandboxToolkit, handlers);
