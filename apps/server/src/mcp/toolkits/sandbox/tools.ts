import {
  McpCapabilityUnavailableError,
  OrchestratorMcpFailure,
  WorkspaceOperationError,
  WorkspaceStatus,
  WorkspaceSnapshot,
  WorkspaceJob,
  WorkspaceArtifacts,
  WorkspaceRequestInput,
  WorkspaceSyncInput,
  WorkspaceRunInput,
  WorkspaceCollectInput,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";

import * as Execution from "../../../sandbox/Execution.ts";
import * as Workspaces from "../../../sandbox/Workspaces.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";

const dependencies = [
  Execution.Execution,
  McpInvocationContext.McpInvocationContext,
  ThreadManagement.ThreadManagementService,
];
const failure = Schema.Union([
  WorkspaceOperationError,
  Workspaces.WorkspaceError,
  OrchestratorMcpFailure,
  McpCapabilityUnavailableError,
]);
export const SandboxToolkit = Toolkit.make(
  Tool.make("workspace_start", {
    description:
      "Start or reconnect this conversation's disposable cloud workspace. Reuse requestId on retry. A creating result needs workspace_status before syncing. Full-access/default only; expires after 30 idle minutes or 24 hours. Global monthly compute budget is $100.",
    parameters: WorkspaceRequestInput,
    success: WorkspaceStatus,
    failure,
    dependencies,
  }),
  Tool.make("workspace_status", {
    description:
      "Read this conversation's cloud workspace state, source snapshot and global remaining compute budget.",
    parameters: Tool.EmptyParams,
    success: Schema.NullOr(WorkspaceStatus),
    failure,
    dependencies,
  }),
  Tool.make("workspace_sync", {
    description:
      "Capture this conversation's canonical project/worktree source, including dirty tracked files and deletions, then atomically sync it to the cloud. Secrets and caches are excluded. Explicit selectedPaths adds untracked files; non-Git folders require a bounded explicit selection. Canonical local files are never overwritten. Existing services keep their prior source; restart them to use refreshed source.",
    parameters: WorkspaceSyncInput,
    success: WorkspaceSnapshot,
    failure,
    dependencies,
  }),
  Tool.make("workspace_run", {
    description:
      "Run command with args in the named synced snapshot. No shell interpretation unless command explicitly invokes a shell. Supply an absolute Unix deadline in milliseconds. Reuse requestId and identical input on retry; results survive later turns. Set service=true for a bounded background service that does not keep an idle workspace alive. Full-access/default only.",
    parameters: WorkspaceRunInput,
    success: WorkspaceJob,
    failure,
    dependencies,
  }),
  Tool.make("workspace_job", {
    description:
      "Reconnect to a job by requestId and read its bounded stdout/stderr and exit status, including from a later conversation turn.",
    parameters: WorkspaceRequestInput,
    success: WorkspaceJob,
    failure,
    dependencies,
  }),
  Tool.make("workspace_collect", {
    description:
      "Copy explicit evidence files from artifacts/, test-results/, playwright-report/, screenshots/ or evidence/ into a separate local evidence receipt, with hashes. Maximum 8 MiB per file and 32 MiB total. Canonical project files are never overwritten.",
    parameters: WorkspaceCollectInput,
    success: WorkspaceArtifacts,
    failure,
    dependencies,
  }),
  Tool.make("workspace_stop", {
    description:
      "Stop this conversation's cloud workspace and its processes. A later explicit workspace_start with a new requestId creates a fresh workspace.",
    parameters: Tool.EmptyParams,
    success: Schema.NullOr(WorkspaceStatus),
    failure,
    dependencies,
  }),
);
