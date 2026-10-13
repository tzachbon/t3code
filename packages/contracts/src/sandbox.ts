import * as Schema from "effect/Schema";

import { ThreadId } from "./baseSchemas.ts";

const RequestId = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/));
const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const RelativePath = Schema.String.check(Schema.isMinLength(1)).check(Schema.isMaxLength(1024));
const Paths = Schema.Array(RelativePath).check(Schema.isMaxLength(1000));
export const WorkspaceRequestInput = Schema.Struct({ requestId: RequestId });
export type WorkspaceRequestInput = typeof WorkspaceRequestInput.Type;
export const WorkspaceSyncInput = Schema.Struct({
  requestId: RequestId,
  selectedPaths: Schema.optional(Paths),
});
export type WorkspaceSyncInput = typeof WorkspaceSyncInput.Type;
export const WorkspaceRunInput = Schema.Struct({
  requestId: RequestId,
  snapshotId: Hash,
  command: Schema.String.check(Schema.isMinLength(1)).check(Schema.isMaxLength(16384)),
  args: Schema.optional(
    Schema.Array(Schema.String.check(Schema.isMaxLength(16384))).check(Schema.isMaxLength(128)),
  ),
  deadline: Schema.Int.check(Schema.isGreaterThan(0)).annotate({
    description: "Absolute Unix deadline in milliseconds, at most 24 hours away.",
  }),
  service: Schema.optional(Schema.Boolean),
});
export type WorkspaceRunInput = typeof WorkspaceRunInput.Type;
export const WorkspaceCollectInput = Schema.Struct({ requestId: RequestId, paths: Paths });
export type WorkspaceCollectInput = typeof WorkspaceCollectInput.Type;
export const WorkspaceStatus = Schema.Struct({
  threadId: ThreadId,
  sandboxId: Schema.NullOr(Schema.String),
  generation: Schema.Int,
  state: Schema.Literals(["creating", "running", "stop-pending", "stopped", "expired"]),
  expiresAt: Schema.Number,
  idleExpiresAt: Schema.Number,
  absoluteExpiresAt: Schema.Number,
  accruedMicroUsd: Schema.Number,
  reservedMicroUsd: Schema.Number,
  remainingMicroUsd: Schema.Number,
  reason: Schema.NullOr(Schema.String),
  sourceSnapshot: Schema.NullOr(Schema.String),
  remoteRoot: Schema.Literal("/tmp/t3/workspace"),
});
export type WorkspaceStatus = typeof WorkspaceStatus.Type;
export const WorkspaceSnapshot = Schema.Struct({
  snapshotId: Hash,
  revision: Schema.NullOr(Schema.String),
  files: Schema.Array(
    Schema.Struct({
      path: RelativePath,
      hash: Schema.NullOr(Hash),
      size: Schema.Int,
      executable: Schema.Boolean,
    }),
  ),
});
export type WorkspaceSnapshot = typeof WorkspaceSnapshot.Type;
export const WorkspaceJob = Schema.Struct({
  requestId: RequestId,
  snapshotId: Hash,
  deadline: Schema.Number,
  service: Schema.Boolean,
  state: Schema.Literals(["running", "finished"]),
  exitCode: Schema.NullOr(Schema.Int),
  stdout: Schema.String,
  stderr: Schema.String,
  truncated: Schema.Boolean,
});
export type WorkspaceJob = typeof WorkspaceJob.Type;
export const WorkspaceArtifacts = Schema.Struct({
  requestId: RequestId,
  snapshotId: Hash,
  directory: Schema.String,
  files: Schema.Array(Schema.Struct({ path: RelativePath, hash: Hash, size: Schema.Int })),
});
export type WorkspaceArtifacts = typeof WorkspaceArtifacts.Type;
export class WorkspaceOperationError extends Schema.TaggedError<WorkspaceOperationError>()(
  "WorkspaceOperationError",
  {
    operation: Schema.Literals(["access", "snapshot", "sync", "run", "job", "collect"]),
    reason: Schema.Literals([
      "invalid-input",
      "unsafe-path",
      "size-limit",
      "source-changed",
      "selection-required",
      "request-conflict",
      "busy",
      "not-found",
      "provider",
      "filesystem",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Remote workspace ${this.operation} failed (${this.reason}).`;
  }
}
