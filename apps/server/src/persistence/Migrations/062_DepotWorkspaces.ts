import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE depot_workspace_manager (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    owner TEXT NOT NULL, frozen_until INTEGER NOT NULL DEFAULT 0
  )`;
  yield* sql`CREATE TABLE depot_workspaces (
    thread_id TEXT NOT NULL, generation INTEGER NOT NULL,
    name TEXT NOT NULL UNIQUE, request_id TEXT NOT NULL,
    sandbox_id TEXT UNIQUE, state TEXT NOT NULL,
    created_at INTEGER NOT NULL, idle_expires_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL, reserved_expires_at INTEGER NOT NULL,
    absolute_expires_at INTEGER NOT NULL,
    stopped_at INTEGER, vcpus INTEGER NOT NULL,
    reason TEXT, source_snapshot TEXT,
    PRIMARY KEY(thread_id, generation), UNIQUE(thread_id, request_id)
  ) WITHOUT ROWID`;
  yield* sql`CREATE UNIQUE INDEX depot_workspaces_live_thread ON depot_workspaces(thread_id)
    WHERE state IN ('creating', 'running', 'stop-pending')`;
  yield* sql`CREATE TABLE depot_workspace_jobs (
    thread_id TEXT NOT NULL, generation INTEGER NOT NULL,
    request_id TEXT NOT NULL, request_hash TEXT NOT NULL,
    deadline INTEGER NOT NULL, purpose TEXT NOT NULL,
    state TEXT NOT NULL, exit_code INTEGER, artifact_manifest TEXT,
    PRIMARY KEY(thread_id, request_id)
  ) WITHOUT ROWID`;
});
