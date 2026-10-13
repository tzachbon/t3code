import { afterEach, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture exercises the actual native filesystem and Python supervisor, isolated in temporary directories.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture exercises the actual native filesystem and Python supervisor, isolated in temporary directories.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture exercises the actual native filesystem and Python supervisor, isolated in temporary directories.
import * as NodePath from "node:path";

import * as SourceSnapshot from "./SourceSnapshot.ts";

const roots: string[] = [];
const root = async () => {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-snapshot-test-"));
  roots.push(directory);
  return directory;
};
afterEach(async () => {
  await Promise.all(
    roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
});
const capture = (root: string, selectedPaths?: readonly string[]) =>
  Effect.flatMap(SourceSnapshot.SourceSnapshot, (service) =>
    service.capture(root, { selectedPaths }),
  ).pipe(Effect.provide(SourceSnapshot.layer));
it.effect(
  "captures dirty tracked binary content and deletions, excludes secrets and unselected untracked files",
  () =>
    Effect.gen(function* () {
      const directory = yield* Effect.promise(root);
      yield* Effect.promise(async () => {
        NodeChildProcess.execFileSync("git", ["init", "-q", directory]);
        await NodeFSP.writeFile(NodePath.join(directory, "binary.bin"), Buffer.from([0, 1, 2]));
        await NodeFSP.writeFile(NodePath.join(directory, "deleted.txt"), "old");
        await NodeFSP.writeFile(NodePath.join(directory, ".env"), "SECRET=hidden");
        NodeChildProcess.execFileSync("git", ["-C", directory, "add", "."]);
        await NodeFSP.writeFile(NodePath.join(directory, "binary.bin"), Buffer.from([255, 0, 4]));
        await NodeFSP.unlink(NodePath.join(directory, "deleted.txt"));
        await NodeFSP.writeFile(NodePath.join(directory, "untracked.txt"), "excluded");
      });
      const snapshot = yield* capture(directory);
      expect(snapshot.manifest.files.map((file) => file.path)).toEqual([
        "binary.bin",
        "deleted.txt",
      ]);
      expect(snapshot.files.get("binary.bin")).toEqual(Buffer.from([255, 0, 4]));
      expect(snapshot.manifest.files.find((file) => file.path === "deleted.txt")?.hash).toBeNull();
      expect((yield* capture(directory)).manifest.snapshotId).toBe(snapshot.manifest.snapshotId);
    }),
);
it.effect(
  "requires explicit non-Git selection and rejects traversal, secret files, and symlink escapes",
  () =>
    Effect.gen(function* () {
      const directory = yield* Effect.promise(root);
      yield* Effect.promise(async () => {
        await NodeFSP.writeFile(NodePath.join(directory, "chosen.txt"), "chosen");
        await NodeFSP.writeFile(NodePath.join(directory, ".env.local"), "hidden");
        await NodeFSP.symlink(NodeOS.tmpdir(), NodePath.join(directory, "outside"));
      });
      expect((yield* capture(directory).pipe(Effect.flip)).reason).toBe("selection-required");
      for (const selected of ["../chosen.txt", ".env.local", "outside/chosen.txt"])
        expect((yield* capture(directory, [selected]).pipe(Effect.flip)).reason).toBe(
          "unsafe-path",
        );
      expect(
        (yield* capture(directory, ["chosen.txt"])).manifest.files.map((file) => file.path),
      ).toEqual(["chosen.txt"]);
    }),
);
it.effect("records staged deletions from HEAD and retains staged additions", () =>
  Effect.gen(function* () {
    const directory = yield* Effect.promise(root);
    yield* Effect.promise(async () => {
      NodeChildProcess.execFileSync("git", ["init", "-q", directory]);
      await NodeFSP.writeFile(NodePath.join(directory, "removed.txt"), "committed");
      NodeChildProcess.execFileSync("git", ["-C", directory, "add", "."]);
      NodeChildProcess.execFileSync("git", [
        "-C",
        directory,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-qm",
        "initial",
      ]);
      NodeChildProcess.execFileSync("git", ["-C", directory, "rm", "-q", "removed.txt"]);
      await NodeFSP.writeFile(NodePath.join(directory, "added.txt"), "staged");
      NodeChildProcess.execFileSync("git", ["-C", directory, "add", "added.txt"]);
    });
    const result = yield* capture(directory);
    expect(result.manifest.revision).toMatch(/^[a-f0-9]{40}$/);
    expect(result.manifest.files.map((file) => [file.path, file.hash === null])).toEqual([
      ["added.txt", false],
      ["removed.txt", true],
    ]);
  }),
);
