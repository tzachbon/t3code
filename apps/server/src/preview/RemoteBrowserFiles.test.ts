// @effect-diagnostics nodeBuiltinImport:off -- Execute the sandbox Python reader against real isolated test files.
import type { Sandbox, SandboxClient } from "@depot/sandbox";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { readRemoteFile } from "./RemoteBrowserFiles.ts";

const execute = NodeUtil.promisify(NodeChildProcess.execFile);
const roots: string[] = [];
const snapshots: string[] = [];
afterEach(async () => {
  await Promise.all(
    roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
  await Promise.all(snapshots.splice(0).map((path) => NodeFSP.rm(path, { force: true })));
});
async function fixture(override?: Uint8Array[], virtualWorkspace = false) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-browser-files-test-"));
  roots.push(root);
  const unlink = vi.fn(async (path: string) => NodeFSP.unlink(path));
  const sandbox = {
    sandboxId: "test-sandbox",
    fs: () => ({ unlink }),
    runCommand: async ({ cmd, args }: { cmd: string; args: string[] }) => {
      let code = 0;
      // Map the fake sandbox's canonical directory into its isolated host fixture.
      if (virtualWorkspace)
        args = args.map((arg) => arg.replaceAll("/tmp/t3/runtime/workspaces", root));
      const output = await execute(cmd, args, { maxBuffer: 4096 }).catch(
        (error: { code: number; stdout: string }) => {
          code = error.code;
          return { stdout: error.stdout };
        },
      );
      if (output.stdout.trim()) snapshots.push(output.stdout.trim());
      return { wait: async () => ({ exitCode: code }), stdout: async () => output.stdout };
    },
  } as unknown as Sandbox;
  const client = {
    rpc: {
      readFile: async function* ({ path }: { path: string }) {
        for (const data of override ?? [await NodeFSP.readFile(path)]) yield { data };
      },
    },
  } as unknown as SandboxClient;
  return { root, sandbox, client, unlink };
}
it("streams exact binary bytes from a confined file and removes its staging copy", async () => {
  const fixtureFiles = await fixture();
  const bytes = Buffer.from([0, 255, 128, 10, 0, 200]);
  await NodeFSP.writeFile(NodePath.join(fixtureFiles.root, "binary.dat"), bytes);
  const result = await readRemoteFile({ ...fixtureFiles, relative: "binary.dat", maxBytes: 6 });
  expect(result).toEqual(bytes);
  expect(fixtureFiles.unlink).toHaveBeenCalledTimes(1);
  await expect(NodeFSP.stat(snapshots[0]!)).rejects.toMatchObject({ code: "ENOENT" });
});
it("rejects traversal, symlink parents, and oversized files before transfer", async () => {
  const fixtureFiles = await fixture();
  await NodeFSP.writeFile(NodePath.join(fixtureFiles.root, "large"), "12345");
  await NodeFSP.symlink(fixtureFiles.root, NodePath.join(fixtureFiles.root, "link"));
  await expect(
    readRemoteFile({ ...fixtureFiles, relative: "../large", maxBytes: 4 }),
  ).rejects.toThrow("Invalid remote file path");
  await expect(
    readRemoteFile({ ...fixtureFiles, relative: "link/large", maxBytes: 5 }),
  ).rejects.toThrow("unavailable, unsafe");
  await expect(readRemoteFile({ ...fixtureFiles, relative: "large", maxBytes: 4 })).rejects.toThrow(
    "size limit",
  );
  expect(fixtureFiles.unlink).not.toHaveBeenCalled();
});
it("caps actual streamed bytes when a remote file changes after its size check", async () => {
  const fixtureFiles = await fixture([Buffer.from("1234"), Buffer.from("5")]);
  await NodeFSP.writeFile(NodePath.join(fixtureFiles.root, "growing"), "1");
  await expect(
    readRemoteFile({ ...fixtureFiles, relative: "growing", maxBytes: 4 }),
  ).rejects.toThrow("size limit");
  expect(fixtureFiles.unlink).toHaveBeenCalledTimes(1);
});
it("reads the atomically replaced snapshot alias and rejects redirected targets and nested links", async () => {
  const files = await fixture(undefined, true);
  const snapshotId = "a".repeat(64);
  const root = `/tmp/t3/runtime/workspaces/${snapshotId}`;
  const alias = NodePath.join(files.root, snapshotId);
  const first = NodePath.join(files.root, `${snapshotId}-first`);
  const fresh = NodePath.join(files.root, `${snapshotId}-fresh`);
  await NodeFSP.mkdir(first);
  await NodeFSP.mkdir(fresh);
  await NodeFSP.writeFile(NodePath.join(first, "binary.dat"), "old mutation");
  const bytes = Buffer.from([0, 255, 128, 0, 10]);
  await NodeFSP.writeFile(NodePath.join(fresh, "binary.dat"), bytes);
  await NodeFSP.symlink(first, alias);
  await NodeFSP.symlink(fresh, `${alias}.next`);
  await NodeFSP.rename(`${alias}.next`, alias);
  expect(await readRemoteFile({ ...files, root, relative: "binary.dat", maxBytes: 5 })).toEqual(
    bytes,
  );
  await NodeFSP.symlink(fresh, NodePath.join(fresh, "nested"));
  await expect(
    readRemoteFile({ ...files, root, relative: "nested/binary.dat", maxBytes: 5 }),
  ).rejects.toThrow("unavailable, unsafe");
  await NodeFSP.unlink(alias);
  await NodeFSP.symlink(files.root, alias);
  await expect(
    readRemoteFile({ ...files, root, relative: "binary.dat", maxBytes: 5 }),
  ).rejects.toThrow("unavailable, unsafe");
  await NodeFSP.unlink(alias);
  await NodeFSP.symlink(first, `${fresh}-link`);
  await NodeFSP.symlink(`${fresh}-link`, alias);
  await expect(
    readRemoteFile({ ...files, root, relative: "binary.dat", maxBytes: 5 }),
  ).rejects.toThrow("unavailable, unsafe");
});
