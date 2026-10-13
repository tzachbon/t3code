// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native filesystem descriptors and no-follow flags are needed for race-safe source capture.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous SHA-256 identifies immutable manifests and remote receipt paths without runtime I/O.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native filesystem descriptors and no-follow flags are needed for race-safe source capture.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native filesystem descriptors and no-follow flags are needed for race-safe source capture.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native filesystem descriptors and no-follow flags are needed for race-safe source capture.
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { WorkspaceOperationError, type WorkspaceSnapshot } from "@t3tools/contracts";

const MAX_FILE_BYTES = 8 * 1024 * 1024;
const MAX_TOTAL_BYTES = 128 * 1024 * 1024;
const MAX_FILES = 10000;
const execute = NodeUtil.promisify(NodeChildProcess.execFile);
const isOperationError = Schema.is(WorkspaceOperationError);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const hash = (content: Uint8Array | string) =>
  NodeCrypto.createHash("sha256").update(content).digest("hex");
const fail = (reason: WorkspaceOperationError["reason"]) =>
  new WorkspaceOperationError({ operation: "snapshot", reason });

/** Source and evidence paths share the same trust boundary. Symlinks are refused, even inside the root. */
export function validateRelativePath(relative: string): void {
  if (
    !relative ||
    relative.length > 1024 ||
    relative.includes("\\") ||
    [...relative].some((char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127) ||
    NodePath.posix.isAbsolute(relative) ||
    relative.split("/").some((part) => !part || part === "." || part === "..")
  )
    throw fail("unsafe-path");
}
export function sensitivePath(relative: string): boolean {
  return relative
    .split("/")
    .some(
      (part) =>
        /^(?:\.git|\.claude|\.agents|\.codex|\.vscode|\.idea|\.env(?:\..*)?|\.npmrc|\.pypirc|\.netrc|\.ssh|\.aws|\.azure|\.config)$/i.test(
          part,
        ) ||
        /(?:^|[._-])(?:credentials?|secrets?|tokens?|private[-_]?key)(?:[._-]|$)/i.test(part) ||
        /\.(?:pem|key|p12|pfx|keystore)$/i.test(part),
    );
}
export function excludedSourcePath(relative: string): boolean {
  return (
    sensitivePath(relative) ||
    /^(?:artifacts|screenshots)\//.test(relative) ||
    /(?:^|\/)(?:tests?|testing)\/evidence(?:\/|$)/.test(relative) ||
    relative
      .split("/")
      .some(
        (part) =>
          /^(?:test-results|playwright-report|node_modules|vendor|\.venv|venv|__pycache__|\.cache|\.next|\.nuxt|\.turbo|\.t3|coverage|dist|build)$/i.test(
            part,
          ) || /^(?:.*[-_])?outputs?$/i.test(part),
      )
  );
}

export class SourceSnapshot extends Context.Service<
  SourceSnapshot,
  {
    readonly capture: (
      root: string,
      input: { readonly selectedPaths?: readonly string[] | undefined },
    ) => Effect.Effect<
      {
        readonly manifest: WorkspaceSnapshot;
        readonly files: ReadonlyMap<string, Buffer>;
      },
      WorkspaceOperationError
    >;
  }
>()("t3/sandbox/SourceSnapshot") {}

export const layer = Layer.succeed(
  SourceSnapshot,
  SourceSnapshot.of({
    capture: (root, { selectedPaths }) =>
      Effect.tryPromise({
        try: async () => {
          const canonical = await NodeFSP.realpath(root);
          if (
            selectedPaths &&
            (selectedPaths.length > 1000 || new Set(selectedPaths).size !== selectedPaths.length)
          )
            throw fail("invalid-input");
          for (const selected of selectedPaths ?? []) {
            validateRelativePath(selected);
            if (excludedSourcePath(selected)) throw fail("unsafe-path");
          }
          const git = async (args: string[]) =>
            (await execute("git", ["-C", canonical, ...args], { maxBuffer: 8 * 1024 * 1024 }))
              .stdout;
          const gitRoot = await git(["rev-parse", "--show-toplevel"]).then(
            (value) => value.trim(),
            () => null,
          );
          // A nested non-repository folder must not silently snapshot its parent's project.
          const isGit = gitRoot !== null && (await NodeFSP.realpath(gitRoot)) === canonical;
          if (!isGit && (!selectedPaths || selectedPaths.length === 0))
            throw fail("selection-required");
          const gitState = async () => {
            if (!isGit) return null;
            const revision = await git(["rev-parse", "--verify", "HEAD"]).then(
              (value) => value.trim(),
              () => null,
            );
            const cached = (await git(["ls-files", "--cached", "-z"])).split("\0").filter(Boolean);
            const committed = revision
              ? (await git(["ls-tree", "-r", "--name-only", "-z", "HEAD"]))
                  .split("\0")
                  .filter(Boolean)
              : [];
            const tracked = [...new Set([...cached, ...committed])];
            const index = await git(["ls-files", "--stage", "-z"]);
            const status = await git(["status", "--porcelain=v1", "-z", "--untracked-files=no"]);
            return {
              revision,
              tracked,
              fingerprint: hash(index + "\0" + status + "\0" + revision),
            };
          };
          const before = await gitState();
          const candidates = [
            ...new Set([...(before?.tracked ?? []), ...(selectedPaths ?? [])]),
          ].sort();
          if (candidates.length > MAX_FILES) throw fail("size-limit");
          const files = new Map<string, Buffer>();
          const entries: WorkspaceSnapshot["files"][number][] = [];
          let total = 0;
          const read = async (relative: string) => {
            validateRelativePath(relative);
            let cursor = canonical;
            const parts = relative.split("/");
            for (const part of parts.slice(0, -1)) {
              cursor = NodePath.join(cursor, part);
              const stat = await NodeFSP.lstat(cursor).catch((error: NodeJS.ErrnoException) => {
                if (error.code === "ENOENT") return null;
                throw error;
              });
              if (!stat) return null;
              if (!stat.isDirectory() || stat.isSymbolicLink()) throw fail("unsafe-path");
            }
            const filename = NodePath.join(canonical, relative);
            const stat = await NodeFSP.lstat(filename).catch((error: NodeJS.ErrnoException) => {
              if (error.code === "ENOENT") return null;
              throw error;
            });
            if (!stat) return null;
            if (!stat.isFile() || stat.isSymbolicLink()) throw fail("unsafe-path");
            if (stat.size > MAX_FILE_BYTES) throw fail("size-limit");
            const handle = await NodeFSP.open(
              filename,
              NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
            );
            try {
              const opened = await handle.stat();
              if (opened.dev !== stat.dev || opened.ino !== stat.ino) throw fail("source-changed");
              // Do not allocate from an unchecked racing file size.
              const bytes = Buffer.alloc(stat.size + 1);
              const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
              const final = await handle.stat();
              if (
                bytesRead !== stat.size ||
                final.size !== stat.size ||
                final.mtimeMs !== stat.mtimeMs ||
                final.ctimeMs !== stat.ctimeMs
              )
                throw fail("source-changed");
              // Parent rename/symlink replacement during read must not change the selected identity.
              if ((await NodeFSP.realpath(filename)) !== filename) throw fail("unsafe-path");
              return { bytes: bytes.subarray(0, bytesRead), executable: (stat.mode & 0o111) !== 0 };
            } finally {
              await handle.close();
            }
          };
          for (const relative of candidates) {
            validateRelativePath(relative);
            if (excludedSourcePath(relative)) continue;
            const file = await read(relative);
            if (file) {
              total += file.bytes.length;
              if (total > MAX_TOTAL_BYTES) throw fail("size-limit");
              files.set(relative, file.bytes);
            }
            entries.push({
              path: relative,
              hash: file ? hash(file.bytes) : null,
              size: file?.bytes.length ?? 0,
              executable: file?.executable ?? false,
            });
          }
          // A second complete read catches content changes, deletions, chmod, and untracked-selection races.
          for (const entry of entries) {
            const final = await read(entry.path);
            if (
              (final ? hash(final.bytes) : null) !== entry.hash ||
              (final?.executable ?? false) !== entry.executable
            )
              throw fail("source-changed");
          }
          if ((await gitState())?.fingerprint !== before?.fingerprint) throw fail("source-changed");
          const revision = before?.revision ?? null;
          const snapshotId = hash(encodeJson({ revision, files: entries }));
          return { manifest: { snapshotId, revision, files: entries }, files };
        },
        catch: (cause) =>
          isOperationError(cause)
            ? cause
            : new WorkspaceOperationError({ operation: "snapshot", reason: "filesystem", cause }),
      }),
  }),
);
