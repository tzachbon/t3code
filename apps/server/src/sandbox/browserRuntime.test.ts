// @effect-diagnostics nodeBuiltinImport:off -- Executes the generated Node readiness probe against a controlled proc filesystem.
import * as NodeChildProcess from "node:child_process";
import { expect } from "vite-plus/test";
import { it } from "@effect/vitest";
import type { Sandbox } from "@depot/sandbox";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";

import { ensure } from "./browserRuntime.ts";

function sandbox(exits: number[]) {
  const commands: Array<{ cmd: string; args: string[]; detached?: boolean }> = [];
  return {
    commands,
    value: {
      runCommand: async (command: typeof commands[number]) => {
        commands.push(command);
        const exitCode = exits.shift();
        if (exitCode === undefined) throw new Error("Unexpected command");
        return { wait: async () => ({ exitCode }) };
      },
    } as unknown as Sandbox,
  };
}

it.effect("publishes the helper atomically under the pinned installer lock and reuses a ready browser", () => Effect.gen(function* () {
  const fake = sandbox([0, 0]);
  yield* ensure(fake.value);
  expect(fake.commands).toHaveLength(2);
  const setup = fake.commands[0]!;
  expect(setup.cmd).toBe("/usr/bin/timeout");
  expect(setup.args.slice(0, 3)).toEqual(["120", "/usr/bin/python3", "-c"]);
  expect(setup.args[3]).toContain("fcntl.flock(lock, fcntl.LOCK_EX)");
  expect(setup.args[3]).toContain("os.O_NOFOLLOW");
  expect(setup.args[3]).toContain("636aa5c79f2693632e9921b8bbb050038ba11672e02346c06c20f991aed096f9");
  expect(setup.args[3]).toContain("os.replace(temporary_helper.name, root / 'helper.mjs')");
  expect(fake.commands.some(command => command.detached)).toBe(false);
}));

it.effect.each([
  { exits: [1], stage: "install", count: 1 },
  { exits: [0, 3], stage: "ready", count: 2 },
] as const)("fails closed at $stage without launching a replacement browser", ({ exits, stage, count }) => Effect.gen(function* () {
  const fake = sandbox([...exits]);
  const result = yield* Effect.exit(ensure(fake.value));
  expect(Exit.isFailure(result)).toBe(true);
  if (Exit.isFailure(result)) {
    expect(Cause.squash(result.cause)).toMatchObject({ _tag: "BrowserRuntimeError", stage });
  }
  expect(fake.commands).toHaveLength(count);
  expect(fake.commands.some(command => command.detached)).toBe(false);
}));

// Execute the exact generated probe with a controlled proc filesystem and real process.exit.
it.effect.each([
  { missing: "ENOENT", expected: 0 },
  { missing: "EACCES", expected: 3 },
] as const)("readiness probe handles tcp6 $missing", ({ missing, expected }) => Effect.gen(function* () {
  const fake = sandbox([0, 0]);
  yield* ensure(fake.value);
  const probe = fake.commands[1]!.args[2]!;
  const fixture = `
const fs = {
 readFileSync(path) {
  if (path === '/proc/net/tcp6') throw Object.assign(new Error('fixture'), { code: '${missing}' });
  if (path === '/proc/net/tcp') return 'header\\n 0: 0100007F:2406 00000000:0000 0A 00000000:00000000 00:00000000 00000000 1000 0 3989\\n';
  if (path.endsWith('/browser.pid')) return '123';
  if (path === '/proc/123/cmdline') return '/tmp/t3-managed-browser/154.0.8037.92/chrome-headless-shell-linux64/chrome-headless-shell\\0--user-data-dir=/tmp/t3-managed-browser/profile\\0';
  throw new Error('Unexpected fixture read ' + path);
 },
 readlinkSync(path) {
  if (path === '/proc/123/exe') return '/tmp/t3-managed-browser/154.0.8037.92/chrome-headless-shell-linux64/chrome-headless-shell';
  if (path === '/proc/123/fd/41') return 'socket:[3989]';
  throw new Error('Unexpected fixture link ' + path);
 },
 readdirSync(path) { if (path === '/proc/123/fd') return ['41']; throw new Error('Unexpected fixture directory'); }
};
globalThis.fetch = async () => ({ json: async () => ({ Browser: 'HeadlessChrome/154.0.8037.92' }) });
`;
  const result = NodeChildProcess.spawnSync(process.execPath, ["--input-type=module", "-e", fixture + probe.replace("import * as fs from 'node:fs';", "")], { timeout: 5000 });
  expect(result.error).toBeUndefined();
  expect(result.stderr.toString()).toBe("");
  expect(result.status).toBe(expected);
}));
