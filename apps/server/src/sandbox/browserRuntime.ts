// @effect-diagnostics globalFetch:off -- Fetch belongs to the generated remote program, which has no Effect runtime.
// @effect-diagnostics globalConsole:off -- Remote stdout/stderr are the framed API transport.
// @effect-diagnostics nodeBuiltinImport:off -- Emits stdlib-only programs executed inside the owned Depot sandbox.
// @effect-diagnostics globalTimers:off -- Timers belong to the generated remote program, which has no Effect runtime.
import type { Sandbox } from "@depot/sandbox";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const RUNTIME_DIRECTORY = "/tmp/t3-managed-browser";
const VERSION = "154.0.8037.92";
const EXECUTABLE = `${RUNTIME_DIRECTORY}/${VERSION}/chrome-headless-shell-linux64/chrome-headless-shell`;

export class BrowserRuntimeError extends Schema.TaggedError<BrowserRuntimeError>()("BrowserRuntimeError", {
  stage: Schema.Literals(["install", "launch", "ready"]), cause: Schema.Defect(),
}) {
  override get message() { return "The remote browser could not start securely."; }
}

// The pin matches preview/PreviewBrowser.ts. Verify the entire archive before extracting any entry.
const installer = String.raw`
import urllib.request, hashlib, zipfile, pathlib, stat, os, shutil, fcntl
root = pathlib.Path('/tmp/t3-managed-browser')
root.mkdir(mode=0o700, exist_ok=True)
assert not root.is_symlink(), 'Runtime directory is a symlink'
os.chmod(root, 0o700)
(root / 'channels').mkdir(mode=0o700, exist_ok=True)
assert not (root / 'channels').is_symlink(), 'Channel directory is a symlink'
with os.fdopen(os.open(root / 'install.lock', os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600), 'w') as lock:
 fcntl.flock(lock, fcntl.LOCK_EX)
 target = root / '154.0.8037.92'
 assert not target.is_symlink(), 'Browser directory is a symlink'
 if not (target / '.verified').exists():
  temporary = root / 'installing'
  shutil.rmtree(temporary, ignore_errors=True)
  temporary.mkdir(mode=0o700)
  try:
   archive = temporary / 'browser.zip'
   digest = hashlib.sha256()
   size = 0
   with urllib.request.urlopen('https://storage.googleapis.com/chrome-for-testing-public/154.0.8037.92/linux64/chrome-headless-shell-linux64.zip', timeout=60) as response, archive.open('xb') as output:
    while True:
     chunk = response.read(1024 * 1024)
     if not chunk: break
     size += len(chunk)
     assert size <= 120477194, 'Archive size exceeded'
     digest.update(chunk)
     output.write(chunk)
   assert size == 120477194 and digest.hexdigest() == '636aa5c79f2693632e9921b8bbb050038ba11672e02346c06c20f991aed096f9', 'Archive pin mismatch'
   with zipfile.ZipFile(archive) as bundle:
    entries = bundle.infolist()
    assert len(entries) <= 1024 and sum(i.file_size for i in entries) <= 512 * 1024 * 1024, 'Archive expansion limit'
    for entry in entries:
     name = pathlib.PurePosixPath(entry.filename)
     mode = entry.external_attr >> 16
     assert not name.is_absolute() and '..' not in name.parts and name.parts[0] == 'chrome-headless-shell-linux64', 'Unsafe archive path'
     assert stat.S_IFMT(mode) in (0, stat.S_IFREG, stat.S_IFDIR) and not (entry.flag_bits & 1) and entry.compress_type in (0, 8), 'Unsafe archive type'
    bundle.extractall(temporary)
    for entry in entries:
     mode = entry.external_attr >> 16
     if mode: os.chmod(temporary / entry.filename, stat.S_IMODE(mode) & 0o777)
   assert (temporary / 'chrome-headless-shell-linux64/chrome-headless-shell').is_file(), 'Missing browser'
   archive.unlink()
   (temporary / '.verified').write_text('154.0.8037.92\n')
   if target.exists(): shutil.rmtree(target)
   temporary.rename(target)
  finally:
   shutil.rmtree(temporary, ignore_errors=True)
`;

/** Self-contained after TypeScript compilation; no local imports or credential-bearing closures. */
async function remoteHelper() {
  const fs = await import("node:fs");
  const child = await import("node:child_process");
  const readline = await import("node:readline");
  const events = await import("node:events");
  const directory = process.argv[2]!;
  if (!/^\/tmp\/t3-managed-browser\/channels\/[a-f0-9-]+$/.test(directory)) throw new Error("Invalid channel path");
  fs.mkdirSync(directory, { mode: 0o700 });
  const path = `${directory}/input`;
  child.execFileSync("/usr/bin/mkfifo", ["-m", "600", path]);
  const input = fs.createReadStream("", { fd: fs.openSync(path, fs.constants.O_RDWR), autoClose: true });
  const response = await fetch("http://127.0.0.1:9222/json/version");
  const metadata = await response.json();
  if (typeof metadata !== "object" || metadata === null || !("webSocketDebuggerUrl" in metadata) || typeof metadata.webSocketDebuggerUrl !== "string") throw new Error("Invalid browser metadata");
  const endpoint = new URL(metadata.webSocketDebuggerUrl);
  if (endpoint.protocol !== "ws:" || endpoint.hostname !== "127.0.0.1" || endpoint.port !== "9222") throw new Error("Invalid browser endpoint");
  const ws = new WebSocket(endpoint);
  await events.EventEmitter.once(ws, "open");
  let stopped = false;
  const stop = () => {
    if (stopped) return;
    stopped = true;
    ws.close();
    input.destroy();
    try { fs.unlinkSync(path); fs.rmdirSync(directory); } catch { /* Only this connection's FIFO is disposable. */ }
    setTimeout(() => process.exit(0), 50).unref();
  };
  process.on("SIGTERM", stop);
  ws.addEventListener("close", stop);
  ws.addEventListener("error", stop);
  readline.createInterface({ input, crlfDelay: Infinity }).on("line", (line) => {
    if (line === '{"__close":true}') { stop(); return; }
    if (ws.bufferedAmount + Buffer.byteLength(line) > 8 * 1024 * 1024) { stop(); return; }
    JSON.parse(line);
    ws.send(line);
  });
  let writing = Promise.resolve();
  let queuedBytes = 0;
  ws.addEventListener("message", (event) => {
    const bytes = typeof event.data === "string" ? Buffer.byteLength(event.data) : event.data.size;
    queuedBytes += bytes;
    if (queuedBytes > 8 * 1024 * 1024) { stop(); return; }
    writing = writing.then(async () => {
      const line = typeof event.data === "string" ? event.data : await event.data.text();
      if (!process.stdout.write(`${line}\n`)) await events.EventEmitter.once(process.stdout, "drain");
      queuedBytes -= bytes;
    }).catch(stop);
  });
  console.error("READY");
}

const helper = `(${remoteHelper.toString()})().catch(() => { console.error("Remote browser helper failed."); process.exit(1); });`;

const probe = String.raw`
import * as fs from 'node:fs';
const deadline = Date.now() + Number(process.argv[1]);
const owned = () => {
 const listeners = ['tcp', 'tcp6'].flatMap(name => {
  try { return fs.readFileSync('/proc/net/' + name, 'utf8').trim().split('\n').slice(1); }
  catch (error) {
   if (name === 'tcp6' && error.code === 'ENOENT') return [];
   process.exit(3);
  }
 })
  .map(line => line.trim().split(/\s+/)).filter(row => row[1].endsWith(':2406') && row[3] === '0A');
 if (!listeners.length) return false;
 try {
  const pid = fs.readFileSync('/tmp/t3-managed-browser/browser.pid', 'utf8').trim();
  if (!/^\d+$/.test(pid)) process.exit(3);
  const executable = '/tmp/t3-managed-browser/154.0.8037.92/chrome-headless-shell-linux64/chrome-headless-shell';
  if (fs.readlinkSync('/proc/' + pid + '/exe') !== executable) process.exit(3);
  const args = fs.readFileSync('/proc/' + pid + '/cmdline', 'utf8').split('\0');
  if (!args.includes('--user-data-dir=/tmp/t3-managed-browser/profile')) process.exit(3);
  const sockets = fs.readdirSync('/proc/' + pid + '/fd').flatMap(fd => { try { return [fs.readlinkSync('/proc/' + pid + '/fd/' + fd)]; } catch { return []; } });
  if (listeners.some(row => row[1] !== '0100007F:2406' || !sockets.includes('socket:[' + row[9] + ']'))) process.exit(3);
  return true;
 } catch { process.exit(3); }
};
for (;;) {
 try {
  if (!owned()) throw new Error('Not listening');
  const response = await fetch('http://127.0.0.1:9222/json/version', { signal: AbortSignal.timeout(1000) });
  const info = await response.json();
  if (info.Browser !== 'HeadlessChrome/154.0.8037.92' && info.Browser !== 'Chrome/154.0.8037.92') process.exit(3);
  process.exit(0);
 } catch {
  if (Date.now() >= deadline) process.exit(9);
  await new Promise(resolve => setTimeout(resolve, 100));
 }
}
`;

/** Installs once per workspace. Disconnecting a CDP channel never closes this browser. */
export const ensure = Effect.fn("sandbox.browserRuntime.ensure")(function* (sandbox: Sandbox) {
  const execute = (cmd: string, args: string[], stage: "install" | "ready") => Effect.tryPromise({
    try: async () => (await (await sandbox.runCommand({ cmd, args })).wait()).exitCode,
    catch: (cause) => new BrowserRuntimeError({ stage, cause }),
  });
  // Helper publication shares the install lock; running helpers keep their already-loaded source.
  const setup = `${installer}
 import base64, tempfile
 with tempfile.NamedTemporaryFile(dir=root, prefix='helper-', delete=False) as temporary_helper:
  temporary_helper.write(base64.b64decode('${Buffer.from(helper).toString("base64")}'))
 os.replace(temporary_helper.name, root / 'helper.mjs')
`;
  const installed = yield* execute("/usr/bin/timeout", ["120", "/usr/bin/python3", "-c", setup], "install");
  if (installed !== 0) return yield* new BrowserRuntimeError({ stage: "install", cause: new Error("Browser archive installation failed.") });
  const running = yield* execute("/usr/local/bin/node", ["--input-type=module", "-e", probe, "0"], "ready");
  if (running === 0) return;
  if (running !== 9) return yield* new BrowserRuntimeError({ stage: "ready", cause: new Error("Another browser owns the loopback port.") });
  yield* Effect.tryPromise({
    try: () => sandbox.runCommand({
      cmd: "/usr/bin/python3",
      args: ["-c", `import os, fcntl, pathlib
root = '${RUNTIME_DIRECTORY}'
executable = '${EXECUTABLE}'
with os.fdopen(os.open(root + '/launch.lock', os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600), 'w') as lock:
 fcntl.flock(lock, fcntl.LOCK_EX)
 try:
  pid = pathlib.Path(root + '/browser.pid').read_text().strip()
  if pid.isdecimal() and os.readlink('/proc/' + pid + '/exe') == executable:
   raise SystemExit(0)
 except OSError: pass
 with os.fdopen(os.open(root + '/browser.pid', os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600), 'w') as marker:
  marker.write(str(os.getpid()))
 log = os.open(root + '/chrome.log', os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
 os.dup2(log, 1)
 os.dup2(log, 2)
 os.execv(executable, [executable, '--headless', '--disable-dev-shm-usage', '--disable-gpu', '--force-device-scale-factor=2', '--disable-blink-features=AutomationControlled', '--remote-debugging-address=127.0.0.1', '--remote-debugging-port=9222', '--user-data-dir=${RUNTIME_DIRECTORY}/profile'])`],
      detached: true,
    }),
    catch: (cause) => new BrowserRuntimeError({ stage: "launch", cause }),
  });
  const ready = yield* execute("/usr/local/bin/node", ["--input-type=module", "-e", probe, "10000"], "ready");
  if (ready !== 0) return yield* new BrowserRuntimeError({ stage: "ready", cause: new Error("The secure browser did not become ready.") });
});
