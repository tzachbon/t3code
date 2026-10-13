// @effect-diagnostics nodeBuiltinImport:off -- Bounded SDK file transfer, never a host filesystem path.
import type { Sandbox, SandboxClient } from "@depot/sandbox";

/** Open every component relative to a directory descriptor; source commands cannot redirect a read through symlinks. */
const SNAPSHOT_FILE = String.raw`
import os,sys,stat,tempfile,re
root,relative,limit=sys.argv[1:4];limit=int(limit)
fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY)
staged=None
try:
 root_parts=root.strip('/').split('/')
 parts=root_parts+relative.split('/')
 for i,part in enumerate(parts):
  if not part or part in ('.','..') or '/' in part: sys.exit(73)
  flags=os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK
  if i<len(parts)-1: flags|=os.O_DIRECTORY
  if i==len(root_parts)-1 and re.fullmatch('/tmp/t3/runtime/workspaces/[a-f0-9]{64}',root):
   # Execution atomically publishes only this root alias; every target component stays no-follow.
   target=os.readlink(part,dir_fd=fd)
   if not re.fullmatch(re.escape(root)+'-[a-zA-Z0-9_-]+',target): sys.exit(73)
   part=target.rsplit('/',1)[-1]
  next_fd=os.open(part,flags,dir_fd=fd);os.close(fd);fd=next_fd
 info=os.fstat(fd)
 if not stat.S_ISREG(info.st_mode) or info.st_size>limit: sys.exit(74)
 out,staged=tempfile.mkstemp(prefix='t3-browser-file-')
 total=0
 with os.fdopen(out,'wb') as target:
  while True:
   chunk=os.read(fd,min(65536,limit-total+1))
   if not chunk: break
   total+=len(chunk)
   if total>limit: sys.exit(74)
   target.write(chunk)
 os.chmod(staged,0o400)
 print(staged)
 staged=None
finally:
 os.close(fd)
 if staged is not None: os.unlink(staged)
`;

/** Snapshot before streaming, then cap the actual bytes as well as the preflight size. */
export async function readRemoteFile(input: {
  readonly sandbox: Sandbox;
  readonly client: SandboxClient;
  readonly root: string;
  readonly relative: string;
  readonly maxBytes: number;
}): Promise<Buffer> {
  const { sandbox, client, root, relative, maxBytes } = input;
  if (
    !root.startsWith("/") ||
    !relative ||
    relative.length > 1024 ||
    // eslint-disable-next-line no-control-regex -- Reject control bytes at the remote file trust boundary.
    /[\\\x00-\x1f\x7f]/.test(relative) ||
    relative.split("/").some((part) => !part || part === "." || part === "..") ||
    !Number.isSafeInteger(maxBytes) ||
    maxBytes < 0
  ) {
    throw new Error("Invalid remote file path or size limit.");
  }
  const command = await sandbox.runCommand({
    cmd: "/usr/bin/timeout",
    args: ["30", "/usr/bin/python3", "-c", SNAPSHOT_FILE, root, relative, String(maxBytes)],
  });
  const result = await command.wait();
  if (result.exitCode !== 0)
    throw new Error("The remote file is unavailable, unsafe, or exceeds the size limit.");
  const path = (await command.stdout()).trim();
  if (!/^\/tmp\/t3-browser-file-[a-zA-Z0-9_-]+$/.test(path))
    throw new Error("Invalid remote file snapshot.");
  const abort = new AbortController();
  try {
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    for await (const chunk of client.rpc.readFile(
      { sandbox: { selector: { case: "id", value: sandbox.sandboxId } }, path },
      { signal: AbortSignal.any([abort.signal, AbortSignal.timeout(30_000)]) },
    )) {
      bytes += chunk.data.byteLength;
      if (bytes > maxBytes) throw new Error("The remote file exceeds the size limit.");
      chunks.push(chunk.data);
    }
    return Buffer.concat(chunks);
  } finally {
    abort.abort();
    await sandbox
      .fs()
      .unlink(path)
      .catch(() => {});
  }
}
