# T3 Code

T3 Code is a minimal GUI for coding agents. A Node WebSocket server wraps provider CLIs and agents (Codex, Claude Code, Cursor, Grok, OpenCode, Antigravity) and serves web, desktop, and mobile clients.

Everything here is a good default; the developer's preferences override it. In replies, **you** are the agent reading this; **we** (maintainers) are Theo, Julius and the T3 Code team you are talking to; a **user** directs coding agents inside T3 Code. Other shared terms: [glossary](docs/internals/glossary.md).

## The three ways to hurt yourself

Most contributions come from T3 Code itself, often controlled remotely, so the instance you run inside is live.

1. **Killing by pattern.** Never `pkill -f`, `pgrep | kill`, or `kill` a PID you found by matching a name, path, or worktree string. Your own agent process has this worktree's path in its argv, and this machine runs several other dev servers at once. Kill only a PID you captured at spawn, or the owner of your port from `ss -H -ltnp` after confirming `/proc/<pid>/cwd` is your worktree.
2. **Writing to the live install.** `~/.t3/userdata` is the developer's real T3 Code database, in use while you work. Reading and copying it are fine ([test data](docs/operations/development.md#test-data)). Never start a server against it, never open it read-write, never clean it up.
3. **Baking in origins.** Never set `VITE_HTTP_URL` or `VITE_WS_URL` for dev. Dev is single-origin and Vite proxies `/api`, `/ws`, `/oauth`, and `/.well-known`. Setting them bakes localhost into the bundle and silently breaks every remote browser.

## Verifying

Prove the change with the smallest check: `vp test run <files>` for the tests you touched, plus targeted lint and typecheck. CI owns the full suite; run repo-wide checks (`vp check`, `vp run -r test`, `vp run -r typecheck`) only when asked. Ask before computer use or opening browsers.

## Before the named work, read

- Writing or reviewing code: [CODING_STANDARDS.md](CODING_STANDARDS.md); before calling frontend work done, walk [hit every surface](CODING_STANDARDS.md#hit-every-surface) and say which entries applied.
- Starting, sharing or seeding a dev server, or verifying in a real client: [agent dev servers](docs/operations/development.md#agent-dev-servers).
- Adding or changing docs: [documentation rules](docs/README.md#documentation-rules).
- Opening a PR (only when the developer asks for one), babysitting it, or writing plans and notes: [pull requests and work artifacts](CODING_STANDARDS.md#pull-requests-and-work-artifacts).

Commit and PR titles are conventional commits in plain language: `fix(web): new threads no longer spike CPU`.
