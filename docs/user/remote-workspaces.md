# Remote workspaces

A conversation can run Chrome, Playwright, an application and its supporting
services together in a disposable Depot workspace. Different conversations have
separate files, processes and browser profiles, so they can use the same ports.
The Browser panel shows the remote browser and retains its existing human and
agent control rules.

The environment owner enables this for every project by setting `DEPOT_API_TOKEN`
and `DEPOT_ORG_ID` in the T3 server's runtime environment. Keep the token in your
secret manager; do not put it in project files. No Tailscale connection is needed.
New Browser tabs use Depot when enabled; existing local tabs stay local.

Ask the agent to start a workspace, sync the current candidate, and run the
project's own setup and test commands. Git-tracked changes are included; select
new files explicitly. Credentials, environment files, dependencies and caches
are excluded, along with generated builds, test evidence and reports. Source
snapshots allow up to 10,000 files, 8 MiB per file and 128 MiB total; symlinks are
refused. Projects outside Git require an explicit file selection.
The editable local project remains the source of truth. Collect remote results
and evidence before stopping; collection does not overwrite your source files.
Each new sync resets the remote working copy to the selected local candidate.
Running services retain their previous working directory until explicitly
restarted, so collect their results before syncing another candidate.

Workspaces survive conversation turns. They expire after 30 minutes without
human or agent activity. A bounded foreground job can keep a workspace alive
until its deadline; a dev server or status poll alone does not. A workspace ends
after at most 24 hours. Export what you need and explicitly recreate it; running
processes and open tabs are not restored. Restarting the T3 server ends its
previous workspace leases.

The default compute budget is $100 per UTC month across all managed workspaces.
At the published 4-CPU rate, one workspace costs about $0.72 per hour, including
idle time. T3 reserves estimated exposure before allocating or extending a
workspace. Budget exhaustion requests immediate shutdown of every managed
workspace, including active jobs. This estimate does not include unrelated Depot
usage or guarantee the provider's final invoice during an outage.

Use **Stop workspace** in the Browser panel or ask the agent to stop it when
finished. The owner can lower the monthly budget with
`DEPOT_WORKSPACES_MONTHLY_BUDGET_MICRO_USD` (integer microdollars, at most
`100000000`), or set `DEPOT_WORKSPACES_DISABLED=1` to disable new workspaces and
request shutdown of existing managed workspaces.
