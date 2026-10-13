import { useAtomValue } from "@effect/atom-react";
import { ThreadId, type EnvironmentId, type PreviewSessionSnapshot } from "@t3tools/contracts";
import { useEffect, useRef, useState } from "react";

import { randomUUID } from "~/lib/utils";
import { Button } from "~/components/ui/button";
import { previewEnvironment } from "~/state/preview";
import { useAtomCommand } from "~/state/use-atom-command";

/** Status polling observes a lease; only real browser input extends it. */
export function BrowserWorkspaceControls({
  environmentId,
  tab,
  visible,
}: {
  readonly environmentId: EnvironmentId;
  readonly tab: PreviewSessionSnapshot;
  readonly visible: boolean;
}) {
  const adjust = useAtomCommand(previewEnvironment.adjust, { reportFailure: false });
  const mutate = useAtomCommand(previewEnvironment.adjust, "remote workspace change");
  const canOperate = useAtomValue(previewEnvironment.adjust.permissionAtom(environmentId));
  const [busy, setBusy] = useState(false);
  const recreateRequest = useRef<string | null>(null);
  const target = { threadId: ThreadId.make(tab.threadId), tabId: tab.tabId };
  useEffect(() => {
    if (!visible || !canOperate) return;
    const poll = () => {
      void adjust({
        environmentId,
        input: {
          threadId: ThreadId.make(tab.threadId),
          tabId: tab.tabId,
          workspaceAction: "status",
        },
      });
    };
    poll();
    const timer = setInterval(poll, 15_000);
    return () => clearInterval(timer);
  }, [adjust, canOperate, environmentId, tab.threadId, tab.tabId, visible]);
  const workspace = tab.workspace;
  const terminal = workspace?.state === "stopped" || workspace?.state === "expired";
  const run = async (action: "stop" | "recreate") => {
    setBusy(true);
    if (action === "recreate") recreateRequest.current ??= randomUUID();
    try {
      const result = await mutate({
        environmentId,
        input: {
          ...target,
          workspaceAction: action,
          ...(action === "recreate" ? { workspaceRequestId: recreateRequest.current! } : {}),
        },
      });
      if (result._tag === "Success") recreateRequest.current = null;
    } finally {
      setBusy(false);
    }
  };
  return (
    <div
      className="flex items-center gap-2 border-b border-border px-3 py-1 text-xs"
      aria-label="Remote workspace"
    >
      <span role="status" className="min-w-0 flex-1">
        Depot · {workspace?.state ?? "Starting"}
        {workspace
          ? ` · $${(workspace.accruedMicroUsd / 1_000_000).toFixed(2)} used · $${(workspace.remainingMicroUsd / 1_000_000).toFixed(2)} left`
          : ""}
        {workspace?.state === "running"
          ? ` · Idle deadline ${new Date(Math.min(workspace.idleExpiresAt, workspace.absoluteExpiresAt)).toLocaleTimeString()}`
          : ""}
        {workspace?.reason ? ` · ${workspace.reason}` : ""}
      </span>
      {terminal ? (
        <Button
          size="compact"
          variant="outline"
          disabled={!canOperate || busy}
          onClick={() => void run("recreate")}
        >
          Recreate workspace
        </Button>
      ) : (
        <Button
          size="compact"
          variant="outline"
          disabled={!canOperate || busy || !workspace || workspace.state === "stop-pending"}
          onClick={() => void run("stop")}
        >
          Stop workspace
        </Button>
      )}
    </div>
  );
}
