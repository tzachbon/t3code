import { useAtomValue } from "@effect/atom-react";
import { ThreadId, type EnvironmentId, type PreviewSessionSnapshot } from "@t3tools/contracts";
import { useEffect, useRef, useState } from "react";
import { Pressable, View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { uuidv4 } from "../../lib/uuid";
import { previewEnvironment } from "../../state/preview";
import { useAtomCommand } from "../../state/use-atom-command";

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
  const run = async () => {
    setBusy(true);
    if (terminal) recreateRequest.current ??= uuidv4();
    try {
      const result = await mutate({
        environmentId,
        input: {
          threadId: ThreadId.make(tab.threadId),
          tabId: tab.tabId,
          workspaceAction: terminal ? "recreate" : "stop",
          ...(terminal ? { workspaceRequestId: recreateRequest.current! } : {}),
        },
      });
      if (result._tag === "Success") recreateRequest.current = null;
    } finally {
      setBusy(false);
    }
  };
  const disabled = !canOperate || busy || !workspace || workspace.state === "stop-pending";
  return (
    <View className="flex-row items-center gap-2 border-b border-border px-3 py-2">
      <Text accessibilityLiveRegion="polite" className="flex-1 text-xs text-muted-foreground">
        Depot · {workspace?.state ?? "Starting"}
        {workspace
          ? ` · $${(workspace.accruedMicroUsd / 1_000_000).toFixed(2)} used · $${(workspace.remainingMicroUsd / 1_000_000).toFixed(2)} left`
          : ""}
        {workspace?.state === "running"
          ? ` · Idle deadline ${new Date(Math.min(workspace.idleExpiresAt, workspace.absoluteExpiresAt)).toLocaleTimeString()}`
          : ""}
        {workspace?.reason ? ` · ${workspace.reason}` : ""}
      </Text>
      <Pressable
        accessibilityRole="button"
        accessibilityLabel={terminal ? "Recreate workspace" : "Stop workspace"}
        accessibilityState={{ disabled }}
        disabled={disabled}
        onPress={() => void run()}
        className="rounded-md border border-border px-3 py-2"
      >
        <Text className="text-xs text-foreground">{terminal ? "Recreate" : "Stop"}</Text>
      </Pressable>
    </View>
  );
}
