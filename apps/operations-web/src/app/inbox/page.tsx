import { AppShell } from "@/components/app-shell";
import { RecentExplorer } from "@/components/recent-table";

export default function InboxPage() {
  return (
    <AppShell active="/inbox">
      <div className="mb-4 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Known-Group Inbox</h1>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Only the configured group scope&apos;s committed handler-row evidence. No global
          PROCESSED label or consumer census.
        </p>
      </div>
      <RecentExplorer
        kind="inbox"
        title="KNOWN_GROUP_INBOX recent walk"
        description="Scope must be the registered known-group. Retention/coverage limitations apply."
        defaultOwner="inventory-cell"
        defaultScope="inventory-three-demo"
      />
    </AppShell>
  );
}
