import { AppShell } from "@/components/app-shell";
import { RecentExplorer } from "@/components/recent-table";

export default function EventsPage() {
  return (
    <AppShell active="/events">
      <div className="mb-4 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Recent Events</h1>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Indexed recent Outbox current facts via /ops/v1/recent/events. Not a complete event
          timeline. Discovering a ref does not authorize old /events/&#123;id&#125; detail.
        </p>
      </div>
      <RecentExplorer
        kind="events"
        title="OUTBOX_EVENT recent walk"
        description="Limit ≤16 · ≤8 pages · one-hour UTC window · OWNER_DIRECT"
        defaultOwner="order-cell"
        defaultScope="owner"
      />
    </AppShell>
  );
}
