import { AppShell } from "@/components/app-shell";
import { RecentExplorer } from "@/components/recent-table";
import { Panel } from "@/components/panel";

export default function RequestsPage() {
  return (
    <AppShell active="/requests">
      <div className="mb-4 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Requests</h1>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Only when an independent REQUEST_RECEIPT grant is configured. Receipt identity is
          separate from event identity. Cached response bytes, idempotency keys and
          principal/client details are not exposed.
        </p>
      </div>
      <Panel title="Privacy bounds" description="UI intentionally omits receipt payload fields.">
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          If the grant is missing, Console returns SCOPE_DENIED / empty — shown as an error or
          unavailable state, not invented rows.
        </p>
      </Panel>
      <div className="mt-4">
        <RecentExplorer
          kind="requests"
          title="REQUEST_RECEIPT recent walk"
          description="Requires owner receipt-client + independent order-request grant"
          defaultOwner="order-cell"
          defaultScope="order-request"
        />
      </div>
    </AppShell>
  );
}
