import { AppShell } from "@/components/app-shell";
import { RecentExplorer } from "@/components/recent-table";

export default function WorkflowsPage() {
  return (
    <AppShell active="/workflows">
      <div className="mb-4 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Workflows</h1>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Owner-specific scopes (order, reservation, authorization-simulation). Creation time ≠
          state-entry age. CANCELLING remains until owner acknowledgement. No fabricated
          transition history.
        </p>
      </div>
      <div className="space-y-4">
        <RecentExplorer
          kind="workflows"
          title="Order workflows"
          description="scope=order"
          defaultOwner="order-cell"
          defaultScope="order"
        />
        <RecentExplorer
          kind="workflows"
          title="Inventory reservations"
          description="scope=reservation"
          defaultOwner="inventory-cell"
          defaultScope="reservation"
        />
        <RecentExplorer
          kind="workflows"
          title="Payment authorization simulation"
          description="Not a bank/PSP settlement workflow"
          defaultOwner="payment-cell"
          defaultScope="authorization-simulation"
          paymentNote
        />
      </div>
    </AppShell>
  );
}
