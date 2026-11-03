import { AppShell } from "@/components/app-shell";
import { ErrorState, Panel } from "@/components/panel";
import { Badge } from "@/components/ui/badge";
import { getRecentGrants, redactedErrorMessage } from "@/server/ops-client";

export const dynamic = "force-dynamic";

export default async function DiagnosticsPage() {
  let grantsError: string | null = null;
  let grants = null;
  try {
    grants = await getRecentGrants();
  } catch (err) {
    grantsError = redactedErrorMessage(err);
  }

  return (
    <AppShell active="/diagnostics">
      <div className="mb-4 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Diagnostics</h1>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Safe operational guidance only. No arbitrary logs, SQL, PromQL, LogQL, or trace URL
          proxy.
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Contract reminders">
          <ul className="list-disc space-y-2 pl-5 text-sm">
            <li>
              sourceMode = <Badge tone="neutral">OWNER_DIRECT</Badge> — no durable Operations
              projection.
            </li>
            <li>
              Broker ACK / quarantine / other consumers remain{" "}
              <Badge tone="warning">UNKNOWN</Badge>.
            </li>
            <li>
              relation = <Badge tone="neutral">RELATION_UNKNOWN</Badge> — do not invent causal
              links.
            </li>
            <li>
              LATE_COMMIT_GAP and RECONCILE_FROM_START must stay visible; a new walk can find
              older late commits but never promises completeness.
            </li>
            <li>
              Cursor is opaque to this UI. Expired/tampered cursor → restart walk (≤8 pages).
            </li>
            <li>
              FRESH describes the observation sample, not global business health.
            </li>
          </ul>
        </Panel>

        <Panel title="Release / regression blockers">
          <ul className="list-disc space-y-2 pl-5 text-sm">
            <li>
              OPS-03D FULL regression remains failed (retained native PG / deadline / KRaft
              observations). This UI does not relabel that result.
            </li>
            <li>General Event Explorer and durable projection are BLOCKED.</li>
            <li>M36 OAuth resource/delegation is not deployed — no verified human/tenant authz.</li>
            <li>
              Replay / Retry / Pause / Resume buttons are disabled and no mutation BFF route is
              registered.
            </li>
          </ul>
        </Panel>

        <Panel title="Configured grants (safe list)">
          {grantsError ? <ErrorState>{grantsError}</ErrorState> : null}
          {grants ? (
            <ul className="space-y-1 font-mono text-xs">
              {grants.map((g) => (
                <li key={`${g.owner}-${g.kind}-${g.scope}`}>
                  {g.environment}/{g.owner}/{g.kind}/{g.scope}
                </li>
              ))}
            </ul>
          ) : null}
        </Panel>

        <Panel title="Manual operator steps">
          <ol className="list-decimal space-y-2 pl-5 text-sm">
            <li>Confirm Java Console is bound to 127.0.0.1:9190 with ops03d search enabled.</li>
            <li>Start this app with `pnpm dev` (already forces 127.0.0.1).</li>
            <li>Use Search / Refresh explicitly; keep auto-polling off for lists.</li>
            <li>
              If availability=RESTART_WALK_REQUIRED, restart the walk with the same from/to — do
              not continue a stale cursor.
            </li>
            <li>Never tunnel/proxy this UI or Console into another trust domain.</li>
          </ol>
        </Panel>
      </div>
    </AppShell>
  );
}
