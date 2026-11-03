import Link from "next/link";
import { AppShell } from "@/components/app-shell";
import { EmptyState, ErrorState, Panel } from "@/components/panel";
import { MetaRow } from "@/components/meta-row";
import { Badge, freshnessTone } from "@/components/ui/badge";
import { getOverview, getRecentGrants, redactedErrorMessage } from "@/server/ops-client";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  let overviewError: string | null = null;
  let grantsError: string | null = null;
  let overview = null;
  let grants = null;
  try {
    overview = await getOverview();
  } catch (err) {
    overviewError = redactedErrorMessage(err);
  }
  try {
    grants = await getRecentGrants();
  } catch (err) {
    grantsError = redactedErrorMessage(err);
  }

  return (
    <AppShell active="/">
      <div className="mb-4 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Overview</h1>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Configured owner sources and snapshot freshness. This is not a global health
          dashboard. SELECT 1 / DB readable ≠ all systems healthy.
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel
          title="Snapshot sources"
          description="EXPLICIT_INVENTORY_PARTIAL_NO_HISTORY — other consumers, broker ACK and DLT remain unknown."
        >
          {overviewError ? <ErrorState>{overviewError}</ErrorState> : null}
          {overview ? (
            <div className="space-y-3">
              <MetaRow
                asOf={overview.asOf}
                coverage={overview.coverage}
                uncertainty={overview.uncertainty}
              />
              {overview.data.length === 0 ? (
                <EmptyState>No configured snapshot sources.</EmptyState>
              ) : (
                <ul className="space-y-2">
                  {overview.data.map((row) => (
                    <li
                      key={row.source}
                      className="rounded border border-zinc-200 px-3 py-2 dark:border-zinc-700"
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-medium">{row.source}</span>
                        <Badge tone={freshnessTone(row.freshness)}>{row.freshness}</Badge>
                        <Badge tone="neutral">{row.uncertainty}</Badge>
                      </div>
                      <div className="mt-1 text-xs text-zinc-500">
                        epoch {row.snapshot?.epoch ?? "—"} · observedAt{" "}
                        {row.snapshot?.observedAt ?? "missing"} · DB/readiness facts are
                        owner-scoped observations only
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          ) : null}
        </Panel>

        <Panel
          title="Recent-search grants"
          description="Explicit configured scopes for /ops/v1/recent/*. Not a live membership census."
        >
          {grantsError ? <ErrorState>{grantsError}</ErrorState> : null}
          {grants ? (
            grants.length === 0 ? (
              <EmptyState>No recent grants configured / search disabled.</EmptyState>
            ) : (
              <ul className="space-y-2 text-sm">
                {grants.map((g) => (
                  <li key={`${g.owner}-${g.kind}-${g.scope}`} className="font-mono text-xs">
                    {g.environment}/{g.owner}/{g.kind}/{g.scope}
                  </li>
                ))}
              </ul>
            )
          ) : null}
          <div className="mt-4 flex flex-wrap gap-2 text-sm">
            <Link className="underline" href="/events">
              Recent Events
            </Link>
            <Link className="underline" href="/workflows">
              Workflows
            </Link>
            <Link className="underline" href="/diagnostics">
              Diagnostics
            </Link>
          </div>
        </Panel>
      </div>
    </AppShell>
  );
}
