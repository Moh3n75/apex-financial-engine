import { AppShell } from "@/components/app-shell";
import { EmptyState, ErrorState, Panel } from "@/components/panel";
import { MetaRow } from "@/components/meta-row";
import { Badge, freshnessTone } from "@/components/ui/badge";
import { getCells, getRecentGrants, redactedErrorMessage } from "@/server/ops-client";
import { formatLocal, formatUtc } from "@/lib/format";
import type { RecentGrant } from "@/server/schemas";

export const dynamic = "force-dynamic";

export default async function CellsPage() {
  let error: string | null = null;
  let cells = null;
  let grants: RecentGrant[] = [];
  try {
    cells = await getCells(0, 8);
    try {
      grants = await getRecentGrants();
    } catch {
      grants = [];
    }
  } catch (err) {
    error = redactedErrorMessage(err);
  }

  return (
    <AppShell active="/cells">
      <div className="mb-4 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Cells & Sources</h1>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          One logical owner/endpoint snapshot at a time. Do not invent all-live membership or
          shared backlog across replicas.
        </p>
      </div>

      {error ? <ErrorState>{error}</ErrorState> : null}
      {cells ? (
        <Panel title="Owner snapshots" description={cells.coverage}>
          <MetaRow asOf={cells.asOf} coverage={cells.coverage} uncertainty={cells.uncertainty} />
          <div className="mt-4 space-y-3">
            {cells.data.map((row) => {
              const health = row.snapshot?.facts.filter((f) => f.kind === "HEALTH") ?? [];
              return (
                <article
                  key={row.source}
                  className="rounded border border-zinc-200 p-3 dark:border-zinc-700"
                >
                  <div className="flex flex-wrap items-center gap-2">
                    <h3 className="font-medium">{row.source}</h3>
                    <Badge tone={freshnessTone(row.freshness)}>{row.freshness}</Badge>
                  </div>
                  <p className="mt-1 text-xs text-zinc-500">
                    reporter epoch {row.snapshot?.epoch ?? "—"} · receivedAt UTC{" "}
                    {formatUtc(row.receivedAt)} · local {formatLocal(row.receivedAt)}
                  </p>
                  {row.snapshot == null ? (
                    <EmptyState>Snapshot MISSING for this owner.</EmptyState>
                  ) : (
                    <ul className="mt-2 space-y-1 text-sm">
                      {health.map((h) => (
                        <li key={h.factKey}>
                          {h.scope}: <Badge tone="neutral">{h.state}</Badge> ({h.certainty}) —
                          not inferred global HEALTHY
                        </li>
                      ))}
                      {health.length === 0 ? (
                        <li className="text-zinc-500">No HEALTH facts in this inventory sample.</li>
                      ) : null}
                    </ul>
                  )}
                </article>
              );
            })}
          </div>
        </Panel>
      ) : null}

      <div className="mt-4">
        <Panel title="Configured recent scope grants">
          {!grants || grants.length === 0 ? (
            <EmptyState>Recent grants unavailable or empty.</EmptyState>
          ) : (
            <ul className="space-y-1 font-mono text-xs">
              {grants.map((g) => (
                <li key={`${g.owner}-${g.kind}-${g.scope}`}>
                  {g.owner} · {g.kind} · {g.scope}
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>
    </AppShell>
  );
}
