import { AppShell } from "@/components/app-shell";
import { EmptyState, ErrorState, Panel } from "@/components/panel";
import { MetaRow } from "@/components/meta-row";
import { Badge, freshnessTone } from "@/components/ui/badge";
import { getActiveOperations, redactedErrorMessage } from "@/server/ops-client";
import { elapsedLabel, formatLocal, formatUtc } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function OperationsPage() {
  let error: string | null = null;
  let active = null;
  try {
    active = await getActiveOperations();
  } catch (err) {
    error = redactedErrorMessage(err);
  }

  return (
    <AppShell active="/operations">
      <div className="mb-4 space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Active Operations</h1>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Bounded registry observations. Stage field is operation kind (e.g. RPC_SERVER), not
          internal runtime stage. No inferred deadline, ETA, completion-on-timeout, or global
          in-flight census.
        </p>
      </div>

      {error ? <ErrorState>{error}</ErrorState> : null}
      {active ? (
        <Panel title="Registry samples" description={active.coverage}>
          <MetaRow asOf={active.asOf} coverage={active.coverage} uncertainty={active.uncertainty} />
          <div className="mt-4 space-y-3">
            {active.data.map((row) => (
              <article
                key={row.source}
                className="rounded border border-zinc-200 p-3 dark:border-zinc-700"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <h3 className="font-medium">{row.source}</h3>
                  <Badge tone={freshnessTone(row.freshness)}>{row.freshness}</Badge>
                  <Badge tone="neutral">
                    registry {row.registryAvailable ? "available" : "unavailable"}
                  </Badge>
                </div>
                <p className="mt-1 text-xs text-zinc-500">
                  lastObservedAt UTC {formatUtc(row.lastObservedAt)} · local{" "}
                  {formatLocal(row.lastObservedAt)} · expired {row.expired} · rejected{" "}
                  {row.rejected}
                </p>
                <p className="text-xs text-zinc-500">{row.coverage}</p>
                {row.attempts.length === 0 ? (
                  <EmptyState>
                    No current attempts in this observation (or freshness is not FRESH).
                  </EmptyState>
                ) : (
                  <table className="mt-2 min-w-full text-left text-sm">
                    <thead className="text-xs uppercase text-zinc-500">
                      <tr>
                        <th className="py-1 pr-2">operationId</th>
                        <th className="py-1 pr-2">boot epoch</th>
                        <th className="py-1 pr-2">kind (stage)</th>
                        <th className="py-1">elapsed</th>
                      </tr>
                    </thead>
                    <tbody>
                      {row.attempts.map((a) => (
                        <tr key={a.operationId} className="border-t border-zinc-100 dark:border-zinc-800">
                          <td className="py-1 pr-2 font-mono text-xs">{a.operationId}</td>
                          <td className="py-1 pr-2 font-mono text-xs">{a.epoch}</td>
                          <td className="py-1 pr-2">{a.stage}</td>
                          <td className="py-1">{elapsedLabel(a.elapsedNanos)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </article>
            ))}
          </div>
        </Panel>
      ) : null}
    </AppShell>
  );
}
