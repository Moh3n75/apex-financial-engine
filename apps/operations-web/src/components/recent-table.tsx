"use client";

import { useMemo, useState } from "react";
import useSWR from "swr";
import { Badge } from "@/components/ui/badge";
import { EmptyState, ErrorState, Panel } from "@/components/panel";
import { MetaRow } from "@/components/meta-row";
import { formatLocal, formatUtc } from "@/lib/format";
import type { RecentView } from "@/server/schemas";

type Kind = "events" | "inbox" | "requests" | "workflows";

async function fetcher(url: string): Promise<RecentView> {
  const res = await fetch(url, { cache: "no-store" });
  const body = await res.json();
  if (!res.ok) throw new Error(body.message ?? "Request failed");
  return body as RecentView;
}

function defaultWindow(minutes: number) {
  const to = new Date();
  const from = new Date(to.getTime() - minutes * 60_000);
  return {
    from: from.toISOString().replace(/\.\d{3}Z$/, "Z"),
    to: to.toISOString().replace(/\.\d{3}Z$/, "Z"),
  };
}

export function RecentExplorer({
  kind,
  title,
  description,
  defaultOwner,
  defaultScope,
  paymentNote,
}: {
  kind: Kind;
  title: string;
  description: string;
  defaultOwner: string;
  defaultScope: string;
  paymentNote?: boolean;
}) {
  const initial = useMemo(() => defaultWindow(15), []);
  const [environment, setEnvironment] = useState("test");
  const [owner, setOwner] = useState(defaultOwner);
  const [scope, setScope] = useState(defaultScope);
  const [state, setState] = useState("ANY");
  const [limit, setLimit] = useState(16);
  const [from, setFrom] = useState(initial.from);
  const [to, setTo] = useState(initial.to);
  const [cursor, setCursor] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [committed, setCommitted] = useState({
    environment,
    owner,
    scope,
    state,
    limit,
    from,
    to,
  });
  const [refreshKey, setRefreshKey] = useState(0);
  // Do not hit Console until the operator submits Search (avoids mount fan-out / SHED).
  const [armed, setArmed] = useState(false);

  const qs = new URLSearchParams({
    environment: committed.environment,
    owner: committed.owner,
    scope: committed.scope,
    from: committed.from,
    to: committed.to,
    state: committed.state,
    limit: String(committed.limit),
  });
  if (cursor) qs.set("cursor", cursor);

  const apiPath = `/api/ops/recent/${kind}?${qs.toString()}`;
  // refreshKey is SWR-key only — never forwarded as a query param (BFF allowlists keys).
  const { data, error, isLoading, mutate } = useSWR(
    armed ? ([apiPath, refreshKey] as const) : null,
    ([url]) => fetcher(url),
    { revalidateOnFocus: false, revalidateOnReconnect: false },
  );

  function applySearch() {
    setCursor("");
    setPageIndex(0);
    setCommitted({ environment, owner, scope, state, limit, from, to });
    setArmed(true);
    setRefreshKey((v) => v + 1);
  }

  function quick(minutes: number) {
    const w = defaultWindow(minutes);
    setFrom(w.from);
    setTo(w.to);
  }

  const coverage = data?.page?.coverage ?? [];
  const items = data?.page?.items ?? [];
  const next = data?.page?.nextCursor ?? "";
  const restartRequired =
    data?.availability === "RESTART_WALK_REQUIRED" ||
    coverage.includes("RESTART_WALK_REQUIRED");

  return (
    <Panel title={title} description={description}>
      {paymentNote ? (
        <div className="mb-3 rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-950 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-100">
          Payment scope is <strong>authorization-simulation</strong> only — not bank/PSP
          settlement.
        </div>
      ) : null}

      <form
        className="mb-4 grid gap-3 md:grid-cols-3"
        onSubmit={(e) => {
          e.preventDefault();
          applySearch();
        }}
      >
        <label className="text-xs">
          Environment
          <input
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-950"
            value={environment}
            onChange={(e) => setEnvironment(e.target.value)}
          />
        </label>
        <label className="text-xs">
          Owner
          <input
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-950"
            value={owner}
            onChange={(e) => setOwner(e.target.value)}
          />
        </label>
        <label className="text-xs">
          Scope
          <input
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-950"
            value={scope}
            onChange={(e) => setScope(e.target.value)}
          />
        </label>
        <label className="text-xs">
          From (UTC Z)
          <input
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-950"
            value={from}
            onChange={(e) => setFrom(e.target.value)}
          />
        </label>
        <label className="text-xs">
          To (UTC Z)
          <input
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-950"
            value={to}
            onChange={(e) => setTo(e.target.value)}
          />
        </label>
        <label className="text-xs">
          State
          <input
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-950"
            value={state}
            onChange={(e) => setState(e.target.value)}
          />
        </label>
        <label className="text-xs">
          Limit (1–16)
          <input
            type="number"
            min={1}
            max={16}
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-950"
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value))}
          />
        </label>
        <div className="flex flex-wrap items-end gap-2">
          <button
            type="button"
            className="rounded border border-zinc-300 px-2 py-1.5 text-xs dark:border-zinc-700"
            onClick={() => quick(5)}
          >
            Last 5m
          </button>
          <button
            type="button"
            className="rounded border border-zinc-300 px-2 py-1.5 text-xs dark:border-zinc-700"
            onClick={() => quick(15)}
          >
            Last 15m
          </button>
          <button
            type="button"
            className="rounded border border-zinc-300 px-2 py-1.5 text-xs dark:border-zinc-700"
            onClick={() => quick(60)}
          >
            Last 60m
          </button>
        </div>
        <div className="flex items-end gap-2">
          <button
            type="submit"
            className="rounded bg-zinc-900 px-3 py-1.5 text-sm text-white dark:bg-zinc-100 dark:text-zinc-900"
          >
            Search
          </button>
          <button
            type="button"
            className="rounded border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-700"
            onClick={() => {
              setRefreshKey((v) => v + 1);
              void mutate();
            }}
          >
            Refresh
          </button>
        </div>
      </form>

      <p className="mb-3 text-xs text-zinc-500">
        Automatic polling is OFF. Window bounds are frozen for a walk (max 8 pages). Changing
        filters mid-walk is rejected by restarting from page 1. sourceAssignedAt is creation /
        assigned time — not COMMIT.
      </p>

      {!armed ? (
        <EmptyState>Submit Search to request a bounded owner observation (no automatic fetch).</EmptyState>
      ) : null}
      {error ? <ErrorState>{error.message}</ErrorState> : null}
      {armed && isLoading && !data ? <EmptyState>Loading observation…</EmptyState> : null}

      {data ? (
        <div className="space-y-3">
          <MetaRow
            asOf={data.asOf}
            receivedAt={data.receivedAt}
            freshness={data.freshness}
            availability={data.availability}
            coverage={data.coverage}
            uncertainty={data.uncertainty}
            sourceMode={data.sourceMode}
          />
          <div className="flex flex-wrap gap-1">
            <Badge tone="neutral">BROKER ACK {data.brokerAck}</Badge>
            <Badge tone="neutral">QUARANTINE {data.quarantine}</Badge>
            <Badge tone="neutral">OTHER CONSUMERS UNKNOWN</Badge>
            <Badge tone="neutral">DLT NOT INSPECTED</Badge>
            {coverage.map((g) => (
              <Badge key={g} tone={g.includes("LATE") || g.includes("WALK") ? "warning" : "info"}>
                {g}
              </Badge>
            ))}
          </div>

          {restartRequired ? (
            <ErrorState>
              Cursor/index precondition requires restart. Clear cursor and Search again — do not
              claim lossless continuation.
            </ErrorState>
          ) : null}

          {!data.page ? (
            <EmptyState>
              No page payload ({data.availability}). This is not proof of zero backlog.
            </EmptyState>
          ) : items.length === 0 ? (
            <EmptyState>
              No matching rows in observed candidate window. Incomplete coverage may still apply
              ({coverage.join(", ") || "see coverage"}). An empty post-filter result does not mean
              zero events/backlog.
            </EmptyState>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="border-b border-zinc-200 text-xs uppercase tracking-wide text-zinc-500 dark:border-zinc-700">
                  <tr>
                    <th className="px-2 py-2">Ref</th>
                    <th className="px-2 py-2">State</th>
                    <th className="px-2 py-2">sourceAssignedAt</th>
                    <th className="px-2 py-2">Evidence</th>
                    <th className="px-2 py-2">Certainty</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((item) => (
                    <tr key={item.ref} className="border-b border-zinc-100 dark:border-zinc-800">
                      <td className="px-2 py-2 font-mono text-xs">{item.ref}</td>
                      <td className="px-2 py-2">
                        <Badge tone="neutral">{item.state}</Badge>
                      </td>
                      <td className="px-2 py-2 text-xs">
                        <div>UTC {formatUtc(item.sourceAssignedAt)}</div>
                        <div className="text-zinc-500">local {formatLocal(item.sourceAssignedAt)}</div>
                        <div className="text-zinc-500">created/assigned — not commit</div>
                      </td>
                      <td className="px-2 py-2 font-mono text-xs">
                        {item.evidenceClass}
                        <div>{item.evidenceKey}</div>
                      </td>
                      <td className="px-2 py-2 text-xs">{item.certainty}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <div className="flex flex-wrap items-center gap-2 text-xs">
            <span>
              Page {pageIndex + 1} / ≤8 · candidatesExamined{" "}
              {data.page?.candidatesExamined ?? "—"} · relation{" "}
              {data.page?.relation ?? "RELATION_UNKNOWN"}
            </span>
            <button
              type="button"
              disabled={!next || pageIndex >= 7 || restartRequired}
              className="rounded border border-zinc-300 px-2 py-1 disabled:opacity-40 dark:border-zinc-700"
              onClick={() => {
                setCursor(next);
                setPageIndex((p) => p + 1);
              }}
            >
              Next page
            </button>
            <button
              type="button"
              className="rounded border border-zinc-300 px-2 py-1 dark:border-zinc-700"
              onClick={() => {
                setCursor("");
                setPageIndex(0);
                setRefreshKey((v) => v + 1);
              }}
            >
              Restart search
            </button>
          </div>
        </div>
      ) : null}
    </Panel>
  );
}
