import { AppShell } from "@/components/app-shell";
import { ErrorState } from "@/components/panel";
import { TopologyView } from "@/components/topology-view";
import { getTopology, redactedErrorMessage } from "@/server/ops-client";
import type { Topology } from "@/lib/topology";

export const dynamic = "force-dynamic";
export default async function TopologyPage() {
  let graph: Topology | null = null;
  let error: string | null = null;
  try { graph = await getTopology(); } catch (err) { error = redactedErrorMessage(err); }
  return <AppShell active="/topology">
    <h1 className="mb-2 text-2xl font-semibold tracking-tight">Topology</h1>
    <p className="mb-4 text-sm text-zinc-600 dark:text-zinc-400">Apex Control Center · Read-only service and instance evidence</p>
    {error ? <ErrorState>{error}</ErrorState> : null}
    {graph ? <TopologyView graph={graph} /> : null}
  </AppShell>;
}
