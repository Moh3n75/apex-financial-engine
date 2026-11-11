import "server-only";
import { topologySchema, type Topology } from "@/lib/topology";
import { OperationsOriginError } from "@/lib/origin-policy";
import { optionalUpstreamBearer } from "./auth/access";
import { isM36aEnabled } from "./auth/config";
import { AuthError } from "./auth/errors";
import { isFixtureMode, resolveOperationsOrigin } from "./origin";
import {
  fixtureGrants,
  fixtureOverview,
  fixtureRecentView,
} from "./fixtures";
import {
  activeOpsDataSchema,
  cellsDataSchema,
  overviewDataSchema,
  recentGrantSchema,
  recentViewSchema,
  snapshotResponseSchema,
  type EvidenceCacheView,
  type RecentGrant,
  type RecentView,
  type SearchQuery,
  type SnapshotEnvelope,
} from "./schemas";
import { toUpstreamSearchParams } from "./query";

const FETCH_TIMEOUT_MS = 4_000;
const MAX_RESPONSE_BYTES = 65_536;
const MAX_TOPOLOGY_RESPONSE_BYTES = 262_144;
const MAX_CONCURRENT = 4;
let inFlight = 0;

export class OpsClientError extends Error {
  readonly status: number;
  readonly code: string;
  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

async function boundedFetch(path: string, init?: RequestInit, maxBytes = MAX_RESPONSE_BYTES): Promise<Response> {
  if (inFlight >= MAX_CONCURRENT) {
    throw new OpsClientError(429, "CONCURRENCY_BUDGET", "Server-side concurrency budget exceeded.");
  }
  inFlight += 1;
  const controller = new AbortController();
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new OpsClientError(504, "SOURCE_TIMEOUT", "Operations Console exchange timed out."));
      }, FETCH_TIMEOUT_MS);
    });
    return await Promise.race([deadline, (async () => {
      const origin = resolveOperationsOrigin();
      const headers: Record<string, string> = {
        Accept: "application/json",
        // Never forward browser Cookie. Only a server session Bearer reaches Console.
      };
      if (isM36aEnabled()) {
        const bearer = await optionalUpstreamBearer();
        if (!bearer) {
          throw new OpsClientError(401, "UNAUTHENTICATED", "Sign-in required.");
        }
        headers.Authorization = `Bearer ${bearer}`;
      }
      controller.signal.throwIfAborted();
      const res = await fetch(`${origin}${path}`, {
        ...init,
        method: "GET",
        signal: controller.signal,
        cache: "no-store",
        headers,
        redirect: "error",
      });
      reader = res.body?.getReader();
      if (controller.signal.aborted) {
        void reader?.cancel().catch(() => {});
        controller.signal.throwIfAborted();
      }
      if (res.status === 401) {
        throw new OpsClientError(401, "UNAUTHENTICATED", "Console rejected credentials.");
      }
      if (res.status === 403) {
        throw new OpsClientError(403, "FORBIDDEN", "Console denied this grant.");
      }
      const chunks: Uint8Array[] = [];
      let size = 0;
      const length = res.headers.get("content-length");
      if (length && Number(length) > maxBytes) {
        throw new OpsClientError(502, "RESPONSE_TOO_LARGE", "Upstream response exceeded byte cap.");
      }
      while (reader) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > maxBytes) {
          throw new OpsClientError(502, "RESPONSE_TOO_LARGE", "Upstream response exceeded byte cap.");
        }
        chunks.push(value);
      }
      const body = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
      return new Response(size ? body : null, { status: res.status, headers: res.headers });
    })()]);
  } catch (err) {
    if (err instanceof OpsClientError || err instanceof AuthError) throw err;
    if (err instanceof OperationsOriginError) {
      throw new OpsClientError(500, "CONFIGURATION_ERROR", err.message);
    }
    throw new OpsClientError(503, "SOURCE_UNAVAILABLE", "Operations Console is unreachable from this host.");
  } finally {
    clearTimeout(timer);
    controller.abort();
    void reader?.cancel().catch(() => {});
    inFlight -= 1;
  }
}

async function readJsonBounded(res: Response, maxBytes = MAX_RESPONSE_BYTES): Promise<unknown> {
  const buf = await res.arrayBuffer();
  if (buf.byteLength > maxBytes) {
    throw new OpsClientError(502, "RESPONSE_TOO_LARGE", "Upstream response exceeded byte cap.");
  }
  const text = new TextDecoder().decode(buf);
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new OpsClientError(502, "INVALID_JSON", "Upstream returned non-JSON.");
  }
}

function wrapSnapshot<T>(
  raw: unknown,
  dataSchema: { parse: (v: unknown) => T },
): SnapshotEnvelope<T> {
  const env = snapshotResponseSchema.parse(raw);
  return {
    asOf: env.asOf,
    coverage: env.coverage,
    uncertainty: env.uncertainty,
    data: dataSchema.parse(env.data),
  };
}

export async function getOverview(): Promise<SnapshotEnvelope<EvidenceCacheView[]>> {
  if (isFixtureMode()) {
    return {
      asOf: new Date().toISOString(),
      coverage: "EXPLICIT_INVENTORY_PARTIAL_NO_HISTORY",
      uncertainty: "OTHER_CONSUMERS_BROKER_ACK_DLT_QUARANTINE_UNKNOWN",
      data: fixtureOverview,
    };
  }
  const res = await boundedFetch("/ops/v1/overview");
  if (!res.ok) throw new OpsClientError(res.status, "UPSTREAM", "Overview unavailable.");
  return wrapSnapshot(await readJsonBounded(res), overviewDataSchema);
}

export async function getCells(offset = 0, limit = 8): Promise<SnapshotEnvelope<EvidenceCacheView[]>> {
  if (offset < 0 || offset > 8 || limit < 1 || limit > 8) {
    throw new OpsClientError(400, "BAD_REQUEST", "Invalid cells pagination.");
  }
  if (isFixtureMode()) {
    return {
      asOf: new Date().toISOString(),
      coverage: "EXPLICIT_INVENTORY_PARTIAL_NO_HISTORY",
      uncertainty: "OTHER_CONSUMERS_BROKER_ACK_DLT_QUARANTINE_UNKNOWN",
      data: fixtureOverview.slice(offset, offset + limit),
    };
  }
  const res = await boundedFetch(`/ops/v1/cells?offset=${offset}&limit=${limit}`);
  if (!res.ok) throw new OpsClientError(res.status, "UPSTREAM", "Cells unavailable.");
  return wrapSnapshot(await readJsonBounded(res), cellsDataSchema);
}

export async function getActiveOperations(): Promise<
  SnapshotEnvelope<ReturnType<typeof activeOpsDataSchema.parse>>
> {
  if (isFixtureMode()) {
    return {
      asOf: new Date().toISOString(),
      coverage: "EXPLICIT_INVENTORY_PARTIAL_NO_HISTORY",
      uncertainty: "OTHER_CONSUMERS_BROKER_ACK_DLT_QUARANTINE_UNKNOWN",
      data: fixtureOverview.map((v) => ({
        source: v.source,
        freshness: v.freshness,
        lastObservedAt: v.snapshot?.observedAt ?? null,
        attempts: v.freshness === "FRESH" ? (v.snapshot?.attempts ?? []) : [],
        registryAvailable: Boolean(v.snapshot?.activeAvailable),
        expired: v.snapshot?.expiredAttempts ?? 0,
        rejected: v.snapshot?.rejectedAttempts ?? 0,
        coverage: "PARTIAL_MEMBERSHIP_TTL_EXPIRY_UNKNOWN_NO_COMPLETION",
      })),
    };
  }
  const res = await boundedFetch("/ops/v1/operations/active");
  if (!res.ok) throw new OpsClientError(res.status, "UPSTREAM", "Active operations unavailable.");
  return wrapSnapshot(await readJsonBounded(res), activeOpsDataSchema);
}

export async function getRecentGrants(): Promise<RecentGrant[]> {
  if (isFixtureMode()) return fixtureGrants;
  const res = await boundedFetch("/ops/v1/recent/search/sources");
  if (res.status === 404) {
    throw new OpsClientError(404, "SEARCH_DISABLED", "Recent search is not enabled on Console.");
  }
  if (!res.ok) throw new OpsClientError(res.status, "UPSTREAM", "Grants unavailable.");
  const raw = await readJsonBounded(res);
  return recentGrantSchema.array().parse(raw);
}

type RecentKindPath = "events" | "inbox" | "requests" | "workflows";

export async function getRecent(
  kind: RecentKindPath,
  query: SearchQuery,
): Promise<RecentView> {
  if (isFixtureMode()) {
    const map: Record<
      RecentKindPath,
      "OUTBOX_EVENT" | "KNOWN_GROUP_INBOX" | "REQUEST_RECEIPT" | "WORKFLOW"
    > = {
      events: "OUTBOX_EVENT",
      inbox: "KNOWN_GROUP_INBOX",
      requests: "REQUEST_RECEIPT",
      workflows: "WORKFLOW",
    };
    return fixtureRecentView(map[kind], query.owner, query.scope);
  }
  const qs = toUpstreamSearchParams(query).toString();
  const res = await boundedFetch(`/ops/v1/recent/${kind}?${qs}`);
  if (res.status === 403) {
    throw new OpsClientError(403, "SCOPE_DENIED", "Owner/scope grant denied.");
  }
  if (res.status === 400) {
    throw new OpsClientError(400, "BAD_REQUEST", "Console rejected search bounds.");
  }
  if (res.status === 404) {
    throw new OpsClientError(404, "SEARCH_DISABLED", "Recent search is not enabled on Console.");
  }
  if (!res.ok) {
    throw new OpsClientError(res.status, "UPSTREAM", "Recent search unavailable.");
  }
  return recentViewSchema.parse(await readJsonBounded(res));
}

export function redactedErrorMessage(err: unknown): string {
  if (err instanceof OpsClientError) return err.message;
  return "Request failed.";
}

export async function getTopology(): Promise<Topology> {
  if (isFixtureMode()) {
    return topologySchema.parse({
      schemaVersion: 1, asOf: new Date().toISOString(),
      coverage: "SYNTHETIC_DEMO_EMPTY_TOPOLOGY",
      capabilities: { declared: "CONFIGURED_OWNER_QUERIES_ONLY", instances: "OWNER_SNAPSHOTS_PARTIAL",
        observedInteractions: "NOT_CONFIGURED", messaging: "NOT_CONFIGURED", traces: "NOT_CONFIGURED",
        runtimeHealth: "NOT_CONFIGURED" },
      nodes: [], connections: [],
    });
  }
  const res = await boundedFetch("/ops/v1/topology?schemaVersion=2", undefined, MAX_TOPOLOGY_RESPONSE_BYTES);
  if (!res.ok) throw new OpsClientError(res.status, "UPSTREAM", "Topology unavailable.");
  return topologySchema.parse(await readJsonBounded(res, MAX_TOPOLOGY_RESPONSE_BYTES));
}
