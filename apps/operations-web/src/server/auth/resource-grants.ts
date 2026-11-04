import "server-only";
import { AuthError } from "./errors";
import type { OpsSession } from "./session";
import {
  evaluateOwnerVisible,
  evaluateResourceGrant,
  parseResourceGrantsJson,
  type ResourceAuthConfig,
  type ResourceGrant,
} from "@/lib/resource-grants";

export type { ResourceAuthConfig, ResourceGrant };

let cached: ResourceAuthConfig | null = null;

/** Env JSON: [{"subject":"*","environment":"test","owner":"order-cell","kinds":"OUTBOX_EVENT,OVERVIEW","scopes":"owner,order"}] */
export function loadResourceAuthConfig(): ResourceAuthConfig {
  if (cached) return cached;
  const enabled = process.env.APEX_OPS_M36B_RESOURCE_AUTH === "true";
  const raw = process.env.APEX_OPS_M36B_RESOURCE_GRANTS ?? "[]";
  let grants: ResourceGrant[] = [];
  if (enabled) {
    try {
      grants = parseResourceGrantsJson(raw);
    } catch {
      throw new AuthError(500, "AUTH_MISCONFIGURED", "M36B resource grants are misconfigured.");
    }
  }
  cached = { enabled, grants };
  return cached;
}

/** Test-only reset for vitest. */
export function resetResourceAuthCache(): void {
  cached = null;
}

export function assertResourceGrant(
  session: OpsSession,
  environment: string,
  owner: string,
  kind: string,
  scope: string,
): void {
  const cfg = loadResourceAuthConfig();
  if (!evaluateResourceGrant(cfg, session, environment, owner, kind, scope)) {
    throw new AuthError(403, "FORBIDDEN", "Resource grant denied.");
  }
}

export function assertOwnerVisible(session: OpsSession, environment: string, owner: string): boolean {
  return evaluateOwnerVisible(loadResourceAuthConfig(), session, environment, owner);
}

export function assertNoTenantRowFilter(params: URLSearchParams): void {
  if (params.has("tenantFilter")) {
    throw new AuthError(403, "FORBIDDEN", "Tenant row filter is unsupported.");
  }
}
