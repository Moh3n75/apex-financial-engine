import { NextResponse } from "next/server";
import { isM36aEnabled } from "@/server/auth/config";
import { requireOpsSession } from "@/server/auth/access";
import { AuthError } from "@/server/auth/errors";
import {
  assertNoTenantRowFilter,
  assertResourceGrant,
} from "@/server/auth/resource-grants";
import type { OpsSession } from "@/server/auth/session";
import { OpsClientError, redactedErrorMessage } from "@/server/ops-client";
import { QueryValidationError } from "@/server/query";

export const noStoreHeaders = {
  "Cache-Control": "no-store",
  "X-Content-Type-Options": "nosniff",
};

/** Enforce M36A session on BFF ops routes when enabled. */
export async function assertOpsBffAuth(): Promise<NextResponse | null> {
  if (!isM36aEnabled()) return null;
  try {
    await requireOpsSession();
    return null;
  } catch (err) {
    return jsonErr(err);
  }
}

/** Session + optional M36B resource precheck (Console remains authoritative). */
export async function requireOpsSessionForBff(): Promise<OpsSession> {
  return requireOpsSession();
}

export function assertRecentResourceGrant(
  session: OpsSession,
  kindPath: string,
  environment: string,
  owner: string,
  scope: string,
  searchParams: URLSearchParams,
): void {
  assertNoTenantRowFilter(searchParams);
  const kindMap: Record<string, string> = {
    events: "OUTBOX_EVENT",
    inbox: "KNOWN_GROUP_INBOX",
    requests: "REQUEST_RECEIPT",
    workflows: "WORKFLOW",
  };
  const kind = kindMap[kindPath];
  if (!kind) {
    throw new QueryValidationError("Unknown recent kind.");
  }
  assertResourceGrant(session, environment, owner, kind, scope);
}

export function rejectMutation(): NextResponse {
  return NextResponse.json(
    { code: "METHOD_NOT_ALLOWED", message: "Mutations are disabled." },
    { status: 405, headers: noStoreHeaders },
  );
}

export function jsonOk(data: unknown, status = 200): NextResponse {
  return NextResponse.json(data, { status, headers: noStoreHeaders });
}

export function jsonErr(err: unknown): NextResponse {
  if (err instanceof QueryValidationError) {
    return NextResponse.json(
      { code: "BAD_REQUEST", message: err.message },
      { status: 400, headers: noStoreHeaders },
    );
  }
  if (err instanceof AuthError) {
    return NextResponse.json(
      { code: err.code, message: err.message },
      { status: err.status, headers: noStoreHeaders },
    );
  }
  if (err instanceof OpsClientError) {
    return NextResponse.json(
      { code: err.code, message: err.message },
      { status: err.status, headers: noStoreHeaders },
    );
  }
  return NextResponse.json(
    { code: "INTERNAL", message: redactedErrorMessage(err) },
    { status: 500, headers: noStoreHeaders },
  );
}
