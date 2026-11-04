import { isM36aEnabled } from "@/server/auth/config";
import { AuthError } from "@/server/auth/errors";
import { getRecent } from "@/server/ops-client";
import { parseSearchParams, QueryValidationError } from "@/server/query";
import {
  assertOpsBffAuth,
  assertRecentResourceGrant,
  jsonErr,
  jsonOk,
  rejectMutation,
  requireOpsSessionForBff,
} from "../../_shared";

export const dynamic = "force-dynamic";

const KINDS = new Set(["events", "inbox", "requests", "workflows"] as const);
type Kind = "events" | "inbox" | "requests" | "workflows";

export async function GET(
  req: Request,
  ctx: { params: Promise<{ kind: string }> },
) {
  try {
    const denied = await assertOpsBffAuth();
    if (denied) return denied;
    const { kind } = await ctx.params;
    if (!KINDS.has(kind as Kind)) {
      throw new QueryValidationError("Unknown recent kind.");
    }
    const url = new URL(req.url);
    // Fail closed before schema parse so tenantFilter never looks like a soft 400.
    if (url.searchParams.has("tenantFilter")) {
      throw new AuthError(403, "FORBIDDEN", "Tenant row filter is unsupported.");
    }
    const query = parseSearchParams(url.searchParams);
    if (isM36aEnabled()) {
      const session = await requireOpsSessionForBff();
      assertRecentResourceGrant(
        session,
        kind,
        query.environment,
        query.owner,
        query.scope,
        url.searchParams,
      );
    }
    return jsonOk(await getRecent(kind as Kind, query));
  } catch (err) {
    return jsonErr(err);
  }
}

export function POST() {
  return rejectMutation();
}
export function PUT() {
  return rejectMutation();
}
export function PATCH() {
  return rejectMutation();
}
export function DELETE() {
  return rejectMutation();
}
