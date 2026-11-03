import { getRecent } from "@/server/ops-client";
import { parseSearchParams, QueryValidationError } from "@/server/query";
import { jsonErr, jsonOk, rejectMutation } from "../../_shared";

export const dynamic = "force-dynamic";

const KINDS = new Set(["events", "inbox", "requests", "workflows"] as const);
type Kind = "events" | "inbox" | "requests" | "workflows";

export async function GET(
  req: Request,
  ctx: { params: Promise<{ kind: string }> },
) {
  try {
    const { kind } = await ctx.params;
    if (!KINDS.has(kind as Kind)) {
      throw new QueryValidationError("Unknown recent kind.");
    }
    const url = new URL(req.url);
    const query = parseSearchParams(url.searchParams);
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
