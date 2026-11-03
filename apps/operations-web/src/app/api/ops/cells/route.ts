import { getCells } from "@/server/ops-client";
import { QueryValidationError } from "@/server/query";
import { assertOpsBffAuth, jsonErr, jsonOk, rejectMutation } from "../_shared";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  try {
    const denied = await assertOpsBffAuth();
    if (denied) return denied;
    const url = new URL(req.url);
    const allowed = new Set(["offset", "limit"]);
    for (const key of url.searchParams.keys()) {
      if (!allowed.has(key)) {
        throw new QueryValidationError("Unknown query parameter rejected.");
      }
    }
    const offset = Number(url.searchParams.get("offset") ?? "0");
    const limit = Number(url.searchParams.get("limit") ?? "8");
    return jsonOk(await getCells(offset, limit));
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
