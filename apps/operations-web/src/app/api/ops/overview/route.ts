import { getOverview } from "@/server/ops-client";
import { assertOpsBffAuth, jsonErr, jsonOk, rejectMutation } from "../_shared";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const denied = await assertOpsBffAuth();
    if (denied) return denied;
    return jsonOk(await getOverview());
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
