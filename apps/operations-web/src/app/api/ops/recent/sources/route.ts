import { getRecentGrants } from "@/server/ops-client";
import { jsonErr, jsonOk, rejectMutation } from "../../_shared";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    return jsonOk(await getRecentGrants());
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
