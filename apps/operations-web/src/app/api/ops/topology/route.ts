import { getTopology } from "@/server/ops-client";
import { assertOpsBffAuth, jsonErr, jsonOk, rejectMutation } from "../_shared";

export const dynamic = "force-dynamic";
export async function GET() {
  try {
    const denied = await assertOpsBffAuth();
    if (denied) return denied;
    return jsonOk(await getTopology());
  } catch (err) {
    return jsonErr(err);
  }
}
export const POST = rejectMutation;
export const PUT = rejectMutation;
export const PATCH = rejectMutation;
export const DELETE = rejectMutation;
