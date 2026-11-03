import "server-only";
import { validateOperationsOrigin } from "@/lib/origin-policy";

export { validateOperationsOrigin };

export function resolveOperationsOrigin(): string {
  return validateOperationsOrigin(process.env.APEX_OPERATIONS_API_ORIGIN);
}

export function isFixtureMode(): boolean {
  if (process.env.OPS04B_REQUIRE_LIVE === "true" && process.env.APEX_OPS_UI_FIXTURE_MODE === "true") {
    throw new Error("OPS04B_REQUIRE_LIVE forbids APEX_OPS_UI_FIXTURE_MODE=true.");
  }
  return process.env.APEX_OPS_UI_FIXTURE_MODE === "true";
}
