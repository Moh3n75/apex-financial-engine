import "server-only";
import { validateOperationsOrigin } from "@/lib/origin-policy";

export { validateOperationsOrigin };

export function resolveOperationsOrigin(): string {
  return validateOperationsOrigin(process.env.APEX_OPERATIONS_API_ORIGIN);
}

export function isFixtureMode(): boolean {
  return process.env.APEX_OPS_UI_FIXTURE_MODE === "true";
}
