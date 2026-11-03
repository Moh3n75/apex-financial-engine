/** Pure M36A enablement fence (safe for unit tests; no server-only). */
export function isM36aEnabledFromEnv(
  env: Record<string, string | undefined> = process.env,
): boolean {
  if (env.APEX_OPS_UI_FIXTURE_MODE === "true") {
    return false;
  }
  return env.APEX_OPS_M36A_ENABLED === "true";
}
