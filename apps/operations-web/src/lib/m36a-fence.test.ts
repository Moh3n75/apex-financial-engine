import { describe, expect, it } from "vitest";
import { isM36aEnabledFromEnv } from "./m36a-flags";

describe("M36A enablement fence", () => {
  it("fixture mode disables M36A even when enabled flag is true", () => {
    expect(
      isM36aEnabledFromEnv({
        APEX_OPS_M36A_ENABLED: "true",
        APEX_OPS_UI_FIXTURE_MODE: "true",
      }),
    ).toBe(false);
  });

  it("requires explicit enable flag", () => {
    expect(isM36aEnabledFromEnv({})).toBe(false);
    expect(isM36aEnabledFromEnv({ APEX_OPS_M36A_ENABLED: "true" })).toBe(true);
  });
});
