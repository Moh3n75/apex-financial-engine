import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("server-only", () => ({}));
vi.mock("./auth/config", () => ({ isM36aEnabled: () => false }));
import { getOverview, getTopology } from "./ops-client";
import { GET } from "@/app/api/ops/topology/route";

afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); });

describe("live BFF local setup", () => {
  it("returns a useful configuration error without attempting network access", async () => {
    vi.stubEnv("APEX_OPERATIONS_API_ORIGIN", "");
    vi.stubEnv("APEX_OPS_UI_FIXTURE_MODE", "false");
    const fetch = vi.fn(); vi.stubGlobal("fetch", fetch);
    await expect(getOverview()).rejects.toMatchObject({ status: 500, code: "CONFIGURATION_ERROR" });
    const response = await GET();
    expect(response.status).toBe(500);
    expect(await response.json()).toMatchObject({ code: "CONFIGURATION_ERROR", message: expect.stringContaining(".env.local") });
    expect(fetch).not.toHaveBeenCalled();
  });
  it("redacts invalid settings and distinguishes actual network failures", async () => {
    vi.stubEnv("APEX_OPS_UI_FIXTURE_MODE", "false");
    vi.stubEnv("APEX_OPERATIONS_API_ORIGIN", "http://user:private-secret@127.0.0.1:9190");
    const bad = await GET();
    expect(bad.status).toBe(500);
    expect(JSON.stringify(await bad.json())).not.toContain("private-secret");
    vi.stubEnv("APEX_OPERATIONS_API_ORIGIN", "http://127.0.0.1:9190");
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("private transport details")));
    await expect(getTopology()).rejects.toMatchObject({ status: 503, code: "SOURCE_UNAVAILABLE" });
  });
  it("uses real no-auth loopback HTTP with no cookie or bearer forwarding", async () => {
    vi.stubEnv("APEX_OPS_UI_FIXTURE_MODE", "false");
    vi.stubEnv("APEX_OPERATIONS_API_ORIGIN", "http://127.0.0.1:9190");
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ asOf: "2026-11-10T10:00:00Z",
      coverage: "PARTIAL", uncertainty: "UNKNOWN", data: [] })));
    vi.stubGlobal("fetch", fetch);
    expect((await getOverview()).data).toEqual([]);
    expect(fetch).toHaveBeenCalledWith("http://127.0.0.1:9190/ops/v1/overview", expect.objectContaining({
      method: "GET", cache: "no-store", redirect: "error", headers: { Accept: "application/json" },
    }));
  });
});
