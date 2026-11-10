import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));
vi.mock("@/server/auth/config", () => ({ isM36aEnabled: vi.fn(() => true) }));
vi.mock("@/server/auth/access", () => ({ requireOpsSession: vi.fn() }));
vi.mock("@/server/ops-client", () => ({
  getTopology: vi.fn(), OpsClientError: class extends Error {},
  redactedErrorMessage: () => "Request failed.",
}));
import { requireOpsSession } from "@/server/auth/access";
import { AuthError } from "@/server/auth/errors";
import { getTopology } from "@/server/ops-client";
import { GET, POST, PUT, PATCH, DELETE } from "./route";
import { topologySchema } from "@/lib/topology";

describe("topology BFF boundary", () => {
  beforeEach(() => vi.clearAllMocks());
  it("rejects a missing session before fetching topology", async () => {
    vi.mocked(requireOpsSession).mockRejectedValue(new AuthError(401, "UNAUTHENTICATED", "Sign-in required."));
    expect((await GET()).status).toBe(401);
    expect(getTopology).not.toHaveBeenCalled();
  });
  it("returns no-store data and redacts unexpected upstream errors", async () => {
    vi.mocked(requireOpsSession).mockResolvedValue({} as Awaited<ReturnType<typeof requireOpsSession>>);
    vi.mocked(getTopology).mockResolvedValue(topologySchema.parse({
      schemaVersion: 1, asOf: "2026-11-09T10:00:00Z", coverage: "EMPTY",
      capabilities: { declared: "CONFIGURED_OWNER_QUERIES_ONLY", instances: "OWNER_SNAPSHOTS_PARTIAL",
        observedInteractions: "NOT_CONFIGURED", messaging: "NOT_CONFIGURED", traces: "NOT_CONFIGURED",
        runtimeHealth: "NOT_CONFIGURED" }, nodes: [], connections: [],
    }));
    const response = await GET();
    expect(response.status).toBe(200);
    expect(response.headers.get("Cache-Control")).toBe("no-store");
    vi.mocked(getTopology).mockRejectedValue(new Error("private infrastructure detail"));
    expect(await (await GET()).text()).not.toContain("private infrastructure detail");
  });
  it("rejects every mutation method", () => {
    for (const method of [POST, PUT, PATCH, DELETE]) expect(method().status).toBe(405);
  });
});
