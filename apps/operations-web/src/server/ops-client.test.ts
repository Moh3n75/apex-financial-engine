import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("server-only", () => ({}));
vi.mock("./auth/access", () => ({ optionalUpstreamBearer: vi.fn() }));
vi.mock("./auth/config", () => ({ isM36aEnabled: () => false }));
vi.mock("./origin", () => ({ isFixtureMode: () => false, resolveOperationsOrigin: () => "http://127.0.0.1:9190" }));
import { getRecentGrants, getTopology } from "./ops-client";

afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });
describe("bounded Console exchange", () => {
  it("allows bounded detailed topology while enforcing its incremental 256 KiB cap", async () => {
    const graph = { schemaVersion: 2, asOf: "2026-11-10T10:00:00Z", coverage: "TEST_FIXTURE",
      capabilities: { declared: "UNKNOWN", instances: "UNKNOWN", observedInteractions: "NOT_CONFIGURED", messaging: "NOT_CONFIGURED", traces: "NOT_CONFIGURED", runtimeHealth: "NOT_CONFIGURED" },
      nodes: [], connections: [], providers: [{ name: "fixture", status: "FIXTURE", coverage: "TEST_ONLY", sourceMode: "FIXTURE", collectedAt: null }], padding: " ".repeat(70000) };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(graph))));
    expect((await getTopology()).nodes).toEqual([]);
    const cancel = vi.fn();
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(new ReadableStream({ start(c) { c.enqueue(new Uint8Array(150000)); c.enqueue(new Uint8Array(150000)); }, cancel }))));
    await expect(getTopology()).rejects.toMatchObject({ code: "RESPONSE_TOO_LARGE" });
    expect(cancel).toHaveBeenCalled();
  });
  it("times out stalled body consumption, cancels the stream and releases the slot", async () => {
    vi.useFakeTimers();
    const cancel = vi.fn();
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(new ReadableStream({
      start(c) { c.enqueue(new TextEncoder().encode("[")); }, cancel,
    }))));
    const result = expect(getRecentGrants()).rejects.toMatchObject({ code: "SOURCE_TIMEOUT" });
    await vi.advanceTimersByTimeAsync(4001);
    await result;
    expect(cancel).toHaveBeenCalled();
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("[]")));
    expect(await getRecentGrants()).toEqual([]);
  });
  it("enforces the cap incrementally without waiting for stream completion", async () => {
    const cancel = vi.fn();
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(new ReadableStream({
      start(c) { c.enqueue(new Uint8Array(40000)); c.enqueue(new Uint8Array(30000)); }, cancel,
    }))));
    await expect(getRecentGrants()).rejects.toMatchObject({ code: "RESPONSE_TOO_LARGE" });
    expect(cancel).toHaveBeenCalled();
  });
  it("holds all four slots until body consumption ends and releases them on failure", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("fetch", vi.fn().mockImplementation(() => Promise.resolve(new Response(new ReadableStream()))));
    const requests = Array.from({ length: 4 }, () => getRecentGrants().catch(e => e.code));
    await vi.advanceTimersByTimeAsync(0); // Headers have arrived; all bodies are still open.
    await expect(getRecentGrants()).rejects.toMatchObject({ code: "CONCURRENCY_BUDGET" });
    await vi.advanceTimersByTimeAsync(4001);
    expect(await Promise.all(requests)).toEqual(Array(4).fill("SOURCE_TIMEOUT"));
    vi.stubGlobal("fetch", vi.fn().mockImplementation(() => Promise.resolve(new Response("[]"))));
    expect(await Promise.all(Array.from({ length: 4 }, () => getRecentGrants()))).toEqual([[], [], [], []]);
  });
  it("uses an absolute deadline even while a slow stream keeps producing chunks", async () => {
    vi.useFakeTimers();
    let interval: ReturnType<typeof setInterval>;
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(new ReadableStream({
      start(c) { interval = setInterval(() => c.enqueue(new TextEncoder().encode(" ")), 500); },
      cancel() { clearInterval(interval); },
    }))));
    const pending = expect(getRecentGrants()).rejects.toMatchObject({ code: "SOURCE_TIMEOUT" });
    await vi.advanceTimersByTimeAsync(4001);
    await pending;
    expect(vi.getTimerCount()).toBe(0);
  });
  it("releases slots after invalid JSON, HTTP denial and stream failures", async () => {
    for (const response of [new Response("oops"), new Response(null, { status: 403 }),
      new Response(new ReadableStream({ start(c) { c.error(new Error("private")); } }))]) {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response));
      await expect(getRecentGrants()).rejects.toBeInstanceOf(Error);
    }
    vi.stubGlobal("fetch", vi.fn().mockImplementation(() => Promise.resolve(new Response("[]"))));
    expect(await Promise.all(Array.from({ length: 4 }, () => getRecentGrants()))).toHaveLength(4);
  });
});
