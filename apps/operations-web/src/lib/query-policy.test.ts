import { describe, expect, it } from "vitest";
import { assertOpaqueCursor, parseSearchParams, QueryValidationError } from "./query-policy";
import { recentViewSchema } from "@/server/schemas";

describe("parseSearchParams", () => {
  it("accepts a bounded recent query", () => {
    const to = new Date();
    const from = new Date(to.getTime() - 10 * 60_000);
    const params = new URLSearchParams({
      environment: "test",
      owner: "order-cell",
      scope: "owner",
      from: from.toISOString().replace(/\.\d{3}Z$/, "Z"),
      to: to.toISOString().replace(/\.\d{3}Z$/, "Z"),
      limit: "16",
    });
    const q = parseSearchParams(params);
    expect(q.owner).toBe("order-cell");
    expect(q.limit).toBe(16);
    expect(q.state).toBe("ANY");
  });

  it("rejects unknown and duplicate keys", () => {
    const base = new URLSearchParams({
      environment: "test",
      owner: "order-cell",
      scope: "owner",
      from: "2026-11-03T11:00:00Z",
      to: "2026-11-03T11:30:00Z",
    });
    base.append("sql", "select 1");
    expect(() => parseSearchParams(base)).toThrow(QueryValidationError);

    const dup = new URLSearchParams();
    dup.append("environment", "test");
    dup.append("environment", "prod");
    dup.append("owner", "order-cell");
    dup.append("scope", "owner");
    dup.append("from", "2026-11-03T11:00:00Z");
    dup.append("to", "2026-11-03T11:30:00Z");
    expect(() => parseSearchParams(dup)).toThrow(QueryValidationError);
  });

  it("treats cursor as opaque charset/length only", () => {
    expect(() => assertOpaqueCursor("abc.DEF-123_")).not.toThrow();
    expect(() => assertOpaqueCursor("bad cursor!")).toThrow(QueryValidationError);
  });
});

describe("recentViewSchema", () => {
  it("accepts handoff-shaped FRESH page and preserves LATE_COMMIT_GAP", () => {
    const view = recentViewSchema.parse({
      asOf: "2026-11-03T10:19:30.633Z",
      receivedAt: "2026-11-03T10:19:30.633Z",
      freshness: "FRESH",
      availability: "AVAILABLE",
      coverage: "OWNER_DIRECT_PARTIAL_CURRENT_STATE_NO_HISTORY_EPHEMERAL",
      brokerAck: "UNKNOWN_NO_VERIFIED_RECEIPT",
      quarantine: "UNKNOWN_NOT_INSPECTED",
      uncertainty: "PARTIAL_NO_OWNER_REVISION",
      priorConflicts: [],
      sourceMode: "OWNER_DIRECT",
      page: {
        schemaVersion: 1,
        environment: "test",
        owner: "order-cell",
        instance: "one",
        epoch: "11111111-1111-4111-8111-111111111111",
        kind: "WORKFLOW",
        scope: "order",
        fromInclusive: "2026-11-03T10:00:00.000Z",
        toExclusive: "2026-11-03T10:20:00.000Z",
        scanStartedAt: "2026-11-03T10:19:30.633Z",
        scanCompletedAt: "2026-11-03T10:19:30.700Z",
        asOf: "2026-11-03T10:19:30.633Z",
        maxAgeMillis: 10000,
        candidatesExamined: 1,
        coverage: [
          "PARTIAL_CURRENT_STATE",
          "RETENTION_UNQUALIFIED",
          "RECONCILE_FROM_START",
          "INDEXED_RECENT_RANGE",
          "STATE_POST_FILTERED",
          "LATE_COMMIT_GAP",
        ],
        items: [
          {
            ref: "3d7e1657-aa49-4e14-9d67-4412727140cb",
            state: "PENDING_PAYMENT",
            sourceAssignedAt: "2026-11-03T10:19:28.248Z",
            evidenceKey: "WORKFLOW:3d7e1657-aa49-4e14-9d67-4412727140cb",
            evidenceClass: "OWNER_COMMITTED_ROW",
            certainty: "VERIFIED_WITHIN_SCOPE",
          },
        ],
        nextCursor: "",
        relation: "RELATION_UNKNOWN",
      },
    });
    expect(view.page?.coverage).toContain("LATE_COMMIT_GAP");
    expect(view.brokerAck).toBe("UNKNOWN_NO_VERIFIED_RECEIPT");
  });

  it("accepts MISSING / PERMISSION_GAP without inventing a page", () => {
    const view = recentViewSchema.parse({
      asOf: "2026-11-03T10:19:30.633Z",
      receivedAt: null,
      freshness: "MISSING",
      availability: "PERMISSION_GAP",
      coverage: "PARTIAL_NO_DATA_AUTHORIZATION_REVALIDATED",
      page: null,
      brokerAck: "UNKNOWN_NO_VERIFIED_RECEIPT",
      quarantine: "UNKNOWN_NOT_INSPECTED",
      uncertainty: "UNKNOWN",
      priorConflicts: [],
      sourceMode: "OWNER_DIRECT",
    });
    expect(view.page).toBeNull();
    expect(view.availability).toBe("PERMISSION_GAP");
  });

  it("fails closed on unexpected freshness values", () => {
    expect(() =>
      recentViewSchema.parse({
        asOf: "2026-11-03T10:19:30.633Z",
        receivedAt: null,
        freshness: "ALL_HEALTHY",
        availability: "AVAILABLE",
        coverage: "x",
        page: null,
        brokerAck: "UNKNOWN_NO_VERIFIED_RECEIPT",
        quarantine: "UNKNOWN_NOT_INSPECTED",
        uncertainty: "UNKNOWN",
        priorConflicts: [],
        sourceMode: "OWNER_DIRECT",
      }),
    ).toThrow();
  });
});
