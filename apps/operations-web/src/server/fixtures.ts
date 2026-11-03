import "server-only";
import type { EvidenceCacheView, RecentGrant, RecentView } from "./schemas";

/** Isolated synthetic fixtures for Playwright/unit when Java Console is unavailable. Marked SYNTHETIC. */

const asOf = "2026-11-03T12:00:00.000Z";
const from = "2026-11-03T11:15:00.000Z";
const to = "2026-11-03T12:00:00.000Z";

export const fixtureGrants: RecentGrant[] = [
  { environment: "test", owner: "order-cell", kind: "OUTBOX_EVENT", scope: "owner" },
  { environment: "test", owner: "order-cell", kind: "WORKFLOW", scope: "order" },
  { environment: "test", owner: "order-cell", kind: "REQUEST_RECEIPT", scope: "order-request" },
  {
    environment: "test",
    owner: "inventory-cell",
    kind: "KNOWN_GROUP_INBOX",
    scope: "inventory-three-demo",
  },
  { environment: "test", owner: "inventory-cell", kind: "WORKFLOW", scope: "reservation" },
  {
    environment: "test",
    owner: "payment-cell",
    kind: "WORKFLOW",
    scope: "authorization-simulation",
  },
];

export function fixtureRecentView(
  kind: "OUTBOX_EVENT" | "KNOWN_GROUP_INBOX" | "REQUEST_RECEIPT" | "WORKFLOW",
  owner: string,
  scope: string,
): RecentView {
  const ref = "3d7e1657-aa49-4e14-9d67-4412727140cb";
  return {
    asOf,
    receivedAt: asOf,
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
      owner,
      instance: "fixture-1",
      epoch: "11111111-1111-4111-8111-111111111111",
      kind,
      scope,
      fromInclusive: from,
      toExclusive: to,
      scanStartedAt: asOf,
      scanCompletedAt: asOf,
      asOf,
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
          ref,
          state: owner === "payment-cell" ? "AUTHORIZED" : "PENDING_PAYMENT",
          sourceAssignedAt: "2026-11-03T11:19:28.248Z",
          evidenceKey: `${kind}:${ref}`,
          evidenceClass: "OWNER_COMMITTED_ROW",
          certainty: "VERIFIED_WITHIN_SCOPE",
        },
      ],
      nextCursor: "",
      relation: "RELATION_UNKNOWN",
    },
  };
}

export const fixtureOverview: EvidenceCacheView[] = [
  {
    source: "order-cell",
    receivedAt: asOf,
    freshness: "FRESH",
    uncertainty: "PARTIAL_MEMBERSHIP_NO_HISTORY",
    conflicts: [],
    snapshot: {
      schemaVersion: 1,
      environment: "test",
      owner: "order-cell",
      instance: "one",
      epoch: "f110b896-6f91-4942-b714-76da17f59f89",
      observedAt: asOf,
      maxAgeMillis: 10000,
      coverage: "EXPLICIT_KEYS_PARTIAL_NO_HISTORY",
      facts: [
        {
          kind: "HEALTH",
          ref: "00000000-0000-0000-0000-000000000000",
          scope: "owner-db-read",
          state: "READABLE",
          evidenceClass: "ATTEMPT_OBSERVATION",
          certainty: "OBSERVED_ONLY",
          sourceAssignedAt: null,
          factKey: "HEALTH:00000000-0000-0000-0000-000000000000:owner-db-read",
        },
        {
          kind: "WORKFLOW",
          ref: "197ededd-b010-427a-b2f4-6613672cd869",
          scope: "order",
          state: "PENDING_PAYMENT",
          evidenceClass: "OWNER_COMMITTED_ROW",
          certainty: "VERIFIED_WITHIN_SCOPE",
          sourceAssignedAt: "2026-11-03T11:19:28.248Z",
          factKey: "WORKFLOW:197ededd-b010-427a-b2f4-6613672cd869:order",
        },
      ],
      attempts: [
        {
          operationId: "77fc4f5b-c8a3-4b52-bcbd-aa829b5080a0",
          epoch: "51ed9d9e-b2e0-4383-a70f-0a4ea786377b",
          stage: "RPC_SERVER",
          elapsedNanos: 148268600,
        },
      ],
      activeAvailable: true,
      expiredAttempts: 0,
      rejectedAttempts: 0,
    },
  },
  {
    source: "payment-cell",
    receivedAt: null,
    snapshot: null,
    freshness: "MISSING",
    uncertainty: "UNKNOWN",
    conflicts: [],
  },
];
