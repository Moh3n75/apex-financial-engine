import { z } from "zod";

/** Schemas derived from apex-operations-console OpenAPI / Java DTOs. Fail closed. */

/** Accept Java Instant / OffsetDateTime wire forms (incl. nano fractions). */
const zInstant = z.string().refine((s) => !Number.isNaN(Date.parse(s)), "instant");
const zOwnerName = z.string().regex(/^[A-Za-z0-9._-]{1,64}$/);
const zScope = z.string().regex(/^[A-Za-z0-9._-]{1,200}$/);
const zState = z.string().regex(/^[A-Z_]{1,40}$/);
const zCursor = z.string().regex(/^[A-Za-z0-9._-]{0,256}$/);

export const recentCoverageGapSchema = z.enum([
  "PARTIAL_CURRENT_STATE",
  "RETENTION_UNQUALIFIED",
  "RECONCILE_FROM_START",
  "CANDIDATE_PAGE_LIMIT",
  "WALK_LIMIT",
  "CLOCK_UNCERTAIN",
  "INDEXED_RECENT_RANGE",
  "STATE_POST_FILTERED",
  "LATE_COMMIT_GAP",
  "RESTART_WALK_REQUIRED",
]);

export const recentItemSchema = z.object({
  ref: z.string().uuid(),
  state: zState,
  sourceAssignedAt: zInstant,
  evidenceKey: z.string().regex(/^[A-Z_]{1,24}:[a-f0-9-]{36}$/),
  evidenceClass: z.literal("OWNER_COMMITTED_ROW"),
  certainty: z.enum(["VERIFIED_WITHIN_SCOPE", "UNKNOWN"]),
});

export const recentPageSchema = z.object({
  schemaVersion: z.literal(1),
  environment: zOwnerName,
  owner: zOwnerName,
  instance: zOwnerName,
  epoch: z.string().uuid(),
  kind: z.enum(["OUTBOX_EVENT", "KNOWN_GROUP_INBOX", "REQUEST_RECEIPT", "WORKFLOW"]),
  scope: zScope,
  fromInclusive: zInstant,
  toExclusive: zInstant,
  scanStartedAt: zInstant,
  scanCompletedAt: zInstant,
  asOf: zInstant,
  maxAgeMillis: z.literal(10000),
  candidatesExamined: z.number().int().min(0).max(16),
  coverage: z.array(recentCoverageGapSchema),
  items: z.array(recentItemSchema).max(16),
  nextCursor: zCursor,
  relation: z.literal("RELATION_UNKNOWN"),
});

export const recentViewSchema = z.object({
  asOf: zInstant,
  receivedAt: zInstant.nullable(),
  freshness: z.enum(["FRESH", "STALE", "MISSING", "UNKNOWN", "CLOCK_UNCERTAIN"]),
  availability: z.enum([
    "AVAILABLE",
    "SHED",
    "BACKOFF",
    "SOURCE_UNAVAILABLE",
    "PERMISSION_GAP",
    "RESTART_WALK_REQUIRED",
  ]),
  coverage: z.string(),
  page: recentPageSchema.nullable(),
  brokerAck: z.literal("UNKNOWN_NO_VERIFIED_RECEIPT"),
  quarantine: z.literal("UNKNOWN_NOT_INSPECTED"),
  uncertainty: z.enum(["UNKNOWN", "PARTIAL_NO_OWNER_REVISION", "CONFLICTING"]),
  priorConflicts: z.array(recentPageSchema).max(2),
  sourceMode: z.literal("OWNER_DIRECT"),
});

export const recentGrantSchema = z.object({
  environment: zOwnerName,
  owner: zOwnerName,
  kind: z.enum(["OUTBOX_EVENT", "KNOWN_GROUP_INBOX", "REQUEST_RECEIPT", "WORKFLOW"]),
  scope: zScope,
});

const snapshotFactSchema = z.object({
  kind: z.enum(["OUTBOX", "INBOX", "WORKFLOW", "RECEIPT", "HEALTH"]),
  ref: z.string().uuid(),
  scope: z.string(),
  state: z.string(),
  evidenceClass: z.enum(["OWNER_COMMITTED_ROW", "ATTEMPT_OBSERVATION"]),
  certainty: z.enum(["VERIFIED_WITHIN_SCOPE", "OBSERVED_ONLY", "UNKNOWN"]),
  sourceAssignedAt: zInstant.nullable(),
  factKey: z.string(),
});

const snapshotAttemptSchema = z.object({
  operationId: z.string().uuid(),
  epoch: z.string().uuid(),
  stage: z.string(),
  elapsedNanos: z.number().nonnegative(),
});

const ownerSnapshotSchema = z.object({
  schemaVersion: z.literal(1),
  environment: z.string(),
  owner: z.string(),
  instance: z.string(),
  epoch: z.string().uuid(),
  observedAt: zInstant,
  maxAgeMillis: z.number(),
  coverage: z.string(),
  facts: z.array(snapshotFactSchema),
  attempts: z.array(snapshotAttemptSchema),
  activeAvailable: z.boolean(),
  expiredAttempts: z.number(),
  rejectedAttempts: z.number(),
});

export const evidenceCacheViewSchema = z.object({
  source: z.string(),
  receivedAt: zInstant.nullable(),
  snapshot: ownerSnapshotSchema.nullable(),
  freshness: z.string(),
  uncertainty: z.string(),
  conflicts: z.array(ownerSnapshotSchema),
});

export const snapshotResponseSchema = z.object({
  asOf: zInstant,
  coverage: z.string(),
  uncertainty: z.string(),
  data: z.unknown(),
});

export const overviewDataSchema = z.array(evidenceCacheViewSchema);
export const cellsDataSchema = z.array(evidenceCacheViewSchema);
export const activeOpsDataSchema = z.array(
  z.object({
    source: z.string(),
    freshness: z.string(),
    lastObservedAt: zInstant.nullable(),
    attempts: z.array(snapshotAttemptSchema),
    registryAvailable: z.boolean(),
    expired: z.number(),
    rejected: z.number(),
    coverage: z.string(),
  }),
);

export type RecentView = z.infer<typeof recentViewSchema>;
export type RecentGrant = z.infer<typeof recentGrantSchema>;
export type EvidenceCacheView = z.infer<typeof evidenceCacheViewSchema>;
export type SnapshotEnvelope<T> = {
  asOf: string;
  coverage: string;
  uncertainty: string;
  data: T;
};

export const searchQuerySchema = z.object({
  environment: zOwnerName,
  owner: zOwnerName,
  scope: zScope,
  from: z.string().regex(/^\d{4}-\d{2}-\d{2}T.+(Z)$/),
  to: z.string().regex(/^\d{4}-\d{2}-\d{2}T.+(Z)$/),
  state: zState.default("ANY"),
  limit: z.coerce.number().int().min(1).max(16).default(16),
  cursor: zCursor.optional().default(""),
});

export type SearchQuery = z.infer<typeof searchQuerySchema>;
