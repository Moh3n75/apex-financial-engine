import { searchQuerySchema, type SearchQuery } from "@/server/schemas";

const ALLOWED_KEYS = new Set([
  "environment",
  "owner",
  "scope",
  "from",
  "to",
  "state",
  "limit",
  "cursor",
]);

export class QueryValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueryValidationError";
  }
}

export function parseSearchParams(input: URLSearchParams): SearchQuery {
  const keys = [...input.keys()];
  const unique = new Set(keys);
  if (unique.size !== keys.length) {
    throw new QueryValidationError("Duplicate query parameters are rejected.");
  }
  for (const key of unique) {
    if (!ALLOWED_KEYS.has(key)) {
      throw new QueryValidationError(`Unknown query parameter rejected: ${key}`);
    }
  }

  const parsed = searchQuerySchema.safeParse({
    environment: input.get("environment") ?? undefined,
    owner: input.get("owner") ?? undefined,
    scope: input.get("scope") ?? undefined,
    from: input.get("from") ?? undefined,
    to: input.get("to") ?? undefined,
    state: input.get("state") ?? "ANY",
    limit: input.get("limit") ?? "16",
    cursor: input.get("cursor") ?? "",
  });
  if (!parsed.success) {
    throw new QueryValidationError("Invalid search bounds.");
  }

  const from = Date.parse(parsed.data.from);
  const to = Date.parse(parsed.data.to);
  if (!Number.isFinite(from) || !Number.isFinite(to) || !(from < to)) {
    throw new QueryValidationError("Invalid UTC window.");
  }
  const spanMs = to - from;
  if (spanMs > 60 * 60 * 1000) {
    throw new QueryValidationError("Window exceeds one hour.");
  }
  const now = Date.now();
  if (from < now - 60 * 60 * 1000 - 5000) {
    throw new QueryValidationError("from is older than one-hour lookback.");
  }
  if (to > now + 5000) {
    throw new QueryValidationError("to is in the future.");
  }
  return parsed.data;
}

export function toUpstreamSearchParams(q: SearchQuery): URLSearchParams {
  const p = new URLSearchParams();
  p.set("environment", q.environment);
  p.set("owner", q.owner);
  p.set("scope", q.scope);
  p.set("from", q.from);
  p.set("to", q.to);
  p.set("state", q.state);
  p.set("limit", String(q.limit));
  if (q.cursor) p.set("cursor", q.cursor);
  return p;
}

export function assertOpaqueCursor(cursor: string | undefined): void {
  if (!cursor) return;
  if (!/^[A-Za-z0-9._-]{1,256}$/.test(cursor)) {
    throw new QueryValidationError("Cursor rejected.");
  }
}
