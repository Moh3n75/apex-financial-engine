/**
 * Pure M36B deployment grant matching (shared by BFF + unit tests).
 * Console remains the authoritative enforcement point.
 */

export type ResourceGrant = {
  subject: string;
  environment: string;
  owner: string;
  kinds: string[];
  scopes: string[];
};

export type ResourceAuthConfig = {
  enabled: boolean;
  grants: ResourceGrant[];
};

function parseKindsOrScopes(raw: string | undefined): string[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function parseResourceGrantsJson(raw: string): ResourceGrant[] {
  const parsed = JSON.parse(raw) as Array<Record<string, string>>;
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error("empty grants");
  }
  const grants = parsed.map((g) => ({
    subject: String(g.subject ?? "").trim(),
    environment: String(g.environment ?? "test").trim(),
    owner: String(g.owner ?? "").trim(),
    kinds: parseKindsOrScopes(g.kinds).map((k) => k.toUpperCase()),
    scopes: parseKindsOrScopes(g.scopes).map((s) => s.toLowerCase()),
  }));
  for (const g of grants) {
    if (!g.subject || !g.owner || !g.environment || g.kinds.length === 0 || g.scopes.length === 0) {
      throw new Error("invalid grant row");
    }
  }
  return grants;
}

function subjectMatches(grantSubject: string, sub: string): boolean {
  if (grantSubject === "*") return true;
  return grantSubject === sub;
}

export function grantAllows(
  g: ResourceGrant,
  sub: string,
  environment: string,
  owner: string,
  kind: string,
  scope: string,
): boolean {
  if (!subjectMatches(g.subject, sub)) return false;
  if (g.environment.toLowerCase() !== environment.toLowerCase()) return false;
  if (g.owner.toLowerCase() !== owner.toLowerCase()) return false;
  const kindU = kind.toUpperCase();
  const scopeL = scope.toLowerCase();
  return g.kinds.includes(kindU) && g.scopes.includes(scopeL);
}

export function evaluateResourceGrant(
  cfg: ResourceAuthConfig,
  session: { sub: string },
  environment: string,
  owner: string,
  kind: string,
  scope: string,
): boolean {
  if (!cfg.enabled) return true;
  if (!session.sub || !environment || !owner || !kind || !scope) return false;
  return cfg.grants.some((g) =>
    grantAllows(g, session.sub, environment, owner, kind, scope),
  );
}

export function evaluateOwnerVisible(
  cfg: ResourceAuthConfig,
  session: { sub: string },
  environment: string,
  owner: string,
): boolean {
  if (!cfg.enabled) return true;
  return cfg.grants.some(
    (g) =>
      subjectMatches(g.subject, session.sub) &&
      g.environment.toLowerCase() === environment.toLowerCase() &&
      g.owner.toLowerCase() === owner.toLowerCase(),
  );
}
