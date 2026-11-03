import "server-only";
import { isM36aEnabled } from "./config";
import { AuthError } from "./errors";
import { refreshSession } from "./oidc";
import { hasOpsConsoleScope, readSession, type OpsSession } from "./session";

export async function requireOpsSession(): Promise<OpsSession> {
  if (!isM36aEnabled()) {
    throw new AuthError(500, "AUTH_MISCONFIGURED", "Auth check invoked while M36A disabled.");
  }
  let session = await readSession();
  if (!session) {
    throw new AuthError(401, "UNAUTHENTICATED", "Sign-in required.");
  }
  if (session.expiresAt < Date.now() + 15_000) {
    session = (await refreshSession(session)) ?? session;
  }
  if (session.expiresAt < Date.now()) {
    throw new AuthError(401, "SESSION_EXPIRED", "Session expired.");
  }
  if (!hasOpsConsoleScope(session)) {
    throw new AuthError(403, "FORBIDDEN", "Missing ops.console.read.");
  }
  return session;
}

export async function optionalUpstreamBearer(): Promise<string | undefined> {
  if (!isM36aEnabled()) return undefined;
  const session = await requireOpsSession();
  return session.accessToken;
}
