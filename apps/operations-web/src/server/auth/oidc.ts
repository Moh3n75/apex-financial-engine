import "server-only";
import { generators, Issuer, type Client, type TokenSet } from "openid-client";
import { loadAuthConfig } from "./config";
import {
  consumeOidcTransient,
  hasOpsConsoleScope,
  sessionFromTokens,
  writeOidcTransient,
  writeSession,
  type OpsSession,
} from "./session";

let cachedClient: Client | null = null;
let cachedIssuer: string | null = null;

async function getClient(): Promise<Client> {
  const cfg = loadAuthConfig();
  if (!cfg.enabled) {
    throw new Error("M36A auth is disabled.");
  }
  if (cachedClient && cachedIssuer === cfg.issuer) {
    return cachedClient;
  }
  const discovered = await Issuer.discover(cfg.issuer);
  cachedClient = new discovered.Client({
    client_id: cfg.clientId,
    client_secret: cfg.clientSecret,
    redirect_uris: [cfg.redirectUri],
    response_types: ["code"],
    token_endpoint_auth_method: "client_secret_basic",
  });
  cachedIssuer = cfg.issuer;
  return cachedClient;
}

export async function beginLogin(returnTo = "/"): Promise<string> {
  const cfg = loadAuthConfig();
  const client = await getClient();
  const state = generators.state();
  const nonce = generators.nonce();
  const codeVerifier = generators.codeVerifier();
  const codeChallenge = generators.codeChallenge(codeVerifier);
  await writeOidcTransient({
    state,
    nonce,
    codeVerifier,
    returnTo: safeReturnTo(returnTo),
  });
  return client.authorizationUrl({
    scope: cfg.scopes,
    state,
    nonce,
    code_challenge: codeChallenge,
    code_challenge_method: "S256",
    redirect_uri: cfg.redirectUri,
  });
}

export async function finishLogin(callbackUrl: string): Promise<{ returnTo: string; session: OpsSession }> {
  const cfg = loadAuthConfig();
  const client = await getClient();
  const transient = await consumeOidcTransient();
  if (!transient) {
    throw new Error("OIDC state missing or expired.");
  }
  const params = client.callbackParams(callbackUrl);
  if (params.state !== transient.state) {
    throw new Error("OIDC state mismatch.");
  }
  const tokenSet: TokenSet = await client.callback(cfg.redirectUri, params, {
    state: transient.state,
    nonce: transient.nonce,
    code_verifier: transient.codeVerifier,
  });
  if (!tokenSet.access_token) {
    throw new Error("Token response missing access_token.");
  }
  const session = sessionFromTokens({
    access_token: tokenSet.access_token,
    refresh_token: tokenSet.refresh_token,
    id_token: tokenSet.id_token,
    expires_in: tokenSet.expires_in,
  });
  if (!session.sub || !hasOpsConsoleScope(session)) {
    throw new Error("Access token missing required ops.console.read scope.");
  }
  // id_token aud is the RP client_id; access-token aud must be the Console resource.
  const { decodeJwt } = await import("jose");
  const accessClaims = decodeJwt(tokenSet.access_token);
  const accessAud = accessClaims.aud;
  const audList = Array.isArray(accessAud) ? accessAud : accessAud ? [accessAud] : [];
  if (!audList.map(String).includes(cfg.audience)) {
    throw new Error("Access token audience rejected.");
  }
  if (String(accessClaims.principal_type ?? "") !== "user") {
    throw new Error("Only user principal_type is accepted for the Operations Console.");
  }
  await writeSession(session);
  return { returnTo: transient.returnTo || "/", session };
}

export async function refreshSession(session: OpsSession): Promise<OpsSession | null> {
  if (!session.refreshToken) return null;
  const client = await getClient();
  const tokenSet = await client.refresh(session.refreshToken);
  if (!tokenSet.access_token) return null;
  const next = sessionFromTokens({
    access_token: tokenSet.access_token,
    refresh_token: tokenSet.refresh_token ?? session.refreshToken,
    id_token: tokenSet.id_token ?? session.idToken,
    expires_in: tokenSet.expires_in,
  });
  if (!hasOpsConsoleScope(next)) return null;
  await writeSession(next);
  return next;
}

/** Path-only same-origin return; rejects protocol-relative and absolute URLs. */
export function safeReturnTo(returnTo: string): string {
  if (!returnTo.startsWith("/") || returnTo.startsWith("//") || returnTo.includes("\\")) {
    return "/";
  }
  try {
    const u = new URL(returnTo, "http://127.0.0.1:3000");
    if (u.hostname !== "127.0.0.1" && u.hostname !== "localhost") {
      return "/";
    }
    return u.pathname + u.search + u.hash;
  } catch {
    return "/";
  }
}

export async function endLoginAtIssuer(idToken?: string): Promise<string | null> {
  const cfg = loadAuthConfig();
  if (!cfg.enabled) return null;
  try {
    const client = await getClient();
    if (typeof client.endSessionUrl === "function" && idToken) {
      return client.endSessionUrl({
        id_token_hint: idToken,
        post_logout_redirect_uri: "http://127.0.0.1:3000/",
      });
    }
  } catch {
    /* Identity may omit end_session; local clear is enough */
  }
  return null;
}
