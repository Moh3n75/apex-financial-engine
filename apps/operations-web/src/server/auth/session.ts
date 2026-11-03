import "server-only";
import { CompactEncrypt, compactDecrypt, decodeJwt } from "jose";
import { cookies } from "next/headers";
import { loadAuthConfig } from "./config";

export const SESSION_COOKIE = "apex_ops_session";
export const OIDC_STATE_COOKIE = "apex_ops_oidc";

export type OpsSession = {
  accessToken: string;
  refreshToken?: string;
  idToken?: string;
  expiresAt: number;
  sub: string;
  tenantId: string;
  scopes: string[];
};

export type OidcTransient = {
  state: string;
  nonce: string;
  codeVerifier: string;
  returnTo: string;
};

function secretKey(): Uint8Array {
  const cfg = loadAuthConfig();
  return new TextEncoder().encode(cfg.sessionSecret.padEnd(32, "0").slice(0, 64));
}

async function seal(payload: unknown): Promise<string> {
  const encoded = new TextEncoder().encode(JSON.stringify(payload));
  return new CompactEncrypt(encoded)
    .setProtectedHeader({ alg: "dir", enc: "A256GCM" })
    .encrypt(await cryptoKey());
}

async function cryptoKey(): Promise<CryptoKey> {
  const raw = secretKey();
  // Copy into a definite ArrayBuffer view for DOM lib BufferSource typing.
  const material = Uint8Array.from(raw);
  const hash = await crypto.subtle.digest("SHA-256", material);
  return crypto.subtle.importKey("raw", hash, "AES-GCM", false, ["encrypt", "decrypt"]);
}

async function unseal<T>(token: string): Promise<T | null> {
  try {
    const { plaintext } = await compactDecrypt(token, await cryptoKey());
    return JSON.parse(new TextDecoder().decode(plaintext)) as T;
  } catch {
    return null;
  }
}

export async function readSession(): Promise<OpsSession | null> {
  if (!loadAuthConfig().enabled) return null;
  const jar = await cookies();
  const raw = jar.get(SESSION_COOKIE)?.value;
  if (!raw) return null;
  const session = await unseal<OpsSession>(raw);
  if (!session?.accessToken || !session.expiresAt) return null;
  return session;
}

export async function writeSession(session: OpsSession): Promise<void> {
  const cfg = loadAuthConfig();
  const jar = await cookies();
  jar.set(SESSION_COOKIE, await seal(session), {
    httpOnly: true,
    sameSite: "lax",
    secure: cfg.cookieSecure,
    path: "/",
    maxAge: 60 * 60 * 8,
  });
}

export async function clearSession(): Promise<void> {
  const jar = await cookies();
  jar.delete(SESSION_COOKIE);
  jar.delete(OIDC_STATE_COOKIE);
}

export async function writeOidcTransient(value: OidcTransient): Promise<void> {
  const cfg = loadAuthConfig();
  const jar = await cookies();
  jar.set(OIDC_STATE_COOKIE, await seal(value), {
    httpOnly: true,
    sameSite: "lax",
    secure: cfg.cookieSecure,
    path: "/",
    maxAge: 600,
  });
}

export async function consumeOidcTransient(): Promise<OidcTransient | null> {
  const jar = await cookies();
  const raw = jar.get(OIDC_STATE_COOKIE)?.value;
  jar.delete(OIDC_STATE_COOKIE);
  if (!raw) return null;
  return unseal<OidcTransient>(raw);
}

export function sessionFromTokens(tokens: {
  access_token: string;
  refresh_token?: string;
  id_token?: string;
  expires_in?: number;
}): OpsSession {
  const claims = decodeJwt(tokens.access_token);
  const scopeClaim = claims.scope;
  const scopes =
    typeof scopeClaim === "string"
      ? scopeClaim.split(/\s+/).filter(Boolean)
      : Array.isArray(scopeClaim)
        ? scopeClaim.map(String)
        : [];
  const expiresAt =
    typeof claims.exp === "number"
      ? claims.exp * 1000
      : Date.now() + (tokens.expires_in ?? 300) * 1000;
  return {
    accessToken: tokens.access_token,
    refreshToken: tokens.refresh_token,
    idToken: tokens.id_token,
    expiresAt,
    sub: String(claims.sub ?? ""),
    tenantId: String(claims.tenant_id ?? ""),
    scopes,
  };
}

export function hasOpsConsoleScope(session: OpsSession): boolean {
  return session.scopes.includes("ops.console.read");
}
