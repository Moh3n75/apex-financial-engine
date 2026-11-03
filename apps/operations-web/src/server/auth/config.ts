import "server-only";
import { isM36aEnabledFromEnv } from "@/lib/m36a-flags";

export type AuthConfig = {
  enabled: boolean;
  issuer: string;
  clientId: string;
  clientSecret: string;
  redirectUri: string;
  scopes: string;
  sessionSecret: string;
  cookieSecure: boolean;
  audience: string;
};

export function isM36aEnabled(): boolean {
  return isM36aEnabledFromEnv();
}

export function loadAuthConfig(): AuthConfig {
  const enabled = isM36aEnabled();
  const issuer = (process.env.APEX_OIDC_ISSUER ?? "").replace(/\/$/, "");
  const clientId = process.env.APEX_OIDC_CLIENT_ID ?? "";
  const clientSecret = process.env.APEX_OIDC_CLIENT_SECRET ?? "";
  const redirectUri = process.env.APEX_OIDC_REDIRECT_URI ?? "http://127.0.0.1:3000/api/auth/callback";
  const scopes = process.env.APEX_OIDC_SCOPES ?? "openid profile ops.console.read";
  const sessionSecret = process.env.APEX_OPS_SESSION_SECRET ?? "";
  const audience = process.env.APEX_OIDC_AUDIENCE ?? "apex-operations-console";
  const cookieSecure = process.env.APEX_OPS_COOKIE_SECURE === "true";

  if (enabled) {
    if (!issuer.startsWith("http://127.0.0.1") && !issuer.startsWith("https://")) {
      throw new Error("APEX_OIDC_ISSUER must be https or loopback http for M36A.");
    }
    if (!clientId || !clientSecret || sessionSecret.length < 32) {
      throw new Error("M36A requires client id/secret and APEX_OPS_SESSION_SECRET (≥32 chars).");
    }
    let redirect: URL;
    try {
      redirect = new URL(redirectUri);
    } catch {
      throw new Error("APEX_OIDC_REDIRECT_URI is invalid.");
    }
    if (redirect.hostname !== "127.0.0.1" && redirect.hostname !== "localhost") {
      throw new Error("APEX_OIDC_REDIRECT_URI must be loopback for M36A.");
    }
  }

  return {
    enabled,
    issuer,
    clientId,
    clientSecret,
    redirectUri,
    scopes,
    sessionSecret,
    cookieSecure,
    audience,
  };
}
