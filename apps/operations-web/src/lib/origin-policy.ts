/** Loopback-only Operations Console origins. Remote hosts are rejected. */

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost"]);

/** Safe configuration diagnostics: never include the supplied value (it may contain secrets). */
export class OperationsOriginError extends Error {}

export function validateOperationsOrigin(raw: string | undefined): string {
  if (!raw?.trim()) {
    throw new OperationsOriginError("APEX_OPERATIONS_API_ORIGIN is required. Set APEX_OPERATIONS_API_ORIGIN=http://127.0.0.1:9190 in apps/operations-web/.env.local and restart Next.js.");
  }
  let url: URL;
  try {
    url = new URL(raw.trim());
  } catch {
    throw new OperationsOriginError("APEX_OPERATIONS_API_ORIGIN is not a valid absolute URL.");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new OperationsOriginError("APEX_OPERATIONS_API_ORIGIN protocol rejected.");
  }
  if (url.username || url.password || url.search || url.hash || url.pathname !== "/") {
    throw new OperationsOriginError("APEX_OPERATIONS_API_ORIGIN must be origin-only (no path/query/userinfo).");
  }
  if (!LOOPBACK_HOSTS.has(url.hostname)) {
    throw new OperationsOriginError("APEX_OPERATIONS_API_ORIGIN is outside the fixed loopback allowlist.");
  }
  const port = url.port ? Number(url.port) : url.protocol === "https:" ? 443 : 80;
  if (!Number.isInteger(port) || port < 1024 || port > 65535) {
    throw new OperationsOriginError("APEX_OPERATIONS_API_ORIGIN port must be an explicit loopback port 1024–65535.");
  }
  // Normalize localhost → 127.0.0.1 so BFF fetches a single canonical origin.
  return `${url.protocol}//127.0.0.1:${port}`;
}
