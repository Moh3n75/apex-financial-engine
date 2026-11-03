const ALLOWED_ORIGINS = new Set(["http://127.0.0.1:9190", "http://localhost:9190"]);

export function validateOperationsOrigin(raw: string | undefined): string {
  if (!raw?.trim()) {
    throw new Error("APEX_OPERATIONS_API_ORIGIN is required (server-only loopback origin).");
  }
  let url: URL;
  try {
    url = new URL(raw.trim());
  } catch {
    throw new Error("APEX_OPERATIONS_API_ORIGIN is not a valid absolute URL.");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("APEX_OPERATIONS_API_ORIGIN protocol rejected.");
  }
  if (url.username || url.password || url.search || url.hash || url.pathname !== "/") {
    throw new Error("APEX_OPERATIONS_API_ORIGIN must be origin-only (no path/query/userinfo).");
  }
  const origin = `${url.protocol}//${url.host}`;
  if (!ALLOWED_ORIGINS.has(origin)) {
    throw new Error("APEX_OPERATIONS_API_ORIGIN is outside the fixed loopback allowlist.");
  }
  return origin.replace("localhost", "127.0.0.1");
}
