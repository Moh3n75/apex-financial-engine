import { expect, test } from "@playwright/test";

test.describe("M36B OIDC negatives", () => {
  test("mismatched state on callback fails closed", async ({ request, baseURL }) => {
    const origin = baseURL ?? "http://127.0.0.1:3000";
    // Hit callback with junk state/code without prior OIDC cookie → redirect to login error.
    const res = await request.get(
      origin + "/api/auth/callback?code=forged&state=mismatch-state-value",
      { maxRedirects: 0 },
    );
    expect([302, 303, 307, 308]).toContain(res.status());
    const loc = res.headers()["location"] ?? "";
    expect(loc).toMatch(/login|error/i);
  });

  test("malicious returnTo is not honored as open redirect", async ({ request, baseURL }) => {
    const origin = baseURL ?? "http://127.0.0.1:3000";
    const res = await request.get(
      origin + "/api/auth/login?returnTo=https://evil.example/phish",
      { maxRedirects: 0 },
    );
    // beginLogin clamps non-path returnTo to "/"; authorize redirect stays on Identity issuer.
    expect([302, 303, 307, 308]).toContain(res.status());
    const loc = res.headers()["location"] ?? "";
    expect(loc).not.toContain("evil.example");
  });

  test("protocol-relative returnTo rejected to path-only", async ({ request, baseURL }) => {
    const origin = baseURL ?? "http://127.0.0.1:3000";
    const res = await request.get(origin + "/api/auth/login?returnTo=//evil.example", {
      maxRedirects: 0,
    });
    expect([302, 303, 307, 308]).toContain(res.status());
    const loc = res.headers()["location"] ?? "";
    expect(loc).not.toMatch(/^\/\/evil/);
  });

  test("oversized query on callback does not authenticate", async ({ request, baseURL }) => {
    const origin = baseURL ?? "http://127.0.0.1:3000";
    const huge = "x".repeat(8_000);
    const res = await request.get(origin + `/api/auth/callback?code=${huge}&state=${huge}`, {
      maxRedirects: 0,
    });
    expect([302, 303, 307, 308, 400, 414, 431, 500]).toContain(res.status());
    if (res.status() >= 300 && res.status() < 400) {
      const loc = res.headers()["location"] ?? "";
      expect(loc).toMatch(/login|error/i);
    }
  });
});
