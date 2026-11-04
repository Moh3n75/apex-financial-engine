import { expect, test } from "@playwright/test";
import { assertNoTokenLeakInHtml, loginViaIdentity } from "./m36a-auth-helpers";
import fs from "node:fs";

test("M36B browser granted vs denied owner comparison", async ({ page }) => {
  const handshakePath = process.env.OPS04B_HANDSHAKE;
  if (!handshakePath || !fs.existsSync(handshakePath)) {
    throw new Error("OPS04B_HANDSHAKE required");
  }
  const hs = JSON.parse(fs.readFileSync(handshakePath, "utf8")) as {
    orderId: string;
  };
  expect(hs.orderId).toBeTruthy();

  await loginViaIdentity(page);
  assertNoTokenLeakInHtml(await page.content());

  // Fresh bounded window — handshake windowTo is often still in the future vs BFF policy.
  const to = new Date();
  const from = new Date(to.getTime() - 10 * 60_000);
  const fromQ = encodeURIComponent(from.toISOString());
  const toQ = encodeURIComponent(to.toISOString());

  const ok = await page.request.get(
    `/api/ops/recent/events?environment=test&owner=order-cell&scope=owner&from=${fromQ}&to=${toQ}&limit=16`,
  );
  const okBody = await ok.text();
  expect(ok.status(), okBody).toBe(200);
  expect(okBody.toLowerCase()).not.toContain("refresh_token");
  expect(okBody).not.toMatch(/eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\./);

  const denied = await page.request.get(
    `/api/ops/recent/events?environment=test&owner=payment-cell&scope=owner&from=${fromQ}&to=${toQ}&limit=16`,
  );
  expect(denied.status()).toBe(403);
  const deniedBody = await denied.text();
  expect(deniedBody).not.toContain("payment_authorizations");
  expect(deniedBody.toLowerCase()).not.toContain("stack trace");

  const tenant = await page.request.get(
    `/api/ops/recent/events?environment=test&owner=order-cell&scope=owner&from=${fromQ}&to=${toQ}&limit=16&tenantFilter=x`,
  );
  expect(tenant.status()).toBe(403);
});
