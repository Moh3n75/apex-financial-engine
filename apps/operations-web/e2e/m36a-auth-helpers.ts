import { expect, type Page } from "@playwright/test";

/**
 * Real browser OIDC against disposable Identity. Never injects cookies or bearer tokens.
 */
export async function loginViaIdentity(page: Page): Promise<void> {
  const user = process.env.APEX_OPS_M36A_LOGIN ?? "alice";
  const password = process.env.APEX_OPS_M36A_PASSWORD;
  if (!password) {
    throw new Error("APEX_OPS_M36A_PASSWORD required for authenticated live Playwright");
  }

  await page.goto("/login");
  await expect(page.getByRole("heading", { name: /sign-in/i })).toBeVisible();
  await page.getByRole("link", { name: /continue with apex identity/i }).click();

  // Identity login form (loopback issuer)
  await page.waitForURL(/\/login|\/oauth2\/authorize/, { timeout: 30_000 });
  if (page.url().includes("/login")) {
    await page.locator('input[name="username"], input#username').first().fill(user);
    await page.locator('input[name="password"], input#password').first().fill(password);
    await page.locator('button[type="submit"], input[type="submit"]').first().click();
  }

  // Optional consent screen
  await page.waitForTimeout(500);
  if (page.url().includes("/oauth2/authorize") || (await page.getByText(/consent|approve|allow/i).count()) > 0) {
    const submit = page.locator('button[type="submit"], input[type="submit"]').first();
    if (await submit.isVisible().catch(() => false)) {
      await submit.click();
    }
  }

  await page.waitForURL((url) => !url.pathname.startsWith("/login") && !url.href.includes("/oauth2/"), {
    timeout: 60_000,
  });
  // Session cookie is httpOnly — assert authenticated shell rather than reading cookie value.
  await expect(page.getByText("LOCAL", { exact: true })).toBeVisible({ timeout: 30_000 });
}

export function assertNoTokenLeakInHtml(html: string) {
  expect(html).not.toMatch(/eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+/); // JWT-shaped
  expect(html.toLowerCase()).not.toContain("client_secret");
  expect(html.toLowerCase()).not.toContain("refresh_token");
}
