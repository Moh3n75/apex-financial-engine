import { expect, test } from "@playwright/test";
import { assertNoTokenLeakInHtml, loginViaIdentity } from "./m36a-auth-helpers";

/**
 * Login on Next A; session cookie must decrypt on Next B with shared APEX_OPS_SESSION_SECRET.
 */
test("M36B two-instance session continuity", async ({ browser }) => {
  const originA = process.env.OPS04B_NEXT_ORIGIN ?? "http://127.0.0.1:3000";
  const originB = process.env.M36B_NEXT_B_ORIGIN ?? "http://127.0.0.1:3001";

  const context = await browser.newContext({ baseURL: originA });
  const page = await context.newPage();
  await loginViaIdentity(page);
  const htmlA = await page.content();
  assertNoTokenLeakInHtml(htmlA);

  const cookies = await context.cookies();
  const session = cookies.find((c) => c.name === "apex_ops_session");
  expect(session, "httpOnly session cookie present").toBeTruthy();
  expect(session!.httpOnly).toBe(true);
  expect(session!.sameSite?.toLowerCase()).toBe("lax");

  // Host-only cookie on 127.0.0.1 is port-agnostic — set domain explicitly for B.
  const contextB = await browser.newContext({ baseURL: originB });
  await contextB.addCookies(
    cookies
      .filter((c) => c.name === "apex_ops_session" || c.name === "apex_ops_oidc")
      .map((c) => ({
        name: c.name,
        value: c.value,
        domain: "127.0.0.1",
        path: c.path || "/",
        httpOnly: c.httpOnly,
        secure: c.secure,
        sameSite: (c.sameSite as "Lax" | "Strict" | "None") || "Lax",
        expires: c.expires > 0 ? c.expires : undefined,
      })),
  );
  const pageB = await contextB.newPage();
  await pageB.goto(originB + "/");
  await expect(pageB.getByText("LOCAL", { exact: true })).toBeVisible({ timeout: 30_000 });
  const sessionProbe = await pageB.request.get(originB + "/api/auth/session");
  expect(sessionProbe.status()).toBe(200);
  const body = await sessionProbe.json();
  expect(body.authenticated).toBe(true);
  assertNoTokenLeakInHtml(await pageB.content());

  await context.close();
  await contextB.close();
});
