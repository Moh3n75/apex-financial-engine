import { expect, test } from "@playwright/test";
import { loginViaIdentity } from "./m36a-auth-helpers";

/**
 * Session sealed on Next A must fail closed on an instance with a different sealing key.
 */
test("M36B wrong session key denies without auto-accept", async ({ browser }) => {
  const originA = process.env.OPS04B_NEXT_ORIGIN ?? "http://127.0.0.1:3000";
  const originWrong = process.env.M36B_NEXT_B_ORIGIN;
  if (!originWrong) {
    throw new Error("M36B_NEXT_B_ORIGIN must point at wrong-key Next instance");
  }

  const context = await browser.newContext({ baseURL: originA });
  const page = await context.newPage();
  await loginViaIdentity(page);
  const cookies = await context.cookies();

  const wrong = await browser.newContext({ baseURL: originWrong });
  await wrong.addCookies(
    cookies
      .filter((c) => c.name === "apex_ops_session")
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
  const pageW = await wrong.newPage();
  const sessionProbe = await pageW.request.get(originWrong + "/api/auth/session");
  expect(sessionProbe.status()).toBeGreaterThanOrEqual(200);
  const body = await sessionProbe.json();
  expect(body.authenticated).toBeFalsy();

  await context.close();
  await wrong.close();
});
