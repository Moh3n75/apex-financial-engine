import { defineConfig, devices } from "@playwright/test";

/**
 * Authenticated-negative Playwright. Requires an already-running Next with
 * APEX_OPS_M36A_ENABLED=true (no fixture). Does not start the webServer.
 */
export default defineConfig({
  testDir: "./e2e",
  testMatch: /m36a-unauth\.spec\.ts/,
  fullyParallel: false,
  retries: 0,
  reporter: "list",
  use: {
    baseURL: "http://127.0.0.1:3000",
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chrome",
      use: { ...devices["Desktop Chrome"], channel: "chrome" },
    },
  ],
});
