import { defineConfig, devices } from "@playwright/test";

/**
 * Authenticated live Browser → OIDC Identity → Next BFF → M36A Console.
 * Next must already be running with APEX_OPS_M36A_ENABLED=true and fixture mode OFF.
 */
export default defineConfig({
  testDir: "./e2e",
  testMatch: /m36a-live-.*\.spec\.ts/,
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  timeout: 180_000,
  reporter: [["list"], ["json", { outputFile: "test-results/m36a-live-results.json" }]],
  use: {
    baseURL: process.env.OPS04B_NEXT_ORIGIN ?? "http://127.0.0.1:3000",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chrome-m36a-live",
      use: { ...devices["Desktop Chrome"], channel: "chrome" },
    },
  ],
});
