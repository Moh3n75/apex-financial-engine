import { defineConfig, devices } from "@playwright/test";

/**
 * M36B live: 2×Next session continuity, OIDC negatives, resource browser under JWT Console.
 */
export default defineConfig({
  testDir: "./e2e",
  testMatch: /m36b-.*\.spec\.ts/,
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  timeout: 180_000,
  reporter: [["list"], ["json", { outputFile: "test-results/m36b-results.json" }]],
  use: {
    baseURL: process.env.OPS04B_NEXT_ORIGIN ?? "http://127.0.0.1:3000",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chrome-m36b",
      use: { ...devices["Desktop Chrome"], channel: "chrome" },
    },
  ],
});
