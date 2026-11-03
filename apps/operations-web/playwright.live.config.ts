import { defineConfig, devices } from "@playwright/test";

/**
 * Live Browser → BFF → Java Console acceptance.
 * Next.js must already be running with OPS04B_REQUIRE_LIVE=true and fixture mode OFF.
 * Never starts a fixture webServer.
 */
export default defineConfig({
  testDir: "./e2e",
  testMatch: /live-.*\.spec\.ts/,
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  timeout: 120_000,
  reporter: [["list"], ["json", { outputFile: "test-results/live-results.json" }]],
  use: {
    baseURL: process.env.OPS04B_NEXT_ORIGIN ?? "http://127.0.0.1:3000",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chrome-live",
      use: { ...devices["Desktop Chrome"], channel: "chrome" },
    },
  ],
});
