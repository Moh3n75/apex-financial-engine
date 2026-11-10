import { defineConfig, devices } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e", testMatch: /control-center-live\.spec\.ts/, workers: 1,
  timeout: 120_000, retries: 0, reporter: "list",
  use: { baseURL: "http://127.0.0.1:3000", trace: "off", screenshot: "off" },
  projects: [{ name: "authenticated-live", use: { ...devices["Desktop Chrome"], channel: "chrome" } }],
});
