import fs from "node:fs";
import { expect } from "@playwright/test";

export type LiveHandshake = {
  phase: string;
  fixtureMode: boolean;
  consoleOrigin: string;
  nextOrigin: string;
  environment: string;
  orderId: string;
  eventIds: Record<string, string[]>;
  workflowStates: Record<string, string>;
  inboxGroup: string;
  windowFrom: string;
  windowTo: string;
  consolePort: number;
};

/** Truncate fractional seconds to millis for HTML form / BFF window bounds. */
export function toFormInstant(raw: string): string {
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) throw new Error(`Invalid handshake instant: ${raw}`);
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

export function requireLiveHandshake(expectedPhase?: string): LiveHandshake {
  if (process.env.APEX_OPS_UI_FIXTURE_MODE === "true") {
    throw new Error("LIVE_E2E refused: APEX_OPS_UI_FIXTURE_MODE must not be true for live suite.");
  }
  const path = process.env.OPS04B_HANDSHAKE;
  if (!path || !fs.existsSync(path)) {
    throw new Error(
      "LIVE_E2E refused: OPS04B_HANDSHAKE missing — live Java stack did not publish readiness.",
    );
  }
  const raw = JSON.parse(fs.readFileSync(path, "utf8")) as LiveHandshake;
  if (raw.fixtureMode) {
    throw new Error("LIVE_E2E refused: handshake.fixtureMode=true (synthetic).");
  }
  if (!raw.consoleOrigin.startsWith("http://127.0.0.1:")) {
    throw new Error("LIVE_E2E refused: consoleOrigin is not loopback.");
  }
  if (!raw.orderId || !/^[0-9a-f-]{36}$/i.test(raw.orderId)) {
    throw new Error("LIVE_E2E refused: handshake missing real orderId.");
  }
  if (expectedPhase && raw.phase !== expectedPhase) {
    throw new Error(`LIVE_E2E refused: expected phase=${expectedPhase}, got ${raw.phase}`);
  }
  return raw;
}

export function assertNoDirectUpstream(leaked: string[], consolePort: number) {
  const bad = leaked.filter(
    (u) =>
      u.includes(`:${consolePort}`) ||
      u.includes(":9190") ||
      /:(5432|9092)\b/.test(u) ||
      (u.includes("/ops/v1/") && !u.includes("127.0.0.1:3000")),
  );
  expect(bad, `browser must not call Console/owner/DB directly: ${bad.join(", ")}`).toEqual([]);
}
