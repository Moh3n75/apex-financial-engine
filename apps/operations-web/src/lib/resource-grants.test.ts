import { describe, expect, it } from "vitest";
import {
  evaluateResourceGrant,
  parseResourceGrantsJson,
} from "./resource-grants";

describe("resource-grants", () => {
  it("allows exact grant and denies other owner equally", () => {
    const grants = parseResourceGrantsJson(
      JSON.stringify([
        {
          subject: "user-a",
          environment: "test",
          owner: "order-cell",
          kinds: "OUTBOX_EVENT",
          scopes: "owner",
        },
      ]),
    );
    const cfg = { enabled: true, grants };
    expect(
      evaluateResourceGrant(cfg, { sub: "user-a" }, "test", "order-cell", "OUTBOX_EVENT", "owner"),
    ).toBe(true);
    expect(
      evaluateResourceGrant(cfg, { sub: "user-a" }, "test", "payment-cell", "OUTBOX_EVENT", "owner"),
    ).toBe(false);
    expect(
      evaluateResourceGrant(cfg, { sub: "user-b" }, "test", "order-cell", "OUTBOX_EVENT", "owner"),
    ).toBe(false);
  });
});
