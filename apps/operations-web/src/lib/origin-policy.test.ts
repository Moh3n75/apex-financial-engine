import { describe, expect, it } from "vitest";
import { validateOperationsOrigin } from "./origin-policy";

describe("validateOperationsOrigin", () => {
  it("allowlists only loopback Console origin", () => {
    expect(validateOperationsOrigin("http://127.0.0.1:9190")).toBe("http://127.0.0.1:9190");
    expect(validateOperationsOrigin("http://localhost:9190")).toBe("http://127.0.0.1:9190");
  });

  it("rejects remote or path-bearing origins", () => {
    expect(() => validateOperationsOrigin("https://evil.example:9190")).toThrow(/allowlist/);
    expect(() => validateOperationsOrigin("http://127.0.0.1:9190/ops")).toThrow(/origin-only/);
    expect(() => validateOperationsOrigin(undefined)).toThrow(/required/);
  });
});
