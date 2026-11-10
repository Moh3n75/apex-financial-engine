import { describe, expect, it } from "vitest";
import { validateOperationsOrigin } from "./origin-policy";

describe("validateOperationsOrigin", () => {
  it("allowlists loopback Console origins including alternate ports", () => {
    expect(validateOperationsOrigin("http://127.0.0.1:9190")).toBe("http://127.0.0.1:9190");
    expect(validateOperationsOrigin("http://localhost:9190")).toBe("http://127.0.0.1:9190");
    expect(validateOperationsOrigin("http://127.0.0.1:19290")).toBe("http://127.0.0.1:19290");
    expect(validateOperationsOrigin("https://localhost:9190")).toBe("https://127.0.0.1:9190");
  });

  it("rejects remote or path-bearing origins", () => {
    expect(() => validateOperationsOrigin("https://evil.example:9190")).toThrow(/allowlist/);
    expect(() => validateOperationsOrigin("http://127.0.0.1:9190/ops")).toThrow(/origin-only/);
    expect(() => validateOperationsOrigin("http://127.0.0.1:80")).toThrow(/port/);
    expect(() => validateOperationsOrigin(undefined)).toThrow(/required/);
  });
});
