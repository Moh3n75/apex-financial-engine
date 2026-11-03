export function formatUtc(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toISOString().replace(".000Z", "Z");
  } catch {
    return "—";
  }
}

export function formatLocal(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString(undefined, {
      dateStyle: "medium",
      timeStyle: "medium",
    });
  } catch {
    return "—";
  }
}

export function elapsedLabel(nanos: number): string {
  const ms = nanos / 1_000_000;
  if (ms < 1) return `${nanos} ns`;
  if (ms < 1000) return `${ms.toFixed(1)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}
