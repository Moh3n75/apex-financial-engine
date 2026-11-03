import { cn } from "@/lib/cn";

const tones: Record<string, string> = {
  neutral: "bg-zinc-100 text-zinc-800 border-zinc-300 dark:bg-zinc-800 dark:text-zinc-100 dark:border-zinc-600",
  success: "bg-emerald-50 text-emerald-900 border-emerald-300 dark:bg-emerald-950 dark:text-emerald-100 dark:border-emerald-700",
  warning: "bg-amber-50 text-amber-950 border-amber-300 dark:bg-amber-950 dark:text-amber-100 dark:border-amber-700",
  danger: "bg-rose-50 text-rose-950 border-rose-300 dark:bg-rose-950 dark:text-rose-100 dark:border-rose-700",
  info: "bg-sky-50 text-sky-950 border-sky-300 dark:bg-sky-950 dark:text-sky-100 dark:border-sky-700",
};

export function Badge({
  children,
  tone = "neutral",
  className,
}: {
  children: React.ReactNode;
  tone?: keyof typeof tones;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded border px-2 py-0.5 text-xs font-medium tracking-wide",
        tones[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

export function freshnessTone(v: string): keyof typeof tones {
  if (v === "FRESH") return "success";
  if (v === "STALE" || v === "CLOCK_UNCERTAIN") return "warning";
  if (v === "MISSING" || v === "UNKNOWN") return "danger";
  return "neutral";
}

export function availabilityTone(v: string): keyof typeof tones {
  if (v === "AVAILABLE") return "success";
  if (v === "BACKOFF" || v === "SHED") return "warning";
  if (v === "PERMISSION_GAP" || v === "RESTART_WALK_REQUIRED" || v === "SOURCE_UNAVAILABLE")
    return "danger";
  return "neutral";
}
