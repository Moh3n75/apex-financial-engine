import { cn } from "@/lib/cn";

export function Panel({
  title,
  description,
  children,
  className,
}: {
  title: string;
  description?: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section
      className={cn(
        "rounded-lg border border-zinc-200 bg-white p-4 shadow-sm dark:border-zinc-800 dark:bg-zinc-900",
        className,
      )}
    >
      <div className="mb-3">
        <h2 className="text-base font-semibold">{title}</h2>
        {description ? (
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">{description}</p>
        ) : null}
      </div>
      {children}
    </section>
  );
}

export function EmptyState({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-md border border-dashed border-zinc-300 bg-zinc-50 px-3 py-6 text-sm text-zinc-600 dark:border-zinc-700 dark:bg-zinc-950 dark:text-zinc-400">
      {children}
    </div>
  );
}

export function ErrorState({ children }: { children: React.ReactNode }) {
  return (
    <div
      role="alert"
      className="rounded-md border border-rose-300 bg-rose-50 px-3 py-3 text-sm text-rose-950 dark:border-rose-800 dark:bg-rose-950 dark:text-rose-100"
    >
      {children}
    </div>
  );
}
