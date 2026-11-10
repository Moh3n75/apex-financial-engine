import Link from "next/link";
import { Badge } from "@/components/ui/badge";

const nav = [
  { href: "/", label: "Overview" },
  { href: "/cells", label: "Cells & Sources" },
  { href: "/topology", label: "Topology" },
  { href: "/events", label: "Recent Events" },
  { href: "/inbox", label: "Known-Group Inbox" },
  { href: "/workflows", label: "Workflows" },
  { href: "/requests", label: "Requests" },
  { href: "/operations", label: "Active Operations" },
  { href: "/diagnostics", label: "Diagnostics" },
];

export function Shell({
  children,
  active,
  syntheticDemo = false,
  m36aEnabled = false,
  signedIn = false,
  subject,
}: {
  children: React.ReactNode;
  active: string;
  syntheticDemo?: boolean;
  m36aEnabled?: boolean;
  signedIn?: boolean;
  subject?: string;
}) {
  return (
    <div className="min-h-screen bg-zinc-50 text-zinc-900 dark:bg-zinc-950 dark:text-zinc-50">
      <header className="border-b border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center gap-3 px-4 py-3">
          <div className="mr-4">
            <div className="text-sm font-semibold tracking-wide">APEX Operations</div>
            <div className="text-xs text-zinc-500">Restricted OPS-04 console preview</div>
          </div>
          <Badge tone="warning">LOCAL</Badge>
          <Badge tone="info">READ-ONLY</Badge>
          <Badge tone="danger">NOT PRODUCTION</Badge>
          {syntheticDemo ? <Badge tone="warning">SYNTHETIC DEMO</Badge> : null}
          {!m36aEnabled && !syntheticDemo ? (
            <Badge tone="neutral">NO M36 AUTH</Badge>
          ) : null}
          {m36aEnabled ? <Badge tone="info">M36A OIDC</Badge> : null}
          {m36aEnabled && signedIn ? <Badge tone="info">SIGNED IN</Badge> : null}
          {m36aEnabled && signedIn ? (
            <form action="/api/auth/logout" method="post" className="ml-auto">
              <button
                type="submit"
                className="rounded px-2 py-1 text-xs text-zinc-600 hover:bg-zinc-100 dark:text-zinc-300 dark:hover:bg-zinc-800"
              >
                Sign out{subject ? ` (${subject.slice(0, 12)})` : ""}
              </button>
            </form>
          ) : null}
        </div>
        <nav
          aria-label="Primary"
          className="mx-auto flex max-w-7xl gap-1 overflow-x-auto px-2 pb-2"
        >
          {nav.map((item) => {
            const isActive = active === item.href;
            return (
              <Link
                key={item.href}
                href={item.href}
                className={
                  isActive
                    ? "rounded-md bg-zinc-900 px-3 py-1.5 text-sm text-white dark:bg-zinc-100 dark:text-zinc-900"
                    : "rounded-md px-3 py-1.5 text-sm text-zinc-600 hover:bg-zinc-100 dark:text-zinc-300 dark:hover:bg-zinc-800"
                }
                aria-current={isActive ? "page" : undefined}
              >
                {item.label}
              </Link>
            );
          })}
        </nav>
      </header>
      <main className="mx-auto max-w-7xl px-4 py-6">{children}</main>
      <footer className="mx-auto max-w-7xl px-4 pb-8 text-xs text-zinc-500">
        Browser talks only to this Next.js process on 127.0.0.1. Java Console remains
        loopback-only. No Replay/Retry/Pause/Resume actions are registered. OPS-03D full
        regression is not green; this UI is not release qualification.
      </footer>
    </div>
  );
}
