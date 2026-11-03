import { redirect } from "next/navigation";
import { isM36aEnabled } from "@/server/auth/config";
import { hasOpsConsoleScope, readSession } from "@/server/auth/session";
import { isFixtureMode } from "@/server/origin";
import { Shell } from "@/components/shell";

export async function AppShell({
  children,
  active,
}: {
  children: React.ReactNode;
  active: string;
}) {
  const fixture = isFixtureMode();
  const m36a = isM36aEnabled();
  let signedIn = false;
  let subject: string | undefined;

  if (m36a) {
    const session = await readSession();
    // Expiry is enforced again on BFF/token refresh; shell only gates missing/forbidden sessions.
    if (!session || !hasOpsConsoleScope(session)) {
      redirect("/login");
    }
    signedIn = true;
    subject = session.sub || undefined;
  }

  return (
    <Shell
      active={active}
      syntheticDemo={fixture}
      m36aEnabled={m36a}
      signedIn={signedIn}
      subject={subject}
    >
      {children}
    </Shell>
  );
}
