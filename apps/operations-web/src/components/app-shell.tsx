import { isFixtureMode } from "@/server/origin";
import { Shell } from "@/components/shell";

export async function AppShell({
  children,
  active,
}: {
  children: React.ReactNode;
  active: string;
}) {
  return (
    <Shell active={active} syntheticDemo={isFixtureMode()}>
      {children}
    </Shell>
  );
}
