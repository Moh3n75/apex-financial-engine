import { NextResponse } from "next/server";
import { isM36aEnabled } from "@/server/auth/config";
import { hasOpsConsoleScope, readSession } from "@/server/auth/session";

export const dynamic = "force-dynamic";

/** Safe session probe — never returns tokens. */
export async function GET() {
  if (!isM36aEnabled()) {
    return NextResponse.json({ enabled: false, authenticated: false });
  }
  const session = await readSession();
  if (!session || !hasOpsConsoleScope(session)) {
    return NextResponse.json({ enabled: true, authenticated: false });
  }
  return NextResponse.json({
    enabled: true,
    authenticated: true,
    sub: session.sub,
    tenantId: session.tenantId,
    scopes: session.scopes,
    expiresAt: session.expiresAt,
  });
}
