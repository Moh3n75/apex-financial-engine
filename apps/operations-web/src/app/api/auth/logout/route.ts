import { NextResponse } from "next/server";
import { isM36aEnabled } from "@/server/auth/config";
import { endLoginAtIssuer } from "@/server/auth/oidc";
import { clearSession, readSession } from "@/server/auth/session";

export const dynamic = "force-dynamic";

export async function POST() {
  if (!isM36aEnabled()) {
    return NextResponse.json({ ok: true });
  }
  const session = await readSession();
  await clearSession();
  const end = await endLoginAtIssuer(session?.idToken);
  if (end) {
    return NextResponse.redirect(end, 303);
  }
  return NextResponse.redirect("http://127.0.0.1:3000/login", 303);
}

export async function GET() {
  return POST();
}
