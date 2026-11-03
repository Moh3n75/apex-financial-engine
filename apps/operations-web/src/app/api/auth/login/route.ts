import { NextResponse } from "next/server";
import { isM36aEnabled, loadAuthConfig } from "@/server/auth/config";
import { beginLogin } from "@/server/auth/oidc";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  if (!isM36aEnabled()) {
    return NextResponse.json(
      { code: "AUTH_DISABLED", message: "M36A authentication is not enabled." },
      { status: 404 },
    );
  }
  try {
    loadAuthConfig();
    const url = new URL(req.url);
    const returnTo = url.searchParams.get("returnTo") || "/";
    const location = await beginLogin(returnTo);
    return NextResponse.redirect(location);
  } catch {
    return NextResponse.json(
      { code: "AUTH_START_FAILED", message: "Unable to start login." },
      { status: 500 },
    );
  }
}
