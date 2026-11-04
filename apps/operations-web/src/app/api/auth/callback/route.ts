import { NextResponse } from "next/server";
import { isM36aEnabled } from "@/server/auth/config";
import { finishLogin } from "@/server/auth/oidc";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  if (!isM36aEnabled()) {
    return NextResponse.json(
      { code: "AUTH_DISABLED", message: "M36A authentication is not enabled." },
      { status: 404 },
    );
  }
  try {
    const { returnTo } = await finishLogin(req.url);
    if (!returnTo.startsWith("/") || returnTo.startsWith("//")) {
      return NextResponse.redirect("http://127.0.0.1:3000/");
    }
    const dest = new URL(returnTo, "http://127.0.0.1:3000");
    if (dest.hostname !== "127.0.0.1" && dest.hostname !== "localhost") {
      return NextResponse.redirect("http://127.0.0.1:3000/");
    }
    return NextResponse.redirect(dest);
  } catch {
    return NextResponse.redirect("http://127.0.0.1:3000/login?error=callback");
  }
}
