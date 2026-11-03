import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const SESSION_COOKIE = "apex_ops_session";

/**
 * Edge gate only: cookie presence when M36A is on.
 * Cryptographic session + scope checks happen in BFF/RSC (Node).
 */
export function middleware(req: NextRequest) {
  if (process.env.APEX_OPS_UI_FIXTURE_MODE === "true") {
    return NextResponse.next();
  }
  if (process.env.APEX_OPS_M36A_ENABLED !== "true") {
    return NextResponse.next();
  }

  const { pathname } = req.nextUrl;
  if (
    pathname.startsWith("/api/auth") ||
    pathname === "/login" ||
    pathname.startsWith("/_next") ||
    pathname === "/favicon.ico"
  ) {
    return NextResponse.next();
  }

  const session = req.cookies.get(SESSION_COOKIE)?.value;
  if (!session) {
    if (pathname.startsWith("/api/")) {
      return NextResponse.json(
        { code: "UNAUTHENTICATED", message: "Sign-in required." },
        {
          status: 401,
          headers: {
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
          },
        },
      );
    }
    const login = req.nextUrl.clone();
    login.pathname = "/login";
    login.search = "";
    return NextResponse.redirect(login);
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
