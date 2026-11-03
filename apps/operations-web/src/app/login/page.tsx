import Link from "next/link";

export default async function LoginPage({
  searchParams,
}: {
  searchParams: Promise<{ error?: string }>;
}) {
  const sp = await searchParams;
  return (
    <main className="mx-auto flex min-h-screen max-w-lg flex-col justify-center gap-4 px-4">
      <h1 className="text-2xl font-semibold">APEX Operations sign-in</h1>
      <p className="text-sm text-zinc-600 dark:text-zinc-400">
        Local M36A OIDC login via APEX Identity. Browser never talks to the Java Console
        directly.
      </p>
      {sp.error ? (
        <p className="rounded border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-900">
          Sign-in failed. Try again.
        </p>
      ) : null}
      <Link
        className="inline-flex w-fit rounded bg-zinc-900 px-4 py-2 text-sm text-white dark:bg-zinc-100 dark:text-zinc-900"
        href="/api/auth/login?returnTo=/"
      >
        Continue with APEX Identity
      </Link>
    </main>
  );
}
