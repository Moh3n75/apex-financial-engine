import { NextResponse } from "next/server";
import { OpsClientError, redactedErrorMessage } from "@/server/ops-client";
import { QueryValidationError } from "@/server/query";

export const noStoreHeaders = {
  "Cache-Control": "no-store",
  "X-Content-Type-Options": "nosniff",
};

export function rejectMutation(): NextResponse {
  return NextResponse.json(
    { code: "METHOD_NOT_ALLOWED", message: "Mutations are disabled." },
    { status: 405, headers: noStoreHeaders },
  );
}

export function jsonOk(data: unknown, status = 200): NextResponse {
  return NextResponse.json(data, { status, headers: noStoreHeaders });
}

export function jsonErr(err: unknown): NextResponse {
  if (err instanceof QueryValidationError) {
    return NextResponse.json(
      { code: "BAD_REQUEST", message: err.message },
      { status: 400, headers: noStoreHeaders },
    );
  }
  if (err instanceof OpsClientError) {
    return NextResponse.json(
      { code: err.code, message: err.message },
      { status: err.status, headers: noStoreHeaders },
    );
  }
  return NextResponse.json(
    { code: "INTERNAL", message: redactedErrorMessage(err) },
    { status: 500, headers: noStoreHeaders },
  );
}
