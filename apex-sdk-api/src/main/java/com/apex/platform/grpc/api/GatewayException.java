package com.apex.platform.grpc.api;

/** Safe transport/business classification. Unknown is never a business rejection. */
public final class GatewayException extends RuntimeException {
  public enum Code {
    INVALID_ARGUMENT,
    UNIMPLEMENTED,
    ALREADY_EXISTS,
    NOT_FOUND,
    UNAUTHENTICATED,
    PERMISSION_DENIED,
    UNAVAILABLE,
    DEADLINE_EXCEEDED,
    CANCELLED,
    FAILED_PRECONDITION,
    RESOURCE_EXHAUSTED,
    INTERNAL
  }

  private final Code code;
  private final boolean outcomeUnknown;

  public GatewayException(Code code, boolean outcomeUnknown) {
    super(code.name());
    this.code = code;
    this.outcomeUnknown = outcomeUnknown;
  }

  public Code code() {
    return code;
  }

  public boolean outcomeUnknown() {
    return outcomeUnknown;
  }
}
