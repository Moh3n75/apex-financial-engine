package com.apex.platform.grpc.api;

import java.util.concurrent.CancellationException;

/** Cancellation of a mutation does not establish rollback or business rejection. */
public final class GatewayCancellationException extends CancellationException {
  private final boolean outcomeUnknown;

  public GatewayCancellationException(boolean unknown) {
    super("CANCELLED");
    outcomeUnknown = unknown;
  }

  public boolean outcomeUnknown() {
    return outcomeUnknown;
  }
}
