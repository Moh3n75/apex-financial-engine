package com.apex.platform.grpc.api;

import java.util.Objects;
import java.util.UUID;

/** answerUnknown refers to an unavailable read perspective, never to a business denial. */
public record ResponderResult<T>(
    String responder,
    UUID invocationId,
    Status status,
    T response,
    GatewayException.Code errorCode,
    boolean answerUnknown) {
  public enum Status {
    SUCCESS,
    NOT_FOUND,
    FAILED,
    TIMED_OUT,
    UNAVAILABLE,
    CANCELLED
  }

  public ResponderResult {
    Objects.requireNonNull(responder);
    Objects.requireNonNull(invocationId);
    Objects.requireNonNull(status);
    if ((status == Status.SUCCESS) != (response != null)
        || (status == Status.SUCCESS) != (errorCode == null)
        || (status == Status.SUCCESS && answerUnknown))
      throw new IllegalArgumentException("Invalid responder outcome");
  }
}
