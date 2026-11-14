package com.apex.platform.grpc.api;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Retain these options (especially requestId/key) for a retry. Never automatically retry mutations.
 */
public record RequestOptions(UUID requestId, String idempotencyKey, Duration deadline) {
  public RequestOptions {
    Objects.requireNonNull(requestId, "requestId");
    if (idempotencyKey == null || !idempotencyKey.matches("[A-Za-z0-9._:-]{1,128}"))
      throw new IllegalArgumentException("Invalid idempotency key");
    if (deadline == null
        || deadline.toMillis() < 1
        || deadline.compareTo(Duration.ofSeconds(30)) > 0)
      throw new IllegalArgumentException("Deadline must be 1ms..30s");
  }

  public static RequestOptions withIdempotencyKey(String key) {
    return new RequestOptions(UUID.randomUUID(), key, Duration.ofSeconds(5));
  }

  public RequestOptions withDeadline(Duration value) {
    return new RequestOptions(requestId, idempotencyKey, value);
  }

  public RequestOptions withRequestId(UUID value) {
    return new RequestOptions(value, idempotencyKey, deadline);
  }
}
