package com.apex.platform.grpc.api;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

public record QueryOptions(UUID requestId, Duration deadline) {
  public QueryOptions {
    Objects.requireNonNull(requestId, "requestId");
    if (deadline == null
        || deadline.toMillis() < 1
        || deadline.compareTo(Duration.ofSeconds(30)) > 0)
      throw new IllegalArgumentException("Deadline must be 1ms..30s");
  }

  public static QueryOptions defaults() {
    return new QueryOptions(UUID.randomUUID(), Duration.ofSeconds(5));
  }
}
