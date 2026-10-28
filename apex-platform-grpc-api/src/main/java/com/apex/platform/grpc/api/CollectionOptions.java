package com.apex.platform.grpc.api;

import java.time.Duration;
import java.util.*;

/** Explicit, finite read-only fanout policy. List input allows duplicate detection. */
public record CollectionOptions(
    List<String> expectedResponders, Duration totalDeadline, boolean allowPartial) {
  public CollectionOptions {
    Objects.requireNonNull(expectedResponders, "expectedResponders");
    if (expectedResponders.isEmpty()
        || expectedResponders.size() > 16
        || expectedResponders.stream()
            .anyMatch(s -> s == null || !s.matches("[A-Za-z0-9._-]{1,64}"))
        || new HashSet<>(expectedResponders).size() != expectedResponders.size())
      throw new IllegalArgumentException("Require 1..16 distinct explicit responders");
    expectedResponders = expectedResponders.stream().sorted().toList();
    if (totalDeadline == null
        || totalDeadline.toMillis() < 1
        || totalDeadline.compareTo(Duration.ofSeconds(30)) > 0)
      throw new IllegalArgumentException("Collection deadline must be 1ms..30s");
  }
}
