package com.apex.platform.grpc.api;

import java.util.List;

/** Exactly one named slot per requested target, ordered by logical target ID. */
public record CollectionResult<T>(List<ResponderResult<T>> responders) {
  public CollectionResult {
    responders = List.copyOf(responders);
  }

  public boolean completeSuccess() {
    return responders.stream().allMatch(r -> r.status() == ResponderResult.Status.SUCCESS);
  }
}
