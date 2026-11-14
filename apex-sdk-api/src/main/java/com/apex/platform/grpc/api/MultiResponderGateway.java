package com.apex.platform.grpc.api;

import java.util.concurrent.CompletionStage;

/** Bounded read-only queries to explicit targets; never collects replies to Kafka publication. */
public interface MultiResponderGateway {
  <R> CompletionStage<CollectionResult<R>> collectQueries(
      Object query, Class<R> responseType, CollectionOptions options);
}
