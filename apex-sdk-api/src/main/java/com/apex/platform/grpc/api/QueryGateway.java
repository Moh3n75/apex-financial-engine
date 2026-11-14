package com.apex.platform.grpc.api;

import java.util.concurrent.CompletionStage;

public interface QueryGateway {
  default <R> R query(String destination, Object query, Class<R> responseType) {
    return query(destination, query, responseType, QueryOptions.defaults());
  }

  <R> R query(String destination, Object query, Class<R> responseType, QueryOptions options);

  <R> CompletionStage<R> queryAsync(
      String destination, Object query, Class<R> responseType, QueryOptions options);

  <R> R queryLocal(Object query, Class<R> responseType, QueryOptions options);
}
