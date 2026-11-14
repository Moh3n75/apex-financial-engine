package com.apex.platform.grpc.api;

import java.util.concurrent.CompletionStage;

public interface RequestReplyGateway {
  <R> R request(String destination, Object request, Class<R> responseType, RequestOptions options);

  <R> CompletionStage<R> requestAsync(
      String destination, Object request, Class<R> responseType, RequestOptions options);
}
