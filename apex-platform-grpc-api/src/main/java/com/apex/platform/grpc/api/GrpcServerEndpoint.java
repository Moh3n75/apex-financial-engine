package com.apex.platform.grpc.api;

/** Actual bound server endpoint, published locally after the gRPC server has started. */
public record GrpcServerEndpoint(String cellId, int port) {
  public GrpcServerEndpoint {
    if (cellId == null || !cellId.matches("[a-zA-Z0-9._-]{1,64}") || port < 1 || port > 65535)
      throw new IllegalArgumentException("Invalid bound gRPC endpoint");
  }
}
