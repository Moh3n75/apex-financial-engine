package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.internal.wire.*;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

final class GenericGrpcService extends CellGatewayGrpc.CellGatewayImplBase {
  private final RpcDispatcher dispatcher;
  private final GrpcProperties properties;

  GenericGrpcService(RpcDispatcher d, GrpcProperties p) {
    dispatcher = d;
    properties = p;
  }

  @Override
  public void invokeRequest(Invocation request, StreamObserver<Reply> reply) {
    invoke(false, request, reply);
  }

  @Override
  public void invokeQuery(Invocation request, StreamObserver<Reply> reply) {
    invoke(true, request, reply);
  }

  private void invoke(boolean query, Invocation request, StreamObserver<Reply> observer) {
    try {
      byte[] payload = dispatcher.invoke(query, request);
      // A receipt/result is returned only after the owning executor has committed.
      if (io.grpc.Context.current().isCancelled()) return;
      observer.onNext(
          Reply.newBuilder()
              .setContractType(request.getContractType())
              .setContractVersion(request.getContractVersion())
              .setContentType("application/json")
              .setPayload(ByteString.copyFrom(payload))
              .setRequestId(request.getRequestId())
              .setCorrelationId(request.getCorrelationId())
              .setResponderCell(properties.getCellId())
              .build());
      observer.onCompleted();
    } catch (Throwable failure) {
      observer.onError(RpcErrors.wire(failure));
    }
  }
}
