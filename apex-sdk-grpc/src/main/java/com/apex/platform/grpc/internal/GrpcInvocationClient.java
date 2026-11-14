package com.apex.platform.grpc.internal;

import com.apex.platform.core.context.*;
import com.apex.platform.grpc.api.*;
import com.apex.platform.grpc.internal.wire.*;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.transaction.support.TransactionSynchronizationManager;

final class GrpcInvocationClient implements RequestReplyGateway, InstanceQueryGateway, AutoCloseable {
  private final GrpcProperties properties;
  private final RpcCodec codec;
  private final ExecutionContextProvider contexts;
  private final CellGrpcChannelProvider channels;
  private RpcObservation observations=RpcObservation.NONE;
  GrpcInvocationClient withObservations(RpcObservation value){observations=value;return this;}

  GrpcInvocationClient(
      GrpcProperties p, RpcCodec c, ExecutionContextProvider ctx, GrpcChannelFactory factory) {
    this(p, c, ctx, factory, null);
  }

  GrpcInvocationClient(GrpcProperties p, RpcCodec c, ExecutionContextProvider ctx,
      GrpcChannelFactory factory, DiscoveredCellChannelFactory discovered) {
    properties = p;
    codec = c;
    contexts = ctx;
    channels = new CellGrpcChannelProvider(p, factory, discovered);
  }

  boolean supportsDiscovery() { return channels.supportsDiscovery(); }

  @Override
  public <R> R request(String destination, Object request, Class<R> type, RequestOptions options) {
    return RpcErrors.await(
        requestAsync(destination, request, type, options), options.deadline(), true);
  }

  @Override
  public <R> CompletionStage<R> requestAsync(
      String destination, Object request, Class<R> type, RequestOptions options) {
    return observations.watch("rpc.client",()->invoke(
        false,
        destination,
        request,
        type,
        options.requestId(),
        options.idempotencyKey(),
        options.deadline()));
  }

  <R> CompletionStage<R> query(
      String destination, Object query, Class<R> type, QueryOptions options) {
    return observations.watch("rpc.client",()->invoke(true, destination, query, type, options.requestId(), "", options.deadline()));
  }

  private <R> CompletionStage<R> invoke(
      boolean query,
      String destination,
      Object value,
      Class<R> resultType,
      UUID requestId,
      String key,
      Duration deadline) {
    return invoke(query,destination,value,resultType,requestId,key,deadline,null);
  }
  @Override public <R> R queryInstance(String cell,String registryId,Object query,Class<R> response,QueryOptions options) {
    return RpcErrors.await(invoke(true,cell,query,response,options.requestId(),"",options.deadline(),registryId),options.deadline(),false);
  }
  private <R> CompletionStage<R> invoke(boolean query,String destination,Object value,Class<R> resultType,
      UUID requestId,String key,Duration deadline,String registryId) {
    try {
      if (TransactionSynchronizationManager.isActualTransactionActive())
        throw new GatewayException(GatewayException.Code.FAILED_PRECONDITION, false);
      var route = properties.getDestinations().get(destination);
      if (route == null && !channels.supportsDiscovery())
        throw new GatewayException(GatewayException.Code.NOT_FOUND, false);
      if (route != null && route.local()) throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT, false);
      if (deadline.compareTo(properties.getMaxDeadline()) > 0)
        throw new IllegalArgumentException("Deadline exceeds configured maximum");
      var contract = Contracts.inspect(value.getClass(), query);
      if (contract.response() != resultType)
        throw new IllegalArgumentException("Declared response mismatch");
      byte[] payload = codec.encode(value, contract.payload());
      var current = RpcContexts.currentOrRoot(contexts, properties.getCellId());
      var wire =
          Invocation.newBuilder()
              .setContractType(contract.type())
              .setContractVersion(contract.version())
              .setContentType("application/json")
              .setPayload(ByteString.copyFrom(payload))
              .setRequestId(requestId.toString())
              .setIdempotencyKey(key)
              .setCorrelationId(current.correlationId().toString())
              .setCausationId(current.causationId() == null ? "" : current.causationId().toString())
              .setOriginCell(properties.getCellId())
              .setTargetCell(destination)
              .build();
      var headers = new Metadata();
      headers.put(RpcServerAuth.CELL, properties.getCellId());
      headers.put(RpcServerAuth.CORRELATION, current.correlationId().toString());
      headers.put(RpcServerAuth.REQUEST, requestId.toString());
      var stub =
          CellGatewayGrpc.newFutureStub(registryId==null?channels.channel(destination):channels.instanceChannel(destination,registryId))
              .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS)
              .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
      var nativeResult = query ? stub.invokeQuery(wire) : stub.invokeRequest(wire);
      var result = new RpcFuture<R>(!query);
      result.cancellation = () -> nativeResult.cancel(true);
      nativeResult.addListener(
          () -> {
            Reply reply;
            try {
              reply = nativeResult.get(0, TimeUnit.MILLISECONDS);
            } catch (Throwable failure) {
              result.completeExceptionally(RpcErrors.failure(failure, !query));
              return;
            }
            try {
              if (!reply.getRequestId().equals(wire.getRequestId())
                  || !reply.getCorrelationId().equals(wire.getCorrelationId())
                  || !reply.getResponderCell().equals(destination)
                  || !reply.getContractType().equals(contract.type())
                  || reply.getContractVersion() != contract.version()
                  || !reply.getContentType().equals("application/json"))
                throw new IllegalStateException("Invalid RPC response metadata");
              result.complete(codec.decode(reply.getPayload().toByteArray(), resultType));
            } catch (Throwable unsafeReply) {
              result.completeExceptionally(
                  new GatewayException(GatewayException.Code.INTERNAL, !query));
            }
          },
          Runnable::run);
      return result;
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(RpcErrors.failure(failure, !query));
    }
  }

  @Override
  public void close() {
    channels.close();
  }
}
