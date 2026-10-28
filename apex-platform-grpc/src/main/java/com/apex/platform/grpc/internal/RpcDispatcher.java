package com.apex.platform.grpc.internal;

import com.apex.platform.core.context.*;
import com.apex.platform.grpc.api.*;
import com.apex.platform.grpc.internal.wire.Invocation;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.ObjectProvider;

public final class RpcDispatcher {
  private final HandlerRegistry registry;
  private final RpcCodec codec;
  private final ExecutionContextProvider context;
  private final GrpcProperties properties;
  private final ObjectProvider<TransactionalRequestExecutor> executors;

  RpcDispatcher(
      HandlerRegistry r,
      RpcCodec c,
      ExecutionContextProvider p,
      GrpcProperties settings,
      ObjectProvider<TransactionalRequestExecutor> e) {
    registry = r;
    codec = c;
    context = p;
    properties = settings;
    executors = e;
  }

  byte[] invoke(boolean query, Invocation wire) {
    if (!wire.getContentType().equals("application/json")
        || !wire.getTargetCell().equals(properties.getCellId())
        || !wire.getContractType().matches("[A-Z][A-Z0-9_]{0,63}")
        || wire.getContractVersion() < 1
        || wire.getContractVersion() > 10000)
      throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT, false);
    var verified = RpcServerAuth.VERIFIED.get();
    if (verified == null
        || !wire.getOriginCell().equals(verified.caller().cellId())
        || !wire.getRequestId().equals(verified.request().toString())
        || !wire.getCorrelationId().equals(verified.correlation().toString()))
      throw new GatewayException(GatewayException.Code.UNAUTHENTICATED, false);
    UUID cause =
        wire.getCausationId().isEmpty() ? null : RpcServerAuth.parseId(wire.getCausationId());
    if ((query && !wire.getIdempotencyKey().isEmpty())
        || (!query && !wire.getIdempotencyKey().matches("[A-Za-z0-9._:-]{1,128}")))
      throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT, false);
    var h = registry.find(query, wire.getContractType(), wire.getContractVersion());
    Object payload = codec.decode(wire.getPayload().toByteArray(), h.contract().payload());
    var deadline =
        Instant.now()
            .plusMillis(
                Math.max(
                    0,
                    io.grpc.Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS)));
    var meta =
        new RequestContext(
            verified.request(),
            wire.getIdempotencyKey(),
            wire.getContractType(),
            wire.getContractVersion(),
            verified.correlation(),
            cause,
            verified.caller().cellId(),
            properties.getCellId(),
            verified.caller().tenant(),
            verified.caller().principal(),
            deadline);
    if (query) return invokeHandler(h, payload, meta);
    var executor = executors.getIfAvailable();
    if (executor == null)
      throw new GatewayException(GatewayException.Code.FAILED_PRECONDITION, false);
    return executor.execute(
        meta,
        codec.fingerprint(payload, h.contract().payload()),
        () -> invokeHandler(h, payload, meta));
  }

  byte[] invokeHandler(HandlerRegistry.Handler handler, Object payload, RequestContext metadata) {
    try (var scope =
        context.open(
            new ExecutionContext(
                metadata.correlationId(),
                metadata.requestId(),
                properties.getCellId(),
                properties.getCellId()))) {
      return codec.encode(handler.invoke(payload, metadata), handler.contract().response());
    } catch (Throwable failure) {
      throw RpcErrors.failure(failure, !handler.contract().query());
    }
  }
}
