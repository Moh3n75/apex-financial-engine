package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.GatewayException;
import com.apex.platform.grpc.security.PeerIdentityVerifier;
import io.grpc.*;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

final class RpcServerAuth implements ServerInterceptor {
  static final Metadata.Key<String> CELL =
      Metadata.Key.of("x-apex-cell-id", Metadata.ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> CORRELATION =
      Metadata.Key.of("x-apex-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> REQUEST =
      Metadata.Key.of("x-apex-request-id", Metadata.ASCII_STRING_MARSHALLER);
  static final Context.Key<Verified> VERIFIED = Context.key("apex-verified-rpc");

  record Verified(PeerIdentityVerifier.Caller caller, UUID request, UUID correlation) {}

  private final GrpcProperties properties;
  private final PeerIdentityVerifier verifier;

  RpcServerAuth(GrpcProperties p, PeerIdentityVerifier verifier) {
    properties = p;
    this.verifier = verifier;
  }

  @Override
  public <Q, S> ServerCall.Listener<Q> interceptCall(
      ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
    try {
      int count = 0;
      for (String name : headers.keys()) {
        if (name.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
          var values = headers.getAll(Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER));
          if (values != null) for (var ignored : values) count++;
        } else {
          var values = headers.getAll(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER));
          if (values != null) for (var ignored : values) count++;
        }
      }
      if (count > 32) throw new GatewayException(GatewayException.Code.RESOURCE_EXHAUSTED, false);
      Deadline deadline = Context.current().getDeadline();
      if (deadline == null
          || deadline.timeRemaining(TimeUnit.MILLISECONDS) > properties.getMaxDeadline().toMillis())
        throw new IllegalArgumentException("Finite bounded deadline required");
      PeerIdentityVerifier.Caller caller;
      if (properties.isInsecureLoopbackTestOnly()) {
        var remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        String id = headers.get(CELL);
        if (!(remote instanceof InetSocketAddress a)
            || !a.getAddress().isLoopbackAddress()
            || !properties.getTestClients().contains(id))
          throw new GatewayException(GatewayException.Code.UNAUTHENTICATED, false);
        caller = new PeerIdentityVerifier.Caller(id, "test-only", "unverified-loopback:" + id);
      } else {
        if (call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION) == null)
          throw new GatewayException(GatewayException.Code.UNAUTHENTICATED, false);
        caller = verifier.verify(call, headers);
        if (caller == null)
          throw new GatewayException(GatewayException.Code.UNAUTHENTICATED, false);
      }
      if (!caller.cellId().equals(headers.get(CELL)))
        throw new GatewayException(GatewayException.Code.UNAUTHENTICATED, false);
      var v =
          new Verified(caller, parseId(headers.get(REQUEST)), parseId(headers.get(CORRELATION)));
      return Contexts.interceptCall(Context.current().withValue(VERIFIED, v), call, headers, next);
    } catch (Throwable failure) {
      call.close(Status.fromThrowable(RpcErrors.wire(failure)), new Metadata());
      return new ServerCall.Listener<>() {};
    }
  }

  static UUID parseId(String value) {
    if (value == null
        || !value.matches(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
      throw new IllegalArgumentException("Canonical UUID required");
    return UUID.fromString(value);
  }
}
