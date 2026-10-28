package com.apex.platform.grpc.internal;

import com.apex.platform.core.context.ExecutionContextProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.grpc.client.autoconfigure.GrpcClientProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.grpc.client.*;

@AutoConfiguration(
    after = GrpcCommonAutoConfiguration.class,
    afterName = "org.springframework.boot.grpc.client.autoconfigure.GrpcClientAutoConfiguration")
@ConditionalOnClass(GrpcClientProperties.class)
@ConditionalOnProperty(prefix = "apex.grpc.client", name = "enabled", havingValue = "true")
public class GrpcClientAutoConfiguration {
  @Bean(destroyMethod = "close")
  GrpcInvocationClient requestReplyGateway(
      GrpcProperties p,
      RpcCodec codec,
      ExecutionContextProvider ctx,
      GrpcChannelFactory factory,
      GrpcClientProperties boot) {
    p.getDestinations()
        .forEach(
            (logical, d) -> {
              if (d.local()) return;
              var channel = boot.getChannel().get(d.channel());
              if (channel == null
                  || channel.getTarget() == null
                  || !channel
                      .getTarget()
                      .matches("(static://|dns:///)[A-Za-z0-9.\\[\\]:_-]+:[0-9]{1,5}")
                  || channel.isBypassCertificateValidation())
                throw new IllegalStateException(
                    "Explicit safe Boot gRPC target required for " + logical);
              int port =
                  Integer.parseInt(
                      channel.getTarget().substring(channel.getTarget().lastIndexOf(':') + 1));
              if (port < 1 || port > 65535)
                throw new IllegalStateException("Invalid gRPC destination port for " + logical);
              if (!Boolean.TRUE.equals(channel.getSsl().getEnabled())) {
                if (!p.isInsecureLoopbackTestOnly()
                    || !channel
                        .getTarget()
                        .matches("static://(127\\.0\\.0\\.1|localhost|\\[::1\\]):[0-9]{1,5}"))
                  throw new IllegalStateException(
                      "Explicit gRPC TLS required outside explicitly enabled loopback tests");
              }
            });
    return new GrpcInvocationClient(p, codec, ctx, factory);
  }

  @Bean
  @SuppressWarnings({"rawtypes", "unchecked"})
  GrpcChannelBuilderCustomizer apexGrpcClientLimits(GrpcProperties p) {
    return (name, builder) -> {
      builder.disableRetry();
      builder.maxInboundMessageSize(p.getMaxPayloadBytes() + 8192);
      builder.maxInboundMetadataSize(8192);
    };
  }
}
