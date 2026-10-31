package com.apex.discovery.grpc;

import com.apex.platform.discovery.*;
import com.apex.platform.grpc.internal.GrpcProperties;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.grpc.client.autoconfigure.GrpcClientProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.grpc.client.GrpcChannelBuilderCustomizer;

@AutoConfiguration(
    afterName = {
      "com.apex.discovery.eureka.EurekaCellAutoConfiguration",
      "com.apex.discovery.kubernetes.KubernetesCellAutoConfiguration",
      "org.springframework.boot.grpc.client.autoconfigure.GrpcClientAutoConfiguration"
    },
    beforeName = "com.apex.platform.grpc.internal.GrpcClientAutoConfiguration")
@ConditionalOnProperty(name = "apex.discovery.grpc.enabled", havingValue = "true")
@EnableConfigurationProperties(DiscoveryProperties.class)
public class DiscoveredGrpcAutoConfiguration {
  @Bean(destroyMethod = "shutdownNow")
  ScheduledExecutorService cellEndpointRefresh() {
    var timer =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().daemon().name("apex-discovery-grpc-", 0).factory());
    timer.setRemoveOnCancelPolicy(true);
    return timer;
  }

  @Bean
  @SuppressWarnings({"rawtypes", "unchecked"})
  GrpcChannelBuilderCustomizer discoveredGrpcChannels(
      DiscoveryProperties discovery,
      GrpcProperties routes,
      GrpcClientProperties boot,
      CellServiceDirectory directory,
      ScheduledExecutorService cellEndpointRefresh) {
    discovery.validate();
    if ("none".equals(discovery.getProvider()))
      throw new IllegalStateException("Explicit discovery provider required");
    var channels = new HashMap<String, String>();
    routes
        .getDestinations()
        .forEach(
            (logical, route) -> {
              if (route.local()) return;
              var target = discovery.getTargets().get(logical);
              var channel = boot.getChannel().get(route.channel());
              if (target == null
                  || channel == null
                  || !Boolean.TRUE.equals(channel.getSsl().getEnabled())
                  || channel.isBypassCertificateValidation()
                  || !("dns:///" + target.authority() + ":443").equals(channel.getTarget())
                  || channels.putIfAbsent(route.channel(), logical) != null)
                throw new IllegalStateException(
                    "Discovery channel requires unique logical route and pinned TLS authority");
              if (directory.endpoints(logical).isEmpty())
                throw new IllegalStateException("Required discovered gRPC endpoints missing");
            });
    return (name, builder) -> {
      String logical = channels.get(name);
      if (logical == null) return;
      String authority = discovery.getTargets().get(logical).authority();
      builder.nameResolverFactory(
          new DirectoryNameResolverFactory(
              directory, logical, authority, cellEndpointRefresh, discovery.getRefreshInterval()));
      builder.overrideAuthority(authority);
      builder.intercept(new DiscoveryFreshnessInterceptor(directory, logical));
      builder.defaultLoadBalancingPolicy("round_robin");
      builder.defaultServiceConfig(
          Map.of("loadBalancingConfig", List.of(Map.of("round_robin", Map.of()))));
      builder.disableServiceConfigLookUp();
      builder.disableRetry();
    };
  }
}
