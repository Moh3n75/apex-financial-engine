package com.apex.discovery.grpc;

import com.apex.platform.discovery.*;
import com.apex.platform.grpc.internal.DiscoveredCellChannelFactory;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.grpc.client.autoconfigure.GrpcClientProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.grpc.client.*;

@AutoConfiguration(
    afterName = {
      "com.apex.discovery.eureka.EurekaCellAutoConfiguration",
      "com.apex.discovery.kubernetes.KubernetesCellAutoConfiguration",
      "org.springframework.boot.grpc.client.autoconfigure.GrpcClientAutoConfiguration"
    },
    beforeName = "com.apex.platform.grpc.internal.GrpcClientAutoConfiguration")
@ConditionalOnProperty(name = "apex.discovery.grpc.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(name = "apex.grpc.client.enabled", havingValue = "true")
@ConditionalOnBean(CellServiceDirectory.class)
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
  DiscoveredCellChannelFactory discoveredCellChannels(
      DiscoveryProperties discovery,
      GrpcClientProperties boot,
      CellServiceDirectory directory,
      ScheduledExecutorService cellEndpointRefresh,
      ObjectProvider<GrpcChannelFactory> factories) {
    discovery.validate();
    // One infrastructure-owned TLS configuration for any logical Cell. No destination
    // address/channel map is needed, and editable registry metadata never supplies trust.
    var tls = boot.getChannel().get("default");
    if (tls == null || tls.isBypassCertificateValidation()
        || Boolean.FALSE.equals(tls.getSsl().getEnabled())
        || (!Boolean.TRUE.equals(tls.getSsl().getEnabled()) && tls.getSsl().getBundle() == null))
      throw new IllegalStateException("Automatic Cell discovery requires verified default gRPC TLS");
    return new DiscoveredCellChannelFactory() {
      public io.grpc.ManagedChannel createChannel(String logicalCell){return create(logicalCell,null);}
      public io.grpc.ManagedChannel createInstanceChannel(String logicalCell,String registryId){return create(logicalCell,registryId);}
      private io.grpc.ManagedChannel create(String logicalCell,String registryId) {
      String logical = CellEndpoint.logicalName(logicalCell);
      String target = "apex:///" + logical+(registryId==null?"":"?instance="+Base64.getUrlEncoder().withoutPadding().encodeToString(registryId.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      CellServiceDirectory selected=registryId==null?directory:cell->directory.endpoints(cell).stream().filter(e->e.instanceId().equals(registryId)).toList();
      if (boot.getChannel().containsKey(target))
        throw new IllegalStateException("Logical discovery targets cannot be overridden by named channels");
      String authority = discovery.trustedAuthority(logical);
      var options = ChannelBuilderOptions.defaults().withCustomizer((name, builder) -> {
        builder.nameResolverFactory(new DirectoryNameResolverFactory(
            selected, logical, authority, cellEndpointRefresh, discovery.getRefreshInterval(),target));
        builder.overrideAuthority(authority);
        builder.intercept(new DiscoveryFreshnessInterceptor(selected, logical));
        builder.defaultLoadBalancingPolicy("round_robin");
        builder.defaultServiceConfig(Map.of("loadBalancingConfig", List.of(Map.of("round_robin", Map.of()))));
        builder.disableServiceConfigLookUp();
        builder.disableRetry();
      });
      return factories.getObject().createChannel(target, options);
      }
    };
  }
}
