package com.apex.platform.contracts;

import com.apex.platform.grpc.internal.HandlerRegistry;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.jdbc.PublisherRouting;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration(afterName="com.apex.discovery.eureka.EurekaCellAutoConfiguration")
@ConditionalOnProperty(name = "apex.contract-discovery.enabled", havingValue = "true")
public class ContractDiscoveryAutoConfiguration {
  @Bean
  @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean(HandlerRegistry.class)
  @ConditionalOnProperty(name="apex.contract-discovery.local-http.enabled",havingValue="true")
  @ConditionalOnProperty(name={"apex.grpc.server.enabled","apex.grpc.client.enabled"},havingValue="false",matchIfMissing=true)
  HandlerRegistry localMetadataHandlerRegistry(org.springframework.beans.factory.config.ConfigurableListableBeanFactory beans) {
    return new HandlerRegistry(beans); // Metadata only: no channel, dispatcher or network listener is created.
  }
  @Bean ContractDiscoveryProvider contractDiscoveryProvider(Environment env,
      ObjectProvider<EventHandlerRegistry> events, ObjectProvider<HandlerRegistry> grpc,
      ObjectProvider<PublishedContracts> catalogs,ObjectProvider<com.apex.platform.discovery.CellRuntimeIdentity> identities,
      ObjectProvider<com.apex.platform.grpc.api.GrpcCallerDeclarations> callers) {
    var routing = Binder.get(env).bind("apex.messaging.publisher.routing", Bindable.of(PublisherRouting.class))
        .orElse(new PublisherRouting(false, java.util.List.of()));
    return new ContractDiscoveryProvider(env, routing, events.getIfAvailable(), grpc.getIfAvailable(), catalogs.orderedStream().toList())
        .withIdentity(identities.getIfAvailable()).withCallers(callers.orderedStream().toList());
  }
}
