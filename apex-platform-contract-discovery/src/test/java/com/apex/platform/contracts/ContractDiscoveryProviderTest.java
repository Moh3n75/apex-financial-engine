package com.apex.platform.contracts;

import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.*;
import com.apex.platform.grpc.internal.*;
import com.apex.platform.messaging.consumer.*;
import com.apex.platform.messaging.jdbc.PublisherRouting;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

class ContractDiscoveryProviderTest {
  @Test void optInAndActualGrpcRegistrationExposeSanitizedDescriptor() {
    new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(ContractDiscoveryAutoConfiguration.class))
        .run(c -> assertThat(c).doesNotHaveBean(ContractDiscoveryProvider.class));
    new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(ContractDiscoveryAutoConfiguration.class))
        .withPropertyValues("apex.contract-discovery.enabled=true", "spring.application.name=sample", "apex.cell.id=sample")
        .run(c -> {
          var provider = c.getBean(ContractDiscoveryProvider.class);
          var beans = new DefaultListableBeanFactory(); beans.registerSingleton("provider", provider);
          var registry = new HandlerRegistry(beans); registry.afterSingletonsInstantiated();
          assertThat(registry.descriptors()).singleElement().satisfies(d -> {
            assertThat(d.type()).isEqualTo("OPS_CONTRACT_DISCOVERY"); assertThat(d.kind()).isEqualTo("QUERY");
          });
          assertThat(provider.discover(new ContractDiscoveryQuery()).gaps()).contains("KAFKA_CLUSTER_IDENTITY_UNKNOWN");
          var codec = new RpcCodec(65536);
          var manifest = provider.discover(new ContractDiscoveryQuery());
          assertThat(codec.decode(codec.encode(manifest, ContractManifest.class), ContractManifest.class)).isEqualTo(manifest);
        });
  }

  @Test void effectiveOverridesAndDisabledPublisherAreHonored() {
    var env = new MockEnvironment().withProperty("spring.application.name", "sample")
        .withProperty("apex.cell.id", "sample").withProperty("apex.messaging.cell.enabled", "true")
        .withProperty("apex.messaging.consumer.enabled", "true")
        .withProperty("apex.messaging.consumer.topics", "overridden.events.v2,replay.events.v2")
        .withProperty("spring.kafka.consumer.group-id", "overridden-group")
        .withProperty("spring.kafka.listener.auto-startup", "false");
    var routing = new PublisherRouting(true, List.of(new PublisherRouting.Route("SAMPLE", 2, "overridden.events.v2")));
    var provider = new ContractDiscoveryProvider(env, routing, new DefaultEventHandlerRegistry(List.of()), null, List.of());
    var manifest = provider.discover(new ContractDiscoveryQuery());
    assertThat(manifest.publications().getFirst().topic()).isEqualTo("overridden.events.v2");
    assertThat(manifest.subscriptions()).containsExactly("overridden.events.v2", "replay.events.v2");
    assertThat(manifest.consumerGroup()).isEqualTo("overridden-group");
    assertThat(manifest.gaps()).contains("LISTENER_AUTO_START_DISABLED");
    env.setProperty("apex.messaging.outbox.publisher.enabled", "false");
    assertThat(provider.discover(new ContractDiscoveryQuery()).publications()).isEmpty();
    EventHandlerRegistry custom = (type, version) -> Optional.empty();
    assertThat(new ContractDiscoveryProvider(env, routing, custom, null, List.of())
        .discover(new ContractDiscoveryQuery()).gaps()).contains("EVENT_HANDLER_DESCRIPTORS_UNAVAILABLE");
  }
}
