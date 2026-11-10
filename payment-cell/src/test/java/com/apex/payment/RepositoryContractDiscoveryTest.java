package com.apex.payment;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import com.apex.operations.contracts.*;
import com.apex.platform.contracts.*;
import com.apex.platform.grpc.internal.HandlerRegistry;
import com.apex.platform.messaging.consumer.internal.DiscoveringEventHandlerRegistry;
import com.apex.platform.messaging.jdbc.PublisherRouting;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.context.properties.bind.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mock.env.MockEnvironment;
import java.nio.file.*;
import java.util.*;

/** Actual cell YAML and actual registered contract annotations; no broker or running cell implied. */
class RepositoryContractDiscoveryTest {
  @Test void actualRepositoryDeclarations() throws Exception {
    var env = new MockEnvironment().withProperty("apex.contract-discovery.environment", "repository-test")
        .withProperty("apex.contract-discovery.cluster", "test-cluster");
    for (var source : new YamlPropertySourceLoader().load("cell", new ClassPathResource("application.yml"))) env.getPropertySources().addLast(source);
    try (var context = new AnnotationConfigApplicationContext()) {
      context.register(PublishedContractConfiguration.class);
      context.registerBean("handler0", com.apex.payment.PaymentHandler.class, () -> mock(com.apex.payment.PaymentHandler.class));
      context.registerBean("handler1", com.apex.payment.grpc.PaymentPerspectiveHandler.class, () -> mock(com.apex.payment.grpc.PaymentPerspectiveHandler.class));
      context.refresh();
      var events = new DiscoveringEventHandlerRegistry(context.getBeanFactory()); events.afterSingletonsInstantiated();
      var grpc = new HandlerRegistry(context.getBeanFactory()); grpc.afterSingletonsInstantiated();
      var routing = Binder.get(env).bind("apex.messaging.publisher.routing", Bindable.of(PublisherRouting.class))
          .orElse(new PublisherRouting(false, List.of()));
      var provider = new ContractDiscoveryProvider(env, routing, events, grpc,
          List.copyOf(context.getBeansOfType(PublishedContracts.class).values()));
      var manifest = provider.discover(new ContractDiscoveryQuery());
      assertThat(manifest.service()).isEqualTo("payment-cell");
      assertThat(manifest.publications().stream().map(p -> p.contract().key())).containsExactlyInAnyOrder("PAYMENT_AUTHORIZED:1", "PAYMENT_DECLINED:1");
      assertThat(manifest.eventHandlers().stream().map(ContractManifest.Contract::key)).containsExactlyInAnyOrder("ORDER_READY_FOR_PAYMENT:1");
      assertThat(manifest.grpcHandlers().stream().map(r -> r.contract().key())).containsExactlyInAnyOrder("GET_ORDER_PERSPECTIVE:1");
      assertThat(manifest.subscriptions()).containsExactlyInAnyOrder("apex.order.payment-requests.v1");
      assertThat(manifest.sourceMode()).isEqualTo("EFFECTIVE_DECLARATION");
      // Persist as STATIC_DECLARATION: this test harness is not a running deployment.
      var exported = new ContractManifest(1, manifest.environment(), manifest.service(), manifest.cell(),
          manifest.instance(), manifest.epoch(), manifest.collectedAt(), "STATIC_DECLARATION", manifest.cluster(),
          manifest.publications(), manifest.subscriptions(), manifest.consumerGroup(), manifest.eventHandlers(), manifest.grpcHandlers(), manifest.gaps());
      var json = tools.jackson.databind.json.JsonMapper.builder().build().writeValueAsString(exported);
      assertThat(json).doesNotContain("password", "bootstrap-servers", "payload", "jdbc:", "localhost");
      Files.createDirectories(Path.of("target"));
      Files.writeString(Path.of("target/contract-manifest.json"), json);
    }
  }
}
