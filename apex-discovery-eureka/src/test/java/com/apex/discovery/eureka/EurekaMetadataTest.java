package com.apex.discovery.eureka;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtilsProperties;
import org.springframework.cloud.netflix.eureka.EurekaInstanceConfigBean;
import org.springframework.mock.env.MockEnvironment;

@Tag("unit")
class EurekaMetadataTest {
  @Test
  void initialConstructorFetchDoesNotInventAConfirmedRefreshClock() {
    var client = org.mockito.Mockito.mock(com.netflix.discovery.DiscoveryClient.class);
    org.mockito.Mockito.when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(-1L, 5L);
    EurekaCellAutoConfiguration.awaitFirstRegistry(client, java.time.Duration.ofMillis(200));
    org.mockito.Mockito.when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(-1L);
    assertThatThrownBy(
            () ->
                EurekaCellAutoConfiguration.awaitFirstRegistry(
                    client, java.time.Duration.ofMillis(50)))
        .hasMessageContaining("No confirmed initial Eureka refresh");
  }

  @Test
  void freshnessWorksThroughTheSpringCloudScopedInterfaceProxy() {
    var nativeClient = org.mockito.Mockito.mock(com.netflix.discovery.DiscoveryClient.class);
    org.mockito.Mockito.when(nativeClient.getLastSuccessfulRegistryFetchTimePeriod())
        .thenReturn(12L);
    var factory = new org.springframework.aop.framework.ProxyFactory();
    factory.setTarget(nativeClient);
    factory.setInterfaces(com.netflix.discovery.EurekaClient.class);
    var proxy = (com.netflix.discovery.EurekaClient) factory.getProxy();
    assertThat(proxy).isNotInstanceOf(com.netflix.discovery.DiscoveryClient.class);
    var indicator =
        new EurekaCellAutoConfiguration().registryFreshness(proxy, new MockEnvironment());
    assertThat(indicator.health().getStatus().getCode()).isEqualTo("UP");
  }

  MockEnvironment configuration() {
    return new MockEnvironment()
        .withProperty("apex.discovery.registration.enabled", "true")
        .withProperty("apex.cell.id", "order-cell")
        .withProperty("spring.application.name", "order-cell")
        .withProperty("apex.discovery.instance-id", "replica-2")
        .withProperty("apex.discovery.authority", "orders.example")
        .withProperty("server.port", "8083")
        .withProperty("spring.grpc.server.port", "9091");
  }

  @Test
  void registrationSeparatesPortsAndPinsLogicalIdentity() {
    try (var inet = new InetUtils(new InetUtilsProperties())) {
      var instance = new EurekaInstanceConfigBean(inet);
      instance.setHostname("127.0.0.1");
      EurekaCellAutoConfiguration.apexEurekaRegistrationMetadata(configuration())
          .postProcessBeforeInitialization(instance, "instance");
      assertThat(instance.getAppname()).isEqualTo("order-cell");
      assertThat(instance.getInstanceId()).isEqualTo("order-cell:replica-2");
      assertThat(instance.getMetadataMap())
          .containsEntry("apex.grpc.port", "9091")
          .containsEntry("apex.grpc.authority", "orders.example");
    }
  }

  @Test
  void conflictingMetadataAndApplicationFailClosed() {
    try (var inet = new InetUtils(new InetUtilsProperties())) {
      var instance = new EurekaInstanceConfigBean(inet);
      instance.setHostname("127.0.0.1");
      instance.getMetadataMap().put("apex.grpc.port", "1");
      assertThatThrownBy(
              () ->
                  EurekaCellAutoConfiguration.apexEurekaRegistrationMetadata(configuration())
                      .postProcessBeforeInitialization(instance, "instance"))
          .hasMessageContaining("Conflicting");
      var wrong = configuration().withProperty("spring.application.name", "inventory-cell");
      assertThatThrownBy(
              () ->
                  EurekaCellAutoConfiguration.apexEurekaRegistrationMetadata(wrong)
                      .postProcessBeforeInitialization(instance, "instance"))
          .hasMessageContaining("must agree");
    }
  }

  @Test
  void disabledRegistrationDoesNotRewriteExistingInstance() {
    try (var inet = new InetUtils(new InetUtilsProperties())) {
      var instance = new EurekaInstanceConfigBean(inet);
      instance.setInstanceId("existing");
      EurekaCellAutoConfiguration.apexEurekaRegistrationMetadata(new MockEnvironment())
          .postProcessBeforeInitialization(instance, "instance");
      assertThat(instance.getInstanceId()).isEqualTo("existing");
      assertThat(instance.getMetadataMap()).doesNotContainKeys("apex.grpc.port", "apex.cell-id");
    }
  }
}
