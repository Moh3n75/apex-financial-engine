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
  @org.springframework.context.annotation.Configuration(proxyBeanMethods=false)
  @org.springframework.boot.autoconfigure.EnableAutoConfiguration(excludeName="org.springframework.cloud.netflix.eureka.EurekaClientAutoConfiguration")
  static class BoundServer {
    @org.springframework.context.annotation.Bean InetUtils inet(){return new InetUtils(new InetUtilsProperties());}
    @org.springframework.context.annotation.Bean EurekaInstanceConfigBean instance(InetUtils inet){
      var bean=new EurekaInstanceConfigBean(inet);bean.setHostname("127.0.0.1");bean.setNonSecurePort(8080);return bean;
    }
    @org.springframework.context.annotation.Bean com.netflix.appinfo.ApplicationInfoManager manager(EurekaInstanceConfigBean instance){
      return new com.netflix.appinfo.ApplicationInfoManager(instance,new org.springframework.cloud.netflix.eureka.InstanceInfoFactory().create(instance));
    }
    @org.springframework.context.annotation.Bean com.netflix.discovery.EurekaClient client(){return org.mockito.Mockito.mock(com.netflix.discovery.EurekaClient.class);}
    @org.springframework.context.annotation.Bean com.netflix.discovery.EurekaClientConfig config(){return org.mockito.Mockito.mock(com.netflix.discovery.EurekaClientConfig.class);}
    @org.springframework.context.annotation.Bean java.util.concurrent.atomic.AtomicInteger nativePort(){return new java.util.concurrent.atomic.AtomicInteger();}
    @org.springframework.context.annotation.Bean org.springframework.context.ApplicationListener<org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent> nativePortEvent(java.util.concurrent.atomic.AtomicInteger nativePort){
      return event->nativePort.set(event.getPort());
    }
  }

  @Test void realAutoAllocatedServerPublishesActualPortThroughLifecycleToEurekaInstanceInfo() {
    var env=new org.springframework.core.env.StandardEnvironment();
    env.getPropertySources().remove(org.springframework.core.env.StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    env.getPropertySources().remove(org.springframework.core.env.StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    try(var context=new org.springframework.boot.builder.SpringApplicationBuilder(BoundServer.class).environment(env)
        .web(org.springframework.boot.WebApplicationType.NONE).run("--spring.config.location=optional:classpath:/no-external-eureka.yml",
            "--spring.main.banner-mode=off","--spring.application.name=order-cell","--apex.discovery.provider=eureka",
            "--apex.discovery.registration.enabled=true",
            "--apex.grpc.server.enabled=true","--apex.grpc.insecure-loopback-test-only=true","--apex.grpc.test-clients=client-cell",
            "--spring.grpc.server.address=127.0.0.1","--spring.grpc.server.port=0","--spring.grpc.server.ssl.enabled=false")) {
      int actual=context.getBean(java.util.concurrent.atomic.AtomicInteger.class).get();assertThat(actual).isPositive();
      var instance=context.getBean(EurekaInstanceConfigBean.class);
      assertThat(instance.getInstanceId()).startsWith("order-cell:");
      assertThat(instance.getMetadataMap()).containsEntry("apex.grpc.port",Integer.toString(actual)).containsEntry("apex.grpc.enabled","true");
      assertThat(context.getBean(com.netflix.appinfo.ApplicationInfoManager.class).getInfo().getMetadata())
          .containsEntry("apex.grpc.port",Integer.toString(actual)).containsEntry("apex.grpc.enabled","true");
    }
  }
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
        .withProperty("apex.grpc.server.enabled", "true")
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
          .doesNotContainKey("apex.grpc.port")
          .containsEntry("apex.grpc.enabled", "false")
          .containsEntry("apex.grpc.authority", "orders.example");
      var manager=org.mockito.Mockito.mock(com.netflix.appinfo.ApplicationInfoManager.class);
      new EurekaCellAutoConfiguration().actualGrpcRegistration(instance,manager,configuration())
          .onApplicationEvent(new org.springframework.context.PayloadApplicationEvent<>(this,
              new com.apex.platform.grpc.api.GrpcServerEndpoint("order-cell",19091)));
      assertThat(instance.getMetadataMap()).containsEntry("apex.grpc.port","19091").containsEntry("apex.grpc.enabled","true");
      org.mockito.Mockito.verify(manager).registerAppMetadata(java.util.Map.of("apex.grpc.port","19091","apex.grpc.enabled","true"));
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
      EurekaCellAutoConfiguration.apexEurekaRegistrationMetadata(new MockEnvironment().withProperty("apex.discovery.registration.enabled","false"))
          .postProcessBeforeInitialization(instance, "instance");
      assertThat(instance.getInstanceId()).isEqualTo("existing");
      assertThat(instance.getMetadataMap()).doesNotContainKeys("apex.grpc.port", "apex.cell-id");
    }
  }

  @Test void directoryCreationDoesNotRequireAnInitialRegistryFetch() {
    var client=org.mockito.Mockito.mock(com.netflix.discovery.DiscoveryClient.class);
    org.mockito.Mockito.when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(-1L);
    var directory=new EurekaCellAutoConfiguration().eurekaCellDirectory(client,
        org.mockito.Mockito.mock(com.netflix.discovery.EurekaClientConfig.class),new com.apex.platform.discovery.DiscoveryProperties(),new MockEnvironment());
    assertThatThrownBy(()->directory.endpoints("order-cell")).hasMessageContaining("stale");
  }

  @Test void zeroPortIsNeverAdvertisedAndDisabledGrpcIgnoresBoundEvents() {
    try(var inet=new InetUtils(new InetUtilsProperties())) {
      var env=configuration().withProperty("spring.grpc.server.port","0").withProperty("apex.grpc.server.enabled","false");
      var instance=new EurekaInstanceConfigBean(inet); instance.setHostname("127.0.0.1");
      EurekaCellAutoConfiguration.apexEurekaRegistrationMetadata(env).postProcessBeforeInitialization(instance,"instance");
      var manager=org.mockito.Mockito.mock(com.netflix.appinfo.ApplicationInfoManager.class);
      new EurekaCellAutoConfiguration().actualGrpcRegistration(instance,manager,env)
          .onApplicationEvent(new org.springframework.context.PayloadApplicationEvent<>(this,new com.apex.platform.grpc.api.GrpcServerEndpoint("order-cell",19092)));
      assertThat(instance.getMetadataMap()).doesNotContainKey("apex.grpc.port").containsEntry("apex.grpc.enabled","false");
      org.mockito.Mockito.verifyNoInteractions(manager);
    }
  }
}
