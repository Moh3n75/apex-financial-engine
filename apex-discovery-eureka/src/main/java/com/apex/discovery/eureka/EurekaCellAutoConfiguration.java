package com.apex.discovery.eureka;

import com.apex.platform.discovery.*;
import com.netflix.discovery.EurekaClient;
import java.util.*;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.netflix.eureka.EurekaDiscoveryClient;
import org.springframework.cloud.netflix.eureka.EurekaInstanceConfigBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration(
    afterName = "org.springframework.cloud.netflix.eureka.EurekaClientAutoConfiguration")
@ConditionalOnProperty(name = "apex.discovery.provider", havingValue = "eureka")
@EnableConfigurationProperties(DiscoveryProperties.class)
public class EurekaCellAutoConfiguration {
  @Bean
  CellServiceDirectory eurekaCellDirectory(
      EurekaClient client,
      com.netflix.discovery.EurekaClientConfig config,
      DiscoveryProperties p,
      Environment env) {
    long maxAge = env.getProperty("apex.discovery.max-stale-ms", Long.class, 60000L);
    if (maxAge < 1000 || maxAge > 120000)
      throw new IllegalStateException("Invalid registry freshness bound");
    var startupWait =
        env.getProperty(
            "apex.discovery.startup-wait",
            java.time.Duration.class,
            java.time.Duration.ofSeconds(35));
    if (startupWait.toMillis() < 250 || startupWait.compareTo(java.time.Duration.ofSeconds(60)) > 0)
      throw new IllegalStateException("Invalid bounded discovery startup wait");
    awaitFirstRegistry(client, startupWait);
    return new CloudCellServiceDirectory(
        new EurekaDiscoveryClient(client, config),
        p,
        () -> {
          long age = registryAge(client);
          return age >= 0 && age <= maxAge;
        });
  }

  @Bean
  static BeanPostProcessor apexEurekaRegistrationMetadata(Environment env) {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessBeforeInitialization(Object bean, String name) {
        if (bean instanceof EurekaInstanceConfigBean instance
            && env.getProperty("apex.discovery.registration.enabled", Boolean.class, false)) {
          String cell = CellEndpoint.logicalName(env.getRequiredProperty("apex.cell.id"));
          if (!cell.equals(env.getRequiredProperty("spring.application.name")))
            throw new IllegalStateException("Eureka application and logical Cell must agree");
          String replica = env.getRequiredProperty("apex.discovery.instance-id");
          String authority = env.getRequiredProperty("apex.discovery.authority");
          int http = env.getRequiredProperty("server.port", Integer.class);
          int grpc =
              env.getProperty(
                  "apex.discovery.grpc-advertised-port",
                  Integer.class,
                  env.getRequiredProperty("spring.grpc.server.port", Integer.class));
          new CellEndpoint(
              cell,
              cell + ":" + replica,
              instance.getHostName(false),
              http,
              grpc,
              false,
              authority);
          instance.setAppname(cell);
          instance.setVirtualHostName(cell);
          instance.setInstanceId(cell + ":" + replica);
          var metadata = new HashMap<>(instance.getMetadataMap());
          Map.of(
                  "apex.cell-id",
                  cell,
                  "apex.grpc.port",
                  Integer.toString(grpc),
                  "apex.grpc.authority",
                  authority)
              .forEach(
                  (k, v) -> {
                    if (metadata.containsKey(k) && !v.equals(metadata.get(k)))
                      throw new IllegalStateException("Conflicting Eureka transport metadata");
                    metadata.put(k, v);
                  });
          instance.setMetadataMap(metadata);
        }
        return bean;
      }
    };
  }

  @Bean
  @ConditionalOnProperty(name = "apex.discovery.registration.enabled", havingValue = "true")
  org.springframework.beans.factory.SmartInitializingSingleton uniqueRegistrationIdentity(
      EurekaClient client, EurekaInstanceConfigBean instance) {
    return () -> {
      for (var other : client.getInstancesByVipAddress(instance.getVirtualHostName(), false)) {
        if (instance.getInstanceId().equals(other.getInstanceId())
            && (instance.getNonSecurePort() != other.getPort()
                || !instance.getHostName(false).equals(other.getHostName())
                || !instance
                    .getMetadataMap()
                    .get("apex.grpc.port")
                    .equals(other.getMetadata().get("apex.grpc.port"))))
          throw new IllegalStateException(
              "Duplicate active Eureka instanceId with different endpoint");
      }
    };
  }

  @Bean
  org.springframework.boot.health.contributor.HealthIndicator registryFreshness(
      EurekaClient client, Environment env) {
    return () -> {
      long age = registryAge(client);
      boolean fresh =
          age >= 0 && age <= env.getProperty("apex.discovery.max-stale-ms", Long.class, 60000L);
      return fresh
          ? org.springframework.boot.health.contributor.Health.up().build()
          : org.springframework.boot.health.contributor.Health.down()
              .withDetail("code", "REGISTRY_STALE")
              .build();
    };
  }

  // EurekaClient's public interface omits the freshness clock. Resolve Spring Cloud's scoped
  // infrastructure proxy on each read so refresh-scope replacement is respected; unknown adapters
  // remain fail-closed. This does not bypass any business invocation or authorization advice.
  static long registryAge(EurekaClient client) {
    try {
      Object target = client;
      for (int depth = 0;
          depth < 4 && target instanceof org.springframework.aop.framework.Advised proxy;
          depth++) target = proxy.getTargetSource().getTarget();
      return target instanceof com.netflix.discovery.DiscoveryClient nativeClient
          ? nativeClient.getLastSuccessfulRegistryFetchTimePeriod()
          : Long.MAX_VALUE;
    } catch (Exception unavailable) {
      return Long.MAX_VALUE;
    }
  }

  static void awaitFirstRegistry(EurekaClient client, java.time.Duration bound) {
    long deadline = System.nanoTime() + bound.toNanos();
    do {
      long age = registryAge(client);
      if (age >= 0 && age < Long.MAX_VALUE) return;
      try {
        Thread.sleep(25);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Discovery initialization interrupted");
      }
    } while (System.nanoTime() < deadline);
    throw new IllegalStateException("No confirmed initial Eureka refresh within startup bound");
  }
}
