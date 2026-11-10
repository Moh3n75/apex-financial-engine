package com.apex.discovery.eureka;

import com.apex.platform.discovery.*;
import com.apex.platform.grpc.api.GrpcServerEndpoint;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.discovery.EurekaClient;

import java.util.*;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.netflix.eureka.EurekaDiscoveryClient;
import org.springframework.cloud.netflix.eureka.EurekaInstanceConfigBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.core.env.Environment;

@AutoConfiguration(
        afterName = "org.springframework.cloud.netflix.eureka.EurekaClientAutoConfiguration")
@ConditionalOnProperty(name = "apex.discovery.provider", havingValue = "eureka")
@EnableConfigurationProperties(DiscoveryProperties.class)
public class EurekaCellAutoConfiguration {
    static BeanPostProcessor apexEurekaRegistrationMetadata(Environment env) {
        return apexEurekaRegistrationMetadata(env,new org.springframework.beans.factory.support.DefaultListableBeanFactory().getBeanProvider(CellRuntimeIdentity.class));
    }
    @Bean
    @ConditionalOnProperty(name="apex.discovery.membership.enabled",havingValue="true")
    CellRuntimeIdentity cellRuntimeIdentity(Environment env) {
        return new CellRuntimeIdentity(env.getRequiredProperty("apex.contract-discovery.environment"),
                env.getRequiredProperty("apex.cell.id"),env.getRequiredProperty("apex.contract-discovery.instance"),UUID.randomUUID());
    }
    @Bean
    CellServiceDirectory eurekaCellDirectory(
            EurekaClient client,
            com.netflix.discovery.EurekaClientConfig config,
            DiscoveryProperties p,
            Environment env) {
        long maxAge = env.getProperty("apex.discovery.max-stale-ms", Long.class, 60000L);
        if (maxAge < 1000 || maxAge > 120000)
            throw new IllegalStateException("Invalid registry freshness bound");
        return new CloudCellServiceDirectory(
                new EurekaDiscoveryClient(client, config),
                p,
                () -> {
                    long age = registryAge(client);
                    return age >= 0 && age <= maxAge;
                },"EUREKA",env.getProperty("apex.discovery.registry-id","unknown"));
    }

    @Bean
    static BeanPostProcessor apexEurekaRegistrationMetadata(Environment env,org.springframework.beans.factory.ObjectProvider<CellRuntimeIdentity> identities) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessBeforeInitialization(Object bean, String name) {
                if (bean instanceof EurekaInstanceConfigBean instance
                        && env.getProperty("apex.discovery.registration.enabled", Boolean.class, false)) {
                    String cell = CellEndpoint.logicalName(env.getProperty("apex.cell.id",
                            env.getProperty("apex.grpc.cell-id", env.getRequiredProperty("spring.application.name"))));
                    if (!cell.equals(env.getRequiredProperty("spring.application.name")))
                        throw new IllegalStateException("Eureka application and logical Cell must agree");
                    String replica = env.getProperty("apex.discovery.instance-id", UUID.randomUUID().toString());
                    String authority = env.getProperty("apex.discovery.authority", cell);
                    if (!replica.matches("[A-Za-z0-9._:-]{1,64}") || !authority.matches("[A-Za-z0-9.-]{1,253}"))
                        throw new IllegalStateException("Invalid registered Cell identity/authority");
                    instance.setAppname(cell);
                    instance.setVirtualHostName(cell);
                    var runtime=identities.getIfAvailable();
                    instance.setInstanceId(runtime==null?cell+":"+replica:cell+":"+runtime.epoch());
                    var metadata = new HashMap<>(instance.getMetadataMap());
                    if(runtime!=null)metadata.putAll(Map.of("apex.environment",runtime.environment(),"apex.instance-id",runtime.instanceId(),"apex.epoch",runtime.epoch().toString()));
                    Map.of(
                                    "apex.cell-id",
                                    cell,
                                    "apex.grpc.authority",
                                    authority)
                            .forEach(
                                    (k, v) -> {
                                        if (metadata.containsKey(k) && !v.equals(metadata.get(k)))
                                            throw new IllegalStateException("Conflicting Eureka transport metadata");
                                        metadata.put(k, v);
                                    });
                    // Configuration is not proof of a listening server, especially for port=0.
                    String configuredPort = env.getProperty("apex.discovery.grpc-advertised-port",
                            env.getProperty("spring.grpc.server.port", "0"));
                    if (metadata.containsKey("apex.grpc.port")
                            && !metadata.get("apex.grpc.port").equals(configuredPort))
                        throw new IllegalStateException("Conflicting Eureka transport metadata");
                    metadata.remove("apex.grpc.port");
                    metadata.put("apex.grpc.enabled", "false");
                    instance.setMetadataMap(metadata);
                }
                return bean;
            }
        };
    }

    @Bean
    @ConditionalOnProperty(name = "apex.discovery.registration.enabled", havingValue = "true")
    ApplicationListener<PayloadApplicationEvent<GrpcServerEndpoint>> actualGrpcRegistration(
            EurekaInstanceConfigBean instance, ApplicationInfoManager manager, Environment env) {
        return event -> {
            if (!env.getProperty("apex.grpc.server.enabled", Boolean.class, false)) return;
            var endpoint = event.getPayload();
            if (!endpoint.cellId().equals(instance.getAppname()))
                throw new IllegalStateException("Bound gRPC Cell does not match Eureka identity");
            int port = env.getProperty("apex.discovery.grpc-advertised-port", Integer.class, endpoint.port());
            new CellEndpoint(endpoint.cellId(), instance.getInstanceId(), instance.getHostName(false),
                    instance.getNonSecurePort(), port, false, instance.getMetadataMap().get("apex.grpc.authority"));
            var metadata = Map.of("apex.grpc.port", Integer.toString(port), "apex.grpc.enabled", "true");
            instance.getMetadataMap().putAll(metadata);
            // Marks InstanceInfo dirty so Eureka's normal instance-info replication publishes it.
            manager.registerAppMetadata(metadata);
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
                        .getOrDefault("apex.grpc.port", "")
                        .equals(other.getMetadata().getOrDefault("apex.grpc.port", ""))))
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
                 depth++)
                target = proxy.getTargetSource().getTarget();
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
