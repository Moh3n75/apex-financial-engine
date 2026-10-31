package com.apex.gateway;

import static org.springframework.cloud.gateway.server.mvc.filter.LoadBalancerFilterFunctions.lb;
import static org.springframework.cloud.gateway.server.mvc.handler.GatewayRouterFunctions.route;
import static org.springframework.cloud.gateway.server.mvc.handler.HandlerFunctions.http;

import com.apex.platform.discovery.*;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClient;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.function.*;

@SpringBootApplication
@LoadBalancerClient(name = "order-cell", configuration = OrderHttpBalancer.class)
public class GatewayApplication {
  public static void main(String[] args) {
    var application = new SpringApplication(GatewayApplication.class);
    application.addInitializers(context -> validateExposure(context.getEnvironment()));
    application.run(args);
  }

  @Bean
  SmartInitializingSingleton gatewayExposureAndRoutes(
      Environment e, CellServiceDirectory directory) {
    return () -> {
      validateExposure(e);
      if (directory.endpoints("order-cell").isEmpty())
        throw new IllegalStateException("Required Order endpoints unavailable");
    };
  }

  static void validateExposure(Environment e) {
    if (!"loopback-development".equals(e.getProperty("apex.gateway.mode", "disabled"))
        || !java.util.Set.of("127.0.0.1", "::1").contains(e.getProperty("server.address", "")))
      throw new IllegalStateException(
          "Public Gateway exposure is disabled until authenticated ingress is implemented; explicit"
              + " loopback-development only");
  }

  @Bean
  RouterFunction<ServerResponse> orderRoutes() {
    return route("order-placements")
        .route(RequestPredicates.path("/api/v1/order-placements/**"), http())
        .filter(upstreamFailures())
        .filter(lb("order-cell"))
        .build();
  }

  static HandlerFilterFunction<ServerResponse, ServerResponse> upstreamFailures() {
    return (request, next) -> {
      try {
        return next.handle(request);
      } catch (org.springframework.web.client.HttpStatusCodeException unavailable) {
        return ServerResponse.status(unavailable.getStatusCode())
            .body(java.util.Map.of("code", "UPSTREAM_UNAVAILABLE", "outcome", "UNKNOWN"));
      } catch (org.springframework.web.client.ResourceAccessException transport) {
        return ServerResponse.status(504)
            .body(java.util.Map.of("code", "UPSTREAM_TRANSPORT_FAILURE", "outcome", "UNKNOWN"));
      }
    };
  }

  @Bean
  HealthIndicator discoveredTargets(CellServiceDirectory directory) {
    return () -> {
      try {
        return directory.endpoints("order-cell").isEmpty()
            ? Health.down().build()
            : Health.up().build();
      } catch (RuntimeException unavailable) {
        return Health.down().withDetail("code", "DISCOVERY_UNAVAILABLE").build();
      }
    };
  }

  @Bean
  EdgeHeaders edgeHeaders() {
    return new EdgeHeaders();
  }

  @Bean
  org.springframework.boot.restclient.RestClientCustomizer gatewayHttpTimeouts() {
    return builder -> {
      var factory =
          new org.springframework.http.client.JdkClientHttpRequestFactory(
              java.net.http.HttpClient.newBuilder()
                  .connectTimeout(java.time.Duration.ofSeconds(2))
                  .followRedirects(java.net.http.HttpClient.Redirect.NEVER)
                  .build());
      factory.setReadTimeout(java.time.Duration.ofSeconds(5));
      builder.requestFactory(factory);
    };
  }
}
