package com.apex.gateway;

import com.apex.platform.discovery.CellServiceDirectory;
import java.util.List;
import org.springframework.cloud.client.*;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;

/** Child load-balancer context only: explicit Order HTTP route, never the gRPC port. */
public class OrderHttpBalancer {
  @Bean
  ServiceInstanceListSupplier orderHttpInstances(CellServiceDirectory directory) {
    return new ServiceInstanceListSupplier() {
      public String getServiceId() {
        return "order-cell";
      }

      public Flux<List<ServiceInstance>> get() {
        return Flux.defer(
            () -> {
              try {
                return Flux.just(
                    directory.endpoints(getServiceId()).stream()
                        .map(
                            e ->
                                (ServiceInstance)
                                    new DefaultServiceInstance(
                                        e.instanceId(),
                                        e.serviceId(),
                                        e.host(),
                                        e.httpPort(),
                                        e.https()))
                        .toList());
              } catch (RuntimeException unavailable) {
                return Flux.just(List.of());
              }
            });
      }
    };
  }
}
