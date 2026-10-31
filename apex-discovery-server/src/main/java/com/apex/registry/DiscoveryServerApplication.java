package com.apex.registry;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@SpringBootApplication
@EnableEurekaServer
public class DiscoveryServerApplication {
  public static void main(String[] args) {
    var application = new SpringApplication(DiscoveryServerApplication.class);
    application.addInitializers(context -> validateExposure(context.getEnvironment()));
    application.run(args);
  }

  @Bean
  SmartInitializingSingleton registryExposureGuard(Environment e) {
    return () -> validateExposure(e);
  }

  static void validateExposure(Environment e) {
    if (!e.getProperty("apex.registry.loopback-development", Boolean.class, false)
        || !java.util.Set.of("127.0.0.1", "::1").contains(e.getProperty("server.address", "")))
      throw new IllegalStateException(
          "Registry exposure requires a separately reviewed secured deployment; loopback"
              + " development is explicit only");
  }
}
