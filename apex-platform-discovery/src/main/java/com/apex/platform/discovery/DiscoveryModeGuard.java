package com.apex.platform.discovery;

import java.util.*;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.*;

/** Runs before provider bean creation: disabled means no accidental registration or API access. */
public final class DiscoveryModeGuard implements EnvironmentPostProcessor, Ordered {
  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication app) {
    String mode = env.getProperty("apex.discovery.provider", "none");
    var profiles = Set.of(env.getActiveProfiles());
    if (!Set.of("none", "eureka", "kubernetes").contains(mode)
        || (profiles.contains("eureka") && profiles.contains("kubernetes"))
        || (!mode.equals("none") && !profiles.contains(mode)))
      throw new IllegalStateException("Exactly one explicit discovery provider/profile required");
    var flags = new HashMap<String, Object>();
    boolean registering =
        mode.equals("eureka")
            && env.getProperty("apex.discovery.registration.enabled", Boolean.class, false);
    flags.put("spring.cloud.service-registry.auto-registration.enabled", registering);
    if (registering && !env.containsProperty("management.endpoint.health.group.readiness.include"))
      flags.put("management.endpoint.health.group.readiness.include", "*");
    flags.put("eureka.client.enabled", mode.equals("eureka"));
    flags.put(
        "eureka.client.register-with-eureka",
        mode.equals("eureka")
            && env.getProperty("apex.discovery.registration.enabled", Boolean.class, false));
    flags.put(
        "eureka.client.healthcheck.enabled",
        mode.equals("eureka")
            && env.getProperty("apex.discovery.registration.enabled", Boolean.class, false));
    flags.put("spring.cloud.kubernetes.enabled", mode.equals("kubernetes"));
    // The verified EndpointSlice adapter owns this DiscoveryClient. Do not start a competing
    // legacy Endpoints informer if an unrelated Kubernetes starter is present.
    flags.put("spring.cloud.kubernetes.discovery.enabled", false);
    if (mode.equals("eureka")) {
      String value = env.getProperty("eureka.client.service-url.defaultZone", "");
      if (value.isBlank()) throw new IllegalStateException("Explicit Eureka registry URL required");
      for (String address : value.split(",", -1)) {
        java.net.URI uri = java.net.URI.create(address.trim());
        boolean loopback =
            java.util.Set.of("127.0.0.1", "localhost", "[::1]").contains(uri.getHost());
        if (uri.getHost() == null
            || uri.getUserInfo() != null
            || !("https".equals(uri.getScheme())
                || ("http".equals(uri.getScheme())
                    && loopback
                    && env.getProperty(
                        "apex.discovery.insecure-loopback-test-only", Boolean.class, false))))
          throw new IllegalStateException(
              "Trusted HTTPS Eureka URL required outside explicit loopback test");
      }
    }
    if (mode.equals("kubernetes")) {
      if (env.getProperty("spring.cloud.kubernetes.discovery.all-namespaces", Boolean.class, false)
          || env.getProperty("spring.cloud.kubernetes.client.namespace", "").isBlank())
        throw new IllegalStateException("Explicit single Kubernetes namespace required");
      flags.put("spring.cloud.kubernetes.discovery.all-namespaces", false);
      flags.put("spring.cloud.kubernetes.discovery.use-endpoint-slices", true);
      flags.put("spring.cloud.kubernetes.discovery.include-not-ready-addresses", false);
      flags.put("spring.cloud.kubernetes.discovery.primary-port-name", "http");
      flags.put("spring.cloud.kubernetes.discovery.metadata.add-ports", true);
      flags.put("spring.cloud.kubernetes.config.enabled", false);
      flags.put("spring.cloud.kubernetes.secrets.enabled", false);
    }
    env.getPropertySources().addFirst(new MapPropertySource("apexDiscoverySafety", flags));
  }
}
