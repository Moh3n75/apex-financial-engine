package com.apex.discovery.kubernetes;

import com.apex.platform.discovery.*;
import io.fabric8.kubernetes.client.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration
@ConditionalOnProperty(name = "apex.discovery.provider", havingValue = "kubernetes")
@EnableConfigurationProperties(DiscoveryProperties.class)
public class KubernetesCellAutoConfiguration {
  @Bean(destroyMethod = "close")
  KubernetesClient apexKubernetesClient(Environment e) throws Exception {
    URI api = URI.create(e.getRequiredProperty("spring.cloud.kubernetes.client.master-url"));
    boolean test =
        e.getProperty("apex.discovery.insecure-loopback-test-only", Boolean.class, false)
            && java.util.Set.of("localhost", "127.0.0.1").contains(api.getHost());
    if (api.getUserInfo() != null
        || api.getHost() == null
        || !("https".equals(api.getScheme()) || (test && "http".equals(api.getScheme()))))
      throw new IllegalStateException("Explicit trusted Kubernetes HTTPS API required");
    var config =
        new ConfigBuilder(Config.empty())
            .withMasterUrl(api.toString())
            .withTrustCerts(false)
            .withNamespace(e.getRequiredProperty("spring.cloud.kubernetes.client.namespace"))
            .withConnectionTimeout(2000)
            .withRequestTimeout(2000)
            .withRequestRetryBackoffLimit(0);
    if (!test) {
      config.withCaCertFile(e.getRequiredProperty("apex.discovery.kubernetes.ca-file"));
      String token =
          Files.readString(Path.of(e.getRequiredProperty("apex.discovery.kubernetes.token-file")))
              .strip();
      if (token.isEmpty() || token.length() > 32768)
        throw new IllegalStateException("Mounted service-account token required");
      config.withOauthToken(token);
    }
    return new KubernetesClientBuilder().withConfig(config.build()).build();
  }

  @Bean(destroyMethod = "close")
  EndpointSliceDiscoveryClient apexEndpointSlices(
      KubernetesClient api, Environment e, DiscoveryProperties p) {
    return new EndpointSliceDiscoveryClient(
        api,
        e.getRequiredProperty("spring.cloud.kubernetes.client.namespace"),
        p,
        Duration.ofMillis(e.getProperty("apex.discovery.max-stale-ms", Long.class, 15000L)),
        System::nanoTime,
        true);
  }

  @Bean
  CellServiceDirectory kubernetesCellDirectory(
      EndpointSliceDiscoveryClient provider, DiscoveryProperties p) {
    return new CloudCellServiceDirectory(provider, p, provider::fresh);
  }
}
