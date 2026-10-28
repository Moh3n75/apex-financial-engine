package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.security.*;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(
    after = GrpcCommonAutoConfiguration.class,
    before = GrpcServerAutoConfiguration.class)
@ConditionalOnProperty(prefix = "apex.grpc.server", name = "enabled", havingValue = "true")
@ConditionalOnProperty(prefix = "apex.grpc.security", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(PeerSecurityProperties.class)
public class GrpcPeerSecurityAutoConfiguration {
  @Bean
  CertificatePeerPolicy certificatePeerPolicy(
      PeerSecurityProperties policy,
      GrpcProperties p,
      org.springframework.beans.factory.config.ConfigurableListableBeanFactory beans) {
    if (p.isInsecureLoopbackTestOnly())
      throw new IllegalStateException(
          "Certificate security cannot use unverified loopback identity");
    if (beans.getBeanNamesForType(PeerIdentityVerifier.class, false, false).length > 1
        || beans.getBeanNamesForType(InvocationAuthorizer.class, false, false).length > 1)
      throw new IllegalStateException(
          "Select one certificate policy or a custom verifier/authorizer; do not combine policies");
    return new CertificatePeerPolicy(policy);
  }
}
