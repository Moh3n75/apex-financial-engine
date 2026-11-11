package com.apex.platform.contracts;

import com.apex.operations.contracts.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Explicit local metadata read of the same provider used by gRPC. Never enabled for a non-local bind/profile. */
@AutoConfiguration(after=ContractDiscoveryAutoConfiguration.class)
@ConditionalOnWebApplication(type=ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(name="org.springframework.web.bind.annotation.RestController")
@ConditionalOnBean(ContractDiscoveryProvider.class)
@ConditionalOnProperty(name="apex.contract-discovery.local-http.enabled",havingValue="true")
@Profile("local-ops")
public class LocalContractHttpAutoConfiguration {
  @Bean
  @ConditionalOnMissingBean(LocalContractRead.class)
  LocalContractRead localContractRead(ContractDiscoveryProvider provider,Environment env) {
    return new LocalContractRead(provider,env);
  }
}
