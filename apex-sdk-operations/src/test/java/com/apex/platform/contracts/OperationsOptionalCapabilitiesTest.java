package com.apex.platform.contracts;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.assertThat;

class OperationsOptionalCapabilitiesTest {
  @Test void metadataBacksOffWithoutMessagingAndRpc() {
    new ApplicationContextRunner()
        .withClassLoader(new FilteredClassLoader("com.apex.platform.messaging", "com.apex.platform.grpc.internal"))
        .withConfiguration(AutoConfigurations.of(ContractDiscoveryAutoConfiguration.class))
        .withPropertyValues("apex.contract-discovery.enabled=true")
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context).doesNotHaveBean("contractDiscoveryProvider");
        });
  }
}
