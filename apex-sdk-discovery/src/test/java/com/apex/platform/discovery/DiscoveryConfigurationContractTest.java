package com.apex.platform.discovery;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.*;

class DiscoveryConfigurationContractTest {
    private final ApplicationContextRunner runner=new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DiscoveryPolicyAutoConfiguration.class));
    @Test void noneDoesNotCreateProviderClients() {
        runner.run(context -> {assertThat(context).hasNotFailed();assertThat(context).doesNotHaveBean(CellServiceDirectory.class);});
    }
    @Test void invalidProviderAndRefreshFailBeforeProviderCreation() {
        runner.withPropertyValues("apex.discovery.provider=typo").run(context ->
                assertThat(context).hasFailed().getFailure().hasMessageContaining("discovery"));
        runner.withPropertyValues("apex.discovery.refresh-interval=0ms").run(context -> assertThat(context).hasFailed());
    }
}
