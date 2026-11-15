package com.apex.platform.messaging.integration;

import com.apex.platform.messaging.jdbc.PlatformCellMessagingAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.*;

class MessagingConfigurationContractTest {
    private final ApplicationContextRunner runner=new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PlatformCellMessagingAutoConfiguration.class));
    @Test void disabledMessagingDoesNotValidateUnusedSettings() {
        runner.withPropertyValues("apex.messaging.consumer.concurrency=0", "apex.messaging.outbox.publisher.interval-ms=0")
                .run(context -> assertThat(context).hasNotFailed());
    }
    @Test void selectedCapabilitiesRejectBoundsBeforeInfrastructureCreation() {
        runner.withPropertyValues("apex.messaging.cell.enabled=true","apex.messaging.outbox.publisher.interval-ms=0")
                .run(context -> assertThat(context).hasFailed().getFailure().hasMessageContaining("interval-ms"));
        runner.withPropertyValues("apex.messaging.cell.enabled=true","apex.messaging.outbox.publisher.enabled=false",
                "apex.messaging.consumer.enabled=true","apex.messaging.consumer.concurrency=0")
                .run(context -> assertThat(context).hasFailed().getFailure().hasMessageContaining("concurrency"));
    }
}
