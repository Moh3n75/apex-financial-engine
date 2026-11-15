package com.apex.platform.grpc.internal;

import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.*;

class GrpcConfigurationContractTest {
    private final ApplicationContextRunner runner=new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(
            ApexExecutionContextAutoConfiguration.class,GrpcCommonAutoConfiguration.class))
            .withPropertyValues("apex.grpc.server.enabled=true","spring.application.name=application-fallback");
    @Test void identityFallbackPrecedenceRemainsCompatible() {
        runner.run(context->{assertThat(context).hasNotFailed();assertThat(context.getBean(GrpcProperties.class).getCellId()).isEqualTo("application-fallback");});
        runner.withPropertyValues("apex.cell.id=cell-fallback").run(context->{assertThat(context).hasNotFailed();
            assertThat(context.getBean(GrpcProperties.class).getCellId()).isEqualTo("cell-fallback");});
        runner.withPropertyValues("apex.cell.id=cell-fallback","apex.grpc.cell-id=explicit").run(context->{assertThat(context).hasNotFailed();
            assertThat(context.getBean(GrpcProperties.class).getCellId()).isEqualTo("explicit");});
    }
    @Test void typedSizeAndDeadlineLimitsFailBeforeTransportStartup() {
        runner.withPropertyValues("apex.grpc.max-payload-bytes=1").run(context->assertThat(context).hasFailed());
        runner.withPropertyValues("apex.grpc.max-deadline=0ms").run(context->assertThat(context).hasFailed());
    }
}
