
package com.apex.platform.context.autoconfigure;

import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class ApexExecutionContextAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    ApexExecutionContextAutoConfiguration.class
                            )
                    );

    @Test
    void shouldCreateDefaultProvider() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(context)
                    .hasSingleBean(ExecutionContextProvider.class);

            assertThat(context.getBean(
                    ExecutionContextProvider.class
            )).isInstanceOf(
                    ThreadLocalExecutionContextProvider.class
            );
        });
    }

    @Test
    void shouldRespectCustomProvider() {
        ExecutionContextProvider custom =
                new ThreadLocalExecutionContextProvider();

        runner
                .withBean(
                        ExecutionContextProvider.class,
                        () -> custom
                )
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    assertThat(context.getBean(
                            ExecutionContextProvider.class
                    )).isSameAs(custom);
                });
    }
}
