
package com.apex.platform.webmvc.integration;

import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;
import com.apex.platform.webmvc.ApexHttpExecutionContextFilter;

import com.apex.platform.webmvc.ApexWebMvcAutoConfiguration;
import jakarta.servlet.Filter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class ApexWebMvcAutoConfigurationTest {

    private final WebApplicationContextRunner runner =
            new WebApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    ApexWebMvcAutoConfiguration.class
                            )
                    )
                    .withPropertyValues(
                            "spring.application.name=customer-cell",
                            "apex.cell.id=customer-cell"
                    );

    @Test
    void shouldRegisterFilterWhenProviderExists() {

        runner
                .withBean(
                        ExecutionContextProvider.class,
                        ThreadLocalExecutionContextProvider::new
                )
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    assertThat(context).hasSingleBean(
                            ApexHttpExecutionContextFilter.class
                    );

                    assertThat(context).hasBean(
                            "apexHttpExecutionContextFilter"
                    );
                });
    }

    @Test
    void shouldNotRegisterFilterWithoutProvider() {

        runner.run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(context).doesNotHaveBean(
                    ApexHttpExecutionContextFilter.class
            );
        });
    }

    @Test
    void shouldNotRegisterFilterWhenDisabled() {

        runner
                .withBean(
                        ExecutionContextProvider.class,
                        ThreadLocalExecutionContextProvider::new
                )
                .withPropertyValues(
                        "apex.webmvc.context.enabled=false"
                )
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    assertThat(context).doesNotHaveBean(
                            ApexHttpExecutionContextFilter.class
                    );
                });
    }

    @Test
    void shouldNotRegisterDuplicateFilter() {

        Filter existingFilter = (request, response, chain) ->
                chain.doFilter(request, response);

        runner
                .withBean(
                        ExecutionContextProvider.class,
                        ThreadLocalExecutionContextProvider::new
                )
                .withBean(
                        "apexHttpExecutionContextFilter",
                        Filter.class,
                        () -> existingFilter
                )
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    assertThat(
                            context.getBean(
                                    "apexHttpExecutionContextFilter"
                            )
                    ).isSameAs(existingFilter);

                    assertThat(context).doesNotHaveBean(
                            ApexHttpExecutionContextFilter.class
                    );
                });
    }
}
