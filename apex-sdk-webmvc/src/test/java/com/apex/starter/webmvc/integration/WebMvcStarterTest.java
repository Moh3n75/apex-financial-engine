package com.apex.starter.webmvc.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.webmvc.ApexHttpExecutionContextFilter;

@Tag("integration")
@SpringBootTest(classes = WebMvcStarterTest.HttpCell.class, properties = {
        "spring.application.name=http-only-cell", "apex.cell.id=http-only",
        "spring.config.location=optional:classpath:/m23-test.yml", "spring.config.additional-location=",
        "spring.config.import=", "spring.profiles.active=", "spring.profiles.include="})
@ContextConfiguration(initializers = WebMvcStarterTest.IsolatedEnvironment.class)
@AutoConfigureMockMvc
class WebMvcStarterTest {
    @Autowired private ApplicationContext applicationContext;
    @Autowired private MockMvc mockMvc;
    @Autowired private ExecutionContextProvider provider;
    @Autowired private ProbeController controller;

    @Test
    void discoversOneProviderAndOneFilterThroughBoot() {
        assertThat(applicationContext.getBeansOfType(ExecutionContextProvider.class)).hasSize(1);
        assertThat(applicationContext.getBeansOfType(ApexHttpExecutionContextFilter.class))
                .hasSize(1).containsKey("apexHttpExecutionContextFilter");
        assertThat(controller.provider).isSameAs(provider);
    }

    @Test
    void requestUsesTheRegisteredProviderAndClosesItsRootScope() throws Exception {
        assertThatIllegalStateException().isThrownBy(provider::requireCurrent);
        mockMvc.perform(get("/probe")).andExpect(status().isOk());
        ExecutionContext first = controller.observed.get();
        assertThat(first.correlationId()).isNotNull();
        assertThat(first.causationId()).isNull();
        assertThat(first.sourceService()).isEqualTo("http-only-cell");
        assertThat(first.cellId()).isEqualTo("http-only");
        assertThatIllegalStateException().isThrownBy(provider::requireCurrent);
        mockMvc.perform(get("/probe")).andExpect(status().isOk());
        assertThat(controller.observed.get().correlationId()).isNotEqualTo(first.correlationId());
        assertThatIllegalStateException().isThrownBy(provider::requireCurrent);
    }

    @Test
    void requestFailureAlsoClosesTheScope() {
        assertThatThrownBy(() -> mockMvc.perform(get("/failure")))
                .hasRootCauseMessage("test request failure");
        assertThatIllegalStateException().isThrownBy(provider::requireCurrent);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(ProbeController.class)
    static class HttpCell {}

    @RestController
    static class ProbeController {
        private final ExecutionContextProvider provider;
        private final AtomicReference<ExecutionContext> observed = new AtomicReference<>();

        ProbeController(ExecutionContextProvider provider) {
            this.provider = provider;
        }

        @GetMapping("/probe")
        String probe() {
            observed.set(provider.requireCurrent());
            return "ok";
        }

        @GetMapping("/failure")
        String failure() {
            provider.requireCurrent();
            throw new IllegalStateException("test request failure");
        }
    }

    static class IsolatedEnvironment implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext context) {
            context.getEnvironment().getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
            context.getEnvironment().getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        }
    }
}
