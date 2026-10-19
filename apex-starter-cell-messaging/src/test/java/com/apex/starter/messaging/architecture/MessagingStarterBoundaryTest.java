package com.apex.starter.messaging.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.util.ClassUtils;

@Tag("architecture")
class MessagingStarterBoundaryTest {
    @Test
    void messagingStarterDoesNotSupplyWebMvc() {
        ClassLoader loader = getClass().getClassLoader();
        assertThat(ClassUtils.isPresent("org.springframework.web.servlet.DispatcherServlet", loader)).isFalse();
        assertThat(ClassUtils.isPresent("com.apex.platform.webmvc.ApexHttpExecutionContextFilter", loader)).isFalse();
    }
}
