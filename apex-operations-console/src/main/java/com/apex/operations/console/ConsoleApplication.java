package com.apex.operations.console;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.server.servlet.ConfigurableServletWebServerFactory;
import org.springframework.core.env.Environment;

/**
 * OAuth2 resource-server auto-config is excluded; M36A registers an explicit chain when enabled.
 */
@SpringBootApplication(excludeName = {
        "org.springframework.boot.security.oauth2.server.resource.autoconfigure.servlet.OAuth2ResourceServerAutoConfiguration",
        "org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration"
})
public class ConsoleApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsoleApplication.class, args);
    }

    @Bean
    WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> loopback(Environment e) {
        return f -> {
            String a = e.getProperty("server.address", "127.0.0.1");
            if (!a.equals("127.0.0.1") && !a.equals("::1")) throw new IllegalStateException("OPS_LOOPBACK_REQUIRED");
            try {
                f.setAddress(java.net.InetAddress.getByName(a));
            } catch (Exception x) {
                throw new IllegalStateException("OPS_BIND");
            }
        };
    }
}
