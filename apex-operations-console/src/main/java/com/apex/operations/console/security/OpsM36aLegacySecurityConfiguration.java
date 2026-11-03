package com.apex.operations.console.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

/**
 * When M36A is off, keep pre-M36 anonymous loopback Console behavior for existing OPS-03* ITs.
 * ReadOnlyFilter remains the GET-only / size gate.
 */
@Configuration
@EnableWebSecurity
@ConditionalOnProperty(
    prefix = "apex.ops.m36a",
    name = "enabled",
    havingValue = "false",
    matchIfMissing = true)
public class OpsM36aLegacySecurityConfiguration {
  @Bean
  SecurityFilterChain opsLegacyAnonymousChain(HttpSecurity http) throws Exception {
    http.csrf(AbstractHttpConfigurer::disable)
        .cors(AbstractHttpConfigurer::disable)
        .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());
    return http.build();
  }
}
