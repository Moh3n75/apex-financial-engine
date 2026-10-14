package com.apex.customer.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PostgreSQLContainer;

public final class CustomerPostgresSupport {

    private CustomerPostgresSupport() {
    }

    public static PostgreSQLContainer<?> newContainer() {
        return new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("customer_db")
                .withUsername("customer_user")
                .withPassword("local_dev_only");
    }

    public static void registerDatasource(
            DynamicPropertyRegistry registry,
            PostgreSQLContainer<?> postgres
    ) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("server.port", () -> "0");
    }
}
