package com.apex.customer.support;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PostgreSQLContainer;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/**
 * Prefers Testcontainers when Docker is available; otherwise starts Embedded PostgreSQL
 * so DB/API tests remain meaningful without silently skipping.
 */
public final class CustomerDatabaseExtension implements BeforeAllCallback {

    private static final ExtensionContext.Namespace NS =
            ExtensionContext.Namespace.create(CustomerDatabaseExtension.class);

    private static final Object LOCK = new Object();
    private static volatile boolean started;
    private static PostgreSQLContainer<?> container;
    private static EmbeddedPostgres embedded;
    private static String jdbcUrl;
    private static String username;
    private static String password;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        ensureStarted();
    }

    public static void ensureStarted() throws Exception {
        if (started) {
            return;
        }
        synchronized (LOCK) {
            if (started) {
                return;
            }
            if (DockerAvailability.isDockerAvailable()) {
                container = CustomerPostgresSupport.newContainer();
                container.start();
                jdbcUrl = container.getJdbcUrl();
                username = container.getUsername();
                password = container.getPassword();
            } else {
                embedded = EmbeddedPostgres.builder().start();
                username = "postgres";
                password = "postgres";
                String adminUrl = embedded.getJdbcUrl("postgres", "postgres");
                try (Connection connection = DriverManager.getConnection(adminUrl, username, password);
                     Statement statement = connection.createStatement()) {
                    statement.execute("CREATE DATABASE customer_db");
                } catch (Exception ignored) {
                    // database may already exist across class reloads
                }
                jdbcUrl = embedded.getJdbcUrl("postgres", "customer_db");
            }
            started = true;
            Runtime.getRuntime().addShutdownHook(new Thread(CustomerDatabaseExtension::shutdownQuietly));
        }
    }

    public static void registerDatasource(DynamicPropertyRegistry registry) {
        try {
            ensureStarted();
        } catch (Exception ex) {
            throw new IllegalStateException("Customer test database failed to start", ex);
        }
        registry.add("spring.datasource.url", () -> jdbcUrl);
        registry.add("spring.datasource.username", () -> username);
        registry.add("spring.datasource.password", () -> password);
        registry.add("server.port", () -> "0");
    }

    public static String mode() {
        return container != null ? "testcontainers" : "embedded-postgres";
    }

    private static void shutdownQuietly() {
        if (container != null) {
            container.stop();
        }
        if (embedded != null) {
            try {
                embedded.close();
            } catch (IOException ignored) {
                // shutdown hook
            }
        }
    }
}
