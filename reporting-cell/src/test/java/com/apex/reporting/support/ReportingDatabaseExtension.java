package com.apex.reporting.support;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.test.context.DynamicPropertyRegistry;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

public final class ReportingDatabaseExtension implements BeforeAllCallback {

    private static final Object LOCK = new Object();
    private static volatile boolean started;
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
            embedded = EmbeddedPostgres.builder().start();
            username = "postgres";
            password = "postgres";
            String adminUrl = embedded.getJdbcUrl("postgres", "postgres");
            try (Connection connection = DriverManager.getConnection(adminUrl, username, password);
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE DATABASE reporting_db");
            } catch (Exception ignored) {
                // already exists
            }
            jdbcUrl = embedded.getJdbcUrl("postgres", "reporting_db");
            started = true;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (embedded != null) {
                        embedded.close();
                    }
                } catch (IOException ignored) {
                    // shutdown
                }
            }));
        }
    }

    public static void registerDatasource(DynamicPropertyRegistry registry) {
        try {
            ensureStarted();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
        registry.add("spring.datasource.url", () -> jdbcUrl);
        registry.add("spring.datasource.username", () -> username);
        registry.add("spring.datasource.password", () -> password);
        registry.add("server.port", () -> "0");
        registry.add("spring.flyway.locations", () -> "classpath:db/migration,classpath:db/platform");
    }
}
