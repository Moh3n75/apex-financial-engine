package com.apex.operations.owner;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;

/** A suite owns its pool and database; bounded reads measure work on warm connections. */
final class OperationsTestDatabase {
    private OperationsTestDatabase() {}

    static HikariDataSource pooled(DataSource database) throws Exception {
        var config = new HikariConfig();
        config.setDataSource(database);
        config.setMaximumPoolSize(4);
        config.setMinimumIdle(2);
        config.setPoolName("operations-test");
        var pool = new HikariDataSource(config);
        // Establish both connections before timed reads, without sleeping or retrying reads.
        try (var first = pool.getConnection(); var second = pool.getConnection()) {
            first.isValid(1);
            second.isValid(1);
        }
        return pool;
    }

    static DataSource slowAcquisition(DataSource database) {
        return new org.springframework.jdbc.datasource.DelegatingDataSource(database) {
            @Override public java.sql.Connection getConnection() throws java.sql.SQLException {
                try { Thread.sleep(800); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new java.sql.SQLException("Test acquisition interrupted", interrupted);
                }
                return super.getConnection();
            }
        };
    }
}
