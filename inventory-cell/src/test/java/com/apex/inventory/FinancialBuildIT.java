package com.apex.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Build-time schema fixture only: no application profile or deployment credentials. */
@Tag("integration")
class FinancialBuildIT {
    @Test
    void generatesRealFinancialSchemaAndCompilesAllFinancialTests() throws Exception {
        Path root = Path.of(System.getProperty("saga.repositoryRoot")).toAbsolutePath().normalize();
        Path migrations = root.resolve("apex-bootstrap/src/main/resources/db/migration");
        Path output = root.resolve("out");
        Files.createDirectories(output);
        try (var postgres = EmbeddedPostgres.builder().start()) {
            var flyway = Flyway.configure().dataSource(postgres.getPostgresDatabase())
                    .locations("filesystem:" + migrations).load();
            var migrated = flyway.migrate();
            assertThat(migrated.migrationsExecuted).isEqualTo(16);
            assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("16");
            var hashes = new ArrayList<Map<String, String>>();
            try (var files = Files.list(migrations)) {
                for (Path file : files.sorted().toList()) {
                    hashes.add(Map.of("file", file.getFileName().toString(), "sha256", OrderInventorySagaIT.sha(file)));
                }
            }
            List<String> properties = List.of("-Djooq.codegen.skip=false",
                    "-Dapex.jooq.url=" + postgres.getJdbcUrl("postgres", "postgres"),
                    "-Dapex.jooq.user=postgres", "-Dapex.jooq.password=");
            run(root, output.resolve("m30-financial-compile.log"), properties,
                    "-pl", "apex-bootstrap", "-am", "clean", "test-compile");
            Path generated = root.resolve("apex-infrastructure/target/generated-sources/jooq");
            long generatedFiles;
            try (var files = Files.walk(generated)) {
                generatedFiles = files.filter(p -> p.toString().endsWith(".java")).count();
            }
            assertThat(generatedFiles).isGreaterThanOrEqualTo(29);
            assertThat(root.resolve("apex-bootstrap/target/test-classes/com/apex/apexbootstrap/purchase/PurchaseIntegrationTest.class"))
                    .isRegularFile();
            run(root, output.resolve("m30-financial-focused.log"), properties,
                    "-pl", "apex-bootstrap", "-am", "test",
                    "-Dtest=CreditAccountTest,CreditReservationTest,FinancialTransactionTest,ThreadLocalExecutionContextProviderTest",
                    "-Dsurefire.failIfNoSpecifiedTests=false");
            Files.writeString(output.resolve("m30-financial-schema-evidence.json"), OrderInventorySagaIT.JSON
                    .writerWithDefaultPrettyPrinter().writeValueAsString(Map.of(
                            "migrationVersion", "16", "migrations", hashes, "generatedJavaFiles", generatedFiles,
                            "compileExit", 0, "focusedTestsExit", 0, "database", "owned disposable native PostgreSQL",
                            "domainLogicChanged", false)));
        }
    }

    private static void run(Path root, Path log, List<String> properties, String... goals) throws Exception {
        String maven = System.getProperty("m30.maven", "mvn.cmd");
        var command = new ArrayList<>(List.of(maven, "-B", "-ntp", "-o"));
        command.addAll(properties); command.addAll(List.of(goals));
        var builder = new ProcessBuilder(command).directory(root.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().keySet().removeIf(k -> k.matches("(?i)(SPRING_.*|APEX_.*|CELL_.*|KAFKA_.*|MAVEN_ARGS|MAVEN_OPTS|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
        Process child = builder.start();
        try {
            assertThat(child.waitFor(5, TimeUnit.MINUTES)).as("Bounded isolated Financial Maven build; see %s", log).isTrue();
            assertThat(child.exitValue()).as("Financial Maven result; see %s", log).isZero();
        } finally {
            if (child.isAlive()) { child.destroyForcibly(); child.waitFor(10, TimeUnit.SECONDS); }
        }
    }
}
