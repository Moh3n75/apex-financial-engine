package com.apex.reporting.support;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("unit")
class M22BuildPrerequisiteTest {
    @TempDir Path root;

    @Test
    void rejectsMissingReceiptBeforeStartingInfrastructure() {
        assertThatThrownBy(() -> M22BuildPrerequisite.verify(root)).hasMessageContaining("Missing clean-build receipt");
    }

    @Test
    void rejectsChangedSourceEvenWhenOldJarStillExists() throws Exception {
        fixture();
        Files.writeString(root.resolve("customer-cell/src/main/changed.txt"), "new source");
        assertThatThrownBy(() -> M22BuildPrerequisite.verify(root)).hasMessageContaining("source differs");
    }

    @Test
    void rejectsArtifactReplacedAfterReceipt() throws Exception {
        fixture();
        Files.writeString(root.resolve("customer-cell/target/customer-cell.jar"), "stale artifact");
        assertThatThrownBy(() -> M22BuildPrerequisite.verify(root)).hasMessageContaining("Artifact differs");
    }

    @ParameterizedTest
    @ValueSource(strings = {"apex-starter-webmvc", "apex-starter-cell-messaging"})
    void rejectsChangedStarterPomEvenWithoutMainSources(String module) throws Exception {
        fixture();
        Files.writeString(root.resolve(module + "/pom.xml"), "changed dependency graph");
        assertThatThrownBy(() -> M22BuildPrerequisite.verify(root)).hasMessageContaining("source differs");
    }

    @ParameterizedTest
    @ValueSource(strings = {"apex-platform-context-autoconfigure", "apex-platform-webmvc"})
    void rejectsChangedHttpOrContextSource(String module) throws Exception {
        fixture();
        Files.writeString(root.resolve(module + "/src/main/changed.txt"), "changed source");
        assertThatThrownBy(() -> M22BuildPrerequisite.verify(root)).hasMessageContaining("source differs");
    }

    private void fixture() throws Exception {
        Files.writeString(root.resolve("pom.xml"), "root");
        Files.createDirectories(root.resolve("tools/m22"));
        Files.writeString(root.resolve("tools/m22/verify-cells.ps1"), "build script");
        Properties evidence = new Properties();
        evidence.setProperty("schemaVersion", "1");
        for (String module : List.of("apex-platform-core", "apex-platform-events", "apex-platform-context-autoconfigure",
                "apex-platform-webmvc", "apex-platform-messaging", "apex-starter-webmvc",
                "apex-starter-cell-messaging", "customer-cell")) {
            if (!module.startsWith("apex-starter-")) {
                Files.createDirectories(root.resolve(module + "/src/main"));
            }
            Files.createDirectories(root.resolve(module + "/target"));
            Files.writeString(root.resolve(module + "/pom.xml"), module);
            Path jar = root.resolve(module.equals("customer-cell") ? "customer-cell/target/customer-cell.jar"
                    : module + "/target/" + module + "-1.0-SNAPSHOT.jar");
            Files.writeString(jar, "original artifact");
            evidence.setProperty(module + ".sha256", M22BuildPrerequisite.sha256(jar));
        }
        evidence.setProperty("sourceSha256", M22BuildPrerequisite.sourceFingerprint(root));
        Files.createDirectories(root.resolve("out"));
        try (var writer = Files.newBufferedWriter(root.resolve("out/m22-build-evidence.properties"))) {
            evidence.store(writer, "isolated test receipt");
        }
    }
}
