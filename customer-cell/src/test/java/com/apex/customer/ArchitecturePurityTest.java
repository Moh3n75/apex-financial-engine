package com.apex.customer;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * Guards M21 boundaries: generic template stays clean; this Cell has no local
 * platform util/security/observability/sdk/deployment trees.
 */
class ArchitecturePurityTest {

    @Test
    void genericTemplateContainsNoCustomerBusinessCode() throws Exception {
        Path templateSrc = Path.of("..", "apex-cell-template", "src");
        if (!Files.isDirectory(templateSrc)) {
            return;
        }
        try (var paths = Files.walk(templateSrc)) {
            boolean leaked = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String name = p.getFileName().toString().toLowerCase();
                        return name.endsWith(".java") || name.endsWith(".sql") || name.endsWith(".json");
                    })
                    .anyMatch(p -> {
                        try {
                            String text = Files.readString(p);
                            return text.contains("CUST-10001")
                                    || text.contains("CreateCustomer")
                                    || text.contains("CREATE TABLE customers");
                        } catch (Exception ex) {
                            return false;
                        }
                    });
            assertThat(leaked).isFalse();
        }
    }

    @Test
    void customerCellDoesNotShipLocalPlatformPackages() {
        assertThat(Files.exists(Path.of("src/main/java/com/apex/customer/util"))).isFalse();
        assertThat(Files.exists(Path.of("src/main/java/com/apex/customer/security"))).isFalse();
        assertThat(Files.exists(Path.of("src/main/java/com/apex/customer/observability"))).isFalse();
        assertThat(Files.exists(Path.of("src/main/java/com/apex/customer/sdk"))).isFalse();
        assertThat(Files.exists(Path.of("deployment"))).isFalse();
        assertThat(Files.exists(Path.of("kubernetes"))).isFalse();
    }
}
