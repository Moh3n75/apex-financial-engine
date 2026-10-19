package com.apex.reporting.support;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;

/** Content-based build receipt, written only by the ordered clean-build script. */
public final class M22BuildPrerequisite {
    private static final List<String> MODULES = List.of(
            "apex-platform-core", "apex-platform-events", "apex-platform-context-autoconfigure",
            "apex-platform-webmvc", "apex-platform-messaging", "apex-starter-webmvc",
            "apex-starter-cell-messaging", "customer-cell");

    private M22BuildPrerequisite() {}

    public static Path verify(Path root) throws Exception {
        Path receipt = root.resolve("out/m22-build-evidence.properties");
        require(Files.isRegularFile(receipt), "Missing clean-build receipt");
        Properties evidence = new Properties();
        try (Reader input = Files.newBufferedReader(receipt, StandardCharsets.UTF_8)) {
            evidence.load(input);
        }
        require("1".equals(evidence.getProperty("schemaVersion")), "Unsupported build receipt");
        require(sourceFingerprint(root).equals(evidence.getProperty("sourceSha256")),
                "Customer/Platform source differs from the clean-build receipt");
        for (String module : MODULES) {
            Path jar = root.resolve(module.equals("customer-cell") ? "customer-cell/target/customer-cell.jar"
                    : module + "/target/" + module + "-1.0-SNAPSHOT.jar");
            require(Files.isRegularFile(jar), "Missing artifact: " + module);
            require(sha256(jar).equals(evidence.getProperty(module + ".sha256")),
                    "Artifact differs from clean-build receipt: " + module);
        }
        // The Reporting JVM must use the same Platform artifacts as the Customer build.
        for (Class<?> type : List.of(com.apex.platform.core.context.ExecutionContext.class,
                com.apex.platform.events.EventEnvelope.class,
                com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration.class,
                com.apex.platform.messaging.outbox.OutboxAppender.class)) {
            Path loaded = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
            String module = type.getPackageName().startsWith("com.apex.platform.core") ? "apex-platform-core"
                    : type.getPackageName().startsWith("com.apex.platform.events") ? "apex-platform-events"
                    : type.getPackageName().startsWith("com.apex.platform.context") ? "apex-platform-context-autoconfigure"
                    : "apex-platform-messaging";
            require(Files.isRegularFile(loaded) && sha256(loaded).equals(evidence.getProperty(module + ".sha256")),
                    "Reporting loaded a different Platform artifact: " + module);
        }
        System.out.println("M22_BUILD_VERIFIED sourceSha256=" + evidence.getProperty("sourceSha256")
                + " customerJarSha256=" + evidence.getProperty("customer-cell.sha256"));
        return root.resolve("customer-cell/target/customer-cell.jar");
    }

    static String sourceFingerprint(Path root) throws Exception {
        List<String> paths = new ArrayList<>(List.of("pom.xml", "tools/m22/verify-cells.ps1"));
        for (String module : MODULES) {
            paths.add(module + "/pom.xml");
            Path mainSources = root.resolve(module + "/src/main");
            if (Files.isDirectory(mainSources)) {
                try (var files = Files.walk(mainSources)) {
                    files.filter(Files::isRegularFile).forEach(path ->
                            paths.add(root.relativize(path).toString().replace('\\', '/')));
                }
            }
        }
        paths.sort(String::compareTo);
        StringBuilder manifest = new StringBuilder();
        for (String path : paths) {
            manifest.append(path).append('\n').append(sha256(root.resolve(path))).append('\n');
        }
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(manifest.toString().getBytes(StandardCharsets.UTF_8)));
    }

    static String sha256(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(file)) {
            byte[] buffer = new byte[8192];
            for (int read; (read = input.read(buffer)) != -1;) digest.update(buffer, 0, read);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void require(boolean condition, String problem) throws IOException {
        if (!condition) throw new IOException(problem + ". Run tools/m22/verify-cells.ps1 with Java 21 first.");
    }
}
