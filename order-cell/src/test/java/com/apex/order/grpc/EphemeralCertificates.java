package com.apex.order.grpc;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.*;

/** Test-owned CA/key material only; never packaged in a business JAR or checked into Git. */
final class EphemeralCertificates implements AutoCloseable {
  final Path directory = Files.createTempDirectory("apex-m32-tls-");
  final String password = UUID.randomUUID().toString();
  final Path passwordFile = directory.resolve("password.txt");
  final Path ca = directory.resolve("ca.p12"), trust = directory.resolve("trust.p12");
  final Path server, primary, observer, wrongSan, foreign, rogue;

  EphemeralCertificates() throws Exception {
    try {
      Files.writeString(passwordFile, password);
      key("ca", "CN=APEX M32 ephemeral CA", "bc:critical=ca:true");
      tool(
          "-exportcert",
          "-rfc",
          "-alias",
          "ca",
          "-keystore",
          ca.toString(),
          "-file",
          directory.resolve("ca.crt").toString());
      tool(
          "-importcert",
          "-noprompt",
          "-alias",
          "ca",
          "-keystore",
          trust.toString(),
          "-file",
          directory.resolve("ca.crt").toString());
      server = signed("server", "dns:localhost,ip:127.0.0.1", "serverAuth");
      primary =
          signed("primary", "uri:spiffe://apex.test/tenant/alpha/cell/m32-client", "clientAuth");
      observer =
          signed("observer", "uri:spiffe://apex.test/tenant/alpha/cell/m32-observer", "clientAuth");
      foreign =
          signed("foreign", "uri:spiffe://apex.test/tenant/beta/cell/m32-foreign", "clientAuth");
      wrongSan = signed("wrong-san", "dns:not-a-cell", "clientAuth");
      // Self-signed client identity is not chained to the server's trusted CA.
      rogue =
          key(
              "rogue",
              "CN=untrusted-client",
              "san=uri:spiffe://apex.test/tenant/alpha/cell/m32-client");
    } catch (Exception | Error failure) {
      try {
        close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  Path key(String alias, String subject, String extension) throws Exception {
    Path store = directory.resolve(alias + ".p12");
    tool(
        "-genkeypair",
        "-noprompt",
        "-alias",
        alias,
        "-dname",
        subject,
        "-keyalg",
        "RSA",
        "-keysize",
        "2048",
        "-validity",
        "2",
        "-storetype",
        "PKCS12",
        "-keystore",
        store.toString(),
        "-ext",
        extension);
    return store;
  }

  Path signed(String alias, String san, String purpose) throws Exception {
    Path store = key(alias, "CN=" + alias, "san=" + san);
    Path csr = directory.resolve(alias + ".csr"), crt = directory.resolve(alias + ".crt");
    tool("-certreq", "-alias", alias, "-keystore", store.toString(), "-file", csr.toString());
    tool(
        "-gencert",
        "-rfc",
        "-alias",
        "ca",
        "-keystore",
        ca.toString(),
        "-infile",
        csr.toString(),
        "-outfile",
        crt.toString(),
        "-validity",
        "2",
        "-ext",
        "ku=digitalSignature,keyEncipherment",
        "-ext",
        "eku=" + purpose,
        "-ext",
        "san=" + san);
    tool(
        "-importcert",
        "-noprompt",
        "-alias",
        "ca",
        "-keystore",
        store.toString(),
        "-file",
        directory.resolve("ca.crt").toString());
    tool(
        "-importcert",
        "-noprompt",
        "-alias",
        alias,
        "-keystore",
        store.toString(),
        "-file",
        crt.toString());
    return store;
  }

  void tool(String... args) throws Exception {
    var command =
        new ArrayList<>(
            List.of(Path.of(System.getProperty("java.home"), "bin", "keytool").toString()));
    command.addAll(List.of(args));
    command.addAll(List.of("-storepass:file", passwordFile.toString()));
    Path output = directory.resolve("keytool.log");
    var process =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      if (!process.waitFor(20, TimeUnit.SECONDS) || process.exitValue() != 0)
        throw new IllegalStateException(
            "Ephemeral keytool step failed (private diagnostics withheld)");
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
      }
    }
  }

  List<String> bundle(Path keys) {
    var args = new ArrayList<String>();
    String prefix = "--spring.ssl.bundle.jks.m32.";
    args.add(prefix + "truststore.location=" + trust.toUri());
    args.add(prefix + "truststore.password=" + password);
    args.add(prefix + "truststore.type=PKCS12");
    if (keys != null) {
      args.add(prefix + "keystore.location=" + keys.toUri());
      args.add(prefix + "keystore.password=" + password);
      args.add(prefix + "keystore.type=PKCS12");
    }
    return args;
  }

  SSLContext context(Path keys, Path trusted) throws Exception {
    KeyManager[] managers = null;
    if (keys != null) {
      var ks = KeyStore.getInstance("PKCS12");
      try (var in = Files.newInputStream(keys)) {
        ks.load(in, password.toCharArray());
      }
      var km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      km.init(ks, password.toCharArray());
      managers = km.getKeyManagers();
    }
    var ts = KeyStore.getInstance("PKCS12");
    try (var in = Files.newInputStream(trusted)) {
      ts.load(in, password.toCharArray());
    }
    var tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tm.init(ts);
    var context = SSLContext.getInstance("TLS");
    context.init(managers, tm.getTrustManagers(), new SecureRandom());
    return context;
  }

  @Override
  public void close() throws IOException {
    if (Files.exists(directory))
      try (var paths = Files.walk(directory)) {
        for (var p : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
      }
  }
}
