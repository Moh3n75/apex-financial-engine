package com.apex.platform.grpc.internal;

import static org.assertj.core.api.Assertions.*;

import com.apex.platform.grpc.api.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;

/** Real Netty deadline and collection expiry can complete on different threads simultaneously. */
@Tag("integration")
class CollectorDeadlineIntegrationTest {
  @Test
  void repeatedNativeDeadlinesNeverBlockTheSharedDeadlineScheduler() throws Exception {
    try (var server =
            GrpcRuntimeTest.boot(
                GrpcRuntimeTest.App.class,
                "--apex.grpc.server.enabled=true",
                "--apex.grpc.cell-id=m31-server",
                "--apex.grpc.insecure-loopback-test-only=true",
                "--apex.grpc.test-clients[0]=m31-client",
                "--spring.grpc.server.address=127.0.0.1",
                "--spring.grpc.server.port=0");
        var client =
            GrpcRuntimeTest.boot(
                GrpcRuntimeTest.Client.class,
                "--apex.grpc.client.enabled=true",
                "--apex.grpc.cell-id=m31-client",
                "--apex.grpc.insecure-loopback-test-only=true",
                "--apex.grpc.destinations.m31-server.channel=server",
                "--spring.grpc.client.channel.server.target=static://127.0.0.1:"
                    + GrpcRuntimeTest.App.PORT.get(),
                "--spring.grpc.client.channel.server.ssl.enabled=false",
                "--apex.grpc.collector.enabled=true",
                "--apex.grpc.collector.allowed-responders=m31-server")) {
      var gateway = client.getBean(MultiResponderGateway.class);
      for (int i = 0; i < 1000; i++) {
        var invocation =
            gateway
                .collectQueries(
                    new GrpcRuntimeTest.ProbeQuery("slow", 80),
                    GrpcRuntimeTest.ProbeReply.class,
                    new CollectionOptions(List.of("m31-server"), Duration.ofMillis(25), true))
                .toCompletableFuture();
        CollectionResult<GrpcRuntimeTest.ProbeReply> result;
        try {
          result = invocation.get(1, TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException failure) {
          var dump = new StringBuilder();
          for (var t :
              java.lang.management.ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
            dump.append(t.getThreadName())
                .append(" ")
                .append(t.getThreadState())
                .append(" owner=")
                .append(t.getLockOwnerName())
                .append('\n');
            for (var frame : t.getStackTrace()) dump.append("    ").append(frame).append('\n');
          }
          java.nio.file.Files.writeString(
              java.nio.file.Path.of("target/m32-deadline-threads.log"), dump);
          throw failure;
        }
        assertThat(result.responders()).hasSize(1);
        assertThat(result.responders().getFirst().status())
            .isEqualTo(ResponderResult.Status.TIMED_OUT);
      }
    }
  }
}
