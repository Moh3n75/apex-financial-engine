package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.TransactionalRequestExecutor;
import com.apex.platform.grpc.security.PeerIdentityVerifier;
import io.grpc.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.*;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.grpc.server.ServerBuilderCustomizer;

@AutoConfiguration(
    after = GrpcCommonAutoConfiguration.class,
    beforeName = "org.springframework.boot.grpc.server.autoconfigure.GrpcServerAutoConfiguration")
@ConditionalOnClass(GrpcServerProperties.class)
@ConditionalOnProperty(prefix = "apex.grpc.server", name = "enabled", havingValue = "true")
public class GrpcServerAutoConfiguration {
  @Bean(destroyMethod = "shutdown")
  ThreadPoolExecutor grpcServerWorkers() {
    return new ThreadPoolExecutor(
        8,
        8,
        30,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(32),
        Thread.ofPlatform().daemon().name("apex-grpc-server-", 0).factory(),
        new ThreadPoolExecutor.AbortPolicy());
  }

  @Bean
  BindableService apexCellGrpcService(
      RpcDispatcher dispatcher,
      GrpcProperties p,
      Environment env,
      ObjectProvider<PeerIdentityVerifier> verifiers) {
    var verifier = verifiers.getIfAvailable();
    if (p.isInsecureLoopbackTestOnly()) {
      String address = env.getProperty("spring.grpc.server.address", "");
      if (!java.util.Set.of("127.0.0.1", "::1", "localhost").contains(address)
          || p.getTestClients().isEmpty())
        throw new IllegalStateException(
            "Test-only plaintext gRPC requires explicit loopback bind and test-clients");
    } else if (verifier == null
        || env.getProperty("spring.grpc.server.ssl.bundle", "").isBlank()
        || !env.getProperty("spring.grpc.server.ssl.client-auth", "").equalsIgnoreCase("require")) {
      throw new IllegalStateException(
          "Production gRPC requires a trusted PeerIdentityVerifier and an mTLS SSL"
              + " bundle/client-auth=require");
    }
    var service = new GenericGrpcService(dispatcher, p);
    var auth = new RpcServerAuth(p, verifier);
    return () -> ServerInterceptors.intercept(service, auth);
  }

  @Bean
  SmartInitializingSingleton grpcPersistentRequestPreflight(
      HandlerRegistry registry, ObjectProvider<TransactionalRequestExecutor> executor) {
    return () -> {
      if (registry.hasRequests() && executor.getIfAvailable() == null)
        throw new IllegalStateException(
            "Mutating RPC handlers require an owning persistent TransactionalRequestExecutor");
    };
  }

  @Bean
  @SuppressWarnings({"rawtypes", "unchecked"})
  ServerBuilderCustomizer apexGrpcServerLimits(
      GrpcProperties p, ThreadPoolExecutor grpcServerWorkers) {
    return builder -> {
      builder.executor(grpcServerWorkers);
      builder.maxInboundMessageSize(p.getMaxPayloadBytes() + 8192);
      builder.maxInboundMetadataSize(8192);
    };
  }
}
