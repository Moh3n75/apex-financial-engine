package com.apex.platform.grpc.internal;

import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.grpc.api.*;
import java.util.concurrent.*;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = GrpcCommonAutoConfiguration.class)
@ConditionalOnProperty(prefix = "apex.grpc.collector", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(CollectorProperties.class)
public class GrpcCollectorAutoConfiguration {
  @Bean(destroyMethod = "shutdown")
  ThreadPoolExecutor grpcCollectorWorkers() {
    return new ThreadPoolExecutor(
        4,
        4,
        30,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(16),
        Thread.ofPlatform().daemon().name("apex-collection-", 0).factory(),
        new ThreadPoolExecutor.AbortPolicy());
  }

  @Bean
  @ConditionalOnMissingBean(MultiResponderGateway.class)
  MultiResponderGateway multiResponderGateway(
      QueryGateway q,
      ExecutionContextProvider c,
      GrpcProperties p,
      CollectorProperties limits,
      RpcCodec codec,
      ThreadPoolExecutor grpcCollectorWorkers,
      ScheduledExecutorService grpcDeadlines) {
    return new DefaultMultiResponderGateway(
        q, c, p, limits, codec, grpcCollectorWorkers, grpcDeadlines);
  }
}
