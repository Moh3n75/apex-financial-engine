package com.apex.platform.grpc.internal;

import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.grpc.api.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Qualifier;
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

  /**
   * Collection deadlines share one dedicated clock across collection calls (the property under
   * test), but must not share the query-local deadline thread — a saturated query timer must not
   * delay collection expiry.
   */
  @Bean(destroyMethod = "shutdown")
  ScheduledExecutorService grpcCollectorDeadlines() {
    var scheduler =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().daemon().name("apex-collection-deadline-", 0).factory());
    scheduler.setRemoveOnCancelPolicy(true);
    return scheduler;
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
      @Qualifier("grpcCollectorDeadlines") ScheduledExecutorService grpcCollectorDeadlines,
      RpcObservation observations) {
    var delegate =
        new DefaultMultiResponderGateway(
            q, c, p, limits, codec, grpcCollectorWorkers, grpcCollectorDeadlines);
    return new MultiResponderGateway() {
      public <R> java.util.concurrent.CompletionStage<CollectionResult<R>> collectQueries(
          Object query, Class<R> type, CollectionOptions options) {
        return observations.watch("collection", () -> delegate.collectQueries(query, type, options));
      }
    };
  }
}
