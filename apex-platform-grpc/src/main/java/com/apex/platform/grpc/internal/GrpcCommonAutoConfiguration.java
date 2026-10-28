package com.apex.platform.grpc.internal;

import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.grpc.api.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;

@AutoConfiguration(after = ApexExecutionContextAutoConfiguration.class)
@Conditional(GrpcEnabled.class)
@EnableConfigurationProperties(GrpcProperties.class)
public class GrpcCommonAutoConfiguration {
  @Bean
  RpcCodec rpcCodec(GrpcProperties p, Environment environment) {
    if (p.getCellId() == null)
      p.setCellId(
          environment.getProperty(
              "apex.cell.id", environment.getProperty("spring.application.name")));
    p.validate();
    return new RpcCodec(p.getMaxPayloadBytes());
  }

  @Bean
  HandlerRegistry rpcHandlerRegistry(ConfigurableListableBeanFactory beans, RpcCodec codec) {
    return new HandlerRegistry(beans);
  }

  @Bean
  RpcDispatcher rpcDispatcher(
      HandlerRegistry r,
      RpcCodec c,
      ExecutionContextProvider p,
      GrpcProperties g,
      ObjectProvider<TransactionalRequestExecutor> e) {
    return new RpcDispatcher(r, c, p, g, e);
  }

  @Bean(destroyMethod = "shutdown")
  ThreadPoolExecutor grpcQueryWorkers() {
    return new ThreadPoolExecutor(
        4,
        4,
        30,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(32),
        Thread.ofPlatform().daemon().name("apex-local-query-", 0).factory(),
        new ThreadPoolExecutor.AbortPolicy());
  }

  @Bean(destroyMethod = "shutdown")
  ScheduledExecutorService grpcDeadlines() {
    return Executors.newSingleThreadScheduledExecutor(
        Thread.ofPlatform().daemon().name("apex-query-deadline-", 0).factory());
  }

  @Bean
  @ConditionalOnMissingBean(QueryGateway.class)
  QueryGateway queryGateway(
      GrpcProperties p,
      HandlerRegistry r,
      RpcDispatcher d,
      RpcCodec c,
      ExecutionContextProvider ctx,
      ObjectProvider<GrpcInvocationClient> remote,
      ThreadPoolExecutor grpcQueryWorkers,
      ScheduledExecutorService grpcDeadlines) {
    return new DefaultQueryGateway(p, r, d, c, ctx, remote, grpcQueryWorkers, grpcDeadlines);
  }
}
