package com.apex.infrastructure.messaging.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.springframework.stereotype.Component;

import java.util.concurrent.Callable;

@Component
public class ConsumerHandlerMetrics {

    private final Counter success;

    private final Counter failed;

    private final Timer latency;


    public ConsumerHandlerMetrics(
            MeterRegistry registry
    ) {

        this.success =
                Counter.builder(
                                "apex.consumer.handler.success"
                        )
                        .description(
                                "Successfully processed integration events"
                        )
                        .register(registry);


        this.failed =
                Counter.builder(
                                "apex.consumer.handler.failed"
                        )
                        .description(
                                "Failed integration event handler executions"
                        )
                        .register(registry);


        this.latency =
                Timer.builder(
                                "apex.consumer.handler.latency"
                        )
                        .description(
                                "Integration event handler processing latency"
                        )
                        .register(registry);
    }


    public <T> T record(
            Callable<T> operation
    ) {

        try {

            T result =
                    latency.recordCallable(
                            operation
                    );

            success.increment();

            return result;

        } catch (RuntimeException exception) {

            failed.increment();

            throw exception;

        } catch (Exception exception) {

            failed.increment();

            throw new IllegalStateException(
                    exception
            );
        }
    }
}