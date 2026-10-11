package com.apex.infrastructure.outbox;


import com.apex.platform.messaging.outbox.OutboxStatistics;

import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;


@Component
public class OutboxMetricsCollector {


    private final OutboxStatistics statistics;


    private final AtomicLong pendingGauge =
            new AtomicLong(0);


    private final AtomicLong oldestAgeGauge =
            new AtomicLong(0);



    public OutboxMetricsCollector(
            OutboxStatistics statistics,
            MeterRegistry registry
    ) {


        this.statistics = statistics;


        registry.gauge(
                "apex.outbox.pending.count",
                pendingGauge
        );


        registry.gauge(
                "apex.outbox.oldest.age.seconds",
                oldestAgeGauge
        );

    }



    @Scheduled(
            fixedDelay = 10000
    )
    public void collect() {


        pendingGauge.set(
                statistics.pendingCount()
        );


        Instant oldest =
                statistics.oldestUnpublishedEventTime();


        if(oldest == null) {

            oldestAgeGauge.set(0);

        }
        else {

            oldestAgeGauge.set(
                    Duration
                            .between(
                                    oldest,
                                    Instant.now()
                            )
                            .getSeconds()
            );

        }

    }

}