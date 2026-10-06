package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.messaging.consumer.ConsumerFailureStatistics;
import com.apex.platform.messaging.consumer.ConsumerFailureStatus;

import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ConsumerOperationalMetricsCollector {

    private final ConsumerFailureStatistics statistics;

    private final Clock clock;

    private final AtomicLong retrying =
            new AtomicLong();

    private final AtomicLong deadLettered =
            new AtomicLong();

    private final AtomicLong replayFailed =
            new AtomicLong();

    private final AtomicLong unresolved =
            new AtomicLong();

    private final AtomicLong oldestAgeSeconds =
            new AtomicLong();


    public ConsumerOperationalMetricsCollector(
            ConsumerFailureStatistics statistics,
            MeterRegistry registry,
            Clock clock
    ) {

        this.statistics = statistics;
        this.clock = clock;

        registry.gauge(
                "apex.consumer.failure.retrying",
                retrying
        );

        registry.gauge(
                "apex.consumer.failure.deadlettered",
                deadLettered
        );

        registry.gauge(
                "apex.consumer.failure.replay.failed",
                replayFailed
        );

        registry.gauge(
                "apex.consumer.failure.unresolved",
                unresolved
        );

        registry.gauge(
                "apex.consumer.failure.oldest.age.seconds",
                oldestAgeSeconds
        );
    }


    @Scheduled(fixedDelay = 10000)
    public void collect() {

        retrying.set(
                statistics.countByStatus(
                        ConsumerFailureStatus.RETRYING
                )
        );

        deadLettered.set(
                statistics.countByStatus(
                        ConsumerFailureStatus.DEAD_LETTERED
                )
        );

        replayFailed.set(
                statistics.countByStatus(
                        ConsumerFailureStatus.REPLAY_FAILED
                )
        );

        unresolved.set(
                statistics.unresolvedCount()
        );


        Instant oldest =
                statistics.oldestUnresolvedFailureTime();

        if (oldest == null) {

            oldestAgeSeconds.set(0);
            return;
        }

        long age =
                Duration.between(
                                oldest,
                                Instant.now(clock)
                        )
                        .getSeconds();

        oldestAgeSeconds.set(
                Math.max(
                        age,
                        0
                )
        );
    }
}