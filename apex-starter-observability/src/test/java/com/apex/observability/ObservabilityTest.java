package com.apex.observability;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class ObservabilityTest {
    @Test void nativeUnnamedSchedulingObservationNeverBreaksTheScheduledWork() throws Exception {
        var p=policy();p.setActiveEnabled(true);var active=new ActiveOperations(p);
        try(var observer=new BoundedOperationalObserver(p,null,null)){
            var handler=new OperationalObservationHandler(observer,active);var registry=ObservationRegistry.create();
            registry.observationConfig().observationHandler(handler);var called=new AtomicInteger();
            var task=new org.springframework.scheduling.support.ScheduledMethodRunnable(called,
                AtomicInteger.class.getMethod("incrementAndGet"),null,()->registry);
            assertThatCode(task::run).doesNotThrowAnyException();assertThat(called.get()).isEqualTo(1);
            assertThat(active.size()).isZero();assertThat(observer.drain(64)).isEmpty();
            assertThat(handler.supportsContext(null)).isFalse();
            assertThat(handler.supportsContext(new Observation.Context())).isFalse();
        }
    }
    private final ApplicationContextRunner context=new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ObservabilityAutoConfiguration.class));
    static ObservabilityProperties policy(){var p=new ObservabilityProperties();p.setEnabled(true);return p;}
    static OperationalObserver.Sample sample(){return new OperationalObserver.Sample(OperationalObserver.Operation.QUERY,OperationalObserver.Outcome.OBSERVED,1);}
    @Test void disabledDoesNotCreateObserversOrWorkers(){context.run(c->{assertThat(c).doesNotHaveBean(OperationalObserver.class).doesNotHaveBean(ActiveOperations.class);});}
    @Test void enabledWithoutExporterBootsOffline(){context.withPropertyValues("apex.observability.enabled=true").run(c->{
        assertThat(c).hasSingleBean(OperationalObserver.class).hasSingleBean(ActiveOperations.class);
        assertThat(c.getBean(OperationalObserver.class).tryRecord(sample())).isTrue();
        assertThat(c).doesNotHaveBean(ObservationBatchExporter.class);
    });}
    @Test void explicitExportWithoutAdapterDoesNotBecomeStartupDependency(){context.withPropertyValues(
        "apex.observability.enabled=true","apex.observability.export-enabled=true").run(c->assertThat(c).hasNotFailed());}
    @Test void bothCountAndByteBudgetsDropNew(){var p=policy();p.setMaxRecords(4);p.setBatchSize(4);p.setMaxBytes(512);p.setMaxRecordBytes(512);
        try(var o=new BoundedOperationalObserver(p,null,null)){
            assertThat(o.tryRecord(sample())).isTrue();assertThat(o.tryRecord(sample())).isTrue();assertThat(o.tryRecord(sample())).isFalse();
            assertThat(o.statistics().maxRecordsObserved()).isEqualTo(2);assertThat(o.statistics().maxBytesObserved()).isEqualTo(512);
            assertThat(o.dropped(BoundedOperationalObserver.Drop.FULL)).isEqualTo(1);assertThat(o.drain(4)).hasSize(2);
        }
    }
    @Test void oversizedAndClosedAdmissionsNeverThrow(){var p=policy();p.setMaxRecordBytes(32);
        var o=new BoundedOperationalObserver(p,null,null);assertThat(o.tryRecord(sample())).isFalse();
        assertThat(o.dropped(BoundedOperationalObserver.Drop.OVERSIZE)).isEqualTo(1);o.close();assertThat(o.tryRecord(sample())).isFalse();}
    @Test void boundedActiveExpiryIsNotCompletionAndEpochsDiffer(){var p=policy();p.setActiveEnabled(true);p.setMaxActive(1);
        var clock=new AtomicLong();var a=new ActiveOperations(p,clock::get);var h=a.tryRegister(OperationalObserver.Operation.QUERY);
        var first=a.snapshot().getFirst();a.tryRegister(OperationalObserver.Operation.QUERY);
        assertThat(a.size()).isEqualTo(1);assertThat(a.rejectedCount()).isEqualTo(1);
        clock.addAndGet(p.getActiveTtl().toNanos());assertThat(a.snapshot()).isEmpty();assertThat(a.expiredCount()).isEqualTo(1);
        h.close();a.tryRegister(OperationalObserver.Operation.QUERY);assertThat(a.size()).isEqualTo(1);
        var b=new ActiveOperations(p,clock::get);b.tryRegister(OperationalObserver.Operation.QUERY);
        assertThat(b.snapshot().getFirst().bootEpoch()).isNotEqualTo(first.bootEpoch());
    }
    @Test void throwingObserverCannotRetainActiveSlot(){var p=policy();p.setActiveEnabled(true);var active=new ActiveOperations(p);
        var handler=new OperationalObservationHandler(s->{throw new IllegalStateException("synthetic-secret");},active);
        var registry=ObservationRegistry.create();registry.observationConfig().observationHandler(handler);
        var observation=Observation.start("apex.query",registry);assertThat(active.size()).isEqualTo(1);
        assertThatCode(observation::stop).doesNotThrowAnyException();assertThat(active.size()).isZero();}
    @Test void unsupportedMetadataAndCanaryTagsAreNotArchivedOrMetered(){var p=policy();var active=new ActiveOperations(p);
        var meters=new SimpleMeterRegistry();
        meters.config().meterFilter(new ObservabilityAutoConfiguration().apexOperationalMetricBoundary());
        try(var observer=new BoundedOperationalObserver(p,meters,null)){
            var registry=ObservationRegistry.create();registry.observationConfig().observationHandler(new OperationalObservationHandler(observer,active));
            var s=Observation.start("apex.query",registry).lowCardinalityKeyValue("tenant_id","CANARY_SECRET")
                .highCardinalityKeyValue("payload","CANARY_PRIVATE_BODY");s.stop();
            meters.counter("apex.query","tenant_id","CANARY_SECRET").increment();
            assertThat(meters.getMeters().toString()).doesNotContain("CANARY");
            assertThat(observer.drain(p.getMaxRecords()).toString()).doesNotContain("CANARY");
            Observation.start("unknown.CANARY",registry).stop();assertThat(observer.statistics().queuedRecords()).isZero();
        }finally{meters.close();}
    }
    @Test void exporterFailureIsDroppedWithoutChangingBusinessCall(){var p=policy();p.setExportEnabled(true);p.setExportInterval(Duration.ofMillis(5));
        try(var observer=new BoundedOperationalObserver(p,null,b->{throw new IllegalStateException("CANARY_EXPORTER_SECRET");})){
            assertThat(observer.tryRecord(sample())).isTrue();await().atMost(Duration.ofSeconds(3)).until(()->observer.statistics().exportFailures()>0);
            assertThat(observer.dropped(BoundedOperationalObserver.Drop.EXPORT_FAILURE)).isPositive();
        }
    }
    @Test void exporterTimeoutOpensCircuitAndCloseHasFiniteBudget() throws Exception {
        var p=policy();p.setExportEnabled(true);p.setExportInterval(Duration.ofMillis(5));p.setExportTimeout(Duration.ofMillis(20));
        p.setShutdownTimeout(Duration.ofMillis(50));var invocations=new AtomicInteger();var entered=new CountDownLatch(1);
        var release=new CountDownLatch(1);
        var observer=new BoundedOperationalObserver(p,null,b->{invocations.incrementAndGet();entered.countDown();release.await();});
        try{observer.tryRecord(sample());assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(3)).until(()->observer.statistics().circuitOpen());
            assertThat(observer.tryRecord(sample())).isFalse();assertThat(invocations.get()).isEqualTo(1);
            var closed=CompletableFuture.runAsync(observer::close);closed.get(2,TimeUnit.SECONDS);
        }finally{release.countDown();observer.close();}
    }
    @Test void concurrentAdmissionsNeverExceedCapacity() throws Exception {var p=policy();p.setMaxRecords(16);p.setBatchSize(8);
        try(var observer=new BoundedOperationalObserver(p,null,null);var workers=Executors.newFixedThreadPool(8)){
            var tasks=new ArrayList<Future<?>>();for(int i=0;i<8;i++)tasks.add(workers.submit(()->{for(int n=0;n<1000;n++)observer.tryRecord(sample());}));
            for(var task:tasks)task.get(3,TimeUnit.SECONDS);
            assertThat(observer.statistics().queuedRecords()).isLessThanOrEqualTo(16);assertThat(observer.statistics().dropped()).isPositive();
        }
    }
    @Test void invalidResourceBoundsFailLocally(){context.withPropertyValues("apex.observability.enabled=true","apex.observability.max-records=0").run(c->assertThat(c).hasFailed());}

    @Test void bootRegistersTheHandlerOnceAndReusesNativeMeters(){
        context.withConfiguration(AutoConfigurations.of(org.springframework.boot.micrometer.observation.autoconfigure.ObservationAutoConfiguration.class))
            .withPropertyValues("apex.observability.enabled=true","apex.observability.active-enabled=true")
            .run(c->{var registry=c.getBean(ObservationRegistry.class);var observation=Observation.start("apex.query",registry);
                assertThat(c.getBean(ActiveOperations.class).size()).isEqualTo(1);observation.stop();
                assertThat(c.getBean(ActiveOperations.class).size()).isZero();
                assertThat(c.getBean(BoundedOperationalObserver.class).drain(64)).hasSize(1);
                assertThat(c).hasSingleBean(OperationalObservationHandler.class);});
    }

    @Test void realSocketSinkAndUnreachableSinkStayOffBusinessThread() throws Exception {
        var server=com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(),0),0);
        var received=new java.util.concurrent.LinkedBlockingQueue<String>();
        server.createContext("/diagnostics",exchange->{received.offer(new String(exchange.getRequestBody().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));
            exchange.sendResponseHeaders(204,-1);exchange.close();});server.start();
        var p=policy();p.setExportEnabled(true);p.setExportInterval(Duration.ofMillis(5));
        try(var client=java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofMillis(100)).build();
            var observer=new BoundedOperationalObserver(p,null,batch->{
                assertThat(Thread.currentThread().getName()).startsWith("apex-observation-send-");
                client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+server.getAddress().getPort()+"/diagnostics"))
                    .timeout(Duration.ofMillis(200)).POST(java.net.http.HttpRequest.BodyPublishers.ofString(batch.toString())).build(),java.net.http.HttpResponse.BodyHandlers.discarding());
            })){
            assertThat(observer.tryRecord(sample())).isTrue();assertThat(received.poll(3,TimeUnit.SECONDS)).contains("QUERY");
            server.stop(0);observer.tryRecord(sample());
            await().atMost(Duration.ofSeconds(3)).until(()->observer.statistics().exportFailures()>0);
        }finally{server.stop(0);}
    }
}
