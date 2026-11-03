package com.apex.observability;

import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.boot.autoconfigure.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/** Disposable packaged acceptance ONLY. This class is absent from the SDK main JAR. */
@AutoConfiguration(after=ObservabilityAutoConfiguration.class)
@ConditionalOnProperty(name="ops02.fixture.enabled",havingValue="true")
public class FixtureExportConfiguration {
  @Bean ObservationBatchExporter fixtureExporter(Environment env){
    String mode=env.getRequiredProperty("ops02.fixture.mode");
    if(!java.util.Set.of("healthy","failing").contains(mode))throw new IllegalArgumentException("Invalid isolated fixture mode");
    return batch->{if(mode.equals("failing"))throw new IllegalStateException("isolated export outage");};
  }
  @Bean(destroyMethod="shutdownNow") ScheduledExecutorService fixtureReceipt(Environment env,BoundedOperationalObserver observer,ActiveOperations active){
    Path file=Path.of(env.getRequiredProperty("ops02.fixture.receipt"));
    var writer=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("ops02-test-receipt").factory());
    writer.scheduleWithFixedDelay(()->{
      try{var s=observer.statistics();var values=new Properties();
        values.setProperty("pid",Long.toString(ProcessHandle.current().pid()));
        values.setProperty("mode",env.getRequiredProperty("ops02.fixture.mode"));
        values.setProperty("admitted",Long.toString(s.admitted()));values.setProperty("dropped",Long.toString(s.dropped()));
        values.setProperty("exportFailures",Long.toString(s.exportFailures()));
        values.setProperty("maxRecords",Integer.toString(s.maxRecordsObserved()));values.setProperty("maxAccountedBytes",Integer.toString(s.maxBytesObserved()));
        values.setProperty("active",Integer.toString(active.size()));values.setProperty("expired",Long.toString(active.expiredCount()));
        values.setProperty("observedAt",java.time.Instant.now().toString());
        try(var out=Files.newOutputStream(file)){values.store(out,"Isolated aggregate diagnostics; no business payload");}
      }catch(Exception ignored){/* test receipts never affect a Cell */}
    },0,1,TimeUnit.SECONDS);return writer;
  }
}
