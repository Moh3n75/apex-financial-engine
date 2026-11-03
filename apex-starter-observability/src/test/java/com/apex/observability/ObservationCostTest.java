package com.apex.observability;

import static org.assertj.core.api.Assertions.*;
import io.micrometer.observation.*;
import java.lang.management.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;

/** Local admission experiment, not a Saga latency or capacity benchmark. */
class ObservationCostTest {
  @Test void measuredAdmissionABACAStaysBoundedWithoutBusinessErrors() throws Exception {
    var lines=new ArrayList<String>();
    lines.add("round,mode,samples,p50_ns,p95_ns,p99_ns,ops_per_second,thread_cpu_ns_per_op,allocated_bytes_per_op,max_records,max_accounted_bytes,drops,export_failures");
    for(int round=0;round<3;round++)for(String mode:List.of("disabled","healthy","disabled","failing","disabled")){
      var p=new ObservabilityProperties();p.setEnabled(true);p.setActiveEnabled(true);p.setExportEnabled(true);
      p.setExportInterval(Duration.ofMillis(1));var active=new ActiveOperations(p);
      try(var observer=new BoundedOperationalObserver(p,null,b->{if(mode.equals("failing"))throw new IllegalStateException("isolated");})){
        var registry=mode.equals("disabled")?ObservationRegistry.NOOP:ObservationRegistry.create();
        if(!mode.equals("disabled"))registry.observationConfig().observationHandler(new OperationalObservationHandler(observer,active));
        for(int i=0;i<3000;i++)Observation.start("apex.query",registry).stop();
        int count=30000;long[] times=new long[count];
        var bean=ManagementFactory.getThreadMXBean();var allocations=bean instanceof com.sun.management.ThreadMXBean b?b:null;
        long thread=Thread.currentThread().threadId();long bytes=allocations==null?-1:allocations.getThreadAllocatedBytes(thread);
        long cpu=bean.isCurrentThreadCpuTimeSupported()?bean.getCurrentThreadCpuTime():-1;long start=System.nanoTime();
        for(int i=0;i<count;i++){long before=System.nanoTime();Observation.start("apex.query",registry).stop();times[i]=System.nanoTime()-before;}
        long elapsed=System.nanoTime()-start;long cpuEnd=bean.isCurrentThreadCpuTimeSupported()?bean.getCurrentThreadCpuTime():-1;
        long endBytes=allocations==null?-1:allocations.getThreadAllocatedBytes(thread);Arrays.sort(times);var s=observer.statistics();
        assertThat(s.maxRecordsObserved()).isLessThanOrEqualTo(p.getMaxRecords());
        assertThat(s.maxBytesObserved()).isLessThanOrEqualTo(p.getMaxBytes());assertThat(active.size()).isZero();
        if(!mode.equals("disabled"))assertThat(s.admitted()).isPositive();
        lines.add(String.join(",",Integer.toString(round),mode,Integer.toString(count),Long.toString(times[count/2]),
          Long.toString(times[(int)(count*.95)]),Long.toString(times[(int)(count*.99)]),Double.toString(count*1e9/elapsed),
          Double.toString(cpu<0?-1:(cpuEnd-cpu)/(double)count),Double.toString(bytes<0?-1:(endBytes-bytes)/(double)count),
          Integer.toString(s.maxRecordsObserved()),Integer.toString(s.maxBytesObserved()),Long.toString(s.dropped()),Long.toString(s.exportFailures())));
      }
    }
    Path file=Path.of(System.getProperty("ops02.measurement.file","target/ops02-admission.csv"));
    Files.createDirectories(file.toAbsolutePath().getParent());Files.write(file,lines);
  }
}
