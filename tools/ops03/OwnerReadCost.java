import com.apex.operations.owner.MessagingEvidenceReader;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.*;
import java.util.*;
import java.lang.management.ManagementFactory;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.core.io.ClassPathResource;
/** Exploratory owner-read cost only. No business-path latency or SLA inference. */
class OwnerReadCost {
 public static void main(String[] args)throws Exception {
  try(var pg=EmbeddedPostgres.builder().start()){
   var ds=pg.getPostgresDatabase();
   new ResourceDatabasePopulator(new ClassPathResource("db/platform/V1001__platform_outbox_event.sql"),new ClassPathResource("db/platform/V1002__platform_processed_event.sql")).execute(ds);
   var reader=new MessagingEvidenceReader(Set.of("known"));var ids=List.of(UUID.randomUUID());
   try(var c=ds.getConnection()){
    c.setReadOnly(true);c.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
    for(int i=0;i<50;i++)reader.read(c,ids);
    var bean=ManagementFactory.getThreadMXBean();var allocation=(com.sun.management.ThreadMXBean)bean;
    long tid=Thread.currentThread().threadId();long cpu=bean.getCurrentThreadCpuTime(),bytes=allocation.getThreadAllocatedBytes(tid);
    long[] nanos=new long[200];for(int i=0;i<nanos.length;i++){long start=System.nanoTime();reader.read(c,ids);nanos[i]=System.nanoTime()-start;}
    cpu=bean.getCurrentThreadCpuTime()-cpu;bytes=allocation.getThreadAllocatedBytes(tid)-bytes;Arrays.sort(nanos);
    String result="samples,warmup,keys,knownGroups,p50Micros,p95Micros,p99Micros,currentThreadCpuNanos,currentThreadAllocatedBytes\n"+nanos.length+",50,1,1,"+nanos[99]/1000+","+nanos[189]/1000+","+nanos[197]/1000+","+cpu+","+bytes+"\n";
    Files.writeString(Path.of(args[0]),result);
   }
  }
 }
}
