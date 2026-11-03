package com.apex.inventory;

import static com.apex.inventory.OrderInventorySagaIT.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.net.http.HttpClient;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/** Matched modest traffic, not capacity or production SLA. Original M30 assertions remain separate gates. */
@Tag("integration")
class Ops03dRecentLoadIT {
    record Sample(UUID id,long acceptedAt,boolean declined,boolean rejected) {}
    @Test void matchedDisabledEnabledAndConsoleOutageWindowsPreserveBusinessTruth() throws Exception {
        Path root=Path.of(System.getProperty("saga.repositoryRoot")).toAbsolutePath().normalize();
        var policy=JSON.readTree(Files.readString(root.resolve("tools/ops03d/experimental-policy.json")));
        assertThat(policy.path("comparativeWindows").path("requestsPerWindow").asInt()).isEqualTo(30);
        var ops=new Ops03dRecentDiscoveryIT();ops.root=root;
        var streams=List.of(ORDER_TOPIC,INVENTORY_TOPIC,Ops03dRecentDiscoveryIT.REQUEST,Ops03dRecentDiscoveryIT.RELEASE,Ops03dRecentDiscoveryIT.PAYMENT,"apex.order.legacy.events.v1");
        var topics=new ArrayList<String>();for(String topic:streams){topics.add(topic);topics.add(topic+"-dlt");}
        var broker=new EmbeddedKafkaKraftBroker(1,1,topics.toArray(String[]::new));broker.brokerProperty("auto.create.topics.enable","false");broker.brokerListProperty("apex.ops03d.load.brokers");
        Process order=null,inventory=null,payment=null;var results=new ArrayList<Map<String,Object>>();
        Path output=root.resolve("out/ops03d");Files.createDirectories(output);
        try(var certs=new Ops03bCertificates();var odb=durable();var idb=durable();var pdb=durable();var http=HttpClient.newHttpClient()) {
            ops.certs=certs;ops.apiClient=http;
            try {
                broker.afterPropertiesSet();String brokers=broker.getBrokersAsString();int op=freePort(),ip=freePort();
                String og="ops03d-order-"+UUID.randomUUID(),ig="ops03d-inventory-"+UUID.randomUUID(),pg="ops03d-payment-"+UUID.randomUUID();
                order=ops.launch(root.resolve("order-cell/target/order-cell.jar"),odb,brokers,og,op,output.resolve("load-order.log"),ORDER_TOPIC,INVENTORY_TOPIC+","+Ops03dRecentDiscoveryIT.PAYMENT,"--spring.profiles.active=saga,three-cell-saga");
                inventory=ops.launch(root.resolve("inventory-cell/target/inventory-cell.jar"),idb,brokers,ig,ip,output.resolve("load-inventory.log"),INVENTORY_TOPIC,ORDER_TOPIC+","+Ops03dRecentDiscoveryIT.RELEASE,"--spring.profiles.active=saga,three-cell-saga");
                payment=ops.launch(root.resolve("payment-cell/target/payment-cell.jar"),pdb,brokers,pg,0,output.resolve("load-payment.log"),Ops03dRecentDiscoveryIT.PAYMENT,Ops03dRecentDiscoveryIT.REQUEST,"--spring.profiles.active=three-cell-saga,simulation","--apex.messaging.consumer.reliability.max-attempts=10");
                ready(order,output.resolve("load-order.log"),http,op);ready(inventory,output.resolve("load-inventory.log"),http,ip);Ops03dRecentDiscoveryIT.paymentReady(payment,output.resolve("load-payment.log"));
                var orders=new JdbcTemplate(odb.getPostgresDatabase());var stock=new JdbcTemplate(idb.getPostgresDatabase());var pay=new JdbcTemplate(pdb.getPostgresDatabase());var dbs=List.of(orders,stock,pay);
                for(var db:dbs){Ops03dRecentDiscoveryIT.grantReader(db);ops.prepareRetained(db);}
                UUID product=UUID.randomUUID();stock.update("INSERT INTO inventory_stock VALUES (?,10000,0)",product);
                long expectedReserved=0;
                try(var admin=Admin.create(Map.of("bootstrap.servers",brokers))) {
                    for(int window=0;window<8;window++) {
                        String mode=policy.path("comparativeWindows").path("sequence").get(window).asText();
                        stop(ops.console);ops.console=null;
                        if(mode.equals("permission-revoked"))for(var entry:Map.of(orders,"orders",stock,"inventory_reservations",pay,"payment_authorizations").entrySet()) {
                            String key=entry.getValue().equals("orders")?"id":"order_id";
                            entry.getKey().execute("REVOKE SELECT("+key+",status,created_at) ON "+entry.getValue()+" FROM ops03d_reader");
                            assertThat(entry.getKey().queryForObject("SELECT has_column_privilege('ops03d_reader',?,?,'SELECT')",Boolean.class,entry.getValue(),key)).isFalse();
                        }
                        if(mode.equals("enabled")||mode.equals("permission-revoked"))ops.startSearchConsole(certs.primary,"m32-client");
                        if(mode.equals("disabled"))for(var entry:Map.of(orders,"orders",stock,"inventory_reservations",pay,"payment_authorizations").entrySet())entry.getKey().execute("GRANT SELECT("+(entry.getValue().equals("orders")?"id":"order_id")+",status,created_at) ON "+entry.getValue()+" TO ops03d_reader");
                        var queries=new CopyOnWriteArrayList<Double>();var quality=new ConcurrentHashMap<String,AtomicInteger>();
                        var polling=Executors.newSingleThreadScheduledExecutor();
                        if(!mode.equals("disabled"))polling.scheduleWithFixedDelay(()->{
                            for(var grant:Map.of("order-cell","order","inventory-cell","reservation","payment-cell","authorization-simulation").entrySet()) {
                                long before=System.nanoTime();
                                try {
                                    var value=JSON.readTree(ops.search("/ops/v1/recent/workflows",grant.getKey(),grant.getValue()));
                                    quality.computeIfAbsent(value.path("availability").asText(),k->new AtomicInteger()).incrementAndGet();
                                    assertThat(value.path("page").path("candidatesExamined").asInt()).isLessThanOrEqualTo(16);
                                }catch(Exception unavailable){quality.computeIfAbsent("HTTP_UNAVAILABLE",k->new AtomicInteger()).incrementAndGet();}
                                finally {queries.add((System.nanoTime()-before)/1e6);}
                            }
                        },0,1,TimeUnit.SECONDS);
                        var pending=new ConcurrentHashMap<UUID,Sample>();var saga=new ConcurrentHashMap<UUID,Double>();var samples=new ArrayList<Sample>();
                        var running=new AtomicBoolean(true);var failure=new AtomicReference<Throwable>();
                        var monitor=Executors.newSingleThreadExecutor();var terminal=monitor.submit(()->{
                            while(running.get()||!pending.isEmpty()) {
                                try {
                                    for(var sample:pending.values()) {String state=status(orders,sample.id());if(state.equals("CONFIRMED")||state.equals("CANCELLED")){saga.put(sample.id(),(System.nanoTime()-sample.acceptedAt())/1e6);pending.remove(sample.id());}}
                                    Thread.sleep(25);
                                }catch(Throwable error){failure.compareAndSet(null,error);return;}
                            }
                        });
                        var httpTimes=new ArrayList<Double>();var beforeGc=gc(List.of(order,inventory,payment),ops.children);var beforeDb=dbCounters(dbs);var beforePgCpu=pgCpu(dbs);long cpu=cpu(List.of(order,inventory,payment));long begin=System.nanoTime();
                        try {
                            for(int n=0;n<30;n++) {
                                long due=begin+n*1_000_000_000L/3;while(System.nanoTime()<due)LockSupport.parkNanos(Math.min(1_000_000L,due-System.nanoTime()));
                                boolean rejected=n%10==0,declined=!rejected&&n%7==0;long started=System.nanoTime();
                                UUID id=UUID.fromString(post(http,op,List.of(item(product,rejected?100000:1))).path("orderId").asText());
                                httpTimes.add((System.nanoTime()-started)/1e6);var sample=new Sample(id,System.nanoTime(),declined,rejected);samples.add(sample);pending.put(id,sample);
                                if(!rejected)pay.update("INSERT INTO payment_simulation_policy VALUES (?,?)",id,declined?"DECLINED":"AUTHORIZED");
                            }
                            running.set(false);terminal.get(30,TimeUnit.SECONDS);assertThat(failure.get()).isNull();assertThat(pending).isEmpty();assertThat(saga).hasSize(30);
                            long elapsed=System.nanoTime()-begin;
                            for(var sample:samples) {
                                assertThat(status(orders,sample.id())).isEqualTo(sample.declined()||sample.rejected()?"CANCELLED":"CONFIRMED");
                                assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,sample.id())).isEqualTo(sample.rejected()?"REJECTED":sample.declined()?"RELEASED":"RESERVED");
                                if(sample.rejected())assertThat(count(pay,"payment_authorizations","order_id",sample.id())).isZero();
                                else {assertThat(count(pay,"payment_authorizations","order_id",sample.id())).isEqualTo(1);assertThat(pay.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?",String.class,sample.id())).isEqualTo(sample.declined()?"DECLINED":"AUTHORIZED");}
                                if(!sample.declined()&&!sample.rejected())expectedReserved++;
                            }
                            assertStock(stock,product,(int)(10000-expectedReserved),(int)expectedReserved);
                            await().atMost(Duration.ofSeconds(20)).untilAsserted(()->{for(var db:dbs){assertThat(db.queryForObject("SELECT count(*) FROM outbox_event WHERE NOT published OR dead_letter",Long.class)).isZero();}});
                            Ops03dRecentDiscoveryIT.assertNoDlt(admin,streams);
                            var endOffsets=new HashMap<TopicPartition,OffsetSpec>();for(String topic:streams)endOffsets.put(new TopicPartition(topic,0),OffsetSpec.latest());
                            var ends=admin.listOffsets(endOffsets).all().get(10,TimeUnit.SECONDS);
                            for(var entry:Map.of(og,List.of(INVENTORY_TOPIC,Ops03dRecentDiscoveryIT.PAYMENT),ig,List.of(ORDER_TOPIC,Ops03dRecentDiscoveryIT.RELEASE),pg,List.of(Ops03dRecentDiscoveryIT.REQUEST)).entrySet()) {
                                var offsets=admin.listConsumerGroupOffsets(entry.getKey()).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS);
                                for(String topic:entry.getValue()){var tp=new TopicPartition(topic,0);assertThat(offsets.get(tp).offset()).isEqualTo(ends.get(tp).offset());}
                            }
                            var row=new LinkedHashMap<String,Object>();row.put("window",window);row.put("mode",mode);row.put("accepted",samples.size());row.put("completed",saga.size());row.put("httpMs",quantiles(httpTimes));row.put("terminalSagaObservedMs",quantiles(new ArrayList<>(saga.values())));
                            row.put("acceptedToDrainedRps",30/(elapsed/1e9));row.put("businessCpuNanos",cpu(List.of(order,inventory,payment))-cpu);row.put("callerSearchMs",quantiles(queries));row.put("searchQuality",quality.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().get())));
                            row.put("dbBefore",beforeDb);row.put("dbAfter",dbCounters(dbs));row.put("dbCpu",pgCpuDelta(beforePgCpu,pgCpu(dbs)));row.put("gcBefore",beforeGc);row.put("gcAfter",gc(List.of(order,inventory,payment),ops.children));row.put("stockReserved",expectedReserved);row.put("invariants","PASS_OWNER_ROWS_STOCK_OUTBOX_OFFSETS_NO_DLT_SELECTED_INBOX_SEPARATE");results.add(row);
                            Files.writeString(output.resolve("ops03d-load-evidence.json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of("policySha",sha(root.resolve("tools/ops03d/experimental-policy.json")),"windows",results,"productionSla","INCONCLUSIVE","projection","BLOCKED_UNMEASURED_COMPLETE_SOURCE_RISK_AND_RECONCILIATION_BUDGET","hostInterference","Shared Windows host; fixture owner SQL polling in every mode; no confidence interval","notMeasured",List.of("whole-process allocation","DB process IO bytes and complete lifetime CPU","server pool wait distributions","production TLS and max owner cardinality"))));
                        }finally{running.set(false);polling.shutdownNow();assertThat(polling.awaitTermination(5,TimeUnit.SECONDS)).isTrue();monitor.shutdownNow();monitor.awaitTermination(5,TimeUnit.SECONDS);}
                    }
                }
                for(int n:List.of(1,5)) {
                    double baseline=Math.max(((Number)((Map<?,?>)results.get(n-1).get("httpMs")).get("p99")).doubleValue(),((Number)((Map<?,?>)results.get(n+2).get("httpMs")).get("p99")).doubleValue());
                    double enabled=((Number)((Map<?,?>)results.get(n).get("httpMs")).get("p99")).doubleValue();
                    results.get(n).put("experimentalHttpP99Ratio",enabled/baseline);results.get(n).put("experimentalHttpRatioGate",enabled/baseline<=2?"WITHIN_EXPERIMENTAL_GATE":"EXCEEDED_NO_PROJECTION");
                    double sagaBaseline=Math.max(((Number)((Map<?,?>)results.get(n-1).get("terminalSagaObservedMs")).get("p99")).doubleValue(),((Number)((Map<?,?>)results.get(n+2).get("terminalSagaObservedMs")).get("p99")).doubleValue());
                    double sagaEnabled=((Number)((Map<?,?>)results.get(n).get("terminalSagaObservedMs")).get("p99")).doubleValue();
                    results.get(n).put("experimentalSagaP99Ratio",sagaEnabled/sagaBaseline);results.get(n).put("experimentalSagaRatioGate",sagaEnabled/sagaBaseline<=2?"WITHIN_EXPERIMENTAL_GATE":"EXCEEDED_NO_PROJECTION");
                }
                Files.writeString(output.resolve("ops03d-load-windows.json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(results));
            }finally {
                try{stop(ops.console);}finally{try{stop(payment);}finally{try{stop(inventory);}finally{stop(order);broker.destroy();}}}
                Files.writeString(output.resolve("load-cleanup.json"),JSON.writeValueAsString(Map.of("pids",ops.children.stream().map(Process::pid).toList(),"allStopped",ops.children.stream().noneMatch(Process::isAlive))));
            }
        }
    }
    static EmbeddedPostgres durable()throws Exception{return EmbeddedPostgres.builder().setServerConfig("fsync","on").setServerConfig("synchronous_commit","on").setServerConfig("full_page_writes","on").start();}
    static long cpu(List<Process> processes){return processes.stream().mapToLong(p->p.info().totalCpuDuration().orElse(Duration.ZERO).toNanos()).sum();}
    static List<Map<String,Object>> dbCounters(List<JdbcTemplate> dbs){return dbs.stream().map(db->db.queryForMap("SELECT blks_read,blks_hit,blk_read_time,blk_write_time,deadlocks,xact_commit,xact_rollback,numbackends,(SELECT count(*) FROM pg_locks WHERE NOT granted) AS lock_waits,(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active') AS active_statements FROM pg_stat_database WHERE datname=current_database()")).toList();}
    static Map<Long,Long> pgCpu(List<JdbcTemplate> dbs){var values=new HashMap<Long,Long>();for(var db:dbs)for(Long pid:db.queryForList("SELECT pid::bigint FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' LIMIT 32",Long.class))ProcessHandle.of(pid).flatMap(p->p.info().totalCpuDuration()).ifPresent(cpu->values.put(pid,cpu.toNanos()));return values;}
    static Map<String,Object> pgCpuDelta(Map<Long,Long> before,Map<Long,Long> after){long delta=0;int paired=0;for(var e:after.entrySet())if(before.containsKey(e.getKey())){paired++;delta+=Math.max(0,e.getValue()-before.get(e.getKey()));}return Map.of("observedCpuNanos",delta,"pairedBackends",paired,"beforeBackends",before.size(),"afterBackends",after.size(),"coverage","PARTIAL_LIVE_BACKEND_INTERSECTION_NO_POSTMASTER_OR_EXITED_CPU");}
    static List<Map<String,Object>> gc(List<Process> processes,List<Process> owned)throws Exception {
        var result=new ArrayList<Map<String,Object>>();for(var process:processes){var probe=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","jstat.exe").toString(),"-gcutil",Long.toString(process.pid()),"1","1").redirectErrorStream(true).start();owned.add(probe);if(!probe.waitFor(2,TimeUnit.SECONDS)){probe.destroyForcibly();probe.waitFor(2,TimeUnit.SECONDS);result.add(Map.of("pid",process.pid(),"coverage","UNAVAILABLE"));}else {String text=new String(probe.getInputStream().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8);result.add(Map.of("pid",process.pid(),"exit",probe.exitValue(),"snapshot",text.length()>2048?"OVERSIZED_UNAVAILABLE":text));}}return result;
    }
    static Map<String,Object> quantiles(List<Double> values){if(values.isEmpty())return Map.of("samples",0,"coverage","NOT_MEASURED");var sorted=new ArrayList<>(values);Collections.sort(sorted);return Map.of("samples",sorted.size(),"p50",sorted.get((int)Math.ceil(sorted.size()*.50)-1),"p95",sorted.get((int)Math.ceil(sorted.size()*.95)-1),"p99",sorted.get((int)Math.ceil(sorted.size()*.99)-1));}
}
