package com.apex.order.grpc;
import static org.assertj.core.api.Assertions.*;import static org.awaitility.Awaitility.await;
import org.junit.jupiter.api.Test;import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.*;import java.time.*;import java.util.*;import java.net.*;import java.net.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
/** Packaged owner and Console; independent owner fixture SQL, no Console DB access. */
class Ops03ProcessIT {
 final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
 String get(int port,String path)throws Exception {return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+path)).timeout(Duration.ofSeconds(2)).GET().build(),HttpResponse.BodyHandlers.ofString()).body();}
 UUID place(int port,String reference)throws Exception {
 String body="{\"externalReference\":\""+reference+"\",\"description\":\"CANARY_PAYLOAD\",\"items\":[{\"productId\":\""+UUID.randomUUID()+"\",\"quantity\":1}]}";
 var response=http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/api/v1/order-placements")).timeout(Duration.ofSeconds(5)).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),HttpResponse.BodyHandlers.ofString());
 assertThat(response.statusCode()).isEqualTo(201);
 return UUID.fromString(GrpcOrderIT.JSON.readTree(response.body()).path("orderId").asText());
 }
 @Test void packagedMtlsOwnerFactsStalenessAndConsoleLoss()throws Exception {
 Path root=Path.of("..").toAbsolutePath().normalize(),out=root.resolve("order-cell/target/ops03");Files.createDirectories(out);
 Process owner=null,console=null,bad=null;
 try(var db=EmbeddedPostgres.builder().start();var certs=new EphemeralCertificates()){
 int grpc=GrpcOrderIT.freePort(),port=GrpcOrderIT.freePort(),ownerHttp=GrpcOrderIT.freePort();var jdbc=new JdbcTemplate(db.getPostgresDatabase());
 String url=db.getJdbcUrl("postgres","postgres");
 var a=new ArrayList<>(List.of("--spring.profiles.active=grpc","--server.port="+ownerHttp,"--server.address=127.0.0.1","--spring.datasource.url="+url,"--spring.datasource.username=postgres","--spring.datasource.password=","--spring.kafka.listener.auto-startup=false","--spring.kafka.bootstrap-servers=127.0.0.1:1","--apex.messaging.consumer.enabled=false","--apex.messaging.outbox.publisher.enabled=false","--apex.grpc.server.enabled=true","--apex.grpc.cell-id=order-cell","--spring.grpc.server.address=127.0.0.1","--spring.grpc.server.port="+grpc,"--spring.grpc.server.ssl.bundle=m32","--spring.grpc.server.ssl.client-auth=require","--spring.grpc.server.ssl.enabled=true","--spring.grpc.server.reflection.enabled=false","--apex.grpc.security.enabled=true","--apex.grpc.security.tenant=alpha","--apex.grpc.security.peers.ops.uri=spiffe://apex.test/tenant/alpha/cell/m32-client","--apex.grpc.security.peers.ops.contracts=QUERY:OPS_OWNER_SNAPSHOT:1","--apex.grpc.security.peers.observer.uri=spiffe://apex.test/tenant/alpha/cell/m32-observer","--apex.grpc.security.peers.observer.contracts=QUERY:GET_ORDER:1","--apex.ops03.owner-enabled=true","--apex.ops03.environment=test","--apex.ops03.instance=one","--apex.ops03.jdbc-url="+url,"--apex.ops03.jdbc-user=postgres","--apex.ops03.jdbc-password=","--apex.ops03.known-groups=known","--apex.observability.enabled=true","--apex.observability.active-enabled=true","--apex.observability.instance-id=one","--apex.observability.max-active=64"));a.addAll(certs.bundle(certs.server));
 owner=GrpcOrderIT.launch(root.resolve("order-cell/target/ops03-build/order-cell.jar"),out.resolve("owner.log"),a);GrpcOrderIT.ready(owner,out.resolve("owner.log"));
 UUID order=place(ownerHttp,"CANARY_PASSWORD"),receipt=UUID.randomUUID();
 UUID event=jdbc.queryForObject("SELECT event_id FROM outbox_event WHERE aggregate_id=?",UUID.class,order);
 assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?",String.class,order)).isEqualTo("PENDING_INVENTORY");
 // Owner fixture supplies a historical unknown entry time and a committed receipt privacy canary.
 jdbc.update("UPDATE orders SET status_entered_at=NULL WHERE id=?",order);
 jdbc.update("INSERT INTO order_request_receipt(idempotency_key,client_id,tenant_id,principal,contract_type,contract_version,request_id,fingerprint,response,completed_at) VALUES ('CANARY_KEY','CANARY_CLIENT','CANARY_TENANT','CANARY_PRINCIPAL','CREATE_ORDER',1,?,?,?,now())",receipt,"0".repeat(64),"CANARY_RESPONSE".getBytes(java.nio.charset.StandardCharsets.UTF_8));
 var c=new ArrayList<>(List.of("--server.port="+port,"--apex.grpc.cell-id=m32-client","--apex.grpc.client.enabled=true","--apex.grpc.destinations.order-cell.channel=owner","--spring.grpc.client.channel.owner.target=static://127.0.0.1:"+grpc,"--spring.grpc.client.channel.owner.ssl.enabled=true","--spring.grpc.client.channel.owner.ssl.bundle=m32","--apex.ops03.owners=order-cell","--apex.ops03.events="+event+","+receipt,"--apex.ops03.workflows="+order));c.addAll(certs.bundle(certs.primary));
 Path jar=root.resolve("apex-operations-console/target/apex-operations-console-1.0-SNAPSHOT.jar");
 console=GrpcOrderIT.launch(jar,out.resolve("console.log"),c);
 await().ignoreExceptions().atMost(Duration.ofSeconds(45)).untilAsserted(()->assertThat(get(port,"/ops/v1/events/"+event)).contains("PENDING","OWNER_COMMITTED_ROW","UNKNOWN_NO_VERIFIED_RECEIPT"));
 assertThat(get(port,"/ops/v1/workflows/order-cell/order/"+order)).contains("PENDING_INVENTORY").doesNotContain("CANARY_PAYLOAD");
 assertThat(get(port,"/ops/v1/events/"+receipt)).contains("RECEIPT","COMMITTED");
 for(String path:List.of("/ops/v1/events/CANARY_URL_TOKEN","/ops/v1/workflows/CANARY_URL_TOKEN/order/"+order,"/CANARY_URL_TOKEN"))assertThat(get(port,path)).doesNotContain("CANARY_URL_TOKEN");
 String active=get(port,"/ops/v1/operations/active");assertThat(active).contains("RPC_SERVER","PARTIAL_MEMBERSHIP");
 Files.writeString(out.resolve("active.json"),active);
 Files.writeString(out.resolve("event.json"),get(port,"/ops/v1/events/"+event));
 Files.writeString(out.resolve("workflow.json"),get(port,"/ops/v1/workflows/order-cell/order/"+order));
 // Explicit fixture transition: validates current-state propagation, never a Saga execution claim.
 jdbc.update("UPDATE orders SET status='CONFIRMED',status_entered_at=now() WHERE id=?",order);
 jdbc.update("UPDATE outbox_event SET published=true,published_at=now() WHERE event_id=?",event);
 await().ignoreExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(()->assertThat(get(port,"/ops/v1/events/"+event)).contains("PUBLISHED_MARK"));
 assertThat(get(port,"/ops/v1/workflows/order-cell/order/"+order)).contains("CONFIRMED");
 assertThat(get(port,"/ops/v1/overview")).doesNotContain("CANARY_PASSWORD","CANARY_PAYLOAD","CANARY_KEY","CANARY_CLIENT","CANARY_TENANT","CANARY_PRINCIPAL","CANARY_RESPONSE",certs.password);
 var mutation=http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/ops/v1/overview")).POST(HttpRequest.BodyPublishers.noBody()).build(),HttpResponse.BodyHandlers.ofString());assertThat(mutation.statusCode()).isEqualTo(405);
 for(var keys:List.of(certs.observer,certs.wrongSan)){
 int deniedPort=GrpcOrderIT.freePort();var denied=new ArrayList<>(c);denied.removeIf(v->v.startsWith("--server.port=")||v.startsWith("--apex.grpc.cell-id=")||v.startsWith("--spring.ssl.bundle.jks.m32.keystore."));denied.add("--server.port="+deniedPort);denied.add("--apex.grpc.cell-id=m32-observer");
 // Replace key bundle entirely; trust remains test-owned.
 denied.removeIf(v->v.startsWith("--spring.ssl.bundle.jks.m32."));denied.addAll(certs.bundle(keys));
 bad=GrpcOrderIT.launch(jar,out.resolve("denied-"+deniedPort+".log"),denied);
 await().ignoreExceptions().during(Duration.ofSeconds(6)).atMost(Duration.ofSeconds(25)).untilAsserted(()->assertThat(get(deniedPort,"/ops/v1/overview")).contains("MISSING").doesNotContain("OWNER_COMMITTED_ROW"));
 GrpcOrderIT.stop(bad);bad=null;
 }
 String oldEpoch=GrpcOrderIT.JSON.readTree(get(port,"/ops/v1/events/"+event)).path("data").path("sources").get(0).path("snapshot").path("epoch").asText();
 GrpcOrderIT.stop(owner);owner=null;
 await().ignoreExceptions().atMost(Duration.ofSeconds(25)).untilAsserted(()->assertThat(get(port,"/ops/v1/events/"+event)).contains("STALE","PUBLISHED_MARK"));
 owner=GrpcOrderIT.launch(root.resolve("order-cell/target/ops03-build/order-cell.jar"),out.resolve("owner-restarted.log"),a);GrpcOrderIT.ready(owner,out.resolve("owner-restarted.log"));
 await().ignoreExceptions().atMost(Duration.ofSeconds(45)).untilAsserted(()->{
 var view=GrpcOrderIT.JSON.readTree(get(port,"/ops/v1/events/"+event)).path("data").path("sources").get(0);
 assertThat(view.path("freshness").asText()).isEqualTo("FRESH");assertThat(view.path("snapshot").path("epoch").asText()).isNotEqualTo(oldEpoch);
 });
 GrpcOrderIT.stop(console);console=null;
 UUID afterLoss=place(ownerHttp,"after-console-"+UUID.randomUUID());
 assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=?",Integer.class,afterLoss)).isEqualTo(1);
 assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?",String.class,afterLoss)).isEqualTo("PENDING_INVENTORY");
 assertThat(Files.readString(out.resolve("console.log"))).doesNotContain("CANARY_PASSWORD","CANARY_PAYLOAD","CANARY_URL_TOKEN",certs.password);
 assertThat(Files.readString(out.resolve("owner.log"))).doesNotContain("CANARY_PASSWORD","CANARY_PAYLOAD",certs.password);
 jdbc.update("UPDATE orders SET status='CANCELLED' WHERE id=?",order);assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?",String.class,order)).isEqualTo("CANCELLED");
 bad=GrpcOrderIT.launch(jar,out.resolve("nonloopback.log"),List.of("--server.address=0.0.0.0","--server.port=0"));assertThat(bad.waitFor(20,java.util.concurrent.TimeUnit.SECONDS)).isTrue();assertThat(bad.exitValue()).isNotZero();
 Files.writeString(out.resolve("result.txt"),"PASS packaged mTLS owner/Console; fixture-controlled current facts; owner outage stale; owner restart has new epoch; real Order HTTP admission and committed Outbox after Console loss. No live Saga or Kafka qualification.\n");
 }finally{GrpcOrderIT.stop(bad);GrpcOrderIT.stop(console);GrpcOrderIT.stop(owner);}
 }
}
