package com.apex.discovery.grpc;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import com.apex.platform.discovery.*;
import com.apex.platform.grpc.api.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.*;
import org.springframework.context.annotation.*;
import org.springframework.core.env.StandardEnvironment;

/** Real Boot client/server HTTP/2 + mTLS, no explicit destination, Boot channel or target map. */
class DynamicCellChannelsTest {
  @TempDir static Path material;
  static String password;
  static final AtomicReference<List<CellEndpoint>> endpoints=new AtomicReference<>(List.of());
  static final AtomicBoolean fresh=new AtomicBoolean(true);
  static final AtomicInteger calls=new AtomicInteger();

  @CellQuery(type="DYNAMIC_PROBE",version=1,response=Answer.class)
  public record Probe(int delay) {}
  @CellQuery(type="DENIED_PROBE",version=1,response=Answer.class)
  public record Denied() {}
  @CellRequest(type="DYNAMIC_WRITE",version=1,response=Answer.class)
  public record Write(String value) {}
  public record Answer(String replica,UUID requestId) {}
  public static class Handler {
    private final org.springframework.core.env.Environment env;
    Handler(org.springframework.core.env.Environment env){this.env=env;}
    @CellQueryHandler(type="DYNAMIC_PROBE",version=1)
    public Answer probe(Probe probe,RequestContext request) throws InterruptedException {
      calls.incrementAndGet(); if(probe.delay()>0)Thread.sleep(probe.delay());
      return new Answer(env.getRequiredProperty("test.replica"),request.requestId());
    }
    @CellRequestHandler(type="DYNAMIC_WRITE",version=1)
    public Answer write(Write input,RequestContext request) {
      if(input.value().equals("unavailable"))throw new GatewayException(GatewayException.Code.UNAVAILABLE,true);
      return new Answer(env.getRequiredProperty("test.replica"),request.requestId());
    }
  }
  @Configuration(proxyBeanMethods=false) @EnableAutoConfiguration @Import(Handler.class)
  static class ServerApp {
    // Mock owning receipt SPI: tests TRANSPORT metadata/single-attempt behavior only,
    // never substitutes for or qualifies a durable production receipt implementation.
    @Bean TransactionalRequestExecutor owningReceiptSpi() {
      var receipt=org.mockito.Mockito.mock(TransactionalRequestExecutor.class);
      org.mockito.Mockito.when(receipt.execute(org.mockito.ArgumentMatchers.any(),org.mockito.ArgumentMatchers.anyString(),org.mockito.ArgumentMatchers.any()))
          .thenAnswer(invocation->invocation.<java.util.function.Supplier<byte[]>>getArgument(2).get());
      return receipt;
    }
    @Bean AtomicInteger port(){return new AtomicInteger();}
    @Bean ApplicationListener<PayloadApplicationEvent<GrpcServerEndpoint>> actualPort(AtomicInteger port){
      return event->port.set(event.getPayload().port());
    }
  }
  @Configuration(proxyBeanMethods=false) @EnableAutoConfiguration
  static class ClientApp {
    @Bean CellServiceDirectory directory(){return logical->{
      if(!fresh.get())throw new IllegalStateException("stale");
      return logical.equals("inventory-cell")?endpoints.get():List.of();
    };}
  }

  @BeforeAll static void certificates() throws Exception {
    password=UUID.randomUUID().toString(); Files.writeString(material.resolve("password"),password);
    key("server","dns:inventory-cell","serverAuth");
    key("client","uri:spiffe://apex.test/tenant/test/cell/client-cell","clientAuth");
    key("wrong-client","uri:spiffe://apex.test/tenant/test/cell/other-cell","clientAuth");
    key("wrong-server","dns:other-cell","serverAuth");
    key("rogue","uri:spiffe://apex.test/tenant/test/cell/client-cell","clientAuth");
    for(String name:List.of("server","client","wrong-client","wrong-server")) {
      tool("-exportcert","-rfc","-alias",name,"-keystore",material.resolve(name+".p12").toString(),"-file",material.resolve(name+".crt").toString());
      tool("-importcert","-noprompt","-alias",name,"-keystore",material.resolve("trust.p12").toString(),"-file",material.resolve(name+".crt").toString());
    }
  }
  static void key(String alias,String san,String usage)throws Exception {
    tool("-genkeypair","-noprompt","-alias",alias,"-dname","CN="+alias,"-keyalg","RSA","-keysize","2048",
        "-storetype","PKCS12","-validity","2","-keystore",material.resolve(alias+".p12").toString(),
        "-ext","san="+san,"-ext","eku="+usage);
  }
  static void tool(String...args)throws Exception {
    var command=new ArrayList<String>(List.of(Path.of(System.getProperty("java.home"),"bin/keytool").toString()));
    command.addAll(List.of(args)); command.addAll(List.of("-storepass:file",material.resolve("password").toString()));
    var p=new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(material.resolve("keytool.log").toFile()).start();
    try {if(!p.waitFor(20,TimeUnit.SECONDS)||p.exitValue()!=0)throw new IllegalStateException("Ephemeral keytool failed");}
    finally {if(p.isAlive()){p.destroyForcibly();p.waitFor(5,TimeUnit.SECONDS);}}
  }
  static ConfigurableApplicationContext boot(Class<?> app,String key,String...extra) {
    var args=new ArrayList<>(List.of("--spring.config.location=optional:classpath:/no-external-dynamic.yml",
        "--spring.main.banner-mode=off","--spring.grpc.server.enabled="+(app==ServerApp.class),
        "--spring.ssl.bundle.jks.workload.keystore.location="+material.resolve(key+".p12").toUri(),
        "--spring.ssl.bundle.jks.workload.keystore.password="+password,
        "--spring.ssl.bundle.jks.workload.keystore.type=PKCS12",
        "--spring.ssl.bundle.jks.workload.truststore.location="+material.resolve("trust.p12").toUri(),
        "--spring.ssl.bundle.jks.workload.truststore.password="+password,
        "--spring.ssl.bundle.jks.workload.truststore.type=PKCS12"));
    args.addAll(List.of(extra));
    var env=new StandardEnvironment();
    env.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    env.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    return new SpringApplicationBuilder(app).web(WebApplicationType.NONE).environment(env).run(args.toArray(String[]::new));
  }
  static ConfigurableApplicationContext client(String key,String...extra) {
    var args=new ArrayList<>(List.of("--apex.grpc.client.enabled=true","--apex.grpc.cell-id=client-cell",
        "--spring.grpc.client.channel.default.ssl.bundle=workload","--apex.discovery.refresh-interval=250ms")); args.addAll(List.of(extra));
    return boot(ClientApp.class,key,args.toArray(String[]::new));
  }
  static ConfigurableApplicationContext server(String key,String replica) {
    return boot(ServerApp.class,key,"--spring.grpc.server.port=0",
        "--spring.grpc.server.address=127.0.0.1","--spring.grpc.server.ssl.bundle=workload",
        "--spring.grpc.server.ssl.client-auth=require","--apex.grpc.server.enabled=true","--apex.grpc.cell-id=inventory-cell",
        "--apex.grpc.security.enabled=true","--apex.grpc.security.tenant=test",
        "--apex.grpc.security.peers.client.uri=spiffe://apex.test/tenant/test/cell/client-cell",
        "--apex.grpc.security.peers.client.contracts=QUERY:DYNAMIC_PROBE:1,REQUEST:DYNAMIC_WRITE:1","--test.replica="+replica);
  }
  static CellEndpoint endpoint(ConfigurableApplicationContext server,String name) {
    int port=server.getBean(AtomicInteger.class).get(); assertThat(port).isPositive();
    return new CellEndpoint("inventory-cell",name,"127.0.0.1",port==8080?8081:8080,port,false,"inventory-cell");
  }
  static Answer query(QueryGateway gateway){return gateway.query("inventory-cell",new Probe(0),Answer.class);}
  static void unavailable(QueryGateway gateway,String name) {
    assertThatThrownBy(()->gateway.query(name,new Probe(0),Answer.class)).isInstanceOfSatisfying(GatewayException.class,
        e->assertThat(e.code()).isEqualTo(GatewayException.Code.UNAVAILABLE));
  }

  @Test void startsBeforeReplicasThenDiscoversBalancesRefreshesAndFailsClosed() {
    endpoints.set(List.of()); fresh.set(true); calls.set(0);
    try(var client=client("client")) {
      var gateway=client.getBean(QueryGateway.class);
      unavailable(gateway,"inventory-cell"); unavailable(gateway,"missing-cell");
      try(var a=server("server","a")) {
        endpoints.set(List.of(endpoint(a,"a")));
        await().atMost(Duration.ofSeconds(10)).ignoreExceptions().untilAsserted(()->assertThat(query(gateway).replica()).isEqualTo("a"));
        try(var b=server("server","b")) {
          endpoints.set(List.of(endpoint(a,"a"),endpoint(b,"b")));
          var seen=new HashSet<String>();
          await().atMost(Duration.ofSeconds(10)).ignoreExceptions().untilAsserted(()->{
            seen.add(query(gateway).replica()); assertThat(seen).containsExactlyInAnyOrder("a","b");
          });
          var counts=new HashMap<String,Integer>();for(int i=0;i<40;i++)counts.merge(query(gateway).replica(),1,Integer::sum);
          assertThat(counts.get("a")).isBetween(15,25);assertThat(counts.get("b")).isBetween(15,25);
          endpoints.set(List.of(endpoint(b,"b")));
          await().atMost(Duration.ofSeconds(10)).untilAsserted(()->{for(int i=0;i<8;i++)assertThat(query(gateway).replica()).isEqualTo("b");});
          var id=UUID.randomUUID();
          assertThat(gateway.query("inventory-cell",new Probe(0),Answer.class,new QueryOptions(id,Duration.ofSeconds(2))).requestId()).isEqualTo(id);
          assertThatThrownBy(()->gateway.query("inventory-cell",new Probe(500),Answer.class,new QueryOptions(UUID.randomUUID(),Duration.ofMillis(60))))
              .isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.DEADLINE_EXCEEDED));
          assertThatThrownBy(()->gateway.query("inventory-cell",new Denied(),Answer.class)).isInstanceOfSatisfying(GatewayException.class,
              e->assertThat(e.code()).isEqualTo(GatewayException.Code.PERMISSION_DENIED));
          endpoints.set(List.of());int before=calls.get(); unavailable(gateway,"inventory-cell");assertThat(calls).hasValue(before);
          endpoints.set(List.of(endpoint(b,"b")));fresh.set(false);unavailable(gateway,"inventory-cell");fresh.set(true);
        }
      } finally {endpoints.set(List.of());fresh.set(true);}
    }
  }

  @Test void wrongServerNameWrongPeerAndUntrustedClientRemainRejected() {
    fresh.set(true);
    try(var server=server("server","secured")) {
      endpoints.set(List.of(endpoint(server,"secured")));
      try(var denied=client("wrong-client")) {
        assertThatThrownBy(()->query(denied.getBean(QueryGateway.class))).isInstanceOfSatisfying(GatewayException.class,
            e->assertThat(e.code()).isEqualTo(GatewayException.Code.PERMISSION_DENIED));
      }
      try(var rogue=client("rogue")){unavailable(rogue.getBean(QueryGateway.class),"inventory-cell");}
    }
    try(var wrongServer=server("wrong-server","wrong-san");var client=client("client")) {
      endpoints.set(List.of(endpoint(wrongServer,"wrong-san")));
      unavailable(client.getBean(QueryGateway.class),"inventory-cell");
    } finally {endpoints.set(List.of());}
  }

  @Test void automaticDiscoveryRejectsInsecureDefaultAndCertificateBypass() {
    for(String setting:List.of("--spring.grpc.client.channel.default.ssl.enabled=false",
        "--spring.grpc.client.channel.default.bypass-certificate-validation=true")) {
      assertThatThrownBy(()->{try(var ignored=client("client",setting)) {}})
          .hasStackTraceContaining("requires verified default gRPC TLS");
    }
  }

  @Test void dynamicRequestsPreserveIdempotencyMetadataTransactionsAndSingleAttempt() {
    fresh.set(true);
    try(var server=server("server","request-owner");var client=client("client")) {
      endpoints.set(List.of(endpoint(server,"request-owner")));
      var gateway=client.getBean(RequestReplyGateway.class);
      var options=RequestOptions.withIdempotencyKey("stable-transport-key");
      assertThat(gateway.request("inventory-cell",new Write("ok"),Answer.class,options).requestId()).isEqualTo(options.requestId());
      var metadata=org.mockito.ArgumentCaptor.forClass(RequestContext.class);
      var receipt=server.getBean(TransactionalRequestExecutor.class);
      org.mockito.Mockito.verify(receipt).execute(metadata.capture(),org.mockito.ArgumentMatchers.matches("[a-f0-9]{64}"),org.mockito.ArgumentMatchers.any());
      assertThat(metadata.getValue().idempotencyKey()).isEqualTo(options.idempotencyKey());
      assertThat(metadata.getValue().originCell()).isEqualTo("client-cell");
      assertThat(metadata.getValue().targetCell()).isEqualTo("inventory-cell");
      org.mockito.Mockito.clearInvocations(receipt);
      org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
      try {
        assertThatThrownBy(()->gateway.request("inventory-cell",new Write("no-network"),Answer.class,options))
            .isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(GatewayException.Code.FAILED_PRECONDITION));
      } finally {org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);}
      org.mockito.Mockito.verifyNoInteractions(receipt);
      assertThatThrownBy(()->gateway.request("inventory-cell",new Write("unavailable"),Answer.class,options))
          .isInstanceOfSatisfying(GatewayException.class,e->{assertThat(e.code()).isEqualTo(GatewayException.Code.UNAVAILABLE);assertThat(e.outcomeUnknown()).isTrue();});
      org.mockito.Mockito.verify(receipt,org.mockito.Mockito.times(1)).execute(org.mockito.ArgumentMatchers.any(),org.mockito.ArgumentMatchers.anyString(),org.mockito.ArgumentMatchers.any());
      var attempt=client.getBean(QueryGateway.class).queryAsync("inventory-cell",new Probe(500),Answer.class,QueryOptions.defaults()).toCompletableFuture();
      assertThat(attempt.cancel(true)).isTrue();assertThat(attempt.isCancelled()).isTrue();
    } finally {endpoints.set(List.of());}
  }
}
