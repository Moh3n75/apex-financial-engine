package com.apex.inventory;

import static com.apex.inventory.OrderInventorySagaIT.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import com.apex.inventory.m36a.DisposableM36aIdentity;
import com.apex.operations.contracts.*;
import com.apex.platform.grpc.api.*;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import tools.jackson.databind.JsonNode;

/** Real packaged cells + real TLS sockets + Identity + Console + authenticated Next browser. */
@Tag("integration")
class ControlCenterFiveCellLiveIT {
  static final List<String> CELLS = List.of("order-cell", "inventory-cell", "payment-cell", "customer-cell", "reporting-cell");
  static final String ENV = "control-center-live", CLUSTER = "owned-kraft";
  final Path root = Path.of(System.getProperty("saga.repositoryRoot", "..")).toAbsolutePath().normalize();
  final Path out = root.resolve("out/control-center-live");
  final Map<String,Object> receipt = new LinkedHashMap<>();
  final List<Map<String,Object>> calls = new ArrayList<>();
  final List<Process> children = new ArrayList<>();
  final List<EmbeddedPostgres> databases = new ArrayList<>();
  final Map<String,Integer> grpcPorts = new LinkedHashMap<>(), httpPorts = new LinkedHashMap<>();
  final Map<String,ContractManifest> manifests = new LinkedHashMap<>();
  final Map<String,Process> cells = new LinkedHashMap<>();
  Ops03bCertificates certificates;
  HttpClient http;
  DisposableM36aIdentity identity;
  Process console;
  String token, stage = "INITIALIZATION";
  final boolean membershipMode=Boolean.getBoolean("controlcenter.membership");
  int registryPort;
  final Map<String,Path> registryKeys=new HashMap<>();
  Process registry;

  @Configuration(proxyBeanMethods=false)
  @EnableAutoConfiguration(excludeName={"org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration", "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"})
  static class Client {}

  @Test void fivePackagedCellsOverAuthenticatedMtls() throws Exception {
    Files.createDirectories(out);
    receipt.put("sourceMode", "LIVE_EXECUTION"); receipt.put("startedAt", Instant.now().toString());
    receipt.put("verdict", "BLOCKED"); receipt.put("calls", calls);
    receipt.put("securityMode",Map.of("grpc","REQUIRED_MTLS_EXACT_URI_SAN_PER_CONTRACT_GRANTS","insecureLoopback",false,
        "console","M36A_JWT_AND_M36B_OWNER_GRANTS","browser","REAL_OIDC_LOGIN","destinations","EXPLICIT_TEST_OWNED_PORTS"));
    receipt.put("launchConfigurationSource","PACKAGED_APPLICATION_YAML_NOT_COMPARISON_FIXTURES");
    var streams = List.of("apex.order.events.v1", "apex.order.legacy.events.v1", "apex.order.payment-requests.v1",
        "apex.order.inventory-release-requests.v1", "apex.inventory.events.v1", "apex.payment.events.v1", "apex.customer.events.v1");
    var topics = new ArrayList<String>(); for (String s:streams) { topics.add(s); topics.add(s+"-dlt"); }
    var broker = new EmbeddedKafkaKraftBroker(1,1,topics.toArray(String[]::new));
    broker.brokerProperty("auto.create.topics.enable","false"); broker.brokerListProperty("apex.control-center.live.brokers");
    boolean brokerStarted=false;
    try {
      OpsM36a1AuthenticatedLiveIT.assertLoopbackPortFree(9190); OpsM36a1AuthenticatedLiveIT.assertLoopbackPortFree(3000);
      certificates = new Ops03bCertificates();
      if(membershipMode)startRegistry();
      for (String cell:CELLS) {
        assertThat(jar(cell)).isRegularFile(); grpcPorts.put(cell,freePort()); httpPorts.put(cell,freePort());
        databases.add(EmbeddedPostgres.builder().setConnectConfig("sslmode","disable").start());
      }
      stage="IDENTITY_START";
      identity = new DisposableM36aIdentity(jar("apex-identity-server"),out.resolve("identity"),root.resolve("apex-identity-server/src/main/resources/db/migration"));
      identity.start(); token=identity.mintUserAccessToken();
      http=HttpClient.newHttpClient();
      stage="KAFKA_START"; brokerStarted=true; broker.afterPropertiesSet();
      stage="CELL_START";
      for (int i=0;i<CELLS.size();i++) {
        String cell=CELLS.get(i);
        cells.put(cell,launchCell(cell,databases.get(i),broker.getBrokersAsString(),grpcPorts.get(cell),CLUSTER,null));
      }
      for(String cell:CELLS) ready(cells.get(cell),out.resolve(cell+".log"),http,httpPorts.get(cell));
      stage="LIVE_GRPC_DISCOVERY";
      try(var client=client(certificates.primary,"m32-client",grpcPorts)) {
        var gateway=client.getBean(QueryGateway.class);
        for(String cell:CELLS) {
          var manifest=discover(gateway,cell,"PRIMARY");
          assertThat(manifest.schemaVersion()).isEqualTo(1); assertThat(manifest.environment()).isEqualTo(ENV);
          assertThat(manifest.cluster()).isEqualTo(CLUSTER); assertThat(manifest.instance()).isEqualTo(cell+"-one");
          assertThat(manifest.sourceMode()).isEqualTo("EFFECTIVE_DECLARATION");
          manifests.put(cell,manifest); write(cell+"-live-manifest.json",manifest);
          compareStatic(cell,manifest);
        }
      }
      stage="MTLS_NEGATIVES";
      for(var spec:List.of(Map.entry("m32-client",certificates.rogue),Map.entry("m32-observer",certificates.observer))) {
        try(var client=client(spec.getValue(),spec.getKey(),grpcPorts)) {
          assertThatThrownBy(()->discover(client.getBean(QueryGateway.class),"order-cell",spec.getKey().equals("m32-observer")?"CONTRACT_DENIED_PEER":"UNTRUSTED_CA"))
              .isInstanceOfSatisfying(GatewayException.class,e->assertThat(e.code()).isEqualTo(
                  spec.getKey().equals("m32-observer")?GatewayException.Code.PERMISSION_DENIED:GatewayException.Code.UNAVAILABLE));
        }
      }
      stage="CONSOLE_START"; startConsole();
      JsonNode topology=awaitTopology(5,28);
      write("live-topology.json",topology);
      receipt.put("baseline",Map.of("services",5,"nodes",topology.path("nodes").size(),"connections",topology.path("connections").size()));
      assertThat(request("", "GET").statusCode()).isEqualTo(401);
      int mutationStatus=request(token,"POST").statusCode();
      assertThat(mutationStatus).isIn(403,405); // Security denyAll may run before ReadOnlyFilter.
      receipt.put("mutationDeniedStatus",mutationStatus);
      var denied=JSON.readTree(request(identity.mintBobAccessToken(),"GET").body());
      write("denied-owner-topology.json",denied);
      assertThat(denied.path("nodes").size()).isEqualTo(membershipMode?7:6);
      assertThat(denied.path("connections").size()).isEqualTo(4); // Order's own declared publications remain visible
      // Exact counts are checked below against what an owner-only grant may expose.
      receipt.put("deniedOwner",Map.of("visibleOwners",List.of("order-cell"),"nodes",denied.path("nodes").size(),"connections",denied.path("connections").size()));
      stage="ORDER_WORKFLOW";
      var orders=new JdbcTemplate(databases.get(0).getPostgresDatabase());
      var stock=new JdbcTemplate(databases.get(1).getPostgresDatabase());
      var payments=new JdbcTemplate(databases.get(2).getPostgresDatabase());
      UUID sku=UUID.randomUUID(); stock.update("INSERT INTO inventory_stock VALUES (?,10,0)",sku);
      UUID order=UUID.fromString(post(http,httpPorts.get("order-cell"),List.of(item(sku,2))).path("orderId").asText());
      awaitStatus(orders,order,"CONFIRMED");
      assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,order)).isEqualTo("RESERVED");
      assertThat(payments.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?",String.class,order)).isEqualTo("AUTHORIZED");
      receipt.put("workflow",Map.of("order","CONFIRMED","inventory","RESERVED","payment","AUTHORIZED","topologyEvidence","DECLARED_ONLY"));
      stage="BROWSER_OIDC";
      if(membershipMode)System.setProperty("controlcenter.membership.expected","5");
      Process next=OpsM36a1AuthenticatedLiveIT.startNextM36a(root,9190,3000,out.resolve("next.log"),identity,"live-session-"+UUID.randomUUID()+UUID.randomUUID());
      children.add(next);
      // Reuse the OPS-04B readiness strategy: java.net.http's upgrade probe is unreliable with this host's Next server.
      await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(()->{
        assertThat(next.isAlive()).isTrue();
        var connection=(HttpURLConnection)URI.create("http://127.0.0.1:3000/login").toURL().openConnection();
        connection.setConnectTimeout(3000); connection.setReadTimeout(3000);
        try {assertThat(connection.getResponseCode()).isEqualTo(200);} finally {connection.disconnect();}
      });
      int browser=OpsM36a1AuthenticatedLiveIT.runPlaywright(root,out.resolve("live-topology.json"),"e2e/control-center-live.spec.ts",out.resolve("browser.log"),"playwright.control-center-live.config.ts",identity.password,true);
      receipt.put("browserExit",browser); assertThat(browser).isZero();
      if(membershipMode) {
        stage="AUTHORITATIVE_MULTI_INSTANCE";
        membershipScenarios(broker.getBrokersAsString());
        receipt.put("verdict","LIVE_MEMBERSHIP_ACCEPTANCE_PASS");
        receipt.put("unimplemented",List.of("observed_otel_interactions","trace_queries","kafka_admin_provider","runtime_health_provider","kubernetes_live_membership_validation"));
        stage="COMPLETE";return;
      }
      stage="UNAVAILABLE_AND_EXPIRED";
      stop(cells.get("reporting-cell"));
      await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(()->{
        var graph=JSON.readTree(request(token,"GET").body());
        var provider=provider(graph,"reporting-cell");
        assertThat(provider.path("status").asText()).isEqualTo("SOURCE_UNAVAILABLE");
        assertThat(Instant.parse(provider.path("collectedAt").asText())).isBefore(Instant.now().minusSeconds(30));
        assertThat(graph.path("connections").size()).isEqualTo(28); // retained declarations, never live traffic
        write("expired-unavailable-topology.json",graph);
      });
      receipt.put("expiredUnavailable",Map.of("status","SOURCE_UNAVAILABLE","ageOverSeconds",30,"retainsDeclaredEdges",true));
      stage="MISSING_CONSUMER"; stop(console); startConsole();
      var missing=awaitTopology(4,26); write("missing-consumer-topology.json",missing);
      receipt.put("missingConsumer",Map.of("availableManifests",4,"connections",26));
      stage="WRONG_CLUSTER";
      cells.put("reporting-cell",launchCell("reporting-cell",databases.get(4),broker.getBrokersAsString(),grpcPorts.get("reporting-cell"),"wrong-cluster",null));
      ready(cells.get("reporting-cell"),out.resolve("reporting-cell.log"),http,httpPorts.get("reporting-cell"));
      var wrong=awaitTopology(5,26); assertThat(wrong.path("unresolved").toString()).contains("NO_VISIBLE_EXACT_TOPIC_CONTRACT_MATCH");
      write("wrong-cluster-topology.json",wrong);
      receipt.put("wrongCluster",Map.of("connections",26,"unresolved",true));
      stage="DUPLICATE_INSTANCE";
      int duplicatePort=freePort();
      Process duplicate=launchCell("reporting-cell",databases.get(4),broker.getBrokersAsString(),duplicatePort,"wrong-cluster","duplicate");
      await().atMost(Duration.ofSeconds(60)).untilAsserted(()->{
        assertThat(duplicate.isAlive()).isTrue();
        assertThat(Files.readString(out.resolve("reporting-cell-duplicate.log"))).contains("Started ReportingCellApplication");
      });
      var duplicatePorts=new LinkedHashMap<>(grpcPorts); duplicatePorts.put("reporting-cell",duplicatePort);
      try(var originalClient=client(certificates.primary,"m32-client",grpcPorts);var duplicateClient=client(certificates.primary,"m32-client",duplicatePorts)) {
        var original=discover(originalClient.getBean(QueryGateway.class),"reporting-cell","DUPLICATE_ORIGINAL");
        var second=discover(duplicateClient.getBean(QueryGateway.class),"reporting-cell","DUPLICATE_SECOND");
        assertThat(second.instance()).isEqualTo(original.instance()); assertThat(second.epoch()).isNotEqualTo(original.epoch());
        receipt.put("duplicateInstance",Map.of("twoRealEpochs",true,"service",original.service(),"instance",original.instance(),
            "firstEpoch",original.epoch(),"secondEpoch",second.epoch(),"automaticDetection","UNIMPLEMENTED_SINGLE_DESTINATION_PROVIDER"));
      } finally {stop(duplicate);}
      receipt.put("verdict","LIVE_PARTIAL_DUPLICATE_DETECTION_UNIMPLEMENTED");
      receipt.put("unimplemented",List.of("duplicate_instance_registry_detection","observed_otel_interactions","trace_queries","kafka_admin_provider","runtime_health_provider","complete_runtime_membership"));
      stage="COMPLETE";
    } catch(Exception|AssertionError failure) {
      receipt.put("failedStage",stage); receipt.put("failureClass",failure.getClass().getName()); throw failure;
    } finally {
      for(var child:children.reversed()) try {stop(child);} catch(Exception ignored) {}
      if(http!=null) http.close();
      if(identity!=null) identity.close();
      for(var db:databases.reversed()) db.close();
      if(brokerStarted) broker.destroy();
      if(certificates!=null) certificates.close();
      receipt.put("completedAt",Instant.now().toString()); receipt.put("stage",stage);
      receipt.put("cellsContacted",List.copyOf(manifests.keySet())); receipt.put("ownedChildrenStopped",children.stream().noneMatch(Process::isAlive));
      write("live-acceptance.json",receipt);
    }
  }

  Path jar(String module) {return out.resolve("jars/"+module+".jar");}
  void write(String file,Object value)throws Exception {Files.writeString(out.resolve(file),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value));}
  Process launchCell(String cell,EmbeddedPostgres db,String brokers,int grpc,String cluster,String suffix)throws Exception {
    // Resolve packaged configuration, not comparison fixtures. Drift in a golden file must not configure the process under test.
    var packaged=new StandardEnvironment();
    packaged.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    packaged.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    try(var zip=new java.util.zip.ZipFile(jar(cell).toFile())) {
      var yaml=new org.springframework.core.io.ByteArrayResource(zip.getInputStream(zip.getEntry("BOOT-INF/classes/application.yml")).readAllBytes());
      for(var source:new org.springframework.boot.env.YamlPropertySourceLoader().load(cell,yaml)) packaged.getPropertySources().addLast(source);
    }
    String publish=packaged.getRequiredProperty("apex.messaging.publisher.topic");
    String subscribe=packaged.getProperty("apex.messaging.consumer.topics","unused");
    String group=packaged.getProperty("spring.kafka.consumer.group-id","unused");
    var args=new ArrayList<>(certificates.bundle(certificates.server));
    args.addAll(List.of("--spring.profiles.active=", "--eureka.client.enabled=false","--spring.cloud.discovery.enabled=false",
        "--apex.grpc.cell-id="+cell,"--apex.grpc.server.enabled=true","--apex.grpc.insecure-loopback-test-only=false",
        "--spring.grpc.server.enabled=true","--spring.grpc.server.address=127.0.0.1","--spring.grpc.server.port="+grpc,
        "--spring.grpc.server.ssl.enabled=true","--spring.grpc.server.ssl.bundle=m32","--spring.grpc.server.ssl.client-auth=require","--spring.grpc.server.reflection.enabled=false",
        "--apex.grpc.security.enabled=true","--apex.grpc.security.tenant=alpha",
        "--apex.grpc.security.peers.ops.uri=spiffe://apex.test/tenant/alpha/cell/m32-client","--apex.grpc.security.peers.ops.contracts=QUERY:OPS_CONTRACT_DISCOVERY:1",
        "--apex.grpc.security.peers.observer.uri=spiffe://apex.test/tenant/alpha/cell/m32-observer","--apex.grpc.security.peers.observer.contracts=QUERY:GET_ORDER:1",
        "--apex.contract-discovery.enabled=true","--apex.contract-discovery.environment="+ENV,"--apex.contract-discovery.cluster="+cluster,"--apex.contract-discovery.instance="+cell+"-one",
        "--apex.messaging.consumer.enabled="+packaged.getProperty("apex.messaging.consumer.enabled",Boolean.class,false),
        "--apex.messaging.outbox.publisher.enabled="+packaged.getProperty("apex.messaging.outbox.publisher.enabled",Boolean.class,true)));
    if(membershipMode) {
      args.removeIf(a->a.startsWith("--eureka.client.enabled=")||a.startsWith("--spring.cloud.discovery.enabled=")||a.startsWith("--apex.contract-discovery.instance=")||a.startsWith("--apex.contract-discovery.environment="));
      args.addAll(registryArguments(cell));
      String instance=cell+("replica".equals(suffix)?"-two":"-one");
      args.addAll(List.of("--apex.discovery.provider=eureka","--apex.discovery.membership.enabled=true","--apex.discovery.registry-id=owned-eureka",
          "--apex.discovery.authority=localhost","--apex.discovery.instance-id="+UUID.randomUUID(),"--apex.contract-discovery.instance="+instance,
          "--apex.contract-discovery.environment="+("foreign".equals(suffix)?"other-environment":ENV)));
    }
    Process child=launch(jar(cell),db,brokers,group,suffix==null?httpPorts.get(cell):freePort(),
        out.resolve(cell+(suffix==null?"":"-"+suffix)+".log"),publish,subscribe,args.toArray(String[]::new)); children.add(child); return child;
  }
  ConfigurableApplicationContext client(Path keys,String peer,Map<String,Integer> ports) {
    var args=new ArrayList<>(certificates.bundle(keys));
    args.addAll(List.of("--spring.config.location=optional:classpath:/no-control-center-client.yml","--spring.main.banner-mode=off",
        "--spring.application.name=control-center-acceptance-client","--apex.grpc.cell-id="+peer,"--apex.grpc.client.enabled=true",
        "--apex.grpc.insecure-loopback-test-only=false","--apex.grpc.server.enabled=false","--spring.grpc.server.enabled=false",
        "--eureka.client.enabled=false","--spring.cloud.discovery.enabled=false"));
    destinations(args,ports);
    var env=new StandardEnvironment(); env.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME); env.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    return new SpringApplicationBuilder(Client.class).environment(env).web(WebApplicationType.NONE).run(args.toArray(String[]::new));
  }
  static void destinations(List<String> args,Map<String,Integer> ports) {
    ports.forEach((cell,port)->args.addAll(List.of("--apex.grpc.destinations."+cell+".channel="+cell,
        "--spring.grpc.client.channel."+cell+".target=static://127.0.0.1:"+port,"--spring.grpc.client.channel."+cell+".ssl.enabled=true","--spring.grpc.client.channel."+cell+".ssl.bundle=m32")));
  }
  ContractManifest discover(QueryGateway gateway,String cell,String purpose) {
    try {
      var manifest=gateway.query(cell,new ContractDiscoveryQuery(),ContractManifest.class,new QueryOptions(UUID.randomUUID(),Duration.ofSeconds(3)));
      assertThat(manifest.service()).isEqualTo(cell);
      calls.add(Map.of("cell",cell,"purpose",purpose,"result","SUCCESS","at",Instant.now().toString())); return manifest;
    } catch(GatewayException failure) {
      calls.add(Map.of("cell",cell,"purpose",purpose,"result",failure.code().name(),"at",Instant.now().toString())); throw failure;
    }
  }
  void compareStatic(String cell,ContractManifest live)throws Exception {
    var fixture=JSON.readValue(Files.readString(root.resolve("apex-operations-console/src/test/resources/contracts/"+cell+".json")),ContractManifest.class);
    assertThat(live.publications()).containsExactlyElementsOf(fixture.publications());
    assertThat(live.subscriptions()).containsExactlyElementsOf(fixture.subscriptions());
    assertThat(live.consumerGroup()).isEqualTo(fixture.consumerGroup()); assertThat(live.eventHandlers()).containsExactlyElementsOf(fixture.eventHandlers());
    assertThat(live.grpcHandlers()).containsAll(fixture.grpcHandlers());
    receipt.put("staticComparison:"+cell,Map.of("publicationsSubscriptionsEvents","MATCH","extraGrpc",live.grpcHandlers().stream().filter(r->!fixture.grpcHandlers().contains(r)).map(r->r.kind()+":"+r.contract().key()).toList(),
        "identityDifferences",List.of("environment","cluster","instance","epoch","collectedAt","sourceMode")));
  }
  void startConsole()throws Exception {
    var args=new ArrayList<>(certificates.bundle(certificates.primary));
    args.addAll(List.of("--server.address=127.0.0.1","--server.port=9190","--apex.grpc.cell-id=m32-client","--apex.grpc.client.enabled=true",
        "--apex.ops.contract-discovery.environment="+ENV,"--apex.ops.contract-discovery.owners="+String.join(",",CELLS),
        "--apex.ops.m36a.enabled=true","--apex.ops.m36a.issuer="+identity.issuer,"--apex.ops.m36a.audience=apex-operations-console","--apex.ops.m36a.required-scope=ops.console.read",
        "--apex.ops.m36a.tenant-id="+identity.tenant,"--apex.ops.m36b.resource-auth-enabled=true"));
    destinations(args,grpcPorts);
    if(membershipMode) {
      args.addAll(registryArguments("m32-client"));
      args.addAll(List.of("--apex.ops.membership.enabled=true","--apex.discovery.provider=eureka","--apex.discovery.registration.enabled=false","--eureka.client.register-with-eureka=false",
          "--apex.discovery.registry-id=owned-eureka","--apex.discovery.max-stale-ms=3000","--apex.discovery.refresh-interval=1s",
          "--spring.grpc.client.channel.default.ssl.enabled=true","--spring.grpc.client.channel.default.ssl.bundle=m32"));
      for(String cell:CELLS)args.add("--apex.discovery.targets."+cell+".authority=localhost");
    }
    int i=0;for(String cell:CELLS) {grant(args,i++,identity.user.toString(),cell);} grant(args,i,identity.bobUser.toString(),"order-cell");
    var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx256m","-jar",jar("apex-operations-console").toString())); command.addAll(args);
    var pb=new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(out.resolve("console.log").toFile());
    pb.environment().keySet().removeIf(k->k.matches("(?i)(SPRING_.*|APEX_.*|CELL_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
    console=pb.start(); children.add(console);
    await().ignoreExceptions().atMost(Duration.ofSeconds(90)).untilAsserted(()->{assertThat(console.isAlive()).isTrue(); assertThat(request("","GET").statusCode()).isEqualTo(401);});
  }
  static void grant(List<String> args,int i,String subject,String owner) {
    String p="--apex.ops.m36b.resource-grants["+i+"].";
    args.addAll(List.of(p+"subject="+subject,p+"environment="+ENV,p+"owner="+owner,p+"kinds=OVERVIEW",p+"scopes=owner"));
  }
  HttpResponse<String> request(String bearer,String method)throws Exception {
    var request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:9190/ops/v1/topology?schemaVersion=2")).timeout(Duration.ofSeconds(8));
    if(!bearer.isEmpty())request.header("Authorization","Bearer "+bearer);
    return http.send(request.method(method,HttpRequest.BodyPublishers.noBody()).build(),HttpResponse.BodyHandlers.ofString());
  }
  JsonNode awaitTopology(int services,int edges) {
    var result=new java.util.concurrent.atomic.AtomicReference<JsonNode>();
    await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(2)).ignoreExceptions().untilAsserted(()->{
      var response=request(token,"GET");assertThat(response.statusCode()).isEqualTo(200);
      var graph=JSON.readTree(response.body());assertThat(graph.path("schemaVersion").asInt()).isEqualTo(2);
      int available=0;for(var p:graph.path("providers")) if(p.path("name").asText().startsWith("contracts:")&&p.path("status").asText().equals("AVAILABLE"))available++;
      assertThat(available).isEqualTo(services); assertThat(graph.path("connections").size()).isEqualTo(edges);
      for(var e:graph.path("connections")) {assertThat(e.path("lastSeen").isNull()).isTrue();for(var proof:e.path("evidence"))assertThat(proof.path("kind").asText()).isEqualTo("DECLARED");}
      result.set(graph);
    });return result.get();
  }
  JsonNode provider(JsonNode graph,String cell) {for(var p:graph.path("providers"))if(p.path("name").asText().endsWith(":"+cell))return p;throw new AssertionError("Missing provider");}

  void startRegistry()throws Exception {
    registryPort=freePort();
    var args=new ArrayList<>(certificates.bundle(certificates.server));
    args.addAll(List.of("--server.address=127.0.0.1","--server.port="+registryPort,"--server.ssl.enabled=true","--server.ssl.bundle=m32","--server.ssl.client-auth=need",
        "--eureka.instance.hostname=localhost","--eureka.client.serviceUrl.defaultZone=https://localhost:"+registryPort+"/eureka/",
        "--apex.discovery.secure-registry.enabled=true","--apex.discovery.secure-registry.uri-prefix=spiffe://apex.test/tenant/alpha/cell/",
        "--apex.discovery.secure-registry.readers=m32-client","--apex.discovery.secure-registry.writers="+String.join(",",CELLS),
        "--eureka.server.enable-self-preservation=false","--eureka.server.use-read-only-response-cache=false","--eureka.server.eviction-interval-timer-in-ms=1000"));
    var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx256m","-jar",jar("apex-discovery-server").toString()));command.addAll(args);
    registry=new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(out.resolve("registry.log").toFile()).start();children.add(registry);
    try(var client=registryHttp(certificates.primary)) {
      await().atMost(Duration.ofSeconds(75)).ignoreExceptions().untilAsserted(()->{
        assertThat(registry.isAlive()).isTrue();
        var probe=client.send(HttpRequest.newBuilder(URI.create("https://localhost:"+registryPort+"/eureka/apps")).header("Accept","application/json").timeout(Duration.ofSeconds(3)).GET().build(),HttpResponse.BodyHandlers.discarding());
        assertThat(probe.statusCode()).isEqualTo(200);
      });
    }
  }
  List<String> registryArguments(String cell)throws Exception {
    Path keys=certificates.primary;
    if(!cell.equals("m32-client")) {
      keys=registryKeys.get(cell);
      if(keys==null){keys=certificates.signed("registry-"+cell,"uri:spiffe://apex.test/tenant/alpha/cell/"+cell,"clientAuth");registryKeys.put(cell,keys);}
    }
    return List.of("--eureka.client.enabled=true","--spring.cloud.discovery.enabled=true","--eureka.client.serviceUrl.defaultZone=https://localhost:"+registryPort+"/eureka/",
        "--eureka.instance.hostname=127.0.0.1","--eureka.instance.prefer-ip-address=true","--eureka.instance.ip-address=127.0.0.1",
        "--eureka.client.tls.enabled=true","--eureka.client.tls.key-store="+keys.toUri(),"--eureka.client.tls.key-store-type=PKCS12",
        "--eureka.client.tls.key-store-password="+certificates.password,"--eureka.client.tls.key-password="+certificates.password,
        "--eureka.client.tls.trust-store="+certificates.trust.toUri(),"--eureka.client.tls.trust-store-type=PKCS12","--eureka.client.tls.trust-store-password="+certificates.password,
        "--eureka.client.registry-fetch-interval-seconds=1","--eureka.client.initial-instance-info-replication-interval-seconds=1",
        "--eureka.client.instance-info-replication-interval-seconds=1","--eureka.instance.lease-renewal-interval-in-seconds=2","--eureka.instance.lease-expiration-duration-in-seconds=8");
  }
  HttpClient registryHttp(Path keys)throws Exception{return HttpClient.newBuilder().sslContext(certificates.context(keys,certificates.trust)).build();}
  HttpResponse<String> registryResponse(HttpClient client)throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create("https://localhost:"+registryPort+"/eureka/apps/reporting-cell")).header("Accept","application/json").timeout(Duration.ofSeconds(3)).GET().build(),HttpResponse.BodyHandlers.ofString());
  }
  List<JsonNode> members(JsonNode graph,String state) {var result=new ArrayList<JsonNode>();for(var m:graph.path("memberships"))if(m.path("service").asText().equals("reporting-cell")&&(state==null||m.path("membershipState").asText().equals(state)))result.add(m);return result;}
  JsonNode awaitMembers(int count,String state) {
    var result=new java.util.concurrent.atomic.AtomicReference<JsonNode>();
    await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().untilAsserted(()->{
      var response=request(token,"GET");assertThat(response.statusCode()).isEqualTo(200);var graph=JSON.readTree(response.body());
      assertThat(members(graph,state)).hasSize(count);result.set(graph);
    });return result.get();
  }
  void waitCell(Process process,String log)throws Exception {
    await().atMost(Duration.ofSeconds(75)).untilAsserted(()->{assertThat(process.isAlive()).isTrue();assertThat(Files.readString(out.resolve(log))).contains("Started ReportingCellApplication");});
  }
  void membershipBrowser(String expected)throws Exception {
    System.setProperty("controlcenter.membership.expected",expected);
    assertThat(OpsM36a1AuthenticatedLiveIT.runPlaywright(root,out.resolve("live-topology.json"),"e2e/control-center-live.spec.ts",out.resolve("browser-membership-"+expected+".log"),"playwright.control-center-live.config.ts",identity.password,true)).isZero();
  }
  void membershipScenarios(String brokers)throws Exception {
    var baseline=awaitMembers(1,"VERIFIED");write("membership-baseline.json",baseline);
    assertThat(baseline.path("memberships").size()).isEqualTo(5);
    int port=freePort();Process second=launchCell("reporting-cell",databases.get(4),brokers,port,CLUSTER,"replica");waitCell(second,"reporting-cell-replica.log");
    var two=awaitMembers(2,"VERIFIED");write("membership-two-replicas.json",two);
    assertThat(members(two,"DUPLICATE")).isEmpty();
    try(var read=registryHttp(certificates.primary)) {
      var registryJson=JSON.readTree(registryResponse(read).body());
      assertThat(registryJson.path("application").path("instance").size()).isEqualTo(2);
      var registryIds=new HashSet<String>();for(var entry:registryJson.path("application").path("instance"))registryIds.add(entry.path("instanceId").asText());
      for(var member:members(two,"VERIFIED"))assertThat(registryIds).contains(member.path("registryId").asText());
    }
    membershipBrowser("6");
    var old=members(two,"VERIFIED").stream().filter(m->m.path("instanceId").asText().endsWith("-two")).findFirst().orElseThrow();
    stop(second);awaitMembers(1,"VERIFIED");
    second=launchCell("reporting-cell",databases.get(4),brokers,freePort(),CLUSTER,"replica");waitCell(second,"reporting-cell-replica.log");
    var restart=awaitMembers(2,"VERIFIED");assertThat(members(restart,"DUPLICATE")).isEmpty();
    var renewed=members(restart,"VERIFIED").stream().filter(m->m.path("instanceId").asText().endsWith("-two")).findFirst().orElseThrow();
    assertThat(renewed.path("epoch").asText()).isNotEqualTo(old.path("epoch").asText());write("membership-restart.json",restart);
    stop(second);awaitMembers(1,"VERIFIED");
    Process duplicate=launchCell("reporting-cell",databases.get(4),brokers,freePort(),CLUSTER,"duplicate");waitCell(duplicate,"reporting-cell-duplicate.log");
    var conflict=awaitMembers(2,"DUPLICATE");write("membership-duplicate.json",conflict);
    assertThat(members(conflict,"DUPLICATE").stream().map(m->m.path("epoch").asText()).distinct().count()).isEqualTo(2);
    membershipBrowser("DUPLICATE");stop(duplicate);awaitMembers(1,"VERIFIED");
    Process foreign=launchCell("reporting-cell",databases.get(4),brokers,freePort(),CLUSTER,"foreign");waitCell(foreign,"reporting-cell-foreign.log");
    await().atMost(Duration.ofSeconds(45)).untilAsserted(()->{
      var graph=JSON.readTree(request(token,"GET").body());assertThat(graph.toString()).doesNotContain("other-environment");
      assertThat(graph.path("providers").toString()).contains("PARTIAL_REGISTRY_COVERAGE");write("membership-cross-environment.json",graph);
    });stop(foreign);awaitMembers(1,"VERIFIED");
    // Wait for the authoritative lease set to lose the foreign replica, not merely for its RPC to fail.
    await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(()->{
      var graph=JSON.readTree(request(token,"GET").body());
      assertThat(providerNamed(graph,"membership:"+ENV+":reporting-cell").path("coverage").asText()).isEqualTo("SCOPED_REGISTRY_COMPLETE");
      try(var read=registryHttp(certificates.primary)){assertThat(JSON.readTree(registryResponse(read).body()).path("application").path("instance").size()).isEqualTo(1);}
    });
    try(var denied=registryHttp(certificates.observer)){assertThat(registryResponse(denied).statusCode()).isEqualTo(403);}
    stop(cells.get("reporting-cell"));awaitMembers(0,"VERIFIED");
    var disappearedRef=new java.util.concurrent.atomic.AtomicReference<JsonNode>();
    await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(()->{
      var graph=JSON.readTree(request(token,"GET").body());
      assertThat(providerNamed(graph,"membership:"+ENV+":reporting-cell").path("coverage").asText()).isEqualTo("SCOPED_REGISTRY_COMPLETE");
      assertThat(members(graph,"DISAPPEARED")).isNotEmpty();
      assertThat(members(graph,"DISAPPEARED").stream().map(m->m.path("epoch").asText()))
          .contains(manifests.get("reporting-cell").epoch().toString());
      try(var read=registryHttp(certificates.primary)){assertThat(registryResponse(read).statusCode()).isEqualTo(404);}
      disappearedRef.set(graph);
    });
    var disappeared=disappearedRef.get();write("membership-disappeared.json",disappeared);
    assertThat(members(disappeared,"DUPLICATE")).isEmpty();
    stop(registry);
    await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(()->{
      var graph=JSON.readTree(request(token,"GET").body());assertThat(graph.path("providers").toString()).contains("REGISTRY_UNAVAILABLE");
      assertThat(graph.path("connections").size()).isEqualTo(disappeared.path("connections").size());
      assertThat(members(graph,"DISAPPEARED")).isNotEmpty();
      assertThat(members(graph,"DUPLICATE")).isEmpty();write("membership-registry-unavailable.json",graph);
    });
    receipt.put("membershipAcceptance",Map.of("independentRegistryEnumeration",true,"twoSimultaneousReplicas",true,"duplicateAutomaticallyDetected",true,
        "sequentialRestartNotDuplicate",true,"crossEnvironmentFiltered",true,"disappearance",true,"registryFailure",true,"registryDeniedPeer",true,"browserMultiInstance",true));
  }
  JsonNode providerNamed(JsonNode graph,String name){for(var p:graph.path("providers"))if(p.path("name").asText().equals(name))return p;throw new AssertionError("Missing provider");}
}
