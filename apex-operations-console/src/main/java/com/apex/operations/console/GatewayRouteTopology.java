package com.apex.operations.console;

import com.apex.operations.contracts.TopologySnapshot;
import com.apex.operations.contracts.TopologySnapshot.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import java.util.function.BiPredicate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Reads registered Gateway routes, never derives HTTP callers from registry co-presence. */
@Component
@Profile("local")
@ConditionalOnProperty(name="apex.ops.gateway-routes.enabled", havingValue="true")
public final class GatewayRouteTopology implements AutoCloseable {
  private final URI routesUri;
  private final String environment, gateway;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500))
      .followRedirects(HttpClient.Redirect.NEVER).build();
  private final ObjectMapper json = new ObjectMapper();
  private Snapshot cached;
  private record Snapshot(Instant collectedAt, String status, JsonNode routes,JsonNode definitions) {
    Snapshot(Instant collectedAt,String status,JsonNode routes){this(collectedAt,status,routes,null);}
  }
  private record Route(String id, String target, String reason,JsonNode definition) {}

  static TopologySnapshot notConfigured(TopologySnapshot base) {
    if (base.providers().size()>=32) return base;
    var providers=new ArrayList<>(base.providers());
    providers.add(new Provider("gateway-routes","NOT_CONFIGURED","NO_REGISTERED_ROUTE_SOURCE","UNKNOWN",null));
    return new TopologySnapshot(2,base.asOf(),base.coverage(),base.capabilities(),base.nodes(),base.connections(),providers,
        base.unresolved(),base.descriptors(),base.memberships());
  }

  public GatewayRouteTopology(Environment env) {
    environment = env.getRequiredProperty("apex.ops.local-inventory.environment");
    gateway = env.getProperty("apex.ops.gateway-routes.service", "apex-api-gateway");
    var origin = URI.create(env.getRequiredProperty("apex.ops.gateway-routes.origin"));
    if (!Set.of("local", "local-dev").contains(environment)
        || !Set.of("127.0.0.1", "::1").contains(env.getProperty("server.address", "127.0.0.1"))
        || !gateway.matches("[a-z][a-z0-9-]{0,62}")
        || !Set.of("http", "https").contains(origin.getScheme())
        || !"127.0.0.1".equals(origin.getHost()) || origin.getPort()<1024 || origin.getPort()>65535
        || origin.getRawUserInfo()!=null || origin.getRawQuery()!=null || origin.getRawFragment()!=null
        || !(origin.getPath().isEmpty() || origin.getPath().equals("/")))
      throw new IllegalArgumentException("LOCAL_GATEWAY_ORIGIN_REQUIRED");
    routesUri = origin.resolve("/actuator/gateway/routes");
  }

  private synchronized Snapshot read() {
    var now = Instant.now();
    if (cached != null && now.isBefore(cached.collectedAt().plusSeconds(5))) return cached;
    CompletableFuture<HttpResponse<byte[]>> exchange = null;
    long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(1500);
    try {
      exchange = http.sendAsync(HttpRequest.newBuilder(routesUri).GET().timeout(Duration.ofMillis(1500))
          .header("Accept", "application/json").build(), response -> new BoundedBody());
      var response = exchange.get(1500, TimeUnit.MILLISECONDS); // Includes headers AND body consumption.
      if (response.statusCode()!=200) return cached = new Snapshot(now,
          response.statusCode()==401 || response.statusCode()==403 ? "PERMISSION_GAP" : "SOURCE_UNAVAILABLE", null);
      var routes = json.readTree(response.body());
      if (!routes.isArray() || routes.size()>128) return cached = new Snapshot(now,"SOURCE_INVALID",null);
      try {
        long remaining=Math.max(1,TimeUnit.NANOSECONDS.toMillis(deadline-System.nanoTime()));
        exchange=http.sendAsync(HttpRequest.newBuilder(routesUri.resolve("/actuator/gateway/routedefinitions")).GET()
            .timeout(Duration.ofMillis(remaining)).header("Accept","application/json").build(),definitionResponse->new BoundedBody());
        var definitions=exchange.get(remaining,TimeUnit.MILLISECONDS);
        var tree=definitions.statusCode()==200?json.readTree(definitions.body()):null;
        return cached=new Snapshot(now,tree!=null&&tree.isArray()&&tree.size()<=128?"AVAILABLE":"PARTIAL",routes,
            tree!=null&&tree.isArray()&&tree.size()<=128?tree:null);
      }catch(Exception detailsUnavailable){return cached=new Snapshot(now,"PARTIAL",routes);}
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return cached = new Snapshot(now,"SOURCE_UNAVAILABLE",null);
    } catch (Exception unavailable) {
      return cached = new Snapshot(now,"SOURCE_UNAVAILABLE",null);
    } finally { if (exchange != null && !exchange.isDone()) exchange.cancel(true); }
  }

  TopologySnapshot join(TopologySnapshot base, BiPredicate<String,String> visible) {
    String source = environment+":service:"+gateway;
    var nodes = new HashMap<String, Node>(); base.nodes().forEach(n -> nodes.put(n.id(),n));
    var providers = new ArrayList<>(base.providers());
    if (providers.size()>=32) return base;
    var edges = new LinkedHashMap<String,Connection>(); base.connections().forEach(e -> edges.put(e.id(),e));
    var unresolved = new ArrayList<>(base.unresolved());
    String status="NO_VISIBLE_COVERAGE", coverage="NO_VISIBLE_GATEWAY_SERVICE";
    Instant collected = null;
    if (visible.test(environment,gateway) && nodes.containsKey(source) && nodes.get(source).kind().equals("SERVICE")) {
      var snapshot = read(); status=snapshot.status(); collected=snapshot.routes()==null?null:snapshot.collectedAt();
      coverage="REGISTERED_LOGICAL_ROUTES_ONLY_NOT_TRAFFIC_OR_HEALTH";
      if (snapshot.routes()!=null) {
        var authorized = new ArrayList<Route>();
        for (var route : snapshot.routes()) {
          String routeId = route.path("route_id").asText();
          if (!routeId.matches("[A-Za-z0-9._:-]{1,80}")) { status="PARTIAL"; continue; }
          String reason=null, target=null;
          try {
            var uri=URI.create(route.path("uri").asText());
            // Physical URLs and predicates/filters may contain credentials. Never expose or guess their destination.
            if (!"lb".equals(uri.getScheme()) || uri.getHost()==null || uri.getRawUserInfo()!=null
                || uri.getPort()!=-1 || !uri.getPath().isEmpty() || uri.getQuery()!=null || uri.getFragment()!=null)
              reason="NON_LOGICAL_ROUTE_DESTINATION";
            else {
              String service=uri.getHost().toLowerCase(Locale.ROOT);
              if (!service.matches("[a-z][a-z0-9-]{0,62}") || !visible.test(environment,service)) {
                // Do not retain even a route ID when its owner is hidden: generated IDs can contain owner names.
                continue;
              }
              target=environment+":service:"+service;
              if (!nodes.containsKey(target) || !nodes.get(target).kind().equals("SERVICE")) reason="ROUTE_TARGET_NOT_DISCOVERED";
            }
          } catch (RuntimeException invalid) { reason="INVALID_ROUTE_DESTINATION"; }
          JsonNode definition=null;
          if(snapshot.definitions()!=null)for(var candidate:snapshot.definitions())
            if(candidate.path("id").asText().equals(routeId)&&candidate.path("uri").asText().equals(route.path("uri").asText())) {
              if(definition!=null){definition=null;break;}definition=candidate;
            }
          authorized.add(new Route(routeId,target,reason,definition));
        }
        var seen = new HashSet<String>(); var duplicate = new HashSet<String>();
        for(var route:authorized) if(!seen.add(route.id())) duplicate.add(route.id());
        for(var route:authorized) {
          String routeId=route.id(), target=route.target(), reason=duplicate.contains(routeId)?"DUPLICATE_ROUTE_ID":route.reason();
          String contract=target==null?"":"ROUTE:"+routeId;
          if (reason!=null) {
            status="PARTIAL";
            if (unresolved.size()<256) unresolved.add(new Unresolved(source,contract,reason));
          } else if (edges.size()<400) {
            String id=source+":route:"+routeId;
            var proof=new Evidence("DECLARED","SPRING_CLOUD_GATEWAY","REGISTERED_ROUTE:"+routeId,collected,null,"SERVICE");
            if(route.definition()==null)status="PARTIAL";
            var details=new HttpRoute(routeId,nodes.get(target).name(),safePredicates(route.definition()),safeFilters(route.definition()));
            edges.putIfAbsent(id,new Connection(id,source,target,environment,"HTTP","SERVICE",contract,"UNKNOWN",null,List.of(proof),new Communication(details,List.of(),null)));
          } else status="PARTIAL";
        }
      }
    }
    providers.add(new Provider("gateway-routes",status,coverage,"EFFECTIVE_DECLARATION",collected));
    return new TopologySnapshot(2,base.asOf(),base.coverage(),base.capabilities(),base.nodes(),List.copyOf(edges.values()),
        providers,unresolved,base.descriptors(),base.memberships());
  }

  static List<String> safePredicates(JsonNode definition) {
    if(definition==null)return List.of();var result=new ArrayList<String>();
    for(var predicate:definition.path("predicates")) {
      String name=predicate.path("name").asText();if(!name.matches("[A-Za-z]{1,40}"))continue;
      if(name.equals("Path")||name.equals("Method")) {
        var values=new ArrayList<String>();
        predicate.path("args").elements().forEachRemaining(value->{String text=value.asText();
          if(text.length()<=180&&(name.equals("Path")?text.matches("/[A-Za-z0-9/._*{},:-]*"):text.matches("(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)(?:,(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS))*")))values.add(text);
        });
        String joined=String.join(", ",values);
        result.add(name+(values.isEmpty()||joined.length()>350?" (arguments redacted)":": "+joined));
      }else result.add(name+" (arguments redacted)");
    }
    return result.stream().limit(16).toList();
  }
  static List<String> safeFilters(JsonNode definition) {
    if(definition==null)return List.of();var result=new ArrayList<String>();
    for(var filter:definition.path("filters")) {
      String name=filter.path("name").asText();if(name.matches("[A-Za-z]{1,60}"))result.add(name+" (registered; arguments redacted)");
    }
    return result.stream().limit(16).toList();
  }

  /** Cancels oversized streams before buffering; caller's deadline covers even stalled streams. */
  static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
    private final CompletableFuture<byte[]> body=new CompletableFuture<>();
    private final ByteArrayOutputStream bytes=new ByteArrayOutputStream();
    private Flow.Subscription subscription;
    public CompletionStage<byte[]> getBody(){return body;}
    public void onSubscribe(Flow.Subscription value){subscription=value;value.request(1);}
    public void onNext(List<ByteBuffer> buffers){
      for(var buffer:buffers){
        if(bytes.size()+buffer.remaining()>65536){subscription.cancel();body.completeExceptionally(new IllegalArgumentException("SOURCE_TOO_LARGE"));return;}
        var chunk=new byte[buffer.remaining()];buffer.get(chunk);bytes.writeBytes(chunk);
      }
      subscription.request(1);
    }
    public void onError(Throwable error){body.completeExceptionally(error);}
    public void onComplete(){body.complete(bytes.toByteArray());}
  }
  @Override public void close(){http.shutdownNow();}
}
