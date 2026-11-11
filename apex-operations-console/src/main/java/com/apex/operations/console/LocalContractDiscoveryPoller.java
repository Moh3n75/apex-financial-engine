package com.apex.operations.console;

import com.apex.discovery.eureka.EurekaCellAutoConfiguration;
import com.apex.operations.contracts.ContractManifest;
import com.apex.operations.contracts.TopologySnapshot.Provider;
import com.netflix.discovery.*;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.*;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Opt-in local development read of effective in-process manifests, enumerated from the existing registry cache. */
@Component
@Profile("local")
@ConditionalOnProperty(name="apex.ops.local-contracts.enabled",havingValue="true")
public final class LocalContractDiscoveryPoller implements AutoCloseable {
  private final ObjectProvider<EurekaClient> clients;
  private final ObjectProvider<EurekaClientConfig> configs;
  private final String environment,cluster;
  private final ScheduledExecutorService timer=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("ops-local-contract-read").factory());
  private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).followRedirects(HttpClient.Redirect.NEVER).build();
  private final ObjectMapper json=new ObjectMapper().findAndRegisterModules();
  private volatile List<ContractDiscoveryPoller.View> cache=List.of();
  private volatile Provider coverage=new Provider("local-contract-discovery","MISSING","NO_REGISTRY_SAMPLE","LOCAL_HTTP_NOT_ATTESTED",null);
  private volatile Instant refreshed;
  public LocalContractDiscoveryPoller(ObjectProvider<EurekaClient> clients,ObjectProvider<EurekaClientConfig> configs,Environment env) {
    this.clients=clients;this.configs=configs;
    environment=env.getRequiredProperty("apex.ops.local-inventory.environment");
    cluster=env.getRequiredProperty("apex.ops.local-contracts.cluster");
    if(!Set.of("local","local-dev").contains(environment)||!Set.of("127.0.0.1","::1").contains(env.getProperty("server.address","127.0.0.1"))
        || !cluster.matches("[A-Za-z0-9._-]{1,64}")||cluster.equals("unknown"))throw new IllegalArgumentException("LOCAL_CONTRACT_SCOPE_REQUIRED");
  }
  @org.springframework.context.event.EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
  public void start(){timer.scheduleWithFixedDelay(this::poll,0,5,TimeUnit.SECONDS);}
  void poll(){
    var now=Instant.now();var views=new ArrayList<ContractDiscoveryPoller.View>();
    try {
      var client=clients.getIfAvailable();var config=configs.getIfAvailable();
      if(client==null||config==null||!LocalEurekaTopologyInventory.loopbackRegistry(config))throw new IllegalStateException();
      long age=EurekaCellAutoConfiguration.registryAge(client);
      if(age<0||age>60000)throw new IllegalStateException();
      var apps=client.getApplications();if(apps==null)throw new IllegalStateException();
      var addresses=new HashSet<String>(Set.of("127.0.0.1","::1"));
      for(var network:Collections.list(NetworkInterface.getNetworkInterfaces()))for(var address:Collections.list(network.getInetAddresses()))addresses.add(address.getHostAddress());
      int polled=0;boolean partial=false;
      for(var app:apps.getRegisteredApplications())for(var instance:app.getInstances()) {
        if(instance.getStatus()!=InstanceStatus.UP||!"true".equals(instance.getMetadata().get("apex-contract-discovery-http")))continue;
        String tagged=instance.getMetadata().get("apex.environment");if(tagged!=null&&!tagged.equals(environment))continue;
        String owner=app.getName().toLowerCase(Locale.ROOT);if(!owner.matches("[a-z][a-z0-9-]{0,62}"))continue;
        if(++polled>16){partial=true;continue;}
        if(!addresses.contains(instance.getIPAddr())||instance.getPort()<1024||instance.getPort()>65535) {
          views.add(new ContractDiscoveryPoller.View(environment,owner,null,"NONLOCAL_ENDPOINT_REJECTED","LOCAL_HTTP"));partial=true;continue;
        }
        try {
          var manifest=read(instance.getPort());validate(manifest,owner,Instant.now());
          boolean knownCluster=!manifest.cluster().equals("unknown");
          views.add(new ContractDiscoveryPoller.View(environment,owner,manifest,knownCluster?"AVAILABLE":"PARTIAL","LOCAL_HTTP"));
          if(!knownCluster)partial=true;
        }catch(Exception unavailable){views.add(new ContractDiscoveryPoller.View(environment,owner,null,"SOURCE_UNAVAILABLE","LOCAL_HTTP"));partial=true;}
      }
      var signatures=new HashMap<String,Set<List<Object>>>();
      for(var view:views)if(view.manifest()!=null){var m=view.manifest();signatures.computeIfAbsent(view.owner(),key->new HashSet<>()).add(
          List.of(new HashSet<>(m.publications()),new HashSet<>(m.subscriptions()),m.consumerGroup(),new HashSet<>(m.eventHandlers()),new HashSet<>(m.grpcHandlers()),new HashSet<>(m.grpcDependencies())));}
      if(signatures.values().stream().anyMatch(s->s.size()>1))partial=true;
      cache=views.stream().map(v->signatures.getOrDefault(v.owner(),Set.of()).size()>1
          ?new ContractDiscoveryPoller.View(v.environment(),v.owner(),v.manifest(),"PARTIAL","LOCAL_HTTP"):v).toList();refreshed=Instant.now();
      coverage=new Provider("local-contract-discovery",partial?"PARTIAL":views.isEmpty()?"NOT_CONFIGURED":"AVAILABLE",
          views.isEmpty()?"NO_ADVERTISED_LOCAL_HTTP_MANIFESTS":"ADVERTISED_LOCAL_INSTANCES_ONLY_NOT_TRANSPORT_ATTESTED","LOCAL_HTTP_NOT_ATTESTED",now);
    }catch(Exception unavailable){
      // Keep past declarations explicitly stale, never refresh their collection times on registry/transport failure.
      cache=cache.stream().map(v->new ContractDiscoveryPoller.View(v.environment(),v.owner(),v.manifest(),"SOURCE_UNAVAILABLE","LOCAL_HTTP")).toList();
      coverage=new Provider("local-contract-discovery","SOURCE_UNAVAILABLE","NO_FRESH_REGISTRY_SAMPLE","LOCAL_HTTP_NOT_ATTESTED",refreshed);
    }
  }
  ContractManifest read(int port) throws Exception {
    var exchange=http.sendAsync(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/ops/v1/contracts"))
        .GET().timeout(Duration.ofMillis(1200)).header("Accept","application/json").build(),response->new GatewayRouteTopology.BoundedBody());
    try {
      var response=exchange.get(1200,TimeUnit.MILLISECONDS);
      if(response.statusCode()!=200)throw new IllegalArgumentException("MANIFEST_UNAVAILABLE");
      return json.readValue(response.body(),ContractManifest.class);
    }finally{if(!exchange.isDone())exchange.cancel(true);}
  }
  void validate(ContractManifest manifest,String owner,Instant now) {
    if(!manifest.environment().equals(environment)||!manifest.service().equals(owner)||!manifest.cell().equals(owner)
        ||(!manifest.cluster().equals(cluster)&&!manifest.cluster().equals("unknown"))||!manifest.sourceMode().equals("EFFECTIVE_DECLARATION")
        ||manifest.collectedAt().isAfter(now.plusSeconds(1))||manifest.collectedAt().isBefore(now.minusSeconds(30)))
      throw new IllegalArgumentException("MANIFEST_SCOPE_OR_FRESHNESS");
  }
  public List<ContractDiscoveryPoller.View> views(){
    return cache.stream().map(v->v.manifest()!=null&&v.manifest().collectedAt().isBefore(Instant.now().minusSeconds(30))
        ?new ContractDiscoveryPoller.View(v.environment(),v.owner(),v.manifest(),"STALE","LOCAL_HTTP"):v).toList();
  }
  public Provider coverage(){return coverage;}
  @Override public void close(){timer.shutdownNow();http.shutdownNow();}
}
