package com.apex.platform.contracts;

import com.apex.operations.contracts.*;
import com.apex.operations.contracts.ContractManifest.*;
import com.apex.platform.grpc.api.CellQueryHandler;
import com.apex.platform.grpc.internal.HandlerRegistry;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.jdbc.*;
import java.time.Instant;
import java.util.*;
import org.springframework.core.env.Environment;

public final class ContractDiscoveryProvider {
  private final Environment env;
  private final PublisherRouting routing;
  private final EventHandlerRegistry events;
  private final HandlerRegistry grpc;
  private final List<PublishedContracts> catalogs;
  private UUID epoch = UUID.randomUUID();
  private List<com.apex.platform.grpc.api.GrpcCallerDeclarations> callers=List.of();
  ContractDiscoveryProvider withCallers(List<com.apex.platform.grpc.api.GrpcCallerDeclarations> value){callers=List.copyOf(value);return this;}
  ContractDiscoveryProvider withIdentity(com.apex.platform.discovery.CellRuntimeIdentity identity){if(identity!=null)epoch=identity.epoch();return this;}

  public ContractDiscoveryProvider(Environment env, PublisherRouting routing, EventHandlerRegistry events,
      HandlerRegistry grpc, List<PublishedContracts> catalogs) {
    this.env = env; this.routing = routing; this.events = events; this.grpc = grpc; this.catalogs = List.copyOf(catalogs);
  }

  @CellQueryHandler(type = "OPS_CONTRACT_DISCOVERY", version = 1)
  public ContractManifest discover(ContractDiscoveryQuery query) {
    return collect(1,false);
  }
  @CellQueryHandler(type="OPS_CONTRACT_DISCOVERY",version=2)
  public ContractManifest discoverCommunication(ContractDiscoveryQueryV2 query) {
    return collect(2,true);
  }
  private ContractManifest collect(int version,boolean includeCallers) {
    var publications = new ArrayList<Publication>();
    var gaps = new ArrayList<String>();
    boolean messaging = env.getProperty("apex.messaging.cell.enabled", Boolean.class, false);
    if (messaging && env.getProperty("apex.messaging.outbox.publisher.enabled", Boolean.class, true)) {
      if (routing.enabled()) {
        for (var route : routing.routes()) publications.add(new Publication(
            new Contract(route.eventType(), route.eventVersion()), route.topic(), "PUBLISHER_ROUTING"));
      } else {
        for (var catalog : catalogs) for (var contract : catalog.contracts()) publications.add(new Publication(
            new Contract(contract.eventType(), contract.eventVersion()),
            env.getRequiredProperty("apex.messaging.publisher.topic"), "PUBLISHED_CONTRACT_CATALOG"));
        if (publications.isEmpty()) gaps.add("PUBLISHER_CONTRACT_CATALOG_UNAVAILABLE");
      }
    }
    boolean consuming = messaging && env.getProperty("apex.messaging.consumer.enabled", Boolean.class, false);
    var subscriptions = consuming ? List.of(ConsumerTopics.parse(env.getRequiredProperty("apex.messaging.consumer.topics"))) : List.<String>of();
    List<Contract> handlers = List.of();
    if (consuming && events != null) {
      try { handlers = events.descriptors().stream().map(k -> new Contract(k.eventType(), k.eventVersion())).toList(); }
      catch (UnsupportedOperationException unavailable) { gaps.add("EVENT_HANDLER_DESCRIPTORS_UNAVAILABLE"); }
    }
    if (consuming && events == null) gaps.add("EVENT_HANDLER_REGISTRY_UNAVAILABLE");
    if (consuming && !env.getProperty("spring.kafka.listener.auto-startup", Boolean.class, true)) gaps.add("LISTENER_AUTO_START_DISABLED");
    if (grpc == null) gaps.add("GRPC_HANDLER_REGISTRY_UNAVAILABLE");
    if (!env.getProperty("apex.grpc.server.enabled",Boolean.class,false)) gaps.add("GRPC_SERVER_DISABLED");
    if (callers.isEmpty()) gaps.add("GRPC_CALLER_DECLARATIONS_UNAVAILABLE");
    if(!includeCallers)gaps.add("GRPC_CALLER_METADATA_REQUIRES_EXTENDED_MANIFEST");
    String cluster = env.getProperty("apex.contract-discovery.cluster", "unknown");
    if (cluster.equals("unknown")) gaps.add("KAFKA_CLUSTER_IDENTITY_UNKNOWN");
    return new ContractManifest(version, env.getProperty("apex.contract-discovery.environment", "unknown"),
        env.getRequiredProperty("spring.application.name"), env.getRequiredProperty("apex.cell.id"),
        env.getProperty("apex.contract-discovery.instance", "unknown"), epoch, Instant.now(), "EFFECTIVE_DECLARATION",
        cluster, publications, subscriptions, consuming ? env.getRequiredProperty("spring.kafka.consumer.group-id") : "",
        handlers, grpc == null ? List.of() : grpc.descriptors().stream()
            .map(d -> new Rpc(d.kind(), new Contract(d.type(), d.version()))).toList(), gaps, includeCallers?dependencies():List.of());
  }

  private List<GrpcDependency> dependencies() {
    var declarations=new ArrayList<GrpcDependency>();
    for(var catalog:callers)for(var dependency:catalog.dependencies()) {
      boolean query=dependency.contract().isAnnotationPresent(com.apex.platform.grpc.api.CellQuery.class);
      var contract=com.apex.platform.grpc.internal.Contracts.inspect(dependency.contract(),query);
      String prefix="apex.grpc.destinations."+dependency.destination();
      boolean local=env.getProperty(prefix+".local",Boolean.class,false);
      if(local)continue; // An in-process call is not a distributed gRPC dependency.
      boolean client=env.getProperty("apex.grpc.client.enabled",Boolean.class,false);
      boolean discovery=env.getProperty("apex.discovery.provider","static").equals("eureka")
          || env.getProperty("apex.discovery.provider","static").equals("kubernetes")
          || env.getProperty("apex.discovery.provider","static").equals("grpc");
      String availability=client?(discovery?"DISCOVERY_CONFIGURED":env.containsProperty(prefix+".channel")?"CONFIGURED":"TRANSPORT_NOT_CONFIGURED"):"TRANSPORT_NOT_CONFIGURED";
      declarations.add(new GrpcDependency(query?"QUERY":"REQUEST",new Contract(contract.type(),contract.version()),
          dependency.destination(),dependency.destination(),dependency.behavior(),availability));
    }
    return declarations.stream().distinct().toList();
  }
}
