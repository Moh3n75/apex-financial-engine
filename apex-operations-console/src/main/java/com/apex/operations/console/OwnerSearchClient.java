package com.apex.operations.console;

import com.apex.operations.contracts.OwnerSearch;
import com.apex.operations.contracts.OwnerSearch.*;
import com.apex.platform.grpc.api.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

/** One owner per on-demand query. No scheduler, broad polling, SQL or durable-history claim. */
@Component
@ConditionalOnProperty(name="apex.ops03c.search-enabled",havingValue="true")
public final class OwnerSearchClient {
    public record Grant(String environment,String owner,Kind kind,String scope) {}
    public record View(Instant asOf,Instant receivedAt,String freshness,String availability,
                       String coverage,Page page,String brokerAck,String quarantine,
                       String uncertainty,List<Page> priorConflicts) {
        public View(Instant asOf,Instant receivedAt,String freshness,String availability,String coverage,Page page) {
            this(asOf,receivedAt,freshness,availability,coverage,page,"UNKNOWN_NO_VERIFIED_RECEIPT","UNKNOWN_NOT_INSPECTED","UNKNOWN",List.of());
        }
    }
    private record Cached(Page page,Instant receivedAt,List<Page> prior,String uncertainty) {}
    private final String environment;
    private final Set<Grant> grants;
    private final QueryGateway gateway;
    private final Map<String,Semaphore> admissions=new HashMap<>();
    private final Semaphore global=new Semaphore(2);
    private final LinkedHashMap<String,Cached> cache=new LinkedHashMap<>();
    private final Map<String,Instant> next=new HashMap<>();
    private final JsonMapper mapper=JsonMapper.builder().build();
    public OwnerSearchClient(Environment env,ObjectProvider<QueryGateway> provider) {
        int wireCap=env.getProperty("apex.grpc.max-payload-bytes",Integer.class,65536);
        if(wireCap<16384 || wireCap>65536)throw new IllegalStateException("OPS_SEARCH_WIRE_POLICY");
        environment=env.getRequiredProperty("apex.ops03c.environment");
        if(!environment.matches("[A-Za-z0-9._-]{1,64}"))throw new IllegalArgumentException("SEARCH_INVENTORY");
        var configured=new HashSet<Grant>();
        for(String entry:env.getRequiredProperty("apex.ops03c.sources").split(",")) {
            String[] p=entry.split("/",-1);
            if(p.length!=3 || !p[0].matches("[A-Za-z0-9._-]{1,64}") || !p[2].matches("[A-Za-z0-9._-]{1,200}"))
                throw new IllegalArgumentException("SEARCH_INVENTORY");
            if(!configured.add(new Grant(environment,p[0],Kind.valueOf(p[1]),p[2])) || configured.size()>32)
                throw new IllegalArgumentException("SEARCH_INVENTORY");
            admissions.putIfAbsent(p[0],new Semaphore(1));
            if(admissions.size()>8)throw new IllegalArgumentException("SEARCH_INVENTORY");
        }
        grants=Set.copyOf(configured);gateway=provider.getIfAvailable();
        if(gateway==null)throw new IllegalStateException("OWNER_TRANSPORT_REQUIRED");
    }
    public List<Grant> grants() { return grants.stream().sorted(Comparator.comparing(g->g.owner()+g.kind()+g.scope())).toList(); }
    public View read(Query query) {
        if(!grants.contains(new Grant(query.environment(),query.owner(),query.kind(),query.scope())))
            throw new ResponseStatusException(HttpStatus.FORBIDDEN);
        if(query.fromInclusive().isBefore(Instant.now().minusSeconds(3600)) || query.toExclusive().isAfter(Instant.now().plusSeconds(1)))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        String key=query.binding()+"|"+query.cursor();
        Semaphore owner=admissions.get(query.owner());
        if(!owner.tryAcquire())return failed(key,"SHED");
        boolean admitted=false;
        try {
            admitted=global.tryAcquire();
            if(!admitted)return failed(key,"SHED");
            synchronized(next) {if(Instant.now().isBefore(next.getOrDefault(query.owner(),Instant.EPOCH)))return failed(key,"BACKOFF");}
            try {
                Page page=gateway.query(query.owner(),query,Page.class,new QueryOptions(UUID.randomUUID(),Duration.ofMillis(1000)));
                if(!page.environment().equals(query.environment()) || !page.owner().equals(query.owner())
                        || page.kind()!=query.kind() || !page.scope().equals(query.scope())
                        || !page.fromInclusive().equals(query.fromInclusive()) || !page.toExclusive().equals(query.toExclusive())
                        || page.candidatesExamined()>query.limit() || mapper.writeValueAsBytes(page).length>OwnerSearch.MAX_RESPONSE_BYTES)
                    throw new IllegalArgumentException("SEARCH_SOURCE_SCOPE");
                for(var item:page.items())if(!query.state().equals("ANY")&&!query.state().equals(item.state()))throw new IllegalArgumentException("SEARCH_SOURCE_STATE");
                Cached sample;
                synchronized(cache) {
                    var previous=cache.get(key);var prior=new ArrayList<Page>();if(previous!=null)prior.addAll(previous.prior());
                    String uncertainty="PARTIAL_NO_OWNER_REVISION";
                    if(previous!=null && (page.asOf().isBefore(previous.page().asOf()) || page.asOf().equals(previous.page().asOf())&&!page.equals(previous.page()))) {
                        if(!prior.contains(page))prior.add(page);uncertainty="CONFLICTING";
                        page=previous.page();sample=new Cached(page,previous.receivedAt(),bounded(prior),uncertainty);
                    } else {
                        if(previous!=null && (!previous.page().items().equals(page.items())||!previous.page().epoch().equals(page.epoch())))prior.add(previous.page());
                        for(var old:prior)if(old.asOf().equals(page.asOf())&&!old.equals(page))uncertainty="CONFLICTING";
                        sample=new Cached(page,Instant.now(),bounded(prior),uncertainty);
                    }
                    cache.put(key,sample);while(cache.size()>16)cache.remove(cache.keySet().iterator().next());
                }
                synchronized(next) {next.remove(query.owner());}
                return view(sample,"AVAILABLE");
            } catch(GatewayException unavailable) {
                if(unavailable.code()==GatewayException.Code.PERMISSION_DENIED || unavailable.code()==GatewayException.Code.UNAUTHENTICATED)
                    return new View(Instant.now(),null,"UNKNOWN","PERMISSION_GAP","PARTIAL_NO_DATA_AUTHORIZATION_REVALIDATED",null);
                if(unavailable.code()==GatewayException.Code.INVALID_ARGUMENT)throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
                synchronized(next) {next.put(query.owner(),Instant.now().plusSeconds(2));}
                return failed(key,unavailable.code()==GatewayException.Code.RESOURCE_EXHAUSTED?"SHED":"SOURCE_UNAVAILABLE");
            } catch(Exception invalid) {
                synchronized(next) {next.put(query.owner(),Instant.now().plusSeconds(2));}
                return failed(key,"SOURCE_UNAVAILABLE");
            }
        } finally {if(admitted)global.release();owner.release();}
    }
    private static List<Page> bounded(List<Page> pages) {return List.copyOf(pages.subList(Math.max(0,pages.size()-2),pages.size()));}
    private View failed(String key,String code) {
        Cached last;synchronized(cache) {last=cache.get(key);}
        return last==null?new View(Instant.now(),null,"MISSING",code,"PARTIAL_UNKNOWN_NOT_NOT_FOUND",null):view(last,code);
    }
    private View view(Cached value,String code) {
        Instant now=Instant.now();long age=Duration.between(value.page().asOf(),now).toMillis();
        String freshness=age< -1000||value.page().coverage().contains(Gap.CLOCK_UNCERTAIN)?"CLOCK_UNCERTAIN":!code.equals("AVAILABLE")||age>value.page().maxAgeMillis()?"STALE":"FRESH";
        var result=new View(now,value.receivedAt(),freshness,code,"PARTIAL_CURRENT_STATE_NO_HISTORY_EPHEMERAL",value.page(),
                "UNKNOWN_NO_VERIFIED_RECEIPT","UNKNOWN_NOT_INSPECTED",value.uncertainty(),value.prior());
        if(mapper.writeValueAsBytes(result).length>65536)return new View(now,null,"UNKNOWN","SOURCE_UNAVAILABLE","PARTIAL_RESPONSE_BYTE_LIMIT",null);
        return result;
    }
}
