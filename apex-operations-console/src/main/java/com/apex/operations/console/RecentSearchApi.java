package com.apex.operations.console;

import com.apex.operations.contracts.OwnerRecentSearch.*;
import java.time.*;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.*;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/ops/v1/recent")
@ConditionalOnProperty(name="apex.ops03d.search-enabled",havingValue="true")
public final class RecentSearchApi {
    private final OwnerRecentSearchClient client;
    public RecentSearchApi(OwnerRecentSearchClient client) { this.client=client; }
    @GetMapping("/events") public OwnerRecentSearchClient.View events(@RequestParam MultiValueMap<String,String> p) { return read(Kind.OUTBOX_EVENT,p); }
    @GetMapping("/inbox") public OwnerRecentSearchClient.View inbox(@RequestParam MultiValueMap<String,String> p) { return read(Kind.KNOWN_GROUP_INBOX,p); }
    @GetMapping("/requests") public OwnerRecentSearchClient.View requests(@RequestParam MultiValueMap<String,String> p) { return read(Kind.REQUEST_RECEIPT,p); }
    @GetMapping("/workflows") public OwnerRecentSearchClient.View workflows(@RequestParam MultiValueMap<String,String> p) { return read(Kind.WORKFLOW,p); }
    @GetMapping("/search/sources") public List<OwnerRecentSearchClient.Grant> sources() { return client.grants(); }
    @GetMapping(value="/search/openapi",produces="application/json") public ResponseEntity<org.springframework.core.io.Resource> openapi() {
        return ResponseEntity.ok(new org.springframework.core.io.ClassPathResource("ops03d-openapi.json"));
    }
    private OwnerRecentSearchClient.View read(Kind kind,MultiValueMap<String,String> params) {
        try {
            var allowed=Set.of("environment","owner","scope","from","to","state","limit","cursor");
            if(!allowed.containsAll(params.keySet()) || params.values().stream().anyMatch(v->v.size()!=1))throw new IllegalArgumentException();
            String from=params.getFirst("from"),to=params.getFirst("to");
            if(from==null||to==null||!from.endsWith("Z")||!to.endsWith("Z"))throw new IllegalArgumentException();
            var q=new Query(params.getFirst("environment"),params.getFirst("owner"),kind,params.getFirst("scope"),
                    Instant.parse(from),Instant.parse(to),params.getFirst("state")==null?"ANY":params.getFirst("state"),
                    params.getFirst("limit")==null?16:Integer.parseInt(params.getFirst("limit")),
                    params.getFirst("cursor")==null?"":params.getFirst("cursor"));
            return client.read(q);
        } catch(ResponseStatusException expected) {throw expected;}
        catch(Exception invalid) {throw new ResponseStatusException(HttpStatus.BAD_REQUEST);}
    }
}
