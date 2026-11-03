package com.apex.operations.console;

import com.apex.operations.contracts.OwnerSearch.*;
import java.time.*;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.*;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/ops/v1")
@ConditionalOnProperty(name="apex.ops03c.search-enabled",havingValue="true")
public final class SearchApi {
    private final OwnerSearchClient client;
    public SearchApi(OwnerSearchClient client) { this.client=client; }
    @GetMapping("/events") public OwnerSearchClient.View events(@RequestParam MultiValueMap<String,String> p) { return read(Kind.OUTBOX_EVENT,p); }
    @GetMapping("/inbox") public OwnerSearchClient.View inbox(@RequestParam MultiValueMap<String,String> p) { return read(Kind.KNOWN_GROUP_INBOX,p); }
    @GetMapping("/requests") public OwnerSearchClient.View requests(@RequestParam MultiValueMap<String,String> p) { return read(Kind.REQUEST_RECEIPT,p); }
    @GetMapping("/workflows") public OwnerSearchClient.View workflows(@RequestParam MultiValueMap<String,String> p) { return read(Kind.WORKFLOW,p); }
    @GetMapping("/search/sources") public List<OwnerSearchClient.Grant> sources() { return client.grants(); }
    @GetMapping(value="/search/openapi",produces="application/json") public ResponseEntity<org.springframework.core.io.Resource> openapi() {
        return ResponseEntity.ok(new org.springframework.core.io.ClassPathResource("ops03c-openapi.json"));
    }
    private OwnerSearchClient.View read(Kind kind,MultiValueMap<String,String> params) {
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
