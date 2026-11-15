package compatibility;

import com.apex.platform.core.context.*;
import com.apex.platform.events.*;
import com.apex.platform.events.api.*;
import com.apex.platform.grpc.api.*;
import com.apex.operations.contracts.OwnerSearch;
import java.time.*;
import java.util.*;

/** Compiled once against the installed SDK-0.1 API, then executed without recompilation. */
public class FrozenConsumerV1 {
    public static void main(String[] args) {
        var id=UUID.fromString("00000000-0000-0000-0000-000000000001");
        var context=new ExecutionContext(id,null,"frozen","frozen");
        var provider=new ThreadLocalExecutionContextProvider();
        try(var scope=provider.open(context)) {
            if(!provider.requireCurrent().equals(context))throw new AssertionError();
        }
        var metadata=new EventMetadata(id,"FROZEN",1,id,null,id.toString(),"FROZEN","frozen","frozen",Instant.parse("2026-01-01T00:00:00Z"));
        if(new EventEnvelope<>(metadata,"one").metadata().eventVersion()!=1)throw new AssertionError();
        if(!EventContracts.define("FROZEN",1,"FROZEN",String.class,"frozen.v1").schemaId().equals("frozen.v1"))throw new AssertionError();
        if(!RequestOptions.withIdempotencyKey("frozen").withRequestId(id).requestId().equals(id))throw new AssertionError();
        if(!QueryOptions.defaults().deadline().equals(Duration.ofSeconds(5)))throw new AssertionError();
        var query=new OwnerSearch.Query("test","frozen",OwnerSearch.Kind.OUTBOX_EVENT,"frozen",Instant.parse("2026-01-01T00:00:00Z"),Instant.parse("2026-01-01T00:01:00Z"),"ANY",1,"");
        if(!query.firstPage().equals(query))throw new AssertionError();
        System.out.println("FROZEN_CONSUMER_V1_ACCEPTED");
    }
}
