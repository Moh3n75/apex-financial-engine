package com.apex.payment.operations;

import com.apex.operations.contracts.OwnerSearch.*;
import com.apex.operations.owner.*;
import com.apex.operations.owner.BoundedOwnerSearch.Candidate;
import com.apex.platform.grpc.api.*;
import java.util.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** Current owner metadata only; no source writes, payloads or transition history. */
@Component
@ConditionalOnProperty(name={"apex.ops03.owner-enabled","apex.ops03c.search-enabled"},havingValue="true")
public final class PaymentSearchHandler {
    private final BoundedOwnerSearch reader;
    public PaymentSearchHandler(PaymentEvidenceDataSource.Pool pool,Environment env) {
        if(!env.getProperty("apex.grpc.security.enabled",Boolean.class,false)
                || env.getProperty("apex.grpc.insecure-loopback-test-only",Boolean.class,false))
            throw new IllegalStateException("OPS_SEARCH_REQUIRES_MTLS_ACL");
        int wireCap=env.getProperty("apex.grpc.max-payload-bytes",Integer.class,65536);
        if(wireCap<16384 || wireCap>65536)throw new IllegalStateException("OPS_SEARCH_WIRE_POLICY");
        String groups=env.getProperty("apex.ops03.known-groups", "");
        Set<String> known=groups.isBlank()?Set.of():Set.of(groups.split(","));
        new MessagingEvidenceReader(known); // same proven group bounds, never enumerate Kafka
        var registered=new EnumMap<Kind,Set<String>>(Kind.class);
        registered.put(Kind.OUTBOX_EVENT,Set.of("owner"));registered.put(Kind.KNOWN_GROUP_INBOX,known);
        registered.put(Kind.WORKFLOW,Set.of("authorization-simulation"));

        var enabled=new EnumMap<Kind,Set<String>>(Kind.class);
        String configured=env.getRequiredProperty("apex.ops03c.scopes");
        for(String entry:configured.split(",")) {
            String[] pair=entry.split(":",-1);
            if(pair.length!=2) throw new IllegalArgumentException("SEARCH_SCOPE");
            Kind kind=Kind.valueOf(pair[0]);
            if(!registered.getOrDefault(kind,Set.of()).contains(pair[1])) throw new IllegalArgumentException("SEARCH_SCOPE");
            enabled.computeIfAbsent(kind,k->new HashSet<>()).add(pair[1]);
        }
        var states=new EnumMap<Kind,Set<String>>(Kind.class);
        states.put(Kind.OUTBOX_EVENT,MessagingSearchSource.OUTBOX_STATES);
        states.put(Kind.KNOWN_GROUP_INBOX,Set.of("HANDLER_TRANSACTION_COMMITTED"));
        states.put(Kind.WORKFLOW,Set.of("PENDING","AUTHORIZED","DECLINED"));
        states.put(Kind.REQUEST_RECEIPT,Set.of("RECEIPT_COMMITTED","RECEIPT_INCOMPLETE"));
        reader=new BoundedOwnerSearch(pool.dataSource(),env.getRequiredProperty("apex.ops03.environment"),"payment-cell",
                env.getRequiredProperty("apex.ops03.instance"),enabled,states,(c,q,after)-> {
            if(q.kind()==Kind.OUTBOX_EVENT || q.kind()==Kind.KNOWN_GROUP_INBOX) return MessagingSearchSource.read(c,q,after);

            String sql=after.isEmpty()?"SELECT order_id,status,created_at FROM payment_authorizations ORDER BY order_id LIMIT ?":
                "SELECT order_id,status,created_at FROM payment_authorizations WHERE order_id > ? ORDER BY order_id LIMIT ?";
            var rows=new ArrayList<Candidate>();
            try(var statement=c.prepareStatement(sql)) {
                statement.setQueryTimeout(1);int n=1;
                if(!after.isEmpty())statement.setObject(n++,UUID.fromString(after));statement.setInt(n,q.limit());
                try(var r=statement.executeQuery()) {while(r.next()) {
                    UUID id=r.getObject(1,UUID.class);rows.add(new Candidate(id.toString(),id,r.getString(2),MessagingEvidenceReader.time(r,3)));
                }}
            }
            return rows;
        });
    }
    @CellQueryHandler(type="OPS_OWNER_SEARCH",version=1)
    public Page search(Query query) { return reader.read(query); }
}
