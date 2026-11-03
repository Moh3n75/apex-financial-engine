package com.apex.order.modules.order.operations;

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
public final class OrderSearchHandler {
    private final BoundedOwnerSearch reader;
    public OrderSearchHandler(OrderEvidenceDataSource.Pool pool,Environment env) {
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
        registered.put(Kind.WORKFLOW,Set.of("order"));

        String receiptClient=env.getProperty("apex.ops03c.receipt-client", "");
        if(!receiptClient.isEmpty() && !receiptClient.matches("[A-Za-z0-9._-]{1,64}")) throw new IllegalArgumentException("SEARCH_SCOPE");
        if(!receiptClient.isEmpty()) registered.put(Kind.REQUEST_RECEIPT,Set.of("order-request"));

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
        states.put(Kind.WORKFLOW,Set.of("PENDING_INVENTORY","PENDING_PAYMENT","CANCELLING","CONFIRMED","CANCELLED","CREATED"));
        states.put(Kind.REQUEST_RECEIPT,Set.of("RECEIPT_COMMITTED","RECEIPT_INCOMPLETE"));
        reader=new BoundedOwnerSearch(pool.dataSource(),env.getRequiredProperty("apex.ops03.environment"),"order-cell",
                env.getRequiredProperty("apex.ops03.instance"),enabled,states,(c,q,after)-> {
            if(q.kind()==Kind.OUTBOX_EVENT || q.kind()==Kind.KNOWN_GROUP_INBOX) return MessagingSearchSource.read(c,q,after);

            if(q.kind()==Kind.REQUEST_RECEIPT) {
                String sql=after.isEmpty()?"SELECT request_id,created_at,completed_at FROM order_request_receipt WHERE client_id=? ORDER BY request_id LIMIT ?":
                    "SELECT request_id,created_at,completed_at FROM order_request_receipt WHERE client_id=? AND request_id > ? ORDER BY request_id LIMIT ?";
                var rows=new ArrayList<Candidate>();
                try(var statement=c.prepareStatement(sql)) {
                    statement.setQueryTimeout(1);statement.setString(1,receiptClient);int n=2;
                    if(!after.isEmpty())statement.setObject(n++,UUID.fromString(after));statement.setInt(n,q.limit());
                    try(var r=statement.executeQuery()) {while(r.next()) {
                        UUID id=r.getObject(1,UUID.class);rows.add(new Candidate(id.toString(),id,r.getObject(3)==null?"RECEIPT_INCOMPLETE":"RECEIPT_COMMITTED",MessagingEvidenceReader.time(r,2)));
                    }}
                }
                return rows;
            }

            String sql=after.isEmpty()?"SELECT id,status,created_at FROM orders ORDER BY id LIMIT ?":
                "SELECT id,status,created_at FROM orders WHERE id > ? ORDER BY id LIMIT ?";
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
