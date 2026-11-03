package com.apex.order.modules.order.operations;

import com.apex.operations.contracts.OwnerRecentSearch.*;
import com.apex.operations.owner.*;
import com.apex.operations.owner.BoundedRecentSearch.Candidate;
import com.apex.platform.grpc.api.*;
import java.util.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** Current owner metadata only; no source writes, payloads or transition history. */
@Component
@ConditionalOnProperty(name={"apex.ops03.owner-enabled","apex.ops03d.search-enabled"},havingValue="true")
public final class OrderRecentSearchHandler {
    private final BoundedRecentSearch reader;
    public OrderRecentSearchHandler(OrderEvidenceDataSource.Pool pool,Environment env) {
        if(!env.getProperty("apex.grpc.security.enabled",Boolean.class,false)
                || env.getProperty("apex.grpc.insecure-loopback-test-only",Boolean.class,false))
            throw new IllegalStateException("OPS_SEARCH_REQUIRES_MTLS_ACL");
        if(!env.getProperty("apex.ops03d.indexes-reviewed",Boolean.class,false)) throw new IllegalStateException("OPS_RECENT_INDEX_REVIEW_REQUIRED");
        int wireCap=env.getProperty("apex.grpc.max-payload-bytes",Integer.class,65536);
        if(wireCap<16384 || wireCap>65536)throw new IllegalStateException("OPS_SEARCH_WIRE_POLICY");
        String groups=env.getProperty("apex.ops03.known-groups", "");
        Set<String> known=groups.isBlank()?Set.of():Set.of(groups.split(","));
        new MessagingEvidenceReader(known); // same proven group bounds, never enumerate Kafka
        var registered=new EnumMap<Kind,Set<String>>(Kind.class);
        registered.put(Kind.OUTBOX_EVENT,Set.of("owner"));registered.put(Kind.KNOWN_GROUP_INBOX,known);
        registered.put(Kind.WORKFLOW,Set.of("order"));

        String receiptClient=env.getProperty("apex.ops03d.receipt-client", "");
        if(!receiptClient.isEmpty() && !receiptClient.matches("[A-Za-z0-9._-]{1,64}")) throw new IllegalArgumentException("SEARCH_SCOPE");
        if(!receiptClient.isEmpty()) registered.put(Kind.REQUEST_RECEIPT,Set.of("order-request"));

        var enabled=new EnumMap<Kind,Set<String>>(Kind.class);
        String configured=env.getRequiredProperty("apex.ops03d.scopes");
        for(String entry:configured.split(",")) {
            String[] pair=entry.split(":",-1);
            if(pair.length!=2) throw new IllegalArgumentException("SEARCH_SCOPE");
            Kind kind=Kind.valueOf(pair[0]);
            if(!registered.getOrDefault(kind,Set.of()).contains(pair[1])) throw new IllegalArgumentException("SEARCH_SCOPE");
            enabled.computeIfAbsent(kind,k->new HashSet<>()).add(pair[1]);
        }
        var states=new EnumMap<Kind,Set<String>>(Kind.class);
        states.put(Kind.OUTBOX_EVENT,MessagingRecentSource.OUTBOX_STATES);
        states.put(Kind.KNOWN_GROUP_INBOX,Set.of("HANDLER_TRANSACTION_COMMITTED"));
        states.put(Kind.WORKFLOW,Set.of("PENDING_INVENTORY","PENDING_PAYMENT","CANCELLING","CONFIRMED","CANCELLED","CREATED"));
        states.put(Kind.REQUEST_RECEIPT,Set.of("RECEIPT_COMMITTED","RECEIPT_INCOMPLETE"));
        reader=new BoundedRecentSearch(pool.dataSource(),env.getRequiredProperty("apex.ops03.environment"),"order-cell",
                env.getRequiredProperty("apex.ops03.instance"),enabled,states,(c,q,after)-> {
            if(q.kind()==Kind.OUTBOX_EVENT || q.kind()==Kind.KNOWN_GROUP_INBOX) return MessagingRecentSource.read(c,q,after);

            if(q.kind()==Kind.REQUEST_RECEIPT) return receipts(c,q,after,receiptClient);
            return workflow(c,q,after);
        });
    }
    @CellQueryHandler(type="OPS_OWNER_RECENT_SEARCH",version=1)
    public Page search(Query query) { return reader.read(query); }

    private static java.util.List<Candidate> workflow(java.sql.Connection c,Query q,RecentCursor.Position p)throws java.sql.SQLException {
        RecentIndex.require(c,"orders",List.of("created_at DESC","id DESC"));
        String sql="SELECT id,status,created_at FROM public.orders WHERE created_at>=? AND created_at<?"+
                (p.key().isEmpty()?"":" AND (created_at,id)<(?,?)")+" ORDER BY created_at DESC,id DESC LIMIT ?";
        var rows=new ArrayList<Candidate>();
        try(var statement=c.prepareStatement(sql)) {
            statement.setQueryTimeout(1);statement.setObject(1,q.fromInclusive().atOffset(java.time.ZoneOffset.UTC));statement.setObject(2,q.toExclusive().atOffset(java.time.ZoneOffset.UTC));int n=3;
            if(!p.key().isEmpty()){statement.setObject(n++,p.time().atOffset(java.time.ZoneOffset.UTC));statement.setObject(n++,UUID.fromString(p.key()));}statement.setInt(n,q.limit());
            try(var result=statement.executeQuery()){while(result.next()){UUID id=result.getObject(1,UUID.class);rows.add(new Candidate(id.toString(),id,result.getString(2),MessagingEvidenceReader.time(result,3)));}}
        }return rows;
    }

    private static java.util.List<Candidate> receipts(java.sql.Connection c,Query q,RecentCursor.Position p,String client)throws java.sql.SQLException {
        RecentIndex.require(c,"order_request_receipt",List.of("client_id","created_at DESC","request_id DESC"));
        String sql="SELECT request_id,created_at,completed_at FROM public.order_request_receipt WHERE client_id=? AND created_at>=? AND created_at<?"+
                (p.key().isEmpty()?"":" AND (created_at,request_id)<(?,?)")+" ORDER BY created_at DESC,request_id DESC LIMIT ?";
        var rows=new ArrayList<Candidate>();
        try(var statement=c.prepareStatement(sql)) {
            statement.setQueryTimeout(1);statement.setString(1,client);statement.setObject(2,q.fromInclusive().atOffset(java.time.ZoneOffset.UTC));statement.setObject(3,q.toExclusive().atOffset(java.time.ZoneOffset.UTC));int n=4;
            if(!p.key().isEmpty()){statement.setObject(n++,p.time().atOffset(java.time.ZoneOffset.UTC));statement.setObject(n++,UUID.fromString(p.key()));}statement.setInt(n,q.limit());
            try(var result=statement.executeQuery()){while(result.next()){UUID id=result.getObject(1,UUID.class);rows.add(new Candidate(id.toString(),id,result.getObject(3)==null?"RECEIPT_INCOMPLETE":"RECEIPT_COMMITTED",MessagingEvidenceReader.time(result,2)));}}
        }return rows;
    }
}
