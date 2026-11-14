package com.apex.operations.owner;

import com.apex.operations.contracts.OwnerSearch.*;
import com.apex.platform.grpc.api.GatewayException;
import javax.sql.DataSource;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;

/** Executes only trusted owner-authored sources. No SQL or endpoint is accepted on the wire. */
public final class BoundedOwnerSearch {
    public record Candidate(String key, UUID ref, String state, Instant time) {}
    @FunctionalInterface public interface Source {
        List<Candidate> candidates(Connection connection, Query query, String after) throws SQLException;
    }
    private final DataSource db;
    private final String environment, owner, instance;
    private final UUID epoch=UUID.randomUUID();
    private final Map<Kind,Set<String>> scopes, states;
    private final Source source;
    private final Clock clock;
    private final LongSupplier nanos;
    private final SearchCursor cursors;
    private final Semaphore admission=new Semaphore(1);
    private long lastStarted=Long.MIN_VALUE;

    public BoundedOwnerSearch(DataSource db,String environment,String owner,String instance,
                              Map<Kind,Set<String>> scopes,Map<Kind,Set<String>> states,Source source) {
        this(db,environment,owner,instance,scopes,states,source,Clock.systemUTC(),System::nanoTime);
    }
    public BoundedOwnerSearch(DataSource db,String environment,String owner,String instance,
                              Map<Kind,Set<String>> scopes,Map<Kind,Set<String>> states,Source source,
                              Clock clock,LongSupplier nanos) {
        this.db=db;this.environment=environment;this.owner=owner;this.instance=instance;
        this.scopes=copy(scopes);this.states=copy(states);this.source=source;this.clock=clock;this.nanos=nanos;
        this.cursors=new SearchCursor(clock);
    }
    private static Map<Kind,Set<String>> copy(Map<Kind,Set<String>> map) {
        var result=new EnumMap<Kind,Set<String>>(Kind.class);map.forEach((k,v)->result.put(k,Set.copyOf(v)));return Map.copyOf(result);
    }
    public Page read(Query query) {
        if(!environment.equals(query.environment()) || !owner.equals(query.owner())
                || !scopes.getOrDefault(query.kind(),Set.of()).contains(query.scope()))
            throw new GatewayException(GatewayException.Code.PERMISSION_DENIED,false);
        if(!query.state().equals("ANY") && !states.getOrDefault(query.kind(),Set.of()).contains(query.state()))
            throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT,false);
        Instant now=clock.instant();
        if(query.fromInclusive().isBefore(now.minusSeconds(3600)) || query.toExclusive().isAfter(now.plusSeconds(1)))
            throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT,false);
        final SearchCursor.Position position;
        try { position=cursors.read(query); }
        catch(IllegalArgumentException invalid) { throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT,false); }
        if(!admission.tryAcquire()) throw new GatewayException(GatewayException.Code.RESOURCE_EXHAUSTED,false);
        long started=System.nanoTime();
        long admittedAt=nanos.getAsLong();
        try {
            if(lastStarted!=Long.MIN_VALUE && admittedAt-lastStarted<1_000_000_000L)
                throw new GatewayException(GatewayException.Code.RESOURCE_EXHAUSTED,false);
            lastStarted=admittedAt;
            List<Candidate> candidates;
            try(var c=db.getConnection()) {
                c.setReadOnly(true);c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);c.setAutoCommit(false);
                try(var guard=c.createStatement()) {
                    guard.setQueryTimeout(1);
                    guard.execute("SET LOCAL statement_timeout = '250ms'");
                    guard.execute("SET LOCAL lock_timeout = '100ms'");
                    guard.execute("SET LOCAL idle_in_transaction_session_timeout = '1000ms'");
                }
                candidates=List.copyOf(source.candidates(c,query,position.key()));
                c.rollback();
            } catch(SQLException unavailable) {
                String state=unavailable.getSQLState();
                var code="42501".equals(state)?GatewayException.Code.PERMISSION_DENIED:
                    "55P03".equals(state)?GatewayException.Code.RESOURCE_EXHAUSTED:
                    "57014".equals(state)?GatewayException.Code.DEADLINE_EXCEEDED:GatewayException.Code.UNAVAILABLE;
                throw new GatewayException(code,true);
            }
            if(candidates.size()>query.limit() || System.nanoTime()-started>750_000_000L)
                throw new GatewayException(GatewayException.Code.DEADLINE_EXCEEDED,true);
            var items=new ArrayList<Item>();
            for(var row:candidates) {
                if(!row.time().isBefore(query.fromInclusive()) && row.time().isBefore(query.toExclusive())
                        && (query.state().equals("ANY") || query.state().equals(row.state()))) {
                    boolean known=states.getOrDefault(query.kind(),Set.of()).contains(row.state());
                    items.add(new Item(row.ref(),known?row.state():"UNKNOWN",row.time(),query.kind()+":"+row.ref(),
                            Evidence.OWNER_COMMITTED_ROW,known?Certainty.VERIFIED_WITHIN_SCOPE:Certainty.UNKNOWN));
                }
            }
            var gaps=EnumSet.of(Gap.PARTIAL_CURRENT_STATE,Gap.RETENTION_UNQUALIFIED,Gap.RECONCILE_FROM_START);
            String next="";
            if(candidates.size()==query.limit()) {
                gaps.add(Gap.CANDIDATE_PAGE_LIMIT);
                if(position.page()>=8) gaps.add(Gap.WALK_LIMIT);
                else next=cursors.next(query,position,candidates.getLast().key());
            }
            Instant completed=clock.instant();
            if(completed.isBefore(now)) gaps.add(Gap.CLOCK_UNCERTAIN);
            return new Page(1,environment,owner,instance,epoch,query.kind(),query.scope(),query.fromInclusive(),query.toExclusive(),
                    now,completed,now,10000,candidates.size(),List.copyOf(gaps),items,next,"RELATION_UNKNOWN");
        } finally { admission.release(); }
    }
}
