package com.apex.operations.owner;

import com.apex.operations.contracts.OwnerSearch.*;
import com.apex.operations.owner.BoundedOwnerSearch.Candidate;
import java.sql.*;
import java.time.*;
import java.util.*;

/** Proven common platform schemas only. LIMIT bounds candidates before time/state filtering. */
public final class MessagingSearchSource {
    private MessagingSearchSource() {}
    public static final Set<String> OUTBOX_STATES=Set.of("OUTBOX_COMMITTED","PUBLISHER_MARK_COMMITTED","PUBLISHER_DEAD_COMMITTED","RETRY_COMMITTED","LEASED_OBSERVED");
    public static List<Candidate> read(Connection c,Query query,String after) throws SQLException {
        var rows=new ArrayList<Candidate>();
        if(query.kind()==Kind.OUTBOX_EVENT) {
            try(var statement=c.prepareStatement("SELECT id,event_id,published,dead_letter,next_retry_at,claimed_until,created_at FROM outbox_event WHERE id > ? ORDER BY id LIMIT ?")) {
                statement.setQueryTimeout(1);statement.setLong(1,after.isEmpty()?0:Long.parseLong(after));statement.setInt(2,query.limit());
                try(var r=statement.executeQuery()) { while(r.next()) {
                    var lease=r.getObject(6,OffsetDateTime.class);
                    String state=r.getBoolean(3)?"PUBLISHER_MARK_COMMITTED":r.getBoolean(4)?"PUBLISHER_DEAD_COMMITTED":
                        lease!=null && lease.toInstant().isAfter(Instant.now())?"LEASED_OBSERVED":r.getObject(5)!=null?"RETRY_COMMITTED":"OUTBOX_COMMITTED";
                    rows.add(new Candidate(r.getString(1),r.getObject(2,UUID.class),state,MessagingEvidenceReader.time(r,7)));
                }}
            }
        } else if(query.kind()==Kind.KNOWN_GROUP_INBOX) {
            // Separate first-page SQL includes the all-zero UUID rather than excluding a legal key.
            String sql=after.isEmpty()?"SELECT event_id,processed_at FROM processed_event WHERE consumer_group=? ORDER BY event_id LIMIT ?":
                "SELECT event_id,processed_at FROM processed_event WHERE consumer_group=? AND event_id > ? ORDER BY event_id LIMIT ?";
            try(var statement=c.prepareStatement(sql)) {
                statement.setQueryTimeout(1);statement.setString(1,query.scope());int n=2;
                if(!after.isEmpty()) statement.setObject(n++,UUID.fromString(after));statement.setInt(n,query.limit());
                try(var r=statement.executeQuery()) { while(r.next()) {
                    UUID id=r.getObject(1,UUID.class);rows.add(new Candidate(id.toString(),id,"HANDLER_TRANSACTION_COMMITTED",MessagingEvidenceReader.time(r,2)));
                }}
            }
        } else throw new IllegalArgumentException("SEARCH_KIND");
        return List.copyOf(rows);
    }
}
