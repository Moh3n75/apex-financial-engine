package com.apex.operations.owner;

import com.apex.operations.contracts.OwnerRecentSearch.*;
import com.apex.operations.owner.BoundedRecentSearch.Candidate;
import java.sql.*;
import java.time.*;
import java.util.*;

/** Only the proven platform metadata schema; time restriction precedes candidate LIMIT. */
public final class MessagingRecentSource {
    private MessagingRecentSource(){}
    public static final Set<String> OUTBOX_STATES=MessagingSearchSource.OUTBOX_STATES;
    public static List<Candidate> read(Connection c,Query q,RecentCursor.Position p)throws SQLException {
        var rows=new ArrayList<Candidate>();
        if(q.kind()==Kind.OUTBOX_EVENT) {
            RecentIndex.require(c,"outbox_event",List.of("created_at DESC","id DESC"));
            String sql="SELECT id,event_id,published,dead_letter,next_retry_at,claimed_until,created_at FROM public.outbox_event WHERE created_at>=? AND created_at<?"+
                    (p.key().isEmpty()?"":" AND (created_at,id)<(?,?)")+" ORDER BY created_at DESC,id DESC LIMIT ?";
            try(var s=c.prepareStatement(sql)) {
                s.setQueryTimeout(1);s.setObject(1,q.fromInclusive().atOffset(ZoneOffset.UTC));s.setObject(2,q.toExclusive().atOffset(ZoneOffset.UTC));int n=3;
                if(!p.key().isEmpty()){s.setObject(n++,p.time().atOffset(ZoneOffset.UTC));s.setLong(n++,Long.parseLong(p.key()));}s.setInt(n,q.limit());
                try(var r=s.executeQuery()){while(r.next()) {
                    var lease=r.getObject(6,OffsetDateTime.class);
                    String state=r.getBoolean(3)?"PUBLISHER_MARK_COMMITTED":r.getBoolean(4)?"PUBLISHER_DEAD_COMMITTED":
                            lease!=null&&lease.toInstant().isAfter(Instant.now())?"LEASED_OBSERVED":r.getObject(5)!=null?"RETRY_COMMITTED":"OUTBOX_COMMITTED";
                    rows.add(new Candidate(r.getString(1),r.getObject(2,UUID.class),state,MessagingEvidenceReader.time(r,7)));
                }}
            }
        }else if(q.kind()==Kind.KNOWN_GROUP_INBOX) {
            RecentIndex.require(c,"processed_event",List.of("consumer_group","processed_at DESC","event_id DESC"));
            String sql="SELECT event_id,processed_at FROM public.processed_event WHERE consumer_group=? AND processed_at>=? AND processed_at<?"+
                    (p.key().isEmpty()?"":" AND (processed_at,event_id)<(?,?)")+" ORDER BY processed_at DESC,event_id DESC LIMIT ?";
            try(var s=c.prepareStatement(sql)) {
                s.setQueryTimeout(1);s.setString(1,q.scope());s.setObject(2,q.fromInclusive().atOffset(ZoneOffset.UTC));s.setObject(3,q.toExclusive().atOffset(ZoneOffset.UTC));int n=4;
                if(!p.key().isEmpty()){s.setObject(n++,p.time().atOffset(ZoneOffset.UTC));s.setObject(n++,UUID.fromString(p.key()));}s.setInt(n,q.limit());
                try(var r=s.executeQuery()){while(r.next()){UUID id=r.getObject(1,UUID.class);rows.add(new Candidate(id.toString(),id,"HANDLER_TRANSACTION_COMMITTED",MessagingEvidenceReader.time(r,2)));}}
            }
        }else throw new IllegalArgumentException("RECENT_KIND");
        return List.copyOf(rows);
    }
}
