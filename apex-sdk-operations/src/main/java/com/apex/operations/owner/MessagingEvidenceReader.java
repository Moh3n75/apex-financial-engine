package com.apex.operations.owner;
import com.apex.operations.contracts.OwnerSnapshot.*;
import java.sql.*;import java.time.*;import java.util.*;
/** Owner-side only. Caller supplies an independent read-only READ COMMITTED connection. */
public final class MessagingEvidenceReader {
 private final Set<String> groups;
 public MessagingEvidenceReader(Set<String> groups){this.groups=Set.copyOf(groups);if(groups.size()>4||groups.stream().anyMatch(g->!g.matches("[A-Za-z0-9._-]{1,200}")))throw new IllegalArgumentException("GROUPS");}
 public List<Fact> read(Connection c,List<UUID> ids)throws SQLException {
 if(ids.size()>16||!c.isReadOnly()||c.getTransactionIsolation()!=Connection.TRANSACTION_READ_COMMITTED)throw new IllegalArgumentException("READ_POLICY");
 var result=new ArrayList<Fact>();
 for(UUID id:ids){
 try(var q=c.prepareStatement("SELECT published,dead_letter,claimed_until,next_retry_at,occurred_at FROM outbox_event WHERE event_id=?")){
 q.setQueryTimeout(1);q.setObject(1,id);try(var r=q.executeQuery()){
 if(r.next()) {var lease=r.getObject(3,OffsetDateTime.class);var retry=r.getObject(4,OffsetDateTime.class);
 String state=r.getBoolean(1)?"PUBLISHED_MARK":r.getBoolean(2)?"PUBLISHER_DEAD":lease!=null&&lease.toInstant().isAfter(Instant.now())?"LEASED":retry!=null?"RETRY":"PENDING";
 result.add(new Fact(Kind.OUTBOX,id,"owner",state,EvidenceClass.OWNER_COMMITTED_ROW,Certainty.VERIFIED_WITHIN_SCOPE,time(r,5)));
 }else result.add(unknown(Kind.OUTBOX,id,"owner"));}}
 for(String group:groups)try(var q=c.prepareStatement("SELECT processed_at FROM processed_event WHERE consumer_group=? AND event_id=?")){
 q.setQueryTimeout(1);q.setString(1,group);q.setObject(2,id);try(var r=q.executeQuery()){
 result.add(r.next()?new Fact(Kind.INBOX,id,group,"COMMITTED",EvidenceClass.OWNER_COMMITTED_ROW,Certainty.VERIFIED_WITHIN_SCOPE,time(r,1)):unknown(Kind.INBOX,id,group));}}
 }
 return List.copyOf(result);
 }
 public static Fact unknown(Kind kind,UUID id,String scope){return new Fact(kind,id,scope,"UNKNOWN",EvidenceClass.OWNER_COMMITTED_ROW,Certainty.UNKNOWN,null);}
 public static Instant time(ResultSet r,int index)throws SQLException{var t=r.getObject(index,OffsetDateTime.class);return t==null?null:t.toInstant();}
}
