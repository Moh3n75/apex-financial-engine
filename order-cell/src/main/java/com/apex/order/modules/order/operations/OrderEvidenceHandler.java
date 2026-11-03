package com.apex.order.modules.order.operations;
import com.apex.operations.contracts.*;import com.apex.operations.contracts.OwnerSnapshot.*;
import com.apex.operations.owner.MessagingEvidenceReader;import com.apex.observability.ActiveOperations;
import com.apex.platform.grpc.api.*;
import javax.sql.DataSource;import java.sql.*;import java.time.*;import java.util.*;import java.util.concurrent.Semaphore;
import org.springframework.beans.factory.ObjectProvider;import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
@Component
@ConditionalOnProperty(name="apex.ops03.owner-enabled",havingValue="true")
public final class OrderEvidenceHandler {
 private final DataSource db;private final Environment env;private final ObjectProvider<ActiveOperations> active;
 private final UUID epoch=UUID.randomUUID();private final Semaphore slots=new Semaphore(1);private final MessagingEvidenceReader messaging;
 public OrderEvidenceHandler(OrderEvidenceDataSource.Pool pool,Environment env,ObjectProvider<ActiveOperations> active){
 this.db=pool.dataSource();this.env=env;this.active=active;
 if(!env.getProperty("apex.grpc.security.enabled",Boolean.class,false)||env.getProperty("apex.grpc.insecure-loopback-test-only",Boolean.class,false))throw new IllegalStateException("OPS_REQUIRES_MTLS_ACL");
 String configured=env.getProperty("apex.ops03.known-groups","");
 messaging=new MessagingEvidenceReader(configured.isBlank()?Set.of():Set.of(configured.split(",")));
 }
 @CellQueryHandler(type="OPS_OWNER_SNAPSHOT",version=1)
 public OwnerSnapshot read(EvidenceQuery query)throws SQLException {
 if(!slots.tryAcquire())throw new GatewayException(GatewayException.Code.RESOURCE_EXHAUSTED,false);
 Instant scanStartedAt=Instant.now();
 try(var c=db.getConnection()){
 c.setReadOnly(true);c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);c.setAutoCommit(false);
 var facts=new ArrayList<>(messaging.read(c,query.events()));
 for(UUID id:query.workflows())try(var q=c.prepareStatement("SELECT status,status_entered_at FROM orders WHERE id=?")){
 q.setQueryTimeout(1);q.setObject(1,id);try(var r=q.executeQuery()){
 if(r.next()){
 String state=r.getString(1);if(!Set.of("PENDING_INVENTORY","PENDING_PAYMENT","CANCELLING","CONFIRMED","CANCELLED","CREATED").contains(state))state="UNKNOWN";
 facts.add(new Fact(Kind.WORKFLOW,id,"order",state,EvidenceClass.OWNER_COMMITTED_ROW,Certainty.VERIFIED_WITHIN_SCOPE,MessagingEvidenceReader.time(r,2)));
 }else facts.add(MessagingEvidenceReader.unknown(Kind.WORKFLOW,id,"order"));}}
 // Receipt lookup is by restricted request UUID only. Never read identity/key/fingerprint/response.
 for(UUID id:query.events())try(var q=c.prepareStatement("SELECT completed_at FROM order_request_receipt WHERE request_id=? LIMIT 2")){
 q.setQueryTimeout(1);q.setObject(1,id);try(var r=q.executeQuery()){
 if(r.next()){var time=MessagingEvidenceReader.time(r,1);boolean ambiguous=r.next();facts.add(ambiguous?MessagingEvidenceReader.unknown(Kind.RECEIPT,id,"request"):new Fact(Kind.RECEIPT,id,"request",time==null?"PENDING":"COMMITTED",EvidenceClass.OWNER_COMMITTED_ROW,Certainty.VERIFIED_WITHIN_SCOPE,time));}
 else facts.add(MessagingEvidenceReader.unknown(Kind.RECEIPT,id,"request"));}}
 try(var probe=c.prepareStatement("SELECT 1")){probe.setQueryTimeout(1);probe.executeQuery().close();}
 facts.add(new Fact(Kind.HEALTH,new UUID(0,0),"owner-db-read","READABLE",EvidenceClass.ATTEMPT_OBSERVATION,Certainty.OBSERVED_ONLY,null));
 c.rollback();
 var registry=active.getIfAvailable();var entries=registry==null?List.<Attempt>of():registry.snapshot().stream().limit(64).map(e->new Attempt(e.attemptId(),e.bootEpoch(),e.operation().name(),e.elapsedNanos())).toList();
 return new OwnerSnapshot(1,env.getRequiredProperty("apex.ops03.environment"),"order-cell",env.getRequiredProperty("apex.ops03.instance"),epoch,scanStartedAt,10000,"EXPLICIT_KEYS_PARTIAL_NO_HISTORY",facts,entries,registry!=null,registry==null?0:registry.expiredCount(),registry==null?0:registry.rejectedCount());
 }finally{slots.release();}
 }
}
