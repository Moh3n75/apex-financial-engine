package com.apex.payment.operations;
import com.apex.operations.contracts.*;import com.apex.operations.contracts.OwnerSnapshot.*;
import com.apex.operations.owner.MessagingEvidenceReader;import com.apex.observability.ActiveOperations;
import com.apex.platform.grpc.api.*;
import javax.sql.DataSource;import java.sql.*;import java.time.*;import java.util.*;import java.util.concurrent.Semaphore;
import org.springframework.beans.factory.ObjectProvider;import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
/** Owner-local current row metadata. created_at is not a state-entry or commit timestamp. */
@Component
@ConditionalOnProperty(name="apex.ops03.owner-enabled",havingValue="true")
public final class PaymentEvidenceHandler {
 private final DataSource db;private final Environment env;private final ObjectProvider<ActiveOperations> active;
 private final org.springframework.boot.availability.ApplicationAvailability availability;
 private final UUID epoch=UUID.randomUUID();private final Semaphore slots=new Semaphore(1);private final MessagingEvidenceReader messaging;
 public PaymentEvidenceHandler(PaymentEvidenceDataSource.Pool pool,Environment env,ObjectProvider<ActiveOperations> active,org.springframework.boot.availability.ApplicationAvailability availability){
 this.db=pool.dataSource();this.env=env;this.active=active;this.availability=availability;
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
 for(UUID id:query.workflows())try(var q=c.prepareStatement("SELECT status,created_at FROM payment_authorizations WHERE order_id=?")){
 q.setQueryTimeout(1);q.setObject(1,id);try(var r=q.executeQuery()){
 if(r.next()){
 String state=r.getString(1);boolean recognized=Set.of("PENDING","AUTHORIZED","DECLINED").contains(state);
 facts.add(new Fact(Kind.WORKFLOW,id,"authorization-simulation",recognized?state:"UNKNOWN",EvidenceClass.OWNER_COMMITTED_ROW,recognized?Certainty.VERIFIED_WITHIN_SCOPE:Certainty.UNKNOWN,MessagingEvidenceReader.time(r,2)));
 }else facts.add(MessagingEvidenceReader.unknown(Kind.WORKFLOW,id,"authorization-simulation"));}}
 try(var probe=c.prepareStatement("SELECT 1")){probe.setQueryTimeout(1);probe.executeQuery().close();}
 facts.add(new Fact(Kind.HEALTH,new UUID(0,0),"owner-db-read","READABLE",EvidenceClass.ATTEMPT_OBSERVATION,Certainty.OBSERVED_ONLY,null));
 facts.add(new Fact(Kind.HEALTH,new UUID(0,0),"application-readiness",availability.getReadinessState().name(),EvidenceClass.ATTEMPT_OBSERVATION,Certainty.OBSERVED_ONLY,null));
 c.rollback();
 var registry=active.getIfAvailable();var local=registry==null?java.util.List.<ActiveOperations.Entry>of():registry.snapshot();
 facts.add(new Fact(Kind.HEALTH,new UUID(0,0),"active-coverage",registry==null?"NOT_INSTRUMENTED":local.size()>64?"TRUNCATED":"KNOWN_LOCAL_REGISTRY",EvidenceClass.ATTEMPT_OBSERVATION,Certainty.OBSERVED_ONLY,null));
 var entries=registry==null?List.<Attempt>of():local.stream().limit(64).map(e->new Attempt(e.attemptId(),e.bootEpoch(),e.operation().name(),e.elapsedNanos())).toList();
 return new OwnerSnapshot(1,env.getRequiredProperty("apex.ops03.environment"),"payment-cell",env.getRequiredProperty("apex.ops03.instance"),epoch,scanStartedAt,10000,"EXPLICIT_KEYS_PARTIAL_NO_HISTORY",facts,entries,registry!=null,registry==null?0:registry.expiredCount(),registry==null?0:registry.rejectedCount());
 }finally{slots.release();}
 }
}
